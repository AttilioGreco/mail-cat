use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::Stream;
use tokio::sync::Semaphore;
use tracing::{debug, info};

use mc_domain::error::DomainError;
use mc_domain::model::mailbox::Mailbox;
use mc_domain::model::message::{Message, OutgoingMessage};
use mc_domain::traits::email_provider::{
    EmailProvider, FetchRange, MailboxStatus, MessageBodyResult, MessageEnvelopeSummary,
    ProviderNotification, UidFlagEntry,
};
use mc_domain::traits::login_gate::LoginGate;
use mc_domain::value_objects::flag::Flag;
use mc_domain::value_objects::mailbox_path::MailboxPath;
use mc_domain::value_objects::uid::ProviderUid;

use crate::email::smtp::SmtpSender;
use crate::persistence::DownloadMeter;

use self::commands::{
    ServerCaps, append_timeout, delete_uids, exchange, known_caps, message_id_search, move_uids,
    sorted_uids, timed,
};
use self::fetch::isolating;
#[cfg(test)]
use self::{
    commands::{copied_uids, uid_flags},
    fetch::{TextPart, message_start, text_bodies, text_parts, whole_message_text},
    session::{XOAuth2Authenticator, log_in, login_error, xoauth2_rejects_token},
};
use super::mapper::{flag_to_imap, mailbox_path_to_string};
#[cfg(test)]
use mc_domain::traits::login_gate::LoginSecret;

type ImapSession = async_imap::Session<
    async_native_tls::TlsStream<tokio_util::compat::Compat<tokio::net::TcpStream>>,
>;

/// Simultaneous pooled connections per account. Gmail caps an account at ~15
/// connections shared with every other client (phone, webmail); the IDLE watcher
/// holds one more outside the pool.
const MAX_POOLED_CONNECTIONS: usize = 4;

/// TCP + TLS + login (+ CAPABILITY).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// One ordinary command and its reply (SELECT, STORE, COPY/MOVE, SEARCH…).
const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);
/// A body or envelope batch: up to `FETCH_BATCH_SIZE` messages in one reply.
const BODY_FETCH_TIMEOUT: Duration = Duration::from_secs(120);
/// IDLE is re-issued this often: RFC 2177 has servers drop it after 30
/// minutes, and NATs often forget a silent connection sooner.
const IDLE_REISSUE: Duration = Duration::from_secs(25 * 60);
/// What the backfill downloads of each text part: all of nearly any mail,
/// while a huge HTML newsletter or pasted log can't eat the daily cap.
/// Opening the message fetches the rest.
const TEXT_PART_CAP: u32 = 256 * 1024;
/// APPEND gets `COMMAND_TIMEOUT` plus a second per this many bytes.
const UPLOAD_FLOOR_BYTES_PER_SEC: u64 = 64 * 1024;

/// Servers silently drop idle connections, so one parked longer than this gets a
/// NOOP before reuse instead of failing the caller's first real command.
const REVALIDATE_AFTER: Duration = Duration::from_secs(30);

/// IMAP email provider implementation.
///
/// Uses async-native-tls for TLS (natively futures-compatible with async-imap).
/// tokio TcpStream is wrapped via tokio_util::compat before passing to async-native-tls.
pub struct ImapEmailProvider {
    host: String,
    port: u16,
    username: String,
    /// Every login asks it for the secret and reports back to it.
    gate: Arc<dyn LoginGate>,
    use_tls: bool,
    /// Pre-built SMTP sender. `None` when no SMTP config was provided for this
    /// account — sending will be rejected with a clear error.
    smtp_sender: Option<SmtpSender>,
    idle_sessions: Mutex<Vec<(ImapSession, Instant)>>,
    permits: Arc<Semaphore>,
    meter: Option<DownloadMeter>,
    /// Learned on the first login, or by [`known_caps`] before a move or delete.
    caps: OnceLock<ServerCaps>,
}

#[async_trait]
impl EmailProvider for ImapEmailProvider {
    async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
        self.list_all_mailboxes().await
    }

    async fn fetch_message_envelopes(
        &self,
        mailbox: &MailboxPath,
        range: FetchRange,
    ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
        self.envelopes(mailbox, range).await
    }

    async fn fetch_message_detail(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
    ) -> Result<Message, DomainError> {
        let mailbox_name = super::mapper::mailbox_path_to_string(mailbox);
        let MessageBodyResult {
            body_text,
            body_html,
            ..
        } = self
            .fetch_one_body(&mailbox_name, uid.value())
            .await?
            .ok_or_else(|| DomainError::Provider("Message not found".into()))?;

        // Produce a minimal Message with only body fields populated.
        // SyncService::fetch_message_body only reads body_text / body_html / attachments.
        let now = chrono::Utc::now();
        let empty_envelope = mc_domain::Envelope {
            from: vec![],
            to: vec![],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            subject: String::new(),
            date: now,
        };
        let (msg, _event) = Message::receive(
            mc_domain::AccountId::new(),
            mc_domain::MailboxId::new(),
            uid,
            None,
            None,
            vec![],
            empty_envelope,
            vec![],
            0,
            false,
            now,
        );
        Ok(Message {
            body_text,
            body_html,
            ..msg
        })
    }

    async fn fetch_bodies_bulk(
        &self,
        mailbox: &MailboxPath,
        uids: &[ProviderUid],
    ) -> Result<Vec<MessageBodyResult>, DomainError> {
        if uids.is_empty() {
            return Ok(vec![]);
        }

        let mailbox_name = super::mapper::mailbox_path_to_string(mailbox);
        let uid_nums: Vec<u32> = uids.iter().map(|u| u.value()).collect();
        let mut results = Vec::with_capacity(uids.len());
        for chunk in uid_nums.chunks(Self::FETCH_BATCH_SIZE) {
            debug!("Bulk body fetch: {} UIDs", chunk.len());
            results.extend(
                isolating(
                    chunk,
                    |set| self.text_bodies_in(&mailbox_name, set, Some(TEXT_PART_CAP)),
                    |uid| self.message_start_in(&mailbox_name, uid),
                )
                .await?,
            );
        }
        Ok(results)
    }

    async fn find_message(
        &self,
        mailbox: &MailboxPath,
        rfc_id: &str,
    ) -> Result<Vec<ProviderUid>, DomainError> {
        let Some(query) = message_id_search(rfc_id)? else {
            return Ok(Vec::new());
        };
        let mut session = self.selected(&mailbox_path_to_string(mailbox)).await?;
        let found = exchange(COMMAND_TIMEOUT, "Search", session.uid_search(query)).await?;
        self.release(session);
        Ok(sorted_uids(found))
    }

    async fn uids_from(
        &self,
        mailbox: &MailboxPath,
        from: ProviderUid,
    ) -> Result<Vec<ProviderUid>, DomainError> {
        let mut session = self.selected(&mailbox_path_to_string(mailbox)).await?;
        let found = exchange(
            COMMAND_TIMEOUT,
            "Search",
            session.uid_search(format!("UID {}:*", from.value())),
        )
        .await?;
        self.release(session);
        // `n:*` always matches the last message, even when its UID is below n.
        Ok(sorted_uids(found)
            .into_iter()
            .filter(|uid| *uid >= from)
            .collect())
    }

    async fn fetch_raw_message(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
    ) -> Result<Vec<u8>, DomainError> {
        let mut session = self
            .selected(&super::mapper::mailbox_path_to_string(mailbox))
            .await?;
        let raw = Self::fetch_whole(&mut session, uid.value())
            .await?
            .ok_or_else(|| DomainError::Provider("Message not found".into()))?;
        self.release(session);
        self.metered(raw.len() as u64).await;
        Ok(raw)
    }

    async fn set_flags(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
        flags_to_add: &[Flag],
        flags_to_remove: &[Flag],
    ) -> Result<(), DomainError> {
        self.change_flags(mailbox, uid, flags_to_add, flags_to_remove)
            .await
    }

    async fn set_flags_bulk(
        &self,
        mailbox: &MailboxPath,
        uids: &[ProviderUid],
        flags_to_add: &[Flag],
        flags_to_remove: &[Flag],
    ) -> Result<(), DomainError> {
        self.change_flags_bulk(mailbox, uids, flags_to_add, flags_to_remove)
            .await
    }

    async fn move_message(
        &self,
        uid: ProviderUid,
        from: &MailboxPath,
        to: &MailboxPath,
    ) -> Result<Option<ProviderUid>, DomainError> {
        let (from_name, to_name) = (mailbox_path_to_string(from), mailbox_path_to_string(to));
        info!(
            "Moving message {} from {} to {}",
            uid.value(),
            from_name,
            to_name
        );

        let mut session = self.selected(&from_name).await?;
        let caps = known_caps(&self.caps, &mut session).await?;
        let new_uid = timed(
            COMMAND_TIMEOUT,
            "Move",
            move_uids(&mut session, caps, &[uid.value()], &to_name),
        )
        .await?
        .first()
        .map(|&(_, new)| ProviderUid::new(new));
        self.release(session);

        info!(new_uid = ?new_uid, "Message moved successfully");
        Ok(new_uid)
    }

    async fn delete_message(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
        permanently: bool,
    ) -> Result<(), DomainError> {
        let mailbox_name = mailbox_path_to_string(mailbox);
        info!(
            "Deleting message {} from {} (permanent: {})",
            uid.value(),
            mailbox_name,
            permanently
        );

        let mut session = self.selected(&mailbox_name).await?;
        let caps = known_caps(&self.caps, &mut session).await?;
        timed(
            COMMAND_TIMEOUT,
            "Delete",
            delete_uids(&mut session, caps, &[uid.value()], permanently),
        )
        .await?;
        self.release(session);

        info!("Message deleted successfully");
        Ok(())
    }

    /// One session, one `UID STORE` over the whole set — the whole point of
    /// this override. Mirrors `set_flags_bulk`: same range compression, same
    /// chunking to stay under the server's command-line limit.
    async fn delete_messages(
        &self,
        mailbox: &MailboxPath,
        uids: &[ProviderUid],
        permanently: bool,
    ) -> Result<(), DomainError> {
        if uids.is_empty() {
            return Ok(());
        }

        let mailbox_name = mailbox_path_to_string(mailbox);
        info!(
            "Deleting {} messages from {} (permanent: {})",
            uids.len(),
            mailbox_name,
            permanently
        );

        let mut session = self.selected(&mailbox_name).await?;
        let caps = known_caps(&self.caps, &mut session).await?;
        // compress_uid_ranges assumes sorted input — the caller's order is
        // whatever the selection was, so sort here rather than trusting it.
        let mut uid_nums: Vec<u32> = uids.iter().map(|u| u.value()).collect();
        uid_nums.sort_unstable();
        for chunk in uid_nums.chunks(Self::FETCH_BATCH_SIZE) {
            timed(
                COMMAND_TIMEOUT,
                "Delete",
                delete_uids(&mut session, caps, chunk, permanently),
            )
            .await?;
        }
        self.release(session);

        info!("Bulk delete of {} messages completed", uid_nums.len());
        Ok(())
    }

    /// One session and one command set per chunk of the whole selection,
    /// instead of a session per message.
    async fn move_messages(
        &self,
        uids: &[ProviderUid],
        from: &MailboxPath,
        to: &MailboxPath,
    ) -> Result<HashMap<ProviderUid, ProviderUid>, DomainError> {
        if uids.is_empty() {
            return Ok(HashMap::new());
        }

        let (from_name, to_name) = (mailbox_path_to_string(from), mailbox_path_to_string(to));
        info!(
            "Moving {} messages from {} to {}",
            uids.len(),
            from_name,
            to_name
        );

        let mut session = self.selected(&from_name).await?;
        let caps = known_caps(&self.caps, &mut session).await?;
        let mut uid_nums: Vec<u32> = uids.iter().map(|u| u.value()).collect();
        uid_nums.sort_unstable();
        let mut moved = HashMap::new();
        for chunk in uid_nums.chunks(Self::FETCH_BATCH_SIZE) {
            let pairs = timed(
                COMMAND_TIMEOUT,
                "Move",
                move_uids(&mut session, caps, chunk, &to_name),
            )
            .await?;
            moved.extend(
                pairs
                    .into_iter()
                    .map(|(old, new)| (ProviderUid::new(old), ProviderUid::new(new))),
            );
        }
        self.release(session);

        info!("Bulk move of {} messages completed", uid_nums.len());
        Ok(moved)
    }

    async fn send_message(&self, message: &OutgoingMessage) -> Result<(), DomainError> {
        let sender = self.smtp_sender.as_ref().ok_or_else(|| {
            DomainError::Provider(
                "No SMTP configuration for this account — add an [smtp] block to config.toml"
                    .to_owned(),
            )
        })?;
        sender.send(message).await
    }

    async fn append_message(
        &self,
        mailbox: &MailboxPath,
        message: &OutgoingMessage,
        flags: &[Flag],
    ) -> Result<Option<ProviderUid>, DomainError> {
        let raw = crate::email::smtp::format_for_storage(message)?;
        let flags = format!(
            "({})",
            flags.iter().map(flag_to_imap).collect::<Vec<_>>().join(" ")
        );

        let mut session = self.session().await?;
        let mailbox_name = mailbox_path_to_string(mailbox);
        exchange(
            append_timeout(raw.len()),
            "APPEND",
            session.append(&mailbox_name, Some(&flags), None, &raw),
        )
        .await?;

        // async-imap drops the APPENDUID response code, so find the copy by its
        // Message-ID — works on every server, UIDPLUS or not.
        let query = match &message.message_rfc_id {
            Some(rfc_id) => message_id_search(rfc_id)?,
            None => None,
        };
        let uid = match query {
            Some(query) => {
                exchange(
                    COMMAND_TIMEOUT,
                    "Mailbox select",
                    session.select(&mailbox_name),
                )
                .await?;
                exchange(COMMAND_TIMEOUT, "UID SEARCH", session.uid_search(query))
                    .await?
                    .into_iter()
                    .max()
                    .map(ProviderUid::new)
            }
            None => None,
        };
        self.release(session);
        Ok(uid)
    }

    async fn mailbox_status(&self, mailbox: &MailboxPath) -> Result<MailboxStatus, DomainError> {
        self.status_of(mailbox).await
    }

    async fn fetch_uid_flags(
        &self,
        mailbox: &MailboxPath,
    ) -> Result<Vec<UidFlagEntry>, DomainError> {
        self.uid_flags_of(mailbox).await
    }

    fn watch_mailbox(
        &self,
        mailbox: &MailboxPath,
    ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>> {
        self.watch_stream(mailbox)
    }
}

pub use self::session::asks_the_user_to_act;

mod commands;
mod fetch;
mod mailbox;
mod session;

#[cfg(test)]
mod tests;
