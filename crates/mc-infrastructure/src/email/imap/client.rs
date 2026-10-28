use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::Stream;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use mc_domain::error::DomainError;
use mc_domain::model::mailbox::Mailbox;
use mc_domain::model::message::{Message, OutgoingMessage};
use mc_domain::traits::email_provider::{
    EmailProvider, MailboxChanges, MessageBodyResult, MessageEnvelopeSummary, ProviderNotification,
    SendOutcome, SentCopy,
};
use mc_domain::traits::login_gate::LoginGate;
use mc_domain::value_objects::flag::Flag;
use mc_domain::value_objects::mailbox_path::MailboxPath;
use mc_domain::value_objects::remote_id::RemoteId;
use mc_domain::value_objects::sync_cursor::SyncCursor;

use crate::email::smtp::SmtpSender;
use crate::persistence::DownloadMeter;

use self::commands::{
    ServerCaps, append_timeout, delete_uids, exchange, known_caps, message_id_search, move_uids,
    sorted_uids, timed, uid_of, uids_of,
};
use self::fetch::isolating;
use self::session::Counted;
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
    async_native_tls::TlsStream<tokio_util::compat::Compat<Counted<tokio::net::TcpStream>>>,
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
    /// Bytes every connection of this account read off the socket, TLS
    /// included, since the meter last took them.
    received: Arc<AtomicU64>,
    /// Learned on the first login, or by [`known_caps`] before a move or delete.
    caps: OnceLock<ServerCaps>,
}

impl ImapEmailProvider {
    /// Known once any session logged in; a session is always opened first.
    pub(super) fn has_gmail_ext(&self) -> bool {
        self.caps.get().is_some_and(|caps| caps.gmail)
    }
}

#[async_trait]
impl EmailProvider for ImapEmailProvider {
    async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
        self.list_all_mailboxes().await
    }

    async fn mailbox_changes(
        &self,
        mailbox: &MailboxPath,
        since: Option<&SyncCursor>,
    ) -> Result<MailboxChanges, DomainError> {
        self.changes(mailbox, since).await
    }

    async fn fetch_envelopes(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
    ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
        self.envelopes(mailbox, ids).await
    }

    async fn fetch_message_detail(
        &self,
        mailbox: &MailboxPath,
        id: &RemoteId,
    ) -> Result<Message, DomainError> {
        let mailbox_name = super::mapper::mailbox_path_to_string(mailbox);
        let MessageBodyResult {
            body_text,
            body_html,
            ..
        } = self
            .fetch_one_body(&mailbox_name, uid_of(id)?)
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
            id.clone(),
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
        ids: &[RemoteId],
    ) -> Result<Vec<MessageBodyResult>, DomainError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let mailbox_name = super::mapper::mailbox_path_to_string(mailbox);
        let uids = uids_of(ids)?;
        let mut results = Vec::with_capacity(uids.len());
        for chunk in uids.chunks(Self::FETCH_BATCH_SIZE) {
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

    async fn find_by_message_id(
        &self,
        mailbox: &MailboxPath,
        rfc_id: &str,
    ) -> Result<Vec<RemoteId>, DomainError> {
        let Some(query) = message_id_search(rfc_id)? else {
            return Ok(Vec::new());
        };
        let mut session = self.selected(&mailbox_path_to_string(mailbox)).await?;
        let found = exchange(COMMAND_TIMEOUT, "Search", session.uid_search(query)).await?;
        self.release(session);
        Ok(sorted_uids(found))
    }

    async fn fetch_raw_message(
        &self,
        mailbox: &MailboxPath,
        id: &RemoteId,
    ) -> Result<Vec<u8>, DomainError> {
        let mut session = self
            .selected(&super::mapper::mailbox_path_to_string(mailbox))
            .await?;
        let raw = Self::fetch_whole(&mut session, uid_of(id)?)
            .await?
            .ok_or_else(|| DomainError::Provider("Message not found".into()))?;
        self.release(session);
        self.record_downloads().await;
        Ok(raw)
    }

    async fn set_flags_bulk(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
        flags_to_add: &[Flag],
        flags_to_remove: &[Flag],
    ) -> Result<(), DomainError> {
        self.change_flags_bulk(mailbox, ids, flags_to_add, flags_to_remove)
            .await
    }

    /// One session, one `UID STORE` over the whole set per chunk, with the
    /// same range compression and chunking as `set_flags_bulk`.
    async fn delete_messages(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
        permanently: bool,
    ) -> Result<(), DomainError> {
        if ids.is_empty() {
            return Ok(());
        }

        let mailbox_name = mailbox_path_to_string(mailbox);
        info!(
            "Deleting {} messages from {} (permanent: {})",
            ids.len(),
            mailbox_name,
            permanently
        );

        let mut session = self.selected(&mailbox_name).await?;
        let caps = known_caps(&self.caps, &mut session).await?;
        // compress_uid_ranges assumes sorted input — the caller's order is
        // whatever the selection was, so sort here rather than trusting it.
        let mut uids = uids_of(ids)?;
        uids.sort_unstable();
        for chunk in uids.chunks(Self::FETCH_BATCH_SIZE) {
            timed(
                COMMAND_TIMEOUT,
                "Delete",
                delete_uids(&mut session, caps, chunk, permanently),
            )
            .await?;
        }
        self.release(session);

        info!("Bulk delete of {} messages completed", uids.len());
        Ok(())
    }

    /// One session and one command set per chunk of the whole selection,
    /// instead of a session per message.
    async fn move_messages(
        &self,
        ids: &[RemoteId],
        from: &MailboxPath,
        to: &MailboxPath,
    ) -> Result<HashMap<RemoteId, Option<RemoteId>>, DomainError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let (from_name, to_name) = (mailbox_path_to_string(from), mailbox_path_to_string(to));
        info!(
            "Moving {} messages from {} to {}",
            ids.len(),
            from_name,
            to_name
        );

        let mut session = self.selected(&from_name).await?;
        let caps = known_caps(&self.caps, &mut session).await?;
        let mut uids = uids_of(ids)?;
        uids.sort_unstable();
        let mut moved: HashMap<RemoteId, Option<RemoteId>> = uids
            .iter()
            .map(|&uid| (RemoteId::from(uid), None))
            .collect();
        for chunk in uids.chunks(Self::FETCH_BATCH_SIZE) {
            let pairs = timed(
                COMMAND_TIMEOUT,
                "Move",
                move_uids(&mut session, caps, chunk, &to_name),
            )
            .await?;
            for (old, new) in pairs {
                moved.insert(RemoteId::from(old), Some(RemoteId::from(new)));
            }
        }
        self.release(session);

        info!("Bulk move of {} messages completed", uids.len());
        Ok(moved)
    }

    async fn send_message(
        &self,
        message: &OutgoingMessage,
        sent: Option<&MailboxPath>,
    ) -> Result<SendOutcome, DomainError> {
        let sender = self.smtp_sender.as_ref().ok_or_else(|| {
            DomainError::Provider(
                "No SMTP configuration for this account — add an [smtp] block to config.toml"
                    .to_owned(),
            )
        })?;
        sender.send(message).await?;

        let sent_copy = match sent {
            None => SentCopy::NotStored,
            Some(_) if files_submissions_itself(&self.host) => SentCopy::ServerFiled,
            Some(sent) => sent_copy_of(self.append(sent, message, &[Flag::Seen]).await),
        };
        Ok(SendOutcome { sent_copy })
    }

    async fn save_draft(
        &self,
        drafts: &MailboxPath,
        message: &OutgoingMessage,
        replacing: Option<&RemoteId>,
    ) -> Result<RemoteId, DomainError> {
        let appended = self
            .append(drafts, message, &[Flag::Draft, Flag::Seen])
            .await?;
        let remote_id = match appended {
            Some(id) => id,
            None => match message.message_rfc_id.as_deref() {
                Some(rfc_id) => self.find_by_message_id(drafts, rfc_id).await?.pop(),
                None => None,
            }
            .ok_or_else(|| {
                DomainError::Provider("draft stored but not found on the server".into())
            })?,
        };
        if let Some(old) = replacing.filter(|old| **old != remote_id)
            && let Err(e) = self
                .delete_messages(drafts, std::slice::from_ref(old), true)
                .await
        {
            warn!(%e, "new draft saved, but deleting the previous version failed");
        }
        Ok(remote_id)
    }

    fn watch_mailbox(
        &self,
        mailbox: &MailboxPath,
    ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>> {
        self.watch_stream(mailbox)
    }
}

impl ImapEmailProvider {
    /// APPEND `message` to `mailbox`; the id it landed on, when found.
    async fn append(
        &self,
        mailbox: &MailboxPath,
        message: &OutgoingMessage,
        flags: &[Flag],
    ) -> Result<Option<RemoteId>, DomainError> {
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
                    .map(RemoteId::from)
            }
            None => None,
        };
        self.release(session);
        Ok(uid)
    }
}

/// Gmail files every SMTP submission into "Sent Mail" itself; appending our
/// own copy there would show the message twice.
fn files_submissions_itself(host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    host.ends_with("gmail.com") || host.ends_with("googlemail.com")
}

/// The mail is out whatever happens to its copy: a failed APPEND is reported,
/// not propagated.
fn sent_copy_of(stored: Result<Option<RemoteId>, DomainError>) -> SentCopy {
    match stored {
        Ok(remote_id) => SentCopy::Stored { remote_id },
        Err(e) => {
            warn!(%e, "message sent, but storing the Sent copy failed");
            SentCopy::NotStored
        }
    }
}

pub use self::session::asks_the_user_to_act;

mod changes;
mod commands;
mod fetch;
mod mailbox;
mod session;

#[cfg(test)]
mod tests;
