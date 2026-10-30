//! Mailbox listing, flag changes and IDLE watching.

use std::collections::HashSet;
use std::pin::Pin;
use std::time::Duration;

use async_imap::types::NameAttribute;
use futures::Stream;
use tracing::{debug, info, warn};

use mc_domain::error::DomainError;
use mc_domain::model::mailbox::{Mailbox, MailboxType, leaf_name};
use mc_domain::traits::email_provider::ProviderNotification;
use mc_domain::value_objects::flag::Flag;
use mc_domain::value_objects::mailbox_path::MailboxPath;
use mc_domain::value_objects::remote_id::RemoteId;

use super::super::mapper::{self, flag_to_imap, mailbox_path_to_string};
use super::commands::{exchange, store, uids_of};
use super::{COMMAND_TIMEOUT, IDLE_REISSUE, ImapEmailProvider};

impl ImapEmailProvider {
    pub(super) fn watch_stream(
        &self,
        mailbox: &MailboxPath,
    ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>> {
        let mailbox_name = mailbox_path_to_string(mailbox);
        // Clone the original MailboxPath so notifications carry the same path
        // (with the same delimiter) as the one stored in sled from list_mailboxes.
        // Do NOT reconstruct from the string: splitting on '/' would hardcode the
        // wrong delimiter and break PartialEq comparisons downstream.
        let mb_path = mailbox.clone();

        Box::pin(async_stream::stream! {
            use async_imap::extensions::idle::IdleResponse;

            info!("[watch] Starting IMAP IDLE for mailbox '{}'", mailbox_name);

            loop {
                // The gate knows when to connect: not while the account waits for
                // the user, and on the offline schedule after a failure.
                self.gate.ready().await;
                let mut session = match self.create_imap_session().await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("[watch] Session creation failed for '{}': {}", mailbox_name, e);
                        // A floor for errors the gate does not schedule.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                let select_data = match exchange(COMMAND_TIMEOUT, "SELECT", session.select(&mailbox_name)).await {
                    Ok(mb) => mb,
                    Err(e) => {
                        warn!("[watch] SELECT '{}' failed: {}", mailbox_name, e);
                        let _ = exchange(COMMAND_TIMEOUT, "LOGOUT", session.logout()).await;
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                };
                // A Gmail view's UIDs are not the ones its messages are stored
                // under, so a flag change in it is left to the sync of All Mail.
                let view = match self.layout_on(&mut session).await {
                    Ok(layout) => layout.is_some_and(|l| l.is_view(&mailbox_name)),
                    Err(e) => {
                        warn!("[watch] LIST for '{}' failed: {}", mailbox_name, e);
                        false
                    }
                };
                // Remember the next expected UID so we can detect new arrivals after IDLE.
                let uid_next = select_data.uid_next.unwrap_or(1);

                info!("[watch] IDLE active for '{}'", mailbox_name);
                let mut idle = session.idle();
                // init() sends the IDLE command to the server — required before wait_with_timeout
                if let Err(e) = exchange(COMMAND_TIMEOUT, "IDLE", idle.init()).await {
                    warn!("[watch] IDLE init failed: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    continue;
                }
                let (wait_future, _stop_source) =
                    idle.wait_with_timeout(IDLE_REISSUE);

                let idle_response = match wait_future.await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("[watch] IDLE wait error: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };

                // DONE must be answered promptly; one that is not means the
                // connection died during the wait.
                session = match exchange(COMMAND_TIMEOUT, "IDLE DONE", idle.done()).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("[watch] IDLE done error: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                };

                // Inspect response to classify the event
                let raw_upper = match &idle_response {
                    IdleResponse::NewData(data) => {
                        let raw = String::from_utf8_lossy(data.borrow_owner()).to_ascii_uppercase();
                        info!("[watch] IDLE NewData for '{}': {}", mailbox_name, raw.trim());
                        raw
                    }
                    IdleResponse::Timeout => {
                        info!("[watch] IDLE timeout for '{}', reconnecting", mailbox_name);
                        let _ = exchange(COMMAND_TIMEOUT, "LOGOUT", session.logout()).await;
                        continue;
                    }
                    IdleResponse::ManualInterrupt => {
                        info!("[watch] IDLE interrupted for '{}'", mailbox_name);
                        let _ = exchange(COMMAND_TIMEOUT, "LOGOUT", session.logout()).await;
                        break;
                    }
                };

                let has_flags   = raw_upper.contains("FETCH") && raw_upper.contains("FLAGS");
                let has_expunge = raw_upper.contains("EXPUNGE");
                let has_exists  = raw_upper.contains("EXISTS");

                if has_flags && !has_exists && !has_expunge && !view {
                    // Pure flag change (e.g. marking read in another client).
                    // Parse the sequence number from "* N FETCH ..." to resolve UID + flags.
                    let seq_num: Option<u32> = raw_upper
                        .split_whitespace()
                        .nth(1) // skip "*", get seq num
                        .and_then(|s| s.parse().ok());

                    if let Some(seq) = seq_num {
                        use futures::TryStreamExt;
                        let fetches: Result<Vec<async_imap::types::Fetch>, _> =
                            exchange(COMMAND_TIMEOUT, "FETCH (UID FLAGS)", async {
                                session.fetch(seq.to_string(), "(UID FLAGS)").await?.try_collect().await
                            })
                            .await;
                        match fetches {
                            Ok(items) => {
                                for fetch_data in &items {
                                    let flags: Vec<Flag> = fetch_data
                                        .flags()
                                        .filter_map(|f| mapper::imap_flag_to_domain(&f))
                                        .collect();
                                    // No UID is a spurious EXISTS echo, not a flag change.
                                    let Some(uid) = fetch_data.uid else { continue };
                                    info!("[watch] FlagsChanged in '{}': uid={} flags={:?}",
                                        mailbox_name, uid, flags);
                                    yield Ok(ProviderNotification::FlagsChanged {
                                        mailbox: mb_path.clone(),
                                        remote_id: RemoteId::from(uid),
                                        flags,
                                    });
                                }
                            }
                            Err(e) => {
                                warn!("[watch] FETCH (UID FLAGS) failed for seq {}: {}", seq, e);
                                // A timed-out reply may still arrive: never reuse that session.
                                if matches!(e, DomainError::ConnectionTimeout(_)) {
                                    continue;
                                }
                            }
                        }
                    } else {
                        warn!("[watch] Could not parse seq num from IDLE response");
                    }
                } else {
                    // EXISTS (new messages) and/or EXPUNGE.

                    // If new messages arrived, fetch their UIDs and emit NewMessage
                    // for each one. We use uid_next captured from SELECT: any UID
                    // >= uid_next is a message that wasn't there when IDLE started.
                    if has_exists {
                        use futures::TryStreamExt as _;
                        let range = format!("{}:*", uid_next);
                        match exchange(COMMAND_TIMEOUT, "UID FETCH", async {
                            session.uid_fetch(&range, "FLAGS").await?.try_collect::<Vec<_>>().await
                        })
                        .await
                        {
                                Ok(fetches) => {
                                    for fetch in &fetches {
                                        if let Some(uid) = fetch.uid
                                            && uid >= uid_next
                                        {
                                            info!("[watch] NewMessage in '{}': uid={}", mailbox_name, uid);
                                            yield Ok(ProviderNotification::NewMessage {
                                                mailbox: mb_path.clone(),
                                                remote_id: RemoteId::from(uid),
                                            });
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("[watch] UID FETCH for new messages failed: {}", e);
                                    if matches!(e, DomainError::ConnectionTimeout(_)) {
                                        continue;
                                    }
                                }
                        }
                    }

                    // Always emit updated counts (covers both new messages and expunges).
                    let updated_mb = match exchange(COMMAND_TIMEOUT, "STATUS", session.status(&mailbox_name, "(MESSAGES UNSEEN)")).await {
                        Ok(mb) => mb,
                        Err(e) => {
                            warn!("[watch] STATUS failed: {}", e);
                            let _ = exchange(COMMAND_TIMEOUT, "LOGOUT", session.logout()).await;
                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                            continue;
                        }
                    };
                    info!("[watch] CountsChanged in '{}': total={} unseen={}",
                        mailbox_name, updated_mb.exists, updated_mb.unseen.unwrap_or(0));
                    yield Ok(ProviderNotification::MailboxCountChanged {
                        mailbox: mb_path.clone(),
                        total: updated_mb.exists,
                        unseen: updated_mb.unseen.unwrap_or(0),
                    });
                }

                let _ = exchange(COMMAND_TIMEOUT, "LOGOUT", session.logout()).await;
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        })
    }
}

/// One LIST entry, with what the Gmail layout needs besides the mailbox.
pub(super) struct Listed {
    pub(super) mailbox: Mailbox,
    /// The name as the server spells it (modified UTF-7), which commands and
    /// X-GM-LABELS use.
    pub(super) name: String,
    /// Gmail's Starred, marked `\Flagged` in LIST.
    pub(super) starred: bool,
}

pub(super) async fn list_on<T>(
    session: &mut async_imap::Session<T>,
    gmail: bool,
) -> Result<Vec<Listed>, DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    use futures::stream::StreamExt;
    use mc_domain::model::account::AccountId;
    use mc_domain::model::mailbox::MailboxId;

    debug!("Listing mailboxes");
    let mailbox_list: Vec<Result<_, _>> = exchange(COMMAND_TIMEOUT, "List mailboxes", async {
        Ok(session
            .list(None, Some("*"))
            .await?
            .collect::<Vec<_>>()
            .await)
    })
    .await?;

    let mut listed = Vec::new();
    for result in mailbox_list {
        let mb = result.map_err(|e| DomainError::Provider(format!("Mailbox name error: {}", e)))?;

        let imap_name = &mapper::unescape_quoted(mb.name());
        let delimiter_str = mb.delimiter().unwrap_or("/");
        let delimiter = delimiter_str.chars().next().unwrap_or('/');

        let path = mapper::parse_mailbox_path(imap_name, delimiter)?;

        // Classify from SPECIAL-USE attributes when the server sends them.
        let mailbox_type = if gmail {
            mapper::classify_gmail_mailbox(imap_name, mb.attributes())
        } else {
            mapper::classify_mailbox(imap_name, mb.attributes())
        };

        // Create placeholder Mailbox - account_id and id will be set during sync
        let display_path = mapper::decode_utf7(&path.full_path()).into_owned();
        listed.push(Listed {
            name: imap_name.to_owned(),
            starred: mb.attributes().contains(&NameAttribute::Flagged),
            mailbox: Mailbox {
                id: MailboxId::new(),
                account_id: AccountId::new(),
                // As discovery derives it, or every sync would see a rename.
                display_name: leaf_name(&display_path, delimiter).to_owned(),
                display_path,
                path,
                mailbox_type,
                flags: Vec::new(),
                total_messages: 0,
                unread_messages: 0,
                subscribed: true,
                subscription_explicit: false,
            },
        });
    }
    info!("Listed {} mailboxes", listed.len());
    Ok(listed)
}

/// The names `LSUB` lists, as `LIST` spells them. `None` when the server has
/// no usable answer (unsupported, refused, or an empty list): no subscription
/// was ever made there, so nothing can be told unsubscribed.
pub(super) async fn lsub_names<T>(session: &mut async_imap::Session<T>) -> Option<HashSet<String>>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    use futures::stream::StreamExt;

    let reply = exchange(COMMAND_TIMEOUT, "LSUB", async {
        Ok(session
            .lsub(None, Some("*"))
            .await?
            .collect::<Vec<_>>()
            .await)
    })
    .await;
    let names: HashSet<String> = match reply {
        Ok(reply) => reply
            .into_iter()
            .filter_map(Result::ok)
            .map(|n| mapper::unescape_quoted(n.name()))
            .collect(),
        Err(e) => {
            warn!("LSUB failed, treating every mailbox as subscribed: {e}");
            return None;
        }
    };
    (!names.is_empty()).then_some(names)
}

/// Whether the user sees and syncs `listed`, given what `LSUB` returned.
pub(super) fn is_subscribed(listed: &Listed, lsub: Option<&HashSet<String>>, gmail: bool) -> bool {
    match listed.mailbox.mailbox_type {
        MailboxType::Inbox => true,
        // Gmail's Trash and Spam are large and rarely wanted: opt in.
        MailboxType::Trash | MailboxType::Spam if gmail => false,
        _ => lsub.is_none_or(|names| names.contains(&listed.name)),
    }
}

pub(super) async fn set_subscribed_on<T>(
    session: &mut async_imap::Session<T>,
    name: &str,
    subscribed: bool,
) -> Result<(), DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    if subscribed {
        return exchange(COMMAND_TIMEOUT, "SUBSCRIBE", session.subscribe(name)).await;
    }
    exchange(COMMAND_TIMEOUT, "UNSUBSCRIBE", session.unsubscribe(name)).await
}

impl ImapEmailProvider {
    pub(super) async fn list_all_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
        let mut session = self.session().await?;
        let mut listed = list_on(&mut session, self.has_gmail_ext()).await?;
        let lsub = lsub_names(&mut session).await;
        self.release(session);
        if self.has_gmail_ext() {
            self.learn_layout(&listed);
        }
        let gmail = self.layout().is_some();
        for l in &mut listed {
            l.mailbox.subscribed = is_subscribed(l, lsub.as_ref(), gmail);
        }
        Ok(listed.into_iter().map(|l| l.mailbox).collect())
    }

    pub(super) async fn subscribe(
        &self,
        mailbox: &MailboxPath,
        subscribed: bool,
    ) -> Result<(), DomainError> {
        let mut session = self.session().await?;
        set_subscribed_on(&mut session, &mailbox_path_to_string(mailbox), subscribed).await?;
        self.release(session);
        Ok(())
    }

    pub(super) async fn change_flags_bulk(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
        flags_to_add: &[Flag],
        flags_to_remove: &[Flag],
    ) -> Result<(), DomainError> {
        if ids.is_empty() {
            return Ok(());
        }

        info!(
            "Setting flags for {} messages in mailbox {}",
            ids.len(),
            mailbox_path_to_string(mailbox)
        );

        let mut session = self.selected(&mailbox_path_to_string(mailbox)).await?;

        let mut uid_nums = uids_of(ids)?;
        uid_nums.sort_unstable();

        for chunk in uid_nums.chunks(Self::FETCH_BATCH_SIZE) {
            let query = Self::compress_uid_ranges(chunk);

            if !flags_to_add.is_empty() {
                let flags_str: Vec<String> = flags_to_add.iter().map(flag_to_imap).collect();
                let flags_query = flags_str.join(" ");

                store(
                    &mut session,
                    &query,
                    &format!("+FLAGS ({flags_query})"),
                    "Add flags",
                )
                .await?;

                debug!("Added flags: {} for {} UIDs", flags_query, chunk.len());
            }

            if !flags_to_remove.is_empty() {
                let flags_str: Vec<String> = flags_to_remove.iter().map(flag_to_imap).collect();
                let flags_query = flags_str.join(" ");

                store(
                    &mut session,
                    &query,
                    &format!("-FLAGS ({flags_query})"),
                    "Remove flags",
                )
                .await?;

                debug!("Removed flags: {} for {} UIDs", flags_query, chunk.len());
            }
        }

        self.release(session);

        info!("Bulk flags updated successfully");
        Ok(())
    }
}
