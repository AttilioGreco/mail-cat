use std::pin::Pin;

use async_imap::types::Seq;
use async_trait::async_trait;
use futures::Stream;
use tracing::{debug, info, warn};

use mc_domain::error::DomainError;
use mc_domain::model::mailbox::Mailbox;
use mc_domain::model::message::{Message, OutgoingMessage};
use mc_domain::traits::email_provider::{
    EmailProvider, FetchRange, MailboxStatus, MessageEnvelopeSummary, ProviderNotification,
    UidFlagEntry,
};
use mc_domain::value_objects::flag::Flag;
use mc_domain::value_objects::mailbox_path::MailboxPath;
use mc_domain::value_objects::uid::ProviderUid;

use crate::email::smtp::SmtpSender;

use super::mapper::{flag_to_imap, mailbox_path_to_string};

/// IMAP email provider implementation.
///
/// Uses async-native-tls for TLS (natively futures-compatible with async-imap).
/// tokio TcpStream is wrapped via tokio_util::compat before passing to async-native-tls.
pub struct ImapEmailProvider {
    host: String,
    port: u16,
    username: String,
    password: String,
    use_tls: bool,
    // SMTP configuration for sending
    smtp_host: String,
    smtp_port: u16,
    smtp_use_tls: bool,
}

impl ImapEmailProvider {
    /// Create a new IMAP provider configuration.
    ///
    /// For SMTP, uses the same credentials but separate host/port.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        use_tls: bool,
        smtp_host: String,
        smtp_port: u16,
        smtp_use_tls: bool,
    ) -> Self {
        Self {
            host,
            port,
            username,
            password,
            use_tls,
            smtp_host,
            smtp_port,
            smtp_use_tls,
        }
    }

    /// Get configuration for display/debugging.
    pub fn config(&self) -> (&str, u16, &str, bool) {
        (&self.host, self.port, &self.username, self.use_tls)
    }

    /// Create a SmtpSender from this provider's SMTP configuration.
    fn create_smtp_sender(&self) -> SmtpSender {
        SmtpSender::new(
            self.smtp_host.clone(),
            self.smtp_port,
            self.username.clone(),
            self.password.clone(),
            self.smtp_use_tls,
        )
    }

    async fn create_imap_session(
        &self,
    ) -> Result<
        async_imap::Session<
            async_native_tls::TlsStream<tokio_util::compat::Compat<tokio::net::TcpStream>>,
        >,
        DomainError,
    > {
        use tokio_util::compat::TokioAsyncReadCompatExt;

        info!(
            "Connecting to IMAP: {}:{} (TLS: {})",
            self.host, self.port, self.use_tls
        );

        // Connect TCP, then wrap with compat for futures AsyncRead/Write compatibility
        let tcp_stream = tokio::net::TcpStream::connect((&self.host as &str, self.port))
            .await
            .map_err(|e| DomainError::Provider(format!("TCP connection failed: {}", e)))?;
        let compat_stream = tcp_stream.compat();

        // TLS via async-native-tls (natively futures-compatible, no compat needed)
        let tls = async_native_tls::TlsConnector::new();
        let tls_stream = tls
            .connect(&self.host, compat_stream)
            .await
            .map_err(|e| DomainError::Provider(format!("TLS connection failed: {}", e)))?;

        // Create IMAP client and login
        let client = async_imap::Client::new(tls_stream);
        let session = client
            .login(&self.username, &self.password)
            .await
            .map_err(|e| DomainError::Provider(format!("IMAP authentication failed: {:?}", e.0)))?;

        Ok(session)
    }

    /// Helper to convert ProviderUid to IMAP sequence.
    fn uid_to_seq(uid: &ProviderUid) -> Seq {
        Seq::from(uid.value())
    }

    /// Batch size for UID FETCH operations to avoid IMAP command line limits.
    const FETCH_BATCH_SIZE: usize = 500;

    /// Compress a sorted slice of UIDs into IMAP range notation.
    /// e.g. [1,2,3,5,6,10] -> "1:3,5:6,10"
    fn compress_uid_ranges(uids: &[u32]) -> String {
        if uids.is_empty() {
            return String::new();
        }

        let mut ranges = Vec::new();
        let mut start = uids[0];
        let mut end = uids[0];

        for &uid in &uids[1..] {
            if uid == end + 1 {
                end = uid;
            } else {
                if start == end {
                    ranges.push(format!("{}", start));
                } else {
                    ranges.push(format!("{}:{}", start, end));
                }
                start = uid;
                end = uid;
            }
        }

        // Push final range
        if start == end {
            ranges.push(format!("{}", start));
        } else {
            ranges.push(format!("{}:{}", start, end));
        }

        ranges.join(",")
    }

    /// Parse IMAP address list into domain Address list.
    fn parse_imap_addresses(
        addrs: Option<&Vec<async_imap::imap_proto::types::Address>>,
    ) -> Vec<mc_domain::Address> {
        addrs
            .map(|addrs| {
                addrs
                    .iter()
                    .filter_map(|addr| {
                        let mailbox_part = addr.mailbox.as_ref()?;
                        let host = addr.host.as_ref()?;
                        let email_str = format!(
                            "{}@{}",
                            String::from_utf8_lossy(mailbox_part),
                            String::from_utf8_lossy(host)
                        );
                        let email =
                            mc_domain::value_objects::email_address::EmailAddress::new(&email_str)
                                .ok()?;
                        let name = addr
                            .name
                            .as_ref()
                            .map(|n| decode_rfc2047_header(n));
                        Some(mc_domain::Address { email, name })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[async_trait]
impl EmailProvider for ImapEmailProvider {
    async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
        use futures::stream::StreamExt;
        use mc_domain::model::account::AccountId;
        use mc_domain::model::mailbox::MailboxId;

        debug!("Listing mailboxes");
        let mut session = self.create_imap_session().await?;

        let stream = session
            .list(None, Some("*"))
            .await
            .map_err(|e| DomainError::Provider(format!("List mailboxes failed: {}", e)))?;
        let mailbox_list: Vec<Result<_, _>> = stream.collect::<Vec<_>>().await;

        // Logout
        session
            .logout()
            .await
            .map_err(|e| DomainError::Provider(format!("Logout failed: {}", e)))?;

        // Convert IMAP mailbox names to Mailbox objects
        let mut mailboxes = Vec::new();
        for result in mailbox_list {
            let mb =
                result.map_err(|e| DomainError::Provider(format!("Mailbox name error: {}", e)))?;

            let imap_name = mb.name();
            let delimiter_str = mb.delimiter().unwrap_or("/");
            let delimiter = delimiter_str.chars().next().unwrap_or('/');

            // Parse mailbox path
            let path = super::mapper::parse_mailbox_path(imap_name, delimiter)?;

            // Infer mailbox type
            let mailbox_type = super::mapper::infer_mailbox_type(imap_name);

            // Create placeholder Mailbox - account_id and id will be set during sync
            let mailbox = Mailbox {
                id: MailboxId::new(),
                account_id: AccountId::new(),
                path,
                display_name: imap_name.to_string(),
                mailbox_type,
                flags: Vec::new(),
                total_messages: 0,
                unread_messages: 0,
                uid_validity: None,
                uid_next: None,
            };

            mailboxes.push(mailbox);
        }

        info!("Listed {} mailboxes", mailboxes.len());
        Ok(mailboxes)
    }

    async fn fetch_message_envelopes(
        &self,
        mailbox: &MailboxPath,
        range: FetchRange,
    ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
        debug!("Fetching envelopes from mailbox: {:?}", mailbox);
        let mut session = self.create_imap_session().await?;

        // Select mailbox
        let mailbox_name = super::mapper::mailbox_path_to_string(mailbox);
        session.select(&mailbox_name)
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox select failed: {}", e)))?;

        // Use SEARCH to find actual existing UIDs (handles UID gaps from deleted messages)
        let search_criteria = match range {
            FetchRange::All => "ALL".to_string(),
            FetchRange::UidRange { from, to } => format!("UID {}:{}", from, to),
            FetchRange::Last(_) => "ALL".to_string(),
            FetchRange::Since(date) => format!("SINCE {}", date.format("%d-%b-%Y")),
        };

        debug!("Executing UID SEARCH with criteria: {}", search_criteria);
        let search_results = session
            .uid_search(&search_criteria)
            .await
            .map_err(|e| DomainError::Provider(format!("Search failed: {}", e)))?;

        if search_results.is_empty() {
            debug!("No messages found matching search criteria");
            session.logout().await.ok();
            return Ok(Vec::new());
        }

        // Convert HashSet to sorted Vec
        let mut all_uids: Vec<u32> = search_results.into_iter().collect();
        all_uids.sort_unstable();

        // For Last(n), take only the last n UIDs
        let uids_to_fetch = match range {
            FetchRange::Last(n) => {
                let start = all_uids.len().saturating_sub(n as usize);
                &all_uids[start..]
            }
            _ => &all_uids[..],
        };

        info!("Fetching {} envelopes in batches of {}", uids_to_fetch.len(), Self::FETCH_BATCH_SIZE);

        // Fetch in batches to avoid IMAP command line limits
        let mut all_envelopes = Vec::new();
        for (batch_idx, chunk) in uids_to_fetch.chunks(Self::FETCH_BATCH_SIZE).enumerate() {
            let query = Self::compress_uid_ranges(chunk);
            debug!("Batch {}: fetching {} UIDs (query: {})", batch_idx, chunk.len(), &query[..query.len().min(100)]);

            use futures::TryStreamExt;
            let batch_results: Vec<async_imap::types::Fetch> = session
                .uid_fetch(&query, "(ENVELOPE FLAGS INTERNALDATE)")
                .await
                .map_err(|e| {
                    DomainError::Provider(format!("Fetch envelopes failed: {}", e))
                })?
                .try_collect()
                .await
                .map_err(|e| {
                    DomainError::Provider(format!("Fetch stream error: {}", e))
                })?;

            debug!("Batch {}: fetched {} envelopes", batch_idx, batch_results.len());

            for fetch_data in &batch_results {
                if let Some(envelope_data) = fetch_data.envelope() {
                    let provider_uid = ProviderUid::new(fetch_data.uid.ok_or_else(|| {
                        DomainError::Provider("Missing UID in fetch response".to_string())
                    })?);

                    let from = Self::parse_imap_addresses(envelope_data.from.as_ref());
                    let to = Self::parse_imap_addresses(envelope_data.to.as_ref());
                    let cc = Self::parse_imap_addresses(envelope_data.cc.as_ref());

                    let subject = envelope_data
                        .subject
                        .as_ref()
                        .map(|s| decode_rfc2047_header(s))
                        .unwrap_or_default();

                    let date = envelope_data
                        .date
                        .as_ref()
                        .and_then(|date_bytes| {
                            let date_str = String::from_utf8_lossy(date_bytes);
                            chrono::DateTime::parse_from_rfc2822(&date_str)
                                .ok()
                                .map(|dt| dt.with_timezone(&chrono::Utc))
                        })
                        .unwrap_or_else(chrono::Utc::now);

                    let envelope = mc_domain::Envelope {
                        from,
                        to,
                        cc,
                        bcc: Vec::new(),
                        reply_to: Vec::new(),
                        subject,
                        date,
                    };

                    // Parse flags from IMAP response
                    let flags: Vec<Flag> = fetch_data
                        .flags()
                        .filter_map(|f| super::mapper::imap_flag_to_domain(&f))
                        .collect();

                    let internal_date = fetch_data
                        .internal_date()
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .unwrap_or(date);

                    let summary = MessageEnvelopeSummary {
                        provider_uid,
                        message_rfc_id: None,
                        envelope,
                        flags,
                        size_bytes: 0,
                        has_attachments: false,
                        internal_date,
                    };

                    all_envelopes.push(summary);
                } else {
                    debug!("No envelope data for UID {:?}", fetch_data.uid);
                }
            }
        }

        // Logout
        session
            .logout()
            .await
            .map_err(|e| DomainError::Provider(format!("Logout failed: {}", e)))?;

        info!("Fetched {} envelopes total", all_envelopes.len());
        Ok(all_envelopes)
    }

    async fn fetch_message_detail(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
    ) -> Result<Message, DomainError> {
        use futures::TryStreamExt;

        let mut session = self.create_imap_session().await?;
        let mailbox_name = super::mapper::mailbox_path_to_string(mailbox);
        session
            .select(&mailbox_name)
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox select failed: {e}")))?;

        let fetched: Vec<async_imap::types::Fetch> = session
            .uid_fetch(uid.value().to_string(), "BODY.PEEK[]")
            .await
            .map_err(|e| DomainError::Provider(format!("Body fetch failed: {e}")))?
            .try_collect()
            .await
            .map_err(|e| DomainError::Provider(format!("Body stream error: {e}")))?;

        session.logout().await.ok();

        let item = fetched
            .into_iter()
            .next()
            .ok_or_else(|| DomainError::Provider("Message not found".into()))?;

        let (body_text, body_html) = item.body().map(parse_mime_body).unwrap_or((None, None));

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
            empty_envelope,
            vec![],
            0,
            false,
        );
        Ok(Message { body_text, body_html, ..msg })
    }

    async fn fetch_bodies_bulk(
        &self,
        mailbox: &MailboxPath,
        uids: &[ProviderUid],
    ) -> Result<Vec<mc_domain::traits::email_provider::MessageBodyResult>, DomainError> {
        use futures::TryStreamExt;
        use mc_domain::traits::email_provider::MessageBodyResult;

        if uids.is_empty() {
            return Ok(vec![]);
        }

        let mut session = self.create_imap_session().await?;
        let mailbox_name = super::mapper::mailbox_path_to_string(mailbox);
        session
            .select(&mailbox_name)
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox select failed: {e}")))?;

        let uid_nums: Vec<u32> = uids.iter().map(|u| u.value()).collect();
        let mut results = Vec::with_capacity(uids.len());

        for chunk in uid_nums.chunks(Self::FETCH_BATCH_SIZE) {
            let query = Self::compress_uid_ranges(chunk);
            debug!(
                "Bulk body fetch: {} UIDs (query: {}…)",
                chunk.len(),
                &query[..query.len().min(60)]
            );

            let fetched: Vec<async_imap::types::Fetch> = session
                .uid_fetch(&query, "BODY.PEEK[]")
                .await
                .map_err(|e| DomainError::Provider(format!("Body fetch failed: {e}")))?
                .try_collect()
                .await
                .map_err(|e| DomainError::Provider(format!("Body stream error: {e}")))?;

            for item in &fetched {
                let provider_uid = match item.uid {
                    Some(u) => ProviderUid::new(u),
                    None => continue,
                };
                let (body_text, body_html) = item
                    .body()
                    .map(parse_mime_body)
                    .unwrap_or((None, None));

                results.push(MessageBodyResult { provider_uid, body_text, body_html });
            }
        }

        session.logout().await.ok();
        Ok(results)
    }

    async fn fetch_raw_message(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
    ) -> Result<Vec<u8>, DomainError> {
        use futures::TryStreamExt;

        let mut session = self.create_imap_session().await?;
        let mailbox_name = super::mapper::mailbox_path_to_string(mailbox);
        session
            .select(&mailbox_name)
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox select failed: {e}")))?;

        let fetched: Vec<async_imap::types::Fetch> = session
            .uid_fetch(uid.value().to_string(), "BODY.PEEK[]")
            .await
            .map_err(|e| DomainError::Provider(format!("Raw fetch failed: {e}")))?
            .try_collect()
            .await
            .map_err(|e| DomainError::Provider(format!("Raw stream error: {e}")))?;

        session.logout().await.ok();
        fetched
            .into_iter()
            .next()
            .and_then(|f| f.body().map(|b| b.to_vec()))
            .ok_or_else(|| DomainError::Provider("Message not found".into()))
    }

    async fn set_flags(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
        flags_to_add: &[Flag],
        flags_to_remove: &[Flag],
    ) -> Result<(), DomainError> {

        info!(
            "Setting flags for message {} in mailbox {}",
            uid.value(),
            mailbox_path_to_string(mailbox)
        );

        let mut session = self.create_imap_session().await?;

        // Select mailbox
        let mailbox_name = mailbox_path_to_string(mailbox);
        session.select(&mailbox_name)
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox select failed: {}", e)))?;

        let seq = Self::uid_to_seq(&uid);

        // Add flags
        if !flags_to_add.is_empty() {
            let flags_str: Vec<String> = flags_to_add.iter().map(flag_to_imap).collect();
            let flags_query = flags_str.join(" ");

            let _ = session
                .uid_store(format!("{}", seq), format!("+FLAGS ({})", flags_query))
                .await
                .map_err(|e| DomainError::Provider(format!("Add flags failed: {}", e)))?;

            debug!("Added flags: {}", flags_query);
        }

        // Remove flags
        if !flags_to_remove.is_empty() {
            let flags_str: Vec<String> = flags_to_remove.iter().map(flag_to_imap).collect();
            let flags_query = flags_str.join(" ");

            let _ = session
                .uid_store(format!("{}", seq), format!("-FLAGS ({})", flags_query))
                .await
                .map_err(|e| DomainError::Provider(format!("Remove flags failed: {}", e)))?;

            debug!("Removed flags: {}", flags_query);
        }

        // Logout
        session
            .logout()
            .await
            .map_err(|e| DomainError::Provider(format!("Logout failed: {}", e)))?;

        info!("Flags updated successfully");
        Ok(())
    }

    async fn move_message(
        &self,
        uid: ProviderUid,
        from: &MailboxPath,
        to: &MailboxPath,
    ) -> Result<Option<ProviderUid>, DomainError> {

        info!(
            "Moving message {} from {} to {}",
            uid.value(),
            mailbox_path_to_string(from),
            mailbox_path_to_string(to)
        );

        let mut session = self.create_imap_session().await?;

        // Select source mailbox
        let from_name = mailbox_path_to_string(from);
        session
            .select(&from_name)
            .await
            .map_err(|e| DomainError::Provider(format!("Source mailbox select failed: {}", e)))?;

        let seq = Self::uid_to_seq(&uid);
        let to_name = mailbox_path_to_string(to);

        // Copy to destination
        session
            .uid_copy(format!("{}", seq), &to_name)
            .await
            .map_err(|e| DomainError::Provider(format!("Copy failed: {}", e)))?;

        // Mark as deleted in source
        let _ = session
            .uid_store(format!("{}", seq), "+FLAGS (\\Deleted)")
            .await
            .map_err(|e| DomainError::Provider(format!("Delete flag failed: {}", e)))?;

        // Expunge to remove from source
        let _ = session
            .expunge()
            .await
            .map_err(|e| DomainError::Provider(format!("Expunge failed: {}", e)))?;

        // Logout
        session
            .logout()
            .await
            .map_err(|e| DomainError::Provider(format!("Logout failed: {}", e)))?;

        info!("Message moved successfully");
        // Note: IMAP COPY doesn't return the new UID, so we return None
        // A full implementation would re-fetch to get the new UID
        Ok(None)
    }

    async fn delete_message(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
        permanently: bool,
    ) -> Result<(), DomainError> {

        info!(
            "Deleting message {} from {} (permanent: {})",
            uid.value(),
            mailbox_path_to_string(mailbox),
            permanently
        );

        let mut session = self.create_imap_session().await?;

        // Select mailbox
        let mailbox_name = mailbox_path_to_string(mailbox);
        session.select(&mailbox_name)
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox select failed: {}", e)))?;

        let seq = Self::uid_to_seq(&uid);

        // Mark as deleted
        let _ = session
            .uid_store(format!("{}", seq), "+FLAGS (\\Deleted)")
            .await
            .map_err(|e| DomainError::Provider(format!("Delete flag failed: {}", e)))?;

        // If permanent deletion, expunge
        if permanently {
            let _ = session
                .expunge()
                .await
                .map_err(|e| DomainError::Provider(format!("Expunge failed: {}", e)))?;
            debug!("Message permanently deleted via expunge");
        } else {
            debug!("Message marked as deleted (soft delete)");
        }

        // Logout
        session
            .logout()
            .await
            .map_err(|e| DomainError::Provider(format!("Logout failed: {}", e)))?;

        info!("Message deleted successfully");
        Ok(())
    }

    async fn send_message(&self, message: &OutgoingMessage) -> Result<(), DomainError> {
        // Day 9: Send via SMTP using lettre
        let sender = self.create_smtp_sender();
        sender.send(message).await
    }

    async fn mailbox_status(&self, mailbox: &MailboxPath) -> Result<MailboxStatus, DomainError> {

        debug!("Getting mailbox status for: {:?}", mailbox);
        let mut session = self.create_imap_session().await?;

        let mailbox_name = super::mapper::mailbox_path_to_string(mailbox);

        // Use STATUS command to get accurate counts.
        // SELECT's "unseen" field is the sequence number of the first unseen message (RFC 3501),
        // NOT the count — STATUS (MESSAGES UNSEEN UIDNEXT UIDVALIDITY) returns the actual count.
        let mailbox_data = session
            .status(&mailbox_name, "(MESSAGES UNSEEN UIDNEXT UIDVALIDITY)")
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox STATUS failed: {}", e)))?;

        session
            .logout()
            .await
            .map_err(|e| DomainError::Provider(format!("Logout failed: {}", e)))?;

        let status = MailboxStatus {
            uid_validity: mailbox_data.uid_validity.unwrap_or(0),
            uid_next: mailbox_data.uid_next.unwrap_or(0),
            total_messages: mailbox_data.exists,
            unseen_messages: mailbox_data.unseen.unwrap_or(0),
        };

        info!(
            "Mailbox status: total={}, unseen={}, uid_validity={}, uid_next={}",
            status.total_messages, status.unseen_messages, status.uid_validity, status.uid_next
        );
        Ok(status)
    }

    async fn fetch_uid_flags(
        &self,
        mailbox: &MailboxPath,
    ) -> Result<Vec<UidFlagEntry>, DomainError> {
        use futures::TryStreamExt;

        let mailbox_name = mailbox_path_to_string(mailbox);
        let mut session = self.create_imap_session().await?;

        session
            .select(&mailbox_name)
            .await
            .map_err(|e| DomainError::Provider(format!("SELECT failed: {e}")))?;

        // UID FETCH 1:* (FLAGS) — lightweight, returns only UID + flags
        let fetches: Vec<async_imap::types::Fetch> = session
            .uid_fetch("1:*", "(FLAGS)")
            .await
            .map_err(|e| DomainError::Provider(format!("UID FETCH FLAGS failed: {e}")))?
            .try_collect()
            .await
            .map_err(|e| DomainError::Provider(format!("UID FETCH FLAGS stream error: {e}")))?;

        let entries: Vec<UidFlagEntry> = fetches
            .iter()
            .filter_map(|f| {
                let uid = f.uid?;
                let flags: Vec<Flag> = f
                    .flags()
                    .filter_map(|flag| super::mapper::imap_flag_to_domain(&flag))
                    .collect();
                Some(UidFlagEntry {
                    uid: ProviderUid::new(uid),
                    flags,
                })
            })
            .collect();

        info!(
            mailbox = %mailbox_name,
            count = entries.len(),
            "Fetched UID+flags for reconciliation"
        );

        session.logout().await.ok();
        Ok(entries)
    }

    fn watch_mailbox(
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
                // Reuse the same TLS session creation as the rest of the provider
                let mut session = match self.create_imap_session().await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("[watch] Session creation failed: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                };

                let select_data = match session.select(&mailbox_name).await {
                    Ok(mb) => mb,
                    Err(e) => {
                        warn!("[watch] SELECT '{}' failed: {}", mailbox_name, e);
                        let _ = session.logout().await;
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                };
                // Remember the next expected UID so we can detect new arrivals after IDLE.
                let uid_next = select_data.uid_next.unwrap_or(1);

                info!("[watch] IDLE active for '{}'", mailbox_name);
                let mut idle = session.idle();
                // init() sends the IDLE command to the server — required before wait_with_timeout
                if let Err(e) = idle.init().await {
                    warn!("[watch] IDLE init failed: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    continue;
                }
                let (wait_future, _stop_source) =
                    idle.wait_with_timeout(std::time::Duration::from_secs(29 * 60));

                let idle_response = match wait_future.await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("[watch] IDLE wait error: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };

                session = match idle.done().await {
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
                        let _ = session.logout().await;
                        continue;
                    }
                    IdleResponse::ManualInterrupt => {
                        info!("[watch] IDLE interrupted for '{}'", mailbox_name);
                        let _ = session.logout().await;
                        break;
                    }
                };

                let has_flags   = raw_upper.contains("FETCH") && raw_upper.contains("FLAGS");
                let has_expunge = raw_upper.contains("EXPUNGE");
                let has_exists  = raw_upper.contains("EXISTS");

                if has_flags && !has_exists && !has_expunge {
                    // Pure flag change (e.g. marking read in another client).
                    // Parse the sequence number from "* N FETCH ..." to resolve UID + flags.
                    let seq_num: Option<u32> = raw_upper
                        .split_whitespace()
                        .nth(1) // skip "*", get seq num
                        .and_then(|s| s.parse().ok());

                    if let Some(seq) = seq_num {
                        use futures::TryStreamExt;
                        let fetches: Result<Vec<async_imap::types::Fetch>, _> =
                            match session.fetch(seq.to_string(), "(UID FLAGS)").await {
                                Ok(stream) => stream.try_collect().await,
                                Err(e) => Err(e),
                            };
                        match fetches {
                            Ok(items) => {
                                for fetch_data in &items {
                                    let uid = fetch_data.uid.unwrap_or(0);
                                    let flags: Vec<Flag> = fetch_data
                                        .flags()
                                        .filter_map(|f| super::mapper::imap_flag_to_domain(&f))
                                        .collect();
                                    info!("[watch] FlagsChanged in '{}': uid={} flags={:?}",
                                        mailbox_name, uid, flags);
                                    yield Ok(ProviderNotification::FlagsChanged {
                                        mailbox: mb_path.clone(),
                                        uid: ProviderUid::new(uid),
                                        flags,
                                    });
                                }
                            }
                            Err(e) => {
                                warn!("[watch] FETCH (UID FLAGS) failed for seq {}: {}", seq, e);
                                yield Ok(ProviderNotification::FlagsChanged {
                                    mailbox: mb_path.clone(),
                                    uid: ProviderUid::new(0),
                                    flags: vec![],
                                });
                            }
                        }
                    } else {
                        warn!("[watch] Could not parse seq num from IDLE response");
                        yield Ok(ProviderNotification::FlagsChanged {
                            mailbox: mb_path.clone(),
                            uid: ProviderUid::new(0),
                            flags: vec![],
                        });
                    }
                } else {
                    // EXISTS (new messages) and/or EXPUNGE.

                    // If new messages arrived, fetch their UIDs and emit NewMessage
                    // for each one. We use uid_next captured from SELECT: any UID
                    // >= uid_next is a message that wasn't there when IDLE started.
                    if has_exists {
                        use futures::TryStreamExt as _;
                        let range = format!("{}:*", uid_next);
                        match session.uid_fetch(&range, "FLAGS").await {
                            Ok(stream) => match stream.try_collect::<Vec<_>>().await {
                                Ok(fetches) => {
                                    for fetch in &fetches {
                                        if let Some(uid) = fetch.uid
                                            && uid >= uid_next
                                        {
                                            info!("[watch] NewMessage in '{}': uid={}", mailbox_name, uid);
                                            yield Ok(ProviderNotification::NewMessage {
                                                mailbox: mb_path.clone(),
                                                uid: ProviderUid::new(uid),
                                            });
                                        }
                                    }
                                }
                                Err(e) => warn!("[watch] UID FETCH for new messages failed: {}", e),
                            },
                            Err(e) => warn!("[watch] UID FETCH for new messages failed: {}", e),
                        }
                    }

                    // Always emit updated counts (covers both new messages and expunges).
                    let updated_mb = match session.status(&mailbox_name, "(MESSAGES UNSEEN)").await {
                        Ok(mb) => mb,
                        Err(e) => {
                            warn!("[watch] STATUS failed: {}", e);
                            let _ = session.logout().await;
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

                let _ = session.logout().await;
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        })
    }
}

/// Parse a raw RFC 2822 message and extract plain-text and HTML body parts.
fn parse_mime_body(raw: &[u8]) -> (Option<String>, Option<String>) {
    let parsed = match mailparse::parse_mail(raw) {
        Ok(p) => p,
        Err(_) => return (None, None),
    };
    extract_body_parts(&parsed)
}

fn extract_body_parts(mail: &mailparse::ParsedMail) -> (Option<String>, Option<String>) {
    let mut text: Option<String> = None;
    let mut html: Option<String> = None;

    if mail.subparts.is_empty() {
        match mail.ctype.mimetype.as_str() {
            "text/plain" => text = mail.get_body().ok(),
            "text/html" => html = mail.get_body().ok(),
            _ => {}
        }
    } else {
        for part in &mail.subparts {
            let (t, h) = extract_body_parts(part);
            if text.is_none() {
                text = t;
            }
            if html.is_none() {
                html = h;
            }
        }
    }

    (text, html)
}

/// Decode an RFC 2047 encoded header value (e.g. `=?UTF-8?Q?...?=`).
/// Falls back to lossy UTF-8 if parsing fails.
fn decode_rfc2047_header(raw: &[u8]) -> String {
    // mailparse::parse_header expects a full "Name: value\r\n" line
    let header_line = [b"Subject: ", raw, b"\r\n"].concat();
    match mailparse::parse_header(&header_line) {
        Ok((hdr, _)) => hdr.get_value(),
        Err(_) => String::from_utf8_lossy(raw).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_imap_provider_creation() {
        let provider = ImapEmailProvider::new(
            "imap.example.com".to_string(),
            993,
            "user@example.com".to_string(),
            "password".to_string(),
            true,
            "smtp.example.com".to_string(),
            587,
            true,
        );

        let (host, port, username, use_tls) = provider.config();
        assert_eq!(host, "imap.example.com");
        assert_eq!(port, 993);
        assert_eq!(username, "user@example.com");
        assert!(use_tls);
    }
}
