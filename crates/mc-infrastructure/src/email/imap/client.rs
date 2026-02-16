use std::pin::Pin;

use async_imap::types::Seq;
use async_trait::async_trait;
use futures::Stream;
use native_tls::TlsConnector;
use tracing::{debug, info, warn};

use mc_domain::error::DomainError;
use mc_domain::model::mailbox::Mailbox;
use mc_domain::model::message::{Message, OutgoingMessage};
use mc_domain::traits::email_provider::{
    EmailProvider, FetchRange, MailboxStatus, MessageEnvelopeSummary, ProviderNotification,
};
use mc_domain::value_objects::flag::Flag;
use mc_domain::value_objects::mailbox_path::MailboxPath;
use mc_domain::value_objects::uid::ProviderUid;

use crate::email::smtp::SmtpSender;

use super::mapper::{flag_to_imap, mailbox_path_to_string};

/// IMAP email provider implementation.
///
/// Day 8 NOTE: This is a structural implementation focusing on the EmailProvider trait
/// and mapper layer. Full connection management with TLS will be completed in integration.
/// The async-imap crate uses async-std internally, requiring compatibility layers
/// with tokio which will be handled in Day 9.
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

    /// Create an authenticated IMAP session.
    ///
    /// Day 9: Uses async-compat and tokio-util compat to bridge async-imap (async-std) with tokio.
    async fn create_imap_session(
        &self,
    ) -> Result<
        async_imap::Session<
            tokio_util::compat::Compat<tokio_native_tls::TlsStream<tokio::net::TcpStream>>,
        >,
        DomainError,
    > {
        use async_compat::CompatExt;
        use tokio_util::compat::TokioAsyncReadCompatExt;

        info!(
            "Connecting to IMAP: {}:{} (TLS: {})",
            self.host, self.port, self.use_tls
        );

        // Connect with TLS
        let tcp_stream = tokio::net::TcpStream::connect((&self.host as &str, self.port))
            .await
            .map_err(|e| DomainError::Provider(format!("TCP connection failed: {}", e)))?;

        let tls = TlsConnector::builder()
            .build()
            .map_err(|e| DomainError::Provider(format!("TLS setup failed: {}", e)))?;

        let tls_stream = tokio_native_tls::TlsConnector::from(tls)
            .connect(&self.host, tcp_stream)
            .await
            .map_err(|e| DomainError::Provider(format!("TLS connection failed: {}", e)))?;

        // Wrap with compat to convert tokio AsyncRead/Write to futures AsyncRead/Write
        let compat_stream = TokioAsyncReadCompatExt::compat(tls_stream);

        // Create IMAP client
        let client = async_imap::Client::new(compat_stream);

        // Login with async-compat wrapper (wraps async-std future for tokio)
        let session = CompatExt::compat(client.login(&self.username, &self.password))
            .await
            .map_err(|e| DomainError::Provider(format!("IMAP authentication failed: {:?}", e.0)))?;

        Ok(session)
    }

    /// Helper to convert ProviderUid to IMAP sequence.
    fn uid_to_seq(uid: &ProviderUid) -> Seq {
        Seq::from(uid.value())
    }
}

#[async_trait]
impl EmailProvider for ImapEmailProvider {
    async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
        use async_compat::CompatExt;
        use futures::stream::StreamExt;
        use mc_domain::model::account::AccountId;
        use mc_domain::model::mailbox::MailboxId;

        debug!("Listing mailboxes");
        let mut session = self.create_imap_session().await?;

        // Get mailbox list as stream and collect into Vec
        let mailbox_stream = CompatExt::compat(session.list(None, Some("*")))
            .await
            .map_err(|e| DomainError::Provider(format!("List mailboxes failed: {}", e)))?;

        // Collect stream results
        let mailbox_list: Vec<Result<_, _>> = mailbox_stream.collect().await;

        // Logout
        CompatExt::compat(session.logout())
            .await
            .map_err(|e| DomainError::Provider(format!("Logout failed: {}", e)))?;

        // Convert IMAP mailbox names to Mailbox objects
        let mut mailboxes = Vec::new();
        for result in mailbox_list {
            let mb = result.map_err(|e| DomainError::Provider(format!("Mailbox name error: {}", e)))?;

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
                account_id: AccountId::new(), // Placeholder, will be updated during sync
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
        use async_compat::CompatExt;
        use futures::stream::StreamExt;

        debug!("Fetching envelopes from mailbox: {:?}", mailbox);
        let mut session = self.create_imap_session().await?;

        // Select mailbox
        let mailbox_name = super::mapper::mailbox_path_to_string(mailbox);
        CompatExt::compat(session.select(&mailbox_name))
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox select failed: {}", e)))?;

        // Build UID range query
        let uid_query = match range {
            FetchRange::All => "1:*".to_string(),
            FetchRange::UidRange { from, to } => format!("{}:{}", from, to),
            FetchRange::Last(n) => {
                // Fetch last N: request from (max-n+1):*
                format!("{}:*", std::cmp::max(1, (1_000_000_u32).saturating_sub(n)))
            }
            FetchRange::Since(_date) => {
                // Will use SEARCH instead for Since
                String::new()
            }
        };

        // Special handling for Since - use SEARCH instead of FETCH
        let actual_query = if let FetchRange::Since(date) = range {
            // Use SEARCH to get UIDs since date
            let search_results = CompatExt::compat(session.uid_search(&format!("SINCE {}", date.format("%d-%b-%Y"))))
                .await
                .map_err(|e| DomainError::Provider(format!("Search failed: {}", e)))?;

            if search_results.is_empty() {
                CompatExt::compat(session.logout()).await.ok();
                return Ok(Vec::new());
            }

            search_results
                .iter()
                .map(|u| u.to_string())
                .collect::<Vec<_>>()
                .join(",")
        } else {
            uid_query
        };

        // Fetch envelopes and basic headers
        let fetch_stream = CompatExt::compat(
            session.uid_fetch(&actual_query, "ENVELOPE BODYSTRUCTURE")
        )
        .await
        .map_err(|e| DomainError::Provider(format!("Fetch envelopes failed: {}", e)))?;

        // Collect fetch results
        let fetch_results: Vec<Result<_, _>> = fetch_stream.collect().await;

        let mut envelopes = Vec::new();
        for result in fetch_results {
            let fetch_data = result.map_err(|e| DomainError::Provider(format!("Fetch error: {}", e)))?;
            
            if let Some(envelope_data) = fetch_data.envelope() {
                let provider_uid = ProviderUid::new(fetch_data.uid.ok_or_else(|| {
                    DomainError::Provider("Missing UID in fetch response".to_string())
                })?);

                // Convert IMAP envelope to domain Envelope
                let from: Vec<mc_domain::Address> = envelope_data
                    .from
                    .as_ref()
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
                                let email = mc_domain::value_objects::email_address::EmailAddress::new(&email_str).ok()?;
                                let name = addr.name.as_ref().map(|n| String::from_utf8_lossy(n).to_string());
                                Some(mc_domain::Address { email, name })
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                let to: Vec<mc_domain::Address> = envelope_data
                    .to
                    .as_ref()
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
                                let email = mc_domain::value_objects::email_address::EmailAddress::new(&email_str).ok()?;
                                let name = addr.name.as_ref().map(|n| String::from_utf8_lossy(n).to_string());
                                Some(mc_domain::Address { email, name })
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                let cc: Vec<mc_domain::Address> = envelope_data
                    .cc
                    .as_ref()
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
                                let email = mc_domain::value_objects::email_address::EmailAddress::new(&email_str).ok()?;
                                let name = addr.name.as_ref().map(|n| String::from_utf8_lossy(n).to_string());
                                Some(mc_domain::Address { email, name })
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                let subject = envelope_data
                    .subject
                    .as_ref()
                    .map(|s| String::from_utf8_lossy(s).to_string())
                    .unwrap_or_default();

                // Parse date
                let date = envelope_data
                    .date
                    .as_ref()
                    .and_then(|date_bytes| {
                        let date_str = String::from_utf8_lossy(date_bytes);
                        chrono::DateTime::parse_from_rfc2822(&date_str)
                            .ok()
                            .map(|dt| dt.with_timezone(&chrono::Utc))
                    })
                    .unwrap_or_else(|| chrono::Utc::now());

                let envelope = mc_domain::Envelope {
                    from,
                    to,
                    cc,
                    bcc: Vec::new(), // IMAP doesn't provide BCC in envelope
                    reply_to: Vec::new(), // TODO: extract from headers if needed
                    subject,
                    date,
                };

                // Check for attachments in body structure
                let has_attachments = false; // TODO: properly detect attachments from body structure

                let summary = MessageEnvelopeSummary {
                    provider_uid,
                    message_rfc_id: None, // TODO: extract from headers if available
                    envelope,
                    flags: Vec::new(), // TODO: extract from fetch_data.flags()
                    size_bytes: 0, // TODO: get from RFC822.SIZE
                    has_attachments,
                    internal_date: chrono::Utc::now(), // TODO: use INTERNALDATE from fetch
                };

                envelopes.push(summary);
            }
        }

        // Logout
        CompatExt::compat(session.logout())
            .await
            .map_err(|e| DomainError::Provider(format!("Logout failed: {}", e)))?;

        info!("Fetched {} envelopes", envelopes.len());
        Ok(envelopes)
    }

    async fn fetch_message_detail(
        &self,
        _mailbox: &MailboxPath,
        _uid: ProviderUid,
    ) -> Result<Message, DomainError> {
        debug!("Fetching message detail (Day 8 stub)");
        // Day 9: Full implementation
        Err(DomainError::Provider(
            "fetch_message_detail implementation pending full TLS integration (Day 9)".to_string(),
        ))
    }

    async fn fetch_raw_message(
        &self,
        _mailbox: &MailboxPath,
        _uid: ProviderUid,
    ) -> Result<Vec<u8>, DomainError> {
        debug!("Fetching raw message (Day 8 stub)");
        // Day 9: Full implementation
        Err(DomainError::Provider(
            "fetch_raw_message implementation pending full TLS integration (Day 9)".to_string(),
        ))
    }

    async fn set_flags(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
        flags_to_add: &[Flag],
        flags_to_remove: &[Flag],
    ) -> Result<(), DomainError> {
        use async_compat::CompatExt;

        info!(
            "Setting flags for message {} in mailbox {}",
            uid.value(),
            mailbox_path_to_string(mailbox)
        );

        let mut session = self.create_imap_session().await?;

        // Select mailbox
        let mailbox_name = mailbox_path_to_string(mailbox);
        CompatExt::compat(session.select(&mailbox_name))
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox select failed: {}", e)))?;

        let seq = Self::uid_to_seq(&uid);

        // Add flags
        if !flags_to_add.is_empty() {
            let flags_str: Vec<String> = flags_to_add.iter().map(flag_to_imap).collect();
            let flags_query = flags_str.join(" ");

            let _ = CompatExt::compat(
                session.uid_store(format!("{}", seq), format!("+FLAGS ({})", flags_query)),
            )
            .await
            .map_err(|e| DomainError::Provider(format!("Add flags failed: {}", e)))?;

            debug!("Added flags: {}", flags_query);
        }

        // Remove flags
        if !flags_to_remove.is_empty() {
            let flags_str: Vec<String> = flags_to_remove.iter().map(flag_to_imap).collect();
            let flags_query = flags_str.join(" ");

            let _ = CompatExt::compat(
                session.uid_store(format!("{}", seq), format!("-FLAGS ({})", flags_query)),
            )
            .await
            .map_err(|e| DomainError::Provider(format!("Remove flags failed: {}", e)))?;

            debug!("Removed flags: {}", flags_query);
        }

        // Logout
        CompatExt::compat(session.logout())
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
        use async_compat::CompatExt;

        info!(
            "Moving message {} from {} to {}",
            uid.value(),
            mailbox_path_to_string(from),
            mailbox_path_to_string(to)
        );

        let mut session = self.create_imap_session().await?;

        // Select source mailbox
        let from_name = mailbox_path_to_string(from);
        CompatExt::compat(session.select(&from_name))
            .await
            .map_err(|e| DomainError::Provider(format!("Source mailbox select failed: {}", e)))?;

        let seq = Self::uid_to_seq(&uid);
        let to_name = mailbox_path_to_string(to);

        // Copy to destination
        CompatExt::compat(session.uid_copy(format!("{}", seq), &to_name))
            .await
            .map_err(|e| DomainError::Provider(format!("Copy failed: {}", e)))?;

        // Mark as deleted in source
        let _ = CompatExt::compat(session.uid_store(format!("{}", seq), "+FLAGS (\\Deleted)"))
            .await
            .map_err(|e| DomainError::Provider(format!("Delete flag failed: {}", e)))?;

        // Expunge to remove from source
        let _ = CompatExt::compat(session.expunge())
            .await
            .map_err(|e| DomainError::Provider(format!("Expunge failed: {}", e)))?;

        // Logout
        CompatExt::compat(session.logout())
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
        use async_compat::CompatExt;

        info!(
            "Deleting message {} from {} (permanent: {})",
            uid.value(),
            mailbox_path_to_string(mailbox),
            permanently
        );

        let mut session = self.create_imap_session().await?;

        // Select mailbox
        let mailbox_name = mailbox_path_to_string(mailbox);
        CompatExt::compat(session.select(&mailbox_name))
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox select failed: {}", e)))?;

        let seq = Self::uid_to_seq(&uid);

        // Mark as deleted
        let _ = CompatExt::compat(session.uid_store(format!("{}", seq), "+FLAGS (\\Deleted)"))
            .await
            .map_err(|e| DomainError::Provider(format!("Delete flag failed: {}", e)))?;

        // If permanent deletion, expunge
        if permanently {
            let _ = CompatExt::compat(session.expunge())
                .await
                .map_err(|e| DomainError::Provider(format!("Expunge failed: {}", e)))?;
            debug!("Message permanently deleted via expunge");
        } else {
            debug!("Message marked as deleted (soft delete)");
        }

        // Logout
        CompatExt::compat(session.logout())
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

    async fn mailbox_status(&self, _mailbox: &MailboxPath) -> Result<MailboxStatus, DomainError> {
        debug!("Getting mailbox status (Day 8 stub)");
        // Day 9: Full implementation
        Err(DomainError::Provider(
            "mailbox_status implementation pending full TLS integration (Day 9)".to_string(),
        ))
    }

    fn watch_mailbox(
        &self,
        mailbox: &MailboxPath,
    ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>> {
        // Day 9: IMAP IDLE implementation with fallback to polling
        let mailbox_name = mailbox_path_to_string(mailbox);
        let host = self.host.clone();
        let port = self.port;
        let username = self.username.clone();
        let password = self.password.clone();

        Box::pin(async_stream::stream! {
            use async_compat::CompatExt;
            use tokio_util::compat::TokioAsyncReadCompatExt;

            info!("Starting IMAP IDLE watch for mailbox: {}", mailbox_name);

            loop {
                // Create session
                let tcp_stream = match tokio::net::TcpStream::connect((&host as &str, port)).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        warn!("Watch TCP connection failed: {}", e);
                        yield Err(DomainError::Provider(format!("TCP connection failed: {}", e)));
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                };

                let tls = match TlsConnector::builder().build() {
                    Ok(tls) => tls,
                    Err(e) => {
                        warn!("Watch TLS setup failed: {}", e);
                        yield Err(DomainError::Provider(format!("TLS setup failed: {}", e)));
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                };

                let tls_stream = match tokio_native_tls::TlsConnector::from(tls)
                    .connect(&host, tcp_stream)
                    .await
                {
                    Ok(stream) => stream,
                    Err(e) => {
                        warn!("Watch TLS connection failed: {}", e);
                        yield Err(DomainError::Provider(format!("TLS connection failed: {}", e)));
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                };

                // Wrap with compat for async-imap
                let compat_stream = TokioAsyncReadCompatExt::compat(tls_stream);
                let client = async_imap::Client::new(compat_stream);

                let mut session = match CompatExt::compat(client.login(&username, &password)).await {
                    Ok(session) => session,
                    Err(e) => {
                        warn!("Watch login failed: {:?}", e);
                        yield Err(DomainError::Provider(format!("Authentication failed: {:?}", e.0)));
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                };

                // Select mailbox
                let _select_result = match CompatExt::compat(session.select(&mailbox_name)).await {
                    Ok(mb) => mb,
                    Err(e) => {
                        warn!("Watch mailbox select failed: {}", e);
                        yield Err(DomainError::Provider(format!("Mailbox select failed: {}", e)));
                        let _ = CompatExt::compat(session.logout()).await;
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                };

                // Start IDLE
                let mut idle = session.idle();

                // Wait for IDLE events with timeout
                // wait_with_timeout returns (future, StopSource), we need the first element
                let (wait_future, _stop_source) = idle.wait_with_timeout(std::time::Duration::from_secs(29 * 60));
                let idle_response = match CompatExt::compat(wait_future).await {
                    Ok(response) => response,
                    Err(e) => {
                        warn!("IDLE wait failed: {}", e);
                        // Try to recover session and continue
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };

                // Get session back
                session = match CompatExt::compat(idle.done()).await {
                    Ok(session) => session,
                    Err(e) => {
                        warn!("IDLE done failed: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                };

                // IDLE returned, indicating activity in the mailbox
                debug!("IDLE activity detected: {:?}", idle_response);

                // Get updated mailbox status
                let updated_mb = match CompatExt::compat(session.select(&mailbox_name)).await {
                    Ok(mb) => mb,
                    Err(e) => {
                        warn!("Failed to get updated mailbox status: {}", e);
                        let _ = CompatExt::compat(session.logout()).await;
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };

                // Parse mailbox path from name (split by '/')
                let parts: Vec<String> = mailbox_name.split('/').map(|s| s.to_string()).collect();
                let mb_path = match mc_domain::value_objects::mailbox_path::MailboxPath::new(parts, '/') {
                    Ok(path) => path,
                    Err(_) => {
                        warn!("Failed to parse mailbox path");
                        let _ = CompatExt::compat(session.logout()).await;
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };

                // Yield notification with mailbox counts
                yield Ok(ProviderNotification::MailboxCountChanged {
                    mailbox: mb_path,
                    total: updated_mb.exists,
                    unseen: updated_mb.unseen.unwrap_or(0),
                });

                // Logout
                let _ = CompatExt::compat(session.logout()).await;

                // Small delay before reconnecting
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        })
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
