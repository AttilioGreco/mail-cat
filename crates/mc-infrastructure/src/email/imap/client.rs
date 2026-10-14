use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_imap::imap_proto::types::{BodyStructure, ContentEncoding, SectionPath};
use async_imap::types::Seq;
use async_trait::async_trait;
use futures::Stream;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tracing::{debug, info, warn};

use mc_domain::error::DomainError;
use mc_domain::model::mailbox::Mailbox;
use mc_domain::model::message::{Message, OutgoingMessage};
use mc_domain::traits::email_provider::{
    EmailProvider, FetchRange, MailboxStatus, MessageBodyResult, MessageEnvelopeSummary,
    ProviderNotification, UidFlagEntry,
};
use mc_domain::value_objects::flag::Flag;
use mc_domain::value_objects::mailbox_path::MailboxPath;
use mc_domain::value_objects::uid::ProviderUid;

use crate::email::smtp::SmtpSender;
use crate::persistence::DownloadMeter;

use super::mapper::{flag_to_imap, mailbox_path_to_string};

/// Credential an `ImapEmailProvider` authenticates with.
///
/// `OAuthToken` wraps a shared cell so a background token refresh (see
/// `mc-server`'s periodic OAuth refresh loop) is visible on the very next
/// connection without restarting anything.
pub enum ImapCredential {
    Password(String),
    OAuthToken(Arc<RwLock<String>>),
}

/// Reconnect delay after `failures` consecutive failed attempts.
///
/// Transient faults (network down, server restarting) recover quickly, so they
/// keep the short cadence and cap at 5 minutes. A rejected credential does not
/// recover without the user acting, so it backs off far harder — retrying every
/// 30s gets the client rate-limited or blocked by the provider, and buries the
/// real problem under thousands of identical log lines.
fn backoff_delay(failures: u32, auth_rejected: bool) -> std::time::Duration {
    let (base_secs, cap_secs) = if auth_rejected { (60, 3600) } else { (30, 300) };
    // Double per failure, saturating — shift beyond 31 would overflow.
    let factor = 1u64 << failures.saturating_sub(1).min(20);
    std::time::Duration::from_secs(base_secs * factor).min(std::time::Duration::from_secs(cap_secs))
}

type ImapSession =
    async_imap::Session<async_native_tls::TlsStream<tokio_util::compat::Compat<tokio::net::TcpStream>>>;

/// Simultaneous pooled connections per account. Gmail caps an account at ~15
/// connections shared with every other client (phone, webmail); the IDLE watcher
/// holds one more outside the pool.
const MAX_POOLED_CONNECTIONS: usize = 4;

/// Servers silently drop idle connections, so one parked longer than this gets a
/// NOOP before reuse instead of failing the caller's first real command.
const REVALIDATE_AFTER: Duration = Duration::from_secs(30);

/// A checked-out connection. Dropping it closes the connection; only
/// `ImapEmailProvider::release` returns it to the pool, so a session left
/// mid-command by an error is never reused.
struct PooledSession {
    session: ImapSession,
    _permit: OwnedSemaphorePermit,
}

impl Deref for PooledSession {
    type Target = ImapSession;

    fn deref(&self) -> &ImapSession {
        &self.session
    }
}

impl DerefMut for PooledSession {
    fn deref_mut(&mut self) -> &mut ImapSession {
        &mut self.session
    }
}

/// SASL XOAUTH2 authenticator — sends the full bearer-token response
/// immediately, ignoring the (empty) server challenge, per Google's/
/// Microsoft's XOAUTH2 spec.
struct XOAuth2Authenticator {
    user: String,
    access_token: String,
}

impl async_imap::Authenticator for XOAuth2Authenticator {
    type Response = String;

    fn process(&mut self, _challenge: &[u8]) -> Self::Response {
        format!("user={}\x01auth=Bearer {}\x01\x01", self.user, self.access_token)
    }
}

/// IMAP email provider implementation.
///
/// Uses async-native-tls for TLS (natively futures-compatible with async-imap).
/// tokio TcpStream is wrapped via tokio_util::compat before passing to async-native-tls.
pub struct ImapEmailProvider {
    host: String,
    port: u16,
    username: String,
    credential: ImapCredential,
    use_tls: bool,
    /// Pre-built SMTP sender. `None` when no SMTP config was provided for this
    /// account — sending will be rejected with a clear error.
    smtp_sender: Option<SmtpSender>,
    idle_sessions: Mutex<Vec<(ImapSession, Instant)>>,
    permits: Arc<Semaphore>,
    meter: Option<DownloadMeter>,
}

impl ImapEmailProvider {
    pub fn new(
        host: String,
        port: u16,
        username: String,
        credential: ImapCredential,
        use_tls: bool,
        smtp_sender: Option<SmtpSender>,
    ) -> Self {
        Self {
            host,
            port,
            username,
            credential,
            use_tls,
            smtp_sender,
            idle_sessions: Mutex::default(),
            permits: Arc::new(Semaphore::new(MAX_POOLED_CONNECTIONS)),
            meter: None,
        }
    }

    /// Count every message download against the account's daily cap.
    #[must_use]
    pub fn with_download_meter(mut self, meter: DownloadMeter) -> Self {
        self.meter = Some(meter);
        self
    }

    async fn metered(&self, bytes: u64) {
        if let Some(meter) = &self.meter {
            meter.record(bytes).await;
        }
    }

    /// Downloads only the text a body needs — its first non-attachment
    /// text/plain and text/html parts — never attachments: one BODYSTRUCTURE
    /// round trip, then one fetch per distinct part layout. Every UID that
    /// came back gets a result (bodiless if it has no text), so it dequeues.
    async fn fetch_text_bodies(
        &self,
        session: &mut ImapSession,
        uids: &[u32],
    ) -> Result<Vec<MessageBodyResult>, DomainError> {
        use futures::TryStreamExt;

        let structures: Vec<async_imap::types::Fetch> = session
            .uid_fetch(Self::compress_uid_ranges(uids), "(UID BODYSTRUCTURE)")
            .await
            .map_err(|e| DomainError::Provider(format!("Body structure fetch failed: {e}")))?
            .try_collect()
            .await
            .map_err(|e| DomainError::Provider(format!("Body structure stream error: {e}")))?;

        let parts_by_uid: HashMap<u32, Vec<TextPart>> = structures
            .iter()
            // An unsolicited FLAGS update for the same UID carries no structure:
            // skipping it keeps the real one. A UID with none comes back missing.
            .filter_map(|item| Some((item.uid?, text_parts(item.bodystructure()?))))
            .collect();
        // Messages laid out alike share one FETCH of the same sections.
        let mut layouts: HashMap<Vec<Vec<u32>>, Vec<u32>> = HashMap::new();
        for (uid, parts) in &parts_by_uid {
            layouts.entry(parts.iter().map(|p| p.section.clone()).collect()).or_default().push(*uid);
        }
        // ponytail: a BODYSTRUCTURE reply's size isn't exposed; 1 KiB is a typical one.
        let mut bytes = structures.len() as u64 * 1024;

        let mut results = Vec::with_capacity(structures.len());
        for (sections, message_uids) in layouts {
            if sections.is_empty() {
                results.extend(message_uids.into_iter().map(|uid| MessageBodyResult {
                    provider_uid: ProviderUid::new(uid),
                    body_text: None,
                    body_html: None,
                }));
                continue;
            }
            let wanted: Vec<String> = sections
                .iter()
                .map(|s| format!("BODY.PEEK[{}]", s.iter().map(u32::to_string).collect::<Vec<_>>().join(".")))
                .collect();
            let fetched: Vec<async_imap::types::Fetch> = session
                .uid_fetch(Self::compress_uid_ranges(&message_uids), format!("(UID {})", wanted.join(" ")))
                .await
                .map_err(|e| DomainError::Provider(format!("Body fetch failed: {e}")))?
                .try_collect()
                .await
                .map_err(|e| DomainError::Provider(format!("Body stream error: {e}")))?;
            for item in &fetched {
                let Some((uid, parts)) = item.uid.and_then(|uid| Some((uid, parts_by_uid.get(&uid)?))) else {
                    continue;
                };
                let mut result = MessageBodyResult {
                    provider_uid: ProviderUid::new(uid),
                    body_text: None,
                    body_html: None,
                };
                for part in parts {
                    let Some(data) = item.section(&SectionPath::Part(part.section.clone(), None)) else {
                        continue;
                    };
                    bytes += data.len() as u64;
                    let text = part.decode(data);
                    if part.html {
                        result.body_html = text;
                    } else {
                        result.body_text = text;
                    }
                }
                results.push(result);
            }
        }
        self.metered(bytes).await;
        Ok(results)
    }

    async fn selected(&self, mailbox_name: &str) -> Result<PooledSession, DomainError> {
        let mut session = self.session().await?;
        session
            .select(mailbox_name)
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox select failed: {e}")))?;
        Ok(session)
    }

    /// One message's text, on fresh connections: the text parts if its
    /// BODYSTRUCTURE parses, else the whole message. Some servers send
    /// structures the parser rejects; without this such a message would never
    /// get a body and would block the backfill behind it for good.
    async fn fetch_one_body(
        &self,
        mailbox_name: &str,
        uid: u32,
    ) -> Result<Option<MessageBodyResult>, DomainError> {
        let mut session = self.selected(mailbox_name).await?;
        if let Ok(found) = self.fetch_text_bodies(&mut session, &[uid]).await {
            self.release(session);
            return Ok(found.into_iter().next());
        }
        // The failed response may be half-read: that connection is dropped.
        let mut session = self.selected(mailbox_name).await?;
        let raw = Self::fetch_whole(&mut session, uid).await?;
        self.release(session);
        let Some(raw) = raw else { return Ok(None) };
        self.metered(raw.len() as u64).await;
        let (body_text, body_html) = whole_message_text(&raw);
        Ok(Some(MessageBodyResult { provider_uid: ProviderUid::new(uid), body_text, body_html }))
    }

    async fn fetch_whole(session: &mut ImapSession, uid: u32) -> Result<Option<Vec<u8>>, DomainError> {
        use futures::TryStreamExt;

        let fetched: Vec<async_imap::types::Fetch> = session
            .uid_fetch(uid.to_string(), "BODY.PEEK[]")
            .await
            .map_err(|e| DomainError::Provider(format!("Raw fetch failed: {e}")))?
            .try_collect()
            .await
            .map_err(|e| DomainError::Provider(format!("Raw stream error: {e}")))?;
        Ok(fetched.into_iter().find_map(|f| f.body().map(<[u8]>::to_vec)))
    }

    /// Get IMAP configuration for display/debugging.
    pub fn config(&self) -> (&str, u16, &str, bool) {
        (&self.host, self.port, &self.username, self.use_tls)
    }

    /// Check out a pooled connection, reusing an idle one when it is still alive.
    /// Waits while `MAX_POOLED_CONNECTIONS` are in use.
    async fn session(&self) -> Result<PooledSession, DomainError> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .expect("IMAP pool semaphore is never closed");
        // Pop in its own statement: a `while let` would hold the lock across the NOOP.
        loop {
            let idle = self.idle_sessions.lock().expect("IMAP pool lock poisoned").pop();
            let Some((mut session, parked_at)) = idle else { break };
            if parked_at.elapsed() < REVALIDATE_AFTER || session.noop().await.is_ok() {
                return Ok(PooledSession { session, _permit: permit });
            }
            debug!("Dropping stale pooled IMAP connection");
        }
        let session = self.create_imap_session().await?;
        Ok(PooledSession { session, _permit: permit })
    }

    fn release(&self, pooled: PooledSession) {
        self.idle_sessions
            .lock()
            .expect("IMAP pool lock poisoned")
            .push((pooled.session, Instant::now()));
    }

    async fn create_imap_session(&self) -> Result<ImapSession, DomainError> {
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

        // Create IMAP client and authenticate.
        // Client::new does NOT read the server greeting — the caller must.
        // login() tolerates the leftover greeting (it skips untagged lines
        // until its tagged reply), but authenticate()'s SASL handshake mistakes
        // the greeting for its first reply, then blocks forever on the `+ `
        // continuation it never answers. Consume the greeting first.
        let mut client = async_imap::Client::new(tls_stream);
        client
            .read_response()
            .await
            .map_err(|e| DomainError::Provider(format!("IMAP greeting read failed: {e}")))?
            .ok_or_else(|| DomainError::Provider("IMAP connection closed before greeting".into()))?;
        // Credential rejection must surface as AuthenticationFailed, not the generic
        // Provider error. Callers retry Provider errors (transient network faults);
        // a rejected credential never recovers on its own, so collapsing the two
        // makes a dead account get hammered forever instead of backing off.
        let session = match &self.credential {
            ImapCredential::Password(password) => client
                .login(&self.username, password)
                .await
                .map_err(|e| {
                    DomainError::AuthenticationFailed(format!(
                        "IMAP authentication failed: {:?}",
                        e.0
                    ))
                })?,
            ImapCredential::OAuthToken(token_cell) => {
                let access_token = token_cell.read().await.clone();
                let auth = XOAuth2Authenticator { user: self.username.clone(), access_token };
                client
                    .authenticate("XOAUTH2", auth)
                    .await
                    .map_err(|e| {
                        DomainError::AuthenticationFailed(format!(
                            "IMAP XOAUTH2 authentication failed: {:?}",
                            e.0
                        ))
                    })?
            }
        };

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
        let mut session = self.session().await?;

        let stream = session
            .list(None, Some("*"))
            .await
            .map_err(|e| DomainError::Provider(format!("List mailboxes failed: {}", e)))?;
        let mailbox_list: Vec<Result<_, _>> = stream.collect::<Vec<_>>().await;
        self.release(session);

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

            // Classify from SPECIAL-USE attributes when the server sends them.
            let mailbox_type = super::mapper::classify_mailbox(imap_name, mb.attributes());

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
        let mut session = self.session().await?;

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
            self.release(session);
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
                .uid_fetch(&query, "(ENVELOPE FLAGS INTERNALDATE BODY.PEEK[HEADER])")
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
            // ponytail: the ENVELOPE structure repeats the header, so it costs
            // about as much again; counted as the header bytes twice.
            let header_bytes: usize = batch_results.iter().filter_map(|f| f.header()).map(<[u8]>::len).sum();
            self.metered(2 * header_bytes as u64).await;

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

                    // Parse threading headers from raw RFC header block.
                    let (message_rfc_id, in_reply_to, references) = fetch_data
                        .header()
                        .map(parse_threading_headers)
                        .unwrap_or((None, None, Vec::new()));

                    let summary = MessageEnvelopeSummary {
                        provider_uid,
                        message_rfc_id,
                        in_reply_to,
                        references,
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
        self.release(session);

        info!("Fetched {} envelopes total", all_envelopes.len());
        Ok(all_envelopes)
    }

    async fn fetch_message_detail(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
    ) -> Result<Message, DomainError> {
        let mailbox_name = super::mapper::mailbox_path_to_string(mailbox);
        let MessageBodyResult { body_text, body_html, .. } = self
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
        Ok(Message { body_text, body_html, ..msg })
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
        let mut session = self.selected(&mailbox_name).await?;

        let uid_nums: Vec<u32> = uids.iter().map(|u| u.value()).collect();
        let mut results = Vec::with_capacity(uids.len());
        for chunk in uid_nums.chunks(Self::FETCH_BATCH_SIZE) {
            debug!("Bulk body fetch: {} UIDs", chunk.len());
            match self.fetch_text_bodies(&mut session, chunk).await {
                Ok(found) => results.extend(found),
                // One message the parser rejects fails its whole chunk: go one by one.
                Err(e) => {
                    warn!(mailbox = %mailbox_name, error = %e, "bulk text fetch failed, retrying message by message");
                    for &uid in chunk {
                        results.extend(self.fetch_one_body(&mailbox_name, uid).await?);
                    }
                    session = self.selected(&mailbox_name).await?;
                }
            }
        }

        self.release(session);
        Ok(results)
    }

    async fn fetch_raw_message(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
    ) -> Result<Vec<u8>, DomainError> {
        let mut session = self.selected(&super::mapper::mailbox_path_to_string(mailbox)).await?;
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

        info!(
            "Setting flags for message {} in mailbox {}",
            uid.value(),
            mailbox_path_to_string(mailbox)
        );

        let mut session = self.session().await?;

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
        self.release(session);

        info!("Flags updated successfully");
        Ok(())
    }

    async fn set_flags_bulk(
        &self,
        mailbox: &MailboxPath,
        uids: &[ProviderUid],
        flags_to_add: &[Flag],
        flags_to_remove: &[Flag],
    ) -> Result<(), DomainError> {
        if uids.is_empty() {
            return Ok(());
        }

        info!(
            "Setting flags for {} messages in mailbox {}",
            uids.len(),
            mailbox_path_to_string(mailbox)
        );

        let mut session = self.session().await?;
        let mailbox_name = mailbox_path_to_string(mailbox);
        session
            .select(&mailbox_name)
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox select failed: {e}")))?;

        let uid_nums: Vec<u32> = uids.iter().map(|u| u.value()).collect();

        for chunk in uid_nums.chunks(Self::FETCH_BATCH_SIZE) {
            let query = Self::compress_uid_ranges(chunk);

            if !flags_to_add.is_empty() {
                let flags_str: Vec<String> = flags_to_add.iter().map(flag_to_imap).collect();
                let flags_query = flags_str.join(" ");

                let _ = session
                    .uid_store(&query, format!("+FLAGS ({})", flags_query))
                    .await
                    .map_err(|e| DomainError::Provider(format!("Add flags failed: {}", e)))?;

                debug!("Added flags: {} for {} UIDs", flags_query, chunk.len());
            }

            if !flags_to_remove.is_empty() {
                let flags_str: Vec<String> = flags_to_remove.iter().map(flag_to_imap).collect();
                let flags_query = flags_str.join(" ");

                let _ = session
                    .uid_store(&query, format!("-FLAGS ({})", flags_query))
                    .await
                    .map_err(|e| DomainError::Provider(format!("Remove flags failed: {}", e)))?;

                debug!("Removed flags: {} for {} UIDs", flags_query, chunk.len());
            }
        }

        self.release(session);

        info!("Bulk flags updated successfully");
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

        let mut session = self.session().await?;

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
        self.release(session);

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

        let mut session = self.session().await?;

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

        let mut session = self.session().await?;
        session
            .select(&mailbox_name)
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox select failed: {e}")))?;

        // compress_uid_ranges assumes sorted input — the caller's order is
        // whatever the selection was, so sort here rather than trusting it.
        let mut uid_nums: Vec<u32> = uids.iter().map(|u| u.value()).collect();
        uid_nums.sort_unstable();

        for chunk in uid_nums.chunks(Self::FETCH_BATCH_SIZE) {
            let query = Self::compress_uid_ranges(chunk);
            let _ = session
                .uid_store(&query, "+FLAGS (\\Deleted)")
                .await
                .map_err(|e| DomainError::Provider(format!("Delete flag failed: {e}")))?;
        }

        // One expunge for the whole batch, not one per chunk.
        if permanently {
            let _ = session
                .expunge()
                .await
                .map_err(|e| DomainError::Provider(format!("Expunge failed: {e}")))?;
        }

        self.release(session);

        info!("Bulk delete of {} messages completed", uid_nums.len());
        Ok(())
    }

    /// One session, one `UID COPY` + one `UID STORE` + one `EXPUNGE` for the
    /// whole set. IMAP has no MOVE guarantee across servers, so this keeps the
    /// copy-then-flag-then-expunge shape of `move_message`.
    async fn move_messages(
        &self,
        uids: &[ProviderUid],
        from: &MailboxPath,
        to: &MailboxPath,
    ) -> Result<(), DomainError> {
        if uids.is_empty() {
            return Ok(());
        }

        let from_name = mailbox_path_to_string(from);
        let to_name = mailbox_path_to_string(to);
        info!("Moving {} messages from {} to {}", uids.len(), from_name, to_name);

        let mut session = self.session().await?;
        session
            .select(&from_name)
            .await
            .map_err(|e| DomainError::Provider(format!("Source mailbox select failed: {e}")))?;

        let mut uid_nums: Vec<u32> = uids.iter().map(|u| u.value()).collect();
        uid_nums.sort_unstable();

        for chunk in uid_nums.chunks(Self::FETCH_BATCH_SIZE) {
            let query = Self::compress_uid_ranges(chunk);
            session
                .uid_copy(&query, &to_name)
                .await
                .map_err(|e| DomainError::Provider(format!("Copy failed: {e}")))?;
            let _ = session
                .uid_store(&query, "+FLAGS (\\Deleted)")
                .await
                .map_err(|e| DomainError::Provider(format!("Delete flag failed: {e}")))?;
        }

        let _ = session
            .expunge()
            .await
            .map_err(|e| DomainError::Provider(format!("Expunge failed: {e}")))?;

        self.release(session);

        info!("Bulk move of {} messages completed", uid_nums.len());
        Ok(())
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
        session
            .append(&mailbox_name, Some(&flags), None, &raw)
            .await
            .map_err(|e| DomainError::Provider(format!("APPEND to {mailbox_name} failed: {e}")))?;

        // async-imap drops the APPENDUID response code, so find the copy by its
        // Message-ID — works on every server, UIDPLUS or not.
        let uid = match &message.message_rfc_id {
            Some(rfc_id) => {
                session
                    .select(&mailbox_name)
                    .await
                    .map_err(|e| DomainError::Provider(format!("Mailbox select failed: {e}")))?;
                let query = format!("HEADER Message-ID \"{}\"", rfc_id.replace(['"', '\\'], ""));
                session
                    .uid_search(query)
                    .await
                    .map_err(|e| DomainError::Provider(format!("UID SEARCH failed: {e}")))?
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

        debug!("Getting mailbox status for: {:?}", mailbox);
        let mut session = self.session().await?;

        let mailbox_name = super::mapper::mailbox_path_to_string(mailbox);

        // Use STATUS command to get accurate counts.
        // SELECT's "unseen" field is the sequence number of the first unseen message (RFC 3501),
        // NOT the count — STATUS (MESSAGES UNSEEN UIDNEXT UIDVALIDITY) returns the actual count.
        let mailbox_data = session
            .status(&mailbox_name, "(MESSAGES UNSEEN UIDNEXT UIDVALIDITY)")
            .await
            .map_err(|e| DomainError::Provider(format!("Mailbox STATUS failed: {}", e)))?;

        self.release(session);

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
        let mut session = self.session().await?;

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

        // ponytail: `* n FETCH (UID u FLAGS (...))` is about 40 bytes a message.
        self.metered(fetches.len() as u64 * 40).await;
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

        self.release(session);
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

            // Consecutive connect failures, used to back off. Reset on every success.
            let mut failures: u32 = 0;

            loop {
                // Reuse the same TLS session creation as the rest of the provider
                let mut session = match self.create_imap_session().await {
                    Ok(s) => {
                        failures = 0;
                        s
                    }
                    Err(e) => {
                        let fatal = matches!(e, DomainError::AuthenticationFailed(_));
                        failures = failures.saturating_add(1);
                        // A rejected credential will not fix itself: retrying every
                        // 30s just burns connections and gets the client rate-limited
                        // (or blocked) by the provider. Yield the error so the account
                        // can be flagged, then back off hard.
                        if fatal {
                            warn!(
                                "[watch] Authentication rejected for '{}' (attempt {}): {}",
                                mailbox_name, failures, e
                            );
                            yield Err(e);
                        } else {
                            warn!("[watch] Session creation failed: {}", e);
                        }
                        tokio::time::sleep(backoff_delay(failures, fatal)).await;
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

/// Parse RFC threading headers (Message-ID, In-Reply-To, References) from raw
/// header bytes returned by `BODY.PEEK[HEADER]`.
///
/// Returns `(message_rfc_id, in_reply_to, references)`.
fn parse_threading_headers(bytes: &[u8]) -> (Option<String>, Option<String>, Vec<String>) {
    let text = String::from_utf8_lossy(bytes);
    let mut message_id: Option<String> = None;
    let mut in_reply_to: Option<String> = None;
    let mut references: Vec<String> = Vec::new();

    // Accumulate folded header lines then process each complete header.
    let mut current_name = String::new();
    let mut current_value = String::new();

    let flush = |name: &str, value: &str,
                 mid: &mut Option<String>,
                 irt: &mut Option<String>,
                 refs: &mut Vec<String>| {
        match name.to_lowercase().as_str() {
            "message-id" => *mid = extract_first_msgid(value),
            "in-reply-to" => *irt = extract_first_msgid(value),
            "references" => *refs = extract_all_msgids(value),
            _ => {}
        }
    };

    for line in text.lines() {
        if line.starts_with(' ') || line.starts_with('\t') {
            // Folded continuation of previous header.
            current_value.push(' ');
            current_value.push_str(line.trim());
        } else if let Some(colon) = line.find(':') {
            // New header — flush the previous one first.
            if !current_name.is_empty() {
                flush(
                    &current_name,
                    &current_value,
                    &mut message_id,
                    &mut in_reply_to,
                    &mut references,
                );
            }
            current_name = line[..colon].trim().to_string();
            current_value = line[colon + 1..].trim().to_string();
        }
    }
    // Flush the last header.
    if !current_name.is_empty() {
        flush(
            &current_name,
            &current_value,
            &mut message_id,
            &mut in_reply_to,
            &mut references,
        );
    }

    (message_id, in_reply_to, references)
}

/// Extract the first `<…>` message-ID token from a header value.
fn extract_first_msgid(value: &str) -> Option<String> {
    let start = value.find('<')?;
    let end = value[start..].find('>')? + start;
    Some(value[start..=end].to_string())
}

/// Extract all `<…>` message-ID tokens from a References header value.
fn extract_all_msgids(value: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut rest = value;
    while let Some(start) = rest.find('<') {
        rest = &rest[start..];
        if let Some(end) = rest.find('>') {
            ids.push(rest[..=end].to_string());
            rest = &rest[end + 1..];
        } else {
            break;
        }
    }
    ids
}

/// A text part of a message, downloaded on its own instead of the whole
/// message, and what it takes to decode it.
#[derive(Debug, PartialEq)]
struct TextPart {
    /// IMAP part number, `[1, 2]` = `BODY[1.2]`.
    section: Vec<u32>,
    html: bool,
    /// MIME headers rebuilt from BODYSTRUCTURE, so mailparse can undo the
    /// transfer encoding and charset.
    headers: String,
}

impl TextPart {
    fn decode(&self, data: &[u8]) -> Option<String> {
        let part = [self.headers.as_bytes(), data].concat();
        mailparse::parse_mail(&part).ok()?.get_body().ok()
    }
}

/// The first text/plain and text/html parts that aren't attachments, in
/// MIME order — what a reader shows of a whole message.
/// Encapsulated messages (forwarded as attachment) are not descended into.
fn text_parts(structure: &BodyStructure) -> Vec<TextPart> {
    fn walk(node: &BodyStructure, path: &mut Vec<u32>, found: &mut Vec<TextPart>) {
        match node {
            BodyStructure::Multipart { bodies, .. } => {
                for (n, body) in (1..).zip(bodies) {
                    path.push(n);
                    walk(body, path, found);
                    path.pop();
                }
            }
            BodyStructure::Text { common, other, .. } => {
                let attachment = common
                    .disposition
                    .as_ref()
                    .is_some_and(|d| d.ty.eq_ignore_ascii_case("attachment"));
                let html = match common.ty.subtype.to_ascii_lowercase().as_str() {
                    "plain" => false,
                    "html" => true,
                    _ => return,
                };
                if attachment || found.iter().any(|p| p.html == html) {
                    return;
                }
                let charset = common
                    .ty
                    .params
                    .iter()
                    .flatten()
                    .find(|(k, _)| k.eq_ignore_ascii_case("charset"))
                    .map_or("us-ascii", |(_, v)| v);
                let encoding = match &other.transfer_encoding {
                    ContentEncoding::SevenBit => "7bit",
                    ContentEncoding::EightBit => "8bit",
                    ContentEncoding::Binary => "binary",
                    ContentEncoding::Base64 => "base64",
                    ContentEncoding::QuotedPrintable => "quoted-printable",
                    ContentEncoding::Other(other) => other,
                };
                found.push(TextPart {
                    // A single-part message's body is part 1 (RFC 3501 §6.4.5).
                    section: if path.is_empty() { vec![1] } else { path.clone() },
                    html,
                    headers: format!(
                        "Content-Type: text/{}; charset=\"{charset}\"\r\nContent-Transfer-Encoding: {encoding}\r\n\r\n",
                        common.ty.subtype
                    ),
                });
            }
            _ => {}
        }
    }
    let mut found = Vec::new();
    walk(structure, &mut Vec::new(), &mut found);
    found
}

/// First non-attachment text/plain and text/html of a whole message — the
/// fallback when its BODYSTRUCTURE can't be parsed.
fn whole_message_text(raw: &[u8]) -> (Option<String>, Option<String>) {
    let Ok(mail) = mailparse::parse_mail(raw) else { return (None, None) };
    let (mut text, mut html) = (None, None);
    for part in mail.parts().filter(|p| {
        p.subparts.is_empty() && p.get_content_disposition().disposition != mailparse::DispositionType::Attachment
    }) {
        let slot = match part.ctype.mimetype.as_str() {
            "text/plain" => &mut text,
            "text/html" => &mut html,
            _ => continue,
        };
        if slot.is_none() {
            *slot = part.get_body().ok();
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

    fn structure_of(response: &[u8]) -> Vec<TextPart> {
        use async_imap::imap_proto::types::{AttributeValue, Response};
        let (_, Response::Fetch(_, attrs)) = async_imap::imap_proto::parser::parse_response(response).unwrap()
        else {
            panic!("not a FETCH response")
        };
        let structure = attrs
            .iter()
            .find_map(|a| match a {
                AttributeValue::BodyStructure(s) => Some(s),
                _ => None,
            })
            .unwrap();
        text_parts(structure)
    }

    #[test]
    fn text_parts_skip_attachments_and_keep_the_first_of_each_kind() {
        // mixed( alternative(plain qp, html base64), plain attachment, pdf )
        let parts = structure_of(
            b"* 1 FETCH (UID 7 BODYSTRUCTURE (((\"TEXT\" \"PLAIN\" (\"CHARSET\" \"iso-8859-1\") NIL NIL \"QUOTED-PRINTABLE\" 12 1 NIL NIL NIL)\
(\"TEXT\" \"HTML\" (\"CHARSET\" \"utf-8\") NIL NIL \"BASE64\" 40 1 NIL NIL NIL) \"ALTERNATIVE\" (\"BOUNDARY\" \"b2\") NIL NIL)\
(\"TEXT\" \"PLAIN\" (\"NAME\" \"notes.txt\") NIL NIL \"7BIT\" 5000 80 NIL (\"ATTACHMENT\" (\"FILENAME\" \"notes.txt\")) NIL)\
(\"APPLICATION\" \"PDF\" (\"NAME\" \"big.pdf\") NIL NIL \"BASE64\" 9000000 NIL (\"ATTACHMENT\" (\"FILENAME\" \"big.pdf\")) NIL) \"MIXED\" (\"BOUNDARY\" \"b1\") NIL NIL))\r\n",
        );
        let sections: Vec<_> = parts.iter().map(|p| (p.section.clone(), p.html)).collect();
        assert_eq!(sections, [(vec![1, 1], false), (vec![1, 2], true)]);

        assert_eq!(parts[0].decode(b"caff=E8 =\r\nok").as_deref(), Some("caff\u{e8} ok"));
        assert_eq!(parts[1].decode(b"PHA+Y2lhbzwvcD4=").as_deref(), Some("<p>ciao</p>"));
    }

    #[test]
    fn whole_message_fallback_skips_text_attachments() {
        let raw = b"Content-Type: multipart/mixed; boundary=b\r\n\r\n\
--b\r\nContent-Type: text/plain\r\nContent-Disposition: attachment; filename=log.txt\r\n\r\nlog\r\n\
--b\r\nContent-Type: text/plain\r\n\r\nciao\r\n\
--b\r\nContent-Type: text/html\r\n\r\n<p>ciao</p>\r\n--b--\r\n";
        let (text, html) = whole_message_text(raw);
        assert_eq!(text.as_deref().map(str::trim), Some("ciao"));
        assert_eq!(html.as_deref().map(str::trim), Some("<p>ciao</p>"));
    }

    #[test]
    fn a_single_part_message_is_part_one() {
        let parts = structure_of(
            b"* 1 FETCH (UID 8 BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"utf-8\") NIL NIL \"7BIT\" 3 1 NIL NIL NIL))\r\n",
        );
        assert_eq!(parts.iter().map(|p| p.section.clone()).collect::<Vec<_>>(), [vec![1]]);
    }

    #[test]
    fn backoff_grows_and_caps_per_failure_kind() {
        let secs = |f, auth| backoff_delay(f, auth).as_secs();

        // Transient: keeps the old 30s cadence on the first failure, caps at 5 min.
        assert_eq!(secs(1, false), 30);
        assert_eq!(secs(2, false), 60);
        assert_eq!(secs(4, false), 240);
        assert_eq!(secs(10, false), 300, "transient failures cap at 5 minutes");

        // Auth rejection: starts higher and caps far higher — it will not self-heal.
        assert_eq!(secs(1, true), 60);
        assert_eq!(secs(2, true), 120);
        assert_eq!(secs(10, true), 3600, "auth failures cap at 1 hour");

        // A large failure count must not overflow the shift.
        assert_eq!(secs(u32::MAX, true), 3600);
        assert_eq!(secs(u32::MAX, false), 300);
    }

    #[test]
    fn test_imap_provider_creation() {
        let provider = ImapEmailProvider::new(
            "imap.example.com".to_string(),
            993,
            "user@example.com".to_string(),
            ImapCredential::Password("password".to_string()),
            true,
            None, // no SMTP sender in unit test
        );

        let (host, port, username, use_tls) = provider.config();
        assert_eq!(host, "imap.example.com");
        assert_eq!(port, 993);
        assert_eq!(username, "user@example.com");
        assert!(use_tls);
    }

    #[test]
    fn xoauth2_sasl_string_has_expected_format() {
        let mut auth = XOAuth2Authenticator {
            user: "me@gmail.com".into(),
            access_token: "ya29.abc".into(),
        };
        let response = async_imap::Authenticator::process(&mut auth, &[]);
        assert_eq!(response, "user=me@gmail.com\x01auth=Bearer ya29.abc\x01\x01");
    }
}
