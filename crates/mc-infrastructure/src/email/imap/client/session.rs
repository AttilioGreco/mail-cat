//! Pooled connections, login and the provider's own helpers.

use std::io;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};

use mc_domain::error::DomainError;
use mc_domain::traits::email_provider::MessageBodyResult;
use mc_domain::traits::login_gate::{LoginGate, LoginSecret, login_with};
use mc_domain::value_objects::remote_id::RemoteId;

use crate::email::smtp::SmtpSender;
use crate::persistence::DownloadMeter;

use super::commands::{ServerCaps, exchange, timed};
use super::fetch::{decode_rfc2047_header, message_start, text_bodies, whole_message_text};
use super::{
    BODY_FETCH_TIMEOUT, COMMAND_TIMEOUT, CONNECT_TIMEOUT, ImapEmailProvider, ImapSession,
    MAX_POOLED_CONNECTIONS, REVALIDATE_AFTER, TEXT_PART_CAP,
};

/// A checked-out connection. Dropping it closes the connection; only
/// `ImapEmailProvider::release` returns it to the pool, so a session left
/// mid-command by an error is never reused.
pub(super) struct PooledSession {
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

/// SASL XOAUTH2 authenticator: the bearer token answers the initial empty
/// challenge. A non-empty challenge is the server's JSON error; per Google's
/// and Microsoft's XOAUTH2 spec the client answers it with an empty line and
/// the server then sends the tagged NO. The error is kept to classify it.
pub(super) struct XOAuth2Authenticator {
    pub(super) user: String,
    pub(super) access_token: String,
    pub(super) error: Option<Vec<u8>>,
}

impl async_imap::Authenticator for &mut XOAuth2Authenticator {
    type Response = String;

    fn process(&mut self, challenge: &[u8]) -> Self::Response {
        if challenge.is_empty() {
            return format!(
                "user={}\x01auth=Bearer {}\x01\x01",
                self.user, self.access_token
            );
        }
        self.error = Some(challenge.to_vec());
        String::new()
    }
}

/// True when an XOAUTH2 error challenge says the token itself was refused.
pub(super) fn xoauth2_rejects_token(challenge: &[u8]) -> bool {
    #[derive(serde::Deserialize)]
    struct Challenge {
        status: String,
    }
    serde_json::from_slice::<Challenge>(challenge)
        .is_ok_and(|c| matches!(c.status.as_str(), "400" | "401"))
}

/// RFC 5530 codes that mean the credential itself was refused.
const REJECTING_CODES: [&str; 3] = ["AUTHENTICATIONFAILED", "AUTHORIZATIONFAILED", "EXPIRED"];

/// Gmail alerts that only the user can clear (an app password, a browser
/// login). Retrying keeps Google's block in place, so they refuse the
/// credential like a wrong password does.
const USER_ACTION_ALERTS: [&str; 2] = [
    "Application-specific password required",
    "Please log in via your web browser",
];

/// Gmail alerts that the account is over a usage limit: passing, but only
/// after a while, so the gate backs off harder than for a network fault.
const THROTTLE_ALERTS: [&str; 2] = [
    "Too many simultaneous connections",
    "Account exceeded command or bandwidth limits",
];

/// Whether a refused login's text is one of Google's "do this first" alerts,
/// rather than a plain refusal of the credential.
pub fn asks_the_user_to_act(refusal: &str) -> bool {
    refusal.starts_with("[WEBALERT") || USER_ACTION_ALERTS.iter().any(|a| refusal.contains(a))
}

#[derive(Debug, PartialEq, Eq)]
enum LoginVerdict {
    Refused,
    Throttled,
    Transient,
}

/// Reads a tagged NO to LOGIN/AUTHENTICATE. async-imap only hands it over as
/// `code: {code:?}, info: {info:?}` (hence the pinned version), and imap-proto
/// parses only RFC 3501 codes: ALERT comes out in `code`, while an RFC 5530
/// code (AUTHENTICATIONFAILED, UNAVAILABLE, LIMIT…) or Gmail's WEBALERT stays
/// at the start of `info`. A NO without any code is what old servers send on a
/// bad password. Returns the verdict and the server's own text.
fn read_no(reply: &str) -> (LoginVerdict, String) {
    let (code, info) = reply
        .strip_prefix("code: ")
        .and_then(|rest| rest.split_once(", info: "))
        .unwrap_or(("None", "None"));
    let text = info
        .strip_prefix("Some(\"")
        .and_then(|t| t.strip_suffix("\")"))
        .map(|t| t.replace("\\\"", "\"").replace("\\\\", "\\"))
        .unwrap_or_default();
    let has = |needles: &[&str]| needles.iter().any(|n| text.contains(n));

    let verdict = if text.contains("[AUTHENTICATIONFAILED]") || text.starts_with("[WEBALERT") {
        LoginVerdict::Refused
    } else if code == "Some(Alert)" {
        if asks_the_user_to_act(&text) {
            LoginVerdict::Refused
        } else if has(&THROTTLE_ALERTS) {
            LoginVerdict::Throttled
        } else {
            LoginVerdict::Transient
        }
    } else if code != "None" {
        LoginVerdict::Transient
    } else if let Some(coded) = text.strip_prefix('[') {
        if REJECTING_CODES.iter().any(|c| coded.starts_with(c)) {
            LoginVerdict::Refused
        } else {
            LoginVerdict::Transient
        }
    } else {
        LoginVerdict::Refused
    };
    (verdict, text)
}

/// What a failed login means to the gate. Never echoes the input: a Validate
/// error quotes a character of the password.
pub(super) fn login_error(e: async_imap::error::Error, token_refused: bool) -> DomainError {
    use async_imap::error::Error;
    match e {
        Error::No(reply) => match read_no(&reply) {
            (_, text) if token_refused => DomainError::AuthenticationFailed(text),
            (LoginVerdict::Refused, text) => DomainError::AuthenticationFailed(text),
            (LoginVerdict::Throttled, text) => DomainError::Throttled(text),
            (LoginVerdict::Transient, text) => {
                DomainError::ConnectionFailed(format!("IMAP login refused for now: {text}"))
            }
        },
        Error::Validate(_) => DomainError::ConnectionFailed(
            "IMAP login: credential has a character IMAP cannot send".into(),
        ),
        other => DomainError::ConnectionFailed(format!("IMAP login failed: {other}")),
    }
}

/// LOGIN or AUTHENTICATE XOAUTH2 on a client that has read the greeting. Only
/// a refused credential surfaces as AuthenticationFailed (the gate refreshes,
/// retries, then pauses the account); anything else is retried later.
pub(super) async fn log_in<T>(
    client: async_imap::Client<T>,
    username: &str,
    secret: LoginSecret,
) -> Result<async_imap::Session<T>, DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    match secret {
        LoginSecret::Password(password) => client
            .login(username, password)
            .await
            .map_err(|(e, _)| login_error(e, false)),
        LoginSecret::OAuthToken(access_token) => {
            let mut auth = XOAuth2Authenticator {
                user: username.to_owned(),
                access_token,
                error: None,
            };
            let result = client.authenticate("XOAUTH2", &mut auth).await;
            let refused = auth.error.as_deref().is_some_and(xoauth2_rejects_token);
            result.map_err(|(e, _)| login_error(e, refused))
        }
    }
}

impl ImapEmailProvider {
    pub fn new(
        host: String,
        port: u16,
        username: String,
        gate: Arc<dyn LoginGate>,
        use_tls: bool,
        smtp_sender: Option<SmtpSender>,
    ) -> Self {
        Self {
            host,
            port,
            username,
            gate,
            use_tls,
            smtp_sender,
            idle_sessions: Mutex::default(),
            permits: Arc::new(Semaphore::new(MAX_POOLED_CONNECTIONS)),
            meter: None,
            received: Arc::default(),
            caps: OnceLock::new(),
            keywords: OnceLock::new(),
            layout: Mutex::default(),
        }
    }

    /// Count everything the account's connections download against its daily cap.
    #[must_use]
    pub fn with_download_meter(mut self, meter: DownloadMeter) -> Self {
        self.meter = Some(meter);
        self
    }

    /// Hands the bytes read since the last call to the meter. Called after
    /// each download; traffic in between (IDLE, STATUS, moves) rides along
    /// with the next one.
    pub(super) async fn record_downloads(&self) {
        let bytes = self.received.swap(0, Ordering::Relaxed);
        if let Some(meter) = &self.meter {
            meter.record(bytes).await;
        }
    }

    /// The text of `uids` in `mailbox_name` on a pooled connection (see
    /// [`text_bodies`]). The outer error is the connection or SELECT failing,
    /// the inner one this fetch.
    pub(super) async fn text_bodies_in(
        &self,
        mailbox_name: &str,
        uids: &[u32],
        cap: Option<u32>,
    ) -> Result<Result<Vec<MessageBodyResult>, DomainError>, DomainError> {
        let mut session = self.selected(mailbox_name).await?;
        // A failed response may be half-read: that connection is dropped, never
        // pooled, and before the caller checks out the next one.
        let found = match text_bodies(&mut session, uids, cap).await {
            Ok(found) => found,
            Err(e) => return Ok(Err(e)),
        };
        self.release(session);
        self.record_downloads().await;
        Ok(Ok(found))
    }

    /// [`message_start`] on a pooled connection, errors split as in
    /// [`Self::text_bodies_in`].
    pub(super) async fn message_start_in(
        &self,
        mailbox_name: &str,
        uid: u32,
    ) -> Result<Result<Option<MessageBodyResult>, DomainError>, DomainError> {
        let mut session = self.selected(mailbox_name).await?;
        let found = match message_start(&mut session, uid, TEXT_PART_CAP).await {
            Ok(found) => found,
            Err(e) => return Ok(Err(e)),
        };
        self.release(session);
        self.record_downloads().await;
        Ok(Ok(found))
    }

    pub(super) async fn selected(&self, mailbox_name: &str) -> Result<PooledSession, DomainError> {
        let mut session = self.session().await?;
        let layout = self.layout_on(&mut session).await?;
        // A Gmail view has no ids of its own: its messages are All Mail's.
        let name = layout
            .as_ref()
            .map_or(mailbox_name, |l| l.backing(mailbox_name));
        let selected = exchange(COMMAND_TIMEOUT, "Mailbox select", session.select(name)).await?;
        self.keywords.get_or_init(|| {
            selected
                .permanent_flags
                .contains(&async_imap::types::Flag::MayCreate)
        });
        Ok(session)
    }

    /// One message's whole text, for a reader who opened it: the text parts
    /// if its BODYSTRUCTURE parses, else the whole message. Some servers send
    /// structures the parser rejects; without this such a message could never
    /// be read.
    pub(super) async fn fetch_one_body(
        &self,
        mailbox_name: &str,
        uid: u32,
    ) -> Result<Option<MessageBodyResult>, DomainError> {
        match self.text_bodies_in(mailbox_name, &[uid], None).await? {
            Ok(found) => return Ok(found.into_iter().next()),
            // A silent server stays silent: no fallback round on it.
            Err(e @ DomainError::ConnectionTimeout(_)) => return Err(e),
            Err(_) => {}
        }
        let mut session = self.selected(mailbox_name).await?;
        let raw = Self::fetch_whole(&mut session, uid).await?;
        self.release(session);
        let Some(raw) = raw else { return Ok(None) };
        self.record_downloads().await;
        let (body_text, body_html) = whole_message_text(&raw);
        Ok(Some(MessageBodyResult {
            remote_id: RemoteId::from(uid),
            body_text,
            body_html,
            truncated: false,
        }))
    }

    pub(super) async fn fetch_whole(
        session: &mut ImapSession,
        uid: u32,
    ) -> Result<Option<Vec<u8>>, DomainError> {
        use futures::TryStreamExt;

        let fetched: Vec<async_imap::types::Fetch> =
            exchange(BODY_FETCH_TIMEOUT, "Raw fetch", async {
                session
                    .uid_fetch(uid.to_string(), "BODY.PEEK[]")
                    .await?
                    .try_collect()
                    .await
            })
            .await?;
        Ok(fetched
            .into_iter()
            .find_map(|f| f.body().map(<[u8]>::to_vec)))
    }

    /// Get IMAP configuration for display/debugging.
    pub fn config(&self) -> (&str, u16, &str, bool) {
        (&self.host, self.port, &self.username, self.use_tls)
    }

    /// Check out a pooled connection, reusing an idle one when it is still alive.
    /// Waits while `MAX_POOLED_CONNECTIONS` are in use.
    pub(super) async fn session(&self) -> Result<PooledSession, DomainError> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .expect("IMAP pool semaphore is never closed");
        // Pop in its own statement: a `while let` would hold the lock across the NOOP.
        loop {
            let idle = self
                .idle_sessions
                .lock()
                .expect("IMAP pool lock poisoned")
                .pop();
            let Some((mut session, parked_at)) = idle else {
                break;
            };
            if parked_at.elapsed() < REVALIDATE_AFTER
                || exchange(COMMAND_TIMEOUT, "NOOP", session.noop())
                    .await
                    .is_ok()
            {
                return Ok(PooledSession {
                    session,
                    _permit: permit,
                });
            }
            debug!("Dropping stale pooled IMAP connection");
        }
        let session = self.create_imap_session().await?;
        Ok(PooledSession {
            session,
            _permit: permit,
        })
    }

    /// One login and logout outside the pool, to prove the server accepts the
    /// credential for this username before anyone relies on it.
    pub async fn verify_login(&self) -> Result<(), DomainError> {
        let mut session = self.create_imap_session().await?;
        let _ = exchange(COMMAND_TIMEOUT, "LOGOUT", session.logout()).await;
        Ok(())
    }

    pub(super) fn release(&self, pooled: PooledSession) {
        self.idle_sessions
            .lock()
            .expect("IMAP pool lock poisoned")
            .push((pooled.session, Instant::now()));
    }

    pub(super) async fn create_imap_session(&self) -> Result<ImapSession, DomainError> {
        login_with(self.gate.as_ref(), |secret| {
            timed(
                CONNECT_TIMEOUT,
                "IMAP connect and login",
                self.connect_and_login(secret),
            )
        })
        .await
    }

    async fn connect_and_login(&self, secret: LoginSecret) -> Result<ImapSession, DomainError> {
        use tokio_util::compat::TokioAsyncReadCompatExt;

        info!(
            "Connecting to IMAP: {}:{} (TLS: {})",
            self.host, self.port, self.use_tls
        );

        // Connect TCP, then wrap with compat for futures AsyncRead/Write compatibility
        let tcp_stream = tokio::net::TcpStream::connect((&self.host as &str, self.port))
            .await
            .map_err(|e| DomainError::ConnectionFailed(format!("TCP connection failed: {}", e)))?;
        let compat_stream = Counted::new(tcp_stream, Arc::clone(&self.received)).compat();

        // TLS via async-native-tls (natively futures-compatible, no compat needed)
        let tls = async_native_tls::TlsConnector::new();
        let tls_stream = tls
            .connect(&self.host, compat_stream)
            .await
            .map_err(|e| DomainError::ConnectionFailed(format!("TLS connection failed: {}", e)))?;

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
            .map_err(|e| DomainError::ConnectionFailed(format!("IMAP greeting read failed: {e}")))?
            .ok_or_else(|| {
                DomainError::ConnectionFailed("IMAP connection closed before greeting".into())
            })?;
        let mut session = log_in(client, &self.username, secret).await?;
        if self.caps.get().is_none() {
            match exchange(COMMAND_TIMEOUT, "CAPABILITY", session.capabilities()).await {
                Ok(caps) if let Some(caps) = ServerCaps::from_names(|name| caps.has_str(name)) => {
                    let _ = self.caps.set(caps);
                }
                Ok(_) => warn!(
                    "CAPABILITY answered without a protocol version, asking again before the next move or delete"
                ),
                Err(e) => {
                    warn!(error = %e, "CAPABILITY failed, asking again before the next move or delete")
                }
            }
        }

        Ok(session)
    }

    /// Batch size for UID FETCH operations to avoid IMAP command line limits.
    pub(super) const FETCH_BATCH_SIZE: usize = 500;
    /// Messages per `UID FETCH (FLAGS)`: about 40 bytes each in the reply.
    pub(super) const FLAGS_BATCH_SIZE: usize = 5_000;

    /// Compress a sorted slice of UIDs into IMAP range notation.
    /// e.g. [1,2,3,5,6,10] -> "1:3,5:6,10"
    pub(super) fn compress_uid_ranges(uids: &[u32]) -> String {
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
    pub(super) fn parse_imap_addresses(
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
                        let name = addr.name.as_ref().map(|n| decode_rfc2047_header(n));
                        Some(mc_domain::Address { email, name })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// A stream adding every byte it reads to a shared total: the meter counts
/// what the server actually sent, which is never less than what a provider's
/// own download quota can see.
#[derive(Debug)]
pub(super) struct Counted<S> {
    inner: S,
    total: Arc<AtomicU64>,
}

impl<S> Counted<S> {
    pub(super) fn new(inner: S, total: Arc<AtomicU64>) -> Self {
        Self { inner, total }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Counted<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        self.total
            .fetch_add((buf.filled().len() - before) as u64, Ordering::Relaxed);
        poll
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Counted<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
