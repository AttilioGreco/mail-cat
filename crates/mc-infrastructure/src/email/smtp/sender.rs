use std::sync::Arc;

use lettre::{
    message::{
        header::{ContentType, HeaderName, HeaderValue},
        Attachment, Mailbox, MultiPart, SinglePart,
    },
    transport::smtp::{
        authentication::{Credentials, Mechanism},
        response::{Category, Code, Detail, Severity},
    },
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
};
use mc_domain::{
    error::DomainError,
    traits::login_gate::{login_with, LoginGate, LoginSecret},
    model::{
        account::{SmtpConnectionConfig, SmtpSecurity},
        message::{Address, OutgoingAttachment, OutgoingMessage},
    },
};
/// Async SMTP sender. `Static` builds its transport once at startup, for an
/// SMTP password of its own. `Gated` logs in with the account's gate secret
/// (the IMAP password or the OAuth token) and rebuilds the transport on every
/// send/test — cheap, no I/O happens until `.send()`/`.test_connection()`.
enum SmtpBackend {
    Static(AsyncSmtpTransport<Tokio1Executor>),
    Gated {
        host: String,
        port: u16,
        security: SmtpSecurity,
        username: String,
    },
}

/// Either way the account's gate hears how SMTP logins go, for the user to
/// see; a refused SMTP login fails the send and never changes the account's
/// state (IMAP alone judges the credential).
pub struct SmtpSender {
    backend: SmtpBackend,
    gate: Arc<dyn LoginGate>,
}

impl SmtpSender {
    /// A sender with an SMTP password of its own.
    pub fn build(
        config: &SmtpConnectionConfig,
        fallback_username: &str,
        fallback_password: &str,
        gate: Arc<dyn LoginGate>,
    ) -> Result<Self, DomainError> {
        let username = config.username.as_deref().unwrap_or(fallback_username);
        let password = config.password.as_deref().unwrap_or(fallback_password);
        let creds = Credentials::new(username.to_owned(), password.to_owned());

        let builder = Self::transport_builder(&config.host, config.security)?;
        let transport = builder.port(config.port).credentials(creds).build();
        Ok(Self { backend: SmtpBackend::Static(transport), gate })
    }

    /// A sender that logs in with the account's own credential from its gate:
    /// a refused OAuth token gets the forced refresh and one retry.
    pub fn build_gated(config: &SmtpConnectionConfig, fallback_username: &str, gate: Arc<dyn LoginGate>) -> Self {
        Self {
            backend: SmtpBackend::Gated {
                host: config.host.clone(),
                port: config.port,
                security: config.security,
                username: config.username.clone().unwrap_or_else(|| fallback_username.to_owned()),
            },
            gate,
        }
    }

    fn transport_builder(
        host: &str,
        security: SmtpSecurity,
    ) -> Result<lettre::transport::smtp::AsyncSmtpTransportBuilder, DomainError> {
        match security {
            SmtpSecurity::Smtps => AsyncSmtpTransport::<Tokio1Executor>::relay(host)
                .map_err(|e| DomainError::Provider(format!("SMTP relay build error: {e}"))),
            SmtpSecurity::StartTls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host)
                .map_err(|e| DomainError::Provider(format!("SMTP starttls build error: {e}"))),
        }
    }

    fn gated_transport(
        host: &str,
        port: u16,
        security: SmtpSecurity,
        username: &str,
        secret: LoginSecret,
    ) -> Result<AsyncSmtpTransport<Tokio1Executor>, DomainError> {
        let builder = Self::transport_builder(host, security)?.port(port);
        Ok(match secret {
            LoginSecret::Password(password) => builder.credentials(Credentials::new(username.to_owned(), password)),
            LoginSecret::OAuthToken(token) => builder
                .credentials(Credentials::new(username.to_owned(), token))
                .authentication(vec![Mechanism::Xoauth2]),
        }
        .build())
    }

    /// Send a NOOP to verify the SMTP server is reachable.
    pub async fn test_connection(&self) -> Result<bool, DomainError> {
        let context = "SMTP connection test failed";
        match &self.backend {
            SmtpBackend::Static(t) => {
                noted(self.gate.as_ref(), t.test_connection().await.map_err(|e| smtp_error(context, &e, false)))
            }
            SmtpBackend::Gated { host, port, security, username } => {
                login_with(&SmtpView(self.gate.as_ref()), |secret| async move {
                    let oauth = matches!(secret, LoginSecret::OAuthToken(_));
                    Self::gated_transport(host, *port, *security, username, secret)?
                        .test_connection()
                        .await
                        .map_err(|e| smtp_error(context, &e, oauth))
                })
                .await
            }
        }
    }

    /// Send an outgoing message via SMTP. A refused OAuth token is retried
    /// once with a refreshed one; the refusal comes before DATA, so the
    /// message is never sent twice.
    pub async fn send(&self, msg: &OutgoingMessage) -> Result<(), DomainError> {
        let email = build_message(msg, false)?;
        let context = "SMTP send error";
        match &self.backend {
            SmtpBackend::Static(t) => {
                let gate = self.gate.as_ref();
                noted(gate, t.send(email).await.map(drop).map_err(|e| sent_error(gate, context, &e, false)))
            }
            SmtpBackend::Gated { host, port, security, username } => {
                let gate = self.gate.as_ref();
                login_with(&SmtpView(gate), |secret| {
                    let email = email.clone();
                    async move {
                        let oauth = matches!(secret, LoginSecret::OAuthToken(_));
                        Self::gated_transport(host, *port, *security, username, secret)?
                            .send(email)
                            .await
                            .map(drop)
                            .map_err(|e| sent_error(gate, context, &e, oauth))
                    }
                })
                .await
            }
        }
    }
}

/// Tells the gate how a Static sender's login went.
fn noted<T>(gate: &dyn LoginGate, result: Result<T, DomainError>) -> Result<T, DomainError> {
    match &result {
        Ok(_) => gate.smtp_login(None),
        Err(DomainError::AuthenticationFailed(why)) => gate.smtp_login(Some(why.clone())),
        Err(_) => {}
    }
    result
}

/// The account's gate as SMTP may use it. SMTP never judges the account: a
/// refused password or a token still refused after the refresh is reported
/// for the user to see and fails the send; connection faults stay out of the
/// shared offline state, which belongs to IMAP.
struct SmtpView<'a>(&'a dyn LoginGate);

#[allow(clippy::double_must_use)] // async_trait's expansion
#[async_trait::async_trait]
impl LoginGate for SmtpView<'_> {
    async fn secret(&self) -> Result<LoginSecret, DomainError> {
        self.0.secret().await
    }

    async fn login_failed(&self, secret: &LoginSecret, error: &DomainError, retried: bool) -> bool {
        let DomainError::AuthenticationFailed(why) = error else { return false };
        self.0.smtp_login(Some(why.clone()));
        // The token is shared with IMAP: a refused one may be refreshed once.
        !retried && self.0.smtp_token_refused(secret).await
    }

    fn login_succeeded(&self) {
        self.0.smtp_login(None);
    }

    async fn ready(&self) {
        self.0.ready().await;
    }
}

/// A send that failed after the login (a refused recipient, a full mailbox)
/// still proves the SMTP login works, so its earlier refusal is cleared.
fn sent_error(gate: &dyn LoginGate, context: &str, e: &lettre::transport::smtp::Error, oauth: bool) -> DomainError {
    // Replies outside the 53x authentication family come from MAIL/RCPT/DATA.
    if e.status().is_some_and(|code| code.category != Category::Unspecified3) {
        gate.smtp_login(None);
    }
    smtp_error(context, e, oauth)
}

/// 535: the server refused the credentials.
fn rejects_credentials(code: Code) -> bool {
    code.severity == Severity::PermanentNegativeCompletion
        && code.category == Category::Unspecified3
        && code.detail == Detail::Five
}

/// Splits lettre's errors the way the login gate needs them: a refused login,
/// a dead link, or anything else (a refused recipient, a malformed message).
fn smtp_error(context: &str, e: &lettre::transport::smtp::Error, oauth: bool) -> DomainError {
    let message = format!("{context}: {e}");
    // A refused XOAUTH2 token comes back as a 334 error challenge, which lettre
    // fails on client-side before anything else is sent.
    let refused_token = oauth && e.is_client() && e.to_string().contains("does not expect a challenge");
    if refused_token || e.status().is_some_and(rejects_credentials) {
        DomainError::AuthenticationFailed(message)
    } else if e.is_timeout() {
        DomainError::ConnectionTimeout(message)
    } else if e.is_tls() || !(e.is_response() || e.is_client() || e.is_transient() || e.is_permanent()) {
        DomainError::ConnectionFailed(message)
    } else {
        DomainError::Provider(message)
    }
}

// ── Message assembly ──────────────────────────────────────────────────────────

/// RFC 5322 bytes of `msg` as stored by IMAP APPEND (Sent copy, drafts).
/// Unlike the SMTP wire form it keeps the Bcc header, as every client does
/// for its own copy.
pub fn format_for_storage(msg: &OutgoingMessage) -> Result<Vec<u8>, DomainError> {
    Ok(build_message(msg, true)?.formatted())
}

/// MIME layout: mixed( related( alternative(text, html), inline images ), attachments ),
/// each layer omitted when it would be empty.
fn build_message(msg: &OutgoingMessage, keep_bcc: bool) -> Result<Message, DomainError> {
    let mut builder = Message::builder()
        .from(to_mailbox(&msg.from)?)
        .subject(msg.subject.clone());

    for addr in &msg.to {
        builder = builder.to(to_mailbox(addr)?);
    }
    for addr in &msg.cc {
        builder = builder.cc(to_mailbox(addr)?);
    }
    for addr in &msg.bcc {
        builder = builder.bcc(to_mailbox(addr)?);
    }
    if keep_bcc {
        builder = builder.keep_bcc();
    }
    if let Some(id) = &msg.message_rfc_id {
        builder = builder.message_id(Some(id.clone()));
    }
    if let Some(irt) = &msg.in_reply_to {
        builder = builder.in_reply_to(irt.clone());
    }
    if !msg.references.is_empty() {
        builder = builder.references(msg.references.join(" "));
    }
    for (name, value) in &msg.extra_headers {
        let name = HeaderName::new_from_ascii(name.clone())
            .map_err(|e| DomainError::Provider(format!("Invalid header name '{name}': {e}")))?;
        builder = builder.raw_header(HeaderValue::new(name, value.clone()));
    }

    let text = SinglePart::plain(msg.body_text.clone().unwrap_or_default());
    let build_err = |e: lettre::error::Error| DomainError::Provider(format!("SMTP message build error: {e}"));

    let (inline, attached): (Vec<_>, Vec<_>) = msg
        .attachments
        .iter()
        .partition(|a| a.content_id.is_some() && msg.body_html.is_some());

    let Some(html) = &msg.body_html else {
        if attached.is_empty() {
            return builder.singlepart(text).map_err(build_err);
        }
        let mixed = attached
            .into_iter()
            .fold(MultiPart::mixed().singlepart(text), |mp, a| mp.singlepart(attachment_part(a)));
        return builder.multipart(mixed).map_err(build_err);
    };

    let alternative = MultiPart::alternative()
        .singlepart(text)
        .singlepart(SinglePart::html(html.clone()));
    let content = if inline.is_empty() {
        alternative
    } else {
        inline
            .into_iter()
            .fold(MultiPart::related().multipart(alternative), |mp, a| mp.singlepart(attachment_part(a)))
    };
    let body = if attached.is_empty() {
        content
    } else {
        attached
            .into_iter()
            .fold(MultiPart::mixed().multipart(content), |mp, a| mp.singlepart(attachment_part(a)))
    };
    builder.multipart(body).map_err(build_err)
}

fn attachment_part(a: &OutgoingAttachment) -> SinglePart {
    let content_type = ContentType::parse(&a.mime_type)
        .unwrap_or_else(|_| ContentType::parse("application/octet-stream").expect("valid mime"));
    let attachment = match &a.content_id {
        Some(cid) => Attachment::new_inline_with_name(cid.clone(), a.filename.clone()),
        None => Attachment::new(a.filename.clone()),
    };
    attachment.body(a.data.clone(), content_type)
}

fn to_mailbox(addr: &Address) -> Result<Mailbox, DomainError> {
    let email: lettre::Address = addr
        .email
        .as_str()
        .parse()
        .map_err(|e| DomainError::Provider(format!("Indirizzo non valido '{}': {e}", addr.email)))?;
    Ok(Mailbox::new(addr.name.clone(), email))
}

#[cfg(test)]
mod tests;
