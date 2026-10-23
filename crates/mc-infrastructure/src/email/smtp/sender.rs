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
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn only_535_rejects_credentials() {
        let code = |severity, category, detail| Code::new(severity, category, detail);
        use Severity::{PermanentNegativeCompletion as Permanent, TransientNegativeCompletion as Transient};
        assert!(rejects_credentials(code(Permanent, Category::Unspecified3, Detail::Five)));
        assert!(!rejects_credentials(code(Permanent, Category::Unspecified3, Detail::Zero)), "530 asks to log in first");
        assert!(!rejects_credentials(code(Permanent, Category::MailSystem, Detail::Zero)), "550 refused recipient");
        assert!(!rejects_credentials(code(Transient, Category::Unspecified3, Detail::Four)));
    }

    #[test]
    fn a_challenged_xoauth2_login_is_a_refused_token() {
        // What lettre fails with when the server answers XOAUTH2 with its 334 error challenge.
        let creds = Credentials::new("me@gmail.com".into(), "ya29.stale".into());
        let e = Mechanism::Xoauth2.response(&creds, Some("eyJzdGF0dXMiOiI0MDAifQ==")).unwrap_err();
        assert!(matches!(smtp_error("SMTP send error", &e, true), DomainError::AuthenticationFailed(_)));
        assert!(!smtp_error("SMTP send error", &e, true).to_string().contains("ya29"), "never echoes the token");
        // The same client error on a PLAIN login is not a refused token.
        assert!(!matches!(smtp_error("SMTP send error", &e, false), DomainError::AuthenticationFailed(_)));
    }

    /// Records what SMTP tells the account's gate.
    #[derive(Default)]
    struct Recorder {
        secret: Mutex<Option<LoginSecret>>,
        judged: Mutex<Vec<bool>>,
        smtp: Mutex<Vec<Option<String>>>,
    }

    #[allow(clippy::double_must_use)]
    #[async_trait::async_trait]
    impl LoginGate for Recorder {
        async fn secret(&self) -> Result<LoginSecret, DomainError> {
            Ok(self.secret.lock().unwrap().clone().unwrap())
        }
        async fn login_failed(&self, _: &LoginSecret, _: &DomainError, _: bool) -> bool {
            panic!("SMTP must not judge the account");
        }
        async fn smtp_token_refused(&self, rejected: &LoginSecret) -> bool {
            self.judged.lock().unwrap().push(matches!(rejected, LoginSecret::OAuthToken(_)));
            matches!(rejected, LoginSecret::OAuthToken(_))
        }
        fn login_succeeded(&self) {
            panic!("SMTP must not report a login to the account state");
        }
        async fn ready(&self) {}
        fn smtp_login(&self, refused: Option<String>) {
            self.smtp.lock().unwrap().push(refused);
        }
    }

    fn refused() -> DomainError {
        DomainError::AuthenticationFailed("permanent error (535): 5.7.8 Username and Password not accepted".into())
    }

    /// A wrong `[accounts.smtp] username`: the send fails, the user is told,
    /// and the account's state is never asked to change.
    #[tokio::test]
    async fn a_refused_smtp_password_never_judges_the_account() {
        let gate = Recorder::default();
        *gate.secret.lock().unwrap() = Some(LoginSecret::Password("hunter2".into()));

        let result: Result<(), _> = login_with(&SmtpView(&gate), |_| async { Err(refused()) }).await;

        assert!(matches!(result, Err(DomainError::AuthenticationFailed(_))));
        assert_eq!(*gate.judged.lock().unwrap(), [false], "no refresh for a password, no verdict");
        assert_eq!(gate.smtp.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_refused_smtp_token_is_refreshed_once_and_never_pauses() {
        let gate = Recorder::default();
        *gate.secret.lock().unwrap() = Some(LoginSecret::OAuthToken("ya29.a".into()));
        let attempts = Mutex::new(0);

        let result: Result<(), _> = login_with(&SmtpView(&gate), |_| {
            *attempts.lock().unwrap() += 1;
            async { Err(refused()) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(*attempts.lock().unwrap(), 2, "one retry after the forced refresh");
        assert_eq!(*gate.judged.lock().unwrap(), [true], "one refresh request, nothing more");
    }

    fn addr(email: &str) -> Address {
        Address {
            name: None,
            email: mc_domain::EmailAddress::new(email).unwrap(),
        }
    }

    fn outgoing() -> OutgoingMessage {
        OutgoingMessage {
            from: Address {
                name: Some("Mario Rossi".into()),
                email: mc_domain::EmailAddress::new("mario@example.com").unwrap(),
            },
            to: vec![addr("anna@example.com")],
            cc: vec![],
            bcc: vec![addr("hidden@example.com")],
            subject: "Ciao".into(),
            body_text: Some("testo".into()),
            body_html: Some("<p>testo <img src=\"cid:img1\"></p>".into()),
            in_reply_to: Some("<orig@example.com>".into()),
            references: vec!["<root@example.com>".into(), "<orig@example.com>".into()],
            attachments: vec![],
            message_rfc_id: Some("<new@example.com>".into()),
            extra_headers: vec![("X-Mail-Cat-Mode".into(), "reply".into())],
        }
    }

    fn attachment(name: &str, cid: Option<&str>) -> OutgoingAttachment {
        OutgoingAttachment {
            filename: name.into(),
            mime_type: "image/png".into(),
            content_id: cid.map(Into::into),
            data: vec![1, 2, 3],
        }
    }

    fn formatted(msg: &OutgoingMessage, keep_bcc: bool) -> String {
        String::from_utf8(build_message(msg, keep_bcc).unwrap().formatted()).unwrap()
    }

    #[test]
    fn mime_nests_mixed_related_alternative() {
        let mut msg = outgoing();
        msg.attachments = vec![attachment("doc.png", None), attachment("img1.png", Some("img1"))];
        let raw = formatted(&msg, false);

        let mixed = raw.find("multipart/mixed").expect("mixed layer");
        let related = raw.find("multipart/related").expect("related layer");
        let alternative = raw.find("multipart/alternative").expect("alternative layer");
        assert!(mixed < related && related < alternative, "{raw}");
        assert!(raw.contains("Content-ID: <img1>"), "{raw}");
        assert!(raw.contains("Content-Disposition: attachment; filename=\"doc.png\""), "{raw}");
        assert!(raw.contains("From: \"Mario Rossi\" <mario@example.com>") || raw.contains("From: Mario Rossi <mario@example.com>"), "{raw}");
        assert!(raw.contains("Message-ID: <new@example.com>"), "{raw}");
        assert!(raw.contains("In-Reply-To: <orig@example.com>"), "{raw}");
        assert!(raw.contains("X-Mail-Cat-Mode: reply"), "{raw}");
    }

    #[test]
    fn mime_omits_empty_layers() {
        let raw = formatted(&outgoing(), false);
        assert!(raw.contains("multipart/alternative"));
        assert!(!raw.contains("multipart/mixed") && !raw.contains("multipart/related"), "{raw}");

        let mut plain = outgoing();
        plain.body_html = None;
        plain.attachments = vec![attachment("a.png", Some("ignored-without-html"))];
        let raw = formatted(&plain, false);
        assert!(raw.contains("multipart/mixed") && !raw.contains("multipart/alternative"), "{raw}");
    }

    #[test]
    fn long_extra_header_is_folded_and_unfolds_intact() {
        use mailparse::MailHeaderMap;
        let words: Vec<String> = (0..400).map(|i| format!("{i:076}")).collect();
        let mut msg = outgoing();
        msg.extra_headers = vec![("X-Mail-Cat-Markdown".into(), words.join(" "))];
        let raw = format_for_storage(&msg).unwrap();
        assert!(raw.split(|b| *b == b'\n').all(|l| l.len() <= 998));
        let parsed = mailparse::parse_mail(&raw).unwrap();
        let value = parsed.headers.get_first_value("X-Mail-Cat-Markdown").unwrap();
        assert_eq!(value.split_whitespace().collect::<Vec<_>>(), words);
    }

    #[test]
    fn bcc_is_kept_only_in_the_stored_copy() {
        assert!(!formatted(&outgoing(), false).contains("hidden@example.com"));
        assert!(formatted(&outgoing(), true).contains("Bcc: hidden@example.com"));
    }
}
