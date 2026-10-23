use std::sync::Arc;

use lettre::{
    message::{
        header::{ContentType, HeaderName, HeaderValue},
        Attachment, Mailbox, MultiPart, SinglePart,
    },
    transport::smtp::{
        authentication::{Credentials, Mechanism},
        response::{Category, Code, Severity},
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
/// Async SMTP sender. `Static` builds its transport once at startup (existing
/// password-account behavior, unchanged). `OAuth` rebuilds the transport on
/// every send/test with the token the account's gate hands out — the transport
/// itself is cheap to construct (no I/O happens until
/// `.send()`/`.test_connection()` runs).
enum SmtpBackend {
    Static(AsyncSmtpTransport<Tokio1Executor>),
    OAuth {
        host: String,
        port: u16,
        security: SmtpSecurity,
        username: String,
        gate: Arc<dyn LoginGate>,
    },
}

pub struct SmtpSender {
    backend: SmtpBackend,
}

impl SmtpSender {
    /// Build an `SmtpSender` from the given SMTP config, falling back to
    /// `fallback_username` / `fallback_password` when the config does not
    /// provide its own credentials.
    pub fn build(
        config: &SmtpConnectionConfig,
        fallback_username: &str,
        fallback_password: &str,
    ) -> Result<Self, DomainError> {
        let username = config.username.as_deref().unwrap_or(fallback_username);
        let password = config.password.as_deref().unwrap_or(fallback_password);
        let creds = Credentials::new(username.to_owned(), password.to_owned());

        let builder = Self::transport_builder(&config.host, config.security)?;
        let transport = builder.port(config.port).credentials(creds).build();
        Ok(Self { backend: SmtpBackend::Static(transport) })
    }

    /// OAuth-authenticated sender, logging in through the same gate as the
    /// account's IMAP provider.
    pub fn build_oauth(config: &SmtpConnectionConfig, username: &str, gate: Arc<dyn LoginGate>) -> Self {
        Self {
            backend: SmtpBackend::OAuth {
                host: config.host.clone(),
                port: config.port,
                security: config.security,
                username: username.to_owned(),
                gate,
            },
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

    fn oauth_transport(
        host: &str,
        port: u16,
        security: SmtpSecurity,
        username: &str,
        secret: LoginSecret,
    ) -> Result<AsyncSmtpTransport<Tokio1Executor>, DomainError> {
        let (LoginSecret::OAuthToken(access_token) | LoginSecret::Password(access_token)) = secret;
        let creds = Credentials::new(username.to_owned(), access_token);
        let builder = Self::transport_builder(host, security)?;
        Ok(builder
            .port(port)
            .credentials(creds)
            .authentication(vec![Mechanism::Xoauth2])
            .build())
    }

    /// Send a NOOP to verify the SMTP server is reachable.
    pub async fn test_connection(&self) -> Result<bool, DomainError> {
        let context = "SMTP connection test failed";
        match &self.backend {
            SmtpBackend::Static(t) => t.test_connection().await.map_err(|e| smtp_error(context, &e)),
            SmtpBackend::OAuth { host, port, security, username, gate } => {
                login_with(gate.as_ref(), |secret| async move {
                    Self::oauth_transport(host, *port, *security, username, secret)?
                        .test_connection()
                        .await
                        .map_err(|e| smtp_error(context, &e))
                })
                .await
            }
        }
    }

    /// Send an outgoing message via SMTP. An OAuth login the server refuses is
    /// retried once with a refreshed token; the refusal comes before DATA, so
    /// the message is never sent twice.
    pub async fn send(&self, msg: &OutgoingMessage) -> Result<(), DomainError> {
        let email = build_message(msg, false)?;
        let context = "SMTP send error";
        match &self.backend {
            SmtpBackend::Static(t) => t.send(email).await.map(drop).map_err(|e| smtp_error(context, &e)),
            SmtpBackend::OAuth { host, port, security, username, gate } => {
                login_with(gate.as_ref(), |secret| {
                    let email = email.clone();
                    async move {
                        Self::oauth_transport(host, *port, *security, username, secret)?
                            .send(email)
                            .await
                            .map(drop)
                            .map_err(|e| smtp_error(context, &e))
                    }
                })
                .await
            }
        }
    }
}

/// 53x is the authentication family (535 bad credentials, 534 Gmail's "log in
/// via the web", 530 auth required).
fn rejects_credentials(code: Code) -> bool {
    code.severity == Severity::PermanentNegativeCompletion && code.category == Category::Unspecified3
}

/// Splits lettre's errors the way the login gate needs them: a refused login,
/// a dead link, or anything else (a refused recipient, a malformed message).
fn smtp_error(context: &str, e: &lettre::transport::smtp::Error) -> DomainError {
    let message = format!("{context}: {e}");
    if e.status().is_some_and(rejects_credentials) {
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
    use lettre::transport::smtp::response::Detail;

    #[test]
    fn only_the_53x_family_rejects_credentials() {
        let code = |severity, category, detail| Code::new(severity, category, detail);
        use Severity::{PermanentNegativeCompletion as Permanent, TransientNegativeCompletion as Transient};
        assert!(rejects_credentials(code(Permanent, Category::Unspecified3, Detail::Five)));
        assert!(rejects_credentials(code(Permanent, Category::Unspecified3, Detail::Four)));
        assert!(!rejects_credentials(code(Permanent, Category::MailSystem, Detail::Zero)), "550 refused recipient");
        assert!(!rejects_credentials(code(Transient, Category::Unspecified3, Detail::Four)));
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
