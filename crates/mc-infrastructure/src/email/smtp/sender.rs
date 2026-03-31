use lettre::{
    message::{header::ContentType, Mailbox, MultiPart, SinglePart},
    transport::smtp::authentication::Credentials,
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
};
use mc_domain::{
    error::DomainError,
    model::{
        account::{SmtpConnectionConfig, SmtpSecurity},
        message::{Address, OutgoingMessage},
    },
};

/// Async SMTP sender backed by lettre's connection-pooled transport.
///
/// Build once per account at startup and reuse across sends.
pub struct SmtpSender {
    transport: AsyncSmtpTransport<Tokio1Executor>,
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

        let builder = match config.security {
            SmtpSecurity::Smtps => {
                AsyncSmtpTransport::<Tokio1Executor>::relay(&config.host)
                    .map_err(|e| DomainError::Provider(format!("SMTP relay build error: {e}")))?
            }
            SmtpSecurity::StartTls => {
                AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&config.host)
                    .map_err(|e| DomainError::Provider(format!("SMTP starttls build error: {e}")))?
            }
        };

        let transport = builder.port(config.port).credentials(creds).build();
        Ok(Self { transport })
    }

    /// Send a NOOP to verify the SMTP server is reachable.
    pub async fn test_connection(&self) -> Result<bool, DomainError> {
        self.transport
            .test_connection()
            .await
            .map_err(|e| DomainError::Provider(format!("SMTP connection test failed: {e}")))
    }

    /// Send an outgoing message via SMTP.
    pub async fn send(&self, msg: &OutgoingMessage) -> Result<(), DomainError> {
        let email = build_message(msg)?;
        self.transport
            .send(email)
            .await
            .map_err(|e| DomainError::Provider(format!("SMTP send error: {e}")))?;
        Ok(())
    }
}

// ── Message assembly ──────────────────────────────────────────────────────────

fn build_message(msg: &OutgoingMessage) -> Result<Message, DomainError> {
    let mut builder = Message::builder()
        .from(to_mailbox(&msg.from)?)
        .reply_to(to_mailbox(&msg.from)?)
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

    if let Some(irt) = &msg.in_reply_to {
        builder = builder.in_reply_to(irt.clone());
    }
    if !msg.references.is_empty() {
        builder = builder.references(msg.references.join(" "));
    }

    let body_text = msg.body_text.clone().unwrap_or_default();

    let email = match &msg.body_html {
        Some(html) => builder
            .multipart(
                MultiPart::alternative()
                    .singlepart(
                        SinglePart::builder()
                            .header(ContentType::TEXT_PLAIN)
                            .body(body_text),
                    )
                    .singlepart(
                        SinglePart::builder()
                            .header(ContentType::TEXT_HTML)
                            .body(html.clone()),
                    ),
            )
            .map_err(|e| DomainError::Provider(format!("SMTP message build error: {e}")))?,
        None => builder
            .body(body_text)
            .map_err(|e| DomainError::Provider(format!("SMTP message build error: {e}")))?,
    };

    Ok(email)
}

fn to_mailbox(addr: &Address) -> Result<Mailbox, DomainError> {
    let email: lettre::Address = addr
        .email
        .as_str()
        .parse()
        .map_err(|e| DomainError::Provider(format!("Invalid email '{}': {e}", addr.email)))?;
    Ok(Mailbox::new(addr.name.clone(), email))
}
