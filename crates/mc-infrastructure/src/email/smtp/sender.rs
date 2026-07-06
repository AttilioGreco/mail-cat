use std::sync::Arc;

use lettre::{
    message::{header::ContentType, Mailbox, MultiPart, SinglePart},
    transport::smtp::authentication::{Credentials, Mechanism},
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
};
use mc_domain::{
    error::DomainError,
    model::{
        account::{SmtpConnectionConfig, SmtpSecurity},
        message::{Address, OutgoingMessage},
    },
};
use tokio::sync::RwLock;

/// Async SMTP sender. `Static` builds its transport once at startup (existing
/// password-account behavior, unchanged). `OAuth` rebuilds the transport on
/// every send/test since the shared access token can rotate between calls —
/// the transport itself is cheap to construct (no I/O happens until
/// `.send()`/`.test_connection()` runs).
enum SmtpBackend {
    Static(AsyncSmtpTransport<Tokio1Executor>),
    OAuth {
        host: String,
        port: u16,
        security: SmtpSecurity,
        username: String,
        token: Arc<RwLock<String>>,
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

    /// OAuth-authenticated sender. `token` is shared with the account's IMAP
    /// provider so a background token refresh is visible immediately.
    pub fn build_oauth(
        config: &SmtpConnectionConfig,
        username: &str,
        token: Arc<RwLock<String>>,
    ) -> Self {
        Self {
            backend: SmtpBackend::OAuth {
                host: config.host.clone(),
                port: config.port,
                security: config.security,
                username: username.to_owned(),
                token,
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

    async fn oauth_transport(
        host: &str,
        port: u16,
        security: SmtpSecurity,
        username: &str,
        token: &RwLock<String>,
    ) -> Result<AsyncSmtpTransport<Tokio1Executor>, DomainError> {
        let access_token = token.read().await.clone();
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
        match &self.backend {
            SmtpBackend::Static(t) => t.test_connection().await,
            SmtpBackend::OAuth { host, port, security, username, token } => {
                Self::oauth_transport(host, *port, *security, username, token)
                    .await?
                    .test_connection()
                    .await
            }
        }
        .map_err(|e| DomainError::Provider(format!("SMTP connection test failed: {e}")))
    }

    /// Send an outgoing message via SMTP.
    pub async fn send(&self, msg: &OutgoingMessage) -> Result<(), DomainError> {
        let email = build_message(msg)?;
        match &self.backend {
            SmtpBackend::Static(t) => t.send(email).await,
            SmtpBackend::OAuth { host, port, security, username, token } => {
                Self::oauth_transport(host, *port, *security, username, token)
                    .await?
                    .send(email)
                    .await
            }
        }
        .map_err(|e| DomainError::Provider(format!("SMTP send error: {e}")))?;
        Ok(())
    }

    #[cfg(test)]
    async fn current_token_for_test(&self) -> String {
        match &self.backend {
            SmtpBackend::OAuth { token, .. } => token.read().await.clone(),
            SmtpBackend::Static(_) => panic!("current_token_for_test called on a Static sender"),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn test_config() -> SmtpConnectionConfig {
        SmtpConnectionConfig {
            host: "smtp.example.com".into(),
            port: 587,
            security: SmtpSecurity::StartTls,
            username: None,
            password: None,
        }
    }

    #[tokio::test]
    async fn oauth_sender_reads_current_token_from_shared_cell() {
        let token = Arc::new(RwLock::new("first-token".to_string()));
        let sender = SmtpSender::build_oauth(&test_config(), "me@example.com", token.clone());

        // Simulate the background refresher rotating the token.
        *token.write().await = "refreshed-token".to_string();

        assert_eq!(sender.current_token_for_test().await, "refreshed-token");
    }
}
