use lettre::message::{header, Mailbox, Message, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{SmtpTransport, Transport};
use tracing::{debug, info};

use mc_domain::error::DomainError;
use mc_domain::model::message::OutgoingMessage;

/// SMTP sender implementation using lettre.
///
/// Handles email sending with authentication, TLS, and attachment support.
pub struct SmtpSender {
    host: String,
    port: u16,
    username: String,
    password: String,
    use_tls: bool,
}

impl SmtpSender {
    /// Create a new SMTP sender configuration.
    pub fn new(host: String, port: u16, username: String, password: String, use_tls: bool) -> Self {
        Self {
            host,
            port,
            username,
            password,
            use_tls,
        }
    }

    /// Send an outgoing message via SMTP.
    ///
    /// Day 9: Complete implementation with lettre.
    pub async fn send(&self, outgoing: &OutgoingMessage) -> Result<(), DomainError> {
        info!("Sending email via SMTP to {} recipients", outgoing.to.len());

        // Build lettre message
        let email = self.build_message(outgoing)?;

        // Create SMTP transport
        let transport = self.create_transport()?;

        // Send message (blocking operation, run in spawn_blocking)
        let transport_clone = transport.clone();
        let email_clone = email.clone();

        tokio::task::spawn_blocking(move || {
            transport_clone
                .send(&email_clone)
                .map_err(|e| DomainError::Provider(format!("SMTP send failed: {}", e)))
        })
        .await
        .map_err(|e| DomainError::Provider(format!("Task join error: {}", e)))??;

        info!("Email sent successfully");
        Ok(())
    }

    /// Build lettre Message from domain OutgoingMessage.
    fn build_message(&self, outgoing: &OutgoingMessage) -> Result<Message, DomainError> {
        // Parse from address - Address.email is a String
        let from: Mailbox = outgoing
            .from
            .email
            .as_str()
            .parse()
            .map_err(|e| DomainError::Provider(format!("Invalid from address: {}", e)))?;

        // Parse to addresses
        let to: Vec<Mailbox> = outgoing
            .to
            .iter()
            .map(|addr| {
                addr.email
                    .as_str()
                    .parse()
                    .map_err(|e| DomainError::Provider(format!("Invalid to address: {}", e)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Parse cc addresses
        let cc: Vec<Mailbox> = outgoing
            .cc
            .iter()
            .map(|addr| {
                addr.email
                    .as_str()
                    .parse()
                    .map_err(|e| DomainError::Provider(format!("Invalid cc address: {}", e)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Parse bcc addresses
        let bcc: Vec<Mailbox> = outgoing
            .bcc
            .iter()
            .map(|addr| {
                addr.email
                    .as_str()
                    .parse()
                    .map_err(|e| DomainError::Provider(format!("Invalid bcc address: {}", e)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Start building message
        let mut email_builder = Message::builder()
            .from(from)
            .subject(&outgoing.subject);

        // Add to recipients
        for mailbox in to {
            email_builder = email_builder.to(mailbox);
        }

        // Add cc recipients
        for mailbox in cc {
            email_builder = email_builder.cc(mailbox);
        }

        // Add bcc recipients
        for mailbox in bcc {
            email_builder = email_builder.bcc(mailbox);
        }

        // Add In-Reply-To header if present
        if let Some(ref in_reply_to) = outgoing.in_reply_to {
            email_builder = email_builder.header(header::InReplyTo::from(in_reply_to.clone()));
        }

        // Add References header if present
        if !outgoing.references.is_empty() {
            let references = outgoing.references.join(" ");
            email_builder = email_builder.header(header::References::from(references));
        }

        // Build body (multipart if both text and HTML)
        let body = if outgoing.body_html.is_some() && outgoing.body_text.is_some() {
            // Multipart alternative: text + HTML
            MultiPart::alternative()
                .singlepart(
                    SinglePart::builder()
                        .header(header::ContentType::TEXT_PLAIN)
                        .body(outgoing.body_text.clone().unwrap_or_default()),
                )
                .singlepart(
                    SinglePart::builder()
                        .header(header::ContentType::TEXT_HTML)
                        .body(outgoing.body_html.clone().unwrap_or_default()),
                )
        } else if let Some(ref html) = outgoing.body_html {
            // HTML only
            MultiPart::alternative().singlepart(
                SinglePart::builder()
                    .header(header::ContentType::TEXT_HTML)
                    .body(html.clone()),
            )
        } else {
            // Text only (or empty)
            MultiPart::alternative().singlepart(
                SinglePart::builder()
                    .header(header::ContentType::TEXT_PLAIN)
                    .body(outgoing.body_text.clone().unwrap_or_default()),
            )
        };

        // TODO: Add attachment support (Day 9 extension or Day 10)
        if !outgoing.attachments.is_empty() {
            debug!(
                "Note: {} attachments not yet supported, will be added",
                outgoing.attachments.len()
            );
        }

        // Build final message
        let email = email_builder
            .multipart(body)
            .map_err(|e| DomainError::Provider(format!("Failed to build email: {}", e)))?;

        Ok(email)
    }

    /// Create SMTP transport with authentication and TLS.
    fn create_transport(&self) -> Result<SmtpTransport, DomainError> {
        debug!(
            "Creating SMTP transport: {}:{} (TLS: {})",
            self.host, self.port, self.use_tls
        );

        let credentials = Credentials::new(self.username.clone(), self.password.clone());

        let transport = if self.use_tls {
            SmtpTransport::relay(&self.host)
                .map_err(|e| DomainError::Provider(format!("SMTP relay setup failed: {}", e)))?
                .port(self.port)
                .credentials(credentials)
                .build()
        } else {
            SmtpTransport::builder_dangerous(&self.host)
                .port(self.port)
                .credentials(credentials)
                .build()
        };

        Ok(transport)
    }

    /// Get configuration for display/debugging.
    pub fn config(&self) -> (&str, u16, &str, bool) {
        (&self.host, self.port, &self.username, self.use_tls)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mc_domain::model::message::Address;
    use mc_domain::value_objects::email_address::EmailAddress;

    #[test]
    fn test_smtp_sender_creation() {
        let sender = SmtpSender::new(
            "smtp.example.com".to_string(),
            587,
            "user@example.com".to_string(),
            "password".to_string(),
            true,
        );

        let (host, port, username, use_tls) = sender.config();
        assert_eq!(host, "smtp.example.com");
        assert_eq!(port, 587);
        assert_eq!(username, "user@example.com");
        assert!(use_tls);
    }

    #[test]
    fn test_build_message_simple() {
        let sender = SmtpSender::new(
            "smtp.example.com".to_string(),
            587,
            "sender@example.com".to_string(),
            "password".to_string(),
            true,
        );

        let outgoing = OutgoingMessage {
            from: Address {
                name: Some("Sender Name".to_string()),
                email: EmailAddress::new("sender@example.com").unwrap(),
            },
            to: vec![Address {
                name: Some("Recipient Name".to_string()),
                email: EmailAddress::new("recipient@example.com").unwrap(),
            }],
            cc: vec![],
            bcc: vec![],
            subject: "Test Subject".to_string(),
            body_text: Some("Hello, world!".to_string()),
            body_html: None,
            in_reply_to: None,
            references: vec![],
            attachments: vec![],
        };

        let message = sender.build_message(&outgoing);
        assert!(message.is_ok());
    }

    #[test]
    fn test_build_message_multipart() {
        let sender = SmtpSender::new(
            "smtp.example.com".to_string(),
            587,
            "sender@example.com".to_string(),
            "password".to_string(),
            true,
        );

        let outgoing = OutgoingMessage {
            from: Address {
                name: None,
                email: EmailAddress::new("sender@example.com").unwrap(),
            },
            to: vec![Address {
                name: None,
                email: EmailAddress::new("recipient@example.com").unwrap(),
            }],
            cc: vec![],
            bcc: vec![],
            subject: "Test Subject".to_string(),
            body_text: Some("Plain text version".to_string()),
            body_html: Some("<p>HTML version</p>".to_string()),
            in_reply_to: Some("<previous@example.com>".to_string()),
            references: vec!["<ref1@example.com>".to_string(), "<ref2@example.com>".to_string()],
            attachments: vec![],
        };

        let message = sender.build_message(&outgoing);
        assert!(message.is_ok());
    }
}
