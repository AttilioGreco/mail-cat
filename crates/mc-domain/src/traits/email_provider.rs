use std::pin::Pin;

use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};

use crate::error::DomainError;
use crate::model::mailbox::Mailbox;
use crate::model::message::{Envelope, Message, OutgoingMessage};
use crate::value_objects::flag::Flag;
use crate::value_objects::mailbox_path::MailboxPath;
use crate::value_objects::uid::ProviderUid;

/// Abstraction over email backends: IMAP, Gmail API, Outlook API, etc.
///
/// Implementors live in `mc-infrastructure`; the domain only knows this trait.
///
/// # Design notes
/// - Uses `async fn` (Rust 2024 native, no `async-trait` crate needed).
/// - `watch_mailbox` returns a `BoxStream` instead of `impl Stream` for
///   compatibility with trait-object usage and easier mocking.
/// - Intended to be used via **generics** (`<P: EmailProvider>`) in the
///   application layer. The server wiring knows the concrete type.
pub trait EmailProvider: Send + Sync {
    /// List all mailboxes (folders) for the account.
    async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError>;

    /// Fetch message envelopes (headers) from a mailbox.
    /// Returns lightweight summaries; bodies are fetched separately.
    async fn fetch_message_envelopes(
        &self,
        mailbox: &MailboxPath,
        range: FetchRange,
    ) -> Result<Vec<MessageEnvelopeSummary>, DomainError>;

    /// Fetch the full message detail including body and attachments metadata.
    async fn fetch_message_detail(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
    ) -> Result<Message, DomainError>;

    /// Fetch raw message bytes (RFC 2822).
    async fn fetch_raw_message(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
    ) -> Result<Vec<u8>, DomainError>;

    /// Set or unset flags on a message.
    async fn set_flags(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
        flags_to_add: &[Flag],
        flags_to_remove: &[Flag],
    ) -> Result<(), DomainError>;

    /// Move a message between mailboxes. Returns the new UID if available.
    async fn move_message(
        &self,
        uid: ProviderUid,
        from: &MailboxPath,
        to: &MailboxPath,
    ) -> Result<Option<ProviderUid>, DomainError>;

    /// Delete a message (move to Trash or permanent delete).
    async fn delete_message(
        &self,
        mailbox: &MailboxPath,
        uid: ProviderUid,
        permanently: bool,
    ) -> Result<(), DomainError>;

    /// Send a message via the provider's send mechanism (SMTP, API, etc.).
    async fn send_message(&self, message: &OutgoingMessage) -> Result<(), DomainError>;

    /// Get UIDVALIDITY and UIDNEXT for a mailbox (for sync state detection).
    async fn mailbox_status(&self, mailbox: &MailboxPath) -> Result<MailboxStatus, DomainError>;

    /// Subscribe to real-time notifications (IMAP IDLE, push, etc.).
    ///
    /// Returns a boxed stream for trait-object compatibility.
    /// Implementations should reconnect internally on transient failures.
    fn watch_mailbox(
        &self,
        mailbox: &MailboxPath,
    ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>>;
}

/// Range specification for fetching messages.
#[derive(Debug, Clone)]
pub enum FetchRange {
    /// Fetch messages by IMAP UID range (inclusive).
    UidRange { from: u32, to: u32 },
    /// Fetch the last N messages.
    Last(u32),
    /// Fetch messages since a given date.
    Since(DateTime<Utc>),
    /// Fetch all messages.
    All,
}

/// Lightweight envelope summary for list views (without body).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEnvelopeSummary {
    pub provider_uid: ProviderUid,
    pub message_rfc_id: Option<String>,
    pub envelope: Envelope,
    pub flags: Vec<Flag>,
    pub size_bytes: u64,
    pub has_attachments: bool,
    pub internal_date: DateTime<Utc>,
}

/// Mailbox metadata for sync comparison.
#[derive(Debug, Clone)]
pub struct MailboxStatus {
    pub uid_validity: u32,
    pub uid_next: u32,
    pub total_messages: u32,
    pub unseen_messages: u32,
}

/// Real-time notifications from provider.
#[derive(Debug, Clone)]
pub enum ProviderNotification {
    NewMessage {
        mailbox: MailboxPath,
        uid: ProviderUid,
    },
    FlagsChanged {
        mailbox: MailboxPath,
        uid: ProviderUid,
        flags: Vec<Flag>,
    },
    MessageExpunged {
        mailbox: MailboxPath,
        uid: ProviderUid,
    },
    MailboxCountChanged {
        mailbox: MailboxPath,
        total: u32,
        unseen: u32,
    },
}
