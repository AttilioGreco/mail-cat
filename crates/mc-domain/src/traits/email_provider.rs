use std::collections::HashMap;
use std::pin::Pin;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};

use crate::error::DomainError;
use crate::model::account::MailboxModel;
use crate::model::mailbox::Mailbox;
use crate::model::message::{Envelope, Message, OutgoingMessage};
use crate::value_objects::flag::Flag;
use crate::value_objects::mailbox_path::MailboxPath;
use crate::value_objects::message_key::MessageKey;
use crate::value_objects::remote_id::RemoteId;
use crate::value_objects::sync_cursor::SyncCursor;

/// Abstraction over email backends: IMAP, Gmail API, Outlook API, etc.
///
/// Implementors live in `mc-infrastructure`; the domain only knows this trait.
///
/// # Design notes
/// - Uses `async-trait` for Send-safe futures required by tonic/gRPC.
/// - `watch_mailbox` returns a `BoxStream` instead of `impl Stream` for
///   compatibility with trait-object usage and easier mocking.
/// - Intended to be used via **generics** (`<P: EmailProvider>`) in the
///   application layer. The server wiring knows the concrete type.
#[async_trait]
pub trait EmailProvider: Send + Sync {
    /// List all mailboxes (folders) for the account.
    async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError>;

    /// What changed in `mailbox` since `since` (`None`: never synced). The
    /// provider owns the strategy and the cursor format; the caller applies
    /// the changes and stores [`MailboxChanges::cursor`] only once it has.
    async fn mailbox_changes(
        &self,
        mailbox: &MailboxPath,
        since: Option<&SyncCursor>,
    ) -> Result<MailboxChanges, DomainError>;

    /// Envelopes (headers) of these messages, in no particular order; ids the
    /// server no longer has are simply absent. Bodies are fetched separately.
    async fn fetch_envelopes(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
    ) -> Result<Vec<MessageEnvelopeSummary>, DomainError>;

    /// Fetch the full message detail including body and attachments metadata.
    async fn fetch_message_detail(
        &self,
        mailbox: &MailboxPath,
        id: &RemoteId,
    ) -> Result<Message, DomainError>;

    /// Fetch bodies for multiple messages in bulk over one connection.
    /// Text only, each part cut at a size cap (see `MessageBodyResult::truncated`):
    /// this feeds the background backfill, which must never pull attachments.
    /// Returns one result per message; errors on individual messages are returned inline.
    async fn fetch_bodies_bulk(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
    ) -> Result<Vec<MessageBodyResult>, DomainError>;

    /// Ids of the messages with RFC Message-ID `rfc_id` (`<…>`) in
    /// `mailbox`, oldest first (callers `.rev()` for the newest); several when
    /// the mailbox holds copies. How a message whose id is unknown is found
    /// again.
    async fn find_by_message_id(
        &self,
        mailbox: &MailboxPath,
        rfc_id: &str,
    ) -> Result<Vec<RemoteId>, DomainError>;

    /// Fetch raw message bytes (RFC 2822).
    async fn fetch_raw_message(
        &self,
        mailbox: &MailboxPath,
        id: &RemoteId,
    ) -> Result<Vec<u8>, DomainError>;

    /// Set or unset flags on multiple messages in one provider round-trip.
    async fn set_flags_bulk(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
        flags_to_add: &[Flag],
        flags_to_remove: &[Flag],
    ) -> Result<(), DomainError>;

    /// Move many messages in one request: with one connection per operation,
    /// a loop over a few hundred messages is a few hundred TLS handshakes,
    /// which is what gets an account throttled.
    ///
    /// Returns the id each moved message got in `to`; `None` for those the
    /// provider did not report.
    async fn move_messages(
        &self,
        ids: &[RemoteId],
        from: &MailboxPath,
        to: &MailboxPath,
    ) -> Result<HashMap<RemoteId, Option<RemoteId>>, DomainError>;

    /// Delete many messages in one request (move to Trash or permanent
    /// delete). Same rationale as [`Self::move_messages`].
    async fn delete_messages(
        &self,
        mailbox: &MailboxPath,
        ids: &[RemoteId],
        permanently: bool,
    ) -> Result<(), DomainError>;

    /// Add `add` to and remove `remove` from the mailboxes the emails are in,
    /// in one atomic change per email (`in_mailbox` is one they are all in).
    /// An email keeps its `RemoteId` in the mailboxes it is added to, and must
    /// stay in at least one. Only providers whose mailboxes are labels (JMAP)
    /// support it; plain IMAP mailboxes are folders, moved with
    /// [`Self::move_messages`].
    async fn change_labels(
        &self,
        _ids: &[RemoteId],
        _in_mailbox: &MailboxPath,
        _add: &[MailboxPath],
        _remove: &[MailboxPath],
    ) -> Result<(), DomainError> {
        Err(DomainError::NotSupported(
            "this account's mailboxes are folders, not labels".into(),
        ))
    }

    /// The mailbox whose sync carries `mailbox`'s messages, when `mailbox` is
    /// only a view over it (a Gmail label over All Mail). Views are never
    /// synced themselves, and their memberships hold the backing mailbox's ids.
    fn backed_by(&self, _mailbox: &MailboxPath) -> Option<MailboxPath> {
        None
    }

    /// How this account's mailboxes should be presented unless the user chose,
    /// when the server decides it (Gmail: labels).
    fn preferred_mailbox_model(&self) -> Option<MailboxModel> {
        None
    }

    /// Whether the server takes arbitrary keywords (tags) on messages, when
    /// known: IMAP learns it from PERMANENTFLAGS on the first SELECT.
    fn supports_tags(&self) -> Option<bool> {
        Some(true)
    }

    /// Send a message via the provider's send mechanism (SMTP, API, etc.).
    /// `sent` is the account's Sent mailbox, if known: the provider files the
    /// sent copy there when the server does not do it by itself, and reports
    /// what happened in [`SendOutcome`]. Failing to store the copy never fails
    /// a send that went out.
    async fn send_message(
        &self,
        message: &OutgoingMessage,
        sent: Option<&MailboxPath>,
    ) -> Result<SendOutcome, DomainError>;

    /// Store `message` as a draft in `drafts`, then drop the draft it
    /// `replacing` (best effort: the new one is already safe). Returns the id
    /// the new draft has.
    async fn save_draft(
        &self,
        drafts: &MailboxPath,
        message: &OutgoingMessage,
        replacing: Option<&RemoteId>,
    ) -> Result<RemoteId, DomainError>;

    /// Subscribe to real-time notifications (IMAP IDLE, push, etc.).
    ///
    /// Returns a boxed stream for trait-object compatibility.
    /// Implementations should reconnect internally on transient failures.
    fn watch_mailbox(
        &self,
        mailbox: &MailboxPath,
    ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>>;
}

/// What became of the copy of a sent message that belongs in Sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendOutcome {
    pub sent_copy: SentCopy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SentCopy {
    /// The provider filed it in the Sent mailbox (IMAP APPEND, JMAP
    /// `onSuccessUpdateEmail`); its id there when known.
    Stored { remote_id: Option<RemoteId> },
    /// The server files submissions itself (Gmail SMTP): the next Sent sync
    /// brings the copy.
    ServerFiled,
    /// No Sent mailbox was given, or the provider could not store the copy.
    NotStored,
}

/// What a provider reports for one mailbox since a cursor.
#[derive(Debug, Clone)]
pub struct MailboxChanges {
    /// To store once the changes are fully applied, and to pass back next time.
    pub cursor: SyncCursor,
    /// The cursor passed in could not be used.
    pub reset: Option<Reset>,
    /// Arrived since the cursor (every message on a first sync or after a
    /// reset), newest first.
    pub added: Vec<RemoteId>,
    /// The [`MessageKey`] of those `added` the provider can tell across
    /// mailboxes; the others are absent.
    pub keys: HashMap<RemoteId, MessageKey>,
    /// The views (see [`EmailProvider::backed_by`]) each of `added` and each
    /// changed message is in, besides this mailbox. Empty for providers
    /// without views.
    pub labels: HashMap<RemoteId, Vec<MailboxPath>>,
    /// State of the messages already known.
    pub existing: Existing,
    /// The server's totals for the mailbox.
    pub counts: MailboxCounts,
}

/// Why a cursor was not usable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reset {
    /// Every id seen before in this mailbox is meaningless now (IMAP
    /// mailbox renumbered by its server): messages must be matched again by
    /// Message-ID.
    IdsInvalidated,
    /// Ids still hold, but the changes since the cursor cannot be computed
    /// (JMAP `cannotCalculateChanges`): `existing` is a full listing.
    Rescan,
}

/// The state of messages the caller already knows, as the server sees it.
/// Provider-specific markers (IMAP `\Deleted`, `\Recent`) are resolved by the
/// provider: a message that is soft-deleted counts as absent.
#[derive(Debug, Clone)]
pub enum Existing {
    /// Every message the mailbox has, with its flags: one that is absent
    /// from it is gone.
    Snapshot(Vec<(RemoteId, Vec<Flag>)>),
    /// What changed since the cursor, and every id still there: a known id
    /// absent from `present` is gone (for servers that report changes but not
    /// expunges).
    Listed {
        present: Vec<RemoteId>,
        changed: Vec<(RemoteId, Vec<Flag>)>,
    },
    /// Only what changed since the cursor.
    Delta {
        changed: Vec<(RemoteId, Vec<Flag>)>,
        removed: Vec<RemoteId>,
    },
}

/// Message counts as the server reports them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailboxCounts {
    pub total: u32,
    pub unread: u32,
}

/// Lightweight envelope summary for list views (without body).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEnvelopeSummary {
    pub remote_id: RemoteId,
    /// RFC Message-ID header (e.g. `<abc@mail.example.com>`).
    pub message_rfc_id: Option<String>,
    /// RFC In-Reply-To header.
    pub in_reply_to: Option<String>,
    /// RFC References header (ordered, oldest first).
    pub references: Vec<String>,
    pub envelope: Envelope,
    pub flags: Vec<Flag>,
    pub size_bytes: u64,
    pub has_attachments: bool,
    pub internal_date: DateTime<Utc>,
    pub key: Option<MessageKey>,
}

/// Result of a bulk body fetch for a single message.
#[derive(Debug, Clone)]
pub struct MessageBodyResult {
    pub remote_id: RemoteId,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    /// Only the start of a long text part was downloaded.
    pub truncated: bool,
}

/// Real-time notifications from provider.
#[derive(Debug, Clone)]
pub enum ProviderNotification {
    NewMessage {
        mailbox: MailboxPath,
        remote_id: RemoteId,
    },
    FlagsChanged {
        mailbox: MailboxPath,
        remote_id: RemoteId,
        flags: Vec<Flag>,
    },
    // Note: expunges are NOT delivered as a dedicated notification. Plain IMAP
    // IDLE reports EXPUNGE only by sequence number, which can't be reliably
    // resolved back to a UID after the message is gone. Instead the watcher emits
    // MailboxCountChanged, and the mailbox sync detects the deletion.
    MailboxCountChanged {
        mailbox: MailboxPath,
        total: u32,
        unseen: u32,
    },
}
