use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::account::{AccountId, ConnectionConfig, ProviderType};
use crate::model::attachment::Attachment;
use crate::model::contact::{ContactEmail, ContactSource};
use crate::model::mailbox::{MailboxId, MailboxType};
use crate::model::message::{Envelope, Membership};
use crate::value_objects::contact_id::ContactId;
use crate::value_objects::email_address::EmailAddress;
use crate::value_objects::flag::Flag;
use crate::value_objects::mailbox_path::MailboxPath;
use crate::value_objects::message_id::MessageId;
use crate::value_objects::message_key::MessageKey;
use crate::value_objects::remote_id::RemoteId;

/// Metadata wrapper for all domain events.
///
/// # Semantics
/// - `sequence_number`: per-aggregate, starts at 1 for the first event.
/// - `global_position`: monotonically increasing across all events in the store.
///   Assigned by the EventStore on append. 0 means not yet persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: Uuid,
    pub aggregate_id: String,
    pub aggregate_type: AggregateType,
    pub sequence_number: u64,
    pub global_position: u64,
    pub occurred_at: DateTime<Utc>,
    pub payload: DomainEvent,
}

impl EventEnvelope {
    pub fn new(
        aggregate_id: String,
        aggregate_type: AggregateType,
        sequence_number: u64,
        payload: DomainEvent,
    ) -> Self {
        Self {
            event_id: Uuid::now_v7(),
            aggregate_id,
            aggregate_type,
            sequence_number,
            global_position: 0, // assigned by EventStore on persist
            occurred_at: Utc::now(),
            payload,
        }
    }
}

/// Aggregate type discriminator for the event store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregateType {
    Account,
    Mailbox,
    Message,
    Contact,
}

impl std::fmt::Display for AggregateType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Account => write!(f, "account"),
            Self::Mailbox => write!(f, "mailbox"),
            Self::Message => write!(f, "message"),
            Self::Contact => write!(f, "contact"),
        }
    }
}

/// Complete catalog of domain events.
///
/// These are the source of truth in the event-sourced architecture.
/// Every state change is represented as an event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DomainEvent {
    // ── Account Events ──────────────────────────────────────────────
    /// A new email account was configured.
    AccountAdded {
        account_id: AccountId,
        name: String,
        email: EmailAddress,
        provider_type: ProviderType,
    },

    /// An existing account's settings were updated.
    AccountUpdated {
        account_id: AccountId,
        name: Option<String>,
        connection_config: Option<ConnectionConfig>,
    },

    /// An account was removed.
    AccountRemoved { account_id: AccountId },

    /// A sync operation started for an account.
    AccountSyncStarted { account_id: AccountId },

    /// A sync operation completed successfully.
    AccountSyncCompleted {
        account_id: AccountId,
        mailboxes_synced: u32,
        messages_synced: u32,
    },

    /// A sync operation failed.
    AccountSyncFailed {
        account_id: AccountId,
        error: String,
    },

    /// A background token refresh discovered the account's OAuth refresh
    /// token is dead (revoked/invalid). The keychain keeps the old bundle
    /// until a successful re-login replaces it.
    AccountAuthExpired {
        account_id: AccountId,
        error: String,
    },

    /// The server rejected a password account's password.
    AccountCredentialsRejected {
        account_id: AccountId,
        error: String,
    },

    /// The user fixed an account flagged `AccountAuthExpired` or
    /// `AccountCredentialsRejected`: a fresh OAuth login or a new password.
    AccountReauthenticated { account_id: AccountId },

    // ── Mailbox Events ──────────────────────────────────────────────
    /// A new mailbox was discovered during sync.
    MailboxDiscovered {
        mailbox_id: MailboxId,
        account_id: AccountId,
        path: MailboxPath,
        mailbox_type: MailboxType,
    },

    /// Mailbox special-use classification changed.
    ///
    /// Emitted when a resync sees a different type than the one recorded at
    /// discovery — a folder's type is derived from the server's LIST response, so
    /// improved detection (or a server enabling SPECIAL-USE) must be able to
    /// correct mailboxes that were already discovered.
    MailboxTypeChanged {
        mailbox_id: MailboxId,
        mailbox_type: MailboxType,
    },

    /// Mailbox message counts were updated.
    MailboxCountsUpdated {
        mailbox_id: MailboxId,
        total_messages: u32,
        unread_messages: u32,
    },

    /// A mailbox was renamed.
    MailboxRenamed {
        mailbox_id: MailboxId,
        old_path: MailboxPath,
        new_path: MailboxPath,
    },

    /// A mailbox was deleted.
    MailboxDeleted {
        mailbox_id: MailboxId,
        account_id: AccountId,
    },

    // ── Message Events ──────────────────────────────────────────────
    /// A new message was received (envelope fetched from provider).
    MessageReceived {
        message_id: MessageId,
        account_id: AccountId,
        mailbox_id: MailboxId,
        remote_id: RemoteId,
        /// RFC Message-ID header (e.g. `<abc@mail.example.com>`).
        message_rfc_id: Option<String>,
        /// RFC In-Reply-To header (Message-ID of the direct parent).
        in_reply_to: Option<String>,
        /// RFC References header (ordered ancestor Message-IDs, oldest first).
        references: Vec<String>,
        envelope: Envelope,
        flags: Vec<Flag>,
        size_bytes: u64,
        has_attachments: bool,
        received_at: DateTime<Utc>,
        /// Brought back by a sync repairing local state (a row dropped while
        /// the server kept the message), not newly arrived: nothing to notify.
        restored: bool,
        /// The provider's id for the message across mailboxes, when it has
        /// one: later copies of it join this message instead of making rows.
        #[serde(default)]
        provider_key: Option<MessageKey>,
    },

    /// A message body was fetched and stored (`MessageBodyStore`). The body
    /// itself is not in the event: it is re-fetchable server state, and keeping
    /// it here made the log the largest copy of every mailbox.
    MessageBodyFetched {
        message_id: MessageId,
        attachments: Vec<Attachment>,
    },

    /// Flags on a message were changed. Flags belong to the message, whatever
    /// mailboxes it is in.
    MessageFlagged {
        message_id: MessageId,
        flags_added: Vec<Flag>,
        flags_removed: Vec<Flag>,
    },

    /// The mailboxes a message is in changed: a move is one `removed` plus one
    /// `added`. An `added` entry for a mailbox the message is already in only
    /// replaces its remote id (learned late, or forgotten when the server
    /// renumbered the mailbox). Never removes the last mailbox: that is
    /// `MessageDeleted`.
    MessageMailboxesChanged {
        message_id: MessageId,
        added: Vec<Membership>,
        removed: Vec<MailboxId>,
    },

    /// A message is gone: its last mailbox was removed, or it was expunged.
    /// `mailbox_ids` are the mailboxes it was in, for subscribers following
    /// one mailbox.
    MessageDeleted {
        message_id: MessageId,
        mailbox_ids: Vec<MailboxId>,
    },

    /// A message was sent via SMTP or provider API.
    MessageSent {
        message_id: MessageId,
        account_id: AccountId,
        /// The account's Sent mailbox, resolved at send time.
        mailbox_id: MailboxId,
        envelope: Envelope,
        /// The Message-ID we sent with, so the server copy that later syncs
        /// into Sent replaces this one instead of showing up beside it.
        message_rfc_id: Option<String>,
        in_reply_to: Option<String>,
        references: Vec<String>,
        body_text: Option<String>,
        body_html: Option<String>,
        sent_at: DateTime<Utc>,
    },

    /// A draft was saved.
    DraftSaved {
        message_id: MessageId,
        account_id: AccountId,
        envelope: Envelope,
        body_text: Option<String>,
        body_html: Option<String>,
    },

    /// A draft was discarded.
    DraftDiscarded { message_id: MessageId },

    // ── Contact Events ──────────────────────────────────────────────
    /// A new contact was added to the address book.
    ContactAdded {
        contact_id: ContactId,
        display_name: String,
        emails: Vec<ContactEmail>,
        source: ContactSource,
        external_id: Option<String>,
        /// Account this contact is scoped to. `None` = shared across all accounts.
        account_scope: Option<AccountId>,
    },

    /// An existing contact was updated.
    ContactUpdated {
        contact_id: ContactId,
        display_name: Option<String>,
        emails: Option<Vec<ContactEmail>>,
        phones: Option<Vec<String>>,
        /// `Some(Some(org))` = set, `Some(None)` = clear, `None` = no change
        organization: Option<Option<String>>,
        /// `Some(Some(note))` = set, `Some(None)` = clear, `None` = no change
        notes: Option<Option<String>>,
    },

    /// A contact was removed from the address book.
    ContactRemoved { contact_id: ContactId },

    /// Two contacts were merged: `discarded_id` was absorbed into `kept_id`.
    /// After this event `discarded_id` no longer exists.
    ContactMerged {
        kept_id: ContactId,
        discarded_id: ContactId,
        /// Union of emails from both contacts, stored on the surviving contact.
        merged_emails: Vec<ContactEmail>,
    },
}

impl DomainEvent {
    /// Returns the aggregate type this event belongs to.
    pub fn aggregate_type(&self) -> AggregateType {
        match self {
            Self::AccountAdded { .. }
            | Self::AccountUpdated { .. }
            | Self::AccountRemoved { .. }
            | Self::AccountSyncStarted { .. }
            | Self::AccountSyncCompleted { .. }
            | Self::AccountSyncFailed { .. }
            | Self::AccountAuthExpired { .. }
            | Self::AccountCredentialsRejected { .. }
            | Self::AccountReauthenticated { .. } => AggregateType::Account,

            Self::MailboxDiscovered { .. }
            | Self::MailboxTypeChanged { .. }
            | Self::MailboxCountsUpdated { .. }
            | Self::MailboxRenamed { .. }
            | Self::MailboxDeleted { .. } => AggregateType::Mailbox,

            Self::MessageReceived { .. }
            | Self::MessageBodyFetched { .. }
            | Self::MessageFlagged { .. }
            | Self::MessageMailboxesChanged { .. }
            | Self::MessageDeleted { .. }
            | Self::MessageSent { .. }
            | Self::DraftSaved { .. }
            | Self::DraftDiscarded { .. } => AggregateType::Message,

            Self::ContactAdded { .. }
            | Self::ContactUpdated { .. }
            | Self::ContactRemoved { .. }
            | Self::ContactMerged { .. } => AggregateType::Contact,
        }
    }

    /// Returns a human-readable event name.
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::AccountAdded { .. } => "AccountAdded",
            Self::AccountUpdated { .. } => "AccountUpdated",
            Self::AccountRemoved { .. } => "AccountRemoved",
            Self::AccountSyncStarted { .. } => "AccountSyncStarted",
            Self::AccountSyncCompleted { .. } => "AccountSyncCompleted",
            Self::AccountSyncFailed { .. } => "AccountSyncFailed",
            Self::AccountAuthExpired { .. } => "AccountAuthExpired",
            Self::AccountCredentialsRejected { .. } => "AccountCredentialsRejected",
            Self::AccountReauthenticated { .. } => "AccountReauthenticated",
            Self::MailboxDiscovered { .. } => "MailboxDiscovered",
            Self::MailboxTypeChanged { .. } => "MailboxTypeChanged",
            Self::MailboxCountsUpdated { .. } => "MailboxCountsUpdated",
            Self::MailboxRenamed { .. } => "MailboxRenamed",
            Self::MailboxDeleted { .. } => "MailboxDeleted",
            Self::MessageReceived { .. } => "MessageReceived",
            Self::MessageBodyFetched { .. } => "MessageBodyFetched",
            Self::MessageFlagged { .. } => "MessageFlagged",
            Self::MessageMailboxesChanged { .. } => "MessageMailboxesChanged",
            Self::MessageDeleted { .. } => "MessageDeleted",
            Self::MessageSent { .. } => "MessageSent",
            Self::DraftSaved { .. } => "DraftSaved",
            Self::DraftDiscarded { .. } => "DraftDiscarded",
            Self::ContactAdded { .. } => "ContactAdded",
            Self::ContactUpdated { .. } => "ContactUpdated",
            Self::ContactRemoved { .. } => "ContactRemoved",
            Self::ContactMerged { .. } => "ContactMerged",
        }
    }
}

#[cfg(test)]
mod tests;
