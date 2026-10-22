use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::account::{AccountId, ConnectionConfig, ProviderType};
use crate::model::attachment::Attachment;
use crate::model::contact::{ContactEmail, ContactSource};
use crate::model::mailbox::{MailboxId, MailboxType};
use crate::model::message::Envelope;
use crate::value_objects::contact_id::ContactId;
use crate::value_objects::email_address::EmailAddress;
use crate::value_objects::flag::Flag;
use crate::value_objects::mailbox_path::MailboxPath;
use crate::value_objects::message_id::MessageId;
use crate::value_objects::uid::ProviderUid;

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
    /// token is dead (revoked/invalid). Tokens have already been cleared
    /// from the keychain by the time this is appended.
    AccountAuthExpired {
        account_id: AccountId,
        error: String,
    },

    /// The user completed a fresh OAuth login for an account that was
    /// previously flagged `AccountAuthExpired`.
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
        provider_uid: ProviderUid,
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
        #[serde(default)]
        restored: bool,
    },

    /// A message body was fetched and stored (`MessageBodyStore`). The body
    /// itself is not in the event: it is re-fetchable server state, and keeping
    /// it here made the log the largest copy of every mailbox.
    MessageBodyFetched {
        message_id: MessageId,
        attachments: Vec<Attachment>,
    },

    /// Flags on a message were changed.
    MessageFlagged {
        message_id: MessageId,
        mailbox_id: MailboxId,
        flags_added: Vec<Flag>,
        flags_removed: Vec<Flag>,
    },

    /// A message was moved between mailboxes.
    ///
    /// Provider UIDs are per-mailbox (IMAP), so the old one is meaningless in
    /// `to_mailbox_id`: `provider_uid` is the message's id there, `None` while
    /// unknown. A move with `from == to` only records a UID learned later.
    /// `#[serde(default)]`: events stored before the field read as "unknown".
    MessageMoved {
        message_id: MessageId,
        from_mailbox_id: MailboxId,
        to_mailbox_id: MailboxId,
        #[serde(default)]
        provider_uid: Option<ProviderUid>,
    },

    /// A message was deleted.
    MessageDeleted {
        message_id: MessageId,
        mailbox_id: MailboxId,
        permanently: bool,
    },

    /// A message was sent via SMTP or provider API.
    MessageSent {
        message_id: MessageId,
        account_id: AccountId,
        /// The account's Sent mailbox, resolved at send time.
        /// `#[serde(default)]` so events persisted before this field existed
        /// still deserialize (falling back to a fresh placeholder id, same
        /// as the old behavior).
        #[serde(default)]
        mailbox_id: MailboxId,
        envelope: Envelope,
        /// The Message-ID we sent with, so the server copy that later syncs
        /// into Sent replaces this one instead of showing up beside it.
        #[serde(default)]
        message_rfc_id: Option<String>,
        #[serde(default)]
        in_reply_to: Option<String>,
        #[serde(default)]
        references: Vec<String>,
        #[serde(default)]
        body_text: Option<String>,
        #[serde(default)]
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
            | Self::AccountReauthenticated { .. } => AggregateType::Account,

            Self::MailboxDiscovered { .. }
            | Self::MailboxTypeChanged { .. }
            | Self::MailboxCountsUpdated { .. }
            | Self::MailboxRenamed { .. }
            | Self::MailboxDeleted { .. } => AggregateType::Mailbox,

            Self::MessageReceived { .. }
            | Self::MessageBodyFetched { .. }
            | Self::MessageFlagged { .. }
            | Self::MessageMoved { .. }
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
            Self::AccountReauthenticated { .. } => "AccountReauthenticated",
            Self::MailboxDiscovered { .. } => "MailboxDiscovered",
            Self::MailboxTypeChanged { .. } => "MailboxTypeChanged",
            Self::MailboxCountsUpdated { .. } => "MailboxCountsUpdated",
            Self::MailboxRenamed { .. } => "MailboxRenamed",
            Self::MailboxDeleted { .. } => "MailboxDeleted",
            Self::MessageReceived { .. } => "MessageReceived",
            Self::MessageBodyFetched { .. } => "MessageBodyFetched",
            Self::MessageFlagged { .. } => "MessageFlagged",
            Self::MessageMoved { .. } => "MessageMoved",
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
mod tests {
    use super::*;

    #[test]
    fn event_envelope_creation() {
        let event = DomainEvent::AccountRemoved {
            account_id: AccountId::new(),
        };
        let envelope = EventEnvelope::new("acc-123".into(), AggregateType::Account, 1, event);

        assert_eq!(envelope.aggregate_id, "acc-123");
        assert_eq!(envelope.aggregate_type, AggregateType::Account);
        assert_eq!(envelope.sequence_number, 1);
    }

    #[test]
    fn event_aggregate_type_classification() {
        let account_event = DomainEvent::AccountAdded {
            account_id: AccountId::new(),
            name: "Test".into(),
            email: EmailAddress::new("test@example.com").unwrap(),
            provider_type: ProviderType::Imap,
        };
        assert_eq!(account_event.aggregate_type(), AggregateType::Account);

        let mailbox_event = DomainEvent::MailboxDeleted {
            mailbox_id: MailboxId::new(),
            account_id: AccountId::new(),
        };
        assert_eq!(mailbox_event.aggregate_type(), AggregateType::Mailbox);

        let message_event = DomainEvent::DraftDiscarded {
            message_id: MessageId::new(),
        };
        assert_eq!(message_event.aggregate_type(), AggregateType::Message);
    }

    #[test]
    fn event_name() {
        let event = DomainEvent::MessageFlagged {
            message_id: MessageId::new(),
            mailbox_id: MailboxId::new(),
            flags_added: vec![Flag::Seen],
            flags_removed: vec![],
        };
        assert_eq!(event.event_name(), "MessageFlagged");
    }

    #[test]
    fn event_serialization_roundtrip() {
        let event = DomainEvent::AccountAdded {
            account_id: AccountId::new(),
            name: "Work Email".into(),
            email: EmailAddress::new("work@company.com").unwrap(),
            provider_type: ProviderType::Imap,
        };

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: DomainEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.event_name(), "AccountAdded");
        assert_eq!(deserialized.aggregate_type(), AggregateType::Account);
    }

    #[test]
    fn envelope_serialization_roundtrip() {
        let event = DomainEvent::MailboxCountsUpdated {
            mailbox_id: MailboxId::new(),
            total_messages: 42,
            unread_messages: 5,
        };
        let envelope = EventEnvelope::new("mbx-1".into(), AggregateType::Mailbox, 3, event);

        let json = serde_json::to_string(&envelope).unwrap();
        let deserialized: EventEnvelope = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.aggregate_id, "mbx-1");
        assert_eq!(deserialized.sequence_number, 3);
        assert_eq!(deserialized.payload.event_name(), "MailboxCountsUpdated");
    }

    #[test]
    fn message_received_event_serialization() {
        let event = DomainEvent::MessageReceived {
            message_id: MessageId::new(),
            account_id: AccountId::new(),
            mailbox_id: MailboxId::new(),
            provider_uid: ProviderUid::new(1234),
            message_rfc_id: None,
            in_reply_to: None,
            references: vec![],
            envelope: Envelope {
                from: vec![],
                to: vec![],
                cc: vec![],
                bcc: vec![],
                reply_to: vec![],
                subject: "Test subject".into(),
                date: Utc::now(),
            },
            flags: vec![Flag::Recent],
            size_bytes: 4096,
            has_attachments: false,
            received_at: Utc::now(),
            restored: false,
        };

        let json = serde_json::to_string_pretty(&event).unwrap();
        assert!(json.contains("MessageReceived"));
        assert!(json.contains("Test subject"));
        assert!(json.contains("1234"));

        let deserialized: DomainEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.event_name(), "MessageReceived");
    }

    #[test]
    fn aggregate_type_display() {
        assert_eq!(format!("{}", AggregateType::Account), "account");
        assert_eq!(format!("{}", AggregateType::Mailbox), "mailbox");
        assert_eq!(format!("{}", AggregateType::Message), "message");
    }

    #[test]
    fn account_auth_expired_aggregate_type_and_name() {
        let event = DomainEvent::AccountAuthExpired {
            account_id: AccountId::new(),
            error: "invalid_grant".into(),
        };
        assert_eq!(event.aggregate_type(), AggregateType::Account);
        assert_eq!(event.event_name(), "AccountAuthExpired");
    }

    #[test]
    fn account_reauthenticated_aggregate_type_and_name() {
        let event = DomainEvent::AccountReauthenticated {
            account_id: AccountId::new(),
        };
        assert_eq!(event.aggregate_type(), AggregateType::Account);
        assert_eq!(event.event_name(), "AccountReauthenticated");
    }
}
