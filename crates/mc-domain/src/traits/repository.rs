use async_trait::async_trait;
use mc_shared::pagination::{PagedResult, Pagination};
use serde::{Deserialize, Serialize};

use crate::error::DomainError;
use crate::events::DomainEvent;
use crate::model::account::{AccountId, AccountStatus, ProviderType};
use crate::model::contact::{Contact, ContactEmail, ContactSource};
use crate::model::mailbox::{Mailbox, MailboxId};
use crate::model::message::Message;
use crate::value_objects::contact_id::ContactId;
use crate::value_objects::email_address::EmailAddress;
use crate::value_objects::flag::Flag;
use crate::value_objects::message_id::MessageId;
use crate::value_objects::thread_id::ThreadId;
use crate::value_objects::uid::ProviderUid;

// ── Write-side Repositories ─────────────────────────────────────────────
//
// Write repositories work with the event store to persist aggregate state.
// They are responsible for loading (rehydrating) aggregates from events and
// appending new events.

use crate::model::account::Account;

/// Write-side repository for Account aggregate.
#[async_trait]
pub trait AccountWriteRepository: Send + Sync {
    /// Persist new events for the account. The repository handles event store
    /// interaction (append with expected sequence).
    async fn save(&self, account: &Account, events: Vec<DomainEvent>) -> Result<(), DomainError>;

    /// Load an account by rehydrating from the event store.
    async fn load(&self, id: &AccountId) -> Result<Option<Account>, DomainError>;
}

// ── Read-side Repositories (Projections) ────────────────────────────────
//
// Read repositories query the projected read models (materialized views).
// They are populated by event handlers that process domain events.
// Read models are eventually consistent with the event store.

/// Read model for account list views.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountSummary {
    pub id: AccountId,
    pub name: String,
    pub email: EmailAddress,
    pub provider_type: ProviderType,
    pub status: AccountStatus,
    pub total_unread: u32,
}

/// How much of an account is actually downloaded locally.
///
/// `total_messages` is what the *server* reports across the account's mailboxes;
/// the other two are local facts. So "still to download" is
/// `total_messages - synced_messages` envelopes and `pending_bodies` bodies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSyncStats {
    /// Sum of server-side message counts over all known mailboxes.
    pub total_messages: u32,
    /// Envelopes stored in the local read model.
    pub synced_messages: u32,
    /// Stored envelopes whose body is still queued for download.
    pub pending_bodies: u32,
}

/// Read-side repository for accounts.
#[async_trait]
pub trait AccountReadRepository: Send + Sync {
    async fn get_account(&self, id: &AccountId) -> Result<Option<AccountSummary>, DomainError>;
    async fn list_accounts(&self) -> Result<Vec<AccountSummary>, DomainError>;

    /// Download progress for one account. Walks the local trees, so it is a
    /// scan — call it for a settings view, not per message.
    async fn account_stats(&self, id: &AccountId) -> Result<AccountSyncStats, DomainError>;
}

/// Read model for message list views (without body).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageSummary {
    pub id: MessageId,
    pub account_id: AccountId,
    pub mailbox_id: MailboxId,
    pub provider_uid: Option<ProviderUid>,
    pub from_name: Option<String>,
    pub from_email: String,
    #[serde(default)]
    pub to: Vec<String>,
    #[serde(default)]
    pub cc: Vec<String>,
    pub subject: String,
    pub flags: Vec<Flag>,
    pub size_bytes: u64,
    pub has_attachments: bool,
    pub received_at: chrono::DateTime<chrono::Utc>,
}

/// Read-side repository for mailboxes.
#[async_trait]
pub trait MailboxReadRepository: Send + Sync {
    async fn get_mailbox(&self, id: &MailboxId) -> Result<Option<Mailbox>, DomainError>;
    async fn list_mailboxes(&self, account_id: &AccountId) -> Result<Vec<Mailbox>, DomainError>;
}

/// Read-side repository for messages.
#[async_trait]
pub trait MessageReadRepository: Send + Sync {
    async fn get_message(&self, id: &MessageId) -> Result<Option<Message>, DomainError>;

    async fn list_messages(
        &self,
        mailbox_id: &MailboxId,
        pagination: &Pagination,
    ) -> Result<PagedResult<MessageSummary>, DomainError>;

    async fn get_thread(&self, message_id: &MessageId) -> Result<Vec<Message>, DomainError>;

    /// Find a message summary by its provider UID within a specific mailbox.
    async fn find_by_provider_uid(
        &self,
        mailbox_id: &MailboxId,
        provider_uid: u32,
    ) -> Result<Option<MessageSummary>, DomainError>;

    /// All unread (no `Seen` flag) message summaries in a mailbox.
    async fn find_unread_by_mailbox(
        &self,
        mailbox_id: &MailboxId,
    ) -> Result<Vec<MessageSummary>, DomainError>;

    /// Scan every stored message, optionally filtered by account.
    ///
    /// Used for contact backfill: extracts `from`/`to`/`cc` addresses from the
    /// stored envelopes and feeds them into `EnsureContact`.  Returns full
    /// `Message` objects so the caller can access envelope addresses.
    async fn scan_all_messages(
        &self,
        account_id: Option<&AccountId>,
    ) -> Result<Vec<Message>, DomainError>;
}

/// Checkpoint store for projections — tracks last processed global_position.
#[async_trait]
pub trait ProjectionCheckpoint: Send + Sync {
    async fn get_checkpoint(&self, projection_name: &str) -> Result<u64, DomainError>;
    async fn save_checkpoint(
        &self,
        projection_name: &str,
        global_position: u64,
    ) -> Result<(), DomainError>;
}

/// A message whose body still needs downloading, drained by the background
/// backfill worker.
#[derive(Debug, Clone)]
pub struct BackfillEntry {
    pub message_id: MessageId,
    pub mailbox_id: MailboxId,
    pub provider_uid: u32,
}

/// Queue of message bodies still to be downloaded, drained newest-first.
///
/// Populated by the `MessageReceived` projection and drained by
/// `MessageBodyFetched` — so it is event-sourced: it rebuilds correctly on
/// replay and holds exactly "received but body not yet fetched".
#[async_trait]
pub trait BodyBackfillQueue: Send + Sync {
    /// Pending entries for an account, newest received first, up to `limit`.
    async fn list_pending_backfill(
        &self,
        account_id: &AccountId,
        limit: usize,
    ) -> Result<Vec<BackfillEntry>, DomainError>;

    /// How many bodies are still queued for this account — the "N messages
    /// left to download" number, unbounded by any page size.
    async fn count_pending_backfill(&self, account_id: &AccountId) -> Result<u32, DomainError>;
}

// ── Contact Repositories ─────────────────────────────────────────────────────

/// Write-side repository for Contact aggregate.
#[async_trait]
pub trait ContactWriteRepository: Send + Sync {
    /// Persist new events for the contact.
    async fn save(&self, contact: &Contact, events: Vec<DomainEvent>) -> Result<(), DomainError>;

    /// Load a contact by rehydrating from the event store.
    async fn load(&self, id: &ContactId) -> Result<Option<Contact>, DomainError>;
}

/// Lightweight read model for contact list views.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContactSummary {
    pub id: ContactId,
    pub display_name: String,
    /// Primary email address (or first available).
    pub primary_email: EmailAddress,
    pub emails: Vec<ContactEmail>,
    pub organization: Option<String>,
    pub source: ContactSource,
    /// Account this contact is scoped to. `None` = shared across all accounts.
    pub account_scope: Option<AccountId>,
}

/// Read-side repository for contacts.
#[async_trait]
pub trait ContactReadRepository: Send + Sync {
    async fn get_contact(&self, id: &ContactId) -> Result<Option<ContactSummary>, DomainError>;

    async fn list_contacts(
        &self,
        pagination: &Pagination,
    ) -> Result<PagedResult<ContactSummary>, DomainError>;

    /// Find contacts that have the given email address (any label).
    async fn find_by_email(
        &self,
        email: &EmailAddress,
    ) -> Result<Vec<ContactSummary>, DomainError>;

    /// Find contacts scoped to a specific account that have the given email address.
    async fn find_by_email_in_account(
        &self,
        email: &EmailAddress,
        account_id: &AccountId,
    ) -> Result<Vec<ContactSummary>, DomainError>;

    /// Search contacts by display name or email prefix.
    async fn search_contacts(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<ContactSummary>, DomainError>;

    /// Find pairs of contacts that share at least one email address.
    /// Used to surface merge candidates to the user.
    async fn find_duplicate_candidates(
        &self,
        account_id: Option<&AccountId>,
    ) -> Result<Vec<(ContactSummary, ContactSummary)>, DomainError>;
}

// ── Conversation Read Repository ─────────────────────────────────────────────

/// Read model for conversation (thread) list views.
///
/// Projected from `MessageReceived` and flag-change events.
/// Never expose write methods — projection writes are internal to the infra layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationSummary {
    pub thread_id: ThreadId,
    pub account_id: AccountId,
    /// Subject with Re:/Fwd: stripped.
    pub subject: String,
    pub message_count: u32,
    pub unread_count: u32,
    pub last_message_at: chrono::DateTime<chrono::Utc>,
    /// Display name (or address) of the most recent sender.
    pub latest_sender: String,
    /// A representative domain message_id (UUID) usable to call get_thread.
    #[serde(default)]
    pub sample_message_id: MessageId,
}

/// Read-side repository for conversation (thread) summaries.
#[async_trait]
pub trait ConversationReadRepository: Send + Sync {
    /// Get a single conversation by thread ID.
    async fn get_conversation(
        &self,
        thread_id: &ThreadId,
        account_id: &AccountId,
    ) -> Result<Option<ConversationSummary>, DomainError>;

    /// List conversations for an account, newest first.
    ///
    /// When `mailbox_id` is set, conversations (and their stats: message count,
    /// unread count, latest sender/date) are scoped to that mailbox only —
    /// matching how Thunderbird threads messages within the selected folder,
    /// rather than merging a thread's messages across every mailbox.
    async fn list_conversations(
        &self,
        account_id: &AccountId,
        mailbox_id: Option<&MailboxId>,
        pagination: &Pagination,
    ) -> Result<PagedResult<ConversationSummary>, DomainError>;

    /// Wipe and rebuild the conversation + thread index by replaying all stored events.
    /// Returns the number of events replayed.
    async fn rebuild_index(&self) -> Result<u64, DomainError>;
}
