use async_trait::async_trait;
use mc_shared::pagination::{PagedResult, Pagination};
use serde::{Deserialize, Serialize};

use crate::error::DomainError;
use crate::model::account::{AccountId, AccountStatus, ProviderType};
use crate::model::mailbox::{Mailbox, MailboxId};
use crate::model::message::Message;
use crate::value_objects::email_address::EmailAddress;
use crate::value_objects::flag::Flag;
use crate::value_objects::message_id::MessageId;
use crate::value_objects::uid::ProviderUid;

// ── Write-side Repositories ─────────────────────────────────────────────
//
// Write repositories work with the event store to persist aggregate state.
// They are responsible for loading (rehydrating) aggregates from events and
// appending new events.

use crate::events::DomainEvent;
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

/// Read-side repository for accounts.
#[async_trait]
pub trait AccountReadRepository: Send + Sync {
    async fn get_account(&self, id: &AccountId) -> Result<Option<AccountSummary>, DomainError>;
    async fn list_accounts(&self) -> Result<Vec<AccountSummary>, DomainError>;
}

/// Read model for message list views (without body).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageSummary {
    pub id: MessageId,
    pub account_id: AccountId,
    pub mailbox_id: MailboxId,
    pub provider_uid: Option<ProviderUid>,
    pub from_display: String,
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
