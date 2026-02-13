use mc_shared::pagination::Pagination;
use serde::{Deserialize, Serialize};

use crate::error::DomainError;
use crate::model::account::AccountId;
use crate::model::mailbox::MailboxId;
use crate::model::message::Message;
use crate::value_objects::message_id::MessageId;

/// Full-text search index for email messages.
///
/// The search index is treated as a **projection** of domain events:
/// - Indexed on `MessageBodyFetched` (subject + from/to are indexed immediately
///   on `MessageReceived` for basic searchability before body fetch).
/// - Removed on `MessageDeleted`.
/// - Fully rebuildable from the event store.
///
/// Implementations should batch commits for performance (e.g., commit every N
/// documents or every M milliseconds).
pub trait SearchIndex: Send + Sync {
    /// Index a message for full-text search.
    /// Idempotent: re-indexing the same message_id updates the existing document.
    async fn index_message(&self, message: &Message) -> Result<(), DomainError>;

    /// Remove a message from the index.
    async fn remove_message(&self, message_id: &MessageId) -> Result<(), DomainError>;

    /// Full-text search across indexed messages.
    async fn search(
        &self,
        query: &str,
        account_id: Option<&AccountId>,
        mailbox_id: Option<&MailboxId>,
        pagination: &Pagination,
    ) -> Result<SearchResults, DomainError>;

    /// Rebuild the entire index from scratch.
    ///
    /// The caller provides an iterator of messages. Returns the number of
    /// documents indexed.
    async fn rebuild(&self, messages: Vec<Message>) -> Result<u64, DomainError>;
}

/// Search results container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResults {
    pub hits: Vec<SearchHit>,
    pub total_count: u64,
}

/// A single search result hit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    pub message_id: MessageId,
    pub score: f32,
    pub snippet: Option<String>,
}
