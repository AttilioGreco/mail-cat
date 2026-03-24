use async_trait::async_trait;
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
#[async_trait]
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

    /// Suggest completions for search-as-you-type.
    ///
    /// Given a prefix and an optional field scope, returns matching values
    /// grouped by field with document frequency.
    async fn suggest(
        &self,
        prefix: &str,
        scope: Option<SuggestScope>,
        max_per_field: usize,
    ) -> Result<Vec<Suggestion>, DomainError>;

    /// Rebuild the entire index from scratch.
    ///
    /// The caller provides an iterator of messages. Returns the number of
    /// documents indexed.
    async fn rebuild(&self, messages: Vec<Message>) -> Result<u64, DomainError>;
}

/// Which field(s) to scope suggestions to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SuggestScope {
    From,
    To,
    Subject,
    Body,
}

/// Search results container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResults {
    pub hits: Vec<SearchHit>,
    pub total_count: u64,
}

/// A single search result hit with envelope summary for display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    pub message_id: MessageId,
    pub score: f32,
    pub snippet: Option<String>,
    pub subject: String,
    pub from_name: Option<String>,
    pub from_email: String,
    pub date: i64,
}

/// A single suggestion from the search index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Suggestion {
    /// Which field this suggestion came from.
    pub field: String,
    /// The suggested value (e.g. an email address, a subject line).
    pub value: String,
    /// How many documents contain this value.
    pub doc_count: u32,
}
