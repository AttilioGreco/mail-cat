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
/// A projection of the read model that stores no content: `search` returns
/// message ids, and the caller hydrates them from the read model. Rebuildable
/// by replaying the event log (see `SearchProjectionHandler`).
#[async_trait]
pub trait SearchIndex: Send + Sync {
    /// Index a message for full-text search.
    /// Idempotent: re-indexing the same message_id updates the existing document.
    async fn index_message(&self, message: &Message) -> Result<(), DomainError>;

    /// Remove a message from the index.
    async fn remove_message(&self, message_id: &MessageId) -> Result<(), DomainError>;

    /// Remove every message of an account from the index.
    async fn remove_account(&self, account_id: &AccountId) -> Result<(), DomainError>;

    /// Full-text search across indexed messages, best match first.
    async fn search(
        &self,
        query: &str,
        account_id: Option<&AccountId>,
        mailbox_id: Option<&MailboxId>,
        pagination: &Pagination,
    ) -> Result<IndexResults, DomainError>;

    /// Highlighted excerpt of `message`'s body for `query`, as HTML. The index
    /// keeps no bodies, so the caller passes the hydrated message.
    fn snippet(&self, query: &str, message: &Message) -> Option<String>;

    /// Commit any pending writes and reload the reader so they become durable
    /// and visible. Used by the search-index projection to bound the checkpoint
    /// to what's durably committed.
    async fn flush(&self) -> Result<(), DomainError>;
}

/// Blanket impl so an `Arc<impl SearchIndex>` (shared handle) also satisfies the
/// generic `SI: SearchIndex` bound (e.g. in the search-index projection).
#[async_trait]
impl<T: SearchIndex + ?Sized> SearchIndex for std::sync::Arc<T> {
    async fn index_message(&self, message: &Message) -> Result<(), DomainError> {
        (**self).index_message(message).await
    }
    async fn remove_message(&self, message_id: &MessageId) -> Result<(), DomainError> {
        (**self).remove_message(message_id).await
    }
    async fn remove_account(&self, account_id: &AccountId) -> Result<(), DomainError> {
        (**self).remove_account(account_id).await
    }
    async fn search(
        &self,
        query: &str,
        account_id: Option<&AccountId>,
        mailbox_id: Option<&MailboxId>,
        pagination: &Pagination,
    ) -> Result<IndexResults, DomainError> {
        (**self)
            .search(query, account_id, mailbox_id, pagination)
            .await
    }
    fn snippet(&self, query: &str, message: &Message) -> Option<String> {
        (**self).snippet(query, message)
    }
    async fn flush(&self) -> Result<(), DomainError> {
        (**self).flush().await
    }
}

/// What the index returns: ids only, the read model has the content.
#[derive(Debug, Clone, Default)]
pub struct IndexResults {
    pub hits: Vec<IndexHit>,
    pub total_count: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct IndexHit {
    pub message_id: MessageId,
    pub score: f32,
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
    /// Every mailbox the message is in: what a search result is acted on from.
    pub mailbox_ids: Vec<MailboxId>,
    pub score: f32,
    pub snippet: Option<String>,
    pub subject: String,
    pub from_name: Option<String>,
    pub from_email: String,
    pub date: i64,
}

/// A search-as-you-type completion drawn from stored messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Suggestion {
    /// Which field this suggestion came from.
    pub field: String,
    /// The suggested value (e.g. an email address, a subject line).
    pub value: String,
    /// How many documents contain this value.
    pub doc_count: u32,
}
