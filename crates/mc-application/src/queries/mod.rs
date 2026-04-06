//! CQRS queries — read-side data retrieval.
//!
//! Each variant represents a read use case. Queries are dispatched through the
//! [`QueryBus`](crate::bus::QueryBus) and return projected read models.

use mc_domain::{
    AccountId, AccountSummary, ContactSearchHit, ConversationSummary, Mailbox, MailboxId, Message,
    MessageSummary, SearchResults, SuggestScope, Suggestion, ThreadId,
};
use mc_shared::pagination::{PagedResult, Pagination};

use crate::error::ApplicationError;

/// All read-side queries in the application.
#[derive(Debug, Clone)]
pub enum Query {
    // ── Account ────────────────────────────────────────────────────────
    /// Get a single account summary.
    GetAccount { account_id: AccountId },

    /// List all configured accounts.
    ListAccounts,

    // ── Mailbox ────────────────────────────────────────────────────────
    /// Get a single mailbox.
    GetMailbox { mailbox_id: MailboxId },

    /// List mailboxes for an account.
    ListMailboxes { account_id: AccountId },

    // ── Message ────────────────────────────────────────────────────────
    /// Get a single message with full body.
    GetMessage { message_id: MessageId },

    /// List messages in a mailbox (summaries, paginated).
    ListMessages {
        mailbox_id: MailboxId,
        pagination: Pagination,
    },

    /// Get a thread (all messages sharing references).
    GetThread { message_id: MessageId },

    // ── Search ─────────────────────────────────────────────────────────
    /// Full-text search across messages.
    SearchMessages {
        query: String,
        account_id: Option<AccountId>,
        mailbox_id: Option<MailboxId>,
        pagination: Pagination,
    },

    /// Search-as-you-type suggestions.
    SuggestMessages {
        prefix: String,
        scope: Option<SuggestScope>,
        max_per_field: usize,
    },

    // ── Contact ────────────────────────────────────────────────────────
    /// Autocomplete search for contacts by name or email prefix.
    ///
    /// `requesting_account_id` scopes the result: own-account contacts first,
    /// cross-account contacts appended at the end (marked `is_cross_account = true`)
    /// only when own-account results are insufficient.
    SearchContacts {
        query: String,
        limit: usize,
        requesting_account_id: Option<AccountId>,
    },

    /// Find pairs of contacts that share at least one email address.
    /// Returns candidates for manual merge by the user.
    FindContactDuplicates { account_id: Option<AccountId> },

    // ── Conversation ────────────────────────────────────────────────────
    /// List all conversations for an account, newest first.
    ListConversations {
        account_id: AccountId,
        pagination: Pagination,
    },

    /// Get a single conversation by thread ID.
    GetConversation {
        thread_id: ThreadId,
        account_id: AccountId,
    },

    /// Wipe and rebuild the conversation + thread index from the event store.
    RebuildConversationIndex,
}

use mc_domain::MessageId;

/// Result of a successfully dispatched query.
///
/// Each variant corresponds to the expected return type of a [`Query`] variant.
/// Handlers must return the matching variant.
#[derive(Debug, Clone)]
pub enum QueryResult {
    Account(Option<AccountSummary>),
    Accounts(Vec<AccountSummary>),
    Mailbox(Option<Mailbox>),
    Mailboxes(Vec<Mailbox>),
    Message(Box<Option<Message>>),
    Messages(PagedResult<MessageSummary>),
    Thread(Box<Vec<Message>>),
    SearchResults(SearchResults),
    Suggestions(Vec<Suggestion>),
    ContactHits(Vec<ContactSearchHit>),
    ContactDuplicates(Vec<(ContactSearchHit, ContactSearchHit)>),
    Conversations(PagedResult<ConversationSummary>),
    ConversationDetail(Option<ConversationSummary>),
    /// Number of events replayed during rebuild.
    IndexRebuilt(u64),
}

// ── Typed extraction helpers ───────────────────────────────────────────────
//
// These make it ergonomic for callers to extract the expected type
// without pattern matching every time.

impl QueryResult {
    pub fn into_account(self) -> Result<Option<AccountSummary>, ApplicationError> {
        match self {
            Self::Account(v) => Ok(v),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }

    pub fn into_accounts(self) -> Result<Vec<AccountSummary>, ApplicationError> {
        match self {
            Self::Accounts(v) => Ok(v),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }

    pub fn into_mailbox(self) -> Result<Option<Mailbox>, ApplicationError> {
        match self {
            Self::Mailbox(v) => Ok(v),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }

    pub fn into_mailboxes(self) -> Result<Vec<Mailbox>, ApplicationError> {
        match self {
            Self::Mailboxes(v) => Ok(v),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }

    pub fn into_message(self) -> Result<Option<Message>, ApplicationError> {
        match self {
            Self::Message(v) => Ok(*v),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }

    pub fn into_messages(self) -> Result<PagedResult<MessageSummary>, ApplicationError> {
        match self {
            Self::Messages(v) => Ok(v),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }

    pub fn into_thread(self) -> Result<Vec<Message>, ApplicationError> {
        match self {
            Self::Thread(v) => Ok(*v),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }

    pub fn into_search_results(self) -> Result<SearchResults, ApplicationError> {
        match self {
            Self::SearchResults(v) => Ok(v),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }

    pub fn into_suggestions(self) -> Result<Vec<Suggestion>, ApplicationError> {
        match self {
            Self::Suggestions(v) => Ok(v),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }

    pub fn into_contact_hits(self) -> Result<Vec<ContactSearchHit>, ApplicationError> {
        match self {
            Self::ContactHits(v) => Ok(v),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }

    pub fn into_contact_duplicates(
        self,
    ) -> Result<Vec<(ContactSearchHit, ContactSearchHit)>, ApplicationError> {
        match self {
            Self::ContactDuplicates(v) => Ok(v),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }

    pub fn into_conversations(
        self,
    ) -> Result<PagedResult<ConversationSummary>, ApplicationError> {
        match self {
            Self::Conversations(v) => Ok(v),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }

    pub fn into_conversation(self) -> Result<Option<ConversationSummary>, ApplicationError> {
        match self {
            Self::ConversationDetail(v) => Ok(v),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }

    pub fn into_index_rebuilt(self) -> Result<u64, ApplicationError> {
        match self {
            Self::IndexRebuilt(n) => Ok(n),
            _ => Err(ApplicationError::Internal(
                "unexpected query result variant".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn into_accounts_ok() {
        let result = QueryResult::Accounts(vec![]);
        let accounts = result.into_accounts().unwrap();
        assert!(accounts.is_empty());
    }

    #[test]
    fn into_accounts_wrong_variant_errors() {
        let result = QueryResult::Mailboxes(vec![]);
        let err = result.into_accounts().unwrap_err();
        assert!(matches!(err, ApplicationError::Internal(_)));
    }
}
