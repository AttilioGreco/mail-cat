//! Contact search index — fast prefix/full-text search for autocomplete.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::DomainError;
use crate::model::account::AccountId;
use crate::traits::repository::ContactSummary;
use crate::value_objects::contact_id::ContactId;

/// Fast search index for contacts — used for autocomplete in the compose window.
///
/// The index is a projection of ContactAdded / ContactUpdated / ContactRemoved events.
/// Re-indexing the same ContactId is idempotent (upsert semantics).
#[async_trait]
pub trait ContactIndex: Send + Sync {
    /// Index (or re-index) a contact from its read-model summary.
    async fn index_contact(&self, summary: &ContactSummary) -> Result<(), DomainError>;

    /// Remove a contact from the index.
    async fn remove_contact(&self, id: &ContactId) -> Result<(), DomainError>;

    /// Autocomplete search: matches display_name prefix OR any email address prefix.
    ///
    /// When `requesting_account_id` is given, contacts scoped to that account are
    /// returned first. Contacts from other accounts (cross-account) are appended at
    /// the end of the list only if own-account results are insufficient, marked with
    /// `is_cross_account = true`.
    ///
    /// Returns at most `limit` hits.
    async fn search_contacts(
        &self,
        query: &str,
        limit: usize,
        requesting_account_id: Option<&AccountId>,
    ) -> Result<Vec<ContactSearchHit>, DomainError>;

    /// Find pairs of contacts sharing at least one email address — merge candidates.
    ///
    /// When `account_id` is given, only contacts scoped to that account are considered.
    async fn find_duplicate_candidates(
        &self,
        account_id: Option<&AccountId>,
    ) -> Result<Vec<(ContactSearchHit, ContactSearchHit)>, DomainError>;
}

/// A single result from contact autocomplete.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContactSearchHit {
    pub id: ContactId,
    pub display_name: String,
    pub primary_email: String,
    /// All email addresses for this contact (for multi-email display).
    pub all_emails: Vec<String>,
    pub organization: Option<String>,
    /// Account this contact is scoped to. `None` = globally shared.
    pub account_scope: Option<AccountId>,
    /// True when this hit comes from a different account than the requester.
    /// Such hits are shown greyed-out at the bottom of the suggestion list.
    pub is_cross_account: bool,
}
