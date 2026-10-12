//! Contact autocomplete result type.

use serde::{Deserialize, Serialize};

use crate::model::account::AccountId;
use crate::traits::repository::ContactSummary;
use crate::value_objects::contact_id::ContactId;

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

impl ContactSearchHit {
    /// With a requesting account, every other scope — global included — is
    /// cross-account.
    pub fn from_summary(summary: ContactSummary, requesting_account: Option<&AccountId>) -> Self {
        Self {
            is_cross_account: requesting_account.is_some_and(|a| summary.account_scope.as_ref() != Some(a)),
            id: summary.id,
            display_name: summary.display_name,
            primary_email: summary.primary_email.as_str().to_owned(),
            all_emails: summary.emails.iter().map(|e| e.address.as_str().to_owned()).collect(),
            organization: summary.organization,
            account_scope: summary.account_scope,
        }
    }
}
