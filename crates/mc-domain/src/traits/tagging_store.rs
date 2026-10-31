//! Where the label classifier keeps its groups, settings and counts.

use async_trait::async_trait;

use crate::error::DomainError;
use crate::model::account::AccountId;
use crate::model::tagging::{
    AutoApplied, Example, GroupId, LabelSettings, Lesson, ModelStats, Scope, SourceFilter,
    TagSource, TrainingGroup,
};
use crate::value_objects::message_id::MessageId;

#[async_trait]
pub trait TaggingStore: Send + Sync {
    // ── Training groups ──
    async fn list_groups(&self) -> Result<Vec<TrainingGroup>, DomainError>;
    async fn create_group(&self, name: &str) -> Result<TrainingGroup, DomainError>;
    async fn rename_group(&self, id: GroupId, name: &str) -> Result<(), DomainError>;
    /// Its accounts fall back to no group. The default group cannot be deleted.
    async fn delete_group(&self, id: GroupId) -> Result<(), DomainError>;
    /// `None` puts the account in no group. An account never assigned is in
    /// the default group.
    async fn assign_account(
        &self,
        account: AccountId,
        group: Option<GroupId>,
    ) -> Result<(), DomainError>;
    async fn scope_of(&self, account: AccountId) -> Result<Scope, DomainError>;
    async fn scope_accounts(&self, scope: Scope) -> Result<Vec<AccountId>, DomainError>;
    /// Every scope that has at least one account, for startup training.
    async fn scopes(&self) -> Result<Vec<Scope>, DomainError>;

    // ── Per-label settings ──
    async fn settings(&self, scope: Scope) -> Result<Vec<LabelSettings>, DomainError>;
    async fn put_settings(&self, scope: Scope, settings: &LabelSettings)
    -> Result<(), DomainError>;

    // ── Counts ──
    /// Counts for `tokens` of every label of the scope.
    async fn stats(&self, scope: Scope, tokens: &[String]) -> Result<ModelStats, DomainError>;
    async fn teach(&self, scope: Scope, lesson: Lesson<'_>) -> Result<(), DomainError>;
    /// Positive examples per label of the scope.
    async fn positives(&self, scope: Scope) -> Result<Vec<(String, u32)>, DomainError>;
    /// Forget the scope's counts and examples (not its settings).
    async fn reset(&self, scope: Scope) -> Result<(), DomainError>;
    /// Whether the scope was trained from existing labels yet.
    async fn is_trained(&self, scope: Scope) -> Result<bool, DomainError>;
    async fn mark_trained(&self, scope: Scope) -> Result<(), DomainError>;

    async fn example(&self, message: MessageId) -> Result<Option<Example>, DomainError>;
    async fn put_example(&self, example: &Example) -> Result<(), DomainError>;

    // ── Messages as the classifier reads them ──
    async fn sources(&self, ids: &[MessageId]) -> Result<Vec<TagSource>, DomainError>;
    /// Messages of `accounts` past `cursor` (ids are time-ordered), oldest
    /// first, or newest first. Trash, Spam and Drafts are skipped; so are
    /// accounts that do not label with mailboxes.
    async fn source_page(
        &self,
        accounts: &[AccountId],
        filter: SourceFilter,
        cursor: Option<MessageId>,
        newest_first: bool,
        limit: u32,
    ) -> Result<Vec<TagSource>, DomainError>;

    // ── Auto-applied labels ──
    async fn record_auto(&self, applied: &AutoApplied) -> Result<(), DomainError>;
    async fn autos(&self, message: MessageId) -> Result<Vec<AutoApplied>, DomainError>;
    /// Forget the record; `true` when there was one.
    async fn take_auto(&self, message: MessageId, label: &str) -> Result<bool, DomainError>;
}
