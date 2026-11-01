//! Where the label classifier keeps its groups, settings and counts.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::error::DomainError;
use crate::model::account::AccountId;
use crate::model::discovery::{DiscoveryPoint, Proposal, ProposalId, ProposalStatus};
use crate::model::embedding::ModelProfile;
use crate::model::tagging::{
    AutoApplied, BackgroundSpeed, Example, GroupId, LabelExamples, LabelSettings, LabelledVector,
    Lesson, ModelStats, Scope, SourceFilter, TagSource, TrainingGroup,
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
    /// The newest messages the scope's model was taught `label` from, and
    /// how many it has per account.
    async fn label_examples(
        &self,
        scope: Scope,
        label: &str,
        limit: u32,
    ) -> Result<LabelExamples, DomainError>;
    /// Positive examples per label of the scope.
    async fn positives(&self, scope: Scope) -> Result<Vec<(String, u32)>, DomainError>;
    /// Forget the scope's counts and examples (not its settings).
    async fn reset(&self, scope: Scope) -> Result<(), DomainError>;
    /// Whether the scope was trained from existing labels yet.
    async fn is_trained(&self, scope: Scope) -> Result<bool, DomainError>;
    async fn mark_trained(&self, scope: Scope) -> Result<(), DomainError>;
    /// Make the next [`Self::is_trained`] false: the labels changed under it.
    async fn mark_untrained(&self, scope: Scope) -> Result<(), DomainError>;

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
    /// The records an accepted proposal made, to undo that acceptance.
    async fn proposal_autos(&self, proposal: ProposalId) -> Result<Vec<AutoApplied>, DomainError>;
    /// Forget the record; `true` when there was one.
    async fn take_auto(&self, message: MessageId, label: &str) -> Result<bool, DomainError>;

    // ── Embeddings ──
    /// Whether the user agreed to download the model and embed their mail.
    async fn embeddings_enabled(&self) -> Result<bool, DomainError>;
    async fn set_embeddings_enabled(&self, enabled: bool) -> Result<(), DomainError>;
    async fn background_speed(&self) -> Result<BackgroundSpeed, DomainError>;
    async fn set_background_speed(&self, speed: BackgroundSpeed) -> Result<(), DomainError>;
    /// Light until the user picks another.
    async fn model_profile(&self) -> Result<ModelProfile, DomainError>;
    async fn set_model_profile(&self, profile: ModelProfile) -> Result<(), DomainError>;
    /// Id of the execution target ("cpu" until the user picks another).
    async fn execution_target(&self) -> Result<String, DomainError>;
    async fn set_execution_target(&self, target: &str) -> Result<(), DomainError>;
    /// Replaces any vector the messages have, whatever model made it.
    async fn put_vectors(
        &self,
        model: &str,
        vectors: &[(MessageId, Vec<f32>)],
    ) -> Result<(), DomainError>;
    /// The vectors `model` made for `ids`; a message with none is absent.
    async fn vectors(
        &self,
        model: &str,
        ids: &[MessageId],
    ) -> Result<HashMap<MessageId, Vec<f32>>, DomainError>;
    /// Vectors of the messages the scope's model was taught from.
    async fn scope_vectors(
        &self,
        scope: Scope,
        model: &str,
    ) -> Result<Vec<LabelledVector>, DomainError>;
    /// Messages of the accounts the model can read, of that kind.
    async fn count_sources(
        &self,
        accounts: &[AccountId],
        filter: SourceFilter,
    ) -> Result<u64, DomainError>;
    /// (messages with a vector of `model`, messages that could have one).
    async fn embedding_counts(&self, model: &str) -> Result<(u64, u64), DomainError>;
    /// Messages with a body and no vector of `model`, newest first, below
    /// `before`. Trash, Spam and Drafts are skipped.
    async fn unembedded(
        &self,
        model: &str,
        before: Option<MessageId>,
        limit: u32,
    ) -> Result<Vec<TagSource>, DomainError>;

    // ── Discovery ──
    /// Vectors of `model` for the readable, unlabelled mail of `accounts`
    /// that can take a label: Inbox mail first, then the newest.
    async fn unlabelled_points(
        &self,
        accounts: &[AccountId],
        model: &str,
        limit: u32,
    ) -> Result<Vec<DiscoveryPoint>, DomainError>;
    async fn proposals(&self, status: ProposalStatus) -> Result<Vec<Proposal>, DomainError>;
    async fn proposal(&self, id: ProposalId) -> Result<Option<Proposal>, DomainError>;
    /// Inserts, or replaces the proposal of that id.
    async fn put_proposal(&self, proposal: &Proposal) -> Result<(), DomainError>;
    async fn delete_proposal(&self, id: ProposalId) -> Result<(), DomainError>;
}
