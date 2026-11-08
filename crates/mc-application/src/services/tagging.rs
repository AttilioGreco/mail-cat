//! Label suggestion and auto-apply. A naive Bayes model per training group
//! learns which labels the user puts on which mail (see `bayes`); a label
//! can be left alone, suggested, or applied by itself when the model is sure.
//!
//! Learning follows the labels: those on existing mail at first run, then
//! every change that reaches the read model, whether the user made it here or
//! elsewhere. Feedback (rejects, undone auto-applies) is a stronger negative.

mod bayes;
mod discovery;
mod eval;
mod features;
mod knn;
mod naming;
mod placement;
mod retro;
mod stats;
mod training;
mod vectors;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use mc_domain::{
    AccountId, AutoApplied, BackgroundSpeed, Confidence, DomainEvent, Embedder, EventEnvelope,
    Flag, GroupId, LabelMode, LabelNamer, LabelSettings, Mailbox, MailboxId, MailboxReadRepository,
    MailboxType, MessageId, ModelProfile, Scope, TagSource, TaggingStore, TrainingGroup,
};

use crate::bus::{CommandBus, EventSubscriber};
use crate::commands::Command;
use crate::error::ApplicationError;

pub use discovery::{
    AccountChange, AccountCount, ChangeKind, DiscoveryPhase, DiscoveryProgress, LabelChange,
    LabelDetail, LabelEntry, LabelHome, LabelPresence, LabelScope, MailboxStep, MatchAccount,
    MatchReport, OpResult, OpStatus, PlanOp, PlanReport, PlanSummary, ProposalMessage,
    ProposalView, StepKind, UndoReport,
};
pub use eval::{EvalRow, LabelEval};
pub use naming::KeywordNamer;
use placement::{Placement, display_name};
pub use retro::{LabelApply, Preview, PreviewItem};
pub use stats::{ApplyKind, ApplyProgress, BackgroundStats, EmbedderState, RetrainProgress};

/// Live auto-apply only touches recent mail: an initial sync delivers years
/// of "new" messages, and old mail is for the previewed bulk apply.
const AUTO_MAX_AGE: chrono::Duration = chrono::Duration::days(14);
/// Pause between provider calls made on the classifier's behalf.
const PACE: Duration = Duration::from_millis(500);

/// One label's settings with what the model has to go on.
#[derive(Debug, Clone)]
pub struct LabelStatus {
    pub settings: LabelSettings,
    pub positives: u32,
}

#[derive(Debug, Clone)]
pub struct Suggestion {
    pub label: String,
    /// `None` where the label is a keyword, or the account has no such mailbox yet.
    pub mailbox_id: Option<MailboxId>,
    pub name: String,
    pub confidence: Confidence,
}

#[derive(Debug, Clone)]
pub struct AutoLabel {
    pub label: String,
    /// `None` where the label is a keyword, or the account has no such mailbox yet.
    pub mailbox_id: Option<MailboxId>,
    pub name: String,
    pub archived: bool,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct MessageTags {
    pub suggestions: Vec<Suggestion>,
    pub auto: Vec<AutoLabel>,
}

/// Per scope, when its labelled vectors were read and the vectors.
type Pools = HashMap<Scope, (std::time::Instant, Arc<knn::Pool>)>;

/// A label to put on a message by itself.
struct AutoJob {
    message_id: MessageId,
    account_id: AccountId,
    label: String,
    score: f64,
    place: Placement,
}

pub struct TaggingService<S, R, C> {
    store: S,
    read: R,
    commands: Arc<C>,
    /// Messages read and scored per query when scanning existing mail.
    page: u32,
    jobs: mpsc::UnboundedSender<AutoJob>,
    inbox: Mutex<Option<mpsc::UnboundedReceiver<AutoJob>>>,
    /// `None` where no model is wired in: the classifier is words only.
    embedder: Option<Arc<dyn Embedder>>,
    /// The user's consent, mirrored from the store so scoring need not ask.
    embedding_on: AtomicBool,
    /// Rouses the embedding worker: a body arrived, or consent was given.
    wake: Notify,
    namer: Arc<dyn LabelNamer>,
    /// A search for new labels is running; one at a time.
    discovering: AtomicBool,
    /// Rouses the discovery worker: the embedding backlog was just cleared.
    discovery_wake: Notify,
    speed: Mutex<BackgroundSpeed>,
    /// Held while a setting goes to the store and into its mirror, so the
    /// load at startup cannot overwrite a change made meanwhile.
    settings: tokio::sync::Mutex<()>,
    /// The model profile and execution target the user picked.
    model_choice: Mutex<(ModelProfile, String)>,
    /// Scopes being retrained right now: the embedding backlog gives way.
    retraining: AtomicUsize,
    activity: Mutex<stats::Activity>,
    /// Labelling jobs take turns, so a plan accepting dozens of proposals does
    /// not hammer the providers with all of them at once.
    apply_turn: tokio::sync::Mutex<()>,
    pools: Mutex<Pools>,
}

/// Runs `work` polled on a thread of its own at background priority, for the
/// CPU-heavy retraining; its I/O still goes through the runtime. The thread is
/// not reused, as the priority cannot be raised back.
async fn niced<T: Send + 'static>(
    work: impl Future<Output = Result<T, ApplicationError>> + Send + 'static,
) -> Result<T, ApplicationError> {
    let runtime = tokio::runtime::Handle::current();
    let (done, result) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        if let Err(e) = mc_shared::priority::lower_thread_priority() {
            tracing::debug!(error = %e, "label classifier: could not lower the thread priority");
        }
        let _ = done.send(runtime.block_on(work));
    });
    result
        .await
        .map_err(|_| ApplicationError::Internal("background thread ended unexpectedly".into()))?
}

pub(crate) fn label_key(name: &str) -> String {
    name.trim().to_lowercase()
}

/// A label name as a `/`-separated path: segments trimmed, none empty.
pub(crate) fn label_path(name: &str) -> Result<String, ApplicationError> {
    let segments: Vec<&str> = name.split('/').map(str::trim).collect();
    if segments.iter().any(|s| s.is_empty()) {
        return Err(ApplicationError::PreconditionFailed(format!(
            "{:?} is not a label name: empty name or empty path segment",
            name.trim()
        )));
    }
    Ok(segments.join("/"))
}

/// The account's own label of that name (case-insensitive).
pub(crate) fn label_mailbox<'a>(mailboxes: &'a [Mailbox], label: &str) -> Option<&'a Mailbox> {
    mailboxes
        .iter()
        .find(|m| m.mailbox_type == MailboxType::Custom && label_key(&m.display_path) == label)
}

pub(crate) fn inbox_of(mailboxes: &[Mailbox]) -> Option<&Mailbox> {
    mailboxes
        .iter()
        .find(|m| m.mailbox_type == MailboxType::Inbox)
}

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    pub fn new(store: S, read: R, commands: Arc<C>) -> Self {
        let (jobs, inbox) = mpsc::unbounded_channel();
        Self {
            store,
            read,
            commands,
            page: retro::PAGE,
            jobs,
            inbox: Mutex::new(Some(inbox)),
            embedder: None,
            embedding_on: AtomicBool::new(false),
            wake: Notify::new(),
            namer: Arc::new(KeywordNamer),
            discovering: AtomicBool::new(false),
            discovery_wake: Notify::new(),
            speed: Mutex::new(BackgroundSpeed::default()),
            settings: tokio::sync::Mutex::new(()),
            model_choice: Mutex::new((ModelProfile::default(), "cpu".into())),
            retraining: AtomicUsize::new(0),
            activity: Mutex::default(),
            apply_turn: tokio::sync::Mutex::new(()),
            pools: Mutex::new(HashMap::new()),
        }
    }

    /// Add the embedding signal; it stays dormant until the user consents.
    #[must_use]
    pub fn with_embedder(mut self, embedder: Arc<dyn Embedder>) -> Self {
        self.embedder = Some(embedder);
        self
    }

    /// Name discovered labels with `namer` instead of by keywords.
    #[must_use]
    pub fn with_namer(mut self, namer: Arc<dyn LabelNamer>) -> Self {
        self.namer = namer;
        self
    }

    /// Scan existing mail `page` messages at a time.
    #[must_use]
    pub fn with_page_size(mut self, page: u32) -> Self {
        self.page = page;
        self
    }

    /// Follow the event bus, apply labels as they are decided, and train
    /// from existing labels first if that has not been done. Call once.
    pub fn spawn(self: &Arc<Self>, mut events: EventSubscriber, shutdown: CancellationToken) {
        let Some(mut jobs) = self
            .inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        else {
            return;
        };
        let service = Arc::clone(self);
        let stop = shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = service.load_consent().await {
                warn!(error = %e, "label classifier: could not read the embeddings setting");
            }
            let worker = Arc::clone(&service);
            tokio::spawn({
                let stop = stop.clone();
                async move { worker.run_embedder(stop).await }
            });
            let finder = Arc::clone(&service);
            tokio::spawn({
                let stop = stop.clone();
                async move { finder.run_discovery(stop).await }
            });
            let boot = Arc::clone(&service);
            if let Err(e) = niced(async move { boot.bootstrap().await }).await {
                warn!(error = %e, "label classifier: initial training failed");
            }
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    event = events.recv() => match event {
                        Some(e) => Self::on_event(&service, &e).await,
                        None => break,
                    },
                }
            }
        });
        let service = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    job = jobs.recv() => match job {
                        Some(job) => service.apply_auto(job).await,
                        None => break,
                    },
                }
                tokio::time::sleep(PACE).await;
            }
        });
    }

    async fn on_event(self: &Arc<Self>, envelope: &EventEnvelope) {
        let (message_id, auto) = match &envelope.payload {
            DomainEvent::MessageReceived {
                message_id,
                restored: false,
                ..
            }
            | DomainEvent::MessageBodyFetched { message_id, .. } => (*message_id, true),
            DomainEvent::MessageMailboxesChanged { message_id, .. } => (*message_id, false),
            // The labels of a labels account are its Custom mailboxes: a
            // mailbox gaining or losing that type changes what was taught.
            // A renamed label keeps its mail but its examples still carry the
            // old key until the scope learns again.
            // ponytail: one relearn per renamed descendant; coalesce if trees get big.
            DomainEvent::MailboxTypeChanged { mailbox_id, .. }
            | DomainEvent::MailboxRenamed { mailbox_id, .. } => {
                let (this, mailbox_id) = (Arc::clone(self), *mailbox_id);
                if let Err(e) = niced(async move { this.relearn_mailbox(&mailbox_id).await }).await
                {
                    warn!(%mailbox_id, error = %e, "label classifier: retraining after a mailbox change failed");
                }
                return;
            }
            // The row is gone by now, so the event names the account.
            DomainEvent::MailboxDeleted {
                mailbox_id,
                account_id,
            } => {
                let (this, account_id) = (Arc::clone(self), *account_id);
                if let Err(e) = niced(async move { this.relearn_scope_of(account_id).await }).await
                {
                    warn!(%mailbox_id, error = %e, "label classifier: retraining after a mailbox deletion failed");
                }
                return;
            }
            // A keyword is how a folders account labels.
            DomainEvent::MessageFlagged {
                message_id,
                flags_added,
                flags_removed,
            } if flags_added
                .iter()
                .chain(flags_removed)
                .any(|f| matches!(f, Flag::Custom(_))) =>
            {
                (*message_id, false)
            }
            _ => return,
        };
        if auto {
            self.wake.notify_one();
        }
        if let Err(e) = self.consider(message_id, auto).await {
            warn!(%message_id, error = %e, "label classifier: message skipped");
        }
    }

    async fn relearn_mailbox(&self, mailbox: &MailboxId) -> Result<(), ApplicationError> {
        let Some(mailbox) = self.read.get_mailbox(mailbox).await? else {
            return Ok(());
        };
        self.relearn_scope_of(mailbox.account_id).await
    }

    async fn relearn_scope_of(&self, account: AccountId) -> Result<(), ApplicationError> {
        let scope = self.store.scope_of(account).await?;
        self.store.mark_untrained(scope).await?;
        self.bootstrap().await
    }

    /// Learn from the message as it is now, then decide on labelling it.
    async fn consider(&self, id: MessageId, auto: bool) -> Result<(), ApplicationError> {
        let Some(source) = self.store.sources(&[id]).await?.pop() else {
            return Ok(());
        };
        let scope = self.store.scope_of(source.account_id).await?;
        if !source.labels.is_empty() || self.store.example(id).await?.is_some() {
            self.retrain(&source, scope, None, false).await?;
        }
        if auto {
            self.queue_auto(&source, scope).await?;
        }
        Ok(())
    }

    async fn queue_auto(&self, source: &TagSource, scope: Scope) -> Result<(), ApplicationError> {
        // A scope being rebuilt has no counts to judge by.
        if Utc::now() - source.received_at > AUTO_MAX_AGE
            || !source.can_label()
            || !self.store.is_trained(scope).await?
        {
            return Ok(());
        }
        let applying: Vec<LabelSettings> = self
            .store
            .settings(scope)
            .await?
            .into_iter()
            .filter(|s| s.mode.applies() && !source.labels.contains(&s.label))
            .collect();
        if applying.is_empty() {
            return Ok(());
        }
        let rejected = self
            .store
            .example(source.id)
            .await?
            .map(|e| e.negatives)
            .unwrap_or_default();
        let done: HashSet<String> = self
            .store
            .autos(source.id)
            .await?
            .into_iter()
            .map(|a| a.label)
            .collect();
        let scores = self
            .score(source, scope, applying.iter().map(|s| s.label.as_str()))
            .await?;
        let group = self.group_mailboxes(scope).await?;
        for setting in &applying {
            let Some(score) = scores.iter().find(|s| s.label == setting.label) else {
                continue;
            };
            if score.score < setting.threshold
                || rejected.contains(&setting.label)
                || done.contains(&setting.label)
            {
                continue;
            }
            let _ = self.jobs.send(AutoJob {
                message_id: source.id,
                account_id: source.account_id,
                label: setting.label.clone(),
                score: score.score,
                place: Placement::of(source, &group, &setting.label, setting.mode),
            });
        }
        Ok(())
    }

    async fn score(
        &self,
        source: &TagSource,
        scope: Scope,
        labels: impl IntoIterator<Item = &str>,
    ) -> Result<Vec<mc_domain::Score>, ApplicationError> {
        let tokens = features::tokens(source, true);
        let stats = self.store.stats(scope, &tokens).await?;
        let nearest = self.nearest(scope, &[source.id]).await?;
        Ok(bayes::scores(
            &stats,
            labels,
            &tokens,
            nearest.get(&source.id),
        ))
    }

    /// The record goes in before the change so that the event the change
    /// raises already finds the label marked as ours.
    async fn apply_auto(&self, job: AutoJob) {
        match self.still_wanted(&job).await {
            Ok(true) => {}
            Ok(false) => return,
            Err(e) => {
                warn!(message_id = %job.message_id, error = %e, "label classifier: not applying, could not check");
                return;
            }
        }
        let record = AutoApplied {
            message_id: job.message_id,
            label: job.label,
            score: job.score,
            archived: job.place.archives(),
            confirmed: false,
            proposal_id: None,
            at: Utc::now(),
        };
        if let Err(e) = self.store.record_auto(&record).await {
            warn!(error = %e, "label classifier: not applying, could not record");
            return;
        }
        match self
            .put(job.account_id, vec![job.message_id], &job.place)
            .await
        {
            Ok(_) => {
                info!(message_id = %job.message_id, label = %record.label, "label applied automatically")
            }
            Err(e) => {
                warn!(message_id = %job.message_id, error = %e, "label classifier: apply failed");
                let _ = self.store.take_auto(job.message_id, &record.label).await;
            }
        }
    }

    /// Jobs wait in a queue paced by `PACE`; the message or the settings may
    /// have changed by the time one runs.
    async fn still_wanted(&self, job: &AutoJob) -> Result<bool, ApplicationError> {
        let Some(source) = self.store.sources(&[job.message_id]).await?.pop() else {
            return Ok(false);
        };
        if !source.can_label()
            || source.labels.contains(&job.label)
            || (job.place.archives() && !source.in_inbox)
        {
            return Ok(false);
        }
        let scope = self.store.scope_of(source.account_id).await?;
        Ok(self
            .store
            .settings(scope)
            .await?
            .iter()
            .any(|s| s.label == job.label && s.mode.applies()))
    }

    // ── Groups ──

    pub async fn groups(&self) -> Result<Vec<TrainingGroup>, ApplicationError> {
        Ok(self.store.list_groups().await?)
    }

    pub async fn create_group(&self, name: &str) -> Result<TrainingGroup, ApplicationError> {
        let name = non_empty(name)?;
        Ok(self.store.create_group(name).await?)
    }

    pub async fn rename_group(&self, id: GroupId, name: &str) -> Result<(), ApplicationError> {
        Ok(self.store.rename_group(id, non_empty(name)?).await?)
    }

    pub async fn delete_group(&self, id: GroupId) -> Result<(), ApplicationError> {
        let members = self
            .store
            .list_groups()
            .await?
            .into_iter()
            .find(|g| g.id == id)
            .map(|g| g.accounts)
            .unwrap_or_default();
        self.store.delete_group(id).await?;
        self.forget_pools();
        // The members are in the default group now, which learns from them.
        if !members.is_empty() {
            self.train_scope(Scope::Group(GroupId::DEFAULT)).await?;
        }
        Ok(())
    }

    /// Train again every scope that lost its counts (an account was removed).
    pub async fn retrain_pending(&self) -> Result<(), ApplicationError> {
        self.bootstrap().await
    }

    /// Move an account between groups (`None`: learn alone). Both ends
    /// are retrained, as a model is only what its members taught it.
    pub async fn assign_account(
        &self,
        account: AccountId,
        group: Option<GroupId>,
    ) -> Result<(), ApplicationError> {
        let before = self.store.scope_of(account).await?;
        self.store.assign_account(account, group).await?;
        let after = self.store.scope_of(account).await?;
        if before == after {
            return Ok(());
        }
        for scope in [before, after] {
            self.train_scope(scope).await?;
        }
        Ok(())
    }

    // ── Per-label settings ──

    pub async fn label_status(
        &self,
        account: AccountId,
        label: &str,
    ) -> Result<LabelStatus, ApplicationError> {
        let (scope, label) = (self.store.scope_of(account).await?, label_key(label));
        let positives = self
            .store
            .positives(scope)
            .await?
            .into_iter()
            .find_map(|(l, n)| (l == label).then_some(n))
            .unwrap_or_default();
        let settings = self
            .store
            .settings(scope)
            .await?
            .into_iter()
            .find(|s| s.label == label)
            .unwrap_or_else(|| LabelSettings::off(&label));
        Ok(LabelStatus {
            settings,
            positives,
        })
    }

    pub async fn set_label(
        &self,
        account: AccountId,
        label: &str,
        mode: LabelMode,
        threshold: f64,
    ) -> Result<LabelStatus, ApplicationError> {
        if !(0.5..=1.0).contains(&threshold) {
            return Err(ApplicationError::PreconditionFailed(
                "threshold must be between 0.5 and 1".into(),
            ));
        }
        let status = self.label_status(account, label).await?;
        if mode != LabelMode::Off && status.positives < LabelSettings::MIN_POSITIVES {
            return Err(ApplicationError::PreconditionFailed(format!(
                "servono altri esempi: {} di {}",
                status.positives,
                LabelSettings::MIN_POSITIVES
            )));
        }
        let settings = LabelSettings {
            label: status.settings.label,
            mode,
            threshold,
            color: status.settings.color,
        };
        let scope = self.store.scope_of(account).await?;
        self.store.put_settings(scope, &settings).await?;
        Ok(LabelStatus { settings, ..status })
    }

    /// Coloring needs no examples: it never changes what the classifier does.
    pub async fn set_label_color(
        &self,
        account: AccountId,
        label: &str,
        color: Option<&str>,
    ) -> Result<LabelStatus, ApplicationError> {
        if color.is_some_and(|c| !LabelSettings::is_color(c)) {
            return Err(mc_domain::DomainError::Validation(format!(
                "unknown label color {color:?}"
            ))
            .into());
        }
        let scope = self.store.scope_of(account).await?;
        self.store
            .set_color(scope, &label_key(label), color)
            .await?;
        self.label_status(account, label).await
    }

    /// The colors of the account's training group, by lowercased label.
    pub async fn label_colors(
        &self,
        account: AccountId,
    ) -> Result<HashMap<String, String>, ApplicationError> {
        let scope = self.store.scope_of(account).await?;
        Ok(self
            .store
            .settings(scope)
            .await?
            .into_iter()
            .filter_map(|s| Some((s.label, s.color?)))
            .collect())
    }

    // ── On a message ──

    /// What the classifier offers for a message and what it did to it.
    pub async fn tags_for(&self, id: MessageId) -> Result<MessageTags, ApplicationError> {
        let Some(source) = self.store.sources(&[id]).await?.pop() else {
            return Ok(MessageTags::default());
        };
        let scope = self.store.scope_of(source.account_id).await?;
        let group = self.group_mailboxes(scope).await?;
        let own = group.get(&source.account_id).map_or(&[][..], Vec::as_slice);
        let name_of = |label: &str| match label_mailbox(own, label) {
            Some(m) => (Some(m.id), m.display_name.clone()),
            None => (None, display_name(&group, label)),
        };

        let auto = self
            .store
            .autos(id)
            .await?
            .into_iter()
            .filter(|a| source.labels.contains(&a.label))
            .map(|a| {
                let (mailbox_id, name) = name_of(&a.label);
                AutoLabel {
                    label: a.label,
                    mailbox_id,
                    name,
                    archived: a.archived,
                    at: a.at,
                }
            })
            .collect();

        if !source.can_label() {
            return Ok(MessageTags {
                suggestions: vec![],
                auto,
            });
        }
        let enabled: Vec<String> = self
            .store
            .settings(scope)
            .await?
            .into_iter()
            .filter(|s| s.mode != LabelMode::Off && !source.labels.contains(&s.label))
            .map(|s| s.label)
            .collect();
        if enabled.is_empty() {
            return Ok(MessageTags {
                suggestions: vec![],
                auto,
            });
        }
        let rejected = self
            .store
            .example(id)
            .await?
            .map(|e| e.negatives)
            .unwrap_or_default();
        let suggestions = self
            .score(&source, scope, enabled.iter().map(String::as_str))
            .await?
            .into_iter()
            .filter(|s| !rejected.contains(&s.label))
            .filter_map(|s| {
                let (mailbox_id, name) = name_of(&s.label);
                Some(Suggestion {
                    confidence: Confidence::of(s.score)?,
                    label: s.label,
                    mailbox_id,
                    name,
                })
            })
            .collect();
        Ok(MessageTags { suggestions, auto })
    }

    /// Put the suggested label on the message, creating it on the account
    /// if it lacks it. The model learns it as it learns any label, when the
    /// change comes back as an event.
    pub async fn accept(&self, id: MessageId, label: &str) -> Result<(), ApplicationError> {
        let source = self.source(id).await?;
        if !source.can_label() {
            return Err(ApplicationError::PreconditionFailed(
                "the account does not keep custom keywords".into(),
            ));
        }
        let scope = self.store.scope_of(source.account_id).await?;
        let group = self.group_mailboxes(scope).await?;
        let place = Placement::of(&source, &group, &label_key(label), LabelMode::Apply);
        self.put(source.account_id, vec![id], &place).await
    }

    pub async fn reject(&self, id: MessageId, label: &str) -> Result<(), ApplicationError> {
        let source = self.source(id).await?;
        let scope = self.store.scope_of(source.account_id).await?;
        self.retrain(&source, scope, Some(&label_key(label)), false)
            .await
    }

    /// Take back a label we applied: off the message, back in the Inbox if we
    /// archived it, and a strong negative for the model.
    pub async fn undo_auto(&self, id: MessageId, label: &str) -> Result<(), ApplicationError> {
        let label = label_key(label);
        let source = self.source(id).await?;
        let record = self
            .store
            .autos(id)
            .await?
            .into_iter()
            .find(|a| a.label == label)
            .ok_or_else(|| ApplicationError::NotFound("Automatic label", label.clone()))?;
        let mailboxes = self.read.list_mailboxes(&source.account_id).await?;
        let inbox = inbox_of(&mailboxes).filter(|_| record.archived);
        if source.keywords {
            self.drop_keyword(&source, &label, inbox).await?;
        } else {
            let target = label_mailbox(&mailboxes, &label)
                .ok_or_else(|| ApplicationError::NotFound("Label", label.clone()))?;
            self.change_labels(
                vec![id],
                inbox.map(|m| m.id).into_iter().collect(),
                vec![target.id],
            )
            .await?;
        }
        self.store.take_auto(id, &label).await?;
        let scope = self.store.scope_of(source.account_id).await?;
        self.retrain(&source, scope, Some(&label), false).await
    }

    /// Take a keyword label off a message of a folders account; archiving
    /// there is a move, so moving back to the Inbox is the undo.
    pub(super) async fn drop_keyword(
        &self,
        source: &TagSource,
        label: &str,
        inbox: Option<&Mailbox>,
    ) -> Result<(), ApplicationError> {
        self.commands
            .dispatch(Command::SetMessageFlags {
                message_id: source.id,
                flags_to_add: vec![],
                flags_to_remove: placement::stored_keywords(source, label),
            })
            .await?;
        if let (Some(inbox), Some(folder)) = (inbox, source.folder) {
            self.commands
                .dispatch(Command::MoveMessage {
                    message_id: source.id,
                    from_mailbox_id: folder,
                    to_mailbox_id: inbox.id,
                    to_mailbox_path: inbox.path.clone(),
                })
                .await?;
        }
        Ok(())
    }

    async fn source(&self, id: MessageId) -> Result<TagSource, ApplicationError> {
        self.store
            .sources(&[id])
            .await?
            .pop()
            .ok_or_else(|| ApplicationError::NotFound("Message", id.to_string()))
    }
}

fn non_empty(name: &str) -> Result<&str, ApplicationError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(ApplicationError::PreconditionFailed("name is empty".into()));
    }
    Ok(name)
}
