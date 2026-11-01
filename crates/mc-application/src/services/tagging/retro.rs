//! Applying a label to mail already in the group: first a preview, then,
//! once confirmed, batches through the ordinary label commands so every change
//! reaches the provider through its queue and throttling.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{info, warn};

use mc_domain::{
    AccountId, AutoApplied, LabelMode, LabelSettings, MailboxReadRepository, MessageId, ModelStats,
    ProposalId, Scope, SourceFilter, TagSource, TaggingStore,
};

use super::placement::{GroupMailboxes, Placement};
use super::stats::ApplyKind;
use super::{TaggingService, bayes, display_name, features, label_key};
use crate::bus::CommandBus;
use crate::error::ApplicationError;

/// Messages read and scored per query.
pub(super) const PAGE: u32 = 500;
/// Tokens asked of the store at once, under its own cap.
const TOKEN_CHUNK: usize = 1000;
/// Messages per provider request, and the pause after each.
pub(super) const BATCH: usize = 25;
pub(super) const BATCH_PAUSE: Duration = Duration::from_secs(1);
const SAMPLE: usize = 5;

#[derive(Debug, Clone)]
pub struct PreviewItem {
    pub message_id: MessageId,
    pub subject: String,
    pub from: String,
}

#[derive(Debug, Clone, Default)]
pub struct Preview {
    pub to_label: u32,
    /// Of those, how many would also leave the Inbox.
    pub to_archive: u32,
    /// The surest ones.
    pub sample: Vec<PreviewItem>,
}

/// What labelling the existing mail with one label would do.
#[derive(Debug, Clone)]
pub struct LabelApply {
    pub scope: String,
    /// As the group spells it.
    pub name: String,
    pub label: String,
    pub mode: LabelMode,
    pub threshold: f64,
    pub positives: u32,
    pub to_label: u32,
    /// The surest ones.
    pub sample: Vec<PreviewItem>,
}

/// One message to label.
#[derive(Debug, Clone)]
pub(super) struct Pick {
    message_id: MessageId,
    account_id: AccountId,
    score: f64,
    place: Placement,
    subject: String,
    from: String,
    /// The message, when the model is to learn the label from it once put.
    taught: Option<TagSource>,
}

impl Pick {
    /// A message to label on the user's say-so.
    pub fn new(source: &TagSource, place: Placement, score: f64) -> Self {
        Self {
            message_id: source.id,
            account_id: source.account_id,
            score,
            place,
            subject: source.subject.clone(),
            from: source
                .from_name
                .clone()
                .unwrap_or_else(|| source.from_email.clone()),
            taught: Some(source.clone()),
        }
    }
}

/// What a scan decided so far.
#[derive(Default)]
pub(super) struct Plan {
    pub picks: Vec<Pick>,
}

impl Plan {
    /// Judge one message scored for the label.
    pub fn consider(
        &mut self,
        source: &TagSource,
        score: f64,
        threshold: f64,
        mode: LabelMode,
        group: &GroupMailboxes,
        label: &str,
    ) {
        if source.labels.iter().any(|l| l == label) || score < threshold || !source.can_label() {
            return;
        }
        self.picks.push(Pick {
            message_id: source.id,
            account_id: source.account_id,
            score,
            place: Placement::of(source, group, label, mode),
            subject: source.subject.clone(),
            from: source
                .from_name
                .clone()
                .unwrap_or_else(|| source.from_email.clone()),
            taught: None,
        });
    }

    pub fn preview(&self) -> Preview {
        let mut best: Vec<&Pick> = self.picks.iter().collect();
        best.sort_by(|a, b| b.score.total_cmp(&a.score));
        Preview {
            to_label: self.picks.len() as u32,
            to_archive: self.picks.iter().filter(|p| p.place.archives()).count() as u32,
            sample: best
                .into_iter()
                .take(SAMPLE)
                .map(|p| PreviewItem {
                    message_id: p.message_id,
                    subject: p.subject.clone(),
                    from: p.from.clone(),
                })
                .collect(),
        }
    }
}

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    /// Walk the group's mail a page at a time (keyset, oldest first), scoring
    /// each page against one read of the counts for its tokens.
    pub(super) async fn plan(
        &self,
        account: AccountId,
        label: &str,
        mode: LabelMode,
        threshold: f64,
    ) -> Result<Plan, ApplicationError> {
        let scope = self.store.scope_of(account).await?;
        let accounts = self.store.scope_accounts(scope).await?;
        let group = self.group_mailboxes(scope).await?;
        let mut plan = Plan::default();
        let mut cursor = None;
        loop {
            let page = self
                .store
                .source_page(&accounts, SourceFilter::Any, cursor, false, self.page)
                .await?;
            let Some(last) = page.last().map(|s| s.id) else {
                return Ok(plan);
            };
            cursor = Some(last);
            let candidates: Vec<(&TagSource, Vec<String>)> = page
                .iter()
                .filter(|s| !s.labels.iter().any(|l| l == label))
                .map(|s| (s, features::tokens(s, true)))
                .collect();
            let wanted = candidates.iter().flat_map(|(_, t)| t.iter().cloned());
            let stats = self.stats_for(scope, wanted.collect()).await?;
            let ids: Vec<MessageId> = candidates.iter().map(|(s, _)| s.id).collect();
            let nearest = self.nearest(scope, &ids).await?;
            for (source, tokens) in candidates {
                let Some(score) = bayes::score(&stats, label, &tokens, nearest.get(&source.id))
                else {
                    return Ok(plan);
                };
                if score < threshold || self.rejected(source.id, label).await? {
                    continue;
                }
                plan.consider(source, score, threshold, mode, &group, label);
            }
        }
    }

    /// The counts for `tokens`, asked in chunks the store accepts.
    async fn stats_for(
        &self,
        scope: Scope,
        tokens: BTreeSet<String>,
    ) -> Result<ModelStats, ApplicationError> {
        let tokens: Vec<String> = tokens.into_iter().collect();
        let mut chunks: Vec<&[String]> = tokens.chunks(TOKEN_CHUNK).collect();
        if chunks.is_empty() {
            chunks.push(&[]);
        }
        let mut all = ModelStats::default();
        for chunk in chunks {
            let part = self.store.stats(scope, chunk).await?;
            all.documents = part.documents;
            all.corpus.extend(part.corpus);
            for (label, counts) in part.labels {
                let into = all.labels.entry(label).or_default();
                into.positive_docs = counts.positive_docs;
                into.negative_docs = counts.negative_docs;
                into.tokens.extend(counts.tokens);
            }
        }
        Ok(all)
    }

    async fn rejected(&self, id: MessageId, label: &str) -> Result<bool, ApplicationError> {
        Ok(self
            .store
            .example(id)
            .await?
            .is_some_and(|e| e.negatives.iter().any(|l| l == label)))
    }

    /// What enabling the mode on the label would do to existing mail.
    pub async fn preview_apply(
        &self,
        account: AccountId,
        label: &str,
        mode: LabelMode,
        threshold: f64,
    ) -> Result<Preview, ApplicationError> {
        Ok(self
            .plan(account, &label_key(label), mode, threshold)
            .await?
            .preview())
    }

    /// Save the mode and start labelling existing mail in the background.
    /// Returns what the preview promised, as of now.
    pub async fn confirm_apply(
        self: &Arc<Self>,
        account: AccountId,
        label: &str,
        mode: LabelMode,
        threshold: f64,
    ) -> Result<Preview, ApplicationError> {
        if !mode.applies() {
            return Err(ApplicationError::PreconditionFailed(
                "mode does not apply labels".into(),
            ));
        }
        let label = label_key(label);
        self.set_label(account, &label, mode, threshold).await?;
        let plan = self.plan(account, &label, mode, threshold).await?;
        let preview = plan.preview();
        let service = Arc::clone(self);
        tokio::spawn(async move { service.run(plan.picks, label, false, None).await });
        Ok(preview)
    }

    /// What the classifier would label among the existing mail, for every label
    /// of the group with enough examples, in one pass over the mail: a page's
    /// words and neighbours are read once and scored for all the labels.
    /// Without `only`, a dry run covers every such label and a run those whose
    /// mode is not off. A run queues one throttled job per label; modes stay as
    /// they are and nothing leaves the Inbox.
    pub async fn apply_all(
        self: &Arc<Self>,
        account: Option<AccountId>,
        only: &[String],
        dry_run: bool,
    ) -> Result<Vec<LabelApply>, ApplicationError> {
        let only: HashSet<String> = only.iter().map(|l| label_key(l)).collect();
        let scopes = match account {
            Some(a) => vec![self.store.scope_of(a).await?],
            None => self.store.scopes().await?,
        };
        let mut found = HashSet::new();
        let (mut out, mut jobs) = (Vec::new(), Vec::new());
        for scope in scopes {
            let settings = self.store.settings(scope).await?;
            let mut chosen = Vec::new();
            for (label, positives) in self.store.positives(scope).await? {
                let set = settings
                    .iter()
                    .find(|s| s.label == label)
                    .cloned()
                    .unwrap_or_else(|| LabelSettings::off(&label));
                let wanted = if only.is_empty() {
                    dry_run || set.mode != LabelMode::Off
                } else {
                    only.contains(&label)
                };
                if !wanted || positives < LabelSettings::MIN_POSITIVES {
                    continue;
                }
                if !dry_run && self.is_applying(&label) {
                    return Err(ApplicationError::PreconditionFailed(format!(
                        "{label:?} is still being applied or undone: wait for it to finish"
                    )));
                }
                found.insert(label.clone());
                chosen.push((label, set, positives));
            }
            if chosen.is_empty() {
                continue;
            }
            let group = self.group_mailboxes(scope).await?;
            let plans = self.plan_all(scope, &chosen, &group).await?;
            for ((label, set, positives), plan) in chosen.into_iter().zip(plans) {
                let preview = plan.preview();
                out.push(LabelApply {
                    scope: scope.key(),
                    name: display_name(&group, &label),
                    label: label.clone(),
                    mode: set.mode,
                    threshold: set.threshold,
                    positives,
                    to_label: preview.to_label,
                    sample: preview.sample,
                });
                jobs.push((label, plan.picks));
            }
        }
        if let Some(missing) = only.iter().find(|l| !found.contains(*l)) {
            return Err(ApplicationError::PreconditionFailed(format!(
                "{missing:?} is not a label with at least {} examples",
                LabelSettings::MIN_POSITIVES
            )));
        }
        if !dry_run {
            for (label, picks) in jobs.into_iter().filter(|(_, p)| !p.is_empty()) {
                let service = Arc::clone(self);
                tokio::spawn(async move { service.run(picks, label, false, None).await });
            }
        }
        out.sort_by(|a, b| {
            b.to_label
                .cmp(&a.to_label)
                .then_with(|| a.name.cmp(&b.name))
        });
        Ok(out)
    }

    /// `plan` for several labels of one scope at once, in the order given.
    async fn plan_all(
        &self,
        scope: Scope,
        labels: &[(String, LabelSettings, u32)],
        group: &GroupMailboxes,
    ) -> Result<Vec<Plan>, ApplicationError> {
        let accounts = self.store.scope_accounts(scope).await?;
        let mut plans: Vec<Plan> = labels.iter().map(|_| Plan::default()).collect();
        let mut cursor = None;
        loop {
            let page = self
                .store
                .source_page(&accounts, SourceFilter::Any, cursor, false, self.page)
                .await?;
            let Some(last) = page.last().map(|s| s.id) else {
                return Ok(plans);
            };
            cursor = Some(last);
            let candidates: Vec<(&TagSource, Vec<String>)> = page
                .iter()
                .filter(|s| s.can_label())
                .map(|s| (s, features::tokens(s, true)))
                .collect();
            let wanted = candidates.iter().flat_map(|(_, t)| t.iter().cloned());
            let stats = self.stats_for(scope, wanted.collect()).await?;
            let ids: Vec<MessageId> = candidates.iter().map(|(s, _)| s.id).collect();
            let nearest = self.nearest(scope, &ids).await?;
            for (source, tokens) in candidates {
                for ((label, set, _), plan) in labels.iter().zip(&mut plans) {
                    let Some(score) = bayes::score(&stats, label, &tokens, nearest.get(&source.id))
                    else {
                        continue;
                    };
                    if score < set.threshold
                        || source.labels.contains(label)
                        || self.rejected(source.id, label).await?
                    {
                        continue;
                    }
                    plan.consider(source, score, set.threshold, LabelMode::Apply, group, label);
                }
            }
        }
    }

    /// Label the picks in batches, each recorded first so it can be undone.
    /// `confirmed`: the user chose these, so the model learns the label from
    /// them as it does from any label of theirs.
    ///
    /// Jobs take turns and show in the background stats from the moment they
    /// are queued. `proposal` is the acceptance the labels belong to.
    pub(super) async fn run(
        &self,
        picks: Vec<Pick>,
        label: String,
        confirmed: bool,
        proposal: Option<ProposalId>,
    ) {
        let job = self.begin_job(&label, ApplyKind::Apply, picks.len() as u64);
        let _turn = self.apply_turn.lock().await;
        self.apply_picks(picks, &label, confirmed, proposal, job)
            .await;
        self.end_job(job);
    }

    async fn apply_picks(
        &self,
        picks: Vec<Pick>,
        label: &str,
        confirmed: bool,
        proposal: Option<ProposalId>,
        job: u64,
    ) {
        let mut groups: HashMap<(AccountId, Placement), Vec<Pick>> = HashMap::new();
        for pick in picks {
            groups
                .entry((pick.account_id, pick.place.clone()))
                .or_default()
                .push(pick);
        }
        let (mut done, mut total) = (0, 0);
        for ((account, place), group) in groups {
            total += group.len();
            let scope = match self.store.scope_of(account).await {
                Ok(scope) => scope,
                Err(e) => {
                    warn!(error = %e, "label classifier: bulk apply stopped");
                    return;
                }
            };
            for chunk in group.chunks(BATCH) {
                for pick in chunk {
                    let record = AutoApplied {
                        message_id: pick.message_id,
                        label: label.to_owned(),
                        score: pick.score,
                        archived: place.archives(),
                        confirmed,
                        proposal_id: proposal,
                        at: Utc::now(),
                    };
                    if let Err(e) = self.store.record_auto(&record).await {
                        warn!(error = %e, "label classifier: bulk apply stopped");
                        return;
                    }
                }
                let ids: Vec<MessageId> = chunk.iter().map(|p| p.message_id).collect();
                let change = self.put(account, ids.clone(), &place).await;
                if let Err(e) = change {
                    warn!(error = %e, done, total, "label classifier: bulk apply stopped");
                    self.forget_unapplied(&ids, label).await;
                    return;
                }
                done += chunk.len();
                let archived = if place.archives() { chunk.len() } else { 0 };
                self.advance_job(job, chunk.len() as u64, archived as u64);
                for source in chunk.iter().filter_map(|p| p.taught.as_ref()) {
                    let mut labelled = source.clone();
                    labelled.labels.push(label.to_owned());
                    if let Err(e) = self.retrain(&labelled, scope, None, false).await {
                        warn!(error = %e, "label classifier: could not learn from a bulk apply");
                    }
                }
                tokio::time::sleep(BATCH_PAUSE).await;
            }
        }
        info!(label, done, "bulk label apply finished");
    }
}
