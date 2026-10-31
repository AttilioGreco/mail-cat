//! Applying a label to mail already in the group: first a preview, then,
//! once confirmed, batches through the ordinary label commands so every change
//! reaches the provider through its queue and throttling.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{info, warn};

use mc_domain::{
    AccountId, AutoApplied, LabelMode, MailboxReadRepository, MessageId, ModelStats, Scope,
    SourceFilter, TagSource, TaggingStore,
};

use super::placement::{GroupMailboxes, Placement};
use super::{TaggingService, bayes, features, label_key};
use crate::bus::CommandBus;
use crate::error::ApplicationError;

/// Messages read and scored per query.
pub(super) const PAGE: u32 = 500;
/// Tokens asked of the store at once, under its own cap.
const TOKEN_CHUNK: usize = 1000;
/// Messages per provider request, and the pause after each.
const BATCH: usize = 25;
const BATCH_PAUSE: Duration = Duration::from_secs(1);
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

/// One message to label.
#[derive(Debug, Clone)]
pub(super) struct Pick {
    message_id: MessageId,
    account_id: AccountId,
    score: f64,
    place: Placement,
    subject: String,
    from: String,
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
            for (source, tokens) in candidates {
                let Some(score) = bayes::bayes_score(&stats, label, &tokens) else {
                    return Ok(plan);
                };
                let score = bayes::combine(&bayes::Signals { bayes: score });
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
        tokio::spawn(async move { service.run(plan.picks, label).await });
        Ok(preview)
    }

    async fn run(&self, picks: Vec<Pick>, label: String) {
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
            for chunk in group.chunks(BATCH) {
                for pick in chunk {
                    let record = AutoApplied {
                        message_id: pick.message_id,
                        label: label.clone(),
                        score: pick.score,
                        archived: place.archives(),
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
                    self.forget_unapplied(&ids, &label).await;
                    return;
                }
                done += chunk.len();
                tokio::time::sleep(BATCH_PAUSE).await;
            }
        }
        info!(label, done, "bulk label apply finished");
    }
}
