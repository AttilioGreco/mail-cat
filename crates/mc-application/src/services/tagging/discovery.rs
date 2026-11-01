//! Discovery: groups of similar unlabelled mail, found in the background and
//! proposed to the user as labels. Accepting one creates the label, puts it
//! on the mail and teaches the model; rejecting it keeps the cluster from
//! being proposed again.

mod cluster;
mod curate;
#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use mc_domain::{
    AccountId, ClusterSummary, LabelMode, LabelSettings, MailboxReadRepository, MessageId,
    Proposal, ProposalId, ProposalSample, ProposalStatus, Scope, TagSource, TaggingStore,
};

use super::placement::Home;
use super::retro::Pick;
use super::{Placement, TaggingService, features, label_key, non_empty};
use crate::bus::CommandBus;
use crate::error::ApplicationError;

/// Smallest group worth proposing; also what HDBSCAN is told.
const MIN_CLUSTER: usize = 30;
/// How many neighbours a point needs to count as dense (HDBSCAN `min_samples`).
const MIN_SAMPLES: usize = 5;
/// Mail clustered at most: past it, the newest not in the Inbox is left out.
const MAX_POINTS: u32 = 40_000;
/// Mail outside every cluster that is read, to tell clusters from the rest.
const REST_SAMPLE: usize = 2000;
const SCAN_PAGE: usize = 500;
/// Clusters sharing this much of their members (Jaccard) are the same cluster
/// from one run to the next.
const SAME_CLUSTER: f64 = 0.5;
const INTERVAL: Duration = Duration::from_secs(6 * 3600);
/// Startup is busy enough already.
const STARTUP_DELAY: Duration = Duration::from_secs(300);
const RETRAIN_WAIT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryPhase {
    Loading,
    Clustering,
    Naming,
}

#[derive(Debug, Clone)]
pub struct DiscoveryProgress {
    pub scope: String,
    pub phase: DiscoveryPhase,
    pub done: u64,
    pub total: u64,
}

/// A proposal and the group whose mail it is made of.
#[derive(Debug, Clone)]
pub struct ProposalView {
    pub proposal: Proposal,
    pub group: String,
    /// Dominant sender domains, most frequent first; empty unless asked for.
    pub senders: Vec<(String, u32)>,
}

pub use curate::{LabelEntry, LabelScope, ProposalMessage};

/// Marks a discovery as over when dropped, however it ends.
struct Discovering<'a, S, R, C>(&'a TaggingService<S, R, C>)
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static;

impl<S, R, C> Drop for Discovering<'_, S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    fn drop(&mut self) {
        self.0.lock_activity().discovery = None;
        self.0.discovering.store(false, Ordering::Relaxed);
    }
}

fn jaccard(a: &HashSet<MessageId>, b: &HashSet<MessageId>) -> f64 {
    let both = a.intersection(b).count();
    both as f64 / (a.len() + b.len() - both).max(1) as f64
}

fn domain_of(email: &str) -> String {
    email
        .rsplit_once('@')
        .map_or(email, |(_, d)| d)
        .to_lowercase()
}

fn sample_of(source: &TagSource) -> ProposalSample {
    ProposalSample {
        message_id: source.id,
        subject: source.subject.clone(),
        from: source
            .from_name
            .clone()
            .unwrap_or_else(|| source.from_email.clone()),
    }
}

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    // ── Running ──

    /// Look for new labels now, in the background: in `account`'s group, or
    /// in every group. `false` when a search is already running.
    pub async fn start_discovery(
        self: &Arc<Self>,
        account: Option<AccountId>,
    ) -> Result<bool, ApplicationError> {
        if !self.embeddings_enabled() {
            return Err(ApplicationError::PreconditionFailed(
                "servono gli embedding attivi per cercare nuove etichette".into(),
            ));
        }
        let only = match account {
            Some(a) => Some(self.store.scope_of(a).await?),
            None => None,
        };
        if self.discovering.swap(true, Ordering::Relaxed) {
            return Ok(false);
        }
        self.progress(None, DiscoveryPhase::Loading, 0, 0);
        let service = Arc::clone(self);
        tokio::spawn(async move { service.discover_scopes(only).await });
        Ok(true)
    }

    /// Every `INTERVAL`, and when the embedding backlog has just been cleared.
    pub(super) async fn run_discovery(self: &Arc<Self>, stop: CancellationToken) {
        let mut wait = STARTUP_DELAY;
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
                _ = self.discovery_wake.notified() => {}
            }
            wait = INTERVAL;
            if !self.embeddings_enabled() || self.discovering.swap(true, Ordering::Relaxed) {
                continue;
            }
            // Until the backlog is embedded the clusters would be of a part
            // of the mail; its end wakes this up again.
            match self.embedding_counts().await {
                Ok((embedded, eligible)) if embedded >= eligible => {
                    self.discover_scopes(None).await;
                }
                _ => self.discovering.store(false, Ordering::Relaxed),
            }
        }
    }

    /// The caller has set `discovering`; this clears it.
    async fn discover_scopes(self: &Arc<Self>, only: Option<Scope>) {
        let _over = Discovering(self);
        let scopes = match self.store.scopes().await {
            Ok(scopes) => scopes,
            Err(e) => return warn!(error = %e, "discovery: could not list the groups"),
        };
        for scope in scopes.into_iter().filter(|s| only.is_none_or(|o| o == *s)) {
            let this = Arc::clone(self);
            match super::niced(async move { this.discover(scope).await }).await {
                Ok(found) => info!(found, "discovery: proposals are up to date"),
                Err(e) => warn!(error = %e, "discovery: failed"),
            }
        }
    }

    /// `scope` is `None` between asking for a search and starting on a group.
    fn progress(&self, scope: Option<Scope>, phase: DiscoveryPhase, done: usize, total: usize) {
        self.lock_activity().discovery = Some((scope, phase, done as u64, total as u64));
    }

    pub(super) async fn discovery_progress(
        &self,
    ) -> Result<Option<DiscoveryProgress>, ApplicationError> {
        let Some((scope, phase, done, total)) = self.lock_activity().discovery else {
            return Ok(None);
        };
        Ok(Some(DiscoveryProgress {
            scope: match scope {
                Some(scope) => self.scope_names().await?.remove(&scope).unwrap_or_default(),
                None => String::new(),
            },
            phase,
            done,
            total,
        }))
    }

    pub(super) async fn scope_names(&self) -> Result<HashMap<Scope, String>, ApplicationError> {
        Ok(self
            .store
            .list_groups()
            .await?
            .into_iter()
            .map(|g| (Scope::Group(g.id), g.name))
            .collect())
    }

    /// Cluster the group's unlabelled mail and bring its pending proposals up
    /// to date; returns how many there are. CPU-bound: run it niced.
    pub(super) async fn discover(&self, scope: Scope) -> Result<usize, ApplicationError> {
        let Some(embedder) = self.embedder.clone() else {
            return Ok(0);
        };
        self.progress(Some(scope), DiscoveryPhase::Loading, 0, 0);
        self.wait_while_retraining().await?;
        let accounts = self.store.scope_accounts(scope).await?;
        let points = self
            .store
            .unlabelled_points(&accounts, &embedder.model().to_string(), MAX_POINTS)
            .await?;

        self.progress(Some(scope), DiscoveryPhase::Clustering, 0, 0);
        let vectors: Vec<&[f32]> = points.iter().map(|p| p.vector.as_slice()).collect();
        let clusters = cluster::find(&vectors, MIN_CLUSTER, MIN_SAMPLES);

        let known = self.store.proposals(ProposalStatus::Rejected).await?;
        let rejected: Vec<HashSet<MessageId>> = known
            .into_iter()
            .filter(|p| p.scope == scope)
            .map(|p| p.members.into_iter().collect())
            .collect();
        let clusters: Vec<_> = clusters
            .into_iter()
            .filter(|c| {
                let members: HashSet<MessageId> = c.members.iter().map(|&i| points[i].id).collect();
                !rejected
                    .iter()
                    .any(|r| jaccard(&members, r) >= SAME_CLUSTER)
            })
            .collect();

        let made = self.describe(scope, &points, &clusters).await?;
        self.refresh(scope, made).await
    }

    async fn wait_while_retraining(&self) -> Result<(), ApplicationError> {
        while self.retraining().await? {
            tokio::time::sleep(RETRAIN_WAIT).await;
        }
        Ok(())
    }

    /// Read the clusters' messages and the words that set each apart, and
    /// name them.
    async fn describe(
        &self,
        scope: Scope,
        points: &[mc_domain::DiscoveryPoint],
        clusters: &[cluster::Cluster],
    ) -> Result<Vec<Proposal>, ApplicationError> {
        let mut class_of: HashMap<MessageId, usize> = HashMap::new();
        let mut sampled: HashSet<MessageId> = HashSet::new();
        for (c, cluster) in clusters.iter().enumerate() {
            class_of.extend(cluster.members.iter().map(|&i| (points[i].id, c)));
            sampled.extend(cluster.central.iter().map(|&i| points[i].id));
        }
        let rest = points
            .iter()
            .map(|p| p.id)
            .filter(|id| !class_of.contains_key(id))
            .take(REST_SAMPLE);
        let wanted: Vec<MessageId> = class_of.keys().copied().chain(rest).collect();

        let mut terms = cluster::Terms::new(clusters.len());
        let mut senders = vec![HashMap::<String, usize>::new(); clusters.len()];
        let mut shown: HashMap<MessageId, ProposalSample> = HashMap::new();
        for (done, page) in wanted.chunks(SCAN_PAGE).enumerate() {
            self.progress(
                Some(scope),
                DiscoveryPhase::Naming,
                done * SCAN_PAGE,
                wanted.len(),
            );
            self.wait_while_retraining().await?;
            for source in self.store.sources(page).await? {
                let class = class_of.get(&source.id).copied();
                terms.add(class, &features::tokens(&source, true));
                if let Some(c) = class {
                    *senders[c].entry(domain_of(&source.from_email)).or_default() += 1;
                }
                if sampled.contains(&source.id) {
                    shown.insert(source.id, sample_of(&source));
                }
            }
            self.breathe().await;
        }

        let mut made = Vec::with_capacity(clusters.len());
        for (c, cluster) in clusters.iter().enumerate() {
            let samples: Vec<ProposalSample> = cluster
                .central
                .iter()
                .filter_map(|&i| shown.get(&points[i].id).cloned())
                .collect();
            let mut by_sender: Vec<_> = senders[c].drain().collect();
            by_sender.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let named = self
                .namer
                .name(&ClusterSummary {
                    size: cluster.members.len(),
                    keywords: terms.keywords(c),
                    senders: by_sender,
                    sample_subjects: samples.iter().map(|s| s.subject.clone()).collect(),
                })
                .await;
            made.push(Proposal {
                id: ProposalId::new(),
                scope,
                name: named.name,
                keywords: named.keywords,
                members: cluster.members.iter().map(|&i| points[i].id).collect(),
                inbox: cluster
                    .members
                    .iter()
                    .filter(|&&i| points[i].in_inbox)
                    .count() as u32,
                samples,
                status: ProposalStatus::Pending,
                created_at: Utc::now(),
            });
        }
        Ok(made)
    }

    /// Replace the scope's pending proposals with `made`. A proposal that
    /// matches a pending one keeps its id, so what the user is looking at
    /// does not change under them.
    async fn refresh(&self, scope: Scope, made: Vec<Proposal>) -> Result<usize, ApplicationError> {
        let mut stale: Vec<(Proposal, HashSet<MessageId>)> = self
            .store
            .proposals(ProposalStatus::Pending)
            .await?
            .into_iter()
            .filter(|p| p.scope == scope)
            .map(|p| {
                let members = p.members.iter().copied().collect();
                (p, members)
            })
            .collect();
        let found = made.len();
        for mut proposal in made {
            let members: HashSet<MessageId> = proposal.members.iter().copied().collect();
            let same = stale
                .iter()
                .map(|(_, old)| jaccard(&members, old))
                .enumerate()
                .filter(|(_, j)| *j >= SAME_CLUSTER)
                .max_by(|a, b| a.1.total_cmp(&b.1))
                .map(|(i, _)| i);
            if let Some(i) = same {
                let (old, _) = stale.swap_remove(i);
                proposal.id = old.id;
                proposal.created_at = old.created_at;
            }
            self.store.put_proposal(&proposal).await?;
        }
        for (old, _) in stale {
            self.store.delete_proposal(old.id).await?;
        }
        Ok(found)
    }

    // ── The user's answer ──

    async fn pending(&self, id: ProposalId) -> Result<Proposal, ApplicationError> {
        self.store
            .proposal(id)
            .await?
            .filter(|p| p.status == ProposalStatus::Pending)
            .ok_or_else(|| ApplicationError::NotFound("Proposal", id.as_uuid().to_string()))
    }

    pub async fn reject_proposal(&self, id: ProposalId) -> Result<(), ApplicationError> {
        let proposal = self.pending(id).await?;
        Ok(self
            .store
            .put_proposal(&Proposal {
                status: ProposalStatus::Rejected,
                ..proposal
            })
            .await?)
    }

    /// Create the label on the accounts that have messages in the cluster and
    /// put it on those, in the background and through the usual throttled
    /// queue, `archive`d out of the Inbox if asked. The model learns them as
    /// the user's own, and the label starts in `Suggest`. Returns how many
    /// messages are being labelled.
    pub async fn accept_proposal(
        self: &Arc<Self>,
        id: ProposalId,
        name: Option<&str>,
        archive: bool,
    ) -> Result<u32, ApplicationError> {
        let proposal = self.pending(id).await?;
        let name = non_empty(name.unwrap_or(&proposal.name))?.to_owned();
        self.label_with(proposal, name, archive).await
    }

    /// Put `name` on the proposal's mail, creating it where an account lacks it.
    async fn label_with(
        self: &Arc<Self>,
        proposal: Proposal,
        name: String,
        archive: bool,
    ) -> Result<u32, ApplicationError> {
        let label = label_key(&name);
        let group = self.group_mailboxes(proposal.scope).await?;
        let mode = if archive {
            LabelMode::ApplyArchive
        } else {
            LabelMode::Apply
        };

        let mut created: HashMap<AccountId, mc_domain::MailboxId> = HashMap::new();
        let mut picks = Vec::new();
        for page in proposal.members.chunks(SCAN_PAGE) {
            for source in self.store.sources(page).await? {
                // Labelled since, or on an account that cannot keep the label.
                if !source.labels.is_empty() || !source.can_label() {
                    continue;
                }
                let mut place = Placement::of(&source, &group, &label, mode);
                // The label is keyed in lowercase; created, it takes the user's spelling.
                if matches!(place.home, Home::Create(_)) {
                    let mailbox = match created.get(&source.account_id) {
                        Some(&mailbox) => mailbox,
                        None => {
                            let mailbox = self.create_label(source.account_id, &name).await?;
                            created.insert(source.account_id, mailbox);
                            mailbox
                        }
                    };
                    place.home = Home::Mailbox(mailbox);
                }
                picks.push(Pick::new(&source, place, 1.0));
            }
        }

        let queued = picks.len() as u32;
        let settings = self.store.settings(proposal.scope).await?;
        if settings
            .iter()
            .all(|s| s.label != label || s.mode == LabelMode::Off)
        {
            let suggest = LabelSettings {
                mode: LabelMode::Suggest,
                ..LabelSettings::off(&label)
            };
            self.store.put_settings(proposal.scope, &suggest).await?;
        }
        self.store
            .put_proposal(&Proposal {
                name,
                status: ProposalStatus::Accepted,
                ..proposal
            })
            .await?;
        let service = Arc::clone(self);
        tokio::spawn(async move { service.run(picks, label, true).await });
        Ok(queued)
    }
}
