//! Curating proposals before they are accepted: listing with the evidence
//! (senders, sample messages), renaming, merging, and putting an existing
//! label instead of a new one. Meant for people or assistants driving the CLI.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};

use mc_domain::{
    AccountId, ClusterSummary, MailboxReadRepository, MessageId, Proposal, ProposalId,
    ProposalSample, ProposalStatus, Scope, TaggingStore,
};

use super::{ProposalView, REST_SAMPLE, SCAN_PAGE, cluster, domain_of, sample_of};
use crate::bus::CommandBus;
use crate::error::ApplicationError;
use crate::services::tagging::placement::display_name;
use crate::services::tagging::{
    LabelStatus, TaggingService, features, label_key, label_mailbox, non_empty,
};

/// Dominant sender domains kept per proposal.
const SENDERS: usize = 8;
/// Samples a merged proposal keeps, as many as discovery gives a cluster.
const SAMPLES: usize = 5;
const EXCERPT_CHARS: usize = 200;

/// A message of a proposal, read closely enough to judge the proposal by.
#[derive(Debug, Clone)]
pub struct ProposalMessage {
    pub message_id: MessageId,
    pub received_at: DateTime<Utc>,
    pub from: String,
    pub subject: String,
    /// Start of the plain text; empty until the body is downloaded.
    pub excerpt: String,
}

#[derive(Debug, Clone)]
pub struct LabelEntry {
    /// As the scope spells it.
    pub name: String,
    pub status: LabelStatus,
}

/// The labels one model learns, with the accounts it learns from.
#[derive(Debug, Clone)]
pub struct LabelScope {
    pub scope: String,
    /// Empty for an account that learns alone.
    pub group: String,
    pub accounts: Vec<AccountId>,
    pub labels: Vec<LabelEntry>,
}

/// What reading a set of messages came to.
struct Reading {
    summary: ClusterSummary,
    senders: Vec<(String, u32)>,
    inbox: u32,
    samples: Vec<ProposalSample>,
}

fn excerpt(body: Option<&str>) -> String {
    let mut out = String::new();
    for word in body.unwrap_or_default().split_whitespace() {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(word);
        if out.chars().count() >= EXCERPT_CHARS {
            return out.chars().take(EXCERPT_CHARS).collect();
        }
    }
    out
}

fn top_senders(counts: HashMap<String, u32>) -> Vec<(String, u32)> {
    let mut by_sender: Vec<_> = counts.into_iter().collect();
    by_sender.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    by_sender.truncate(SENDERS);
    by_sender
}

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    /// Proposals of the account's scope (every scope without one), of that
    /// status (any without one), Inbox-heavy first within a status.
    /// `senders` reads every member to count the sender domains.
    pub async fn list_proposals(
        &self,
        account: Option<AccountId>,
        status: Option<ProposalStatus>,
        senders: bool,
    ) -> Result<Vec<ProposalView>, ApplicationError> {
        let only = match account {
            Some(a) => Some(self.store.scope_of(a).await?),
            None => None,
        };
        let statuses = status.map_or(
            vec![
                ProposalStatus::Pending,
                ProposalStatus::Accepted,
                ProposalStatus::Rejected,
            ],
            |s| vec![s],
        );
        let names = self.scope_names().await?;
        let mut views = Vec::new();
        for status in statuses {
            for proposal in self.store.proposals(status).await? {
                if only.is_some_and(|o| o != proposal.scope) {
                    continue;
                }
                let senders = match senders {
                    true => self.senders_of(&proposal.members).await?,
                    false => Vec::new(),
                };
                views.push(ProposalView {
                    group: names.get(&proposal.scope).cloned().unwrap_or_default(),
                    proposal,
                    senders,
                });
            }
        }
        Ok(views)
    }

    async fn senders_of(
        &self,
        members: &[MessageId],
    ) -> Result<Vec<(String, u32)>, ApplicationError> {
        let mut counts = HashMap::<String, u32>::new();
        for page in members.chunks(SCAN_PAGE) {
            for source in self.store.sources(page).await? {
                *counts.entry(domain_of(&source.from_email)).or_default() += 1;
            }
        }
        Ok(top_senders(counts))
    }

    /// A proposal of any status and up to `samples` of its messages: the
    /// central ones discovery picked first, then the rest in order.
    pub async fn proposal_detail(
        &self,
        id: ProposalId,
        samples: usize,
    ) -> Result<(ProposalView, Vec<ProposalMessage>), ApplicationError> {
        let proposal = self
            .store
            .proposal(id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Proposal", id.as_uuid().to_string()))?;
        let mut wanted: Vec<MessageId> = proposal.samples.iter().map(|s| s.message_id).collect();
        let shown: HashSet<MessageId> = wanted.iter().copied().collect();
        wanted.extend(proposal.members.iter().filter(|m| !shown.contains(m)));
        wanted.truncate(samples);

        let mut found: HashMap<MessageId, _> = self
            .store
            .sources(&wanted)
            .await?
            .into_iter()
            .map(|s| (s.id, s))
            .collect();
        let messages = wanted
            .iter()
            .filter_map(|id| found.remove(id))
            .map(|s| ProposalMessage {
                message_id: s.id,
                received_at: s.received_at,
                excerpt: excerpt(s.body.as_deref()),
                from: sample_of(&s).from,
                subject: s.subject,
            })
            .collect();
        let group = self
            .scope_names()
            .await?
            .remove(&proposal.scope)
            .unwrap_or_default();
        let senders = self.senders_of(&proposal.members).await?;
        Ok((
            ProposalView {
                proposal,
                group,
                senders,
            },
            messages,
        ))
    }

    pub async fn rename_proposal(
        &self,
        id: ProposalId,
        name: &str,
    ) -> Result<ProposalView, ApplicationError> {
        let proposal = Proposal {
            name: non_empty(name)?.to_owned(),
            ..self.pending(id).await?
        };
        self.store.put_proposal(&proposal).await?;
        let group = self
            .scope_names()
            .await?
            .remove(&proposal.scope)
            .unwrap_or_default();
        Ok(ProposalView {
            proposal,
            group,
            senders: Vec::new(),
        })
    }

    /// Replace pending proposals of one scope by a single new one: the union
    /// of their members, with keywords, senders and samples read again.
    /// Keywords weigh the merged mail against the scope's other pending
    /// proposals (discovery weighs a cluster against the mail outside every
    /// cluster, which is not kept). The sources are deleted, not rejected.
    pub async fn merge_proposals(
        &self,
        ids: &[ProposalId],
        name: Option<&str>,
    ) -> Result<ProposalView, ApplicationError> {
        let unique: HashSet<ProposalId> = ids.iter().copied().collect();
        if unique.len() < 2 {
            return Err(ApplicationError::PreconditionFailed(
                "merging takes at least two different proposals".into(),
            ));
        }
        let mut sources = Vec::with_capacity(unique.len());
        for &id in &unique {
            sources.push(self.pending(id).await?);
        }
        let scope = sources[0].scope;
        if sources.iter().any(|p| p.scope != scope) {
            return Err(ApplicationError::PreconditionFailed(
                "proposals of different training groups cannot be merged".into(),
            ));
        }
        // Oldest first, so the result does not depend on the order asked for.
        sources.sort_by_key(|p| (p.created_at, *p.id.as_uuid()));

        let mut seen = HashSet::new();
        let members: Vec<MessageId> = sources
            .iter()
            .flat_map(|p| p.members.iter().copied())
            .filter(|m| seen.insert(*m))
            .collect();
        let rest: Vec<MessageId> = self
            .store
            .proposals(ProposalStatus::Pending)
            .await?
            .into_iter()
            .filter(|p| p.scope == scope && !unique.contains(&p.id))
            .flat_map(|p| p.members)
            .filter(|m| !seen.contains(m))
            .take(REST_SAMPLE)
            .collect();

        let shown: Vec<&[ProposalSample]> = sources.iter().map(|p| p.samples.as_slice()).collect();
        let reading = self.read(&members, &rest, &shown).await?;
        let named = self.namer.name(&reading.summary).await;
        let name = match name {
            Some(name) => non_empty(name)?.to_owned(),
            None => named.name,
        };
        let merged = Proposal {
            id: ProposalId::new(),
            scope,
            name,
            keywords: named.keywords,
            members,
            inbox: reading.inbox,
            samples: reading.samples,
            status: ProposalStatus::Pending,
            created_at: Utc::now(),
        };
        self.store.put_proposal(&merged).await?;
        for old in &sources {
            self.store.delete_proposal(old.id).await?;
        }
        let group = self.scope_names().await?.remove(&scope).unwrap_or_default();
        Ok(ProposalView {
            proposal: merged,
            group,
            senders: reading.senders,
        })
    }

    /// Read `members` as one class and `rest` as the outside; samples are
    /// taken in turn from each of `earlier` (what the parts showed).
    async fn read(
        &self,
        members: &[MessageId],
        rest: &[MessageId],
        earlier: &[&[ProposalSample]],
    ) -> Result<Reading, ApplicationError> {
        let inside: HashSet<MessageId> = members.iter().copied().collect();
        let mut terms = cluster::Terms::new(1);
        let mut senders = HashMap::<String, u32>::new();
        let mut inbox = 0;
        for page in members.chunks(SCAN_PAGE).chain(rest.chunks(SCAN_PAGE)) {
            for source in self.store.sources(page).await? {
                let member = inside.contains(&source.id);
                terms.add(member.then_some(0), &features::tokens(&source, true));
                if member {
                    *senders.entry(domain_of(&source.from_email)).or_default() += 1;
                    inbox += u32::from(source.in_inbox);
                }
            }
        }
        let mut samples = Vec::with_capacity(SAMPLES);
        for round in 0..SAMPLES {
            samples.extend(earlier.iter().filter_map(|s| s.get(round)).cloned());
        }
        samples.truncate(SAMPLES);
        let senders = top_senders(senders);
        Ok(Reading {
            summary: ClusterSummary {
                size: members.len(),
                keywords: terms.keywords(0),
                senders: senders
                    .iter()
                    .map(|(d, n)| (d.clone(), *n as usize))
                    .collect(),
                sample_subjects: samples.iter().map(|s| s.subject.clone()).collect(),
            },
            senders,
            inbox,
            samples,
        })
    }

    /// Like accepting, but with a label the proposal's group already has,
    /// spelled as the group spells it. Returns how many messages are queued.
    pub async fn assign_proposal(
        self: &Arc<Self>,
        id: ProposalId,
        label: &str,
        archive: bool,
    ) -> Result<u32, ApplicationError> {
        let proposal = self.pending(id).await?;
        let key = label_key(label);
        let group = self.group_mailboxes(proposal.scope).await?;
        let known = group
            .values()
            .any(|boxes| label_mailbox(boxes, &key).is_some())
            || self
                .store
                .positives(proposal.scope)
                .await?
                .iter()
                .any(|(l, _)| *l == key);
        if !known {
            return Err(ApplicationError::NotFound("Label", label.trim().to_owned()));
        }
        let name = display_name(&group, &key);
        self.label_with(proposal, name, archive).await
    }

    /// Every label of the account's scope (of every scope without one) with
    /// its settings and how many examples it has.
    pub async fn list_labels(
        &self,
        account: Option<AccountId>,
    ) -> Result<Vec<LabelScope>, ApplicationError> {
        let only = match account {
            Some(a) => Some(self.store.scope_of(a).await?),
            None => None,
        };
        let names = self.scope_names().await?;
        let mut out = Vec::new();
        for scope in self.store.scopes().await? {
            if only.is_some_and(|o| o != scope) {
                continue;
            }
            out.push(self.label_scope(scope, &names).await?);
        }
        Ok(out)
    }

    async fn label_scope(
        &self,
        scope: Scope,
        names: &HashMap<Scope, String>,
    ) -> Result<LabelScope, ApplicationError> {
        let group = self.group_mailboxes(scope).await?;
        let settings = self.store.settings(scope).await?;
        let mut labels: Vec<LabelEntry> = self
            .store
            .positives(scope)
            .await?
            .into_iter()
            .map(|(label, positives)| {
                let settings = settings
                    .iter()
                    .find(|s| s.label == label)
                    .cloned()
                    .unwrap_or_else(|| mc_domain::LabelSettings::off(&label));
                LabelEntry {
                    name: display_name(&group, &label),
                    status: LabelStatus {
                        settings,
                        positives,
                    },
                }
            })
            .collect();
        labels.sort_by(|a, b| {
            b.status
                .positives
                .cmp(&a.status.positives)
                .then_with(|| a.name.cmp(&b.name))
        });
        Ok(LabelScope {
            scope: scope.key(),
            group: names.get(&scope).cloned().unwrap_or_default(),
            accounts: self.store.scope_accounts(scope).await?,
            labels,
        })
    }
}
