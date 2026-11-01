//! Undoing an accepted proposal: its label comes off the mail it was put on,
//! archived mail returns to the Inbox, the model forgets those examples, and
//! the labels the acceptance created are deleted when nothing is left in them.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::{info, warn};

use mc_domain::{
    AccountId, AutoApplied, MailboxReadRepository, MessageId, Proposal, ProposalId, ProposalStatus,
    TagSource, TaggingStore,
};

use super::SCAN_PAGE;
use super::plan::AccountCount;
use crate::bus::CommandBus;
use crate::commands::Command;
use crate::error::ApplicationError;
use crate::services::tagging::retro::{BATCH, BATCH_PAUSE};
use crate::services::tagging::stats::ApplyKind;
use crate::services::tagging::{TaggingService, inbox_of, label_key, label_mailbox};

#[derive(Debug, Clone)]
pub struct UndoReport {
    pub dry_run: bool,
    pub to_unlabel: u32,
    /// Of those, how many return to the Inbox.
    pub to_unarchive: u32,
    pub accounts: Vec<AccountCount>,
    /// Labels this acceptance created that would be empty afterwards.
    pub labels_to_delete: Vec<String>,
    /// What the proposal becomes.
    pub status: ProposalStatus,
}

/// A message still carrying the label the acceptance put on it.
struct Undo {
    source: TagSource,
    archived: bool,
}

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    /// Without `dry_run`, the work is queued like an apply (throttled, shown
    /// in the background stats) and the proposal changes status once it is
    /// done. `reject`: it becomes rejected instead of pending again.
    pub async fn undo_proposal(
        self: &Arc<Self>,
        id: ProposalId,
        reject: bool,
        dry_run: bool,
    ) -> Result<UndoReport, ApplicationError> {
        let proposal = self
            .store
            .proposal(id)
            .await?
            .filter(|p| p.status == ProposalStatus::Accepted)
            .ok_or_else(|| {
                ApplicationError::PreconditionFailed(format!(
                    "{} is not an accepted proposal",
                    id.as_uuid()
                ))
            })?;
        let label = label_key(&proposal.name);
        if self.is_applying(&label) {
            return Err(ApplicationError::PreconditionFailed(format!(
                "{label:?} is still being applied or undone: wait for it to finish"
            )));
        }
        let (work, stale) = self.undo_work(id).await?;
        let mut per_account: HashMap<AccountId, AccountCount> = HashMap::new();
        for u in &work {
            let n = per_account
                .entry(u.source.account_id)
                .or_insert(AccountCount {
                    account: u.source.account_id,
                    messages: 0,
                    archived: 0,
                });
            n.messages += 1;
            n.archived += u32::from(u.archived);
        }
        let mut accounts: Vec<_> = per_account.into_values().collect();
        accounts.sort_by_key(|a| *a.account.as_uuid());
        let report = UndoReport {
            dry_run,
            to_unlabel: work.len() as u32,
            to_unarchive: work.iter().filter(|u| u.archived).count() as u32,
            labels_to_delete: self.deletable(&proposal, &accounts).await?,
            accounts,
            status: if reject {
                ProposalStatus::Rejected
            } else {
                ProposalStatus::Pending
            },
        };
        if dry_run {
            return Ok(report);
        }
        let job = self.begin_job(&label, ApplyKind::Undo, work.len() as u64);
        let service = Arc::clone(self);
        let status = report.status;
        tokio::spawn(async move {
            let _turn = service.apply_turn.lock().await;
            match service.take_off(&proposal, &label, work, stale, job).await {
                Ok(()) => service.finish_undo(proposal, status).await,
                Err(e) => warn!(label, error = %e, "undoing an acceptance stopped"),
            }
            service.end_job(job);
        });
        Ok(report)
    }

    /// The records the acceptance made, split into those whose message still
    /// carries the label and the rest (the user took it off already).
    async fn undo_work(
        &self,
        id: ProposalId,
    ) -> Result<(Vec<Undo>, Vec<AutoApplied>), ApplicationError> {
        let records: HashMap<MessageId, AutoApplied> = self
            .store
            .proposal_autos(id)
            .await?
            .into_iter()
            .filter(|r| r.confirmed)
            .map(|r| (r.message_id, r))
            .collect();
        let ids: Vec<MessageId> = records.keys().copied().collect();
        let mut work = Vec::new();
        let mut carrying = std::collections::HashSet::new();
        for page in ids.chunks(SCAN_PAGE) {
            for source in self.store.sources(page).await? {
                let record = &records[&source.id];
                if source.labels.contains(&record.label) {
                    carrying.insert(source.id);
                    work.push(Undo {
                        archived: record.archived,
                        source,
                    });
                }
            }
        }
        // Oldest first, so the order does not depend on the hash map.
        work.sort_by_key(|u| *u.source.id.as_uuid());
        let stale = records
            .into_values()
            .filter(|r| !carrying.contains(&r.message_id))
            .collect();
        Ok((work, stale))
    }

    /// Created labels that the undo would leave empty.
    async fn deletable(
        &self,
        proposal: &Proposal,
        accounts: &[AccountCount],
    ) -> Result<Vec<String>, ApplicationError> {
        let mut out = Vec::new();
        for id in &proposal.created {
            let Some(mailbox) = self.read.get_mailbox(id).await? else {
                continue;
            };
            let removed = accounts
                .iter()
                .find(|a| a.account == mailbox.account_id)
                .map_or(0, |a| a.messages);
            if mailbox.total_messages <= removed {
                out.push(mailbox.display_path);
            }
        }
        Ok(out)
    }

    /// The records go first: a message that is not ours any more must not be
    /// taken for a label the user removed (which would teach a negative). A
    /// failed change gives them back.
    async fn take_off(
        &self,
        proposal: &Proposal,
        label: &str,
        work: Vec<Undo>,
        stale: Vec<AutoApplied>,
        job: u64,
    ) -> Result<(), ApplicationError> {
        for record in &stale {
            self.store.take_auto(record.message_id, label).await?;
        }
        let mut groups: HashMap<(AccountId, bool), Vec<Undo>> = HashMap::new();
        for u in work {
            groups
                .entry((u.source.account_id, u.archived))
                .or_default()
                .push(u);
        }
        for ((account, archived), group) in groups {
            let mailboxes = self.read.list_mailboxes(&account).await?;
            let inbox = inbox_of(&mailboxes).filter(|_| archived);
            let scope = self.store.scope_of(account).await?;
            for chunk in group.chunks(BATCH) {
                let mut taken = Vec::new();
                for u in chunk {
                    if let Some(record) = self
                        .store
                        .autos(u.source.id)
                        .await?
                        .into_iter()
                        .find(|r| r.label == label)
                    {
                        self.store.take_auto(u.source.id, label).await?;
                        taken.push(record);
                    }
                }
                if let Err(e) = self.unlabel(chunk, label, inbox, &mailboxes).await {
                    for record in &taken {
                        let _ = self.store.record_auto(record).await;
                    }
                    return Err(e);
                }
                for u in chunk {
                    let mut now = u.source.clone();
                    now.labels.retain(|l| l != label);
                    if let Err(e) = self.retrain(&now, scope, None, false).await {
                        warn!(error = %e, "label classifier: could not withdraw an example");
                    }
                }
                let moved = if archived { chunk.len() } else { 0 };
                self.advance_job(job, chunk.len() as u64, moved as u64);
                tokio::time::sleep(BATCH_PAUSE).await;
            }
        }
        info!(label, proposal = %proposal.id.as_uuid(), "acceptance undone");
        Ok(())
    }

    async fn unlabel(
        &self,
        chunk: &[Undo],
        label: &str,
        inbox: Option<&mc_domain::Mailbox>,
        mailboxes: &[mc_domain::Mailbox],
    ) -> Result<(), ApplicationError> {
        if chunk[0].source.keywords {
            for u in chunk {
                self.drop_keyword(&u.source, label, inbox).await?;
            }
            return Ok(());
        }
        // Without the mailbox there is nothing to take off.
        let Some(target) = label_mailbox(mailboxes, label) else {
            return Ok(());
        };
        self.change_labels(
            chunk.iter().map(|u| u.source.id).collect(),
            inbox.map(|m| m.id).into_iter().collect(),
            vec![target.id],
        )
        .await
    }

    /// Delete the labels the acceptance created if empty (the command refuses
    /// a mailbox that still holds mail), then reopen the proposal.
    async fn finish_undo(&self, proposal: Proposal, status: ProposalStatus) {
        for &mailbox_id in &proposal.created {
            let deleted = self
                .commands
                .dispatch(Command::DeleteMailbox {
                    mailbox_id,
                    move_to: None,
                })
                .await;
            if let Err(e) = deleted {
                info!(%mailbox_id, error = %e, "created label kept");
            }
        }
        let reopened = Proposal {
            status,
            created: Vec::new(),
            ..proposal
        };
        if let Err(e) = self.store.put_proposal(&reopened).await {
            warn!(error = %e, "could not reopen the proposal");
        }
    }
}
