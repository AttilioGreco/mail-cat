//! Doing a planned label change: the mail and mailboxes first, then the
//! classifier's side.

use super::*;
use crate::commands::Command;
use crate::services::tagging::placement::keyword_of;
use crate::services::tagging::retro::{BATCH, BATCH_PAUSE};
use mc_domain::{AutoApplied, ProposalStatus};

/// An auto-apply record taken off its label, and the label it moved to.
type Taken = (AutoApplied, Option<String>);

/// The renames that concern `key`, and with `subtree` everything below it.
fn pairs_of<'a>(
    pairs: &'a [(String, Option<String>)],
    key: &'a str,
    subtree: bool,
) -> impl Iterator<Item = &'a (String, Option<String>)> {
    pairs
        .iter()
        .filter(move |(k, _)| k == key || (subtree && within(k, key)))
}

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    pub(super) async fn run_change(
        &self,
        change: &Change,
        job: u64,
    ) -> Result<(), ApplicationError> {
        let mut moves = HashMap::new();
        for work in &change.works {
            self.change_account(change, work, job, &mut moves).await?;
        }
        self.settle(change, &moves).await
    }

    /// `moves` gets the mailboxes that went away and, if this change made the
    /// label that took their place, that label's mailbox.
    async fn change_account(
        &self,
        change: &Change,
        work: &AccountWork,
        job: u64,
        moves: &mut HashMap<MailboxId, Option<MailboxId>>,
    ) -> Result<(), ApplicationError> {
        let pairs = change.renamed();
        if work.folders {
            self.recolour(change, work, &pairs, job).await?;
            // A folder goes with its mail in one command, so children first.
            for step in work.steps.iter().rev() {
                if let Some(made) = self.folder_step(work, step, job).await? {
                    moves.insert(step.from.id, made);
                }
            }
            return Ok(());
        }
        let refuge = match change.into {
            None => self.refuge(work.account).await?,
            Some(_) => None,
        };
        let mut made = Vec::with_capacity(work.steps.len());
        for step in &work.steps {
            made.push(self.label_step(work, step, &pairs, refuge, job).await?);
        }
        for (step, made) in work.steps.iter().zip(made).rev() {
            if step.deletes {
                self.delete_mailbox(step.from.id, None).await?;
                moves.insert(step.from.id, made);
            }
        }
        Ok(())
    }

    /// Returns the label this step made for the mail, if it did.
    async fn label_step(
        &self,
        work: &AccountWork,
        step: &Step,
        pairs: &[(String, Option<String>)],
        refuge: Option<MailboxId>,
        job: u64,
    ) -> Result<Option<MailboxId>, ApplicationError> {
        let key = step.key();
        if let (StepKind::Rename, Some((_, name))) = (step.how, &step.into) {
            let pairs: Vec<_> = pairs_of(pairs, &key, true).cloned().collect();
            let ids: Vec<_> = work
                .held
                .iter()
                .filter(|h| h.within(&key))
                .map(|h| h.id)
                .collect();
            let mut taken = Vec::new();
            for chunk in ids.chunks(BATCH) {
                taken.extend(self.rekey(chunk, &pairs).await?);
            }
            if let Err(e) = self.rename_mailbox(step.from.id, name).await {
                self.restore(&taken).await;
                return Err(e);
            }
            return Ok(None);
        }
        let (destination, made) = match (&step.into, step.target) {
            (None, _) => (None, None),
            (Some(_), Some(target)) => (Some(target), None),
            (Some((_, name)), None) => {
                let made = self.create_label(work.account, name).await?;
                (Some(made), Some(made))
            }
        };
        let pairs: Vec<_> = pairs_of(pairs, &key, false).cloned().collect();
        let mine: Vec<_> = work.held.iter().filter(|h| h.has(&key)).collect();
        for chunk in mine.chunks(BATCH) {
            let ids: Vec<_> = chunk.iter().map(|h| h.id).collect();
            let taken = self.rekey(&ids, &pairs).await?;
            if let Err(e) = self
                .relabel_chunk(chunk, step.from.id, destination, refuge)
                .await
            {
                self.restore(&taken).await;
                return Err(e);
            }
            self.advance_job(job, chunk.len() as u64, 0);
            tokio::time::sleep(BATCH_PAUSE).await;
        }
        Ok(made)
    }

    /// A folder of a folders account: renamed, or deleted with its mail
    /// moved to the new folder. `Some(made)` when it went away.
    async fn folder_step(
        &self,
        work: &AccountWork,
        step: &Step,
        job: u64,
    ) -> Result<Option<Option<MailboxId>>, ApplicationError> {
        let Some((_, name)) = &step.into else {
            return Ok(None);
        };
        if step.how == StepKind::Rename {
            self.rename_mailbox(step.from.id, name).await?;
            return Ok(None);
        }
        let (destination, made) = match step.target {
            Some(target) => (target, None),
            None => {
                let made = self.create_label(work.account, name).await?;
                (made, Some(made))
            }
        };
        self.delete_mailbox(step.from.id, Some(destination)).await?;
        self.advance_job(job, u64::from(step.from.total_messages), 0);
        Ok(Some(made))
    }

    async fn rename_mailbox(
        &self,
        mailbox_id: MailboxId,
        new_name: &str,
    ) -> Result<(), ApplicationError> {
        self.commands
            .dispatch(Command::RenameMailbox {
                mailbox_id,
                new_name: new_name.to_owned(),
            })
            .await
            .map(drop)
    }

    /// Swap the keyword on each message of a folders account (drop it when
    /// deleting).
    async fn recolour(
        &self,
        change: &Change,
        work: &AccountWork,
        pairs: &[(String, Option<String>)],
        job: u64,
    ) -> Result<(), ApplicationError> {
        for chunk in work.held.chunks(BATCH) {
            let ids: Vec<_> = chunk.iter().map(|h| h.id).collect();
            let taken = self.rekey(&ids, pairs).await?;
            for held in chunk {
                let set = self.commands.dispatch(Command::SetMessageFlags {
                    message_id: held.id,
                    flags_to_add: held
                        .labels
                        .iter()
                        .filter_map(|(key, _)| change.rewrite(key))
                        .map(|key| Flag::Custom(keyword_of(&key)))
                        .collect(),
                    flags_to_remove: held.labels.iter().flat_map(|(_, f)| f.clone()).collect(),
                });
                if let Err(e) = set.await {
                    self.restore(&taken).await;
                    return Err(e);
                }
            }
            self.advance_job(job, chunk.len() as u64, 0);
            tokio::time::sleep(BATCH_PAUSE).await;
        }
        Ok(())
    }

    /// Put `dest` on the chunk and take the old label off, keeping every other
    /// label and the Inbox. Without `dest` (deleting), a message that would be
    /// left in no mailbox goes to `refuge`.
    async fn relabel_chunk(
        &self,
        chunk: &[&Held],
        from: MailboxId,
        dest: Option<MailboxId>,
        refuge: Option<MailboxId>,
    ) -> Result<(), ApplicationError> {
        let ids = |held: &[&Held]| held.iter().map(|h| h.id).collect::<Vec<_>>();
        if let Some(dest) = dest {
            return self.change_labels(ids(chunk), vec![dest], vec![from]).await;
        }
        let (alone, shared): (Vec<_>, Vec<_>) = chunk.iter().partition(|h| h.mailboxes <= 1);
        if !shared.is_empty() {
            self.change_labels(ids(&shared), vec![], vec![from]).await?;
        }
        if alone.is_empty() {
            return Ok(());
        }
        let refuge = refuge.ok_or_else(|| {
            ApplicationError::PreconditionFailed(format!(
                "{} messages are in this label only and the account has no Archive or Inbox to keep them in",
                alone.len()
            ))
        })?;
        self.change_labels(ids(&alone), vec![refuge], vec![from])
            .await
    }

    async fn refuge(&self, account: AccountId) -> Result<Option<MailboxId>, ApplicationError> {
        let boxes = self.read.list_mailboxes(&account).await?;
        Ok([MailboxType::Archive, MailboxType::All, MailboxType::Inbox]
            .iter()
            .find_map(|t| boxes.iter().find(|m| m.mailbox_type == *t))
            .map(|m| m.id))
    }

    async fn delete_mailbox(
        &self,
        mailbox_id: MailboxId,
        move_to: Option<MailboxId>,
    ) -> Result<(), ApplicationError> {
        self.commands
            .dispatch(Command::DeleteMailbox {
                mailbox_id,
                move_to,
            })
            .await
            .map(drop)
            .map_err(|e| match e {
                // The mailbox's own refusal ("say where to move them") would
                // send the user to a flag this command does not have.
                ApplicationError::PreconditionFailed(why) => ApplicationError::PreconditionFailed(
                    format!("the label's mailbox was not deleted: {why}; mail the job could not move off it is still there, run the command again"),
                ),
                other => other,
            })
    }

    /// Move the auto-apply records of the messages to the new labels before
    /// the change: a record whose label the message lost would be taken for a
    /// label the user removed, and undoing the acceptance would lose track of
    /// the mail. Returns what was taken, to give back if the change fails.
    async fn rekey(
        &self,
        ids: &[MessageId],
        pairs: &[(String, Option<String>)],
    ) -> Result<Vec<Taken>, ApplicationError> {
        let mut taken = Vec::new();
        for &id in ids {
            for record in self.store.autos(id).await? {
                let Some((_, new)) = pairs.iter().find(|(k, _)| *k == record.label) else {
                    continue;
                };
                self.store.take_auto(id, &record.label).await?;
                if let Some(label) = new {
                    let renamed = AutoApplied {
                        label: label.clone(),
                        ..record.clone()
                    };
                    self.store.record_auto(&renamed).await?;
                }
                taken.push((record, new.clone()));
            }
        }
        Ok(taken)
    }

    async fn restore(&self, taken: &[Taken]) {
        for (record, new) in taken {
            if let Some(label) = new {
                let _ = self.store.take_auto(record.message_id, label).await;
            }
            let _ = self.store.record_auto(record).await;
        }
    }

    /// The classifier's side, once the mail is done: settings and rejections
    /// follow the labels, and accepted proposals keep pointing at what undoing
    /// them should touch. `moves`: the mailboxes that went away, and the one
    /// this change created in their place; an acceptance's `created` entry
    /// follows only that one, so an undo never deletes a label that was there
    /// before. A renamed mailbox keeps its id, so those entries stay.
    async fn settle(
        &self,
        change: &Change,
        moves: &HashMap<MailboxId, Option<MailboxId>>,
    ) -> Result<(), ApplicationError> {
        let key = &change.label.0;
        for (old, new) in change.renamed() {
            self.store
                .move_label_rows(change.scope, &old, new.as_deref())
                .await?;
        }
        let accepted = self.store.proposals(ProposalStatus::Accepted).await?;
        for before in accepted.into_iter().filter(|p| p.scope == change.scope) {
            let mut p = before.clone();
            p.created = before
                .created
                .iter()
                .filter_map(|id| moves.get(id).map_or(Some(*id), |new| *new))
                .collect();
            let named = label_key(&p.name);
            match &change.into {
                Some((_, name)) if within(&named, key) && (!change.mail_only || named == *key) => {
                    let depth = key.split('/').count();
                    let tail = p.name.split('/').skip(depth);
                    p.name = std::iter::once(name.as_str())
                        .chain(tail)
                        .collect::<Vec<_>>()
                        .join("/");
                }
                None if named == *key => {
                    for record in self.store.proposal_autos(p.id).await? {
                        if record.label == *key {
                            self.store.take_auto(record.message_id, key).await?;
                        }
                    }
                    p.status = ProposalStatus::Rejected;
                    p.created.clear();
                }
                _ => {}
            }
            if p != before {
                self.store.put_proposal(&p).await?;
            }
        }
        self.forget_pools();
        Ok(())
    }
}
