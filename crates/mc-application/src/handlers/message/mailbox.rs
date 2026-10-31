//! Deleting a user mailbox: first emptying it into another one.

use mc_shared::pagination::Pagination;

use super::labels::bulk_counts;
use super::*;

/// The read model's largest page.
const BATCH: u32 = 200;

impl<
    ES: EventStore + 'static,
    PR: ProviderRegistry + 'static,
    MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore + 'static,
    B: EventPublisher + 'static,
> MessageCommandHandler<ES, PR, MR, B>
{
    /// Delete a Custom mailbox on the server and here. Its messages are first
    /// moved to `move_to` (a delete to Trash when that is the Trash), through
    /// the same paths as any move; the first batch that does not go through
    /// stops everything, and the mailbox is deleted only once it is found
    /// empty again. A system mailbox is never deleted, nor one with messages
    /// and nowhere to put them.
    pub async fn handle_delete_mailbox(
        &self,
        mailbox_id: MailboxId,
        move_to: Option<MailboxId>,
    ) -> Result<CommandResult, ApplicationError> {
        let mailbox = self
            .message_read_repo
            .get_mailbox(&mailbox_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Mailbox", mailbox_id.to_string()))?;
        if mailbox.mailbox_type != MailboxType::Custom
            || mailbox.path.full_path().eq_ignore_ascii_case("INBOX")
        {
            return Err(ApplicationError::PreconditionFailed(format!(
                "{} is a system mailbox and cannot be deleted",
                mailbox.display_path
            )));
        }

        let held = self.held_in(mailbox_id).await?;
        let mut moved = 0;
        if held > 0 {
            let to = move_to.ok_or_else(|| {
                ApplicationError::PreconditionFailed(format!(
                    "{} holds {held} messages: say where to move them",
                    mailbox.display_path
                ))
            })?;
            moved = self.empty_into(&mailbox, to, held).await?;
        }

        let _turn = self.turn(mailbox.account_id).await;
        self.provider_registry
            .get_provider(&mailbox.account_id)?
            .delete_mailbox(&mailbox.path)
            .await?;
        let seq = self
            .event_store
            .current_sequence(&mailbox_id.to_string())
            .await?;
        self.append_and_publish(&mailbox_id, AggregateType::Mailbox, seq, mailbox.delete())
            .await?;
        info!(%mailbox_id, moved, "mailbox deleted");
        Ok(CommandResult::MailboxDeleted { moved })
    }

    async fn held_in(&self, mailbox_id: MailboxId) -> Result<u64, ApplicationError> {
        Ok(self
            .message_read_repo
            .list_messages(&mailbox_id, &Pagination::new(1, 1))
            .await?
            .total_count)
    }

    /// Move everything `source` holds to `to_id`; how many moved.
    async fn empty_into(
        &self,
        source: &Mailbox,
        to_id: MailboxId,
        held: u64,
    ) -> Result<u32, ApplicationError> {
        let to = self
            .message_read_repo
            .get_mailbox(&to_id)
            .await?
            .filter(|to| to.account_id == source.account_id && to.id != source.id)
            .ok_or_else(|| {
                ApplicationError::PreconditionFailed(
                    "the destination must be another mailbox of the same account".into(),
                )
            })?;
        let stopped = |moved: u32, why: String| {
            ApplicationError::PreconditionFailed(format!(
                "{why} after moving {moved} of {held} messages out of {}: mailbox kept",
                source.display_path
            ))
        };

        let mut moved = 0;
        // Each pass takes a page off the front; bounded in case a batch is
        // reported done yet still listed.
        for _ in 0..=held.div_ceil(u64::from(BATCH)) {
            let page = self
                .message_read_repo
                .list_messages(&source.id, &Pagination::new(1, BATCH))
                .await?;
            if page.items.is_empty() {
                return Ok(moved);
            }
            let messages = page.items.iter().map(|m| (m.id, source.id)).collect();
            let result = if to.mailbox_type == MailboxType::Trash {
                self.handle_delete_many(messages, false).await
            } else {
                self.handle_move_many(messages, to.id, to.path.clone())
                    .await
            };
            let (done, failed) = bulk_counts(result.map_err(|e| stopped(moved, e.to_string()))?);
            moved += done;
            if failed > 0 {
                return Err(stopped(moved, format!("{failed} messages not moved")));
            }
        }
        match self.held_in(source.id).await? {
            0 => Ok(moved),
            left => Err(stopped(moved, format!("{left} messages still listed"))),
        }
    }
}
