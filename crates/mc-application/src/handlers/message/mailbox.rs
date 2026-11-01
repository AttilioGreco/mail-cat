//! Deleting a user mailbox (first emptying it into another one) and renaming one.

use mc_domain::PendingRename;
use mc_shared::pagination::Pagination;

use super::labels::bulk_counts;
use super::*;
use crate::services::mailbox_rename::{self, display_delimiter, is_below, rename_events};

/// The read model's largest page.
const BATCH: u32 = 200;

impl<
    ES: EventStore + 'static,
    PR: ProviderRegistry + 'static,
    MR: MessageReadRepository
        + ReadModelProjector
        + MailboxReadRepository
        + MessageBodyStore
        + PendingRenameStore
        + 'static,
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

    /// Rename a Custom mailbox on the server, then here: the mailbox and every
    /// mailbox below it get a `MailboxRenamed`, with the path and spelling the
    /// server lists afterwards, so the next sync finds them where it expects
    /// them instead of seeing mailboxes gone and new ones. Messages keep their
    /// mailbox ids, and a sync cursor is keyed by that id, so nothing is read
    /// again here; a server that changes the mailbox's UIDVALIDITY makes the
    /// next sync start that mailbox over by the usual rule.
    ///
    /// The account's sync slot is held throughout, and the rename is written
    /// down before the server is asked: a crash in between is settled by the
    /// next sync (or rename) from what the server lists.
    pub async fn handle_rename_mailbox(
        &self,
        mailbox_id: MailboxId,
        new_name: &str,
    ) -> Result<CommandResult, ApplicationError> {
        let mailbox = self.mailbox(mailbox_id).await?;
        if mailbox.mailbox_type != MailboxType::Custom
            || mailbox.path.full_path().eq_ignore_ascii_case("INBOX")
        {
            return Err(ApplicationError::PreconditionFailed(format!(
                "{} is a system mailbox and cannot be renamed",
                mailbox.display_path
            )));
        }
        let new_name = new_name
            .split('/')
            .map(str::trim)
            .collect::<Vec<_>>()
            .join("/");
        if new_name.split('/').any(str::is_empty) {
            return Err(ApplicationError::PreconditionFailed(
                "mailbox name is empty or has an empty level".into(),
            ));
        }
        let _no_sync = self.sync_locks.lock(&mailbox.account_id).await;
        let _turn = self.turn(mailbox.account_id).await;
        // An earlier rename may have gone through on the server only.
        self.settle_renames(&mailbox.account_id).await?;
        let mailbox = self.mailbox(mailbox_id).await?;

        let delimiter = display_delimiter(&mailbox);
        let own = mailbox.display_path.clone();
        let new_display = new_name.replace('/', &delimiter.to_string());
        if own == new_display {
            return Ok(CommandResult::MailboxRenamed { renamed: 0 });
        }
        let known = self
            .message_read_repo
            .list_mailboxes(&mailbox.account_id)
            .await?;
        let wanted = new_display.to_lowercase();
        let below = format!("{wanted}{delimiter}");
        let taken = known.iter().any(|m| {
            let shown = m.display_path.to_lowercase();
            m.id != mailbox.id
                && !is_below(&m.display_path, &own, delimiter)
                && (shown == wanted || shown.starts_with(&below))
        });
        if taken || wanted.starts_with(&format!("{}{delimiter}", own.to_lowercase())) {
            return Err(ApplicationError::PreconditionFailed(format!(
                "{new_name:?} is taken, or inside {own:?} itself"
            )));
        }

        let provider = self.provider_registry.get_provider(&mailbox.account_id)?;
        self.message_read_repo
            .put_pending_rename(&PendingRename {
                mailbox_id,
                account_id: mailbox.account_id,
                old_display: own.clone(),
                new_display,
            })
            .await?;
        let new_root = provider.rename_mailbox(&mailbox.path, &new_name).await?;
        let listed = provider.list_mailboxes().await?;
        // Servers that ignore case in names may list another spelling.
        let root = listed.iter().find(|l| l.path == new_root).or_else(|| {
            listed.iter().find(|l| {
                !new_root.is_opaque()
                    && l.path
                        .full_path()
                        .eq_ignore_ascii_case(&new_root.full_path())
            })
        });
        let Some(root) = root else {
            return Err(ApplicationError::PreconditionFailed(format!(
                "{own:?} was renamed on the server but is not listed there yet: the next sync records it"
            )));
        };
        let renames = rename_events(&known, &own, mailbox_id, root, &listed)?;
        let renamed = u32::try_from(renames.events.len()).unwrap_or(u32::MAX);
        self.append_renames(renames.events).await?;
        // Mailboxes not listed are left to the next sync to settle.
        if renames.missing == 0 {
            self.message_read_repo
                .clear_pending_rename(&mailbox_id)
                .await?;
        }
        info!(%mailbox_id, renamed, "mailbox renamed");
        Ok(CommandResult::MailboxRenamed { renamed })
    }

    async fn mailbox(&self, id: MailboxId) -> Result<Mailbox, ApplicationError> {
        self.message_read_repo
            .get_mailbox(&id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Mailbox", id.to_string()))
    }

    /// Record the renames earlier commands sent to the server and never
    /// wrote down, as the server lists them now.
    async fn settle_renames(&self, account_id: &AccountId) -> Result<(), ApplicationError> {
        let provider = self.provider_registry.get_provider(account_id)?;
        let settled =
            mailbox_rename::settle(&*self.message_read_repo, &*provider, account_id).await?;
        for (pending, events) in settled {
            self.append_renames(events).await?;
            self.message_read_repo
                .clear_pending_rename(&pending)
                .await?;
        }
        Ok(())
    }

    async fn append_renames(
        &self,
        events: Vec<(MailboxId, DomainEvent)>,
    ) -> Result<(), ApplicationError> {
        for (id, event) in events {
            let seq = self.event_store.current_sequence(&id.to_string()).await?;
            self.append_and_publish(&id, AggregateType::Mailbox, seq, event)
                .await?;
        }
        Ok(())
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
