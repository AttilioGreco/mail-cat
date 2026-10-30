//! Moving and deleting messages on the server and recording the result.

use super::*;

impl<
    ES: EventStore + 'static,
    PR: ProviderRegistry + 'static,
    MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore + 'static,
    B: EventPublisher + 'static,
> MessageCommandHandler<ES, PR, MR, B>
{
    /// Move the message out of `from_mailbox_id`, which must still hold it
    /// when the account's turn comes: an action queued ahead may have moved
    /// it, and acting from another of its mailboxes instead would not be what
    /// was asked. One already in `to_mailbox_id` stays put.
    pub async fn handle_move(
        &self,
        message_id: MessageId,
        from_mailbox_id: MailboxId,
        to_mailbox_id: MailboxId,
        to_mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        self.apply_one(
            message_id,
            from_mailbox_id,
            to_mailbox(to_mailbox_id, to_mailbox_path),
        )
        .await?;
        info!(%message_id, "message moved");
        Ok(CommandResult::MessageMoved)
    }

    /// Standard client behaviour: a plain delete moves the message to the
    /// Trash folder (Thunderbird-style). Only a permanent delete, a delete of
    /// a message already in Trash, or the absence of a Trash folder actually
    /// expunges it. Like a move, it acts from `mailbox_id` only: a double
    /// click finds the message already in Trash, no longer in the mailbox it
    /// was asked from, and fails instead of purging it.
    pub async fn handle_delete(
        &self,
        message_id: MessageId,
        mailbox_id: MailboxId,
        permanently: bool,
    ) -> Result<CommandResult, ApplicationError> {
        self.apply_one(message_id, mailbox_id, to_trash_or_expunged(permanently))
            .await?;
        info!(%message_id, "message deleted");
        Ok(CommandResult::MessageDeleted)
    }

    /// One message's move/delete from `source_id`; `destination` as in
    /// [`Self::apply_many`]. A message (or its mailbox) gone altogether by its
    /// turn is nothing left to do; one still around but no longer in
    /// `source_id` is an error.
    async fn apply_one(
        &self,
        message_id: MessageId,
        source_id: MailboxId,
        destination: impl Fn(&Mailbox, &[Mailbox]) -> Destination,
    ) -> Result<(), ApplicationError> {
        let account_id = match self.load_message(&message_id).await {
            Ok((message, _)) => message.account_id,
            Err(ApplicationError::NotFound(..)) => return Ok(()),
            Err(e) => return Err(e),
        };
        let _turn = self.turn(account_id).await;
        let (mut message, mut seq) = match self.load_message(&message_id).await {
            Err(ApplicationError::NotFound(..)) => return Ok(()),
            loaded => loaded?,
        };
        require_membership(&message, source_id)?;
        let mailboxes = self.message_read_repo.list_mailboxes(&account_id).await?;
        let Some(source) = mailboxes.iter().find(|m| m.id == source_id) else {
            return Ok(());
        };
        let Some(dest) = destination(source, &mailboxes) else {
            return Ok(());
        };
        let remote_id = self.remote_id(&mut message, &mut seq, &source.path).await?;
        let dest = dest.as_ref().map(|(id, path)| (*id, path));
        match self
            .apply_on_server(account_id, source, dest, vec![(message_id, remote_id)])
            .await
        {
            (_, Some(refused)) => Err(refused),
            _ => Ok(()),
        }
    }

    /// Bulk delete. Applies the same Trash-vs-expunge rules as [`Self::handle_delete`]
    /// per message, grouped by source mailbox: one provider call per group
    /// (per [`SERVER_CHUNK`]). The per-message path would open a connection
    /// each, and deleting a search result set of 800 would mean 800 of them.
    ///
    /// Each message comes with the mailbox it is deleted from. One that can't
    /// be loaded, is not in that mailbox, has no remote id, or whose mailbox
    /// has vanished is counted in `failed` and skipped — a stale search index
    /// must not abort the rest of the batch halfway through. So is every
    /// message of a chunk the provider refused, and of the chunks after it.
    pub async fn handle_delete_many(
        &self,
        messages: Vec<(MessageId, MailboxId)>,
        permanently: bool,
    ) -> Result<CommandResult, ApplicationError> {
        self.apply_many(messages, to_trash_or_expunged(permanently))
            .await
    }

    /// Bulk move to a single destination, grouped like [`Self::handle_delete_many`].
    pub async fn handle_move_many(
        &self,
        messages: Vec<(MessageId, MailboxId)>,
        to_mailbox_id: MailboxId,
        to_mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        self.apply_many(messages, to_mailbox(to_mailbox_id, to_mailbox_path))
            .await
    }

    /// Bulk move/delete of (message, mailbox it is acted on from). `destination` picks,
    /// from a message's source mailbox (and its account's mailboxes), where it
    /// goes: `Some(None)` expunges, `None` skips it. The message must still be
    /// in that mailbox when its account's turn comes.
    async fn apply_many(
        &self,
        messages: Vec<(MessageId, MailboxId)>,
        destination: impl Fn(&Mailbox, &[Mailbox]) -> Destination,
    ) -> Result<CommandResult, ApplicationError> {
        let (mut failed, mut succeeded) = (0u32, 0u32);
        let mut by_account: HashMap<AccountId, Vec<(MessageId, MailboxId)>> = HashMap::new();
        for (message_id, source_id) in messages {
            match self.load_message(&message_id).await {
                Ok((message, _)) if message.membership(source_id).is_some() => {
                    by_account
                        .entry(message.account_id)
                        .or_default()
                        .push((message_id, source_id));
                }
                _ => failed += 1,
            }
        }

        type Group = (MailboxId, Option<(MailboxId, MailboxPath)>);
        for (account_id, ids) in by_account {
            let _turn = self.turn(account_id).await;
            let mailboxes = self.message_read_repo.list_mailboxes(&account_id).await?;
            let mut groups: HashMap<Group, Vec<(MessageId, RemoteId)>> = HashMap::new();
            for (message_id, source_id) in ids {
                // Gone while it waited for its turn: an earlier action took it.
                let Ok((mut message, mut seq)) = self.load_message(&message_id).await else {
                    continue;
                };
                let source = mailboxes.iter().find(|m| m.id == source_id);
                let Some(source) = source.filter(|_| message.membership(source_id).is_some())
                else {
                    failed += 1;
                    continue;
                };
                let Some(dest) = destination(source, &mailboxes) else {
                    continue;
                };
                let Ok(remote_id) = self.remote_id(&mut message, &mut seq, &source.path).await
                else {
                    failed += 1;
                    continue;
                };
                groups
                    .entry((source.id, dest))
                    .or_default()
                    .push((message_id, remote_id));
            }

            for ((from, dest), members) in groups {
                let Some(from) = mailboxes.iter().find(|m| m.id == from) else {
                    continue;
                };
                let count = members.len() as u32;
                let (done, refused) = self
                    .apply_on_server(
                        account_id,
                        from,
                        dest.as_ref().map(|(id, path)| (*id, path)),
                        members,
                    )
                    .await;
                if let Some(e) = refused {
                    warn!(%e, from = %from.path, applied = done, "bulk change refused by the server part-way");
                }
                succeeded += done;
                failed += count - done;
            }
        }

        info!(succeeded, failed, "bulk change applied");
        Ok(CommandResult::MessagesBulkChanged { succeeded, failed })
    }

    /// Move `members` (message, remote id) from `from` to `dest`, or expunge them
    /// when `dest` is `None`, on the server first, a chunk at a time, each
    /// chunk recorded here as soon as the server confirms it. Returns how
    /// many the server applied and, if it refused a chunk, why: that chunk
    /// and the ones after it changed nowhere.
    async fn apply_on_server(
        &self,
        account_id: AccountId,
        from: &Mailbox,
        dest: Option<(MailboxId, &MailboxPath)>,
        members: Vec<(MessageId, RemoteId)>,
    ) -> (u32, Option<ApplicationError>) {
        let provider = match self.provider_registry.get_provider(&account_id) {
            Ok(provider) => provider,
            Err(e) => return (0, Some(e.into())),
        };
        // An unsubscribed destination is never synced: the message simply
        // leaves here, as if expunged, instead of becoming a row of a mailbox
        // nobody keeps up to date.
        let kept_here = match dest {
            Some((to_id, _)) => self
                .message_read_repo
                .get_mailbox(&to_id)
                .await
                .ok()
                .flatten()
                .is_none_or(|to| to.subscribed),
            None => false,
        };
        let recorded_dest = dest.filter(|_| kept_here);
        let mut done = 0;
        let mut out_of_step = false;
        for chunk in members.chunks(SERVER_CHUNK) {
            let ids: Vec<RemoteId> = chunk.iter().map(|(_, id)| id.clone()).collect();
            let applied = match dest {
                Some((_, to)) => provider.move_messages(&ids, &from.path, to).await,
                None => provider
                    .delete_messages(&from.path, &ids, true)
                    .await
                    .map(|()| HashMap::new()),
            };
            let new_ids = match applied {
                Ok(new_ids) => new_ids,
                Err(e) => {
                    // A timeout or a dropped connection may come after the
                    // server applied it: let a sync tell.
                    self.request_resync(account_id, &from.path, dest.map(|(_, to)| to));
                    return (done, Some(e.into()));
                }
            };
            for (message_id, id) in chunk {
                let message_id = *message_id;
                if let Err(e) = self
                    .record_applied(
                        &provider,
                        message_id,
                        from.id,
                        recorded_dest,
                        new_ids.get(id).cloned().flatten(),
                    )
                    .await
                {
                    warn!(%message_id, %e, "done on the server but not recorded here, resyncing");
                    out_of_step = true;
                }
            }
            done += chunk.len() as u32;
        }
        if out_of_step {
            self.request_resync(account_id, &from.path, dest.map(|(_, to)| to));
        }
        (done, None)
    }

    /// Record a move or expunge the server confirmed. A moved message is
    /// recorded in `dest` only with evidence of its remote id there — reported by
    /// the server, else found by Message-ID. With neither, the server had
    /// nothing under that id any more (another client moved it) or the
    /// destination copy already belongs to another row: either way the real
    /// copy is, or will be, synced as a row of its own, so this one goes.
    /// `Err` means local state is out of step and needs a resync.
    async fn record_applied(
        &self,
        provider: &PR::Provider,
        message_id: MessageId,
        from: MailboxId,
        dest: Option<(MailboxId, &MailboxPath)>,
        reported_id: Option<RemoteId>,
    ) -> Result<(), ApplicationError> {
        let Some((to_id, to_path)) = dest else {
            return self
                .record(&message_id, |m| m.remove_from(from))
                .await
                .map(drop);
        };
        let new_id = match reported_id {
            Some(id) => Some(id),
            None => {
                let (message, _) = self.load_message(&message_id).await?;
                self.locate(provider, &message, &to_id, to_path).await.unwrap_or_else(|e| {
                    warn!(%message_id, %e, "moved, but its new remote id could not be looked up");
                    None
                })
            }
        };
        let Some(new_id) = new_id else {
            self.record(&message_id, |m| m.remove_from(from)).await?;
            return Err(ApplicationError::Internal(format!(
                "server confirmed the move to {to_path} without the new id"
            )));
        };
        // The destination already had that copy as a row of its own: a
        // Gmail label the message already carried, All Mail as the archive.
        // Recording the move too would make two rows of one server message.
        if self
            .message_read_repo
            .held_ids(&to_id, std::slice::from_ref(&new_id))
            .await?
            .contains(&new_id)
        {
            return self
                .record(&message_id, |m| m.remove_from(from))
                .await
                .map(drop);
        }
        self.record(&message_id, |m| {
            m.move_to(from, to_id, Some(new_id.clone()))
        })
        .await
        .map(drop)
    }

    pub(super) fn request_resync(
        &self,
        account_id: AccountId,
        from: &MailboxPath,
        to: Option<&MailboxPath>,
    ) {
        let Some(resync) = &self.resync else {
            return;
        };
        for path in std::iter::once(from).chain(to) {
            // Closed only on shutdown.
            let _ = resync.send((account_id, path.clone()));
        }
    }
}

/// Where a move/delete sends a message from its source mailbox (given the
/// account's mailboxes): `Some(Some(_))` a mailbox, `Some(None)` expunged,
/// `None` nowhere — it stays put.
type Destination = Option<Option<(MailboxId, MailboxPath)>>;

/// Moving a message to the folder it already sits in is a no-op, not a copy.
fn to_mailbox(id: MailboxId, path: MailboxPath) -> impl Fn(&Mailbox, &[Mailbox]) -> Destination {
    move |source, _| (source.id != id).then(|| Some((id, path.clone())))
}

/// Deleting from Trash expunges; from anywhere else it files into Trash, or
/// expunges when the account has none.
fn to_trash_or_expunged(permanently: bool) -> impl Fn(&Mailbox, &[Mailbox]) -> Destination {
    move |source, mailboxes| {
        if permanently || source.mailbox_type == MailboxType::Trash {
            return Some(None);
        }
        let trash = mailboxes
            .iter()
            .find(|m| m.mailbox_type == MailboxType::Trash);
        Some(trash.map(|t| (t.id, t.path.clone())))
    }
}
