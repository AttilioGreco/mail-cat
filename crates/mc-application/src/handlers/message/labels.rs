//! Labels: changing which mailboxes a message is in without moving it, and
//! archiving, whose meaning depends on the account's [`MailboxModel`].

use mc_domain::MailboxModel;

use super::*;

/// What one message is asked to gain and lose.
type Plan = (MessageId, Vec<MailboxId>, Vec<MailboxId>);

impl<
    ES: EventStore + 'static,
    PR: ProviderRegistry + 'static,
    MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore + 'static,
    B: EventPublisher + 'static,
> MessageCommandHandler<ES, PR, MR, B>
{
    /// Add and remove the same labels on every message. Refused up front,
    /// before anything reaches the server, when a message would be left in no
    /// mailbox: that is a delete, and has its own command.
    pub async fn handle_change_labels(
        &self,
        message_ids: Vec<MessageId>,
        add: Vec<MailboxId>,
        remove: Vec<MailboxId>,
    ) -> Result<CommandResult, ApplicationError> {
        let plans = message_ids
            .into_iter()
            .map(|id| (id, add.clone(), remove.clone()))
            .collect();
        self.apply_labels(plans, true).await
    }

    /// Take the messages out of the Inbox. Folders accounts file them in
    /// Archive (else All Mail), as a move. Labels accounts only drop the Inbox
    /// label, which keeps their other labels; a message whose Inbox was its
    /// only label is filed in the Archive mailbox, and left alone when the
    /// account has none (a message cannot be in no mailbox). Messages not in
    /// the Inbox, or with nowhere to go, count as `failed`.
    pub async fn handle_archive(
        &self,
        accounts: &[Account],
        message_ids: Vec<MessageId>,
    ) -> Result<CommandResult, ApplicationError> {
        let mut failed = 0;
        let mut plans: Vec<Plan> = Vec::new();
        let mut moves: HashMap<AccountId, Vec<(MessageId, MailboxId)>> = HashMap::new();
        let mut mailboxes: HashMap<AccountId, Vec<Mailbox>> = HashMap::new();
        for id in message_ids {
            let Some(account) = self
                .load_message(&id)
                .await
                .ok()
                .and_then(|(m, _)| accounts.iter().find(|a| a.id == m.account_id))
            else {
                failed += 1;
                continue;
            };
            if let std::collections::hash_map::Entry::Vacant(slot) = mailboxes.entry(account.id) {
                slot.insert(self.message_read_repo.list_mailboxes(&account.id).await?);
            }
            let boxes = &mailboxes[&account.id];
            let of_type = |t| boxes.iter().find(|m| m.mailbox_type == t);
            let (message, _) = self.load_message(&id).await?;
            let Some(inbox) =
                of_type(MailboxType::Inbox).filter(|i| message.membership(i.id).is_some())
            else {
                failed += 1;
                continue;
            };
            let archive = of_type(MailboxType::Archive);
            match account.mailbox_model {
                MailboxModel::Folders => match archive.or(of_type(MailboxType::All)) {
                    Some(_) => moves.entry(account.id).or_default().push((id, inbox.id)),
                    None => failed += 1,
                },
                MailboxModel::Labels if message.memberships.len() > 1 => {
                    plans.push((id, vec![], vec![inbox.id]));
                }
                MailboxModel::Labels => match archive {
                    Some(archive) => plans.push((id, vec![archive.id], vec![inbox.id])),
                    None => failed += 1,
                },
            }
        }

        let (mut succeeded, labelled) = bulk_counts(self.apply_labels(plans, false).await?);
        failed += labelled;
        for (account_id, messages) in moves {
            let boxes = &mailboxes[&account_id];
            let target = boxes
                .iter()
                .find(|m| m.mailbox_type == MailboxType::Archive)
                .or_else(|| boxes.iter().find(|m| m.mailbox_type == MailboxType::All))
                .expect("a move was only planned with a target");
            let moved = self
                .handle_move_many(messages, target.id, target.path.clone())
                .await?;
            let (done, not_done) = bulk_counts(moved);
            succeeded += done;
            failed += not_done;
        }
        Ok(CommandResult::MessagesBulkChanged { succeeded, failed })
    }

    /// Apply each plan on the server, in one request per distinct change and
    /// chunk, then record it. `strict` turns a plan that would leave its
    /// message in no mailbox into an error; otherwise it counts as `failed`.
    async fn apply_labels(
        &self,
        plans: Vec<Plan>,
        strict: bool,
    ) -> Result<CommandResult, ApplicationError> {
        let mut failed = 0;
        let mut by_account: HashMap<AccountId, Vec<Plan>> = HashMap::new();
        for plan in plans {
            match self.load_message(&plan.0).await {
                Ok((message, _)) => by_account.entry(message.account_id).or_default().push(plan),
                Err(_) => failed += 1,
            }
        }

        type Change = (MailboxId, Vec<MailboxId>, Vec<MailboxId>);
        let mut succeeded = 0;
        for (account_id, plans) in by_account {
            let _turn = self.turn(account_id).await;
            let mailboxes = self.message_read_repo.list_mailboxes(&account_id).await?;
            let path_of = |id: &MailboxId| {
                mailboxes
                    .iter()
                    .find(|m| m.id == *id)
                    .map(|m| m.path.clone())
                    .ok_or_else(|| ApplicationError::NotFound("Mailbox", id.to_string()))
            };
            let mut groups: HashMap<Change, Vec<(MessageId, RemoteId)>> = HashMap::new();
            for (id, add, remove) in plans {
                let Ok((mut message, mut seq)) = self.load_message(&id).await else {
                    failed += 1;
                    continue;
                };
                for mailbox in add.iter().chain(&remove) {
                    path_of(mailbox)?;
                }
                let add: Vec<_> = add
                    .into_iter()
                    .filter(|m| message.membership(*m).is_none())
                    .collect();
                let remove: Vec<_> = remove
                    .into_iter()
                    .filter(|m| message.membership(*m).is_some())
                    .collect();
                if add.is_empty() && remove.is_empty() {
                    continue;
                }
                if message.memberships.len() + add.len() <= remove.len() {
                    if strict {
                        return Err(ApplicationError::PreconditionFailed(
                            "a message must stay in at least one mailbox".into(),
                        ));
                    }
                    failed += 1;
                    continue;
                }
                let Some(held) = message.memberships.first().map(|m| m.mailbox_id) else {
                    continue;
                };
                let Ok(remote_id) = self
                    .remote_id(&mut message, &mut seq, &path_of(&held)?)
                    .await
                else {
                    failed += 1;
                    continue;
                };
                groups
                    .entry((held, add, remove))
                    .or_default()
                    .push((id, remote_id));
            }

            let provider = self.provider_registry.get_provider(&account_id)?;
            for ((held, add, remove), members) in groups {
                let (held_path, add_paths, remove_paths) = (
                    path_of(&held)?,
                    add.iter().map(&path_of).collect::<Result<Vec<_>, _>>()?,
                    remove.iter().map(&path_of).collect::<Result<Vec<_>, _>>()?,
                );
                for chunk in members.chunks(SERVER_CHUNK) {
                    let ids: Vec<RemoteId> = chunk.iter().map(|(_, id)| id.clone()).collect();
                    if let Err(e) = provider
                        .change_labels(&ids, &held_path, &add_paths, &remove_paths)
                        .await
                    {
                        self.request_resync(account_id, &held_path, None);
                        return Err(e.into());
                    }
                    for (message_id, remote_id) in chunk {
                        let added = add
                            .iter()
                            .map(|&mailbox_id| Membership {
                                mailbox_id,
                                remote_id: Some(remote_id.clone()),
                            })
                            .collect::<Vec<_>>();
                        if let Err(e) = self
                            .record(message_id, |m| m.relabel(added.clone(), remove.clone()))
                            .await
                        {
                            warn!(%message_id, %e, "labels changed on the server but not recorded here, resyncing");
                            self.request_resync(account_id, &held_path, None);
                        }
                    }
                    succeeded += chunk.len() as u32;
                }
            }
        }
        info!(succeeded, failed, "labels changed");
        Ok(CommandResult::MessagesBulkChanged { succeeded, failed })
    }
}

fn bulk_counts(result: CommandResult) -> (u32, u32) {
    match result {
        CommandResult::MessagesBulkChanged { succeeded, failed } => (succeeded, failed),
        _ => (0, 0),
    }
}
