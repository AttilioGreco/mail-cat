//! Putting a label on messages. A labels account carries it as a mailbox,
//! created on the server first if the account has none; a folders account
//! carries it as a keyword (tag), which needs no creation.

use std::collections::HashMap;

use mc_domain::{
    AccountId, Flag, LabelMode, Mailbox, MailboxId, MailboxReadRepository, MessageId, Scope,
    TagSource, TaggingStore,
};

use super::{TaggingService, inbox_of, label_mailbox};
use crate::bus::CommandBus;
use crate::commands::{Command, CommandResult};
use crate::error::ApplicationError;

/// Where a label sits on one account.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum Home {
    Mailbox(MailboxId),
    Keyword(String),
    /// A mailbox the account lacks, by the name to give it.
    Create(String),
}

/// What it takes to label a message of one account.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct Placement {
    pub home: Home,
    /// The Inbox, dropped in the same change (labels accounts).
    pub remove: Option<MailboxId>,
    /// Archive afterwards (folders accounts, whose archive is a move).
    pub archive: bool,
}

/// Mailboxes of the accounts that learn together.
pub(super) type GroupMailboxes = HashMap<AccountId, Vec<Mailbox>>;

/// A keyword is an atom: anything else in the name becomes `_`, which the
/// store reads back as a space, so the label keeps its identity.
pub(super) fn keyword_of(label: &str) -> String {
    label
        .chars()
        .map(|c| {
            if c.is_ascii_graphic() && !"(){]%*\"\\".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl Placement {
    pub fn of(source: &TagSource, group: &GroupMailboxes, label: &str, mode: LabelMode) -> Self {
        let archive = mode == LabelMode::ApplyArchive && source.in_inbox;
        if source.keywords {
            return Self {
                home: Home::Keyword(keyword_of(label)),
                remove: None,
                archive,
            };
        }
        let own = group.get(&source.account_id).map_or(&[][..], Vec::as_slice);
        let home = label_mailbox(own, label).map_or_else(
            || Home::Create(display_name(group, label)),
            |m| Home::Mailbox(m.id),
        );
        Self {
            home,
            remove: archive.then(|| inbox_of(own).map(|m| m.id)).flatten(),
            archive: false,
        }
    }

    pub fn archives(&self) -> bool {
        self.archive || self.remove.is_some()
    }
}

/// The label as the group spells it, else as the classifier keys it.
pub(super) fn display_name(group: &GroupMailboxes, label: &str) -> String {
    group
        .values()
        .find_map(|boxes| label_mailbox(boxes, label))
        .map_or_else(|| label.to_owned(), |m| m.display_path.clone())
}

fn all_changed(result: CommandResult) -> Result<(), ApplicationError> {
    match result {
        CommandResult::MessagesBulkChanged { failed: 0, .. } => Ok(()),
        CommandResult::MessagesBulkChanged { failed, .. } => Err(
            ApplicationError::PreconditionFailed(format!("{failed} messages not changed")),
        ),
        _ => Ok(()),
    }
}

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    pub(super) async fn group_mailboxes(
        &self,
        scope: Scope,
    ) -> Result<GroupMailboxes, ApplicationError> {
        let mut group = HashMap::new();
        for id in self.store.scope_accounts(scope).await? {
            group.insert(id, self.read.list_mailboxes(&id).await?);
        }
        Ok(group)
    }

    /// Label the messages of `account` as `place` says, creating the label on
    /// the account first when it lacks it.
    pub(super) async fn put(
        &self,
        account: AccountId,
        ids: Vec<MessageId>,
        place: &Placement,
    ) -> Result<(), ApplicationError> {
        match &place.home {
            Home::Keyword(keyword) => {
                for &message_id in &ids {
                    self.commands
                        .dispatch(Command::SetMessageFlags {
                            message_id,
                            flags_to_add: vec![Flag::Custom(keyword.clone())],
                            flags_to_remove: vec![],
                        })
                        .await?;
                }
            }
            Home::Mailbox(mailbox) => {
                self.change_labels(
                    ids.clone(),
                    vec![*mailbox],
                    place.remove.into_iter().collect(),
                )
                .await?;
            }
            Home::Create(name) => {
                let mailbox = self.create_label(account, name).await?;
                self.change_labels(
                    ids.clone(),
                    vec![mailbox],
                    place.remove.into_iter().collect(),
                )
                .await?;
            }
        }
        if place.archive {
            let archived = self
                .commands
                .dispatch(Command::ArchiveMessages { message_ids: ids });
            all_changed(archived.await?)?;
        }
        Ok(())
    }

    async fn create_label(
        &self,
        account: AccountId,
        name: &str,
    ) -> Result<MailboxId, ApplicationError> {
        let created = self
            .commands
            .dispatch(Command::CreateLabel {
                account_id: account,
                name: name.to_owned(),
            })
            .await?;
        match created {
            CommandResult::LabelCreated { mailbox_id } => Ok(mailbox_id),
            other => Err(ApplicationError::PreconditionFailed(format!(
                "unexpected result creating a label: {other:?}"
            ))),
        }
    }

    /// Add and remove labels, failing when the command could not change a
    /// message (it reports that as a count, not an error).
    pub(super) async fn change_labels(
        &self,
        ids: Vec<MessageId>,
        add: Vec<MailboxId>,
        remove: Vec<MailboxId>,
    ) -> Result<(), ApplicationError> {
        let changed = self.commands.dispatch(Command::ChangeLabels {
            message_ids: ids,
            add,
            remove,
        });
        all_changed(changed.await?)
    }

    /// Forget auto-apply records of messages that did not get the label after
    /// all (the change failed for them).
    pub(super) async fn forget_unapplied(&self, ids: &[MessageId], label: &str) {
        let Ok(sources) = self.store.sources(ids).await else {
            return;
        };
        for source in sources
            .iter()
            .filter(|s| !s.labels.iter().any(|l| l == label))
        {
            let _ = self.store.take_auto(source.id, label).await;
        }
    }
}
