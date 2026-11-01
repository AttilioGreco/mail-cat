//! Renaming (or merging into another label) and deleting an existing label on
//! every account of a training group. Mail is never deleted: it keeps its
//! other labels, and a message the label was the only home of is filed in the
//! Archive so it stays reachable.
//!
//! A label the account has no `to` for is renamed on the server, children
//! and all; a `to` that exists is merged into, level by level. Every step can
//! be repeated, so a job that stops halfway is finished by running the
//! command again. The counts the model keeps are not edited: deleting a
//! mailbox makes the scope learn again from the mail, and keyword changes
//! retrain message by message, as for any label change.

mod plan;
mod run;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use tracing::{info, warn};

use mc_domain::{
    AccountId, Flag, Mailbox, MailboxId, MailboxReadRepository, MailboxType, MessageId, Scope,
    SourceFilter, TaggingStore,
};

use self::plan::plan_steps;
use super::SCAN_PAGE;
use crate::bus::CommandBus;
use crate::error::ApplicationError;
use crate::services::tagging::placement::{display_name, stored_keywords};
use crate::services::tagging::stats::ApplyKind;
use crate::services::tagging::{TaggingService, label_key, label_mailbox, label_path};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    /// A label mailbox (Gmail, JMAP): other labels of the mail are kept.
    Mailbox,
    /// A real folder of a folders account: its mail moves as a whole.
    Folder,
    /// A keyword on the mail of a folders account.
    Keyword,
}

impl ChangeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mailbox => "mailbox",
            Self::Folder => "folder",
            Self::Keyword => "keyword",
        }
    }
}

/// What happens to one mailbox of the label's tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepKind {
    /// Renamed on the server, its children following.
    Rename,
    /// Its mail joins the mailbox the account has for the new label.
    Merge,
    /// The new label is made and its mail moved there.
    CreateMove,
    Delete,
}

impl StepKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rename => "rename",
            Self::Merge => "merge",
            Self::CreateMove => "create+move",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MailboxStep {
    pub label: String,
    /// The new name; `None` when deleting.
    pub into: Option<String>,
    pub how: StepKind,
    pub messages: u32,
}

#[derive(Debug, Clone)]
pub struct AccountChange {
    pub account: AccountId,
    pub kind: ChangeKind,
    pub messages: u32,
    /// Renaming: some mailbox joins a label the account has already.
    pub merges: bool,
    pub deletes_mailbox: bool,
    /// Deleting: messages whose only mailbox is the label, which go to the
    /// Archive (else the Inbox).
    pub rehomed: u32,
    pub mailboxes: Vec<MailboxStep>,
}

#[derive(Debug, Clone)]
pub struct LabelChange {
    pub dry_run: bool,
    pub label: String,
    /// The new name; `None` when deleting.
    pub into: Option<String>,
    /// Some account of the group already has the new label.
    pub merge: bool,
    /// Only the label's own mail moves; it and its children stay.
    pub mail_only: bool,
    pub messages: u32,
    pub accounts: Vec<AccountChange>,
}

/// A message carrying the label (or one below it), with the mailboxes
/// holding it.
struct Held {
    id: MessageId,
    mailboxes: u32,
    /// The labels of the tree it has, each with the keywords it holds for it
    /// (folders accounts).
    labels: Vec<(String, Vec<Flag>)>,
}

impl Held {
    fn has(&self, key: &str) -> bool {
        self.labels.iter().any(|(l, _)| l == key)
    }

    fn within(&self, key: &str) -> bool {
        self.labels.iter().any(|(l, _)| within(l, key))
    }
}

/// `label` is `key` or below it.
fn within(label: &str, key: &str) -> bool {
    label == key || label.strip_prefix(key).is_some_and(|r| r.starts_with('/'))
}

struct Step {
    from: Mailbox,
    into: Option<Named>,
    how: StepKind,
    /// The mailbox the account has for the new label (merging).
    target: Option<MailboxId>,
    /// Whether the mailbox is deleted once its mail is gone.
    deletes: bool,
}

impl Step {
    fn key(&self) -> String {
        label_key(&self.from.display_path)
    }
}

struct AccountWork {
    account: AccountId,
    folders: bool,
    /// Parents before their children.
    steps: Vec<Step>,
    held: Vec<Held>,
}

impl AccountWork {
    fn kind(&self) -> ChangeKind {
        match (self.steps.is_empty(), self.folders) {
            (false, true) => ChangeKind::Folder,
            (true, true) => ChangeKind::Keyword,
            _ => ChangeKind::Mailbox,
        }
    }
}

/// The label's key and the name to show for it.
type Named = (String, String);

struct Change {
    scope: Scope,
    label: Named,
    into: Option<Named>,
    merge: bool,
    mail_only: bool,
    /// The label and, unless `mail_only`, everything below it that exists.
    keys: Vec<String>,
    works: Vec<AccountWork>,
}

impl Change {
    fn case_only(&self) -> bool {
        self.into.as_ref().is_some_and(|t| t.0 == self.label.0)
    }

    /// The key a label of the tree gets; `None` when it is deleted.
    fn rewrite(&self, key: &str) -> Option<String> {
        let (to, _) = self.into.as_ref()?;
        Some(format!("{to}{}", &key[self.label.0.len()..]))
    }

    /// The keys of the tree that change, with what they become.
    fn renamed(&self) -> Vec<(String, Option<String>)> {
        self.keys
            .iter()
            .map(|k| (k.clone(), self.rewrite(k)))
            .filter(|(k, new)| new.as_ref() != Some(k))
            .collect()
    }

    fn report(&self, dry_run: bool) -> LabelChange {
        let accounts: Vec<_> = self
            .works
            .iter()
            .map(|w| {
                let steps: Vec<_> = w
                    .steps
                    .iter()
                    .map(|s| MailboxStep {
                        label: s.from.display_path.clone(),
                        into: s.into.as_ref().map(|t| t.1.clone()),
                        how: s.how,
                        messages: self.step_messages(w, s),
                    })
                    .collect();
                let folder_mail: u32 = w
                    .steps
                    .iter()
                    .filter(|s| s.how != StepKind::Rename)
                    .map(|s| s.from.total_messages)
                    .sum();
                AccountChange {
                    account: w.account,
                    kind: w.kind(),
                    messages: w.held.len() as u32 + if w.folders { folder_mail } else { 0 },
                    merges: w.steps.iter().any(|s| s.how == StepKind::Merge),
                    deletes_mailbox: w.steps.iter().any(|s| s.deletes),
                    rehomed: if self.into.is_none() && !w.folders {
                        w.held.iter().filter(|h| h.mailboxes <= 1).count() as u32
                    } else {
                        0
                    },
                    mailboxes: steps,
                }
            })
            .collect();
        LabelChange {
            dry_run,
            label: self.label.1.clone(),
            into: self.into.as_ref().map(|t| t.1.clone()),
            merge: self.merge,
            mail_only: self.mail_only,
            messages: accounts.iter().map(|a| a.messages).sum(),
            accounts,
        }
    }

    /// The mail the mailbox holds itself. A case-only rename reads no message,
    /// so the mailbox's own count stands in.
    fn step_messages(&self, work: &AccountWork, step: &Step) -> u32 {
        if work.folders || self.case_only() {
            return step.from.total_messages;
        }
        let key = step.key();
        work.held.iter().filter(|h| h.has(&key)).count() as u32
    }
}

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    /// Without `dry_run`, queued like an apply (throttled, shown in the
    /// background stats). A `to` the account has already is a merge; one it
    /// lacks is a rename on the server. `mail_only` moves just the mail of
    /// `from` itself.
    pub async fn rename_label(
        self: &Arc<Self>,
        from: &str,
        to: &str,
        account: Option<AccountId>,
        mail_only: bool,
        dry_run: bool,
    ) -> Result<LabelChange, ApplicationError> {
        self.change_label(from, Some(to), account, mail_only, dry_run)
            .await
    }

    pub async fn delete_label(
        self: &Arc<Self>,
        label: &str,
        account: Option<AccountId>,
        dry_run: bool,
    ) -> Result<LabelChange, ApplicationError> {
        self.change_label(label, None, account, false, dry_run)
            .await
    }

    async fn change_label(
        self: &Arc<Self>,
        label: &str,
        into: Option<&str>,
        account: Option<AccountId>,
        mail_only: bool,
        dry_run: bool,
    ) -> Result<LabelChange, ApplicationError> {
        let change = self.plan_change(label, into, account, mail_only).await?;
        let report = change.report(dry_run);
        if dry_run {
            return Ok(report);
        }
        for key in [Some(&change.label), change.into.as_ref()]
            .into_iter()
            .flatten()
        {
            if self.is_applying(&key.0) {
                return Err(ApplicationError::PreconditionFailed(format!(
                    "{:?} is still being applied or undone: wait for it to finish",
                    key.0
                )));
            }
        }
        let kind = if change.into.is_some() {
            ApplyKind::Relabel
        } else {
            ApplyKind::Delete
        };
        let job = self.begin_job(&change.label.0, kind, u64::from(report.messages));
        let service = Arc::clone(self);
        tokio::spawn(async move {
            let _turn = service.apply_turn.lock().await;
            match service.run_change(&change, job).await {
                Ok(()) => info!(label = change.label.0, "label changed"),
                Err(e) => warn!(label = change.label.0, error = %e, "changing a label stopped"),
            }
            service.end_job(job);
        });
        Ok(report)
    }

    async fn plan_change(
        &self,
        label: &str,
        into: Option<&str>,
        account: Option<AccountId>,
        mail_only: bool,
    ) -> Result<Change, ApplicationError> {
        let from = label_path(label)?;
        let key = label_key(&from);
        let into = into
            .map(|t| label_path(t).map(|name| (label_key(&name), name)))
            .transpose()?;
        if let Some((to, name)) = &into {
            if *name == from || (mail_only && *to == key) {
                return Err(ApplicationError::PreconditionFailed(format!(
                    "{from:?} and {name:?} are the same label"
                )));
            }
            if !mail_only && within(to, &key) && *to != key {
                return Err(ApplicationError::PreconditionFailed(
                    "a label cannot be moved inside itself; --mail-only moves just its own mail \
                     there"
                        .into(),
                ));
            }
        }
        for k in std::iter::once(&key).chain(into.as_ref().map(|t| &t.0)) {
            if k == "inbox" || MailboxType::from_name(k).is_some() {
                return Err(ApplicationError::PreconditionFailed(format!(
                    "{k:?} is a system mailbox, not a label"
                )));
            }
        }

        let scope = self.scope_with_label(&key, account).await?;
        let group = self.group_mailboxes(scope).await?;
        if into.is_none() {
            self.refuse_children(scope, &key, &group).await?;
        }
        let case_only = into.as_ref().is_some_and(|t| t.0 == key);
        // The label as the group spells it, unless the spelling is the change.
        let into = into.map(|(to, name)| {
            let shown = group
                .values()
                .find_map(|boxes| label_mailbox(boxes, &to))
                .filter(|_| !case_only)
                .map_or(name, |m| m.display_path.clone());
            (to, shown)
        });
        let subtree = !mail_only && !case_only;
        let mut keys = BTreeSet::from([key.clone()]);
        if subtree {
            keys.extend(self.labels_below(scope, &key, &group).await?);
        }

        let mut accounts = self.store.scope_accounts(scope).await?;
        accounts.sort_by_key(|a| *a.as_uuid());
        let mut works = Vec::new();
        for account in accounts {
            let own = group.get(&account).map_or(&[][..], Vec::as_slice);
            let folders = self.store.labels_with_keywords(account).await?;
            let steps = plan_steps(own, &key, &from, into.as_ref(), mail_only, folders)?;
            if steps.is_empty() && !folders {
                continue;
            }
            if into.is_none() && folders && !steps.is_empty() {
                return Err(ApplicationError::PreconditionFailed(format!(
                    "{from:?} is a folder of account {account}: its mail has nowhere to go. \
                     Use `mc mailboxes delete --move-to`"
                )));
            }
            let held = if case_only {
                vec![]
            } else {
                self.holders(account, &key, folders, subtree).await?
            };
            if steps.is_empty() && held.is_empty() {
                continue;
            }
            keys.extend(
                held.iter()
                    .flat_map(|h| h.labels.iter().map(|(l, _)| l.clone())),
            );
            works.push(AccountWork {
                account,
                folders,
                steps,
                held,
            });
        }
        if works.is_empty() {
            return Err(ApplicationError::NotFound("Label", from));
        }
        let merge = match &into {
            Some((to, _)) if !case_only => self.label_known(scope, to, &group).await?,
            _ => false,
        };
        Ok(Change {
            scope,
            label: (key.clone(), display_name(&group, &key)),
            into,
            merge,
            mail_only,
            keys: keys.into_iter().collect(),
            works,
        })
    }

    /// The only scope that has the label, or the account's.
    async fn scope_with_label(
        &self,
        key: &str,
        account: Option<AccountId>,
    ) -> Result<Scope, ApplicationError> {
        let scopes = match account {
            Some(a) => vec![self.store.scope_of(a).await?],
            None => self.store.scopes().await?,
        };
        let mut found = Vec::new();
        for scope in scopes {
            let group = self.group_mailboxes(scope).await?;
            if self.label_known(scope, key, &group).await? {
                found.push(scope);
            }
        }
        match found.as_slice() {
            [scope] => Ok(*scope),
            [] => Err(ApplicationError::NotFound("Label", key.to_owned())),
            _ => Err(ApplicationError::PreconditionFailed(format!(
                "label {key:?} is in several groups: pass --account"
            ))),
        }
    }

    /// The labels below `key` that a mailbox of the group or the model's
    /// examples name.
    async fn labels_below(
        &self,
        scope: Scope,
        key: &str,
        group: &HashMap<AccountId, Vec<Mailbox>>,
    ) -> Result<Vec<String>, ApplicationError> {
        let named = group
            .values()
            .flatten()
            .filter(|m| m.mailbox_type == MailboxType::Custom)
            .map(|m| label_key(&m.display_path));
        let learnt = self
            .store
            .positives(scope)
            .await?
            .into_iter()
            .map(|(l, _)| l);
        Ok(named
            .chain(learnt)
            .filter(|l| l != key && within(l, key))
            .collect())
    }

    async fn refuse_children(
        &self,
        scope: Scope,
        key: &str,
        group: &HashMap<AccountId, Vec<Mailbox>>,
    ) -> Result<(), ApplicationError> {
        match self.labels_below(scope, key, group).await?.first() {
            Some(child) => Err(ApplicationError::PreconditionFailed(format!(
                "{key:?} has child labels (such as {child:?}): rename or delete the children \
                 first, or rename the label to move the whole tree"
            ))),
            None => Ok(()),
        }
    }

    /// The messages of the account that carry the label, or one below it.
    async fn holders(
        &self,
        account: AccountId,
        key: &str,
        folders: bool,
        subtree: bool,
    ) -> Result<Vec<Held>, ApplicationError> {
        let mut held = Vec::new();
        let mut cursor = None;
        loop {
            let page = self
                .store
                .source_page(
                    &[account],
                    SourceFilter::Held,
                    cursor,
                    false,
                    SCAN_PAGE as u32,
                )
                .await?;
            let Some(last) = page.last().map(|s| s.id) else {
                return Ok(held);
            };
            cursor = Some(last);
            for source in page.into_iter().filter(|s| s.keywords == folders) {
                let labels: Vec<_> = source
                    .labels
                    .iter()
                    .filter(|l| if subtree { within(l, key) } else { *l == key })
                    .map(|l| (l.clone(), stored_keywords(&source, l)))
                    .collect();
                if !labels.is_empty() {
                    held.push(Held {
                        id: source.id,
                        mailboxes: source.mailboxes,
                        labels,
                    });
                }
            }
        }
    }
}
