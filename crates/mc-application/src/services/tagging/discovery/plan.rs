//! A curation plan: an ordered list of operations on proposals and labels,
//! checked as a whole before any of it is done. Validation walks the plan
//! against a model of the proposals (a merge makes a proposal that later
//! operations can name by its alias), so nothing is applied unless all of it
//! can be.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use mc_domain::{
    AccountId, LabelMode, LabelSettings, MailboxReadRepository, MessageId, ProposalId,
    ProposalStatus, Scope, TaggingStore,
};

use super::SCAN_PAGE;
use crate::bus::CommandBus;
use crate::error::ApplicationError;
use crate::services::tagging::placement::{GroupMailboxes, Home, Placement, display_name};
use crate::services::tagging::{TaggingService, label_key, label_path};

#[derive(Debug, Clone, PartialEq)]
pub enum PlanOp {
    /// `alias`: lets later operations name the result as `@alias`.
    Merge {
        ids: Vec<String>,
        name: Option<String>,
        alias: Option<String>,
    },
    Rename {
        id: String,
        name: String,
    },
    Assign {
        id: String,
        label: String,
        archive: bool,
    },
    Accept {
        id: String,
        name: Option<String>,
        archive: bool,
    },
    Reject {
        id: String,
    },
    Mode {
        label: String,
        mode: LabelMode,
        threshold: Option<f64>,
        /// Also label the mail already there (apply modes).
        existing: bool,
        /// Which account's scope, when the label is in several.
        account: Option<AccountId>,
    },
}

impl PlanOp {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Merge { .. } => "merge",
            Self::Rename { .. } => "rename",
            Self::Assign { .. } => "assign",
            Self::Accept { .. } => "accept",
            Self::Reject { .. } => "reject",
            Self::Mode { .. } => "mode",
        }
    }
}

/// Mail of one account an operation touches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountCount {
    pub account: AccountId,
    pub messages: u32,
    /// Of `messages`, how many leave (or, undoing, return to) the Inbox.
    pub archived: u32,
}

#[derive(Debug, Clone, Default)]
pub struct PlanSummary {
    pub merged: u32,
    pub renamed: u32,
    pub accepted: u32,
    pub assigned: u32,
    pub rejected: u32,
    pub modes: u32,
    /// Labels no account has yet, with the accounts that will get them.
    pub labels_to_create: Vec<(String, Vec<AccountId>)>,
    /// Labels that already exist and receive mail or a new mode.
    pub labels_touched: Vec<String>,
    pub to_label: u32,
    pub to_archive: u32,
    pub accounts: Vec<AccountCount>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpStatus {
    /// Dry run: it would be done.
    Planned,
    Done,
    Failed,
    /// An earlier operation failed.
    Skipped,
}

#[derive(Debug, Clone)]
pub struct OpResult {
    /// 1-based position in the plan.
    pub index: usize,
    pub kind: &'static str,
    pub status: OpStatus,
    pub detail: String,
    /// The proposal a merge made.
    pub proposal: Option<ProposalId>,
    /// Messages queued for labelling by an accept or assign.
    pub queued: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct PlanReport {
    pub dry_run: bool,
    pub summary: PlanSummary,
    pub results: Vec<OpResult>,
}

/// A proposal as the plan sees it at some point: real, or made by a merge.
struct Item {
    scope: Scope,
    /// `None`: a merge nobody named; its name is only known once merged.
    name: Option<String>,
    members: Vec<MessageId>,
    /// The operation that consumed it, if any.
    used_by: Option<usize>,
}

/// What validating the plan found, and what it will do.
#[derive(Default)]
struct Check {
    items: HashMap<String, Item>,
    aliases: HashMap<String, String>,
    groups: HashMap<Scope, GroupMailboxes>,
    /// (account, label) created by an earlier operation of the plan.
    created: HashSet<(AccountId, String)>,
    /// Labels (per scope) an earlier accept or assign puts mail under.
    labels: HashSet<(Scope, String)>,
    summary: PlanSummary,
    per_account: HashMap<AccountId, AccountCount>,
    creations: BTreeMap<String, Vec<AccountId>>,
    touched: Vec<String>,
    results: Vec<OpResult>,
    /// The account each `mode` operation acts through.
    accounts: HashMap<usize, AccountId>,
    errors: Vec<String>,
}

fn valid_alias(alias: &str) -> bool {
    !alias.is_empty()
        && alias
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    /// Check every operation, then (unless `dry_run`) do them in order. A plan
    /// that fails validation changes nothing and the error lists every
    /// problem. Execution stops at the first operation that fails: what was
    /// done before it stays done, and the report says so.
    pub async fn apply_plan(
        self: &Arc<Self>,
        ops: &[PlanOp],
        dry_run: bool,
    ) -> Result<PlanReport, ApplicationError> {
        let mut check = self.check_plan(ops).await?;
        if !check.errors.is_empty() {
            return Err(ApplicationError::PreconditionFailed(
                check.errors.join("\n"),
            ));
        }
        let summary = check.summarize();
        if dry_run {
            return Ok(PlanReport {
                dry_run,
                summary,
                results: check.results,
            });
        }
        let results = self.run_plan(ops, check.accounts).await;
        Ok(PlanReport {
            dry_run,
            summary,
            results,
        })
    }

    async fn check_plan(&self, ops: &[PlanOp]) -> Result<Check, ApplicationError> {
        let mut check = Check::default();
        if ops.is_empty() {
            check.errors.push("the plan has no operations".into());
        }
        for (i, op) in ops.iter().enumerate() {
            let n = i + 1;
            match self.check_op(&mut check, n, op).await? {
                Ok(detail) => check.results.push(OpResult {
                    index: n,
                    kind: op.kind(),
                    status: OpStatus::Planned,
                    detail,
                    proposal: None,
                    queued: None,
                }),
                Err(e) => check.errors.push(format!("op {n} ({}): {e}", op.kind())),
            }
        }
        Ok(check)
    }

    /// The outer error is the store failing; the inner one a problem with the plan.
    async fn check_op(
        &self,
        check: &mut Check,
        n: usize,
        op: &PlanOp,
    ) -> Result<Result<String, String>, ApplicationError> {
        Ok(match op {
            PlanOp::Merge { ids, name, alias } => {
                self.check_merge(check, n, ids, name.as_deref(), alias.as_deref())
                    .await?
            }
            PlanOp::Rename { id, name } => {
                let Ok(name) = label_path(name).map_err(|e| e.to_string()) else {
                    return Ok(Err(format!("{:?} is not a label name", name.trim())));
                };
                match self.open_item(check, id).await? {
                    Ok(key) => {
                        let item = check.items.get_mut(&key).expect("opened");
                        item.name = Some(name.clone());
                        check.summary.renamed += 1;
                        Ok(format!("rename to {name:?}"))
                    }
                    Err(e) => Err(e),
                }
            }
            PlanOp::Reject { id } => match self.open_item(check, id).await? {
                Ok(key) => {
                    let item = check.items.get_mut(&key).expect("opened");
                    item.used_by = Some(n);
                    check.summary.rejected += 1;
                    Ok(format!("reject {} messages", item.members.len()))
                }
                Err(e) => Err(e),
            },
            PlanOp::Assign { id, label, archive } => {
                self.check_label_op(check, n, id, Some(label), None, *archive)
                    .await?
            }
            PlanOp::Accept { id, name, archive } => {
                self.check_label_op(check, n, id, None, name.as_deref(), *archive)
                    .await?
            }
            PlanOp::Mode { .. } => self.check_mode(check, n, op).await?,
        })
    }

    async fn check_merge(
        &self,
        check: &mut Check,
        n: usize,
        ids: &[String],
        name: Option<&str>,
        alias: Option<&str>,
    ) -> Result<Result<String, String>, ApplicationError> {
        let name = match name.map(label_path).transpose() {
            Ok(name) => name,
            Err(e) => return Ok(Err(e.to_string())),
        };
        if let Some(alias) = alias
            && (!valid_alias(alias) || check.aliases.contains_key(alias))
        {
            return Ok(Err(format!(
                "alias {alias:?} is empty, has odd characters or is already used"
            )));
        }
        let mut keys = Vec::new();
        for id in ids {
            match self.open_item(check, id).await? {
                Ok(key) if keys.contains(&key) => {
                    return Ok(Err(format!("{id} is listed twice")));
                }
                Ok(key) => keys.push(key),
                Err(e) => return Ok(Err(e)),
            }
        }
        if keys.len() < 2 {
            return Ok(Err("merging takes at least two proposals".into()));
        }
        let scope = check.items[&keys[0]].scope;
        if keys.iter().any(|k| check.items[k].scope != scope) {
            return Ok(Err(
                "proposals of different training groups cannot be merged".into(),
            ));
        }
        let mut seen = HashSet::new();
        let mut members = Vec::new();
        for key in &keys {
            let item = check.items.get_mut(key).expect("opened");
            item.used_by = Some(n);
            members.extend(item.members.iter().copied().filter(|m| seen.insert(*m)));
        }
        let detail = format!(
            "merge {} proposals into {} messages",
            keys.len(),
            members.len()
        );
        let key = format!("merge#{n}");
        check.items.insert(
            key.clone(),
            Item {
                scope,
                name,
                members,
                used_by: None,
            },
        );
        if let Some(alias) = alias {
            check.aliases.insert(alias.to_owned(), key);
        }
        check.summary.merged += 1;
        Ok(Ok(detail))
    }

    /// An accept (`label` None: `name` or the proposal's) or an assign.
    async fn check_label_op(
        &self,
        check: &mut Check,
        n: usize,
        id: &str,
        label: Option<&String>,
        name: Option<&str>,
        archive: bool,
    ) -> Result<Result<String, String>, ApplicationError> {
        let key = match self.open_item(check, id).await? {
            Ok(key) => key,
            Err(e) => return Ok(Err(e)),
        };
        let (scope, proposal_name) = {
            let item = &check.items[&key];
            (item.scope, item.name.clone())
        };
        let wanted = match (label, name.or(proposal_name.as_deref())) {
            (Some(label), _) => label.trim().to_owned(),
            (None, Some(name)) => match label_path(name) {
                Ok(name) => name,
                Err(e) => return Ok(Err(e.to_string())),
            },
            (None, None) => {
                return Ok(Err(
                    "name the merge or the accept: the merged name is only known after merging"
                        .into(),
                ));
            }
        };
        let label_key = label_key(&wanted);
        if let std::collections::hash_map::Entry::Vacant(slot) = check.groups.entry(scope) {
            slot.insert(self.group_mailboxes(scope).await?);
        }
        let known = check.labels.contains(&(scope, label_key.clone()))
            || self
                .label_known(scope, &label_key, &check.groups[&scope])
                .await?;
        if label.is_some() && !known {
            return Ok(Err(format!(
                "label {wanted:?} does not exist in the proposal's group"
            )));
        }
        let shown = if known {
            display_name(&check.groups[&scope], &label_key)
        } else {
            wanted.clone()
        };
        let members = std::mem::take(&mut check.items.get_mut(&key).expect("opened").members);
        let reach = self
            .reach(check, scope, &members, (&label_key, &shown), archive)
            .await?;
        let item = check.items.get_mut(&key).expect("opened");
        item.members = members;
        item.used_by = Some(n);
        check.labels.insert((scope, label_key));
        if label.is_some() {
            check.summary.assigned += 1;
        } else {
            check.summary.accepted += 1;
        }
        if known {
            check.touched.push(shown.clone());
        }
        Ok(Ok(format!(
            "{} {} messages ({} leave the Inbox) as {shown:?}",
            if label.is_some() { "assign" } else { "accept" },
            reach.0,
            reach.1
        )))
    }

    /// How many of the members a label would reach, and how many of those
    /// leave the Inbox; adds them to the plan's totals and notes the
    /// accounts that would have to create the label.
    async fn reach(
        &self,
        check: &mut Check,
        scope: Scope,
        members: &[MessageId],
        (label, shown): (&str, &str),
        archive: bool,
    ) -> Result<(u32, u32), ApplicationError> {
        let mode = if archive {
            LabelMode::ApplyArchive
        } else {
            LabelMode::Apply
        };
        let (mut total, mut archived) = (0, 0);
        for page in members.chunks(SCAN_PAGE) {
            for source in self.store.sources(page).await? {
                if !source.labels.is_empty() || !source.can_label() {
                    continue;
                }
                let place = Placement::of(&source, &check.groups[&scope], label, mode);
                if matches!(place.home, Home::Create(_))
                    && check.created.insert((source.account_id, label.to_owned()))
                {
                    check
                        .creations
                        .entry(shown.to_owned())
                        .or_default()
                        .push(source.account_id);
                }
                let count = check
                    .per_account
                    .entry(source.account_id)
                    .or_insert(AccountCount {
                        account: source.account_id,
                        messages: 0,
                        archived: 0,
                    });
                count.messages += 1;
                count.archived += u32::from(place.archives());
                total += 1;
                archived += u32::from(place.archives());
            }
        }
        Ok((total, archived))
    }

    async fn check_mode(
        &self,
        check: &mut Check,
        n: usize,
        op: &PlanOp,
    ) -> Result<Result<String, String>, ApplicationError> {
        let PlanOp::Mode {
            label,
            mode,
            threshold,
            existing,
            account,
        } = op
        else {
            unreachable!("check_op passes modes only");
        };
        let key = label_key(label);
        if check.labels.iter().any(|(_, l)| *l == key) {
            return Ok(Err(format!(
                "{label:?} is created or filled by this plan: set its mode once the apply is over"
            )));
        }
        if *existing && !mode.applies() {
            return Ok(Err(
                "`existing` only goes with apply or apply_archive".into()
            ));
        }
        if threshold.is_some_and(|t| !(0.5..=1.0).contains(&t)) {
            return Ok(Err("threshold must be between 0.5 and 1".into()));
        }
        let account = match account {
            Some(a) => *a,
            None => match self.account_of_label(&key).await? {
                Ok(a) => a,
                Err(e) => return Ok(Err(e)),
            },
        };
        let status = self.label_status(account, &key).await?;
        if status.positives == 0 {
            return Ok(Err(format!("label {label:?} does not exist")));
        }
        if *mode != LabelMode::Off && status.positives < LabelSettings::MIN_POSITIVES {
            return Ok(Err(format!(
                "{label:?} has {} examples of the {} it needs",
                status.positives,
                LabelSettings::MIN_POSITIVES
            )));
        }
        check.accounts.insert(n, account);
        check.summary.modes += 1;
        check.touched.push(label.trim().to_owned());
        Ok(Ok(format!(
            "set {label:?} to {}{}",
            mode.as_str(),
            if *existing {
                " and label existing mail"
            } else {
                ""
            }
        )))
    }

    /// An account of the only scope that has the label.
    async fn account_of_label(
        &self,
        key: &str,
    ) -> Result<Result<AccountId, String>, ApplicationError> {
        let mut found = Vec::new();
        for scope in self.store.scopes().await? {
            if self
                .store
                .positives(scope)
                .await?
                .iter()
                .any(|(l, _)| l == key)
            {
                found.extend(self.store.scope_accounts(scope).await?.into_iter().next());
            }
        }
        Ok(match found.len() {
            1 => Ok(found[0]),
            0 => Err(format!("label {key:?} does not exist")),
            _ => Err(format!(
                "label {key:?} is in several groups: add \"account\""
            )),
        })
    }

    /// The plan's item for a reference (`@alias` or a proposal id), loaded
    /// from the store on first use; it must be pending and not used yet.
    async fn open_item(
        &self,
        check: &mut Check,
        reference: &str,
    ) -> Result<Result<String, String>, ApplicationError> {
        let key = if let Some(alias) = reference.strip_prefix('@') {
            match check.aliases.get(alias) {
                Some(key) => key.clone(),
                None => return Ok(Err(format!("no merge is called @{alias} before this"))),
            }
        } else {
            let Ok(uuid) = uuid::Uuid::parse_str(reference) else {
                return Ok(Err(format!("{reference:?} is not a proposal id or @alias")));
            };
            let key = uuid.to_string();
            if !check.items.contains_key(&key) {
                let found = self
                    .store
                    .proposal(ProposalId::from_uuid(uuid))
                    .await?
                    .filter(|p| p.status == ProposalStatus::Pending);
                let Some(p) = found else {
                    return Ok(Err(format!("{reference} is not a pending proposal")));
                };
                check.items.insert(
                    key.clone(),
                    Item {
                        scope: p.scope,
                        name: Some(p.name),
                        members: p.members,
                        used_by: None,
                    },
                );
            }
            key
        };
        Ok(match check.items[&key].used_by {
            Some(by) => Err(format!(
                "{reference} was already merged, answered or rejected in op {by}"
            )),
            None => Ok(key),
        })
    }

    /// Do the operations in order; the first failure ends the run.
    async fn run_plan(
        self: &Arc<Self>,
        ops: &[PlanOp],
        accounts: HashMap<usize, AccountId>,
    ) -> Vec<OpResult> {
        let mut aliases: HashMap<String, ProposalId> = HashMap::new();
        let mut results = Vec::with_capacity(ops.len());
        let mut failed = false;
        for (i, op) in ops.iter().enumerate() {
            let mut result = OpResult {
                index: i + 1,
                kind: op.kind(),
                status: OpStatus::Skipped,
                detail: String::new(),
                proposal: None,
                queued: None,
            };
            if !failed {
                match self
                    .run_op(op, accounts.get(&(i + 1)).copied(), &mut aliases)
                    .await
                {
                    Ok((detail, proposal, queued)) => {
                        (result.status, result.detail) = (OpStatus::Done, detail);
                        (result.proposal, result.queued) = (proposal, queued);
                    }
                    Err(e) => {
                        failed = true;
                        (result.status, result.detail) = (OpStatus::Failed, e.to_string());
                    }
                }
            }
            results.push(result);
        }
        results
    }

    async fn run_op(
        self: &Arc<Self>,
        op: &PlanOp,
        account: Option<AccountId>,
        aliases: &mut HashMap<String, ProposalId>,
    ) -> Result<(String, Option<ProposalId>, Option<u32>), ApplicationError> {
        let id_of = |reference: &str| -> Result<ProposalId, ApplicationError> {
            match reference.strip_prefix('@') {
                Some(alias) => aliases
                    .get(alias)
                    .copied()
                    .ok_or_else(|| ApplicationError::NotFound("Alias", reference.to_owned())),
                None => uuid::Uuid::parse_str(reference)
                    .map(ProposalId::from_uuid)
                    .map_err(|_| ApplicationError::NotFound("Proposal", reference.to_owned())),
            }
        };
        Ok(match op {
            PlanOp::Merge { ids, name, alias } => {
                let ids = ids
                    .iter()
                    .map(|i| id_of(i))
                    .collect::<Result<Vec<_>, _>>()?;
                let merged = self.merge_proposals(&ids, name.as_deref()).await?;
                let id = merged.proposal.id;
                if let Some(alias) = alias {
                    aliases.insert(alias.clone(), id);
                }
                (
                    format!(
                        "merged into {:?} ({} messages)",
                        merged.proposal.name,
                        merged.proposal.size()
                    ),
                    Some(id),
                    None,
                )
            }
            PlanOp::Rename { id, name } => {
                let renamed = self.rename_proposal(id_of(id)?, name).await?;
                (
                    format!("renamed to {:?}", renamed.proposal.name),
                    None,
                    None,
                )
            }
            PlanOp::Assign { id, label, archive } => {
                let queued = self.assign_proposal(id_of(id)?, label, *archive).await?;
                (
                    format!("labelling {queued} messages as {label:?}"),
                    None,
                    Some(queued),
                )
            }
            PlanOp::Accept { id, name, archive } => {
                let queued = self
                    .accept_proposal(id_of(id)?, name.as_deref(), *archive)
                    .await?;
                (format!("labelling {queued} messages"), None, Some(queued))
            }
            PlanOp::Reject { id } => {
                self.reject_proposal(id_of(id)?).await?;
                ("rejected".into(), None, None)
            }
            PlanOp::Mode {
                label,
                mode,
                threshold,
                existing,
                ..
            } => {
                let account = account
                    .ok_or_else(|| ApplicationError::Internal("mode without an account".into()))?;
                let threshold = match threshold {
                    Some(t) => *t,
                    None => self.label_status(account, label).await?.settings.threshold,
                };
                if *existing {
                    let preview = self.confirm_apply(account, label, *mode, threshold).await?;
                    (
                        format!(
                            "{label:?} is {}; labelling {} existing messages",
                            mode.as_str(),
                            preview.to_label
                        ),
                        None,
                        Some(preview.to_label),
                    )
                } else {
                    self.set_label(account, label, *mode, threshold).await?;
                    (format!("{label:?} is {}", mode.as_str()), None, None)
                }
            }
        })
    }
}

impl Check {
    fn summarize(&mut self) -> PlanSummary {
        let mut summary = std::mem::take(&mut self.summary);
        summary.accounts = self.per_account.values().copied().collect();
        summary.accounts.sort_by_key(|a| *a.account.as_uuid());
        summary.to_label = summary.accounts.iter().map(|a| a.messages).sum();
        summary.to_archive = summary.accounts.iter().map(|a| a.archived).sum();
        summary.labels_to_create = std::mem::take(&mut self.creations).into_iter().collect();
        self.touched.sort();
        self.touched.dedup();
        summary.labels_touched = std::mem::take(&mut self.touched);
        summary
    }
}
