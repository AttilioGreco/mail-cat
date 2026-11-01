//! Curating labels in bulk: plans, undoing an acceptance, one label in view.

use crate::client::MailCatClient;
use crate::discovery::{ProposalMessage, message};
use crate::error::Result;
use crate::proto::mail_cat::v1 as pb;
use crate::tagging::{LabelStatus, status};

/// One step of a plan. `kind` is `merge`, `rename`, `assign`, `accept`,
/// `reject` or `mode`; the fields a kind does not use stay empty.
#[derive(Debug, Clone, Default)]
pub struct CurationOp {
    pub kind: String,
    pub ids: Vec<String>,
    pub id: String,
    pub name: String,
    pub label: String,
    pub alias: String,
    pub archive: bool,
    pub mode: String,
    pub threshold: Option<f64>,
    pub existing: bool,
    pub account_id: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountCount {
    pub account_id: String,
    pub messages: u32,
    pub archived: u32,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelCreation {
    pub name: String,
    pub account_ids: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanSummary {
    pub merged: u32,
    pub renamed: u32,
    pub accepted: u32,
    pub assigned: u32,
    pub rejected: u32,
    pub modes: u32,
    pub labels_to_create: Vec<LabelCreation>,
    pub labels_touched: Vec<String>,
    pub to_label: u32,
    pub to_archive: u32,
    pub accounts: Vec<AccountCount>,
}

/// `status`: `planned` (dry run), `done`, `failed` or `skipped`.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpResult {
    pub index: u32,
    pub kind: String,
    pub status: String,
    pub detail: String,
    /// The proposal a merge made.
    pub proposal_id: Option<String>,
    /// Messages queued for labelling.
    pub queued: Option<u32>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanReport {
    pub dry_run: bool,
    pub summary: PlanSummary,
    pub results: Vec<OpResult>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UndoReport {
    pub dry_run: bool,
    pub to_unlabel: u32,
    pub to_unarchive: u32,
    pub accounts: Vec<AccountCount>,
    pub labels_to_delete: Vec<String>,
    /// What the proposal becomes: `pending` or `rejected`.
    pub status: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelChangeAccount {
    pub account_id: String,
    /// `mailbox`, `folder` or `keyword`.
    pub kind: String,
    pub messages: u32,
    /// Renaming: the account has the new label already.
    pub merges: bool,
    pub deletes_mailbox: bool,
    /// Deleting: messages in no other mailbox, filed in the Archive.
    pub rehomed: u32,
    /// Every mailbox of the label's tree the account changes, parents first.
    pub mailboxes: Vec<LabelChangeMailbox>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelChangeMailbox {
    pub label: String,
    /// `None` when deleting.
    pub into: Option<String>,
    /// `rename`, `merge`, `create+move` or `delete`.
    pub how: String,
    pub messages: u32,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelChange {
    pub dry_run: bool,
    pub label: String,
    /// The new name; `None` when deleting.
    pub into: Option<String>,
    pub merge: bool,
    /// Only the label's own mail moves; it and its children stay.
    pub mail_only: bool,
    pub messages: u32,
    pub accounts: Vec<LabelChangeAccount>,
}

fn label_change(r: pb::LabelChangeReport) -> LabelChange {
    LabelChange {
        dry_run: r.dry_run,
        label: r.label,
        into: Some(r.into).filter(|t| !t.is_empty()),
        merge: r.merge,
        mail_only: r.mail_only,
        messages: r.messages,
        accounts: r
            .accounts
            .into_iter()
            .map(|a| LabelChangeAccount {
                account_id: a.account_id,
                kind: a.kind,
                messages: a.messages,
                merges: a.merges,
                deletes_mailbox: a.deletes_mailbox,
                rehomed: a.rehomed,
                mailboxes: a
                    .mailboxes
                    .into_iter()
                    .map(|m| LabelChangeMailbox {
                        label: m.label,
                        into: Some(m.into).filter(|t| !t.is_empty()),
                        how: m.how,
                        messages: m.messages,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// `kind`: `mailbox`, `keyword` (no mailbox; its mail carries the keyword)
/// or `missing`.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelPresence {
    pub account_id: String,
    pub kind: String,
    pub mailbox_id: String,
    pub path: String,
    pub messages: u32,
    pub keyword: String,
    /// Mail of the account the model learnt the label from.
    pub examples: u32,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelDetail {
    pub scope: String,
    pub group: String,
    pub account_ids: Vec<String>,
    pub name: String,
    #[serde(flatten)]
    pub status: LabelStatus,
    pub accounts: Vec<LabelPresence>,
    pub samples: Vec<ProposalMessage>,
}

fn account_count(a: pb::AccountCount) -> AccountCount {
    AccountCount {
        account_id: a.account_id,
        messages: a.messages,
        archived: a.archived,
    }
}

impl MailCatClient {
    /// Check the plan and, unless `dry_run`, do it. A plan that does not
    /// validate is an error listing every problem.
    pub async fn apply_curation_plan(
        &self,
        ops: &[CurationOp],
        dry_run: bool,
    ) -> Result<PlanReport> {
        let ops = ops
            .iter()
            .cloned()
            .map(|o| pb::CurationOp {
                kind: o.kind,
                ids: o.ids,
                id: o.id,
                name: o.name,
                label: o.label,
                alias: o.alias,
                archive: o.archive,
                mode: o.mode,
                threshold: o.threshold,
                existing: o.existing,
                account_id: o.account_id,
            })
            .collect();
        let resp = self
            .tagging()
            .apply_curation_plan(pb::ApplyCurationPlanRequest { ops, dry_run })
            .await?
            .into_inner();
        let s = resp.summary.unwrap_or_default();
        Ok(PlanReport {
            dry_run: resp.dry_run,
            summary: PlanSummary {
                merged: s.merged,
                renamed: s.renamed,
                accepted: s.accepted,
                assigned: s.assigned,
                rejected: s.rejected,
                modes: s.modes,
                labels_to_create: s
                    .labels_to_create
                    .into_iter()
                    .map(|c| LabelCreation {
                        name: c.name,
                        account_ids: c.account_ids,
                    })
                    .collect(),
                labels_touched: s.labels_touched,
                to_label: s.to_label,
                to_archive: s.to_archive,
                accounts: s.accounts.into_iter().map(account_count).collect(),
            },
            results: resp
                .results
                .into_iter()
                .map(|r| OpResult {
                    index: r.index,
                    kind: r.kind,
                    status: r.status,
                    detail: r.detail,
                    proposal_id: Some(r.proposal_id).filter(|p| !p.is_empty()),
                    queued: r.queued,
                })
                .collect(),
        })
    }

    /// Take an accepted proposal's label off its mail (in the background
    /// unless `dry_run`); `reject` leaves the proposal rejected, not pending.
    pub async fn undo_label_proposal(
        &self,
        id: &str,
        reject: bool,
        dry_run: bool,
    ) -> Result<UndoReport> {
        let r = self
            .tagging()
            .undo_label_proposal(pb::UndoLabelProposalRequest {
                id: id.into(),
                dry_run,
                reject,
            })
            .await?
            .into_inner();
        Ok(UndoReport {
            dry_run: r.dry_run,
            to_unlabel: r.to_unlabel,
            to_unarchive: r.to_unarchive,
            accounts: r.accounts.into_iter().map(account_count).collect(),
            labels_to_delete: r.labels_to_delete,
            status: r.status,
        })
    }

    /// Rename `from` to `to` on every account of its group, or merge it into
    /// `to` when that exists (in the background unless `dry_run`).
    pub async fn rename_label(
        &self,
        from: &str,
        to: &str,
        account_id: Option<&str>,
        mail_only: bool,
        dry_run: bool,
    ) -> Result<LabelChange> {
        let r = self
            .tagging()
            .rename_label(pb::RenameLabelRequest {
                from: from.into(),
                to: to.into(),
                account_id: account_id.map(str::to_owned),
                dry_run,
                mail_only,
            })
            .await?
            .into_inner();
        Ok(label_change(r))
    }

    /// Take a label off all its mail and delete it on every account of its
    /// group (in the background unless `dry_run`).
    pub async fn delete_label(
        &self,
        label: &str,
        account_id: Option<&str>,
        dry_run: bool,
    ) -> Result<LabelChange> {
        let r = self
            .tagging()
            .delete_label(pb::DeleteLabelRequest {
                label: label.into(),
                account_id: account_id.map(str::to_owned),
                dry_run,
            })
            .await?
            .into_inner();
        Ok(label_change(r))
    }

    /// An existing label, closely: one entry per training scope that has it.
    pub async fn get_label(
        &self,
        label: &str,
        account_id: Option<&str>,
        samples: u32,
    ) -> Result<Vec<LabelDetail>> {
        let resp = self
            .tagging()
            .get_label(pb::GetLabelRequest {
                label: label.into(),
                account_id: account_id.map(str::to_owned),
                samples,
            })
            .await?
            .into_inner();
        Ok(resp
            .labels
            .into_iter()
            .map(|d| LabelDetail {
                scope: d.scope,
                group: d.group,
                account_ids: d.account_ids,
                name: d.name,
                status: status(d.status.unwrap_or_default()),
                accounts: d
                    .accounts
                    .into_iter()
                    .map(|p| LabelPresence {
                        account_id: p.account_id,
                        kind: p.kind,
                        mailbox_id: p.mailbox_id,
                        path: p.path,
                        messages: p.messages,
                        keyword: p.keyword,
                        examples: p.examples,
                    })
                    .collect(),
                samples: d.samples.into_iter().map(message).collect(),
            })
            .collect())
    }
}
