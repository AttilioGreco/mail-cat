//! The TOON form of what the `labels` commands print (see `crate::toon`).
//! Field names are those of the JSON.

use mc_client::{
    AccountCount, BackgroundStats, LabelDetail, LabelProposal, LabelScope, PlanReport,
    ProposalDetail, ProposalMessage, UndoReport,
};

use crate::toon::{Toon, l, n, s};

fn senders(p: &LabelProposal) -> Vec<String> {
    p.senders
        .iter()
        .map(|x| format!("{}({})", x.domain, x.count))
        .collect()
}

pub fn proposals(found: &[LabelProposal]) -> Toon {
    let rows: Vec<_> = found
        .iter()
        .map(|p| {
            vec![
                s(&p.id),
                s(&p.name),
                n(p.size),
                n(p.inbox),
                n(p.curated),
                s(&p.status),
                s(&p.group),
                l(&p.keywords),
                l(senders(p)),
            ]
        })
        .collect();
    let mut t = Toon::default();
    t.table(
        "proposals",
        &[
            "id", "name", "size", "inbox", "curated", "status", "group", "keywords", "senders",
        ],
        &rows,
    );
    t
}

fn messages(t: &mut Toon, name: &str, found: &[ProposalMessage]) {
    let rows: Vec<_> = found
        .iter()
        .map(|m| {
            vec![
                s(&m.message_id),
                s(&m.received_at),
                s(&m.from),
                s(&m.subject),
                s(&m.excerpt),
            ]
        })
        .collect();
    t.table(
        name,
        &["messageId", "receivedAt", "from", "subject", "excerpt"],
        &rows,
    );
}

pub fn proposal(detail: &ProposalDetail) -> Toon {
    let p = &detail.proposal;
    let mut t = Toon::default();
    t.field("id", s(&p.id))
        .field("name", s(&p.name))
        .field("status", s(&p.status))
        .field("curated", n(p.curated))
        .field("group", s(&p.group))
        .field("scope", s(&p.scope))
        .field("size", n(p.size))
        .field("inbox", n(p.inbox))
        .list("keywords", &p.keywords)
        .list("senders", &senders(p));
    messages(&mut t, "samples", &detail.messages);
    t
}

pub fn labels(scopes: &[LabelScope]) -> Toon {
    let rows: Vec<_> = scopes
        .iter()
        .flat_map(|sc| {
            sc.labels.iter().map(move |label| {
                vec![
                    s(&sc.scope),
                    s(&sc.group),
                    s(&label.name),
                    s(&label.status.label),
                    s(&label.status.mode),
                    n(label.status.threshold),
                    n(label.status.positives),
                ]
            })
        })
        .collect();
    let mut t = Toon::default();
    t.table(
        "labels",
        &[
            "scope",
            "group",
            "name",
            "label",
            "mode",
            "threshold",
            "positives",
        ],
        &rows,
    );
    t
}

pub fn label(d: &LabelDetail) -> Toon {
    let mut t = Toon::default();
    t.field("name", s(&d.name))
        .field("label", s(&d.status.label))
        .field("scope", s(&d.scope))
        .field("group", s(&d.group))
        .field("mode", s(&d.status.mode))
        .field("threshold", n(d.status.threshold))
        .field("positives", n(d.status.positives))
        .field("minPositives", n(d.status.min_positives));
    let rows: Vec<_> = d
        .accounts
        .iter()
        .map(|a| {
            vec![
                s(&a.account_id),
                s(&a.kind),
                s(&a.path),
                n(a.messages),
                s(&a.keyword),
                n(a.examples),
            ]
        })
        .collect();
    t.table(
        "accounts",
        &[
            "accountId",
            "kind",
            "path",
            "messages",
            "keyword",
            "examples",
        ],
        &rows,
    );
    messages(&mut t, "samples", &d.samples);
    t
}

fn counts(t: &mut Toon, accounts: &[AccountCount]) {
    let rows: Vec<_> = accounts
        .iter()
        .map(|a| vec![s(&a.account_id), n(a.messages), n(a.archived)])
        .collect();
    t.table("accounts", &["accountId", "messages", "archived"], &rows);
}

pub fn plan(r: &PlanReport) -> Toon {
    let m = &r.summary;
    let mut t = Toon::default();
    t.field("dryRun", n(r.dry_run))
        .field("merged", n(m.merged))
        .field("renamed", n(m.renamed))
        .field("accepted", n(m.accepted))
        .field("assigned", n(m.assigned))
        .field("rejected", n(m.rejected))
        .field("modes", n(m.modes))
        .field("toLabel", n(m.to_label))
        .field("toArchive", n(m.to_archive));
    let creations: Vec<_> = m
        .labels_to_create
        .iter()
        .map(|c| vec![s(&c.name), l(&c.account_ids)])
        .collect();
    t.table("labelsToCreate", &["name", "accountIds"], &creations);
    t.list("labelsTouched", &m.labels_touched);
    counts(&mut t, &m.accounts);
    let results: Vec<_> = r
        .results
        .iter()
        .map(|x| {
            vec![
                n(x.index),
                s(&x.kind),
                s(&x.status),
                s(&x.detail),
                s(x.proposal_id.as_deref().unwrap_or_default()),
                x.queued.map_or_else(String::new, n),
            ]
        })
        .collect();
    t.table(
        "results",
        &["index", "kind", "status", "detail", "proposalId", "queued"],
        &results,
    );
    t
}

pub fn undo(r: &UndoReport) -> Toon {
    let mut t = Toon::default();
    t.field("dryRun", n(r.dry_run))
        .field("toUnlabel", n(r.to_unlabel))
        .field("toUnarchive", n(r.to_unarchive))
        .field("status", s(&r.status))
        .list("labelsToDelete", &r.labels_to_delete);
    counts(&mut t, &r.accounts);
    t
}

pub fn stats(b: &BackgroundStats) -> Toon {
    let mut t = Toon::default();
    t.field("speed", s(&b.speed))
        .field("state", s(&b.state))
        .field("model", s(&b.model))
        .field("modelLoaded", n(b.model_loaded))
        .field("embedded", n(b.embedded))
        .field("eligible", n(b.eligible))
        .field("perSecond", n(format!("{:.1}", b.per_second)))
        .field("etaSeconds", n(b.eta_seconds));
    if let Some(d) = &b.discovery {
        t.field(
            "discovery",
            s(&format!("{} {} {}/{}", d.scope, d.phase, d.done, d.total)),
        );
    }
    let retrains: Vec<_> = b
        .retrains
        .iter()
        .map(|r| vec![s(&r.scope), n(r.done), n(r.total)])
        .collect();
    t.table("retrains", &["scope", "done", "total"], &retrains);
    let applying: Vec<_> = b
        .applying
        .iter()
        .map(|a| {
            vec![
                s(&a.label),
                s(&a.kind),
                n(a.done),
                n(a.total),
                n(a.archived),
            ]
        })
        .collect();
    t.table(
        "applying",
        &["label", "kind", "done", "total", "archived"],
        &applying,
    );
    t
}
