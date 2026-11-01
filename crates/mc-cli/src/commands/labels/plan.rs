//! `mc labels proposals plan|undo`: curation plans and undoing an acceptance.

use std::path::Path;

use anyhow::{Context, Result, bail};
use colored::Colorize;
use mc_client::{AccountCount, CurationOp, MailCatClient, PlanReport, UndoReport};
use serde::Deserialize;

use super::super::accounts::find_account;
use super::super::{Format, print_json};
use super::render;

/// The plan file: an array of steps, or `{"ops": [...]}`. Each step is an
/// object with one key naming the operation.
#[derive(Deserialize)]
#[serde(untagged)]
enum File {
    Steps(Vec<Step>),
    Wrapped { ops: Vec<Step> },
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Step {
    Merge(Merge),
    Rename(Rename),
    Assign(Assign),
    Accept(Accept),
    Reject(Reject),
    Mode(Mode),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Merge {
    ids: Vec<String>,
    name: Option<String>,
    /// Later steps name the result as `@alias`.
    #[serde(rename = "as")]
    alias: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rename {
    id: String,
    name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Assign {
    id: String,
    label: String,
    #[serde(default)]
    archive: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Accept {
    id: String,
    name: Option<String>,
    #[serde(default)]
    archive: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reject {
    id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Mode {
    label: String,
    mode: String,
    threshold: Option<f64>,
    #[serde(default)]
    existing: bool,
    /// Account ID, name or email; needed when the label is in several groups.
    account: Option<String>,
}

fn parse(text: &str) -> Result<Vec<Step>> {
    Ok(
        match serde_json::from_str(text).context("the plan is not valid")? {
            File::Steps(steps) | File::Wrapped { ops: steps } => steps,
        },
    )
}

async fn op_of(step: Step, client: &MailCatClient) -> Result<CurationOp> {
    let op = |kind: &str| CurationOp {
        kind: kind.into(),
        ..CurationOp::default()
    };
    Ok(match step {
        Step::Merge(m) => CurationOp {
            ids: m.ids,
            name: m.name.unwrap_or_default(),
            alias: m.alias.unwrap_or_default(),
            ..op("merge")
        },
        Step::Rename(r) => CurationOp {
            id: r.id,
            name: r.name,
            ..op("rename")
        },
        Step::Assign(a) => CurationOp {
            id: a.id,
            label: a.label,
            archive: a.archive,
            ..op("assign")
        },
        Step::Accept(a) => CurationOp {
            id: a.id,
            name: a.name.unwrap_or_default(),
            archive: a.archive,
            ..op("accept")
        },
        Step::Reject(r) => CurationOp {
            id: r.id,
            ..op("reject")
        },
        Step::Mode(m) => CurationOp {
            label: m.label,
            mode: m.mode,
            threshold: m.threshold,
            existing: m.existing,
            account_id: match m.account {
                Some(a) => Some(find_account(client, &a).await?.id),
                None => None,
            },
            ..op("mode")
        },
    })
}

pub async fn run(file: &Path, dry_run: bool, client: MailCatClient, fmt: Format) -> Result<()> {
    let text =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    let mut ops = Vec::new();
    for step in parse(&text)? {
        ops.push(op_of(step, &client).await?);
    }
    let report = client.apply_curation_plan(&ops, dry_run).await?;
    match fmt {
        Format::Json => print_json(&report)?,
        Format::Toon => render::plan(&report).print(),
        Format::Table => print_plan(&report),
    }
    if report.results.iter().any(|r| r.status == "failed") {
        bail!("the plan stopped at a failed step");
    }
    Ok(())
}

fn accounts_line(accounts: &[AccountCount]) -> String {
    accounts
        .iter()
        .map(|a| {
            format!(
                "{}: {} ({} leave the Inbox)",
                a.account_id, a.messages, a.archived
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn print_plan(r: &PlanReport) {
    let m = &r.summary;
    println!(
        "{} merges, {} renames, {} accepts, {} assigns, {} rejects, {} mode changes",
        m.merged, m.renamed, m.accepted, m.assigned, m.rejected, m.modes
    );
    println!(
        "{} messages to label, {} of them leave the Inbox",
        m.to_label, m.to_archive
    );
    if !m.accounts.is_empty() {
        println!("Per account: {}", accounts_line(&m.accounts));
    }
    for c in &m.labels_to_create {
        println!(
            "Creates {} on {} account(s)",
            c.name.bold(),
            c.account_ids.len()
        );
    }
    if !m.labels_touched.is_empty() {
        println!("Existing labels touched: {}", m.labels_touched.join(", "));
    }
    println!();
    for x in &r.results {
        println!("{:>3}. {:<6} {:<8} {}", x.index, x.kind, x.status, x.detail);
    }
    if r.dry_run {
        println!(
            "\n{}",
            "Dry run: nothing changed. Add --yes to do it.".yellow()
        );
    } else {
        println!("\nLabelling continues in the background: `mc labels stats`.");
    }
}

pub async fn undo(
    id: &str,
    dry_run: bool,
    reject: bool,
    client: MailCatClient,
    fmt: Format,
) -> Result<()> {
    let report = client.undo_label_proposal(id, reject, dry_run).await?;
    match fmt {
        Format::Json => return print_json(&report),
        Format::Toon => {
            render::undo(&report).print();
            return Ok(());
        }
        Format::Table => {}
    }
    print_undo(&report);
    Ok(())
}

fn print_undo(r: &UndoReport) {
    println!(
        "{} messages lose the label, {} return to the Inbox",
        r.to_unlabel, r.to_unarchive
    );
    if !r.accounts.is_empty() {
        println!("Per account: {}", accounts_line(&r.accounts));
    }
    if !r.labels_to_delete.is_empty() {
        println!("Deletes the label(s): {}", r.labels_to_delete.join(", "));
    }
    println!("The proposal becomes {}", r.status);
    if r.dry_run {
        println!(
            "{}",
            "Dry run: nothing changed. Add --yes to do it.".yellow()
        );
    } else {
        println!("Working in the background: `mc labels stats`.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plan_is_an_array_or_an_ops_object_of_one_key_steps() {
        let steps = parse(
            r#"[{"merge": {"ids": ["a", "b"], "as": "gh"}},
                {"accept": {"id": "@gh", "name": "Dev/GitHub", "archive": true}},
                {"reject": {"id": "c"}},
                {"mode": {"label": "news", "mode": "apply", "existing": true}}]"#,
        )
        .unwrap();
        assert_eq!(steps.len(), 4);
        assert!(matches!(&steps[0], Step::Merge(m) if m.alias.as_deref() == Some("gh")));
        assert!(matches!(&steps[1], Step::Accept(a) if a.archive && a.id == "@gh"));
        assert_eq!(
            parse(r#"{"ops": [{"reject": {"id": "c"}}]}"#)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn typos_are_errors_not_silence() {
        assert!(parse(r#"[{"accept": {"id": "a", "archived": true}}]"#).is_err());
        assert!(parse(r#"[{"accpet": {"id": "a"}}]"#).is_err());
        assert!(parse(r#"[{"merge": {"ids": ["a"]}, "reject": {"id": "b"}}]"#).is_err());
    }
}
