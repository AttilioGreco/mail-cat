//! `mc labels assign-matching|apply-all`: label existing mail in bulk, by who
//! sent it or by what the classifier has learnt.

use anyhow::Result;
use clap::Args;
use colored::Colorize;
use mc_client::{ApplyAllReport, ApplyPreviewItem, MailCatClient, MatchReport};
use tabled::{Table, Tabled};

use super::super::{Format, print_json};
use super::{proposals::optional_account_id, render};

#[derive(Args)]
pub struct AssignArgs {
    /// Label path, any case; created on the accounts that lack it
    label: String,
    /// Sender address, or a domain that also covers its subdomains
    /// (`facile.it` matches `preventivo@remind.facile.it`); repeatable
    #[arg(long)]
    from: Vec<String>,
    /// List-Id of a mailing list; repeatable
    #[arg(long)]
    list_id: Vec<String>,
    /// Account ID, name or email; selects the training group (needed when the
    /// label is new and there are several groups)
    #[arg(long)]
    account: Option<String>,
    /// Also take the mail out of the Inbox
    #[arg(long)]
    archive: bool,
    /// Do it (in the background); without it only prints what would happen
    #[arg(long)]
    yes: bool,
}

#[derive(Args)]
pub struct ApplyAllArgs {
    /// Only these labels, any case; repeatable (default: every label that is
    /// not off, and in the preview every label with enough examples)
    #[arg(long = "label")]
    labels: Vec<String>,
    /// Account ID, name or email; only its training group (all when left out)
    #[arg(long)]
    account: Option<String>,
    /// Do it (in the background); without it only prints what would happen
    #[arg(long)]
    yes: bool,
}

pub async fn assign(args: AssignArgs, client: MailCatClient, fmt: Format) -> Result<()> {
    let account = optional_account_id(&client, args.account.as_deref()).await?;
    let report = client
        .assign_matching_mail(
            &args.label,
            &args.from,
            &args.list_id,
            account.as_deref(),
            args.archive,
            !args.yes,
        )
        .await?;
    match fmt {
        Format::Json => return print_json(&report),
        Format::Toon => {
            render::match_report(&report).print();
            return Ok(());
        }
        Format::Table => {}
    }
    show_match(&report);
    Ok(())
}

fn show_match(r: &MatchReport) {
    let creates = if r.creates {
        " (the label will be created)"
    } else {
        ""
    };
    println!("{}{creates}", r.label.bold());
    println!(
        "{} messages to label, {} leave the Inbox, {} already have it",
        r.to_label, r.to_archive, r.already
    );
    for a in &r.accounts {
        let created = if a.creates { ", creates the label" } else { "" };
        println!(
            "  {}: {} to label, {} archived, {} already{created}",
            a.account_id, a.messages, a.archived, a.already
        );
    }
    samples(&r.sample);
    finish(r.dry_run);
}

fn samples(items: &[ApplyPreviewItem]) {
    for i in items {
        println!("    {}  {}", i.from, i.subject.dimmed());
    }
}

fn finish(dry_run: bool) {
    if dry_run {
        println!(
            "{}",
            "Dry run: nothing changed. Add --yes to do it.".yellow()
        );
    } else {
        println!("Working in the background: `mc labels stats`.");
    }
}

pub async fn apply_all(args: ApplyAllArgs, client: MailCatClient, fmt: Format) -> Result<()> {
    let account = optional_account_id(&client, args.account.as_deref()).await?;
    let report = client
        .apply_all_labels(&args.labels, account.as_deref(), !args.yes)
        .await?;
    match fmt {
        Format::Json => return print_json(&report),
        Format::Toon => {
            render::apply_all(&report).print();
            return Ok(());
        }
        Format::Table => {}
    }
    show_all(&report);
    Ok(())
}

#[derive(Tabled)]
struct Row {
    #[tabled(rename = "Label")]
    name: String,
    #[tabled(rename = "Mode")]
    mode: String,
    #[tabled(rename = "Threshold")]
    threshold: String,
    #[tabled(rename = "Examples")]
    positives: u32,
    #[tabled(rename = "To label")]
    to_label: u32,
}

fn show_all(r: &ApplyAllReport) {
    if r.labels.is_empty() {
        println!("{}", "No label to apply.".yellow());
        return;
    }
    let rows = r.labels.iter().map(|l| Row {
        name: l.name.clone(),
        mode: l.mode.clone(),
        threshold: format!("{:.2}", l.threshold),
        positives: l.positives,
        to_label: l.to_label,
    });
    println!("{}", Table::new(rows));
    println!(
        "{} messages to label with {} labels; modes stay as they are and nothing is archived",
        r.to_label,
        r.labels.len()
    );
    finish(r.dry_run);
}
