//! `mc labels proposals|list|mode`: the commands an assistant (or a person)
//! uses to curate discovered labels. See docs/mc-labels-cli.md.

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand, ValueEnum};
use colored::Colorize;
use mc_client::{ApplyPreview, LabelProposal, MailCatClient};
use serde_json::json;
use tabled::{Table, Tabled};

use super::super::accounts::find_account;
use super::super::{Format, print_json};
use super::{plan, render};

#[derive(Clone, Copy, ValueEnum)]
pub enum StatusArg {
    Pending,
    Accepted,
    Rejected,
    All,
}

impl StatusArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::All => "all",
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum ModeArg {
    Off,
    Suggest,
    Apply,
    #[value(name = "apply_archive")]
    ApplyArchive,
}

impl ModeArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Suggest => "suggest",
            Self::Apply => "apply",
            Self::ApplyArchive => "apply_archive",
        }
    }

    fn applies(self) -> bool {
        matches!(self, Self::Apply | Self::ApplyArchive)
    }
}

#[derive(Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct ProposalsArgs {
    /// Account ID, name or email; only its training group (all when left out)
    #[arg(long)]
    account: Option<String>,
    /// Which proposals to list
    #[arg(long, value_enum, default_value = "pending")]
    status: StatusArg,
    #[command(subcommand)]
    cmd: Option<ProposalsCmd>,
}

#[derive(Subcommand)]
enum ProposalsCmd {
    /// One proposal with sample messages
    Show {
        /// Proposal ID (full UUID, from `mc labels proposals`)
        id: String,
        /// How many sample messages to print
        #[arg(long, default_value_t = 10)]
        samples: u32,
    },
    /// Change a pending proposal's name
    Rename { id: String, name: String },
    /// Replace pending proposals of one training group by a single new one
    Merge {
        #[arg(required = true, num_args = 2..)]
        ids: Vec<String>,
        /// Name of the merged proposal (default: from its keywords)
        #[arg(long)]
        name: Option<String>,
    },
    /// Create the label and put it on the proposal's mail (in the background)
    Accept {
        id: String,
        /// The label's name (default: the proposal's)
        #[arg(long)]
        name: Option<String>,
        /// Also take the mail out of the Inbox
        #[arg(long)]
        archive: bool,
    },
    /// Refuse the proposal; the same cluster is not proposed again
    Reject { id: String },
    /// Check a curation plan (a JSON file of ordered steps) and, with --yes,
    /// do it. Nothing is applied unless the whole plan validates
    Plan {
        /// JSON file; see docs/mc-labels-cli.md for the schema
        file: std::path::PathBuf,
        /// Only check it and print what it would do (also the default
        /// without --yes)
        #[arg(long)]
        dry_run: bool,
        /// Do it
        #[arg(long)]
        yes: bool,
    },
    /// Take an accepted proposal's label off its mail again, move archived
    /// mail back to the Inbox and delete the label if it created it and it
    /// is empty. Prints what it would do until --yes
    Undo {
        id: String,
        /// Do it (in the background)
        #[arg(long)]
        yes: bool,
        /// The proposal becomes rejected instead of pending again
        #[arg(long)]
        reject: bool,
    },
    /// Put a label the group already has on the proposal's mail instead of
    /// creating a new one
    Assign {
        id: String,
        /// Existing label, any case (see `mc labels list`)
        label: String,
        /// Also take the mail out of the Inbox
        #[arg(long)]
        archive: bool,
    },
}

#[derive(Args)]
pub struct ModeArgs {
    /// Label name, any case
    label: String,
    #[arg(value_enum)]
    mode: ModeArg,
    /// Confidence from which apply modes act, 0.5 to 1.0 (default: the label's)
    #[arg(long)]
    threshold: Option<f64>,
    /// Account ID, name or email (needed when there is more than one)
    #[arg(long)]
    account: Option<String>,
    /// Also label the mail already there (apply modes)
    #[arg(long)]
    existing: bool,
    /// Do it; apply modes only print what they would do without it
    #[arg(long)]
    yes: bool,
}

pub async fn run(args: ProposalsArgs, client: MailCatClient, fmt: Format) -> Result<()> {
    let json = fmt == Format::Json;
    let Some(cmd) = args.cmd else {
        return list_proposals(args.account.as_deref(), args.status, client, fmt).await;
    };
    match cmd {
        ProposalsCmd::Show { id, samples } => show(&id, samples, client, fmt).await,
        ProposalsCmd::Plan { file, dry_run, yes } => {
            plan::run(&file, dry_run || !yes, client, fmt).await
        }
        ProposalsCmd::Undo { id, yes, reject } => plan::undo(&id, !yes, reject, client, fmt).await,
        ProposalsCmd::Rename { id, name } => {
            let renamed = client.rename_label_proposal(&id, &name).await?;
            report(&renamed, "Renamed", json)
        }
        ProposalsCmd::Merge { ids, name } => {
            let merged = client.merge_label_proposals(&ids, name.as_deref()).await?;
            report(&merged, "Merged", json)
        }
        ProposalsCmd::Accept { id, name, archive } => {
            let queued = client
                .accept_label_proposal(&id, name.as_deref(), archive)
                .await?;
            answered(&id, "accepted", Some(queued), json)
        }
        ProposalsCmd::Reject { id } => {
            client.reject_label_proposal(&id).await?;
            answered(&id, "rejected", None, json)
        }
        ProposalsCmd::Assign { id, label, archive } => {
            let queued = client.assign_label_proposal(&id, &label, archive).await?;
            answered(&id, "accepted", Some(queued), json)
        }
    }
}

/// `account` as an ID: the one given (ID, name or email), else the only account.
async fn account_id(client: &MailCatClient, account: Option<&str>) -> Result<String> {
    if let Some(wanted) = account {
        return Ok(find_account(client, wanted).await?.id);
    }
    let mut accounts = client.list_accounts().await?;
    if accounts.len() != 1 {
        bail!("there are {} accounts: pass --account", accounts.len());
    }
    Ok(accounts.remove(0).id)
}

async fn optional_account_id(
    client: &MailCatClient,
    account: Option<&str>,
) -> Result<Option<String>> {
    match account {
        Some(wanted) => Ok(Some(find_account(client, wanted).await?.id)),
        None => Ok(None),
    }
}

#[derive(Tabled)]
struct ProposalRow {
    #[tabled(rename = "ID")]
    id: String,
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Msgs")]
    size: u32,
    #[tabled(rename = "Inbox")]
    inbox: u32,
    #[tabled(rename = "Keywords")]
    keywords: String,
    #[tabled(rename = "Senders")]
    senders: String,
    #[tabled(rename = "Group")]
    group: String,
    #[tabled(rename = "Status")]
    status: String,
}

fn senders_line(p: &LabelProposal, shown: usize) -> String {
    p.senders
        .iter()
        .take(shown)
        .map(|s| format!("{}×{}", s.domain, s.count))
        .collect::<Vec<_>>()
        .join(" ")
}

pub async fn list_proposals(
    account: Option<&str>,
    status: StatusArg,
    client: MailCatClient,
    fmt: Format,
) -> Result<()> {
    let account = optional_account_id(&client, account).await?;
    let found = client
        .find_label_proposals(account.as_deref(), status.as_str(), true)
        .await?;
    match fmt {
        Format::Json => return print_json(&found),
        Format::Toon => {
            render::proposals(&found).print();
            return Ok(());
        }
        Format::Table => {}
    }
    if found.is_empty() {
        println!("{}", "No labels proposed.".yellow());
        return Ok(());
    }
    let rows = found.iter().map(|p| ProposalRow {
        id: p.id.clone(),
        name: p.name.clone(),
        size: p.size,
        inbox: p.inbox,
        keywords: p
            .keywords
            .iter()
            .take(5)
            .cloned()
            .collect::<Vec<_>>()
            .join(" · "),
        senders: senders_line(p, 3),
        group: p.group.clone(),
        status: if p.curated {
            format!("{} (curated)", p.status)
        } else {
            p.status.clone()
        },
    });
    println!("{}", Table::new(rows));
    Ok(())
}

async fn show(id: &str, samples: u32, client: MailCatClient, fmt: Format) -> Result<()> {
    let detail = client.get_label_proposal(id, samples).await?;
    match fmt {
        Format::Json => return print_json(&detail),
        Format::Toon => {
            render::proposal(&detail).print();
            return Ok(());
        }
        Format::Table => {}
    }
    let p = &detail.proposal;
    println!("{} ({}) {}", p.name.bold(), p.status, p.id);
    println!(
        "Group: {} · {} messages, {} in the Inbox",
        p.group, p.size, p.inbox
    );
    println!("Keywords: {}", p.keywords.join(" · "));
    println!("Senders: {}", senders_line(p, 8));
    for m in &detail.messages {
        let day = m.received_at.get(..10).unwrap_or(&m.received_at);
        println!("\n{day}  {}  {}", m.from, m.subject.bold());
        if !m.excerpt.is_empty() {
            println!("    {}", m.excerpt.dimmed());
        }
        println!("    {}", m.message_id.dimmed());
    }
    Ok(())
}

fn report(p: &LabelProposal, verb: &str, json: bool) -> Result<()> {
    if json {
        return print_json(p);
    }
    println!(
        "{verb}: {} ({} messages, {}) {}",
        p.name.bold(),
        p.size,
        p.keywords
            .iter()
            .take(5)
            .cloned()
            .collect::<Vec<_>>()
            .join(" · "),
        p.id
    );
    Ok(())
}

fn answered(id: &str, status: &str, queued: Option<u32>, json: bool) -> Result<()> {
    if json {
        return print_json(&json!({ "id": id, "status": status, "queued": queued }));
    }
    match queued {
        Some(n) => println!("{id}: {status}, labelling {n} messages in the background"),
        None => println!("{id}: {status}"),
    }
    Ok(())
}

pub async fn list(account: Option<&str>, client: MailCatClient, fmt: Format) -> Result<()> {
    let account = optional_account_id(&client, account).await?;
    let scopes = client.list_labels(account.as_deref()).await?;
    match fmt {
        Format::Json => return print_json(&scopes),
        Format::Toon => {
            render::labels(&scopes).print();
            return Ok(());
        }
        Format::Table => {}
    }
    #[derive(Tabled)]
    struct Row {
        #[tabled(rename = "Label")]
        name: String,
        #[tabled(rename = "Examples")]
        positives: u32,
        #[tabled(rename = "Mode")]
        mode: String,
        #[tabled(rename = "Threshold")]
        threshold: String,
    }
    for scope in scopes {
        let title = if scope.group.is_empty() {
            scope.scope.as_str()
        } else {
            scope.group.as_str()
        };
        println!("{} ({} accounts)", title.bold(), scope.account_ids.len());
        let rows = scope.labels.into_iter().map(|l| Row {
            name: l.name,
            positives: l.status.positives,
            mode: l.status.mode,
            threshold: format!("{:.2}", l.status.threshold),
        });
        println!("{}", Table::new(rows));
    }
    Ok(())
}

/// One existing label: settings, where each account keeps it, recent mail.
pub async fn show_label(
    label: &str,
    account: Option<&str>,
    samples: u32,
    client: MailCatClient,
    fmt: Format,
) -> Result<()> {
    let account = optional_account_id(&client, account).await?;
    let found = client.get_label(label, account.as_deref(), samples).await?;
    match fmt {
        Format::Json => return print_json(&found),
        Format::Toon => {
            for d in &found {
                render::label(d).print();
            }
            return Ok(());
        }
        Format::Table => {}
    }
    for d in &found {
        println!("{} ({}) {}", d.name.bold(), d.status.mode, d.group);
        println!(
            "{} examples (needs {}), threshold {:.2}",
            d.status.positives, d.status.min_positives, d.status.threshold
        );
        for a in &d.accounts {
            match a.kind.as_str() {
                "mailbox" => println!(
                    "  {}: mailbox {} ({} messages), {} examples",
                    a.account_id, a.path, a.messages, a.examples
                ),
                "keyword" => println!(
                    "  {}: keyword {}, {} examples",
                    a.account_id, a.keyword, a.examples
                ),
                _ => println!("  {}: not there yet", a.account_id),
            }
        }
        for m in &d.samples {
            let day = m.received_at.get(..10).unwrap_or(&m.received_at);
            println!("\n{day}  {}  {}", m.from, m.subject.bold());
            if !m.excerpt.is_empty() {
                println!("    {}", m.excerpt.dimmed());
            }
        }
    }
    Ok(())
}

/// Apply modes act on mail by themselves, so they wait for `--yes`; off and
/// suggest touch nothing.
pub(super) fn is_dry_run(mode: ModeArg, yes: bool) -> bool {
    mode.applies() && !yes
}

pub async fn mode(args: ModeArgs, client: MailCatClient, json: bool) -> Result<()> {
    if args.existing && !args.mode.applies() {
        bail!("--existing only goes with apply or apply_archive");
    }
    let account = account_id(&client, args.account.as_deref()).await?;
    let label = &args.label;
    let threshold = match args.threshold {
        Some(t) => t,
        None => client.get_label_settings(&account, label).await?.threshold,
    };
    let mode = args.mode.as_str();

    let dry_run = is_dry_run(args.mode, args.yes);
    let preview: Option<ApplyPreview> = if args.existing {
        Some(
            client
                .preview_apply(&account, label, mode, threshold)
                .await?,
        )
    } else {
        None
    };
    if !dry_run {
        if args.existing {
            client
                .confirm_apply(&account, label, mode, threshold)
                .await?;
        } else {
            client
                .set_label_settings(&account, label, mode, threshold)
                .await
                .with_context(|| format!("setting {label:?} to {mode}"))?;
        }
    }
    if json {
        return print_json(&json!({
            "label": args.label,
            "mode": mode,
            "threshold": threshold,
            "dryRun": dry_run,
            "existingMail": preview,
        }));
    }
    if let Some(p) = &preview {
        println!(
            "{} existing messages would be labelled, {} also leave the Inbox",
            p.to_label, p.to_archive
        );
    }
    if dry_run {
        println!(
            "{}",
            "Dry run: nothing changed. Add --yes to do it.".yellow()
        );
    } else {
        println!("{label}: {mode} at {threshold:.2}");
    }
    Ok(())
}
