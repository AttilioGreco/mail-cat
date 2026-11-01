mod matching;
mod plan;
mod proposals;
mod relabel;
mod render;
#[cfg(test)]
mod tests;

use anyhow::Result;
use clap::Subcommand;
use colored::Colorize;
use mc_client::MailCatClient;
use tabled::{Table, Tabled};

use super::{Format, print_json};

#[derive(Subcommand)]
pub enum LabelsCmd {
    /// Hold out part of the labelled mail and print the classifier's
    /// precision and recall per label and threshold
    Eval {
        /// Account ID (from `mc accounts list`); its training group is evaluated
        #[arg(long)]
        account: String,
    },
    /// Show what the background embedding and retraining are doing
    Stats,
    /// Look for new labels in similar unlabelled mail, wait for the search to
    /// end and list what it found
    Discover {
        /// Account ID (from `mc accounts list`); its training group is searched
        /// (every group when left out)
        #[arg(long)]
        account: Option<String>,
    },
    /// Review, curate and answer the labels discovery proposes
    Proposals(proposals::ProposalsArgs),
    /// List the existing labels with their mode, threshold and example count
    List {
        /// Account ID, name or email; only its training group (all when left out)
        #[arg(long)]
        account: Option<String>,
    },
    /// One existing label: group scope, mode, threshold, examples, where
    /// each account keeps it, and recent mail under it
    Show {
        /// Label name, any case
        label: String,
        /// Account ID, name or email; only its training group (all that have
        /// the label when left out)
        #[arg(long)]
        account: Option<String>,
        /// How many recent messages to print
        #[arg(long, default_value_t = 10)]
        samples: u32,
    },
    /// Set what happens to mail the classifier is sure about. Apply modes
    /// only print what they would do until --yes
    Mode(proposals::ModeArgs),
    /// Rename an existing label on every account of its group, or move it
    /// under another path. A label that exists already is merged into. Prints
    /// what it would do until --yes
    Rename(relabel::RenameArgs),
    /// Take a label off all its mail and delete it on every account of its
    /// group; the mail is kept. Prints what it would do until --yes
    Delete(relabel::DeleteArgs),
    /// Put a label on all the mail from some senders, domains or mailing lists
    /// in its group (creating the label where missing); the model learns it
    /// as yours. Prints what it would do until --yes
    AssignMatching(matching::AssignArgs),
    /// Label the existing mail the classifier is sure of, for many labels at
    /// once. Modes are not changed and nothing is archived. Prints what it
    /// would do until --yes
    ApplyAll(matching::ApplyAllArgs),
}

pub async fn run(cmd: LabelsCmd, client: MailCatClient, fmt: Format) -> Result<()> {
    let json = fmt == Format::Json;
    match cmd {
        LabelsCmd::Eval { account } => eval(&account, client, json).await,
        LabelsCmd::Stats => stats(client, fmt).await,
        LabelsCmd::Discover { account } => discover(account.as_deref(), client, fmt).await,
        LabelsCmd::Proposals(args) => proposals::run(args, client, fmt).await,
        LabelsCmd::List { account } => proposals::list(account.as_deref(), client, fmt).await,
        LabelsCmd::Show {
            label,
            account,
            samples,
        } => proposals::show_label(&label, account.as_deref(), samples, client, fmt).await,
        LabelsCmd::Mode(args) => proposals::mode(args, client, json).await,
        LabelsCmd::Rename(args) => relabel::rename(args, client, fmt).await,
        LabelsCmd::Delete(args) => relabel::delete(args, client, fmt).await,
        LabelsCmd::AssignMatching(args) => matching::assign(args, client, fmt).await,
        LabelsCmd::ApplyAll(args) => matching::apply_all(args, client, fmt).await,
    }
}

#[derive(Tabled)]
struct Row {
    #[tabled(rename = "Label")]
    label: String,
    #[tabled(rename = "Held out")]
    held_out: u32,
    #[tabled(rename = "Signal")]
    signal: &'static str,
    #[tabled(rename = "Threshold")]
    threshold: String,
    #[tabled(rename = "Flagged")]
    flagged: u32,
    #[tabled(rename = "Correct")]
    correct: u32,
    #[tabled(rename = "Precision")]
    precision: String,
    #[tabled(rename = "Recall")]
    recall: String,
}

async fn eval(account: &str, client: MailCatClient, json: bool) -> Result<()> {
    let report = client.eval_labels(account).await?;
    if json {
        return super::print_json(&report);
    }
    if report.is_empty() {
        println!("{}", "No label has enough examples to evaluate.".yellow());
        return Ok(());
    }
    let rows: Vec<Row> = report
        .into_iter()
        .flat_map(|l| {
            let (label, held_out) = (l.label, l.held_out);
            // Without a vector in the held-out mail the two are the same.
            let signals = [("words", l.bayes_rows), ("words+knn", l.rows)];
            signals.into_iter().flat_map(move |(signal, rows)| {
                let label = label.clone();
                rows.into_iter().map(move |r| Row {
                    label: label.clone(),
                    held_out,
                    signal,
                    threshold: format!("{:.2}", r.threshold),
                    flagged: r.flagged,
                    correct: r.correct,
                    precision: format!("{:.0}%", r.precision * 100.0),
                    recall: format!("{:.0}%", r.recall * 100.0),
                })
            })
        })
        .collect();
    println!("{}", Table::new(rows));
    println!(
        "{}",
        "Mail under a label you never applied counts against precision. words+knn equals \
         words where the held-out mail has no embedding (embeddings off, or not yet done)."
            .dimmed()
    );
    Ok(())
}

async fn stats(client: MailCatClient, fmt: Format) -> Result<()> {
    let s = client.get_background_stats().await?;
    match fmt {
        Format::Json => return print_json(&s),
        Format::Toon => {
            render::stats(&s).print();
            return Ok(());
        }
        Format::Table => {}
    }
    println!(
        "Speed: {} ({} workers x {} threads, batch {}, pause {} ms)",
        s.speed, s.workers, s.threads, s.batch, s.pause_ms
    );
    println!(
        "Embedder: {} ({}, {})",
        s.state,
        if s.model.is_empty() {
            "no model"
        } else {
            &s.model
        },
        if s.model_loaded {
            "in memory"
        } else {
            "not loaded"
        }
    );
    println!("Embedded: {} / {}", s.embedded, s.eligible);
    if let Some(d) = &s.discovery {
        println!("Discovery {}: {} {}/{}", d.scope, d.phase, d.done, d.total);
    }
    println!("Throughput: {:.1} msg/s", s.per_second);
    if s.eta_seconds > 0 {
        println!("ETA: {} min", s.eta_seconds.div_ceil(60));
    }
    if s.worker_bytes > 0 {
        println!("Memory per worker: ~{} MB", s.worker_bytes / 1_000_000);
    }
    for r in &s.retrains {
        println!("Retraining {}: {} / {}", r.scope, r.done, r.total);
    }
    for a in &s.applying {
        let verb = match a.kind.as_str() {
            "undo" => "Undoing",
            "relabel" => "Renaming",
            "delete" => "Deleting",
            _ => "Applying",
        };
        println!(
            "{verb} {}: {} / {} (archived {})",
            a.label, a.done, a.total, a.archived
        );
    }
    Ok(())
}

async fn discover(account: Option<&str>, client: MailCatClient, fmt: Format) -> Result<()> {
    let json = fmt == Format::Json;
    if !client.discover_labels(account).await? {
        eprintln!(
            "{}",
            "A search was already running; waiting for it.".yellow()
        );
    }
    loop {
        // The search is asynchronous on the server; it is over once the
        // stats stop reporting it.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let Some(d) = client.get_background_stats().await?.discovery else {
            break;
        };
        if !json {
            eprint!("\r{} {} {}/{}   ", d.scope, d.phase, d.done, d.total);
        }
    }
    if !json {
        eprintln!();
    }
    proposals::list_proposals(None, proposals::StatusArg::Pending, client, fmt).await
}
