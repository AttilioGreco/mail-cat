use anyhow::Result;
use clap::Subcommand;
use colored::Colorize;
use mc_client::MailCatClient;
use tabled::{Table, Tabled};

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
    /// List the labels discovery proposes
    Proposals,
}

pub async fn run(cmd: LabelsCmd, client: MailCatClient, json: bool) -> Result<()> {
    match cmd {
        LabelsCmd::Eval { account } => eval(&account, client, json).await,
        LabelsCmd::Stats => stats(client, json).await,
        LabelsCmd::Discover { account } => discover(account.as_deref(), client, json).await,
        LabelsCmd::Proposals => proposals(client, json).await,
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

async fn stats(client: MailCatClient, json: bool) -> Result<()> {
    let s = client.get_background_stats().await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&s)?);
        return Ok(());
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
    Ok(())
}

async fn discover(account: Option<&str>, client: MailCatClient, json: bool) -> Result<()> {
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
    proposals(client, json).await
}

#[derive(Tabled)]
struct ProposalRow {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Messages")]
    size: u32,
    #[tabled(rename = "In Inbox")]
    inbox: u32,
    #[tabled(rename = "Keywords")]
    keywords: String,
    #[tabled(rename = "Example")]
    example: String,
    #[tabled(rename = "ID")]
    id: String,
}

async fn proposals(client: MailCatClient, json: bool) -> Result<()> {
    let found = client.list_label_proposals().await?;
    if json {
        return super::print_json(&found);
    }
    if found.is_empty() {
        println!("{}", "No labels proposed.".yellow());
        return Ok(());
    }
    let rows: Vec<ProposalRow> = found
        .into_iter()
        .map(|p| ProposalRow {
            example: p
                .samples
                .first()
                .map(|s| s.subject.clone())
                .unwrap_or_default(),
            keywords: p.keywords.join(" · "),
            name: p.name,
            size: p.size,
            inbox: p.inbox,
            id: p.id,
        })
        .collect();
    println!("{}", Table::new(rows));
    Ok(())
}
