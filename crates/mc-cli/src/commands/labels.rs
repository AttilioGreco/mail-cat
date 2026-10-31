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
}

pub async fn run(cmd: LabelsCmd, client: MailCatClient, json: bool) -> Result<()> {
    match cmd {
        LabelsCmd::Eval { account } => eval(&account, client, json).await,
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
