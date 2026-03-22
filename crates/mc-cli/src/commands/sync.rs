use anyhow::Result;
use colored::Colorize;
use mc_client::{MailCatClient, SyncPhase};
use tokio_stream::StreamExt;

pub async fn run(account_id: String, full: bool, client: MailCatClient) -> Result<()> {
    println!(
        "{} Syncing account {} ({})",
        "→".blue(),
        account_id.bold(),
        if full { "full" } else { "incremental" }
    );

    let mut stream = client.sync_account(&account_id, full).await?;

    while let Some(event) = stream.next().await {
        let progress = event?;

        let phase_str = match &progress.phase {
            SyncPhase::DiscoveringMailboxes => "Discovering mailboxes".cyan().to_string(),
            SyncPhase::FetchingEnvelopes => "Fetching envelopes   ".yellow().to_string(),
            SyncPhase::FetchingBodies => "Fetching bodies      ".yellow().to_string(),
            SyncPhase::Indexing => "Indexing             ".magenta().to_string(),
            SyncPhase::Complete => "Complete             ".green().bold().to_string(),
            SyncPhase::Error(msg) => format!("Error: {msg}").red().to_string(),
        };

        let mailbox = progress
            .mailbox_name
            .as_deref()
            .map(|m| format!(" [{m}]").dimmed().to_string())
            .unwrap_or_default();

        let count = match progress.total {
            Some(total) => format!(" {}/{}", progress.processed, total),
            None if progress.processed > 0 => format!(" {}", progress.processed),
            None => String::new(),
        };

        println!("  {phase_str}{mailbox}{}", count.dimmed());

        if matches!(progress.phase, SyncPhase::Complete | SyncPhase::Error(_)) {
            break;
        }
    }

    Ok(())
}
