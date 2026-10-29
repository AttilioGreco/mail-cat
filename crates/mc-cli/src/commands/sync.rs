use anyhow::{Context, Result};
use colored::Colorize;
use mc_client::{MailCatClient, SyncPhase};
use tokio_stream::StreamExt;

pub async fn run(
    account_id: String,
    full: bool,
    mailbox: Option<String>,
    client: MailCatClient,
) -> Result<()> {
    let path = match &mailbox {
        Some(wanted) => Some(resolve_mailbox(&client, &account_id, wanted).await?),
        None => None,
    };
    println!(
        "{} Syncing account {} ({})",
        "→".blue(),
        account_id.bold(),
        match &path {
            Some(path) => path.as_str(),
            None if full => "full",
            None => "incremental",
        }
    );

    let mut stream = client
        .sync_account(&account_id, full, path.as_deref())
        .await?;

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

async fn resolve_mailbox(client: &MailCatClient, account_id: &str, wanted: &str) -> Result<String> {
    client
        .list_mailboxes(account_id)
        .await?
        .into_iter()
        .find(|m| {
            m.id == wanted
                || m.path == wanted
                || m.display_path == wanted
                || m.display_name == wanted
        })
        .map(|m| m.path)
        .with_context(|| format!("no mailbox matching {wanted:?} (id, path, display path or name)"))
}
