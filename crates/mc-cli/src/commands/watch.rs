use anyhow::Result;
use colored::Colorize;
use mc_client::{MailCatClient, types::MailboxNotificationKind};
use tokio_stream::StreamExt;

pub async fn run(mailbox_id: String, client: MailCatClient) -> Result<()> {
    println!(
        "{} Watching mailbox {} for real-time events (Ctrl+C to stop)",
        "→".blue(),
        mailbox_id.bold(),
    );
    println!(
        "{}",
        "  (Flag changes trigger ApplyRemoteFlagChange on the server)".dimmed()
    );

    let mut stream = client.watch_mailbox(&mailbox_id).await?;

    while let Some(result) = stream.next().await {
        let notification = result?;

        let label = match &notification.event {
            MailboxNotificationKind::FlagsChanged => {
                format!("{}", "FLAGS CHANGED".cyan().bold())
            }
            MailboxNotificationKind::CountsChanged { total, unseen } => {
                format!(
                    "{} — total={} unseen={}",
                    "COUNTS CHANGED".yellow().bold(),
                    total,
                    unseen
                )
            }
            MailboxNotificationKind::NewMessage { remote_id } => {
                format!("{} — id={}", "NEW MESSAGE".green().bold(), remote_id)
            }
            MailboxNotificationKind::MessageExpunged { message_id } => {
                format!("{} — message={}", "EXPUNGED".red().bold(), message_id)
            }
        };

        println!("  [{}] {}", "event".dimmed(), label);
    }

    println!("{}", "Stream ended.".dimmed());
    Ok(())
}
