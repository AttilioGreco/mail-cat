//! CLI debug tool — watch INBOX mailboxes for real-time events.
//!
//! Usage:
//!   cargo run -p mc-client --example watch
//!   MAIL_CAT_ENDPOINT=http://[::1]:50051 cargo run -p mc-client --example watch

use mc_client::{MailCatClient, MailboxNotificationKind};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint =
        std::env::var("MAIL_CAT_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());

    println!("Connecting to {}...", endpoint);
    let client = MailCatClient::connect(&endpoint).await?;
    println!("Connected.\n");

    let accounts = client.list_accounts().await?;
    if accounts.is_empty() {
        println!("No accounts found.");
        return Ok(());
    }

    // Collect INBOX mailbox IDs per account
    let mut inboxes: Vec<(String, String, String)> = vec![]; // (account_name, mb_path, mb_id)

    for account in &accounts {
        println!(
            "Account: {} <{}>\n  id: {}",
            account.name, account.email, account.id
        );
        let mailboxes = client.list_mailboxes(&account.id).await?;
        for mb in &mailboxes {
            let is_inbox = mb.mailbox_type == mc_client::MailboxKind::Inbox;
            println!(
                "  {:<35} {:>5} msgs  {:>3} unread  [{}]{}",
                mb.path,
                mb.total_messages,
                mb.unread_messages,
                mb.id,
                if is_inbox { "  ← watching" } else { "" },
            );
            if is_inbox {
                inboxes.push((account.name.clone(), mb.path.clone(), mb.id.clone()));
            }
        }
        println!();
    }

    if inboxes.is_empty() {
        println!("No INBOX found to watch.");
        return Ok(());
    }

    // Spawn one watcher task per INBOX
    for (account_name, _mailbox_path, mailbox_id) in inboxes {
        let c = client.clone();
        tokio::spawn(async move {
            let mut stream = match c.watch_mailbox(&mailbox_id).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[{}] watch_mailbox failed: {}", account_name, e);
                    return;
                }
            };

            while let Some(item) = stream.next().await {
                match item {
                    Ok(n) => {
                        let ts = chrono::Utc::now().format("%H:%M:%S");
                        let desc = match n.event {
                            MailboxNotificationKind::NewMessage { remote_id } => {
                                format!("NewMessage id={}", remote_id)
                            }
                            MailboxNotificationKind::FlagsChanged => "FlagsChanged".to_string(),
                            MailboxNotificationKind::MessageExpunged { message_id } => {
                                format!("MessageExpunged message={}", message_id)
                            }
                            MailboxNotificationKind::CountsChanged { total, unseen } => {
                                format!("CountsChanged total={} unseen={}", total, unseen)
                            }
                        };
                        println!("[{}][{}] {}", ts, account_name, desc);
                    }
                    Err(e) => {
                        eprintln!("[{}] stream error: {}", account_name, e);
                        break;
                    }
                }
            }
            println!("[{}] stream ended.", account_name);
        });
    }

    println!("Listening for events — Ctrl+C to exit.\n");
    tokio::signal::ctrl_c().await?;
    println!("Done.");
    Ok(())
}
