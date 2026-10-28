use anyhow::Result;
use clap::Subcommand;
use mc_client::MailCatClient;

#[derive(Subcommand)]
pub enum ConversationsCmd {
    /// List conversation threads for an account (newest first)
    List {
        /// Account ID
        account_id: String,
        /// Max conversations to show
        #[arg(long, default_value = "50")]
        limit: u32,
        /// Scope to a single mailbox (Thunderbird-style per-folder threading)
        #[arg(long)]
        mailbox: Option<String>,
    },

    /// Show all messages in a thread containing the given message
    Thread {
        /// Any message ID in the thread
        message_id: String,
    },

    /// Wipe and rebuild the conversation/thread index from the event store
    RebuildIndex,
}

pub async fn run(cmd: ConversationsCmd, client: MailCatClient) -> Result<()> {
    match cmd {
        ConversationsCmd::List {
            account_id,
            limit,
            mailbox,
        } => {
            let convs = client
                .list_conversations(&account_id, mailbox.as_deref(), 1, limit)
                .await?;

            if convs.is_empty() {
                println!("No conversations found for account {account_id}");
                println!("(Tip: sync the account first with `mc sync <account_id>`)");
                return Ok(());
            }

            println!(
                "{:<6} {:<40} {:<25} {:>5} {:>6}",
                "UNREAD", "SUBJECT", "FROM", "MSGS", "LATEST"
            );
            println!("{}", "─".repeat(90));

            for c in &convs {
                let unread = if c.unread_count > 0 {
                    format!("[{}]", c.unread_count)
                } else {
                    String::new()
                };
                let subject = truncate(&c.subject, 38);
                let sender = truncate(&c.latest_sender, 23);
                let date = c.last_message_at.format("%Y-%m-%d %H:%M").to_string();
                println!(
                    "{:<6} {:<40} {:<25} {:>5} {}",
                    unread, subject, sender, c.message_count, date
                );
            }

            println!("\n{} conversations", convs.len());
        }

        ConversationsCmd::RebuildIndex => {
            println!("Rebuilding conversation index...");
            let events = client.rebuild_conversation_index().await?;
            println!("Done. Replayed {events} events.");
        }

        ConversationsCmd::Thread { message_id } => {
            let messages = client.get_thread(&message_id).await?;

            if messages.is_empty() {
                println!("No thread found for message {message_id}");
                return Ok(());
            }

            println!("Thread: {} message(s)\n", messages.len());
            for (i, msg) in messages.iter().enumerate() {
                println!("── Message {} ── ID: {}", i + 1, msg.id);
                if let Some(text) = &msg.text_body {
                    let preview: String = text.chars().take(300).collect();
                    println!("{}", preview.trim());
                    if text.len() > 300 {
                        println!("... ({} chars total)", text.len());
                    }
                } else if msg.html_body.is_some() {
                    println!("[HTML body — use UI to view]");
                } else {
                    println!("[No body loaded yet — try syncing]");
                }
                println!();
            }
        }
    }

    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        format!("{}…", s.chars().take(max - 1).collect::<String>())
    }
}
