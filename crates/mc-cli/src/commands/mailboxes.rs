use anyhow::{Context, Result};
use clap::Subcommand;
use colored::Colorize;
use mc_client::MailCatClient;
use tabled::{Table, Tabled};

#[derive(Subcommand)]
pub enum MailboxesCmd {
    /// List mailboxes for an account
    List {
        /// Account ID (from `mc accounts list`)
        account_id: String,
    },
    /// Show a mailbox and sync it
    Subscribe {
        /// Account ID (from `mc accounts list`)
        account_id: String,
        /// Mailbox id, path, display path or name
        mailbox: String,
    },
    /// Hide a mailbox and delete its local messages
    Unsubscribe {
        /// Account ID (from `mc accounts list`)
        account_id: String,
        /// Mailbox id, path, display path or name
        mailbox: String,
    },
}

pub async fn run(cmd: MailboxesCmd, client: MailCatClient, json: bool) -> Result<()> {
    match cmd {
        MailboxesCmd::List { account_id } => list(account_id, client, json).await,
        MailboxesCmd::Subscribe {
            account_id,
            mailbox,
        } => subscribe(&client, &account_id, &mailbox, true).await,
        MailboxesCmd::Unsubscribe {
            account_id,
            mailbox,
        } => subscribe(&client, &account_id, &mailbox, false).await,
    }
}

#[derive(Tabled)]
struct MailboxRow {
    #[tabled(rename = "ID")]
    id: String,
    #[tabled(rename = "Path")]
    display_path: String,
    #[tabled(rename = "Display Name")]
    display_name: String,
    #[tabled(rename = "Subscribed")]
    subscribed: String,
    #[tabled(rename = "Total")]
    total: u32,
    #[tabled(rename = "Unread")]
    unread: String,
}

async fn list(account_id: String, client: MailCatClient, json: bool) -> Result<()> {
    let mailboxes = client.list_mailboxes(&account_id).await?;
    if json {
        return super::print_json(&mailboxes);
    }

    if mailboxes.is_empty() {
        println!("{}", "No mailboxes found.".yellow());
        return Ok(());
    }

    let rows: Vec<MailboxRow> = mailboxes
        .into_iter()
        .map(|m| MailboxRow {
            id: m.id,
            display_path: m.display_path,
            display_name: m.display_name,
            subscribed: if m.subscribed { "yes" } else { "no" }.to_owned(),
            total: m.total_messages,
            unread: if m.unread_messages > 0 {
                m.unread_messages.to_string().bold().blue().to_string()
            } else {
                "0".dimmed().to_string()
            },
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}

async fn subscribe(
    client: &MailCatClient,
    account_id: &str,
    wanted: &str,
    subscribed: bool,
) -> Result<()> {
    let mailbox = client
        .list_mailboxes(account_id)
        .await?
        .into_iter()
        .find(|m| {
            [&m.id, &m.path, &m.display_path, &m.display_name]
                .iter()
                .any(|v| v.as_str() == wanted)
        })
        .with_context(|| {
            format!("no mailbox matching {wanted:?} (id, path, display path or name)")
        })?;
    if let Some(warning) = client
        .set_mailbox_subscribed(&mailbox.id, subscribed)
        .await?
    {
        eprintln!("{} {warning}", "warning:".yellow());
    }
    println!(
        "{} {}",
        if subscribed {
            "Subscribed"
        } else {
            "Unsubscribed"
        },
        mailbox.display_path
    );
    Ok(())
}
