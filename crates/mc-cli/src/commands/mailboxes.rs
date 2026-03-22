use anyhow::Result;
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
}

pub async fn run(cmd: MailboxesCmd, client: MailCatClient) -> Result<()> {
    match cmd {
        MailboxesCmd::List { account_id } => list(account_id, client).await,
    }
}

#[derive(Tabled)]
struct MailboxRow {
    #[tabled(rename = "ID")]
    id: String,
    #[tabled(rename = "Path")]
    path: String,
    #[tabled(rename = "Display Name")]
    display_name: String,
    #[tabled(rename = "Total")]
    total: u32,
    #[tabled(rename = "Unread")]
    unread: String,
}

async fn list(account_id: String, client: MailCatClient) -> Result<()> {
    let mailboxes = client.list_mailboxes(&account_id).await?;

    if mailboxes.is_empty() {
        println!("{}", "No mailboxes found.".yellow());
        return Ok(());
    }

    let rows: Vec<MailboxRow> = mailboxes
        .into_iter()
        .map(|m| MailboxRow {
            id: m.id,
            path: m.path,
            display_name: m.display_name,
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
