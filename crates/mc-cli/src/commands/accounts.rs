use anyhow::Result;
use clap::Subcommand;
use colored::Colorize;
use mc_client::MailCatClient;
use tabled::{Table, Tabled};

#[derive(Subcommand)]
pub enum AccountsCmd {
    /// List all configured accounts
    List,
}

pub async fn run(cmd: AccountsCmd, client: MailCatClient) -> Result<()> {
    match cmd {
        AccountsCmd::List => list(client).await,
    }
}

#[derive(Tabled)]
struct AccountRow {
    #[tabled(rename = "ID")]
    id: String,
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Email")]
    email: String,
    #[tabled(rename = "Status")]
    status: String,
}

async fn list(client: MailCatClient) -> Result<()> {
    let accounts = client.list_accounts().await?;

    if accounts.is_empty() {
        println!("{}", "No accounts found.".yellow());
        return Ok(());
    }

    let rows: Vec<AccountRow> = accounts
        .into_iter()
        .map(|a| AccountRow {
            id: a.id,
            name: a.name,
            email: a.email,
            status: if a.is_active {
                "active".green().to_string()
            } else {
                "inactive".red().to_string()
            },
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}
