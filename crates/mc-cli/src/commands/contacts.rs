use anyhow::Result;
use clap::Subcommand;
use colored::Colorize;
use mc_client::MailCatClient;
use tabled::{Table, Tabled};

#[derive(Subcommand)]
pub enum ContactsCmd {
    /// Search the contact autocomplete index
    Search {
        /// Search query (name, email, or organization prefix)
        query: String,
        /// Limit the number of results (default: 10)
        #[arg(short, long, default_value = "10")]
        limit: u32,
        /// Show only contacts belonging to this account ID
        #[arg(long)]
        account_id: Option<String>,
    },

    /// Backfill the address book from stored messages.
    ///
    /// Scans all emails already in the local storage, extracts every
    /// from/to/cc address, and creates a contact for each one that doesn't
    /// already exist.  Safe to run multiple times (idempotent).
    ///
    /// Run this once after a fresh install to populate contacts from
    /// historical mail without re-syncing from the server.
    Backfill {
        /// Scope to a specific account ID (default: all accounts)
        #[arg(long)]
        account_id: Option<String>,
    },

    /// Rebuild the Tantivy contact search index from stored contact data.
    ///
    /// Use this when the index is out of sync with the stored contacts
    /// (e.g. after restoring from backup). Contacts must already exist in
    /// the address book — if the book is empty, run `backfill` first.
    RebuildIndex {
        /// Scope the rebuild to a specific account ID (default: all accounts)
        #[arg(long)]
        account_id: Option<String>,
    },
}

pub async fn run(cmd: ContactsCmd, client: MailCatClient) -> Result<()> {
    match cmd {
        ContactsCmd::Search { query, limit, account_id } => {
            search(query, limit, account_id, client).await
        }
        ContactsCmd::Backfill { account_id } => backfill(account_id, client).await,
        ContactsCmd::RebuildIndex { account_id } => rebuild_index(account_id, client).await,
    }
}

// ── search ────────────────────────────────────────────────────────────────────

#[derive(Tabled)]
struct ContactRow {
    #[tabled(rename = "Display Name")]
    display_name: String,
    #[tabled(rename = "Primary Email")]
    primary_email: String,
    #[tabled(rename = "Organization")]
    organization: String,
    #[tabled(rename = "Scope")]
    scope: String,
}

async fn search(
    query: String,
    limit: u32,
    account_id: Option<String>,
    client: MailCatClient,
) -> Result<()> {
    let hits = client.search_contacts(&query, limit, account_id).await?;

    if hits.is_empty() {
        println!("{}", "No contacts found.".yellow());
        return Ok(());
    }

    let rows: Vec<ContactRow> = hits
        .into_iter()
        .map(|h| ContactRow {
            display_name: if h.display_name.is_empty() {
                "—".dimmed().to_string()
            } else {
                h.display_name.clone()
            },
            primary_email: h.primary_email.clone(),
            organization: h.organization.unwrap_or_else(|| "—".dimmed().to_string()),
            scope: if h.is_cross_account {
                "other account".yellow().to_string()
            } else {
                "own".green().to_string()
            },
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}

// ── backfill ──────────────────────────────────────────────────────────────────

async fn backfill(account_id: Option<String>, client: MailCatClient) -> Result<()> {
    let scope_msg = match &account_id {
        Some(id) => format!("account {id}"),
        None => "all accounts".to_string(),
    };

    println!("Backfilling contacts from stored messages for {}…", scope_msg.bold());
    println!("{}", "This may take a while for large mailboxes.".dimmed());

    let (new_contacts, already_existed) =
        client.backfill_contacts_from_messages(account_id).await?;

    println!(
        "{}  {} new contact(s) created, {} already existed.",
        "✓".green().bold(),
        new_contacts.to_string().bold(),
        already_existed.to_string().dimmed(),
    );

    if new_contacts > 0 {
        println!(
            "{}",
            "Tip: run `mc contacts rebuild-index` if the search autocomplete is not yet updated."
                .dimmed()
        );
    }

    Ok(())
}

// ── rebuild-index ─────────────────────────────────────────────────────────────

async fn rebuild_index(account_id: Option<String>, client: MailCatClient) -> Result<()> {
    let scope_msg = match &account_id {
        Some(id) => format!("account {id}"),
        None => "all accounts".to_string(),
    };

    println!("Rebuilding contact index for {}…", scope_msg.bold());

    let indexed_count = client.rebuild_contact_index(account_id).await?;

    println!(
        "{}  {} contact(s) indexed.",
        "✓".green().bold(),
        indexed_count.to_string().bold(),
    );

    Ok(())
}
