use anyhow::Result;
use clap::{Args, Subcommand};
use colored::Colorize;
use mc_client::{ContactFields, MailCatClient, RemoteContact};
use tabled::{Table, Tabled};

use super::accounts::find_account;
use super::{Format, print_json};
use crate::toon::{Toon, l, n, s};

/// What `add` and `update` set on a contact. A flag left out keeps the field
/// (`update`) or leaves it empty (`add`); `--org ""` and `--notes ""` clear.
#[derive(Args, Default)]
pub struct ContactFlags {
    /// Display name
    #[arg(long)]
    name: Option<String>,
    /// Email address; repeat for several (the first is the primary one)
    #[arg(long)]
    email: Vec<String>,
    /// Phone number; repeat for several
    #[arg(long)]
    phone: Vec<String>,
    #[arg(long)]
    org: Option<String>,
    #[arg(long)]
    notes: Option<String>,
}

impl From<ContactFlags> for ContactFields {
    fn from(flags: ContactFlags) -> Self {
        let list = |items: Vec<String>| Some(items).filter(|items| !items.is_empty());
        Self {
            display_name: flags.name,
            emails: list(flags.email),
            phones: list(flags.phone),
            organization: flags.org,
            notes: flags.notes,
        }
    }
}

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

    /// List the address books the server holds for an account
    Books {
        /// Account name, email or ID
        account: String,
    },

    /// List contacts synced with a server, with their sync state (dirty =
    /// local edit not yet acknowledged, push_failed = why the server refused it)
    List {
        /// Account name, email or ID
        account: String,
        /// Only this address book (see `books`)
        #[arg(long)]
        book: Option<String>,
    },

    /// Show a synced contact with its raw vCard
    Show { id: String },

    /// Create a contact in an address book of the account
    Add {
        /// Account name, email or ID
        account: String,
        /// Address book collection (see `books`); there is no default
        #[arg(long)]
        book: String,
        #[command(flatten)]
        flags: ContactFlags,
    },

    /// Edit a synced contact; what the app does not model is kept
    Update {
        id: String,
        #[command(flatten)]
        flags: ContactFlags,
    },

    /// Delete a synced contact (on the server too)
    Delete { id: String },

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

pub async fn run(cmd: ContactsCmd, client: MailCatClient, format: Format) -> Result<()> {
    match cmd {
        ContactsCmd::Books { account } => books(&client, &account, format).await,
        ContactsCmd::List { account, book } => list(&client, &account, book, format).await,
        ContactsCmd::Show { id } => show(&client, &id, format).await,
        ContactsCmd::Add {
            account,
            book,
            flags,
        } => {
            let account = find_account(&client, &account).await?;
            let id = client
                .create_remote_contact(&account.id, &book, flags.into())
                .await?;
            println!("{} created {id}", "✓".green());
            Ok(())
        }
        ContactsCmd::Update { id, flags } => {
            client.update_remote_contact(&id, flags.into()).await?;
            println!("{} updated {id}", "✓".green());
            Ok(())
        }
        ContactsCmd::Delete { id } => {
            client.delete_remote_contact(&id).await?;
            println!("{} deleted {id}", "✓".green());
            Ok(())
        }
        ContactsCmd::Search {
            query,
            limit,
            account_id,
        } => search(query, limit, account_id, client).await,
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

    println!(
        "Backfilling contacts from stored messages for {}…",
        scope_msg.bold()
    );
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

// ── synced contacts ───────────────────────────────────────────────────────────

async fn books(client: &MailCatClient, account: &str, format: Format) -> Result<()> {
    let account = find_account(client, account).await?;
    let books = client.list_address_books(&account.id).await?;
    if format == Format::Json {
        return print_json(&books);
    }
    let rows: Vec<_> = books
        .iter()
        .map(|b| vec![s(&b.collection), s(&b.name), n(b.read_only)])
        .collect();
    Toon::default()
        .table("books", &["collection", "name", "read_only"], &rows)
        .print();
    Ok(())
}

async fn list(
    client: &MailCatClient,
    account: &str,
    book: Option<String>,
    format: Format,
) -> Result<()> {
    let account = find_account(client, account).await?;
    let contacts = client.list_remote_contacts(&account.id, book).await?;
    if format == Format::Json {
        return print_json(&contacts);
    }
    let rows: Vec<_> = contacts
        .iter()
        .map(|c| {
            vec![
                s(&c.id),
                s(&c.display_name),
                l(&c.emails),
                l(&c.phones),
                s(c.organization.as_deref().unwrap_or_default()),
                s(&c.address_book),
                n(c.dirty),
                s(c.push_failed.as_deref().unwrap_or_default()),
            ]
        })
        .collect();
    let fields = [
        "id",
        "name",
        "emails",
        "phones",
        "org",
        "book",
        "dirty",
        "push_failed",
    ];
    Toon::default().table("contacts", &fields, &rows).print();
    Ok(())
}

async fn show(client: &MailCatClient, id: &str, format: Format) -> Result<()> {
    let c: RemoteContact = client.get_remote_contact(id).await?;
    if format == Format::Json {
        return print_json(&c);
    }
    let mut t = Toon::default();
    t.field("id", s(&c.id))
        .field("book", s(&c.address_book))
        .field("href", s(&c.href))
        .field("name", s(&c.display_name))
        .list("emails", &c.emails)
        .list("phones", &c.phones)
        .field("org", s(c.organization.as_deref().unwrap_or_default()))
        .field("notes", s(c.notes.as_deref().unwrap_or_default()))
        .field("dirty", n(c.dirty));
    if let Some(reason) = &c.push_failed {
        t.field("push_failed", s(reason));
    }
    t.field("raw", s(&c.raw)).print();
    Ok(())
}
