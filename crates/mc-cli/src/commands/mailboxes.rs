use anyhow::Result;
use anyhow::bail;
use clap::Subcommand;
use colored::Colorize;
use mc_client::{MailCatClient, Mailbox, MailboxKind};
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
    /// Delete an empty user mailbox on the server, first moving its messages
    Delete {
        /// Account ID (from `mc accounts list`)
        account_id: String,
        /// Mailbox id, path, display path or name
        mailbox: String,
        /// Where its messages go: a mailbox, or a role (sent, trash, archive,
        /// inbox, junk, drafts). Required when it holds any
        #[arg(long)]
        move_to: Option<String>,
        /// Do not ask for confirmation
        #[arg(long, short)]
        yes: bool,
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
        MailboxesCmd::Delete {
            account_id,
            mailbox,
            move_to,
            yes,
        } => delete(&client, &account_id, &mailbox, move_to.as_deref(), yes).await,
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
    let mailboxes = client.list_mailboxes(account_id).await?;
    let mailbox = find(&mailboxes, wanted)?;
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

/// The one mailbox `wanted` names: id or path first, then display path, then
/// name. A tier that matches several is ambiguous, not a reason to try the
/// next: deleting the wrong twin is not recoverable.
fn find<'a>(mailboxes: &'a [Mailbox], wanted: &str) -> Result<&'a Mailbox> {
    let tiers: [fn(&Mailbox) -> [&str; 2]; 3] = [
        |m| [&m.id, &m.path],
        |m| [&m.display_path, ""],
        |m| [&m.display_name, ""],
    ];
    if wanted.is_empty() {
        bail!("empty mailbox name");
    }
    for tier in tiers {
        let found: Vec<_> = mailboxes
            .iter()
            .filter(|m| tier(m).contains(&wanted))
            .collect();
        match found.as_slice() {
            [] => {}
            [one] => return Ok(one),
            many => bail!(
                "{wanted:?} names {} mailboxes, use the id of one: {}",
                many.len(),
                many.iter()
                    .map(|m| format!("{} ({})", m.id, m.display_path))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
    bail!("no mailbox matching {wanted:?} (id, path, display path or name)")
}

/// `--move-to`: a mailbox as for [`find`], or a role keyword.
fn find_destination<'a>(mailboxes: &'a [Mailbox], wanted: &str) -> Result<&'a Mailbox> {
    let role = match wanted {
        "sent" => Some(MailboxKind::Sent),
        "trash" => Some(MailboxKind::Trash),
        "archive" => Some(MailboxKind::Archive),
        "inbox" => Some(MailboxKind::Inbox),
        "junk" | "spam" => Some(MailboxKind::Spam),
        "drafts" => Some(MailboxKind::Drafts),
        _ => None,
    };
    let Some(role) = role.filter(|_| !mailboxes.iter().any(|m| m.id == wanted || m.path == wanted))
    else {
        return find(mailboxes, wanted);
    };
    let mut with_role = mailboxes.iter().filter(|m| m.mailbox_type == role);
    match (with_role.next(), with_role.next()) {
        (Some(only), None) => Ok(only),
        (None, _) => bail!("this account has no {wanted} mailbox"),
        _ => bail!("this account has several {wanted} mailboxes, name the one you want"),
    }
}

async fn delete(
    client: &MailCatClient,
    account_id: &str,
    wanted: &str,
    move_to: Option<&str>,
    yes: bool,
) -> Result<()> {
    let mailboxes = client.list_mailboxes(account_id).await?;
    let mailbox = find(&mailboxes, wanted)?;
    if mailbox.mailbox_type != MailboxKind::Custom {
        bail!(
            "{} is a system mailbox ({:?}), it cannot be deleted",
            mailbox.display_path,
            mailbox.mailbox_type
        );
    }
    let destination = move_to
        .map(|to| find_destination(&mailboxes, to))
        .transpose()?;
    if mailbox.total_messages > 0 && destination.is_none() {
        bail!(
            "{} holds {} messages: pass --move-to <mailbox|role> to say where they go",
            mailbox.display_path,
            mailbox.total_messages
        );
    }
    if destination.is_some_and(|d| d.id == mailbox.id) {
        bail!("--move-to names the mailbox being deleted");
    }
    println!(
        "account:     {account_id}\nmailbox:     {} ({})\nmessages:    {}\nmove to:     {}",
        mailbox.display_path,
        mailbox.id,
        mailbox.total_messages,
        destination.map_or("-".to_owned(), |d| format!("{} ({})", d.display_path, d.id)),
    );
    if !yes {
        print!("Delete this mailbox on the server? [y/N] ");
        std::io::Write::flush(&mut std::io::stdout())?;
        let mut answer = String::new();
        std::io::stdin().read_line(&mut answer)?;
        if !answer.trim().eq_ignore_ascii_case("y") {
            bail!("not confirmed, nothing changed");
        }
    }
    let moved = client
        .delete_mailbox(&mailbox.id, destination.map(|d| d.id.as_str()))
        .await?;
    println!(
        "{} deleted {} ({moved} messages moved)",
        "✓".green(),
        mailbox.display_path
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mailbox(id: &str, path: &str, display: &str, kind: MailboxKind) -> Mailbox {
        Mailbox {
            id: id.into(),
            account_id: "a".into(),
            path: path.into(),
            display_path: display.into(),
            display_name: display.into(),
            mailbox_type: kind,
            total_messages: 0,
            unread_messages: 0,
            subscribed: true,
        }
    }

    /// gapmilano: a role-less `Trash` beside Gmail's real one, same display path.
    fn gmail() -> Vec<Mailbox> {
        vec![
            mailbox("1", "Trash", "Trash", MailboxKind::Custom),
            mailbox("2", "[Gmail]/Trash", "Trash", MailboxKind::Trash),
            mailbox("3", "[Gmail]/Sent Mail", "Sent Mail", MailboxKind::Custom),
            mailbox(
                "4",
                "[Gmail]/Posta inviata",
                "Posta inviata",
                MailboxKind::Sent,
            ),
        ]
    }

    #[test]
    fn a_path_wins_over_a_display_path_shared_with_a_twin() {
        let boxes = gmail();
        assert_eq!(find(&boxes, "Trash").unwrap().id, "1");
        assert_eq!(find(&boxes, "[Gmail]/Sent Mail").unwrap().id, "3");
        assert_eq!(find(&boxes, "Sent Mail").unwrap().id, "3");
    }

    #[test]
    fn a_name_shared_by_two_mailboxes_is_ambiguous() {
        let mut boxes = gmail();
        boxes.push(mailbox(
            "5",
            "x/Sent Mail",
            "Sent Mail",
            MailboxKind::Custom,
        ));
        assert!(
            find(&boxes, "Sent Mail")
                .unwrap_err()
                .to_string()
                .contains("2 mailboxes")
        );
    }

    #[test]
    fn a_role_keyword_picks_the_mailbox_with_that_role() {
        let boxes = gmail();
        assert_eq!(find_destination(&boxes, "sent").unwrap().id, "4");
        assert_eq!(find_destination(&boxes, "trash").unwrap().id, "2");
        assert!(find_destination(&boxes, "archive").is_err());
        assert_eq!(find_destination(&boxes, "4").unwrap().id, "4");
    }
}
