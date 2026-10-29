use anyhow::Result;
use colored::Colorize;
use mc_client::{MailCatClient, MailboxKind};
use tabled::{Table, Tabled};

#[derive(Tabled)]
struct MatchRow {
    #[tabled(rename = "Mailbox")]
    mailbox: String,
    #[tabled(rename = "Date")]
    date: String,
    #[tabled(rename = "From")]
    from: String,
    #[tabled(rename = "Subject")]
    subject: String,
}

pub(super) async fn delete_matching(
    account: String,
    mailbox: Option<String>,
    subject: Option<String>,
    from: Option<String>,
    yes: bool,
    client: MailCatClient,
) -> Result<()> {
    // Chunked only to report progress: the server already batches the IMAP side.
    const CHUNK: usize = 2000;

    let contains = |field: &str, needle: &Option<String>| {
        needle
            .as_ref()
            .is_none_or(|n| field.to_lowercase().contains(&n.to_lowercase()))
    };
    let account = client
        .list_accounts()
        .await?
        .into_iter()
        .find(|a| {
            [&a.id, &a.name, &a.email]
                .iter()
                .any(|v| v.eq_ignore_ascii_case(&account))
        })
        .ok_or_else(|| anyhow::anyhow!("no account named {account:?} (see `mc accounts list`)"))?;
    let all = client.list_mailboxes(&account.id).await?;
    let mailboxes: Vec<_> = match &mailbox {
        Some(wanted) => all
            .into_iter()
            .filter(|m| {
                &m.id == wanted
                    || m.path.eq_ignore_ascii_case(wanted)
                    || m.display_path.eq_ignore_ascii_case(wanted)
            })
            .collect(),
        None if all.iter().any(|m| m.mailbox_type == MailboxKind::All) => all
            .into_iter()
            .filter(|m| m.mailbox_type == MailboxKind::All)
            .collect(),
        None => all
            .into_iter()
            .filter(|m| m.mailbox_type != MailboxKind::Trash)
            .collect(),
    };
    if mailboxes.is_empty() {
        anyhow::bail!(
            "no such mailbox in {} (see `mc mailboxes list {}`)",
            account.name,
            account.id
        );
    }

    // A message in several mailboxes is deleted once, from the first that lists it.
    let mut seen = std::collections::HashSet::new();
    let mut matches = Vec::new();
    for mb in &mailboxes {
        for m in client.list_all_messages(&mb.id).await? {
            if contains(&m.subject, &subject)
                && contains(&m.from_email, &from)
                && seen.insert(m.id.clone())
            {
                matches.push((m, mb));
            }
        }
    }

    if !matches.is_empty() {
        let rows = matches.iter().map(|(m, mb)| MatchRow {
            mailbox: mb.display_path.clone(),
            date: m.date.format("%Y-%m-%d %H:%M").to_string(),
            from: m.from_email.clone(),
            subject: m.subject.chars().take(100).collect(),
        });
        println!("{}", Table::new(rows).with(tabled::settings::Style::psql()));
    }
    println!(
        "{} {} matching messages",
        "→".blue(),
        matches.len().to_string().bold()
    );
    if !yes || matches.is_empty() {
        if !matches.is_empty() {
            println!("{}", "Dry run: add --yes to move them to Trash.".yellow());
        }
        return Ok(());
    }

    let refs: Vec<(String, String)> = matches
        .into_iter()
        .map(|(m, mb)| (m.id, mb.id.clone()))
        .collect();
    let (mut succeeded, mut failed) = (0, 0);
    for chunk in refs.chunks(CHUNK) {
        let (ok, ko) = client.delete_messages(chunk.to_vec(), false).await?;
        succeeded += ok;
        failed += ko;
        println!("  {} {succeeded}/{} deleted", "…".dimmed(), refs.len());
    }
    println!("  {} {succeeded} deleted, {failed} failed", "✓".green());
    Ok(())
}
