use anyhow::Result;
use clap::Subcommand;
use colored::Colorize;
use mc_client::{ComposeInput, MailCatClient, MessageFlag};
mod delete_matching;
mod send_test;

use delete_matching::delete_matching;
use send_test::send_test;
use tabled::{Table, Tabled};

#[derive(Subcommand)]
pub enum MessagesCmd {
    /// List messages in a mailbox
    List {
        /// Mailbox ID (from `mc mailboxes list <account-id>`)
        mailbox_id: String,
        /// Also show message IDs (verbose)
        #[arg(short, long)]
        verbose: bool,
    },
    /// Fetch and display the full body of a message
    Get {
        /// Message ID (from `mc messages list <mailbox-id>`)
        message_id: String,
        /// Mailbox ID the message is read from (from `mc mailboxes list <account-id>`)
        #[arg(long)]
        mailbox: String,
        /// Show raw text body instead of HTML (stripped)
        #[arg(long)]
        text: bool,
    },
    /// Show a message's metadata: mailboxes with provider ids, Message-ID, flags
    Show {
        /// Message ID
        message_id: String,
        /// Mailbox ID the message is read from
        #[arg(long)]
        mailbox: String,
    },
    /// Mark a message as read (set \Seen flag)
    MarkRead {
        /// Message ID
        message_id: String,
    },
    /// Mark a message as unread (remove \Seen flag)
    MarkUnread {
        /// Message ID
        message_id: String,
    },
    /// Star a message (set \Flagged flag)
    Star {
        /// Message ID
        message_id: String,
    },
    /// Unstar a message (remove \Flagged flag)
    Unstar {
        /// Message ID
        message_id: String,
    },
    /// Delete a message
    Delete {
        /// Message ID
        message_id: String,
        /// Mailbox ID to delete the message from
        #[arg(long)]
        mailbox: String,
        /// Permanently expunge (instead of just marking as deleted)
        #[arg(long)]
        permanent: bool,
    },
    /// Delete every message in a mailbox matching the filters. Without --yes
    /// it only counts the matches and shows a few.
    DeleteMatching {
        /// Account name, email or ID
        #[arg(long)]
        account: String,
        /// Mailbox ID, path or display path; defaults to All Mail where the account has one
        /// (Gmail), else every mailbox but Trash
        #[arg(long)]
        mailbox: Option<String>,
        /// Case-insensitive substring of the subject
        #[arg(long, required_unless_present = "from")]
        subject: Option<String>,
        /// Case-insensitive substring of the sender address
        #[arg(long)]
        from: Option<String>,
        /// Actually delete (to Trash)
        #[arg(long)]
        yes: bool,
    },
    /// Move a message to another mailbox
    Move {
        /// Message ID
        message_id: String,
        /// Mailbox ID to move the message out of
        #[arg(long)]
        mailbox: String,
        /// Target mailbox ID
        to_mailbox_id: String,
    },
    /// Add and remove mailboxes (labels) on messages, without moving them
    Labels {
        /// Message IDs
        #[arg(required = true)]
        message_ids: Vec<String>,
        /// Mailbox IDs to add
        #[arg(long)]
        add: Vec<String>,
        /// Mailbox IDs to remove
        #[arg(long)]
        remove: Vec<String>,
    },
    /// Add and remove tags (keywords) on a message
    Tag {
        /// Message ID
        message_id: String,
        #[arg(long)]
        add: Vec<String>,
        #[arg(long)]
        remove: Vec<String>,
    },
    /// Take messages out of the Inbox (move to Archive, or drop the Inbox label)
    Archive {
        /// Message IDs
        #[arg(required = true)]
        message_ids: Vec<String>,
    },
    /// Search messages by full-text query
    Search {
        /// Search query (supports from:, to:, sbj: prefixes)
        query: String,
        /// Limit to a specific account
        #[arg(long)]
        account_id: Option<String>,
        /// Limit to a specific mailbox
        #[arg(long)]
        mailbox_id: Option<String>,
    },
    /// Send a real test email and verify it round-trips correctly: body
    /// persisted, visible in the mailbox list with sender/recipient, and
    /// (if --reply-to is given) threaded with the message it replies to.
    SendTest {
        /// Account ID to send from (see `mc accounts list`); its identity is the sender
        account_id: String,
        /// Recipient address
        #[arg(long, default_value = "attilio.greco@4sigma.it")]
        to: String,
        /// Reply to an existing message (verifies threading)
        #[arg(long)]
        reply_to: Option<String>,
        /// Mailbox ID of the message being replied to (required with --reply-to)
        #[arg(long, requires = "reply_to")]
        reply_mailbox: Option<String>,
    },
    /// Save a draft; prints its id. `--replace` saves a new version of an earlier draft.
    Draft {
        /// Account ID to save the draft in
        account_id: String,
        #[arg(long)]
        to: String,
        #[arg(long)]
        subject: String,
        #[arg(long)]
        body: String,
        /// Draft ID (from an earlier `draft`) this one replaces
        #[arg(long)]
        replace: Option<String>,
    },
    /// Autocomplete suggestions for search-as-you-type
    Suggest {
        /// Prefix to search for
        prefix: String,
        /// Scope: from, to, subject, body (optional)
        #[arg(long)]
        scope: Option<String>,
        /// Max suggestions per field
        #[arg(long, default_value = "5")]
        max: u32,
    },
}

pub async fn run(cmd: MessagesCmd, client: MailCatClient, json: bool) -> Result<()> {
    match cmd {
        MessagesCmd::List {
            mailbox_id,
            verbose,
        } => list(mailbox_id, verbose, json, client).await,
        MessagesCmd::Show {
            message_id,
            mailbox,
        } => show(message_id, mailbox, json, client).await,
        MessagesCmd::Get {
            message_id,
            mailbox,
            text,
        } => get(message_id, mailbox, text, client).await,
        MessagesCmd::MarkRead { message_id } => {
            set_flags(message_id, &["seen"], &[], "Marked as read", client).await
        }
        MessagesCmd::MarkUnread { message_id } => {
            set_flags(message_id, &[], &["seen"], "Marked as unread", client).await
        }
        MessagesCmd::Star { message_id } => {
            set_flags(message_id, &["flagged"], &[], "Starred", client).await
        }
        MessagesCmd::Unstar { message_id } => {
            set_flags(message_id, &[], &["flagged"], "Unstarred", client).await
        }
        MessagesCmd::Delete {
            message_id,
            mailbox,
            permanent,
        } => delete(message_id, mailbox, permanent, client).await,
        MessagesCmd::DeleteMatching {
            account,
            mailbox,
            subject,
            from,
            yes,
        } => delete_matching(account, mailbox, subject, from, yes, client).await,
        MessagesCmd::Move {
            message_id,
            mailbox,
            to_mailbox_id,
        } => move_msg(message_id, mailbox, to_mailbox_id, client).await,
        MessagesCmd::Labels {
            message_ids,
            add,
            remove,
        } => {
            let (ok, failed) = client.change_labels(message_ids, add, remove).await?;
            println!(
                "{} labels changed on {ok} message(s), {failed} skipped",
                "✓".green()
            );
            Ok(())
        }
        MessagesCmd::Tag {
            message_id,
            add,
            remove,
        } => {
            let m = client.modify_tags(&message_id, add, remove).await?;
            println!("{} tags: {}", "✓".green(), m.tags.join(" "));
            Ok(())
        }
        MessagesCmd::Archive { message_ids } => {
            let (ok, failed) = client.archive_messages(message_ids).await?;
            println!("{} archived {ok} message(s), {failed} skipped", "✓".green());
            Ok(())
        }
        MessagesCmd::Search {
            query,
            account_id,
            mailbox_id,
        } => search(query, account_id, mailbox_id, client).await,
        MessagesCmd::Suggest { prefix, scope, max } => suggest(prefix, scope, max, client).await,
        MessagesCmd::Draft {
            account_id,
            to,
            subject,
            body,
            replace,
        } => {
            let id = client
                .save_draft(ComposeInput {
                    draft_id: replace,
                    account_id,
                    to: vec![to],
                    cc: vec![],
                    bcc: vec![],
                    subject,
                    body_md: body,
                    mode: "new".to_owned(),
                    source_message_id: None,
                    attachments: vec![],
                    forwarded_attachment_ids: vec![],
                })
                .await?;
            println!("{id}");
            Ok(())
        }
        MessagesCmd::SendTest {
            account_id,
            to,
            reply_to,
            reply_mailbox,
        } => send_test(account_id, to, reply_to.zip(reply_mailbox), client).await,
    }
}

#[derive(Tabled)]
struct MessageRow {
    #[tabled(rename = "ID")]
    id: String,
    #[tabled(rename = "From")]
    from: String,
    #[tabled(rename = "Subject")]
    subject: String,
    #[tabled(rename = "Date")]
    date: String,
    #[tabled(rename = "R")]
    read: String,
}

async fn list(mailbox_id: String, verbose: bool, json: bool, client: MailCatClient) -> Result<()> {
    // list_messages takes (account_id, mailbox_id) but mailbox_id is self-contained
    let messages = client.list_messages("", &mailbox_id).await?;
    if json {
        return super::print_json(&messages);
    }

    if messages.is_empty() {
        println!("{}", "No messages found.".yellow());
        return Ok(());
    }

    println!(
        "{} {} messages\n",
        "→".blue(),
        messages.len().to_string().bold()
    );

    let rows: Vec<MessageRow> = messages
        .into_iter()
        .map(|m| {
            let id = if verbose {
                m.id.clone()
            } else {
                m.id[..8.min(m.id.len())].to_string() + "…"
            };

            let subject = if m.subject.is_empty() {
                "(no subject)".dimmed().to_string()
            } else if !m.is_read {
                m.subject.bold().to_string()
            } else {
                m.subject.clone()
            };

            let from = m
                .from_name
                .as_deref()
                .unwrap_or(&m.from_email)
                .chars()
                .take(25)
                .collect::<String>();

            MessageRow {
                id,
                from,
                subject,
                date: m.date.format("%d %b %H:%M").to_string(),
                read: if m.is_read {
                    " ".into()
                } else {
                    "●".blue().to_string()
                },
            }
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}

async fn get(
    message_id: String,
    mailbox: String,
    _text_only: bool,
    client: MailCatClient,
) -> Result<()> {
    let body = client.get_message(&message_id, &mailbox).await?;

    println!("{}", "─".repeat(60).dimmed());

    if let Some(text) = &body.text_body {
        println!("{}", text);
    } else if let Some(html) = &body.html_body {
        // Strip HTML tags for terminal display
        let stripped = strip_html(html);
        println!("{}", stripped);
    } else {
        println!("{}", "(empty message)".dimmed());
    }

    println!("{}", "─".repeat(60).dimmed());
    Ok(())
}

async fn show(
    message_id: String,
    mailbox: String,
    json: bool,
    client: MailCatClient,
) -> Result<()> {
    let m = client.get_message(&message_id, &mailbox).await?;
    if json {
        return super::print_json(&m);
    }
    println!("id:         {}", m.id);
    println!("subject:    {}", m.subject);
    println!("message-id: {}", m.rfc_message_id.as_deref().unwrap_or("-"));
    println!("flags:      {}", m.flags.join(" "));
    println!("tags:       {}", m.tags.join(" "));
    for x in &m.memberships {
        println!(
            "mailbox:    {} remote_id={}",
            x.mailbox_id,
            x.remote_id.as_deref().unwrap_or("?")
        );
    }
    Ok(())
}

fn str_to_flag(s: &str) -> Option<MessageFlag> {
    match s {
        "seen" => Some(MessageFlag::Seen),
        "answered" => Some(MessageFlag::Answered),
        "flagged" => Some(MessageFlag::Flagged),
        "deleted" => Some(MessageFlag::Deleted),
        "draft" => Some(MessageFlag::Draft),
        _ => None,
    }
}

async fn set_flags(
    message_id: String,
    add: &[&str],
    remove: &[&str],
    label: &str,
    client: MailCatClient,
) -> Result<()> {
    let flags_add: Vec<MessageFlag> = add.iter().filter_map(|s| str_to_flag(s)).collect();
    let flags_remove: Vec<MessageFlag> = remove.iter().filter_map(|s| str_to_flag(s)).collect();

    println!(
        "{} Setting flags on {}...",
        "→".blue(),
        message_id[..8.min(message_id.len())].bold()
    );

    let updated = client
        .set_flags(&message_id, &flags_add, &flags_remove)
        .await?;

    println!(
        "  {} {} — is_read={}",
        "✓".green(),
        label,
        if updated.is_read { "true" } else { "false" }
    );

    Ok(())
}

async fn delete(
    message_id: String,
    mailbox: String,
    permanent: bool,
    client: MailCatClient,
) -> Result<()> {
    println!(
        "{} Deleting {} (permanent={})...",
        "→".blue(),
        message_id[..8.min(message_id.len())].bold(),
        permanent
    );

    client
        .delete_message(&message_id, &mailbox, permanent)
        .await?;

    println!("  {} Message deleted", "✓".green());
    Ok(())
}

async fn move_msg(
    message_id: String,
    mailbox: String,
    to_mailbox_id: String,
    client: MailCatClient,
) -> Result<()> {
    println!(
        "{} Moving {} to mailbox {}...",
        "→".blue(),
        message_id[..8.min(message_id.len())].bold(),
        to_mailbox_id[..8.min(to_mailbox_id.len())].bold(),
    );

    let updated = client
        .move_message(&message_id, &mailbox, &to_mailbox_id)
        .await?;

    println!(
        "  {} Message moved (mailbox_ids={})",
        "✓".green(),
        updated.mailbox_ids.join(",")
    );
    Ok(())
}

async fn search(
    query: String,
    account_id: Option<String>,
    mailbox_id: Option<String>,
    client: MailCatClient,
) -> Result<()> {
    println!("{} Searching: {}", "→".blue(), query.bold());

    let hits = client
        .search(&query, account_id.as_deref(), mailbox_id.as_deref())
        .await?;

    if hits.is_empty() {
        println!("{}", "  No results.".yellow());
        return Ok(());
    }

    println!("  {} result(s)\n", hits.len().to_string().bold());

    #[derive(Tabled)]
    struct HitRow {
        #[tabled(rename = "Score")]
        score: String,
        #[tabled(rename = "From")]
        from: String,
        #[tabled(rename = "Subject")]
        subject: String,
        #[tabled(rename = "ID")]
        id: String,
    }

    let rows: Vec<HitRow> = hits
        .into_iter()
        .map(|h| HitRow {
            score: format!("{:.2}", h.score),
            from: h
                .from_name
                .unwrap_or(h.from_email)
                .chars()
                .take(25)
                .collect(),
            subject: h.subject.chars().take(40).collect(),
            id: h.message_id[..8.min(h.message_id.len())].to_string() + "…",
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}

async fn suggest(
    prefix: String,
    scope: Option<String>,
    max: u32,
    client: MailCatClient,
) -> Result<()> {
    println!(
        "{} Suggesting: prefix={} scope={:?}",
        "→".blue(),
        prefix.bold(),
        scope
    );

    let suggestions = client.suggest(&prefix, scope.as_deref(), max).await?;

    if suggestions.is_empty() {
        println!("{}", "  No suggestions.".yellow());
        return Ok(());
    }

    println!("  {} suggestion(s)\n", suggestions.len().to_string().bold());

    #[derive(Tabled)]
    struct SuggestionRow {
        #[tabled(rename = "Field")]
        field: String,
        #[tabled(rename = "Value")]
        value: String,
        #[tabled(rename = "Count")]
        count: String,
    }

    let rows: Vec<SuggestionRow> = suggestions
        .into_iter()
        .map(|s| SuggestionRow {
            field: s.field,
            value: s.value,
            count: s.doc_count.to_string(),
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}

fn strip_html(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    for ch in html.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    // Collapse multiple blank lines
    out.lines()
        .fold((String::new(), 0usize), |(mut acc, blanks), line| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                if blanks < 1 {
                    acc.push('\n');
                }
                (acc, blanks + 1)
            } else {
                acc.push_str(trimmed);
                acc.push('\n');
                (acc, 0)
            }
        })
        .0
}
