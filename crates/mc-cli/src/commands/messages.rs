use anyhow::Result;
use clap::Subcommand;
use colored::Colorize;
use mc_client::{ComposeInput, MailCatClient, MessageFlag};
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

pub async fn run(cmd: MessagesCmd, client: MailCatClient) -> Result<()> {
    match cmd {
        MessagesCmd::List { mailbox_id, verbose } => list(mailbox_id, verbose, client).await,
        MessagesCmd::Get { message_id, mailbox, text } => get(message_id, mailbox, text, client).await,
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
        MessagesCmd::Move {
            message_id,
            mailbox,
            to_mailbox_id,
        } => move_msg(message_id, mailbox, to_mailbox_id, client).await,
        MessagesCmd::Search {
            query,
            account_id,
            mailbox_id,
        } => search(query, account_id, mailbox_id, client).await,
        MessagesCmd::Suggest {
            prefix,
            scope,
            max,
        } => suggest(prefix, scope, max, client).await,
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

async fn list(mailbox_id: String, verbose: bool, client: MailCatClient) -> Result<()> {
    // list_messages takes (account_id, mailbox_id) but mailbox_id is self-contained
    let messages = client.list_messages("", &mailbox_id).await?;

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

async fn get(message_id: String, mailbox: String, _text_only: bool, client: MailCatClient) -> Result<()> {
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

async fn delete(message_id: String, mailbox: String, permanent: bool, client: MailCatClient) -> Result<()> {
    println!(
        "{} Deleting {} (permanent={})...",
        "→".blue(),
        message_id[..8.min(message_id.len())].bold(),
        permanent
    );

    client.delete_message(&message_id, &mailbox, permanent).await?;

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

    let updated = client.move_message(&message_id, &mailbox, &to_mailbox_id).await?;

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
    println!(
        "{} Searching: {}",
        "→".blue(),
        query.bold()
    );

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
            from: h.from_name.unwrap_or(h.from_email).chars().take(25).collect(),
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

    let suggestions = client
        .suggest(&prefix, scope.as_deref(), max)
        .await?;

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

async fn send_test(
    account_id: String,
    to: String,
    reply_to: Option<(String, String)>,
    client: MailCatClient,
) -> Result<()> {
    if let Some((parent_id, parent_mailbox)) = &reply_to
        && client.get_message(parent_id, parent_mailbox).await?.rfc_message_id.is_none()
    {
        println!(
            "{}",
            "  ⚠ parent message has no RFC Message-ID yet (e.g. a locally-sent \
             message not yet re-synced from IMAP) — this reply will start a new thread"
                .yellow()
        );
    }

    let stamp = chrono::Utc::now().to_rfc3339();
    let subject = format!("[mail-cat test] {stamp}");
    let body_md = format!(
        "Messaggio di test inviato da mail-cat CLI alle {stamp}.\n\
         Verifica: invio SMTP, persistenza del corpo, visibilità in lista, threading."
    );

    println!("{} Sending test message to {}...", "→".blue(), to.bold());

    let message_id = client
        .send_compose(ComposeInput {
            draft_id: None,
            account_id: account_id.clone(),
            to: vec![to],
            cc: vec![],
            bcc: vec![],
            subject,
            body_md: body_md.clone(),
            mode: if reply_to.is_some() { "reply" } else { "new" }.to_owned(),
            source_message_id: reply_to.as_ref().map(|(id, _)| id.clone()),
            attachments: vec![],
            forwarded_attachment_ids: vec![],
        })
        .await?;

    println!("  {} sent, message_id={}", "✓".green(), message_id.bold());

    // Verify (1): message is visible in some mailbox's list right away
    // (should be the Sent folder), with sender/recipient populated.
    let mailboxes = client.list_mailboxes(&account_id).await?;
    let mut found = None;
    for mbx in mailboxes {
        let msgs = client.list_messages(&account_id, &mbx.id).await?;
        if let Some(summary) = msgs.into_iter().find(|m| m.id == message_id) {
            found = Some((mbx, summary));
            break;
        }
    }
    match &found {
        Some((mbx, summary)) => {
            println!(
                "  {} visible in mailbox list ({})",
                "✓".green(),
                mbx.display_name
            );
            println!(
                "      from={} to={:?}",
                summary.from_email, summary.to
            );
        }
        None => println!(
            "  {} not visible in any mailbox list yet (should appear immediately after send)",
            "✗".red()
        ),
    }

    // Verify (2): body round-trips instead of coming back empty. The stored
    // text is the markdown plus signature and quote, so it starts with it.
    if let Some((mbx, _)) = &found {
        let fetched = client.get_message(&message_id, &mbx.id).await?;
        let body_ok = fetched
            .text_body
            .as_deref()
            .is_some_and(|t| t.starts_with(body_md.as_str()));
        println!(
            "  {} body persisted correctly",
            if body_ok { "✓".green() } else { "✗".red() }
        );
    }

    // Verify (3): if replying, the new message threads with its parent.
    if let Some((parent_id, _)) = &reply_to {
        let thread = client.get_thread(&message_id).await?;
        let in_thread = thread.iter().any(|m| &m.id == parent_id);
        println!(
            "  {} threaded with parent message ({} message(s) in thread)",
            if in_thread { "✓".green() } else { "✗".red() },
            thread.len()
        );
    }

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
        .fold(
            (String::new(), 0usize),
            |(mut acc, blanks), line| {
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
            },
        )
        .0
}
