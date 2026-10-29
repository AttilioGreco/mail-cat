use anyhow::Result;
use colored::Colorize;
use mc_client::{ComposeInput, MailCatClient};

pub(super) async fn send_test(
    account_id: String,
    to: String,
    reply_to: Option<(String, String)>,
    client: MailCatClient,
) -> Result<()> {
    if let Some((parent_id, parent_mailbox)) = &reply_to
        && client
            .get_message(parent_id, parent_mailbox)
            .await?
            .rfc_message_id
            .is_none()
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
            println!("      from={} to={:?}", summary.from_email, summary.to);
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
            if in_thread {
                "✓".green()
            } else {
                "✗".red()
            },
            thread.len()
        );
    }

    Ok(())
}
