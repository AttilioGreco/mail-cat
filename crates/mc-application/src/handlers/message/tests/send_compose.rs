use super::*;

#[tokio::test]
async fn send_compose_sends_appends_to_sent_and_records_it() {
    let f = fixture("imap.example.com", |_, _| None);
    let result = f
        .handler
        .handle_send_compose(
            std::slice::from_ref(&f.account),
            compose_input(f.account.id),
        )
        .await
        .unwrap();
    assert!(matches!(result, CommandResult::MessageSent { .. }));
    drain_spawned().await;

    let sent = f.provider.sent.read().unwrap();
    let out = &sent[0];
    assert_eq!(out.from.name.as_deref(), Some("Mario Rossi"));
    assert_eq!(out.to[0].name.as_deref(), Some("Anna"));
    assert_eq!(out.attachments.len(), 1, "attachments must reach SMTP");
    assert!(
        out.body_html
            .as_deref()
            .unwrap()
            .contains("<strong>ciao</strong>")
    );
    assert_eq!(out.body_text.as_deref(), Some("**ciao**\n\n-- \nMario"));

    let appended = f.provider.appended.read().unwrap();
    assert_eq!(appended.len(), 1);
    assert_eq!(appended[0].0, f.sent_box.path);
    assert_eq!(appended[0].1.message_rfc_id, out.message_rfc_id);

    // The sent event carries the body, or a refetch of the sent message comes back empty.
    let published = f.bus.published.read().unwrap();
    let DomainEvent::MessageSent {
        body_text,
        mailbox_id,
        ..
    } = &published.last().unwrap().payload
    else {
        panic!("expected MessageSent event");
    };
    assert_eq!(body_text, &out.body_text);
    assert_eq!(*mailbox_id, f.sent_box.id);
}

#[tokio::test]
async fn gmail_sent_copy_is_not_appended() {
    let f = fixture("imap.gmail.com", |_, _| None);
    f.handler
        .handle_send_compose(
            std::slice::from_ref(&f.account),
            compose_input(f.account.id),
        )
        .await
        .unwrap();
    drain_spawned().await;
    assert!(f.provider.appended.read().unwrap().is_empty());
}

#[tokio::test]
async fn send_compose_rejects_bad_or_missing_recipients() {
    let f = fixture("imap.example.com", |_, _| None);
    let accounts = std::slice::from_ref(&f.account);

    let mut input = compose_input(f.account.id);
    input.cc = vec!["anna@".into()];
    let err = f
        .handler
        .handle_send_compose(accounts, input)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("anna@"), "{err}");

    let mut input = compose_input(f.account.id);
    input.to.clear();
    assert!(
        f.handler
            .handle_send_compose(accounts, input)
            .await
            .is_err()
    );
    assert!(f.provider.sent.read().unwrap().is_empty());
}

#[tokio::test]
async fn reply_all_is_prepared_and_sent_threaded_with_quote() {
    let f = fixture("imap.example.com", |account, inbox| {
        Some(source_message(account, inbox))
    });
    let source = f.seeded.clone().unwrap();
    let accounts = std::slice::from_ref(&f.account);

    let CommandResult::ComposePrepared(view) = f
        .handler
        .handle_prepare_compose(accounts, ComposeMode::ReplyAll, Some(source.id), None)
        .await
        .unwrap()
    else {
        panic!("expected ComposePrepared");
    };
    assert_eq!(view.subject, "Re: Budget");
    assert_eq!(view.to, ["Anna <anna@example.com>"]);
    assert_eq!(view.cc, ["bob@example.com"]);
    assert!(
        view.quoted_html
            .as_deref()
            .unwrap()
            .contains("testo originale")
    );

    let input = ComposeInput {
        to: view.to.clone(),
        cc: view.cc.clone(),
        subject: view.subject.clone(),
        mode: view.mode,
        source_message_id: view.source_message_id,
        attachments: vec![],
        ..compose_input(view.account_id)
    };
    f.handler
        .handle_send_compose(accounts, input)
        .await
        .unwrap();

    let sent = f.provider.sent.read().unwrap();
    assert_eq!(sent[0].in_reply_to.as_deref(), Some("<orig@example.com>"));
    assert_eq!(
        sent[0].references,
        [
            "<root@example.com>",
            "<parent@example.com>",
            "<orig@example.com>"
        ]
    );
    assert!(
        sent[0]
            .body_text
            .as_deref()
            .unwrap()
            .ends_with("ha scritto:\n> testo originale")
    );

    let published = f.bus.published.read().unwrap();
    assert!(published.iter().any(|e| matches!(
        &e.payload,
        DomainEvent::MessageFlagged { message_id, flags_added, .. }
            if *message_id == source.id && flags_added == &[Flag::Answered]
    )));
}

#[tokio::test]
async fn forward_carries_kept_attachments_and_inline_images() {
    let f = fixture("imap.example.com", |account, inbox| {
        Some(source_message(account, inbox))
    });
    let source = f.seeded.clone().unwrap();
    *f.provider.raw.write().unwrap() = b"Content-Type: multipart/mixed; boundary=\"M\"\r\n\r\n\
--M\r\nContent-Type: multipart/related; boundary=\"R\"\r\n\r\n\
--R\r\nContent-Type: text/html\r\n\r\n<p>Ecco <img src=\"cid:logo@x\"></p>\r\n\
--R\r\nContent-Type: image/png\r\nContent-ID: <logo@x>\r\nContent-Transfer-Encoding: base64\r\n\r\nAQID\r\n\
--R--\r\n\
--M\r\nContent-Type: application/pdf; name=\"a.pdf\"\r\nContent-Disposition: attachment; filename=\"a.pdf\"\r\nContent-Transfer-Encoding: base64\r\n\r\nBAUG\r\n\
--M--\r\n"
        .to_vec();
    let accounts = std::slice::from_ref(&f.account);

    let CommandResult::ComposePrepared(view) = f
        .handler
        .handle_prepare_compose(accounts, ComposeMode::Forward, Some(source.id), None)
        .await
        .unwrap()
    else {
        panic!("expected ComposePrepared");
    };
    assert_eq!(view.subject, "Fwd: Budget");
    assert!(view.to.is_empty());
    let names: Vec<_> = view
        .forwarded_attachments
        .iter()
        .map(|a| a.filename.as_str())
        .collect();
    assert_eq!(names, ["a.pdf"]);

    let input = ComposeInput {
        subject: view.subject.clone(),
        mode: view.mode,
        source_message_id: view.source_message_id,
        attachments: vec![],
        forwarded_attachment_ids: vec![view.forwarded_attachments[0].id.clone()],
        ..compose_input(view.account_id)
    };
    f.handler
        .handle_send_compose(accounts, input)
        .await
        .unwrap();

    let sent = f.provider.sent.read().unwrap();
    let parts: Vec<_> = sent[0]
        .attachments
        .iter()
        .map(|a| {
            (
                a.filename.as_str(),
                a.content_id.as_deref(),
                a.data.as_slice(),
            )
        })
        .collect();
    assert_eq!(
        parts,
        [
            ("a.pdf", None, &[4u8, 5, 6][..]),
            ("allegato", Some("logo@x"), &[1, 2, 3][..])
        ]
    );
    assert!(
        sent[0]
            .body_html
            .as_deref()
            .unwrap()
            .contains("Inizio messaggio inoltrato")
    );
    assert!(sent[0].in_reply_to.is_none());
}

#[tokio::test]
async fn draft_restores_markdown_recipients_and_attachments() {
    let f = fixture("imap.example.com", |account, inbox| {
        Some(source_message(account, inbox))
    });
    let draft = f.seeded.clone().unwrap();
    let headers: String =
        compose::draft_headers(ComposeMode::Forward, None, "**ciao**\n![](cid:img@x)")
            .into_iter()
            .map(|(k, v)| format!("{k}: {v}\r\n"))
            .collect();
    *f.provider.raw.write().unwrap() = format!(
        "{headers}To: Anna <anna@example.com>\r\nBcc: b@example.com\r\nSubject: Bozza\r\n\
Content-Type: multipart/mixed; boundary=\"M\"\r\n\r\n\
--M\r\nContent-Type: text/plain\r\n\r\nciao\r\n\
--M\r\nContent-Type: application/pdf\r\nContent-Disposition: attachment; filename=\"a.pdf\"\r\n\r\nPDF\r\n\
--M\r\nContent-Type: image/png\r\nContent-ID: <img@x>\r\n\r\nPNG\r\n\
--M--\r\n"
    )
    .into_bytes();

    let CommandResult::ComposePrepared(view) = f
        .handler
        .handle_prepare_compose(
            std::slice::from_ref(&f.account),
            ComposeMode::Draft,
            Some(draft.id),
            None,
        )
        .await
        .unwrap()
    else {
        panic!("expected ComposePrepared");
    };
    assert_eq!(view.draft_id, Some(draft.id));
    assert_eq!(view.body_md, "**ciao**\n![](cid:img@x)");
    assert_eq!(view.to, ["Anna <anna@example.com>"]);
    assert_eq!(view.bcc, ["b@example.com"]);
    assert_eq!(view.subject, "Bozza");
    // A forward whose original is unknown carries on as a new message.
    assert_eq!(view.mode, ComposeMode::New);
    assert_eq!(view.forwarded_attachments.len(), 1);
    let inline: Vec<_> = view
        .inline_images
        .iter()
        .map(|i| (i.content_id.as_deref(), i.data.as_slice()))
        .collect();
    assert_eq!(inline, [(Some("img@x"), &b"PNG"[..])]);
}

#[tokio::test]
async fn save_draft_appends_stores_locally_and_replaces_previous() {
    let f = fixture("imap.example.com", |account, inbox| {
        Some(source_message(account, inbox))
    });
    let old = f.seeded.clone().unwrap();
    let mut input = compose_input(f.account.id);
    input.draft_id = Some(old.id);
    input.to.clear();

    let CommandResult::DraftSaved { message_id } = f
        .handler
        .handle_save_draft(std::slice::from_ref(&f.account), input)
        .await
        .unwrap()
    else {
        panic!("expected DraftSaved");
    };
    assert_ne!(message_id, old.id);

    let appended = f.provider.appended.read().unwrap();
    let (path, draft, flags) = &appended[0];
    assert_eq!(*path, f.drafts_box.path);
    assert!(flags.contains(&Flag::Draft));
    assert!(
        draft
            .extra_headers
            .iter()
            .any(|(k, v)| k == "X-Mail-Cat-Mode" && v == "new")
    );
    assert!(f.provider.sent.read().unwrap().is_empty());

    let events: Vec<_> = f
        .bus
        .published
        .read()
        .unwrap()
        .iter()
        .map(|e| (e.aggregate_id.clone(), e.payload.event_name()))
        .collect();
    assert!(events.contains(&(message_id.to_string(), "MessageReceived")));
    assert!(events.contains(&(message_id.to_string(), "MessageBodyFetched")));
    assert!(events.contains(&(old.id.to_string(), "MessageDeleted")));
}
