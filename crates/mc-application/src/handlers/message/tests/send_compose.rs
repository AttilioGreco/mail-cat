use super::*;

#[tokio::test]
async fn send_compose_hands_the_sent_mailbox_to_the_provider_and_records_it() {
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

    assert_eq!(
        *f.provider.sent_to.read().unwrap(),
        [Some(f.sent_box.path.clone())]
    );

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

/// The placeholder `handle_send_compose` recorded: the one message in the
/// store that starts with `MessageSent`.
fn placeholder_of(es: &MockEventStore) -> MessageId {
    es.events
        .read()
        .unwrap()
        .values()
        .find_map(|events| match events.first().map(|e| &e.payload) {
            Some(DomainEvent::MessageSent { message_id, .. }) => Some(*message_id),
            _ => None,
        })
        .expect("a sent placeholder")
}

async fn send_reporting(f: &Fixture, copy: SentCopy) -> MessageId {
    *f.provider.sent_copy.write().unwrap() = Some(copy);
    let CommandResult::MessageSent { message_id } = f
        .handler
        .handle_send_compose(
            std::slice::from_ref(&f.account),
            compose_input(f.account.id),
        )
        .await
        .unwrap()
    else {
        panic!("expected MessageSent");
    };
    message_id
}

/// The remote id the placeholder has in Sent; `Err` once it is gone.
async fn sent_copy_id(f: &Fixture, id: MessageId) -> Result<Option<RemoteId>, ApplicationError> {
    let (message, _) = f.handler.load_message(&id).await?;
    Ok(message.membership(f.sent_box.id).unwrap().remote_id.clone())
}

fn event_names(es: &MockEventStore, id: MessageId) -> Vec<&'static str> {
    es.events.read().unwrap()[&id.to_string()]
        .iter()
        .map(|e| e.payload.event_name())
        .collect()
}

#[tokio::test]
async fn a_sent_copy_stored_with_its_id_makes_the_placeholder_that_message() {
    let f = fixture("imap.example.com", |_, _| None);
    let id = send_reporting(
        &f,
        SentCopy::Stored {
            remote_id: Some(RemoteId::from(9)),
        },
    )
    .await;

    assert_eq!(sent_copy_id(&f, id).await.unwrap(), Some(RemoteId::from(9)));
}

/// Without an id, a stored copy is matched by Message-ID when Sent syncs;
/// so is one the server files itself. A copy that could not be stored leaves
/// the placeholder as the only trace of the message.
#[tokio::test]
async fn a_sent_copy_without_an_id_leaves_the_placeholder_for_the_sync() {
    for copy in [
        SentCopy::Stored { remote_id: None },
        SentCopy::ServerFiled,
        SentCopy::NotStored,
    ] {
        let f = fixture("imap.example.com", |_, _| None);
        let id = send_reporting(&f, copy.clone()).await;

        assert_eq!(sent_copy_id(&f, id).await.unwrap(), None, "{copy:?}");
        assert_eq!(event_names(&f.es, id), ["MessageSent"], "{copy:?}");
    }
}

#[tokio::test]
async fn a_failed_send_takes_its_placeholder_back() {
    let f = fixture("imap.example.com", |_, _| None);
    *f.provider.send_fails.write().unwrap() = true;

    let err = f
        .handler
        .handle_send_compose(
            std::slice::from_ref(&f.account),
            compose_input(f.account.id),
        )
        .await
        .unwrap_err();

    assert!(err.to_string().contains("SMTP refused"), "{err}");
    let id = placeholder_of(&f.es);
    assert_eq!(event_names(&f.es, id), ["MessageSent", "MessageDeleted"]);
    assert!(matches!(
        f.handler.load_message(&id).await,
        Err(ApplicationError::NotFound(..))
    ));
}

/// A Sent sync can store the server copy while the send is still returning:
/// it supersedes the placeholder, so the id arriving afterwards has nowhere
/// to go and must not bring the placeholder back.
#[tokio::test]
async fn an_id_arriving_after_a_sync_superseded_the_placeholder_is_dropped() {
    let f = fixture("imap.example.com", |_, _| None);
    let es = Arc::clone(&f.es);
    *f.provider.during_send.write().unwrap() = Some(Box::new(move || {
        let id = placeholder_of(&es);
        let mut events = es.events.write().unwrap();
        let list = events.get_mut(&id.to_string()).unwrap();
        let seq = list.len() as u64 + 1;
        list.push(EventEnvelope::new(
            id.to_string(),
            AggregateType::Message,
            seq,
            DomainEvent::MessageDeleted {
                message_id: id,
                mailbox_ids: vec![],
            },
        ));
    }));

    let id = send_reporting(
        &f,
        SentCopy::Stored {
            remote_id: Some(RemoteId::from(9)),
        },
    )
    .await;

    assert_eq!(event_names(&f.es, id), ["MessageSent", "MessageDeleted"]);
}

/// The other order: the sync stored the server copy as a row of its own and
/// has yet to supersede the placeholder. The id is that row's, not the
/// placeholder's, or the message would be there twice.
#[tokio::test]
async fn an_id_another_row_already_holds_is_not_given_to_the_placeholder() {
    let f = fixture_holding("imap.example.com", |_, _| None, vec![RemoteId::from(9)]);
    let id = send_reporting(
        &f,
        SentCopy::Stored {
            remote_id: Some(RemoteId::from(9)),
        },
    )
    .await;

    assert_eq!(sent_copy_id(&f, id).await.unwrap(), None);
    assert_eq!(event_names(&f.es, id), ["MessageSent"]);
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
async fn save_draft_stores_locally_and_replaces_previous() {
    let f = fixture("imap.example.com", |_, _| None);
    let old = source_message(f.account.id, f.drafts_box.id);
    f.es.add(&old);
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

    let saved = f.provider.saved_drafts.read().unwrap();
    let (path, draft, replacing) = &saved[0];
    assert_eq!(*path, f.drafts_box.path);
    assert_eq!(
        *replacing,
        Some(RemoteId::from(42)),
        "the provider drops the old version"
    );
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

#[tokio::test]
async fn a_first_draft_replaces_nothing() {
    let f = fixture("imap.example.com", |_, _| None);
    let mut input = compose_input(f.account.id);
    input.to.clear();

    f.handler
        .handle_save_draft(std::slice::from_ref(&f.account), input)
        .await
        .unwrap();

    assert_eq!(f.provider.saved_drafts.read().unwrap()[0].2, None);
}
