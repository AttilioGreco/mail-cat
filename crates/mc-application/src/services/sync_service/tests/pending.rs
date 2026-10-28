//! Messages waiting for a remote id, and our own sent mail and drafts coming back.

use super::*;

#[tokio::test]
async fn sent_copy_syncing_back_replaces_the_one_shown_at_send_time() {
    let account_id = AccountId::new();
    let (sent, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Sent".to_string()], '/').unwrap(),
        MailboxType::Sent,
    );
    let placeholder = MessageSummary {
        id: MessageId::new(),
        account_id,
        memberships: vec![Membership {
            mailbox_id: sent.id,
            remote_id: None,
        }],
        from_name: None,
        from_email: "sender@example.com".into(),
        to: vec![],
        cc: vec![],
        subject: "Test Subject".into(),
        flags: vec![Flag::Seen],
        size_bytes: 0,
        has_attachments: false,
        received_at: chrono::Utc::now(),
    };
    let mut server_copy = make_envelope_summary(7);
    server_copy.message_rfc_id = Some("<sent@example.com>".into());

    let projector = Arc::new(MockProjector::new());
    projector.stored.lock().unwrap().push(placeholder.clone());
    projector.mailboxes.lock().unwrap().push(sent.clone());
    let event_store = Arc::new(MockEventStore::new());
    event_store.events.lock().unwrap().push((
        placeholder.id.to_string(),
        DomainEvent::MessageSent {
            message_id: placeholder.id,
            account_id,
            mailbox_id: sent.id,
            envelope: test_envelope(),
            message_rfc_id: Some("<sent@example.com>".into()),
            in_reply_to: None,
            references: vec![],
            body_text: None,
            body_html: None,
            sent_at: chrono::Utc::now(),
        },
    ));
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![sent])
        .with_envelopes(vec![server_copy]);
    let service = make_service(provider, event_store, projector.clone());
    service.start_sync(&account_id).await.unwrap();

    let message_events: Vec<_> = projector
        .projected_events()
        .into_iter()
        .filter(|e| {
            matches!(
                e,
                DomainEvent::MessageReceived { .. } | DomainEvent::MessageDeleted { .. }
            )
        })
        .collect();
    assert!(
        matches!(
            message_events.as_slice(),
            [DomainEvent::MessageReceived { .. }, DomainEvent::MessageDeleted { message_id, .. }]
                if *message_id == placeholder.id
        ),
        "server copy stored, then the placeholder dropped: {message_events:?}"
    );
}

/// Gmail files our sent mail under another label too, and that label syncs
/// first: the Sent copy joins the stored message by key, and the placeholder
/// shown at send time still goes.
#[tokio::test]
async fn a_sent_copy_joining_by_key_replaces_the_placeholder() {
    let account_id = AccountId::new();
    let inbox = inbox_mailbox(account_id);
    let (sent, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Sent".to_string()], '/').unwrap(),
        MailboxType::Sent,
    );
    let placeholder = MessageSummary {
        id: MessageId::new(),
        account_id,
        memberships: vec![Membership {
            mailbox_id: sent.id,
            remote_id: None,
        }],
        from_name: None,
        from_email: "sender@example.com".into(),
        to: vec![],
        cc: vec![],
        subject: "Test Subject".into(),
        flags: vec![Flag::Seen],
        size_bytes: 0,
        has_attachments: false,
        received_at: chrono::Utc::now(),
    };
    let mut server_copy = make_envelope_summary(7);
    server_copy.message_rfc_id = Some("<sent@example.com>".into());
    server_copy.key = Some(mc_domain::MessageKey::new("1278455344230334865"));

    let projector = Arc::new(MockProjector::new());
    projector.stored.lock().unwrap().push(placeholder.clone());
    projector
        .rfc_ids
        .lock()
        .unwrap()
        .insert(placeholder.id, "<sent@example.com>".into());
    projector
        .mailboxes
        .lock()
        .unwrap()
        .extend([inbox.clone(), sent.clone()]);
    let event_store = Arc::new(MockEventStore::new());
    event_store.events.lock().unwrap().push((
        placeholder.id.to_string(),
        DomainEvent::MessageSent {
            message_id: placeholder.id,
            account_id,
            mailbox_id: sent.id,
            envelope: test_envelope(),
            message_rfc_id: Some("<sent@example.com>".into()),
            in_reply_to: None,
            references: vec![],
            body_text: None,
            body_html: None,
            sent_at: chrono::Utc::now(),
        },
    ));
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![inbox, sent])
        .with_envelopes(vec![server_copy]);
    let service = make_service(provider, event_store, projector.clone());
    service.start_sync(&account_id).await.unwrap();

    let stored = projector.stored.lock().unwrap();
    assert!(
        stored.iter().all(|m| m.id != placeholder.id),
        "placeholder superseded: {stored:?}"
    );
    assert_eq!(stored.len(), 1, "one message, in Inbox and Sent");
    assert_eq!(stored[0].memberships.len(), 2);
}

/// The provider already filed the sent copy and reported its id, which went
/// to the placeholder: the Sent sync that then sees that copy has nothing to
/// store, and nothing to supersede.
#[tokio::test]
async fn a_placeholder_that_already_holds_the_copys_id_is_not_duplicated() {
    let account_id = AccountId::new();
    let (sent, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Sent".to_string()], '/').unwrap(),
        MailboxType::Sent,
    );
    let placeholder = MessageSummary {
        id: MessageId::new(),
        account_id,
        memberships: vec![Membership {
            mailbox_id: sent.id,
            remote_id: Some(RemoteId::from(7)),
        }],
        from_name: None,
        from_email: "sender@example.com".into(),
        to: vec![],
        cc: vec![],
        subject: "Test Subject".into(),
        flags: vec![Flag::Seen],
        size_bytes: 0,
        has_attachments: false,
        received_at: chrono::Utc::now(),
    };
    let mut server_copy = make_envelope_summary(7);
    server_copy.message_rfc_id = Some("<sent@example.com>".into());

    let projector = Arc::new(MockProjector::new());
    projector.stored.lock().unwrap().push(placeholder.clone());
    projector.mailboxes.lock().unwrap().push(sent.clone());
    let event_store = Arc::new(MockEventStore::new());
    event_store.events.lock().unwrap().push((
        placeholder.id.to_string(),
        DomainEvent::MessageSent {
            message_id: placeholder.id,
            account_id,
            mailbox_id: sent.id,
            envelope: test_envelope(),
            message_rfc_id: Some("<sent@example.com>".into()),
            in_reply_to: None,
            references: vec![],
            body_text: None,
            body_html: None,
            sent_at: chrono::Utc::now(),
        },
    ));
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![sent])
        .with_envelopes(vec![server_copy]);
    let service = make_service(provider, event_store, projector.clone());
    service.start_sync(&account_id).await.unwrap();

    let stored_or_dropped = projector
        .projected_events()
        .into_iter()
        .filter(|e| {
            matches!(
                e,
                DomainEvent::MessageReceived { .. } | DomainEvent::MessageDeleted { .. }
            )
        })
        .count();
    assert_eq!(
        stored_or_dropped, 0,
        "the placeholder is already the message"
    );
}

#[tokio::test]
async fn moved_message_gets_its_uid_back_instead_of_a_duplicate() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let moved = seed(
        &event_store,
        &projector,
        &archive,
        None,
        "<moved@example.com>",
    );
    projector.mailboxes.lock().unwrap().push(archive.clone());

    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(vec![envelope(9, "<moved@example.com>")]);
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(
        received(&projector),
        0,
        "no second copy of the moved message"
    );
    assert_eq!(row_uid(&projector, moved), Some(Some(9)));
}

/// The read model can lag the log: a message it still lists here, waiting
/// for a UID, may have been moved on. Re-attaching from that stale view
/// appended a `MessageMailboxesChanged` re-adding Archive and pulled the
/// message back.
#[tokio::test]
async fn reattach_does_not_pull_back_a_message_moved_on() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let moved = seed(
        &event_store,
        &projector,
        &archive,
        None,
        "<moved@example.com>",
    );
    let trash = MailboxId::new();
    event_store.events.lock().unwrap().push((
        moved.to_string(),
        DomainEvent::MessageMailboxesChanged {
            message_id: moved,
            added: vec![Membership {
                mailbox_id: trash,
                remote_id: None,
            }],
            removed: vec![archive.id],
        },
    ));
    projector.mailboxes.lock().unwrap().push(archive.clone());

    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive.clone()])
        .with_envelopes(vec![envelope(9, "<moved@example.com>")]);
    make_service(provider, event_store.clone(), projector)
        .start_sync(&account_id)
        .await
        .unwrap();

    let pulled_back = event_store.appended_events().into_iter().any(|(id, e)| {
        id == moved.to_string() && matches!(e, DomainEvent::MessageMailboxesChanged { added, removed, .. } if removed.is_empty() && added.iter().any(|m| m.mailbox_id == archive.id))
    });
    assert!(!pulled_back);
}

/// Only a real placeholder in Sent is replaced by the server copy; a
/// received message waiting for its UID elsewhere is not one.
#[tokio::test]
async fn sent_copy_leaves_a_waiting_message_elsewhere_alone() {
    let account_id = AccountId::new();
    let (sent, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Sent".to_string()], '/').unwrap(),
        MailboxType::Sent,
    );
    let archive = archive_mailbox(account_id);
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let waiting = seed(
        &event_store,
        &projector,
        &archive,
        None,
        "<sent@example.com>",
    );
    projector
        .mailboxes
        .lock()
        .unwrap()
        .extend([sent.clone(), archive]);

    let mut provider = MockEmailProvider::new()
        .with_mailboxes(vec![sent])
        .with_envelopes(vec![envelope(7, "<sent@example.com>")]);
    provider
        .found
        .insert("Archive".into(), vec![RemoteId::from(4)]);
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(
        row_uid(&projector, waiting),
        Some(None),
        "still there, still waiting"
    );
}

/// A row waiting for a UID that the server no longer has goes; reconcile
/// works by UID and would keep it, unactionable, forever. A Sent
/// placeholder waits for its server copy and stays.
#[tokio::test]
async fn a_uid_less_row_the_server_no_longer_has_is_deleted_but_a_placeholder_stays() {
    let account_id = AccountId::new();
    let inbox = inbox_mailbox(account_id);
    let (sent, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Sent".to_string()], '/').unwrap(),
        MailboxType::Sent,
    );
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let stranded = seed(&event_store, &projector, &inbox, None, "<gone@example.com>");
    let placeholder = MessageId::new();
    event_store.events.lock().unwrap().push((
        placeholder.to_string(),
        DomainEvent::MessageSent {
            message_id: placeholder,
            account_id,
            mailbox_id: sent.id,
            envelope: test_envelope(),
            message_rfc_id: Some("<out@example.com>".into()),
            in_reply_to: None,
            references: vec![],
            body_text: None,
            body_html: None,
            sent_at: chrono::Utc::now(),
        },
    ));
    let mut row = archived(account_id, sent.id, None);
    row.id = placeholder;
    projector.stored.lock().unwrap().push(row);
    projector
        .mailboxes
        .lock()
        .unwrap()
        .extend([inbox.clone(), sent.clone()]);

    let provider = MockEmailProvider::new().with_mailboxes(vec![inbox, sent]);
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(row_uid(&projector, stranded), None, "deleted");
    assert_eq!(
        row_uid(&projector, placeholder),
        Some(None),
        "still waiting for its server copy"
    );
}

/// A move the server confirmed without saying where the message went
/// leaves it waiting in its old mailbox; the copy that turns up in the
/// destination is that message, not a new one.
#[tokio::test]
async fn an_orphan_is_found_again_in_another_mailbox_under_its_own_id() {
    let account_id = AccountId::new();
    let (inbox, archive) = (inbox_mailbox(account_id), archive_mailbox(account_id));
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let orphan = seed(
        &event_store,
        &projector,
        &inbox,
        None,
        "<moved@example.com>",
    );
    projector
        .mailboxes
        .lock()
        .unwrap()
        .extend([inbox, archive.clone()]);

    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive.clone()])
        .with_envelopes(vec![envelope(9, "<moved@example.com>")]);
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(received(&projector), 0, "no duplicate");
    let row = projector
        .stored
        .lock()
        .unwrap()
        .iter()
        .find(|m| m.id == orphan)
        .cloned()
        .unwrap();
    assert_eq!(
        (
            row.memberships[0].mailbox_id,
            row.memberships[0].remote_id.clone()
        ),
        (archive.id, Some(RemoteId::from(9)))
    );
}

/// The same Message-ID can legitimately sit in two mailboxes. One still
/// on the server in its own mailbox is not taken over by the other copy.
#[tokio::test]
async fn a_copy_still_in_its_own_mailbox_is_not_taken_over() {
    let account_id = AccountId::new();
    let (inbox, archive) = (inbox_mailbox(account_id), archive_mailbox(account_id));
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let waiting = seed(&event_store, &projector, &inbox, None, "<both@example.com>");
    let held = seed(
        &event_store,
        &projector,
        &inbox,
        Some(3),
        "<both@example.com>",
    );
    projector
        .mailboxes
        .lock()
        .unwrap()
        .extend([inbox, archive.clone()]);

    let mut provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(vec![envelope(9, "<both@example.com>")]);
    provider
        .found
        .insert("INBOX".into(), vec![RemoteId::from(3), RemoteId::from(5)]);
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(received(&projector), 1, "a second copy of its own");
    assert_eq!(row_uid(&projector, waiting), Some(None));
    assert_eq!(row_uid(&projector, held), Some(Some(3)));
}

/// The ids of a mailbox were invalidated and then ingested whole: the received
/// messages still unmatched are gone, but a Sent placeholder waits for its
/// server copy as it did before.
#[tokio::test]
async fn after_a_reset_unmatched_messages_go_but_sent_placeholders_stay() {
    let account_id = AccountId::new();
    let (sent, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Sent".to_string()], '/').unwrap(),
        MailboxType::Sent,
    );
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let vanished = seed(
        &event_store,
        &projector,
        &sent,
        Some(5),
        "<vanished@example.com>",
    );
    let placeholder = MessageId::new();
    event_store.events.lock().unwrap().push((
        placeholder.to_string(),
        DomainEvent::MessageSent {
            message_id: placeholder,
            account_id,
            mailbox_id: sent.id,
            envelope: test_envelope(),
            message_rfc_id: Some("<out@example.com>".into()),
            in_reply_to: None,
            references: vec![],
            body_text: None,
            body_html: None,
            sent_at: chrono::Utc::now(),
        },
    ));
    let mut row = archived(account_id, sent.id, None);
    row.id = placeholder;
    projector.stored.lock().unwrap().push(row);
    projector.mailboxes.lock().unwrap().push(sent.clone());
    projector
        .put_cursor(&account_id, &sent.id.to_string(), &cursor_at(1, 50))
        .await
        .unwrap();

    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![sent])
        .with_status(2, 1, 0, 0);
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(
        row_uid(&projector, vanished),
        None,
        "not in the mailbox any more"
    );
    assert_eq!(
        row_uid(&projector, placeholder),
        Some(None),
        "still waiting for its server copy"
    );
}
