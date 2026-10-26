use super::*;

#[tokio::test]
async fn a_message_without_uid_is_found_on_the_server_before_acting() {
    let (msg, archive, es) = uidless_in_archive();
    let provider = Arc::new(MockEmailProvider::new());
    *provider.found.write().unwrap() = vec![RemoteId::from(55)];
    *provider.detail_response.write().unwrap() = Some(Message {
        body_text: Some("text".into()),
        ..msg.clone()
    });
    let registry = Arc::new(MockProviderRegistry::new(provider.clone()));
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![archive.clone()]));
    let handler =
        MessageCommandHandler::new(es, registry, read_repo, Arc::new(MockEventBus::new()));

    handler.handle_fetch_body(msg.id, archive.id).await.unwrap();

    assert_eq!(*provider.fetched.read().unwrap(), [RemoteId::from(55)]);
    let (stored, _) = handler.load_message(&msg.id).await.unwrap();
    assert_eq!(
        uid_of(&stored),
        Some(RemoteId::from(55)),
        "found UID is recorded"
    );
}

#[tokio::test]
async fn a_message_gone_from_the_server_is_a_clear_error() {
    let (msg, archive, es) = uidless_in_archive();
    let registry = Arc::new(MockProviderRegistry::new(
        Arc::new(MockEmailProvider::new()),
    ));
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![archive.clone()]));
    let handler =
        MessageCommandHandler::new(es, registry, read_repo, Arc::new(MockEventBus::new()));

    let err = handler
        .handle_fetch_body(msg.id, archive.id)
        .await
        .unwrap_err();
    assert!(matches!(err, ApplicationError::NotFound(..)), "{err:?}");
}

#[tokio::test]
async fn marking_a_mailbox_read_reaches_messages_without_uid() {
    let (msg, archive, es) = uidless_in_archive();
    let provider = Arc::new(MockEmailProvider::new());
    *provider.found.write().unwrap() = vec![RemoteId::from(55)];
    let unread = mc_domain::MessageSummary {
        id: msg.id,
        account_id: msg.account_id,
        memberships: vec![Membership {
            mailbox_id: archive.id,
            remote_id: None,
        }],
        from_name: None,
        from_email: "sender@example.com".into(),
        to: vec![],
        cc: vec![],
        subject: "Test".into(),
        flags: vec![],
        size_bytes: 0,
        has_attachments: false,
        received_at: chrono::Utc::now(),
    };
    let read_repo = Arc::new(MockMessageReadRepo {
        unread: vec![unread],
        ..MockMessageReadRepo::with_mailboxes(vec![archive.clone()])
    });
    let registry = Arc::new(MockProviderRegistry::new(provider.clone()));
    let handler =
        MessageCommandHandler::new(es, registry, read_repo, Arc::new(MockEventBus::new()));

    handler
        .handle_mark_mailbox_read(archive.id, archive.path)
        .await
        .unwrap();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }

    assert_eq!(*provider.flagged_bulk.read().unwrap(), [RemoteId::from(55)]);
}

/// Two copies of one Message-ID in the mailbox: the one another row
/// already holds is not this message's.
#[tokio::test]
async fn the_uid_found_skips_copies_other_rows_hold() {
    let (msg, archive, es) = uidless_in_archive();
    let provider = Arc::new(MockEmailProvider::new());
    *provider.found.write().unwrap() = vec![RemoteId::from(55), RemoteId::from(60)];
    *provider.detail_response.write().unwrap() = Some(msg.clone());
    let read_repo = Arc::new(MockMessageReadRepo {
        held: vec![RemoteId::from(60)],
        ..MockMessageReadRepo::with_mailboxes(vec![archive.clone()])
    });
    let handler = MessageCommandHandler::new(
        es,
        Arc::new(MockProviderRegistry::new(provider.clone())),
        read_repo,
        Arc::new(MockEventBus::new()),
    );

    handler.handle_fetch_body(msg.id, archive.id).await.unwrap();

    assert_eq!(*provider.fetched.read().unwrap(), [RemoteId::from(55)]);
}

/// Acting from a mailbox the message is not in is an error: its id there
/// would mean nothing, and it is not looked up in the mailbox it is in.
#[tokio::test]
async fn a_body_is_never_fetched_from_a_mailbox_the_message_is_not_in() {
    let (msg, archive, es) = uidless_in_archive();
    let provider = Arc::new(MockEmailProvider::new());
    *provider.found.write().unwrap() = vec![RemoteId::from(55)];
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![archive]));
    let handler = MessageCommandHandler::new(
        es,
        Arc::new(MockProviderRegistry::new(provider.clone())),
        read_repo,
        Arc::new(MockEventBus::new()),
    );

    let err = handler
        .handle_fetch_body(msg.id, mailbox_of(&msg))
        .await
        .unwrap_err();

    assert!(matches!(err, ApplicationError::NotFound(..)), "{err:?}");
    assert!(provider.fetched.read().unwrap().is_empty());
}

/// A message in two mailboxes: INBOX with id 42 and `other` with `other_id`.
fn in_inbox_and(other_id: Option<u32>) -> (Message, Mailbox, Arc<MockEventStore>) {
    let msg = make_test_message();
    let other = test_mailbox(msg.account_id, "Other", MailboxType::Archive);
    let es = Arc::new(MockEventStore::with_message(&msg));
    es.events
        .write()
        .unwrap()
        .get_mut(&msg.id.to_string())
        .unwrap()
        .push(EventEnvelope::new(
            msg.id.to_string(),
            AggregateType::Message,
            2,
            DomainEvent::MessageMailboxesChanged {
                message_id: msg.id,
                added: vec![Membership {
                    mailbox_id: other.id,
                    remote_id: other_id.map(RemoteId::from),
                }],
                removed: vec![],
            },
        ));
    (msg, other, es)
}

fn body_reader(
    msg: &Message,
    other: &Mailbox,
    es: Arc<MockEventStore>,
) -> (
    MessageCommandHandler<MockEventStore, MockProviderRegistry, MockMessageReadRepo, MockEventBus>,
    Arc<MockEmailProvider>,
) {
    let provider = Arc::new(MockEmailProvider::new());
    *provider.detail_response.write().unwrap() = Some(Message {
        body_text: Some("text".into()),
        ..msg.clone()
    });
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![
        inbox_of(msg),
        other.clone(),
    ]));
    let handler = MessageCommandHandler::new(
        es,
        Arc::new(MockProviderRegistry::new(provider.clone())),
        read_repo,
        Arc::new(MockEventBus::new()),
    );
    (handler, provider)
}

/// The mailbox asked for wins, whichever copy would sort first.
#[tokio::test]
async fn the_body_comes_from_the_copy_in_the_mailbox_asked_for() {
    let (msg, other, es) = in_inbox_and(Some(9));
    let (handler, provider) = body_reader(&msg, &other, es);

    handler.handle_fetch_body(msg.id, other.id).await.unwrap();
    handler
        .handle_fetch_body(msg.id, mailbox_of(&msg))
        .await
        .unwrap();

    assert_eq!(
        *provider.fetched.read().unwrap(),
        [RemoteId::from(9), RemoteId::from(42)]
    );
}

/// Reading is harmless on any copy: with no id in the mailbox asked for, and
/// none found there, the copy that has one stands in.
#[tokio::test]
async fn the_body_falls_back_to_a_copy_with_an_id_when_the_asked_one_has_none() {
    let (msg, other, es) = in_inbox_and(None);
    let (handler, provider) = body_reader(&msg, &other, es);

    handler.handle_fetch_body(msg.id, other.id).await.unwrap();

    assert_eq!(*provider.fetched.read().unwrap(), [RemoteId::from(42)]);
}
