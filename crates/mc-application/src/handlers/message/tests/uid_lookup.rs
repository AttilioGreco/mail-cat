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

    handler
        .handle_fetch_body(msg.id, archive.path.clone())
        .await
        .unwrap();

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
        .handle_set_flags(msg.id, archive.path, vec![Flag::Seen], vec![])
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

    handler
        .handle_fetch_body(msg.id, archive.path.clone())
        .await
        .unwrap();

    assert_eq!(*provider.fetched.read().unwrap(), [RemoteId::from(55)]);
}

#[tokio::test]
async fn a_uid_is_never_looked_up_in_another_mailbox() {
    let (msg, archive, es) = uidless_in_archive();
    let provider = Arc::new(MockEmailProvider::new());
    *provider.found.write().unwrap() = vec![RemoteId::from(55)];
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![archive]));
    let handler = MessageCommandHandler::new(
        es,
        Arc::new(MockProviderRegistry::new(provider)),
        read_repo,
        Arc::new(MockEventBus::new()),
    );

    let err = handler
        .handle_fetch_body(msg.id, MailboxPath::inbox())
        .await
        .unwrap_err();
    assert!(
        matches!(err, ApplicationError::PreconditionFailed(_)),
        "{err:?}"
    );
}
