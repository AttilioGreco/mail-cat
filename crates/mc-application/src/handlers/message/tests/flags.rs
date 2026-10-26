use super::*;

#[tokio::test]
async fn handle_set_flags_marks_as_read() {
    let msg = make_test_message();
    let msg_id = msg.id;

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(
        Arc::new(MockEmailProvider::new()),
    ));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)]));
    let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

    let result = handler
        .handle_set_flags(msg_id, MailboxPath::inbox(), vec![Flag::Seen], vec![])
        .await
        .unwrap();

    assert!(matches!(result, CommandResult::FlagsSet));
    assert_eq!(bus.last_event_name().unwrap(), "MessageFlagged");
}

#[tokio::test]
async fn handle_mark_mailbox_read_with_no_unread_is_noop() {
    let msg = make_test_message();
    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(
        Arc::new(MockEmailProvider::new()),
    ));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::new()); // find_unread_by_mailbox -> []
    let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

    let result = handler
        .handle_mark_mailbox_read(MailboxId::new(), MailboxPath::inbox())
        .await
        .unwrap();

    assert!(matches!(
        result,
        CommandResult::MailboxMarkedRead { count: 0 }
    ));
    assert!(bus.last_event_name().is_none());
}

#[tokio::test]
async fn marking_a_mailbox_read_continues_past_a_failing_message() {
    let msg = make_test_message();
    let summary = |id| mc_domain::MessageSummary {
        id,
        account_id: msg.account_id,
        memberships: vec![Membership {
            mailbox_id: mailbox_of(&msg),
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
    let provider = Arc::new(MockEmailProvider::new());
    let read_repo = MockMessageReadRepo {
        unread: vec![summary(MessageId::new()), summary(msg.id)],
        ..MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])
    };
    let handler = MessageCommandHandler::new(
        Arc::new(MockEventStore::with_message(&msg)),
        Arc::new(MockProviderRegistry::new(provider.clone())),
        Arc::new(read_repo),
        Arc::new(MockEventBus::new()),
    );

    let result = handler
        .handle_mark_mailbox_read(mailbox_of(&msg), MailboxPath::inbox())
        .await
        .unwrap();
    drain_spawned().await;

    assert!(
        matches!(result, CommandResult::MailboxMarkedRead { count: 1 }),
        "{result:?}"
    );
    assert!(handler.load_message(&msg.id).await.unwrap().0.is_read());
    assert_eq!(*provider.flagged_bulk.read().unwrap(), [RemoteId::from(42)]);
}
