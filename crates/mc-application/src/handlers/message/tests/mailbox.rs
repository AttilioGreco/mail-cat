use super::*;

struct Deleting {
    handler: MessageCommandHandler<
        MockEventStore,
        MockProviderRegistry,
        MockMessageReadRepo,
        MockEventBus,
    >,
    provider: Arc<MockEmailProvider>,
    repo: Arc<MockMessageReadRepo>,
    bus: Arc<MockEventBus>,
    junk: mc_domain::Mailbox,
    sent: mc_domain::Mailbox,
}

/// A role-less `Sent` holding `count` messages, next to the real `Sent Items`.
fn deleting(count: usize, provider: MockEmailProvider) -> Deleting {
    let account = AccountId::new();
    let junk = test_mailbox(account, "Sent", MailboxType::Custom);
    let sent = test_mailbox(account, "Sent Items", MailboxType::Sent);
    let es = Arc::new(MockEventStore::new());
    let mut listing = Vec::new();
    for uid in 0..count {
        let (message, _) = Message::receive(
            account,
            junk.id,
            RemoteId::from(uid as u32 + 1),
            None,
            None,
            vec![],
            test_envelope(),
            vec![],
            10,
            false,
            chrono::Utc::now(),
        );
        es.add(&message);
        listing.push(mc_domain::MessageSummary {
            id: message.id,
            account_id: account,
            memberships: message.memberships.clone(),
            from_name: None,
            from_email: "a@example.com".into(),
            to: vec![],
            cc: vec![],
            subject: String::new(),
            flags: vec![],
            size_bytes: 10,
            has_attachments: false,
            received_at: message.received_at,
        });
    }
    let repo = Arc::new(MockMessageReadRepo {
        mailboxes: vec![
            junk.clone(),
            sent.clone(),
            test_mailbox(account, "INBOX", MailboxType::Inbox),
        ],
        listing: std::sync::Mutex::new(listing),
        ..MockMessageReadRepo::new()
    });
    let provider = Arc::new(provider);
    let bus = Arc::new(MockEventBus::new());
    let handler = MessageCommandHandler::new(
        es,
        Arc::new(MockProviderRegistry::new(provider.clone())),
        repo.clone(),
        bus.clone(),
    );
    Deleting {
        handler,
        provider,
        repo,
        bus,
        junk,
        sent,
    }
}

fn refused(result: Result<CommandResult, ApplicationError>) -> String {
    match result {
        Err(ApplicationError::PreconditionFailed(why)) => why,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn a_system_mailbox_is_never_deleted() {
    let d = deleting(0, MockEmailProvider::new());

    let why = refused(d.handler.handle_delete_mailbox(d.sent.id, None).await);

    assert!(why.contains("system mailbox"), "{why}");
    let inbox = d.repo.mailboxes[2].id;
    refused(d.handler.handle_delete_mailbox(inbox, None).await);
    assert!(d.provider.deleted_mailboxes.read().unwrap().is_empty());
}

#[tokio::test]
async fn a_mailbox_with_messages_needs_somewhere_to_move_them() {
    let d = deleting(2, MockEmailProvider::moving_to(900));

    let why = refused(d.handler.handle_delete_mailbox(d.junk.id, None).await);

    assert!(why.contains("holds 2 messages"), "{why}");
    assert!(d.provider.moved.read().unwrap().is_empty());
    assert!(d.provider.deleted_mailboxes.read().unwrap().is_empty());
}

#[tokio::test]
async fn messages_are_moved_then_the_mailbox_is_deleted() {
    let d = deleting(3, MockEmailProvider::moving_to(900));

    let result = d
        .handler
        .handle_delete_mailbox(d.junk.id, Some(d.sent.id))
        .await
        .unwrap();

    assert!(matches!(result, CommandResult::MailboxDeleted { moved: 3 }));
    assert_eq!(d.provider.moved.read().unwrap().len(), 3);
    assert_eq!(
        *d.provider.deleted_mailboxes.read().unwrap(),
        std::slice::from_ref(&d.junk.path)
    );
    assert_eq!(d.bus.last_event_name().unwrap(), "MailboxDeleted");
}

#[tokio::test]
async fn an_empty_mailbox_is_deleted_without_a_destination() {
    let d = deleting(0, MockEmailProvider::new());

    d.handler
        .handle_delete_mailbox(d.junk.id, None)
        .await
        .unwrap();

    assert_eq!(d.provider.deleted_mailboxes.read().unwrap().len(), 1);
}

#[tokio::test]
async fn a_failed_move_keeps_the_mailbox() {
    let provider = MockEmailProvider::moving_to(900);
    *provider.refuse.write().unwrap() = true;
    let d = deleting(2, provider);

    let why = refused(
        d.handler
            .handle_delete_mailbox(d.junk.id, Some(d.sent.id))
            .await,
    );

    assert!(
        why.contains("mailbox kept") && why.contains("of 2"),
        "{why}"
    );
    assert!(d.provider.deleted_mailboxes.read().unwrap().is_empty());
    assert_ne!(d.bus.last_event_name().as_deref(), Some("MailboxDeleted"));
}

#[tokio::test]
async fn the_destination_must_be_another_mailbox_of_the_account() {
    let d = deleting(1, MockEmailProvider::moving_to(900));

    refused(
        d.handler
            .handle_delete_mailbox(d.junk.id, Some(d.junk.id))
            .await,
    );
    refused(
        d.handler
            .handle_delete_mailbox(d.junk.id, Some(MailboxId::new()))
            .await,
    );
    assert!(d.provider.deleted_mailboxes.read().unwrap().is_empty());
}
