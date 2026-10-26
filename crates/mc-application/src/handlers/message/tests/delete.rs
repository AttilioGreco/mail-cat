use super::*;

#[tokio::test]
async fn handle_delete_produces_event() {
    let msg = make_test_message();
    let msg_id = msg.id;
    let mbx_id = mailbox_of(&msg);

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::new(
        MockEmailProvider::moving_to(900),
    )));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)]));
    let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

    let result = handler
        .handle_delete(msg_id, mbx_id, MailboxPath::inbox(), false)
        .await
        .unwrap();

    assert!(matches!(result, CommandResult::MessageDeleted));
    assert_eq!(bus.last_event_name().unwrap(), "MessageDeleted");
}

#[tokio::test]
async fn handle_delete_moves_to_trash_when_trash_exists() {
    let msg = make_test_message();
    let msg_id = msg.id;
    let mbx_id = mailbox_of(&msg);

    // A Trash folder exists for this account and the message is not in it,
    // so a plain delete must move-to-Trash (MessageMailboxesChanged), not expunge.
    let trash = mc_domain::Mailbox {
        id: MailboxId::new(),
        account_id: msg.account_id,
        path: MailboxPath::parse("Trash", '/').unwrap(),
        display_name: "Trash".into(),
        mailbox_type: MailboxType::Trash,
        flags: vec![],
        total_messages: 0,
        unread_messages: 0,
    };

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::new(
        MockEmailProvider::moving_to(900),
    )));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![
        inbox_of(&msg),
        trash,
    ]));
    let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

    let result = handler
        .handle_delete(msg_id, mbx_id, MailboxPath::inbox(), false)
        .await
        .unwrap();

    assert!(matches!(result, CommandResult::MessageDeleted));
    assert_eq!(bus.last_event_name().unwrap(), "MessageMailboxesChanged");
}

/// The bulk path must survive ids it cannot resolve: a search index that is
/// behind the mailbox will hand it messages that no longer exist, and one of
/// those must not abort the rest of the batch.
#[tokio::test]
async fn handle_delete_many_counts_partial_success() {
    let msg = make_test_message();
    let msg_id = msg.id;

    let source = mc_domain::Mailbox {
        id: mailbox_of(&msg),
        account_id: msg.account_id,
        path: MailboxPath::inbox(),
        display_name: "INBOX".into(),
        mailbox_type: MailboxType::Inbox,
        flags: vec![],
        total_messages: 0,
        unread_messages: 0,
    };
    let trash = mc_domain::Mailbox {
        id: MailboxId::new(),
        account_id: msg.account_id,
        path: MailboxPath::parse("Trash", '/').unwrap(),
        display_name: "Trash".into(),
        mailbox_type: MailboxType::Trash,
        flags: vec![],
        total_messages: 0,
        unread_messages: 0,
    };

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::new(
        MockEmailProvider::moving_to(900),
    )));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![source, trash]));
    let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

    // One real message, one id that isn't in the event store.
    let result = handler
        .handle_delete_many(vec![msg_id, MessageId::new()], false)
        .await
        .unwrap();

    assert!(
        matches!(
            result,
            CommandResult::MessagesBulkChanged {
                succeeded: 1,
                failed: 1
            }
        ),
        "expected 1 ok / 1 skipped, got {result:?}"
    );
    // Trash exists and the message wasn't in it, so a plain bulk delete moves.
    assert_eq!(bus.last_event_name().unwrap(), "MessageMailboxesChanged");
}

#[tokio::test]
async fn handle_delete_permanent_expunges_even_with_trash() {
    let msg = make_test_message();
    let msg_id = msg.id;
    let mbx_id = mailbox_of(&msg);

    let trash = mc_domain::Mailbox {
        id: MailboxId::new(),
        account_id: msg.account_id,
        path: MailboxPath::parse("Trash", '/').unwrap(),
        display_name: "Trash".into(),
        mailbox_type: MailboxType::Trash,
        flags: vec![],
        total_messages: 0,
        unread_messages: 0,
    };

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(
        Arc::new(MockEmailProvider::new()),
    ));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![
        inbox_of(&msg),
        trash,
    ]));
    let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

    // permanently = true → expunge, MessageDeleted, even though Trash exists.
    let result = handler
        .handle_delete(msg_id, mbx_id, MailboxPath::inbox(), true)
        .await
        .unwrap();

    assert!(matches!(result, CommandResult::MessageDeleted));
    assert_eq!(bus.last_event_name().unwrap(), "MessageDeleted");
}

#[tokio::test]
async fn handle_nonexistent_message_returns_not_found() {
    let es = Arc::new(MockEventStore::new());
    let registry = Arc::new(MockProviderRegistry::new(
        Arc::new(MockEmailProvider::new()),
    ));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::new());
    let handler = MessageCommandHandler::new(es, registry, read_repo, bus);

    let err = handler
        .handle_set_flags(
            MessageId::new(),
            MailboxPath::inbox(),
            vec![Flag::Seen],
            vec![],
        )
        .await
        .unwrap_err();

    assert!(matches!(err, ApplicationError::NotFound(..)));
}

#[tokio::test]
async fn a_delete_queued_behind_a_move_follows_it_to_trash() {
    let msg = make_test_message();
    let m = moving(&msg, MockEmailProvider::moving_to(900));
    let archive = m.archive.clone();

    m.handler
        .handle_move(
            msg.id,
            mailbox_of(&msg),
            archive.id,
            MailboxPath::inbox(),
            archive.path.clone(),
        )
        .await
        .unwrap();
    let result = m
        .handler
        .handle_delete(msg.id, mailbox_of(&msg), MailboxPath::inbox(), false)
        .await
        .unwrap();

    assert!(matches!(result, CommandResult::MessageDeleted));
    assert_eq!(
        *m.provider.moved.read().unwrap(),
        [RemoteId::from(42), RemoteId::from(900)]
    );
    assert_eq!(
        m.location(msg.id).await,
        (m.trash.id, Some(RemoteId::from(900)))
    );

    // Moving it where it already is changes nothing.
    m.handler
        .handle_move(
            msg.id,
            mailbox_of(&msg),
            m.trash.id,
            MailboxPath::inbox(),
            m.trash.path.clone(),
        )
        .await
        .unwrap();
    assert_eq!(m.provider.moved.read().unwrap().len(), 2);
}

#[tokio::test]
async fn the_same_delete_sent_twice_is_done_once() {
    let msg = make_test_message();
    let m = moving(&msg, MockEmailProvider::new());

    m.handler
        .handle_delete(msg.id, mailbox_of(&msg), MailboxPath::inbox(), true)
        .await
        .unwrap();
    let again = m
        .handler
        .handle_delete(msg.id, mailbox_of(&msg), MailboxPath::inbox(), true)
        .await;

    assert!(
        matches!(again, Ok(CommandResult::MessageDeleted)),
        "{again:?}"
    );
}

/// A double click on delete: the second one, asked while the message was
/// still in INBOX, finds it in Trash by its turn and leaves it there.
/// Asked again once it is in Trash, a delete does purge it.
#[tokio::test]
async fn a_double_delete_leaves_the_message_in_trash() {
    let msg = make_test_message();
    let m = moving(&msg, MockEmailProvider::moving_to(900));
    let turn = m.handler.turn(msg.account_id).await;
    let (msg_id, inbox) = (msg.id, mailbox_of(&msg));
    let deletes: Vec<_> = (0..2)
        .map(|_| {
            let handler = m.handler.clone();
            tokio::spawn(async move {
                handler
                    .handle_delete(msg_id, inbox, MailboxPath::inbox(), false)
                    .await
            })
        })
        .collect();
    drain_spawned().await;
    drop(turn);
    for delete in deletes {
        assert!(matches!(
            delete.await.unwrap(),
            Ok(CommandResult::MessageDeleted)
        ));
    }
    assert_eq!(
        m.location(msg.id).await,
        (m.trash.id, Some(RemoteId::from(900)))
    );

    m.handler
        .handle_delete(msg.id, m.trash.id, m.trash.path.clone(), false)
        .await
        .unwrap();
    assert!(matches!(
        m.handler.load_message(&msg.id).await,
        Err(ApplicationError::NotFound(..))
    ));
}

/// The same for bulk delete.
#[tokio::test]
async fn a_double_bulk_delete_leaves_the_message_in_trash() {
    let msg = make_test_message();
    let m = moving(&msg, MockEmailProvider::moving_to(900));
    let turn = m.handler.turn(msg.account_id).await;
    let deletes: Vec<_> = (0..2)
        .map(|_| {
            let handler = m.handler.clone();
            tokio::spawn(async move { handler.handle_delete_many(vec![msg.id], false).await })
        })
        .collect();
    drain_spawned().await;
    drop(turn);
    for delete in deletes {
        delete.await.unwrap().unwrap();
    }
    assert_eq!(
        m.location(msg.id).await,
        (m.trash.id, Some(RemoteId::from(900)))
    );
}
