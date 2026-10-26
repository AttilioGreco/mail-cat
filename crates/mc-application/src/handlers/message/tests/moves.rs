use super::*;

#[tokio::test]
async fn handle_move_produces_event() {
    let msg = make_test_message();
    let msg_id = msg.id;

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::new(
        MockEmailProvider::moving_to(900),
    )));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)]));
    let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

    let result = handler
        .handle_move(
            msg_id,
            mailbox_of(&msg),
            MailboxId::new(),
            MailboxPath::parse("Archive", '/').unwrap(),
        )
        .await
        .unwrap();

    assert!(matches!(result, CommandResult::MessageMoved));
    assert_eq!(bus.last_event_name().unwrap(), "MessageMailboxesChanged");
}

#[tokio::test]
async fn bulk_move_finds_messages_without_uid() {
    let (msg, archive, es) = uidless_in_archive();
    let provider = Arc::new(MockEmailProvider::new());
    *provider.found.write().unwrap() = vec![RemoteId::from(55)];
    let registry = Arc::new(MockProviderRegistry::new(provider.clone()));
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![archive.clone()]));
    let handler =
        MessageCommandHandler::new(es, registry, read_repo, Arc::new(MockEventBus::new()));

    let result = handler
        .handle_move_many(
            vec![(msg.id, archive.id)],
            MailboxId::new(),
            MailboxPath::parse("Trash", '/').unwrap(),
        )
        .await
        .unwrap();

    assert!(matches!(
        result,
        CommandResult::MessagesBulkChanged {
            succeeded: 1,
            failed: 0
        }
    ));
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert_eq!(*provider.moved.read().unwrap(), [RemoteId::from(55)]);
}

#[tokio::test]
async fn handle_move_records_the_uid_the_provider_reports() {
    let msg = make_test_message();
    let msg_id = msg.id;
    let archive = MailboxId::new();
    let provider = MockEmailProvider::new();
    *provider.moved_uid.write().unwrap() = Some(RemoteId::from(900));

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::new(provider)));
    let handler = MessageCommandHandler::new(
        es.clone(),
        registry,
        Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])),
        Arc::new(MockEventBus::new()),
    );

    handler
        .handle_move(
            msg_id,
            mailbox_of(&msg),
            archive,
            MailboxPath::parse("Archive", '/').unwrap(),
        )
        .await
        .unwrap();

    // The server moved it before anything changed here; the inbox UID
    // would address some other message in Archive.
    let (moved, _) = handler.load_message(&msg_id).await.unwrap();
    assert_eq!(mailbox_of(&moved), archive);
    assert_eq!(uid_of(&moved), Some(RemoteId::from(900)));
}

/// A move the server refuses changes nothing locally.
#[tokio::test]
async fn a_refused_move_changes_nothing() {
    let msg = make_test_message();
    let provider = MockEmailProvider::new();
    *provider.refuse.write().unwrap() = true;
    let es = Arc::new(MockEventStore::with_message(&msg));
    let handler = MessageCommandHandler::new(
        es,
        Arc::new(MockProviderRegistry::new(Arc::new(provider))),
        Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])),
        Arc::new(MockEventBus::new()),
    );

    let result = handler
        .handle_move(
            msg.id,
            mailbox_of(&msg),
            MailboxId::new(),
            MailboxPath::parse("Archive", '/').unwrap(),
        )
        .await;

    assert!(result.is_err());
    let (stored, _) = handler.load_message(&msg.id).await.unwrap();
    assert_eq!(
        (mailbox_of(&stored), uid_of(&stored)),
        (mailbox_of(&msg), uid_of(&msg))
    );
}

#[tokio::test]
async fn a_move_confirmed_without_evidence_records_no_destination() {
    let msg = with_rfc_id("<ghost@example.com>");
    let mut m = moving(&msg, MockEmailProvider::new());
    let archive = m.archive.clone();

    m.handler
        .handle_move(msg.id, mailbox_of(&msg), archive.id, archive.path.clone())
        .await
        .unwrap();

    assert!(matches!(
        m.handler.load_message(&msg.id).await,
        Err(ApplicationError::NotFound(..))
    ));
    assert_eq!(m.resync_requests(), [MailboxPath::inbox(), archive.path]);
}

/// A phone moved it to Archive; the Archive sync already stored that copy
/// as a row of its own while this one kept its stale INBOX UID. Archiving
/// it here: the server has nothing at that UID (OK, no COPYUID) and the
/// Archive copy is the other row's. This row goes rather than lingering
/// UID-less in INBOX, and nothing is duplicated.
#[tokio::test]
async fn archiving_a_message_another_client_already_moved_leaves_one_row() {
    let msg = with_rfc_id("<phone@example.com>");
    let provider = MockEmailProvider::new();
    *provider.found.write().unwrap() = vec![RemoteId::from(900)];
    let mut m = moving_with(
        &msg,
        provider,
        MockMessageReadRepo {
            held: vec![RemoteId::from(900)],
            ..Default::default()
        },
    );
    let archive = m.archive.clone();

    m.handler
        .handle_move(msg.id, mailbox_of(&msg), archive.id, archive.path.clone())
        .await
        .unwrap();

    assert!(
        matches!(
            m.handler.load_message(&msg.id).await,
            Err(ApplicationError::NotFound(..))
        ),
        "no UID-less row left"
    );
    let created = m.es.events.read().unwrap().len();
    assert_eq!(created, 1, "no new row");
    assert_eq!(m.resync_requests(), [MailboxPath::inbox(), archive.path]);
}

/// Gmail: archiving to All Mail, whose COPYUID names the UID the synced
/// All Mail row already holds. That row is the message there; this one
/// goes instead of becoming a second row of the same server message.
#[tokio::test]
async fn a_move_onto_a_uid_another_row_holds_leaves_that_row_alone() {
    let msg = make_test_message();
    let m = moving_with(
        &msg,
        MockEmailProvider::moving_to(900),
        MockMessageReadRepo {
            held: vec![RemoteId::from(900)],
            ..Default::default()
        },
    );
    let archive = m.archive.clone();

    m.handler
        .handle_move(msg.id, mailbox_of(&msg), archive.id, archive.path.clone())
        .await
        .unwrap();

    assert!(
        matches!(
            m.handler.load_message(&msg.id).await,
            Err(ApplicationError::NotFound(..))
        ),
        "no second row in Archive"
    );
    assert_eq!(m.es.events.read().unwrap().len(), 1, "and no new one");
}

#[tokio::test]
async fn a_timeout_after_a_move_resyncs_both_mailboxes() {
    let msg = make_test_message();
    let provider = MockEmailProvider::moving_to(900);
    *provider.time_out.write().unwrap() = true;
    let mut m = moving(&msg, provider);
    let archive = m.archive.clone();

    let result = m
        .handler
        .handle_move(msg.id, mailbox_of(&msg), archive.id, archive.path.clone())
        .await;

    assert!(result.is_err());
    assert_eq!(
        m.location(msg.id).await,
        (mailbox_of(&msg), Some(RemoteId::from(42)))
    );
    assert_eq!(m.resync_requests(), [MailboxPath::inbox(), archive.path]);
}

/// The server moved it; recording that loses to a sync appending first.
/// It is reloaded and recorded, not reported as a failure the server
/// never had.
#[tokio::test]
async fn recording_a_done_move_retries_past_a_concurrent_append() {
    let msg = make_test_message();
    let mut m = moving(&msg, MockEmailProvider::moving_to(900));
    let archive = m.archive.clone();
    *m.es.conflicts.lock().unwrap() = 1;

    m.handler
        .handle_move(msg.id, mailbox_of(&msg), archive.id, archive.path.clone())
        .await
        .unwrap();

    let (stored, _) = m.handler.load_message(&msg.id).await.unwrap();
    assert_eq!(
        (mailbox_of(&stored), uid_of(&stored)),
        (archive.id, Some(RemoteId::from(900)))
    );
    assert!(
        stored.flags.contains(&Flag::Flagged),
        "the other writer's change is kept"
    );
    assert!(m.resync_requests().is_empty());

    // Losing every time is still no error to the user: a resync repairs it.
    let msg = make_test_message();
    let mut m = moving(&msg, MockEmailProvider::moving_to(900));
    let archive = m.archive.clone();
    *m.es.conflicts.lock().unwrap() = RECORD_ATTEMPTS;
    m.handler
        .handle_move(msg.id, mailbox_of(&msg), archive.id, archive.path.clone())
        .await
        .unwrap();
    assert!(!m.resync_requests().is_empty());
}

/// The server applied the first chunk and refused the second: the first
/// is recorded, only the rest counts as failed.
#[tokio::test]
async fn a_refused_chunk_keeps_the_chunks_before_it() {
    let first = make_test_message();
    let provider = MockEmailProvider::moving_to(900);
    *provider.refuse_after.write().unwrap() = Some(SERVER_CHUNK);
    let Moving {
        handler,
        es,
        archive,
        ..
    } = moving(&first, provider);
    let mut ids = vec![first.id];
    for _ in 1..=SERVER_CHUNK {
        let other = Message {
            id: MessageId::new(),
            ..first.clone()
        };
        es.add(&other);
        ids.push(other.id);
    }

    let result = handler
        .handle_move_many(
            ids.iter().map(|id| (*id, mailbox_of(&first))).collect(),
            archive.id,
            archive.path.clone(),
        )
        .await
        .unwrap();

    assert!(
        matches!(
            result,
            CommandResult::MessagesBulkChanged {
                succeeded: 500,
                failed: 1
            }
        ),
        "{result:?}"
    );
    let mut in_archive = 0;
    for id in &ids {
        if mailbox_of(&handler.load_message(id).await.unwrap().0) == archive.id {
            in_archive += 1;
        }
    }
    assert_eq!(in_archive, SERVER_CHUNK);
}

#[tokio::test]
async fn a_flag_store_follows_a_move_queued_ahead_of_it() {
    let msg = make_test_message();
    let Moving {
        handler,
        provider,
        archive,
        ..
    } = moving(&msg, MockEmailProvider::moving_to(900));
    let turn = handler.turn(msg.account_id).await;

    let mover = handler.clone();
    let archive_path = archive.path.clone();
    let (msg_id, inbox) = (msg.id, mailbox_of(&msg));
    let moved = tokio::spawn(async move {
        mover
            .handle_move(msg_id, inbox, archive.id, archive_path)
            .await
    });
    drain_spawned().await;
    handler
        .handle_set_flags(msg.id, vec![Flag::Seen], vec![])
        .await
        .unwrap();
    drop(turn);
    moved.await.unwrap().unwrap();
    drain_spawned().await;

    assert_eq!(
        *provider.flagged_bulk.read().unwrap(),
        [RemoteId::from(900)]
    );
    assert_eq!(*provider.flagged_in.read().unwrap(), [archive.path]);
}

#[tokio::test]
async fn a_move_from_a_mailbox_the_message_is_not_in_fails() {
    let msg = make_test_message();
    let m = moving(&msg, MockEmailProvider::moving_to(900));

    let err = m
        .handler
        .handle_move(msg.id, m.trash.id, m.archive.id, m.archive.path.clone())
        .await
        .unwrap_err();

    assert!(matches!(err, ApplicationError::NotFound(..)), "{err:?}");
    assert!(m.provider.moved.read().unwrap().is_empty());
    assert_eq!(
        m.location(msg.id).await,
        (mailbox_of(&msg), Some(RemoteId::from(42)))
    );
}

/// A bulk selection names each message's own mailbox: one batch acts from
/// INBOX and Archive at once, and a pair naming a mailbox the message is not
/// in is counted as failed rather than redirected.
#[tokio::test]
async fn a_bulk_change_acts_from_the_mailbox_each_message_is_named_with() {
    let in_inbox = make_test_message();
    let m = moving(&in_inbox, MockEmailProvider::moving_to(900));
    let in_archive = Message {
        id: MessageId::new(),
        memberships: vec![Membership {
            mailbox_id: m.archive.id,
            remote_id: Some(RemoteId::from(77)),
        }],
        ..in_inbox.clone()
    };
    m.es.add(&in_archive);
    let misnamed = Message {
        id: MessageId::new(),
        ..in_inbox.clone()
    };
    m.es.add(&misnamed);

    let result = m
        .handler
        .handle_delete_many(
            vec![
                (in_inbox.id, mailbox_of(&in_inbox)),
                (in_archive.id, m.archive.id),
                (misnamed.id, m.archive.id),
            ],
            false,
        )
        .await
        .unwrap();

    assert!(
        matches!(
            result,
            CommandResult::MessagesBulkChanged {
                succeeded: 2,
                failed: 1
            }
        ),
        "{result:?}"
    );
    let mut moved = m.provider.moved.read().unwrap().clone();
    moved.sort_by_key(|id| id.to_string());
    assert_eq!(moved, [RemoteId::from(42), RemoteId::from(77)]);
    assert_eq!(m.location(in_inbox.id).await.0, m.trash.id);
    assert_eq!(m.location(in_archive.id).await.0, m.trash.id);
    assert_eq!(
        m.location(misnamed.id).await.0,
        mailbox_of(&misnamed),
        "untouched"
    );
}
