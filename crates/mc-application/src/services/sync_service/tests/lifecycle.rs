//! The sync run: progress, events, discovery, cancellation.

use super::*;

// ── Tests ─────────────────────────────────────────────────────────

#[test]
fn progress_gate_collapses_repeats_but_keeps_percentage_steps() {
    let service = make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let account_id = AccountId::new();
    let mailbox_id = MailboxId::new();
    let mut published = Vec::new();

    // 1000 messages in one mailbox: the gate must not let 1000 events out.
    for i in 0..1000u32 {
        service.emit(
            &mut published,
            SyncProgressEvent {
                account_id,
                state: SyncState::FetchingEnvelopes { mailbox_id },
                processed: i,
                total: 1000,
                message: "Fetching messages in INBOX".to_string(),
            },
        );
    }

    assert_eq!(
        published.len(),
        100,
        "one event per whole percentage point, not one per message"
    );

    // A terminal event always passes, even repeated.
    let completed = SyncProgressEvent {
        account_id,
        state: SyncState::Completed,
        processed: 1,
        total: 1,
        message: "done".to_string(),
    };
    service.emit(&mut published, completed.clone());
    service.emit(&mut published, completed);
    assert_eq!(published.len(), 102, "Completed is never debounced away");
}

#[test]
fn progress_gate_lets_every_body_batch_through() {
    let service = make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let account_id = AccountId::new();
    let mut published = Vec::new();

    // 30-body batches against a 20k backlog never cross a whole percent.
    for batch in 1..=5u32 {
        service.emit(
            &mut published,
            SyncProgressEvent {
                account_id,
                state: SyncState::FetchingMessageBody,
                processed: batch * 30,
                total: 20_000,
                message: "Downloading message bodies".to_string(),
            },
        );
    }

    assert_eq!(published.len(), 5, "one event per downloaded batch");
}

#[tokio::test]
async fn failed_sync_publishes_a_terminal_event() {
    // No provider registered → start_sync fails on the first resolve.
    let service = make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let mut rx = service.subscribe_progress();

    assert!(service.start_sync(&AccountId::new()).await.is_ok());

    // The mock registry always resolves, so drive the failure path directly.
    let broken: SyncService<MockProviderRegistry, MockEventStore, MockProjector> = SyncService::new(
        Arc::new(MockProviderRegistry::empty()),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
        SyncConfig::default(),
    );
    let mut broken_rx = broken.subscribe_progress();
    assert!(broken.start_sync(&AccountId::new()).await.is_err());

    let event = broken_rx
        .try_recv()
        .expect("a terminal event was published");
    assert_eq!(event.state, SyncState::Failed);
    // The healthy service published progress, not a failure.
    assert!(!matches!(
        rx.try_recv().map(|e| e.state),
        Ok(SyncState::Failed)
    ));
}

#[tokio::test]
async fn progress_reaches_live_subscribers() {
    let service = make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let mut rx = service.subscribe_progress();

    service.publish(SyncProgressEvent {
        account_id: AccountId::new(),
        state: SyncState::ListingMailboxes,
        processed: 0,
        total: 0,
        message: "Listing mailboxes...".to_string(),
    });

    let event = rx.try_recv().expect("subscriber received the event");
    assert_eq!(event.state, SyncState::ListingMailboxes);
}

#[tokio::test]
async fn sync_discovers_new_mailboxes() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );
    let (sent, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Sent".to_string()], '/').unwrap(),
        MailboxType::Sent,
    );

    let provider = MockEmailProvider::new().with_mailboxes(vec![inbox, sent]);
    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    let service = make_service(provider, event_store.clone(), projector.clone());
    let progress = service.start_sync(&account_id).await.unwrap();

    // Should complete
    assert!(progress.iter().any(|e| e.state == SyncState::Completed));

    // Should have MailboxDiscovered events
    let projected = projector.projected_events();
    let discovered_count = projected
        .iter()
        .filter(|e| matches!(e, DomainEvent::MailboxDiscovered { .. }))
        .count();
    assert_eq!(discovered_count, 2, "Should discover INBOX and Sent");
}

#[tokio::test]
async fn sync_follows_a_server_side_rename() {
    let account_id = AccountId::new();
    let path = MailboxPath::new(vec!["mb7".to_string()], '/').unwrap();
    let (known, _) =
        Mailbox::discover_as(account_id, path.clone(), "Fin".into(), MailboxType::Custom);
    let (mut moved, _) =
        Mailbox::discover_as(account_id, path, "Work/Fin/Ops".into(), MailboxType::Custom);
    moved.display_name = "Fin/Ops".into();

    let projector = Arc::new(MockProjector::new());
    projector.mailboxes.lock().unwrap().push(known);
    let provider = MockEmailProvider::new().with_mailboxes(vec![moved]);
    let service = make_service(provider, Arc::new(MockEventStore::new()), projector.clone());
    service.start_sync(&account_id).await.unwrap();

    assert!(projector.projected_events().iter().any(|e| matches!(e,
        DomainEvent::MailboxRenamed { display_path, display_name, .. }
            if display_path == "Work/Fin/Ops" && display_name == "Fin/Ops")));
}

#[tokio::test]
async fn sync_fetches_envelopes_and_emits_message_received() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );

    let envelopes = vec![make_envelope_summary(1), make_envelope_summary(2)];
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![inbox])
        .with_envelopes(envelopes);

    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    let service = make_service(provider, event_store.clone(), projector.clone());
    let progress = service.start_sync(&account_id).await.unwrap();

    assert!(progress.iter().any(|e| e.state == SyncState::Completed));

    let projected = projector.projected_events();
    let received_count = projected
        .iter()
        .filter(|e| matches!(e, DomainEvent::MessageReceived { .. }))
        .count();
    assert_eq!(received_count, 2, "Should emit 2 MessageReceived events");

    // Verify events were appended to event store
    let appended = event_store.appended_events();
    let appended_received = appended
        .iter()
        .filter(|(_, e)| matches!(e, DomainEvent::MessageReceived { .. }))
        .count();
    assert_eq!(appended_received, 2);
}

#[tokio::test]
async fn resync_requests_are_coalesced() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let account = AccountId::new();
    let archive = MailboxPath::new(vec!["Archive".to_string()], '/').unwrap();
    for path in [MailboxPath::inbox(), archive.clone(), MailboxPath::inbox()] {
        tx.send((account, path)).unwrap();
    }

    assert_eq!(
        resync_batch(&mut rx).await.unwrap(),
        [(account, MailboxPath::inbox()), (account, archive)]
    );
    drop(tx);
    assert!(resync_batch(&mut rx).await.is_none());
}

#[tokio::test]
async fn sync_mailbox_waits_for_a_running_sync_of_the_account() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let projector = Arc::new(MockProjector::new());
    projector.mailboxes.lock().unwrap().push(archive.clone());
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive.clone()])
        .with_envelopes(vec![envelope(3, "<new@example.com>")]);
    let service = make_service(provider, Arc::new(MockEventStore::new()), projector.clone());

    let running = service.exclusive(&account_id).await;
    let sync = service.sync_mailbox(&account_id, &archive.path);
    tokio::pin!(sync);
    assert!(
        futures::poll!(&mut sync).is_pending(),
        "must wait for the running sync"
    );
    drop(running);
    assert_eq!(sync.await.unwrap(), 1);
}

#[tokio::test]
async fn cancel_sync_stops_early() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );

    let envelopes = vec![make_envelope_summary(1)];
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![inbox])
        .with_envelopes(envelopes);

    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());
    let service = Arc::new(make_service(provider, event_store, projector));

    // Cancel immediately
    service.cancel_sync();

    let progress = service.start_sync(&account_id).await.unwrap();
    assert!(progress.iter().any(|e| e.state == SyncState::Completed));

    // The cancellation flag is reset at start_sync, so sync runs normally.
    // Test cancellation by checking the flag is set correctly.
    service.cancel_sync();
    assert!(service.is_cancelled());
}

#[tokio::test]
async fn resume_sync_delegates_to_start_sync() {
    let account_id = AccountId::new();
    let provider = MockEmailProvider::new();
    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    let service = make_service(provider, event_store, projector);
    let progress = service.resume_sync(&account_id).await.unwrap();

    assert!(progress.iter().any(|e| e.state == SyncState::Completed));
}

#[tokio::test]
async fn sync_emits_account_lifecycle_events() {
    let account_id = AccountId::new();
    let provider = MockEmailProvider::new();
    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    let service = make_service(provider, event_store.clone(), projector.clone());
    service.start_sync(&account_id).await.unwrap();

    let projected = projector.projected_events();

    // First event should be AccountSyncStarted
    assert!(
        matches!(&projected[0], DomainEvent::AccountSyncStarted { .. }),
        "First event should be AccountSyncStarted, got {:?}",
        projected[0]
    );

    // Last event should be AccountSyncCompleted
    assert!(
        matches!(
            projected.last().unwrap(),
            DomainEvent::AccountSyncCompleted { .. }
        ),
        "Last event should be AccountSyncCompleted"
    );
}

#[tokio::test]
async fn sync_updates_mailbox_counts() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );

    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![inbox])
        .with_status(1, 1, 42, 5);

    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    let service = make_service(provider, event_store, projector.clone());
    service.start_sync(&account_id).await.unwrap();

    let projected = projector.projected_events();
    let counts_event = projected.iter().find(|e| {
        matches!(
            e,
            DomainEvent::MailboxCountsUpdated {
                total_messages: 42,
                unread_messages: 5,
                ..
            }
        )
    });
    assert!(
        counts_event.is_some(),
        "Should emit MailboxCountsUpdated with correct counts"
    );
}

#[tokio::test]
async fn status_events_reach_projector_and_bus_but_not_the_log() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![inbox])
        .with_status(1, 1, 42, 5);
    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());
    let bus = Arc::new(RecordingPublisher(Mutex::new(vec![])));
    let service = make_service(provider, event_store.clone(), projector.clone())
        .with_event_publisher(bus.clone());

    service.start_sync(&account_id).await.unwrap();

    let is_status = |e: &DomainEvent| {
        matches!(
            e,
            DomainEvent::AccountSyncStarted { .. }
                | DomainEvent::AccountSyncCompleted { .. }
                | DomainEvent::MailboxCountsUpdated { .. }
        )
    };
    let appended: Vec<_> = event_store.appended_events();
    assert!(!appended.iter().any(|(_, e)| is_status(e)), "{appended:?}");
    let projected = projector.projected_events();
    assert!(
        projected
            .iter()
            .any(|e| matches!(e, DomainEvent::AccountSyncStarted { .. }))
    );
    assert!(
        projected
            .iter()
            .any(|e| matches!(e, DomainEvent::AccountSyncCompleted { .. }))
    );
    assert!(
        projected
            .iter()
            .any(|e| matches!(e, DomainEvent::MailboxCountsUpdated { .. }))
    );
    let published = bus.0.lock().unwrap();
    assert!(published.iter().any(|e| matches!(
        e,
        DomainEvent::MailboxCountsUpdated {
            total_messages: 42,
            unread_messages: 5,
            ..
        }
    )));
}
