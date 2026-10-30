//! Body backfill and its download cap.

use super::*;

#[test]
fn a_throttled_fetch_stops_the_backfill_pass() {
    assert!(stops_the_pass(&DomainError::Throttled("bandwidth".into())));
    assert!(!stops_the_pass(&DomainError::MessageNotFound(
        "gone".into()
    )));
}

#[test]
fn backfill_runs_at_full_speed_until_ninety_percent_of_the_cap() {
    const MB: u64 = 1 << 20;
    let limit = Some(2000 * MB);

    // Batches of 50 back to back, capped account or not.
    assert_eq!(BACKFILL_BATCH, 50);
    assert_eq!(backfill_pause(0, limit), None);
    assert_eq!(backfill_pause(1799 * MB, limit), None);
    assert_eq!(backfill_pause(50_000 * MB, None), None);
    // At 90%: wait for the window to free, no trickle.
    assert_eq!(backfill_pause(1800 * MB, limit), Some(BACKFILL_CAP_WAIT));
    assert_eq!(backfill_pause(2500 * MB, limit), Some(BACKFILL_CAP_WAIT));
}

/// A credential rejection must stay typed all the way to `run_backfill`, which
/// is what lets it back off instead of retrying once a minute forever.
#[test]
fn auth_failure_survives_as_typed_error_not_stringified() {
    let err = ApplicationError::from(DomainError::AuthenticationFailed("nope".into()));
    assert!(matches!(
        err,
        ApplicationError::Domain(DomainError::AuthenticationFailed(_))
    ));
}

#[tokio::test]
async fn backfill_waits_while_the_account_needs_the_user() {
    use crate::services::auth_gate::{AuthGates, Credential};

    let service = Arc::new(make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    ));
    let mut rx = service.subscribe_progress();
    let refresh: crate::RefreshFn =
        Arc::new(|_, _| Box::pin(async { Err(DomainError::Provider("unused".into())) }));
    let keychain: Arc<dyn mc_domain::SecretStore> = Arc::new(NoKeychain);
    let gates = AuthGates::new(refresh, keychain, tokio::sync::mpsc::unbounded_channel().0);
    let account_id = AccountId::new();
    let gate = gates.add(
        account_id,
        "me@example.com",
        Credential::Password("old".into()),
    );
    let rejected = DomainError::AuthenticationFailed("NO".into());
    gate.login_failed(&LoginSecret::Password("old".into()), &rejected, false)
        .await;

    let shutdown = CancellationToken::new();
    let worker = tokio::spawn({
        let (service, gate, shutdown) = (service.clone(), gate.clone(), shutdown.clone());
        async move { service.run_backfill(account_id, None, gate, shutdown).await }
    });

    let paused = tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await;
    assert!(paused.is_err(), "a paused account's backfill must not run");

    gate.set_password("new".into());
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv()).await;
    shutdown.cancel();
    let _ = worker.await;
    assert!(event.expect("resumed after the password change").is_ok());
}

#[tokio::test]
async fn empty_backfill_queue_announces_itself() {
    // Regression: the worker used to go silent once the queue drained, so a
    // client kept rendering the last in-progress percentage forever.
    let service = Arc::new(make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()), // empty queue
    ));
    let mut rx = service.subscribe_progress();

    let shutdown = CancellationToken::new();
    let worker = {
        let service = service.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let gate = Arc::new(LoginSecret::Password(String::new()));
            service
                .run_backfill(AccountId::new(), None, gate, shutdown)
                .await
        })
    };

    let event = rx.recv().await.expect("idle worker published its state");
    shutdown.cancel();
    let _ = worker.await;

    assert_eq!(event.state, SyncState::FetchingMessageBody);
    assert_eq!(event.total, 0, "no backlog → no percentage to render");
    assert!(event.message.contains("All message bodies"));
}

/// Backfill's whole point is avoiding one IMAP reconnect per message.
/// `fetch_message_bodies_bulk` must fetch a batch of messages in the same
/// mailbox over a single `fetch_bodies_bulk` call, not one call per message.
#[tokio::test]
async fn fetch_message_bodies_bulk_uses_one_session_for_whole_batch() {
    let account_id = AccountId::new();
    let mailbox_path = MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap();
    let mailbox_id = MailboxId::new();

    let provider = MockEmailProvider::new();
    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());
    let service = make_service(provider, event_store.clone(), projector.clone());

    let entries: Vec<BackfillEntry> = (1..=3u32)
        .map(|uid| BackfillEntry {
            message_id: MessageId::new(),
            mailbox_id,
            remote_id: RemoteId::from(uid),
            rfc_message_id: None,
            size_bytes: 0,
            from_email: String::new(),
        })
        .collect();

    service
        .fetch_message_bodies_bulk(&account_id, &mailbox_path, &entries)
        .await
        .unwrap();

    // One `fetch_bodies_bulk` call for all 3 entries — not 3 separate calls.
    let registry_provider = &service.provider_registry.get_provider(&account_id).unwrap();
    assert_eq!(registry_provider.bulk_fetch_call_count(), 1);
    assert_eq!(
        registry_provider.bulk_fetch_calls.lock().unwrap()[0].len(),
        3
    );

    // Each entry got its own MessageBodyFetched event, keyed by message_id.
    let appended = event_store.appended_events();
    let body_fetched_ids: Vec<MessageId> = appended
        .iter()
        .filter_map(|(_, e)| match e {
            DomainEvent::MessageBodyFetched { message_id, .. } => Some(*message_id),
            _ => None,
        })
        .collect();
    assert_eq!(body_fetched_ids.len(), 3);
    for entry in &entries {
        assert!(body_fetched_ids.contains(&entry.message_id));
    }

    // Bodies went to the body store, once each, not into the events.
    let bodies = projector.stored_bodies();
    assert_eq!(bodies.len(), 3);
    for entry in &entries {
        let text = bodies.get(&entry.message_id).and_then(|(t, _)| t.clone());
        assert!(text.is_some_and(|t| t.starts_with("body-")), "{bodies:?}");
    }
    for (_, event) in &appended {
        let shown = format!("{event:?}");
        assert!(
            !shown.contains("body-"),
            "body leaked into the log: {shown}"
        );
    }
}

/// Gmail lists a message once per label plus All Mail: the backfill
/// downloads it once and every copy gets that body.
#[tokio::test]
async fn backfill_downloads_a_message_once_for_all_its_copies() {
    let account_id = AccountId::new();
    let (inbox, discovered) =
        Mailbox::discover(account_id, MailboxPath::inbox(), MailboxType::Inbox);
    let projector = Arc::new(MockProjector::new());
    let event_store = Arc::new(MockEventStore::new());
    let mut events = vec![discovered];
    let mut copy = |uid: u32, rfc: &str| {
        let (message, event) = Message::receive(
            account_id,
            inbox.id,
            RemoteId::from(uid),
            Some(rfc.into()),
            None,
            vec![],
            test_envelope(),
            vec![],
            10,
            false,
            chrono::Utc::now(),
        );
        events.push(event);
        message.id
    };
    let (first, second, other) = (copy(1, "<a@x>"), copy(2, "<a@x>"), copy(3, "<c@x>"));
    let (stored, late) = (copy(9, "<d@x>"), copy(4, "<d@x>"));
    for event in events {
        projector
            .project(&EventEnvelope::new(
                String::new(),
                AggregateType::Message,
                1,
                event,
            ))
            .await
            .unwrap();
    }
    projector
        .put_body(&stored, Some("stored before".into()), None, false)
        .await
        .unwrap();

    let service = Arc::new(make_service(
        MockEmailProvider::new(),
        event_store.clone(),
        projector.clone(),
    ));
    let mut progress = service.subscribe_progress();
    let shutdown = CancellationToken::new();
    let worker = tokio::spawn({
        let (service, shutdown) = (Arc::clone(&service), shutdown.clone());
        let gate: Arc<dyn LoginGate> = Arc::new(mc_domain::LoginSecret::Password("x".into()));
        async move { service.run_backfill(account_id, None, gate, shutdown).await }
    });
    while progress.recv().await.unwrap().message != "All message bodies downloaded" {}
    shutdown.cancel();
    worker.await.unwrap();

    let provider = service.provider_registry.get_provider(&account_id).unwrap();
    assert_eq!(
        *provider.bulk_fetch_calls.lock().unwrap(),
        [vec![1, 3]],
        "one copy of <a>, none of <d>"
    );
    let bodies = projector.stored_bodies();
    assert_eq!(bodies[&first], bodies[&second]);
    assert_eq!(bodies[&other].0.as_deref(), Some("body-3"));
    assert_eq!(bodies[&late].0.as_deref(), Some("stored before"));
    let fetched = event_store
        .appended_events()
        .iter()
        .filter(|(_, e)| matches!(e, DomainEvent::MessageBodyFetched { .. }))
        .count();
    assert_eq!(fetched, 4, "every copy recorded as fetched");
}

#[tokio::test]
async fn backfill_downloads_both_when_a_message_id_is_reused_with_another_size() {
    let account_id = AccountId::new();
    let (inbox, discovered) =
        Mailbox::discover(account_id, MailboxPath::inbox(), MailboxType::Inbox);
    let projector = Arc::new(MockProjector::new());
    let mut events = vec![discovered];
    for (uid, size) in [(1, 10), (2, 999)] {
        let (_, event) = Message::receive(
            account_id,
            inbox.id,
            RemoteId::from(uid),
            Some("<a@x>".into()),
            None,
            vec![],
            test_envelope(),
            vec![],
            size,
            false,
            chrono::Utc::now(),
        );
        events.push(event);
    }
    for event in events {
        projector
            .project(&EventEnvelope::new(
                String::new(),
                AggregateType::Message,
                1,
                event,
            ))
            .await
            .unwrap();
    }

    let service = make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        projector,
    );
    let batch = service
        .next_backfill_batch(&account_id, &HashSet::new())
        .await
        .unwrap();

    assert_eq!(batch.len(), 2);
}

/// Regression: the worker starts before any envelope exists and used to sleep
/// the whole idle poll (60 s) before noticing the first messages.
#[tokio::test]
async fn ingesting_wakes_an_idle_backfill_worker() {
    let account_id = AccountId::new();
    let (inbox, discovered) =
        Mailbox::discover(account_id, MailboxPath::inbox(), MailboxType::Inbox);
    let projector = Arc::new(MockProjector::new());
    projector
        .project(&EventEnvelope::new(
            String::new(),
            AggregateType::Message,
            1,
            discovered,
        ))
        .await
        .unwrap();
    let provider = Arc::new(MockEmailProvider::new());
    let service = Arc::new(make_shared_service(
        provider.clone(),
        Arc::new(MockEventStore::new()),
        projector,
    ));
    let mut progress = service.subscribe_progress();
    let shutdown = CancellationToken::new();
    let worker = tokio::spawn({
        let (service, shutdown) = (Arc::clone(&service), shutdown.clone());
        let gate: Arc<dyn LoginGate> = Arc::new(LoginSecret::Password("x".into()));
        async move { service.run_backfill(account_id, None, gate, shutdown).await }
    });
    // Idle: the queue was empty.
    while progress.recv().await.unwrap().message != "All message bodies downloaded" {}

    let registry_provider = service.provider_registry.get_provider(&account_id).unwrap();
    service
        .ingest(&registry_provider, &inbox, &[envelope(1, "<a@x>")], false)
        .await
        .unwrap();

    let woke = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while registry_provider.bulk_fetch_call_count() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    shutdown.cancel();
    worker.await.unwrap();
    assert!(woke.is_ok(), "the worker slept through the new message");
}
