//! Reconciling known messages with the server, and restoring the ones missing locally.

use super::*;

/// A row dropped here while the server kept the message, below the
/// checkpoint where ingest never looks again: reconcile brings it back,
/// quietly (no new-mail notification). A copy marked \Deleted on the
/// server stays gone.
#[tokio::test]
async fn a_message_still_on_the_server_comes_back_below_the_checkpoint() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let projector = synced_up_to(&archive, 5).await;
    let mut deleted = envelope(4, "<deleted@example.com>");
    deleted.flags = vec![Flag::Deleted];
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(vec![envelope(3, "<kept@example.com>"), deleted]);

    make_service(provider, Arc::new(MockEventStore::new()), projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(stored_uids(&projector), [3]);
    assert!(
        projector.projected_events().iter().all(|e| !matches!(
            e,
            DomainEvent::MessageReceived {
                restored: false,
                provider_key: None,
                ..
            }
        )),
        "a restored message is not announced as new"
    );
}

/// A draft discarded here is deleted on the server only afterwards; a
/// Drafts sync in between must not bring it back.
#[tokio::test]
async fn drafts_are_never_restored() {
    let account_id = AccountId::new();
    let (drafts, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Drafts".to_string()], '/').unwrap(),
        MailboxType::Drafts,
    );
    let projector = synced_up_to(&drafts, 5).await;
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![drafts])
        .with_envelopes(vec![envelope(3, "<draft@example.com>")]);

    make_service(provider, Arc::new(MockEventStore::new()), projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert!(stored_uids(&projector).is_empty());
}

/// One sync restores at most RESTORE_CAP, newest first, in UID-set
/// fetches of a batch each; the rest wait for the next sync.
#[tokio::test]
async fn restoring_is_capped_per_sync() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let total = RESTORE_CAP as u32 + 1;
    let projector = synced_up_to(&archive, total + 1).await;
    let provider = Arc::new(
        MockEmailProvider::new()
            .with_mailboxes(vec![archive])
            .with_envelopes(batch_of(total)),
    );
    let registry = Arc::new(MockProviderRegistry::new(provider.clone()));
    let service = SyncService::new(
        registry,
        Arc::new(MockEventStore::new()),
        projector.clone(),
        SyncConfig::default(),
    );

    service.start_sync(&account_id).await.unwrap();

    let uids = stored_uids(&projector);
    assert_eq!(
        (uids.len(), uids[0]),
        (RESTORE_CAP, 2),
        "the oldest one waits"
    );
    assert_eq!(
        *provider.envelope_fetches.lock().unwrap(),
        [ENVELOPE_BATCH; RESTORE_CAP / ENVELOPE_BATCH]
    );

    service.start_sync(&account_id).await.unwrap();
    assert_eq!(
        stored_uids(&projector).len(),
        total as usize,
        "and comes back next sync"
    );
}

/// A batch the server fails on doesn't hold back the others.
#[tokio::test]
async fn a_failing_restore_batch_is_skipped() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let projector = synced_up_to(&archive, 601).await;
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(batch_of(600));
    *provider.fail_envelopes_with.lock().unwrap() = Some(1);

    make_service(provider, Arc::new(MockEventStore::new()), projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    // Newest first: the batch with the oldest message is the one that fails.
    assert_eq!(stored_uids(&projector), (101..=600).collect::<Vec<_>>());
}

/// The listing is applied before the new mail is ingested, which can take
/// minutes: a snapshot that old would undo what the user did meanwhile.
#[tokio::test]
async fn the_snapshot_is_applied_before_new_mail_is_ingested() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let projector = synced_up_to(&archive, 5).await;
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(vec![
            envelope(3, "<restored@example.com>"),
            envelope(6, "<new@example.com>"),
        ]);

    make_service(provider, Arc::new(MockEventStore::new()), projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    let received: Vec<bool> = projector
        .projected_events()
        .iter()
        .filter_map(|e| match e {
            DomainEvent::MessageReceived { restored, .. } => Some(*restored),
            _ => None,
        })
        .collect();
    assert_eq!(received, [true, false], "restored first, then new");
}

fn scripted_changes(existing: Existing, reset: Option<Reset>) -> MailboxChanges {
    MailboxChanges {
        cursor: SyncCursor::new("scripted"),
        reset,
        added: vec![],
        keys: Default::default(),
        existing,
        counts: MailboxCounts {
            total: 1,
            unread: 0,
        },
    }
}

/// A provider that reports only what changed: its removals and flag changes
/// apply to the known messages, one it never mentions is left alone, and a
/// `Rescan` reset (ids still valid) forgets no id.
#[tokio::test]
async fn a_delta_applies_to_known_messages_and_a_rescan_forgets_nothing() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let reflagged = seed(
        &event_store,
        &projector,
        &archive,
        Some(1),
        "<a@example.com>",
    );
    let removed = seed(
        &event_store,
        &projector,
        &archive,
        Some(2),
        "<b@example.com>",
    );
    let untouched = seed(
        &event_store,
        &projector,
        &archive,
        Some(3),
        "<c@example.com>",
    );
    projector.mailboxes.lock().unwrap().push(archive.clone());

    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive.clone()])
        .with_envelopes(vec![envelope(7, "<back@example.com>")]);
    *provider.scripted.lock().unwrap() = Some(scripted_changes(
        Existing::Delta {
            // Seen is gone; 7 is a message no local row holds.
            changed: vec![(RemoteId::from(1), vec![]), (RemoteId::from(7), vec![])],
            removed: vec![RemoteId::from(2)],
        },
        Some(Reset::Rescan),
    ));
    let event_store_for_asserts = event_store.clone();
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    let events = projector.projected_events();
    assert!(events.iter().any(|e| matches!(
        e,
        DomainEvent::MessageFlagged { message_id, flags_removed, .. }
            if *message_id == reflagged && *flags_removed == [Flag::Seen]
    )));
    assert_eq!(row_uid(&projector, removed), None);
    assert_eq!(row_uid(&projector, untouched), Some(Some(3)));
    assert_eq!(
        row_uid(&projector, reflagged),
        Some(Some(1)),
        "a rescan forgets no id"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, DomainEvent::MessageReceived { restored: true, .. }))
            .count(),
        1,
        "the unknown message comes back quietly"
    );
    assert!(
        !event_store_for_asserts
            .appended_events()
            .iter()
            .any(|(_, e)| matches!(e, DomainEvent::MessageMailboxesChanged { added, .. } if added.iter().any(|m| m.remote_id.is_none()))),
        "no id was forgotten"
    );
}

/// Gmail lists one email under every label: the copy in the second
/// mailbox joins the message the first created, and costs no envelope.
#[tokio::test]
async fn a_copy_with_a_known_key_joins_the_message_without_a_fetch() {
    let account_id = AccountId::new();
    let mailboxes = vec![inbox_mailbox(account_id), archive_mailbox(account_id)];
    let mut copy = envelope(1, "<label@example.com>");
    copy.key = Some(mc_domain::MessageKey::new("1278455344230334865"));
    let provider = Arc::new(
        MockEmailProvider::new()
            .with_mailboxes(mailboxes)
            .with_envelopes(vec![copy]),
    );
    let projector = Arc::new(MockProjector::new());
    let service = make_shared_service(
        provider.clone(),
        Arc::new(MockEventStore::new()),
        projector.clone(),
    );

    service.start_sync(&account_id).await.unwrap();

    let stored = projector.stored.lock().unwrap();
    assert_eq!(stored.len(), 1, "one message for the two copies");
    assert_eq!(stored[0].memberships.len(), 2);
    assert!(stored[0].memberships.iter().all(|m| m.remote_id.is_some()));
    assert_eq!(*provider.envelope_fetches.lock().unwrap(), [1]);
}
