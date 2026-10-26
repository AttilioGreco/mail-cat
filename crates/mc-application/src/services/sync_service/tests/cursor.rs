//! Cursors: saved last, resumed after failures, and the errors that stop a pass.

use super::*;

/// A batch that fails leaves the cursor where it was: the next sync redoes
/// the mailbox from it, but skips what the batches before already stored.
#[tokio::test]
async fn a_failed_batch_leaves_the_cursor_and_the_next_sync_resumes() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let projector = Arc::new(MockProjector::new());
    projector.mailboxes.lock().unwrap().push(archive.clone());
    // Newest first: the oldest message is alone in the second batch.
    let provider = Arc::new(
        MockEmailProvider::new()
            .with_mailboxes(vec![archive.clone()])
            .with_envelopes(batch_of(501)),
    );
    *provider.fail_envelopes_with.lock().unwrap() = Some(1);
    let service = make_shared_service(
        provider.clone(),
        Arc::new(MockEventStore::new()),
        projector.clone(),
    );

    service.start_sync(&account_id).await.unwrap();
    assert_eq!(received(&projector), 500);
    assert_eq!(cursor_of(&projector, &archive).await, None, "not complete");

    *provider.fail_envelopes_with.lock().unwrap() = None;
    service.start_sync(&account_id).await.unwrap();
    assert_eq!(received(&projector), 501);
    assert_eq!(
        cursor_of(&projector, &archive).await,
        Some(cursor_at(1, 502))
    );
    assert_eq!(
        *provider.envelope_fetches.lock().unwrap(),
        [500, 1, 1],
        "the second sync fetched only the missing message"
    );
}

/// Ids invalidated, and the sync cut short before the cursor moved: the next
/// sync meets the same reset and ends where an uninterrupted one would.
#[tokio::test]
async fn an_interrupted_reset_is_redone_and_ends_the_same() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let kept = seed(
        &event_store,
        &projector,
        &archive,
        Some(3),
        "<m1@example.com>",
    );
    projector.mailboxes.lock().unwrap().push(archive.clone());
    projector
        .put_cursor(&account_id, &archive.id.to_string(), &cursor_at(1, 50))
        .await
        .unwrap();

    let provider = Arc::new(
        MockEmailProvider::new()
            .with_mailboxes(vec![archive.clone()])
            .with_envelopes(batch_of(2))
            .with_status(2, 3, 2, 0),
    );
    *provider.fail_envelopes_with.lock().unwrap() = Some(2);
    let service = make_shared_service(provider.clone(), event_store, projector.clone());

    service.start_sync(&account_id).await.unwrap();
    assert_eq!(
        row_uid(&projector, kept),
        Some(None),
        "its id is void and not yet matched"
    );
    assert_eq!(
        cursor_of(&projector, &archive).await,
        Some(cursor_at(1, 50))
    );

    *provider.fail_envelopes_with.lock().unwrap() = None;
    service.start_sync(&account_id).await.unwrap();
    assert_eq!(
        row_uid(&projector, kept),
        Some(Some(1)),
        "matched again by Message-ID"
    );
    assert_eq!(received(&projector), 1, "only the other message is new");
    assert_eq!(cursor_of(&projector, &archive).await, Some(cursor_at(2, 3)));
}

/// The server invalidated Archive's ids (1 -> 2). Id 9 now belongs to a
/// different message: the stale local id 9 used to hide it.
#[tokio::test]
async fn a_reset_reingests_the_whole_mailbox_and_matches_by_message_id() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let kept = seed(
        &event_store,
        &projector,
        &archive,
        Some(9),
        "<kept@example.com>",
    );
    let gone = seed(
        &event_store,
        &projector,
        &archive,
        Some(5),
        "<gone@example.com>",
    );
    projector.mailboxes.lock().unwrap().push(archive.clone());
    projector
        .put_cursor(&account_id, &archive.id.to_string(), &cursor_at(1, 50))
        .await
        .unwrap();

    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive.clone()])
        .with_envelopes(vec![
            envelope(3, "<kept@example.com>"),
            envelope(9, "<new@example.com>"),
        ])
        .with_status(2, 10, 2, 0);
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(received(&projector), 1, "the message now at id 9 is new");
    assert_eq!(
        row_uid(&projector, kept),
        Some(Some(3)),
        "re-attached by Message-ID"
    );
    assert_eq!(
        row_uid(&projector, gone),
        None,
        "not in the mailbox any more: deleted"
    );
    assert_eq!(
        cursor_of(&projector, &archive).await,
        Some(cursor_at(2, 10))
    );
}

#[tokio::test]
async fn envelope_whose_uid_is_already_held_is_not_ingested_again() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let held = archived(account_id, archive.id, Some(RemoteId::from(9)));

    let projector = Arc::new(MockProjector::new());
    projector.stored.lock().unwrap().push(held);
    projector.mailboxes.lock().unwrap().push(archive.clone());
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(vec![make_envelope_summary(9)]);
    let service = make_service(provider, Arc::new(MockEventStore::new()), projector.clone());
    service.start_sync(&account_id).await.unwrap();

    assert_eq!(received(&projector), 0);
}

#[tokio::test]
async fn incremental_sync_uses_the_cursor() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );

    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![inbox.clone()])
        .with_status(1, 10, 9, 2);
    let provider = Arc::new(provider);

    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    // Already synced up to next_id=10
    projector
        .put_cursor(&account_id, &inbox.id.to_string(), &cursor_at(1, 10))
        .await
        .unwrap();

    let service = make_shared_service(provider.clone(), event_store.clone(), projector.clone());
    service.start_sync(&account_id).await.unwrap();

    assert_eq!(
        received(&projector),
        0,
        "Should not fetch when the cursor is current"
    );
    assert!(provider.envelope_fetches.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sync_saves_the_cursor_after_the_mailbox() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );

    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![inbox])
        .with_status(42, 100, 99, 3);

    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    let service = make_service(provider, event_store, projector.clone());
    service.start_sync(&account_id).await.unwrap();

    let discovered = projector.mailboxes.lock().unwrap()[0].clone();
    assert_eq!(
        cursor_of(&projector, &discovered).await,
        Some(cursor_at(42, 100)),
        "Should save the cursor the provider reported"
    );
}

/// Login, connection and throttling failures would fail every other mailbox
/// alike: the pass stops at the first, instead of hammering the server.
#[tokio::test]
async fn a_throttled_mailbox_changes_aborts_the_whole_pass() {
    let account_id = AccountId::new();
    let (inbox, archive) = (inbox_mailbox(account_id), archive_mailbox(account_id));
    let mut provider = MockEmailProvider::new().with_mailboxes(vec![inbox, archive]);
    provider.changes_error = Some(|| DomainError::Throttled("too many connections".into()));
    let provider = Arc::new(provider);
    let service = make_shared_service(
        provider.clone(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let mut progress = service.subscribe_progress();

    let result = service.start_sync(&account_id).await;

    assert!(
        matches!(
            result,
            Err(ApplicationError::Domain(DomainError::Throttled(_)))
        ),
        "{result:?}"
    );
    assert_eq!(
        provider.changes_calls.load(Ordering::SeqCst),
        1,
        "the second mailbox was not tried"
    );
    let mut last = None;
    while let Ok(event) = progress.try_recv() {
        last = Some(event.state);
    }
    assert_eq!(last, Some(SyncState::Failed));
}

#[tokio::test]
async fn a_timed_out_envelope_fetch_aborts_the_pass_and_keeps_the_cursor() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let projector = Arc::new(MockProjector::new());
    projector.mailboxes.lock().unwrap().push(archive.clone());
    let mut provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive.clone()])
        .with_envelopes(batch_of(2));
    provider.envelope_error = || DomainError::ConnectionTimeout("envelopes".into());
    *provider.fail_envelopes_with.lock().unwrap() = Some(1);
    let service = make_service(provider, Arc::new(MockEventStore::new()), projector.clone());

    let result = service.start_sync(&account_id).await;

    assert!(
        matches!(
            result,
            Err(ApplicationError::Domain(DomainError::ConnectionTimeout(_)))
        ),
        "{result:?}"
    );
    assert_eq!(cursor_of(&projector, &archive).await, None);
}

/// Any other failure costs only its mailbox.
#[tokio::test]
async fn another_mailbox_changes_failure_skips_just_that_mailbox() {
    let account_id = AccountId::new();
    let (inbox, archive) = (inbox_mailbox(account_id), archive_mailbox(account_id));
    let mut provider = MockEmailProvider::new().with_mailboxes(vec![inbox, archive]);
    provider.changes_error = Some(|| DomainError::Provider("broken".into()));
    let provider = Arc::new(provider);
    let service = make_shared_service(
        provider.clone(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );

    let progress = service.start_sync(&account_id).await.unwrap();

    assert!(progress.iter().any(|e| e.state == SyncState::Completed));
    assert_eq!(provider.changes_calls.load(Ordering::SeqCst), 2);
}
