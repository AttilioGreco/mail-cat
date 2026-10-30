//! Which mailboxes are subscribed: the server's say until the user decides.

use super::*;

fn custom(account_id: AccountId, name: &str) -> Mailbox {
    Mailbox::discover(
        account_id,
        MailboxPath::new(vec![name.to_owned()], '/').unwrap(),
        MailboxType::Custom,
    )
    .0
}

fn unsubscribed(mut mailbox: Mailbox) -> Mailbox {
    mailbox.subscribed = false;
    mailbox
}

/// An account with an Inbox and a Work folder, both holding one server message.
struct World {
    account_id: AccountId,
    inbox: Mailbox,
    work: Mailbox,
    provider: Arc<MockEmailProvider>,
    event_store: Arc<MockEventStore>,
    projector: Arc<MockProjector>,
    service: SyncService<MockProviderRegistry, MockEventStore, MockProjector>,
}

impl World {
    fn new(work_subscribed: bool) -> Self {
        let account_id = AccountId::new();
        let inbox = inbox_mailbox(account_id);
        let mut work = custom(account_id, "Work");
        work.subscribed = work_subscribed;
        let provider = Arc::new(
            MockEmailProvider::new()
                .with_mailboxes(vec![inbox.clone(), work.clone()])
                .with_envelopes(vec![envelope(1, "<one@example.com>")]),
        );
        let event_store = Arc::new(MockEventStore::new());
        let projector = Arc::new(MockProjector::new());
        let service = make_shared_service(provider.clone(), event_store.clone(), projector.clone());
        Self {
            account_id,
            inbox,
            work,
            provider,
            event_store,
            projector,
            service,
        }
    }

    fn local(&self, path: &str) -> Mailbox {
        self.projector
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .find(|m| m.path.full_path() == path)
            .cloned()
            .unwrap()
    }

    fn work_id(&self) -> MailboxId {
        self.local("Work").id
    }
}

#[tokio::test]
async fn an_unsubscribed_mailbox_is_discovered_but_not_synced() {
    let w = World::new(false);

    w.service.start_sync(&w.account_id).await.unwrap();

    assert!(!w.local("Work").subscribed);
    assert!(w.local("INBOX").subscribed);
    assert_eq!(*w.provider.changes_paths.lock().unwrap(), ["INBOX"]);
    assert_eq!(received(&w.projector), 1, "only the Inbox was ingested");
}

#[tokio::test]
async fn the_server_decides_until_the_user_does() {
    let w = World::new(true);
    w.service.start_sync(&w.account_id).await.unwrap();
    assert_eq!(received(&w.projector), 2);

    // Unsubscribed elsewhere: follows, and the folder's mail goes.
    let mut provider = MockEmailProvider::new()
        .with_mailboxes(vec![w.inbox.clone(), unsubscribed(w.work.clone())])
        .with_envelopes(vec![envelope(1, "<one@example.com>")]);
    provider.status = w.provider.status.clone();
    let service = make_service(provider, w.event_store.clone(), w.projector.clone());
    service.start_sync(&w.account_id).await.unwrap();

    assert!(!w.local("Work").subscribed);
    assert!(!w.local("Work").subscription_explicit);
    assert!(
        w.projector
            .stored
            .lock()
            .unwrap()
            .iter()
            .all(|m| m.membership(w.work_id()).is_none())
    );
}

#[tokio::test]
async fn an_explicit_choice_survives_rediscovery() {
    let w = World::new(true);
    w.service.start_sync(&w.account_id).await.unwrap();
    w.service
        .set_mailbox_subscribed(&w.work_id(), false)
        .await
        .unwrap();

    // The server still lists Work as subscribed.
    w.service.start_sync(&w.account_id).await.unwrap();

    let work = w.local("Work");
    assert!(!work.subscribed && work.subscription_explicit);
}

#[tokio::test]
async fn unsubscribing_removes_only_the_messages_that_are_nowhere_else() {
    let w = World::new(true);
    w.service.start_sync(&w.account_id).await.unwrap();
    let (work, inbox) = (w.local("Work"), w.local("INBOX"));
    let only_here = seed(&w.event_store, &w.projector, &work, Some(7), "<a@x>");
    let shared = seed(&w.event_store, &w.projector, &work, Some(8), "<b@x>");
    w.projector
        .stored
        .lock()
        .unwrap()
        .iter_mut()
        .find(|m| m.id == shared)
        .unwrap()
        .memberships
        .push(Membership {
            mailbox_id: inbox.id,
            remote_id: Some(RemoteId::from(8)),
        });

    w.service
        .set_mailbox_subscribed(&work.id, false)
        .await
        .unwrap();
    assert!(
        w.projector
            .stored
            .lock()
            .unwrap()
            .iter()
            .any(|m| m.id == only_here),
        "the RPC returns before the messages are dropped"
    );
    w.service.run_subscription_jobs().await;

    let stored = w.projector.stored.lock().unwrap();
    assert!(stored.iter().all(|m| m.id != only_here));
    let survivor = stored.iter().find(|m| m.id == shared).unwrap();
    assert!(survivor.membership(work.id).is_none() && survivor.membership(inbox.id).is_some());
    assert!(
        stored.iter().any(|m| m.membership(inbox.id).is_some()),
        "Inbox mail untouched"
    );
}

#[tokio::test]
async fn the_user_choice_reaches_the_server_and_a_cursor_is_dropped() {
    let w = World::new(true);
    w.service.start_sync(&w.account_id).await.unwrap();
    assert!(cursor_of(&w.projector, &w.local("Work")).await.is_some());

    let warning = w
        .service
        .set_mailbox_subscribed(&w.work_id(), false)
        .await
        .unwrap();

    assert_eq!(warning, None);
    assert_eq!(
        *w.provider.subscriptions.lock().unwrap(),
        [("Work".to_owned(), false)]
    );
    assert!(cursor_of(&w.projector, &w.local("Work")).await.is_none());
}

#[tokio::test]
async fn subscribing_again_syncs_the_mailbox_afresh() {
    let w = World::new(true);
    w.service.start_sync(&w.account_id).await.unwrap();
    w.service
        .set_mailbox_subscribed(&w.work_id(), false)
        .await
        .unwrap();
    w.service.run_subscription_jobs().await;
    assert_eq!(received(&w.projector), 2);

    w.service
        .set_mailbox_subscribed(&w.work_id(), true)
        .await
        .unwrap();
    assert_eq!(received(&w.projector), 2, "the first sync is a job");
    w.service.run_subscription_jobs().await;

    assert!(w.local("Work").subscribed);
    assert_eq!(received(&w.projector), 3, "the message came back");
    assert!(cursor_of(&w.projector, &w.local("Work")).await.is_some());
}

#[tokio::test]
async fn a_server_that_refuses_still_leaves_the_choice_applied_locally() {
    let mut provider = MockEmailProvider::new();
    provider.subscription_error = Some(|| DomainError::Provider("NO not allowed".into()));
    let account_id = AccountId::new();
    let work = custom(account_id, "Work");
    provider.mailboxes = vec![inbox_mailbox(account_id), work];
    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());
    let service = make_service(provider, event_store, projector.clone());
    service.start_sync(&account_id).await.unwrap();
    let work_id = projector
        .mailboxes
        .lock()
        .unwrap()
        .iter()
        .find(|m| m.path.full_path() == "Work")
        .unwrap()
        .id;

    let warning = service
        .set_mailbox_subscribed(&work_id, false)
        .await
        .unwrap();

    assert!(warning.unwrap().contains("not allowed"));
    let work = projector.get_mailbox(&work_id).await.unwrap().unwrap();
    assert!(!work.subscribed && work.subscription_explicit);
}

#[tokio::test]
async fn the_inbox_cannot_be_unsubscribed() {
    let w = World::new(true);
    w.service.start_sync(&w.account_id).await.unwrap();

    let err = w
        .service
        .set_mailbox_subscribed(&w.local("INBOX").id, false)
        .await
        .unwrap_err();

    assert!(
        matches!(err, ApplicationError::PreconditionFailed(_)),
        "{err:?}"
    );
    assert!(w.local("INBOX").subscribed);
}

#[tokio::test]
async fn a_restart_finishes_a_drop_it_interrupted() {
    let w = World::new(true);
    w.service.start_sync(&w.account_id).await.unwrap();
    let work = w.local("Work");
    let leftover = seed(&w.event_store, &w.projector, &work, Some(7), "<a@x>");
    // Unsubscribed, but the job never ran.
    w.service
        .set_mailbox_subscribed(&work.id, false)
        .await
        .unwrap();

    w.service.start_sync(&w.account_id).await.unwrap();

    let stored = w.projector.stored.lock().unwrap();
    assert!(stored.iter().all(|m| m.id != leftover));
}

#[tokio::test]
async fn sent_drafts_and_archive_cannot_be_unsubscribed() {
    let account_id = AccountId::new();
    let mut mailboxes = vec![inbox_mailbox(account_id)];
    for (name, kind) in [
        ("Sent", MailboxType::Sent),
        ("Drafts", MailboxType::Drafts),
        ("All Mail", MailboxType::All),
    ] {
        mailboxes.push(
            Mailbox::discover(
                account_id,
                MailboxPath::new(vec![name.to_owned()], '/').unwrap(),
                kind,
            )
            .0,
        );
    }
    let provider = MockEmailProvider::new().with_mailboxes(mailboxes);
    let projector = Arc::new(MockProjector::new());
    let service = make_service(provider, Arc::new(MockEventStore::new()), projector.clone());
    service.start_sync(&account_id).await.unwrap();

    for name in ["Sent", "Drafts", "All Mail"] {
        let id = projector
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .find(|m| m.path.full_path() == name)
            .unwrap()
            .id;
        let err = service
            .set_mailbox_subscribed(&id, false)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, ApplicationError::PreconditionFailed(m) if m.contains(name)),
            "{err:?}"
        );
    }
}
