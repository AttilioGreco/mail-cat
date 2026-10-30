//! Mailboxes that are views over another (Gmail labels over All Mail).

use mc_domain::MailboxModel;

use super::*;

fn path(name: &str) -> MailboxPath {
    MailboxPath::new(vec![name.to_owned()], '/').unwrap()
}

fn mailbox(account_id: AccountId, name: &str, kind: MailboxType) -> Mailbox {
    Mailbox::discover(account_id, path(name), kind).0
}

fn labelled(added: &[u32], labels: &[(u32, &[&str])], existing: Existing) -> MailboxChanges {
    MailboxChanges {
        cursor: SyncCursor::new("scripted"),
        reset: None,
        added: added.iter().copied().map(RemoteId::from).collect(),
        keys: Default::default(),
        labels: labels
            .iter()
            .map(|(uid, names)| {
                (
                    RemoteId::from(*uid),
                    names.iter().map(|n| path(n)).collect(),
                )
            })
            .collect(),
        existing,
        counts: MailboxCounts {
            total: 1,
            unread: 0,
        },
    }
}

fn snapshot(uids: &[u32]) -> Existing {
    Existing::Snapshot(
        uids.iter()
            .map(|uid| (RemoteId::from(*uid), vec![]))
            .collect(),
    )
}

struct Gmail {
    account_id: AccountId,
    provider: Arc<MockEmailProvider>,
    projector: Arc<MockProjector>,
    event_store: Arc<MockEventStore>,
}

impl Gmail {
    fn new() -> Self {
        let account_id = AccountId::new();
        let all_mail = mailbox(account_id, "All Mail", MailboxType::All);
        let work = mailbox(account_id, "Work", MailboxType::Custom);
        let mut provider = MockEmailProvider::new()
            .with_mailboxes(vec![all_mail.clone(), work])
            .with_envelopes(vec![envelope(1, "<one@example.com>")]);
        provider.views.insert("Work".into(), all_mail.path.clone());
        provider.preferred_model = Some(MailboxModel::Labels);
        Self {
            account_id,
            provider: Arc::new(provider),
            projector: Arc::new(MockProjector::new()),
            event_store: Arc::new(MockEventStore::new()),
        }
    }

    fn service(&self) -> SyncService<MockProviderRegistry, MockEventStore, MockProjector> {
        make_shared_service(
            self.provider.clone(),
            self.event_store.clone(),
            self.projector.clone(),
        )
    }

    fn script(&self, changes: MailboxChanges) {
        *self.provider.scripted.lock().unwrap() = Some(changes);
    }

    /// Names of the mailboxes the one message is in.
    fn memberships(&self) -> Vec<String> {
        let stored = self.projector.stored.lock().unwrap();
        assert!(stored.len() <= 1, "one message");
        let mailboxes = self.projector.mailboxes.lock().unwrap();
        stored
            .first()
            .map(|m| {
                m.memberships
                    .iter()
                    .map(|m| {
                        let held = mailboxes.iter().find(|b| b.id == m.mailbox_id);
                        held.unwrap().path.full_path()
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[tokio::test]
async fn a_view_is_not_synced_and_the_labels_of_a_new_message_become_memberships() {
    let gmail = Gmail::new();
    gmail.script(labelled(&[1], &[(1, &["Work"])], snapshot(&[1])));

    gmail.service().start_sync(&gmail.account_id).await.unwrap();

    assert_eq!(*gmail.provider.changes_paths.lock().unwrap(), ["All Mail"]);
    assert_eq!(gmail.memberships(), ["All Mail", "Work"]);
    let stored = gmail.projector.stored.lock().unwrap();
    assert!(
        stored[0]
            .memberships
            .iter()
            .all(|m| m.remote_id == Some(RemoteId::from(1))),
        "a view holds the id All Mail gave the message"
    );
    let counted = gmail.projector.projected_events().into_iter().any(|e| {
        matches!(e, DomainEvent::MailboxCountsUpdated { mailbox_id, total_messages: 1, .. }
            if gmail.projector.mailboxes.lock().unwrap().iter().any(|m| m.id == mailbox_id && m.path.full_path() == "Work"))
    });
    assert!(counted, "the view's total comes from the read model");
}

#[tokio::test]
async fn a_relabelled_message_changes_views_and_one_that_left_all_mail_leaves_them() {
    let gmail = Gmail::new();
    let service = gmail.service();
    gmail.script(labelled(&[1], &[(1, &["Work"])], snapshot(&[1])));
    service.start_sync(&gmail.account_id).await.unwrap();

    // Work was removed on the server: only that message's labels are sent.
    gmail.script(labelled(
        &[],
        &[(1, &[])],
        Existing::Listed {
            present: vec![RemoteId::from(1)],
            changed: vec![(RemoteId::from(1), vec![])],
        },
    ));
    service.start_sync(&gmail.account_id).await.unwrap();
    assert_eq!(gmail.memberships(), ["All Mail"]);

    // Labelled again, then expunged: the views go with All Mail's membership.
    gmail.script(labelled(&[], &[(1, &["Work"])], snapshot(&[1])));
    service.start_sync(&gmail.account_id).await.unwrap();
    assert_eq!(gmail.memberships().len(), 2);
    gmail.script(labelled(
        &[],
        &[],
        Existing::Listed {
            present: vec![],
            changed: vec![],
        },
    ));
    service.start_sync(&gmail.account_id).await.unwrap();
    assert!(gmail.memberships().is_empty(), "the message is gone");
}

#[tokio::test]
async fn a_message_not_mentioned_keeps_its_views() {
    let gmail = Gmail::new();
    let service = gmail.service();
    gmail.script(labelled(&[1], &[(1, &["Work"])], snapshot(&[1])));
    service.start_sync(&gmail.account_id).await.unwrap();

    gmail.script(labelled(
        &[],
        &[],
        Existing::Listed {
            present: vec![RemoteId::from(1)],
            changed: vec![],
        },
    ));
    service.start_sync(&gmail.account_id).await.unwrap();
    assert_eq!(gmail.memberships(), ["All Mail", "Work"]);
}

#[tokio::test]
async fn syncing_a_view_syncs_its_backing_mailbox() {
    let gmail = Gmail::new();
    gmail.script(labelled(&[1], &[(1, &["Work"])], snapshot(&[1])));
    let service = gmail.service();
    service.start_sync(&gmail.account_id).await.unwrap();
    gmail.provider.changes_paths.lock().unwrap().clear();

    service
        .sync_mailbox(&gmail.account_id, &path("Work"))
        .await
        .unwrap();

    assert_eq!(*gmail.provider.changes_paths.lock().unwrap(), ["All Mail"]);
}

fn model_changes(event_store: &MockEventStore) -> Vec<MailboxModel> {
    event_store
        .appended_events()
        .into_iter()
        .filter_map(|(_, e)| match e {
            DomainEvent::AccountMailboxModelChanged { model, .. } => Some(model),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn the_preferred_model_is_recorded_once_and_never_over_a_choice() {
    let gmail = Gmail::new();
    let service = gmail.service();
    service.start_sync(&gmail.account_id).await.unwrap();
    service.start_sync(&gmail.account_id).await.unwrap();
    assert_eq!(model_changes(&gmail.event_store), [MailboxModel::Labels]);

    let chosen = Gmail::new();
    chosen.event_store.events.lock().unwrap().push((
        chosen.account_id.to_string(),
        DomainEvent::AccountMailboxModelChanged {
            account_id: chosen.account_id,
            model: MailboxModel::Folders,
        },
    ));
    chosen
        .service()
        .start_sync(&chosen.account_id)
        .await
        .unwrap();
    assert_eq!(model_changes(&chosen.event_store), [MailboxModel::Folders]);
}
