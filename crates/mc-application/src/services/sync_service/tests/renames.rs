//! Renames the server made that the local store never heard of.

use super::*;
use mc_domain::{PendingRename, PendingRenameStore};

fn custom(account_id: AccountId, name: &str) -> Mailbox {
    Mailbox::discover(
        account_id,
        MailboxPath::new(name.split('/').map(str::to_owned).collect(), '/').unwrap(),
        MailboxType::Custom,
    )
    .0
}

/// Local `Old` and `Old/Kid`, with a rename to `New` pending; the server
/// lists what `listed` says.
async fn after_crash(
    listed: &[&str],
) -> (
    Mailbox,
    Mailbox,
    Arc<MockProjector>,
    SyncService<MockProviderRegistry, MockEventStore, MockProjector>,
) {
    let account = AccountId::new();
    let (old, kid) = (custom(account, "Old"), custom(account, "Old/Kid"));
    let provider = MockEmailProvider::new().with_mailboxes(
        std::iter::once(inbox_mailbox(account))
            .chain(listed.iter().map(|name| custom(account, name)))
            .collect(),
    );
    let projector = Arc::new(MockProjector::new());
    *projector.mailboxes.lock().unwrap() = vec![old.clone(), kid.clone()];
    projector
        .put_pending_rename(&PendingRename {
            mailbox_id: old.id,
            account_id: account,
            old_display: "Old".into(),
            new_display: "New".into(),
        })
        .await
        .unwrap();
    let service = make_service(provider, Arc::new(MockEventStore::new()), projector.clone());
    (old, kid, projector, service)
}

fn renamed_to(projector: &MockProjector) -> Vec<(MailboxId, String)> {
    projector
        .projected_events()
        .iter()
        .filter_map(|e| match e {
            DomainEvent::MailboxRenamed {
                mailbox_id,
                display_path,
                ..
            } => Some((*mailbox_id, display_path.clone())),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_rename_the_server_made_is_recorded_before_mailboxes_are_matched() {
    let (old, kid, projector, service) = after_crash(&["New", "New/Kid"]).await;

    service.start_sync(&old.account_id).await.unwrap();

    assert_eq!(
        renamed_to(&projector),
        [(old.id, "New".into()), (kid.id, "New/Kid".into())]
    );
    assert!(
        projector
            .pending_renames(&old.account_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_rename_the_server_never_made_is_only_forgotten() {
    let (old, _, projector, service) = after_crash(&["Old", "Old/Kid"]).await;

    service.start_sync(&old.account_id).await.unwrap();

    assert!(renamed_to(&projector).is_empty());
    assert!(
        projector
            .pending_renames(&old.account_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_rename_that_cannot_be_told_is_dropped_without_events() {
    let (old, _, projector, service) = after_crash(&["Old", "New"]).await;

    service.start_sync(&old.account_id).await.unwrap();

    assert!(renamed_to(&projector).is_empty());
    assert!(
        projector
            .pending_renames(&old.account_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_sync_waits_for_whoever_holds_the_account() {
    let (old, _, projector, service) = after_crash(&["New", "New/Kid"]).await;
    let service = Arc::new(service);
    let held = service.exclusive(&old.account_id).await;

    let sync = tokio::spawn({
        let (service, account) = (Arc::clone(&service), old.account_id);
        async move { service.start_sync(&account).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(renamed_to(&projector).is_empty(), "sync ran under the lock");

    drop(held);
    sync.await.unwrap().unwrap();
    assert_eq!(renamed_to(&projector).len(), 2);
}
