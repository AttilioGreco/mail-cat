use super::*;

use mc_domain::DomainEvent::ContactConflictLost;

/// A synced contact `a` whose server copy `remote_raw` was then written by
/// somebody else, and which the user edited locally to "Local".
async fn conflicted(remote_raw: &str) -> (Env, ContactId) {
    let env = Env::with_book();
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.sync().await;
    let a = env.contact_at("/book/a.vcf").await;
    env.fake.write("/book/a.vcf", remote_raw);
    env.edit_local(a, "Local").await;
    (env, a)
}

#[tokio::test]
async fn the_newer_local_edit_wins_with_one_more_put() {
    let (env, a) = conflicted(&vcard("a", "Remote", Some(LONG_AGO))).await;
    env.sync().await;

    assert!(env.fake.raw("/book/a.vcf").unwrap().contains("Local"));
    assert_eq!(env.fake.calls_to("put"), 2, "the refused one and the retry");
    assert_eq!(env.lost().await, 0);
    assert!(env.pending().await.is_empty());
    assert!(env.contact(a).await.raw.unwrap().contains("Local"));
}

#[tokio::test]
async fn the_newer_remote_edit_wins_and_the_local_one_stays_in_the_history() {
    let (env, a) = conflicted(&vcard("a", "Remote", Some(FAR_AHEAD))).await;
    env.sync().await;

    assert!(env.fake.raw("/book/a.vcf").unwrap().contains("Remote"));
    assert_eq!(env.fake.calls_to("put"), 1);
    assert_eq!(env.lost().await, 1);
    assert!(env.pending().await.is_empty());
    assert!(env.contact(a).await.raw.unwrap().contains("Remote"));
    let history = env.history(a).await;
    assert!(
        history.iter().any(
            |e| matches!(e, ContactConflictLost { local_raw, .. } if local_raw.contains("FN:Local"))
        ),
        "{history:?}"
    );
}

#[tokio::test]
async fn a_remote_copy_without_a_timestamp_wins() {
    let (env, _) = conflicted(&vcard("a", "Remote", None)).await;
    env.sync().await;
    assert!(env.fake.raw("/book/a.vcf").unwrap().contains("Remote"));
    assert_eq!(env.lost().await, 1);
}

#[tokio::test]
async fn an_edit_of_a_resource_deleted_on_the_server_recreates_it() {
    let env = Env::with_book();
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.sync().await;
    let a = env.contact_at("/book/a.vcf").await;
    env.fake.remove("/book/a.vcf");
    env.edit_local(a, "Local").await;
    env.sync().await;

    assert!(env.fake.raw("/book/a.vcf").unwrap().contains("Local"));
    assert_eq!(env.lost().await, 0);
    assert!(env.pending().await.is_empty());
    assert_eq!(env.contact_count().await, 1);
}

#[tokio::test]
async fn a_delete_loses_to_a_newer_server_edit_and_the_contact_returns() {
    let env = Env::with_book();
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.sync().await;
    let a = env.contact_at("/book/a.vcf").await;
    env.fake
        .write("/book/a.vcf", &vcard("a", "Edited", Some(FAR_AHEAD)));
    env.handler.handle_remove_contact(a).await.unwrap();
    env.settled().await;
    env.sync().await;

    assert!(env.fake.raw("/book/a.vcf").unwrap().contains("Edited"));
    assert_eq!(env.lost().await, 1);
    let back = env.contact_at("/book/a.vcf").await;
    assert_ne!(
        back, a,
        "the old aggregate is removed; this is a new contact"
    );
    assert!(env.pending().await.is_empty());
}

#[tokio::test]
async fn a_vanished_collection_records_the_unpushed_edit_first() {
    let env = Env::with_book();
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.sync().await;
    let a = env.contact_at("/book/a.vcf").await;
    env.edit_local(a, "Local").await;
    env.fake.remove_book(BOOK);
    // The only book gone reads as a glitch until a second pass agrees.
    env.sync().await;
    env.sync().await;

    assert_eq!(env.lost().await, 1);
    let history = env.history(a).await;
    assert!(matches!(
        history.last(),
        Some(ContactConflictLost { local_raw, .. }) if local_raw.contains("FN:Local")
    ));
    assert!(
        env.repo
            .list_address_books(&env.account)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(env.contact_count().await, 0);
    assert_eq!(env.fake.calls_to("put"), 0);
    assert_eq!(env.cursor("carddav:/book/").await, None);
}

#[tokio::test]
async fn a_collection_turned_read_only_gives_the_edit_up_to_the_server_copy() {
    let env = Env::with_book();
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.sync().await;
    let a = env.contact_at("/book/a.vcf").await;
    env.edit_local(a, "Local").await;
    env.fake.set_book_read_only(BOOK);
    env.sync().await;

    assert_eq!(env.lost().await, 1);
    assert_eq!(env.fake.calls_to("put"), 0);
    assert!(env.pending().await.is_empty());
    assert!(env.contact(a).await.raw.unwrap().contains("FN:Ann"));
}

#[tokio::test]
async fn an_older_acknowledgement_does_not_clear_a_newer_edit() {
    let env = Env::with_book();
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.sync().await;
    let a = env.contact_at("/book/a.vcf").await;
    env.edit_local(a, "One").await;
    let in_flight = env.pending().await.remove(0);
    // A second edit is made while the first is on its way to the server.
    env.edit_local(a, "Two").await;

    let ctx = Ctx {
        account: env.account,
        backend: env.fake.clone(),
    };
    env.svc
        .acknowledge(
            &ctx,
            &env.target(),
            &in_flight,
            Href::new("/book/a.vcf"),
            Etag::new("\"new\""),
            None,
        )
        .await
        .unwrap();

    let pending = env.pending().await;
    assert_eq!(pending.len(), 1);
    assert!(matches!(&pending[0].change, PendingChange::Put { raw } if raw.contains("FN:Two")));
    assert!(pending[0].sequence > in_flight.sequence);
    assert_eq!(pending[0].binding.etag, Some(Etag::new("\"new\"")));
}

// The process dying after the server stored a create and before the
// acknowledgement was recorded.

#[tokio::test]
async fn a_refused_create_that_is_our_own_is_bound_not_duplicated() {
    let env = Env::with_book();
    let id = env.create_local("n", "New").await;
    env.fake
        .script_put(fake::Script::StoreThenFail(connection_lost));
    assert!(env.svc.sync_account(&env.account).await.is_err());
    assert_eq!(env.fake.hrefs(), ["/book/n.vcf"]);
    assert_eq!(env.pending().await.len(), 1);

    env.sync().await;

    assert_eq!(env.fake.hrefs(), ["/book/n.vcf"], "no duplicate");
    assert!(env.pending().await.is_empty());
    assert_eq!(env.contact_at("/book/n.vcf").await, id);
    assert_eq!(env.contact_count().await, 1);
    assert_eq!(env.lost().await, 0);
}

#[tokio::test]
async fn a_create_the_server_named_itself_is_bound_by_uid_in_the_pull() {
    let env = Env::with_book();
    env.fake.google();
    let id = env.create_local("n", "New").await;
    env.fake
        .script_put(fake::Script::StoreThenFail(connection_lost));
    assert!(env.svc.sync_account(&env.account).await.is_err());
    assert_eq!(env.fake.hrefs(), ["/book/server-1.vcf"]);

    // Creates by POST wait for the pull, which finds this one by UID first.
    env.sync().await;

    assert_eq!(env.fake.hrefs(), ["/book/server-1.vcf"], "no duplicate");
    assert_eq!(env.contact_at("/book/server-1.vcf").await, id);
    assert_eq!(env.contact_count().await, 1);

    // The server had rewritten the body, so the bound contact still has a
    // push to do, on top of the server's version.
    env.sync().await;
    assert!(env.pending().await.is_empty());
    assert_eq!(env.fake.hrefs().len(), 1);
    assert_eq!(env.contact_count().await, 1);
}

#[tokio::test]
async fn an_edit_made_while_a_conflict_is_settled_is_not_replaced() {
    let (env, a) = conflicted(&vcard("a", "Remote", Some(FAR_AHEAD))).await;
    let in_flight = env.pending().await.remove(0);
    // The user saves again while the server is being asked about "Local".
    env.edit_local(a, "Later").await;
    let ctx = Ctx {
        account: env.account,
        backend: env.fake.clone(),
    };
    let remote = mc_domain::ContactsProvider::fetch_contacts(
        env.fake.as_ref(),
        &env.target().collection,
        &[Href::new("/book/a.vcf")],
    )
    .await
    .unwrap()
    .remove(0);

    env.svc
        .remote_wins(&ctx, &env.target(), &in_flight, Some(&remote))
        .await
        .unwrap();

    let pending = env.pending().await;
    assert_eq!(pending.len(), 1, "the newer edit is still to push");
    assert!(matches!(&pending[0].change, PendingChange::Put { raw } if raw.contains("FN:Later")));
    assert!(env.contact(a).await.raw.unwrap().contains("Later"));
}
