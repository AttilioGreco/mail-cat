use super::*;

#[tokio::test]
async fn pass_pulls_pushes_first_and_repeats_as_nothing() {
    let env = Env::with_book();
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.fake.write("/book/b.vcf", &vcard("b", "Bob", None));
    env.sync().await;
    assert_eq!(env.contact_count().await, 2);
    assert!(env.cursor("carddav:/book/").await.is_some());
    let a = env.contact_at("/book/a.vcf").await;
    assert_eq!(
        env.contact(a).await.account_scope,
        Some(env.account),
        "remote contacts are scoped to their account"
    );

    // A local edit and a server change, then one pass.
    env.edit_local(a, "Annie").await;
    env.fake.write("/book/c.vcf", &vcard("c", "Cy", None));
    env.sync().await;

    let calls = env.fake.calls();
    let put = calls.iter().position(|c| c == "put /book/a.vcf").unwrap();
    let pull = calls
        .iter()
        .rposition(|c| c.starts_with("changes"))
        .unwrap();
    assert!(put < pull, "push before pull: {calls:?}");
    assert!(env.fake.raw("/book/a.vcf").unwrap().contains("Annie"));
    env.contact_at("/book/c.vcf").await;
    assert!(env.pending().await.is_empty());

    // The pull saw our own write come back with the etag we already hold:
    // created, edited, acknowledged, and nothing more.
    let history = env.history(a).await;
    assert_eq!(history.len(), 3, "{history:?}");

    let fetches = env.fake.calls_to("fetch");
    env.sync().await;
    assert_eq!(env.fake.calls_to("fetch"), fetches);
    assert_eq!(env.history(a).await.len(), 3);
}

#[tokio::test]
async fn deletes_travel_both_ways() {
    let env = Env::with_book();
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.fake.write("/book/b.vcf", &vcard("b", "Bob", None));
    env.sync().await;
    let a = env.contact_at("/book/a.vcf").await;

    env.fake.remove("/book/b.vcf");
    env.handler.handle_remove_contact(a).await.unwrap();
    env.settled().await;
    env.sync().await;

    assert!(env.fake.hrefs().is_empty(), "{:?}", env.fake.hrefs());
    assert_eq!(env.contact_count().await, 0);
}

#[tokio::test]
async fn an_expired_token_relists_and_drops_what_the_server_lost() {
    let env = Env::with_book();
    for (href, uid) in [("a", "a"), ("b", "b"), ("c", "c")] {
        env.fake
            .write(&format!("/book/{href}.vcf"), &vcard(uid, uid, None));
    }
    env.sync().await;
    let c = env.contact_at("/book/c.vcf").await;

    env.fake.remove("/book/b.vcf");
    env.fake.remove("/book/c.vcf");
    env.fake.expire_token();
    // c has an edit the server never saw, and its push is refused (the
    // first push of the pass: pending edits go in the order they were made).
    env.edit_local(c, "Cee").await;
    env.create_local("fresh", "Fresh").await;
    env.fake.script_put(fake::Script::Fail(server_error));
    env.sync().await;

    let etags = env
        .repo
        .contact_etags(&env.account, &CollectionId::new(BOOK))
        .await
        .unwrap();
    let mut kept: Vec<_> = etags.keys().map(|h| h.to_string()).collect();
    kept.sort();
    // b is gone: absent on the server and not dirty. c stays: dirty. The
    // create made locally is not a "local-only leftover".
    assert_eq!(kept, ["/book/a.vcf", "/book/c.vcf", "/book/fresh.vcf"]);
    assert_eq!(env.pending().await.len(), 1);
}

#[tokio::test]
async fn a_failing_server_stops_the_pass_and_leaves_the_cursors_alone() {
    let env = Env::with_book();
    env.fake.add_book(book("/other/"));
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.fake.write("/other/o.vcf", &vcard("o", "Oz", None));
    env.sync().await;
    let (first, second) = (
        env.cursor("carddav:/book/").await,
        env.cursor("carddav:/other/").await,
    );
    assert!(first.is_some() && second.is_some());

    env.fake.write("/book/n.vcf", &vcard("n", "New", None));
    env.fake.write("/other/p.vcf", &vcard("p", "Pat", None));
    env.fake.fail_changes(BOOK, connection_lost);
    let stopped = env.svc.sync_account(&env.account).await;
    assert!(matches!(
        stopped,
        Err(ApplicationError::Domain(DomainError::ConnectionFailed(_)))
    ));
    assert_eq!(env.cursor("carddav:/book/").await, first);
    assert_eq!(env.cursor("carddav:/other/").await, second);
    assert!(
        env.repo
            .contact_at(&env.account, &Href::new("/other/p.vcf"))
            .await
            .unwrap()
            .is_none(),
        "the rest of the pass was not run"
    );
}

#[tokio::test]
async fn a_refused_push_is_recorded_and_retried_once_per_pass() {
    let env = Env::with_book();
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.fake.write("/book/b.vcf", &vcard("b", "Bob", None));
    env.sync().await;
    let (a, b) = (
        env.contact_at("/book/a.vcf").await,
        env.contact_at("/book/b.vcf").await,
    );
    env.edit_local(a, "Annie").await;
    env.edit_local(b, "Bobby").await;
    env.fake.script_put(fake::Script::Fail(server_error));

    env.sync().await;
    assert_eq!(env.fake.calls_to("put"), 2, "the other resource still went");
    let pending = env.pending().await;
    assert_eq!(pending.len(), 1);
    assert!(pending[0].failure.is_some());

    env.sync().await;
    assert_eq!(env.fake.calls_to("put"), 3);
    assert!(env.pending().await.is_empty());
}

#[tokio::test]
async fn a_server_that_rewrites_the_body_replaces_the_local_copy() {
    let env = Env::with_book();
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.sync().await;
    env.fake.google();
    let a = env.contact_at("/book/a.vcf").await;
    env.edit_local(a, "Annie").await;
    env.sync().await;

    let raw = env.contact(a).await.raw.unwrap();
    assert!(raw.contains("X-SERVER:1") && raw.contains("Annie"), "{raw}");
    assert!(env.pending().await.is_empty());
    assert_eq!(env.fake.raw("/book/a.vcf").unwrap(), raw);
}

#[tokio::test]
async fn the_fast_path_and_the_pass_never_push_one_resource_twice() {
    let env = Env::with_book();
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.sync().await;
    let a = env.contact_at("/book/a.vcf").await;
    env.edit_local(a, "Annie").await;

    let (target, only) = (env.target(), a.to_string());
    let (fast, pass) = tokio::join!(
        env.svc.push_now(&env.account, &target, &only),
        env.svc.sync_account(&env.account),
    );
    pass.unwrap();
    fast.unwrap();
    assert_eq!(env.fake.calls_to("put /book/a.vcf"), 1);
}

#[tokio::test]
async fn an_empty_relist_is_believed_only_when_the_next_pass_agrees() {
    let env = Env::with_book();
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.sync().await;

    env.fake.remove("/book/a.vcf");
    env.fake.expire_token();
    env.sync().await;
    assert_eq!(
        env.contact_count().await,
        1,
        "one empty answer may be a glitch"
    );

    env.fake.expire_token();
    env.sync().await;
    assert_eq!(env.contact_count().await, 0);
}

#[tokio::test]
async fn an_empty_collection_list_is_believed_only_when_the_next_pass_agrees() {
    let env = Env::with_book();
    env.fake.write("/book/a.vcf", &vcard("a", "Ann", None));
    env.sync().await;

    env.fake.remove_book(BOOK);
    env.sync().await;
    assert_eq!(env.contact_count().await, 1);

    env.sync().await;
    assert_eq!(env.contact_count().await, 0);
}
