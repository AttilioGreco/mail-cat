use super::*;

fn mailbox(id: &str) -> MailboxPath {
    MailboxPath::new(vec![id.into()], '/').unwrap()
}

#[tokio::test]
async fn labels_become_one_atomic_patch_per_email_in_chunks() {
    let (base, calls) = recording(|_, _| json!({})).await;
    provider(&base)
        .change_labels(
            &ids(&["a", "b", "c"]),
            &inbox(),
            &[mailbox("mb2")],
            &[mailbox("mb3")],
        )
        .await
        .unwrap();

    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2, "three ids at two per request");
    let patch = json!({"mailboxIds/mb2": true, "mailboxIds/mb3": null});
    assert_eq!(calls[0].1["update"], json!({"a": patch, "b": patch}));
    assert_eq!(calls[1].1["update"], json!({"c": patch}));
}

#[tokio::test]
async fn an_empty_change_sends_nothing() {
    let (base, calls) = recording(|_, _| json!({})).await;
    provider(&base)
        .change_labels(&ids(&["a"]), &inbox(), &[], &[])
        .await
        .unwrap();
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn removing_the_last_mailbox_is_a_clear_error() {
    let (base, _) = recording(|_, _| {
        json!({"notUpdated": {"a": {"type": "invalidProperties", "description": "mailboxIds"}}})
    })
    .await;
    let err = provider(&base)
        .change_labels(&ids(&["a"]), &inbox(), &[], &[inbox()])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("at least one mailbox"), "{err}");
}

#[tokio::test]
async fn vanished_emails_are_no_error() {
    let (base, _) = recording(|_, _| json!({"notUpdated": {"a": {"type": "notFound"}}})).await;
    provider(&base)
        .change_labels(&ids(&["a"]), &inbox(), &[mailbox("mb2")], &[])
        .await
        .unwrap();
}

fn mailbox_reply(name: &str, args: &Value) -> Value {
    match name {
        "Mailbox/get" => json!({"list": [
            {"id": "mb1", "name": "Inbox"},
            {"id": "mb2", "name": "Shopping"},
            {"id": "mb3", "name": "Ebay", "parentId": "mb2"},
            {"id": "mb4", "name": "Ebay"},
        ]}),
        _ => {
            let created: serde_json::Map<String, Value> = args["create"]
                .as_object()
                .unwrap()
                .keys()
                .map(|k| (k.clone(), json!({"id": format!("id-{k}")})))
                .collect();
            json!({ "created": created })
        }
    }
}

#[tokio::test]
async fn a_path_creates_the_missing_parents_and_nests_the_leaf() {
    let (base, calls) = recording(mailbox_reply).await;

    let id = provider(&base)
        .create_mailbox("Clients/Acme/2026")
        .await
        .unwrap();

    let calls = calls.lock().unwrap();
    assert_eq!(id, "id-m2");
    assert_eq!(
        calls[1].1["create"],
        json!({
            "m0": {"name": "Clients", "parentId": null, "isSubscribed": true},
            "m1": {"name": "Acme", "parentId": "#m0", "isSubscribed": true},
            "m2": {"name": "2026", "parentId": "#m1", "isSubscribed": true},
        })
    );
}

#[tokio::test]
async fn only_the_levels_below_an_existing_parent_are_created() {
    let (base, calls) = recording(mailbox_reply).await;

    let id = provider(&base)
        .create_mailbox("shopping/Amazon")
        .await
        .unwrap();

    assert_eq!(id, "id-m1");
    assert_eq!(
        calls.lock().unwrap()[1].1["create"],
        json!({"m1": {"name": "Amazon", "parentId": "mb2", "isSubscribed": true}}),
        "the parent is found whatever its case"
    );
}

#[tokio::test]
async fn an_existing_path_is_not_created_again_and_a_flat_twin_is_not_it() {
    let (base, calls) = recording(mailbox_reply).await;

    assert_eq!(
        provider(&base)
            .create_mailbox("Shopping/Ebay")
            .await
            .unwrap(),
        "mb3"
    );
    assert_eq!(calls.lock().unwrap().len(), 1, "only the lookup");
    assert_eq!(
        provider(&base).create_mailbox("Ebay").await.unwrap(),
        "mb4",
        "the top-level Ebay is another mailbox"
    );
}

#[tokio::test]
async fn a_refused_creation_says_why() {
    let (base, _) = recording(|name, _| match name {
        "Mailbox/get" => json!({"list": []}),
        _ => json!({"notCreated": {"m0": {"type": "overQuota"}}}),
    })
    .await;

    let err = provider(&base)
        .create_mailbox("A/B")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("overQuota"), "{err}");
}
