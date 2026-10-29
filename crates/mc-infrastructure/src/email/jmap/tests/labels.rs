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
