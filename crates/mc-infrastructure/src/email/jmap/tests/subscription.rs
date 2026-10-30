use super::*;

fn mailbox(id: &str) -> MailboxPath {
    MailboxPath::new(vec![id.into()], '/').unwrap()
}

#[tokio::test]
async fn is_subscribed_is_read_per_mailbox_and_the_inbox_is_always_on() {
    let base = serve(|_, _| {
        json!({"list": [
            {"id": "mb1", "name": "Inbox", "role": "inbox", "isSubscribed": false},
            {"id": "mb2", "name": "Work", "isSubscribed": false},
            {"id": "mb3", "name": "Play", "isSubscribed": true},
            {"id": "mb4", "name": "Legacy"},
        ]})
    })
    .await;

    let mailboxes = provider(&base).list_mailboxes().await.unwrap();

    let flags: Vec<bool> = mailboxes.iter().map(|m| m.subscribed).collect();
    assert_eq!(flags, [true, false, true, true]);
    assert!(mailboxes.iter().all(|m| !m.subscription_explicit));
}

#[tokio::test]
async fn subscribing_patches_is_subscribed_on_the_mailbox() {
    let (base, calls) = recording(|_, _| json!({})).await;
    let provider = provider(&base);

    provider
        .set_subscribed(&mailbox("mb2"), false)
        .await
        .unwrap();
    provider
        .set_subscribed(&mailbox("mb2"), true)
        .await
        .unwrap();

    let calls = calls.lock().unwrap();
    assert_eq!(calls[0].0, "Mailbox/set");
    assert_eq!(
        calls[0].1["update"],
        json!({"mb2": {"isSubscribed": false}})
    );
    assert_eq!(calls[1].1["update"], json!({"mb2": {"isSubscribed": true}}));
}

#[tokio::test]
async fn a_refused_subscription_is_an_error() {
    let (base, _) = recording(
        |_, _| json!({"notUpdated": {"mb2": {"type": "forbidden", "description": "read-only"}}}),
    )
    .await;

    let err = provider(&base)
        .set_subscribed(&mailbox("mb2"), false)
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("mb2: forbidden"), "{err}");
}
