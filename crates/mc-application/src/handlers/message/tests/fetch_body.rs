use super::*;

#[tokio::test]
async fn handle_fetch_body_stores_body_through_body_store() {
    let msg = make_test_message();
    let msg_id = msg.id;
    let provider = MockEmailProvider::new();
    *provider.detail_response.write().unwrap() = Some(Message {
        body_text: Some("fetched text".into()),
        body_html: Some("<p>fetched</p>".into()),
        ..msg.clone()
    });

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::new(provider)));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)]));
    let handler = MessageCommandHandler::new(es, registry, read_repo.clone(), bus.clone());

    handler
        .handle_fetch_body(msg_id, MailboxPath::inbox())
        .await
        .unwrap();

    assert_eq!(
        read_repo.stored_bodies.lock().unwrap().as_slice(),
        &[(
            msg_id,
            Some("fetched text".to_string()),
            Some("<p>fetched</p>".to_string())
        )]
    );
    assert_eq!(bus.last_event_name().unwrap(), "MessageBodyFetched");
}

/// Gmail: the same mail under another label already has its whole text.
#[tokio::test]
async fn handle_fetch_body_takes_a_full_body_from_another_copy() {
    let msg = Message {
        message_rfc_id: Some("<m@x>".into()),
        ..make_test_message()
    };
    let provider = Arc::new(MockEmailProvider::new());
    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::clone(&provider)));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo {
        shared: vec![(msg.id, false)],
        ..MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])
    });
    let handler = MessageCommandHandler::new(es, registry, read_repo.clone(), bus.clone());

    handler
        .handle_fetch_body(msg.id, MailboxPath::inbox())
        .await
        .unwrap();

    assert!(
        provider.fetched.read().unwrap().is_empty(),
        "nothing downloaded"
    );
    assert_eq!(bus.last_event_name().unwrap(), "MessageBodyFetched");
}

/// Only the start of it is anywhere: the server still sends the rest.
#[tokio::test]
async fn handle_fetch_body_still_downloads_when_the_copy_is_truncated() {
    let msg = Message {
        message_rfc_id: Some("<m@x>".into()),
        ..make_test_message()
    };
    let provider = Arc::new(MockEmailProvider::new());
    *provider.detail_response.write().unwrap() = Some(Message {
        body_text: Some("whole".into()),
        ..msg.clone()
    });
    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::clone(&provider)));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo {
        shared: vec![(msg.id, true)],
        ..MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])
    });
    let handler = MessageCommandHandler::new(es, registry, read_repo.clone(), bus.clone());

    handler
        .handle_fetch_body(msg.id, MailboxPath::inbox())
        .await
        .unwrap();

    assert_eq!(*provider.fetched.read().unwrap(), [RemoteId::from(42)]);
    assert_eq!(
        read_repo.stored_bodies.lock().unwrap()[0].1.as_deref(),
        Some("whole")
    );
}

/// Opening an unread message marks it read at the same moment, so the
/// `MessageFlagged` lands while the body is still being downloaded. The
/// fetch used to append at the sequence it read before the download and
/// lose the race — leaving the reading pane blank.
#[tokio::test]
async fn handle_fetch_body_survives_a_flag_change_during_the_download() {
    let msg = make_test_message();
    let msg_id = msg.id;
    let es = Arc::new(MockEventStore::with_message(&msg));
    let provider = MockEmailProvider::new();
    *provider.detail_response.write().unwrap() = Some(Message {
        body_text: Some("fetched text".into()),
        ..msg.clone()
    });
    let store = Arc::clone(&es);
    *provider.during_fetch.write().unwrap() = Some(Box::new(move || {
        let mut events = store.events.write().unwrap();
        let list = events.get_mut(&msg_id.to_string()).unwrap();
        let seq = list.len() as u64 + 1;
        list.push(EventEnvelope::new(
            msg_id.to_string(),
            AggregateType::Message,
            seq,
            DomainEvent::MessageFlagged {
                message_id: msg_id,
                flags_added: vec![Flag::Seen],
                flags_removed: vec![],
            },
        ));
    }));

    let registry = Arc::new(MockProviderRegistry::new(Arc::new(provider)));
    let bus = Arc::new(MockEventBus::new());
    let handler = MessageCommandHandler::new(
        es,
        registry,
        Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])),
        bus.clone(),
    );

    handler
        .handle_fetch_body(msg_id, MailboxPath::inbox())
        .await
        .unwrap();
    assert_eq!(bus.last_event_name().unwrap(), "MessageBodyFetched");
}
