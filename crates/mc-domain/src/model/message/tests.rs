use super::*;

fn test_envelope() -> Envelope {
    Envelope {
        from: vec![Address {
            name: Some("Sender".into()),
            email: EmailAddress::new("sender@example.com").unwrap(),
        }],
        to: vec![Address {
            name: None,
            email: EmailAddress::new("recipient@example.com").unwrap(),
        }],
        cc: vec![],
        bcc: vec![],
        reply_to: vec![],
        subject: "Test subject".into(),
        date: Utc::now(),
    }
}

#[test]
fn receive_message_produces_event() {
    let (msg, event) = Message::receive(
        AccountId::new(),
        MailboxId::new(),
        ProviderUid::new(42),
        None,
        None,
        vec![],
        test_envelope(),
        vec![Flag::Recent],
        2048,
        false,
        Utc::now(),
    );

    assert_eq!(msg.envelope.subject, "Test subject");
    assert_eq!(msg.provider_uid, Some(ProviderUid::new(42)));
    assert!(!msg.is_read());
    assert_eq!(event.event_name(), "MessageReceived");
}

#[test]
fn set_body_produces_event() {
    let (mut msg, _) = Message::receive(
        AccountId::new(),
        MailboxId::new(),
        ProviderUid::new(1),
        None,
        None,
        vec![],
        test_envelope(),
        vec![],
        1024,
        false,
        Utc::now(),
    );

    let event = msg.set_body(
        Some("Hello plain text".into()),
        Some("<p>Hello HTML</p>".into()),
        vec![],
    );

    assert_eq!(msg.body_text.as_deref(), Some("Hello plain text"));
    assert_eq!(msg.body_html.as_deref(), Some("<p>Hello HTML</p>"));
    assert_eq!(event.event_name(), "MessageBodyFetched");
    let json = serde_json::to_string(&event).unwrap();
    assert!(
        !json.contains("Hello"),
        "body must not travel in the event: {json}"
    );
}

#[test]
fn mark_as_read_and_unread() {
    let (mut msg, _) = Message::receive(
        AccountId::new(),
        MailboxId::new(),
        ProviderUid::new(1),
        None,
        None,
        vec![],
        test_envelope(),
        vec![],
        512,
        false,
        Utc::now(),
    );

    assert!(!msg.is_read());

    let event = msg.mark_as_read();
    assert!(msg.is_read());
    assert_eq!(event.event_name(), "MessageFlagged");

    let event = msg.mark_as_unread();
    assert!(!msg.is_read());
    assert_eq!(event.event_name(), "MessageFlagged");
}

#[test]
fn toggle_flagged() {
    let (mut msg, _) = Message::receive(
        AccountId::new(),
        MailboxId::new(),
        ProviderUid::new(1),
        None,
        None,
        vec![],
        test_envelope(),
        vec![],
        512,
        false,
        Utc::now(),
    );

    assert!(!msg.is_flagged());
    msg.toggle_flagged();
    assert!(msg.is_flagged());
    msg.toggle_flagged();
    assert!(!msg.is_flagged());
}

#[test]
fn move_message() {
    let original_mailbox = MailboxId::new();
    let target_mailbox = MailboxId::new();

    let (mut msg, _) = Message::receive(
        AccountId::new(),
        original_mailbox,
        ProviderUid::new(1),
        None,
        None,
        vec![],
        test_envelope(),
        vec![],
        512,
        false,
        Utc::now(),
    );

    let event = msg.move_to(target_mailbox, None);

    assert_eq!(msg.mailbox_id, target_mailbox);
    assert_eq!(event.event_name(), "MessageMoved");
    // UIDs are per-mailbox: the source UID must not follow the message.
    assert_eq!(msg.provider_uid, None);

    assert!(
        msg.attach_uid(original_mailbox, ProviderUid::new(7))
            .is_none(),
        "left that mailbox"
    );
    let attached = msg
        .attach_uid(target_mailbox, ProviderUid::new(42))
        .unwrap();
    assert!(
        msg.attach_uid(target_mailbox, ProviderUid::new(43))
            .is_none(),
        "already attached"
    );
    let rehydrated = Message::rehydrate(&[
        DomainEvent::MessageReceived {
            message_id: msg.id,
            account_id: msg.account_id,
            mailbox_id: original_mailbox,
            provider_uid: ProviderUid::new(1),
            message_rfc_id: None,
            in_reply_to: None,
            references: vec![],
            envelope: test_envelope(),
            flags: vec![],
            size_bytes: 512,
            has_attachments: false,
            received_at: Utc::now(),
            restored: false,
        },
        event,
        attached,
    ])
    .unwrap();
    assert_eq!(rehydrated.mailbox_id, target_mailbox);
    assert_eq!(rehydrated.provider_uid, Some(ProviderUid::new(42)));

    assert!(
        msg.forget_uid(original_mailbox).is_none(),
        "not in that mailbox"
    );
    assert!(msg.forget_uid(target_mailbox).is_some());
    assert_eq!(msg.provider_uid, None);
    assert!(
        msg.forget_uid(target_mailbox).is_none(),
        "nothing left to forget"
    );
}

#[test]
fn move_event_stored_before_the_uid_field_still_reads() {
    let event: DomainEvent = serde_json::from_value(serde_json::json!({
        "MessageMoved": {
            "message_id": MessageId::new(),
            "from_mailbox_id": MailboxId::new(),
            "to_mailbox_id": MailboxId::new(),
        }
    }))
    .unwrap();
    assert!(matches!(
        event,
        DomainEvent::MessageMoved {
            provider_uid: None,
            ..
        }
    ));
}

#[test]
fn delete_message() {
    let (msg, _) = Message::receive(
        AccountId::new(),
        MailboxId::new(),
        ProviderUid::new(1),
        None,
        None,
        vec![],
        test_envelope(),
        vec![],
        512,
        false,
        Utc::now(),
    );

    let soft_delete = msg.delete(false);
    assert_eq!(soft_delete.event_name(), "MessageDeleted");

    let hard_delete = msg.delete(true);
    assert_eq!(hard_delete.event_name(), "MessageDeleted");
}

#[test]
fn message_serialization_roundtrip() {
    let (msg, _) = Message::receive(
        AccountId::new(),
        MailboxId::new(),
        ProviderUid::new(99),
        None,
        None,
        vec![],
        test_envelope(),
        vec![Flag::Seen, Flag::Flagged],
        4096,
        true,
        Utc::now(),
    );

    let json = serde_json::to_string(&msg).unwrap();
    let deserialized: Message = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.envelope.subject, "Test subject");
    assert!(deserialized.is_read());
    assert!(deserialized.is_flagged());
    assert_eq!(deserialized.size_bytes, 4096);
}

#[test]
fn rehydrate_message_from_events() {
    let msg_id = MessageId::new();
    let account_id = AccountId::new();
    let mailbox_id = MailboxId::new();
    let target_mailbox = MailboxId::new();
    let received_at = Utc::now();

    let events = vec![
        DomainEvent::MessageReceived {
            message_id: msg_id,
            account_id,
            mailbox_id,
            provider_uid: ProviderUid::new(100),
            message_rfc_id: None,
            in_reply_to: None,
            references: vec![],
            envelope: test_envelope(),
            flags: vec![],
            size_bytes: 2048,
            has_attachments: false,
            received_at,
            restored: false,
        },
        DomainEvent::MessageBodyFetched {
            message_id: msg_id,
            attachments: vec![],
        },
        DomainEvent::MessageFlagged {
            message_id: msg_id,
            mailbox_id,
            flags_added: vec![Flag::Seen, Flag::Flagged],
            flags_removed: vec![],
        },
        DomainEvent::MessageMoved {
            message_id: msg_id,
            from_mailbox_id: mailbox_id,
            to_mailbox_id: target_mailbox,
            provider_uid: None,
        },
    ];

    let message = Message::rehydrate(&events).unwrap();
    // Bodies live in MessageBodyStore, not the log.
    assert!(message.body_text.is_none());
    assert!(message.is_read());
    assert!(message.is_flagged());
    assert_eq!(message.mailbox_id, target_mailbox);
}

#[test]
fn rehydrate_empty_events_returns_none() {
    let result = Message::rehydrate(&[]);
    assert!(result.is_none());
}

#[test]
fn change_flags_no_duplicates() {
    let (mut msg, _) = Message::receive(
        AccountId::new(),
        MailboxId::new(),
        ProviderUid::new(1),
        None,
        None,
        vec![],
        test_envelope(),
        vec![Flag::Seen],
        512,
        false,
        Utc::now(),
    );

    // Adding Seen again should not duplicate
    msg.change_flags(vec![Flag::Seen], vec![]);
    let seen_count = msg.flags.iter().filter(|f| f.is_seen()).count();
    assert_eq!(seen_count, 1);
}
