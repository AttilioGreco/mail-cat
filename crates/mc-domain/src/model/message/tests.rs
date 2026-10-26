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
        RemoteId::from(42),
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
    assert_eq!(msg.memberships[0].remote_id, Some(RemoteId::from(42)));
    assert!(!msg.is_read());
    assert_eq!(event.event_name(), "MessageReceived");
}

#[test]
fn set_body_produces_event() {
    let (mut msg, _) = Message::receive(
        AccountId::new(),
        MailboxId::new(),
        RemoteId::from(1),
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
        RemoteId::from(1),
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
        RemoteId::from(1),
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

fn received_in(mailbox_id: MailboxId, uid: u32) -> Message {
    Message::receive(
        AccountId::new(),
        mailbox_id,
        RemoteId::from(uid),
        None,
        None,
        vec![],
        test_envelope(),
        vec![],
        512,
        false,
        Utc::now(),
    )
    .0
}

fn uid_in(msg: &Message, mailbox_id: MailboxId) -> Option<RemoteId> {
    msg.membership(mailbox_id).and_then(|m| m.remote_id.clone())
}

#[test]
fn move_message() {
    let original_mailbox = MailboxId::new();
    let target_mailbox = MailboxId::new();
    let mut msg = received_in(original_mailbox, 1);

    let event = msg.move_to(original_mailbox, target_mailbox, None).unwrap();

    assert_eq!(event.event_name(), "MessageMailboxesChanged");
    assert_eq!(
        msg.memberships,
        [Membership {
            mailbox_id: target_mailbox,
            remote_id: None
        }]
    );
    assert!(
        msg.move_to(original_mailbox, target_mailbox, None)
            .is_none(),
        "no longer in the source"
    );

    assert!(
        msg.attach_remote_id(original_mailbox, RemoteId::from(7))
            .is_none(),
        "left that mailbox"
    );
    let attached = msg
        .attach_remote_id(target_mailbox, RemoteId::from(42))
        .unwrap();
    assert!(
        msg.attach_remote_id(target_mailbox, RemoteId::from(43))
            .is_none(),
        "already attached"
    );
    let rehydrated = Message::rehydrate(&[
        DomainEvent::MessageReceived {
            message_id: msg.id,
            account_id: msg.account_id,
            mailbox_id: original_mailbox,
            remote_id: RemoteId::from(1),
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
    assert_eq!(
        rehydrated.memberships,
        [Membership {
            mailbox_id: target_mailbox,
            remote_id: Some(RemoteId::from(42))
        }]
    );

    assert!(
        msg.forget_remote_id(original_mailbox).is_none(),
        "not in that mailbox"
    );
    assert!(msg.forget_remote_id(target_mailbox).is_some());
    assert_eq!(uid_in(&msg, target_mailbox), None);
    assert!(
        msg.forget_remote_id(target_mailbox).is_none(),
        "nothing left to forget"
    );
}

#[test]
fn a_message_is_in_many_mailboxes_and_deleted_with_the_last() {
    let (inbox, label) = (MailboxId::new(), MailboxId::new());
    let mut msg = received_in(inbox, 1);

    let added = msg.add_to(label, Some(RemoteId::from(9))).unwrap();
    assert!(msg.add_to(label, None).is_none(), "already there");
    assert_eq!(msg.memberships.len(), 2);

    let removed = msg.remove_from(inbox).unwrap();
    assert_eq!(removed.event_name(), "MessageMailboxesChanged");
    assert!(msg.remove_from(inbox).is_none(), "not there any more");
    assert_eq!(uid_in(&msg, label), Some(RemoteId::from(9)));
    assert!(!msg.is_deleted());

    let gone = msg.remove_from(label).unwrap();
    assert!(
        matches!(&gone, DomainEvent::MessageDeleted { mailbox_ids, .. } if mailbox_ids == &[label])
    );
    assert!(msg.is_deleted());

    let mut replayed = received_in(inbox, 1);
    for event in [added, removed, gone] {
        replayed.apply(&event);
    }
    assert!(replayed.is_deleted());
}

#[test]
fn an_added_membership_for_a_mailbox_already_held_only_replaces_its_uid() {
    let inbox = MailboxId::new();
    let mut msg = received_in(inbox, 1);
    msg.apply(&DomainEvent::MessageMailboxesChanged {
        message_id: msg.id,
        added: vec![Membership {
            mailbox_id: inbox,
            remote_id: Some(RemoteId::from(5)),
        }],
        removed: vec![],
    });
    assert_eq!(msg.memberships.len(), 1);
    assert_eq!(uid_in(&msg, inbox), Some(RemoteId::from(5)));
}

#[test]
fn delete_message() {
    let (a, b) = (MailboxId::new(), MailboxId::new());
    let mut msg = received_in(a, 1);
    msg.add_to(b, None);

    let event = msg.delete();

    assert!(
        matches!(&event, DomainEvent::MessageDeleted { mailbox_ids, .. } if mailbox_ids == &[a, b])
    );
    assert!(msg.is_deleted());
}

#[test]
fn message_serialization_roundtrip() {
    let (msg, _) = Message::receive(
        AccountId::new(),
        MailboxId::new(),
        RemoteId::from(99),
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
            remote_id: RemoteId::from(100),
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
            flags_added: vec![Flag::Seen, Flag::Flagged],
            flags_removed: vec![],
        },
        DomainEvent::MessageMailboxesChanged {
            message_id: msg_id,
            added: vec![Membership {
                mailbox_id: target_mailbox,
                remote_id: None,
            }],
            removed: vec![mailbox_id],
        },
    ];

    let message = Message::rehydrate(&events).unwrap();
    // Bodies live in MessageBodyStore, not the log.
    assert!(message.body_text.is_none());
    assert!(message.is_read());
    assert!(message.is_flagged());
    assert_eq!(
        message.memberships,
        [Membership {
            mailbox_id: target_mailbox,
            remote_id: None
        }]
    );
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
        RemoteId::from(1),
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
