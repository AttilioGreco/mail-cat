use super::*;

#[test]
fn event_envelope_creation() {
    let event = DomainEvent::AccountRemoved {
        account_id: AccountId::new(),
    };
    let envelope = EventEnvelope::new("acc-123".into(), AggregateType::Account, 1, event);

    assert_eq!(envelope.aggregate_id, "acc-123");
    assert_eq!(envelope.aggregate_type, AggregateType::Account);
    assert_eq!(envelope.sequence_number, 1);
}

#[test]
fn event_aggregate_type_classification() {
    let account_event = DomainEvent::AccountAdded {
        account_id: AccountId::new(),
        name: "Test".into(),
        email: EmailAddress::new("test@example.com").unwrap(),
        provider_type: ProviderType::Imap,
    };
    assert_eq!(account_event.aggregate_type(), AggregateType::Account);

    let mailbox_event = DomainEvent::MailboxDeleted {
        mailbox_id: MailboxId::new(),
        account_id: AccountId::new(),
    };
    assert_eq!(mailbox_event.aggregate_type(), AggregateType::Mailbox);

    let message_event = DomainEvent::DraftDiscarded {
        message_id: MessageId::new(),
    };
    assert_eq!(message_event.aggregate_type(), AggregateType::Message);
}

#[test]
fn event_name() {
    let event = DomainEvent::MessageFlagged {
        message_id: MessageId::new(),
        mailbox_id: MailboxId::new(),
        flags_added: vec![Flag::Seen],
        flags_removed: vec![],
    };
    assert_eq!(event.event_name(), "MessageFlagged");
}

#[test]
fn event_serialization_roundtrip() {
    let event = DomainEvent::AccountAdded {
        account_id: AccountId::new(),
        name: "Work Email".into(),
        email: EmailAddress::new("work@company.com").unwrap(),
        provider_type: ProviderType::Imap,
    };

    let json = serde_json::to_string(&event).unwrap();
    let deserialized: DomainEvent = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.event_name(), "AccountAdded");
    assert_eq!(deserialized.aggregate_type(), AggregateType::Account);
}

#[test]
fn envelope_serialization_roundtrip() {
    let event = DomainEvent::MailboxCountsUpdated {
        mailbox_id: MailboxId::new(),
        total_messages: 42,
        unread_messages: 5,
    };
    let envelope = EventEnvelope::new("mbx-1".into(), AggregateType::Mailbox, 3, event);

    let json = serde_json::to_string(&envelope).unwrap();
    let deserialized: EventEnvelope = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.aggregate_id, "mbx-1");
    assert_eq!(deserialized.sequence_number, 3);
    assert_eq!(deserialized.payload.event_name(), "MailboxCountsUpdated");
}

#[test]
fn message_received_event_serialization() {
    let event = DomainEvent::MessageReceived {
        message_id: MessageId::new(),
        account_id: AccountId::new(),
        mailbox_id: MailboxId::new(),
        provider_uid: ProviderUid::new(1234),
        message_rfc_id: None,
        in_reply_to: None,
        references: vec![],
        envelope: Envelope {
            from: vec![],
            to: vec![],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            subject: "Test subject".into(),
            date: Utc::now(),
        },
        flags: vec![Flag::Recent],
        size_bytes: 4096,
        has_attachments: false,
        received_at: Utc::now(),
        restored: false,
    };

    let json = serde_json::to_string_pretty(&event).unwrap();
    assert!(json.contains("MessageReceived"));
    assert!(json.contains("Test subject"));
    assert!(json.contains("1234"));

    let deserialized: DomainEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.event_name(), "MessageReceived");
}

#[test]
fn aggregate_type_display() {
    assert_eq!(format!("{}", AggregateType::Account), "account");
    assert_eq!(format!("{}", AggregateType::Mailbox), "mailbox");
    assert_eq!(format!("{}", AggregateType::Message), "message");
}

#[test]
fn account_auth_expired_aggregate_type_and_name() {
    let event = DomainEvent::AccountAuthExpired {
        account_id: AccountId::new(),
        error: "invalid_grant".into(),
    };
    assert_eq!(event.aggregate_type(), AggregateType::Account);
    assert_eq!(event.event_name(), "AccountAuthExpired");
}

#[test]
fn account_reauthenticated_aggregate_type_and_name() {
    let event = DomainEvent::AccountReauthenticated {
        account_id: AccountId::new(),
    };
    assert_eq!(event.aggregate_type(), AggregateType::Account);
    assert_eq!(event.event_name(), "AccountReauthenticated");
}
