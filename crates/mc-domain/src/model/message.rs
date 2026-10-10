use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::events::DomainEvent;
use crate::model::account::AccountId;
use crate::model::attachment::Attachment;
use crate::model::mailbox::MailboxId;
use crate::value_objects::email_address::EmailAddress;
use crate::value_objects::flag::Flag;
use crate::value_objects::message_id::MessageId;
use crate::value_objects::uid::ProviderUid;

/// An address with optional display name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address {
    pub name: Option<String>,
    pub email: EmailAddress,
}

/// Message envelope (header summary).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub from: Vec<Address>,
    pub to: Vec<Address>,
    pub cc: Vec<Address>,
    pub bcc: Vec<Address>,
    pub reply_to: Vec<Address>,
    pub subject: String,
    pub date: DateTime<Utc>,
}

/// Message aggregate root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: MessageId,
    pub account_id: AccountId,
    pub mailbox_id: MailboxId,
    pub provider_uid: Option<ProviderUid>,
    pub message_rfc_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub envelope: Envelope,
    pub flags: Vec<Flag>,
    pub size_bytes: u64,
    pub has_attachments: bool,
    pub attachments: Vec<Attachment>,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    pub received_at: DateTime<Utc>,
    pub internal_date: DateTime<Utc>,
}

impl Message {
    /// Create a message from provider data (received during sync) and produce the event.
    pub fn receive(
        account_id: AccountId,
        mailbox_id: MailboxId,
        provider_uid: ProviderUid,
        message_rfc_id: Option<String>,
        in_reply_to: Option<String>,
        references: Vec<String>,
        envelope: Envelope,
        flags: Vec<Flag>,
        size_bytes: u64,
        has_attachments: bool,
        internal_date: DateTime<Utc>,
    ) -> (Self, DomainEvent) {
        let id = MessageId::new();
        let received_at = internal_date;

        let event = DomainEvent::MessageReceived {
            message_id: id,
            account_id,
            mailbox_id,
            provider_uid,
            message_rfc_id: message_rfc_id.clone(),
            in_reply_to: in_reply_to.clone(),
            references: references.clone(),
            envelope: envelope.clone(),
            flags: flags.clone(),
            size_bytes,
            has_attachments,
            received_at,
        };

        let message = Self {
            id,
            account_id,
            mailbox_id,
            provider_uid: Some(provider_uid),
            message_rfc_id,
            in_reply_to,
            references,
            envelope,
            flags,
            size_bytes,
            has_attachments,
            attachments: Vec::new(),
            body_text: None,
            body_html: None,
            received_at,
            internal_date: received_at,
        };

        (message, event)
    }

    /// Attach the body content after fetching it from the provider.
    pub fn set_body(
        &mut self,
        body_text: Option<String>,
        body_html: Option<String>,
        attachments: Vec<Attachment>,
    ) -> DomainEvent {
        self.body_text = body_text.clone();
        self.body_html = body_html.clone();
        self.attachments = attachments.clone();
        self.has_attachments = !self.attachments.is_empty();

        DomainEvent::MessageBodyFetched {
            message_id: self.id,
            attachments,
        }
    }

    /// Change flags on the message and produce the event.
    pub fn change_flags(
        &mut self,
        flags_to_add: Vec<Flag>,
        flags_to_remove: Vec<Flag>,
    ) -> DomainEvent {
        for flag in &flags_to_remove {
            self.flags.retain(|f| f != flag);
        }
        for flag in &flags_to_add {
            if !self.flags.contains(flag) {
                self.flags.push(flag.clone());
            }
        }

        DomainEvent::MessageFlagged {
            message_id: self.id,
            mailbox_id: self.mailbox_id,
            flags_added: flags_to_add,
            flags_removed: flags_to_remove,
        }
    }

    /// Mark as read (add Seen flag).
    pub fn mark_as_read(&mut self) -> DomainEvent {
        self.change_flags(vec![Flag::Seen], vec![])
    }

    /// Mark as unread (remove Seen flag).
    pub fn mark_as_unread(&mut self) -> DomainEvent {
        self.change_flags(vec![], vec![Flag::Seen])
    }

    /// Toggle the Flagged (starred) flag.
    pub fn toggle_flagged(&mut self) -> DomainEvent {
        if self.flags.contains(&Flag::Flagged) {
            self.change_flags(vec![], vec![Flag::Flagged])
        } else {
            self.change_flags(vec![Flag::Flagged], vec![])
        }
    }

    /// Move the message to another mailbox and produce the event.
    pub fn move_to(&mut self, to_mailbox_id: MailboxId) -> DomainEvent {
        let from = self.mailbox_id;
        self.mailbox_id = to_mailbox_id;

        DomainEvent::MessageMoved {
            message_id: self.id,
            from_mailbox_id: from,
            to_mailbox_id,
        }
    }

    /// Produce a deletion event.
    pub fn delete(&self, permanently: bool) -> DomainEvent {
        DomainEvent::MessageDeleted {
            message_id: self.id,
            mailbox_id: self.mailbox_id,
            permanently,
        }
    }

    /// Whether the message has been read.
    pub fn is_read(&self) -> bool {
        self.flags.contains(&Flag::Seen)
    }

    /// Whether the message is flagged/starred.
    pub fn is_flagged(&self) -> bool {
        self.flags.contains(&Flag::Flagged)
    }

    // ── Event Sourcing rehydration ─────────────────────────────────────

    /// Apply a single domain event to mutate aggregate state.
    pub fn apply(&mut self, event: &DomainEvent) {
        match event {
            DomainEvent::MessageReceived { .. } => {
                // Initial state is set by the constructor.
            }
            DomainEvent::MessageBodyFetched { attachments, .. } => {
                self.attachments = attachments.clone();
                self.has_attachments = !self.attachments.is_empty();
            }
            DomainEvent::MessageFlagged {
                flags_added,
                flags_removed,
                ..
            } => {
                for flag in flags_removed {
                    self.flags.retain(|f| f != flag);
                }
                for flag in flags_added {
                    if !self.flags.contains(flag) {
                        self.flags.push(flag.clone());
                    }
                }
            }
            DomainEvent::MessageMoved { to_mailbox_id, .. } => {
                self.mailbox_id = *to_mailbox_id;
            }
            DomainEvent::MessageDeleted { .. }
            | DomainEvent::MessageSent { .. }
            | DomainEvent::DraftSaved { .. }
            | DomainEvent::DraftDiscarded { .. } => {
                // These don't further mutate in-memory message state.
            }
            _ => {} // Ignore events belonging to other aggregates.
        }
    }

    /// Rehydrate a Message from a sequence of domain events.
    ///
    /// The first event must be `MessageReceived`. Returns `None` if the
    /// events slice is empty or doesn't start with a creation event.
    pub fn rehydrate(events: &[DomainEvent]) -> Option<Self> {
        let first = events.first()?;
        let DomainEvent::MessageReceived {
            message_id,
            account_id,
            mailbox_id,
            provider_uid,
            message_rfc_id,
            in_reply_to,
            references,
            envelope,
            flags,
            size_bytes,
            has_attachments,
            received_at,
        } = first
        else {
            return None;
        };

        let mut message = Self {
            id: *message_id,
            account_id: *account_id,
            mailbox_id: *mailbox_id,
            provider_uid: Some(*provider_uid),
            message_rfc_id: message_rfc_id.clone(),
            in_reply_to: in_reply_to.clone(),
            references: references.clone(),
            envelope: envelope.clone(),
            flags: flags.clone(),
            size_bytes: *size_bytes,
            has_attachments: *has_attachments,
            attachments: Vec::new(),
            body_text: None,
            body_html: None,
            received_at: *received_at,
            internal_date: *received_at,
        };

        for event in &events[1..] {
            message.apply(event);
        }

        Some(message)
    }
}

/// Outgoing message for sending.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutgoingMessage {
    pub from: Address,
    pub to: Vec<Address>,
    pub cc: Vec<Address>,
    pub bcc: Vec<Address>,
    pub subject: String,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub attachments: Vec<OutgoingAttachment>,
}

/// Attachment for outgoing messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutgoingAttachment {
    pub filename: String,
    pub mime_type: String,
    pub data: Vec<u8>,
}

#[cfg(test)]
mod tests {
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
        assert!(!json.contains("Hello"), "body must not travel in the event: {json}");
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

        let event = msg.move_to(target_mailbox);

        assert_eq!(msg.mailbox_id, target_mailbox);
        assert_eq!(event.event_name(), "MessageMoved");
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
}
