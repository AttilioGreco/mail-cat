use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::events::DomainEvent;
use crate::model::account::AccountId;
use crate::value_objects::flag::Flag;
use crate::value_objects::mailbox_path::MailboxPath;

/// Unique mailbox identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MailboxId(Uuid);

impl MailboxId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for MailboxId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MailboxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for MailboxId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

/// Mailbox type classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MailboxType {
    Inbox,
    Sent,
    Drafts,
    Trash,
    Spam,
    Archive,
    Custom,
}

/// Mailbox aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mailbox {
    pub id: MailboxId,
    pub account_id: AccountId,
    pub path: MailboxPath,
    pub display_name: String,
    pub mailbox_type: MailboxType,
    pub flags: Vec<Flag>,
    pub total_messages: u32,
    pub unread_messages: u32,
    pub uid_validity: Option<u32>,
    pub uid_next: Option<u32>,
}

impl Mailbox {
    /// Create a new mailbox discovered during sync and produce the event.
    pub fn discover(
        account_id: AccountId,
        path: MailboxPath,
        mailbox_type: MailboxType,
    ) -> (Self, DomainEvent) {
        let id = MailboxId::new();
        let display_name = path.name().to_string();

        let event = DomainEvent::MailboxDiscovered {
            mailbox_id: id,
            account_id,
            path: path.clone(),
            mailbox_type,
        };

        let mailbox = Self {
            id,
            account_id,
            path,
            display_name,
            mailbox_type,
            flags: Vec::new(),
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        };

        (mailbox, event)
    }

    /// Update message counts and produce the event.
    pub fn update_counts(&mut self, total: u32, unread: u32) -> DomainEvent {
        self.total_messages = total;
        self.unread_messages = unread;

        DomainEvent::MailboxCountsUpdated {
            mailbox_id: self.id,
            total_messages: total,
            unread_messages: unread,
        }
    }

    /// Rename the mailbox and produce the event.
    pub fn rename(&mut self, new_path: MailboxPath) -> DomainEvent {
        let old_path = self.path.clone();
        self.display_name = new_path.name().to_string();
        self.path = new_path.clone();

        DomainEvent::MailboxRenamed {
            mailbox_id: self.id,
            old_path,
            new_path,
        }
    }

    /// Produce a deletion event.
    pub fn delete(&self) -> DomainEvent {
        DomainEvent::MailboxDeleted {
            mailbox_id: self.id,
            account_id: self.account_id,
        }
    }

    // ── Event Sourcing rehydration ─────────────────────────────────────

    /// Apply a single domain event to mutate aggregate state.
    pub fn apply(&mut self, event: &DomainEvent) {
        match event {
            DomainEvent::MailboxDiscovered { .. } => {
                // Initial state is set by the constructor.
            }
            DomainEvent::MailboxCountsUpdated {
                total_messages,
                unread_messages,
                ..
            } => {
                self.total_messages = *total_messages;
                self.unread_messages = *unread_messages;
            }
            DomainEvent::MailboxRenamed { new_path, .. } => {
                self.display_name = new_path.name().to_string();
                self.path = new_path.clone();
            }
            DomainEvent::MailboxDeleted { .. } => {
                // Mailbox is logically deleted; upstream handles removal.
            }
            _ => {} // Ignore events belonging to other aggregates.
        }
    }

    /// Rehydrate a Mailbox from a sequence of domain events.
    ///
    /// The first event must be `MailboxDiscovered`. Returns `None` if the
    /// events slice is empty or doesn't start with a creation event.
    pub fn rehydrate(events: &[DomainEvent]) -> Option<Self> {
        let first = events.first()?;
        let DomainEvent::MailboxDiscovered {
            mailbox_id,
            account_id,
            path,
            mailbox_type,
        } = first
        else {
            return None;
        };

        let display_name = path.name().to_string();
        let mut mailbox = Self {
            id: *mailbox_id,
            account_id: *account_id,
            path: path.clone(),
            display_name,
            mailbox_type: *mailbox_type,
            flags: Vec::new(),
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        };

        for event in &events[1..] {
            mailbox.apply(event);
        }

        Some(mailbox)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discover_mailbox_produces_event() {
        let (mailbox, event) =
            Mailbox::discover(AccountId::new(), MailboxPath::inbox(), MailboxType::Inbox);

        assert_eq!(mailbox.display_name, "INBOX");
        assert_eq!(mailbox.mailbox_type, MailboxType::Inbox);
        assert_eq!(mailbox.total_messages, 0);
        assert_eq!(event.event_name(), "MailboxDiscovered");
    }

    #[test]
    fn update_counts() {
        let (mut mailbox, _) =
            Mailbox::discover(AccountId::new(), MailboxPath::inbox(), MailboxType::Inbox);

        let event = mailbox.update_counts(150, 12);

        assert_eq!(mailbox.total_messages, 150);
        assert_eq!(mailbox.unread_messages, 12);
        assert_eq!(event.event_name(), "MailboxCountsUpdated");
    }

    #[test]
    fn rename_mailbox() {
        let (mut mailbox, _) = Mailbox::discover(
            AccountId::new(),
            MailboxPath::parse("Work", '/').unwrap(),
            MailboxType::Custom,
        );

        let event = mailbox.rename(MailboxPath::parse("Work-Archive", '/').unwrap());

        assert_eq!(mailbox.display_name, "Work-Archive");
        assert_eq!(mailbox.path.full_path(), "Work-Archive");
        assert_eq!(event.event_name(), "MailboxRenamed");
    }

    #[test]
    fn rehydrate_mailbox_from_events() {
        let id = MailboxId::new();
        let account_id = AccountId::new();
        let events = vec![
            DomainEvent::MailboxDiscovered {
                mailbox_id: id,
                account_id,
                path: MailboxPath::inbox(),
                mailbox_type: MailboxType::Inbox,
            },
            DomainEvent::MailboxCountsUpdated {
                mailbox_id: id,
                total_messages: 42,
                unread_messages: 5,
            },
            DomainEvent::MailboxRenamed {
                mailbox_id: id,
                old_path: MailboxPath::inbox(),
                new_path: MailboxPath::parse("Primary", '/').unwrap(),
            },
        ];

        let mailbox = Mailbox::rehydrate(&events).unwrap();
        assert_eq!(mailbox.total_messages, 42);
        assert_eq!(mailbox.unread_messages, 5);
        assert_eq!(mailbox.display_name, "Primary");
        assert_eq!(mailbox.path.full_path(), "Primary");
    }

    #[test]
    fn rehydrate_empty_events_returns_none() {
        let result = Mailbox::rehydrate(&[]);
        assert!(result.is_none());
    }

    #[test]
    fn mailbox_serialization_roundtrip() {
        let (mailbox, _) = Mailbox::discover(
            AccountId::new(),
            MailboxPath::parse("Sent/2024", '/').unwrap(),
            MailboxType::Sent,
        );

        let json = serde_json::to_string(&mailbox).unwrap();
        let deserialized: Mailbox = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.display_name, "2024");
        assert_eq!(deserialized.mailbox_type, MailboxType::Sent);
    }
}
