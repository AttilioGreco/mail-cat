use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::events::DomainEvent;
use crate::model::account::AccountId;
use crate::value_objects::flag::Flag;
use crate::value_objects::mailbox_path::MailboxPath;

/// Unique mailbox identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
            mailbox_id: id.clone(),
            account_id: account_id.clone(),
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
            mailbox_id: self.id.clone(),
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
            mailbox_id: self.id.clone(),
            old_path,
            new_path,
        }
    }

    /// Produce a deletion event.
    pub fn delete(&self) -> DomainEvent {
        DomainEvent::MailboxDeleted {
            mailbox_id: self.id.clone(),
            account_id: self.account_id.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discover_mailbox_produces_event() {
        let (mailbox, event) = Mailbox::discover(
            AccountId::new(),
            MailboxPath::inbox(),
            MailboxType::Inbox,
        );

        assert_eq!(mailbox.display_name, "INBOX");
        assert_eq!(mailbox.mailbox_type, MailboxType::Inbox);
        assert_eq!(mailbox.total_messages, 0);
        assert_eq!(event.event_name(), "MailboxDiscovered");
    }

    #[test]
    fn update_counts() {
        let (mut mailbox, _) = Mailbox::discover(
            AccountId::new(),
            MailboxPath::inbox(),
            MailboxType::Inbox,
        );

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
