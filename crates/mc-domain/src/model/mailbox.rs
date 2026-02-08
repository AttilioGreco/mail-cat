use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
