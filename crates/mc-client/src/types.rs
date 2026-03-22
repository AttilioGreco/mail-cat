/// Clean domain types exposed to frontends — no proto types leak outside this crate.
use chrono::{DateTime, Utc};

/// A real-time event pushed by the server when a mailbox changes.
#[derive(Debug, Clone)]
pub struct MailboxNotification {
    pub mailbox_id: String,
    pub event: MailboxNotificationKind,
}

#[derive(Debug, Clone)]
pub enum MailboxNotificationKind {
    CountsChanged { total: u32, unseen: u32 },
    NewMessage { provider_uid: u32 },
    MessageExpunged { provider_uid: u32 },
    FlagsChanged,
}

#[derive(Debug, Clone)]
pub struct Account {
    pub id: String,
    pub name: String,
    pub email: String,
    pub is_active: bool,
}

#[derive(Debug, Clone)]
pub struct Mailbox {
    pub id: String,
    pub account_id: String,
    pub path: String,
    pub display_name: String,
    pub total_messages: u32,
    pub unread_messages: u32,
}

#[derive(Debug, Clone)]
pub struct MessageSummary {
    pub id: String,
    pub mailbox_id: String,
    pub subject: String,
    pub from_name: Option<String>,
    pub from_email: String,
    pub to: Vec<String>,
    pub date: DateTime<Utc>,
    pub is_read: bool,
    pub has_attachments: bool,
}

#[derive(Debug, Clone)]
pub struct MessageBody {
    pub id: String,
    pub html_body: Option<String>,
    pub text_body: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SyncProgress {
    pub account_id: String,
    pub phase: SyncPhase,
    pub mailbox_name: Option<String>,
    pub processed: u32,
    pub total: Option<u32>,
}

/// Flag that can be set/removed on a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageFlag {
    Seen,
    Answered,
    Flagged,
    Deleted,
    Draft,
}

/// Parameters for sending a new email.
#[derive(Debug, Clone)]
pub struct SendMessageParams {
    pub account_id: String,
    pub from_email: String,
    pub from_name: Option<String>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub subject: String,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
}

/// A search result hit.
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub message_id: String,
    pub score: f32,
    pub snippet: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncPhase {
    DiscoveringMailboxes,
    FetchingEnvelopes,
    FetchingBodies,
    Indexing,
    Complete,
    Error(String),
}
