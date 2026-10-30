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
    NewMessage { remote_id: String },
    MessageExpunged { message_id: String },
    FlagsChanged,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Account {
    pub id: String,
    pub name: String,
    pub email: String,
    pub status: AccountStatus,
    pub mailbox_model: MailboxModel,
    /// Whether the server accepts tags; `None` until a sync found out.
    pub supports_tags: Option<bool>,
    /// Live auth state; `None` when the server runs no gate for the account.
    pub auth: Option<AuthStatus>,
}

/// How an account's mailboxes are presented: a tree of folders, or labels
/// on messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum MailboxModel {
    Folders,
    Labels,
}

/// An account's live auth state, as the server's auth gate sees it.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AuthStatus {
    pub account_id: String,
    pub state: AuthState,
    /// Why the account is in `state`; `None` while active.
    pub reason: Option<String>,
    /// The most recent failure, kept after the account recovers.
    pub last_error: Option<String>,
    pub since: Option<DateTime<Utc>>,
    /// When an offline account is retried.
    pub next_retry: Option<DateTime<Utc>>,
    /// `None` for a password account.
    pub oauth_provider: Option<OAuthProvider>,
    pub token_expires_at: Option<DateTime<Utc>>,
    /// Why the last SMTP login was refused; never affects `state`.
    pub smtp_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum AuthState {
    Active,
    Offline,
    NeedsReauth,
    CredentialsRejected,
    OAuthClientInvalid,
    Unknown,
}

impl AuthState {
    /// Machine-readable tag, used as the wire value towards the desktop UI.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Offline => "offline",
            Self::NeedsReauth => "needs_reauth",
            Self::CredentialsRejected => "credentials_rejected",
            Self::OAuthClientInvalid => "oauth_client_invalid",
            Self::Unknown => "unknown",
        }
    }
}

/// Health of an account, as the server sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum AccountStatus {
    Active,
    Disabled,
    /// Sync is failing for a reason other than credentials.
    Error,
    /// The OAuth token is gone or rejected — needs an interactive re-login.
    NeedsReauth,
    /// The server rejected the password — the user has to update it.
    CredentialsRejected,
    Unknown,
}

impl AccountStatus {
    /// Machine-readable tag, used as the wire value towards the desktop UI.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Disabled => "disabled",
            Self::Error => "error",
            Self::NeedsReauth => "needs_reauth",
            Self::CredentialsRejected => "credentials_rejected",
            Self::Unknown => "unknown",
        }
    }
}

/// How much of an account is downloaded locally.
#[derive(Debug, Clone, Copy, Default)]
pub struct AccountStats {
    /// Messages the server reports across all known mailboxes.
    pub total_messages: u32,
    /// Envelopes stored locally.
    pub synced_messages: u32,
    /// Stored envelopes whose body is still queued for download.
    pub pending_bodies: u32,
}

/// IMAP/SMTP settings for a manually configured account.
#[derive(Debug, Clone)]
pub struct ImapAccountParams {
    pub name: String,
    pub email: String,
    pub imap_host: String,
    pub imap_port: u16,
    pub smtp_host: Option<String>,
    pub smtp_port: Option<u16>,
    pub use_tls: bool,
    pub username: String,
    pub password: String,
}

/// Server and credentials for a password-authenticated JMAP account.
#[derive(Debug, Clone)]
pub struct JmapAccountParams {
    pub name: String,
    pub email: String,
    pub jmap_url: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Mailbox {
    pub id: String,
    pub account_id: String,
    /// Opaque provider id; match on it, never display it.
    pub path: String,
    /// Human-readable hierarchy.
    pub display_path: String,
    pub display_name: String,
    /// Special-use classification. Folder names are localized, so this is the
    /// only reliable way to find "the Sent folder".
    pub mailbox_type: MailboxKind,
    pub total_messages: u32,
    pub unread_messages: u32,
    /// Shown in the sidebar and synced; unsubscribed ones only appear in the
    /// folder management dialog.
    pub subscribed: bool,
}

/// RFC 6154 special-use classification of a folder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum MailboxKind {
    Inbox,
    Sent,
    Drafts,
    Trash,
    Spam,
    Archive,
    /// Gmail's "All Mail" (RFC 6154 `\All`) — doubles as its archive target.
    All,
    Important,
    Custom,
}

impl MailboxKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Inbox => "inbox",
            Self::Sent => "sent",
            Self::Drafts => "drafts",
            Self::Trash => "trash",
            Self::Spam => "spam",
            Self::Archive => "archive",
            Self::All => "all",
            Self::Important => "important",
            Self::Custom => "custom",
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MessageSummary {
    pub id: String,
    /// Every mailbox the message is in.
    pub mailbox_ids: Vec<String>,
    pub subject: String,
    pub from_name: Option<String>,
    pub from_email: String,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub date: DateTime<Utc>,
    pub is_read: bool,
    /// `\Flagged` — needed for a real star toggle, not just "add a star".
    pub is_flagged: bool,
    pub has_attachments: bool,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Membership {
    pub mailbox_id: String,
    pub remote_id: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MessageBody {
    pub id: String,
    pub subject: String,
    pub mailbox_ids: Vec<String>,
    /// Provider ids per mailbox (IMAP UID, JMAP email id).
    pub memberships: Vec<Membership>,
    /// Wire names like `FLAG_SEEN`.
    pub flags: Vec<String>,
    pub tags: Vec<String>,
    pub html_body: Option<String>,
    pub text_body: Option<String>,
    pub rfc_message_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SyncProgress {
    pub account_id: String,
    pub phase: SyncPhase,
    pub mailbox_name: Option<String>,
    pub processed: u32,
    pub total: Option<u32>,
    /// Human-readable description of what is happening ("Syncing INBOX...").
    pub message: Option<String>,
}

/// Flag that can be set/removed on a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum MessageFlag {
    Seen,
    Answered,
    Flagged,
    Deleted,
    Draft,
}

/// A sender the compose window can pick.
#[derive(Debug, Clone)]
pub struct ComposeIdentity {
    pub account_id: String,
    pub display_name: String,
    pub email: String,
    pub signature_html: Option<String>,
}

/// An attachment of an existing message (forward source, stored draft).
#[derive(Debug, Clone)]
pub struct ForwardedAttachment {
    pub id: String,
    pub filename: String,
    pub size: u64,
    pub mime_type: String,
}

/// Image a restored draft shows inline, referenced as `cid:<content_id>`.
#[derive(Debug, Clone)]
pub struct InlineImage {
    pub content_id: String,
    pub mime_type: String,
    pub data: Vec<u8>,
}

/// Starting state of the compose window, computed by the server.
#[derive(Debug, Clone)]
pub struct ComposeView {
    pub draft_id: Option<String>,
    pub account_id: String,
    pub identities: Vec<ComposeIdentity>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub subject: String,
    pub body_md: String,
    pub quoted_html: Option<String>,
    pub signature_html: Option<String>,
    pub forwarded_attachments: Vec<ForwardedAttachment>,
    pub inline_images: Vec<InlineImage>,
    /// "new" | "reply" | "replyAll" | "forward" | "draft"
    pub mode: String,
    pub source_message_id: Option<String>,
}

/// A file attached in the compose window. `content_id` set = inline image.
#[derive(Debug, Clone)]
pub struct ComposeAttachment {
    pub filename: String,
    pub mime_type: String,
    pub content_id: Option<String>,
    pub data: Vec<u8>,
}

/// What the compose window sends or saves as a draft.
#[derive(Debug, Clone)]
pub struct ComposeInput {
    pub draft_id: Option<String>,
    pub account_id: String,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub subject: String,
    pub body_md: String,
    pub mode: String,
    pub source_message_id: Option<String>,
    pub attachments: Vec<ComposeAttachment>,
    pub forwarded_attachment_ids: Vec<String>,
}

/// A search result hit with envelope summary.
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub message_id: String,
    pub mailbox_ids: Vec<String>,
    pub score: f32,
    pub snippet: Option<String>,
    pub subject: String,
    pub from_name: Option<String>,
    pub from_email: String,
    pub date: i64,
}

/// A contact autocomplete result.
#[derive(Debug, Clone)]
pub struct ContactHit {
    pub id: String,
    pub display_name: String,
    pub primary_email: String,
    pub all_emails: Vec<String>,
    pub organization: Option<String>,
    /// Account this contact is scoped to. `None` = shared across all accounts.
    pub account_scope: Option<String>,
    /// True when this hit comes from a different account than the requester.
    pub is_cross_account: bool,
}

/// A single message within a thread, with full envelope + body for thread view.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ThreadMessage {
    pub id: String,
    pub rfc_message_id: Option<String>,
    pub from_name: Option<String>,
    pub from_email: String,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub subject: String,
    pub date: DateTime<Utc>,
    pub is_read: bool,
    pub mailbox_ids: Vec<String>,
    pub html_body: Option<String>,
    pub text_body: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
}

/// A conversation (thread) summary for the inbox list view.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConversationSummary {
    pub thread_id: String,
    pub account_id: String,
    pub subject: String,
    pub message_count: u32,
    pub unread_count: u32,
    pub last_message_at: DateTime<Utc>,
    pub latest_sender: String,
    /// A domain message UUID (not an RFC Message-ID) suitable for passing to `get_thread`.
    pub sample_message_id: String,
}

/// A search suggestion (autocomplete).
#[derive(Debug, Clone)]
pub struct Suggestion {
    pub field: String,
    pub value: String,
    pub doc_count: u32,
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

/// OAuth2 provider for account login (Google/Microsoft).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum OAuthProvider {
    Google,
    Microsoft,
}

/// Streaming event emitted during an OAuth2 login flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OAuthLoginEvent {
    AwaitingBrowser { authorize_url: String },
    Succeeded { email: String },
    Failed { message: String },
}
