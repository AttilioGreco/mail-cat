//! CQRS commands — intent to change domain state.
//!
//! Each variant represents a use case. Commands are dispatched through the
//! [`CommandBus`](crate::bus::CommandBus) and handled by command handlers.

use mc_domain::{
    AccountId, ConnectionConfig, ContactEmail, ContactId, ContactSource, EmailAddress, Flag,
    Identity, MailboxId, MailboxPath, MessageId, ProviderType,
};

use crate::compose::{ComposeInput, ComposeMode, ComposeView};
pub use mc_domain::value_objects::remote_id::RemoteId;

/// All write-side commands in the application.
#[derive(Debug, Clone)]
pub enum Command {
    // ── Account ────────────────────────────────────────────────────────
    /// Configure a new email account.
    AddAccount {
        name: String,
        email: EmailAddress,
        provider_type: ProviderType,
        connection_config: ConnectionConfig,
        identity: Identity,
    },

    /// Update an existing account's settings.
    UpdateAccount {
        account_id: AccountId,
        name: Option<String>,
        connection_config: Option<ConnectionConfig>,
    },

    /// Remove an account and all associated data.
    RemoveAccount { account_id: AccountId },

    /// Start a full sync for an account.
    SyncAccount { account_id: AccountId },

    // ── Message ────────────────────────────────────────────────────────
    /// Fetch the body and attachments for a message, from its copy in
    /// `mailbox_id`.
    FetchMessageBody {
        message_id: MessageId,
        mailbox_id: MailboxId,
    },

    /// Set or unset flags on a message. Flags belong to the message, not to
    /// one of its mailboxes: the server change goes to every copy.
    SetMessageFlags {
        message_id: MessageId,
        flags_to_add: Vec<Flag>,
        flags_to_remove: Vec<Flag>,
    },

    /// Mark every unread message in a mailbox as read, in one provider round-trip.
    MarkMailboxRead {
        mailbox_id: MailboxId,
        mailbox_path: MailboxPath,
    },

    /// Move a message to another mailbox.
    MoveMessage {
        message_id: MessageId,
        from_mailbox_id: MailboxId,
        to_mailbox_id: MailboxId,
        to_mailbox_path: MailboxPath,
    },

    /// Delete a message (soft or permanent).
    DeleteMessage {
        message_id: MessageId,
        mailbox_id: MailboxId,
        permanently: bool,
    },

    /// Delete many messages (soft or permanent) in one provider round trip.
    /// Each is named with the mailbox it is acted on from: a bulk selection
    /// can span folders.
    DeleteMessages {
        messages: Vec<(MessageId, MailboxId)>,
        permanently: bool,
    },

    /// Move many messages, each from the mailbox it is paired with, to one
    /// destination mailbox.
    MoveMessages {
        messages: Vec<(MessageId, MailboxId)>,
        to_mailbox_id: MailboxId,
        to_mailbox_path: MailboxPath,
    },

    /// Compute the compose window's starting state (recipients, subject,
    /// quote, sender). A command, not a query, because forwarding and draft
    /// restore read the original's MIME parts from the provider.
    PrepareCompose {
        mode: ComposeMode,
        message_id: Option<MessageId>,
        account_id: Option<AccountId>,
    },

    /// Build and send a composed message via SMTP or provider API.
    SendCompose { input: ComposeInput },

    /// Store a draft in the Drafts folder, replacing `input.draft_id`.
    SaveDraft { input: ComposeInput },

    /// Discard a draft (locally and on the server).
    DiscardDraft { message_id: MessageId },

    /// Apply a flag change that originated from a remote IMAP client (e.g. IDLE notification).
    /// Does NOT call the provider — flags are already changed on the server.
    /// Only updates the local projection.
    ApplyRemoteFlagChange {
        mailbox_id: MailboxId,
        remote_id: RemoteId,
        new_flags: Vec<Flag>,
    },

    // ── Contact ────────────────────────────────────────────────────────
    /// Add a new contact to the address book (manual creation).
    AddContact {
        display_name: String,
        emails: Vec<ContactEmail>,
        phones: Vec<String>,
        organization: Option<String>,
        notes: Option<String>,
        source: ContactSource,
        external_id: Option<String>,
        account_scope: Option<AccountId>,
    },

    /// Idempotent upsert: create a `LocalEmail` contact for this address/account
    /// if one does not already exist. Used for auto-population from email activity.
    EnsureContact {
        email: EmailAddress,
        /// Display name extracted from the email header (`From: Name <addr>`).
        /// May be empty when the header contains only an address.
        display_name: Option<String>,
        account_id: AccountId,
    },

    /// Update an existing contact.
    UpdateContact {
        contact_id: ContactId,
        display_name: Option<String>,
        emails: Option<Vec<ContactEmail>>,
        phones: Option<Vec<String>>,
        /// `Some(Some(org))` = set, `Some(None)` = clear, `None` = no change
        organization: Option<Option<String>>,
        /// `Some(Some(note))` = set, `Some(None)` = clear, `None` = no change
        notes: Option<Option<String>>,
    },

    /// Remove a contact from the address book.
    RemoveContact { contact_id: ContactId },

    /// Merge two contacts: absorb `discard_id` into `keep_id`.
    /// After this command `discard_id` no longer exists.
    MergeContacts {
        keep_id: ContactId,
        discard_id: ContactId,
    },

    /// Rebuild the contact search index from the stored contact read models.
    /// Useful after a fresh install or when the Tantivy index is lost/corrupted.
    /// If `account_id` is given, only contacts scoped to that account are indexed;
    /// otherwise all contacts are processed.
    RebuildContactIndex { account_id: Option<AccountId> },

    /// Backfill the contact address book by scanning all stored messages and
    /// calling `EnsureContact` for every from/to/cc address found.
    ///
    /// Idempotent: contacts that already exist are left unchanged.
    /// Use this after a fresh install to populate contacts from historical mail.
    BackfillContactsFromMessages { account_id: Option<AccountId> },
}

/// Result of a successfully dispatched command.
#[derive(Debug, Clone)]
pub enum CommandResult {
    AccountAdded { account_id: AccountId },
    AccountUpdated,
    AccountRemoved,
    SyncStarted,
    MessageBodyFetched,
    FlagsSet,
    /// Count of messages marked read.
    MailboxMarkedRead { count: u32 },
    MessageMoved,
    MessageDeleted,
    /// Bulk result: `succeeded` messages changed locally, `failed` were skipped
    /// (already gone, no provider UID, stale search index). Partial success is
    /// normal here and must be reported, not swallowed.
    MessagesBulkChanged { succeeded: u32, failed: u32 },
    ComposePrepared(Box<ComposeView>),
    MessageSent { message_id: MessageId },
    DraftSaved { message_id: MessageId },
    DraftDiscarded,
    RemoteFlagApplied,
    ContactAdded { contact_id: ContactId },
    /// EnsureContact result: `created = true` if a new contact was created,
    /// `false` if it already existed (no-op).
    ContactEnsured { created: bool, contact_id: ContactId },
    ContactUpdated,
    ContactRemoved,
    ContactsMerged,
    /// Number of contacts that were successfully (re-)indexed.
    ContactIndexRebuilt { indexed_count: u32 },
    /// Result of `BackfillContactsFromMessages`.
    /// `new_contacts` = contacts created for the first time.
    /// `already_existed` = addresses already in the address book (skipped).
    ContactsBackfilled { new_contacts: u32, already_existed: u32 },
}
