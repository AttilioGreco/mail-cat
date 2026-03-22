//! CQRS commands — intent to change domain state.
//!
//! Each variant represents a use case. Commands are dispatched through the
//! [`CommandBus`](crate::bus::CommandBus) and handled by command handlers.

use mc_domain::{
    AccountId, ConnectionConfig, EmailAddress, Flag, Identity, MailboxId, MailboxPath, MessageId,
    OutgoingMessage, ProviderType,
};
pub use mc_domain::value_objects::uid::ProviderUid;

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
    /// Fetch the body and attachments for a message.
    FetchMessageBody {
        message_id: MessageId,
        mailbox_path: MailboxPath,
    },

    /// Set or unset flags on a message.
    SetMessageFlags {
        message_id: MessageId,
        mailbox_path: MailboxPath,
        flags_to_add: Vec<Flag>,
        flags_to_remove: Vec<Flag>,
    },

    /// Move a message to another mailbox.
    MoveMessage {
        message_id: MessageId,
        from_mailbox_id: MailboxId,
        to_mailbox_id: MailboxId,
        from_mailbox_path: MailboxPath,
        to_mailbox_path: MailboxPath,
    },

    /// Delete a message (soft or permanent).
    DeleteMessage {
        message_id: MessageId,
        mailbox_id: MailboxId,
        mailbox_path: MailboxPath,
        permanently: bool,
    },

    /// Send a message via SMTP or provider API.
    SendMessage {
        account_id: AccountId,
        outgoing: OutgoingMessage,
    },

    /// Save a draft.
    SaveDraft {
        account_id: AccountId,
        outgoing: OutgoingMessage,
    },

    /// Discard a draft.
    DiscardDraft { message_id: MessageId },

    /// Apply a flag change that originated from a remote IMAP client (e.g. IDLE notification).
    /// Does NOT call the provider — flags are already changed on the server.
    /// Only updates the local projection.
    ApplyRemoteFlagChange {
        mailbox_id: MailboxId,
        provider_uid: ProviderUid,
        new_flags: Vec<Flag>,
    },
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
    MessageMoved,
    MessageDeleted,
    MessageSent { message_id: MessageId },
    DraftSaved { message_id: MessageId },
    DraftDiscarded,
    RemoteFlagApplied,
}
