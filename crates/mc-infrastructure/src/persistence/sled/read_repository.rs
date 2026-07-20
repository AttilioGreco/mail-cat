//! Sled-backed read repositories and write repository for Account.
//!
//! # Storage layout
//!
//! Uses sled trees for projected read models:
//!
//! - **`accounts`**: `"{account_id}"` → JSON(AccountSummary)
//! - **`mailboxes`**: `"{mailbox_id}"` → JSON(Mailbox)
//! - **`mailboxes_by_account`**: `"{account_id}:{mailbox_id}"` → `""`  (index)
//! - **`messages`**: `"{message_id}"` → JSON(Message)
//! - **`message_summaries`**: `"{mailbox_id}:{received_at_desc}:{message_id}"` → JSON(MessageSummary)
//! - **`messages_by_thread`**: `"{in_reply_to}:{message_id}"` → `""` (index)
//! - **`projection_checkpoints`**: `"{projection_name}"` → `"{global_position}"`

use async_trait::async_trait;
use sled::Db;
use std::sync::Arc;

use tracing::debug;

use mc_domain::{
    Account, AccountId, AccountReadRepository, AccountStatus, AccountSummary,
    AccountWriteRepository, AggregateType, BackfillEntry, BodyBackfillQueue, Contact, ContactId,
    ContactReadRepository, ContactSummary, ContactWriteRepository, DomainError, DomainEvent,
    EmailAddress, EventEnvelope, EventStore, Flag, Mailbox, MailboxId, MailboxReadRepository,
    Message, MessageId, MessageReadRepository, MessageSummary, ProjectionCheckpoint, ProviderUid,
    ReadModelProjector,
};

/// Value stored in `BODY_BACKFILL_TREE`; the key already carries account + time.
#[derive(serde::Serialize, serde::Deserialize)]
struct BackfillValue {
    message_id: MessageId,
    mailbox_id: MailboxId,
    provider_uid: u32,
}
use mc_shared::pagination::{PagedResult, Pagination};

const ACCOUNTS_TREE: &str = "accounts";
const MAILBOXES_TREE: &str = "mailboxes";
const MAILBOXES_BY_ACCOUNT_TREE: &str = "mailboxes_by_account";
const MESSAGES_TREE: &str = "messages";
const MESSAGE_SUMMARIES_TREE: &str = "message_summaries";
const PROJECTION_CHECKPOINTS_TREE: &str = "projection_checkpoints";
/// Bodies awaiting download, keyed `{account}:{inverted_received_at}:{message_id}`
/// so a prefix scan yields newest-first for free.
const BODY_BACKFILL_TREE: &str = "body_backfill";
/// `"{contact_id}"` → JSON(ContactSummary)
const CONTACTS_TREE: &str = "contacts";
/// `"{email_address}:{contact_id}"` → `""` — secondary index for fast email lookup
const CONTACTS_BY_EMAIL_TREE: &str = "contacts_by_email";
/// `"{account_id}:{thread_id}"` → JSON(ConversationSummary)
const CONVERSATIONS_TREE: &str = "conversations";
/// `"{message_id}"` → `"{account_id}\x1f{thread_id}"` — index for flag-change updates
const MESSAGE_TO_THREAD_TREE: &str = "message_to_thread";
/// `"{account_id}:{rfc_message_id}"` → `""` — deduplication index: skip conversation count for cross-mailbox copies
const RFC_ID_SEEN_TREE: &str = "rfc_id_seen";
/// `"{account_id}:{thread_id}:{message_id}"` → `""` — reverse index: thread → all domain message_ids
const THREAD_MESSAGES_TREE: &str = "thread_messages";

/// Sled-backed read/write repository.
///
/// Implements all repository traits against a single sled database.
/// Read models are projected from events; the write repository for Account
/// delegates to the EventStore internally.
pub struct SledRepository<ES: EventStore> {
    db: Db,
    event_store: std::sync::Arc<ES>,
}

impl<ES: EventStore> Clone for SledRepository<ES> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            event_store: self.event_store.clone(),
        }
    }
}

impl<ES: EventStore> SledRepository<ES> {
    pub fn new(db: Db, event_store: std::sync::Arc<ES>) -> Self {
        Self { db, event_store }
    }

    fn tree(&self, name: &str) -> Result<sled::Tree, DomainError> {
        self.db
            .open_tree(name)
            .map_err(|e| DomainError::Repository(e.to_string()))
    }
}

// ── AccountWriteRepository ─────────────────────────────────────────────

#[async_trait]
impl<ES: EventStore> AccountWriteRepository for SledRepository<ES> {
    async fn save(&self, account: &Account, events: Vec<DomainEvent>) -> Result<(), DomainError> {
        // Determine expected sequence: load current from event store.
        let current_events = self
            .event_store
            .load_events(&account.id.to_string(), 0)
            .await?;
        let expected_seq = current_events.len() as u64;

        self.event_store
            .append(
                &account.id.to_string(),
                mc_domain::AggregateType::Account,
                expected_seq,
                events,
            )
            .await?;

        // Also update the read model (AccountSummary).
        let summary = AccountSummary {
            id: account.id,
            name: account.name.clone(),
            email: account.email.clone(),
            provider_type: account.provider_type,
            status: account.status,
            total_unread: 0,
        };
        let tree = self.tree(ACCOUNTS_TREE)?;
        let json =
            serde_json::to_vec(&summary).map_err(|e| DomainError::Serialization(e.to_string()))?;
        tree.insert(account.id.to_string().as_bytes(), json)
            .map_err(|e| DomainError::Repository(e.to_string()))?;

        Ok(())
    }

    async fn load(&self, id: &AccountId) -> Result<Option<Account>, DomainError> {
        let events = self.event_store.load_events(&id.to_string(), 0).await?;

        if events.is_empty() {
            return Ok(None);
        }

        let domain_events: Vec<DomainEvent> = events.into_iter().map(|e| e.payload).collect();
        Ok(Account::rehydrate(&domain_events))
    }
}

// ── AccountReadRepository ──────────────────────────────────────────────

#[async_trait]
impl<ES: EventStore> AccountReadRepository for SledRepository<ES> {
    async fn get_account(&self, id: &AccountId) -> Result<Option<AccountSummary>, DomainError> {
        let tree = self.tree(ACCOUNTS_TREE)?;
        let val = tree
            .get(id.to_string().as_bytes())
            .map_err(|e| DomainError::Repository(e.to_string()))?;

        match val {
            Some(bytes) => {
                let summary: AccountSummary = serde_json::from_slice(&bytes)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                Ok(Some(summary))
            }
            None => Ok(None),
        }
    }

    async fn list_accounts(&self) -> Result<Vec<AccountSummary>, DomainError> {
        let tree = self.tree(ACCOUNTS_TREE)?;
        let mut accounts = Vec::new();

        for item in tree.iter() {
            let (_key, value) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let summary: AccountSummary = serde_json::from_slice(&value)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
            accounts.push(summary);
        }

        Ok(accounts)
    }
}

// ── MailboxReadRepository ──────────────────────────────────────────────

#[async_trait]
impl<ES: EventStore> MailboxReadRepository for SledRepository<ES> {
    async fn get_mailbox(&self, id: &MailboxId) -> Result<Option<Mailbox>, DomainError> {
        let tree = self.tree(MAILBOXES_TREE)?;
        let val = tree
            .get(id.to_string().as_bytes())
            .map_err(|e| DomainError::Repository(e.to_string()))?;

        match val {
            Some(bytes) => {
                let mailbox: Mailbox = serde_json::from_slice(&bytes)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                Ok(Some(mailbox))
            }
            None => Ok(None),
        }
    }

    async fn list_mailboxes(&self, account_id: &AccountId) -> Result<Vec<Mailbox>, DomainError> {
        let index_tree = self.tree(MAILBOXES_BY_ACCOUNT_TREE)?;
        let mailbox_tree = self.tree(MAILBOXES_TREE)?;
        let prefix = format!("{}:", account_id);

        let mut mailboxes = Vec::new();
        for item in index_tree.scan_prefix(prefix.as_bytes()) {
            let (key, _) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let key_str =
                std::str::from_utf8(&key).map_err(|e| DomainError::Repository(e.to_string()))?;

            // Key format: "{account_id}:{mailbox_id}"
            if let Some(mailbox_id) = key_str.strip_prefix(&prefix)
                && let Some(val) = mailbox_tree
                    .get(mailbox_id.as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
            {
                let mailbox: Mailbox = serde_json::from_slice(&val)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                mailboxes.push(mailbox);
            }
        }

        Ok(mailboxes)
    }
}

// ── MessageReadRepository ──────────────────────────────────────────────

#[async_trait]
impl<ES: EventStore> MessageReadRepository for SledRepository<ES> {
    async fn get_message(&self, id: &MessageId) -> Result<Option<Message>, DomainError> {
        let tree = self.tree(MESSAGES_TREE)?;
        let val = tree
            .get(id.to_string().as_bytes())
            .map_err(|e| DomainError::Repository(e.to_string()))?;

        match val {
            Some(bytes) => {
                let message: Message = serde_json::from_slice(&bytes)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                Ok(Some(message))
            }
            None => Ok(None),
        }
    }

    async fn list_messages(
        &self,
        mailbox_id: &MailboxId,
        pagination: &Pagination,
    ) -> Result<PagedResult<MessageSummary>, DomainError> {
        let tree = self.tree(MESSAGE_SUMMARIES_TREE)?;
        let prefix = format!("{}:", mailbox_id);

        let mut all_summaries = Vec::new();
        for item in tree.scan_prefix(prefix.as_bytes()) {
            let (_key, value) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let summary: MessageSummary = serde_json::from_slice(&value)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
            all_summaries.push(summary);
        }

        let total_count = all_summaries.len() as u64;
        let offset = pagination.offset() as usize;
        let page_size = pagination.page_size as usize;

        let items: Vec<MessageSummary> = all_summaries
            .into_iter()
            .skip(offset)
            .take(page_size)
            .collect();

        Ok(PagedResult {
            items,
            total_count,
            page: pagination.page,
            page_size: pagination.page_size,
        })
    }

    async fn get_thread(&self, message_id: &MessageId) -> Result<Vec<Message>, DomainError> {
        // 1. Resolve account_id + thread_id from the message-to-thread index.
        let msg_thread_tree = self.tree(MESSAGE_TO_THREAD_TREE)?;
        let Some(val_bytes) = msg_thread_tree
            .get(message_id.to_string().as_bytes())
            .map_err(|e| DomainError::Repository(e.to_string()))?
        else {
            // Not indexed yet — fall back to single-message view.
            return Ok(self.get_message(message_id).await?.into_iter().collect());
        };
        let val = String::from_utf8_lossy(&val_bytes);
        let mut parts = val.splitn(2, '\x1f');
        let account_id = parts.next().unwrap_or_default();
        let thread_id = parts.next().unwrap_or_default();

        // 2. Scan THREAD_MESSAGES_TREE for all message IDs in this thread.
        let thread_tree = self.tree(THREAD_MESSAGES_TREE)?;
        let prefix = format!("{}:{}:", account_id, thread_id);
        let mut messages: Vec<Message> = Vec::new();
        for item in thread_tree.scan_prefix(prefix.as_bytes()) {
            let (key, _) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);
            // Key format: "{account_id}:{thread_id}:{msg_id}"
            if let Some(msg_id_str) = key_str.splitn(3, ':').nth(2) {
                let mid = uuid::Uuid::parse_str(msg_id_str)
                    .map(MessageId::from_uuid)
                    .map_err(|e| DomainError::Repository(format!("bad message id in thread index: {e}")))?;
                if let Some(msg) = self.get_message(&mid).await? {
                    messages.push(msg);
                }
            }
        }

        // 3. Sort by received_at ascending (oldest first, newest last — Gmail style).
        messages.sort_by_key(|m| m.received_at);
        Ok(messages)
    }

    async fn find_by_provider_uid(
        &self,
        mailbox_id: &MailboxId,
        provider_uid: u32,
    ) -> Result<Option<MessageSummary>, DomainError> {
        let tree = self.tree(MESSAGE_SUMMARIES_TREE)?;
        let prefix = format!("{}:", mailbox_id);
        for item in tree.scan_prefix(prefix.as_bytes()) {
            let (_key, value) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let summary: MessageSummary = serde_json::from_slice(&value)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
            if summary.provider_uid == Some(ProviderUid::new(provider_uid)) {
                return Ok(Some(summary));
            }
        }
        Ok(None)
    }

    async fn scan_all_messages(
        &self,
        account_id: Option<&AccountId>,
    ) -> Result<Vec<mc_domain::Message>, DomainError> {
        let tree = self.tree(MESSAGES_TREE)?;
        let mut messages = Vec::new();
        for item in tree.iter() {
            let (_key, value) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let msg: mc_domain::Message = serde_json::from_slice(&value)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
            if let Some(aid) = account_id
                && &msg.account_id != aid {
                    continue;
                }
            messages.push(msg);
        }
        Ok(messages)
    }
}

// ── ConversationReadRepository ────────────────────────────────────────

#[async_trait]
impl<ES: EventStore> mc_domain::traits::repository::ConversationReadRepository
    for SledRepository<ES>
{
    async fn get_conversation(
        &self,
        thread_id: &mc_domain::value_objects::thread_id::ThreadId,
        account_id: &mc_domain::AccountId,
    ) -> Result<Option<mc_domain::traits::repository::ConversationSummary>, DomainError> {
        let key = format!("{}:{}", account_id, thread_id);
        let tree = self.tree(CONVERSATIONS_TREE)?;
        match tree.get(key.as_bytes()).map_err(|e| DomainError::Repository(e.to_string()))? {
            Some(bytes) => {
                let conv = serde_json::from_slice(&bytes)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                Ok(Some(conv))
            }
            None => Ok(None),
        }
    }

    async fn list_conversations(
        &self,
        account_id: &mc_domain::AccountId,
        mailbox_id: Option<&mc_domain::MailboxId>,
        pagination: &mc_shared::pagination::Pagination,
    ) -> Result<
        mc_shared::pagination::PagedResult<mc_domain::traits::repository::ConversationSummary>,
        DomainError,
    > {
        let mut all: Vec<mc_domain::traits::repository::ConversationSummary> = match mailbox_id {
            None => {
                let tree = self.tree(CONVERSATIONS_TREE)?;
                let prefix = format!("{}:", account_id);
                tree.scan_prefix(prefix.as_bytes())
                    .filter_map(|r| r.ok())
                    .filter_map(|(_, v)| serde_json::from_slice::<mc_domain::traits::repository::ConversationSummary>(&v).ok())
                    .collect()
            }
            Some(mailbox_id) => self.list_conversations_for_mailbox(account_id, mailbox_id)?,
        };

        // Newest first.
        all.sort_by(|a, b| b.last_message_at.cmp(&a.last_message_at));

        let total_count = all.len() as u64;
        let page = pagination.page.saturating_sub(1) as usize;
        let size = pagination.page_size as usize;
        let items = all.into_iter().skip(page * size).take(size).collect();

        Ok(mc_shared::pagination::PagedResult { items, total_count, page: pagination.page, page_size: pagination.page_size })
    }

    async fn rebuild_index(&self) -> Result<u64, DomainError> {
        // 1. Wipe the four conversation/thread trees.
        for tree_name in &[
            CONVERSATIONS_TREE,
            MESSAGE_TO_THREAD_TREE,
            RFC_ID_SEEN_TREE,
            THREAD_MESSAGES_TREE,
        ] {
            let tree = self.tree(tree_name)?;
            tree.clear().map_err(|e| DomainError::Repository(e.to_string()))?;
        }

        // 2. Replay all events from the event store (projection is idempotent).
        // Streamed rather than collected into a Vec first: on a large event
        // store that would hold the entire history in memory at once.
        use futures::StreamExt;
        let mut stream = self.event_store.load_all_events(None, 0);
        let mut total = 0u64;
        while let Some(envelope) = stream.next().await {
            self.project(&envelope?).await?;
            total += 1;
        }

        Ok(total)
    }
}

// ── ReadModelProjector ────────────────────────────────────────────────
//
// Handles all 18 DomainEvent variants, updating the appropriate sled trees.
// Each method is idempotent — safe to call on the same event multiple times.

#[async_trait]
impl<ES: EventStore> ReadModelProjector for SledRepository<ES> {
    async fn project(&self, envelope: &EventEnvelope) -> Result<(), DomainError> {
        match &envelope.payload {
            // ── Account events ──────────────────────────────────────
            DomainEvent::AccountAdded {
                account_id,
                name,
                email,
                provider_type,
            } => {
                let summary = AccountSummary {
                    id: *account_id,
                    name: name.clone(),
                    email: email.clone(),
                    provider_type: *provider_type,
                    status: AccountStatus::Active,
                    total_unread: 0,
                };
                let tree = self.tree(ACCOUNTS_TREE)?;
                let json = serde_json::to_vec(&summary)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                tree.insert(account_id.to_string().as_bytes(), json)
                    .map_err(|e| DomainError::Repository(e.to_string()))?;
                debug!(account_id = %account_id, "projected AccountAdded");
            }

            DomainEvent::AccountUpdated {
                account_id,
                name,
                connection_config: _,
            } => {
                let tree = self.tree(ACCOUNTS_TREE)?;
                if let Some(bytes) = tree
                    .get(account_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let mut summary: AccountSummary = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    if let Some(new_name) = name {
                        summary.name = new_name.clone();
                    }
                    let json = serde_json::to_vec(&summary)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    tree.insert(account_id.to_string().as_bytes(), json)
                        .map_err(|e| DomainError::Repository(e.to_string()))?;
                    debug!(account_id = %account_id, "projected AccountUpdated");
                }
            }

            DomainEvent::AccountRemoved { account_id } => {
                let tree = self.tree(ACCOUNTS_TREE)?;
                tree.remove(account_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?;
                debug!(account_id = %account_id, "projected AccountRemoved");
            }

            DomainEvent::AccountAuthExpired { account_id, .. } => {
                let tree = self.tree(ACCOUNTS_TREE)?;
                if let Some(bytes) = tree
                    .get(account_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let mut summary: AccountSummary = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    summary.status = AccountStatus::NeedsReauth;
                    let json = serde_json::to_vec(&summary)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    tree.insert(account_id.to_string().as_bytes(), json)
                        .map_err(|e| DomainError::Repository(e.to_string()))?;
                    debug!(account_id = %account_id, "projected AccountAuthExpired");
                }
            }

            DomainEvent::AccountReauthenticated { account_id } => {
                let tree = self.tree(ACCOUNTS_TREE)?;
                if let Some(bytes) = tree
                    .get(account_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let mut summary: AccountSummary = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    summary.status = AccountStatus::Active;
                    let json = serde_json::to_vec(&summary)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    tree.insert(account_id.to_string().as_bytes(), json)
                        .map_err(|e| DomainError::Repository(e.to_string()))?;
                    debug!(account_id = %account_id, "projected AccountReauthenticated");
                }
            }

            DomainEvent::AccountSyncStarted { .. }
            | DomainEvent::AccountSyncCompleted { .. }
            | DomainEvent::AccountSyncFailed { .. } => {
                // Sync events don't update read models (tracked for audit).
            }

            // ── Mailbox events ──────────────────────────────────────
            DomainEvent::MailboxDiscovered {
                mailbox_id,
                account_id,
                path,
                mailbox_type,
            } => {
                let mailbox = Mailbox {
                    id: *mailbox_id,
                    account_id: *account_id,
                    path: path.clone(),
                    display_name: path.name().to_string(),
                    mailbox_type: *mailbox_type,
                    flags: Vec::new(),
                    total_messages: 0,
                    unread_messages: 0,
                    uid_validity: None,
                    uid_next: None,
                };
                let mailbox_tree = self.tree(MAILBOXES_TREE)?;
                let json = serde_json::to_vec(&mailbox)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                mailbox_tree
                    .insert(mailbox_id.to_string().as_bytes(), json)
                    .map_err(|e| DomainError::Repository(e.to_string()))?;

                // Index: account → mailbox.
                let index_tree = self.tree(MAILBOXES_BY_ACCOUNT_TREE)?;
                let index_key = format!("{}:{}", account_id, mailbox_id);
                index_tree
                    .insert(index_key.as_bytes(), b"")
                    .map_err(|e| DomainError::Repository(e.to_string()))?;

                debug!(mailbox_id = %mailbox_id, "projected MailboxDiscovered");
            }

            DomainEvent::MailboxCountsUpdated {
                mailbox_id,
                total_messages,
                unread_messages,
            } => {
                let tree = self.tree(MAILBOXES_TREE)?;
                if let Some(bytes) = tree
                    .get(mailbox_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let mut mailbox: Mailbox = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    mailbox.total_messages = *total_messages;
                    mailbox.unread_messages = *unread_messages;
                    let json = serde_json::to_vec(&mailbox)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    tree.insert(mailbox_id.to_string().as_bytes(), json)
                        .map_err(|e| DomainError::Repository(e.to_string()))?;
                    debug!(mailbox_id = %mailbox_id, "projected MailboxCountsUpdated");
                }
            }

            DomainEvent::MailboxRenamed {
                mailbox_id,
                new_path,
                ..
            } => {
                let tree = self.tree(MAILBOXES_TREE)?;
                if let Some(bytes) = tree
                    .get(mailbox_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let mut mailbox: Mailbox = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    mailbox.path = new_path.clone();
                    mailbox.display_name = new_path.name().to_string();
                    let json = serde_json::to_vec(&mailbox)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    tree.insert(mailbox_id.to_string().as_bytes(), json)
                        .map_err(|e| DomainError::Repository(e.to_string()))?;
                    debug!(mailbox_id = %mailbox_id, "projected MailboxRenamed");
                }
            }

            DomainEvent::MailboxDeleted {
                mailbox_id,
                account_id,
            } => {
                let mailbox_tree = self.tree(MAILBOXES_TREE)?;
                mailbox_tree
                    .remove(mailbox_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?;

                let index_tree = self.tree(MAILBOXES_BY_ACCOUNT_TREE)?;
                let index_key = format!("{}:{}", account_id, mailbox_id);
                index_tree
                    .remove(index_key.as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?;

                debug!(mailbox_id = %mailbox_id, "projected MailboxDeleted");
            }

            // ── Message events ──────────────────────────────────────
            DomainEvent::MessageReceived {
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
            } => {
                // Full message in messages tree.
                let message = Message {
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
                let msg_tree = self.tree(MESSAGES_TREE)?;
                let msg_json = serde_json::to_vec(&message)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                msg_tree
                    .insert(message_id.to_string().as_bytes(), msg_json)
                    .map_err(|e| DomainError::Repository(e.to_string()))?;

                // Summary in message_summaries tree.
                let from_addr = envelope.from.first();
                let from_name = from_addr.and_then(|a| a.name.clone());
                let from_email = from_addr
                    .map(|a| a.email.to_string())
                    .unwrap_or_default();
                let summary = MessageSummary {
                    id: *message_id,
                    account_id: *account_id,
                    mailbox_id: *mailbox_id,
                    provider_uid: Some(*provider_uid),
                    from_name,
                    from_email,
                    to: envelope.to.iter().map(|a| a.email.to_string()).collect(),
                    cc: envelope.cc.iter().map(|a| a.email.to_string()).collect(),
                    subject: envelope.subject.clone(),
                    flags: flags.clone(),
                    size_bytes: *size_bytes,
                    has_attachments: *has_attachments,
                    received_at: *received_at,
                };
                let summary_tree = self.tree(MESSAGE_SUMMARIES_TREE)?;
                let summary_key = Self::message_summary_key(mailbox_id, received_at, message_id);
                let summary_json = serde_json::to_vec(&summary)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                summary_tree
                    .insert(summary_key.as_bytes(), summary_json)
                    .map_err(|e| DomainError::Repository(e.to_string()))?;

                // Enqueue for background body backfill (drained newest-first).
                // Dequeued by MessageBodyFetched — so on replay this rebuilds to
                // exactly "received but body not yet fetched".
                let backfill_tree = self.tree(BODY_BACKFILL_TREE)?;
                let backfill_key = Self::backfill_key(account_id, received_at, message_id);
                let backfill_val = serde_json::to_vec(&BackfillValue {
                    message_id: *message_id,
                    mailbox_id: *mailbox_id,
                    provider_uid: provider_uid.value(),
                })
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
                backfill_tree
                    .insert(backfill_key.as_bytes(), backfill_val)
                    .map_err(|e| DomainError::Repository(e.to_string()))?;

                debug!(message_id = %message_id, "projected MessageReceived");

                // ── Conversation (thread) projection ──────────────────

                // Idempotency: if this domain message_id is already indexed, skip entirely.
                let msg_thread_tree = self.tree(MESSAGE_TO_THREAD_TREE)?;
                if msg_thread_tree
                    .contains_key(message_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    debug!(message_id = %message_id, "conversation projection: already projected, skipping");
                } else {
                    let thread_id = mc_domain::model::conversation::Conversation::resolve_thread_id(
                        message_rfc_id.as_deref(),
                        in_reply_to.as_deref(),
                        references,
                        &message_id.to_string(),
                    );
                    let subject = mc_domain::model::conversation::Conversation::normalize_subject(
                        &envelope.subject,
                    );
                    let latest_sender = envelope.from.first().map(sender_display).unwrap_or_default();
                    let is_unread = !flags.contains(&Flag::Seen);

                    // Cross-mailbox dedup: same RFC Message-ID = same canonical message.
                    // Don't count it again if we've already seen this RFC ID for this account.
                    let rfc_seen_tree = self.tree(RFC_ID_SEEN_TREE)?;
                    let is_rfc_duplicate = if let Some(rfc_id) = message_rfc_id.as_deref() {
                        let rfc_key = format!("{}:{}", account_id, rfc_id);
                        let already = rfc_seen_tree
                            .contains_key(rfc_key.as_bytes())
                            .map_err(|e| DomainError::Repository(e.to_string()))?;
                        if !already {
                            rfc_seen_tree
                                .insert(rfc_key.as_bytes(), b"")
                                .map_err(|e| DomainError::Repository(e.to_string()))?;
                        }
                        already
                    } else {
                        false
                    };

                    let conv_key = format!("{}:{}", account_id, thread_id);
                    let conv_tree = self.tree(CONVERSATIONS_TREE)?;

                    let mut conv: mc_domain::traits::repository::ConversationSummary =
                        if let Some(bytes) = conv_tree
                            .get(conv_key.as_bytes())
                            .map_err(|e| DomainError::Repository(e.to_string()))?
                        {
                            serde_json::from_slice(&bytes)
                                .map_err(|e| DomainError::Serialization(e.to_string()))?
                        } else {
                            mc_domain::traits::repository::ConversationSummary {
                                thread_id: thread_id.clone(),
                                account_id: *account_id,
                                subject,
                                message_count: 0,
                                unread_count: 0,
                                last_message_at: *received_at,
                                latest_sender: latest_sender.clone(),
                                sample_message_id: *message_id,
                            }
                        };

                    if !is_rfc_duplicate {
                        conv.message_count += 1;
                        if is_unread {
                            conv.unread_count += 1;
                        }
                    }
                    if *received_at > conv.last_message_at {
                        conv.last_message_at = *received_at;
                        conv.latest_sender = latest_sender;
                        // Keep sample_message_id pointing to the newest message so
                        // get_thread lookups work even after the thread root is gone.
                        conv.sample_message_id = *message_id;
                    }

                    // Index: domain message_id → "{account_id}\x1f{thread_id}"
                    let msg_thread_val = format!("{}\x1f{}", account_id, thread_id);
                    msg_thread_tree
                        .insert(message_id.to_string().as_bytes(), msg_thread_val.as_bytes())
                        .map_err(|e| DomainError::Repository(e.to_string()))?;

                    // Reverse index: "{account_id}:{thread_id}:{message_id}" → ""
                    let thread_messages_tree = self.tree(THREAD_MESSAGES_TREE)?;
                    let thread_msg_key = format!("{}:{}:{}", account_id, thread_id, message_id);
                    thread_messages_tree
                        .insert(thread_msg_key.as_bytes(), b"")
                        .map_err(|e| DomainError::Repository(e.to_string()))?;

                    let conv_json = serde_json::to_vec(&conv)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    conv_tree
                        .insert(conv_key.as_bytes(), conv_json)
                        .map_err(|e| DomainError::Repository(e.to_string()))?;

                    debug!(thread_id = %thread_id, rfc_duplicate = is_rfc_duplicate, "projected conversation upsert");
                }
            }

            DomainEvent::MessageBodyFetched {
                message_id,
                body_text,
                body_html,
                attachments,
            } => {
                let tree = self.tree(MESSAGES_TREE)?;
                if let Some(bytes) = tree
                    .get(message_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let mut message: Message = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    message.body_text = body_text.clone();
                    message.body_html = body_html.clone();
                    message.attachments = attachments.clone();
                    message.has_attachments = !attachments.is_empty();
                    let json = serde_json::to_vec(&message)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    tree.insert(message_id.to_string().as_bytes(), json)
                        .map_err(|e| DomainError::Repository(e.to_string()))?;

                    // Drain from the backfill queue — body is now present.
                    let backfill_tree = self.tree(BODY_BACKFILL_TREE)?;
                    let backfill_key =
                        Self::backfill_key(&message.account_id, &message.received_at, message_id);
                    backfill_tree
                        .remove(backfill_key.as_bytes())
                        .map_err(|e| DomainError::Repository(e.to_string()))?;

                    debug!(message_id = %message_id, "projected MessageBodyFetched");
                }
            }

            DomainEvent::MessageFlagged {
                message_id,
                mailbox_id: _,
                flags_added,
                flags_removed,
            } => {
                // Update full message.
                let msg_tree = self.tree(MESSAGES_TREE)?;
                if let Some(bytes) = msg_tree
                    .get(message_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let mut message: Message = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    apply_flag_changes(&mut message.flags, flags_added, flags_removed);
                    let json = serde_json::to_vec(&message)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    msg_tree
                        .insert(message_id.to_string().as_bytes(), json)
                        .map_err(|e| DomainError::Repository(e.to_string()))?;

                    // Update summary flags too (scan for matching summary).
                    self.update_summary_flags(message_id, flags_added, flags_removed)?;
                    debug!(message_id = %message_id, "projected MessageFlagged");

                    // Update conversation unread_count if Seen flag changed.
                    let added_seen = flags_added.contains(&Flag::Seen);
                    let removed_seen = flags_removed.contains(&Flag::Seen);
                    if added_seen || removed_seen {
                        let msg_thread_tree = self.tree(MESSAGE_TO_THREAD_TREE)?;
                        if let Some(val_bytes) = msg_thread_tree
                            .get(message_id.to_string().as_bytes())
                            .map_err(|e| DomainError::Repository(e.to_string()))?
                        {
                            let val = String::from_utf8_lossy(&val_bytes);
                            if let Some((account_str, thread_str)) = val.split_once('\x1f') {
                                let conv_key = format!("{}:{}", account_str, thread_str);
                                let conv_tree = self.tree(CONVERSATIONS_TREE)?;
                                if let Some(bytes) = conv_tree
                                    .get(conv_key.as_bytes())
                                    .map_err(|e| DomainError::Repository(e.to_string()))?
                                {
                                    let mut conv: mc_domain::traits::repository::ConversationSummary =
                                        serde_json::from_slice(&bytes)
                                            .map_err(|e| DomainError::Serialization(e.to_string()))?;
                                    if added_seen && conv.unread_count > 0 {
                                        conv.unread_count -= 1;
                                    } else if removed_seen {
                                        conv.unread_count += 1;
                                    }
                                    let json = serde_json::to_vec(&conv)
                                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                                    conv_tree
                                        .insert(conv_key.as_bytes(), json)
                                        .map_err(|e| DomainError::Repository(e.to_string()))?;
                                }
                            }
                        }
                    }
                }
            }

            DomainEvent::MessageMoved {
                message_id,
                from_mailbox_id: _,
                to_mailbox_id,
            } => {
                let msg_tree = self.tree(MESSAGES_TREE)?;
                if let Some(bytes) = msg_tree
                    .get(message_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let mut message: Message = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    let old_mailbox_id = message.mailbox_id;
                    message.mailbox_id = *to_mailbox_id;
                    let json = serde_json::to_vec(&message)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    msg_tree
                        .insert(message_id.to_string().as_bytes(), json)
                        .map_err(|e| DomainError::Repository(e.to_string()))?;

                    // Re-index summary: remove old key, insert under new mailbox.
                    self.move_message_summary(
                        message_id,
                        &old_mailbox_id,
                        to_mailbox_id,
                        &message.received_at,
                    )?;
                    debug!(message_id = %message_id, "projected MessageMoved");
                }
            }

            DomainEvent::MessageDeleted {
                message_id,
                mailbox_id: _,
                permanently: _,
            } => {
                // Remove full message.
                let msg_tree = self.tree(MESSAGES_TREE)?;
                let maybe_bytes = msg_tree
                    .get(message_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?;
                msg_tree
                    .remove(message_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?;

                // Remove summary — need received_at for the key.
                if let Some(bytes) = maybe_bytes {
                    let message: Message = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    let summary_tree = self.tree(MESSAGE_SUMMARIES_TREE)?;
                    let key = Self::message_summary_key(
                        &message.mailbox_id,
                        &message.received_at,
                        message_id,
                    );
                    summary_tree
                        .remove(key.as_bytes())
                        .map_err(|e| DomainError::Repository(e.to_string()))?;

                    // Clean the conversation/thread read model too — otherwise the
                    // message lingers in ThreadView (get_thread), in the account-wide
                    // conversation list, and skews unread/message counts.
                    let was_unread = !message.flags.contains(&Flag::Seen);
                    self.remove_message_from_conversation(message_id, was_unread)?;

                    // Drop the cross-mailbox dedup marker so that if this same RFC
                    // message reappears elsewhere (e.g. moved into Trash) it is
                    // counted again instead of being silently ignored.
                    if let Some(rfc_id) = &message.message_rfc_id {
                        let rfc_seen_tree = self.tree(RFC_ID_SEEN_TREE)?;
                        let rfc_key = format!("{}:{}", message.account_id, rfc_id);
                        rfc_seen_tree
                            .remove(rfc_key.as_bytes())
                            .map_err(|e| DomainError::Repository(e.to_string()))?;
                    }
                }
                debug!(message_id = %message_id, "projected MessageDeleted");
            }

            DomainEvent::MessageSent {
                message_id,
                account_id,
                mailbox_id,
                envelope,
                in_reply_to,
                references,
                body_text,
                body_html,
                sent_at,
            } => {
                let message = Message {
                    id: *message_id,
                    account_id: *account_id,
                    mailbox_id: *mailbox_id,
                    provider_uid: None,
                    message_rfc_id: None,
                    in_reply_to: in_reply_to.clone(),
                    references: references.clone(),
                    envelope: envelope.clone(),
                    flags: vec![Flag::Seen],
                    size_bytes: 0,
                    has_attachments: false,
                    attachments: Vec::new(),
                    body_text: body_text.clone(),
                    body_html: body_html.clone(),
                    received_at: *sent_at,
                    internal_date: *sent_at,
                };
                let tree = self.tree(MESSAGES_TREE)?;
                let json = serde_json::to_vec(&message)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                tree.insert(message_id.to_string().as_bytes(), json)
                    .map_err(|e| DomainError::Repository(e.to_string()))?;

                // Summary in message_summaries tree — without this, a just-sent
                // message is invisible in the plain (non-conversation) message
                // list until the next IMAP sync rediscovers it.
                let from_addr = envelope.from.first();
                let from_name = from_addr.and_then(|a| a.name.clone());
                let from_email = from_addr.map(|a| a.email.to_string()).unwrap_or_default();
                let summary = MessageSummary {
                    id: *message_id,
                    account_id: *account_id,
                    mailbox_id: *mailbox_id,
                    provider_uid: None,
                    from_name,
                    from_email,
                    to: envelope.to.iter().map(|a| a.email.to_string()).collect(),
                    cc: envelope.cc.iter().map(|a| a.email.to_string()).collect(),
                    subject: envelope.subject.clone(),
                    flags: vec![Flag::Seen],
                    size_bytes: 0,
                    has_attachments: false,
                    received_at: *sent_at,
                };
                let summary_tree = self.tree(MESSAGE_SUMMARIES_TREE)?;
                let summary_key = Self::message_summary_key(mailbox_id, sent_at, message_id);
                let summary_json = serde_json::to_vec(&summary)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                summary_tree
                    .insert(summary_key.as_bytes(), summary_json)
                    .map_err(|e| DomainError::Repository(e.to_string()))?;

                // ── Thread the sent message (idempotent) ──────────────────────
                let msg_thread_tree = self.tree(MESSAGE_TO_THREAD_TREE)?;
                if !msg_thread_tree
                    .contains_key(message_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let thread_id =
                        mc_domain::model::conversation::Conversation::resolve_thread_id(
                            None, // RFC ID assigned by SMTP server, unknown at send time
                            in_reply_to.as_deref(),
                            &references,
                            &message_id.to_string(),
                        );
                    let subject = mc_domain::model::conversation::Conversation::normalize_subject(
                        &envelope.subject,
                    );
                    let latest_sender = envelope.from.first().map(sender_display).unwrap_or_default();

                    let conv_key = format!("{}:{}", account_id, thread_id);
                    let conv_tree = self.tree(CONVERSATIONS_TREE)?;
                    let mut conv: mc_domain::traits::repository::ConversationSummary =
                        if let Some(bytes) = conv_tree
                            .get(conv_key.as_bytes())
                            .map_err(|e| DomainError::Repository(e.to_string()))?
                        {
                            serde_json::from_slice(&bytes)
                                .map_err(|e| DomainError::Serialization(e.to_string()))?
                        } else {
                            mc_domain::traits::repository::ConversationSummary {
                                thread_id: thread_id.clone(),
                                account_id: account_id.clone(),
                                subject,
                                message_count: 0,
                                unread_count: 0,
                                last_message_at: *sent_at,
                                latest_sender: latest_sender.clone(),
                                sample_message_id: message_id.clone(),
                            }
                        };

                    conv.message_count += 1;
                    // Sent messages are always Seen — unread_count unchanged
                    if *sent_at >= conv.last_message_at {
                        conv.last_message_at = *sent_at;
                        conv.latest_sender = latest_sender;
                        conv.sample_message_id = message_id.clone();
                    }

                    let msg_thread_val = format!("{}\x1f{}", account_id, thread_id);
                    msg_thread_tree
                        .insert(message_id.to_string().as_bytes(), msg_thread_val.as_bytes())
                        .map_err(|e| DomainError::Repository(e.to_string()))?;

                    let thread_messages_tree = self.tree(THREAD_MESSAGES_TREE)?;
                    let thread_msg_key =
                        format!("{}:{}:{}", account_id, thread_id, message_id);
                    thread_messages_tree
                        .insert(thread_msg_key.as_bytes(), b"")
                        .map_err(|e| DomainError::Repository(e.to_string()))?;

                    let conv_json = serde_json::to_vec(&conv)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    conv_tree
                        .insert(conv_key.as_bytes(), conv_json)
                        .map_err(|e| DomainError::Repository(e.to_string()))?;
                }
                debug!(message_id = %message_id, "projected MessageSent");
            }

            DomainEvent::DraftSaved {
                message_id,
                account_id,
                envelope,
                body_text,
                body_html,
            } => {
                let message = Message {
                    id: *message_id,
                    account_id: *account_id,
                    mailbox_id: MailboxId::new(), // Drafts folder assigned during sync
                    provider_uid: None,
                    message_rfc_id: None,
                    in_reply_to: None,
                    references: Vec::new(),
                    envelope: envelope.clone(),
                    flags: vec![Flag::Draft],
                    size_bytes: 0,
                    has_attachments: false,
                    attachments: Vec::new(),
                    body_text: body_text.clone(),
                    body_html: body_html.clone(),
                    received_at: chrono::Utc::now(),
                    internal_date: chrono::Utc::now(),
                };
                let tree = self.tree(MESSAGES_TREE)?;
                let json = serde_json::to_vec(&message)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                tree.insert(message_id.to_string().as_bytes(), json)
                    .map_err(|e| DomainError::Repository(e.to_string()))?;
                debug!(message_id = %message_id, "projected DraftSaved");
            }

            DomainEvent::DraftDiscarded { message_id } => {
                let tree = self.tree(MESSAGES_TREE)?;
                tree.remove(message_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?;
                debug!(message_id = %message_id, "projected DraftDiscarded");
            }

            // ── Contact events ──────────────────────────────────────
            DomainEvent::ContactAdded {
                contact_id,
                display_name,
                emails,
                source,
                external_id,
                account_scope,
            } => {
                let primary_email = emails
                    .iter()
                    .find(|e| e.is_primary)
                    .or_else(|| emails.first())
                    .map(|e| e.address.clone())
                    .unwrap_or_else(|| EmailAddress::new("unknown@unknown").unwrap());

                let summary = ContactSummary {
                    id: *contact_id,
                    display_name: display_name.clone(),
                    primary_email: primary_email.clone(),
                    emails: emails.clone(),
                    organization: None,
                    source: source.clone(),
                    account_scope: *account_scope,
                };
                let tree = self.tree(CONTACTS_TREE)?;
                let json = serde_json::to_vec(&summary)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                tree.insert(contact_id.to_string().as_bytes(), json)
                    .map_err(|e| DomainError::Repository(e.to_string()))?;

                // Index: email → contact
                let index_tree = self.tree(CONTACTS_BY_EMAIL_TREE)?;
                for ce in emails {
                    let key = format!("{}:{}", ce.address.as_str(), contact_id);
                    index_tree
                        .insert(key.as_bytes(), b"")
                        .map_err(|e| DomainError::Repository(e.to_string()))?;
                }

                // Suppress unused warning — external_id not stored in summary but consumed
                let _ = external_id;
                debug!(contact_id = %contact_id, "projected ContactAdded");
            }

            DomainEvent::ContactUpdated {
                contact_id,
                display_name,
                emails,
                organization,
                ..
            } => {
                let tree = self.tree(CONTACTS_TREE)?;
                if let Some(bytes) = tree
                    .get(contact_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let mut summary: ContactSummary = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;

                    if let Some(name) = display_name {
                        summary.display_name = name.clone();
                    }
                    if let Some(new_emails) = emails {
                        // Re-index: remove old email index entries, add new
                        let index_tree = self.tree(CONTACTS_BY_EMAIL_TREE)?;
                        for ce in &summary.emails {
                            let key = format!("{}:{}", ce.address.as_str(), contact_id);
                            index_tree
                                .remove(key.as_bytes())
                                .map_err(|e| DomainError::Repository(e.to_string()))?;
                        }
                        for ce in new_emails {
                            let key = format!("{}:{}", ce.address.as_str(), contact_id);
                            index_tree
                                .insert(key.as_bytes(), b"")
                                .map_err(|e| DomainError::Repository(e.to_string()))?;
                        }
                        summary.primary_email = new_emails
                            .iter()
                            .find(|e| e.is_primary)
                            .or_else(|| new_emails.first())
                            .map(|e| e.address.clone())
                            .unwrap_or(summary.primary_email.clone());
                        summary.emails = new_emails.clone();
                    }
                    if let Some(org) = organization {
                        summary.organization = org.clone();
                    }

                    let json = serde_json::to_vec(&summary)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    tree.insert(contact_id.to_string().as_bytes(), json)
                        .map_err(|e| DomainError::Repository(e.to_string()))?;
                    debug!(contact_id = %contact_id, "projected ContactUpdated");
                }
            }

            DomainEvent::ContactRemoved { contact_id } => {
                let tree = self.tree(CONTACTS_TREE)?;
                // Remove email index entries before deleting the summary
                if let Some(bytes) = tree
                    .get(contact_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let summary: ContactSummary = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    let index_tree = self.tree(CONTACTS_BY_EMAIL_TREE)?;
                    for ce in &summary.emails {
                        let key = format!("{}:{}", ce.address.as_str(), contact_id);
                        index_tree
                            .remove(key.as_bytes())
                            .map_err(|e| DomainError::Repository(e.to_string()))?;
                    }
                }
                tree.remove(contact_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?;
                debug!(contact_id = %contact_id, "projected ContactRemoved");
            }

            DomainEvent::ContactMerged { kept_id, discarded_id, merged_emails } => {
                let tree = self.tree(CONTACTS_TREE)?;
                // Update the kept contact's emails in the read model
                if let Some(bytes) = tree
                    .get(kept_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let mut summary: ContactSummary = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;

                    // Re-index: remove old email entries for kept contact
                    let index_tree = self.tree(CONTACTS_BY_EMAIL_TREE)?;
                    for ce in &summary.emails {
                        let key = format!("{}:{}", ce.address.as_str(), kept_id);
                        index_tree
                            .remove(key.as_bytes())
                            .map_err(|e| DomainError::Repository(e.to_string()))?;
                    }

                    summary.emails = merged_emails.clone();
                    summary.primary_email = merged_emails
                        .iter()
                        .find(|e| e.is_primary)
                        .or_else(|| merged_emails.first())
                        .map(|e| e.address.clone())
                        .unwrap_or(summary.primary_email);

                    // Add new email index entries
                    for ce in merged_emails.iter() {
                        let key = format!("{}:{}", ce.address.as_str(), kept_id);
                        index_tree
                            .insert(key.as_bytes(), b"")
                            .map_err(|e| DomainError::Repository(e.to_string()))?;
                    }

                    let json = serde_json::to_vec(&summary)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    tree.insert(kept_id.to_string().as_bytes(), json)
                        .map_err(|e| DomainError::Repository(e.to_string()))?;
                }

                // Remove the discarded contact's email index entries and record
                if let Some(bytes) = tree
                    .get(discarded_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let summary: ContactSummary = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    let index_tree = self.tree(CONTACTS_BY_EMAIL_TREE)?;
                    for ce in &summary.emails {
                        let key = format!("{}:{}", ce.address.as_str(), discarded_id);
                        index_tree
                            .remove(key.as_bytes())
                            .map_err(|e| DomainError::Repository(e.to_string()))?;
                    }
                }
                tree.remove(discarded_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?;

                debug!(kept_id = %kept_id, discarded_id = %discarded_id, "projected ContactMerged");
            }
        }

        Ok(())
    }
}

/// Helper: apply flag additions and removals to a flag vec.
/// Display name for a sender, falling back to the email address when the
/// name is absent OR blank (some providers/clients send an empty-string name
/// rather than omitting it, which would otherwise show as a blank sender).
fn sender_display(addr: &mc_domain::Address) -> String {
    match &addr.name {
        Some(name) if !name.trim().is_empty() => name.clone(),
        _ => addr.email.to_string(),
    }
}

fn apply_flag_changes(flags: &mut Vec<Flag>, added: &[Flag], removed: &[Flag]) {
    for flag in removed {
        flags.retain(|f| f != flag);
    }
    for flag in added {
        if !flags.contains(flag) {
            flags.push(flag.clone());
        }
    }
}

impl<ES: EventStore> SledRepository<ES> {
    /// Key for message_summaries tree: `{mailbox_id}:{received_at_desc}:{message_id}`.
    ///
    /// received_at is inverted (u64::MAX - millis) so lexicographic order = newest first.
    fn message_summary_key(
        mailbox_id: &MailboxId,
        received_at: &chrono::DateTime<chrono::Utc>,
        message_id: &MessageId,
    ) -> String {
        let millis = received_at.timestamp_millis() as u64;
        let inverted = u64::MAX - millis;
        format!("{}:{:016}:{}", mailbox_id, inverted, message_id)
    }

    /// Backfill-queue key: inverted timestamp so a prefix scan is newest-first.
    fn backfill_key(
        account_id: &AccountId,
        received_at: &chrono::DateTime<chrono::Utc>,
        message_id: &MessageId,
    ) -> String {
        let millis = received_at.timestamp_millis() as u64;
        let inverted = u64::MAX - millis;
        format!("{}:{:016}:{}", account_id, inverted, message_id)
    }

    /// Build conversation summaries scoped to a single mailbox (Thunderbird-style
    /// per-folder threading): groups this mailbox's message summaries by thread,
    /// with stats (count, unread, latest sender/date) computed only from the
    /// messages that live in this mailbox — not the whole thread across mailboxes.
    fn list_conversations_for_mailbox(
        &self,
        account_id: &AccountId,
        mailbox_id: &MailboxId,
    ) -> Result<Vec<mc_domain::traits::repository::ConversationSummary>, DomainError> {
        let summary_tree = self.tree(MESSAGE_SUMMARIES_TREE)?;
        let msg_thread_tree = self.tree(MESSAGE_TO_THREAD_TREE)?;
        let prefix = format!("{}:", mailbox_id);

        let mut groups: std::collections::HashMap<String, Vec<MessageSummary>> =
            std::collections::HashMap::new();

        for item in summary_tree.scan_prefix(prefix.as_bytes()) {
            let (_key, value) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let summary: MessageSummary = serde_json::from_slice(&value)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;

            let thread_id = match msg_thread_tree
                .get(summary.id.to_string().as_bytes())
                .map_err(|e| DomainError::Repository(e.to_string()))?
            {
                Some(bytes) => {
                    let val = String::from_utf8_lossy(&bytes);
                    val.splitn(2, '\x1f')
                        .nth(1)
                        .unwrap_or_default()
                        .to_string()
                }
                // Not indexed yet (e.g. just received, projection race) — treat
                // as its own single-message thread rather than dropping it.
                None => summary.id.to_string(),
            };

            groups.entry(thread_id).or_default().push(summary);
        }

        let conversations = groups
            .into_iter()
            .filter_map(|(thread_id, msgs)| {
                let latest = msgs.iter().max_by_key(|m| m.received_at)?;
                let unread_count =
                    msgs.iter().filter(|m| !m.flags.contains(&Flag::Seen)).count() as u32;
                let subject = mc_domain::model::conversation::Conversation::normalize_subject(
                    &latest.subject,
                );
                let latest_sender = match &latest.from_name {
                    Some(name) if !name.trim().is_empty() => name.clone(),
                    _ => latest.from_email.clone(),
                };

                Some(mc_domain::traits::repository::ConversationSummary {
                    thread_id: mc_domain::value_objects::thread_id::ThreadId::new(thread_id),
                    account_id: *account_id,
                    subject,
                    message_count: msgs.len() as u32,
                    unread_count,
                    last_message_at: latest.received_at,
                    latest_sender,
                    sample_message_id: latest.id,
                })
            })
            .collect();

        Ok(conversations)
    }

    /// Remove a message from the conversation/thread read model: drops its
    /// forward and reverse thread-index entries and decrements the conversation
    /// summary counters, deleting the conversation when it becomes empty.
    ///
    /// If the removed message was the conversation's `sample_message_id` (the id
    /// used to open the thread), the sample is repointed to any surviving message
    /// so `get_thread` keeps working.
    fn remove_message_from_conversation(
        &self,
        message_id: &MessageId,
        was_unread: bool,
    ) -> Result<(), DomainError> {
        let msg_thread_tree = self.tree(MESSAGE_TO_THREAD_TREE)?;
        let Some(val_bytes) = msg_thread_tree
            .get(message_id.to_string().as_bytes())
            .map_err(|e| DomainError::Repository(e.to_string()))?
        else {
            return Ok(()); // not threaded (e.g. never projected into a conversation)
        };
        let val = String::from_utf8_lossy(&val_bytes);
        let Some((account_str, thread_str)) = val.split_once('\x1f') else {
            return Ok(());
        };
        let account_str = account_str.to_string();
        let thread_str = thread_str.to_string();

        // Reverse index: "{account}:{thread}:{message_id}" → ""
        let thread_messages_tree = self.tree(THREAD_MESSAGES_TREE)?;
        let thread_msg_key = format!("{}:{}:{}", account_str, thread_str, message_id);
        thread_messages_tree
            .remove(thread_msg_key.as_bytes())
            .map_err(|e| DomainError::Repository(e.to_string()))?;

        // Forward index: message_id → "{account}\x1f{thread}"
        msg_thread_tree
            .remove(message_id.to_string().as_bytes())
            .map_err(|e| DomainError::Repository(e.to_string()))?;

        // Conversation summary counters.
        let conv_key = format!("{}:{}", account_str, thread_str);
        let conv_tree = self.tree(CONVERSATIONS_TREE)?;
        if let Some(bytes) = conv_tree
            .get(conv_key.as_bytes())
            .map_err(|e| DomainError::Repository(e.to_string()))?
        {
            let mut conv: mc_domain::traits::repository::ConversationSummary =
                serde_json::from_slice(&bytes)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
            conv.message_count = conv.message_count.saturating_sub(1);
            if was_unread {
                conv.unread_count = conv.unread_count.saturating_sub(1);
            }

            if conv.message_count == 0 {
                conv_tree
                    .remove(conv_key.as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?;
            } else {
                // Repoint the sample to a surviving message so get_thread works.
                if conv.sample_message_id == *message_id {
                    let prefix = format!("{}:{}:", account_str, thread_str);
                    if let Some(item) = thread_messages_tree.scan_prefix(prefix.as_bytes()).next() {
                        let (key, _) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
                        let key_str = String::from_utf8_lossy(&key);
                        if let Some(mid_str) = key_str.rsplit(':').next()
                            && let Ok(mid) = uuid::Uuid::parse_str(mid_str)
                        {
                            conv.sample_message_id = MessageId::from_uuid(mid);
                        }
                    }
                }
                let json = serde_json::to_vec(&conv)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                conv_tree
                    .insert(conv_key.as_bytes(), json)
                    .map_err(|e| DomainError::Repository(e.to_string()))?;
            }
        }

        Ok(())
    }

    /// Update flag fields in the message summary matching the given message_id.
    fn update_summary_flags(
        &self,
        message_id: &MessageId,
        flags_added: &[Flag],
        flags_removed: &[Flag],
    ) -> Result<(), DomainError> {
        let tree = self.tree(MESSAGE_SUMMARIES_TREE)?;
        // Scan all summaries to find the one for this message.
        // Not optimal for large datasets, but correct. An index by message_id
        // would improve this; acceptable for now.
        for item in tree.iter() {
            let (key, value) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let summary: MessageSummary = serde_json::from_slice(&value)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
            if summary.id == *message_id {
                let mut updated = summary;
                apply_flag_changes(&mut updated.flags, flags_added, flags_removed);
                let json = serde_json::to_vec(&updated)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                tree.insert(key, json)
                    .map_err(|e| DomainError::Repository(e.to_string()))?;
                break;
            }
        }
        Ok(())
    }

    /// Move a message summary from one mailbox key to another.
    fn move_message_summary(
        &self,
        message_id: &MessageId,
        old_mailbox_id: &MailboxId,
        new_mailbox_id: &MailboxId,
        received_at: &chrono::DateTime<chrono::Utc>,
    ) -> Result<(), DomainError> {
        let tree = self.tree(MESSAGE_SUMMARIES_TREE)?;

        let old_key = Self::message_summary_key(old_mailbox_id, received_at, message_id);
        if let Some(bytes) = tree
            .remove(old_key.as_bytes())
            .map_err(|e| DomainError::Repository(e.to_string()))?
        {
            let mut summary: MessageSummary = serde_json::from_slice(&bytes)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
            summary.mailbox_id = *new_mailbox_id;
            let new_key = Self::message_summary_key(new_mailbox_id, received_at, message_id);
            let json = serde_json::to_vec(&summary)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
            tree.insert(new_key.as_bytes(), json)
                .map_err(|e| DomainError::Repository(e.to_string()))?;
        }

        Ok(())
    }
}

// ── BodyBackfillQueue ──────────────────────────────────────────────────

#[async_trait]
impl<ES: EventStore> BodyBackfillQueue for SledRepository<ES> {
    async fn list_pending_backfill(
        &self,
        account_id: &AccountId,
        limit: usize,
    ) -> Result<Vec<BackfillEntry>, DomainError> {
        let tree = self.tree(BODY_BACKFILL_TREE)?;
        let prefix = format!("{}:", account_id);
        let mut out = Vec::new();
        // Keys use an inverted timestamp → prefix scan is newest-first.
        for item in tree.scan_prefix(prefix.as_bytes()).take(limit) {
            let (_key, value) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let v: BackfillValue = serde_json::from_slice(&value)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
            out.push(BackfillEntry {
                message_id: v.message_id,
                mailbox_id: v.mailbox_id,
                provider_uid: v.provider_uid,
            });
        }
        Ok(out)
    }
}

// ── ProjectionCheckpoint ───────────────────────────────────────────────

#[async_trait]
impl<ES: EventStore> ProjectionCheckpoint for SledRepository<ES> {
    async fn get_checkpoint(&self, projection_name: &str) -> Result<u64, DomainError> {
        let tree = self.tree(PROJECTION_CHECKPOINTS_TREE)?;
        let val = tree
            .get(projection_name.as_bytes())
            .map_err(|e| DomainError::Repository(e.to_string()))?;

        match val {
            Some(bytes) => {
                let s = std::str::from_utf8(&bytes)
                    .map_err(|e| DomainError::Repository(e.to_string()))?;
                s.parse::<u64>()
                    .map_err(|e| DomainError::Repository(e.to_string()))
            }
            None => Ok(0),
        }
    }

    async fn save_checkpoint(
        &self,
        projection_name: &str,
        global_position: u64,
    ) -> Result<(), DomainError> {
        let tree = self.tree(PROJECTION_CHECKPOINTS_TREE)?;
        tree.insert(
            projection_name.as_bytes(),
            global_position.to_string().as_bytes(),
        )
        .map_err(|e| DomainError::Repository(e.to_string()))?;
        Ok(())
    }
}

// ── ContactWriteRepository ────────────────────────────────────────────

#[async_trait]
impl<ES: EventStore> ContactWriteRepository for SledRepository<ES> {
    async fn save(&self, contact: &Contact, events: Vec<DomainEvent>) -> Result<(), DomainError> {
        let current_events = self
            .event_store
            .load_events(&contact.id.to_string(), 0)
            .await?;
        let expected_seq = current_events.len() as u64;

        self.event_store
            .append(
                &contact.id.to_string(),
                AggregateType::Contact,
                expected_seq,
                events,
            )
            .await?;
        Ok(())
    }

    async fn load(&self, id: &ContactId) -> Result<Option<Contact>, DomainError> {
        let events = self.event_store.load_events(&id.to_string(), 0).await?;
        if events.is_empty() {
            return Ok(None);
        }
        let domain_events: Vec<DomainEvent> = events.into_iter().map(|e| e.payload).collect();
        Ok(Contact::rehydrate(&domain_events))
    }
}

// ── ContactReadRepository ────────────────────────────────────────────

#[async_trait]
impl<ES: EventStore> ContactReadRepository for SledRepository<ES> {
    async fn get_contact(&self, id: &ContactId) -> Result<Option<ContactSummary>, DomainError> {
        let tree = self.tree(CONTACTS_TREE)?;
        let val = tree
            .get(id.to_string().as_bytes())
            .map_err(|e| DomainError::Repository(e.to_string()))?;
        match val {
            Some(bytes) => {
                let summary: ContactSummary = serde_json::from_slice(&bytes)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                Ok(Some(summary))
            }
            None => Ok(None),
        }
    }

    async fn list_contacts(
        &self,
        pagination: &Pagination,
    ) -> Result<PagedResult<ContactSummary>, DomainError> {
        let tree = self.tree(CONTACTS_TREE)?;
        let mut all: Vec<ContactSummary> = Vec::new();
        for item in tree.iter() {
            let (_k, v) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let s: ContactSummary = serde_json::from_slice(&v)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
            all.push(s);
        }
        let total_count = all.len() as u64;
        let offset = pagination.offset() as usize;
        let page_size = pagination.page_size as usize;
        let items = all.into_iter().skip(offset).take(page_size).collect();
        Ok(PagedResult { items, total_count, page: pagination.page, page_size: pagination.page_size })
    }

    async fn find_by_email(
        &self,
        email: &EmailAddress,
    ) -> Result<Vec<ContactSummary>, DomainError> {
        let index_tree = self.tree(CONTACTS_BY_EMAIL_TREE)?;
        let contact_tree = self.tree(CONTACTS_TREE)?;
        let prefix = format!("{}:", email.as_str());

        let mut results = Vec::new();
        for item in index_tree.scan_prefix(prefix.as_bytes()) {
            let (key, _) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let key_str = std::str::from_utf8(&key)
                .map_err(|e| DomainError::Repository(e.to_string()))?;
            // Key format: "{email}:{contact_id}"
            if let Some(contact_id_str) = key_str.strip_prefix(&prefix)
                && let Some(bytes) = contact_tree
                    .get(contact_id_str.as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let s: ContactSummary = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    results.push(s);
                }
        }
        Ok(results)
    }

    async fn search_contacts(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<ContactSummary>, DomainError> {
        let tree = self.tree(CONTACTS_TREE)?;
        let query_lower = query.to_lowercase();
        let mut results = Vec::new();

        for item in tree.iter() {
            if results.len() >= limit {
                break;
            }
            let (_k, v) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let s: ContactSummary = serde_json::from_slice(&v)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
            let name_match = s.display_name.to_lowercase().contains(&query_lower);
            let email_match = s.primary_email.as_str().to_lowercase().contains(&query_lower);
            if name_match || email_match {
                results.push(s);
            }
        }
        Ok(results)
    }

    async fn find_by_email_in_account(
        &self,
        email: &EmailAddress,
        account_id: &AccountId,
    ) -> Result<Vec<ContactSummary>, DomainError> {
        let index_tree = self.tree(CONTACTS_BY_EMAIL_TREE)?;
        let contact_tree = self.tree(CONTACTS_TREE)?;
        let prefix = format!("{}:", email.as_str());

        let mut results = Vec::new();
        for item in index_tree.scan_prefix(prefix.as_bytes()) {
            let (key, _) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let key_str = std::str::from_utf8(&key)
                .map_err(|e| DomainError::Repository(e.to_string()))?;
            if let Some(contact_id_str) = key_str.strip_prefix(&prefix)
                && let Some(bytes) = contact_tree
                    .get(contact_id_str.as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let s: ContactSummary = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    if s.account_scope.as_ref() == Some(account_id) {
                        results.push(s);
                    }
                }
        }
        Ok(results)
    }

    async fn find_duplicate_candidates(
        &self,
        account_id: Option<&AccountId>,
    ) -> Result<Vec<(ContactSummary, ContactSummary)>, DomainError> {
        let tree = self.tree(CONTACTS_TREE)?;
        let mut all: Vec<ContactSummary> = Vec::new();
        for item in tree.iter() {
            let (_k, v) = item.map_err(|e| DomainError::Repository(e.to_string()))?;
            let s: ContactSummary = serde_json::from_slice(&v)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
            if let Some(aid) = account_id
                && s.account_scope.as_ref() != Some(aid) {
                    continue;
                }
            all.push(s);
        }

        // Build email → Vec<index> map
        use std::collections::HashMap;
        let mut email_map: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, s) in all.iter().enumerate() {
            for ce in &s.emails {
                email_map.entry(ce.address.as_str().to_owned()).or_default().push(i);
            }
        }

        use std::collections::BTreeSet;
        let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
        let mut pairs: Vec<(ContactSummary, ContactSummary)> = Vec::new();

        for indices in email_map.values() {
            if indices.len() < 2 {
                continue;
            }
            for i in 0..indices.len() {
                for j in (i + 1)..indices.len() {
                    let a = &all[indices[i]];
                    let b = &all[indices[j]];
                    let key = if a.id.to_string() < b.id.to_string() {
                        (a.id.to_string(), b.id.to_string())
                    } else {
                        (b.id.to_string(), a.id.to_string())
                    };
                    if seen.insert(key) {
                        pairs.push((a.clone(), b.clone()));
                    }
                }
            }
        }
        Ok(pairs)
    }
}

// ── Shared repository wrapper ──────────────────────────────────────────
// Newtype wrapper to enable implementing domain traits for Arc<SledRepository>

#[derive(Clone)]
pub struct SharedSledRepository<ES: EventStore>(pub Arc<SledRepository<ES>>);

#[async_trait]
impl<ES: EventStore> AccountWriteRepository for SharedSledRepository<ES> {
    async fn save(&self, account: &Account, events: Vec<DomainEvent>) -> Result<(), DomainError> {
        AccountWriteRepository::save(&*self.0, account, events).await
    }

    async fn load(&self, id: &AccountId) -> Result<Option<Account>, DomainError> {
        AccountWriteRepository::load(&*self.0, id).await
    }
}

impl<ES: EventStore> SharedSledRepository<ES> {
    pub fn new(repo: Arc<SledRepository<ES>>) -> Self {
        Self(repo)
    }
}

#[async_trait]
impl<ES: EventStore> ReadModelProjector for SharedSledRepository<ES> {
    async fn project(&self, envelope: &EventEnvelope) -> Result<(), DomainError> {
        self.0.project(envelope).await
    }
}

#[async_trait]
impl<ES: EventStore> ProjectionCheckpoint for SharedSledRepository<ES> {
    async fn get_checkpoint(&self, projection_name: &str) -> Result<u64, DomainError> {
        self.0.get_checkpoint(projection_name).await
    }

    async fn save_checkpoint(
        &self,
        projection_name: &str,
        global_position: u64,
    ) -> Result<(), DomainError> {
        self.0
            .save_checkpoint(projection_name, global_position)
            .await
    }
}

#[async_trait]
impl<ES: EventStore> AccountReadRepository for SharedSledRepository<ES> {
    async fn get_account(&self, id: &AccountId) -> Result<Option<AccountSummary>, DomainError> {
        self.0.get_account(id).await
    }

    async fn list_accounts(&self) -> Result<Vec<AccountSummary>, DomainError> {
        self.0.list_accounts().await
    }
}

#[async_trait]
impl<ES: EventStore> MailboxReadRepository for SharedSledRepository<ES> {
    async fn get_mailbox(&self, id: &MailboxId) -> Result<Option<Mailbox>, DomainError> {
        self.0.get_mailbox(id).await
    }

    async fn list_mailboxes(&self, account_id: &AccountId) -> Result<Vec<Mailbox>, DomainError> {
        self.0.list_mailboxes(account_id).await
    }
}

#[async_trait]
impl<ES: EventStore> ContactWriteRepository for SharedSledRepository<ES> {
    async fn save(&self, contact: &Contact, events: Vec<DomainEvent>) -> Result<(), DomainError> {
        ContactWriteRepository::save(&*self.0, contact, events).await
    }

    async fn load(&self, id: &ContactId) -> Result<Option<Contact>, DomainError> {
        ContactWriteRepository::load(&*self.0, id).await
    }
}

#[async_trait]
impl<ES: EventStore> ContactReadRepository for SharedSledRepository<ES> {
    async fn get_contact(&self, id: &ContactId) -> Result<Option<ContactSummary>, DomainError> {
        self.0.get_contact(id).await
    }

    async fn list_contacts(
        &self,
        pagination: &Pagination,
    ) -> Result<PagedResult<ContactSummary>, DomainError> {
        self.0.list_contacts(pagination).await
    }

    async fn find_by_email(
        &self,
        email: &EmailAddress,
    ) -> Result<Vec<ContactSummary>, DomainError> {
        self.0.find_by_email(email).await
    }

    async fn search_contacts(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<ContactSummary>, DomainError> {
        self.0.search_contacts(query, limit).await
    }

    async fn find_by_email_in_account(
        &self,
        email: &EmailAddress,
        account_id: &AccountId,
    ) -> Result<Vec<ContactSummary>, DomainError> {
        self.0.find_by_email_in_account(email, account_id).await
    }

    async fn find_duplicate_candidates(
        &self,
        account_id: Option<&AccountId>,
    ) -> Result<Vec<(ContactSummary, ContactSummary)>, DomainError> {
        self.0.find_duplicate_candidates(account_id).await
    }
}

#[async_trait]
impl<ES: EventStore> MessageReadRepository for SharedSledRepository<ES> {
    async fn get_message(&self, id: &MessageId) -> Result<Option<Message>, DomainError> {
        self.0.get_message(id).await
    }

    async fn list_messages(
        &self,
        mailbox_id: &MailboxId,
        pagination: &Pagination,
    ) -> Result<PagedResult<MessageSummary>, DomainError> {
        self.0.list_messages(mailbox_id, pagination).await
    }

    async fn get_thread(&self, message_id: &MessageId) -> Result<Vec<Message>, DomainError> {
        self.0.get_thread(message_id).await
    }

    async fn find_by_provider_uid(
        &self,
        mailbox_id: &MailboxId,
        provider_uid: u32,
    ) -> Result<Option<MessageSummary>, DomainError> {
        self.0.find_by_provider_uid(mailbox_id, provider_uid).await
    }

    async fn scan_all_messages(
        &self,
        account_id: Option<&AccountId>,
    ) -> Result<Vec<Message>, DomainError> {
        self.0.scan_all_messages(account_id).await
    }
}

#[async_trait]
impl<ES: EventStore> mc_domain::traits::repository::ConversationReadRepository
    for SharedSledRepository<ES>
{
    async fn get_conversation(
        &self,
        thread_id: &mc_domain::value_objects::thread_id::ThreadId,
        account_id: &mc_domain::AccountId,
    ) -> Result<Option<mc_domain::traits::repository::ConversationSummary>, DomainError> {
        self.0.get_conversation(thread_id, account_id).await
    }

    async fn list_conversations(
        &self,
        account_id: &mc_domain::AccountId,
        mailbox_id: Option<&mc_domain::MailboxId>,
        pagination: &mc_shared::pagination::Pagination,
    ) -> Result<
        mc_shared::pagination::PagedResult<mc_domain::traits::repository::ConversationSummary>,
        DomainError,
    > {
        self.0.list_conversations(account_id, mailbox_id, pagination).await
    }

    async fn rebuild_index(&self) -> Result<u64, DomainError> {
        self.0.rebuild_index().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::sled::event_store::SledEventStore;
    use mc_domain::{
        AccountStatus, Address, AggregateType, AuthConfig, ConnectionConfig, EmailAddress,
        Envelope, EventEnvelope, Flag, Identity, MailboxPath, MailboxType, ProviderType,
        ProviderUid, ReadModelProjector,
    };
    use std::sync::Arc;

    fn make_store_and_repo() -> (Arc<SledEventStore>, SledRepository<SledEventStore>) {
        let db = sled::Config::new().temporary(true).open().unwrap();
        let event_store = Arc::new(SledEventStore::new(db.clone()));
        let repo = SledRepository::new(db, event_store.clone());
        (event_store, repo)
    }

    fn test_config() -> ConnectionConfig {
        ConnectionConfig {
            imap_host: Some("imap.example.com".into()),
            imap_port: Some(993),
            use_tls: true,
            auth: AuthConfig::Plain {
                username: "user@example.com".into(),
                password: "secret".into(),
            },
            smtp: None,
            smtp_host: None,
            smtp_port: None,
        }
    }

    fn test_identity() -> Identity {
        Identity {
            display_name: "Test".into(),
            email: EmailAddress::new("user@example.com").unwrap(),
            signature_html: None,
            signature_text: None,
        }
    }

    #[tokio::test]
    async fn account_write_and_read() {
        let (_es, repo) = make_store_and_repo();

        let (account, event) = Account::create(
            "Work".into(),
            EmailAddress::new("work@example.com").unwrap(),
            ProviderType::Imap,
            test_config(),
            test_identity(),
        );
        let account_id = account.id;

        // Save through write repo.
        AccountWriteRepository::save(&repo, &account, vec![event]).await.unwrap();

        // Load through write repo (rehydrate from event store).
        let loaded = AccountWriteRepository::load(&repo, &account_id).await.unwrap().unwrap();
        assert_eq!(loaded.name, "Work");

        // Read through read repo (projected summary).
        let summary = AccountReadRepository::get_account(&repo, &account_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.name, "Work");
        assert_eq!(summary.status, AccountStatus::Active);
    }

    #[tokio::test]
    async fn list_accounts_returns_all() {
        let (_es, repo) = make_store_and_repo();

        for name in ["Acc1", "Acc2", "Acc3"] {
            let (account, event) = Account::create(
                name.into(),
                EmailAddress::new(&format!("{name}@example.com").to_lowercase()).unwrap(),
                ProviderType::Imap,
                test_config(),
                test_identity(),
            );
            AccountWriteRepository::save(&repo, &account, vec![event]).await.unwrap();
        }

        let accounts = AccountReadRepository::list_accounts(&repo).await.unwrap();
        assert_eq!(accounts.len(), 3);
    }

    #[tokio::test]
    async fn load_nonexistent_account_returns_none() {
        let (_es, repo) = make_store_and_repo();
        let result = AccountWriteRepository::load(&repo, &AccountId::new()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn backfill_queue_newest_first_and_dequeues_on_body() {
        let (_es, repo) = make_store_and_repo();
        let account_id = AccountId::new();
        let mailbox_id = MailboxId::new();
        let older = MessageId::new();
        let newer = MessageId::new();
        let t0 = chrono::Utc::now();
        let t1 = t0 + chrono::Duration::seconds(60); // strictly newer

        let recv = |pos: u64, id: MessageId, uid: u32, at: chrono::DateTime<chrono::Utc>| {
            make_event_envelope(
                pos,
                &id.to_string(),
                AggregateType::Message,
                1,
                DomainEvent::MessageReceived {
                    message_id: id,
                    account_id,
                    mailbox_id,
                    provider_uid: ProviderUid::new(uid),
                    envelope: test_envelope(),
                    flags: vec![],
                    size_bytes: 0,
                    has_attachments: false,
                    message_rfc_id: None,
                    in_reply_to: None,
                    references: vec![],
                    received_at: at,
                },
            )
        };

        repo.project(&recv(1, older, 10, t0)).await.unwrap();
        repo.project(&recv(2, newer, 20, t1)).await.unwrap();

        // Newest received comes first, carrying the right provider_uid.
        let pending = repo.list_pending_backfill(&account_id, 10).await.unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].message_id.to_string(), newer.to_string());
        assert_eq!(pending[0].provider_uid, 20);
        assert_eq!(pending[1].message_id.to_string(), older.to_string());

        // Fetching a body dequeues exactly that entry.
        repo.project(&make_event_envelope(
            3,
            &newer.to_string(),
            AggregateType::Message,
            2,
            DomainEvent::MessageBodyFetched {
                message_id: newer,
                body_text: Some("hi".into()),
                body_html: None,
                attachments: vec![],
            },
        ))
        .await
        .unwrap();

        let pending = repo.list_pending_backfill(&account_id, 10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].message_id.to_string(), older.to_string());
    }

    #[tokio::test]
    async fn projection_checkpoint_roundtrip() {
        let (_es, repo) = make_store_and_repo();

        let pos = repo.get_checkpoint("test_projection").await.unwrap();
        assert_eq!(pos, 0);

        repo.save_checkpoint("test_projection", 42).await.unwrap();

        let pos = repo.get_checkpoint("test_projection").await.unwrap();
        assert_eq!(pos, 42);

        repo.save_checkpoint("test_projection", 100).await.unwrap();
        let pos = repo.get_checkpoint("test_projection").await.unwrap();
        assert_eq!(pos, 100);
    }

    // ── ReadModelProjector integration tests ─────────────────────────

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
            subject: "Hello World".into(),
            date: chrono::Utc::now(),
        }
    }

    fn make_event_envelope(
        global_position: u64,
        aggregate_id: &str,
        aggregate_type: AggregateType,
        seq: u64,
        event: DomainEvent,
    ) -> EventEnvelope {
        let mut env = EventEnvelope::new(aggregate_id.into(), aggregate_type, seq, event);
        env.global_position = global_position;
        env
    }

    #[tokio::test]
    async fn project_account_lifecycle() {
        let (_es, repo) = make_store_and_repo();
        let account_id = AccountId::new();

        // Project AccountAdded.
        let env = make_event_envelope(
            1,
            &account_id.to_string(),
            AggregateType::Account,
            1,
            DomainEvent::AccountAdded {
                account_id,
                name: "Work".into(),
                email: EmailAddress::new("work@example.com").unwrap(),
                provider_type: ProviderType::Imap,
            },
        );
        repo.project(&env).await.unwrap();

        let summary = AccountReadRepository::get_account(&repo, &account_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.name, "Work");
        assert_eq!(summary.status, AccountStatus::Active);

        // Project AccountUpdated.
        let env = make_event_envelope(
            2,
            &account_id.to_string(),
            AggregateType::Account,
            2,
            DomainEvent::AccountUpdated {
                account_id,
                name: Some("Personal".into()),
                connection_config: None,
            },
        );
        repo.project(&env).await.unwrap();

        let summary = AccountReadRepository::get_account(&repo, &account_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.name, "Personal");

        // Project AccountRemoved.
        let env = make_event_envelope(
            3,
            &account_id.to_string(),
            AggregateType::Account,
            3,
            DomainEvent::AccountRemoved {
                account_id,
            },
        );
        repo.project(&env).await.unwrap();

        let result = AccountReadRepository::get_account(&repo, &account_id)
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn project_mailbox_lifecycle() {
        let (_es, repo) = make_store_and_repo();
        let account_id = AccountId::new();
        let mailbox_id = MailboxId::new();

        // Project MailboxDiscovered.
        let env = make_event_envelope(
            1,
            &mailbox_id.to_string(),
            AggregateType::Mailbox,
            1,
            DomainEvent::MailboxDiscovered {
                mailbox_id,
                account_id,
                path: MailboxPath::inbox(),
                mailbox_type: MailboxType::Inbox,
            },
        );
        repo.project(&env).await.unwrap();

        let mailbox = MailboxReadRepository::get_mailbox(&repo, &mailbox_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mailbox.display_name, "INBOX");
        assert_eq!(mailbox.total_messages, 0);

        // Project MailboxCountsUpdated.
        let env = make_event_envelope(
            2,
            &mailbox_id.to_string(),
            AggregateType::Mailbox,
            2,
            DomainEvent::MailboxCountsUpdated {
                mailbox_id,
                total_messages: 42,
                unread_messages: 5,
            },
        );
        repo.project(&env).await.unwrap();

        let mailbox = MailboxReadRepository::get_mailbox(&repo, &mailbox_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mailbox.total_messages, 42);
        assert_eq!(mailbox.unread_messages, 5);

        // Verify list_mailboxes by account works.
        let mailboxes = MailboxReadRepository::list_mailboxes(&repo, &account_id)
            .await
            .unwrap();
        assert_eq!(mailboxes.len(), 1);

        // Project MailboxDeleted.
        let env = make_event_envelope(
            3,
            &mailbox_id.to_string(),
            AggregateType::Mailbox,
            3,
            DomainEvent::MailboxDeleted {
                mailbox_id,
                account_id,
            },
        );
        repo.project(&env).await.unwrap();

        let result = MailboxReadRepository::get_mailbox(&repo, &mailbox_id)
            .await
            .unwrap();
        assert!(result.is_none());
        let mailboxes = MailboxReadRepository::list_mailboxes(&repo, &account_id)
            .await
            .unwrap();
        assert!(mailboxes.is_empty());
    }

    #[tokio::test]
    async fn project_message_lifecycle() {
        let (_es, repo) = make_store_and_repo();
        let account_id = AccountId::new();
        let mailbox_id = MailboxId::new();
        let message_id = MessageId::new();
        let received_at = chrono::Utc::now();

        // Project MessageReceived.
        let env = make_event_envelope(
            1,
            &message_id.to_string(),
            AggregateType::Message,
            1,
            DomainEvent::MessageReceived {
                message_id,
                account_id,
                mailbox_id,
                provider_uid: ProviderUid::new(42),
                envelope: test_envelope(),
                flags: vec![Flag::Recent],
                size_bytes: 2048,
                has_attachments: false,
                message_rfc_id: None,
                in_reply_to: None,
                references: vec![],
                received_at,
            },
        );
        repo.project(&env).await.unwrap();

        // Verify message stored.
        let msg = MessageReadRepository::get_message(&repo, &message_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.envelope.subject, "Hello World");
        assert_eq!(msg.flags, vec![Flag::Recent]);
        assert!(msg.body_text.is_none());

        // Verify summary stored.
        let page = MessageReadRepository::list_messages(
            &repo,
            &mailbox_id,
            &Pagination {
                page: 1,
                page_size: 10,
            },
        )
        .await
        .unwrap();
        assert_eq!(page.total_count, 1);
        assert_eq!(page.items[0].subject, "Hello World");
        assert_eq!(page.items[0].from_name, Some("Sender".to_string()));
        assert_eq!(page.items[0].from_email, "sender@example.com");

        // Project MessageBodyFetched.
        let env = make_event_envelope(
            2,
            &message_id.to_string(),
            AggregateType::Message,
            2,
            DomainEvent::MessageBodyFetched {
                message_id,
                body_text: Some("Hello plain".into()),
                body_html: Some("<p>Hello</p>".into()),
                attachments: vec![],
            },
        );
        repo.project(&env).await.unwrap();

        let msg = MessageReadRepository::get_message(&repo, &message_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.body_text.as_deref(), Some("Hello plain"));

        // Project MessageFlagged (mark as read).
        let env = make_event_envelope(
            3,
            &message_id.to_string(),
            AggregateType::Message,
            3,
            DomainEvent::MessageFlagged {
                message_id,
                mailbox_id: MailboxId::new(),
                flags_added: vec![Flag::Seen],
                flags_removed: vec![Flag::Recent],
            },
        );
        repo.project(&env).await.unwrap();

        let msg = MessageReadRepository::get_message(&repo, &message_id)
            .await
            .unwrap()
            .unwrap();
        assert!(msg.flags.contains(&Flag::Seen));
        assert!(!msg.flags.contains(&Flag::Recent));

        // Verify summary flags also updated.
        let page = MessageReadRepository::list_messages(
            &repo,
            &mailbox_id,
            &Pagination {
                page: 1,
                page_size: 10,
            },
        )
        .await
        .unwrap();
        assert!(page.items[0].flags.contains(&Flag::Seen));

        // Project MessageDeleted.
        let env = make_event_envelope(
            4,
            &message_id.to_string(),
            AggregateType::Message,
            4,
            DomainEvent::MessageDeleted {
                message_id,
                mailbox_id,
                permanently: false,
            },
        );
        repo.project(&env).await.unwrap();

        let result = MessageReadRepository::get_message(&repo, &message_id)
            .await
            .unwrap();
        assert!(result.is_none());

        let page = MessageReadRepository::list_messages(
            &repo,
            &mailbox_id,
            &Pagination {
                page: 1,
                page_size: 10,
            },
        )
        .await
        .unwrap();
        assert_eq!(page.total_count, 0);
    }

    #[tokio::test]
    async fn project_message_deleted_cleans_conversation_and_thread() {
        let (_es, repo) = make_store_and_repo();
        let account_id = AccountId::new();
        let mailbox_id = MailboxId::new();
        let root_id = MessageId::new();
        let reply_id = MessageId::new();
        let now = chrono::Utc::now();

        let received = |mid: MessageId, pos: u64, rfc: &str, irt: Option<&str>| {
            make_event_envelope(
                pos,
                &mid.to_string(),
                AggregateType::Message,
                1,
                DomainEvent::MessageReceived {
                    message_id: mid,
                    account_id,
                    mailbox_id,
                    provider_uid: ProviderUid::new(pos as u32),
                    envelope: test_envelope(),
                    flags: vec![], // unread
                    size_bytes: 10,
                    has_attachments: false,
                    message_rfc_id: Some(rfc.to_string()),
                    in_reply_to: irt.map(|s| s.to_string()),
                    references: vec![],
                    received_at: now,
                },
            )
        };

        // Two messages in the same thread (a reply to the root).
        repo.project(&received(root_id, 1, "<a@x>", None)).await.unwrap();
        repo.project(&received(reply_id, 2, "<b@x>", Some("<a@x>"))).await.unwrap();

        let page = Pagination { page: 1, page_size: 10 };

        // One conversation, two messages, both unread.
        let convs = mc_domain::traits::repository::ConversationReadRepository::list_conversations(
            &repo, &account_id, None, &page,
        )
        .await
        .unwrap();
        assert_eq!(convs.items.len(), 1);
        assert_eq!(convs.items[0].message_count, 2);
        assert_eq!(convs.items[0].unread_count, 2);
        assert_eq!(
            MessageReadRepository::get_thread(&repo, &root_id).await.unwrap().len(),
            2
        );

        // Delete the reply → conversation shrinks, thread drops it.
        repo.project(&make_event_envelope(
            3,
            &reply_id.to_string(),
            AggregateType::Message,
            2,
            DomainEvent::MessageDeleted { message_id: reply_id, mailbox_id, permanently: false },
        ))
        .await
        .unwrap();

        let convs = mc_domain::traits::repository::ConversationReadRepository::list_conversations(
            &repo, &account_id, None, &page,
        )
        .await
        .unwrap();
        assert_eq!(convs.items.len(), 1, "conversation still present with 1 message");
        assert_eq!(convs.items[0].message_count, 1);
        assert_eq!(convs.items[0].unread_count, 1);
        let thread = MessageReadRepository::get_thread(&repo, &root_id).await.unwrap();
        assert_eq!(thread.len(), 1, "deleted reply no longer in thread");
        assert_eq!(thread[0].id, root_id);

        // Delete the root → conversation removed entirely.
        repo.project(&make_event_envelope(
            4,
            &root_id.to_string(),
            AggregateType::Message,
            2,
            DomainEvent::MessageDeleted { message_id: root_id, mailbox_id, permanently: false },
        ))
        .await
        .unwrap();

        let convs = mc_domain::traits::repository::ConversationReadRepository::list_conversations(
            &repo, &account_id, None, &page,
        )
        .await
        .unwrap();
        assert!(convs.items.is_empty(), "empty conversation removed");
        assert!(
            MessageReadRepository::get_thread(&repo, &root_id).await.unwrap().is_empty(),
            "thread fully cleaned"
        );
    }

    #[tokio::test]
    async fn project_message_move_reindexes_summary() {
        let (_es, repo) = make_store_and_repo();
        let account_id = AccountId::new();
        let inbox_id = MailboxId::new();
        let archive_id = MailboxId::new();
        let message_id = MessageId::new();
        let received_at = chrono::Utc::now();

        // Receive message in inbox.
        let env = make_event_envelope(
            1,
            &message_id.to_string(),
            AggregateType::Message,
            1,
            DomainEvent::MessageReceived {
                message_id,
                account_id,
                mailbox_id: inbox_id,
                provider_uid: ProviderUid::new(1),
                envelope: test_envelope(),
                flags: vec![],
                size_bytes: 1024,
                has_attachments: false,
                message_rfc_id: None,
                in_reply_to: None,
                references: vec![],
                received_at,
            },
        );
        repo.project(&env).await.unwrap();

        // Move to archive.
        let env = make_event_envelope(
            2,
            &message_id.to_string(),
            AggregateType::Message,
            2,
            DomainEvent::MessageMoved {
                message_id,
                from_mailbox_id: inbox_id,
                to_mailbox_id: archive_id,
            },
        );
        repo.project(&env).await.unwrap();

        // Message should now reference archive.
        let msg = MessageReadRepository::get_message(&repo, &message_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.mailbox_id, archive_id);

        // Summary should be under archive, not inbox.
        let inbox_msgs = MessageReadRepository::list_messages(
            &repo,
            &inbox_id,
            &Pagination {
                page: 1,
                page_size: 10,
            },
        )
        .await
        .unwrap();
        assert_eq!(inbox_msgs.total_count, 0);

        let archive_msgs = MessageReadRepository::list_messages(
            &repo,
            &archive_id,
            &Pagination {
                page: 1,
                page_size: 10,
            },
        )
        .await
        .unwrap();
        assert_eq!(archive_msgs.total_count, 1);
    }

    /// Regression test for the "reply shows no content / doesn't attach to
    /// the conversation" bug: sending a message must (1) persist its body so
    /// a refetch doesn't come back empty, (2) land in the Sent mailbox's
    /// message list immediately instead of waiting for the next IMAP sync,
    /// with sender AND recipient populated, and (3) thread correctly with
    /// the message it replies to.
    #[tokio::test]
    async fn project_message_sent_persists_body_appears_in_list_and_threads() {
        let (_es, repo) = make_store_and_repo();
        let account_id = AccountId::new();
        let sent_mailbox_id = MailboxId::new();

        // A message received earlier — the thread root, with a real RFC id
        // (as IMAP-sourced messages always have).
        let root_id = MessageId::new();
        let root_rfc_id = "<root@example.com>".to_string();
        let env = make_event_envelope(
            1,
            &root_id.to_string(),
            AggregateType::Message,
            1,
            DomainEvent::MessageReceived {
                message_id: root_id,
                account_id,
                mailbox_id: MailboxId::new(),
                provider_uid: ProviderUid::new(1),
                envelope: test_envelope(),
                flags: vec![Flag::Seen],
                size_bytes: 1024,
                has_attachments: false,
                message_rfc_id: Some(root_rfc_id.clone()),
                in_reply_to: None,
                references: vec![],
                received_at: chrono::Utc::now(),
            },
        );
        repo.project(&env).await.unwrap();

        // Now send a reply to it.
        let reply_id = MessageId::new();
        let sent_at = chrono::Utc::now();
        let env = make_event_envelope(
            1,
            &reply_id.to_string(),
            AggregateType::Message,
            1,
            DomainEvent::MessageSent {
                message_id: reply_id,
                account_id,
                mailbox_id: sent_mailbox_id,
                envelope: test_envelope(),
                in_reply_to: Some(root_rfc_id.clone()),
                references: vec![root_rfc_id.clone()],
                body_text: Some("Hello plain reply".into()),
                body_html: Some("<p>Hello reply</p>".into()),
                sent_at,
            },
        );
        repo.project(&env).await.unwrap();

        // (1) Body must round-trip, not come back empty.
        let sent_msg = MessageReadRepository::get_message(&repo, &reply_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sent_msg.body_text.as_deref(), Some("Hello plain reply"));
        assert_eq!(sent_msg.body_html.as_deref(), Some("<p>Hello reply</p>"));
        assert_eq!(sent_msg.mailbox_id, sent_mailbox_id);

        // (2) Must appear in the Sent mailbox's list immediately, with
        // sender and recipient populated (not blank).
        let sent_list = MessageReadRepository::list_messages(
            &repo,
            &sent_mailbox_id,
            &Pagination { page: 1, page_size: 10 },
        )
        .await
        .unwrap();
        assert_eq!(sent_list.total_count, 1);
        assert_eq!(sent_list.items[0].from_email, "sender@example.com");
        assert_eq!(sent_list.items[0].to, vec!["recipient@example.com".to_string()]);

        // (3) Reply must thread with the root message, not fork into a new
        // conversation.
        let thread = MessageReadRepository::get_thread(&repo, &root_id).await.unwrap();
        let thread_ids: Vec<_> = thread.iter().map(|m| m.id).collect();
        assert!(thread_ids.contains(&root_id));
        assert!(thread_ids.contains(&reply_id));
    }
}
