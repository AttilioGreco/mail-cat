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
    AccountWriteRepository, AggregateType, Contact, ContactId, ContactReadRepository,
    ContactSummary, ContactWriteRepository, DomainError, DomainEvent, EmailAddress, EventEnvelope,
    EventStore, Flag, Mailbox, MailboxId, MailboxReadRepository, Message, MessageId,
    MessageReadRepository, MessageSummary, ProjectionCheckpoint, ProviderUid, ReadModelProjector,
};
use mc_shared::pagination::{PagedResult, Pagination};

const ACCOUNTS_TREE: &str = "accounts";
const MAILBOXES_TREE: &str = "mailboxes";
const MAILBOXES_BY_ACCOUNT_TREE: &str = "mailboxes_by_account";
const MESSAGES_TREE: &str = "messages";
const MESSAGE_SUMMARIES_TREE: &str = "message_summaries";
const PROJECTION_CHECKPOINTS_TREE: &str = "projection_checkpoints";
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
            id: account.id.clone(),
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
            if let Some(aid) = account_id {
                if &msg.account_id != aid {
                    continue;
                }
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
        pagination: &mc_shared::pagination::Pagination,
    ) -> Result<
        mc_shared::pagination::PagedResult<mc_domain::traits::repository::ConversationSummary>,
        DomainError,
    > {
        let tree = self.tree(CONVERSATIONS_TREE)?;
        let prefix = format!("{}:", account_id);

        let mut all: Vec<mc_domain::traits::repository::ConversationSummary> = tree
            .scan_prefix(prefix.as_bytes())
            .filter_map(|r| r.ok())
            .filter_map(|(_, v)| serde_json::from_slice::<mc_domain::traits::repository::ConversationSummary>(&v).ok())
            .collect();

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
        let events = self
            .event_store
            .load_all_events(None, 0)
            .await?;

        let total = events.len() as u64;
        for envelope in &events {
            self.project(envelope).await?;
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
                    id: account_id.clone(),
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
                    id: mailbox_id.clone(),
                    account_id: account_id.clone(),
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
                    id: message_id.clone(),
                    account_id: account_id.clone(),
                    mailbox_id: mailbox_id.clone(),
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
                    raw_headers: None,
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
                    id: message_id.clone(),
                    account_id: account_id.clone(),
                    mailbox_id: mailbox_id.clone(),
                    provider_uid: Some(*provider_uid),
                    from_name,
                    from_email,
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
                    let latest_sender = envelope
                        .from
                        .first()
                        .map(|a| a.name.clone().unwrap_or_else(|| a.email.to_string()))
                        .unwrap_or_default();
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
                                account_id: account_id.clone(),
                                subject,
                                message_count: 0,
                                unread_count: 0,
                                last_message_at: *received_at,
                                latest_sender: latest_sender.clone(),
                                sample_message_id: message_id.clone(),
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
                        conv.sample_message_id = message_id.clone();
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
                    let old_mailbox_id = message.mailbox_id.clone();
                    message.mailbox_id = to_mailbox_id.clone();
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
                mailbox_id,
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
                    let key =
                        Self::message_summary_key(mailbox_id, &message.received_at, message_id);
                    summary_tree
                        .remove(key.as_bytes())
                        .map_err(|e| DomainError::Repository(e.to_string()))?;
                }
                debug!(message_id = %message_id, "projected MessageDeleted");
            }

            DomainEvent::MessageSent {
                message_id,
                account_id,
                envelope,
                in_reply_to,
                references,
                sent_at,
            } => {
                let message = Message {
                    id: message_id.clone(),
                    account_id: account_id.clone(),
                    mailbox_id: MailboxId::new(), // Sent folder assigned during sync
                    provider_uid: None,
                    message_rfc_id: None,
                    in_reply_to: in_reply_to.clone(),
                    references: references.clone(),
                    envelope: envelope.clone(),
                    flags: vec![Flag::Seen],
                    size_bytes: 0,
                    has_attachments: false,
                    attachments: Vec::new(),
                    body_text: None,
                    body_html: None,
                    raw_headers: None,
                    received_at: *sent_at,
                    internal_date: *sent_at,
                };
                let tree = self.tree(MESSAGES_TREE)?;
                let json = serde_json::to_vec(&message)
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                tree.insert(message_id.to_string().as_bytes(), json)
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
                    let latest_sender = envelope
                        .from
                        .first()
                        .map(|a| a.name.clone().unwrap_or_else(|| a.email.to_string()))
                        .unwrap_or_default();

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
                    id: message_id.clone(),
                    account_id: account_id.clone(),
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
                    raw_headers: None,
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
                    id: contact_id.clone(),
                    display_name: display_name.clone(),
                    primary_email: primary_email.clone(),
                    emails: emails.clone(),
                    organization: None,
                    source: source.clone(),
                    account_scope: account_scope.clone(),
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
            summary.mailbox_id = new_mailbox_id.clone();
            let new_key = Self::message_summary_key(new_mailbox_id, received_at, message_id);
            let json = serde_json::to_vec(&summary)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
            tree.insert(new_key.as_bytes(), json)
                .map_err(|e| DomainError::Repository(e.to_string()))?;
        }

        Ok(())
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
            if let Some(contact_id_str) = key_str.strip_prefix(&prefix) {
                if let Some(bytes) = contact_tree
                    .get(contact_id_str.as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    let s: ContactSummary = serde_json::from_slice(&bytes)
                        .map_err(|e| DomainError::Serialization(e.to_string()))?;
                    results.push(s);
                }
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
            if let Some(contact_id_str) = key_str.strip_prefix(&prefix) {
                if let Some(bytes) = contact_tree
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
            if let Some(aid) = account_id {
                if s.account_scope.as_ref() != Some(aid) {
                    continue;
                }
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
        pagination: &mc_shared::pagination::Pagination,
    ) -> Result<
        mc_shared::pagination::PagedResult<mc_domain::traits::repository::ConversationSummary>,
        DomainError,
    > {
        self.0.list_conversations(account_id, pagination).await
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
        let account_id = account.id.clone();

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
                account_id: account_id.clone(),
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
                account_id: account_id.clone(),
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
                account_id: account_id.clone(),
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
                mailbox_id: mailbox_id.clone(),
                account_id: account_id.clone(),
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
                mailbox_id: mailbox_id.clone(),
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
                mailbox_id: mailbox_id.clone(),
                account_id: account_id.clone(),
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
                message_id: message_id.clone(),
                account_id: account_id.clone(),
                mailbox_id: mailbox_id.clone(),
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
                message_id: message_id.clone(),
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
                message_id: message_id.clone(),
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
                message_id: message_id.clone(),
                mailbox_id: mailbox_id.clone(),
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
                message_id: message_id.clone(),
                account_id: account_id.clone(),
                mailbox_id: inbox_id.clone(),
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
                message_id: message_id.clone(),
                from_mailbox_id: inbox_id.clone(),
                to_mailbox_id: archive_id.clone(),
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
}
