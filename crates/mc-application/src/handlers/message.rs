//! Command handler for Message aggregate operations.
//!
//! Message handlers orchestrate local event-sourced state changes.
//! For operations that need remote provider interaction (set flags,
//! move, delete on IMAP), the handler calls the EmailProvider first,
//! then produces domain events on success.

#[allow(unused_imports)]
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use mc_domain::{
    AccountId, AggregateType, DomainEvent, EmailProvider, Envelope, EventEnvelope, EventStore,
    Flag, MailboxId, MailboxPath, MailboxReadRepository, MailboxType, Message, MessageId,
    MessageBodyStore, MessageReadRepository, OutgoingMessage, ProviderRegistry, ReadModelProjector,
    value_objects::uid::ProviderUid,
};
use tracing::info;

use crate::bus::EventPublisher;
use crate::commands::CommandResult;
use crate::error::ApplicationError;

/// Handles Message-related commands.
///
/// Works directly with the EventStore for message aggregate persistence
/// (pure ES pattern). Uses ProviderRegistry to resolve the correct
/// EmailProvider per account for remote operations.
pub struct MessageCommandHandler<ES: EventStore, PR: ProviderRegistry, MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore, B: EventPublisher> {
    event_store: Arc<ES>,
    provider_registry: Arc<PR>,
    message_read_repo: Arc<MR>,
    event_bus: Arc<B>,
}

impl<ES: EventStore, PR: ProviderRegistry, MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore, B: EventPublisher>
    MessageCommandHandler<ES, PR, MR, B>
{
    pub fn new(
        event_store: Arc<ES>,
        provider_registry: Arc<PR>,
        message_read_repo: Arc<MR>,
        event_bus: Arc<B>,
    ) -> Self {
        Self {
            event_store,
            provider_registry,
            message_read_repo,
            event_bus,
        }
    }

    pub async fn handle_fetch_body(
        &self,
        message_id: MessageId,
        mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        let (mut message, seq) = self.load_message(&message_id).await?;

        let provider_uid = message.provider_uid.ok_or_else(|| {
            ApplicationError::PreconditionFailed("message has no provider UID".into())
        })?;

        let provider = self.provider_registry.get_provider(&message.account_id)?;

        // Fetch full message from the remote provider.
        let detail = provider
            .fetch_message_detail(&mailbox_path, provider_uid)
            .await?;

        let event = message.set_body(
            detail.body_text.clone(),
            detail.body_html.clone(),
            detail.attachments.clone(),
        );

        // Body before the event, as in SyncService::store_body.
        self.message_read_repo
            .put_body(&message_id, detail.body_text.clone(), detail.body_html.clone())
            .await?;

        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, "message body fetched");
        Ok(CommandResult::MessageBodyFetched)
    }

    pub async fn handle_set_flags(
        &self,
        message_id: MessageId,
        mailbox_path: MailboxPath,
        flags_to_add: Vec<Flag>,
        flags_to_remove: Vec<Flag>,
    ) -> Result<CommandResult, ApplicationError> {
        let (mut message, seq) = self.load_message(&message_id).await?;

        let provider_uid = message.provider_uid.ok_or_else(|| {
            ApplicationError::PreconditionFailed("message has no provider UID".into())
        })?;

        let provider = self.provider_registry.get_provider(&message.account_id)?;

        // Optimistic update: persist locally first for instant UI feedback,
        // then sync to IMAP in background. Next sync reconciles if IMAP fails.
        let event = message.change_flags(flags_to_add.clone(), flags_to_remove.clone());

        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, "message flags updated locally, syncing to IMAP in background");

        // Fire-and-forget IMAP update
        tokio::spawn(async move {
            if let Err(e) = provider
                .set_flags(&mailbox_path, provider_uid, &flags_to_add, &flags_to_remove)
                .await
            {
                tracing::warn!(%e, "Background IMAP set_flags failed — next sync will reconcile");
            }
        });

        Ok(CommandResult::FlagsSet)
    }

    /// Mark every unread message in a mailbox as read, in a single provider
    /// round-trip (one `set_flags_bulk` call instead of one `set_flags` per
    /// message).
    pub async fn handle_mark_mailbox_read(
        &self,
        mailbox_id: MailboxId,
        mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        let unread = self
            .message_read_repo
            .find_unread_by_mailbox(&mailbox_id)
            .await?;

        if unread.is_empty() {
            return Ok(CommandResult::MailboxMarkedRead { count: 0 });
        }

        let mut provider_uids = Vec::new();
        let account_id = unread[0].account_id.clone();

        for summary in &unread {
            let (mut message, seq) = self.load_message(&summary.id).await?;
            let event = message.change_flags(vec![Flag::Seen], vec![]);
            self.append_and_publish(&summary.id, AggregateType::Message, seq, event)
                .await?;

            if let Some(provider_uid) = summary.provider_uid {
                provider_uids.push(provider_uid);
            }
        }

        info!(%mailbox_id, count = unread.len(), "mailbox marked read locally, syncing to IMAP in background");

        if !provider_uids.is_empty() {
            let provider = self.provider_registry.get_provider(&account_id)?;

            // Fire-and-forget IMAP update, single round-trip for the whole batch.
            tokio::spawn(async move {
                if let Err(e) = provider
                    .set_flags_bulk(&mailbox_path, &provider_uids, &[Flag::Seen], &[])
                    .await
                {
                    tracing::warn!(%e, "Background IMAP set_flags_bulk failed — next sync will reconcile");
                }
            });
        }

        Ok(CommandResult::MailboxMarkedRead {
            count: unread.len() as u32,
        })
    }

    /// Apply a flag change that arrived from a remote IMAP IDLE notification.
    ///
    /// Unlike `handle_set_flags`, this does NOT call the provider (flags are
    /// already changed on the server). It only updates the local projection
    /// by emitting a `MessageFlagged` domain event.
    pub async fn handle_apply_remote_flag_change(
        &self,
        mailbox_id: MailboxId,
        provider_uid: ProviderUid,
        new_flags: Vec<Flag>,
    ) -> Result<CommandResult, ApplicationError> {
        info!(mailbox_id = %mailbox_id, uid = provider_uid.value(), ?new_flags,
            "handle_apply_remote_flag_change: looking up message");

        // Look up the local message by provider UID
        let summary = self
            .message_read_repo
            .find_by_provider_uid(&mailbox_id, provider_uid.value())
            .await?
            .ok_or_else(|| {
                ApplicationError::NotFound(
                    "Message",
                    format!("provider_uid={} mailbox_id={}", provider_uid.value(), mailbox_id),
                )
            })?;

        let message_id = summary.id;
        info!(%message_id, uid = provider_uid.value(), "message found, loading aggregate");

        let (mut message, seq) = self.load_message(&message_id).await?;

        // Compute diff vs current flags
        let flags_to_add: Vec<Flag> = new_flags
            .iter()
            .filter(|f| !message.flags.contains(f))
            .cloned()
            .collect();
        let flags_to_remove: Vec<Flag> = message
            .flags
            .iter()
            .filter(|f| !new_flags.contains(f))
            .cloned()
            .collect();

        info!(%message_id, ?flags_to_add, ?flags_to_remove, "computed flag diff");

        if flags_to_add.is_empty() && flags_to_remove.is_empty() {
            info!(%message_id, "no-op: flags already match");
            return Ok(CommandResult::RemoteFlagApplied);
        }

        let event = message.change_flags(flags_to_add, flags_to_remove);
        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, uid = provider_uid.value(), "remote flag change applied");
        Ok(CommandResult::RemoteFlagApplied)
    }

    pub async fn handle_move(
        &self,
        message_id: MessageId,
        _from_mailbox_id: MailboxId,
        to_mailbox_id: MailboxId,
        from_mailbox_path: MailboxPath,
        to_mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        let (mut message, seq) = self.load_message(&message_id).await?;

        let provider_uid = message.provider_uid.ok_or_else(|| {
            ApplicationError::PreconditionFailed("message has no provider UID".into())
        })?;

        let provider = self.provider_registry.get_provider(&message.account_id)?;

        // Optimistic: update locally first, sync to IMAP in background.
        let event = message.move_to(to_mailbox_id);

        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, "message moved locally, syncing to IMAP in background");

        tokio::spawn(async move {
            if let Err(e) = provider
                .move_message(provider_uid, &from_mailbox_path, &to_mailbox_path)
                .await
            {
                tracing::warn!(%e, "Background IMAP move failed — next sync will reconcile");
            }
        });

        Ok(CommandResult::MessageMoved)
    }

    pub async fn handle_delete(
        &self,
        message_id: MessageId,
        _mailbox_id: MailboxId,
        mailbox_path: MailboxPath,
        permanently: bool,
    ) -> Result<CommandResult, ApplicationError> {
        let (mut message, seq) = self.load_message(&message_id).await?;

        let provider_uid = message.provider_uid.ok_or_else(|| {
            ApplicationError::PreconditionFailed("message has no provider UID".into())
        })?;

        let provider = self.provider_registry.get_provider(&message.account_id)?;

        // Standard client behaviour: a plain delete moves the message to the
        // Trash folder (Thunderbird-style). Only a permanent delete, a delete of
        // a message already in Trash, or the absence of a Trash folder actually
        // expunges it.
        let mailboxes = self.message_read_repo.list_mailboxes(&message.account_id).await?;
        let trash = mailboxes.iter().find(|m| m.mailbox_type == MailboxType::Trash);
        let source_is_trash = mailboxes
            .iter()
            .find(|m| m.id == message.mailbox_id)
            .map(|m| m.mailbox_type == MailboxType::Trash)
            .unwrap_or(false);

        if !permanently
            && !source_is_trash
            && let Some(trash) = trash
        {
            let to_mailbox_id = trash.id;
            let to_path = trash.path.clone();
            let from_path = mailbox_path.clone();

            // Optimistic: move locally first, sync to IMAP in background.
            let event = message.move_to(to_mailbox_id);
            self.append_and_publish(&message_id, AggregateType::Message, seq, event)
                .await?;

            info!(%message_id, "message moved to Trash, syncing to IMAP in background");

            tokio::spawn(async move {
                if let Err(e) = provider
                    .move_message(provider_uid, &from_path, &to_path)
                    .await
                {
                    tracing::warn!(%e, "Background IMAP move-to-Trash failed — next sync will reconcile");
                }
            });

            return Ok(CommandResult::MessageDeleted);
        }

        // Permanent delete (or already in Trash / no Trash folder): expunge on the
        // server and drop from the local read model.
        let event = message.delete(true);
        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, "message permanently deleted, syncing to IMAP in background");

        tokio::spawn(async move {
            if let Err(e) = provider
                .delete_message(&mailbox_path, provider_uid, true)
                .await
            {
                tracing::warn!(%e, "Background IMAP delete failed — next sync will reconcile");
            }
        });

        Ok(CommandResult::MessageDeleted)
    }

    /// Bulk delete. Applies the same Trash-vs-expunge rules as [`Self::handle_delete`]
    /// per message, then groups the survivors by source mailbox and fires **one**
    /// background provider call per group.
    ///
    /// The grouping is the whole point: the per-message path spawns a task each,
    /// so deleting a search result set of 800 would open 800 concurrent IMAP
    /// connections. One `UID STORE` over a compressed UID set is what IMAP is for.
    ///
    /// A message that can't be loaded, has no provider UID, or whose mailbox has
    /// vanished is counted in `failed` and skipped — a stale search index must not
    /// abort the rest of the batch halfway through.
    pub async fn handle_delete_many(
        &self,
        message_ids: Vec<MessageId>,
        permanently: bool,
    ) -> Result<CommandResult, ApplicationError> {
        let mut failed: u32 = 0;
        let mut succeeded: u32 = 0;
        // (account, source path, Some(trash dest) | None = expunge) -> uids
        let mut groups: HashMap<(AccountId, MailboxPath, Option<MailboxPath>), Vec<ProviderUid>> =
            HashMap::new();
        // list_mailboxes hits the read model — once per account, not once per message.
        let mut mailbox_cache: HashMap<AccountId, Vec<mc_domain::Mailbox>> = HashMap::new();

        for message_id in message_ids {
            let Ok((mut message, seq)) = self.load_message(&message_id).await else {
                failed += 1;
                continue;
            };
            let Some(provider_uid) = message.provider_uid else {
                failed += 1;
                continue;
            };

            let account_id = message.account_id;
            if !mailbox_cache.contains_key(&account_id) {
                let boxes = self.message_read_repo.list_mailboxes(&account_id).await?;
                mailbox_cache.insert(account_id, boxes);
            }
            let mailboxes = &mailbox_cache[&account_id];

            let Some(source) = mailboxes.iter().find(|m| m.id == message.mailbox_id) else {
                failed += 1;
                continue;
            };
            let source_path = source.path.clone();
            let source_is_trash = source.mailbox_type == MailboxType::Trash;
            let trash = mailboxes.iter().find(|m| m.mailbox_type == MailboxType::Trash);

            let (event, dest) = if !permanently
                && !source_is_trash
                && let Some(trash) = trash
            {
                (message.move_to(trash.id), Some(trash.path.clone()))
            } else {
                (message.delete(true), None)
            };

            if self
                .append_and_publish(&message_id, AggregateType::Message, seq, event)
                .await
                .is_err()
            {
                failed += 1;
                continue;
            }
            succeeded += 1;

            groups
                .entry((account_id, source_path, dest))
                .or_default()
                .push(provider_uid);
        }

        info!(succeeded, failed, "bulk delete applied locally, syncing to IMAP in background");

        for ((account_id, from_path, dest), uids) in groups {
            let provider = match self.provider_registry.get_provider(&account_id) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(%e, "Bulk delete: no provider for account — next sync will reconcile");
                    continue;
                }
            };
            tokio::spawn(async move {
                let result = match &dest {
                    Some(to) => provider.move_messages(&uids, &from_path, to).await,
                    None => provider.delete_messages(&from_path, &uids, true).await,
                };
                if let Err(e) = result {
                    tracing::warn!(%e, "Background IMAP bulk delete failed — next sync will reconcile");
                }
            });
        }

        Ok(CommandResult::MessagesBulkChanged { succeeded, failed })
    }

    /// Bulk move to a single destination. Same grouping rationale as
    /// [`Self::handle_delete_many`]: one provider call per source mailbox.
    pub async fn handle_move_many(
        &self,
        message_ids: Vec<MessageId>,
        to_mailbox_id: MailboxId,
        to_mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        let mut failed: u32 = 0;
        let mut succeeded: u32 = 0;
        let mut groups: HashMap<(AccountId, MailboxPath), Vec<ProviderUid>> = HashMap::new();
        let mut mailbox_cache: HashMap<AccountId, Vec<mc_domain::Mailbox>> = HashMap::new();

        for message_id in message_ids {
            let Ok((mut message, seq)) = self.load_message(&message_id).await else {
                failed += 1;
                continue;
            };
            let Some(provider_uid) = message.provider_uid else {
                failed += 1;
                continue;
            };
            // Moving a message to the folder it already sits in is a no-op, not
            // a copy — skip it before it reaches IMAP.
            if message.mailbox_id == to_mailbox_id {
                continue;
            }

            let account_id = message.account_id;
            if !mailbox_cache.contains_key(&account_id) {
                let boxes = self.message_read_repo.list_mailboxes(&account_id).await?;
                mailbox_cache.insert(account_id, boxes);
            }
            let Some(source) = mailbox_cache[&account_id]
                .iter()
                .find(|m| m.id == message.mailbox_id)
            else {
                failed += 1;
                continue;
            };
            let source_path = source.path.clone();

            let event = message.move_to(to_mailbox_id);
            if self
                .append_and_publish(&message_id, AggregateType::Message, seq, event)
                .await
                .is_err()
            {
                failed += 1;
                continue;
            }
            succeeded += 1;

            groups
                .entry((account_id, source_path))
                .or_default()
                .push(provider_uid);
        }

        info!(succeeded, failed, "bulk move applied locally, syncing to IMAP in background");

        for ((account_id, from_path), uids) in groups {
            let provider = match self.provider_registry.get_provider(&account_id) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(%e, "Bulk move: no provider for account — next sync will reconcile");
                    continue;
                }
            };
            let to = to_mailbox_path.clone();
            tokio::spawn(async move {
                if let Err(e) = provider.move_messages(&uids, &from_path, &to).await {
                    tracing::warn!(%e, "Background IMAP bulk move failed — next sync will reconcile");
                }
            });
        }

        Ok(CommandResult::MessagesBulkChanged { succeeded, failed })
    }

    pub async fn handle_send(
        &self,
        account_id: AccountId,
        outgoing: OutgoingMessage,
    ) -> Result<CommandResult, ApplicationError> {
        let provider = self.provider_registry.get_provider(&account_id)?;

        // Send via the provider (SMTP or API).
        provider.send_message(&outgoing).await?;

        // Standard client behaviour: replying marks the original message as
        // \Answered. Best-effort — resolve the original by its RFC Message-ID and
        // flag it on the server; the next sync/IDLE reconciles the local flag.
        // ponytail: O(n) scan over stored messages; add an rfc_id→message index if
        // this ever shows up in profiles.
        if let Some(in_reply_to) = outgoing.in_reply_to.clone() {
            let original = self
                .message_read_repo
                .scan_all_messages(Some(&account_id))
                .await
                .unwrap_or_default()
                .into_iter()
                .find(|m| m.message_rfc_id.as_deref() == Some(in_reply_to.as_str()));

            if let Some(orig) = original
                && let Some(uid) = orig.provider_uid
            {
                let mailboxes = self
                    .message_read_repo
                    .list_mailboxes(&account_id)
                    .await
                    .unwrap_or_default();
                if let Some(path) = mailboxes
                    .into_iter()
                    .find(|m| m.id == orig.mailbox_id)
                    .map(|m| m.path)
                {
                    let provider = Arc::clone(&provider);
                    tokio::spawn(async move {
                        if let Err(e) = provider
                            .set_flags(&path, uid, &[Flag::Answered], &[])
                            .await
                        {
                            tracing::warn!(%e, "Failed to set \\Answered on replied-to message");
                        }
                    });
                }
            }
        }

        // Resolve the account's real Sent mailbox so the message lands in the
        // right folder immediately, instead of waiting for the next IMAP sync
        // to reconcile it. Falls back to a placeholder id if the Sent mailbox
        // hasn't been discovered yet (e.g. before the first sync).
        let mailbox_id = self
            .message_read_repo
            .list_mailboxes(&account_id)
            .await?
            .into_iter()
            .find(|m| m.mailbox_type == MailboxType::Sent)
            .map(|m| m.id)
            .unwrap_or_else(MailboxId::new);

        let message_id = MessageId::new();
        let envelope = Envelope {
            from: vec![mc_domain::Address {
                name: outgoing.from.name.clone(),
                email: outgoing.from.email.clone(),
            }],
            to: outgoing
                .to
                .iter()
                .map(|a| mc_domain::Address {
                    name: a.name.clone(),
                    email: a.email.clone(),
                })
                .collect(),
            cc: outgoing
                .cc
                .iter()
                .map(|a| mc_domain::Address {
                    name: a.name.clone(),
                    email: a.email.clone(),
                })
                .collect(),
            bcc: vec![],
            reply_to: vec![],
            subject: outgoing.subject.clone(),
            date: chrono::Utc::now(),
        };

        let event = DomainEvent::MessageSent {
            message_id,
            account_id,
            mailbox_id,
            envelope,
            in_reply_to: outgoing.in_reply_to.clone(),
            references: outgoing.references.clone(),
            body_text: outgoing.body_text.clone(),
            body_html: outgoing.body_html.clone(),
            sent_at: chrono::Utc::now(),
        };

        // New message — no prior events, sequence starts at 0
        self.append_and_publish(&message_id, AggregateType::Message, 0, event)
            .await?;

        info!(%message_id, "message sent");
        Ok(CommandResult::MessageSent { message_id })
    }

    pub async fn handle_discard_draft(
        &self,
        message_id: MessageId,
    ) -> Result<CommandResult, ApplicationError> {
        // Verify the message exists.
        let (_message, seq) = self.load_message(&message_id).await?;

        let event = DomainEvent::DraftDiscarded {
            message_id,
        };

        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, "draft discarded");
        Ok(CommandResult::DraftDiscarded)
    }

    // ── Helpers ────────────────────────────────────────────────────────

    /// Load a message aggregate from the event store.
    /// Returns the rehydrated Message and the current event count (for OCC).
    async fn load_message(&self, id: &MessageId) -> Result<(Message, u64), ApplicationError> {
        let events = self.event_store.load_events(&id.to_string(), 0).await?;
        let event_count = events.len() as u64;

        let domain_events: Vec<DomainEvent> = events.into_iter().map(|e| e.payload).collect();

        let message = Message::rehydrate(&domain_events)
            .ok_or_else(|| ApplicationError::NotFound("Message", id.to_string()))?;

        Ok((message, event_count))
    }

    async fn append_and_publish(
        &self,
        message_id: &MessageId,
        aggregate_type: AggregateType,
        expected_sequence: u64,
        event: DomainEvent,
    ) -> Result<(), ApplicationError> {
        let new_seq = self
            .event_store
            .append(
                &message_id.to_string(),
                aggregate_type,
                expected_sequence,
                vec![event.clone()],
            )
            .await?;

        let envelope =
            EventEnvelope::new(message_id.to_string(), aggregate_type, new_seq, event);

        // Project inline so the read model is updated BEFORE we return.
        // This ensures that any re-query after the command sees fresh data.
        if let Err(e) = self.message_read_repo.project(&envelope).await {
            tracing::warn!("Inline projection failed for {}: {}", envelope.payload.event_name(), e);
        }

        self.event_bus.publish(envelope);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mc_domain::{
        AggregateType, DomainError, EventEnvelope, FetchRange, Mailbox, MailboxStatus,
        MessageEnvelopeSummary, ProviderNotification, ProviderUid,
    };
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::RwLock;

    // ── Mock EventStore ────────────────────────────────────────────────

    struct MockEventStore {
        events: RwLock<HashMap<String, Vec<EventEnvelope>>>,
    }

    impl MockEventStore {
        fn new() -> Self {
            Self {
                events: RwLock::new(HashMap::new()),
            }
        }

        fn with_message(message: &Message) -> Self {
            let store = Self::new();
            // Seed with a MessageReceived event.
            let event = DomainEvent::MessageReceived {
                message_id: message.id,
                account_id: message.account_id,
                mailbox_id: message.mailbox_id,
                provider_uid: message.provider_uid.unwrap(),
                message_rfc_id: message.message_rfc_id.clone(),
                in_reply_to: message.in_reply_to.clone(),
                references: message.references.clone(),
                envelope: message.envelope.clone(),
                flags: message.flags.clone(),
                size_bytes: message.size_bytes,
                has_attachments: message.has_attachments,
                received_at: message.received_at,
            };
            let env = EventEnvelope::new(message.id.to_string(), AggregateType::Message, 1, event);
            store
                .events
                .write()
                .unwrap()
                .insert(message.id.to_string(), vec![env]);
            store
        }
    }

    #[async_trait]
    impl EventStore for MockEventStore {
        async fn append(
            &self,
            aggregate_id: &str,
            _aggregate_type: AggregateType,
            _expected_sequence: u64,
            events: Vec<DomainEvent>,
        ) -> Result<u64, DomainError> {
            let mut store = self.events.write().unwrap();
            let list = store.entry(aggregate_id.to_string()).or_default();
            let mut seq = list.len() as u64;
            for event in events {
                seq += 1;
                let env = EventEnvelope::new(aggregate_id.to_string(), _aggregate_type, seq, event);
                list.push(env);
            }
            Ok(seq)
        }

        async fn load_events(
            &self,
            aggregate_id: &str,
            _from_sequence: u64,
        ) -> Result<Vec<EventEnvelope>, DomainError> {
            Ok(self
                .events
                .read()
                .unwrap()
                .get(aggregate_id)
                .cloned()
                .unwrap_or_default())
        }

        fn load_all_events(
            &self,
            _aggregate_type: Option<AggregateType>,
            _from_global_position: u64,
        ) -> Pin<Box<dyn futures::Stream<Item = Result<EventEnvelope, DomainError>> + Send + '_>> {
            Box::pin(futures::stream::empty())
        }

        fn subscribe(&self) -> Pin<Box<dyn futures::Stream<Item = EventEnvelope> + Send + '_>> {
            Box::pin(futures::stream::empty())
        }
    }

    // ── Mock EmailProvider ─────────────────────────────────────────────

    struct MockEmailProvider {
        /// Message to return from fetch_message_detail.
        detail_response: RwLock<Option<Message>>,
    }

    impl MockEmailProvider {
        fn new() -> Self {
            Self {
                detail_response: RwLock::new(None),
            }
        }
    }

    #[async_trait]
    impl EmailProvider for MockEmailProvider {
        async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
            Ok(vec![])
        }
        async fn fetch_message_envelopes(
            &self,
            _mailbox: &MailboxPath,
            _range: FetchRange,
        ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
            Ok(vec![])
        }
        async fn fetch_message_detail(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
        ) -> Result<Message, DomainError> {
            self.detail_response
                .read()
                .unwrap()
                .clone()
                .ok_or_else(|| DomainError::Provider("no detail configured".into()))
        }
        async fn fetch_raw_message(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
        ) -> Result<Vec<u8>, DomainError> {
            Ok(vec![])
        }
        async fn set_flags(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
            _add: &[Flag],
            _remove: &[Flag],
        ) -> Result<(), DomainError> {
            Ok(())
        }
        async fn set_flags_bulk(
            &self,
            _mailbox: &MailboxPath,
            _uids: &[ProviderUid],
            _add: &[Flag],
            _remove: &[Flag],
        ) -> Result<(), DomainError> {
            Ok(())
        }
        async fn move_message(
            &self,
            _uid: ProviderUid,
            _from: &MailboxPath,
            _to: &MailboxPath,
        ) -> Result<Option<ProviderUid>, DomainError> {
            Ok(None)
        }
        async fn delete_message(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
            _permanently: bool,
        ) -> Result<(), DomainError> {
            Ok(())
        }
        async fn send_message(&self, _message: &OutgoingMessage) -> Result<(), DomainError> {
            Ok(())
        }
        async fn mailbox_status(
            &self,
            _mailbox: &MailboxPath,
        ) -> Result<MailboxStatus, DomainError> {
            Ok(MailboxStatus {
                uid_validity: 1,
                uid_next: 1,
                total_messages: 0,
                unseen_messages: 0,
            })
        }
        async fn fetch_uid_flags(
            &self,
            _mailbox: &MailboxPath,
        ) -> Result<Vec<mc_domain::traits::email_provider::UidFlagEntry>, DomainError> {
            Ok(vec![])
        }
        fn watch_mailbox(
            &self,
            _mailbox: &MailboxPath,
        ) -> Pin<
            Box<dyn futures::Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>,
        > {
            Box::pin(futures::stream::empty())
        }
        async fn fetch_bodies_bulk(
            &self,
            _mailbox: &MailboxPath,
            _uids: &[ProviderUid],
        ) -> Result<Vec<mc_domain::traits::email_provider::MessageBodyResult>, DomainError> {
            Ok(vec![])
        }
    }

    // ── Mock ProviderRegistry ──────────────────────────────────────────

    struct MockProviderRegistry {
        provider: Arc<MockEmailProvider>,
    }

    impl MockProviderRegistry {
        fn new(provider: Arc<MockEmailProvider>) -> Self {
            Self { provider }
        }
    }

    impl ProviderRegistry for MockProviderRegistry {
        type Provider = MockEmailProvider;

        fn get_provider(
            &self,
            _account_id: &AccountId,
        ) -> Result<Arc<MockEmailProvider>, DomainError> {
            Ok(self.provider.clone())
        }

        fn register_provider(&self, _account_id: AccountId, _provider: Arc<MockEmailProvider>) {}

        fn remove_provider(&self, _account_id: &AccountId) {}
    }

    // ── Mock MessageReadRepository ─────────────────────────────────────

    /// (message_id, body_text, body_html) as handed to `MessageBodyStore::put_body`.
    type StoredBody = (MessageId, Option<String>, Option<String>);

    struct MockMessageReadRepo {
        mailboxes: Vec<mc_domain::Mailbox>,
        stored_bodies: std::sync::Mutex<Vec<StoredBody>>,
    }

    impl MockMessageReadRepo {
        fn new() -> Self {
            Self { mailboxes: vec![], stored_bodies: std::sync::Mutex::new(vec![]) }
        }

        fn with_mailboxes(mailboxes: Vec<mc_domain::Mailbox>) -> Self {
            Self { mailboxes, stored_bodies: std::sync::Mutex::new(vec![]) }
        }
    }

    #[async_trait]
    impl MessageBodyStore for MockMessageReadRepo {
        async fn put_body(
            &self,
            message_id: &MessageId,
            body_text: Option<String>,
            body_html: Option<String>,
        ) -> Result<(), DomainError> {
            self.stored_bodies.lock().unwrap().push((*message_id, body_text, body_html));
            Ok(())
        }
    }

    #[async_trait]
    impl mc_domain::MailboxReadRepository for MockMessageReadRepo {
        async fn get_mailbox(
            &self,
            id: &MailboxId,
        ) -> Result<Option<mc_domain::Mailbox>, DomainError> {
            Ok(self.mailboxes.iter().find(|m| m.id == *id).cloned())
        }
        async fn list_mailboxes(
            &self,
            account_id: &AccountId,
        ) -> Result<Vec<mc_domain::Mailbox>, DomainError> {
            Ok(self
                .mailboxes
                .iter()
                .filter(|m| m.account_id == *account_id)
                .cloned()
                .collect())
        }
    }

    #[async_trait]
    impl MessageReadRepository for MockMessageReadRepo {
        async fn get_message(&self, _id: &MessageId) -> Result<Option<Message>, DomainError> {
            Ok(None)
        }
        async fn list_messages(
            &self,
            _mailbox_id: &MailboxId,
            _pagination: &mc_shared::pagination::Pagination,
        ) -> Result<mc_shared::pagination::PagedResult<mc_domain::MessageSummary>, DomainError>
        {
            Ok(mc_shared::pagination::PagedResult {
                items: vec![],
                total_count: 0,
                page: 1,
                page_size: 20,
            })
        }
        async fn get_thread(&self, _id: &MessageId) -> Result<Vec<Message>, DomainError> {
            Ok(vec![])
        }
        async fn find_by_provider_uid(
            &self,
            _mailbox_id: &MailboxId,
            _provider_uid: u32,
        ) -> Result<Option<mc_domain::MessageSummary>, DomainError> {
            Ok(None)
        }

        async fn find_unread_by_mailbox(
            &self,
            _mailbox_id: &MailboxId,
        ) -> Result<Vec<mc_domain::MessageSummary>, DomainError> {
            Ok(vec![])
        }

        async fn scan_all_messages(
            &self,
            _account_id: Option<&mc_domain::AccountId>,
        ) -> Result<Vec<mc_domain::Message>, DomainError> {
            Ok(vec![])
        }

        async fn suggest(
            &self,
            _prefix: &str,
            _scope: Option<mc_domain::SuggestScope>,
            _max_per_field: usize,
        ) -> Result<Vec<mc_domain::Suggestion>, DomainError> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl ReadModelProjector for MockMessageReadRepo {
        async fn project(&self, _envelope: &EventEnvelope) -> Result<(), DomainError> {
            Ok(()) // no-op in tests
        }
    }

    // ── Mock EventBus ──────────────────────────────────────────────────

    struct MockEventBus {
        published: RwLock<Vec<EventEnvelope>>,
    }

    impl MockEventBus {
        fn new() -> Self {
            Self {
                published: RwLock::new(Vec::new()),
            }
        }

        fn last_event_name(&self) -> Option<String> {
            self.published
                .read()
                .unwrap()
                .last()
                .map(|e| e.payload.event_name().to_string())
        }
    }

    impl EventPublisher for MockEventBus {
        fn publish(&self, envelope: EventEnvelope) {
            self.published.write().unwrap().push(envelope);
        }
    }

    // ── Test helpers ───────────────────────────────────────────────────

    use mc_domain::{Address, EmailAddress};

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
            subject: "Test".into(),
            date: chrono::Utc::now(),
        }
    }

    fn make_test_message() -> Message {
        let (msg, _) = Message::receive(
            AccountId::new(),
            MailboxId::new(),
            ProviderUid::new(42),
            None,
            None,
            vec![],
            test_envelope(),
            vec![],
            2048,
            false,
            chrono::Utc::now(),
        );
        msg
    }

    // ── Tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn handle_set_flags_marks_as_read() {
        let msg = make_test_message();
        let msg_id = msg.id;

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::new());
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let result = handler
            .handle_set_flags(msg_id, MailboxPath::inbox(), vec![Flag::Seen], vec![])
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::FlagsSet));
        assert_eq!(bus.last_event_name().unwrap(), "MessageFlagged");
    }

    #[tokio::test]
    async fn handle_mark_mailbox_read_with_no_unread_is_noop() {
        let msg = make_test_message();
        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::new()); // find_unread_by_mailbox -> []
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let result = handler
            .handle_mark_mailbox_read(MailboxId::new(), MailboxPath::inbox())
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::MailboxMarkedRead { count: 0 }));
        assert!(bus.last_event_name().is_none());
    }

    #[tokio::test]
    async fn handle_move_produces_event() {
        let msg = make_test_message();
        let msg_id = msg.id;

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::new());
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let result = handler
            .handle_move(
                msg_id,
                MailboxId::new(),
                MailboxId::new(),
                MailboxPath::inbox(),
                MailboxPath::parse("Archive", '/').unwrap(),
            )
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::MessageMoved));
        assert_eq!(bus.last_event_name().unwrap(), "MessageMoved");
    }

    #[tokio::test]
    async fn handle_delete_produces_event() {
        let msg = make_test_message();
        let msg_id = msg.id;
        let mbx_id = msg.mailbox_id;

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::new());
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let result = handler
            .handle_delete(msg_id, mbx_id, MailboxPath::inbox(), false)
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::MessageDeleted));
        assert_eq!(bus.last_event_name().unwrap(), "MessageDeleted");
    }

    #[tokio::test]
    async fn handle_delete_moves_to_trash_when_trash_exists() {
        let msg = make_test_message();
        let msg_id = msg.id;
        let mbx_id = msg.mailbox_id;

        // A Trash folder exists for this account and the message is not in it,
        // so a plain delete must move-to-Trash (MessageMoved), not expunge.
        let trash = mc_domain::Mailbox {
            id: MailboxId::new(),
            account_id: msg.account_id,
            path: MailboxPath::parse("Trash", '/').unwrap(),
            display_name: "Trash".into(),
            mailbox_type: MailboxType::Trash,
            flags: vec![],
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        };

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(MockEmailProvider::new())));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![trash]));
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let result = handler
            .handle_delete(msg_id, mbx_id, MailboxPath::inbox(), false)
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::MessageDeleted));
        assert_eq!(bus.last_event_name().unwrap(), "MessageMoved");
    }

    /// The bulk path must survive ids it cannot resolve: a search index that is
    /// behind the mailbox will hand it messages that no longer exist, and one of
    /// those must not abort the rest of the batch.
    #[tokio::test]
    async fn handle_delete_many_counts_partial_success() {
        let msg = make_test_message();
        let msg_id = msg.id;

        let source = mc_domain::Mailbox {
            id: msg.mailbox_id,
            account_id: msg.account_id,
            path: MailboxPath::inbox(),
            display_name: "INBOX".into(),
            mailbox_type: MailboxType::Inbox,
            flags: vec![],
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        };
        let trash = mc_domain::Mailbox {
            id: MailboxId::new(),
            account_id: msg.account_id,
            path: MailboxPath::parse("Trash", '/').unwrap(),
            display_name: "Trash".into(),
            mailbox_type: MailboxType::Trash,
            flags: vec![],
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        };

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(MockEmailProvider::new())));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![source, trash]));
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        // One real message, one id that isn't in the event store.
        let result = handler
            .handle_delete_many(vec![msg_id, MessageId::new()], false)
            .await
            .unwrap();

        assert!(
            matches!(result, CommandResult::MessagesBulkChanged { succeeded: 1, failed: 1 }),
            "expected 1 ok / 1 skipped, got {result:?}"
        );
        // Trash exists and the message wasn't in it, so a plain bulk delete moves.
        assert_eq!(bus.last_event_name().unwrap(), "MessageMoved");
    }

    #[tokio::test]
    async fn handle_delete_permanent_expunges_even_with_trash() {
        let msg = make_test_message();
        let msg_id = msg.id;
        let mbx_id = msg.mailbox_id;

        let trash = mc_domain::Mailbox {
            id: MailboxId::new(),
            account_id: msg.account_id,
            path: MailboxPath::parse("Trash", '/').unwrap(),
            display_name: "Trash".into(),
            mailbox_type: MailboxType::Trash,
            flags: vec![],
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        };

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(MockEmailProvider::new())));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![trash]));
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        // permanently = true → expunge, MessageDeleted, even though Trash exists.
        let result = handler
            .handle_delete(msg_id, mbx_id, MailboxPath::inbox(), true)
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::MessageDeleted));
        assert_eq!(bus.last_event_name().unwrap(), "MessageDeleted");
    }

    #[tokio::test]
    async fn handle_nonexistent_message_returns_not_found() {
        let es = Arc::new(MockEventStore::new());
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::new());
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus);

        let err = handler
            .handle_set_flags(
                MessageId::new(),
                MailboxPath::inbox(),
                vec![Flag::Seen],
                vec![],
            )
            .await
            .unwrap_err();

        assert!(matches!(err, ApplicationError::NotFound(..)));
    }

    #[tokio::test]
    async fn handle_send_produces_sent_event() {
        let account_id = AccountId::new();
        let es = Arc::new(MockEventStore::new());
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::new());
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let outgoing = OutgoingMessage {
            from: Address {
                name: Some("Me".into()),
                email: EmailAddress::new("me@example.com").unwrap(),
            },
            to: vec![Address {
                name: None,
                email: EmailAddress::new("you@example.com").unwrap(),
            }],
            cc: vec![],
            bcc: vec![],
            subject: "Hello".into(),
            body_text: Some("Hi".into()),
            body_html: None,
            in_reply_to: None,
            references: vec![],
            attachments: vec![],
        };

        let result = handler.handle_send(account_id, outgoing).await.unwrap();

        assert!(matches!(result, CommandResult::MessageSent { .. }));
        assert_eq!(bus.last_event_name().unwrap(), "MessageSent");

        // Regression: the sent event must carry the body, not drop it —
        // otherwise a refetch of the sent message comes back empty.
        let published = bus.published.read().unwrap();
        let DomainEvent::MessageSent { body_text, mailbox_id, .. } = &published.last().unwrap().payload
        else {
            panic!("expected MessageSent event");
        };
        assert_eq!(body_text.as_deref(), Some("Hi"));
        // No Sent mailbox discovered yet — falls back to a placeholder id
        // rather than erroring out.
        assert_ne!(*mailbox_id, MailboxId::default());
    }

    #[tokio::test]
    async fn handle_send_resolves_real_sent_mailbox() {
        let account_id = AccountId::new();
        let sent_mailbox = mc_domain::Mailbox {
            id: MailboxId::new(),
            account_id,
            path: mc_domain::value_objects::mailbox_path::MailboxPath::parse("Sent", '/').unwrap(),
            display_name: "Sent".into(),
            mailbox_type: mc_domain::MailboxType::Sent,
            flags: vec![],
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        };
        let inbox = mc_domain::Mailbox {
            id: MailboxId::new(),
            account_id,
            path: mc_domain::value_objects::mailbox_path::MailboxPath::inbox(),
            display_name: "INBOX".into(),
            mailbox_type: mc_domain::MailboxType::Inbox,
            flags: vec![],
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        };

        let es = Arc::new(MockEventStore::new());
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(MockEmailProvider::new())));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![
            inbox,
            sent_mailbox.clone(),
        ]));
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let outgoing = OutgoingMessage {
            from: Address {
                name: None,
                email: EmailAddress::new("me@example.com").unwrap(),
            },
            to: vec![Address {
                name: None,
                email: EmailAddress::new("you@example.com").unwrap(),
            }],
            cc: vec![],
            bcc: vec![],
            subject: "Hello".into(),
            body_text: Some("Hi".into()),
            body_html: None,
            in_reply_to: None,
            references: vec![],
            attachments: vec![],
        };

        handler.handle_send(account_id, outgoing).await.unwrap();

        let published = bus.published.read().unwrap();
        let DomainEvent::MessageSent { mailbox_id, .. } = &published.last().unwrap().payload else {
            panic!("expected MessageSent event");
        };
        assert_eq!(*mailbox_id, sent_mailbox.id);
    }

    #[tokio::test]
    async fn handle_fetch_body_stores_body_through_body_store() {
        let msg = make_test_message();
        let msg_id = msg.id;
        let provider = MockEmailProvider::new();
        *provider.detail_response.write().unwrap() = Some(Message {
            body_text: Some("fetched text".into()),
            body_html: Some("<p>fetched</p>".into()),
            ..msg.clone()
        });

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(provider)));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::new());
        let handler = MessageCommandHandler::new(es, registry, read_repo.clone(), bus.clone());

        handler.handle_fetch_body(msg_id, MailboxPath::inbox()).await.unwrap();

        assert_eq!(
            read_repo.stored_bodies.lock().unwrap().as_slice(),
            &[(msg_id, Some("fetched text".to_string()), Some("<p>fetched</p>".to_string()))]
        );
        assert_eq!(bus.last_event_name().unwrap(), "MessageBodyFetched");
    }
}
