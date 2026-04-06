//! Command handler for Message aggregate operations.
//!
//! Message handlers orchestrate local event-sourced state changes.
//! For operations that need remote provider interaction (set flags,
//! move, delete on IMAP), the handler calls the EmailProvider first,
//! then produces domain events on success.

#[allow(unused_imports)]
use async_trait::async_trait;
use std::sync::Arc;

use mc_domain::{
    AccountId, AggregateType, DomainEvent, EmailProvider, Envelope, EventEnvelope, EventStore,
    Flag, MailboxId, MailboxPath, Message, MessageId, MessageReadRepository, OutgoingMessage,
    ProviderRegistry, ReadModelProjector, value_objects::uid::ProviderUid,
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
pub struct MessageCommandHandler<ES: EventStore, PR: ProviderRegistry, MR: MessageReadRepository + ReadModelProjector, B: EventPublisher> {
    event_store: Arc<ES>,
    provider_registry: Arc<PR>,
    message_read_repo: Arc<MR>,
    event_bus: Arc<B>,
}

impl<ES: EventStore, PR: ProviderRegistry, MR: MessageReadRepository + ReadModelProjector, B: EventPublisher>
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

        let message_id = summary.id.clone();
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

    /// Apply a remote expunge: a message was deleted on the server (e.g. IMAP EXPUNGE in IDLE).
    ///
    /// Unlike `handle_delete`, this does NOT call the provider (message is already
    /// gone on the server). It only records the deletion locally by emitting a
    /// `MessageDeleted` domain event.
    pub async fn handle_apply_remote_expunge(
        &self,
        mailbox_id: MailboxId,
        provider_uid: ProviderUid,
    ) -> Result<CommandResult, ApplicationError> {
        info!(mailbox_id = %mailbox_id, uid = provider_uid.value(),
            "handle_apply_remote_expunge: looking up message");

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

        let message_id = summary.id.clone();
        let (message, seq) = self.load_message(&message_id).await?;

        let event = message.delete(true);
        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, uid = provider_uid.value(), "remote expunge applied");
        Ok(CommandResult::RemoteExpungeApplied)
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
        let (message, seq) = self.load_message(&message_id).await?;

        let provider_uid = message.provider_uid.ok_or_else(|| {
            ApplicationError::PreconditionFailed("message has no provider UID".into())
        })?;

        let provider = self.provider_registry.get_provider(&message.account_id)?;

        // Optimistic: update locally first, sync to IMAP in background.
        let event = message.delete(permanently);

        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, permanently, "message deleted locally, syncing to IMAP in background");

        tokio::spawn(async move {
            if let Err(e) = provider
                .delete_message(&mailbox_path, provider_uid, permanently)
                .await
            {
                tracing::warn!(%e, "Background IMAP delete failed — next sync will reconcile");
            }
        });

        Ok(CommandResult::MessageDeleted)
    }

    pub async fn handle_send(
        &self,
        account_id: AccountId,
        outgoing: OutgoingMessage,
    ) -> Result<CommandResult, ApplicationError> {
        let provider = self.provider_registry.get_provider(&account_id)?;

        // Send via the provider (SMTP or API).
        provider.send_message(&outgoing).await?;

        let message_id = MessageId::new();
        let envelope = Envelope {
            from: vec![mc_domain::Address {
                name: Some(outgoing.from.name.clone().unwrap_or_default()),
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
            message_id: message_id.clone(),
            account_id,
            envelope,
            in_reply_to: outgoing.in_reply_to.clone(),
            references: outgoing.references.clone(),
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
            message_id: message_id.clone(),
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
                message_id: message.id.clone(),
                account_id: message.account_id.clone(),
                mailbox_id: message.mailbox_id.clone(),
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

        async fn load_all_events(
            &self,
            _aggregate_type: Option<AggregateType>,
            _from_global_position: u64,
        ) -> Result<Vec<EventEnvelope>, DomainError> {
            Ok(vec![])
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

    struct MockMessageReadRepo;

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

        async fn scan_all_messages(
            &self,
            _account_id: Option<&mc_domain::AccountId>,
        ) -> Result<Vec<mc_domain::Message>, DomainError> {
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
        );
        msg
    }

    // ── Tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn handle_set_flags_marks_as_read() {
        let msg = make_test_message();
        let msg_id = msg.id.clone();

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo);
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let result = handler
            .handle_set_flags(msg_id, MailboxPath::inbox(), vec![Flag::Seen], vec![])
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::FlagsSet));
        assert_eq!(bus.last_event_name().unwrap(), "MessageFlagged");
    }

    #[tokio::test]
    async fn handle_move_produces_event() {
        let msg = make_test_message();
        let msg_id = msg.id.clone();

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo);
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
        let msg_id = msg.id.clone();
        let mbx_id = msg.mailbox_id.clone();

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo);
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let result = handler
            .handle_delete(msg_id, mbx_id, MailboxPath::inbox(), false)
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
        let read_repo = Arc::new(MockMessageReadRepo);
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
        let read_repo = Arc::new(MockMessageReadRepo);
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
    }
}
