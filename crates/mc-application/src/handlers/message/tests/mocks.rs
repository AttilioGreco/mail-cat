use super::*;

// ── Mock EventStore ────────────────────────────────────────────────

pub(super) struct MockEventStore {
    pub(super) events: RwLock<HashMap<String, Vec<EventEnvelope>>>,
    /// Appends that lose to another writer, as a sync appending first.
    pub(super) conflicts: std::sync::Mutex<u32>,
}

impl MockEventStore {
    pub(super) fn new() -> Self {
        Self {
            events: RwLock::new(HashMap::new()),
            conflicts: Default::default(),
        }
    }

    pub(super) fn with_message(message: &Message) -> Self {
        let store = Self::new();
        store.add(message);
        store
    }

    pub(super) fn add(&self, message: &Message) {
        // Seed with a MessageReceived event.
        let event = DomainEvent::MessageReceived {
            message_id: message.id,
            account_id: message.account_id,
            mailbox_id: mailbox_of(message),
            remote_id: uid_of(message).unwrap(),
            message_rfc_id: message.message_rfc_id.clone(),
            in_reply_to: message.in_reply_to.clone(),
            references: message.references.clone(),
            envelope: message.envelope.clone(),
            flags: message.flags.clone(),
            size_bytes: message.size_bytes,
            has_attachments: message.has_attachments,
            received_at: message.received_at,
            restored: false,
        };
        let env = EventEnvelope::new(message.id.to_string(), AggregateType::Message, 1, event);
        self.events
            .write()
            .unwrap()
            .insert(message.id.to_string(), vec![env]);
    }
}

#[async_trait]
impl EventStore for MockEventStore {
    async fn append(
        &self,
        aggregate_id: &str,
        _aggregate_type: AggregateType,
        expected_sequence: u64,
        events: Vec<DomainEvent>,
    ) -> Result<u64, DomainError> {
        let mut store = self.events.write().unwrap();
        let list = store.entry(aggregate_id.to_string()).or_default();
        let mut seq = list.len() as u64;
        let mut conflicts = self.conflicts.lock().unwrap();
        if *conflicts > 0 {
            *conflicts -= 1;
            list.push(EventEnvelope::new(
                aggregate_id.to_string(),
                _aggregate_type,
                seq + 1,
                DomainEvent::MessageFlagged {
                    // Only the flags matter to the aggregate.
                    message_id: MessageId::new(),
                    flags_added: vec![Flag::Flagged],
                    flags_removed: vec![],
                },
            ));
            return Err(DomainError::ConcurrencyConflict {
                expected: expected_sequence,
                actual: seq + 1,
            });
        }
        if seq != expected_sequence {
            return Err(DomainError::ConcurrencyConflict {
                expected: expected_sequence,
                actual: seq,
            });
        }
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

#[derive(Default)]
pub(super) struct MockEmailProvider {
    /// Message to return from fetch_message_detail.
    pub(super) detail_response: RwLock<Option<Message>>,
    pub(super) sent: RwLock<Vec<OutgoingMessage>>,
    pub(super) appended: RwLock<Vec<(MailboxPath, OutgoingMessage, Vec<Flag>)>>,
    pub(super) raw: RwLock<Vec<u8>>,
    /// Runs while the body fetch is "on the wire".
    pub(super) during_fetch: RwLock<Option<Box<dyn Fn() + Send + Sync>>>,
    /// UID reported for the moved message in its destination.
    pub(super) moved_uid: RwLock<Option<RemoteId>>,
    /// What `find_by_message_id` finds on the server.
    pub(super) found: RwLock<Vec<RemoteId>>,
    /// UIDs `set_flags_bulk` was asked to flag.
    pub(super) flagged_bulk: RwLock<Vec<RemoteId>>,
    /// UIDs `fetch_message_detail` was asked for.
    pub(super) fetched: RwLock<Vec<RemoteId>>,
    /// UIDs `move_messages` was asked to move.
    pub(super) moved: RwLock<Vec<RemoteId>>,
    /// Makes `move_messages` fail, as a server refusing it.
    pub(super) refuse: RwLock<bool>,
    /// `move_messages` moves, then times out before the server's answer.
    pub(super) time_out: RwLock<bool>,
    /// Makes `move_messages` fail once this many messages moved.
    pub(super) refuse_after: RwLock<Option<usize>>,
    /// Mailbox of each `set_flags_bulk` call.
    pub(super) flagged_in: RwLock<Vec<MailboxPath>>,
}

impl MockEmailProvider {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// One whose moves report `uid` as the destination UID.
    pub(super) fn moving_to(uid: u32) -> Self {
        Self {
            moved_uid: RwLock::new(Some(RemoteId::from(uid))),
            ..Self::default()
        }
    }
}

#[async_trait]
impl EmailProvider for MockEmailProvider {
    async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
        Ok(vec![])
    }
    async fn mailbox_changes(
        &self,
        _mailbox: &MailboxPath,
        _since: Option<&SyncCursor>,
    ) -> Result<MailboxChanges, DomainError> {
        Ok(MailboxChanges {
            cursor: SyncCursor::new("mock"),
            reset: None,
            added: vec![],
            existing: Existing::Snapshot(vec![]),
            counts: MailboxCounts {
                total: 0,
                unread: 0,
            },
        })
    }
    async fn fetch_envelopes(
        &self,
        _mailbox: &MailboxPath,
        _ids: &[RemoteId],
    ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
        Ok(vec![])
    }
    async fn fetch_message_detail(
        &self,
        _mailbox: &MailboxPath,
        id: &RemoteId,
    ) -> Result<Message, DomainError> {
        self.fetched.write().unwrap().push(id.clone());
        if let Some(during) = self.during_fetch.read().unwrap().as_ref() {
            during();
        }
        self.detail_response
            .read()
            .unwrap()
            .clone()
            .ok_or_else(|| DomainError::Provider("no detail configured".into()))
    }
    async fn find_by_message_id(
        &self,
        _mailbox: &MailboxPath,
        _rfc_id: &str,
    ) -> Result<Vec<RemoteId>, DomainError> {
        Ok(self.found.read().unwrap().clone())
    }
    async fn fetch_raw_message(
        &self,
        _mailbox: &MailboxPath,
        _id: &RemoteId,
    ) -> Result<Vec<u8>, DomainError> {
        Ok(self.raw.read().unwrap().clone())
    }
    async fn set_flags_bulk(
        &self,
        mailbox: &MailboxPath,
        uids: &[RemoteId],
        _add: &[Flag],
        _remove: &[Flag],
    ) -> Result<(), DomainError> {
        self.flagged_in.write().unwrap().push(mailbox.clone());
        self.flagged_bulk.write().unwrap().extend_from_slice(uids);
        Ok(())
    }
    async fn move_messages(
        &self,
        ids: &[RemoteId],
        _from: &MailboxPath,
        _to: &MailboxPath,
    ) -> Result<HashMap<RemoteId, Option<RemoteId>>, DomainError> {
        let mut moved = HashMap::new();
        for id in ids {
            let done = self.moved.read().unwrap().len();
            if *self.refuse.read().unwrap()
                || self.refuse_after.read().unwrap().is_some_and(|n| done >= n)
            {
                return Err(DomainError::Provider("refused".into()));
            }
            self.moved.write().unwrap().push(id.clone());
            if *self.time_out.read().unwrap() {
                return Err(DomainError::ConnectionTimeout("move".into()));
            }
            moved.insert(id.clone(), self.moved_uid.read().unwrap().clone());
        }
        Ok(moved)
    }
    async fn delete_messages(
        &self,
        _mailbox: &MailboxPath,
        _ids: &[RemoteId],
        _permanently: bool,
    ) -> Result<(), DomainError> {
        Ok(())
    }
    async fn send_message(&self, message: &OutgoingMessage) -> Result<(), DomainError> {
        self.sent.write().unwrap().push(message.clone());
        Ok(())
    }
    async fn append_message(
        &self,
        mailbox: &MailboxPath,
        message: &OutgoingMessage,
        flags: &[Flag],
    ) -> Result<Option<RemoteId>, DomainError> {
        self.appended
            .write()
            .unwrap()
            .push((mailbox.clone(), message.clone(), flags.to_vec()));
        Ok(Some(RemoteId::from(7)))
    }
    fn watch_mailbox(
        &self,
        _mailbox: &MailboxPath,
    ) -> Pin<Box<dyn futures::Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>>
    {
        Box::pin(futures::stream::empty())
    }
    async fn fetch_bodies_bulk(
        &self,
        _mailbox: &MailboxPath,
        _uids: &[RemoteId],
    ) -> Result<Vec<mc_domain::traits::email_provider::MessageBodyResult>, DomainError> {
        Ok(vec![])
    }
}

// ── Mock ProviderRegistry ──────────────────────────────────────────

pub(super) struct MockProviderRegistry {
    pub(super) provider: Arc<MockEmailProvider>,
}

impl MockProviderRegistry {
    pub(super) fn new(provider: Arc<MockEmailProvider>) -> Self {
        Self { provider }
    }
}

impl ProviderRegistry for MockProviderRegistry {
    type Provider = MockEmailProvider;

    fn get_provider(&self, _account_id: &AccountId) -> Result<Arc<MockEmailProvider>, DomainError> {
        Ok(self.provider.clone())
    }

    fn register_provider(&self, _account_id: AccountId, _provider: Arc<MockEmailProvider>) {}

    fn remove_provider(&self, _account_id: &AccountId) {}
}

// ── Mock MessageReadRepository ─────────────────────────────────────

/// (message_id, body_text, body_html) as handed to `MessageBodyStore::put_body`.
pub(super) type StoredBody = (MessageId, Option<String>, Option<String>);

#[derive(Default)]
pub(super) struct MockMessageReadRepo {
    pub(super) mailboxes: Vec<mc_domain::Mailbox>,
    pub(super) messages: Vec<Message>,
    pub(super) stored_bodies: std::sync::Mutex<Vec<StoredBody>>,
    pub(super) unread: Vec<mc_domain::MessageSummary>,
    /// UIDs other local rows already hold.
    pub(super) held: Vec<RemoteId>,
    /// What `share_bodies` hands out: copies given another's body.
    pub(super) shared: Vec<(MessageId, bool)>,
}

impl MockMessageReadRepo {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn with_mailboxes(mailboxes: Vec<mc_domain::Mailbox>) -> Self {
        Self {
            mailboxes,
            ..Self::default()
        }
    }
}

#[async_trait]
impl MessageBodyStore for MockMessageReadRepo {
    async fn put_body(
        &self,
        message_id: &MessageId,
        body_text: Option<String>,
        body_html: Option<String>,
        _truncated: bool,
    ) -> Result<(), DomainError> {
        self.stored_bodies
            .lock()
            .unwrap()
            .push((*message_id, body_text, body_html));
        Ok(())
    }

    async fn share_bodies(
        &self,
        _account_id: &AccountId,
        _rfc_message_ids: &[String],
    ) -> Result<Vec<(MessageId, bool)>, DomainError> {
        Ok(self.shared.clone())
    }
}

#[async_trait]
impl mc_domain::MailboxReadRepository for MockMessageReadRepo {
    async fn get_mailbox(&self, id: &MailboxId) -> Result<Option<mc_domain::Mailbox>, DomainError> {
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
    async fn get_message(&self, id: &MessageId) -> Result<Option<Message>, DomainError> {
        Ok(self.messages.iter().find(|m| m.id == *id).cloned())
    }
    async fn list_messages(
        &self,
        _mailbox_id: &MailboxId,
        _pagination: &mc_shared::pagination::Pagination,
    ) -> Result<mc_shared::pagination::PagedResult<mc_domain::MessageSummary>, DomainError> {
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
    async fn find_by_remote_id(
        &self,
        _mailbox_id: &MailboxId,
        _remote_id: &RemoteId,
    ) -> Result<Option<mc_domain::MessageSummary>, DomainError> {
        Ok(None)
    }

    async fn held_ids(
        &self,
        _mailbox_id: &MailboxId,
        uids: &[RemoteId],
    ) -> Result<std::collections::HashSet<RemoteId>, DomainError> {
        Ok(self
            .held
            .iter()
            .filter(|uid| uids.contains(uid))
            .cloned()
            .collect())
    }
    async fn pending_messages(
        &self,
        _mailbox_id: &MailboxId,
    ) -> Result<Vec<MessageId>, DomainError> {
        Ok(vec![])
    }
    async fn find_by_rfc_id(
        &self,
        _account_id: &AccountId,
        _rfc_id: &str,
    ) -> Result<Vec<mc_domain::MessageSummary>, DomainError> {
        Ok(vec![])
    }

    async fn find_unread_by_mailbox(
        &self,
        _mailbox_id: &MailboxId,
    ) -> Result<Vec<mc_domain::MessageSummary>, DomainError> {
        Ok(self.unread.clone())
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

pub(super) struct MockEventBus {
    pub(super) published: RwLock<Vec<EventEnvelope>>,
}

impl MockEventBus {
    pub(super) fn new() -> Self {
        Self {
            published: RwLock::new(Vec::new()),
        }
    }

    pub(super) fn last_event_name(&self) -> Option<String> {
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
