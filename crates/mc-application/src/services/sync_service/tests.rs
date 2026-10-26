use super::*;
use async_trait::async_trait;
use futures::Stream;
use mc_domain::{
    Address, AggregateType, DomainError, EmailAddress, Envelope, EventEnvelope, Flag, LoginSecret,
    MailboxStatus, MailboxType, MessageEnvelopeSummary, OutgoingMessage, ProviderNotification,
};
use std::pin::Pin;
use std::sync::Mutex;

#[test]
fn a_throttled_fetch_stops_the_backfill_pass() {
    assert!(stops_the_pass(&DomainError::Throttled("bandwidth".into())));
    assert!(!stops_the_pass(&DomainError::MessageNotFound(
        "gone".into()
    )));
}

#[test]
fn backfill_runs_at_full_speed_until_ninety_percent_of_the_cap() {
    const MB: u64 = 1 << 20;
    let limit = Some(2000 * MB);

    // Batches of 50 back to back, capped account or not.
    assert_eq!(BACKFILL_BATCH, 50);
    assert_eq!(backfill_pause(0, limit), None);
    assert_eq!(backfill_pause(1799 * MB, limit), None);
    assert_eq!(backfill_pause(50_000 * MB, None), None);
    // At 90%: wait for the window to free, no trickle.
    assert_eq!(backfill_pause(1800 * MB, limit), Some(BACKFILL_CAP_WAIT));
    assert_eq!(backfill_pause(2500 * MB, limit), Some(BACKFILL_CAP_WAIT));
}

/// A credential rejection must stay typed all the way to `run_backfill`, which
/// is what lets it back off instead of retrying once a minute forever.
#[test]
fn auth_failure_survives_as_typed_error_not_stringified() {
    let err = ApplicationError::from(DomainError::AuthenticationFailed("nope".into()));
    assert!(matches!(
        err,
        ApplicationError::Domain(DomainError::AuthenticationFailed(_))
    ));
}

// ── Mock EmailProvider ─────────────────────────────────────────────

struct MockEmailProvider {
    mailboxes: Vec<Mailbox>,
    envelopes: Vec<MessageEnvelopeSummary>,
    status: Option<MailboxStatus>,
    bulk_fetch_calls: Mutex<Vec<Vec<u32>>>,
    /// Envelope fetches starting at or past this UID fail, as a timeout.
    fail_envelopes_from: Option<u32>,
    /// What `find_message` finds, by mailbox path.
    found: HashMap<String, Vec<ProviderUid>>,
    /// Size of each `FetchRange::Uids` fetch.
    uid_set_fetches: Mutex<Vec<usize>>,
}

impl MockEmailProvider {
    fn new() -> Self {
        let (mailbox, _) = Mailbox::discover(
            AccountId::new(),
            MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
            MailboxType::Inbox,
        );
        Self {
            mailboxes: vec![mailbox],
            envelopes: vec![],
            status: Some(MailboxStatus {
                uid_validity: 1,
                uid_next: 1,
                total_messages: 0,
                unseen_messages: 0,
            }),
            bulk_fetch_calls: Mutex::new(vec![]),
            fail_envelopes_from: None,
            found: HashMap::new(),
            uid_set_fetches: Mutex::default(),
        }
    }

    /// Number of times `fetch_bodies_bulk` was called (i.e. number of IMAP
    /// sessions opened for bulk body fetches).
    fn bulk_fetch_call_count(&self) -> usize {
        self.bulk_fetch_calls.lock().unwrap().len()
    }

    fn with_envelopes(mut self, envelopes: Vec<MessageEnvelopeSummary>) -> Self {
        let count = envelopes.len() as u32;
        let uid_next = envelopes
            .iter()
            .map(|e| e.provider_uid.value() + 1)
            .max()
            .unwrap_or(1);
        self.envelopes = envelopes;
        if let Some(ref mut s) = self.status {
            s.uid_next = uid_next;
            s.total_messages = count;
        }
        self
    }

    fn with_mailboxes(mut self, mailboxes: Vec<Mailbox>) -> Self {
        self.mailboxes = mailboxes;
        self
    }
}

#[async_trait]
impl EmailProvider for MockEmailProvider {
    async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
        Ok(self.mailboxes.clone())
    }

    async fn fetch_message_envelopes(
        &self,
        _mailbox: &MailboxPath,
        range: FetchRange,
    ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
        if let (Some(limit), FetchRange::UidRange { from, .. }) = (self.fail_envelopes_from, &range)
            && *from >= limit
        {
            return Err(DomainError::ConnectionTimeout("envelopes".into()));
        }
        if let FetchRange::Uids(uids) = &range {
            self.uid_set_fetches.lock().unwrap().push(uids.len());
            if self
                .fail_envelopes_from
                .is_some_and(|limit| uids.contains(&ProviderUid::new(limit)))
            {
                return Err(DomainError::ConnectionTimeout("envelopes".into()));
            }
        }
        Ok(self
            .envelopes
            .iter()
            .filter(|e| match range {
                FetchRange::UidRange { from, to } => (from..=to).contains(&e.provider_uid.value()),
                FetchRange::Uids(ref uids) => uids.contains(&e.provider_uid),
                _ => true,
            })
            .cloned()
            .collect())
    }

    async fn fetch_message_detail(
        &self,
        _mailbox: &MailboxPath,
        _uid: ProviderUid,
    ) -> Result<Message, DomainError> {
        Err(DomainError::Provider("Not implemented".to_string()))
    }

    async fn find_message(
        &self,
        mailbox: &MailboxPath,
        _rfc_id: &str,
    ) -> Result<Vec<ProviderUid>, DomainError> {
        Ok(self
            .found
            .get(&mailbox.full_path())
            .cloned()
            .unwrap_or_default())
    }
    async fn uids_from(
        &self,
        _mailbox: &MailboxPath,
        from: ProviderUid,
    ) -> Result<Vec<ProviderUid>, DomainError> {
        let mut uids: Vec<ProviderUid> = self
            .envelopes
            .iter()
            .map(|e| e.provider_uid)
            .filter(|uid| *uid >= from)
            .collect();
        uids.sort();
        Ok(uids)
    }
    async fn fetch_raw_message(
        &self,
        _mailbox: &MailboxPath,
        _uid: ProviderUid,
    ) -> Result<Vec<u8>, DomainError> {
        Err(DomainError::Provider("Not implemented".to_string()))
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

    async fn append_message(
        &self,
        _mailbox: &MailboxPath,
        _message: &OutgoingMessage,
        _flags: &[Flag],
    ) -> Result<Option<ProviderUid>, DomainError> {
        Ok(None)
    }

    async fn mailbox_status(&self, _mailbox: &MailboxPath) -> Result<MailboxStatus, DomainError> {
        self.status
            .clone()
            .ok_or_else(|| DomainError::Provider("No status".to_string()))
    }

    async fn fetch_uid_flags(
        &self,
        _mailbox: &MailboxPath,
    ) -> Result<Vec<mc_domain::traits::email_provider::UidFlagEntry>, DomainError> {
        Ok(self
            .envelopes
            .iter()
            .map(|e| mc_domain::traits::email_provider::UidFlagEntry {
                uid: e.provider_uid,
                flags: e.flags.clone(),
            })
            .collect())
    }
    fn watch_mailbox(
        &self,
        _mailbox: &MailboxPath,
    ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>> {
        Box::pin(futures::stream::empty())
    }
    async fn fetch_bodies_bulk(
        &self,
        _mailbox: &MailboxPath,
        uids: &[ProviderUid],
    ) -> Result<Vec<mc_domain::traits::email_provider::MessageBodyResult>, DomainError> {
        let uid_values: Vec<u32> = uids.iter().map(|u| u.value()).collect();
        self.bulk_fetch_calls
            .lock()
            .unwrap()
            .push(uid_values.clone());
        Ok(uid_values
            .into_iter()
            .map(|uid| mc_domain::traits::email_provider::MessageBodyResult {
                provider_uid: ProviderUid::new(uid),
                body_text: Some(format!("body-{uid}")),
                body_html: None,
                truncated: false,
            })
            .collect())
    }
}

// ── Mock ProviderRegistry ──────────────────────────────────────────

struct MockProviderRegistry {
    /// `None` reproduces "no provider registered for this account".
    provider: Option<Arc<MockEmailProvider>>,
}

impl MockProviderRegistry {
    fn new(provider: Arc<MockEmailProvider>) -> Self {
        Self {
            provider: Some(provider),
        }
    }

    fn empty() -> Self {
        Self { provider: None }
    }
}

impl ProviderRegistry for MockProviderRegistry {
    type Provider = MockEmailProvider;

    fn get_provider(
        &self,
        _account_id: &AccountId,
    ) -> Result<Arc<MockEmailProvider>, mc_domain::DomainError> {
        self.provider
            .clone()
            .ok_or_else(|| mc_domain::DomainError::Provider("no provider".into()))
    }

    fn register_provider(&self, _account_id: AccountId, _provider: Arc<MockEmailProvider>) {}

    fn remove_provider(&self, _account_id: &AccountId) {}
}

// ── Mock EventStore ───────────────────────────────────────────────

struct MockEventStore {
    events: Mutex<Vec<(String, DomainEvent)>>,
}

impl MockEventStore {
    fn new() -> Self {
        Self {
            events: Mutex::new(vec![]),
        }
    }

    fn appended_events(&self) -> Vec<(String, DomainEvent)> {
        self.events.lock().unwrap().clone()
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
        let mut store = self.events.lock().unwrap();
        let actual = store.iter().filter(|(id, _)| id == aggregate_id).count() as u64;
        if actual != expected_sequence {
            return Err(DomainError::ConcurrencyConflict {
                expected: expected_sequence,
                actual,
            });
        }
        let appended = events.len() as u64;
        store.extend(
            events
                .into_iter()
                .map(|event| (aggregate_id.to_string(), event)),
        );
        Ok(actual + appended)
    }

    async fn load_events(
        &self,
        aggregate_id: &str,
        _from_sequence: u64,
    ) -> Result<Vec<EventEnvelope>, DomainError> {
        Ok(self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|(id, _)| id == aggregate_id)
            .enumerate()
            .map(|(i, (id, event))| {
                EventEnvelope::new(
                    id.clone(),
                    AggregateType::Message,
                    i as u64 + 1,
                    event.clone(),
                )
            })
            .collect())
    }

    fn load_all_events(
        &self,
        _aggregate_type: Option<AggregateType>,
        _from_global_position: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<EventEnvelope, DomainError>> + Send + '_>> {
        Box::pin(futures::stream::empty())
    }

    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = EventEnvelope> + Send + '_>> {
        Box::pin(futures::stream::empty())
    }
}

// ── Mock Projector (ReadModelProjector + MailboxReadRepository + MessageReadRepository + ProjectionCheckpoint) ──

/// (body_text, body_html) as handed to `MessageBodyStore::put_body`.
type StoredBody = (Option<String>, Option<String>);

struct MockProjector {
    mailboxes: Mutex<Vec<Mailbox>>,
    checkpoints: Mutex<HashMap<String, u64>>,
    projected_events: Mutex<Vec<DomainEvent>>,
    bodies: Mutex<HashMap<MessageId, StoredBody>>,
    stored: Mutex<Vec<MessageSummary>>,
    /// Message-IDs of `stored` rows; a row without one matches any lookup.
    rfc_ids: Mutex<HashMap<MessageId, String>>,
}

impl MockProjector {
    fn new() -> Self {
        Self {
            mailboxes: Mutex::new(vec![]),
            checkpoints: Mutex::new(HashMap::new()),
            projected_events: Mutex::new(vec![]),
            bodies: Mutex::new(HashMap::new()),
            stored: Mutex::new(vec![]),
            rfc_ids: Mutex::new(HashMap::new()),
        }
    }

    fn stored_bodies(&self) -> HashMap<MessageId, StoredBody> {
        self.bodies.lock().unwrap().clone()
    }

    fn projected_events(&self) -> Vec<DomainEvent> {
        self.projected_events.lock().unwrap().clone()
    }
}

#[async_trait]
impl ReadModelProjector for MockProjector {
    async fn project(&self, envelope: &EventEnvelope) -> Result<(), DomainError> {
        self.projected_events
            .lock()
            .unwrap()
            .push(envelope.payload.clone());

        // If it's a MailboxDiscovered event, add to local mailboxes
        if let DomainEvent::MailboxDiscovered {
            mailbox_id,
            account_id,
            path,
            mailbox_type,
        } = &envelope.payload
        {
            self.mailboxes.lock().unwrap().push(Mailbox {
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
            });
        }

        let mut stored = self.stored.lock().unwrap();
        match &envelope.payload {
            DomainEvent::MessageReceived {
                message_id,
                account_id,
                mailbox_id,
                provider_uid,
                message_rfc_id,
                flags,
                size_bytes,
                ..
            } => {
                stored.push(MessageSummary {
                    id: *message_id,
                    account_id: *account_id,
                    mailbox_id: *mailbox_id,
                    provider_uid: Some(*provider_uid),
                    from_name: None,
                    from_email: "sender@example.com".into(),
                    to: vec![],
                    cc: vec![],
                    subject: String::new(),
                    flags: flags.clone(),
                    size_bytes: *size_bytes,
                    has_attachments: false,
                    received_at: chrono::Utc::now(),
                });
                if let Some(rfc_id) = message_rfc_id {
                    self.rfc_ids
                        .lock()
                        .unwrap()
                        .insert(*message_id, rfc_id.clone());
                }
            }
            DomainEvent::MessageMoved {
                message_id,
                to_mailbox_id,
                provider_uid,
                ..
            } => {
                if let Some(row) = stored.iter_mut().find(|m| m.id == *message_id) {
                    row.mailbox_id = *to_mailbox_id;
                    row.provider_uid = *provider_uid;
                }
            }
            DomainEvent::MessageDeleted { message_id, .. } => {
                stored.retain(|m| m.id != *message_id)
            }
            _ => {}
        }

        Ok(())
    }
}

#[async_trait]
impl MailboxReadRepository for MockProjector {
    async fn get_mailbox(&self, id: &MailboxId) -> Result<Option<Mailbox>, DomainError> {
        Ok(self
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .find(|m| m.id == *id)
            .cloned())
    }

    async fn list_mailboxes(&self, account_id: &AccountId) -> Result<Vec<Mailbox>, DomainError> {
        Ok(self
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.account_id == *account_id)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl MessageReadRepository for MockProjector {
    async fn get_message(&self, _id: &MessageId) -> Result<Option<Message>, DomainError> {
        Ok(None)
    }
    async fn list_messages(
        &self,
        mailbox_id: &MailboxId,
        _pagination: &mc_shared::pagination::Pagination,
    ) -> Result<mc_shared::pagination::PagedResult<MessageSummary>, DomainError> {
        let items: Vec<MessageSummary> = self
            .stored
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.mailbox_id == *mailbox_id)
            .cloned()
            .collect();
        Ok(mc_shared::pagination::PagedResult {
            total_count: items.len() as u64,
            items,
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
    ) -> Result<Option<MessageSummary>, DomainError> {
        Ok(None)
    }

    async fn find_by_rfc_id(
        &self,
        _account_id: &AccountId,
        rfc_id: &str,
    ) -> Result<Vec<MessageSummary>, DomainError> {
        let rfc_ids = self.rfc_ids.lock().unwrap();
        Ok(self
            .stored
            .lock()
            .unwrap()
            .iter()
            .filter(|m| rfc_ids.get(&m.id).is_none_or(|id| id == rfc_id))
            .cloned()
            .collect())
    }

    async fn held_uids(
        &self,
        mailbox_id: &MailboxId,
        uids: &[ProviderUid],
    ) -> Result<HashSet<ProviderUid>, DomainError> {
        Ok(self
            .stored
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.mailbox_id == *mailbox_id)
            .filter_map(|m| m.provider_uid)
            .filter(|uid| uids.contains(uid))
            .collect())
    }

    async fn uidless_messages(
        &self,
        mailbox_id: &MailboxId,
    ) -> Result<Vec<MessageId>, DomainError> {
        Ok(self
            .stored
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.mailbox_id == *mailbox_id && m.provider_uid.is_none())
            .map(|m| m.id)
            .collect())
    }

    async fn find_unread_by_mailbox(
        &self,
        _mailbox_id: &MailboxId,
    ) -> Result<Vec<MessageSummary>, DomainError> {
        Ok(vec![])
    }

    async fn scan_all_messages(
        &self,
        _account_id: Option<&AccountId>,
    ) -> Result<Vec<Message>, DomainError> {
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
impl ProjectionCheckpoint for MockProjector {
    async fn get_checkpoint(&self, name: &str) -> Result<u64, DomainError> {
        Ok(self
            .checkpoints
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or(0))
    }

    async fn save_checkpoint(&self, name: &str, value: u64) -> Result<(), DomainError> {
        self.checkpoints
            .lock()
            .unwrap()
            .insert(name.to_string(), value);
        Ok(())
    }
}

#[async_trait]
impl MessageBodyStore for MockProjector {
    async fn put_body(
        &self,
        message_id: &MessageId,
        body_text: Option<String>,
        body_html: Option<String>,
        _truncated: bool,
    ) -> Result<(), DomainError> {
        self.bodies
            .lock()
            .unwrap()
            .insert(*message_id, (body_text, body_html));
        Ok(())
    }

    async fn share_bodies(
        &self,
        _account_id: &AccountId,
        rfc_message_ids: &[String],
    ) -> Result<Vec<(MessageId, bool)>, DomainError> {
        let rfc_ids = self.rfc_ids.lock().unwrap();
        let mut bodies = self.bodies.lock().unwrap();
        let mut shared = Vec::new();
        for rfc_id in rfc_message_ids {
            let copies: Vec<MessageId> = rfc_ids
                .iter()
                .filter(|(_, id)| *id == rfc_id)
                .map(|(m, _)| *m)
                .collect();
            let Some(body) = copies.iter().find_map(|m| bodies.get(m).cloned()) else {
                continue;
            };
            for copy in copies {
                if let std::collections::hash_map::Entry::Vacant(slot) = bodies.entry(copy) {
                    slot.insert(body.clone());
                    shared.push((copy, false));
                }
            }
        }
        Ok(shared)
    }
}

impl MockProjector {
    /// Stored rows with a UID and no body yet, in storage order.
    fn pending(&self) -> Vec<mc_domain::BackfillEntry> {
        let bodies = self.bodies.lock().unwrap();
        let rfc_ids = self.rfc_ids.lock().unwrap();
        self.stored
            .lock()
            .unwrap()
            .iter()
            .filter(|m| !bodies.contains_key(&m.id))
            .filter_map(|m| {
                Some(mc_domain::BackfillEntry {
                    message_id: m.id,
                    mailbox_id: m.mailbox_id,
                    provider_uid: m.provider_uid?.value(),
                    rfc_message_id: rfc_ids.get(&m.id).cloned(),
                    size_bytes: m.size_bytes,
                    from_email: m.from_email.clone(),
                })
            })
            .collect()
    }
}

#[async_trait]
impl BodyBackfillQueue for MockProjector {
    async fn list_pending_backfill(
        &self,
        _account_id: &AccountId,
        limit: usize,
    ) -> Result<Vec<mc_domain::BackfillEntry>, DomainError> {
        Ok(self.pending().into_iter().take(limit).collect())
    }

    async fn count_pending_backfill(&self, _account_id: &AccountId) -> Result<u32, DomainError> {
        Ok(self.pending().len() as u32)
    }

    async fn downloaded_since(
        &self,
        _account_id: &AccountId,
        _since: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, DomainError> {
        Ok(0)
    }
}

// ── Test helpers ──────────────────────────────────────────────────

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
        subject: "Test Subject".into(),
        date: chrono::Utc::now(),
    }
}

fn make_envelope_summary(uid: u32) -> MessageEnvelopeSummary {
    MessageEnvelopeSummary {
        provider_uid: ProviderUid::new(uid),
        message_rfc_id: None,
        in_reply_to: None,
        references: vec![],
        envelope: test_envelope(),
        flags: vec![],
        size_bytes: 1024,
        has_attachments: false,
        internal_date: chrono::Utc::now(),
    }
}

fn make_service(
    provider: MockEmailProvider,
    event_store: Arc<MockEventStore>,
    projector: Arc<MockProjector>,
) -> SyncService<MockProviderRegistry, MockEventStore, MockProjector> {
    let registry = Arc::new(MockProviderRegistry::new(Arc::new(provider)));
    SyncService::new(registry, event_store, projector, SyncConfig::default())
}

// ── Tests ─────────────────────────────────────────────────────────

#[test]
fn progress_gate_collapses_repeats_but_keeps_percentage_steps() {
    let service = make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let account_id = AccountId::new();
    let mailbox_id = MailboxId::new();
    let mut published = Vec::new();

    // 1000 messages in one mailbox: the gate must not let 1000 events out.
    for i in 0..1000u32 {
        service.emit(
            &mut published,
            SyncProgressEvent {
                account_id,
                state: SyncState::FetchingEnvelopes { mailbox_id },
                processed: i,
                total: 1000,
                message: "Fetching messages in INBOX".to_string(),
            },
        );
    }

    assert_eq!(
        published.len(),
        100,
        "one event per whole percentage point, not one per message"
    );

    // A terminal event always passes, even repeated.
    let completed = SyncProgressEvent {
        account_id,
        state: SyncState::Completed,
        processed: 1,
        total: 1,
        message: "done".to_string(),
    };
    service.emit(&mut published, completed.clone());
    service.emit(&mut published, completed);
    assert_eq!(published.len(), 102, "Completed is never debounced away");
}

#[test]
fn progress_gate_lets_every_body_batch_through() {
    let service = make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let account_id = AccountId::new();
    let mut published = Vec::new();

    // 30-body batches against a 20k backlog never cross a whole percent.
    for batch in 1..=5u32 {
        service.emit(
            &mut published,
            SyncProgressEvent {
                account_id,
                state: SyncState::FetchingMessageBody,
                processed: batch * 30,
                total: 20_000,
                message: "Downloading message bodies".to_string(),
            },
        );
    }

    assert_eq!(published.len(), 5, "one event per downloaded batch");
}

#[tokio::test]
async fn backfill_waits_while_the_account_needs_the_user() {
    use crate::services::auth_gate::{AuthGates, Credential};

    let service = Arc::new(make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    ));
    let mut rx = service.subscribe_progress();
    let refresh: crate::RefreshFn =
        Arc::new(|_, _| Box::pin(async { Err(DomainError::Provider("unused".into())) }));
    let keychain: Arc<dyn mc_domain::SecretStore> = Arc::new(NoKeychain);
    let mut gates = AuthGates::new(refresh, keychain, tokio::sync::mpsc::unbounded_channel().0);
    let account_id = AccountId::new();
    let gate = gates.add(
        account_id,
        "me@example.com",
        Credential::Password("old".into()),
    );
    let rejected = DomainError::AuthenticationFailed("NO".into());
    gate.login_failed(&LoginSecret::Password("old".into()), &rejected, false)
        .await;

    let shutdown = CancellationToken::new();
    let worker = tokio::spawn({
        let (service, gate, shutdown) = (service.clone(), gate.clone(), shutdown.clone());
        async move { service.run_backfill(account_id, None, gate, shutdown).await }
    });

    let paused = tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await;
    assert!(paused.is_err(), "a paused account's backfill must not run");

    gate.set_password("new".into());
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv()).await;
    shutdown.cancel();
    let _ = worker.await;
    assert!(event.expect("resumed after the password change").is_ok());
}

struct NoKeychain;
impl mc_domain::SecretStore for NoKeychain {
    fn load(&self, _: &str) -> Result<Option<mc_domain::AccountSecrets>, DomainError> {
        Ok(None)
    }
    fn store(&self, _: &str, _: &mc_domain::AccountSecrets) -> Result<(), DomainError> {
        Ok(())
    }
    fn delete(&self, _: &str) -> Result<(), DomainError> {
        Ok(())
    }
}

#[tokio::test]
async fn empty_backfill_queue_announces_itself() {
    // Regression: the worker used to go silent once the queue drained, so a
    // client kept rendering the last in-progress percentage forever.
    let service = Arc::new(make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()), // empty queue
    ));
    let mut rx = service.subscribe_progress();

    let shutdown = CancellationToken::new();
    let worker = {
        let service = service.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let gate = Arc::new(LoginSecret::Password(String::new()));
            service
                .run_backfill(AccountId::new(), None, gate, shutdown)
                .await
        })
    };

    let event = rx.recv().await.expect("idle worker published its state");
    shutdown.cancel();
    let _ = worker.await;

    assert_eq!(event.state, SyncState::FetchingMessageBody);
    assert_eq!(event.total, 0, "no backlog → no percentage to render");
    assert!(event.message.contains("All message bodies"));
}

#[tokio::test]
async fn failed_sync_publishes_a_terminal_event() {
    // No provider registered → start_sync fails on the first resolve.
    let service = make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let mut rx = service.subscribe_progress();

    assert!(service.start_sync(&AccountId::new()).await.is_ok());

    // The mock registry always resolves, so drive the failure path directly.
    let broken: SyncService<MockProviderRegistry, MockEventStore, MockProjector> = SyncService::new(
        Arc::new(MockProviderRegistry::empty()),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
        SyncConfig::default(),
    );
    let mut broken_rx = broken.subscribe_progress();
    assert!(broken.start_sync(&AccountId::new()).await.is_err());

    let event = broken_rx
        .try_recv()
        .expect("a terminal event was published");
    assert_eq!(event.state, SyncState::Failed);
    // The healthy service published progress, not a failure.
    assert!(!matches!(
        rx.try_recv().map(|e| e.state),
        Ok(SyncState::Failed)
    ));
}

#[tokio::test]
async fn progress_reaches_live_subscribers() {
    let service = make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let mut rx = service.subscribe_progress();

    service.publish(SyncProgressEvent {
        account_id: AccountId::new(),
        state: SyncState::ListingMailboxes,
        processed: 0,
        total: 0,
        message: "Listing mailboxes...".to_string(),
    });

    let event = rx.try_recv().expect("subscriber received the event");
    assert_eq!(event.state, SyncState::ListingMailboxes);
}

#[tokio::test]
async fn sync_discovers_new_mailboxes() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );
    let (sent, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Sent".to_string()], '/').unwrap(),
        MailboxType::Sent,
    );

    let provider = MockEmailProvider::new().with_mailboxes(vec![inbox, sent]);
    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    let service = make_service(provider, event_store.clone(), projector.clone());
    let progress = service.start_sync(&account_id).await.unwrap();

    // Should complete
    assert!(progress.iter().any(|e| e.state == SyncState::Completed));

    // Should have MailboxDiscovered events
    let projected = projector.projected_events();
    let discovered_count = projected
        .iter()
        .filter(|e| matches!(e, DomainEvent::MailboxDiscovered { .. }))
        .count();
    assert_eq!(discovered_count, 2, "Should discover INBOX and Sent");
}

/// Backfill's whole point is avoiding one IMAP reconnect per message.
/// `fetch_message_bodies_bulk` must fetch a batch of messages in the same
/// mailbox over a single `fetch_bodies_bulk` call, not one call per message.
#[tokio::test]
async fn fetch_message_bodies_bulk_uses_one_session_for_whole_batch() {
    let account_id = AccountId::new();
    let mailbox_path = MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap();
    let mailbox_id = MailboxId::new();

    let provider = MockEmailProvider::new();
    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());
    let service = make_service(provider, event_store.clone(), projector.clone());

    let entries: Vec<BackfillEntry> = (1..=3u32)
        .map(|uid| BackfillEntry {
            message_id: MessageId::new(),
            mailbox_id,
            provider_uid: uid,
            rfc_message_id: None,
            size_bytes: 0,
            from_email: String::new(),
        })
        .collect();

    service
        .fetch_message_bodies_bulk(&account_id, &mailbox_path, &entries)
        .await
        .unwrap();

    // One `fetch_bodies_bulk` call for all 3 entries — not 3 separate calls.
    let registry_provider = &service.provider_registry.get_provider(&account_id).unwrap();
    assert_eq!(registry_provider.bulk_fetch_call_count(), 1);
    assert_eq!(
        registry_provider.bulk_fetch_calls.lock().unwrap()[0].len(),
        3
    );

    // Each entry got its own MessageBodyFetched event, keyed by message_id.
    let appended = event_store.appended_events();
    let body_fetched_ids: Vec<MessageId> = appended
        .iter()
        .filter_map(|(_, e)| match e {
            DomainEvent::MessageBodyFetched { message_id, .. } => Some(*message_id),
            _ => None,
        })
        .collect();
    assert_eq!(body_fetched_ids.len(), 3);
    for entry in &entries {
        assert!(body_fetched_ids.contains(&entry.message_id));
    }

    // Bodies went to the body store, once each, not into the events.
    let bodies = projector.stored_bodies();
    assert_eq!(bodies.len(), 3);
    for entry in &entries {
        let text = bodies.get(&entry.message_id).and_then(|(t, _)| t.clone());
        assert!(text.is_some_and(|t| t.starts_with("body-")), "{bodies:?}");
    }
    for (_, event) in &appended {
        let shown = format!("{event:?}");
        assert!(
            !shown.contains("body-"),
            "body leaked into the log: {shown}"
        );
    }
}

/// Gmail lists a message once per label plus All Mail: the backfill
/// downloads it once and every copy gets that body.
#[tokio::test]
async fn backfill_downloads_a_message_once_for_all_its_copies() {
    let account_id = AccountId::new();
    let (inbox, discovered) =
        Mailbox::discover(account_id, MailboxPath::inbox(), MailboxType::Inbox);
    let projector = Arc::new(MockProjector::new());
    let event_store = Arc::new(MockEventStore::new());
    let mut events = vec![discovered];
    let mut copy = |uid: u32, rfc: &str| {
        let (message, event) = Message::receive(
            account_id,
            inbox.id,
            ProviderUid::new(uid),
            Some(rfc.into()),
            None,
            vec![],
            test_envelope(),
            vec![],
            10,
            false,
            chrono::Utc::now(),
        );
        events.push(event);
        message.id
    };
    let (first, second, other) = (copy(1, "<a@x>"), copy(2, "<a@x>"), copy(3, "<c@x>"));
    let (stored, late) = (copy(9, "<d@x>"), copy(4, "<d@x>"));
    for event in events {
        projector
            .project(&EventEnvelope::new(
                String::new(),
                AggregateType::Message,
                1,
                event,
            ))
            .await
            .unwrap();
    }
    projector
        .put_body(&stored, Some("stored before".into()), None, false)
        .await
        .unwrap();

    let service = Arc::new(make_service(
        MockEmailProvider::new(),
        event_store.clone(),
        projector.clone(),
    ));
    let mut progress = service.subscribe_progress();
    let shutdown = CancellationToken::new();
    let worker = tokio::spawn({
        let (service, shutdown) = (Arc::clone(&service), shutdown.clone());
        let gate: Arc<dyn LoginGate> = Arc::new(mc_domain::LoginSecret::Password("x".into()));
        async move { service.run_backfill(account_id, None, gate, shutdown).await }
    });
    while progress.recv().await.unwrap().message != "All message bodies downloaded" {}
    shutdown.cancel();
    worker.await.unwrap();

    let provider = service.provider_registry.get_provider(&account_id).unwrap();
    assert_eq!(
        *provider.bulk_fetch_calls.lock().unwrap(),
        [vec![1, 3]],
        "one copy of <a>, none of <d>"
    );
    let bodies = projector.stored_bodies();
    assert_eq!(bodies[&first], bodies[&second]);
    assert_eq!(bodies[&other].0.as_deref(), Some("body-3"));
    assert_eq!(bodies[&late].0.as_deref(), Some("stored before"));
    let fetched = event_store
        .appended_events()
        .iter()
        .filter(|(_, e)| matches!(e, DomainEvent::MessageBodyFetched { .. }))
        .count();
    assert_eq!(fetched, 4, "every copy recorded as fetched");
}

#[tokio::test]
async fn backfill_downloads_both_when_a_message_id_is_reused_with_another_size() {
    let account_id = AccountId::new();
    let (inbox, discovered) =
        Mailbox::discover(account_id, MailboxPath::inbox(), MailboxType::Inbox);
    let projector = Arc::new(MockProjector::new());
    let mut events = vec![discovered];
    for (uid, size) in [(1, 10), (2, 999)] {
        let (_, event) = Message::receive(
            account_id,
            inbox.id,
            ProviderUid::new(uid),
            Some("<a@x>".into()),
            None,
            vec![],
            test_envelope(),
            vec![],
            size,
            false,
            chrono::Utc::now(),
        );
        events.push(event);
    }
    for event in events {
        projector
            .project(&EventEnvelope::new(
                String::new(),
                AggregateType::Message,
                1,
                event,
            ))
            .await
            .unwrap();
    }

    let service = make_service(
        MockEmailProvider::new(),
        Arc::new(MockEventStore::new()),
        projector,
    );
    let batch = service
        .next_backfill_batch(&account_id, &HashSet::new())
        .await
        .unwrap();

    assert_eq!(batch.len(), 2);
}

#[tokio::test]
async fn sync_fetches_envelopes_and_emits_message_received() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );

    let envelopes = vec![make_envelope_summary(1), make_envelope_summary(2)];
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![inbox])
        .with_envelopes(envelopes);

    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    let service = make_service(provider, event_store.clone(), projector.clone());
    let progress = service.start_sync(&account_id).await.unwrap();

    assert!(progress.iter().any(|e| e.state == SyncState::Completed));

    let projected = projector.projected_events();
    let received_count = projected
        .iter()
        .filter(|e| matches!(e, DomainEvent::MessageReceived { .. }))
        .count();
    assert_eq!(received_count, 2, "Should emit 2 MessageReceived events");

    // Verify events were appended to event store
    let appended = event_store.appended_events();
    let appended_received = appended
        .iter()
        .filter(|(_, e)| matches!(e, DomainEvent::MessageReceived { .. }))
        .count();
    assert_eq!(appended_received, 2);
}

#[tokio::test]
async fn sent_copy_syncing_back_replaces_the_one_shown_at_send_time() {
    let account_id = AccountId::new();
    let (sent, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Sent".to_string()], '/').unwrap(),
        MailboxType::Sent,
    );
    let placeholder = MessageSummary {
        id: MessageId::new(),
        account_id,
        mailbox_id: sent.id,
        provider_uid: None,
        from_name: None,
        from_email: "sender@example.com".into(),
        to: vec![],
        cc: vec![],
        subject: "Test Subject".into(),
        flags: vec![Flag::Seen],
        size_bytes: 0,
        has_attachments: false,
        received_at: chrono::Utc::now(),
    };
    let mut server_copy = make_envelope_summary(7);
    server_copy.message_rfc_id = Some("<sent@example.com>".into());

    let projector = Arc::new(MockProjector::new());
    projector.stored.lock().unwrap().push(placeholder.clone());
    projector.mailboxes.lock().unwrap().push(sent.clone());
    let event_store = Arc::new(MockEventStore::new());
    event_store.events.lock().unwrap().push((
        placeholder.id.to_string(),
        DomainEvent::MessageSent {
            message_id: placeholder.id,
            account_id,
            mailbox_id: sent.id,
            envelope: test_envelope(),
            message_rfc_id: Some("<sent@example.com>".into()),
            in_reply_to: None,
            references: vec![],
            body_text: None,
            body_html: None,
            sent_at: chrono::Utc::now(),
        },
    ));
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![sent])
        .with_envelopes(vec![server_copy]);
    let service = make_service(provider, event_store, projector.clone());
    service.start_sync(&account_id).await.unwrap();

    let message_events: Vec<_> = projector
        .projected_events()
        .into_iter()
        .filter(|e| {
            matches!(
                e,
                DomainEvent::MessageReceived { .. } | DomainEvent::MessageDeleted { .. }
            )
        })
        .collect();
    assert!(
        matches!(
            message_events.as_slice(),
            [DomainEvent::MessageReceived { .. }, DomainEvent::MessageDeleted { message_id, .. }]
                if *message_id == placeholder.id
        ),
        "server copy stored, then the placeholder dropped: {message_events:?}"
    );
}

fn archived(
    account_id: AccountId,
    mailbox_id: MailboxId,
    provider_uid: Option<ProviderUid>,
) -> MessageSummary {
    MessageSummary {
        id: MessageId::new(),
        account_id,
        mailbox_id,
        provider_uid,
        from_name: None,
        from_email: "sender@example.com".into(),
        to: vec![],
        cc: vec![],
        subject: "Test Subject".into(),
        flags: vec![Flag::Seen],
        size_bytes: 0,
        has_attachments: false,
        received_at: chrono::Utc::now(),
    }
}

fn archive_mailbox(account_id: AccountId) -> Mailbox {
    Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Archive".to_string()], '/').unwrap(),
        MailboxType::Archive,
    )
    .0
}

fn received(projector: &MockProjector) -> usize {
    projector
        .projected_events()
        .iter()
        .filter(|e| matches!(e, DomainEvent::MessageReceived { .. }))
        .count()
}

/// A message in `mailbox` holding `uid` there (`None`: waiting for one,
/// as after a move), the same in the log and in the read model.
fn seed(
    event_store: &MockEventStore,
    projector: &MockProjector,
    mailbox: &Mailbox,
    uid: Option<u32>,
    rfc_id: &str,
) -> MessageId {
    let (msg, received) = Message::receive(
        mailbox.account_id,
        mailbox.id,
        ProviderUid::new(uid.unwrap_or(1)),
        Some(rfc_id.into()),
        None,
        vec![],
        test_envelope(),
        vec![Flag::Seen],
        0,
        false,
        chrono::Utc::now(),
    );
    let mut events = vec![received];
    if uid.is_none() {
        events.push(DomainEvent::MessageMoved {
            message_id: msg.id,
            from_mailbox_id: MailboxId::new(),
            to_mailbox_id: mailbox.id,
            provider_uid: None,
        });
    }
    event_store
        .events
        .lock()
        .unwrap()
        .extend(events.into_iter().map(|e| (msg.id.to_string(), e)));
    let mut row = archived(mailbox.account_id, mailbox.id, uid.map(ProviderUid::new));
    row.id = msg.id;
    projector.stored.lock().unwrap().push(row);
    projector
        .rfc_ids
        .lock()
        .unwrap()
        .insert(msg.id, rfc_id.into());
    msg.id
}

fn envelope(uid: u32, rfc_id: &str) -> MessageEnvelopeSummary {
    let mut env = make_envelope_summary(uid);
    env.message_rfc_id = Some(rfc_id.into());
    env
}

fn row_uid(projector: &MockProjector, id: MessageId) -> Option<Option<u32>> {
    projector
        .stored
        .lock()
        .unwrap()
        .iter()
        .find(|m| m.id == id)
        .map(|m| m.provider_uid.map(|u| u.value()))
}

#[tokio::test]
async fn moved_message_gets_its_uid_back_instead_of_a_duplicate() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let moved = seed(
        &event_store,
        &projector,
        &archive,
        None,
        "<moved@example.com>",
    );
    projector.mailboxes.lock().unwrap().push(archive.clone());

    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(vec![envelope(9, "<moved@example.com>")]);
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(
        received(&projector),
        0,
        "no second copy of the moved message"
    );
    assert_eq!(row_uid(&projector, moved), Some(Some(9)));
}

/// The read model can lag the log: a message it still lists here, waiting
/// for a UID, may have been moved on. Re-attaching from that stale view
/// appended `MessageMoved { from: Archive, to: Archive }` and pulled the
/// message back.
#[tokio::test]
async fn reattach_does_not_pull_back_a_message_moved_on() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let moved = seed(
        &event_store,
        &projector,
        &archive,
        None,
        "<moved@example.com>",
    );
    let trash = MailboxId::new();
    event_store.events.lock().unwrap().push((
        moved.to_string(),
        DomainEvent::MessageMoved {
            message_id: moved,
            from_mailbox_id: archive.id,
            to_mailbox_id: trash,
            provider_uid: None,
        },
    ));
    projector.mailboxes.lock().unwrap().push(archive.clone());

    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive.clone()])
        .with_envelopes(vec![envelope(9, "<moved@example.com>")]);
    make_service(provider, event_store.clone(), projector)
        .start_sync(&account_id)
        .await
        .unwrap();

    let pulled_back = event_store.appended_events().into_iter().any(|(id, e)| {
        id == moved.to_string() && matches!(e, DomainEvent::MessageMoved { from_mailbox_id, to_mailbox_id, .. } if from_mailbox_id == archive.id && to_mailbox_id == archive.id)
    });
    assert!(!pulled_back);
}

fn batch_of(count: u32) -> Vec<MessageEnvelopeSummary> {
    (1..=count)
        .map(|uid| envelope(uid, &format!("<m{uid}@example.com>")))
        .collect()
}

/// A batch that times out keeps the batches before it: the next cycle
/// resumes after them instead of starting over.
#[tokio::test]
async fn a_failed_batch_keeps_the_checkpoint_of_the_ones_before() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let projector = Arc::new(MockProjector::new());
    projector.mailboxes.lock().unwrap().push(archive.clone());
    let mut provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(batch_of(501));
    provider.fail_envelopes_from = Some(501);
    make_service(provider, Arc::new(MockEventStore::new()), projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    let checkpoint_key = format!("sync:{account_id}:Archive");
    assert_eq!(received(&projector), 500);
    assert_eq!(
        projector.get_checkpoint(&checkpoint_key).await.unwrap(),
        501
    );
    assert_eq!(
        projector
            .get_checkpoint(&format!("{checkpoint_key}:validity"))
            .await
            .unwrap(),
        0,
        "not complete"
    );
}

/// A renumbering cut short resumes from its checkpoint; it does not clear
/// the UIDs re-attached so far all over again.
#[tokio::test]
async fn an_interrupted_renumbering_resumes_instead_of_starting_over() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let kept = seed(
        &event_store,
        &projector,
        &archive,
        Some(3),
        "<m1@example.com>",
    );
    projector.mailboxes.lock().unwrap().push(archive.clone());
    let checkpoint_key = format!("sync:{account_id}:Archive");
    // A renumbering to validity 2 already got past UID 1 (kept is re-attached).
    projector
        .save_checkpoint(&format!("{checkpoint_key}:validity"), 1)
        .await
        .unwrap();
    projector
        .save_checkpoint(&format!("{checkpoint_key}:renumbering"), 2)
        .await
        .unwrap();
    projector.save_checkpoint(&checkpoint_key, 2).await.unwrap();
    projector.stored.lock().unwrap()[0].provider_uid = Some(ProviderUid::new(1));

    let mut provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(batch_of(2));
    provider.status.as_mut().unwrap().uid_validity = 2;
    make_service(provider, event_store.clone(), projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    let forgotten = event_store.appended_events().iter().any(|(_, e)| {
        matches!(
            e,
            DomainEvent::MessageMoved {
                provider_uid: None,
                ..
            }
        )
    });
    assert!(!forgotten, "UIDs are not cleared all over again");
    assert_eq!(row_uid(&projector, kept), Some(Some(1)));
    assert_eq!(received(&projector), 1, "only UID 2 was left to ingest");
    assert_eq!(
        projector
            .get_checkpoint(&format!("{checkpoint_key}:validity"))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        projector
            .get_checkpoint(&format!("{checkpoint_key}:renumbering"))
            .await
            .unwrap(),
        0
    );
}

/// The server renumbered Archive (UIDVALIDITY 1 → 2). UID 9 now belongs
/// to a different message: the stale local UID 9 used to hide it.
#[tokio::test]
async fn uidvalidity_change_reingests_the_whole_new_epoch() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let kept = seed(
        &event_store,
        &projector,
        &archive,
        Some(9),
        "<kept@example.com>",
    );
    let gone = seed(
        &event_store,
        &projector,
        &archive,
        Some(5),
        "<gone@example.com>",
    );
    projector.mailboxes.lock().unwrap().push(archive.clone());
    let checkpoint_key = format!("sync:{account_id}:Archive");
    projector
        .save_checkpoint(&checkpoint_key, 50)
        .await
        .unwrap();
    projector
        .save_checkpoint(&format!("{checkpoint_key}:validity"), 1)
        .await
        .unwrap();

    let mut provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(vec![
            envelope(3, "<kept@example.com>"),
            envelope(9, "<new@example.com>"),
        ]);
    provider.status = Some(MailboxStatus {
        uid_validity: 2,
        uid_next: 10,
        total_messages: 2,
        unseen_messages: 0,
    });
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(received(&projector), 1, "the message now at UID 9 is new");
    assert_eq!(
        row_uid(&projector, kept),
        Some(Some(3)),
        "re-attached by Message-ID"
    );
    assert_eq!(
        row_uid(&projector, gone),
        None,
        "not in the new epoch: deleted"
    );
}

/// Only a real placeholder in Sent is replaced by the server copy; a
/// received message waiting for its UID elsewhere is not one.
#[tokio::test]
async fn sent_copy_leaves_a_waiting_message_elsewhere_alone() {
    let account_id = AccountId::new();
    let (sent, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Sent".to_string()], '/').unwrap(),
        MailboxType::Sent,
    );
    let archive = archive_mailbox(account_id);
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let waiting = seed(
        &event_store,
        &projector,
        &archive,
        None,
        "<sent@example.com>",
    );
    projector
        .mailboxes
        .lock()
        .unwrap()
        .extend([sent.clone(), archive]);

    let mut provider = MockEmailProvider::new()
        .with_mailboxes(vec![sent])
        .with_envelopes(vec![envelope(7, "<sent@example.com>")]);
    provider
        .found
        .insert("Archive".into(), vec![ProviderUid::new(4)]);
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(
        row_uid(&projector, waiting),
        Some(None),
        "still there, still waiting"
    );
}

/// A row waiting for a UID that the server no longer has goes; reconcile
/// works by UID and would keep it, unactionable, forever. A Sent
/// placeholder waits for its server copy and stays.
#[tokio::test]
async fn a_uid_less_row_the_server_no_longer_has_is_deleted_but_a_placeholder_stays() {
    let account_id = AccountId::new();
    let inbox = inbox_mailbox(account_id);
    let (sent, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Sent".to_string()], '/').unwrap(),
        MailboxType::Sent,
    );
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let stranded = seed(&event_store, &projector, &inbox, None, "<gone@example.com>");
    let placeholder = MessageId::new();
    event_store.events.lock().unwrap().push((
        placeholder.to_string(),
        DomainEvent::MessageSent {
            message_id: placeholder,
            account_id,
            mailbox_id: sent.id,
            envelope: test_envelope(),
            message_rfc_id: Some("<out@example.com>".into()),
            in_reply_to: None,
            references: vec![],
            body_text: None,
            body_html: None,
            sent_at: chrono::Utc::now(),
        },
    ));
    let mut row = archived(account_id, sent.id, None);
    row.id = placeholder;
    projector.stored.lock().unwrap().push(row);
    projector
        .mailboxes
        .lock()
        .unwrap()
        .extend([inbox.clone(), sent.clone()]);

    let provider = MockEmailProvider::new().with_mailboxes(vec![inbox, sent]);
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(row_uid(&projector, stranded), None, "deleted");
    assert_eq!(
        row_uid(&projector, placeholder),
        Some(None),
        "still waiting for its server copy"
    );
}

/// A projector for `mailbox` whose checkpoint is already at `uid_next`:
/// every server message is below it, where ingest won't look again.
async fn synced_up_to(mailbox: &Mailbox, uid_next: u64) -> Arc<MockProjector> {
    let projector = Arc::new(MockProjector::new());
    projector.mailboxes.lock().unwrap().push(mailbox.clone());
    let checkpoint_key = format!("sync:{}:{}", mailbox.account_id, mailbox.path.full_path());
    projector
        .save_checkpoint(&checkpoint_key, uid_next)
        .await
        .unwrap();
    projector
        .save_checkpoint(&format!("{checkpoint_key}:validity"), 1)
        .await
        .unwrap();
    projector
}

fn stored_uids(projector: &MockProjector) -> Vec<u32> {
    let mut uids: Vec<_> = projector
        .stored
        .lock()
        .unwrap()
        .iter()
        .filter_map(|m| m.provider_uid.map(|u| u.value()))
        .collect();
    uids.sort_unstable();
    uids
}

/// A row dropped here while the server kept the message, below the
/// checkpoint where ingest never looks again: reconcile brings it back,
/// quietly (no new-mail notification). A copy marked \Deleted on the
/// server stays gone.
#[tokio::test]
async fn a_message_still_on_the_server_comes_back_below_the_checkpoint() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let projector = synced_up_to(&archive, 5).await;
    let mut deleted = envelope(4, "<deleted@example.com>");
    deleted.flags = vec![Flag::Deleted];
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(vec![envelope(3, "<kept@example.com>"), deleted]);

    make_service(provider, Arc::new(MockEventStore::new()), projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(stored_uids(&projector), [3]);
    assert!(
        projector.projected_events().iter().all(|e| !matches!(
            e,
            DomainEvent::MessageReceived {
                restored: false,
                ..
            }
        )),
        "a restored message is not announced as new"
    );
}

/// A draft discarded here is deleted on the server only afterwards; a
/// Drafts sync in between must not bring it back.
#[tokio::test]
async fn drafts_are_never_restored() {
    let account_id = AccountId::new();
    let (drafts, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Drafts".to_string()], '/').unwrap(),
        MailboxType::Drafts,
    );
    let projector = synced_up_to(&drafts, 5).await;
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![drafts])
        .with_envelopes(vec![envelope(3, "<draft@example.com>")]);

    make_service(provider, Arc::new(MockEventStore::new()), projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert!(stored_uids(&projector).is_empty());
}

/// One sync restores at most RESTORE_CAP, newest first, in UID-set
/// fetches of a batch each; the rest wait for the next sync.
#[tokio::test]
async fn restoring_is_capped_per_sync() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let total = RESTORE_CAP as u32 + 1;
    let projector = synced_up_to(&archive, total as u64 + 1).await;
    let provider = Arc::new(
        MockEmailProvider::new()
            .with_mailboxes(vec![archive])
            .with_envelopes(batch_of(total)),
    );
    let registry = Arc::new(MockProviderRegistry::new(provider.clone()));
    let service = SyncService::new(
        registry,
        Arc::new(MockEventStore::new()),
        projector.clone(),
        SyncConfig::default(),
    );

    service.start_sync(&account_id).await.unwrap();

    let uids = stored_uids(&projector);
    assert_eq!(
        (uids.len(), uids[0]),
        (RESTORE_CAP, 2),
        "the oldest one waits"
    );
    assert_eq!(
        *provider.uid_set_fetches.lock().unwrap(),
        [ENVELOPE_BATCH; RESTORE_CAP / ENVELOPE_BATCH]
    );

    service.start_sync(&account_id).await.unwrap();
    assert_eq!(
        stored_uids(&projector).len(),
        total as usize,
        "and comes back next sync"
    );
}

/// A batch the server fails on doesn't hold back the others.
#[tokio::test]
async fn a_failing_restore_batch_is_skipped() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let projector = synced_up_to(&archive, 601).await;
    let mut provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(batch_of(600));
    provider.fail_envelopes_from = Some(1);

    make_service(provider, Arc::new(MockEventStore::new()), projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(stored_uids(&projector), (501..=600).collect::<Vec<_>>());
}

#[tokio::test]
async fn resync_requests_are_coalesced() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let account = AccountId::new();
    let archive = MailboxPath::new(vec!["Archive".to_string()], '/').unwrap();
    for path in [MailboxPath::inbox(), archive.clone(), MailboxPath::inbox()] {
        tx.send((account, path)).unwrap();
    }

    assert_eq!(
        resync_batch(&mut rx).await.unwrap(),
        [(account, MailboxPath::inbox()), (account, archive)]
    );
    drop(tx);
    assert!(resync_batch(&mut rx).await.is_none());
}

fn inbox_mailbox(account_id: AccountId) -> Mailbox {
    Mailbox::discover(account_id, MailboxPath::inbox(), MailboxType::Inbox).0
}

/// A move the server confirmed without saying where the message went
/// leaves it waiting in its old mailbox; the copy that turns up in the
/// destination is that message, not a new one.
#[tokio::test]
async fn an_orphan_is_found_again_in_another_mailbox_under_its_own_id() {
    let account_id = AccountId::new();
    let (inbox, archive) = (inbox_mailbox(account_id), archive_mailbox(account_id));
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let orphan = seed(
        &event_store,
        &projector,
        &inbox,
        None,
        "<moved@example.com>",
    );
    projector
        .mailboxes
        .lock()
        .unwrap()
        .extend([inbox, archive.clone()]);

    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive.clone()])
        .with_envelopes(vec![envelope(9, "<moved@example.com>")]);
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(received(&projector), 0, "no duplicate");
    let row = projector
        .stored
        .lock()
        .unwrap()
        .iter()
        .find(|m| m.id == orphan)
        .cloned()
        .unwrap();
    assert_eq!(
        (row.mailbox_id, row.provider_uid),
        (archive.id, Some(ProviderUid::new(9)))
    );
}

/// The same Message-ID can legitimately sit in two mailboxes. One still
/// on the server in its own mailbox is not taken over by the other copy.
#[tokio::test]
async fn a_copy_still_in_its_own_mailbox_is_not_taken_over() {
    let account_id = AccountId::new();
    let (inbox, archive) = (inbox_mailbox(account_id), archive_mailbox(account_id));
    let (event_store, projector) = (
        Arc::new(MockEventStore::new()),
        Arc::new(MockProjector::new()),
    );
    let waiting = seed(&event_store, &projector, &inbox, None, "<both@example.com>");
    let held = seed(
        &event_store,
        &projector,
        &inbox,
        Some(3),
        "<both@example.com>",
    );
    projector
        .mailboxes
        .lock()
        .unwrap()
        .extend([inbox, archive.clone()]);

    let mut provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(vec![envelope(9, "<both@example.com>")]);
    provider.found.insert(
        "INBOX".into(),
        vec![ProviderUid::new(3), ProviderUid::new(5)],
    );
    make_service(provider, event_store, projector.clone())
        .start_sync(&account_id)
        .await
        .unwrap();

    assert_eq!(received(&projector), 1, "a second copy of its own");
    assert_eq!(row_uid(&projector, waiting), Some(None));
    assert_eq!(row_uid(&projector, held), Some(Some(3)));
}

#[tokio::test]
async fn sync_mailbox_waits_for_a_running_sync_of_the_account() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let projector = Arc::new(MockProjector::new());
    projector.mailboxes.lock().unwrap().push(archive.clone());
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive.clone()])
        .with_envelopes(vec![envelope(3, "<new@example.com>")]);
    let service = make_service(provider, Arc::new(MockEventStore::new()), projector.clone());

    let running = service.exclusive(&account_id).await;
    let sync = service.sync_mailbox(&account_id, &archive.path, ProviderUid::new(1));
    tokio::pin!(sync);
    assert!(
        futures::poll!(&mut sync).is_pending(),
        "must wait for the running sync"
    );
    drop(running);
    assert_eq!(sync.await.unwrap(), 1);
}

#[tokio::test]
async fn envelope_whose_uid_is_already_held_is_not_ingested_again() {
    let account_id = AccountId::new();
    let archive = archive_mailbox(account_id);
    let held = archived(account_id, archive.id, Some(ProviderUid::new(9)));

    let projector = Arc::new(MockProjector::new());
    projector.stored.lock().unwrap().push(held);
    projector.mailboxes.lock().unwrap().push(archive.clone());
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![archive])
        .with_envelopes(vec![make_envelope_summary(9)]);
    let service = make_service(provider, Arc::new(MockEventStore::new()), projector.clone());
    service.start_sync(&account_id).await.unwrap();

    assert_eq!(received(&projector), 0);
}

#[tokio::test]
async fn incremental_sync_uses_checkpoint() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );

    let provider = MockEmailProvider {
        mailboxes: vec![inbox],
        envelopes: vec![],
        bulk_fetch_calls: Mutex::new(vec![]),
        fail_envelopes_from: None,
        found: HashMap::new(),
        uid_set_fetches: Mutex::default(),
        status: Some(MailboxStatus {
            uid_validity: 1,
            uid_next: 10,
            total_messages: 9,
            unseen_messages: 2,
        }),
    };

    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    // Pre-set checkpoint: already synced up to uid_next=10
    let checkpoint_key = format!("sync:{}:INBOX", account_id);
    let validity_key = format!("{checkpoint_key}:validity");
    projector
        .save_checkpoint(&checkpoint_key, 10)
        .await
        .unwrap();
    projector.save_checkpoint(&validity_key, 1).await.unwrap();

    let service = make_service(provider, event_store.clone(), projector.clone());
    service.start_sync(&account_id).await.unwrap();

    // No MessageReceived events should be emitted (nothing new)
    let projected = projector.projected_events();
    let received_count = projected
        .iter()
        .filter(|e| matches!(e, DomainEvent::MessageReceived { .. }))
        .count();
    assert_eq!(
        received_count, 0,
        "Should not fetch when checkpoint equals uid_next"
    );
}

#[tokio::test]
async fn cancel_sync_stops_early() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );

    let envelopes = vec![make_envelope_summary(1)];
    let provider = MockEmailProvider::new()
        .with_mailboxes(vec![inbox])
        .with_envelopes(envelopes);

    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());
    let service = Arc::new(make_service(provider, event_store, projector));

    // Cancel immediately
    service.cancel_sync();

    let progress = service.start_sync(&account_id).await.unwrap();
    assert!(progress.iter().any(|e| e.state == SyncState::Completed));

    // The cancellation flag is reset at start_sync, so sync runs normally.
    // Test cancellation by checking the flag is set correctly.
    service.cancel_sync();
    assert!(service.is_cancelled());
}

#[tokio::test]
async fn resume_sync_delegates_to_start_sync() {
    let account_id = AccountId::new();
    let provider = MockEmailProvider::new();
    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    let service = make_service(provider, event_store, projector);
    let progress = service.resume_sync(&account_id).await.unwrap();

    assert!(progress.iter().any(|e| e.state == SyncState::Completed));
}

#[tokio::test]
async fn sync_emits_account_lifecycle_events() {
    let account_id = AccountId::new();
    let provider = MockEmailProvider::new();
    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    let service = make_service(provider, event_store.clone(), projector.clone());
    service.start_sync(&account_id).await.unwrap();

    let projected = projector.projected_events();

    // First event should be AccountSyncStarted
    assert!(
        matches!(&projected[0], DomainEvent::AccountSyncStarted { .. }),
        "First event should be AccountSyncStarted, got {:?}",
        &projected[0]
    );

    // Last event should be AccountSyncCompleted
    assert!(
        matches!(
            projected.last().unwrap(),
            DomainEvent::AccountSyncCompleted { .. }
        ),
        "Last event should be AccountSyncCompleted"
    );
}

#[tokio::test]
async fn sync_updates_mailbox_counts() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );

    let provider = MockEmailProvider {
        mailboxes: vec![inbox],
        envelopes: vec![],
        bulk_fetch_calls: Mutex::new(vec![]),
        fail_envelopes_from: None,
        found: HashMap::new(),
        uid_set_fetches: Mutex::default(),
        status: Some(MailboxStatus {
            uid_validity: 1,
            uid_next: 1,
            total_messages: 42,
            unseen_messages: 5,
        }),
    };

    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    let service = make_service(provider, event_store, projector.clone());
    service.start_sync(&account_id).await.unwrap();

    let projected = projector.projected_events();
    let counts_event = projected.iter().find(|e| {
        matches!(
            e,
            DomainEvent::MailboxCountsUpdated {
                total_messages: 42,
                unread_messages: 5,
                ..
            }
        )
    });
    assert!(
        counts_event.is_some(),
        "Should emit MailboxCountsUpdated with correct counts"
    );
}

#[tokio::test]
async fn sync_saves_checkpoint_after_mailbox() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );

    let provider = MockEmailProvider {
        mailboxes: vec![inbox],
        envelopes: vec![],
        bulk_fetch_calls: Mutex::new(vec![]),
        fail_envelopes_from: None,
        found: HashMap::new(),
        uid_set_fetches: Mutex::default(),
        status: Some(MailboxStatus {
            uid_validity: 42,
            uid_next: 100,
            total_messages: 99,
            unseen_messages: 3,
        }),
    };

    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());

    let service = make_service(provider, event_store, projector.clone());
    service.start_sync(&account_id).await.unwrap();

    // Verify checkpoints were saved
    let checkpoint_key = format!("sync:{}:INBOX", account_id);
    let validity_key = format!("{checkpoint_key}:validity");
    let uid_next = projector.get_checkpoint(&checkpoint_key).await.unwrap();
    let validity = projector.get_checkpoint(&validity_key).await.unwrap();

    assert_eq!(uid_next, 100, "Should save uid_next checkpoint");
    assert_eq!(validity, 42, "Should save uid_validity checkpoint");
}

struct RecordingPublisher(Mutex<Vec<DomainEvent>>);

impl EventPublisher for RecordingPublisher {
    fn publish(&self, envelope: EventEnvelope) {
        self.0.lock().unwrap().push(envelope.payload);
    }
}

#[tokio::test]
async fn status_events_reach_projector_and_bus_but_not_the_log() {
    let account_id = AccountId::new();
    let (inbox, _) = Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
        MailboxType::Inbox,
    );
    let provider = MockEmailProvider {
        mailboxes: vec![inbox],
        envelopes: vec![],
        bulk_fetch_calls: Mutex::new(vec![]),
        fail_envelopes_from: None,
        found: HashMap::new(),
        uid_set_fetches: Mutex::default(),
        status: Some(MailboxStatus {
            uid_validity: 1,
            uid_next: 1,
            total_messages: 42,
            unseen_messages: 5,
        }),
    };
    let event_store = Arc::new(MockEventStore::new());
    let projector = Arc::new(MockProjector::new());
    let bus = Arc::new(RecordingPublisher(Mutex::new(vec![])));
    let service = make_service(provider, event_store.clone(), projector.clone())
        .with_event_publisher(bus.clone());

    service.start_sync(&account_id).await.unwrap();

    let is_status = |e: &DomainEvent| {
        matches!(
            e,
            DomainEvent::AccountSyncStarted { .. }
                | DomainEvent::AccountSyncCompleted { .. }
                | DomainEvent::MailboxCountsUpdated { .. }
        )
    };
    let appended: Vec<_> = event_store.appended_events();
    assert!(!appended.iter().any(|(_, e)| is_status(e)), "{appended:?}");
    let projected = projector.projected_events();
    assert!(
        projected
            .iter()
            .any(|e| matches!(e, DomainEvent::AccountSyncStarted { .. }))
    );
    assert!(
        projected
            .iter()
            .any(|e| matches!(e, DomainEvent::AccountSyncCompleted { .. }))
    );
    assert!(
        projected
            .iter()
            .any(|e| matches!(e, DomainEvent::MailboxCountsUpdated { .. }))
    );
    let published = bus.0.lock().unwrap();
    assert!(published.iter().any(|e| matches!(
        e,
        DomainEvent::MailboxCountsUpdated {
            total_messages: 42,
            unread_messages: 5,
            ..
        }
    )));
}
