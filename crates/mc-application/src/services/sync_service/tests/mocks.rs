//! Fakes of everything sync talks to.

use super::*;

// ── Mock EmailProvider ─────────────────────────────────────────────

/// What the mock server reports for its one mailbox.
#[derive(Clone)]
pub(super) struct MockStatus {
    pub(super) epoch: u32,
    pub(super) next_id: u32,
    pub(super) total: u32,
    pub(super) unread: u32,
}

/// The mock's cursor, shaped like a UID-based provider's: `mock:{validity}:{next}`.
pub(super) fn cursor_at(epoch: u32, next_id: u32) -> SyncCursor {
    SyncCursor::new(format!("mock:{epoch}:{next_id}"))
}

pub(super) fn parse_cursor(cursor: &SyncCursor) -> Option<(u32, u32)> {
    let rest = cursor.as_str().strip_prefix("mock:")?;
    let (validity, next) = rest.split_once(':')?;
    Some((validity.parse().ok()?, next.parse().ok()?))
}

pub(super) fn numeric(id: &RemoteId) -> u32 {
    id.as_str().parse().unwrap()
}

pub(super) struct MockEmailProvider {
    pub(super) mailboxes: Vec<Mailbox>,
    /// The messages on the server, in every mailbox alike.
    pub(super) envelopes: Vec<MessageEnvelopeSummary>,
    pub(super) status: Option<MockStatus>,
    pub(super) bulk_fetch_calls: Mutex<Vec<Vec<u32>>>,
    /// Envelope fetches asking for this id fail.
    pub(super) fail_envelopes_with: Mutex<Option<u32>>,
    /// What those failures are.
    pub(super) envelope_error: fn() -> DomainError,
    /// Makes `mailbox_changes` fail.
    pub(super) changes_error: Option<fn() -> DomainError>,
    /// How many times `mailbox_changes` was asked.
    pub(super) changes_calls: AtomicUsize,
    /// Answers `mailbox_changes` instead of the UID-based simulation.
    pub(super) scripted: Mutex<Option<MailboxChanges>>,
    /// What `find_by_message_id` finds, by mailbox path.
    pub(super) found: HashMap<String, Vec<RemoteId>>,
    /// Size of each envelope fetch.
    pub(super) envelope_fetches: Mutex<Vec<usize>>,
}

pub(super) fn provider_error() -> DomainError {
    DomainError::Provider("envelopes".into())
}

impl MockEmailProvider {
    pub(super) fn new() -> Self {
        let (mailbox, _) = Mailbox::discover(
            AccountId::new(),
            MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
            MailboxType::Inbox,
        );
        Self {
            mailboxes: vec![mailbox],
            envelopes: vec![],
            status: Some(MockStatus {
                epoch: 1,
                next_id: 1,
                total: 0,
                unread: 0,
            }),
            bulk_fetch_calls: Mutex::new(vec![]),
            fail_envelopes_with: Mutex::new(None),
            changes_calls: AtomicUsize::new(0),
            envelope_error: provider_error,
            changes_error: None,
            scripted: Mutex::new(None),
            found: HashMap::new(),
            envelope_fetches: Mutex::default(),
        }
    }

    /// Number of times `fetch_bodies_bulk` was called (i.e. number of
    /// sessions opened for bulk body fetches).
    pub(super) fn bulk_fetch_call_count(&self) -> usize {
        self.bulk_fetch_calls.lock().unwrap().len()
    }

    pub(super) fn with_envelopes(mut self, envelopes: Vec<MessageEnvelopeSummary>) -> Self {
        let count = envelopes.len() as u32;
        let next_id = envelopes
            .iter()
            .map(|e| numeric(&e.remote_id) + 1)
            .max()
            .unwrap_or(1);
        self.envelopes = envelopes;
        if let Some(ref mut s) = self.status {
            s.next_id = next_id;
            s.total = count;
        }
        self
    }

    pub(super) fn with_mailboxes(mut self, mailboxes: Vec<Mailbox>) -> Self {
        self.mailboxes = mailboxes;
        self
    }

    pub(super) fn with_status(mut self, epoch: u32, next_id: u32, total: u32, unread: u32) -> Self {
        self.status = Some(MockStatus {
            epoch,
            next_id,
            total,
            unread,
        });
        self
    }
}

#[async_trait]
impl EmailProvider for MockEmailProvider {
    async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
        Ok(self.mailboxes.clone())
    }

    /// A provider that numbers messages per mailbox, like IMAP: a message
    /// flagged `\Deleted` does not exist, and a cursor from another
    /// validity is void.
    async fn mailbox_changes(
        &self,
        _mailbox: &MailboxPath,
        since: Option<&SyncCursor>,
    ) -> Result<MailboxChanges, DomainError> {
        self.changes_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = self.changes_error {
            return Err(error());
        }
        if let Some(scripted) = self.scripted.lock().unwrap().clone() {
            return Ok(scripted);
        }
        let status = self
            .status
            .clone()
            .ok_or_else(|| DomainError::Provider("No status".to_string()))?;
        let (reset, first_new) = match since.map(parse_cursor) {
            None => (None, 1),
            Some(Some((validity, next))) if validity == status.epoch => (None, next),
            Some(_) => (Some(Reset::IdsInvalidated), 1),
        };
        let mut live: Vec<(u32, Vec<Flag>)> = self
            .envelopes
            .iter()
            .filter(|e| !e.flags.contains(&Flag::Deleted))
            .map(|e| (numeric(&e.remote_id), e.flags.clone()))
            .filter(|(uid, _)| *uid < status.next_id)
            .collect();
        live.sort_by_key(|(uid, _)| std::cmp::Reverse(*uid));
        Ok(MailboxChanges {
            cursor: cursor_at(status.epoch, status.next_id),
            reset,
            added: live
                .iter()
                .filter(|(uid, _)| *uid >= first_new)
                .map(|(uid, _)| RemoteId::from(*uid))
                .collect(),
            existing: Existing::Snapshot(
                live.into_iter()
                    .map(|(uid, flags)| (RemoteId::from(uid), flags))
                    .collect(),
            ),
            counts: MailboxCounts {
                total: status.total,
                unread: status.unread,
            },
        })
    }

    async fn fetch_envelopes(
        &self,
        _mailbox: &MailboxPath,
        ids: &[RemoteId],
    ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
        self.envelope_fetches.lock().unwrap().push(ids.len());
        if self
            .fail_envelopes_with
            .lock()
            .unwrap()
            .is_some_and(|bad| ids.iter().any(|id| numeric(id) == bad))
        {
            return Err((self.envelope_error)());
        }
        Ok(self
            .envelopes
            .iter()
            .filter(|e| ids.contains(&e.remote_id))
            .cloned()
            .collect())
    }

    async fn fetch_message_detail(
        &self,
        _mailbox: &MailboxPath,
        _id: &RemoteId,
    ) -> Result<Message, DomainError> {
        Err(DomainError::Provider("Not implemented".to_string()))
    }

    async fn find_by_message_id(
        &self,
        mailbox: &MailboxPath,
        _rfc_id: &str,
    ) -> Result<Vec<RemoteId>, DomainError> {
        Ok(self
            .found
            .get(&mailbox.full_path())
            .cloned()
            .unwrap_or_default())
    }

    async fn fetch_raw_message(
        &self,
        _mailbox: &MailboxPath,
        _id: &RemoteId,
    ) -> Result<Vec<u8>, DomainError> {
        Err(DomainError::Provider("Not implemented".to_string()))
    }

    async fn set_flags_bulk(
        &self,
        _mailbox: &MailboxPath,
        _ids: &[RemoteId],
        _add: &[Flag],
        _remove: &[Flag],
    ) -> Result<(), DomainError> {
        Ok(())
    }

    async fn move_messages(
        &self,
        _ids: &[RemoteId],
        _from: &MailboxPath,
        _to: &MailboxPath,
    ) -> Result<HashMap<RemoteId, Option<RemoteId>>, DomainError> {
        Ok(HashMap::new())
    }

    async fn delete_messages(
        &self,
        _mailbox: &MailboxPath,
        _ids: &[RemoteId],
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
    ) -> Result<Option<RemoteId>, DomainError> {
        Ok(None)
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
        ids: &[RemoteId],
    ) -> Result<Vec<MessageBodyResult>, DomainError> {
        self.bulk_fetch_calls
            .lock()
            .unwrap()
            .push(ids.iter().map(numeric).collect());
        Ok(ids
            .iter()
            .map(|id| MessageBodyResult {
                remote_id: id.clone(),
                body_text: Some(format!("body-{id}")),
                body_html: None,
                truncated: false,
            })
            .collect())
    }
}

// ── Mock ProviderRegistry ──────────────────────────────────────────

pub(super) struct MockProviderRegistry {
    /// `None` reproduces "no provider registered for this account".
    pub(super) provider: Option<Arc<MockEmailProvider>>,
}

impl MockProviderRegistry {
    pub(super) fn new(provider: Arc<MockEmailProvider>) -> Self {
        Self {
            provider: Some(provider),
        }
    }

    pub(super) fn empty() -> Self {
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

pub(super) struct MockEventStore {
    pub(super) events: Mutex<Vec<(String, DomainEvent)>>,
}

impl MockEventStore {
    pub(super) fn new() -> Self {
        Self {
            events: Mutex::new(vec![]),
        }
    }

    pub(super) fn appended_events(&self) -> Vec<(String, DomainEvent)> {
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

// ── Mock Projector (ReadModelProjector + MailboxReadRepository + MessageReadRepository + SyncCursorStore) ──

/// (body_text, body_html) as handed to `MessageBodyStore::put_body`.
pub(super) type StoredBody = (Option<String>, Option<String>);

pub(super) struct MockProjector {
    pub(super) mailboxes: Mutex<Vec<Mailbox>>,
    pub(super) cursors: Mutex<HashMap<(AccountId, String), SyncCursor>>,
    pub(super) projected_events: Mutex<Vec<DomainEvent>>,
    pub(super) bodies: Mutex<HashMap<MessageId, StoredBody>>,
    pub(super) stored: Mutex<Vec<MessageSummary>>,
    /// Message-IDs of `stored` rows; a row without one matches any lookup.
    pub(super) rfc_ids: Mutex<HashMap<MessageId, String>>,
}

impl MockProjector {
    pub(super) fn new() -> Self {
        Self {
            mailboxes: Mutex::new(vec![]),
            cursors: Mutex::new(HashMap::new()),
            projected_events: Mutex::new(vec![]),
            bodies: Mutex::new(HashMap::new()),
            stored: Mutex::new(vec![]),
            rfc_ids: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn stored_bodies(&self) -> HashMap<MessageId, StoredBody> {
        self.bodies.lock().unwrap().clone()
    }

    pub(super) fn projected_events(&self) -> Vec<DomainEvent> {
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
            });
        }

        let mut stored = self.stored.lock().unwrap();
        match &envelope.payload {
            DomainEvent::MessageReceived {
                message_id,
                account_id,
                mailbox_id,
                remote_id,
                message_rfc_id,
                flags,
                size_bytes,
                ..
            } => {
                stored.push(MessageSummary {
                    id: *message_id,
                    account_id: *account_id,
                    memberships: vec![Membership {
                        mailbox_id: *mailbox_id,
                        remote_id: Some(remote_id.clone()),
                    }],
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
            DomainEvent::MessageMailboxesChanged {
                message_id,
                added,
                removed,
            } => {
                if let Some(row) = stored.iter_mut().find(|m| m.id == *message_id) {
                    row.memberships.retain(|m| !removed.contains(&m.mailbox_id));
                    for new in added {
                        match row
                            .memberships
                            .iter_mut()
                            .find(|m| m.mailbox_id == new.mailbox_id)
                        {
                            Some(existing) => *existing = new.clone(),
                            None => row.memberships.push(new.clone()),
                        }
                    }
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
            .filter(|m| m.membership(*mailbox_id).is_some())
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
    async fn find_by_remote_id(
        &self,
        mailbox_id: &MailboxId,
        remote_id: &RemoteId,
    ) -> Result<Option<MessageSummary>, DomainError> {
        Ok(self
            .stored
            .lock()
            .unwrap()
            .iter()
            .find(|m| {
                m.membership(*mailbox_id)
                    .is_some_and(|x| x.remote_id.as_ref() == Some(remote_id))
            })
            .cloned())
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

    async fn held_ids(
        &self,
        mailbox_id: &MailboxId,
        uids: &[RemoteId],
    ) -> Result<HashSet<RemoteId>, DomainError> {
        Ok(self
            .stored
            .lock()
            .unwrap()
            .iter()
            .filter_map(|m| m.membership(*mailbox_id)?.remote_id.clone())
            .filter(|uid| uids.contains(uid))
            .collect())
    }

    async fn pending_messages(
        &self,
        mailbox_id: &MailboxId,
    ) -> Result<Vec<MessageId>, DomainError> {
        Ok(self
            .stored
            .lock()
            .unwrap()
            .iter()
            .filter(|m| {
                m.membership(*mailbox_id)
                    .is_some_and(|x| x.remote_id.is_none())
            })
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
impl SyncCursorStore for MockProjector {
    async fn get_cursor(
        &self,
        account_id: &AccountId,
        collection: &str,
    ) -> Result<Option<SyncCursor>, DomainError> {
        Ok(self
            .cursors
            .lock()
            .unwrap()
            .get(&(*account_id, collection.to_owned()))
            .cloned())
    }

    async fn put_cursor(
        &self,
        account_id: &AccountId,
        collection: &str,
        cursor: &SyncCursor,
    ) -> Result<(), DomainError> {
        self.cursors
            .lock()
            .unwrap()
            .insert((*account_id, collection.to_owned()), cursor.clone());
        Ok(())
    }

    async fn delete_cursor(
        &self,
        account_id: &AccountId,
        collection: &str,
    ) -> Result<(), DomainError> {
        self.cursors
            .lock()
            .unwrap()
            .remove(&(*account_id, collection.to_owned()));
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
    pub(super) fn pending(&self) -> Vec<mc_domain::BackfillEntry> {
        let bodies = self.bodies.lock().unwrap();
        let rfc_ids = self.rfc_ids.lock().unwrap();
        self.stored
            .lock()
            .unwrap()
            .iter()
            .filter(|m| !bodies.contains_key(&m.id))
            .filter_map(|m| {
                let home = m.memberships.first()?;
                Some(mc_domain::BackfillEntry {
                    message_id: m.id,
                    mailbox_id: home.mailbox_id,
                    remote_id: home.remote_id.clone()?,
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

pub(super) struct NoKeychain;

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

pub(super) struct RecordingPublisher(pub(super) Mutex<Vec<DomainEvent>>);

impl EventPublisher for RecordingPublisher {
    fn publish(&self, envelope: EventEnvelope) {
        self.0.lock().unwrap().push(envelope.payload);
    }
}
