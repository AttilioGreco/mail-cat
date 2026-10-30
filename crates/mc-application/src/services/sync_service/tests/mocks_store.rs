use super::*;

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
    pub(super) keys: Mutex<HashMap<mc_domain::MessageKey, MessageId>>,
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
            keys: Mutex::new(HashMap::new()),
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
            display_path,
            mailbox_type,
        } = &envelope.payload
        {
            self.mailboxes.lock().unwrap().push(Mailbox {
                id: *mailbox_id,
                account_id: *account_id,
                path: path.clone(),
                display_path: display_path.clone(),
                display_name: path.name().to_string(),
                mailbox_type: *mailbox_type,
                flags: Vec::new(),
                total_messages: 0,
                unread_messages: 0,
                subscribed: true,
                subscription_explicit: false,
            });
        }

        if let DomainEvent::MailboxSubscriptionChanged {
            mailbox_id,
            subscribed,
            explicit,
        } = &envelope.payload
        {
            if let Some(m) = self
                .mailboxes
                .lock()
                .unwrap()
                .iter_mut()
                .find(|m| m.id == *mailbox_id)
            {
                m.subscribed = *subscribed;
                m.subscription_explicit |= *explicit;
            }
            self.cursors
                .lock()
                .unwrap()
                .retain(|(_, collection), _| *collection != mailbox_id.to_string());
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
                provider_key,
                ..
            } => {
                if let Some(key) = provider_key {
                    self.keys.lock().unwrap().insert(key.clone(), *message_id);
                }
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

    async fn find_by_keys(
        &self,
        _account_id: &AccountId,
        keys: &[mc_domain::MessageKey],
    ) -> Result<HashMap<mc_domain::MessageKey, MessageId>, DomainError> {
        let known = self.keys.lock().unwrap();
        Ok(keys
            .iter()
            .filter_map(|k| Some((k.clone(), *known.get(k)?)))
            .collect())
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
