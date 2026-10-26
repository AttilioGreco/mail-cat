use super::*;
use mc_domain::{
    AggregateType, DomainError, EventEnvelope, Existing, Mailbox, MailboxChanges, MailboxCounts,
    Membership, MessageEnvelopeSummary, ProviderNotification, RemoteId, SyncCursor,
};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::RwLock;

/// The only mailbox a freshly received message is in.
fn mailbox_of(message: &Message) -> MailboxId {
    message.memberships[0].mailbox_id
}

fn uid_of(message: &Message) -> Option<RemoteId> {
    message.memberships[0].remote_id.clone()
}

// ── Mock EventStore ────────────────────────────────────────────────

struct MockEventStore {
    events: RwLock<HashMap<String, Vec<EventEnvelope>>>,
    /// Appends that lose to another writer, as a sync appending first.
    conflicts: std::sync::Mutex<u32>,
}

impl MockEventStore {
    fn new() -> Self {
        Self {
            events: RwLock::new(HashMap::new()),
            conflicts: Default::default(),
        }
    }

    fn with_message(message: &Message) -> Self {
        let store = Self::new();
        store.add(message);
        store
    }

    fn add(&self, message: &Message) {
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
struct MockEmailProvider {
    /// Message to return from fetch_message_detail.
    detail_response: RwLock<Option<Message>>,
    sent: RwLock<Vec<OutgoingMessage>>,
    appended: RwLock<Vec<(MailboxPath, OutgoingMessage, Vec<Flag>)>>,
    raw: RwLock<Vec<u8>>,
    /// Runs while the body fetch is "on the wire".
    during_fetch: RwLock<Option<Box<dyn Fn() + Send + Sync>>>,
    /// UID reported for the moved message in its destination.
    moved_uid: RwLock<Option<RemoteId>>,
    /// What `find_by_message_id` finds on the server.
    found: RwLock<Vec<RemoteId>>,
    /// UIDs `set_flags_bulk` was asked to flag.
    flagged_bulk: RwLock<Vec<RemoteId>>,
    /// UIDs `fetch_message_detail` was asked for.
    fetched: RwLock<Vec<RemoteId>>,
    /// UIDs `move_messages` was asked to move.
    moved: RwLock<Vec<RemoteId>>,
    /// Makes `move_messages` fail, as a server refusing it.
    refuse: RwLock<bool>,
    /// `move_messages` moves, then times out before the server's answer.
    time_out: RwLock<bool>,
    /// Makes `move_messages` fail once this many messages moved.
    refuse_after: RwLock<Option<usize>>,
    /// Mailbox of each `set_flags_bulk` call.
    flagged_in: RwLock<Vec<MailboxPath>>,
}

impl MockEmailProvider {
    fn new() -> Self {
        Self::default()
    }

    /// One whose moves report `uid` as the destination UID.
    fn moving_to(uid: u32) -> Self {
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
            counts: MailboxCounts { total: 0, unread: 0 },
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

    fn get_provider(&self, _account_id: &AccountId) -> Result<Arc<MockEmailProvider>, DomainError> {
        Ok(self.provider.clone())
    }

    fn register_provider(&self, _account_id: AccountId, _provider: Arc<MockEmailProvider>) {}

    fn remove_provider(&self, _account_id: &AccountId) {}
}

// ── Mock MessageReadRepository ─────────────────────────────────────

/// (message_id, body_text, body_html) as handed to `MessageBodyStore::put_body`.
type StoredBody = (MessageId, Option<String>, Option<String>);

#[derive(Default)]
struct MockMessageReadRepo {
    mailboxes: Vec<mc_domain::Mailbox>,
    messages: Vec<Message>,
    stored_bodies: std::sync::Mutex<Vec<StoredBody>>,
    unread: Vec<mc_domain::MessageSummary>,
    /// UIDs other local rows already hold.
    held: Vec<RemoteId>,
    /// What `share_bodies` hands out: copies given another's body.
    shared: Vec<(MessageId, bool)>,
}

impl MockMessageReadRepo {
    fn new() -> Self {
        Self::default()
    }

    fn with_mailboxes(mailboxes: Vec<mc_domain::Mailbox>) -> Self {
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
        RemoteId::from(42),
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

/// The INBOX `msg` sits in.
fn inbox_of(msg: &Message) -> mc_domain::Mailbox {
    mc_domain::Mailbox {
        id: mailbox_of(msg),
        ..test_mailbox(msg.account_id, "INBOX", MailboxType::Inbox)
    }
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
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)]));
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

    assert!(matches!(
        result,
        CommandResult::MailboxMarkedRead { count: 0 }
    ));
    assert!(bus.last_event_name().is_none());
}

#[tokio::test]
async fn handle_move_produces_event() {
    let msg = make_test_message();
    let msg_id = msg.id;

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::new(
        MockEmailProvider::moving_to(900),
    )));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)]));
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
    assert_eq!(bus.last_event_name().unwrap(), "MessageMailboxesChanged");
}

#[tokio::test]
async fn handle_delete_produces_event() {
    let msg = make_test_message();
    let msg_id = msg.id;
    let mbx_id = mailbox_of(&msg);

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::new(
        MockEmailProvider::moving_to(900),
    )));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)]));
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
    let mbx_id = mailbox_of(&msg);

    // A Trash folder exists for this account and the message is not in it,
    // so a plain delete must move-to-Trash (MessageMailboxesChanged), not expunge.
    let trash = mc_domain::Mailbox {
        id: MailboxId::new(),
        account_id: msg.account_id,
        path: MailboxPath::parse("Trash", '/').unwrap(),
        display_name: "Trash".into(),
        mailbox_type: MailboxType::Trash,
        flags: vec![],
        total_messages: 0,
        unread_messages: 0,
    };

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::new(
        MockEmailProvider::moving_to(900),
    )));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![
        inbox_of(&msg),
        trash,
    ]));
    let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

    let result = handler
        .handle_delete(msg_id, mbx_id, MailboxPath::inbox(), false)
        .await
        .unwrap();

    assert!(matches!(result, CommandResult::MessageDeleted));
    assert_eq!(bus.last_event_name().unwrap(), "MessageMailboxesChanged");
}

/// The bulk path must survive ids it cannot resolve: a search index that is
/// behind the mailbox will hand it messages that no longer exist, and one of
/// those must not abort the rest of the batch.
#[tokio::test]
async fn handle_delete_many_counts_partial_success() {
    let msg = make_test_message();
    let msg_id = msg.id;

    let source = mc_domain::Mailbox {
        id: mailbox_of(&msg),
        account_id: msg.account_id,
        path: MailboxPath::inbox(),
        display_name: "INBOX".into(),
        mailbox_type: MailboxType::Inbox,
        flags: vec![],
        total_messages: 0,
        unread_messages: 0,
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
    };

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::new(
        MockEmailProvider::moving_to(900),
    )));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![source, trash]));
    let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

    // One real message, one id that isn't in the event store.
    let result = handler
        .handle_delete_many(vec![msg_id, MessageId::new()], false)
        .await
        .unwrap();

    assert!(
        matches!(
            result,
            CommandResult::MessagesBulkChanged {
                succeeded: 1,
                failed: 1
            }
        ),
        "expected 1 ok / 1 skipped, got {result:?}"
    );
    // Trash exists and the message wasn't in it, so a plain bulk delete moves.
    assert_eq!(bus.last_event_name().unwrap(), "MessageMailboxesChanged");
}

#[tokio::test]
async fn handle_delete_permanent_expunges_even_with_trash() {
    let msg = make_test_message();
    let msg_id = msg.id;
    let mbx_id = mailbox_of(&msg);

    let trash = mc_domain::Mailbox {
        id: MailboxId::new(),
        account_id: msg.account_id,
        path: MailboxPath::parse("Trash", '/').unwrap(),
        display_name: "Trash".into(),
        mailbox_type: MailboxType::Trash,
        flags: vec![],
        total_messages: 0,
        unread_messages: 0,
    };

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(
        Arc::new(MockEmailProvider::new()),
    ));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![
        inbox_of(&msg),
        trash,
    ]));
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

fn test_account(email: &str, host: &str) -> Account {
    let email = EmailAddress::new(email).unwrap();
    Account::create(
        "Lavoro".into(),
        email.clone(),
        mc_domain::ProviderType::Imap,
        mc_domain::ConnectionConfig {
            incoming: mc_domain::IncomingServer::Imap {
                host: host.into(),
                port: 993,
                use_tls: true,
            },
            auth: mc_domain::AuthConfig::Plain {
                username: "u".into(),
                password: String::new(),
            },
            smtp: None,
        },
        mc_domain::Identity {
            display_name: "Mario Rossi".into(),
            email,
            signature_html: None,
            signature_text: Some("Mario".into()),
        },
    )
    .0
}

fn test_mailbox(account_id: AccountId, name: &str, kind: MailboxType) -> mc_domain::Mailbox {
    mc_domain::Mailbox {
        id: MailboxId::new(),
        account_id,
        path: MailboxPath::parse(name, '/').unwrap(),
        display_name: name.into(),
        mailbox_type: kind,
        flags: vec![],
        total_messages: 0,
        unread_messages: 0,
    }
}

fn compose_input(account_id: AccountId) -> ComposeInput {
    ComposeInput {
        draft_id: None,
        account_id,
        to: vec!["Anna <anna@example.com>".into()],
        cc: vec![],
        bcc: vec![],
        subject: "Ciao".into(),
        body_md: "**ciao**".into(),
        mode: ComposeMode::New,
        source_message_id: None,
        attachments: vec![mc_domain::OutgoingAttachment {
            filename: "a.pdf".into(),
            mime_type: "application/pdf".into(),
            content_id: None,
            data: vec![1, 2],
        }],
        forwarded_attachment_ids: vec![],
    }
}

/// Let spawned fire-and-forget provider calls run.
async fn drain_spawned() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

struct Fixture {
    account: Account,
    provider: Arc<MockEmailProvider>,
    bus: Arc<MockEventBus>,
    sent_box: mc_domain::Mailbox,
    drafts_box: mc_domain::Mailbox,
    /// A message stored in INBOX (event store + read model).
    seeded: Option<Message>,
    handler: MessageCommandHandler<
        MockEventStore,
        MockProviderRegistry,
        MockMessageReadRepo,
        MockEventBus,
    >,
}

fn fixture(host: &str, seed: impl FnOnce(AccountId, MailboxId) -> Option<Message>) -> Fixture {
    let account = test_account("mario@example.com", host);
    let inbox = test_mailbox(account.id, "INBOX", MailboxType::Inbox);
    let seeded = seed(account.id, inbox.id);
    let sent_box = test_mailbox(account.id, "Sent", MailboxType::Sent);
    let drafts_box = test_mailbox(account.id, "Drafts", MailboxType::Drafts);
    let provider = Arc::new(MockEmailProvider::new());
    let bus = Arc::new(MockEventBus::new());
    let es = Arc::new(match &seeded {
        Some(m) => MockEventStore::with_message(m),
        None => MockEventStore::new(),
    });
    let mut repo =
        MockMessageReadRepo::with_mailboxes(vec![inbox, sent_box.clone(), drafts_box.clone()]);
    repo.messages.extend(seeded.clone());
    let handler = MessageCommandHandler::new(
        es,
        Arc::new(MockProviderRegistry::new(provider.clone())),
        Arc::new(repo),
        bus.clone(),
    );
    Fixture {
        account,
        provider,
        bus,
        sent_box,
        drafts_box,
        seeded,
        handler,
    }
}

#[tokio::test]
async fn send_compose_sends_appends_to_sent_and_records_it() {
    let f = fixture("imap.example.com", |_, _| None);
    let result = f
        .handler
        .handle_send_compose(
            std::slice::from_ref(&f.account),
            compose_input(f.account.id),
        )
        .await
        .unwrap();
    assert!(matches!(result, CommandResult::MessageSent { .. }));
    drain_spawned().await;

    let sent = f.provider.sent.read().unwrap();
    let out = &sent[0];
    assert_eq!(out.from.name.as_deref(), Some("Mario Rossi"));
    assert_eq!(out.to[0].name.as_deref(), Some("Anna"));
    assert_eq!(out.attachments.len(), 1, "attachments must reach SMTP");
    assert!(
        out.body_html
            .as_deref()
            .unwrap()
            .contains("<strong>ciao</strong>")
    );
    assert_eq!(out.body_text.as_deref(), Some("**ciao**\n\n-- \nMario"));

    let appended = f.provider.appended.read().unwrap();
    assert_eq!(appended.len(), 1);
    assert_eq!(appended[0].0, f.sent_box.path);
    assert_eq!(appended[0].1.message_rfc_id, out.message_rfc_id);

    // The sent event carries the body, or a refetch of the sent message comes back empty.
    let published = f.bus.published.read().unwrap();
    let DomainEvent::MessageSent {
        body_text,
        mailbox_id,
        ..
    } = &published.last().unwrap().payload
    else {
        panic!("expected MessageSent event");
    };
    assert_eq!(body_text, &out.body_text);
    assert_eq!(*mailbox_id, f.sent_box.id);
}

#[tokio::test]
async fn gmail_sent_copy_is_not_appended() {
    let f = fixture("imap.gmail.com", |_, _| None);
    f.handler
        .handle_send_compose(
            std::slice::from_ref(&f.account),
            compose_input(f.account.id),
        )
        .await
        .unwrap();
    drain_spawned().await;
    assert!(f.provider.appended.read().unwrap().is_empty());
}

#[tokio::test]
async fn send_compose_rejects_bad_or_missing_recipients() {
    let f = fixture("imap.example.com", |_, _| None);
    let accounts = std::slice::from_ref(&f.account);

    let mut input = compose_input(f.account.id);
    input.cc = vec!["anna@".into()];
    let err = f
        .handler
        .handle_send_compose(accounts, input)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("anna@"), "{err}");

    let mut input = compose_input(f.account.id);
    input.to.clear();
    assert!(
        f.handler
            .handle_send_compose(accounts, input)
            .await
            .is_err()
    );
    assert!(f.provider.sent.read().unwrap().is_empty());
}

fn source_message(account_id: AccountId, mailbox_id: MailboxId) -> Message {
    let (mut msg, _) = Message::receive(
        account_id,
        mailbox_id,
        RemoteId::from(42),
        Some("<orig@example.com>".into()),
        Some("<parent@example.com>".into()),
        vec!["<root@example.com>".into(), "<parent@example.com>".into()],
        Envelope {
            from: vec![Address {
                name: Some("Anna".into()),
                email: EmailAddress::new("anna@example.com").unwrap(),
            }],
            to: vec![Address {
                name: None,
                email: EmailAddress::new("mario@example.com").unwrap(),
            }],
            cc: vec![Address {
                name: None,
                email: EmailAddress::new("bob@example.com").unwrap(),
            }],
            bcc: vec![],
            reply_to: vec![],
            subject: "R: Budget".into(),
            date: chrono::Utc::now(),
        },
        vec![],
        100,
        false,
        chrono::Utc::now(),
    );
    msg.body_text = Some("testo originale".into());
    msg
}

#[tokio::test]
async fn reply_all_is_prepared_and_sent_threaded_with_quote() {
    let f = fixture("imap.example.com", |account, inbox| {
        Some(source_message(account, inbox))
    });
    let source = f.seeded.clone().unwrap();
    let accounts = std::slice::from_ref(&f.account);

    let CommandResult::ComposePrepared(view) = f
        .handler
        .handle_prepare_compose(accounts, ComposeMode::ReplyAll, Some(source.id), None)
        .await
        .unwrap()
    else {
        panic!("expected ComposePrepared");
    };
    assert_eq!(view.subject, "Re: Budget");
    assert_eq!(view.to, ["Anna <anna@example.com>"]);
    assert_eq!(view.cc, ["bob@example.com"]);
    assert!(
        view.quoted_html
            .as_deref()
            .unwrap()
            .contains("testo originale")
    );

    let input = ComposeInput {
        to: view.to.clone(),
        cc: view.cc.clone(),
        subject: view.subject.clone(),
        mode: view.mode,
        source_message_id: view.source_message_id,
        attachments: vec![],
        ..compose_input(view.account_id)
    };
    f.handler
        .handle_send_compose(accounts, input)
        .await
        .unwrap();

    let sent = f.provider.sent.read().unwrap();
    assert_eq!(sent[0].in_reply_to.as_deref(), Some("<orig@example.com>"));
    assert_eq!(
        sent[0].references,
        [
            "<root@example.com>",
            "<parent@example.com>",
            "<orig@example.com>"
        ]
    );
    assert!(
        sent[0]
            .body_text
            .as_deref()
            .unwrap()
            .ends_with("ha scritto:\n> testo originale")
    );

    let published = f.bus.published.read().unwrap();
    assert!(published.iter().any(|e| matches!(
        &e.payload,
        DomainEvent::MessageFlagged { message_id, flags_added, .. }
            if *message_id == source.id && flags_added == &[Flag::Answered]
    )));
}

#[tokio::test]
async fn forward_carries_kept_attachments_and_inline_images() {
    let f = fixture("imap.example.com", |account, inbox| {
        Some(source_message(account, inbox))
    });
    let source = f.seeded.clone().unwrap();
    *f.provider.raw.write().unwrap() = b"Content-Type: multipart/mixed; boundary=\"M\"\r\n\r\n\
--M\r\nContent-Type: multipart/related; boundary=\"R\"\r\n\r\n\
--R\r\nContent-Type: text/html\r\n\r\n<p>Ecco <img src=\"cid:logo@x\"></p>\r\n\
--R\r\nContent-Type: image/png\r\nContent-ID: <logo@x>\r\nContent-Transfer-Encoding: base64\r\n\r\nAQID\r\n\
--R--\r\n\
--M\r\nContent-Type: application/pdf; name=\"a.pdf\"\r\nContent-Disposition: attachment; filename=\"a.pdf\"\r\nContent-Transfer-Encoding: base64\r\n\r\nBAUG\r\n\
--M--\r\n"
        .to_vec();
    let accounts = std::slice::from_ref(&f.account);

    let CommandResult::ComposePrepared(view) = f
        .handler
        .handle_prepare_compose(accounts, ComposeMode::Forward, Some(source.id), None)
        .await
        .unwrap()
    else {
        panic!("expected ComposePrepared");
    };
    assert_eq!(view.subject, "Fwd: Budget");
    assert!(view.to.is_empty());
    let names: Vec<_> = view
        .forwarded_attachments
        .iter()
        .map(|a| a.filename.as_str())
        .collect();
    assert_eq!(names, ["a.pdf"]);

    let input = ComposeInput {
        subject: view.subject.clone(),
        mode: view.mode,
        source_message_id: view.source_message_id,
        attachments: vec![],
        forwarded_attachment_ids: vec![view.forwarded_attachments[0].id.clone()],
        ..compose_input(view.account_id)
    };
    f.handler
        .handle_send_compose(accounts, input)
        .await
        .unwrap();

    let sent = f.provider.sent.read().unwrap();
    let parts: Vec<_> = sent[0]
        .attachments
        .iter()
        .map(|a| {
            (
                a.filename.as_str(),
                a.content_id.as_deref(),
                a.data.as_slice(),
            )
        })
        .collect();
    assert_eq!(
        parts,
        [
            ("a.pdf", None, &[4u8, 5, 6][..]),
            ("allegato", Some("logo@x"), &[1, 2, 3][..])
        ]
    );
    assert!(
        sent[0]
            .body_html
            .as_deref()
            .unwrap()
            .contains("Inizio messaggio inoltrato")
    );
    assert!(sent[0].in_reply_to.is_none());
}

#[tokio::test]
async fn draft_restores_markdown_recipients_and_attachments() {
    let f = fixture("imap.example.com", |account, inbox| {
        Some(source_message(account, inbox))
    });
    let draft = f.seeded.clone().unwrap();
    let headers: String =
        compose::draft_headers(ComposeMode::Forward, None, "**ciao**\n![](cid:img@x)")
            .into_iter()
            .map(|(k, v)| format!("{k}: {v}\r\n"))
            .collect();
    *f.provider.raw.write().unwrap() = format!(
        "{headers}To: Anna <anna@example.com>\r\nBcc: b@example.com\r\nSubject: Bozza\r\n\
Content-Type: multipart/mixed; boundary=\"M\"\r\n\r\n\
--M\r\nContent-Type: text/plain\r\n\r\nciao\r\n\
--M\r\nContent-Type: application/pdf\r\nContent-Disposition: attachment; filename=\"a.pdf\"\r\n\r\nPDF\r\n\
--M\r\nContent-Type: image/png\r\nContent-ID: <img@x>\r\n\r\nPNG\r\n\
--M--\r\n"
    )
    .into_bytes();

    let CommandResult::ComposePrepared(view) = f
        .handler
        .handle_prepare_compose(
            std::slice::from_ref(&f.account),
            ComposeMode::Draft,
            Some(draft.id),
            None,
        )
        .await
        .unwrap()
    else {
        panic!("expected ComposePrepared");
    };
    assert_eq!(view.draft_id, Some(draft.id));
    assert_eq!(view.body_md, "**ciao**\n![](cid:img@x)");
    assert_eq!(view.to, ["Anna <anna@example.com>"]);
    assert_eq!(view.bcc, ["b@example.com"]);
    assert_eq!(view.subject, "Bozza");
    // A forward whose original is unknown carries on as a new message.
    assert_eq!(view.mode, ComposeMode::New);
    assert_eq!(view.forwarded_attachments.len(), 1);
    let inline: Vec<_> = view
        .inline_images
        .iter()
        .map(|i| (i.content_id.as_deref(), i.data.as_slice()))
        .collect();
    assert_eq!(inline, [(Some("img@x"), &b"PNG"[..])]);
}

#[tokio::test]
async fn save_draft_appends_stores_locally_and_replaces_previous() {
    let f = fixture("imap.example.com", |account, inbox| {
        Some(source_message(account, inbox))
    });
    let old = f.seeded.clone().unwrap();
    let mut input = compose_input(f.account.id);
    input.draft_id = Some(old.id);
    input.to.clear();

    let CommandResult::DraftSaved { message_id } = f
        .handler
        .handle_save_draft(std::slice::from_ref(&f.account), input)
        .await
        .unwrap()
    else {
        panic!("expected DraftSaved");
    };
    assert_ne!(message_id, old.id);

    let appended = f.provider.appended.read().unwrap();
    let (path, draft, flags) = &appended[0];
    assert_eq!(*path, f.drafts_box.path);
    assert!(flags.contains(&Flag::Draft));
    assert!(
        draft
            .extra_headers
            .iter()
            .any(|(k, v)| k == "X-Mail-Cat-Mode" && v == "new")
    );
    assert!(f.provider.sent.read().unwrap().is_empty());

    let events: Vec<_> = f
        .bus
        .published
        .read()
        .unwrap()
        .iter()
        .map(|e| (e.aggregate_id.clone(), e.payload.event_name()))
        .collect();
    assert!(events.contains(&(message_id.to_string(), "MessageReceived")));
    assert!(events.contains(&(message_id.to_string(), "MessageBodyFetched")));
    assert!(events.contains(&(old.id.to_string(), "MessageDeleted")));
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
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)]));
    let handler = MessageCommandHandler::new(es, registry, read_repo.clone(), bus.clone());

    handler
        .handle_fetch_body(msg_id, MailboxPath::inbox())
        .await
        .unwrap();

    assert_eq!(
        read_repo.stored_bodies.lock().unwrap().as_slice(),
        &[(
            msg_id,
            Some("fetched text".to_string()),
            Some("<p>fetched</p>".to_string())
        )]
    );
    assert_eq!(bus.last_event_name().unwrap(), "MessageBodyFetched");
}

/// Gmail: the same mail under another label already has its whole text.
#[tokio::test]
async fn handle_fetch_body_takes_a_full_body_from_another_copy() {
    let msg = Message {
        message_rfc_id: Some("<m@x>".into()),
        ..make_test_message()
    };
    let provider = Arc::new(MockEmailProvider::new());
    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::clone(&provider)));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo {
        shared: vec![(msg.id, false)],
        ..MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])
    });
    let handler = MessageCommandHandler::new(es, registry, read_repo.clone(), bus.clone());

    handler
        .handle_fetch_body(msg.id, MailboxPath::inbox())
        .await
        .unwrap();

    assert!(
        provider.fetched.read().unwrap().is_empty(),
        "nothing downloaded"
    );
    assert_eq!(bus.last_event_name().unwrap(), "MessageBodyFetched");
}

/// Only the start of it is anywhere: the server still sends the rest.
#[tokio::test]
async fn handle_fetch_body_still_downloads_when_the_copy_is_truncated() {
    let msg = Message {
        message_rfc_id: Some("<m@x>".into()),
        ..make_test_message()
    };
    let provider = Arc::new(MockEmailProvider::new());
    *provider.detail_response.write().unwrap() = Some(Message {
        body_text: Some("whole".into()),
        ..msg.clone()
    });
    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::clone(&provider)));
    let bus = Arc::new(MockEventBus::new());
    let read_repo = Arc::new(MockMessageReadRepo {
        shared: vec![(msg.id, true)],
        ..MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])
    });
    let handler = MessageCommandHandler::new(es, registry, read_repo.clone(), bus.clone());

    handler
        .handle_fetch_body(msg.id, MailboxPath::inbox())
        .await
        .unwrap();

    assert_eq!(*provider.fetched.read().unwrap(), [RemoteId::from(42)]);
    assert_eq!(
        read_repo.stored_bodies.lock().unwrap()[0].1.as_deref(),
        Some("whole")
    );
}

/// Opening an unread message marks it read at the same moment, so the
/// `MessageFlagged` lands while the body is still being downloaded. The
/// fetch used to append at the sequence it read before the download and
/// lose the race — leaving the reading pane blank.
#[tokio::test]
async fn handle_fetch_body_survives_a_flag_change_during_the_download() {
    let msg = make_test_message();
    let msg_id = msg.id;
    let es = Arc::new(MockEventStore::with_message(&msg));
    let provider = MockEmailProvider::new();
    *provider.detail_response.write().unwrap() = Some(Message {
        body_text: Some("fetched text".into()),
        ..msg.clone()
    });
    let store = Arc::clone(&es);
    *provider.during_fetch.write().unwrap() = Some(Box::new(move || {
        let mut events = store.events.write().unwrap();
        let list = events.get_mut(&msg_id.to_string()).unwrap();
        let seq = list.len() as u64 + 1;
        list.push(EventEnvelope::new(
            msg_id.to_string(),
            AggregateType::Message,
            seq,
            DomainEvent::MessageFlagged {
                message_id: msg_id,
                flags_added: vec![Flag::Seen],
                flags_removed: vec![],
            },
        ));
    }));

    let registry = Arc::new(MockProviderRegistry::new(Arc::new(provider)));
    let bus = Arc::new(MockEventBus::new());
    let handler = MessageCommandHandler::new(
        es,
        registry,
        Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])),
        bus.clone(),
    );

    handler
        .handle_fetch_body(msg_id, MailboxPath::inbox())
        .await
        .unwrap();
    assert_eq!(bus.last_event_name().unwrap(), "MessageBodyFetched");
}

/// A message moved into a new "Archive" mailbox whose UID there is not
/// known — as after a move on a server without UIDPLUS, or any move
/// stored before moves carried the UID.
fn uidless_in_archive() -> (Message, mc_domain::Mailbox, Arc<MockEventStore>) {
    let (msg, _) = Message::receive(
        AccountId::new(),
        MailboxId::new(),
        RemoteId::from(42),
        Some("<moved@example.com>".into()),
        None,
        vec![],
        test_envelope(),
        vec![],
        2048,
        false,
        chrono::Utc::now(),
    );
    let archive = mc_domain::Mailbox {
        id: MailboxId::new(),
        account_id: msg.account_id,
        path: MailboxPath::parse("Archive", '/').unwrap(),
        display_name: "Archive".into(),
        mailbox_type: MailboxType::Archive,
        flags: vec![],
        total_messages: 0,
        unread_messages: 0,
    };
    let es = Arc::new(MockEventStore::with_message(&msg));
    es.events
        .write()
        .unwrap()
        .get_mut(&msg.id.to_string())
        .unwrap()
        .push(EventEnvelope::new(
            msg.id.to_string(),
            AggregateType::Message,
            2,
            DomainEvent::MessageMailboxesChanged {
                message_id: msg.id,
                added: vec![Membership {
                    mailbox_id: archive.id,
                    remote_id: None,
                }],
                removed: vec![mailbox_of(&msg)],
            },
        ));
    (msg, archive, es)
}

#[tokio::test]
async fn a_message_without_uid_is_found_on_the_server_before_acting() {
    let (msg, archive, es) = uidless_in_archive();
    let provider = Arc::new(MockEmailProvider::new());
    *provider.found.write().unwrap() = vec![RemoteId::from(55)];
    *provider.detail_response.write().unwrap() = Some(Message {
        body_text: Some("text".into()),
        ..msg.clone()
    });
    let registry = Arc::new(MockProviderRegistry::new(provider.clone()));
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![archive.clone()]));
    let handler =
        MessageCommandHandler::new(es, registry, read_repo, Arc::new(MockEventBus::new()));

    handler
        .handle_fetch_body(msg.id, archive.path.clone())
        .await
        .unwrap();

    assert_eq!(*provider.fetched.read().unwrap(), [RemoteId::from(55)]);
    let (stored, _) = handler.load_message(&msg.id).await.unwrap();
    assert_eq!(
        uid_of(&stored),
        Some(RemoteId::from(55)),
        "found UID is recorded"
    );
}

#[tokio::test]
async fn a_message_gone_from_the_server_is_a_clear_error() {
    let (msg, archive, es) = uidless_in_archive();
    let registry = Arc::new(MockProviderRegistry::new(
        Arc::new(MockEmailProvider::new()),
    ));
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![archive.clone()]));
    let handler =
        MessageCommandHandler::new(es, registry, read_repo, Arc::new(MockEventBus::new()));

    let err = handler
        .handle_set_flags(msg.id, archive.path, vec![Flag::Seen], vec![])
        .await
        .unwrap_err();
    assert!(matches!(err, ApplicationError::NotFound(..)), "{err:?}");
}

#[tokio::test]
async fn marking_a_mailbox_read_reaches_messages_without_uid() {
    let (msg, archive, es) = uidless_in_archive();
    let provider = Arc::new(MockEmailProvider::new());
    *provider.found.write().unwrap() = vec![RemoteId::from(55)];
    let unread = mc_domain::MessageSummary {
        id: msg.id,
        account_id: msg.account_id,
        memberships: vec![Membership {
            mailbox_id: archive.id,
            remote_id: None,
        }],
        from_name: None,
        from_email: "sender@example.com".into(),
        to: vec![],
        cc: vec![],
        subject: "Test".into(),
        flags: vec![],
        size_bytes: 0,
        has_attachments: false,
        received_at: chrono::Utc::now(),
    };
    let read_repo = Arc::new(MockMessageReadRepo {
        unread: vec![unread],
        ..MockMessageReadRepo::with_mailboxes(vec![archive.clone()])
    });
    let registry = Arc::new(MockProviderRegistry::new(provider.clone()));
    let handler =
        MessageCommandHandler::new(es, registry, read_repo, Arc::new(MockEventBus::new()));

    handler
        .handle_mark_mailbox_read(archive.id, archive.path)
        .await
        .unwrap();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }

    assert_eq!(
        *provider.flagged_bulk.read().unwrap(),
        [RemoteId::from(55)]
    );
}

/// Two copies of one Message-ID in the mailbox: the one another row
/// already holds is not this message's.
#[tokio::test]
async fn the_uid_found_skips_copies_other_rows_hold() {
    let (msg, archive, es) = uidless_in_archive();
    let provider = Arc::new(MockEmailProvider::new());
    *provider.found.write().unwrap() = vec![RemoteId::from(55), RemoteId::from(60)];
    *provider.detail_response.write().unwrap() = Some(msg.clone());
    let read_repo = Arc::new(MockMessageReadRepo {
        held: vec![RemoteId::from(60)],
        ..MockMessageReadRepo::with_mailboxes(vec![archive.clone()])
    });
    let handler = MessageCommandHandler::new(
        es,
        Arc::new(MockProviderRegistry::new(provider.clone())),
        read_repo,
        Arc::new(MockEventBus::new()),
    );

    handler
        .handle_fetch_body(msg.id, archive.path.clone())
        .await
        .unwrap();

    assert_eq!(*provider.fetched.read().unwrap(), [RemoteId::from(55)]);
}

#[tokio::test]
async fn a_uid_is_never_looked_up_in_another_mailbox() {
    let (msg, archive, es) = uidless_in_archive();
    let provider = Arc::new(MockEmailProvider::new());
    *provider.found.write().unwrap() = vec![RemoteId::from(55)];
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![archive]));
    let handler = MessageCommandHandler::new(
        es,
        Arc::new(MockProviderRegistry::new(provider)),
        read_repo,
        Arc::new(MockEventBus::new()),
    );

    let err = handler
        .handle_fetch_body(msg.id, MailboxPath::inbox())
        .await
        .unwrap_err();
    assert!(
        matches!(err, ApplicationError::PreconditionFailed(_)),
        "{err:?}"
    );
}

#[tokio::test]
async fn bulk_move_finds_messages_without_uid() {
    let (msg, archive, es) = uidless_in_archive();
    let provider = Arc::new(MockEmailProvider::new());
    *provider.found.write().unwrap() = vec![RemoteId::from(55)];
    let registry = Arc::new(MockProviderRegistry::new(provider.clone()));
    let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![archive]));
    let handler =
        MessageCommandHandler::new(es, registry, read_repo, Arc::new(MockEventBus::new()));

    let result = handler
        .handle_move_many(
            vec![msg.id],
            MailboxId::new(),
            MailboxPath::parse("Trash", '/').unwrap(),
        )
        .await
        .unwrap();

    assert!(matches!(
        result,
        CommandResult::MessagesBulkChanged {
            succeeded: 1,
            failed: 0
        }
    ));
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert_eq!(*provider.moved.read().unwrap(), [RemoteId::from(55)]);
}

#[tokio::test]
async fn handle_move_records_the_uid_the_provider_reports() {
    let msg = make_test_message();
    let msg_id = msg.id;
    let archive = MailboxId::new();
    let provider = MockEmailProvider::new();
    *provider.moved_uid.write().unwrap() = Some(RemoteId::from(900));

    let es = Arc::new(MockEventStore::with_message(&msg));
    let registry = Arc::new(MockProviderRegistry::new(Arc::new(provider)));
    let handler = MessageCommandHandler::new(
        es.clone(),
        registry,
        Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])),
        Arc::new(MockEventBus::new()),
    );

    handler
        .handle_move(
            msg_id,
            mailbox_of(&msg),
            archive,
            MailboxPath::inbox(),
            MailboxPath::parse("Archive", '/').unwrap(),
        )
        .await
        .unwrap();

    // The server moved it before anything changed here; the inbox UID
    // would address some other message in Archive.
    let (moved, _) = handler.load_message(&msg_id).await.unwrap();
    assert_eq!(mailbox_of(&moved), archive);
    assert_eq!(uid_of(&moved), Some(RemoteId::from(900)));
}

/// A move the server refuses changes nothing locally.
#[tokio::test]
async fn a_refused_move_changes_nothing() {
    let msg = make_test_message();
    let provider = MockEmailProvider::new();
    *provider.refuse.write().unwrap() = true;
    let es = Arc::new(MockEventStore::with_message(&msg));
    let handler = MessageCommandHandler::new(
        es,
        Arc::new(MockProviderRegistry::new(Arc::new(provider))),
        Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])),
        Arc::new(MockEventBus::new()),
    );

    let result = handler
        .handle_move(
            msg.id,
            mailbox_of(&msg),
            MailboxId::new(),
            MailboxPath::inbox(),
            MailboxPath::parse("Archive", '/').unwrap(),
        )
        .await;

    assert!(result.is_err());
    let (stored, _) = handler.load_message(&msg.id).await.unwrap();
    assert_eq!(
        (mailbox_of(&stored), uid_of(&stored)),
        (mailbox_of(&msg), uid_of(&msg))
    );
}

struct Moving {
    handler: MessageCommandHandler<
        MockEventStore,
        MockProviderRegistry,
        MockMessageReadRepo,
        MockEventBus,
    >,
    provider: Arc<MockEmailProvider>,
    es: Arc<MockEventStore>,
    archive: mc_domain::Mailbox,
    trash: mc_domain::Mailbox,
    resync: tokio::sync::mpsc::UnboundedReceiver<(AccountId, MailboxPath)>,
}

impl Moving {
    fn resync_requests(&mut self) -> Vec<MailboxPath> {
        std::iter::from_fn(|| self.resync.try_recv().ok().map(|(_, path)| path)).collect()
    }

    async fn location(&self, id: MessageId) -> (MailboxId, Option<RemoteId>) {
        let (stored, _) = self.handler.load_message(&id).await.unwrap();
        (mailbox_of(&stored), uid_of(&stored))
    }
}

/// INBOX, Archive and Trash of `msg`'s account, and a handler over them
/// that reports resync requests.
fn moving(msg: &Message, provider: MockEmailProvider) -> Moving {
    moving_with(msg, provider, MockMessageReadRepo::default())
}

fn moving_with(msg: &Message, provider: MockEmailProvider, repo: MockMessageReadRepo) -> Moving {
    let archive = test_mailbox(msg.account_id, "Archive", MailboxType::Archive);
    let trash = test_mailbox(msg.account_id, "Trash", MailboxType::Trash);
    let provider = Arc::new(provider);
    let es = Arc::new(MockEventStore::with_message(msg));
    let (tx, resync) = tokio::sync::mpsc::unbounded_channel();
    let repo = MockMessageReadRepo {
        mailboxes: vec![inbox_of(msg), archive.clone(), trash.clone()],
        ..repo
    };
    let handler = MessageCommandHandler::new(
        es.clone(),
        Arc::new(MockProviderRegistry::new(provider.clone())),
        Arc::new(repo),
        Arc::new(MockEventBus::new()),
    )
    .with_resync_requests(tx);
    Moving {
        handler,
        provider,
        es,
        archive,
        trash,
        resync,
    }
}

/// Archive, then delete before the list caught up: the delete waits its
/// turn and acts on the message where the archive put it, with the UID
/// it has there — never the INBOX UID again.
#[tokio::test]
async fn a_delete_queued_behind_a_move_follows_it_to_trash() {
    let msg = make_test_message();
    let m = moving(&msg, MockEmailProvider::moving_to(900));
    let archive = m.archive.clone();

    m.handler
        .handle_move(
            msg.id,
            mailbox_of(&msg),
            archive.id,
            MailboxPath::inbox(),
            archive.path.clone(),
        )
        .await
        .unwrap();
    let result = m
        .handler
        .handle_delete(msg.id, mailbox_of(&msg), MailboxPath::inbox(), false)
        .await
        .unwrap();

    assert!(matches!(result, CommandResult::MessageDeleted));
    assert_eq!(
        *m.provider.moved.read().unwrap(),
        [RemoteId::from(42), RemoteId::from(900)]
    );
    assert_eq!(
        m.location(msg.id).await,
        (m.trash.id, Some(RemoteId::from(900)))
    );

    // Moving it where it already is changes nothing.
    m.handler
        .handle_move(
            msg.id,
            mailbox_of(&msg),
            m.trash.id,
            MailboxPath::inbox(),
            m.trash.path.clone(),
        )
        .await
        .unwrap();
    assert_eq!(m.provider.moved.read().unwrap().len(), 2);
}

fn with_rfc_id(rfc_id: &str) -> Message {
    Message::receive(
        AccountId::new(),
        MailboxId::new(),
        RemoteId::from(42),
        Some(rfc_id.into()),
        None,
        vec![],
        test_envelope(),
        vec![],
        0,
        false,
        chrono::Utc::now(),
    )
    .0
}

/// The server said OK without COPYUID and the destination has no copy
/// to find: nothing may claim the message sits there. The row goes —
/// the real copy syncs as its own — and both mailboxes resync.
#[tokio::test]
async fn a_move_confirmed_without_evidence_records_no_destination() {
    let msg = with_rfc_id("<ghost@example.com>");
    let mut m = moving(&msg, MockEmailProvider::new());
    let archive = m.archive.clone();

    m.handler
        .handle_move(
            msg.id,
            mailbox_of(&msg),
            archive.id,
            MailboxPath::inbox(),
            archive.path.clone(),
        )
        .await
        .unwrap();

    assert!(matches!(
        m.handler.load_message(&msg.id).await,
        Err(ApplicationError::NotFound(..))
    ));
    assert_eq!(m.resync_requests(), [MailboxPath::inbox(), archive.path]);
}

/// A phone moved it to Archive; the Archive sync already stored that copy
/// as a row of its own while this one kept its stale INBOX UID. Archiving
/// it here: the server has nothing at that UID (OK, no COPYUID) and the
/// Archive copy is the other row's. This row goes rather than lingering
/// UID-less in INBOX, and nothing is duplicated.
#[tokio::test]
async fn archiving_a_message_another_client_already_moved_leaves_one_row() {
    let msg = with_rfc_id("<phone@example.com>");
    let provider = MockEmailProvider::new();
    *provider.found.write().unwrap() = vec![RemoteId::from(900)];
    let mut m = moving_with(
        &msg,
        provider,
        MockMessageReadRepo {
            held: vec![RemoteId::from(900)],
            ..Default::default()
        },
    );
    let archive = m.archive.clone();

    m.handler
        .handle_move(
            msg.id,
            mailbox_of(&msg),
            archive.id,
            MailboxPath::inbox(),
            archive.path.clone(),
        )
        .await
        .unwrap();

    assert!(
        matches!(
            m.handler.load_message(&msg.id).await,
            Err(ApplicationError::NotFound(..))
        ),
        "no UID-less row left"
    );
    let created = m.es.events.read().unwrap().len();
    assert_eq!(created, 1, "no new row");
    assert_eq!(m.resync_requests(), [MailboxPath::inbox(), archive.path]);
}

/// Gmail: archiving to All Mail, whose COPYUID names the UID the synced
/// All Mail row already holds. That row is the message there; this one
/// goes instead of becoming a second row of the same server message.
#[tokio::test]
async fn a_move_onto_a_uid_another_row_holds_leaves_that_row_alone() {
    let msg = make_test_message();
    let m = moving_with(
        &msg,
        MockEmailProvider::moving_to(900),
        MockMessageReadRepo {
            held: vec![RemoteId::from(900)],
            ..Default::default()
        },
    );
    let archive = m.archive.clone();

    m.handler
        .handle_move(
            msg.id,
            mailbox_of(&msg),
            archive.id,
            MailboxPath::inbox(),
            archive.path.clone(),
        )
        .await
        .unwrap();

    assert!(
        matches!(
            m.handler.load_message(&msg.id).await,
            Err(ApplicationError::NotFound(..))
        ),
        "no second row in Archive"
    );
    assert_eq!(m.es.events.read().unwrap().len(), 1, "and no new one");
}

#[tokio::test]
async fn the_same_delete_sent_twice_is_done_once() {
    let msg = make_test_message();
    let m = moving(&msg, MockEmailProvider::new());

    m.handler
        .handle_delete(msg.id, mailbox_of(&msg), MailboxPath::inbox(), true)
        .await
        .unwrap();
    let again = m
        .handler
        .handle_delete(msg.id, mailbox_of(&msg), MailboxPath::inbox(), true)
        .await;

    assert!(
        matches!(again, Ok(CommandResult::MessageDeleted)),
        "{again:?}"
    );
}

/// A double click on delete: the second one, asked while the message was
/// still in INBOX, finds it in Trash by its turn and leaves it there.
/// Asked again once it is in Trash, a delete does purge it.
#[tokio::test]
async fn a_double_delete_leaves_the_message_in_trash() {
    let msg = make_test_message();
    let m = moving(&msg, MockEmailProvider::moving_to(900));
    let turn = m.handler.turn(msg.account_id).await;
    let (msg_id, inbox) = (msg.id, mailbox_of(&msg));
    let deletes: Vec<_> = (0..2)
        .map(|_| {
            let handler = m.handler.clone();
            tokio::spawn(async move {
                handler
                    .handle_delete(msg_id, inbox, MailboxPath::inbox(), false)
                    .await
            })
        })
        .collect();
    drain_spawned().await;
    drop(turn);
    for delete in deletes {
        assert!(matches!(
            delete.await.unwrap(),
            Ok(CommandResult::MessageDeleted)
        ));
    }
    assert_eq!(
        m.location(msg.id).await,
        (m.trash.id, Some(RemoteId::from(900)))
    );

    m.handler
        .handle_delete(msg.id, m.trash.id, m.trash.path.clone(), false)
        .await
        .unwrap();
    assert!(matches!(
        m.handler.load_message(&msg.id).await,
        Err(ApplicationError::NotFound(..))
    ));
}

/// The same for bulk delete.
#[tokio::test]
async fn a_double_bulk_delete_leaves_the_message_in_trash() {
    let msg = make_test_message();
    let m = moving(&msg, MockEmailProvider::moving_to(900));
    let turn = m.handler.turn(msg.account_id).await;
    let deletes: Vec<_> = (0..2)
        .map(|_| {
            let handler = m.handler.clone();
            tokio::spawn(async move { handler.handle_delete_many(vec![msg.id], false).await })
        })
        .collect();
    drain_spawned().await;
    drop(turn);
    for delete in deletes {
        delete.await.unwrap().unwrap();
    }
    assert_eq!(
        m.location(msg.id).await,
        (m.trash.id, Some(RemoteId::from(900)))
    );
}

/// The connection dropped after the MOVE went out: it may have been
/// applied. The user sees the error, and both mailboxes resync.
#[tokio::test]
async fn a_timeout_after_a_move_resyncs_both_mailboxes() {
    let msg = make_test_message();
    let provider = MockEmailProvider::moving_to(900);
    *provider.time_out.write().unwrap() = true;
    let mut m = moving(&msg, provider);
    let archive = m.archive.clone();

    let result = m
        .handler
        .handle_move(
            msg.id,
            mailbox_of(&msg),
            archive.id,
            MailboxPath::inbox(),
            archive.path.clone(),
        )
        .await;

    assert!(result.is_err());
    assert_eq!(
        m.location(msg.id).await,
        (mailbox_of(&msg), Some(RemoteId::from(42)))
    );
    assert_eq!(m.resync_requests(), [MailboxPath::inbox(), archive.path]);
}

/// The server moved it; recording that loses to a sync appending first.
/// It is reloaded and recorded, not reported as a failure the server
/// never had.
#[tokio::test]
async fn recording_a_done_move_retries_past_a_concurrent_append() {
    let msg = make_test_message();
    let mut m = moving(&msg, MockEmailProvider::moving_to(900));
    let archive = m.archive.clone();
    *m.es.conflicts.lock().unwrap() = 1;

    m.handler
        .handle_move(
            msg.id,
            mailbox_of(&msg),
            archive.id,
            MailboxPath::inbox(),
            archive.path.clone(),
        )
        .await
        .unwrap();

    let (stored, _) = m.handler.load_message(&msg.id).await.unwrap();
    assert_eq!(
        (mailbox_of(&stored), uid_of(&stored)),
        (archive.id, Some(RemoteId::from(900)))
    );
    assert!(
        stored.flags.contains(&Flag::Flagged),
        "the other writer's change is kept"
    );
    assert!(m.resync_requests().is_empty());

    // Losing every time is still no error to the user: a resync repairs it.
    let msg = make_test_message();
    let mut m = moving(&msg, MockEmailProvider::moving_to(900));
    let archive = m.archive.clone();
    *m.es.conflicts.lock().unwrap() = RECORD_ATTEMPTS;
    m.handler
        .handle_move(
            msg.id,
            mailbox_of(&msg),
            archive.id,
            MailboxPath::inbox(),
            archive.path.clone(),
        )
        .await
        .unwrap();
    assert!(!m.resync_requests().is_empty());
}

/// The server applied the first chunk and refused the second: the first
/// is recorded, only the rest counts as failed.
#[tokio::test]
async fn a_refused_chunk_keeps_the_chunks_before_it() {
    let first = make_test_message();
    let provider = MockEmailProvider::moving_to(900);
    *provider.refuse_after.write().unwrap() = Some(SERVER_CHUNK);
    let Moving {
        handler,
        es,
        archive,
        ..
    } = moving(&first, provider);
    let mut ids = vec![first.id];
    for _ in 1..=SERVER_CHUNK {
        let other = Message {
            id: MessageId::new(),
            ..first.clone()
        };
        es.add(&other);
        ids.push(other.id);
    }

    let result = handler
        .handle_move_many(ids.clone(), archive.id, archive.path.clone())
        .await
        .unwrap();

    assert!(
        matches!(
            result,
            CommandResult::MessagesBulkChanged {
                succeeded: 500,
                failed: 1
            }
        ),
        "{result:?}"
    );
    let mut in_archive = 0;
    for id in &ids {
        if mailbox_of(&handler.load_message(id).await.unwrap().0) == archive.id {
            in_archive += 1;
        }
    }
    assert_eq!(in_archive, SERVER_CHUNK);
}

/// A message that can't be marked must not stop the others, here or on
/// the server.
#[tokio::test]
async fn marking_a_mailbox_read_continues_past_a_failing_message() {
    let msg = make_test_message();
    let summary = |id| mc_domain::MessageSummary {
        id,
        account_id: msg.account_id,
        memberships: vec![Membership {
            mailbox_id: mailbox_of(&msg),
            remote_id: None,
        }],
        from_name: None,
        from_email: "sender@example.com".into(),
        to: vec![],
        cc: vec![],
        subject: "Test".into(),
        flags: vec![],
        size_bytes: 0,
        has_attachments: false,
        received_at: chrono::Utc::now(),
    };
    let provider = Arc::new(MockEmailProvider::new());
    let read_repo = MockMessageReadRepo {
        unread: vec![summary(MessageId::new()), summary(msg.id)],
        ..MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])
    };
    let handler = MessageCommandHandler::new(
        Arc::new(MockEventStore::with_message(&msg)),
        Arc::new(MockProviderRegistry::new(provider.clone())),
        Arc::new(read_repo),
        Arc::new(MockEventBus::new()),
    );

    let result = handler
        .handle_mark_mailbox_read(mailbox_of(&msg), MailboxPath::inbox())
        .await
        .unwrap();
    drain_spawned().await;

    assert!(
        matches!(result, CommandResult::MailboxMarkedRead { count: 1 }),
        "{result:?}"
    );
    assert!(handler.load_message(&msg.id).await.unwrap().0.is_read());
    assert_eq!(
        *provider.flagged_bulk.read().unwrap(),
        [RemoteId::from(42)]
    );
}

/// Open (mark read), then archive at once: the flag store waits for the
/// move and goes to the message where it now is.
#[tokio::test]
async fn a_flag_store_follows_a_move_queued_ahead_of_it() {
    let msg = make_test_message();
    let Moving {
        handler,
        provider,
        archive,
        ..
    } = moving(&msg, MockEmailProvider::moving_to(900));
    let turn = handler.turn(msg.account_id).await;

    let mover = handler.clone();
    let archive_path = archive.path.clone();
    let (msg_id, inbox) = (msg.id, mailbox_of(&msg));
    let moved = tokio::spawn(async move {
        mover
            .handle_move(
                msg_id,
                inbox,
                archive.id,
                MailboxPath::inbox(),
                archive_path,
            )
            .await
    });
    drain_spawned().await;
    handler
        .handle_set_flags(msg.id, MailboxPath::inbox(), vec![Flag::Seen], vec![])
        .await
        .unwrap();
    drop(turn);
    moved.await.unwrap().unwrap();
    drain_spawned().await;

    assert_eq!(
        *provider.flagged_bulk.read().unwrap(),
        [RemoteId::from(900)]
    );
    assert_eq!(*provider.flagged_in.read().unwrap(), [archive.path]);
}
