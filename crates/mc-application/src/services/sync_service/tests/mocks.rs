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
