//! Email synchronization service — orchestrates account sync with checkpoint resume.
//!
//! The SyncService coordinates the full sync workflow:
//! 1. List mailboxes and discover new ones
//! 2. Fetch message envelopes (metadata-only) with incremental UID-based sync
//! 3. Lazy-load message bodies on demand
//! 4. Track progress via checkpoint for resume on restart
//! 5. Emit domain events (MailboxDiscovered, MessageReceived, etc.)

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use mc_domain::{
    AccountId, AggregateType, Attachment, BackfillEntry, BodyBackfillQueue, DomainError, DomainEvent,
    EmailProvider, EventEnvelope, EventStore, FetchRange, Flag, Mailbox, MailboxId, MailboxPath, MailboxStatus, MailboxType,
    MailboxReadRepository, Message, MessageBodyResult, MessageBodyStore, MessageId,
    LoginGate, MessageEnvelopeSummary, MessageReadRepository, MessageSummary,
    ProjectionCheckpoint, ProviderRegistry, ProviderUid, ReadModelProjector,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::bus::EventPublisher;
use crate::error::ApplicationError;
use crate::services::sync_trigger::SyncTrigger;

/// A body fetch failure that would fail every other mailbox of the pass the
/// same way: the account cannot log in or connect, or is over a usage limit.
fn stops_the_pass(e: &DomainError) -> bool {
    matches!(
        e,
        DomainError::AuthenticationFailed(_)
            | DomainError::ConnectionFailed(_)
            | DomainError::ConnectionTimeout(_)
            | DomainError::Throttled(_)
    )
}

/// Bodies the backfill downloads per round, between two looks at the meter.
const BACKFILL_BATCH: usize = 50;

/// Share of the daily cap, in percent, the backfill fills at full speed. The
/// rest is headroom for opened messages and forwards, and for the last batch
/// overshooting.
const BACKFILL_CAP_PERCENT: u64 = 90;

/// How long a backfill at its share of the cap waits before looking again,
/// for old downloads to leave the 24 h window.
const BACKFILL_CAP_WAIT: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// The wait before the next backfill batch, given `spent` bytes downloaded in
/// the last 24 h: none below the backfill's share of the cap. Text-only,
/// capped and shared between copies, a whole mailbox fits well inside a
/// day's cap, so crawling to stretch it would only delay it.
fn backfill_pause(spent: u64, daily_limit: Option<u64>) -> Option<std::time::Duration> {
    daily_limit
        .is_some_and(|limit| spent >= limit / 100 * BACKFILL_CAP_PERCENT)
        .then_some(BACKFILL_CAP_WAIT)
}

/// Tracks the state of a sync operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncState {
    /// Sync not yet started.
    Idle,
    /// Currently listing mailboxes.
    ListingMailboxes,
    /// Currently fetching envelopes for a mailbox.
    FetchingEnvelopes { mailbox_id: MailboxId },
    /// Downloading message bodies (the background backfill worker).
    FetchingMessageBody,
    /// Sync completed successfully.
    Completed,
    /// Sync failed with error.
    Failed,
}

/// Progress event emitted during sync, for streaming to client.
#[derive(Debug, Clone)]
pub struct SyncProgressEvent {
    pub account_id: AccountId,
    pub state: SyncState,
    pub processed: u32,
    pub total: u32,
    pub message: String,
}

/// Configuration for sync behavior.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Maximum number of envelopes to fetch in one batch.
    pub batch_size: usize,
    /// Maximum concurrent message body fetches.
    pub max_concurrent_bodies: usize,
    /// Whether to eagerly fetch all message bodies or lazy-load on demand.
    pub eager_body_fetch: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            batch_size: 50,
            max_concurrent_bodies: 4,
            eager_body_fetch: false,
        }
    }
}

/// Orchestrates email synchronization for an account.
///
/// Manages the full sync lifecycle: mailbox discovery, envelope fetching,
/// checkpoint-based resume, and cancellation.
pub struct SyncService<
    PR: ProviderRegistry,
    ES: EventStore,
    P: ReadModelProjector + MailboxReadRepository + MessageReadRepository + ProjectionCheckpoint + BodyBackfillQueue + MessageBodyStore,
> {
    provider_registry: Arc<PR>,
    event_store: Arc<ES>,
    projector: Arc<P>,
    #[allow(dead_code)]
    config: SyncConfig,
    cancelled: AtomicBool,
    /// Optional event publisher: when set, every stored domain event is also
    /// published here so in-process subscribers (e.g. desktop notifications)
    /// receive events emitted by the sync pipeline.
    event_publisher: Option<Arc<dyn EventPublisher>>,
    /// Live progress fan-out. Most syncs are server-initiated (boot, the 5-minute
    /// tick, the backfill worker), so a client subscribes to watch rather than
    /// starting one itself.
    progress_tx: broadcast::Sender<SyncProgressEvent>,
    /// Last published (phase, percent, message) per account — the debounce gate.
    last_progress: Mutex<HashMap<AccountId, ProgressKey>>,
    /// One sync at a time per account (see [`Self::exclusive`]).
    account_locks: Mutex<HashMap<AccountId, Arc<tokio::sync::Mutex<()>>>>,
}

/// What makes one progress event distinguishable from the previous one.
type ProgressKey = (u8, u32, String);

/// The event for a message the server no longer has in `mailbox_id`: the
/// mailbox leaves its memberships, and the message goes with its last one.
/// Built from the read model's view: the stream is appended as-is.
fn left_mailbox(summary: &MessageSummary, mailbox_id: MailboxId) -> DomainEvent {
    if summary.memberships.len() > 1 {
        return DomainEvent::MessageMailboxesChanged {
            message_id: summary.id,
            added: vec![],
            removed: vec![mailbox_id],
        };
    }
    DomainEvent::MessageDeleted { message_id: summary.id, mailbox_ids: vec![mailbox_id] }
}

fn phase_rank(state: &SyncState) -> u8 {
    match state {
        SyncState::Idle => 0,
        SyncState::ListingMailboxes => 1,
        SyncState::FetchingEnvelopes { .. } => 2,
        SyncState::FetchingMessageBody => 3,
        SyncState::Completed => 4,
        SyncState::Failed => 5,
    }
}

/// Envelopes fetched and stored per batch; the checkpoint advances after each.
const ENVELOPE_BATCH: usize = 500;

/// Most messages one sync restores per mailbox (see `restore`). A mailbox
/// that lost many rows comes back over a few syncs instead of one sync
/// re-downloading it whole while the account waits.
const RESTORE_CAP: usize = 2000;

/// Checkpoint keys of one mailbox's sync.
struct SyncKeys {
    /// Next UID to fetch.
    uid_next: String,
    /// UIDVALIDITY of the epoch fully ingested.
    validity: String,
    /// UIDVALIDITY of the epoch being re-ingested after a change; 0 when none.
    renumbering: String,
}

impl SyncKeys {
    fn new(account_id: &AccountId, path: &MailboxPath) -> Self {
        let uid_next = format!("sync:{account_id}:{path}");
        Self { validity: format!("{uid_next}:validity"), renumbering: format!("{uid_next}:renumbering"), uid_next }
    }
}

fn percent(processed: u32, total: u32) -> u8 {
    if total == 0 {
        return 0;
    }
    ((processed as u64 * 100 / total as u64).min(100)) as u8
}

/// The next request and every one already queued behind it, each mailbox
/// once. `None` once all senders are gone.
async fn resync_batch(
    requests: &mut tokio::sync::mpsc::UnboundedReceiver<(AccountId, MailboxPath)>,
) -> Option<Vec<(AccountId, MailboxPath)>> {
    let mut batch = vec![requests.recv().await?];
    while let Ok(request) = requests.try_recv() {
        if !batch.contains(&request) {
            batch.push(request);
        }
    }
    Some(batch)
}

impl<
    PR: ProviderRegistry,
    ES: EventStore,
    P: ReadModelProjector + MailboxReadRepository + MessageReadRepository + ProjectionCheckpoint + BodyBackfillQueue + MessageBodyStore,
> SyncService<PR, ES, P>
{
    /// Creates a new SyncService with the given dependencies.
    pub fn new(
        provider_registry: Arc<PR>,
        event_store: Arc<ES>,
        projector: Arc<P>,
        config: SyncConfig,
    ) -> Self {
        Self {
            provider_registry,
            event_store,
            projector,
            config,
            cancelled: AtomicBool::new(false),
            event_publisher: None,
            // Slow subscribers lag rather than block the sync; a dropped
            // intermediate percentage costs nothing since the next one supersedes it.
            progress_tx: broadcast::channel(64).0,
            last_progress: Mutex::new(HashMap::new()),
            account_locks: Mutex::new(HashMap::new()),
        }
    }

    /// Holds the account's sync slot. The IDLE-triggered `sync_mailbox` and a
    /// full `start_sync` running side by side would ingest the same envelopes
    /// twice and race each other's checkpoints.
    pub async fn exclusive(&self, account_id: &AccountId) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = Arc::clone(self.account_locks.lock().unwrap().entry(*account_id).or_default());
        lock.lock_owned().await
    }

    /// Subscribe to live sync progress for every account.
    pub fn subscribe_progress(&self) -> broadcast::Receiver<SyncProgressEvent> {
        self.progress_tx.subscribe()
    }

    /// Fan out a progress update to live subscribers, dropping it when it repeats
    /// what was last published for the account.
    ///
    /// The gate is (phase, whole percentage point, message): without it a
    /// 10k-message mailbox would emit 10k events no UI can render. Terminal
    /// states always pass. Returns the event when it passed the gate.
    fn publish(&self, event: SyncProgressEvent) -> Option<SyncProgressEvent> {
        let terminal = matches!(event.state, SyncState::Completed | SyncState::Failed);
        // A body batch is a fraction of a percent of a large backlog: gating it
        // on whole percentages froze the bar for minutes at a time.
        let step = match event.state {
            SyncState::FetchingMessageBody => event.processed,
            _ => u32::from(percent(event.processed, event.total)),
        };
        let key: ProgressKey = (phase_rank(&event.state), step, event.message.clone());

        {
            let mut last = self
                .last_progress
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !terminal && last.get(&event.account_id) == Some(&key) {
                return None;
            }
            last.insert(event.account_id, key);
        }

        // Err just means nobody is listening.
        let _ = self.progress_tx.send(event.clone());
        Some(event)
    }

    /// Publish a progress update and, if it passed the gate, keep it in the log
    /// returned by `start_sync` (which the SyncAccount RPC replays).
    fn emit(&self, progress: &mut Vec<SyncProgressEvent>, event: SyncProgressEvent) {
        if let Some(event) = self.publish(event) {
            progress.push(event);
        }
    }

    /// Attach an event publisher so that domain events stored during sync are
    /// also broadcast to in-process subscribers (desktop notifications, etc.).
    pub fn with_event_publisher(mut self, publisher: Arc<dyn EventPublisher>) -> Self {
        self.event_publisher = Some(publisher);
        self
    }

    /// Starts a full sync for the given account.
    ///
    /// The sync loop:
    /// 1. Emits AccountSyncStarted event
    /// 2. Lists remote mailboxes, discovers new ones
    /// 3. For each mailbox: loads checkpoint, fetches new envelopes, emits MessageReceived events
    /// 4. Updates mailbox counts and saves checkpoint
    /// 5. Emits AccountSyncCompleted event
    pub async fn start_sync(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<SyncProgressEvent>, ApplicationError> {
        let _exclusive = self.exclusive(account_id).await;
        let result = self.run_sync(account_id).await;

        // Every failure path out of `run_sync` is a `?`, so without this nothing
        // ever publishes a terminal event and a watching client sits on the last
        // "in progress" percentage forever.
        if let Err(e) = &result {
            self.publish(SyncProgressEvent {
                account_id: *account_id,
                state: SyncState::Failed,
                processed: 0,
                total: 0,
                message: e.to_string(),
            });
        }

        result
    }

    #[instrument(skip(self), fields(account_id = %account_id))]
    async fn run_sync(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<SyncProgressEvent>, ApplicationError> {
        // Reset cancellation flag at start
        self.cancelled.store(false, Ordering::SeqCst);

        // Resolve the provider for this account
        let provider = self.provider_registry.get_provider(account_id)?;

        let mut progress = vec![];
        let mut mailboxes_synced = 0u32;
        let mut messages_synced = 0u32;

        // 1. Emit AccountSyncStarted
        info!("Starting sync for account");
        let sync_started = DomainEvent::AccountSyncStarted {
            account_id: *account_id,
        };
        self.project_transient(&account_id.to_string(), AggregateType::Account, sync_started)
            .await?;

        // 2. List remote mailboxes
        self.emit(
            &mut progress,
            SyncProgressEvent {
                account_id: *account_id,
                state: SyncState::ListingMailboxes,
                processed: 0,
                total: 0,
                message: "Listing mailboxes...".to_string(),
            },
        );

        let remote_mailboxes = provider.list_mailboxes().await.map_err(|e| {
            error!("Failed to list mailboxes: {}", e);
            // Typed, like fetch_bodies_bulk: callers tell a refused credential from a failed sync.
            if matches!(e, DomainError::AuthenticationFailed(_)) {
                return ApplicationError::Domain(e);
            }
            ApplicationError::SyncFailed(format!("Failed to list mailboxes: {e}"))
        })?;

        info!("Discovered {} remote mailboxes", remote_mailboxes.len());

        // 3. Load local mailboxes for diff
        let local_mailboxes = self
            .projector
            .list_mailboxes(account_id)
            .await
            .map_err(|e| {
                ApplicationError::SyncFailed(format!("Failed to load local mailboxes: {e}"))
            })?;
        let local_by_path: HashMap<&MailboxPath, &Mailbox> =
            local_mailboxes.iter().map(|m| (&m.path, m)).collect();

        // 4. Discover new mailboxes, and correct the type of existing ones.
        for remote in &remote_mailboxes {
            match local_by_path.get(&remote.path) {
                None => {
                    info!(path = %remote.path, "Discovering new mailbox");
                    let (mailbox, event) =
                        Mailbox::discover(*account_id, remote.path.clone(), remote.mailbox_type);
                    self.append_and_project(&mailbox.id.to_string(), AggregateType::Mailbox, event)
                        .await?;
                }
                Some(local) if local.mailbox_type != remote.mailbox_type => {
                    // The type was recorded once at discovery. Detection has since
                    // improved (SPECIAL-USE, localized names), so mailboxes classified
                    // by an older build stay wrong forever unless we correct them here
                    // — which is what silently broke Archive and Trash targeting.
                    info!(
                        path = %remote.path,
                        from = ?local.mailbox_type,
                        to = ?remote.mailbox_type,
                        "Reclassifying mailbox"
                    );
                    let mut mailbox = (*local).clone();
                    let event = mailbox.reclassify(remote.mailbox_type);
                    self.append_and_project(&mailbox.id.to_string(), AggregateType::Mailbox, event)
                        .await?;
                }
                Some(_) => {}
            }
        }

        // 5. Re-load local mailboxes (now includes newly discovered)
        let local_mailboxes = self
            .projector
            .list_mailboxes(account_id)
            .await
            .map_err(|e| {
                ApplicationError::SyncFailed(format!("Failed to reload local mailboxes: {e}"))
            })?;
        let local_by_path: HashMap<&MailboxPath, &Mailbox> =
            local_mailboxes.iter().map(|m| (&m.path, m)).collect();

        // 6. For each remote mailbox: sync envelopes
        let total_mailboxes = remote_mailboxes.len() as u32;
        for remote in &remote_mailboxes {
            if self.is_cancelled() {
                info!("Sync cancelled, stopping");
                break;
            }

            let local = match local_by_path.get(&remote.path) {
                Some(m) => *m,
                None => {
                    warn!(path = %remote.path, "Local mailbox not found after discovery, skipping");
                    continue;
                }
            };

            self.emit(
                &mut progress,
                SyncProgressEvent {
                    account_id: *account_id,
                    state: SyncState::FetchingEnvelopes {
                        mailbox_id: local.id,
                    },
                    processed: mailboxes_synced,
                    total: total_mailboxes,
                    message: format!("Syncing {}...", remote.path),
                },
            );

            // Get remote status (uid_validity, uid_next)
            let status = match provider.mailbox_status(&remote.path).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(path = %remote.path, error = %e, "Failed to get mailbox status, skipping");
                    continue;
                }
            };

            match self.sync_one(&provider, local, &remote.path, 1, status, Some(&mut progress)).await? {
                Some((new, expunged)) => messages_synced = (messages_synced + new).saturating_sub(expunged),
                None if self.is_cancelled() => {
                    info!("Sync cancelled, stopping");
                    break;
                }
                None => continue,
            }
            mailboxes_synced += 1;
        }

        // 7. Emit AccountSyncCompleted
        let sync_completed = DomainEvent::AccountSyncCompleted {
            account_id: *account_id,
            mailboxes_synced,
            messages_synced,
        };
        self.project_transient(&account_id.to_string(), AggregateType::Account, sync_completed)
            .await?;

        info!(
            mailboxes = mailboxes_synced,
            messages = messages_synced,
            "Sync completed"
        );

        self.emit(
            &mut progress,
            SyncProgressEvent {
                account_id: *account_id,
                state: SyncState::Completed,
                processed: mailboxes_synced,
                total: total_mailboxes,
                message: format!(
                    "Sync completed: {mailboxes_synced} mailboxes, {messages_synced} messages"
                ),
            },
        );

        Ok(progress)
    }

    /// Resumes a sync operation from the last checkpoint.
    ///
    /// Delegates to `start_sync` — the checkpoint mechanism automatically
    /// provides incremental sync behavior.
    pub async fn resume_sync(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<SyncProgressEvent>, ApplicationError> {
        info!(account_id = %account_id, "Resuming sync from checkpoint");
        self.start_sync(account_id).await
    }

    /// Fetches the body of a single message from the provider.
    ///
    /// Emits a `MessageBodyFetched` event and projects it into the read model.
    #[instrument(skip(self))]
    pub async fn fetch_message_body(
        &self,
        account_id: &AccountId,
        mailbox_path: &MailboxPath,
        message_id: &MessageId,
        provider_uid: ProviderUid,
    ) -> Result<Option<Message>, ApplicationError> {
        debug!("Fetching message body");

        let provider = self.provider_registry.get_provider(account_id)?;

        let fetched = provider
            .fetch_message_detail(mailbox_path, provider_uid)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to fetch body: {e}")))?;

        self.store_body(
            message_id,
            fetched.body_text.clone(),
            fetched.body_html.clone(),
            false,
            fetched.attachments.clone(),
        )
        .await?;
        // Search indexing is owned by the search-index projection, driven off
        // the MessageBodyFetched event just appended.

        Ok(Some(fetched))
    }

    /// Fetches bodies for multiple messages in the same mailbox over a single
    /// IMAP session (see `EmailProvider::fetch_bodies_bulk`), emitting one
    /// `MessageBodyFetched` event per message. Used by `run_backfill` so a
    /// batch of pending messages costs one connection, not one per message.
    async fn fetch_message_bodies_bulk(
        &self,
        account_id: &AccountId,
        mailbox_path: &MailboxPath,
        entries: &[BackfillEntry],
    ) -> Result<Vec<MessageId>, ApplicationError> {
        if entries.is_empty() {
            return Ok(vec![]);
        }

        let provider = self.provider_registry.get_provider(account_id)?;
        let uids: Vec<ProviderUid> = entries
            .iter()
            .map(|e| ProviderUid::new(e.provider_uid))
            .collect();

        // Keep login and link failures typed (via the transparent Domain variant)
        // so the backfill stops its pass on them instead of retrying every
        // mailbox against an account that cannot connect.
        let results = provider
            .fetch_bodies_bulk(mailbox_path, &uids)
            .await
            .map_err(|e| match e {
                e if stops_the_pass(&e) => ApplicationError::Domain(e),
                other => ApplicationError::SyncFailed(format!(
                    "Failed to bulk fetch bodies: {other}"
                )),
            })?;
        let by_uid: HashMap<u32, MessageBodyResult> =
            results.into_iter().map(|r| (r.provider_uid.value(), r)).collect();

        let mut missing = Vec::new();
        for entry in entries {
            let Some(result) = by_uid.get(&entry.provider_uid) else {
                // Not returned by the server this pass (e.g. deleted meanwhile).
                // Caller (`run_backfill`) tracks retry counts and gives up after
                // too many misses, so this doesn't get retried forever.
                missing.push(entry.message_id);
                continue;
            };
            self.store_body(
                &entry.message_id,
                result.body_text.clone(),
                result.body_html.clone(),
                result.truncated,
                vec![],
            )
            .await?;
        }

        Ok(missing)
    }

    /// Give up on a message missing from the remote server after this many
    /// backfill passes (e.g. it was deleted before its body was fetched).
    ///
    /// ponytail: the give-up set is in-memory only, scoped to one `run_backfill`
    /// loop — it resets on restart, so an abandoned message gets a few more free
    /// retries after every restart. That's fine: giving up just stops the log
    /// spam for this run. If it ever needs to survive restarts too, persist a
    /// `MessageBodyFetchAbandoned`-style event instead (mirroring how
    /// `MessageBodyFetched` dequeues the backfill entry).
    const MAX_BACKFILL_MISSING_ATTEMPTS: u32 = 5;

    /// Background body-backfill loop for one account.
    ///
    /// Drains the backfill queue in its priority order, a batch at a time
    /// (grouped by mailbox, one IMAP session per mailbox — see
    /// `fetch_message_bodies_bulk`), at full speed until most of `daily_limit`
    /// bytes per 24 h is spent (see `backfill_pause`). Only the start of a
    /// body's text is downloaded, once per Message-ID; attachments are fetched
    /// when a message needs them.
    /// Each fetch emits `MessageBodyFetched`, which both indexes the body and
    /// dequeues the entry. Whole-fetch failures (auth/network) leave entries
    /// queued and retried next pass; entries the server no longer has get a
    /// few retries before being abandoned (see `MAX_BACKFILL_MISSING_ATTEMPTS`).
    /// Waits on `gate` before each batch, so it pauses while the account needs
    /// the user and follows the offline retry schedule. Runs until `shutdown` fires.
    pub async fn run_backfill(
        &self,
        account_id: AccountId,
        daily_limit: Option<u64>,
        gate: Arc<dyn LoginGate>,
        shutdown: CancellationToken,
    ) {
        let idle_poll = std::time::Duration::from_secs(60);

        let mut missing_counts: HashMap<MessageId, u32> = HashMap::new();
        let mut abandoned: HashSet<MessageId> = HashSet::new();
        // Bodies downloaded since the queue was last empty. Progress is this
        // against itself plus what's still queued, so the percentage climbs to
        // 100% across batches instead of restarting with each one; new mail
        // only grows the total.
        let mut drained: u32 = 0;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                () = gate.ready() => {}
            }

            let path_by_id: HashMap<MailboxId, MailboxPath> =
                match self.projector.list_mailboxes(&account_id).await {
                    Ok(mbs) => mbs.into_iter().map(|m| (m.id, m.path)).collect(),
                    Err(e) => {
                        warn!(%account_id, error = %e, "backfill: list_mailboxes failed");
                        tokio::select! {
                            _ = shutdown.cancelled() => return,
                            _ = tokio::time::sleep(idle_poll) => continue,
                        }
                    }
                };

            // Everything this account downloaded counts — opened messages and
            // forwards too, not just this worker's batches.
            let spent = self
                .projector
                .downloaded_since(&account_id, chrono::Utc::now() - chrono::TimeDelta::hours(24))
                .await
                .unwrap_or(0);
            if let Some(pause) = backfill_pause(spent, daily_limit) {
                debug!(%account_id, spent, "backfill: daily download cap reached, waiting");
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(pause) => continue,
                }
            }

            let batch = match self.next_backfill_batch(&account_id, &abandoned).await {
                Ok(batch) => batch,
                // Not "all downloaded": the queue could not be read at all.
                Err(e) => {
                    warn!(%account_id, error = %e, "backfill: reading the queue failed");
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        _ = tokio::time::sleep(idle_poll) => continue,
                    }
                }
            };

            if batch.is_empty() {
                drained = 0;
                // Say so explicitly: this worker is otherwise silent while idle, so
                // without an "all done" event a watching client keeps showing the
                // last in-progress percentage forever. `total: 0` means "no backlog",
                // which is what stops a percentage being rendered at all.
                self.publish(SyncProgressEvent {
                    account_id,
                    state: SyncState::FetchingMessageBody,
                    processed: 0,
                    total: 0,
                    message: "All message bodies downloaded".to_string(),
                });

                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(idle_poll) => continue,
                }
            }

            let backlog = self
                .projector
                .count_pending_backfill(&account_id)
                .await
                .unwrap_or(batch.len() as u32);

            let rfc_ids: Vec<String> = batch.iter().filter_map(|e| e.rfc_message_id.clone()).collect();
            // One IMAP session per mailbox for the whole batch.
            let mut by_mailbox: HashMap<MailboxId, Vec<BackfillEntry>> = HashMap::new();
            for entry in batch {
                by_mailbox.entry(entry.mailbox_id).or_default().push(entry);
            }

            let mut fetch_failed = false;
            for (mailbox_id, entries) in by_mailbox {
                let Some(path) = path_by_id.get(&mailbox_id) else {
                    // Mailbox not yet known locally: nothing fetched, so retry
                    // on the failure cadence rather than every second.
                    fetch_failed = true;
                    continue;
                };
                match self.fetch_message_bodies_bulk(&account_id, path, &entries).await {
                    Ok(missing) => {
                        for message_id in missing {
                            let count = missing_counts.entry(message_id).or_insert(0);
                            *count += 1;
                            if *count >= Self::MAX_BACKFILL_MISSING_ATTEMPTS {
                                warn!(
                                    %account_id, %message_id, attempts = *count,
                                    "backfill: message missing from server after max retries, giving up"
                                );
                                missing_counts.remove(&message_id);
                                abandoned.insert(message_id);
                            } else {
                                warn!(
                                    %account_id, %message_id, attempts = *count,
                                    "backfill: message missing from bulk fetch result"
                                );
                            }
                        }
                    }
                    // The gate now holds the account's state: stop the pass here
                    // and let `ready` decide when to try again.
                    Err(ApplicationError::Domain(e)) if stops_the_pass(&e) => {
                        warn!(%account_id, error = %e, "backfill: cannot reach the account, pass stopped");
                        fetch_failed = true;
                        break;
                    }
                    Err(e) => {
                        // Leave them queued; retried next pass (this is a
                        // whole-fetch failure — e.g. network — not a
                        // per-message "gone from server" signal, so no cap).
                        warn!(%account_id, error = %e, "backfill: bulk body fetch failed");
                        fetch_failed = true;
                    }
                }
            }

            // Every other copy of what was just fetched gets it too.
            if let Err(e) = self.share_bodies(&account_id, &rfc_ids).await {
                warn!(%account_id, error = %e, "backfill: sharing fetched bodies failed");
            }

            let remaining = self
                .projector
                .count_pending_backfill(&account_id)
                .await
                .unwrap_or(0);
            drained += backlog.saturating_sub(remaining);
            self.publish(SyncProgressEvent {
                account_id,
                state: SyncState::FetchingMessageBody,
                processed: drained,
                total: drained + remaining,
                message: "Downloading message bodies".to_string(),
            });

            // A failed fetch costs no bytes; without this it would retry at once.
            if fetch_failed {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(idle_poll) => {}
                }
            }
        }
    }

    /// Sync a single mailbox: new envelopes from `from_uid` (or its
    /// checkpoint, if later), then reconciliation of what it holds. No
    /// account-wide scan.
    ///
    /// Called by the IMAP IDLE watcher on `NewMessage` notifications and on
    /// resync requests. Updates the checkpoint so the next `start_sync` won't
    /// re-fetch the same messages.
    ///
    /// Returns the number of new messages stored.
    #[instrument(skip(self), fields(%account_id, %mailbox_path, uid = from_uid.value()))]
    pub async fn sync_mailbox(
        &self,
        account_id: &AccountId,
        mailbox_path: &MailboxPath,
        from_uid: ProviderUid,
    ) -> Result<u32, ApplicationError> {
        let _exclusive = self.exclusive(account_id).await;
        let provider = self.provider_registry.get_provider(account_id)?;

        // Find the local mailbox record
        let local_mailboxes = self
            .projector
            .list_mailboxes(account_id)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("list_mailboxes failed: {e}")))?;
        // Compare by full_path() string so that delimiter differences between
        // the watcher's MailboxPath and the server-discovered stored path don't
        // cause a false mismatch (e.g. inbox() delimiter '/' vs server '.').
        let local = match local_mailboxes
            .iter()
            .find(|m| m.path.full_path() == mailbox_path.full_path())
        {
            Some(m) => m.clone(),
            None => {
                warn!(%mailbox_path, "sync_mailbox: local mailbox not found, skipping");
                return Ok(0);
            }
        };

        // Remote status (uid_next, uid_validity, counts)
        let status = match provider.mailbox_status(mailbox_path).await {
            Ok(s) => s,
            Err(e) => {
                warn!(%mailbox_path, error = %e, "sync_mailbox: mailbox_status failed");
                return Ok(0);
            }
        };

        let (new_count, _) = self
            .sync_one(&provider, &local, mailbox_path, from_uid.value(), status, None)
            .await?
            .unwrap_or_default();
        info!(%mailbox_path, new_count, "sync_mailbox: done");
        Ok(new_count)
    }

    /// Serve the resync requests message commands send when the server did
    /// more than they could record, until the senders are gone. A burst
    /// (a bulk move's chunks) syncs each mailbox once.
    pub async fn serve_resyncs(&self, mut requests: tokio::sync::mpsc::UnboundedReceiver<(AccountId, MailboxPath)>) {
        while let Some(batch) = resync_batch(&mut requests).await {
            for (account_id, path) in batch {
                // UID 0: from the mailbox's checkpoint.
                if let Err(e) = self.sync_mailbox(&account_id, &path, ProviderUid::new(0)).await {
                    warn!(%path, error = %e, "Resync failed");
                }
            }
        }
    }

    /// Cancels an ongoing sync operation.
    ///
    /// The sync loop checks the cancellation flag between mailboxes and
    /// between message batches, completing the current operation before stopping.
    pub fn cancel_sync(&self) {
        info!("Sync cancellation requested");
        self.cancelled.store(true, Ordering::SeqCst);
    }

    // ── Private helpers ───────────────────────────────────────────────

    /// Check whether cancellation has been requested.
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Bring one mailbox up to date: envelopes from its checkpoint (or
    /// `from`, if later) in batches, then counts and reconciliation. Each
    /// batch saves the checkpoint, so a timeout or crash resumes where it
    /// stopped. `(new, expunged)`, or `None` when it stopped early (cancelled,
    /// or the server failed).
    async fn sync_one(
        &self,
        provider: &Arc<PR::Provider>,
        local: &Mailbox,
        path: &MailboxPath,
        from: u32,
        status: MailboxStatus,
        mut progress: Option<&mut Vec<SyncProgressEvent>>,
    ) -> Result<Option<(u32, u32)>, ApplicationError> {
        let keys = SyncKeys::new(&local.account_id, path);
        let (resume, renumbering) = self.resume_point(local, &keys, &status).await?;
        let from = if renumbering { resume } else { resume.max(from) };
        // A renumbering settles its own UID-less rows (drop_unclaimed).
        let waiting = if renumbering {
            Vec::new()
        } else {
            self.projector
                .uidless_messages(&local.id)
                .await
                .map_err(|e| ApplicationError::SyncFailed(format!("uidless_messages failed: {e}")))?
        };

        let mut new_count = 0;
        if from < status.uid_next {
            let uids = match provider.uids_from(path, ProviderUid::new(from)).await {
                Ok(uids) => uids,
                Err(e) => {
                    warn!(%path, error = %e, "Failed to list new UIDs, skipping");
                    return Ok(None);
                }
            };
            let uids: Vec<ProviderUid> = uids.into_iter().filter(|u| u.value() < status.uid_next).collect();
            let label = format!("Fetching messages in {path}");
            for (index, batch) in uids.chunks(ENVELOPE_BATCH).enumerate() {
                // Only a full sync (the one reporting progress) is cancellable.
                if progress.is_some() && self.is_cancelled() {
                    return Ok(None);
                }
                if let Some(progress) = progress.as_deref_mut() {
                    self.emit(
                        progress,
                        SyncProgressEvent {
                            account_id: local.account_id,
                            state: SyncState::FetchingEnvelopes { mailbox_id: local.id },
                            processed: (index * ENVELOPE_BATCH) as u32,
                            total: uids.len() as u32,
                            message: label.clone(),
                        },
                    );
                }
                let (first, last) = (batch[0].value(), batch[batch.len() - 1].value());
                let envelopes = match provider.fetch_message_envelopes(path, FetchRange::UidRange { from: first, to: last }).await {
                    Ok(envelopes) => envelopes,
                    Err(e) => {
                        warn!(%path, error = %e, "Failed to fetch envelopes, resuming next cycle");
                        return Ok(None);
                    }
                };
                new_count += self.ingest(provider, local, path, &envelopes, false).await?;
                self.save_checkpoint(&keys.uid_next, last as u64 + 1).await?;
            }
        }

        if renumbering {
            self.drop_unclaimed(local).await?;
        }
        self.save_checkpoint(&keys.uid_next, status.uid_next as u64).await?;
        self.save_checkpoint(&keys.validity, status.uid_validity as u64).await?;
        if renumbering {
            self.save_checkpoint(&keys.renumbering, 0).await?;
        }

        self.update_mailbox_counts(local, status.total_messages, status.unseen_messages)
            .await?;
        let (mut expunged, flags_changed, restored) =
            self.reconcile_mailbox(provider, local, path, status.uid_next).await?;
        new_count += restored;
        expunged += self.settle_uidless(provider, local, path, waiting).await?;
        if expunged > 0 || flags_changed > 0 || restored > 0 {
            let updated = provider.mailbox_status(path).await.unwrap_or(status);
            self.update_mailbox_counts(local, updated.total_messages, updated.unseen_messages)
                .await?;
        }
        Ok(Some((new_count, expunged)))
    }

    /// Store one batch of envelopes, then their bodies when eager.
    /// `restored`: messages brought back, not new (see `MessageReceived`).
    async fn ingest(
        &self,
        provider: &Arc<PR::Provider>,
        local: &Mailbox,
        path: &MailboxPath,
        envelopes: &[MessageEnvelopeSummary],
        restored: bool,
    ) -> Result<u32, ApplicationError> {
        let (known_uids, awaiting_uid) = self.uid_state(local, envelopes).await?;
        let mut new_messages: Vec<(MessageId, ProviderUid)> = Vec::new();
        for env in envelopes {
            // Soft-deleted remotely (\Deleted set, not yet expunged): skip so it
            // doesn't resurface as "new" on a mailbox with no local history of it.
            if env.flags.contains(&Flag::Deleted) || known_uids.contains(&env.provider_uid) {
                continue;
            }
            if awaiting_uid && self.reattach(provider, local, env).await? {
                continue;
            }
            let placeholder = match self.local_copy(local, env).await {
                Some(stored) if stored.membership(local.id).is_some_and(|m| m.provider_uid.is_some()) => continue,
                other => other,
            };

            let (_message, mut event) = Message::receive(
                local.account_id,
                local.id,
                env.provider_uid,
                env.message_rfc_id.clone(),
                env.in_reply_to.clone(),
                env.references.clone(),
                env.envelope.clone(),
                env.flags.clone(),
                env.size_bytes,
                env.has_attachments,
                env.internal_date,
            );
            if let DomainEvent::MessageReceived { message_id, restored: quiet, .. } = &mut event {
                *quiet = restored;
                let message_id = *message_id;
                new_messages.push((message_id, env.provider_uid));
                self.append_and_project(&message_id.to_string(), AggregateType::Message, event)
                    .await?;
                self.supersede(placeholder).await?;
            }
        }

        if self.config.eager_body_fetch && !new_messages.is_empty() {
            let uids: Vec<ProviderUid> = new_messages.iter().map(|(_, u)| *u).collect();
            let uid_to_msg: HashMap<ProviderUid, MessageId> = new_messages.iter().map(|&(id, uid)| (uid, id)).collect();
            match provider.fetch_bodies_bulk(path, &uids).await {
                Ok(bodies) => {
                    for body in bodies {
                        // Search indexing is owned by the search-index projection.
                        if let Some(message_id) = uid_to_msg.get(&body.provider_uid)
                            && let Err(e) = self.store_body(message_id, body.body_text, body.body_html, body.truncated, vec![]).await
                        {
                            warn!(%message_id, error = %e, "Failed to project body");
                        }
                    }
                }
                Err(e) => warn!(%path, error = %e, "Bulk body fetch failed, skipping"),
            }
        }
        Ok(new_messages.len() as u32)
    }

    /// Mail we put on the server ourselves is stored locally before its
    /// server copy syncs back: a saved draft with the UID APPEND returned, a
    /// sent message with none (`MessageSent`). Scoped to Drafts and Sent: a
    /// per-envelope lookup would be a needless query per message on a big
    /// Inbox sync. Only this mailbox's rows count, and a UID-less one only if
    /// it really is a `MessageSent` placeholder: anything else without a UID
    /// is a received message waiting for it.
    async fn local_copy(&self, mailbox: &Mailbox, env: &MessageEnvelopeSummary) -> Option<MessageSummary> {
        if !matches!(mailbox.mailbox_type, MailboxType::Drafts | MailboxType::Sent) {
            return None;
        }
        let rfc_id = env.message_rfc_id.as_deref()?;
        let copies = self.projector.find_by_rfc_id(&mailbox.account_id, rfc_id).await.ok()?;
        for copy in copies {
            let Some(here) = copy.membership(mailbox.id) else {
                continue;
            };
            let ours = match here.provider_uid {
                Some(uid) => uid == env.provider_uid,
                None => mailbox.mailbox_type == MailboxType::Sent && self.is_sent_placeholder(&copy.id).await,
            };
            if ours {
                return Some(copy);
            }
        }
        None
    }

    async fn is_sent_placeholder(&self, message_id: &MessageId) -> bool {
        self.event_store
            .load_events(&message_id.to_string(), 0)
            .await
            .is_ok_and(|events| matches!(events.first().map(|e| &e.payload), Some(DomainEvent::MessageSent { .. })))
    }

    /// Which envelope UIDs `mailbox` already holds, and whether any message
    /// of the account is waiting for a UID (renumbered by a UIDVALIDITY
    /// change, or left behind by a move the server confirmed without saying
    /// where the message went).
    async fn uid_state(
        &self,
        mailbox: &Mailbox,
        envelopes: &[MessageEnvelopeSummary],
    ) -> Result<(HashSet<ProviderUid>, bool), ApplicationError> {
        let uids: Vec<ProviderUid> = envelopes.iter().map(|e| e.provider_uid).collect();
        let held = self
            .projector
            .held_uids(&mailbox.id, &uids)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("held_uids failed: {e}")))?;
        let mailboxes = self
            .projector
            .list_mailboxes(&mailbox.account_id)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("list_mailboxes failed: {e}")))?;
        for id in std::iter::once(mailbox.id).chain(mailboxes.iter().map(|m| m.id).filter(|id| *id != mailbox.id)) {
            let waiting = self
                .projector
                .uidless_messages(&id)
                .await
                .map_err(|e| ApplicationError::SyncFailed(format!("uidless_messages failed: {e}")))?;
            if !waiting.is_empty() {
                return Ok((held, true));
            }
        }
        Ok((held, false))
    }

    /// The server copy with a Message-ID the account already has keeps that
    /// message's id instead of arriving as a duplicate: a message waiting for
    /// its UID here gets it, else one waiting elsewhere that the server no
    /// longer has there moves here. A copy still present in its own mailbox
    /// is a genuine second copy (self-sent mail in Sent and INBOX) and stays.
    async fn reattach(
        &self,
        provider: &Arc<PR::Provider>,
        mailbox: &Mailbox,
        env: &MessageEnvelopeSummary,
    ) -> Result<bool, ApplicationError> {
        let Some(rfc_id) = env.message_rfc_id.as_deref() else {
            return Ok(false);
        };
        let copies = self
            .projector
            .find_by_rfc_id(&mailbox.account_id, rfc_id)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("find_by_rfc_id failed: {e}")))?;
        // (message, mailbox it waits for a UID in)
        let (here, elsewhere): (Vec<_>, Vec<_>) = copies
            .iter()
            .flat_map(|c| c.memberships.iter().filter(|m| m.provider_uid.is_none()).map(|m| (c.id, m.mailbox_id)))
            .partition(|&(_, in_mailbox)| in_mailbox == mailbox.id);
        for (waiting, _) in here {
            if self.update_message(&waiting, |m| m.attach_uid(mailbox.id, env.provider_uid)).await? {
                return Ok(true);
            }
        }
        for (orphan, from) in elsewhere {
            if self.is_sent_placeholder(&orphan).await || !self.gone_from_server(provider, from, rfc_id).await {
                continue;
            }
            let moved = self
                .update_message(&orphan, |m| {
                    m.membership(from).filter(|x| x.provider_uid.is_none())?;
                    m.move_to(from, mailbox.id, Some(env.provider_uid))
                })
                .await?;
            if moved {
                info!(message_id = %orphan, "Found again in another mailbox, moved there");
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether the server has no copy of a message waiting for a UID in
    /// `mailbox_id` left there that another local row doesn't already hold.
    /// Unsure counts as present: a wrong "gone" would steal the message from
    /// its mailbox.
    async fn gone_from_server(&self, provider: &Arc<PR::Provider>, mailbox_id: MailboxId, rfc_id: &str) -> bool {
        let Ok(Some(own)) = self.projector.get_mailbox(&mailbox_id).await else {
            return false;
        };
        let Ok(found) = provider.find_message(&own.path, rfc_id).await else {
            return false;
        };
        self.projector.held_uids(&own.id, &found).await.is_ok_and(|held| found.iter().all(|uid| held.contains(uid)))
    }

    /// Where ingest resumes in `local`, and whether a renumbering
    /// (UIDVALIDITY change) is under way. Starting one drops every UID the
    /// mailbox held — a stale one would hide the new message that reuses it —
    /// so the new epoch, ingested from UID 1, re-attaches each message by
    /// Message-ID. It is recorded under `keys.renumbering` (the new validity)
    /// and the old validity kept until the epoch is complete, so one cut
    /// short resumes from its checkpoint instead of starting over.
    async fn resume_point(
        &self,
        local: &Mailbox,
        keys: &SyncKeys,
        status: &MailboxStatus,
    ) -> Result<(u32, bool), ApplicationError> {
        let stored = self.projector.get_checkpoint(&keys.validity).await.unwrap_or(0) as u32;
        let resume = || async { (self.projector.get_checkpoint(&keys.uid_next).await.unwrap_or(0) as u32).max(1) };
        if stored == 0 || stored == status.uid_validity {
            return Ok((resume().await, false));
        }
        if self.projector.get_checkpoint(&keys.renumbering).await.unwrap_or(0) as u32 != status.uid_validity {
            warn!(path = %local.path, old = stored, new = status.uid_validity, "UIDVALIDITY changed, full resync needed");
            let all = mc_shared::pagination::Pagination { page: 1, page_size: u32::MAX };
            let held = self
                .projector
                .list_messages(&local.id, &all)
                .await
                .map_err(|e| ApplicationError::SyncFailed(format!("Failed to list local messages: {e}")))?;
            for message in &held.items {
                self.update_message(&message.id, |m| m.forget_uid(local.id)).await?;
            }
            // Checkpoint before marker: a marker with the old epoch's
            // checkpoint would resume past the new epoch's first UIDs.
            self.save_checkpoint(&keys.uid_next, 1).await?;
            self.save_checkpoint(&keys.renumbering, status.uid_validity as u64).await?;
        }
        Ok((resume().await, true))
    }

    /// Rows already waiting for a UID before this sync, and still waiting
    /// after its ingest: reconcile goes by UID and skips them, so nothing
    /// else would ever resolve them. The server copy found by Message-ID
    /// (one no other row holds) gives the UID back; with none the message is
    /// gone and is deleted here. Sent placeholders and drafts are ours and
    /// wait for their server copy instead. A server that can't answer
    /// leaves them for the next sync. Returns how many were deleted.
    async fn settle_uidless(
        &self,
        provider: &Arc<PR::Provider>,
        mailbox: &Mailbox,
        path: &MailboxPath,
        waiting: Vec<MessageId>,
    ) -> Result<u32, ApplicationError> {
        if waiting.is_empty() || mailbox.mailbox_type == MailboxType::Drafts {
            return Ok(0);
        }
        let mut deleted = 0;
        for message_id in waiting {
            if self.is_sent_placeholder(&message_id).await {
                continue;
            }
            let events = self
                .event_store
                .load_events(&message_id.to_string(), 0)
                .await
                .map_err(|e| ApplicationError::SyncFailed(format!("Failed to load events: {e}")))?;
            let events: Vec<DomainEvent> = events.into_iter().map(|e| e.payload).collect();
            let Some(message) = Message::rehydrate(&events).filter(|m| !m.is_deleted()) else {
                continue;
            };
            if message.membership(mailbox.id).is_none_or(|m| m.provider_uid.is_some()) {
                continue;
            }
            let found = match message.message_rfc_id.as_deref() {
                Some(rfc_id) => match provider.find_message(path, rfc_id).await {
                    Ok(found) => found,
                    Err(e) => {
                        warn!(%message_id, error = %e, "Could not look for a UID-less message, keeping it");
                        continue;
                    }
                },
                None => Vec::new(),
            };
            let held = self
                .projector
                .held_uids(&mailbox.id, &found)
                .await
                .map_err(|e| ApplicationError::SyncFailed(format!("held_uids failed: {e}")))?;
            let free = found.into_iter().rev().find(|uid| !held.contains(uid));
            let changed = self
                .update_message(&message_id, |m| {
                    if m.membership(mailbox.id).is_none_or(|x| x.provider_uid.is_some()) {
                        return None;
                    }
                    match free {
                        Some(uid) => m.attach_uid(mailbox.id, uid),
                        None => m.remove_from(mailbox.id),
                    }
                })
                .await?;
            if changed && free.is_none() {
                info!(%message_id, "UID-less and not on the server, deleted locally");
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// After a renumbered mailbox's whole new epoch was ingested, the received
    /// messages there still without a UID are gone from the server. Sent
    /// placeholders are not received messages, and stay.
    async fn drop_unclaimed(&self, mailbox: &Mailbox) -> Result<(), ApplicationError> {
        let waiting = self
            .projector
            .uidless_messages(&mailbox.id)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("uidless_messages failed: {e}")))?;
        for message_id in waiting {
            let dropped = self
                .update_message(&message_id, |m| {
                    m.membership(mailbox.id).filter(|x| x.provider_uid.is_none())?;
                    m.remove_from(mailbox.id)
                })
                .await?;
            if dropped {
                info!(%message_id, "Not in the renumbered mailbox, deleted locally");
            }
        }
        Ok(())
    }

    async fn save_checkpoint(&self, key: &str, value: u64) -> Result<(), ApplicationError> {
        self.projector
            .save_checkpoint(key, value)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Checkpoint save failed: {e}")))
    }

    /// Apply `change` to the message's current state and append what it
    /// produces at that state's sequence: a decision taken from the read model
    /// must not land on a message that has changed since (moved again, say).
    async fn update_message(
        &self,
        message_id: &MessageId,
        change: impl FnOnce(&mut Message) -> Option<DomainEvent>,
    ) -> Result<bool, ApplicationError> {
        let aggregate_id = message_id.to_string();
        let events = self
            .event_store
            .load_events(&aggregate_id, 0)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to load events: {e}")))?;
        let seq = events.len() as u64;
        let events: Vec<DomainEvent> = events.into_iter().map(|e| e.payload).collect();
        let Some(event) = Message::rehydrate(&events).as_mut().and_then(change) else {
            return Ok(false);
        };
        self.append_at(&aggregate_id, AggregateType::Message, seq, event).await?;
        Ok(true)
    }

    /// The server copy just stored replaces the one shown at send time. The
    /// projector counts recipients once per Message-ID, so the copy is not
    /// counted again.
    async fn supersede(&self, placeholder: Option<MessageSummary>) -> Result<(), ApplicationError> {
        let Some(sent) = placeholder else {
            return Ok(());
        };
        let event = DomainEvent::MessageDeleted {
            message_id: sent.id,
            mailbox_ids: sent.memberships.iter().map(|m| m.mailbox_id).collect(),
        };
        self.append_and_project(&sent.id.to_string(), AggregateType::Message, event)
            .await
    }

    /// Body first, then the event: if the append fails, the backfill queue (drained
    /// only by projecting `MessageBodyFetched`) still lists the message, so it is
    /// fetched again instead of being marked done without a record.
    async fn store_body(
        &self,
        message_id: &MessageId,
        body_text: Option<String>,
        body_html: Option<String>,
        truncated: bool,
        attachments: Vec<Attachment>,
    ) -> Result<(), ApplicationError> {
        self.projector
            .put_body(message_id, body_text, body_html, truncated)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to store body: {e}")))?;
        let event = DomainEvent::MessageBodyFetched { message_id: *message_id, attachments };
        self.append_and_project(&message_id.to_string(), AggregateType::Message, event)
            .await
    }

    /// Gives the copies of these messages a body another copy already has
    /// (see `MessageBodyStore::share_bodies`) and records each; returns them.
    async fn share_bodies(
        &self,
        account_id: &AccountId,
        rfc_ids: &[String],
    ) -> Result<Vec<MessageId>, ApplicationError> {
        let shared = self.projector.share_bodies(account_id, rfc_ids).await?;
        for (message_id, _) in &shared {
            let event = DomainEvent::MessageBodyFetched { message_id: *message_id, attachments: vec![] };
            self.append_and_project(&message_id.to_string(), AggregateType::Message, event).await?;
        }
        Ok(shared.into_iter().map(|(id, _)| id).collect())
    }

    /// The next bodies to download: the head of the queue less the copies
    /// of a message whose body is already stored, which get it for free, and
    /// one copy per Message-ID. The others get theirs once it is fetched.
    async fn next_backfill_batch(
        &self,
        account_id: &AccountId,
        abandoned: &HashSet<MessageId>,
    ) -> Result<Vec<BackfillEntry>, ApplicationError> {
        loop {
            let pending = self.projector.list_pending_backfill(account_id, 500).await?;
            let rfc_ids: Vec<String> = pending.iter().filter_map(|e| e.rfc_message_id.clone()).collect();
            let shared: HashSet<MessageId> = self.share_bodies(account_id, &rfc_ids).await?.into_iter().collect();
            let mut seen = HashSet::new();
            let batch: Vec<BackfillEntry> = pending
                .into_iter()
                .filter(|e| !abandoned.contains(&e.message_id) && !shared.contains(&e.message_id))
                .filter(|e| {
                    e.rfc_message_id
                        .as_ref()
                        .is_none_or(|id| seen.insert((id.clone(), e.size_bytes, e.from_email.clone())))
                })
                .take(BACKFILL_BATCH)
                .collect();
            // A page of nothing but copies (All Mail, late in the queue) says
            // nothing about the rows behind it.
            if !batch.is_empty() || shared.is_empty() {
                return Ok(batch);
            }
        }
    }

    /// Append a domain event to the event store and project it into the read model.
    ///
    /// Reads the aggregate's current sequence for optimistic concurrency, then
    /// appends the event and immediately projects it.
    async fn append_and_project(
        &self,
        aggregate_id: &str,
        aggregate_type: AggregateType,
        event: DomainEvent,
    ) -> Result<(), ApplicationError> {
        let expected_seq = self
            .event_store
            .current_sequence(aggregate_id)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to load sequence: {e}")))?;
        self.append_at(aggregate_id, aggregate_type, expected_seq, event).await
    }

    async fn append_at(
        &self,
        aggregate_id: &str,
        aggregate_type: AggregateType,
        expected_seq: u64,
        event: DomainEvent,
    ) -> Result<(), ApplicationError> {
        let new_seq = self
            .event_store
            .append(
                aggregate_id,
                aggregate_type,
                expected_seq,
                vec![event.clone()],
            )
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to append event: {e}")))?;

        let envelope = EventEnvelope::new(aggregate_id.to_string(), aggregate_type, new_seq, event);
        self.project_and_publish(envelope).await
    }

    /// Server-reported state (sync lifecycle, mailbox counts): applied to the
    /// read model and announced on the bus, but never appended — it is not a
    /// domain fact, and appending it every poll grew the log without bound.
    async fn project_transient(
        &self,
        aggregate_id: &str,
        aggregate_type: AggregateType,
        event: DomainEvent,
    ) -> Result<(), ApplicationError> {
        self.project_and_publish(EventEnvelope::new(aggregate_id.to_string(), aggregate_type, 0, event))
            .await
    }

    async fn project_and_publish(&self, envelope: EventEnvelope) -> Result<(), ApplicationError> {
        self.projector
            .project(&envelope)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to project event: {e}")))?;
        if let Some(publisher) = &self.event_publisher {
            publisher.publish(envelope);
        }
        Ok(())
    }

    /// Reconcile local state with remote: detect expunged messages and flag changes.
    ///
    /// Fetches current UIDs+flags from the remote mailbox (lightweight IMAP FETCH FLAGS),
    /// compares with the local read model, and emits:
    /// - `MessageDeleted` for local messages whose UID no longer exists remotely
    /// - `MessageFlagged` for local messages whose flags differ from remote
    ///
    /// and ingests server messages below `uid_next` that no local row holds
    /// (see [`Self::restore`]). Returns `(expunged, flags_changed, restored)`.
    #[instrument(skip(self, provider, local), fields(mailbox_id = %local.id))]
    async fn reconcile_mailbox(
        &self,
        provider: &Arc<<PR as ProviderRegistry>::Provider>,
        local: &Mailbox,
        remote_path: &MailboxPath,
        uid_next: u32,
    ) -> Result<(u32, u32, u32), ApplicationError> {
        let remote_entries = match provider.fetch_uid_flags(remote_path).await {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "fetch_uid_flags failed, skipping reconciliation");
                return Ok((0, 0, 0));
            }
        };

        let remote_by_uid: std::collections::HashMap<u32, &Vec<Flag>> = remote_entries
            .iter()
            .map(|e| (e.uid.value(), &e.flags))
            .collect();

        // Load ALL local message summaries for this mailbox
        let all_pagination = mc_shared::pagination::Pagination {
            page: 1,
            page_size: u32::MAX,
        };
        let local_messages = self
            .projector
            .list_messages(&local.id, &all_pagination)
            .await
            .map_err(|e| {
                ApplicationError::SyncFailed(format!("Failed to list local messages: {e}"))
            })?;

        let mut expunged = 0u32;
        let mut flags_changed = 0u32;

        for summary in &local_messages.items {
            let Some(local_uid) = summary.membership(local.id).and_then(|m| m.provider_uid).map(|uid| uid.value())
            else {
                continue;
            };

            if remote_by_uid
                .get(&local_uid)
                .is_some_and(|flags| flags.contains(&Flag::Deleted))
            {
                // Remotely soft-deleted (\Deleted set, not yet expunged): treat like
                // an expunge rather than just reconciling the flag, so it disappears
                // from the mailbox listing instead of lingering with \Deleted set.
                info!(
                    message_id = %summary.id,
                    uid = local_uid,
                    "Message marked \\Deleted remotely, deleting locally"
                );
                self.append_and_project(&summary.id.to_string(), AggregateType::Message, left_mailbox(summary, local.id))
                    .await?;
                // De-indexing owned by the search-index projection (MessageDeleted).
                expunged += 1;
            } else if let Some(remote_flags) = remote_by_uid.get(&local_uid) {
                // Message exists remotely — check for flag differences.
                // \Recent is server-managed and volatile (present on first fetch,
                // gone after the mailbox is selected) — never diff it, or every
                // reconcile emits spurious flag churn.
                let flags_to_add: Vec<Flag> = remote_flags
                    .iter()
                    .filter(|f| **f != Flag::Recent && !summary.flags.contains(f))
                    .cloned()
                    .collect();
                let flags_to_remove: Vec<Flag> = summary
                    .flags
                    .iter()
                    .filter(|f| **f != Flag::Recent && !remote_flags.contains(f))
                    .cloned()
                    .collect();

                if !flags_to_add.is_empty() || !flags_to_remove.is_empty() {
                    debug!(
                        message_id = %summary.id,
                        uid = local_uid,
                        ?flags_to_add,
                        ?flags_to_remove,
                        "Reconciling flag diff"
                    );
                    let event = DomainEvent::MessageFlagged {
                        message_id: summary.id,
                        flags_added: flags_to_add,
                        flags_removed: flags_to_remove,
                    };
                    self.append_and_project(
                        &summary.id.to_string(),
                        AggregateType::Message,
                        event,
                    )
                    .await?;
                    flags_changed += 1;
                }
            } else {
                // Message no longer exists remotely — expunged
                info!(
                    message_id = %summary.id,
                    uid = local_uid,
                    "Message expunged remotely, deleting locally"
                );
                self.append_and_project(&summary.id.to_string(), AggregateType::Message, left_mailbox(summary, local.id))
                    .await?;
                // De-indexing owned by the search-index projection (MessageDeleted).
                expunged += 1;
            }
        }

        // Holes below the checkpoint: messages on the server no row here
        // holds, because a local change dropped a row the server kept.
        // Ingest only walks up from the checkpoint and would never see them.
        // A \Deleted copy is gone as far as sync goes (and may be one we
        // deleted on a server without UIDPLUS). Not in Drafts: a draft
        // discarded or replaced here is deleted on the server afterwards,
        // and bringing it back in between would resurrect it — a stray
        // server copy is harmless there, a revived draft is not.
        let restorable = local.mailbox_type != MailboxType::Drafts;
        let held: HashSet<u32> =
            local_messages.items.iter().filter_map(|m| m.membership(local.id)?.provider_uid.map(|uid| uid.value())).collect();
        let mut unheld: Vec<u32> = remote_entries
            .iter()
            .filter(|e| restorable && e.uid.value() < uid_next)
            .filter(|e| !held.contains(&e.uid.value()) && !e.flags.contains(&Flag::Deleted))
            .map(|e| e.uid.value())
            .collect();
        unheld.sort_unstable();
        let restored = self.restore(provider, local, remote_path, &unheld).await?;

        if expunged > 0 || flags_changed > 0 || restored > 0 {
            info!(
                expunged,
                flags_changed,
                restored,
                "Reconciliation complete for mailbox"
            );
        }

        Ok((expunged, flags_changed, restored))
    }

    /// Ingest `uids` (ascending) like new mail, but quietly: they were here
    /// before, so nobody is notified of them again. At most [`RESTORE_CAP`]
    /// a sync, newest first; a batch the server fails on is skipped, the
    /// rest still come back, and the next sync tries the remainder.
    async fn restore(
        &self,
        provider: &Arc<PR::Provider>,
        local: &Mailbox,
        path: &MailboxPath,
        uids: &[u32],
    ) -> Result<u32, ApplicationError> {
        let mut restored = 0;
        for batch in uids[uids.len().saturating_sub(RESTORE_CAP)..].chunks(ENVELOPE_BATCH) {
            if self.is_cancelled() {
                break;
            }
            let range = FetchRange::Uids(batch.iter().map(|&uid| ProviderUid::new(uid)).collect());
            match provider.fetch_message_envelopes(path, range).await {
                Ok(envelopes) => restored += self.ingest(provider, local, path, &envelopes, true).await?,
                Err(e) => warn!(%path, error = %e, "Failed to fetch envelopes missing locally, skipping them this sync"),
            }
        }
        Ok(restored)
    }

    /// Update mailbox message counts (transient: read model + bus, not the log).
    async fn update_mailbox_counts(
        &self,
        mailbox: &Mailbox,
        total_messages: u32,
        unread_messages: u32,
    ) -> Result<(), ApplicationError> {
        let event = DomainEvent::MailboxCountsUpdated {
            mailbox_id: mailbox.id,
            total_messages,
            unread_messages,
        };
        self.project_transient(&mailbox.id.to_string(), AggregateType::Mailbox, event)
            .await
    }
}

#[async_trait::async_trait]
impl<
    PR: ProviderRegistry,
    ES: EventStore,
    P: ReadModelProjector + MailboxReadRepository + MessageReadRepository + ProjectionCheckpoint + BodyBackfillQueue + MessageBodyStore,
> SyncTrigger for SyncService<PR, ES, P>
{
    async fn trigger_sync(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<SyncProgressEvent>, ApplicationError> {
        self.start_sync(account_id).await
    }

    fn subscribe_progress(&self) -> broadcast::Receiver<SyncProgressEvent> {
        SyncService::subscribe_progress(self)
    }
}

#[cfg(test)]
mod tests;
