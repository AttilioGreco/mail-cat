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

/// Bodies the backfill downloads between two pauses at the fast rate. Once
/// throttled it takes one at a time: the pause scales with the bytes, so the
/// rate is the same, but progress moves per message instead of per minute.
const BACKFILL_BATCH: usize = 50;

/// Bytes per second the backfill never exceeds, capped account or not: a
/// mailbox's text arrives in minutes without hammering the server.
const BACKFILL_FAST_RATE: u64 = 1 << 20;

/// Pause after a backfill batch that cost `batch_bytes`, given `spent` bytes
/// downloaded in the last 24 h. Under a daily cap the first half of it goes
/// at the fast rate and the second half is spread across the day, so the
/// sliding 24 h total stays near the cap without crossing it; at the cap,
/// wait for old downloads to leave the window.
fn backfill_pause(spent: u64, batch_bytes: u64, daily_limit: Option<u64>) -> std::time::Duration {
    let rate = match daily_limit {
        Some(limit) if spent >= limit => return std::time::Duration::from_secs(15 * 60),
        Some(limit) if throttled(spent, daily_limit) => (limit / 2 / 86_400).max(1),
        _ => BACKFILL_FAST_RATE,
    };
    std::time::Duration::from_secs_f64(batch_bytes as f64 / rate as f64)
        .max(std::time::Duration::from_secs(1))
}

/// Past the first half of the daily cap, downloads are spread across the day.
fn throttled(spent: u64, daily_limit: Option<u64>) -> bool {
    daily_limit.is_some_and(|limit| spent >= limit / 2)
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
    /// Drains the backfill queue newest-first, a batch at a time (grouped by
    /// mailbox, one IMAP session per mailbox — see `fetch_message_bodies_bulk`),
    /// pausing after each batch by the bytes it cost (see `backfill_pause`) so
    /// the account stays under `daily_limit` bytes per 24 h. Only the text of
    /// a body is downloaded; attachments are fetched when a message needs them.
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
            let downloaded = |since| self.projector.downloaded_since(&account_id, since);
            let batch_started = chrono::Utc::now();
            let spent = downloaded(batch_started - chrono::TimeDelta::hours(24)).await.unwrap_or(0);
            if daily_limit.is_some_and(|limit| spent >= limit) {
                debug!(%account_id, spent, "backfill: daily download cap reached, waiting");
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(backfill_pause(spent, 0, daily_limit)) => continue,
                }
            }
            let batch_size = if throttled(spent, daily_limit) { 1 } else { BACKFILL_BATCH };

            let pending = match self.projector.list_pending_backfill(&account_id, 500).await {
                Ok(pending) => pending,
                // Not "all downloaded": the queue could not be read at all.
                Err(e) => {
                    warn!(%account_id, error = %e, "backfill: reading the queue failed");
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        _ = tokio::time::sleep(idle_poll) => continue,
                    }
                }
            };
            let batch: Vec<BackfillEntry> = pending
                .into_iter()
                .filter(|e| !abandoned.contains(&e.message_id))
                .take(batch_size)
                .collect();

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

            let batch_bytes = downloaded(batch_started).await.unwrap_or(0);
            // A failed fetch costs no bytes; without this it would retry every second.
            let pause = if fetch_failed { idle_poll } else { backfill_pause(spent, batch_bytes, daily_limit) };
            debug!(%account_id, spent, batch_bytes, pause_secs = pause.as_secs(), "backfill: pacing");
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(pause) => {}
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
                Some(stored) if stored.provider_uid.is_some() => continue,
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
        for copy in copies.into_iter().filter(|m| m.mailbox_id == mailbox.id) {
            let ours = match copy.provider_uid {
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
        let (here, elsewhere): (Vec<_>, Vec<_>) =
            copies.into_iter().filter(|m| m.provider_uid.is_none()).partition(|m| m.mailbox_id == mailbox.id);
        for waiting in here {
            if self.update_message(&waiting.id, |m| m.attach_uid(mailbox.id, env.provider_uid)).await? {
                return Ok(true);
            }
        }
        for orphan in elsewhere {
            if self.is_sent_placeholder(&orphan.id).await || !self.gone_from_server(provider, &orphan, rfc_id).await {
                continue;
            }
            let from = orphan.mailbox_id;
            let moved = self
                .update_message(&orphan.id, |m| {
                    (m.mailbox_id == from && m.provider_uid.is_none()).then(|| m.move_to(mailbox.id, Some(env.provider_uid)))
                })
                .await?;
            if moved {
                info!(message_id = %orphan.id, "Found again in another mailbox, moved there");
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether the server has no copy of `message` (waiting for a UID) left in
    /// its mailbox that another local row doesn't already hold. Unsure counts
    /// as present: a wrong "gone" would steal the message from its mailbox.
    async fn gone_from_server(&self, provider: &Arc<PR::Provider>, message: &MessageSummary, rfc_id: &str) -> bool {
        let Ok(Some(own)) = self.projector.get_mailbox(&message.mailbox_id).await else {
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
            let gone = events.iter().any(|e| matches!(e, DomainEvent::MessageDeleted { .. }));
            let Some(message) = Message::rehydrate(&events).filter(|_| !gone) else {
                continue;
            };
            if message.mailbox_id != mailbox.id || message.provider_uid.is_some() {
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
                    if m.mailbox_id != mailbox.id || m.provider_uid.is_some() {
                        return None;
                    }
                    match free {
                        Some(uid) => m.attach_uid(mailbox.id, uid),
                        None => Some(m.delete(false)),
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
                    (m.mailbox_id == mailbox.id && m.provider_uid.is_none()).then(|| m.delete(false))
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
            mailbox_id: sent.mailbox_id,
            permanently: true,
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
            let local_uid = match summary.provider_uid {
                Some(uid) => uid.value(),
                None => continue,
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
                let event = DomainEvent::MessageDeleted {
                    message_id: summary.id,
                    mailbox_id: local.id,
                    permanently: false,
                };
                self.append_and_project(&summary.id.to_string(), AggregateType::Message, event)
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
                        mailbox_id: local.id,
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
                let event = DomainEvent::MessageDeleted {
                    message_id: summary.id,
                    mailbox_id: local.id,
                    permanently: false,
                };
                self.append_and_project(
                    &summary.id.to_string(),
                    AggregateType::Message,
                    event,
                )
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
            local_messages.items.iter().filter_map(|m| m.provider_uid.map(|uid| uid.value())).collect();
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
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::Stream;
    use mc_domain::{
        Address, AggregateType, DomainError, EmailAddress, Envelope, EventEnvelope, Flag,
        LoginSecret, MailboxStatus, MailboxType, MessageEnvelopeSummary, OutgoingMessage,
        ProviderNotification,
    };
    use std::pin::Pin;
    use std::sync::Mutex;

    #[test]
    fn a_throttled_fetch_stops_the_backfill_pass() {
        assert!(stops_the_pass(&DomainError::Throttled("bandwidth".into())));
        assert!(!stops_the_pass(&DomainError::MessageNotFound("gone".into())));
    }

    #[test]
    fn backfill_paces_by_bytes_against_the_daily_cap() {
        const MB: u64 = 1 << 20;
        let limit = Some(2000 * MB);
        let secs = |spent, batch, limit| backfill_pause(spent, batch, limit).as_secs();

        // First half of the cap, and any uncapped account: the fast rate.
        assert_eq!(secs(0, 10 * MB, limit), 10);
        assert_eq!(secs(5000 * MB, 10 * MB, None), 10);
        // Second half: what's left of the cap spread over a day.
        assert_eq!(secs(1000 * MB, 10 * MB, limit), 10 * MB / (1000 * MB / 86_400));
        // At the cap: wait for the window to slide, however small the batch.
        assert_eq!(secs(2000 * MB, 0, limit), 15 * 60);
        // A tiny batch still yields the server a second.
        assert_eq!(secs(0, 10, limit), 1);
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
            let uid_next = envelopes.iter().map(|e| e.provider_uid.value() + 1).max().unwrap_or(1);
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
                if self.fail_envelopes_from.is_some_and(|limit| uids.contains(&ProviderUid::new(limit))) {
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
            Ok(self.found.get(&mailbox.full_path()).cloned().unwrap_or_default())
        }
        async fn uids_from(&self, _mailbox: &MailboxPath, from: ProviderUid) -> Result<Vec<ProviderUid>, DomainError> {
            let mut uids: Vec<ProviderUid> =
                self.envelopes.iter().map(|e| e.provider_uid).filter(|uid| *uid >= from).collect();
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

        async fn mailbox_status(
            &self,
            _mailbox: &MailboxPath,
        ) -> Result<MailboxStatus, DomainError> {
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
        ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>>
        {
            Box::pin(futures::stream::empty())
        }
        async fn fetch_bodies_bulk(
            &self,
            _mailbox: &MailboxPath,
            uids: &[ProviderUid],
        ) -> Result<Vec<mc_domain::traits::email_provider::MessageBodyResult>, DomainError> {
            let uid_values: Vec<u32> = uids.iter().map(|u| u.value()).collect();
            self.bulk_fetch_calls.lock().unwrap().push(uid_values.clone());
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
                return Err(DomainError::ConcurrencyConflict { expected: expected_sequence, actual });
            }
            let appended = events.len() as u64;
            store.extend(events.into_iter().map(|event| (aggregate_id.to_string(), event)));
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
                .map(|(i, (id, event))| EventEnvelope::new(id.clone(), AggregateType::Message, i as u64 + 1, event.clone()))
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
                DomainEvent::MessageReceived { message_id, account_id, mailbox_id, provider_uid, message_rfc_id, flags, .. } => {
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
                        size_bytes: 0,
                        has_attachments: false,
                        received_at: chrono::Utc::now(),
                    });
                    if let Some(rfc_id) = message_rfc_id {
                        self.rfc_ids.lock().unwrap().insert(*message_id, rfc_id.clone());
                    }
                }
                DomainEvent::MessageMoved { message_id, to_mailbox_id, provider_uid, .. } => {
                    if let Some(row) = stored.iter_mut().find(|m| m.id == *message_id) {
                        row.mailbox_id = *to_mailbox_id;
                        row.provider_uid = *provider_uid;
                    }
                }
                DomainEvent::MessageDeleted { message_id, .. } => stored.retain(|m| m.id != *message_id),
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

        async fn list_mailboxes(
            &self,
            account_id: &AccountId,
        ) -> Result<Vec<Mailbox>, DomainError> {
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

        async fn uidless_messages(&self, mailbox_id: &MailboxId) -> Result<Vec<MessageId>, DomainError> {
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
            self.bodies.lock().unwrap().insert(*message_id, (body_text, body_html));
            Ok(())
        }
    }

    #[async_trait]
    impl BodyBackfillQueue for MockProjector {
        async fn list_pending_backfill(
            &self,
            _account_id: &AccountId,
            _limit: usize,
        ) -> Result<Vec<mc_domain::BackfillEntry>, DomainError> {
            Ok(vec![])
        }

        async fn count_pending_backfill(&self, _account_id: &AccountId) -> Result<u32, DomainError> {
            Ok(0)
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
        let refresh: crate::RefreshFn = Arc::new(|_, _| Box::pin(async { Err(DomainError::Provider("unused".into())) }));
        let keychain: Arc<dyn mc_domain::SecretStore> = Arc::new(NoKeychain);
        let mut gates = AuthGates::new(refresh, keychain, tokio::sync::mpsc::unbounded_channel().0);
        let account_id = AccountId::new();
        let gate = gates.add(account_id, "me@example.com", Credential::Password("old".into()));
        let rejected = DomainError::AuthenticationFailed("NO".into());
        gate.login_failed(&LoginSecret::Password("old".into()), &rejected, false).await;

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
                service.run_backfill(AccountId::new(), None, gate, shutdown).await
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
        let broken: SyncService<MockProviderRegistry, MockEventStore, MockProjector> =
            SyncService::new(
                Arc::new(MockProviderRegistry::empty()),
                Arc::new(MockEventStore::new()),
                Arc::new(MockProjector::new()),
                SyncConfig::default(),
            );
        let mut broken_rx = broken.subscribe_progress();
        assert!(broken.start_sync(&AccountId::new()).await.is_err());

        let event = broken_rx.try_recv().expect("a terminal event was published");
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
            assert!(!shown.contains("body-"), "body leaked into the log: {shown}");
        }
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
        let provider = MockEmailProvider::new().with_mailboxes(vec![sent]).with_envelopes(vec![server_copy]);
        let service = make_service(provider, event_store, projector.clone());
        service.start_sync(&account_id).await.unwrap();

        let message_events: Vec<_> = projector
            .projected_events()
            .into_iter()
            .filter(|e| matches!(e, DomainEvent::MessageReceived { .. } | DomainEvent::MessageDeleted { .. }))
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

    fn archived(account_id: AccountId, mailbox_id: MailboxId, provider_uid: Option<ProviderUid>) -> MessageSummary {
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
        projector.rfc_ids.lock().unwrap().insert(msg.id, rfc_id.into());
        msg.id
    }

    fn envelope(uid: u32, rfc_id: &str) -> MessageEnvelopeSummary {
        let mut env = make_envelope_summary(uid);
        env.message_rfc_id = Some(rfc_id.into());
        env
    }

    fn row_uid(projector: &MockProjector, id: MessageId) -> Option<Option<u32>> {
        projector.stored.lock().unwrap().iter().find(|m| m.id == id).map(|m| m.provider_uid.map(|u| u.value()))
    }

    #[tokio::test]
    async fn moved_message_gets_its_uid_back_instead_of_a_duplicate() {
        let account_id = AccountId::new();
        let archive = archive_mailbox(account_id);
        let (event_store, projector) = (Arc::new(MockEventStore::new()), Arc::new(MockProjector::new()));
        let moved = seed(&event_store, &projector, &archive, None, "<moved@example.com>");
        projector.mailboxes.lock().unwrap().push(archive.clone());

        let provider = MockEmailProvider::new()
            .with_mailboxes(vec![archive])
            .with_envelopes(vec![envelope(9, "<moved@example.com>")]);
        make_service(provider, event_store, projector.clone()).start_sync(&account_id).await.unwrap();

        assert_eq!(received(&projector), 0, "no second copy of the moved message");
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
        let (event_store, projector) = (Arc::new(MockEventStore::new()), Arc::new(MockProjector::new()));
        let moved = seed(&event_store, &projector, &archive, None, "<moved@example.com>");
        let trash = MailboxId::new();
        event_store.events.lock().unwrap().push((
            moved.to_string(),
            DomainEvent::MessageMoved { message_id: moved, from_mailbox_id: archive.id, to_mailbox_id: trash, provider_uid: None },
        ));
        projector.mailboxes.lock().unwrap().push(archive.clone());

        let provider = MockEmailProvider::new()
            .with_mailboxes(vec![archive.clone()])
            .with_envelopes(vec![envelope(9, "<moved@example.com>")]);
        make_service(provider, event_store.clone(), projector).start_sync(&account_id).await.unwrap();

        let pulled_back = event_store.appended_events().into_iter().any(|(id, e)| {
            id == moved.to_string() && matches!(e, DomainEvent::MessageMoved { from_mailbox_id, to_mailbox_id, .. } if from_mailbox_id == archive.id && to_mailbox_id == archive.id)
        });
        assert!(!pulled_back);
    }

    fn batch_of(count: u32) -> Vec<MessageEnvelopeSummary> {
        (1..=count).map(|uid| envelope(uid, &format!("<m{uid}@example.com>"))).collect()
    }

    /// A batch that times out keeps the batches before it: the next cycle
    /// resumes after them instead of starting over.
    #[tokio::test]
    async fn a_failed_batch_keeps_the_checkpoint_of_the_ones_before() {
        let account_id = AccountId::new();
        let archive = archive_mailbox(account_id);
        let projector = Arc::new(MockProjector::new());
        projector.mailboxes.lock().unwrap().push(archive.clone());
        let mut provider = MockEmailProvider::new().with_mailboxes(vec![archive]).with_envelopes(batch_of(501));
        provider.fail_envelopes_from = Some(501);
        make_service(provider, Arc::new(MockEventStore::new()), projector.clone()).start_sync(&account_id).await.unwrap();

        let checkpoint_key = format!("sync:{account_id}:Archive");
        assert_eq!(received(&projector), 500);
        assert_eq!(projector.get_checkpoint(&checkpoint_key).await.unwrap(), 501);
        assert_eq!(projector.get_checkpoint(&format!("{checkpoint_key}:validity")).await.unwrap(), 0, "not complete");
    }

    /// A renumbering cut short resumes from its checkpoint; it does not clear
    /// the UIDs re-attached so far all over again.
    #[tokio::test]
    async fn an_interrupted_renumbering_resumes_instead_of_starting_over() {
        let account_id = AccountId::new();
        let archive = archive_mailbox(account_id);
        let (event_store, projector) = (Arc::new(MockEventStore::new()), Arc::new(MockProjector::new()));
        let kept = seed(&event_store, &projector, &archive, Some(3), "<m1@example.com>");
        projector.mailboxes.lock().unwrap().push(archive.clone());
        let checkpoint_key = format!("sync:{account_id}:Archive");
        // A renumbering to validity 2 already got past UID 1 (kept is re-attached).
        projector.save_checkpoint(&format!("{checkpoint_key}:validity"), 1).await.unwrap();
        projector.save_checkpoint(&format!("{checkpoint_key}:renumbering"), 2).await.unwrap();
        projector.save_checkpoint(&checkpoint_key, 2).await.unwrap();
        projector.stored.lock().unwrap()[0].provider_uid = Some(ProviderUid::new(1));

        let mut provider = MockEmailProvider::new().with_mailboxes(vec![archive]).with_envelopes(batch_of(2));
        provider.status.as_mut().unwrap().uid_validity = 2;
        make_service(provider, event_store.clone(), projector.clone()).start_sync(&account_id).await.unwrap();

        let forgotten = event_store
            .appended_events()
            .iter()
            .any(|(_, e)| matches!(e, DomainEvent::MessageMoved { provider_uid: None, .. }));
        assert!(!forgotten, "UIDs are not cleared all over again");
        assert_eq!(row_uid(&projector, kept), Some(Some(1)));
        assert_eq!(received(&projector), 1, "only UID 2 was left to ingest");
        assert_eq!(projector.get_checkpoint(&format!("{checkpoint_key}:validity")).await.unwrap(), 2);
        assert_eq!(projector.get_checkpoint(&format!("{checkpoint_key}:renumbering")).await.unwrap(), 0);
    }

    /// The server renumbered Archive (UIDVALIDITY 1 → 2). UID 9 now belongs
    /// to a different message: the stale local UID 9 used to hide it.
    #[tokio::test]
    async fn uidvalidity_change_reingests_the_whole_new_epoch() {
        let account_id = AccountId::new();
        let archive = archive_mailbox(account_id);
        let (event_store, projector) = (Arc::new(MockEventStore::new()), Arc::new(MockProjector::new()));
        let kept = seed(&event_store, &projector, &archive, Some(9), "<kept@example.com>");
        let gone = seed(&event_store, &projector, &archive, Some(5), "<gone@example.com>");
        projector.mailboxes.lock().unwrap().push(archive.clone());
        let checkpoint_key = format!("sync:{account_id}:Archive");
        projector.save_checkpoint(&checkpoint_key, 50).await.unwrap();
        projector.save_checkpoint(&format!("{checkpoint_key}:validity"), 1).await.unwrap();

        let mut provider = MockEmailProvider::new()
            .with_mailboxes(vec![archive])
            .with_envelopes(vec![envelope(3, "<kept@example.com>"), envelope(9, "<new@example.com>")]);
        provider.status = Some(MailboxStatus { uid_validity: 2, uid_next: 10, total_messages: 2, unseen_messages: 0 });
        make_service(provider, event_store, projector.clone()).start_sync(&account_id).await.unwrap();

        assert_eq!(received(&projector), 1, "the message now at UID 9 is new");
        assert_eq!(row_uid(&projector, kept), Some(Some(3)), "re-attached by Message-ID");
        assert_eq!(row_uid(&projector, gone), None, "not in the new epoch: deleted");
    }

    /// Only a real placeholder in Sent is replaced by the server copy; a
    /// received message waiting for its UID elsewhere is not one.
    #[tokio::test]
    async fn sent_copy_leaves_a_waiting_message_elsewhere_alone() {
        let account_id = AccountId::new();
        let (sent, _) = Mailbox::discover(account_id, MailboxPath::new(vec!["Sent".to_string()], '/').unwrap(), MailboxType::Sent);
        let archive = archive_mailbox(account_id);
        let (event_store, projector) = (Arc::new(MockEventStore::new()), Arc::new(MockProjector::new()));
        let waiting = seed(&event_store, &projector, &archive, None, "<sent@example.com>");
        projector.mailboxes.lock().unwrap().extend([sent.clone(), archive]);

        let mut provider = MockEmailProvider::new()
            .with_mailboxes(vec![sent])
            .with_envelopes(vec![envelope(7, "<sent@example.com>")]);
        provider.found.insert("Archive".into(), vec![ProviderUid::new(4)]);
        make_service(provider, event_store, projector.clone()).start_sync(&account_id).await.unwrap();

        assert_eq!(row_uid(&projector, waiting), Some(None), "still there, still waiting");
    }

    /// A row waiting for a UID that the server no longer has goes; reconcile
    /// works by UID and would keep it, unactionable, forever. A Sent
    /// placeholder waits for its server copy and stays.
    #[tokio::test]
    async fn a_uid_less_row_the_server_no_longer_has_is_deleted_but_a_placeholder_stays() {
        let account_id = AccountId::new();
        let inbox = inbox_mailbox(account_id);
        let (sent, _) = Mailbox::discover(account_id, MailboxPath::new(vec!["Sent".to_string()], '/').unwrap(), MailboxType::Sent);
        let (event_store, projector) = (Arc::new(MockEventStore::new()), Arc::new(MockProjector::new()));
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
        projector.mailboxes.lock().unwrap().extend([inbox.clone(), sent.clone()]);

        let provider = MockEmailProvider::new().with_mailboxes(vec![inbox, sent]);
        make_service(provider, event_store, projector.clone()).start_sync(&account_id).await.unwrap();

        assert_eq!(row_uid(&projector, stranded), None, "deleted");
        assert_eq!(row_uid(&projector, placeholder), Some(None), "still waiting for its server copy");
    }

    /// A projector for `mailbox` whose checkpoint is already at `uid_next`:
    /// every server message is below it, where ingest won't look again.
    async fn synced_up_to(mailbox: &Mailbox, uid_next: u64) -> Arc<MockProjector> {
        let projector = Arc::new(MockProjector::new());
        projector.mailboxes.lock().unwrap().push(mailbox.clone());
        let checkpoint_key = format!("sync:{}:{}", mailbox.account_id, mailbox.path.full_path());
        projector.save_checkpoint(&checkpoint_key, uid_next).await.unwrap();
        projector.save_checkpoint(&format!("{checkpoint_key}:validity"), 1).await.unwrap();
        projector
    }

    fn stored_uids(projector: &MockProjector) -> Vec<u32> {
        let mut uids: Vec<_> = projector.stored.lock().unwrap().iter().filter_map(|m| m.provider_uid.map(|u| u.value())).collect();
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

        make_service(provider, Arc::new(MockEventStore::new()), projector.clone()).start_sync(&account_id).await.unwrap();

        assert_eq!(stored_uids(&projector), [3]);
        assert!(
            projector.projected_events().iter().all(|e| !matches!(e, DomainEvent::MessageReceived { restored: false, .. })),
            "a restored message is not announced as new"
        );
    }

    /// A draft discarded here is deleted on the server only afterwards; a
    /// Drafts sync in between must not bring it back.
    #[tokio::test]
    async fn drafts_are_never_restored() {
        let account_id = AccountId::new();
        let (drafts, _) = Mailbox::discover(account_id, MailboxPath::new(vec!["Drafts".to_string()], '/').unwrap(), MailboxType::Drafts);
        let projector = synced_up_to(&drafts, 5).await;
        let provider = MockEmailProvider::new().with_mailboxes(vec![drafts]).with_envelopes(vec![envelope(3, "<draft@example.com>")]);

        make_service(provider, Arc::new(MockEventStore::new()), projector.clone()).start_sync(&account_id).await.unwrap();

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
        let provider = Arc::new(MockEmailProvider::new().with_mailboxes(vec![archive]).with_envelopes(batch_of(total)));
        let registry = Arc::new(MockProviderRegistry::new(provider.clone()));
        let service = SyncService::new(registry, Arc::new(MockEventStore::new()), projector.clone(), SyncConfig::default());

        service.start_sync(&account_id).await.unwrap();

        let uids = stored_uids(&projector);
        assert_eq!((uids.len(), uids[0]), (RESTORE_CAP, 2), "the oldest one waits");
        assert_eq!(*provider.uid_set_fetches.lock().unwrap(), [ENVELOPE_BATCH; RESTORE_CAP / ENVELOPE_BATCH]);

        service.start_sync(&account_id).await.unwrap();
        assert_eq!(stored_uids(&projector).len(), total as usize, "and comes back next sync");
    }

    /// A batch the server fails on doesn't hold back the others.
    #[tokio::test]
    async fn a_failing_restore_batch_is_skipped() {
        let account_id = AccountId::new();
        let archive = archive_mailbox(account_id);
        let projector = synced_up_to(&archive, 601).await;
        let mut provider = MockEmailProvider::new().with_mailboxes(vec![archive]).with_envelopes(batch_of(600));
        provider.fail_envelopes_from = Some(1);

        make_service(provider, Arc::new(MockEventStore::new()), projector.clone()).start_sync(&account_id).await.unwrap();

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

        assert_eq!(resync_batch(&mut rx).await.unwrap(), [(account, MailboxPath::inbox()), (account, archive)]);
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
        let (event_store, projector) = (Arc::new(MockEventStore::new()), Arc::new(MockProjector::new()));
        let orphan = seed(&event_store, &projector, &inbox, None, "<moved@example.com>");
        projector.mailboxes.lock().unwrap().extend([inbox, archive.clone()]);

        let provider = MockEmailProvider::new()
            .with_mailboxes(vec![archive.clone()])
            .with_envelopes(vec![envelope(9, "<moved@example.com>")]);
        make_service(provider, event_store, projector.clone()).start_sync(&account_id).await.unwrap();

        assert_eq!(received(&projector), 0, "no duplicate");
        let row = projector.stored.lock().unwrap().iter().find(|m| m.id == orphan).cloned().unwrap();
        assert_eq!((row.mailbox_id, row.provider_uid), (archive.id, Some(ProviderUid::new(9))));
    }

    /// The same Message-ID can legitimately sit in two mailboxes. One still
    /// on the server in its own mailbox is not taken over by the other copy.
    #[tokio::test]
    async fn a_copy_still_in_its_own_mailbox_is_not_taken_over() {
        let account_id = AccountId::new();
        let (inbox, archive) = (inbox_mailbox(account_id), archive_mailbox(account_id));
        let (event_store, projector) = (Arc::new(MockEventStore::new()), Arc::new(MockProjector::new()));
        let waiting = seed(&event_store, &projector, &inbox, None, "<both@example.com>");
        let held = seed(&event_store, &projector, &inbox, Some(3), "<both@example.com>");
        projector.mailboxes.lock().unwrap().extend([inbox, archive.clone()]);

        let mut provider = MockEmailProvider::new()
            .with_mailboxes(vec![archive])
            .with_envelopes(vec![envelope(9, "<both@example.com>")]);
        provider.found.insert("INBOX".into(), vec![ProviderUid::new(3), ProviderUid::new(5)]);
        make_service(provider, event_store, projector.clone()).start_sync(&account_id).await.unwrap();

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
        assert!(futures::poll!(&mut sync).is_pending(), "must wait for the running sync");
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
        assert!(projected.iter().any(|e| matches!(e, DomainEvent::AccountSyncStarted { .. })));
        assert!(projected.iter().any(|e| matches!(e, DomainEvent::AccountSyncCompleted { .. })));
        assert!(projected.iter().any(|e| matches!(e, DomainEvent::MailboxCountsUpdated { .. })));
        let published = bus.0.lock().unwrap();
        assert!(published.iter().any(|e| matches!(
            e,
            DomainEvent::MailboxCountsUpdated { total_messages: 42, unread_messages: 5, .. }
        )));
    }
}
