//! Email synchronization service — orchestrates account sync.
//!
//! The SyncService coordinates the full sync workflow:
//! 1. List mailboxes and discover new ones
//! 2. Per mailbox, ask the provider what changed since the stored cursor and
//!    apply it (`mailbox`), emitting domain events (MessageReceived, etc.)
//! 3. Lazy-load message bodies on demand, and in the background (`backfill`)

mod backfill;
mod events;
mod ingest;
mod mailbox;
mod pending;
mod subscription;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, broadcast};

use mc_domain::{
    AccountId, AggregateType, BodyBackfillQueue, DomainError, DomainEvent, EmailProvider,
    EventStore, Mailbox, MailboxId, MailboxPath, MailboxReadRepository, MessageBodyStore,
    MessageReadRepository, ProviderRegistry, ReadModelProjector, SyncCursorStore,
};
use tracing::{error, info, instrument, warn};

use crate::bus::EventPublisher;
use crate::error::ApplicationError;
use crate::services::sync_trigger::SyncTrigger;

/// Everything sync needs from the read side and storage; implemented by the
/// SQLite repository.
pub trait SyncStore:
    ReadModelProjector
    + MailboxReadRepository
    + MessageReadRepository
    + SyncCursorStore
    + BodyBackfillQueue
    + MessageBodyStore
{
}

impl<T> SyncStore for T where
    T: ReadModelProjector
        + MailboxReadRepository
        + MessageReadRepository
        + SyncCursorStore
        + BodyBackfillQueue
        + MessageBodyStore
{
}

/// A failure that would fail every other mailbox of the pass the same way: the account cannot log in or connect, or is over a usage limit.
fn stops_the_pass(e: &DomainError) -> bool {
    matches!(
        e,
        DomainError::AuthenticationFailed(_)
            | DomainError::ConnectionFailed(_)
            | DomainError::ConnectionTimeout(_)
            | DomainError::Throttled(_)
    )
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
/// Manages the full sync lifecycle: mailbox discovery, cursor-based incremental
/// sync of each mailbox, and cancellation.
pub struct SyncService<PR: ProviderRegistry, ES: EventStore, P: SyncStore> {
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
    /// Wakes an idle backfill worker when new messages (pending bodies) land.
    /// `notify_one` stores a permit, so a wakeup between the worker's
    /// empty-queue check and its wait is not lost.
    backfill_wake: Notify,
    /// Mailboxes whose subscription the user just changed: the slow part
    /// (dropping thousands of messages, a first sync) runs in
    /// [`Self::serve_subscription_jobs`], not in the RPC.
    subscription_jobs: tokio::sync::mpsc::UnboundedSender<MailboxId>,
    subscription_jobs_rx: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<MailboxId>>>,
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

/// Envelopes fetched and stored per batch.
const ENVELOPE_BATCH: usize = 500;

/// Most messages one sync restores per mailbox (see `mailbox::restore`). A
/// mailbox that lost many rows comes back over a few syncs instead of one
/// sync re-downloading it whole while the account waits.
const RESTORE_CAP: usize = 2000;

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

impl<PR: ProviderRegistry, ES: EventStore, P: SyncStore> SyncService<PR, ES, P> {
    /// Creates a new SyncService with the given dependencies.
    pub fn new(
        provider_registry: Arc<PR>,
        event_store: Arc<ES>,
        projector: Arc<P>,
        config: SyncConfig,
    ) -> Self {
        let (subscription_jobs, jobs_rx) = tokio::sync::mpsc::unbounded_channel();
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
            backfill_wake: Notify::new(),
            subscription_jobs,
            subscription_jobs_rx: Mutex::new(Some(jobs_rx)),
        }
    }

    /// Holds the account's sync slot. The IDLE-triggered `sync_mailbox` and a
    /// full `start_sync` running side by side would ingest the same envelopes
    /// twice and race each other's cursors.
    pub async fn exclusive(&self, account_id: &AccountId) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = Arc::clone(
            self.account_locks
                .lock()
                .unwrap()
                .entry(*account_id)
                .or_default(),
        );
        lock.lock_owned().await
    }

    /// Drops the per-account bookkeeping of a removed account.
    pub fn forget(&self, account_id: &AccountId) {
        self.account_locks.lock().unwrap().remove(account_id);
        self.last_progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(account_id);
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
    /// 3. For each mailbox: applies what changed since its cursor, emits MessageReceived events
    /// 4. Updates mailbox counts and saves the cursor
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
        self.project_transient(
            &account_id.to_string(),
            AggregateType::Account,
            sync_started,
        )
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
        if let Some(model) = provider.preferred_mailbox_model() {
            self.default_mailbox_model(account_id, model).await?;
        }

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

        // 4. Discover new mailboxes; correct type and labels of existing ones.
        for remote in &remote_mailboxes {
            let mut mailbox = match local_by_path.get(&remote.path) {
                None => {
                    info!(path = %remote.path, "Discovering new mailbox");
                    let (mailbox, event) = Mailbox::discover_as(
                        *account_id,
                        remote.path.clone(),
                        remote.display_path.clone(),
                        remote.mailbox_type,
                    );
                    self.append_and_project(&mailbox.id.to_string(), AggregateType::Mailbox, event)
                        .await?;
                    mailbox
                }
                Some(local) => (*local).clone(),
            };
            self.follow_server_subscription(&provider, &mut mailbox, remote)
                .await?;
            if mailbox.mailbox_type != remote.mailbox_type {
                // The type was recorded once at discovery. Detection has since
                // improved (SPECIAL-USE, localized names), so mailboxes classified
                // by an older build stay wrong forever unless we correct them here
                // — which is what silently broke Archive and Trash targeting.
                info!(
                    path = %remote.path,
                    from = ?mailbox.mailbox_type,
                    to = ?remote.mailbox_type,
                    "Reclassifying mailbox"
                );
                let event = mailbox.reclassify(remote.mailbox_type);
                self.append_and_project(&mailbox.id.to_string(), AggregateType::Mailbox, event)
                    .await?;
            }
            // A server-side rename or re-parent keeps the id (JMAP), and a
            // name holding the delimiter is not what discovery's leaf says.
            if mailbox.display_path != remote.display_path
                || mailbox.display_name != remote.display_name
            {
                info!(path = %remote.path, to = %remote.display_path, "Mailbox renamed on the server");
                let event = mailbox.rename(
                    remote.path.clone(),
                    remote.display_path.clone(),
                    remote.display_name.clone(),
                );
                self.append_and_project(&mailbox.id.to_string(), AggregateType::Mailbox, event)
                    .await?;
            }
        }

        self.drop_gone_labels(&provider, &local_mailboxes, &remote_mailboxes)
            .await?;

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
        // Views over another mailbox come with its sync.
        // Nor do the ones the user left out.
        let synced: Vec<&Mailbox> = remote_mailboxes
            .iter()
            .filter(|m| provider.backed_by(&m.path).is_none())
            .filter(|m| local_by_path.get(&m.path).is_some_and(|l| l.subscribed))
            .collect();
        let total_mailboxes = synced.len() as u32;
        for remote in synced {
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
                    message: format!("Syncing {}...", remote.display_path),
                },
            );

            match self.sync_one(&provider, local, Some(&mut progress)).await? {
                Some((new, expunged)) => {
                    messages_synced = (messages_synced + new).saturating_sub(expunged)
                }
                None if self.is_cancelled() => {
                    info!("Sync cancelled, stopping");
                    break;
                }
                None => continue,
            }
            mailboxes_synced += 1;
        }

        if let Some(supported) = provider.supports_tags() {
            let detected = DomainEvent::AccountTagsDetected {
                account_id: *account_id,
                supported,
            };
            self.project_transient(&account_id.to_string(), AggregateType::Account, detected)
                .await?;
        }

        // 7. Emit AccountSyncCompleted
        let sync_completed = DomainEvent::AccountSyncCompleted {
            account_id: *account_id,
            mailboxes_synced,
            messages_synced,
        };
        self.project_transient(
            &account_id.to_string(),
            AggregateType::Account,
            sync_completed,
        )
        .await?;

        info!(
            mailboxes = mailboxes_synced,
            messages = messages_synced,
            "Sync completed"
        );

        self.backfill_wake.notify_one();
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

    /// Resumes a sync operation: each mailbox continues from its stored cursor.
    pub async fn resume_sync(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<SyncProgressEvent>, ApplicationError> {
        info!(account_id = %account_id, "Resuming sync");
        self.start_sync(account_id).await
    }

    /// Sync a single mailbox from its cursor, without an account-wide scan.
    ///
    /// Called when the provider pushes a change and on resync requests.
    ///
    /// Returns the number of new messages stored.
    #[instrument(skip(self), fields(%account_id, %mailbox_path))]
    pub async fn sync_mailbox(
        &self,
        account_id: &AccountId,
        mailbox_path: &MailboxPath,
    ) -> Result<u32, ApplicationError> {
        let _exclusive = self.exclusive(account_id).await;
        let provider = self.provider_registry.get_provider(account_id)?;
        let backing = provider.backed_by(mailbox_path);
        let mailbox_path = backing.as_ref().unwrap_or(mailbox_path);

        // Find the local mailbox record
        let local_mailboxes = self
            .projector
            .list_mailboxes(account_id)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("list_mailboxes failed: {e}")))?;
        // Compare by full_path() string so that delimiter differences between
        // the watcher's MailboxPath and the server-discovered stored path don't
        // cause a false mismatch (e.g. inbox() delimiter '/' vs server '.').
        let Some(local) = local_mailboxes
            .iter()
            .find(|m| m.path.full_path() == mailbox_path.full_path())
        else {
            warn!(%mailbox_path, "sync_mailbox: local mailbox not found, skipping");
            return Ok(0);
        };

        let (new_count, _) = self
            .sync_one(&provider, local, None)
            .await?
            .unwrap_or_default();
        info!(%mailbox_path, new_count, "sync_mailbox: done");
        Ok(new_count)
    }

    /// Serve the resync requests message commands send when the server did
    /// more than they could record, until the senders are gone. A burst
    /// (a bulk move's chunks) syncs each mailbox once.
    pub async fn serve_resyncs(
        &self,
        mut requests: tokio::sync::mpsc::UnboundedReceiver<(AccountId, MailboxPath)>,
    ) {
        while let Some(batch) = resync_batch(&mut requests).await {
            for (account_id, path) in batch {
                if let Err(e) = self.sync_mailbox(&account_id, &path).await {
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

    /// Check whether cancellation has been requested.
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl<PR: ProviderRegistry, ES: EventStore, P: SyncStore> SyncTrigger for SyncService<PR, ES, P> {
    async fn trigger_sync(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<SyncProgressEvent>, ApplicationError> {
        self.start_sync(account_id).await
    }

    async fn trigger_mailbox_sync(
        &self,
        account_id: &AccountId,
        mailbox_path: &str,
    ) -> Result<u32, ApplicationError> {
        // sync_mailbox matches on full_path(), so a single segment holding the
        // whole stored path matches whatever delimiter the server uses.
        let path = MailboxPath::new(vec![mailbox_path.to_owned()], '/')?;
        self.sync_mailbox(account_id, &path).await
    }

    async fn set_mailbox_subscribed(
        &self,
        mailbox_id: &MailboxId,
        subscribed: bool,
    ) -> Result<Option<String>, ApplicationError> {
        SyncService::set_mailbox_subscribed(self, mailbox_id, subscribed).await
    }

    fn subscribe_progress(&self) -> broadcast::Receiver<SyncProgressEvent> {
        SyncService::subscribe_progress(self)
    }
}

#[cfg(test)]
mod tests;
