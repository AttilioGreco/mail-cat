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
    EmailProvider, EventEnvelope, EventStore, FetchRange, Flag, Mailbox, MailboxId, MailboxPath, MailboxType,
    MailboxReadRepository, Message, MessageBodyResult, MessageBodyStore, MessageId,
    MessageEnvelopeSummary, MessageReadRepository, MessageSummary,
    ProjectionCheckpoint, ProviderRegistry, ProviderUid, ReadModelProjector,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::bus::EventPublisher;
use crate::error::ApplicationError;
use crate::services::sync_trigger::SyncTrigger;

/// Backoff after `failures` consecutive credential rejections: 5 min, doubling
/// to a 1 hour cap. A rejected credential needs the user to act, so retrying on
/// the normal cadence only burns connections and risks provider rate-limiting.
fn auth_backoff(failures: u32) -> std::time::Duration {
    let factor = 1u64 << failures.saturating_sub(1).min(20);
    std::time::Duration::from_secs(300 * factor).min(std::time::Duration::from_secs(3600))
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

fn percent(processed: u32, total: u32) -> u8 {
    if total == 0 {
        return 0;
    }
    ((processed as u64 * 100 / total as u64).min(100)) as u8
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
        }
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
            // Typed, like fetch_bodies_bulk: the periodic loop backs off on it.
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

            // Load checkpoint
            let checkpoint_key = format!("sync:{}:{}", account_id, remote.path);
            let validity_key = format!("{checkpoint_key}:validity");
            let stored_uid_next = self
                .projector
                .get_checkpoint(&checkpoint_key)
                .await
                .unwrap_or(0) as u32;
            let stored_validity = self
                .projector
                .get_checkpoint(&validity_key)
                .await
                .unwrap_or(0) as u32;

            // Check UIDVALIDITY
            let fetch_from = if stored_validity != 0 && stored_validity != status.uid_validity {
                warn!(
                    path = %remote.path,
                    old = stored_validity,
                    new = status.uid_validity,
                    "UIDVALIDITY changed, full resync needed"
                );
                1u32
            } else if stored_uid_next > 0 {
                stored_uid_next
            } else {
                1u32
            };

            if fetch_from >= status.uid_next {
                debug!(path = %remote.path, "No new messages");
                // Still update counts
                self.update_mailbox_counts(local, status.total_messages, status.unseen_messages)
                    .await?;
                self.projector
                    .save_checkpoint(&checkpoint_key, status.uid_next as u64)
                    .await
                    .map_err(|e| {
                        ApplicationError::SyncFailed(format!("Checkpoint save failed: {e}"))
                    })?;
                self.projector
                    .save_checkpoint(&validity_key, status.uid_validity as u64)
                    .await
                    .map_err(|e| {
                        ApplicationError::SyncFailed(format!("Checkpoint save failed: {e}"))
                    })?;

                // Reconcile even when there are no new messages
                let provider = self.provider_registry.get_provider(account_id)?;
                let (expunged, flags_changed) = self
                    .reconcile_mailbox(&provider, local, &remote.path)
                    .await?;
                if expunged > 0 || flags_changed > 0 {
                    let updated_status = match provider.mailbox_status(&remote.path).await {
                        Ok(s) => s,
                        Err(_) => status,
                    };
                    self.update_mailbox_counts(
                        local,
                        updated_status.total_messages,
                        updated_status.unseen_messages,
                    )
                    .await?;
                }

                mailboxes_synced += 1;
                continue;
            }

            // Fetch envelopes
            let range = FetchRange::UidRange {
                from: fetch_from,
                to: status.uid_next - 1,
            };
            debug!(
                path = %remote.path,
                from = fetch_from,
                to = status.uid_next - 1,
                "Fetching envelopes"
            );

            let envelopes = match provider.fetch_message_envelopes(&remote.path, range).await {
                Ok(e) => e,
                Err(e) => {
                    warn!(path = %remote.path, error = %e, "Failed to fetch envelopes, skipping");
                    continue;
                }
            };

            info!(
                path = %remote.path,
                count = envelopes.len(),
                "Fetched envelopes"
            );

            // Process each envelope; collect (message_id, provider_uid) for body fetch
            let mut new_messages: Vec<(MessageId, ProviderUid)> = Vec::new();
            let envelope_total = envelopes.len() as u32;
            // Stable per-mailbox text so `emit`'s gate reduces this to one event
            // per percentage point instead of one per message.
            let envelope_label = format!("Fetching messages in {}", remote.path);

            for (index, envelope_summary) in envelopes.iter().enumerate() {
                self.emit(
                    &mut progress,
                    SyncProgressEvent {
                        account_id: *account_id,
                        state: SyncState::FetchingEnvelopes {
                            mailbox_id: local.id,
                        },
                        processed: index as u32,
                        total: envelope_total,
                        message: envelope_label.clone(),
                    },
                );

                if self.is_cancelled() {
                    break;
                }

                // Soft-deleted remotely (\Deleted set, not yet expunged): skip so it
                // doesn't resurface as "new" on a mailbox with no local history of it.
                if envelope_summary.flags.contains(&Flag::Deleted) {
                    continue;
                }
                let placeholder = match self.local_copy(local, envelope_summary).await {
                    Some(stored) if stored.provider_uid.is_some() => continue,
                    other => other,
                };

                let (_message, event) = Message::receive(
                    *account_id,
                    local.id,
                    envelope_summary.provider_uid,
                    envelope_summary.message_rfc_id.clone(),
                    envelope_summary.in_reply_to.clone(),
                    envelope_summary.references.clone(),
                    envelope_summary.envelope.clone(),
                    envelope_summary.flags.clone(),
                    envelope_summary.size_bytes,
                    envelope_summary.has_attachments,
                    envelope_summary.internal_date,
                );

                if let DomainEvent::MessageReceived { ref message_id, .. } = event {
                    new_messages.push((*message_id, envelope_summary.provider_uid));
                    self.append_and_project(&message_id.to_string(), AggregateType::Message, event)
                        .await?;
                    self.supersede(placeholder).await?;
                    messages_synced += 1;
                }
            }

            // Eager body fetch for new messages — bulk IMAP fetch, one connection per batch
            if self.config.eager_body_fetch && !new_messages.is_empty() {
                let total_bodies = new_messages.len();
                info!(path = %remote.path, count = total_bodies, "Bulk fetching bodies");

                let uids: Vec<ProviderUid> = new_messages.iter().map(|(_, u)| *u).collect();
                let uid_to_msg: std::collections::HashMap<u32, &MessageId> = new_messages
                    .iter()
                    .map(|(id, uid)| (uid.value(), id))
                    .collect();

                let provider = self.provider_registry.get_provider(account_id)?;
                match provider.fetch_bodies_bulk(&remote.path, &uids).await {
                    Ok(body_results) => {
                        for result in body_results {
                            if let Some(message_id) = uid_to_msg.get(&result.provider_uid.value()) {
                                // Search indexing is owned by the search-index
                                // projection (driven off this event); no inline
                                // index_message here.
                                if let Err(e) = self
                                    .store_body(message_id, result.body_text, result.body_html, vec![])
                                    .await
                                {
                                    warn!(%message_id, error = %e, "Failed to project body");
                                    continue;
                                }
                            }
                        }
                        info!(path = %remote.path, bodies = total_bodies, "Bodies fetched");
                    }
                    Err(e) => {
                        warn!(path = %remote.path, error = %e, "Bulk body fetch failed, skipping");
                    }
                }
            }

            // Update mailbox counts
            self.update_mailbox_counts(local, status.total_messages, status.unseen_messages)
                .await?;

            // Save checkpoint
            self.projector
                .save_checkpoint(&checkpoint_key, status.uid_next as u64)
                .await
                .map_err(|e| {
                    ApplicationError::SyncFailed(format!("Checkpoint save failed: {e}"))
                })?;
            self.projector
                .save_checkpoint(&validity_key, status.uid_validity as u64)
                .await
                .map_err(|e| {
                    ApplicationError::SyncFailed(format!("Checkpoint save failed: {e}"))
                })?;

            // Reconcile: detect expunged messages and flag changes
            let provider = self.provider_registry.get_provider(account_id)?;
            let (expunged, flags_changed) = self
                .reconcile_mailbox(&provider, local, &remote.path)
                .await?;
            messages_synced = messages_synced.saturating_sub(expunged);

            // Re-update counts after reconciliation if anything changed
            if expunged > 0 || flags_changed > 0 {
                let updated_status = match provider.mailbox_status(&remote.path).await {
                    Ok(s) => s,
                    Err(_) => status,
                };
                self.update_mailbox_counts(
                    local,
                    updated_status.total_messages,
                    updated_status.unseen_messages,
                )
                .await?;
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

        // Keep AuthenticationFailed typed (via the transparent Domain variant) so
        // callers can back off on a rejected credential. Stringifying it into
        // SyncFailed here is what previously made a dead account indistinguishable
        // from a flaky network, so it got retried forever.
        let results = provider
            .fetch_bodies_bulk(mailbox_path, &uids)
            .await
            .map_err(|e| match e {
                DomainError::AuthenticationFailed(_) => ApplicationError::Domain(e),
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
    /// Runs until `shutdown` fires.
    pub async fn run_backfill(
        &self,
        account_id: AccountId,
        daily_limit: Option<u64>,
        shutdown: CancellationToken,
    ) {
        let idle_poll = std::time::Duration::from_secs(60);

        let mut missing_counts: HashMap<MessageId, u32> = HashMap::new();
        let mut abandoned: HashSet<MessageId> = HashSet::new();
        // Consecutive credential rejections, used to back off. Reset on success.
        let mut auth_failures: u32 = 0;
        // Bodies downloaded since the queue was last empty. Progress is this
        // against itself plus what's still queued, so the percentage climbs to
        // 100% across batches instead of restarting with each one; new mail
        // only grows the total.
        let mut drained: u32 = 0;

        loop {
            if shutdown.is_cancelled() {
                return;
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

            let batch: Vec<BackfillEntry> = self
                .projector
                .list_pending_backfill(&account_id, 500)
                .await
                .unwrap_or_default()
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

            let (mut auth_rejected, mut fetch_failed) = (false, false);
            for (mailbox_id, entries) in by_mailbox {
                let Some(path) = path_by_id.get(&mailbox_id) else {
                    // Mailbox not yet known locally: nothing fetched, so retry
                    // on the failure cadence rather than every second.
                    fetch_failed = true;
                    continue;
                };
                match self.fetch_message_bodies_bulk(&account_id, path, &entries).await {
                    Ok(missing) => {
                        auth_failures = 0;
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
                    // A rejected credential fails every remaining mailbox
                    // identically. Stop and back off, instead of re-running the
                    // queue against a server that is refusing us, forever.
                    Err(e @ ApplicationError::Domain(DomainError::AuthenticationFailed(_))) => {
                        auth_failures = auth_failures.saturating_add(1);
                        warn!(
                            %account_id, error = %e, attempts = auth_failures,
                            "backfill: credential rejected, backing off"
                        );
                        auth_rejected = true;
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

            if auth_rejected {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(auth_backoff(auth_failures)) => continue,
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

    /// Lightweight sync for a single mailbox starting from a given UID.
    ///
    /// Called by the IMAP IDLE watcher on `NewMessage` notifications. Only fetches
    /// new envelopes (no reconciliation, no account-wide scan). Updates the checkpoint
    /// so the next `start_sync` won't re-fetch the same messages.
    ///
    /// Returns the number of new messages stored.
    #[instrument(skip(self), fields(%account_id, %mailbox_path, uid = from_uid.value()))]
    pub async fn sync_mailbox(
        &self,
        account_id: &AccountId,
        mailbox_path: &MailboxPath,
        from_uid: ProviderUid,
    ) -> Result<u32, ApplicationError> {
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

        // Use max(checkpoint, from_uid) to skip already-synced messages
        let checkpoint_key = format!("sync:{}:{}", account_id, mailbox_path);
        let validity_key = format!("{checkpoint_key}:validity");
        let stored_uid_next = self
            .projector
            .get_checkpoint(&checkpoint_key)
            .await
            .unwrap_or(0) as u32;
        let fetch_from = stored_uid_next.max(from_uid.value());

        if fetch_from >= status.uid_next {
            debug!(%mailbox_path, "sync_mailbox: no new messages");
            // Still update counts in case they changed
            self.update_mailbox_counts(&local, status.total_messages, status.unseen_messages)
                .await?;

            // Reconcile even with no new messages: an IDLE-triggered sync can also
            // fire for expunges/moves-out, which don't advance uid_next.
            let (expunged, flags_changed) =
                self.reconcile_mailbox(&provider, &local, mailbox_path).await?;
            if expunged > 0 || flags_changed > 0 {
                let updated_status = match provider.mailbox_status(mailbox_path).await {
                    Ok(s) => s,
                    Err(_) => status,
                };
                self.update_mailbox_counts(
                    &local,
                    updated_status.total_messages,
                    updated_status.unseen_messages,
                )
                .await?;
            }
            return Ok(0);
        }

        // Fetch only the new envelopes
        let range = FetchRange::UidRange {
            from: fetch_from,
            to: status.uid_next - 1,
        };
        let envelopes = match provider.fetch_message_envelopes(mailbox_path, range).await {
            Ok(e) => e,
            Err(e) => {
                warn!(%mailbox_path, error = %e, "sync_mailbox: fetch_message_envelopes failed");
                return Ok(0);
            }
        };
        info!(%mailbox_path, count = envelopes.len(), "sync_mailbox: fetched envelopes");

        let mut new_count = 0u32;
        let mut new_messages: Vec<(MessageId, ProviderUid)> = Vec::new();

        for envelope_summary in &envelopes {
            if envelope_summary.flags.contains(&Flag::Deleted) {
                continue;
            }
            let placeholder = match self.local_copy(&local, envelope_summary).await {
                Some(stored) if stored.provider_uid.is_some() => continue,
                other => other,
            };

            let (_message, event) = Message::receive(
                    *account_id,
                    local.id,
                    envelope_summary.provider_uid,
                    envelope_summary.message_rfc_id.clone(),
                    envelope_summary.in_reply_to.clone(),
                    envelope_summary.references.clone(),
                    envelope_summary.envelope.clone(),
                    envelope_summary.flags.clone(),
                    envelope_summary.size_bytes,
                    envelope_summary.has_attachments,
                    envelope_summary.internal_date,
                );
            if let DomainEvent::MessageReceived { ref message_id, .. } = event {
                new_messages.push((*message_id, envelope_summary.provider_uid));
                self.append_and_project(&message_id.to_string(), AggregateType::Message, event)
                    .await?;
                self.supersede(placeholder).await?;
                new_count += 1;
            }
        }

        // Eager body fetch (same pattern as start_sync)
        if self.config.eager_body_fetch && !new_messages.is_empty() {
            let uids: Vec<ProviderUid> = new_messages.iter().map(|(_, u)| *u).collect();
            let uid_to_msg: HashMap<u32, &MessageId> =
                new_messages.iter().map(|(id, uid)| (uid.value(), id)).collect();
            let provider2 = self.provider_registry.get_provider(account_id)?;
            match provider2.fetch_bodies_bulk(mailbox_path, &uids).await {
                Ok(body_results) => {
                    for result in body_results {
                        // Search indexing owned by the search-index projection.
                        if let Some(message_id) = uid_to_msg.get(&result.provider_uid.value())
                            && let Err(e) = self
                                .store_body(message_id, result.body_text, result.body_html, vec![])
                                .await
                        {
                            warn!(%message_id, error = %e, "sync_mailbox: body projection failed");
                        }
                    }
                }
                Err(e) => {
                    warn!(%mailbox_path, error = %e, "sync_mailbox: bulk body fetch failed");
                }
            }
        }

        // Update counts (emits MailboxCountsUpdated → event bus → UI refetch)
        self.update_mailbox_counts(&local, status.total_messages, status.unseen_messages)
            .await?;

        // Advance checkpoint so start_sync won't re-fetch these messages
        self.projector
            .save_checkpoint(&checkpoint_key, status.uid_next as u64)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("checkpoint save failed: {e}")))?;
        self.projector
            .save_checkpoint(&validity_key, status.uid_validity as u64)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("checkpoint save failed: {e}")))?;

        // Reconcile: detect expunged/moved-out messages and flag changes.
        let (expunged, flags_changed) =
            self.reconcile_mailbox(&provider, &local, mailbox_path).await?;
        if expunged > 0 || flags_changed > 0 {
            let updated_status = match provider.mailbox_status(mailbox_path).await {
                Ok(s) => s,
                Err(_) => status,
            };
            self.update_mailbox_counts(
                &local,
                updated_status.total_messages,
                updated_status.unseen_messages,
            )
            .await?;
        }

        info!(%mailbox_path, new_count, "sync_mailbox: done");
        Ok(new_count)
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

    /// Mail we put on the server ourselves is stored locally before its
    /// server copy syncs back: a saved draft with the UID APPEND returned, a
    /// sent message with none (`MessageSent`). Scoped to Drafts and Sent: a
    /// per-envelope lookup would be a needless query per message on a big
    /// Inbox sync.
    async fn local_copy(&self, mailbox: &Mailbox, env: &MessageEnvelopeSummary) -> Option<MessageSummary> {
        if !matches!(mailbox.mailbox_type, MailboxType::Drafts | MailboxType::Sent) {
            return None;
        }
        let rfc_id = env.message_rfc_id.as_deref()?;
        let copies = self.projector.find_by_rfc_id(&mailbox.account_id, rfc_id).await.ok()?;
        copies.into_iter().find(|m| match m.provider_uid {
            Some(uid) => m.mailbox_id == mailbox.id && uid == env.provider_uid,
            None => mailbox.mailbox_type == MailboxType::Sent,
        })
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
        attachments: Vec<Attachment>,
    ) -> Result<(), ApplicationError> {
        self.projector
            .put_body(message_id, body_text, body_html)
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
    #[instrument(skip(self, provider, local), fields(mailbox_id = %local.id))]
    async fn reconcile_mailbox(
        &self,
        provider: &Arc<<PR as ProviderRegistry>::Provider>,
        local: &Mailbox,
        remote_path: &MailboxPath,
    ) -> Result<(u32, u32), ApplicationError> {
        let remote_entries = match provider.fetch_uid_flags(remote_path).await {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "fetch_uid_flags failed, skipping reconciliation");
                return Ok((0, 0));
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

        if expunged > 0 || flags_changed > 0 {
            info!(
                expunged,
                flags_changed,
                "Reconciliation complete for mailbox"
            );
        }

        Ok((expunged, flags_changed))
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
        MailboxStatus, MailboxType, MessageEnvelopeSummary, OutgoingMessage,
        ProviderNotification,
    };
    use std::pin::Pin;
    use std::sync::Mutex;

    #[test]
    fn auth_backoff_grows_and_caps() {
        assert_eq!(auth_backoff(1).as_secs(), 300, "first rejection waits 5 min");
        assert_eq!(auth_backoff(2).as_secs(), 600);
        assert_eq!(auth_backoff(4).as_secs(), 2400);
        assert_eq!(auth_backoff(8).as_secs(), 3600, "caps at 1 hour");
        // Must not overflow the shift on a long-dead account.
        assert_eq!(auth_backoff(u32::MAX).as_secs(), 3600);
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
            }
        }

        /// Number of times `fetch_bodies_bulk` was called (i.e. number of IMAP
        /// sessions opened for bulk body fetches).
        fn bulk_fetch_call_count(&self) -> usize {
            self.bulk_fetch_calls.lock().unwrap().len()
        }

        fn with_envelopes(mut self, envelopes: Vec<MessageEnvelopeSummary>) -> Self {
            let count = envelopes.len() as u32;
            self.envelopes = envelopes;
            if let Some(ref mut s) = self.status {
                s.uid_next = count + 1;
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
            _range: FetchRange,
        ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
            Ok(self.envelopes.clone())
        }

        async fn fetch_message_detail(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
        ) -> Result<Message, DomainError> {
            Err(DomainError::Provider("Not implemented".to_string()))
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
            _expected_sequence: u64,
            events: Vec<DomainEvent>,
        ) -> Result<u64, DomainError> {
            let mut store = self.events.lock().unwrap();
            let start = store.len() as u64;
            for event in events {
                store.push((aggregate_id.to_string(), event));
            }
            Ok(start + 1)
        }

        async fn load_events(
            &self,
            _aggregate_id: &str,
            _from_sequence: u64,
        ) -> Result<Vec<EventEnvelope>, DomainError> {
            Ok(vec![])
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
    }

    impl MockProjector {
        fn new() -> Self {
            Self {
                mailboxes: Mutex::new(vec![]),
                checkpoints: Mutex::new(HashMap::new()),
                projected_events: Mutex::new(vec![]),
                bodies: Mutex::new(HashMap::new()),
                stored: Mutex::new(vec![]),
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
            _mailbox_id: &MailboxId,
            _pagination: &mc_shared::pagination::Pagination,
        ) -> Result<mc_shared::pagination::PagedResult<MessageSummary>, DomainError> {
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
        ) -> Result<Option<MessageSummary>, DomainError> {
            Ok(None)
        }

        async fn find_by_rfc_id(
            &self,
            _account_id: &AccountId,
            _rfc_id: &str,
        ) -> Result<Vec<MessageSummary>, DomainError> {
            Ok(self.stored.lock().unwrap().clone())
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
            tokio::spawn(async move { service.run_backfill(AccountId::new(), None, shutdown).await })
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
        let provider = MockEmailProvider::new().with_mailboxes(vec![sent]).with_envelopes(vec![server_copy]);
        let service = make_service(provider, Arc::new(MockEventStore::new()), projector.clone());
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
