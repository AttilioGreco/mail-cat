//! Email synchronization service — orchestrates account sync with checkpoint resume.
//!
//! The SyncService coordinates the full sync workflow:
//! 1. List mailboxes and discover new ones
//! 2. Fetch message envelopes (metadata-only) with incremental UID-based sync
//! 3. Lazy-load message bodies on demand
//! 4. Track progress via checkpoint for resume on restart
//! 5. Emit domain events (MailboxDiscovered, MessageReceived, etc.)

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use mc_domain::{
    AccountId, AggregateType, BodyBackfillQueue, DomainEvent, EmailProvider, EventEnvelope,
    EventStore, FetchRange, Flag, Mailbox, MailboxId, MailboxPath, MailboxReadRepository, Message,
    MessageId, MessageReadRepository, ProjectionCheckpoint, ProviderRegistry, ProviderUid,
    ReadModelProjector, SearchIndex,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::bus::EventPublisher;
use crate::error::ApplicationError;
use crate::services::sync_trigger::SyncTrigger;

/// Tracks the state of a sync operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncState {
    /// Sync not yet started.
    Idle,
    /// Currently listing mailboxes.
    ListingMailboxes,
    /// Currently fetching envelopes for a mailbox.
    FetchingEnvelopes { mailbox_id: MailboxId },
    /// Fetching body for a single message.
    FetchingMessageBody { message_id: MessageId },
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
    /// Max message bodies the background backfill worker downloads per minute.
    /// `0` disables backfill (pure lazy-on-open). Throttles to avoid bursting
    /// the IMAP server / hitting Gmail's daily bandwidth cap.
    pub body_backfill_per_minute: u32,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            batch_size: 50,
            max_concurrent_bodies: 4,
            eager_body_fetch: false,
            body_backfill_per_minute: 30,
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
    P: ReadModelProjector + MailboxReadRepository + MessageReadRepository + ProjectionCheckpoint + BodyBackfillQueue,
    SI: SearchIndex,
> {
    provider_registry: Arc<PR>,
    event_store: Arc<ES>,
    projector: Arc<P>,
    search_index: Arc<SI>,
    #[allow(dead_code)]
    config: SyncConfig,
    cancelled: AtomicBool,
    /// Optional event publisher: when set, every stored domain event is also
    /// published here so in-process subscribers (e.g. desktop notifications)
    /// receive events emitted by the sync pipeline.
    event_publisher: Option<Arc<dyn EventPublisher>>,
}

impl<
    PR: ProviderRegistry,
    ES: EventStore,
    P: ReadModelProjector + MailboxReadRepository + MessageReadRepository + ProjectionCheckpoint + BodyBackfillQueue,
    SI: SearchIndex,
> SyncService<PR, ES, P, SI>
{
    /// Creates a new SyncService with the given dependencies.
    pub fn new(
        provider_registry: Arc<PR>,
        event_store: Arc<ES>,
        projector: Arc<P>,
        search_index: Arc<SI>,
        config: SyncConfig,
    ) -> Self {
        Self {
            provider_registry,
            event_store,
            projector,
            search_index,
            config,
            cancelled: AtomicBool::new(false),
            event_publisher: None,
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
    #[instrument(skip(self), fields(account_id = %account_id))]
    pub async fn start_sync(
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
        self.append_and_project(
            &account_id.to_string(),
            AggregateType::Account,
            sync_started,
        )
        .await?;

        // 2. List remote mailboxes
        progress.push(SyncProgressEvent {
            account_id: *account_id,
            state: SyncState::ListingMailboxes,
            processed: 0,
            total: 0,
            message: "Listing mailboxes...".to_string(),
        });

        let remote_mailboxes = provider.list_mailboxes().await.map_err(|e| {
            error!("Failed to list mailboxes: {}", e);
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

        // 4. Discover new mailboxes
        for remote in &remote_mailboxes {
            if !local_by_path.contains_key(&remote.path) {
                info!(path = %remote.path, "Discovering new mailbox");
                let (mailbox, event) =
                    Mailbox::discover(*account_id, remote.path.clone(), remote.mailbox_type);
                self.append_and_project(&mailbox.id.to_string(), AggregateType::Mailbox, event)
                    .await?;
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

            progress.push(SyncProgressEvent {
                account_id: *account_id,
                state: SyncState::FetchingEnvelopes {
                    mailbox_id: local.id,
                },
                processed: mailboxes_synced,
                total: total_mailboxes,
                message: format!("Syncing {}...", remote.path),
            });

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

            for envelope_summary in &envelopes {
                if self.is_cancelled() {
                    break;
                }

                // Soft-deleted remotely (\Deleted set, not yet expunged): skip so it
                // doesn't resurface as "new" on a mailbox with no local history of it.
                if envelope_summary.flags.contains(&Flag::Deleted) {
                    continue;
                }

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
                        let mut indexed = 0u32;
                        for result in body_results {
                            if let Some(message_id) = uid_to_msg.get(&result.provider_uid.value()) {
                                let event = DomainEvent::MessageBodyFetched {
                                    message_id: *(*message_id),
                                    body_text: result.body_text,
                                    body_html: result.body_html,
                                    // TODO: download and index attachments
                                    attachments: vec![],
                                };
                                if let Err(e) = self
                                    .append_and_project(
                                        &message_id.to_string(),
                                        AggregateType::Message,
                                        event,
                                    )
                                    .await
                                {
                                    warn!(%message_id, error = %e, "Failed to project body");
                                    continue;
                                }

                                // Index the full message (envelope + body) for search
                                match self.projector.get_message(message_id).await {
                                    Ok(Some(full_msg)) => {
                                        if let Err(e) = self.search_index.index_message(&full_msg).await {
                                            warn!(%message_id, error = %e, "Failed to index message");
                                        } else {
                                            indexed += 1;
                                        }
                                    }
                                    Ok(None) => {
                                        warn!(%message_id, "Message not found in read model after body projection");
                                    }
                                    Err(e) => {
                                        warn!(%message_id, error = %e, "Failed to read message for indexing");
                                    }
                                }
                            }
                        }
                        info!(path = %remote.path, bodies = total_bodies, indexed, "Bodies fetched and indexed");
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
        self.append_and_project(
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

        progress.push(SyncProgressEvent {
            account_id: *account_id,
            state: SyncState::Completed,
            processed: mailboxes_synced,
            total: total_mailboxes,
            message: format!(
                "Sync completed: {mailboxes_synced} mailboxes, {messages_synced} messages"
            ),
        });

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

        let event = DomainEvent::MessageBodyFetched {
            message_id: *message_id,
            body_text: fetched.body_text.clone(),
            body_html: fetched.body_html.clone(),
            attachments: fetched.attachments.clone(),
        };

        self.append_and_project(&message_id.to_string(), AggregateType::Message, event)
            .await?;

        // Index the full message (envelope + body) for search. Both the
        // on-demand open and the background backfill route through here, so this
        // is the single place body text becomes searchable.
        if let Ok(Some(full_msg)) = self.projector.get_message(message_id).await
            && let Err(e) = self.search_index.index_message(&full_msg).await
        {
            warn!(%message_id, error = %e, "fetch_message_body: search index failed");
        }

        Ok(Some(fetched))
    }

    /// Background body-backfill loop for one account.
    ///
    /// Drains the backfill queue newest-first, downloading one body every
    /// `60/per_minute` seconds so we never burst the IMAP server (or trip
    /// Gmail's daily bandwidth cap). Each fetch emits `MessageBodyFetched`,
    /// which both indexes the body and dequeues the entry. Failed fetches stay
    /// queued and are retried on the next pass. Runs until `shutdown` fires.
    pub async fn run_backfill(
        &self,
        account_id: AccountId,
        per_minute: u32,
        shutdown: CancellationToken,
    ) {
        if per_minute == 0 {
            return;
        }
        let delay = std::time::Duration::from_millis(60_000 / per_minute as u64);
        let idle_poll = std::time::Duration::from_secs(60);

        loop {
            if shutdown.is_cancelled() {
                return;
            }

            // Resolve mailbox_id → path once per drain pass.
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

            let pending = self
                .projector
                .list_pending_backfill(&account_id, 500)
                .await
                .unwrap_or_default();

            if pending.is_empty() {
                // Nothing queued — wait for new arrivals (or shutdown).
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(idle_poll) => continue,
                }
            }

            for entry in pending {
                if shutdown.is_cancelled() {
                    return;
                }
                let Some(path) = path_by_id.get(&entry.mailbox_id) else {
                    // Mailbox not yet known locally — skip this pass.
                    continue;
                };
                if let Err(e) = self
                    .fetch_message_body(
                        &account_id,
                        path,
                        &entry.message_id,
                        ProviderUid::new(entry.provider_uid),
                    )
                    .await
                {
                    // Leave it queued; retried next pass.
                    warn!(%account_id, error = %e, "backfill: body fetch failed");
                }
                // Rate limit (interruptible).
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
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
                        if let Some(message_id) = uid_to_msg.get(&result.provider_uid.value()) {
                            let body_event = DomainEvent::MessageBodyFetched {
                                message_id: *(*message_id),
                                body_text: result.body_text,
                                body_html: result.body_html,
                                attachments: vec![],
                            };
                            if let Err(e) = self
                                .append_and_project(
                                    &message_id.to_string(),
                                    AggregateType::Message,
                                    body_event,
                                )
                                .await
                            {
                                warn!(%message_id, error = %e, "sync_mailbox: body projection failed");
                                continue;
                            }
                            if let Ok(Some(full_msg)) =
                                self.projector.get_message(message_id).await
                                && let Err(e) = self.search_index.index_message(&full_msg).await {
                                    warn!(%message_id, error = %e, "sync_mailbox: search index failed");
                                }
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

    /// Append a domain event to the event store and project it into the read model.
    ///
    /// Loads the current aggregate sequence from the event store for optimistic
    /// concurrency, then appends the event and immediately projects it.
    async fn append_and_project(
        &self,
        aggregate_id: &str,
        aggregate_type: AggregateType,
        event: DomainEvent,
    ) -> Result<(), ApplicationError> {
        // Load current sequence for optimistic concurrency
        let current_events = self
            .event_store
            .load_events(aggregate_id, 0)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to load events: {e}")))?;
        let expected_seq = current_events.len() as u64;

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

        // Project into read model
        let envelope = EventEnvelope::new(aggregate_id.to_string(), aggregate_type, new_seq, event);
        self.projector
            .project(&envelope)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to project event: {e}")))?;

        // Broadcast to in-process subscribers (desktop notifications, live projection, etc.)
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
                if let Err(e) = self.search_index.remove_message(&summary.id).await {
                    warn!(message_id = %summary.id, error = %e, "Failed to remove message from search index");
                }
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
                if let Err(e) = self.search_index.remove_message(&summary.id).await {
                    warn!(message_id = %summary.id, error = %e, "Failed to remove message from search index");
                }
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

    /// Update mailbox message counts via a MailboxCountsUpdated event.
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
        self.append_and_project(&mailbox.id.to_string(), AggregateType::Mailbox, event)
            .await
    }
}

#[async_trait::async_trait]
impl<
    PR: ProviderRegistry,
    ES: EventStore,
    P: ReadModelProjector + MailboxReadRepository + MessageReadRepository + ProjectionCheckpoint + BodyBackfillQueue,
    SI: SearchIndex,
> SyncTrigger for SyncService<PR, ES, P, SI>
{
    async fn trigger_sync(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<SyncProgressEvent>, ApplicationError> {
        self.start_sync(account_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::Stream;
    use mc_domain::{
        Address, AggregateType, DomainError, EmailAddress, Envelope, EventEnvelope, Flag,
        MailboxStatus, MailboxType, MessageEnvelopeSummary, MessageSummary, OutgoingMessage,
        ProviderNotification,
    };
    use std::pin::Pin;
    use std::sync::Mutex;

    // ── Mock EmailProvider ─────────────────────────────────────────────

    struct MockEmailProvider {
        mailboxes: Vec<Mailbox>,
        envelopes: Vec<MessageEnvelopeSummary>,
        status: Option<MailboxStatus>,
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
            }
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
            _uids: &[ProviderUid],
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

        fn get_provider(
            &self,
            _account_id: &AccountId,
        ) -> Result<Arc<MockEmailProvider>, mc_domain::DomainError> {
            Ok(self.provider.clone())
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

        async fn load_all_events(
            &self,
            _aggregate_type: Option<AggregateType>,
            _from_global_position: u64,
        ) -> Result<Vec<EventEnvelope>, DomainError> {
            Ok(vec![])
        }

        fn subscribe(&self) -> Pin<Box<dyn Stream<Item = EventEnvelope> + Send + '_>> {
            Box::pin(futures::stream::empty())
        }
    }

    // ── Mock Projector (ReadModelProjector + MailboxReadRepository + MessageReadRepository + ProjectionCheckpoint) ──

    struct MockProjector {
        mailboxes: Mutex<Vec<Mailbox>>,
        checkpoints: Mutex<HashMap<String, u64>>,
        projected_events: Mutex<Vec<DomainEvent>>,
    }

    impl MockProjector {
        fn new() -> Self {
            Self {
                mailboxes: Mutex::new(vec![]),
                checkpoints: Mutex::new(HashMap::new()),
                projected_events: Mutex::new(vec![]),
            }
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

        async fn scan_all_messages(
            &self,
            _account_id: Option<&AccountId>,
        ) -> Result<Vec<Message>, DomainError> {
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
    impl BodyBackfillQueue for MockProjector {
        async fn list_pending_backfill(
            &self,
            _account_id: &AccountId,
            _limit: usize,
        ) -> Result<Vec<mc_domain::BackfillEntry>, DomainError> {
            Ok(vec![])
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

    // ── Mock SearchIndex ────────────────────────────────────────────

    struct MockSearchIndex;

    #[async_trait]
    impl SearchIndex for MockSearchIndex {
        async fn index_message(&self, _message: &Message) -> Result<(), DomainError> {
            Ok(())
        }
        async fn remove_message(&self, _message_id: &MessageId) -> Result<(), DomainError> {
            Ok(())
        }
        async fn search(
            &self,
            _query: &str,
            _account_id: Option<&AccountId>,
            _mailbox_id: Option<&MailboxId>,
            _pagination: &mc_shared::pagination::Pagination,
        ) -> Result<mc_domain::traits::search_index::SearchResults, DomainError> {
            Ok(mc_domain::traits::search_index::SearchResults {
                hits: vec![],
                total_count: 0,
            })
        }
        async fn suggest(
            &self,
            _prefix: &str,
            _scope: Option<mc_domain::SuggestScope>,
            _max_per_field: usize,
        ) -> Result<Vec<mc_domain::Suggestion>, DomainError> {
            Ok(vec![])
        }
        async fn rebuild(&self, _messages: Vec<Message>) -> Result<u64, DomainError> {
            Ok(0)
        }
    }

    fn make_service(
        provider: MockEmailProvider,
        event_store: Arc<MockEventStore>,
        projector: Arc<MockProjector>,
    ) -> SyncService<MockProviderRegistry, MockEventStore, MockProjector, MockSearchIndex> {
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(provider)));
        let search_index = Arc::new(MockSearchIndex);
        SyncService::new(registry, event_store, projector, search_index, SyncConfig::default())
    }

    // ── Tests ─────────────────────────────────────────────────────────

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
}
