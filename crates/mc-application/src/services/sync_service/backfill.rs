//! The background body backfill: downloads the text of messages whose
//! envelopes sync stored, within the account's daily download cap.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use mc_domain::{
    AccountId, AggregateType, BackfillEntry, DomainEvent, EmailProvider, LoginGate, MailboxId,
    MailboxPath, MessageBodyResult, MessageId, ProviderRegistry, RemoteId,
};

use super::{SyncProgressEvent, SyncService, SyncState, SyncStore, stops_the_pass};
use crate::error::ApplicationError;

/// Bodies the backfill downloads per round, between two looks at the meter.
pub(super) const BACKFILL_BATCH: usize = 50;

/// Share of the daily cap, in percent, the backfill fills at full speed. The
/// rest is headroom for opened messages and forwards, and for the last batch
/// overshooting.
const BACKFILL_CAP_PERCENT: u64 = 90;

/// How long a backfill at its share of the cap waits before looking again,
/// for old downloads to leave the 24 h window.
pub(super) const BACKFILL_CAP_WAIT: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// The wait before the next backfill batch, given `spent` bytes downloaded in
/// the last 24 h: none below the backfill's share of the cap. Text-only,
/// capped and shared between copies, a whole mailbox fits well inside a
/// day's cap, so crawling to stretch it would only delay it.
pub(super) fn backfill_pause(spent: u64, daily_limit: Option<u64>) -> Option<std::time::Duration> {
    daily_limit
        .is_some_and(|limit| spent >= limit / 100 * BACKFILL_CAP_PERCENT)
        .then_some(BACKFILL_CAP_WAIT)
}

impl<PR: ProviderRegistry, ES: mc_domain::EventStore, P: SyncStore> SyncService<PR, ES, P> {
    /// Fetches bodies for multiple messages in the same mailbox over a single
    /// provider session (see `EmailProvider::fetch_bodies_bulk`), emitting one
    /// `MessageBodyFetched` event per message. Used by `run_backfill` so a
    /// batch of pending messages costs one connection, not one per message.
    pub(super) async fn fetch_message_bodies_bulk(
        &self,
        account_id: &AccountId,
        mailbox_path: &MailboxPath,
        entries: &[BackfillEntry],
    ) -> Result<Vec<MessageId>, ApplicationError> {
        if entries.is_empty() {
            return Ok(vec![]);
        }

        let provider = self.provider_registry.get_provider(account_id)?;
        let ids: Vec<RemoteId> = entries.iter().map(|e| e.remote_id.clone()).collect();

        // Keep login and link failures typed (via the transparent Domain variant)
        // so the backfill stops its pass on them instead of retrying every
        // mailbox against an account that cannot connect.
        let results = provider
            .fetch_bodies_bulk(mailbox_path, &ids)
            .await
            .map_err(|e| match e {
                e if stops_the_pass(&e) => ApplicationError::Domain(e),
                other => {
                    ApplicationError::SyncFailed(format!("Failed to bulk fetch bodies: {other}"))
                }
            })?;
        let by_id: HashMap<RemoteId, MessageBodyResult> = results
            .into_iter()
            .map(|r| (r.remote_id.clone(), r))
            .collect();

        let mut missing = Vec::new();
        for entry in entries {
            let Some(result) = by_id.get(&entry.remote_id) else {
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
    /// (grouped by mailbox, one provider session per mailbox — see
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
                .downloaded_since(
                    &account_id,
                    chrono::Utc::now() - chrono::TimeDelta::hours(24),
                )
                .await
                .unwrap_or(0);
            if let Some(pause) = backfill_pause(spent, daily_limit) {
                debug!(%account_id, spent, "backfill: daily download cap reached, waiting");
                // Without this the bar freezes on the last batch with no reason
                // given; an initial envelope sync alone can fill the cap.
                let remaining = self
                    .projector
                    .count_pending_backfill(&account_id)
                    .await
                    .unwrap_or(0);
                self.publish(SyncProgressEvent {
                    account_id,
                    state: SyncState::FetchingMessageBody,
                    processed: drained,
                    total: drained + remaining,
                    message: "Daily download limit reached, resuming as it frees up".to_string(),
                });
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

                // The poll is a safety net; ingest and sync end wake us.
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    () = self.backfill_wake.notified() => continue,
                    _ = tokio::time::sleep(idle_poll) => continue,
                }
            }

            let backlog = self
                .projector
                .count_pending_backfill(&account_id)
                .await
                .unwrap_or(batch.len() as u32);

            let rfc_ids: Vec<String> = batch
                .iter()
                .filter_map(|e| e.rfc_message_id.clone())
                .collect();
            // One provider session per mailbox for the whole batch.
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
                match self
                    .fetch_message_bodies_bulk(&account_id, path, &entries)
                    .await
                {
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

    /// Gives the copies of these messages a body another copy already has
    /// (see `MessageBodyStore::share_bodies`) and records each; returns them.
    async fn share_bodies(
        &self,
        account_id: &AccountId,
        rfc_ids: &[String],
    ) -> Result<Vec<MessageId>, ApplicationError> {
        let shared = self.projector.share_bodies(account_id, rfc_ids).await?;
        for (message_id, _) in &shared {
            let event = DomainEvent::MessageBodyFetched {
                message_id: *message_id,
                attachments: vec![],
            };
            self.append_and_project(&message_id.to_string(), AggregateType::Message, event)
                .await?;
        }
        Ok(shared.into_iter().map(|(id, _)| id).collect())
    }

    /// The next bodies to download: the head of the queue less the copies
    /// of a message whose body is already stored, which get it for free, and
    /// one copy per Message-ID. The others get theirs once it is fetched.
    pub(super) async fn next_backfill_batch(
        &self,
        account_id: &AccountId,
        abandoned: &HashSet<MessageId>,
    ) -> Result<Vec<BackfillEntry>, ApplicationError> {
        loop {
            let pending = self
                .projector
                .list_pending_backfill(account_id, 500)
                .await?;
            let rfc_ids: Vec<String> = pending
                .iter()
                .filter_map(|e| e.rfc_message_id.clone())
                .collect();
            let shared: HashSet<MessageId> = self
                .share_bodies(account_id, &rfc_ids)
                .await?
                .into_iter()
                .collect();
            let mut seen = HashSet::new();
            let batch: Vec<BackfillEntry> = pending
                .into_iter()
                .filter(|e| !abandoned.contains(&e.message_id) && !shared.contains(&e.message_id))
                .filter(|e| {
                    e.rfc_message_id.as_ref().is_none_or(|id| {
                        seen.insert((id.clone(), e.size_bytes, e.from_email.clone()))
                    })
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
}
