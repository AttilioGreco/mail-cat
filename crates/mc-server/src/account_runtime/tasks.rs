//! The account's sync loop and body backfill, each under a supervisor.

use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use mc_application::{AuthGate, AuthState};
use mc_domain::{AccountId, LoginGate};
use mc_infrastructure::SqliteEventStore;

use super::supervisor::supervise;
use super::{AccountRuntime, TaskCtx};
use crate::wiring::{SearchHandlerImpl, SyncServiceImpl};

/// How often an account is re-synced when nothing else prompts it.
const SYNC_PERIOD: Duration = Duration::from_secs(5 * 60);
/// Least wait before re-syncing after a failure the gate did not schedule.
const SYNC_RETRY_FLOOR: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct SyncLoop {
    id: AccountId,
    name: String,
    gate: Arc<AuthGate>,
    stop: CancellationToken,
    first_sync: Arc<Notify>,
    sync: Arc<SyncServiceImpl>,
    search: Arc<SearchHandlerImpl>,
    event_store: Arc<SqliteEventStore>,
}

impl AccountRuntime {
    /// Syncs at start, then every 5 minutes, and as soon as the account
    /// recovers. Each pass waits on the gate: an account that needs the user
    /// stays quiet, an offline one follows the gate's retry schedule. The
    /// first pass is the account's first real login, so its state is known
    /// within seconds.
    pub(super) fn spawn_sync_loop(&self, ctx: TaskCtx, first_sync: Arc<Notify>) {
        let task = SyncLoop {
            id: ctx.id,
            name: ctx.name.clone(),
            gate: ctx.gate.clone(),
            stop: ctx.stop.clone(),
            first_sync,
            sync: self.deps.sync.clone(),
            search: self.deps.search.clone(),
            event_store: self.deps.event_store.clone(),
        };
        supervise(
            ("sync loop", ctx.name),
            ctx.stop,
            &ctx.tasks,
            move |reason| ctx.gate.task_failed(reason),
            move || task.clone().run(),
        );
    }

    /// Message bodies are downloaded here, not by the initial sync (envelopes
    /// only): INBOX first, capped text only, once per Message-ID, at full
    /// speed up to 90% of the account's daily download cap (Gmail locks an
    /// account out past 2.5 GB/day). Each fetch indexes the body for search
    /// and dequeues itself.
    pub(super) fn spawn_backfill(&self, ctx: TaskCtx, limit: Option<u64>) {
        let sync = self.deps.sync.clone();
        let (id, gate, stop) = (ctx.id, ctx.gate.clone(), ctx.stop.clone());
        supervise(
            ("body backfill", ctx.name),
            ctx.stop,
            &ctx.tasks,
            move |reason| ctx.gate.task_failed(reason),
            move || {
                let (sync, gate, stop) = (sync.clone(), gate.clone(), stop.clone());
                async move {
                    sync.run_backfill(id, limit, gate as Arc<dyn LoginGate>, stop)
                        .await
                }
            },
        );
    }
}

impl SyncLoop {
    /// After each sync, run the search-index catch-up: sync appends events in
    /// bulk and the live bus drops under that load, so this lossless replay
    /// from the event store is what makes synced messages searchable.
    // ponytail: concurrent per-account catch-ups are idempotent (writer is
    // mutex-guarded); redundant overlap is fine, no coordination needed.
    async fn run(self) {
        let Self {
            id,
            name,
            gate,
            stop,
            first_sync,
            sync,
            search,
            event_store,
        } = self;
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                () = gate.ready() => {}
            }
            let synced = tokio::select! {
                _ = stop.cancelled() => break,
                synced = sync.start_sync(&id) => synced,
            };
            let failed = match synced {
                Ok(events) => {
                    tracing::info!("Sync done for '{name}': {} event(s)", events.len());
                    // IMAP answered, even over a pooled session: not offline.
                    gate.login_succeeded();
                    first_sync.notify_one();
                    false
                }
                Err(e) => {
                    tracing::error!("Sync failed for '{name}': {e}");
                    true
                }
            };
            if let Err(e) = search.catch_up(event_store.as_ref()).await {
                tracing::warn!("Search index catch-up after sync of '{name}' failed: {e}");
            }
            // A failure while the retry is already due wasn't a login the
            // gate rescheduled: don't spin on it.
            if failed
                && let AuthState::Offline { next_retry } = gate.status().state
                && next_retry <= Utc::now()
            {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = tokio::time::sleep(SYNC_RETRY_FLOOR) => {}
                }
                continue;
            }
            // Offline, the gate's schedule (2s, 5s, …) beats the 5-minute period.
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(SYNC_PERIOD) => {}
                () = gate.recovered() => {}
                () = gate.retry_due() => {}
            }
        }
        tracing::info!("Sync loop stopped for '{name}'");
    }
}
