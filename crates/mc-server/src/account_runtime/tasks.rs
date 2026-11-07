//! The account's sync loop, body backfill and contacts/calendars sync, each
//! under a supervisor.

use chrono::Utc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use mc_application::{AuthGate, AuthState, DavBackend, DavProviders, DavSyncService};
use mc_domain::model::account::DavConfig;
use mc_domain::{AccountId, AccountSecrets, DomainError, LoginGate};
use mc_infrastructure::oauth::{GOOGLE_CALENDAR, GOOGLE_CARDDAV};
use mc_infrastructure::{DavProfile, DavProvider, SqliteEventStore};

use super::supervisor::supervise;
use super::{AccountRuntime, TaskCtx};
use crate::config::{AccountConfig, OAuthProviderConfig};
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
            if !rest(&gate, &stop, failed).await {
                break;
            }
        }
        tracing::info!("Sync loop stopped for '{name}'");
    }
}

/// Waits for the next pass; false when `stop` ended the wait.
async fn rest(gate: &AuthGate, stop: &CancellationToken, failed: bool) -> bool {
    // A failure while the retry is already due wasn't a login the gate
    // rescheduled: don't spin on it.
    if failed
        && let AuthState::Offline { next_retry } = gate.status().state
        && next_retry <= Utc::now()
    {
        return sleep_or_stop(stop, SYNC_RETRY_FLOOR).await;
    }
    // Offline, the gate's schedule (2s, 5s, …) beats the 5-minute period.
    tokio::select! {
        _ = stop.cancelled() => false,
        _ = tokio::time::sleep(SYNC_PERIOD) => true,
        () = gate.recovered() => true,
        () = gate.retry_due() => true,
    }
}

/// The wait before the next DAV pass: the period, doubled per failure in a
/// row up to 30 minutes, so a server that is down is asked rarely.
pub(super) fn dav_delay(failures: u32) -> Duration {
    SYNC_PERIOD
        .saturating_mul(1 << failures.min(3))
        .min(Duration::from_secs(30 * 60))
}

async fn sleep_or_stop(stop: &CancellationToken, wait: Duration) -> bool {
    tokio::select! {
        _ = stop.cancelled() => false,
        _ = tokio::time::sleep(wait) => true,
    }
}

/// What an account gets for contacts and calendars.
pub(super) enum DavSetup {
    /// The endpoints are known: Google's fixed ones or the account's own.
    Known(DavProvider),
    /// A generic server with no URL configured: discovery decides.
    Discover(DavProvider),
    /// A Google token granted before the DAV scopes: mail only.
    NeedsScopes,
    None,
}

pub(super) fn dav_setup(
    config: &AccountConfig,
    secrets: &AccountSecrets,
    account: AccountId,
    dav: Option<&DavConfig>,
    gate: Arc<dyn LoginGate>,
) -> Result<DavSetup, DomainError> {
    let profile = match config.oauth_provider {
        // ponytail: Graph contacts/calendars are phase 3.
        Some(OAuthProviderConfig::Microsoft) => return Ok(DavSetup::None),
        Some(OAuthProviderConfig::Google) => {
            let granted = secrets
                .oauth
                .as_ref()
                .is_some_and(|t| t.grants_all(&[GOOGLE_CARDDAV, GOOGLE_CALENDAR]));
            if !granted {
                return Ok(DavSetup::NeedsScopes);
            }
            DavProfile::Google
        }
        None => DavProfile::Generic,
    };
    let known = profile == DavProfile::Google
        || dav.is_some_and(|d| d.carddav.is_some() || d.caldav.is_some());
    let provider = DavProvider::new(
        account,
        &config.email,
        config.username.clone(),
        gate,
        dav,
        profile,
    )?;
    Ok(if known {
        DavSetup::Known(provider)
    } else {
        DavSetup::Discover(provider)
    })
}

#[derive(Clone)]
struct DavLoop {
    id: AccountId,
    name: String,
    gate: Arc<AuthGate>,
    stop: CancellationToken,
    sync: Arc<DavSyncService>,
    providers: Arc<DavProviders>,
    backend: Arc<dyn DavBackend>,
    /// Set while the server's DAV endpoints are still to be discovered.
    probe: Option<Arc<DavProvider>>,
}

impl AccountRuntime {
    /// Starts the account's contacts and calendars loop, if its setup has one.
    pub(super) fn spawn_dav_loop(&self, ctx: TaskCtx, setup: DavSetup) {
        let (provider, discover) = match setup {
            DavSetup::Known(provider) => (Arc::new(provider), false),
            DavSetup::Discover(_) if !self.dav_discovery.load(Ordering::Relaxed) => return,
            DavSetup::Discover(provider) => (Arc::new(provider), true),
            DavSetup::NeedsScopes => {
                tracing::info!(
                    "'{}' has no contacts and calendars sync until it logs in again",
                    ctx.name
                );
                ctx.gate.dav_unavailable(
                    "contacts and calendars need a new login to grant access".into(),
                );
                return;
            }
            DavSetup::None => return,
        };
        let probe = discover.then(|| provider.clone());
        self.spawn_dav_task(ctx, provider, probe);
    }

    /// `probe` is set while the server's DAV endpoints are still to be
    /// discovered: the backend is registered only once it answers.
    pub(super) fn spawn_dav_task(
        &self,
        ctx: TaskCtx,
        backend: Arc<dyn DavBackend>,
        probe: Option<Arc<DavProvider>>,
    ) {
        let task = DavLoop {
            id: ctx.id,
            name: ctx.name.clone(),
            gate: ctx.gate.clone(),
            stop: ctx.stop.clone(),
            sync: self.deps.dav.clone(),
            providers: self.deps.dav_providers.clone(),
            backend,
            probe,
        };
        supervise(
            ("contacts and calendars loop", ctx.name),
            ctx.stop,
            &ctx.tasks,
            move |reason| ctx.gate.task_failed(reason),
            move || task.clone().run(),
        );
    }
}

impl DavLoop {
    /// Same cadence as the mail sync, as a task of its own so a slow DAV
    /// server never delays mail. The gate does not hear of DAV failures (they
    /// would pause the mail), so the loop backs off on its own.
    async fn run(self) {
        let Self {
            id,
            name,
            gate,
            stop,
            sync,
            providers,
            backend,
            probe,
        } = self;
        let mut registered = probe.is_none();
        let mut failures = 0u32;
        if registered {
            providers.register(id, backend.clone());
        }
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                () = gate.ready() => {}
            }
            let mut failed = false;
            if let Some(probe) = probe.as_ref().filter(|_| !registered) {
                let found = tokio::select! {
                    _ = stop.cancelled() => break,
                    found = probe.is_available() => found,
                };
                match found {
                    Ok(true) => {
                        providers.register(id, backend.clone());
                        registered = true;
                    }
                    // Looked up again only when the account restarts.
                    Ok(false) => {
                        tracing::info!("No CardDAV or CalDAV server found for '{name}'");
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("DAV discovery failed for '{name}': {e}");
                        failed = true;
                    }
                }
            }
            if registered {
                let synced = tokio::select! {
                    _ = stop.cancelled() => break,
                    synced = sync.sync_account(&id) => synced,
                };
                if let Err(e) = synced {
                    tracing::error!("Contacts and calendars sync failed for '{name}': {e}");
                    failed = true;
                }
            }
            failures = if failed { failures + 1 } else { 0 };
            if !sleep_or_stop(&stop, dav_delay(failures)).await {
                break;
            }
        }
        tracing::info!("Contacts and calendars loop stopped for '{name}'");
    }
}
