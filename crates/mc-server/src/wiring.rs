//! Dependency injection and application wiring.

use anyhow::Context;
use chrono::Utc;
use futures::stream::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use mc_application::bus::{InProcessCommandBus, InProcessEventBus, InProcessQueryBus};
use mc_application::projections::{ProjectionEventHandler, SearchProjectionHandler};
use mc_application::{
    AuthGate, AuthGates, AuthState, Credential, EventPublisher, OAuthLoginTrigger, RefreshFn,
    SyncConfig, SyncService, SyncTrigger,
};
use mc_application::{CommandBus, QueryBus};
use mc_domain::model::account::{IncomingServer, SmtpConnectionConfig, SmtpSecurity};
use mc_domain::{
    Account, AccountEventSource, AccountId, AccountReadRepository, AccountSecrets,
    AccountWriteRepository, AggregateType, AuthConfig, ConnectionConfig, DomainError, DomainEvent,
    EmailAddress, EventEnvelope, EventStore, Identity, LoginGate, MailboxId, MailboxPath,
    ProviderNotification, ProviderRegistry,
};
use mc_infrastructure::{
    DownloadMeter, ImapAccountEventSource, ImapEmailProvider, InMemoryProviderRegistry, SqliteDb,
    SqliteEventStore, SqliteRepository, TantivySearchIndex, email::smtp::SmtpSender,
};
use mc_interfaces::grpc::services::{
    AccountServiceImpl, ContactServiceImpl, MailboxServiceImpl, MessageServiceImpl,
    SearchServiceImpl,
};

use crate::config::{AccountConfig, ServerConfig};
use crate::oauth_login::OAuthChallengeManager;

/// How often an account is re-synced when nothing else prompts it.
const SYNC_PERIOD: std::time::Duration = std::time::Duration::from_secs(5 * 60);
/// Least wait before re-syncing after a failure the gate did not schedule.
const SYNC_RETRY_FLOOR: std::time::Duration = std::time::Duration::from_secs(30);

// Type aliases for concrete implementations
type ProviderRegistryImpl = InMemoryProviderRegistry<ImapEmailProvider>;
type CommandBusImpl = InProcessCommandBus<
    SqliteRepository,     // AW
    SqliteRepository,     // CW
    SqliteEventStore,     // ES
    ProviderRegistryImpl, // PR
    SqliteRepository,     // MR
    InProcessEventBus,    // B
>;
type QueryBusImpl = InProcessQueryBus<
    SqliteRepository,   // AR
    SqliteRepository,   // MBR
    SqliteRepository,   // MR
    TantivySearchIndex, // SI
    SqliteRepository,   // CTR
    SqliteRepository,   // CR
>;
type SyncServiceImpl = SyncService<ProviderRegistryImpl, SqliteEventStore, SqliteRepository>;

/// Application context with all wired dependencies.
pub struct AppContext {
    pub account_service: AccountServiceImpl<CommandBusImpl, QueryBusImpl>,
    pub mailbox_service: MailboxServiceImpl<QueryBusImpl>,
    pub message_service: MessageServiceImpl<CommandBusImpl, QueryBusImpl>,
    pub search_service: SearchServiceImpl<QueryBusImpl>,
    pub contact_service: ContactServiceImpl<QueryBusImpl, CommandBusImpl>,
    #[allow(dead_code)]
    pub sync_service: Arc<SyncServiceImpl>,
    #[allow(dead_code)]
    pub provider_registry: Arc<ProviderRegistryImpl>,
    /// Cancel this to trigger graceful shutdown of all background tasks.
    pub shutdown: CancellationToken,
}

impl AppContext {
    /// Wire up all dependencies from configuration.
    ///
    /// Account passwords/tokens are resolved from `secret_store` (the OS
    /// keychain), not from the config file — which by this point has already
    /// had its plaintext migrated out (see `secret_ingest`).
    pub async fn new(
        config: &ServerConfig,
        config_path: &str,
        secret_store: Arc<dyn mc_domain::SecretStore>,
        oauth_challenges: Arc<OAuthChallengeManager>,
    ) -> anyhow::Result<Self> {
        tracing::info!("Initializing application context (config: {config_path})...");

        // Shared shutdown token — cancel this to stop all background tasks.
        let shutdown = CancellationToken::new();

        // ── Infrastructure Layer ────────────────────────────────────────────

        let db_path = std::path::Path::new(&config.db_path);
        if db_path.is_dir() {
            anyhow::bail!(
                "db_path {} is a directory (the old sled database?); point it at a file, \
                 e.g. {}/mail-cat.db",
                db_path.display(),
                db_path.parent().unwrap_or(db_path).display()
            );
        }
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).context("failed to create database directory")?;
        }
        let db = SqliteDb::open(db_path).context("failed to open SQLite database")?;
        tracing::info!("Opened SQLite database at {}", config.db_path);

        let event_store = Arc::new(SqliteEventStore::new(db.clone()));
        // One repository serves every read/write trait, the projector and the
        // checkpoints; the Arc is for the buses.
        let bare_repository = SqliteRepository::new(db.clone(), event_store.clone());
        let repository_arc = Arc::new(bare_repository.clone());
        tracing::info!("Initialized repositories");

        // Create search index
        let search_index = Arc::new(
            TantivySearchIndex::new(&config.index_path).context("failed to create search index")?,
        );
        tracing::info!("Initialized search index at {}", config.index_path);

        // Providers are registered once the accounts are resolved, below.
        let provider_registry = Arc::new(InMemoryProviderRegistry::new());

        // ── Application Layer ───────────────────────────────────────────────

        // Create event bus (capacity: max 1000 events in flight)
        let event_bus = Arc::new(InProcessEventBus::new(1000));
        tracing::info!("Initialized event bus");

        // Message commands ask for these when the server did more than they
        // could record; served once the sync service exists.
        let (resync_tx, resync_rx) =
            tokio::sync::mpsc::unbounded_channel::<(AccountId, MailboxPath)>();

        // Create command bus
        let command_bus = Arc::new(
            InProcessCommandBus::new(
                repository_arc.clone(),    // AW
                repository_arc.clone(),    // CW
                event_store.clone(),       // ES
                provider_registry.clone(), // PR
                repository_arc.clone(),    // MR
                event_bus.clone(),         // B
            )
            .with_resync_requests(resync_tx),
        );
        tracing::info!("Initialized command bus");

        // Create query bus
        let query_bus = Arc::new(InProcessQueryBus::new(
            repository_arc.clone(), // AR
            repository_arc.clone(), // MBR
            repository_arc.clone(), // MR
            search_index.clone(),   // SI
            repository_arc.clone(), // CTR
            repository_arc.clone(), // CR
        ));
        tracing::info!("Initialized query bus");

        // Create sync service with default configuration
        let sync_config = SyncConfig {
            batch_size: 50,
            max_concurrent_bodies: 4,
            // Bodies are pulled by the throttled background backfill worker
            // (newest-first), not inline during the initial envelope sync — so
            // a 30k-message account doesn't burst Gmail's daily bandwidth cap.
            eager_body_fetch: false,
        };
        let sync_service = Arc::new(
            SyncService::new(
                provider_registry.clone(),
                event_store.clone(),
                repository_arc.clone(),
                sync_config,
            )
            .with_event_publisher(event_bus.clone()),
        );
        tracing::info!("Initialized sync service");

        {
            let sync = Arc::clone(&sync_service);
            tokio::spawn(async move { sync.serve_resyncs(resync_rx).await });
        }

        // Create projection event handlers and start catching up
        // Search index is an event-sourced projection with its own checkpoint,
        // refreshing messages from the read model. Shared across boot catch-up,
        // the live subscriber, and post-sync catch-up.
        let search_handler = Arc::new(SearchProjectionHandler::new(
            search_index.clone(),
            bare_repository.clone(),
            bare_repository.clone(),
        ));
        let projection_handler =
            ProjectionEventHandler::new(bare_repository.clone(), bare_repository);
        tracing::info!("Initialized projection handlers");

        // Catch up projections on startup (rebuild read models from events).
        // Read model first so the search projection reads a current read model.
        tracing::info!("Catching up projections from event store...");
        projection_handler
            .catch_up(event_store.as_ref())
            .await
            .context("failed to catch up projections")?;
        if search_index.was_recreated() {
            search_handler
                .reset()
                .await
                .context("failed to reset search index checkpoint")?;
        }
        search_handler
            .catch_up(event_store.as_ref())
            .await
            .context("failed to catch up search index")?;
        tracing::info!("Projections caught up successfully");

        // ── Auto-create accounts from config ─────────────────────────────
        // Do NOT swallow this error: if listing fails we must not fall through to
        // "no accounts exist" and mint duplicate accounts (with fresh IDs, forcing
        // a full IMAP re-sync) on top of whatever is already in the read model.
        let existing_accounts = repository_arc
            .list_accounts()
            .await
            .context("failed to list existing accounts during startup")?;

        // Transitions that must survive a restart (re-login needed, password
        // rejected, recovered) come out of the gates in order; appended below.
        let (auth_events_tx, auth_events) = tokio::sync::mpsc::unbounded_channel();
        let refresh: RefreshFn = Arc::new(|provider, refresh_token| {
            Box::pin(
                async move { mc_infrastructure::oauth::refresh(provider, &refresh_token).await },
            )
        });
        let mut auth_gates = AuthGates::new(refresh, secret_store.clone(), auth_events_tx);

        let mut registered: Vec<(AccountId, String, Arc<AuthGate>)> = Vec::new();
        let mut oauth_names: HashMap<AccountId, String> = HashMap::new();
        let mut created_new = false;
        // Per-account backfill rate override (falls back to the global default).
        let mut download_limits: HashMap<AccountId, Option<u64>> = HashMap::new();

        for account_config in &config.accounts {
            let existing = existing_accounts
                .iter()
                .find(|a| a.email.as_str() == account_config.email);
            let (account_id, name) = match existing {
                Some(summary) => (summary.id, summary.name.clone()),
                None => {
                    let account_id =
                        create_account(account_config, repository_arc.as_ref()).await?;
                    created_new = true;
                    (account_id, account_config.name.clone())
                }
            };

            // Resolve secrets from the OS keychain (plaintext was migrated out
            // of the config file at startup by secret_ingest).
            let secrets = secret_store
                .load(&account_config.email)
                .with_context(|| format!("reading keychain secrets for {}", account_config.email))?
                .unwrap_or_default();
            let imap_password = secrets.password.clone().unwrap_or_default();
            let credential = match account_config.oauth_provider {
                Some(oauth_cfg) => {
                    oauth_names.insert(account_id, name.clone());
                    // An empty refresh token makes the gate ask for a login on
                    // first use instead of knocking with a token it cannot have.
                    let tokens = secrets.oauth.clone().unwrap_or_else(|| {
                        tracing::warn!(
                            "⚠ {}: nessun token OAuth nel keychain — esegui `mc accounts add-oauth`",
                            account_config.email
                        );
                        mc_domain::OAuthSecrets {
                            access_token: String::new(),
                            refresh_token: String::new(),
                            expires_at: Utc::now(),
                        }
                    });
                    Credential::OAuth {
                        provider: oauth_cfg.into(),
                        tokens,
                    }
                }
                None => {
                    if imap_password.is_empty() {
                        tracing::warn!(
                            "⚠ {}: nessuna password nel keychain di sistema — inserisci `password = \"...\"` in config.toml per importarla",
                            account_config.email
                        );
                    }
                    Credential::Password(imap_password.clone())
                }
            };
            let gate = auth_gates.add(account_id, &account_config.email, credential);
            if let Some(summary) = existing {
                gate.restore(summary);
            }

            let smtp_sender =
                build_smtp_sender(account_config, &secrets, &imap_password, &gate).await;
            let login_gate = gate.clone() as Arc<dyn LoginGate>;
            let provider = Arc::new(
                ImapEmailProvider::new(
                    account_config.imap_host.clone(),
                    account_config.imap_port,
                    account_config.username.clone(),
                    login_gate,
                    account_config.use_tls,
                    smtp_sender,
                )
                .with_download_meter(DownloadMeter::new(db.clone(), &account_config.email)),
            );
            provider_registry.register_provider(account_id, provider);
            tracing::info!(
                "Registered provider for '{}' ({})",
                name,
                account_config.email
            );

            // A pause persisted by an older run gets one real login now; the
            // user may have fixed it meanwhile (e.g. a password in config.toml).
            if gate.status().state.is_paused() {
                let gate = gate.clone();
                let (host, port, username, use_tls) = (
                    account_config.imap_host.clone(),
                    account_config.imap_port,
                    account_config.username.clone(),
                    account_config.use_tls,
                );
                tokio::spawn(async move {
                    gate.retry_after_restart(|secret| async move {
                        ImapEmailProvider::new(
                            host,
                            port,
                            username,
                            Arc::new(secret),
                            use_tls,
                            None,
                        )
                        .verify_login()
                        .await
                    })
                    .await;
                });
            }

            download_limits.insert(account_id, account_config.daily_download_limit());
            registered.push((account_id, name, gate));
        }
        let auth_gates = Arc::new(auth_gates);

        // The port every OAuth login goes through — notification button, UI
        // button, add-account dialog. It owns handing a fresh token to the
        // account's gate, so no caller has to remember to.
        let oauth_trigger = Arc::new(crate::oauth_reauth::ReauthTrigger::new(
            oauth_challenges,
            auth_gates.clone(),
            oauth_names,
            secret_store.clone(),
        )) as Arc<dyn OAuthLoginTrigger>;

        // ── Re-fire re-auth notification for accounts still needing it ──────────
        // An OAuth account that died before the user re-authenticated (e.g. server
        // restarted mid-outage) gets its "Ri-autentica" notification reissued
        // immediately instead of staying silently orphaned.
        for account in existing_accounts
            .iter()
            .filter(|a| a.status == mc_domain::AccountStatus::NeedsReauth)
        {
            if let Some(gate) = auth_gates
                .get(&account.id)
                .filter(|g| g.oauth_provider().is_some())
            {
                spawn_reauth_notification(
                    account.id,
                    gate.email().to_owned(),
                    "Autenticazione scaduta — accedi di nuovo".to_owned(),
                    oauth_trigger.clone(),
                );
            }
        }

        // ── Live projection subscriber ───────────────────────────────────────
        // Every publisher has already applied its event to the read model
        // (sync, message commands, and the repository's own save), so the bus
        // only has to bring the search index along. A lagging subscriber
        // skips events; the post-sync and boot catch-up re-index them.
        {
            let mut sub = event_bus.subscribe();
            let search = search_handler.clone();
            let sd = shutdown.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = sd.cancelled() => break,
                        envelope = sub.recv() => match envelope {
                            Some(e) => {
                                if let Err(err) = search.apply_live(&e).await {
                                    tracing::warn!("Live search projection error for {}: {}", e.payload.event_name(), err);
                                }
                            }
                            None => break,
                        },
                    }
                }
                tracing::info!("Live search subscriber exited");
            });
            tracing::info!("Live search subscriber started");
        }

        // ── Auto-populate contacts from received and sent messages ───────────
        {
            let mut sub = event_bus.subscribe();
            let ensure_bus = command_bus.clone();
            let sd = shutdown.clone();
            tokio::spawn(async move {
                // A sender shows up on many messages; ensure each address once
                // per run instead of once per message. Holds only distinct
                // addresses, so it stays small.
                let mut ensured: HashSet<(AccountId, mc_domain::EmailAddress)> = HashSet::new();
                loop {
                    let envelope = tokio::select! {
                        _ = sd.cancelled() => break,
                        e = sub.recv() => match e { Some(v) => v, None => break },
                    };
                    let (account_id, addrs) = match envelope.payload {
                        mc_domain::DomainEvent::MessageReceived {
                            account_id,
                            envelope: mail,
                            ..
                        } => (account_id, [mail.from, mail.to, mail.cc].concat()),
                        // The people you write to are the ones autocomplete most needs.
                        mc_domain::DomainEvent::MessageSent {
                            account_id,
                            envelope: mail,
                            ..
                        } => (account_id, [mail.to, mail.cc, mail.bcc].concat()),
                        _ => continue,
                    };
                    for addr in addrs {
                        if !ensured.insert((account_id, addr.email.clone())) {
                            continue;
                        }
                        let cmd = mc_application::Command::EnsureContact {
                            email: addr.email,
                            display_name: addr.name.filter(|s| !s.is_empty()),
                            account_id,
                        };
                        if let Err(e) = ensure_bus.dispatch(cmd).await {
                            tracing::warn!("EnsureContact from message failed: {e}");
                        }
                    }
                }
                tracing::info!("EnsureContact subscriber exited");
            });
            tracing::info!("EnsureContact (auto-populate) subscriber started");
        }

        // ── Desktop notification subscriber ──────────────────────────────────
        // Listens for domain events on the event bus and fires OS desktop
        // notifications (notify-rust → D-Bus on Linux, WinRT on Windows,
        // NSUserNotification on macOS).
        //
        // Handled events:
        //   • MessageReceived  – new unread mail popup with action buttons:
        //       "Archivia" → MoveMessage to Archive mailbox via command bus
        //       "Apri Mail Cat" → spawns the mail-cat-ui process
        //     Actions are routed by the shared NotificationCenter listener, not
        //     by notify-rust's per-notification wait, so they keep working after
        //     the banner has moved into the shell's notification panel.
        //   • MessageFlagged(Seen) / MessageMailboxesChanged / MessageDeleted – dismiss any
        //     notification still up for that message. Covers reading the mail in
        //     another client (remote flag change → MessageFlagged) as well as
        //     acting on it from the notification itself.
        //   • AccountSyncFailed – error alert (no actions)
        //   • AccountAuthExpired – error alert with "Ri-autentica" action:
        //       drives OAuthChallengeManager::start_login() in-process, opens the
        //       system browser, and on success hot-swaps the new tokens into the
        //       live OAuthRuntimeState cells (no server restart needed)
        {
            let mut sub = event_bus.subscribe();
            let notif_cmd_bus = command_bus.clone() as Arc<dyn mc_application::bus::CommandBus>;
            let notif_q_bus = query_bus.clone() as Arc<dyn mc_application::bus::QueryBus>;
            let notif_ui_bin = config.ui_bin.clone();
            let center = crate::notifications::NotificationCenter::connect().await;
            center.clone().spawn_action_loop(
                notif_cmd_bus.clone(),
                notif_q_bus.clone(),
                notif_ui_bin.clone(),
                shutdown.clone(),
            );
            let notif_gates = auth_gates.clone();
            let notif_oauth_trigger = oauth_trigger.clone();
            let sd = shutdown.clone();
            tokio::spawn(async move {
                loop {
                    let envelope = tokio::select! {
                        _ = sd.cancelled() => break,
                        e = sub.recv() => match e { Some(v) => v, None => break },
                    };
                    match envelope.payload {
                        mc_domain::DomainEvent::MessageReceived {
                            message_id,
                            account_id,
                            mailbox_id,
                            envelope: mail_envelope,
                            flags,
                            restored,
                            ..
                        } => {
                            if restored || flags.contains(&mc_domain::Flag::Seen) {
                                continue;
                            }
                            let from = mail_envelope
                                .from
                                .first()
                                .map(|a| a.name.as_deref().unwrap_or(a.email.as_str()).to_owned())
                                .unwrap_or_else(|| "Unknown".to_owned());
                            let subject = mail_envelope.subject.clone();
                            let cmd = notif_cmd_bus.clone();
                            let q = notif_q_bus.clone();
                            let ui_bin = notif_ui_bin.clone();
                            let center = center.clone();
                            let target = crate::notifications::NotifiedMessage {
                                message_id,
                                account_id,
                                mailbox_id,
                            };
                            // Capture the tokio handle before entering the blocking task so we
                            // can drive async work (command dispatch, mailbox lookup) from the
                            // synchronous fallback action callback.
                            let rt = tokio::runtime::Handle::current();
                            tokio::task::spawn_blocking(move || {
                                let handle = match notify_rust::Notification::new()
                                    .summary(&format!("New message from {from}"))
                                    .body(&subject)
                                    .icon(&app_icon_path())
                                    // "default" is the freedesktop action fired when the
                                    // notification body itself is clicked, which is what
                                    // most people reach for. It carries no button label.
                                    .action("default", "Apri")
                                    .action("archive", "Archivia")
                                    .action("open", "Apri Mail Cat")
                                    .show()
                                {
                                    Ok(h) => h,
                                    Err(e) => {
                                        tracing::warn!("Desktop notification failed: {e}");
                                        return;
                                    }
                                };
                                if center.is_listening() {
                                    // The shared listener owns the actions from here
                                    // on; it survives the banner being swept into the
                                    // notification panel, and keeping the id lets us
                                    // dismiss the notification once the mail is read.
                                    center.track(handle.id(), target);
                                    return;
                                }
                                // Fallback (no session bus): one-shot listener, actions
                                // only work while the notification is on screen.
                                handle.wait_for_action(move |action| {
                                    rt.block_on(crate::notifications::run_action(
                                        action, target, &cmd, &q, &ui_bin,
                                    ));
                                });
                            });
                        }

                        // Mail the user already dealt with — read here or in another
                        // client, archived, or deleted — should not keep a popup up.
                        mc_domain::DomainEvent::MessageFlagged {
                            message_id,
                            ref flags_added,
                            ..
                        } if flags_added.contains(&mc_domain::Flag::Seen) => {
                            center.close_for_message(&message_id).await;
                        }
                        mc_domain::DomainEvent::MessageMailboxesChanged { message_id, .. }
                        | mc_domain::DomainEvent::MessageDeleted { message_id, .. } => {
                            center.close_for_message(&message_id).await;
                        }
                        mc_domain::DomainEvent::AccountSyncFailed { account_id, error } => {
                            tokio::task::spawn_blocking(move || {
                                if let Err(e) = notify_rust::Notification::new()
                                    .summary(&format!("Sync error – {}", account_id.as_uuid()))
                                    .body(&error)
                                    .icon("dialog-error")
                                    .show()
                                {
                                    tracing::warn!("Desktop notification failed: {e}");
                                }
                            });
                        }
                        mc_domain::DomainEvent::AccountAuthExpired { account_id, error } => {
                            // Only an OAuth account can be fixed by logging in again.
                            let Some(gate) = notif_gates
                                .get(&account_id)
                                .filter(|g| g.oauth_provider().is_some())
                            else {
                                continue;
                            };
                            spawn_reauth_notification(
                                account_id,
                                gate.email().to_owned(),
                                error,
                                notif_oauth_trigger.clone(),
                            );
                        }
                        mc_domain::DomainEvent::AccountCredentialsRejected {
                            account_id, ..
                        } => {
                            let Some(email) =
                                notif_gates.get(&account_id).map(|g| g.email().to_owned())
                            else {
                                continue;
                            };
                            tokio::task::spawn_blocking(move || {
                                if let Err(e) = notify_rust::Notification::new()
                                    .summary(&format!("Password rifiutata per {email}"))
                                    .body("Aggiorna la password nelle impostazioni dell'account.")
                                    .icon("dialog-error")
                                    .show()
                                {
                                    tracing::warn!("Desktop notification failed: {e}");
                                }
                            });
                        }
                        _ => {}
                    }
                }
                tracing::info!("Notification subscriber exited");
            });
            tracing::info!("Desktop notification subscriber started");
        }

        // Spawned after the notification subscriber, so a transition at boot
        // (the SMTP check) still pops its notification.
        {
            let event_store = event_store.clone() as Arc<dyn mc_domain::EventStore>;
            let repository = repository_arc.clone() as Arc<dyn AccountWriteRepository>;
            let publisher = event_bus.clone() as Arc<dyn mc_application::EventPublisher>;
            let mut auth_events = auth_events;
            tokio::spawn(async move {
                while let Some((account_id, event)) = auth_events.recv().await {
                    let name = event.event_name();
                    if let Err(e) = append_account_event(
                        account_id,
                        event,
                        &event_store,
                        &repository,
                        &publisher,
                    )
                    .await
                    {
                        tracing::error!(
                            "Failed recording {name} for {}: {e}",
                            account_id.as_uuid()
                        );
                    }
                }
            });
        }

        // Search-index maintenance runs through the search-index projection: the
        // live subscriber above applies runtime events instantly, and
        // `search_handler.catch_up` (boot + after each sync) is the lossless
        // backstop. No separate ad-hoc subscriber needed.

        // Re-run projection catch-up if new accounts were created
        if created_new {
            tracing::info!("Re-running projection catch-up for newly created accounts...");
            projection_handler
                .catch_up(event_store.as_ref())
                .await
                .context("failed to catch up projections after account creation")?;
            tracing::info!("Projections caught up after account creation");
        }

        // Search index was brought up to date incrementally by
        // `search_handler.catch_up` above (checkpoint-driven, no full rebuild).
        // If new accounts were just created, re-run it so any events replayed by
        // the account-creation catch-up are indexed too.
        if created_new {
            search_handler
                .catch_up(event_store.as_ref())
                .await
                .context("failed to catch up search index after account creation")?;
        }

        // ── Sync loop, one per account ──────────────────────────────────────
        // Syncs at boot, then every 5 minutes, and as soon as the account
        // recovers. Each pass waits on the gate: an account that needs the user
        // stays quiet, an offline one follows the gate's retry schedule. The
        // first pass is the account's first real login, so its state is known
        // within seconds of startup.
        // After each sync, run the search-index catch-up: sync appends events in
        // bulk and the live bus drops under that load, so this lossless replay
        // from the event store is what makes synced messages searchable.
        // ponytail: concurrent per-account catch-ups are idempotent (writer is
        // mutex-guarded); redundant overlap is fine, no coordination needed.
        for (account_id, name, gate) in &registered {
            let sync = sync_service.clone();
            let id = *account_id;
            let account_name = name.clone();
            let gate = gate.clone();
            let search = search_handler.clone();
            let es = event_store.clone();
            let sd = shutdown.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = sd.cancelled() => break,
                        () = gate.ready() => {}
                    }
                    let failed = match sync.start_sync(&id).await {
                        Ok(events) => {
                            tracing::info!(
                                "Sync done for '{}': {} event(s)",
                                account_name,
                                events.len()
                            );
                            // IMAP answered, even over a pooled session: not offline.
                            gate.login_succeeded();
                            false
                        }
                        Err(e) => {
                            tracing::error!("Sync failed for '{}': {}", account_name, e);
                            true
                        }
                    };
                    if let Err(e) = search.catch_up(es.as_ref()).await {
                        tracing::warn!(
                            "Search index catch-up after sync of '{}' failed: {}",
                            account_name,
                            e
                        );
                    }
                    // A failure while the retry is already due wasn't a login the
                    // gate rescheduled: don't spin on it.
                    if failed
                        && let AuthState::Offline { next_retry } = gate.status().state
                        && next_retry <= Utc::now()
                    {
                        tokio::select! {
                            _ = sd.cancelled() => break,
                            _ = tokio::time::sleep(SYNC_RETRY_FLOOR) => {}
                        }
                        continue;
                    }
                    // Offline, the gate's schedule (2s, 5s, …) beats the 5-minute period.
                    tokio::select! {
                        _ = sd.cancelled() => break,
                        _ = tokio::time::sleep(SYNC_PERIOD) => {}
                        () = gate.recovered() => {}
                        () = gate.retry_due() => {}
                    }
                }
                tracing::info!("Sync loop stopped for '{}'", account_name);
            });
        }

        // Keeps OAuth tokens ahead of expiry; logins refresh on their own too.
        auth_gates.spawn_refresh_timer(shutdown.clone());

        // ── Background body backfill ─────────────────────────────────────────
        // The initial sync only pulls envelopes; message bodies are downloaded
        // here, INBOX first, capped text only, once per Message-ID, at full
        // speed up to 90% of each account's daily download cap (Gmail locks an
        // account out past 2.5 GB/day).
        // Each fetch indexes the body for search and dequeues itself.
        for (account_id, _name, gate) in &registered {
            let sync = Arc::clone(&sync_service);
            let id = *account_id;
            let sd = shutdown.clone();
            let limit = download_limits.get(account_id).copied().flatten();
            let gate = gate.clone() as Arc<dyn LoginGate>;
            tokio::spawn(async move {
                sync.run_backfill(id, limit, gate, sd).await;
            });
        }

        // ── Server-side mailbox watchers ─────────────────────────────────────
        // One background task per account watches INBOX via IMAP IDLE.
        // Each ProviderNotification is mapped to a domain command/event so that
        // all state changes flow through the single InProcessEventBus:
        //
        //   NewMessage / MailboxCountChanged → debounced sync (Semaphore 1)
        //       └→ SyncService emits MessageReceived + MailboxCountsUpdated
        //
        //   FlagsChanged  → Command::ApplyRemoteFlagChange
        //       └→ emits MessageFlagged on the bus
        //
        //   (Expunges arrive as MailboxCountChanged → sync → reconcile detects the
        //    missing UID and emits MessageDeleted; plain IMAP IDLE gives no reliable
        //    UID for an EXPUNGE.)
        //
        // Subscribers on the bus (LiveProjector, DesktopNotifier, watch_mailbox
        // gRPC stream) react to domain events only — no parallel raw-event bus.
        // Tests need syncs to happen only when they ask; push would race them.
        let idle_enabled = std::env::var_os("MAIL_CAT_NO_IDLE").is_none();
        for (account_id, name, _gate) in registered.iter().filter(|_| idle_enabled) {
            // Load local mailboxes once: used both to build path→MailboxId and
            // to get the server-correct INBOX MailboxPath (with the real delimiter).
            let local_mailboxes = match query_bus
                .dispatch(mc_application::Query::ListMailboxes {
                    account_id: *account_id,
                })
                .await
            {
                Ok(mc_application::QueryResult::Mailboxes(m)) => m,
                _ => vec![],
            };

            // Use the stored INBOX path (has the server's actual delimiter, e.g. '.')
            // so that notifications emitted by watch_mailbox match what's in the read model.
            // Fall back to MailboxPath::inbox() only if INBOX hasn't been synced yet.
            let inbox_path = local_mailboxes
                .iter()
                .find(|mb| mb.path.full_path().to_uppercase() == "INBOX")
                .map(|mb| mb.path.clone())
                .unwrap_or_else(MailboxPath::inbox);

            // path→MailboxId map for FlagsChanged routing.
            let path_to_id: HashMap<String, MailboxId> = local_mailboxes
                .into_iter()
                .map(|mb| (mb.path.full_path().to_string(), mb.id))
                .collect();

            if let Ok(provider) = provider_registry.get_provider(account_id) {
                let event_source = ImapAccountEventSource::new(provider);
                let mut stream = event_source.watch(std::slice::from_ref(&inbox_path));

                let sync = Arc::clone(&sync_service);
                let cmd_bus = command_bus.clone();
                let id = *account_id;
                let account_name = name.clone();
                let sd = shutdown.clone();
                // Semaphore(1): at most one triggered sync in flight per account.
                let sync_gate = Arc::new(tokio::sync::Semaphore::new(1));

                tokio::spawn(async move {
                    tracing::info!("[Watcher] Started INBOX IDLE for '{}'", account_name);

                    loop {
                        let result = tokio::select! {
                            _ = sd.cancelled() => break,
                            item = stream.next() => match item { Some(v) => v, None => break },
                        };
                        {
                            match result {
                                Ok(notification) => match notification {
                                    // ── New message / count changed ──────────────
                                    // Targeted sync of this mailbox from its cursor.
                                    // Emits MessageReceived + MailboxCountsUpdated
                                    // and advances the cursor so start_sync
                                    // won't re-fetch on the next periodic run.
                                    ProviderNotification::NewMessage { ref mailbox, .. }
                                    | ProviderNotification::MailboxCountChanged {
                                        ref mailbox,
                                        ..
                                    } => {
                                        if let Ok(permit) = sync_gate.clone().try_acquire_owned() {
                                            let sync_clone = Arc::clone(&sync);
                                            let id_clone = id;
                                            let mailbox_clone = mailbox.clone();
                                            tokio::spawn(async move {
                                                let _permit = permit;
                                                if let Err(e) = sync_clone
                                                    .sync_mailbox(&id_clone, &mailbox_clone)
                                                    .await
                                                {
                                                    tracing::warn!(
                                                        "[Watcher] Partial sync failed: {e}"
                                                    );
                                                }
                                            });
                                        }
                                    }

                                    // ── Remote flag change ───────────────────────
                                    // Map immediately to a domain command so the UI
                                    // gets a MessageFlagged event without waiting
                                    // for the next full sync.
                                    ProviderNotification::FlagsChanged {
                                        ref mailbox,
                                        ref remote_id,
                                        ref flags,
                                    } => {
                                        if let Some(mb_id) = path_to_id.get(&mailbox.full_path()) {
                                            let cmd =
                                                mc_application::Command::ApplyRemoteFlagChange {
                                                    mailbox_id: *mb_id,
                                                    remote_id: remote_id.clone(),
                                                    new_flags: flags.clone(),
                                                };
                                            if let Err(e) = cmd_bus.dispatch(cmd).await {
                                                tracing::warn!(
                                                    "[Watcher] ApplyRemoteFlagChange id={}: {e}",
                                                    remote_id
                                                );
                                            }
                                        }
                                    }
                                },
                                Err(e) => {
                                    tracing::warn!(
                                        "[Watcher] IDLE error for '{}': {e}",
                                        account_name
                                    );
                                }
                            }
                        } // end inner block
                    } // end loop

                    tracing::info!("[Watcher] INBOX IDLE stopped for '{}'", account_name);
                });
            }
        }
        tracing::info!(
            "Started server-side mailbox watchers for {} account(s)",
            registered.len()
        );

        // ── Interface Layer ─────────────────────────────────────────────────

        // Create gRPC services
        let account_service = AccountServiceImpl::new(command_bus.clone(), query_bus.clone())
            .with_sync_trigger(sync_service.clone() as Arc<dyn SyncTrigger>)
            .with_oauth_trigger(oauth_trigger.clone())
            .with_auth_gates(auth_gates.clone())
            .with_provisioner(
                Arc::new(crate::account_provisioner::ConfigAccountProvisioner::new(
                    std::path::PathBuf::from(config_path),
                    secret_store.clone(),
                )) as Arc<dyn mc_application::AccountProvisioner>,
            );

        let mailbox_service = MailboxServiceImpl::new(query_bus.clone());

        let message_service =
            MessageServiceImpl::new(command_bus.clone(), query_bus.clone(), event_bus.clone());

        let search_service = SearchServiceImpl::new(query_bus.clone());

        let contact_service = ContactServiceImpl::new(query_bus.clone(), command_bus.clone());

        tracing::info!("All services initialized successfully");

        Ok(Self {
            account_service,
            mailbox_service,
            message_service,
            search_service,
            contact_service,
            sync_service,
            provider_registry,
            shutdown,
        })
    }
}

/// Pops a desktop notification with a "Ri-autentica" action for an account whose
/// OAuth refresh token is dead, and drives the full re-auth flow if the user clicks
/// it. Fire-and-forget: spawns a blocking OS thread and returns immediately.
fn spawn_reauth_notification(
    account_id: AccountId,
    email: String,
    error: String,
    trigger: Arc<dyn OAuthLoginTrigger>,
) {
    tokio::task::spawn_blocking(move || {
        let handle = match notify_rust::Notification::new()
            .summary(&format!("{} richiede una nuova autenticazione", email))
            .body(&error)
            .icon("dialog-error")
            .action("reauth", "Ri-autentica")
            .show()
        {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!("Desktop notification failed: {e}");
                return;
            }
        };
        let rt = tokio::runtime::Handle::current();
        handle.wait_for_action(move |action| {
            if action != "reauth" {
                return;
            }
            rt.block_on(async move {
                // The trigger owns reviving the account (tokens back into the
                // live cells, needs_reauth cleared, AccountReauthenticated
                // appended) — here we only drive the browser and report.
                let mut progress = trigger.start_reauth(account_id);
                while let Some(update) = progress.recv().await {
                    match update {
                        mc_application::OAuthLoginProgress::AwaitingBrowser { authorize_url } => {
                            if let Err(e) = mc_shared::browser::open_browser(&authorize_url) {
                                tracing::warn!(
                                    "Failed to open browser for re-auth ({e}) — \
                                     apri manualmente: {authorize_url}"
                                );
                            }
                        }
                        mc_application::OAuthLoginProgress::Succeeded { .. } => {
                            let _ = notify_rust::Notification::new()
                                .summary(&format!("{} ri-autenticato", email))
                                .body("Account riattivato con successo.")
                                .icon("dialog-information")
                                .show();
                        }
                        mc_application::OAuthLoginProgress::Failed { message } => {
                            tracing::warn!("Re-auth failed for {}: {message}", email);
                            let _ = notify_rust::Notification::new()
                                .summary(&format!("Ri-autenticazione fallita per {}", email))
                                .body(&message)
                                .icon("dialog-error")
                                .show();
                        }
                    }
                }
            });
        });
    });
}

/// Creates the aggregate for an account declared in config but not yet in the
/// database.
async fn create_account(
    account_config: &AccountConfig,
    repository: &SqliteRepository,
) -> anyhow::Result<AccountId> {
    let email = EmailAddress::new(&account_config.email).context("invalid email in config")?;
    let smtp = account_config.smtp.as_ref().map(|s| SmtpConnectionConfig {
        host: s.host.clone(),
        port: s.port,
        security: smtp_security(s.security),
        username: s.username.clone(),
        // Secret lives in the keychain, not the persisted aggregate
        // (AuthConfig secret fields are #[serde(skip)]).
        password: None,
    });
    let connection_config = ConnectionConfig {
        incoming: IncomingServer::Imap {
            host: account_config.imap_host.clone(),
            port: account_config.imap_port,
            use_tls: account_config.use_tls,
        },
        auth: AuthConfig::Plain {
            username: account_config.username.clone(),
            password: String::new(), // resolved from keychain at runtime
        },
        smtp,
    };
    let identity = Identity {
        display_name: account_config.name.clone(),
        email: email.clone(),
        signature_html: None,
        signature_text: None,
    };
    let (account, event) = Account::create(
        account_config.name.clone(),
        email,
        account_config.provider_type.into(),
        connection_config,
        identity,
    );
    repository
        .save(&account, vec![event])
        .await
        .context("failed to auto-create account")?;
    tracing::info!(
        "Auto-created account '{}' ({})",
        account_config.name,
        account_config.email
    );
    Ok(account.id)
}

fn smtp_security(security: crate::config::SmtpSecurityConfig) -> SmtpSecurity {
    match security {
        crate::config::SmtpSecurityConfig::Smtps => SmtpSecurity::Smtps,
        crate::config::SmtpSecurityConfig::StartTls => SmtpSecurity::StartTls,
    }
}

/// The account's SMTP sender, checked once at boot unless the account is
/// paused. It takes the account's own credential from the gate (the OAuth
/// token, or the IMAP password) unless it has an SMTP password of its own.
/// Either way a refused SMTP login only shows up as the status's SMTP error:
/// IMAP alone decides whether the account's credential works.
async fn build_smtp_sender(
    account_config: &AccountConfig,
    secrets: &AccountSecrets,
    imap_password: &str,
    gate: &Arc<AuthGate>,
) -> Option<SmtpSender> {
    let Some(smtp_cfg) = &account_config.smtp else {
        tracing::warn!(
            "⚠ SMTP {}: nessun blocco [smtp] in config — invio disabilitato",
            account_config.email
        );
        return None;
    };
    let domain_smtp = SmtpConnectionConfig {
        host: smtp_cfg.host.clone(),
        port: smtp_cfg.port,
        security: smtp_security(smtp_cfg.security),
        username: smtp_cfg.username.clone(),
        password: secrets.smtp_password.clone(),
    };
    let own_password = secrets
        .smtp_password
        .as_deref()
        .is_some_and(|p| p != imap_password);
    let sender = if account_config.oauth_provider.is_some() || !own_password {
        SmtpSender::build_gated(&domain_smtp, &account_config.username, gate.clone())
    } else {
        match SmtpSender::build(
            &domain_smtp,
            &account_config.username,
            imap_password,
            gate.clone(),
        ) {
            Ok(sender) => sender,
            Err(e) => {
                tracing::warn!(
                    "⚠ SMTP {}: configurazione non valida — {e}",
                    account_config.email
                );
                return None;
            }
        }
    };
    if gate.status().state.is_paused() {
        return Some(sender);
    }
    match sender.test_connection().await {
        Ok(true) => tracing::info!(
            "✓ SMTP {} ({}:{}) connessione OK",
            account_config.email,
            smtp_cfg.host,
            smtp_cfg.port
        ),
        Ok(false) => tracing::warn!(
            "⚠ SMTP {}: server raggiungibile ma NOOP fallito",
            account_config.email
        ),
        Err(e) => tracing::warn!("⚠ SMTP {}: non raggiungibile — {e}", account_config.email),
    }
    Some(sender)
}

/// Appends a single event to an `Account` aggregate and publishes it to the
/// in-process event bus, mirroring `SyncService::append_and_project`'s
/// append-then-publish pattern (repository `save()` alone does not publish).
async fn append_account_event(
    account_id: AccountId,
    event: DomainEvent,
    event_store: &Arc<dyn EventStore>,
    account_repository: &Arc<dyn AccountWriteRepository>,
    event_publisher: &Arc<dyn EventPublisher>,
) -> Result<(), DomainError> {
    let mut account = account_repository
        .load(&account_id)
        .await?
        .ok_or_else(|| DomainError::AccountNotFound(account_id.to_string()))?;
    let sequence_number = event_store
        .current_sequence(&account_id.to_string())
        .await?
        + 1;

    account.apply(&event);
    account_repository
        .save(&account, vec![event.clone()])
        .await?;

    event_publisher.publish(EventEnvelope::new(
        account_id.to_string(),
        AggregateType::Account,
        sequence_number,
        event,
    ));
    Ok(())
}

/// Returns the path or name to use as the icon for desktop notifications.
///
/// Prefers the installed hicolor path (`~/.local/share/icons/hicolor/128x128/apps/mail-cat.png`)
/// because libnotify accepts absolute paths directly and renders them at full
/// fidelity regardless of whether `gtk-update-icon-cache` has run.
/// Falls back to the theme name `"mail-cat"` (works after `just install`).
fn app_icon_path() -> String {
    if let Ok(home) = std::env::var("HOME") {
        let path = format!("{home}/.local/share/icons/hicolor/128x128/apps/mail-cat.png");
        if std::path::Path::new(&path).exists() {
            return path;
        }
    }
    "mail-cat".to_owned()
}

#[cfg(test)]
mod tests;
