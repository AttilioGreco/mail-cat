//! Dependency injection and application wiring.

use anyhow::Context;
use chrono::Utc;
use futures::stream::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use mc_application::bus::{InProcessCommandBus, InProcessEventBus, InProcessQueryBus};
use mc_application::{CommandBus, QueryBus};
use mc_application::projections::{ProjectionEventHandler, SearchProjectionHandler};
use mc_domain::ReadModelProjector;
use mc_application::{OAuthLoginTrigger, SyncConfig, SyncService, SyncTrigger};
use mc_domain::{
    Account, AccountEventSource, AccountId, AccountReadRepository, AccountWriteRepository,
    AuthConfig, ConnectionConfig, EmailAddress, Identity, MailboxId, MailboxPath,
    ProviderNotification, ProviderRegistry, ProviderType, ProviderUid,
};
use mc_domain::model::account::{SmtpConnectionConfig, SmtpSecurity};
use mc_infrastructure::{
    email::imap::ImapCredential,
    email::smtp::SmtpSender,
    ImapAccountEventSource, ImapEmailProvider, InMemoryProviderRegistry, SqliteDb,
    SqliteEventStore, SqliteRepository, TantivySearchIndex,
};
use mc_interfaces::grpc::services::{
    AccountServiceImpl, ContactServiceImpl, MailboxServiceImpl, MessageServiceImpl, SearchServiceImpl,
};

use crate::config::ServerConfig;
use crate::oauth_login::OAuthChallengeManager;
use crate::oauth_refresh::{self, OAuthRuntimeState};

// Type aliases for concrete implementations
type ProviderRegistryImpl = InMemoryProviderRegistry<ImapEmailProvider>;
type CommandBusImpl = InProcessCommandBus<
    SqliteRepository,               // AW
    SqliteRepository,               // CW
    SqliteEventStore,               // ES
    ProviderRegistryImpl,           // PR
    SqliteRepository,               // MR
    InProcessEventBus,              // B
>;
type QueryBusImpl = InProcessQueryBus<
    SqliteRepository,               // AR
    SqliteRepository,               // MBR
    SqliteRepository,               // MR
    TantivySearchIndex,             // SI
    SqliteRepository,               // CTR
    SqliteRepository,               // CR
>;
type SyncServiceImpl =
    SyncService<ProviderRegistryImpl, SqliteEventStore, SqliteRepository>;

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
        let bare_repository = SqliteRepository::new(db, event_store.clone());
        let repository_arc = Arc::new(bare_repository.clone());
        tracing::info!("Initialized repositories");

        // Create search index
        let search_index = Arc::new(
            TantivySearchIndex::new(&config.index_path).context("failed to create search index")?,
        );
        tracing::info!("Initialized search index at {}", config.index_path);

        // Create provider registry and build providers from config
        let provider_registry = Arc::new(InMemoryProviderRegistry::new());
        let mut providers_by_email: HashMap<String, Arc<ImapEmailProvider>> = HashMap::new();
        let mut oauth_cells_by_email: HashMap<
            String,
            (
                Arc<RwLock<String>>,
                Arc<RwLock<String>>,
                Arc<RwLock<chrono::DateTime<Utc>>>,
                mc_domain::OAuthProvider,
            ),
        > = HashMap::new();
        for account_config in &config.accounts {
            tracing::info!("Creating IMAP provider for {}", account_config.email);

            // Resolve secrets from the OS keychain (plaintext was migrated out
            // of the config file at startup by secret_ingest).
            let secrets = secret_store
                .load(&account_config.email)
                .with_context(|| format!("reading keychain secrets for {}", account_config.email))?
                .unwrap_or_default();
            let imap_password = secrets.imap_password.clone().unwrap_or_default();
            // OAuth accounts authenticate with a token, not a password — don't
            // warn about a missing keychain password for them (it's never set).
            if imap_password.is_empty() && account_config.oauth_provider.is_none() {
                tracing::warn!(
                    "⚠ {}: nessuna password nel keychain di sistema — inserisci `password = \"...\"` in config.toml per importarla",
                    account_config.email
                );
            }

            // ── Resolve credential: OAuth2 token cell or plain password ────
            let credential = if let Some(oauth_cfg) = account_config.oauth_provider {
                let oauth_secrets = secrets.oauth.clone().unwrap_or_else(|| {
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
                let access_token = Arc::new(RwLock::new(oauth_secrets.access_token.clone()));
                let refresh_token = Arc::new(RwLock::new(oauth_secrets.refresh_token.clone()));
                let expires_at = Arc::new(RwLock::new(oauth_secrets.expires_at));
                oauth_cells_by_email.insert(
                    account_config.email.clone(),
                    (access_token.clone(), refresh_token, expires_at, oauth_cfg.into()),
                );
                ImapCredential::OAuthToken(access_token)
            } else {
                ImapCredential::Password(imap_password.clone())
            };

            // ── Build SMTP sender (optional) ──────────────────────────────
            let smtp_sender = if let Some(smtp_cfg) = &account_config.smtp {
                let domain_smtp = SmtpConnectionConfig {
                    host: smtp_cfg.host.clone(),
                    port: smtp_cfg.port,
                    security: match smtp_cfg.security {
                        crate::config::SmtpSecurityConfig::Smtps => SmtpSecurity::Smtps,
                        crate::config::SmtpSecurityConfig::StartTls => SmtpSecurity::StartTls,
                    },
                    username: smtp_cfg.username.clone(),
                    password: secrets.smtp_password.clone(),
                };
                let sender_result = match &credential {
                    ImapCredential::OAuthToken(token) => {
                        Ok(SmtpSender::build_oauth(&domain_smtp, &account_config.username, token.clone()))
                    }
                    ImapCredential::Password(_) => {
                        SmtpSender::build(&domain_smtp, &account_config.username, &imap_password)
                    }
                };
                match sender_result {
                    Ok(sender) => {
                        match sender.test_connection().await {
                            Ok(true) => tracing::info!(
                                "✓ SMTP {} ({}:{}) connessione OK",
                                account_config.email, smtp_cfg.host, smtp_cfg.port
                            ),
                            Ok(false) => tracing::warn!(
                                "⚠ SMTP {}: server raggiungibile ma NOOP fallito",
                                account_config.email
                            ),
                            Err(e) => tracing::warn!(
                                "⚠ SMTP {}: non raggiungibile — {e}",
                                account_config.email
                            ),
                        }
                        Some(sender)
                    }
                    Err(e) => {
                        tracing::warn!(
                            "⚠ SMTP {}: configurazione non valida — {e}",
                            account_config.email
                        );
                        None
                    }
                }
            } else {
                tracing::warn!(
                    "⚠ SMTP {}: nessun blocco [smtp] in config — invio disabilitato",
                    account_config.email
                );
                None
            };

            let provider = Arc::new(ImapEmailProvider::new(
                account_config.imap_host.clone(),
                account_config.imap_port,
                account_config.username.clone(),
                credential,
                account_config.use_tls,
                smtp_sender,
            ));
            providers_by_email.insert(account_config.email.clone(), provider);
        }
        tracing::info!(
            "Created {} email provider(s) from config",
            providers_by_email.len()
        );

        // ── Application Layer ───────────────────────────────────────────────

        // Create event bus (capacity: max 1000 events in flight)
        let event_bus = Arc::new(InProcessEventBus::new(1000));
        tracing::info!("Initialized event bus");

        // Create command bus
        let command_bus = Arc::new(InProcessCommandBus::new(
            repository_arc.clone(),    // AW
            repository_arc.clone(),    // CW
            event_store.clone(),       // ES
            provider_registry.clone(), // PR
            repository_arc.clone(),    // MR
            event_bus.clone(),         // B
        ));
        tracing::info!("Initialized command bus");

        // Create query bus
        let query_bus = Arc::new(InProcessQueryBus::new(
            repository_arc.clone(),    // AR
            repository_arc.clone(),    // MBR
            repository_arc.clone(),    // MR
            search_index.clone(),      // SI
            repository_arc.clone(),    // CTR
            repository_arc.clone(),    // CR
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
            body_backfill_per_minute: config.body_backfill_per_minute,
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

        // Create projection event handlers and start catching up
        let live_projector = bare_repository.clone();
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
        let existing_emails: HashSet<String> = existing_accounts
            .iter()
            .map(|a| a.email.as_str().to_string())
            .collect();

        let mut registered_ids: Vec<(AccountId, String)> = Vec::new();
        let mut oauth_states: Vec<OAuthRuntimeState> = Vec::new();
        let mut created_new = false;
        // Per-account backfill rate override (falls back to the global default).
        let mut backfill_rates: HashMap<AccountId, u32> = HashMap::new();

        for account_config in &config.accounts {
            let provider = providers_by_email
                .remove(&account_config.email)
                .expect("provider must exist for config account");

            if existing_emails.contains(&account_config.email) {
                // Already in DB — just register the provider
                let summary = existing_accounts
                    .iter()
                    .find(|a| a.email.as_str() == account_config.email)
                    .unwrap();
                provider_registry.register_provider(summary.id, provider);
                tracing::info!(
                    "Registered provider for existing account '{}' ({})",
                    summary.name,
                    summary.email
                );
                registered_ids.push((summary.id, summary.name.clone()));
                if let Some((access_token, refresh_token, expires_at, provider)) =
                    oauth_cells_by_email.remove(&account_config.email)
                {
                    oauth_states.push(OAuthRuntimeState {
                        account_id: summary.id,
                        name: summary.name.clone(),
                        email: account_config.email.clone(),
                        provider,
                        access_token,
                        refresh_token,
                        expires_at,
                        needs_reauth: Arc::new(RwLock::new(
                            summary.status == mc_domain::AccountStatus::NeedsReauth,
                        )),
                    });
                }
                backfill_rates.insert(
                    summary.id,
                    account_config
                        .body_backfill_per_minute
                        .unwrap_or(config.body_backfill_per_minute),
                );
            } else {
                // Not in DB — create via aggregate + save
                let email =
                    EmailAddress::new(&account_config.email).context("invalid email in config")?;
                let provider_type: ProviderType = account_config.provider_type.into();
                let smtp_domain_config = account_config.smtp.as_ref().map(|s| SmtpConnectionConfig {
                    host: s.host.clone(),
                    port: s.port,
                    security: match s.security {
                        crate::config::SmtpSecurityConfig::Smtps => SmtpSecurity::Smtps,
                        crate::config::SmtpSecurityConfig::StartTls => SmtpSecurity::StartTls,
                    },
                    username: s.username.clone(),
                    // Secret lives in the keychain, not the persisted aggregate
                    // (AuthConfig secret fields are #[serde(skip)]).
                    password: None,
                });
                let connection_config = ConnectionConfig {
                    imap_host: Some(account_config.imap_host.clone()),
                    imap_port: Some(account_config.imap_port),
                    use_tls: account_config.use_tls,
                    auth: AuthConfig::Plain {
                        username: account_config.username.clone(),
                        password: String::new(), // resolved from keychain at runtime
                    },
                    smtp: smtp_domain_config,
                    smtp_host: None,
                    smtp_port: None,
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
                    provider_type,
                    connection_config,
                    identity,
                );
                let account_id = account.id;

                repository_arc
                    .save(&account, vec![event])
                    .await
                    .context("failed to auto-create account")?;

                provider_registry.register_provider(account_id, provider);
                tracing::info!(
                    "Auto-created account '{}' ({}) and registered provider",
                    account_config.name,
                    account_config.email
                );
                registered_ids.push((account_id, account_config.name.clone()));
                if let Some((access_token, refresh_token, expires_at, provider)) =
                    oauth_cells_by_email.remove(&account_config.email)
                {
                    oauth_states.push(OAuthRuntimeState {
                        account_id,
                        name: account_config.name.clone(),
                        email: account_config.email.clone(),
                        provider,
                        access_token,
                        refresh_token,
                        expires_at,
                        needs_reauth: Arc::new(RwLock::new(false)),
                    });
                }
                backfill_rates.insert(
                    account_id,
                    account_config
                        .body_backfill_per_minute
                        .unwrap_or(config.body_backfill_per_minute),
                );
                created_new = true;
            }
        }

        // The port every OAuth login goes through — notification button, UI
        // button, add-account dialog. It owns pushing a fresh token back into the
        // live runtime state, so no caller has to remember to.
        let oauth_trigger = Arc::new(crate::oauth_reauth::ReauthTrigger::new(
            oauth_challenges,
            oauth_states.clone(),
            secret_store.clone(),
            event_store.clone() as Arc<dyn mc_domain::EventStore>,
            repository_arc.clone() as Arc<dyn mc_domain::AccountWriteRepository>,
            event_bus.clone() as Arc<dyn mc_application::EventPublisher>,
        )) as Arc<dyn OAuthLoginTrigger>;

        // ── Re-fire re-auth notification for accounts still needing it ──────────
        // On a fresh boot, needs_reauth is seeded from the persisted read-model
        // status above — so an account that died before the user re-authenticated
        // (e.g. server restarted mid-outage) gets its "Ri-autentica" notification
        // reissued immediately instead of staying silently orphaned.
        for state in &oauth_states {
            if *state.needs_reauth.read().await {
                spawn_reauth_notification(
                    state.clone(),
                    "Autenticazione scaduta — accedi di nuovo".to_owned(),
                    oauth_trigger.clone(),
                );
            }
        }

        // ── Live projection subscriber ───────────────────────────────────────
        // Subscribe the read-model projector to the event bus so that domain
        // events emitted at runtime (e.g. ApplyRemoteFlagChange → MessageFlagged)
        // are immediately reflected in the read model without waiting for
        // the next server restart.
        {
            let mut sub = event_bus.subscribe();
            let live_projector = live_projector.clone();
            let search = search_handler.clone();
            let sd = shutdown.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = sd.cancelled() => break,
                        envelope = sub.recv() => match envelope {
                            Some(e) => {
                                // Read model first, then search projection reads
                                // the now-current read model for this message.
                                if let Err(err) = live_projector.project(&e).await {
                                    tracing::warn!("Live projection error for {}: {}", e.payload.event_name(), err);
                                }
                                if let Err(err) = search.apply_live(&e).await {
                                    tracing::warn!("Live search projection error for {}: {}", e.payload.event_name(), err);
                                }
                            }
                            None => break,
                        },
                    }
                }
                tracing::info!("Live projection subscriber exited");
            });
            tracing::info!("Live projection subscriber started");
        }

        // ── Auto-populate contacts from incoming messages ────────────────────
        {
            let mut sub = event_bus.subscribe();
            let ensure_bus = command_bus.clone();
            let sd = shutdown.clone();
            tokio::spawn(async move {
                loop {
                    let envelope = tokio::select! {
                        _ = sd.cancelled() => break,
                        e = sub.recv() => match e { Some(v) => v, None => break },
                    };
                    if let mc_domain::DomainEvent::MessageReceived {
                        account_id,
                        envelope: mail_envelope,
                        ..
                    } = envelope.payload
                    {
                        let addrs: Vec<(mc_domain::EmailAddress, Option<String>)> =
                            mail_envelope.from.iter()
                                .chain(mail_envelope.to.iter())
                                .chain(mail_envelope.cc.iter())
                                .map(|a| (a.email.clone(), a.name.clone()))
                                .collect();
                        for (email, name) in addrs {
                            let cmd = mc_application::Command::EnsureContact {
                                email,
                                display_name: name.filter(|s| !s.is_empty()),
                                account_id,
                            };
                            if let Err(e) = ensure_bus.dispatch(cmd).await {
                                tracing::warn!("EnsureContact from message failed: {e}");
                            }
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
        //   • MessageFlagged(Seen) / MessageMoved / MessageDeleted – dismiss any
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
            let notif_cmd_bus =
                command_bus.clone() as Arc<dyn mc_application::bus::CommandBus>;
            let notif_q_bus = query_bus.clone() as Arc<dyn mc_application::bus::QueryBus>;
            let notif_ui_bin = config.ui_bin.clone();
            let center = crate::notifications::NotificationCenter::connect().await;
            center.clone().spawn_action_loop(
                notif_cmd_bus.clone(),
                notif_q_bus.clone(),
                notif_ui_bin.clone(),
                shutdown.clone(),
            );
            let notif_oauth_states = oauth_states.clone();
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
                            ..
                        } => {
                            if flags.contains(&mc_domain::Flag::Seen) {
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
                        mc_domain::DomainEvent::MessageMoved { message_id, .. }
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
                            let Some(state) = notif_oauth_states
                                .iter()
                                .find(|s| s.account_id == account_id)
                                .cloned()
                            else {
                                tracing::warn!(
                                    "AccountAuthExpired for unknown account {}",
                                    account_id.as_uuid()
                                );
                                continue;
                            };
                            spawn_reauth_notification(
                                state,
                                error,
                                notif_oauth_trigger.clone(),
                            );
                        }
                        _ => {}
                    }
                }
                tracing::info!("Notification subscriber exited");
            });
            tracing::info!("Desktop notification subscriber started");
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

        // ── Auto-sync registered accounts in background ─────────────────
        // After each sync, run the search-index catch-up: sync appends events in
        // bulk and the live bus drops under that load, so this lossless replay
        // from the event store is what makes synced messages searchable.
        // ponytail: concurrent per-account catch-ups are idempotent (writer is
        // mutex-guarded); redundant overlap is fine, no coordination needed.
        for (account_id, name) in &registered_ids {
            let sync = sync_service.clone();
            let id = *account_id;
            let account_name = name.clone();
            let search = search_handler.clone();
            let es = event_store.clone();
            tokio::spawn(async move {
                match sync.start_sync(&id).await {
                    Ok(events) => {
                        tracing::info!(
                            "Background sync done for '{}': {} event(s)",
                            account_name,
                            events.len()
                        );
                    }
                    Err(e) => {
                        tracing::error!("Background sync failed for '{}': {}", account_name, e);
                    }
                }
                if let Err(e) = search.catch_up(es.as_ref()).await {
                    tracing::warn!("Search index catch-up after sync of '{}' failed: {}", account_name, e);
                }
            });
        }
        if !registered_ids.is_empty() {
            tracing::info!(
                "Launched background sync for {} account(s)",
                registered_ids.len()
            );
        }

        // ── Periodic re-sync (every 5 minutes) ──────────────────────────────
        if !registered_ids.is_empty() {
            let sync_periodic = sync_service.clone();
            let ids: Vec<(AccountId, String)> = registered_ids.clone();
            let sd = shutdown.clone();
            let search = search_handler.clone();
            let es = event_store.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5 * 60));
                interval.tick().await; // skip immediate first tick (initial sync already running)
                // A rejected credential never recovers on its own, and Gmail keeps an
                // account throttled for as long as we keep knocking every 5 minutes.
                let mut auth_backoff: HashMap<AccountId, (u32, tokio::time::Instant)> =
                    HashMap::new();
                loop {
                    tokio::select! {
                        _ = sd.cancelled() => break,
                        _ = interval.tick() => {}
                    }
                    tracing::info!("Periodic re-sync triggered for {} account(s)", ids.len());
                    for (account_id, name) in &ids {
                        if auth_backoff
                            .get(account_id)
                            .is_some_and(|&(_, retry_at)| tokio::time::Instant::now() < retry_at)
                        {
                            continue;
                        }
                        match sync_periodic.start_sync(account_id).await {
                            Err(
                                e @ mc_application::ApplicationError::Domain(
                                    mc_domain::DomainError::AuthenticationFailed(_),
                                ),
                            ) => {
                                let failures =
                                    auth_backoff.get(account_id).map_or(0, |&(f, _)| f) + 1;
                                // 10, 20, 40 minutes, then hourly.
                                let delay = tokio::time::Duration::from_secs(
                                    ((5 * 60) << failures.min(4)).min(3600),
                                );
                                auth_backoff.insert(
                                    *account_id,
                                    (failures, tokio::time::Instant::now() + delay),
                                );
                                tracing::error!(
                                    "Periodic sync for '{}' rejected credentials (attempt {}), next try in {}m: {}",
                                    name,
                                    failures,
                                    delay.as_secs() / 60,
                                    e
                                );
                            }
                            Ok(events) => {
                                auth_backoff.remove(account_id);
                                tracing::info!(
                                    "Periodic sync done for '{}': {} event(s)",
                                    name,
                                    events.len()
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Periodic sync failed for '{}': {}",
                                    name,
                                    e
                                );
                            }
                        }
                    }
                    // Heal the search index from anything the bus dropped during
                    // the bulk sync above (checkpoint-driven, incremental).
                    if let Err(e) = search.catch_up(es.as_ref()).await {
                        tracing::warn!("Periodic search index catch-up failed: {e}");
                    }
                }
                tracing::info!("Periodic re-sync timer stopped");
            });
            tracing::info!("Periodic re-sync timer started (every 5 minutes)");
        }

        // ── Periodic OAuth token refresh (piggybacks the same 5-minute cadence) ──
        oauth_refresh::spawn_refresh_loop(
            oauth_states,
            secret_store.clone(),
            event_store.clone() as Arc<dyn mc_domain::EventStore>,
            repository_arc.clone() as Arc<dyn mc_domain::AccountWriteRepository>,
            event_bus.clone() as Arc<dyn mc_application::EventPublisher>,
            shutdown.clone(),
        );

        // ── Background body backfill ─────────────────────────────────────────
        // The initial sync only pulls envelopes; message bodies are downloaded
        // here, newest-first, throttled to `body_backfill_per_minute` per account
        // so a large mailbox doesn't burst the IMAP server / Gmail's daily cap.
        // Each fetch indexes the body for search and dequeues itself.
        for (account_id, _name) in &registered_ids {
            let sync = Arc::clone(&sync_service);
            let id = *account_id;
            let sd = shutdown.clone();
            let rate = backfill_rates
                .get(account_id)
                .copied()
                .unwrap_or(config.body_backfill_per_minute);
            tokio::spawn(async move {
                sync.run_backfill(id, rate, sd).await;
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
        for (account_id, name) in &registered_ids {
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
                let mut stream = event_source.watch(&[inbox_path.clone()]);

                let sync = Arc::clone(&sync_service);
                let cmd_bus = command_bus.clone();
                let id = *account_id;
                let account_name = name.clone();
                let sd = shutdown.clone();
                let watch_repository =
                    repository_arc.clone() as Arc<dyn mc_domain::AccountWriteRepository>;
                let watch_event_store = event_store.clone() as Arc<dyn mc_domain::EventStore>;
                let watch_event_publisher =
                    event_bus.clone() as Arc<dyn mc_application::EventPublisher>;

                // Semaphore(1): at most one triggered sync in flight per account.
                let sync_gate = Arc::new(tokio::sync::Semaphore::new(1));

                tokio::spawn(async move {
                    tracing::info!("[Watcher] Started INBOX IDLE for '{}'", account_name);

                    // The watcher keeps retrying (with backoff) after a credential
                    // rejection, but the account must be flagged only once — otherwise
                    // every retry appends another AccountAuthExpired and re-fires the
                    // desktop notification.
                    let mut auth_reported = false;

                    loop {
                    let result = tokio::select! {
                        _ = sd.cancelled() => break,
                        item = stream.next() => match item { Some(v) => v, None => break },
                    };
                    {
                        match result {
                            Ok(notification) => match notification {
                                // ── New message ──────────────────────────────
                                // Targeted partial sync: fetch only from the
                                // notified UID onward in this specific mailbox.
                                // Emits MessageReceived + MailboxCountsUpdated
                                // and advances the checkpoint so start_sync
                                // won't re-fetch on the next periodic run.
                                ProviderNotification::NewMessage {
                                    ref mailbox,
                                    ref uid,
                                } => {
                                    if let Ok(permit) = sync_gate.clone().try_acquire_owned() {
                                        let sync_clone = Arc::clone(&sync);
                                        let id_clone = id;
                                        let mailbox_clone = mailbox.clone();
                                        let from_uid = *uid;
                                        tokio::spawn(async move {
                                            let _permit = permit;
                                            if let Err(e) = sync_clone
                                                .sync_mailbox(&id_clone, &mailbox_clone, from_uid)
                                                .await
                                            {
                                                tracing::warn!(
                                                    "[Watcher] Partial sync failed: {e}"
                                                );
                                            }
                                        });
                                    }
                                }

                                // ── Count changed (no UID) ────────────────────
                                // Fallback: let the checkpoint decide what to fetch.
                                // ProviderUid(0) → fetch_from = max(0, checkpoint) = checkpoint.
                                ProviderNotification::MailboxCountChanged {
                                    ref mailbox, ..
                                } => {
                                    if let Ok(permit) = sync_gate.clone().try_acquire_owned() {
                                        let sync_clone = Arc::clone(&sync);
                                        let id_clone = id;
                                        let mailbox_clone = mailbox.clone();
                                        tokio::spawn(async move {
                                            let _permit = permit;
                                            if let Err(e) = sync_clone
                                                .sync_mailbox(
                                                    &id_clone,
                                                    &mailbox_clone,
                                                    ProviderUid::new(0),
                                                )
                                                .await
                                            {
                                                tracing::warn!(
                                                    "[Watcher] Count-triggered sync failed: {e}"
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
                                    ref uid,
                                    ref flags,
                                } if uid.value() > 0 => {
                                    if let Some(mb_id) = path_to_id.get(&mailbox.full_path()) {
                                        let cmd = mc_application::Command::ApplyRemoteFlagChange {
                                            mailbox_id: *mb_id,
                                            provider_uid: mc_application::commands::ProviderUid::new(
                                                uid.value(),
                                            ),
                                            new_flags: flags.clone(),
                                        };
                                        if let Err(e) = cmd_bus.dispatch(cmd).await {
                                            tracing::warn!(
                                                "[Watcher] ApplyRemoteFlagChange uid={}: {e}",
                                                uid.value()
                                            );
                                        }
                                    }
                                }

                                // uid=0 FlagsChanged are spurious IMAP EXISTS echoes — ignore.
                                ProviderNotification::FlagsChanged { .. } => {}
                            },
                            // A rejected credential never recovers on its own. Flag the
                            // account so it surfaces in the UI and drives the re-auth
                            // flow, instead of rotting behind a WARN line forever.
                            Err(mc_domain::DomainError::AuthenticationFailed(msg)) => {
                                if auth_reported {
                                    continue;
                                }
                                auth_reported = true;
                                tracing::error!(
                                    "[Watcher] '{}' rejected our credential ({msg}) — \
                                     flagging account, backing off",
                                    account_name
                                );
                                if let Err(e) = crate::oauth_refresh::append_account_event(
                                    id,
                                    mc_domain::DomainEvent::AccountAuthExpired {
                                        account_id: id,
                                        error: msg,
                                    },
                                    &watch_event_store,
                                    &watch_repository,
                                    &watch_event_publisher,
                                )
                                .await
                                {
                                    tracing::error!(
                                        "[Watcher] '{}': failed recording AccountAuthExpired: {e}",
                                        account_name
                                    );
                                }
                            }
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
        if !registered_ids.is_empty() {
            tracing::info!(
                "Started server-side mailbox watchers for {} account(s)",
                registered_ids.len()
            );
        }

        // ── Interface Layer ─────────────────────────────────────────────────

        // Create gRPC services
        let account_service = AccountServiceImpl::new(command_bus.clone(), query_bus.clone())
            .with_sync_trigger(sync_service.clone() as Arc<dyn SyncTrigger>)
            .with_oauth_trigger(oauth_trigger.clone())
            .with_provisioner(Arc::new(
                crate::account_provisioner::ConfigAccountProvisioner::new(
                    std::path::PathBuf::from(config_path),
                    secret_store.clone(),
                ),
            ) as Arc<dyn mc_application::AccountProvisioner>);

        let mailbox_service = MailboxServiceImpl::new(query_bus.clone());

        let message_service = MessageServiceImpl::new(
            command_bus.clone(),
            query_bus.clone(),
            event_bus.clone(),
        );

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
    state: OAuthRuntimeState,
    error: String,
    trigger: Arc<dyn OAuthLoginTrigger>,
) {
    tokio::task::spawn_blocking(move || {
        let handle = match notify_rust::Notification::new()
            .summary(&format!("{} richiede una nuova autenticazione", state.email))
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
                let mut progress = trigger.start_reauth(state.account_id);
                while let Some(update) = progress.recv().await {
                    match update {
                        mc_application::OAuthLoginProgress::AwaitingBrowser {
                            authorize_url,
                        } => {
                            if let Err(e) =
                                mc_shared::browser::open_browser(&authorize_url)
                            {
                                tracing::warn!(
                                    "Failed to open browser for re-auth ({e}) — \
                                     apri manualmente: {authorize_url}"
                                );
                            }
                        }
                        mc_application::OAuthLoginProgress::Succeeded { .. } => {
                            let _ = notify_rust::Notification::new()
                                .summary(&format!("{} ri-autenticato", state.email))
                                .body("Account riattivato con successo.")
                                .icon("dialog-information")
                                .show();
                        }
                        mc_application::OAuthLoginProgress::Failed { message } => {
                            tracing::warn!(
                                "Re-auth failed for {}: {message}",
                                state.email
                            );
                            let _ = notify_rust::Notification::new()
                                .summary(&format!(
                                    "Ri-autenticazione fallita per {}",
                                    state.email
                                ))
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
mod tests {
    use super::*;

    #[tokio::test]
    async fn sync_service_integrated_in_app_context() {
        let dir = tempfile::tempdir().unwrap();
        let config = ServerConfig {
            grpc_addr: "127.0.0.1:50051".to_string(),
            db_path: dir.path().join("mail-cat.db").display().to_string(),
            index_path: dir.path().join("index").display().to_string(),
            accounts: vec![],
            ui_bin: "mail-cat-ui".to_string(),
            body_backfill_per_minute: 30,
            log_level: None,
        };

        // Verify AppContext can be created with wired dependencies.
        // No accounts → the secret store is never queried.
        let secret_store: Arc<dyn mc_domain::SecretStore> =
            Arc::new(mc_infrastructure::KeyringSecretStore::new());
        let oauth_challenges = Arc::new(crate::oauth_login::OAuthChallengeManager::new(
            std::path::PathBuf::from("/tmp/test_config_sync.toml"),
            secret_store.clone(),
        ));
        let context = match AppContext::new(&config, "/tmp/test_config_sync.toml", secret_store, oauth_challenges).await {
            Ok(ctx) => ctx,
            Err(e) => panic!("failed to create app context: {}", e),
        };

        // Test that SyncService is available and callable
        let account_id = mc_domain::AccountId::new();
        let result = context.sync_service.start_sync(&account_id).await;

        // With no providers registered, this should fail with a provider error
        match result {
            Ok(_events) => {
                // If this succeeds in the future with registered providers,
                // we'll verify progress events are emitted
            }
            Err(e) => {
                // Verify that the error mentions provider not found
                let msg = e.to_string();
                assert!(
                    msg.contains("no provider registered") || msg.contains("sync failed"),
                    "unexpected error: {}",
                    e
                );
            }
        }
    }
}
