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
use mc_application::projections::ProjectionEventHandler;
use mc_domain::ReadModelProjector;
use mc_application::{SyncConfig, SyncService, SyncTrigger};
use mc_domain::{
    Account, AccountEventSource, AccountId, AccountReadRepository, AccountWriteRepository,
    AuthConfig, ConnectionConfig, EmailAddress, Identity, MailboxId, MailboxPath,
    MessageReadRepository, ProviderNotification, ProviderRegistry, ProviderType, ProviderUid,
    SearchIndex,
};
use mc_domain::model::account::{SmtpConnectionConfig, SmtpSecurity};
use mc_infrastructure::{
    email::imap::ImapCredential,
    email::smtp::SmtpSender,
    ImapAccountEventSource, ImapEmailProvider, InMemoryProviderRegistry, SledEventStore,
    SledRepository, TantivyContactIndex, TantivySearchIndex,
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
    SledRepository<SledEventStore>, // AW
    SledRepository<SledEventStore>, // CW
    TantivyContactIndex,            // CI
    SledEventStore,                 // ES
    ProviderRegistryImpl,           // PR
    SledRepository<SledEventStore>, // MR
    InProcessEventBus,              // B
>;
type QueryBusImpl = InProcessQueryBus<
    SledRepository<SledEventStore>, // AR
    SledRepository<SledEventStore>, // MBR
    SledRepository<SledEventStore>, // MR
    TantivySearchIndex,             // SI
    TantivyContactIndex,            // CI
    SledRepository<SledEventStore>, // CR
>;
type SyncServiceImpl =
    SyncService<ProviderRegistryImpl, SledEventStore, SledRepository<SledEventStore>, TantivySearchIndex>;

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
    /// Sled handle for an explicit flush on shutdown — `std::process::exit`
    /// skips `Drop`, so without this, sled's background flush thread can
    /// lose its most recent writes to a SIGTERM landing mid-cycle.
    pub db: sled::Db,
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

        // Open sled database
        let db = sled::open(&config.db_path).context("failed to open sled database")?;
        tracing::info!("Opened sled database at {}", config.db_path);

        // Create event store
        let event_store = Arc::new(SledEventStore::new(db.clone()));
        tracing::info!("Initialized event store");

        // Create repository (implements both write and read repos + projector)
        // We create a bare version for the projector and an Arc-wrapped version for the buses
        let bare_repository = SledRepository::new(db.clone(), event_store.clone());
        let repository_arc = Arc::new(bare_repository.clone());
        tracing::info!("Initialized repositories");

        // Create search index
        let search_index = Arc::new(
            TantivySearchIndex::new(&config.index_path).context("failed to create search index")?,
        );
        tracing::info!("Initialized search index at {}", config.index_path);

        // Create contact index
        let contact_index_path = format!("{}/contacts", config.index_path);
        let contact_index = Arc::new(
            TantivyContactIndex::new(&contact_index_path)
                .context("failed to create contact index")?,
        );
        tracing::info!("Initialized contact index at {contact_index_path}");

        // Create provider registry and build providers from config
        let provider_registry = Arc::new(InMemoryProviderRegistry::new());
        let mut providers_by_email: HashMap<String, Arc<ImapEmailProvider>> = HashMap::new();
        let mut oauth_states: Vec<OAuthRuntimeState> = Vec::new();
        for account_config in &config.accounts {
            tracing::info!("Creating IMAP provider for {}", account_config.email);

            // Resolve secrets from the OS keychain (plaintext was migrated out
            // of the config file at startup by secret_ingest).
            let secrets = secret_store
                .load(&account_config.email)
                .with_context(|| format!("reading keychain secrets for {}", account_config.email))?
                .unwrap_or_default();
            let imap_password = secrets.imap_password.clone().unwrap_or_default();
            if imap_password.is_empty() {
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
                oauth_states.push(OAuthRuntimeState {
                    email: account_config.email.clone(),
                    provider: oauth_cfg.into(),
                    access_token: access_token.clone(),
                    refresh_token,
                    expires_at,
                });
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
            contact_index.clone(),     // CI
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
            contact_index.clone(),     // CI
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
                search_index.clone(),
                sync_config,
            )
            .with_event_publisher(event_bus.clone()),
        );
        tracing::info!("Initialized sync service");

        // Create projection event handler and start catching up
        let live_projector = bare_repository.clone();
        let projection_handler =
            ProjectionEventHandler::new(bare_repository.clone(), bare_repository);
        tracing::info!("Initialized projection handler");

        // Catch up projections on startup (rebuild read models from events)
        tracing::info!("Catching up projections from event store...");
        projection_handler
            .catch_up(event_store.as_ref())
            .await
            .context("failed to catch up projections")?;
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
        let mut created_new = false;

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
                created_new = true;
            }
        }

        // ── Live projection subscriber ───────────────────────────────────────
        // Subscribe the read-model projector to the event bus so that domain
        // events emitted at runtime (e.g. ApplyRemoteFlagChange → MessageFlagged)
        // are immediately reflected in the sled read model without waiting for
        // the next server restart.
        {
            let mut sub = event_bus.subscribe();
            let live_projector = live_projector.clone();
            let sd = shutdown.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = sd.cancelled() => break,
                        envelope = sub.recv() => match envelope {
                            Some(e) => {
                                if let Err(err) = live_projector.project(&e).await {
                                    tracing::warn!("Live projection error for {}: {}", e.payload.event_name(), err);
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
        //   • AccountSyncFailed – error alert (no actions)
        {
            let mut sub = event_bus.subscribe();
            let notif_cmd_bus = command_bus.clone();
            let notif_q_bus = query_bus.clone();
            let notif_ui_bin = config.ui_bin.clone();
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
                            // Capture the tokio handle before entering the blocking task so we
                            // can drive async work (command dispatch, mailbox lookup) from the
                            // synchronous wait_for_action callback.
                            let rt = tokio::runtime::Handle::current();
                            tokio::task::spawn_blocking(move || {
                                let handle = match notify_rust::Notification::new()
                                    .summary(&format!("New message from {from}"))
                                    .body(&subject)
                                    .icon(&app_icon_path())
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
                                handle.wait_for_action(move |action| match action {
                                    "archive" => {
                                        rt.block_on(async move {
                                            // Fetch all mailboxes for the account so we can
                                            // resolve both the source path and the Archive target.
                                            let mailboxes = match q
                                                .dispatch(mc_application::Query::ListMailboxes {
                                                    account_id,
                                                })
                                                .await
                                            {
                                                Ok(mc_application::QueryResult::Mailboxes(m)) => m,
                                                _ => return,
                                            };
                                            let from_mb =
                                                mailboxes.iter().find(|m| m.id == mailbox_id);
                                            let archive_mb = mailboxes.iter().find(|m| {
                                                m.mailbox_type == mc_domain::MailboxType::Archive
                                            });
                                            match (from_mb, archive_mb) {
                                                (Some(src), Some(dst)) => {
                                                    if let Err(e) = cmd
                                                        .dispatch(
                                                            mc_application::Command::MoveMessage {
                                                                message_id,
                                                                from_mailbox_id: src.id,
                                                                to_mailbox_id: dst.id,
                                                                from_mailbox_path: src.path.clone(),
                                                                to_mailbox_path: dst.path.clone(),
                                                            },
                                                        )
                                                        .await
                                                    {
                                                        tracing::warn!(
                                                            "Archive from notification failed: {e}"
                                                        );
                                                    }
                                                }
                                                _ => tracing::warn!(
                                                    "Archive mailbox not found for account {}",
                                                    account_id.as_uuid()
                                                ),
                                            }
                                        });
                                    }
                                    "open" => {
                                        if let Err(e) =
                                            std::process::Command::new(&ui_bin).spawn()
                                        {
                                            tracing::warn!(
                                                "Failed to open Mail Cat (ui_bin='{ui_bin}'): {e}"
                                            );
                                        }
                                    }
                                    _ => {}
                                });
                            });
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
                        _ => {}
                    }
                }
                tracing::info!("Notification subscriber exited");
            });
            tracing::info!("Desktop notification subscriber started");
        }

        // ── Search-index de-index subscriber ─────────────────────────────────
        // The Tantivy index is a projection of domain events. Sync indexes
        // bodies inline (eager_body_fetch=true) and removes them in reconcile,
        // but permanent local deletes go through the command bus, which never
        // owned the index — so they leaked stale search entries. This subscriber
        // closes that one gap. (Indexing stays with sync's reliable bulk path;
        // remove_message is idempotent, so overlapping with reconcile is fine.)
        {
            let mut sub = event_bus.subscribe();
            let search = search_index.clone();
            let sd = shutdown.clone();
            tokio::spawn(async move {
                loop {
                    let envelope = tokio::select! {
                        _ = sd.cancelled() => break,
                        e = sub.recv() => match e { Some(v) => v, None => break },
                    };
                    if let mc_domain::DomainEvent::MessageDeleted { message_id, .. } =
                        &envelope.payload
                        && let Err(e) = search.remove_message(message_id).await
                    {
                        tracing::warn!("Search de-index for {message_id} failed: {e}");
                    }
                }
                tracing::info!("Search de-index subscriber exited");
            });
            tracing::info!("Search de-index subscriber started");
        }

        // Re-run projection catch-up if new accounts were created
        if created_new {
            tracing::info!("Re-running projection catch-up for newly created accounts...");
            projection_handler
                .catch_up(event_store.as_ref())
                .await
                .context("failed to catch up projections after account creation")?;
            tracing::info!("Projections caught up after account creation");
        }

        // ── Rebuild search index from the read model on startup ──────────────
        // The Tantivy index isn't event-sourced with a checkpoint like the read
        // model, and the bus is best-effort (lag-drops), so it can drift from the
        // event store. Since the server is a long-running daemon (restarts are
        // rare), a full rebuild on boot from the now-current read model is the
        // simple, robust reconciliation: it heals lag-dropped de-indexes, orphaned
        // deletes, and any never-indexed messages. `scan_all_messages` returns only
        // live messages (deleted rows are gone from MESSAGES_TREE), so the rebuilt
        // index contains exactly what should be searchable. Non-fatal: a stale
        // index must not block the server from starting.
        match repository_arc.scan_all_messages(None).await {
            Ok(messages) => match search_index.rebuild(messages).await {
                Ok(count) => tracing::info!("Search index rebuilt on startup: {count} message(s)"),
                Err(e) => tracing::warn!("Startup search-index rebuild failed: {e}"),
            },
            Err(e) => tracing::warn!("Startup search-index rebuild: scan failed: {e}"),
        }

        // ── Auto-sync registered accounts in background ─────────────────
        for (account_id, name) in &registered_ids {
            let sync = sync_service.clone();
            let id = *account_id;
            let account_name = name.clone();
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
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5 * 60));
                interval.tick().await; // skip immediate first tick (initial sync already running)
                loop {
                    tokio::select! {
                        _ = sd.cancelled() => break,
                        _ = interval.tick() => {}
                    }
                    tracing::info!("Periodic re-sync triggered for {} account(s)", ids.len());
                    for (account_id, name) in &ids {
                        match sync_periodic.start_sync(account_id).await {
                            Ok(events) => {
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
                }
                tracing::info!("Periodic re-sync timer stopped");
            });
            tracing::info!("Periodic re-sync timer started (every 5 minutes)");
        }

        // ── Periodic OAuth token refresh (piggybacks the same 5-minute cadence) ──
        oauth_refresh::spawn_refresh_loop(oauth_states, secret_store.clone(), shutdown.clone());

        // ── Background body backfill ─────────────────────────────────────────
        // The initial sync only pulls envelopes; message bodies are downloaded
        // here, newest-first, throttled to `body_backfill_per_minute` per account
        // so a large mailbox doesn't burst the IMAP server / Gmail's daily cap.
        // Each fetch indexes the body for search and dequeues itself.
        for (account_id, _name) in &registered_ids {
            let sync = Arc::clone(&sync_service);
            let id = *account_id;
            let sd = shutdown.clone();
            let rate = config.body_backfill_per_minute;
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
            // so that notifications emitted by watch_mailbox match what's in sled.
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
            .with_oauth_trigger(oauth_challenges as Arc<dyn mc_application::OAuthLoginTrigger>);

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
            db,
        })
    }
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
        // Use in-memory database paths (they will be created by sled)
        let config = ServerConfig {
            grpc_addr: "127.0.0.1:50051".to_string(),
            db_path: "/tmp/test_db_sync".to_string(),
            index_path: "/tmp/test_index_sync".to_string(),
            accounts: vec![],
            ui_bin: "mail-cat-ui".to_string(),
            body_backfill_per_minute: 30,
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
