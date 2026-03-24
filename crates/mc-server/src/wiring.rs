//! Dependency injection and application wiring.

use anyhow::Context;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use mc_application::bus::{InProcessCommandBus, InProcessEventBus, InProcessQueryBus};
use mc_application::projections::ProjectionEventHandler;
use mc_domain::ReadModelProjector;
use mc_application::{SyncConfig, SyncService, SyncTrigger};
use mc_domain::{
    Account, AccountId, AccountReadRepository, AccountWriteRepository, AuthConfig,
    ConnectionConfig, EmailAddress, Identity, ProviderRegistry, ProviderType,
};
use mc_infrastructure::{
    ImapEmailProvider, InMemoryProviderRegistry, SledEventStore, SledRepository, TantivySearchIndex,
};
use mc_interfaces::grpc::services::{
    AccountServiceImpl, MailboxServiceImpl, MessageServiceImpl, SearchServiceImpl,
};

use crate::config::ServerConfig;

// Type aliases for concrete implementations
type ProviderRegistryImpl = InMemoryProviderRegistry<ImapEmailProvider>;
type CommandBusImpl = InProcessCommandBus<
    SledRepository<SledEventStore>,
    SledEventStore,
    ProviderRegistryImpl,
    SledRepository<SledEventStore>,
    InProcessEventBus,
>;
type QueryBusImpl = InProcessQueryBus<
    SledRepository<SledEventStore>,
    SledRepository<SledEventStore>,
    SledRepository<SledEventStore>,
    TantivySearchIndex,
>;
type SyncServiceImpl =
    SyncService<ProviderRegistryImpl, SledEventStore, SledRepository<SledEventStore>, TantivySearchIndex>;

/// Application context with all wired dependencies.
pub struct AppContext {
    pub account_service: AccountServiceImpl<CommandBusImpl, QueryBusImpl>,
    pub mailbox_service: MailboxServiceImpl<QueryBusImpl>,
    pub message_service: MessageServiceImpl<CommandBusImpl, QueryBusImpl, ProviderRegistryImpl>,
    pub search_service: SearchServiceImpl<QueryBusImpl>,
    #[allow(dead_code)]
    pub sync_service: Arc<SyncServiceImpl>,
    #[allow(dead_code)]
    pub provider_registry: Arc<ProviderRegistryImpl>,
}

impl AppContext {
    /// Wire up all dependencies from configuration.
    pub async fn new(config: &ServerConfig) -> anyhow::Result<Self> {
        tracing::info!("Initializing application context...");

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

        // Create provider registry and build providers from config
        let provider_registry = Arc::new(InMemoryProviderRegistry::new());
        let mut providers_by_email: HashMap<String, Arc<ImapEmailProvider>> = HashMap::new();
        for account_config in &config.accounts {
            tracing::info!("Creating IMAP provider for {}", account_config.email);
            let provider = Arc::new(ImapEmailProvider::new(
                account_config.imap_host.clone(),
                account_config.imap_port,
                account_config.username.clone(),
                account_config.password.clone(),
                account_config.use_tls,
                account_config.smtp_host.clone(),
                account_config.smtp_port,
                account_config.use_tls, // Use same TLS setting for SMTP
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
            repository_arc.clone(),
            event_store.clone(),
            provider_registry.clone(),
            repository_arc.clone(),
            event_bus.clone(),
        ));
        tracing::info!("Initialized command bus");

        // Create query bus
        let query_bus = Arc::new(InProcessQueryBus::new(
            repository_arc.clone(),
            repository_arc.clone(),
            repository_arc.clone(),
            search_index.clone(),
        ));
        tracing::info!("Initialized query bus");

        // Create sync service with default configuration
        let sync_config = SyncConfig {
            batch_size: 50,
            max_concurrent_bodies: 4,
            eager_body_fetch: true,
        };
        let sync_service = Arc::new(SyncService::new(
            provider_registry.clone(),
            event_store.clone(),
            repository_arc.clone(),
            search_index.clone(),
            sync_config,
        ));
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
        let existing_accounts = repository_arc.list_accounts().await.unwrap_or_default();
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
                provider_registry.register_provider(summary.id.clone(), provider);
                tracing::info!(
                    "Registered provider for existing account '{}' ({})",
                    summary.name,
                    summary.email
                );
                registered_ids.push((summary.id.clone(), summary.name.clone()));
            } else {
                // Not in DB — create via aggregate + save
                let email =
                    EmailAddress::new(&account_config.email).context("invalid email in config")?;
                let provider_type: ProviderType = account_config.provider_type.into();
                let connection_config = ConnectionConfig {
                    imap_host: Some(account_config.imap_host.clone()),
                    imap_port: Some(account_config.imap_port),
                    smtp_host: Some(account_config.smtp_host.clone()),
                    smtp_port: Some(account_config.smtp_port),
                    use_tls: account_config.use_tls,
                    auth: AuthConfig::Plain {
                        username: account_config.username.clone(),
                        password: account_config.password.clone(),
                    },
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
                let account_id = account.id.clone();

                repository_arc
                    .save(&account, vec![event])
                    .await
                    .context("failed to auto-create account")?;

                provider_registry.register_provider(account_id.clone(), provider);
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
            tokio::spawn(async move {
                while let Some(envelope) = sub.recv().await {
                    if let Err(e) = live_projector.project(&envelope).await {
                        tracing::warn!("Live projection error for {}: {}", envelope.payload.event_name(), e);
                    }
                }
                tracing::info!("Live projection subscriber exited");
            });
            tracing::info!("Live projection subscriber started");
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

        // ── Auto-sync registered accounts in background ─────────────────
        for (account_id, name) in &registered_ids {
            let sync = sync_service.clone();
            let id = account_id.clone();
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
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5 * 60));
                interval.tick().await; // skip immediate first tick (initial sync already running)
                loop {
                    interval.tick().await;
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
            });
            tracing::info!("Periodic re-sync timer started (every 5 minutes)");
        }

        // ── Interface Layer ─────────────────────────────────────────────────

        // Create gRPC services
        let account_service = AccountServiceImpl::new(command_bus.clone(), query_bus.clone())
            .with_sync_trigger(sync_service.clone() as Arc<dyn SyncTrigger>);

        let mailbox_service = MailboxServiceImpl::new(query_bus.clone());

        let message_service = MessageServiceImpl::new(command_bus.clone(), query_bus.clone(), provider_registry.clone());

        let search_service = SearchServiceImpl::new(query_bus.clone());

        tracing::info!("All services initialized successfully");

        Ok(Self {
            account_service,
            mailbox_service,
            message_service,
            search_service,
            sync_service,
            provider_registry,
        })
    }
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
        };

        // Verify AppContext can be created with wired dependencies
        let context = match AppContext::new(&config).await {
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
