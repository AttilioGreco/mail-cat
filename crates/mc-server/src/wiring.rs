//! Dependency injection and application wiring.

use anyhow::Context;
use std::sync::Arc;

use mc_application::bus::{InProcessCommandBus, InProcessEventBus, InProcessQueryBus};
use mc_application::projections::ProjectionEventHandler;
use mc_application::{SyncConfig, SyncService};
use mc_infrastructure::{ImapEmailProvider, SledEventStore, SledRepository, TantivySearchIndex};
use mc_interfaces::grpc::services::{
    AccountServiceImpl, MailboxServiceImpl, MessageServiceImpl, SearchServiceImpl,
};

use crate::config::ServerConfig;

// Type aliases for concrete bus implementations
type CommandBusImpl = InProcessCommandBus<
    SledRepository<SledEventStore>,
    SledEventStore,
    ImapEmailProvider,
    InProcessEventBus,
>;
type QueryBusImpl = InProcessQueryBus<
    SledRepository<SledEventStore>,
    SledRepository<SledEventStore>,
    SledRepository<SledEventStore>,
    TantivySearchIndex,
>;
type SyncServiceImpl =
    SyncService<ImapEmailProvider, SledEventStore, SledRepository<SledEventStore>>;

/// Application context with all wired dependencies.
pub struct AppContext {
    pub account_service: AccountServiceImpl<CommandBusImpl, QueryBusImpl>,
    pub mailbox_service: MailboxServiceImpl<QueryBusImpl>,
    pub message_service: MessageServiceImpl<CommandBusImpl, QueryBusImpl>,
    pub search_service: SearchServiceImpl<QueryBusImpl>,
    #[allow(dead_code)]
    pub sync_service: Arc<SyncServiceImpl>,
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

        // Create email provider (using first account config for now)
        // TODO: Support multiple accounts with a provider factory
        let email_provider = if let Some(account_config) = config.accounts.first() {
            tracing::info!("Initializing IMAP provider for {}", account_config.email);
            Arc::new(ImapEmailProvider::new(
                account_config.imap_host.clone(),
                account_config.imap_port,
                account_config.username.clone(),
                account_config.password.clone(),
                account_config.use_tls,
                account_config.smtp_host.clone(),
                account_config.smtp_port,
                account_config.use_tls, // Use same TLS setting for SMTP
            ))
        } else {
            tracing::warn!("No email accounts configured, using mock provider");
            // For now, create a dummy provider (in production this would error)
            Arc::new(ImapEmailProvider::new(
                "localhost".to_string(),
                993,
                "user".to_string(),
                "pass".to_string(),
                false,
                "smtp.localhost".to_string(),
                587,
                false,
            ))
        };

        // ── Application Layer ───────────────────────────────────────────────

        // Create event bus (capacity: max 1000 events in flight)
        let event_bus = Arc::new(InProcessEventBus::new(1000));
        tracing::info!("Initialized event bus");

        // Create command bus
        let command_bus = Arc::new(InProcessCommandBus::new(
            repository_arc.clone(),
            event_store.clone(),
            email_provider.clone(),
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
            eager_body_fetch: false,
        };
        let sync_service = Arc::new(SyncService::new(
            email_provider.clone(),
            event_store.clone(),
            repository_arc.clone(),
            sync_config,
        ));
        tracing::info!("Initialized sync service");

        // Create projection event handler and start catching up
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

        // ── Interface Layer ─────────────────────────────────────────────────

        // Create gRPC services
        let account_service = AccountServiceImpl::new(command_bus.clone(), query_bus.clone());

        let mailbox_service = MailboxServiceImpl::new(query_bus.clone());

        let message_service = MessageServiceImpl::new(command_bus.clone(), query_bus.clone());

        let search_service = SearchServiceImpl::new(query_bus.clone());

        tracing::info!("All services initialized successfully");

        Ok(Self {
            account_service,
            mailbox_service,
            message_service,
            search_service,
            sync_service,
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

        // Verify AppContext can be created with wired SyncService
        let context = match AppContext::new(&config).await {
            Ok(ctx) => ctx,
            Err(e) => panic!("failed to create app context: {}", e),
        };

        // Test that SyncService is available and callable
        let account_id = mc_domain::AccountId::new();
        let result = context.sync_service.start_sync(&account_id).await;

        // The actual sync will fail because ImapEmailProvider is not fully
        // implemented (pending TLS integration), but this verifies that:
        // 1. SyncService is properly wired in AppContext
        // 2. Error handling works (SyncFailed error type is used)
        match result {
            Ok(_events) => {
                // If this succeeds in the future when ImapEmailProvider is complete,
                // we'll verify progress events are emitted
            }
            Err(e) => {
                // Verify that the error is the expected SyncFailed type
                assert!(
                    e.to_string().contains("sync failed"),
                    "error should be SyncFailed: {}",
                    e
                );
            }
        }
    }
}
