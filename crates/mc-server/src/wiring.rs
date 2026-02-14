//! Dependency injection and application wiring.

use std::sync::Arc;
use anyhow::Context;

use mc_application::bus::{InProcessCommandBus, InProcessEventBus, InProcessQueryBus};
use mc_application::projections::ProjectionEventHandler;
use mc_infrastructure::{ImapEmailProvider, SledEventStore, SledRepository, SharedSledRepository, TantivySearchIndex};
use mc_interfaces::grpc::services::{
    AccountServiceImpl, MailboxServiceImpl, MessageServiceImpl, SearchServiceImpl,
};

use crate::config::ServerConfig;

// Type aliases for concrete bus implementations
type CommandBusImpl = InProcessCommandBus<
    SharedSledRepository<SledEventStore>,
    SledEventStore,
    ImapEmailProvider,
    InProcessEventBus,
>;
type QueryBusImpl = InProcessQueryBus<
    SharedSledRepository<SledEventStore>,
    SharedSledRepository<SledEventStore>,
    SharedSledRepository<SledEventStore>,
    TantivySearchIndex,
>;

/// Application context with all wired dependencies.
pub struct AppContext {
    pub account_service: AccountServiceImpl<CommandBusImpl, QueryBusImpl>,
    pub mailbox_service: MailboxServiceImpl<QueryBusImpl>,
    pub message_service: MessageServiceImpl<CommandBusImpl, QueryBusImpl>,
    pub search_service: SearchServiceImpl<QueryBusImpl>,
}

impl AppContext {
    /// Wire up all dependencies from configuration.
    pub async fn new(config: &ServerConfig) -> anyhow::Result<Self> {
        tracing::info!("Initializing application context...");

        // ── Infrastructure Layer ────────────────────────────────────────────

        // Open sled database
        let db = sled::open(&config.db_path)
            .context("failed to open sled database")?;
        tracing::info!("Opened sled database at {}", config.db_path);

        // Create event store
        let event_store = Arc::new(SledEventStore::new(db.clone()));
        tracing::info!("Initialized event store");

        // Create repository (implements both write and read repos + projector)
        let repository = Arc::new(SledRepository::new(db.clone(), event_store.clone()));
        let shared_repo = SharedSledRepository::new(repository);
        tracing::info!("Initialized repositories");

        // Create search index
        let search_index = Arc::new(
            TantivySearchIndex::new(&config.index_path)
                .context("failed to create search index")?
        );
        tracing::info!("Initialized search index at {}", config.index_path);

        // Create email provider (using first account config for now)
        // TODO: Support multiple accounts with a provider factory
        let email_provider = if let Some(account_config) = config.accounts.first() {
            tracing::info!("Initializing IMAP provider for {}", account_config.email);
            Arc::new(ImapEmailProvider::new(
                account_config.imap_host.clone(),
                account_config.imap_port,
                account_config.use_tls,
                account_config.username.clone(),
                account_config.password.clone(),
                Some((
                    account_config.smtp_host.clone(),
                    account_config.smtp_port,
                )),
            ))
        } else {
            tracing::warn!("No email accounts configured, using mock provider");
            // For now, create a dummy provider (in production this would error)
            Arc::new(ImapEmailProvider::new(
                "localhost".to_string(),
                993,
                false,
                "user".to_string(),
                "pass".to_string(),
                None,
            ))
        };

        // ── Application Layer ───────────────────────────────────────────────

        // Create event bus (capacity: max 1000 events in flight)
        let event_bus = Arc::new(InProcessEventBus::new(1000));
        tracing::info!("Initialized event bus");

        // Create command bus
        let command_bus = Arc::new(InProcessCommandBus::new(
            shared_repo.clone(),
            event_store.clone(),
            email_provider.clone(),
            event_bus.clone(),
        ));
        tracing::info!("Initialized command bus");

        // Create query bus
        let query_bus = Arc::new(InProcessQueryBus::new(
            shared_repo.clone(),
            shared_repo.clone(),
            shared_repo.clone(),
            search_index.clone(),
        ));
        tracing::info!("Initialized query bus");

        // Create projection event handler and start catching up
        let projection_handler = Arc::new(ProjectionEventHandler::new(
            shared_repo.clone(),
            shared_repo.clone(),
        ));
        tracing::info!("Initialized projection handler");

        // Catch up projections on startup (rebuild read models from events)
        tracing::info!("Catching up projections from event store...");
        projection_handler
            .catch_up()
            .await
            .context("failed to catch up projections")?;
        tracing::info!("Projections caught up successfully");

        // ── Interface Layer ─────────────────────────────────────────────────

        // Create gRPC services
        let account_service = AccountServiceImpl::new(
            command_bus.clone(),
            query_bus.clone(),
        );

        let mailbox_service = MailboxServiceImpl::new(query_bus.clone());

        let message_service = MessageServiceImpl::new(
            command_bus.clone(),
            query_bus.clone(),
        );

        let search_service = SearchServiceImpl::new(query_bus.clone());

        tracing::info!("All services initialized successfully");

        Ok(Self {
            account_service,
            mailbox_service,
            message_service,
            search_service,
        })
    }
}
