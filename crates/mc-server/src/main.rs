//! mail-cat gRPC server
//!
//! DDD email client with Event Sourcing + CQRS architecture.
//! Exposes gRPC API for account management, mailbox browsing, message operations, and search.

use anyhow::Context;
use tonic::transport::Server;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod config;
mod wiring;

use config::ServerConfig;
use wiring::AppContext;

use mc_interfaces::grpc::proto::{
    account_service_server::AccountServiceServer, mailbox_service_server::MailboxServiceServer,
    message_service_server::MessageServiceServer, search_service_server::SearchServiceServer,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const GIT_SHA: &str = env!("BUILD_GIT_SHA");
const BUILD_DATE: &str = env!("BUILD_DATE");
const RUSTC: &str = env!("BUILD_RUSTC");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // --version
    if std::env::args().any(|a| a == "--version" || a == "-V") {
        println!("mail-cat-server {VERSION} ({GIT_SHA}, {BUILD_DATE})");
        println!("{RUSTC}");
        return Ok(());
    }

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "mc_server=info,mc_application=debug,mc_infrastructure=debug".into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("🚀 Starting mail-cat-server {VERSION} ({GIT_SHA}, {BUILD_DATE}) [{RUSTC}]");

    // Load configuration
    let config_path =
        std::env::var("MAIL_CAT_CONFIG").unwrap_or_else(|_| "config.toml".to_string());

    let config = ServerConfig::load_or_default(&config_path);
    tracing::info!("Loaded configuration from {}", config_path);
    tracing::info!("gRPC server will listen on {}", config.grpc_addr);
    tracing::info!("Database path: {}", config.db_path);
    tracing::info!("Search index path: {}", config.index_path);
    tracing::info!("Configured accounts: {}", config.accounts.len());

    // Wire up all dependencies
    let app_ctx = AppContext::new(&config)
        .await
        .context("failed to initialize application context")?;

    let shutdown = app_ctx.shutdown.clone();

    // Parse gRPC address
    let addr = config.grpc_addr.parse().context("invalid gRPC address")?;

    tracing::info!("✨ All components initialized successfully");
    tracing::info!("📡 Starting gRPC server on {}...", addr);

    // Create gRPC server with all services
    Server::builder()
        .add_service(AccountServiceServer::new(app_ctx.account_service))
        .add_service(MailboxServiceServer::new(app_ctx.mailbox_service))
        .add_service(MessageServiceServer::new(app_ctx.message_service))
        .add_service(SearchServiceServer::new(app_ctx.search_service))
        .serve_with_shutdown(addr, shutdown_signal())
        .await
        .context("gRPC server error")?;

    tracing::info!("🛑 gRPC server stopped, cancelling background tasks...");
    shutdown.cancel();

    // Brief yield so spawned tasks can react to cancellation before the
    // runtime is torn down.
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    tracing::info!("👋 Server shut down gracefully");
    Ok(())
}

/// Wait for Ctrl+C signal for graceful shutdown.
async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
    tracing::info!("🛑 Received shutdown signal, stopping server...");
}
