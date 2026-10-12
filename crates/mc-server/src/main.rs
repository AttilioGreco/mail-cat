//! mail-cat gRPC server
//!
//! DDD email client with Event Sourcing + CQRS architecture.
//! Exposes gRPC API for account management, mailbox browsing, message operations, and search.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use tonic::transport::Server;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod account_provisioner;
mod config;
mod notifications;
mod oauth_login;
mod oauth_reauth;
mod oauth_refresh;
mod secret_ingest;
mod wiring;

use config::ServerConfig;
use wiring::AppContext;

use mc_interfaces::grpc::proto::{
    account_service_server::AccountServiceServer,
    contact_service_server::ContactServiceServer,
    mailbox_service_server::MailboxServiceServer,
    message_service_server::MessageServiceServer,
    search_service_server::SearchServiceServer,
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

    // Load configuration (before tracing init: log_level is a fallback filter directive)
    let config_path = ServerConfig::default_path();
    let config = ServerConfig::load_or_default(&config_path);

    // Initialize tracing. RUST_LOG always wins; otherwise config.log_level;
    // otherwise the hardcoded default.
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            config
                .log_level
                .clone()
                .unwrap_or_else(|| "mc_server=info,mc_application=debug,mc_infrastructure=debug".into())
                .into()
        }))
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("🚀 Starting mail-cat-server {VERSION} ({GIT_SHA}, {BUILD_DATE}) [{RUSTC}]");
    tracing::info!("Config:  {}", config_path);
    tracing::info!("gRPC:    {}", config.grpc_addr);
    tracing::info!("DB:      {}", config.db_path);
    tracing::info!("Index:   {}", config.index_path);
    if config.accounts.is_empty() {
        tracing::warn!(
            "⚠ No accounts configured — sync and IDLE will not start.\n  \
             Create '{}' with at least one [[accounts]] entry.",
            config_path
        );
    } else {
        tracing::info!("Accounts: {}", config.accounts.len());
    }

    // Secret store (OS keychain). No fallback: if the system keychain is
    // unavailable this surfaces as a clear error below.
    let secret_store: Arc<dyn mc_domain::SecretStore> =
        Arc::new(mc_infrastructure::KeyringSecretStore::new());

    // Migrate any plaintext passwords in the config file into the keychain.
    //
    // Startup-only by design: providers resolve their credentials once during
    // wiring, so a password migrated while the daemon runs would not be picked
    // up until a restart anyway. The inotify watcher that used to do this live
    // bought nothing and could spin a core at 100%, so it was removed.
    secret_ingest::ingest_plaintext_secrets(Path::new(&config_path), secret_store.as_ref())
        .context("failed to migrate plaintext secrets into the system keychain")?;

    let oauth_challenges = std::sync::Arc::new(oauth_login::OAuthChallengeManager::new(
        PathBuf::from(&config_path),
        secret_store.clone(),
    ));

    // Wire up all dependencies
    let app_ctx = AppContext::new(&config, &config_path, secret_store.clone(), oauth_challenges)
        .await
        .context("failed to initialize application context")?;

    let shutdown = app_ctx.shutdown.clone();

    // Parse gRPC address
    let addr = config.grpc_addr.parse().context("invalid gRPC address")?;

    // Fail fast with a clear message instead of a raw transport error chain
    // if another process (e.g. a previous server instance) already holds the port.
    if let Err(e) = std::net::TcpListener::bind(addr) {
        if e.kind() == std::io::ErrorKind::AddrInUse {
            tracing::error!("⚠ Port {addr} is already in use by another process, shutting down.");
            return Ok(());
        }
        return Err(e).context("failed to bind gRPC address");
    }

    tracing::info!("✨ All components initialized successfully");
    tracing::info!("📡 Starting gRPC server on {}...", addr);

    // Create gRPC server with all services
    Server::builder()
        .add_service(AccountServiceServer::new(app_ctx.account_service))
        .add_service(ContactServiceServer::new(app_ctx.contact_service))
        .add_service(MailboxServiceServer::new(app_ctx.mailbox_service))
        .add_service(MessageServiceServer::new(app_ctx.message_service))
        .add_service(SearchServiceServer::new(app_ctx.search_service))
        .serve_with_shutdown(addr, shutdown_signal())
        .await
        .context("gRPC server error")?;

    tracing::info!("🛑 gRPC server stopped, cancelling background tasks...");
    shutdown.cancel();

    // Give spawned tasks a brief window to react to the cancellation token
    // before we force-exit; background IMAP IDLE tasks would otherwise keep the
    // process alive indefinitely.
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    tracing::info!("👋 Server shut down gracefully");
    // Force-exit is safe: every SQLite write is its own committed transaction
    // (WAL), so nothing buffered is lost; an uncommitted one rolls back on open.
    std::process::exit(0);
}

/// Wait for a shutdown signal: Ctrl+C (SIGINT) when run interactively, or
/// SIGTERM (what `systemctl stop`/`restart` actually sends). Without a SIGTERM
/// handler, systemd stopping the service bypasses this entire graceful path —
/// the process is killed outright, with no chance to cancel background tasks.
async fn shutdown_signal() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");

    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.expect("failed to install CTRL+C signal handler");
            tracing::info!("🛑 Received SIGINT, stopping server...");
        }
        _ = sigterm.recv() => {
            tracing::info!("🛑 Received SIGTERM, stopping server...");
        }
    }
}
