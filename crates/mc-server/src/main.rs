//! mail-cat gRPC server
//!
//! DDD email client with Event Sourcing + CQRS architecture.
//! Exposes gRPC API for account management, mailbox browsing, message operations, and search.
//!
//! `mail-cat-server service <install|uninstall|start|stop|status>` manages the
//! per-user autostart instead of running the server (see [`service`]).

// GUI subsystem on Windows so the logon task opens no console window; terminal
// use (`service`, `--version`) reattaches the parent console in `main`.
#![cfg_attr(windows, windows_subsystem = "windows")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use tokio::sync::Notify;
use tonic::transport::Server;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod account_provisioner;
mod account_runtime;
mod config;
mod notifications;
mod oauth_login;
mod oauth_reauth;
mod secret_ingest;
mod service;
#[cfg(test)]
mod tagging_tests;
mod wiring;

use config::ServerConfig;
use mc_interfaces::grpc::services::{RESTART_EXIT_CODE, ServerServiceImpl};
use wiring::AppContext;

use mc_interfaces::grpc::proto::{
    account_service_server::AccountServiceServer, contact_service_server::ContactServiceServer,
    mailbox_service_server::MailboxServiceServer, message_service_server::MessageServiceServer,
    search_service_server::SearchServiceServer, server_service_server::ServerServiceServer,
    tagging_service_server::TaggingServiceServer,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const GIT_SHA: &str = env!("BUILD_GIT_SHA");
const BUILD_DATE: &str = env!("BUILD_DATE");
const RUSTC: &str = env!("BUILD_RUSTC");

/// Largest gRPC message the message service accepts or sends (attachments).
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// How long in-flight calls get to finish once a stop is asked.
const STREAM_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    #[cfg(windows)]
    if !args.is_empty() {
        service::attach_parent_console();
    }
    if args.first().map(String::as_str) == Some("service") {
        std::process::exit(service::run(&args[1..]));
    }
    // A personal mail server is I/O bound: two workers keep up with IMAP and
    // gRPC, while the default (one per core) costs a stack and a malloc arena
    // per thread. Blocking threads only run SQLite calls (one writer, four
    // pooled readers) and short file work, so a small cap is plenty.
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(8)
        .enable_all()
        .build()?
        .block_on(run())
}

/// Append-only, never rotated. Falls back to discarding output rather than
/// failing to start when the file cannot be opened.
#[cfg(windows)]
fn log_file() -> std::sync::Mutex<Box<dyn std::io::Write + Send>> {
    let file = config::app_data_dir().and_then(|dir| {
        std::fs::create_dir_all(&dir).ok()?;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("server.log"))
            .ok()
    });
    std::sync::Mutex::new(match file {
        Some(f) => Box::new(f),
        None => Box::new(std::io::sink()),
    })
}

async fn run() -> anyhow::Result<()> {
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
    // otherwise the hardcoded default. Tantivy logs every segment commit at
    // info, so it stays at warn unless the filter names it explicitly.
    let mut filter = std::env::var("RUST_LOG")
        .ok()
        .or_else(|| config.log_level.clone())
        .unwrap_or_else(|| {
            "mail_cat_server=info,mc_application=debug,mc_infrastructure=debug".into()
        });
    if !filter.contains("tantivy") {
        filter.push_str(",tantivy=warn");
    }
    let fmt = tracing_subscriber::fmt::layer();
    // The Windows logon task has no console and nothing captures stdout, so log
    // to a file there (macOS gets one from launchd, Linux from journald).
    #[cfg(windows)]
    let fmt = fmt.with_ansi(false).with_writer(log_file());
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(filter))
        .with(fmt)
        .init();

    tracing::info!("Starting mail-cat-server {VERSION} ({GIT_SHA}, {BUILD_DATE}) [{RUSTC}]");
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
    // Startup-only by design: it only serves hand-edited config.toml entries.
    // Accounts added at runtime (AddAccount, OAuth login) store their secrets in
    // the keychain themselves and are started live. The inotify watcher that
    // used to do this live bought nothing and could spin a core at 100%, so it
    // was removed.
    secret_ingest::ingest_plaintext_secrets(Path::new(&config_path), secret_store.as_ref())
        .context("failed to migrate plaintext secrets into the system keychain")?;

    let oauth_challenges = std::sync::Arc::new(oauth_login::OAuthChallengeManager::new(
        PathBuf::from(&config_path),
        secret_store.clone(),
    ));

    // Wire up all dependencies
    let app_ctx = AppContext::new(
        &config,
        &config_path,
        secret_store.clone(),
        oauth_challenges,
    )
    .await
    .context("failed to initialize application context")?;

    let shutdown = app_ctx.shutdown.clone();
    let restart = Arc::new(Notify::new());
    let restarting = Arc::new(AtomicBool::new(false));

    // Parse gRPC address
    let addr = config.grpc_addr.parse().context("invalid gRPC address")?;

    // Fail fast with a clear message instead of a raw transport error chain
    // if another process (e.g. a previous server instance) already holds the port.
    if let Err(e) = std::net::TcpListener::bind(addr) {
        if e.kind() == std::io::ErrorKind::AddrInUse {
            tracing::error!("Port {addr} is already in use by another process, shutting down.");
            return Ok(());
        }
        return Err(e).context("failed to bind gRPC address");
    }

    tracing::info!("All components initialized successfully");
    tracing::info!("Starting gRPC server on {}...", addr);

    // Create gRPC server with all services
    let stopping = shutdown.clone();
    let flag = restarting.clone();
    let server = Server::builder()
        .add_service(AccountServiceServer::new(app_ctx.account_service))
        .add_service(ContactServiceServer::new(app_ctx.contact_service))
        .add_service(MailboxServiceServer::new(app_ctx.mailbox_service))
        .add_service(
            // Compose requests carry attachments; tonic's 4 MB default would reject them.
            MessageServiceServer::new(app_ctx.message_service)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES),
        )
        .add_service(SearchServiceServer::new(app_ctx.search_service))
        .add_service(TaggingServiceServer::new(app_ctx.tagging_service))
        .add_service(ServerServiceServer::new(ServerServiceImpl::new(
            restart.clone(),
        )))
        .serve_with_shutdown(addr, async move {
            shutdown_signal(&restart, &flag).await;
            stopping.cancel();
        });
    // Graceful stop waits for every open call, and the UI's watch streams
    // (sync progress, mailboxes, auth) never end on their own: without a cap
    // systemd kills us after its 90 s timeout.
    tokio::select! {
        served = server => served.context("gRPC server error")?,
        () = async {
            shutdown.cancelled().await;
            tokio::time::sleep(STREAM_GRACE).await;
        } => tracing::info!("open watch streams cut after {STREAM_GRACE:?}"),
    }

    tracing::info!("gRPC server stopped, cancelling background tasks...");
    shutdown.cancel();

    // Give spawned tasks a brief window to react to the cancellation token
    // before we force-exit; background IMAP IDLE tasks would otherwise keep the
    // process alive indefinitely.
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    tracing::info!("Server shut down gracefully");
    // Force-exit is safe: every SQLite write is its own committed transaction
    // (WAL), so nothing buffered is lost; an uncommitted one rolls back on open.
    std::process::exit(if restarting.load(Ordering::SeqCst) {
        RESTART_EXIT_CODE
    } else {
        0
    });
}

/// Wait for a shutdown signal: Ctrl+C (SIGINT) when run interactively, or
/// SIGTERM (what `systemctl stop`/`restart` actually sends). Without a SIGTERM
/// handler, systemd stopping the service bypasses this entire graceful path —
/// the process is killed outright, with no chance to cancel background tasks.
///
/// A client's restart request takes the same path; `restarting` then makes the
/// process exit non-zero so the supervisor relaunches it.
async fn shutdown_signal(restart: &Notify, restarting: &AtomicBool) {
    // Windows has no SIGTERM: the Scheduled Task and the updater stop the server
    // by terminating the process, so Ctrl+C is the only graceful path there.
    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await
    };
    #[cfg(not(unix))]
    let sigterm = std::future::pending::<Option<()>>();

    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.expect("failed to install CTRL+C signal handler");
            tracing::info!("Received SIGINT, stopping server...");
        }
        _ = sigterm => {
            tracing::info!("Received SIGTERM, stopping server...");
        }
        () = restart.notified() => {
            restarting.store(true, Ordering::SeqCst);
            tracing::info!("Restart requested, stopping server...");
        }
    }
}
