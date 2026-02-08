use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize tracing with environment-based filtering.
///
/// Set `RUST_LOG` to control log levels, e.g.:
/// - `RUST_LOG=info`
/// - `RUST_LOG=mail_cat=debug,tower=warn`
pub fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer().with_target(true).with_thread_ids(false))
        .init();
}
