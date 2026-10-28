// Traits are consumed via generics, not dyn dispatch.
#![allow(async_fn_in_trait)]

pub mod config;
pub mod email;
pub mod oauth;
pub mod persistence;
pub mod provider_registry;
pub mod search;
pub mod secret;

// Re-exports for ergonomics
pub use email::{ImapAccountEventSource, ImapEmailProvider, SmtpSender};
pub use persistence::{DownloadMeter, SqliteDb, SqliteEventStore, SqliteRepository};
pub use provider_registry::InMemoryProviderRegistry;
pub use search::TantivySearchIndex;
pub use secret::KeyringSecretStore;
