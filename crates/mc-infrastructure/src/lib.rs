// Traits are consumed via generics, not dyn dispatch.
#![allow(async_fn_in_trait)]

pub mod config;
pub mod dav;
pub mod email;
pub mod embedding;
pub mod oauth;
pub mod persistence;
pub mod provider_registry;
pub mod search;
pub mod secret;

// Re-exports for ergonomics
pub use dav::{DavProvider, Profile as DavProfile};
pub use email::{
    AnyAccountEventSource, AnyProvider, ImapEmailProvider, JmapEmailProvider, SmtpSender,
};
pub use embedding::{CATALOG, CatalogEmbedder, ExecutionTarget, LazyEmbedder, ModelSpec};
pub use persistence::{
    DownloadMeter, ReminderLog, SqliteDb, SqliteEventStore, SqliteRepository, SqliteTaggingStore,
};
pub use provider_registry::InMemoryProviderRegistry;
pub use search::TantivySearchIndex;
pub use secret::KeyringSecretStore;
