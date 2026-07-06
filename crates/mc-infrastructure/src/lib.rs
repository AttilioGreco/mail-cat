// Traits are consumed via generics, not dyn dispatch.
#![allow(async_fn_in_trait)]

pub mod config;
pub mod email;
pub mod persistence;
pub mod provider_registry;
pub mod search;
pub mod secret;
pub mod oauth;

// Re-exports for ergonomics
pub use email::{ImapAccountEventSource, ImapEmailProvider, SmtpSender};
pub use persistence::{SharedSledRepository, SledEventStore, SledRepository};
pub use provider_registry::InMemoryProviderRegistry;
pub use search::{TantivyContactIndex, TantivySearchIndex};
pub use secret::KeyringSecretStore;
