/// Domain trait definitions (ports in hexagonal architecture).
///
/// These traits are implemented by adapters in `mc-infrastructure`.
/// The domain and application layers depend only on these abstractions.
pub mod account_event_source;
pub mod contact_index;
pub mod email_provider;
pub mod event_store;
pub mod provider_registry;
pub mod read_model_projector;
pub mod repository;
pub mod search_index;
