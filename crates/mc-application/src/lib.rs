// Traits are consumed via generics, not dyn dispatch.
#![allow(async_fn_in_trait)]

pub mod bus;
pub mod commands;
pub mod error;
pub mod handlers;
pub mod projections;
pub mod queries;
pub mod services;

// Re-exports for ergonomics
pub use bus::{
    CommandBus, EventPublisher, EventSubscriber, InProcessCommandBus, InProcessEventBus,
    InProcessQueryBus, QueryBus,
};
pub use commands::{Command, CommandResult};
pub use error::ApplicationError;
pub use projections::ProjectionEventHandler;
pub use queries::{Query, QueryResult};
pub use services::{
    AccountProvisioner, ImapProvisionParams, OAuthLoginProgress, OAuthLoginTrigger, SyncConfig,
    SyncProgressEvent, SyncService, SyncState, SyncTrigger,
};
