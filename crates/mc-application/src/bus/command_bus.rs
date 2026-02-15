//! Command bus trait — dispatches write-side commands to their handlers.

use crate::commands::{Command, CommandResult};
use crate::error::ApplicationError;
use async_trait::async_trait;

/// Dispatches [`Command`]s to the appropriate handler.
///
/// Implementations wire command variants to concrete handler logic.
/// The application layer defines this trait; the server wiring provides
/// the concrete implementation that knows about all the infrastructure adapters.
#[async_trait]
pub trait CommandBus: Send + Sync {
    /// Dispatch a command and return the result.
    ///
    /// Implementations should:
    /// 1. Match the command variant
    /// 2. Invoke the appropriate handler (which uses domain services/repositories)
    /// 3. Publish resulting domain events to the [`EventPublisher`](crate::bus::EventPublisher)
    /// 4. Return the command result
    async fn dispatch(&self, command: Command) -> Result<CommandResult, ApplicationError>;
}
