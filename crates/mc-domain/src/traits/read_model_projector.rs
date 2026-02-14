use async_trait::async_trait;
use crate::error::DomainError;
use crate::events::EventEnvelope;

/// Port for projecting domain events into read models.
///
/// Implementors update materialized views (account summaries, mailbox lists,
/// message summaries, etc.) based on incoming events. Each call to `project`
/// must be **idempotent** — projecting the same event twice should produce the
/// same result, since the projection handler replays events on rebuild.
#[async_trait]
pub trait ReadModelProjector: Send + Sync {
    /// Project a single event envelope into the read model.
    ///
    /// The projector inspects `envelope.payload` and updates the appropriate
    /// read model stores. Unknown or irrelevant events are silently ignored.
    async fn project(&self, envelope: &EventEnvelope) -> Result<(), DomainError>;
}
