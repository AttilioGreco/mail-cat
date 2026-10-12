use crate::error::DomainError;
use crate::events::EventEnvelope;
use async_trait::async_trait;

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

    /// Projects a catch-up batch in global order. Returns `true` when the
    /// backend also stored `checkpoint` = the last event's position in the
    /// same transaction, so a crash can never leave the two out of step. The
    /// default projects event by event and leaves the checkpoint to the
    /// caller (`false`).
    async fn project_batch(
        &self,
        _checkpoint: &str,
        events: Vec<EventEnvelope>,
    ) -> Result<bool, DomainError> {
        for event in &events {
            self.project(event).await?;
        }
        Ok(false)
    }
}
