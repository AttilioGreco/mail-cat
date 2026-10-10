use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use crate::error::DomainError;
use crate::events::{AggregateType, DomainEvent, EventEnvelope};

/// Append-only event store. Source of truth in the event-sourced architecture.
///
/// # Semantics
///
/// - **`expected_sequence`**: The last known sequence number for the aggregate.
///   Pass `0` for a brand-new aggregate (no events yet). The store checks that the
///   current last sequence matches `expected_sequence` before appending (optimistic
///   concurrency control). If they don't match, returns `DomainError::ConcurrencyConflict`.
///
/// - **`append` return value**: Returns the sequence number of the **last event written**
///   in this batch. For a new aggregate with 3 events, returns `3`.
///
/// - **`global_position`**: A monotonically increasing `u64` assigned by the store on
///   each event append. It is total-ordered across all aggregates and all event types.
///   Used by projections and subscribers to track their checkpoint.
///
/// - **`subscribe`**: Delivers events in **global_position order**, with **at-least-once**
///   semantics. Consumers (projections) must be idempotent and persist their last
///   processed `global_position` as a checkpoint.
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Append events for a given aggregate.
    ///
    /// Returns the sequence number of the last event written.
    /// Fails with `DomainError::ConcurrencyConflict` if `expected_sequence` doesn't
    /// match the actual last sequence in the store.
    async fn append(
        &self,
        aggregate_id: &str,
        aggregate_type: AggregateType,
        expected_sequence: u64,
        events: Vec<DomainEvent>,
    ) -> Result<u64, DomainError>;

    /// Load all events for an aggregate, starting from `from_sequence` (inclusive).
    ///
    /// Pass `0` to load all events from the beginning.
    async fn load_events(
        &self,
        aggregate_id: &str,
        from_sequence: u64,
    ) -> Result<Vec<EventEnvelope>, DomainError>;

    /// The aggregate's last sequence number (`0` if it has none) — the
    /// `expected_sequence` for its next `append`.
    ///
    /// The default replays the whole history; stores should override it, since
    /// appends call it and a message aggregate's history carries its full body.
    async fn current_sequence(&self, aggregate_id: &str) -> Result<u64, DomainError> {
        Ok(self.load_events(aggregate_id, 0).await?.len() as u64)
    }

    /// Load events across all aggregates, optionally filtered by type, as a stream.
    ///
    /// Events are yielded in `global_position` order. Pass `from_global_position = 0`
    /// to read from the beginning. Streamed rather than collected up front: full-history
    /// catch-up (read model, search index) only ever consumes this sequentially, and on
    /// a large event store materializing it all as a `Vec` first would hold the entire
    /// history in memory at once for no reason.
    fn load_all_events(
        &self,
        aggregate_type: Option<AggregateType>,
        from_global_position: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<EventEnvelope, DomainError>> + Send + '_>>;

    /// Subscribe to new events as they are appended.
    ///
    /// Returns a boxed stream delivering events in `global_position` order.
    /// At-least-once delivery: consumers must be idempotent.
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = EventEnvelope> + Send + '_>>;
}
