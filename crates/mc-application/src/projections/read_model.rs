//! Read-model projection handler.
//!
//! Processes domain events from the event store and projects them into the
//! read model via a [`ReadModelProjector`]. Tracks progress with a
//! [`ProjectionCheckpoint`] to enable resume after restart and idempotent replay.

#[allow(unused_imports)]
use async_trait::async_trait;
use futures::TryStreamExt;
use tracing::{debug, info};

use mc_domain::{EventEnvelope, EventStore, ProjectionCheckpoint, ReadModelProjector};

use crate::error::ApplicationError;

/// Name used for the read-model projection checkpoint.
const PROJECTION_NAME: &str = "read_model";

/// Events per transaction during catch-up and rebuild (spec §4).
const BATCH_SIZE: usize = 1000;

/// Handles projection of domain events into read models.
///
/// # Usage
///
/// ```ignore
/// let handler = ProjectionEventHandler::new(projector, checkpoint_store);
///
/// // Process a single event (called from event subscription loop):
/// handler.handle(&envelope).await?;
///
/// // Or rebuild from scratch (replays all events from the store):
/// let count = handler.rebuild(&event_store).await?;
/// ```
pub struct ProjectionEventHandler<P, C> {
    projector: P,
    checkpoint: C,
}

impl<P, C> ProjectionEventHandler<P, C>
where
    P: ReadModelProjector,
    C: ProjectionCheckpoint,
{
    pub fn new(projector: P, checkpoint: C) -> Self {
        Self {
            projector,
            checkpoint,
        }
    }

    /// Process a single event envelope.
    ///
    /// Skips events that have already been processed (based on checkpoint).
    /// After successful projection, advances the checkpoint.
    pub async fn handle(&self, envelope: &EventEnvelope) -> Result<(), ApplicationError> {
        let last_pos = self
            .checkpoint
            .get_checkpoint(PROJECTION_NAME)
            .await
            .map_err(ApplicationError::Domain)?;

        if envelope.global_position <= last_pos {
            debug!(
                global_position = envelope.global_position,
                last_checkpoint = last_pos,
                "skipping already-processed event"
            );
            return Ok(());
        }

        self.project_and_checkpoint(vec![envelope.clone()]).await?;

        debug!(
            global_position = envelope.global_position,
            event = envelope.payload.event_name(),
            "projected event"
        );

        Ok(())
    }

    /// Rebuild the entire read model by replaying all events from the store.
    ///
    /// Resets the checkpoint to 0 before replaying. Returns the number of
    /// events processed.
    pub async fn rebuild<ES: EventStore>(&self, store: &ES) -> Result<u64, ApplicationError> {
        info!("starting read-model rebuild from event store");

        self.checkpoint
            .save_checkpoint(PROJECTION_NAME, 0)
            .await
            .map_err(ApplicationError::Domain)?;
        let count = self.project_from(store, 0).await?;

        info!(count, "read-model rebuild complete");
        Ok(count)
    }

    /// Catch up on any events missed since the last checkpoint.
    ///
    /// Loads events from the store starting after the last checkpoint and
    /// processes them sequentially. Returns the number of events processed.
    pub async fn catch_up<ES: EventStore>(&self, store: &ES) -> Result<u64, ApplicationError> {
        let last_pos = self
            .checkpoint
            .get_checkpoint(PROJECTION_NAME)
            .await
            .map_err(ApplicationError::Domain)?;

        debug!(from_position = last_pos + 1, "catching up on missed events");
        let count = self.project_from(store, last_pos + 1).await?;

        info!(count, "catch-up complete");
        Ok(count)
    }

    async fn project_from<ES: EventStore>(
        &self,
        store: &ES,
        from: u64,
    ) -> Result<u64, ApplicationError> {
        let mut batches = store.load_all_events(None, from).try_chunks(BATCH_SIZE);
        let mut count = 0u64;
        while let Some(batch) = batches
            .try_next()
            .await
            .map_err(|e| ApplicationError::Domain(e.1))?
        {
            count += batch.len() as u64;
            self.project_and_checkpoint(batch).await?;
            info!(progress = count, "projection progress");
        }
        Ok(count)
    }

    /// Projects `events` and advances the checkpoint to the last one — in the
    /// same transaction when the projector supports it.
    async fn project_and_checkpoint(
        &self,
        events: Vec<EventEnvelope>,
    ) -> Result<(), ApplicationError> {
        let Some(last) = events.last().map(|e| e.global_position) else {
            return Ok(());
        };
        let committed = self
            .projector
            .project_batch(PROJECTION_NAME, events)
            .await
            .map_err(ApplicationError::Domain)?;
        if !committed {
            self.checkpoint
                .save_checkpoint(PROJECTION_NAME, last)
                .await
                .map_err(ApplicationError::Domain)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mc_domain::{
        AccountId, AggregateType, DomainError, DomainEvent, EmailAddress, EventEnvelope,
        ProviderType,
    };
    use std::sync::{Arc, Mutex};

    // ── Mock ReadModelProjector ──────────────────────────────────────

    #[derive(Clone)]
    struct MockProjector {
        projected: Arc<Mutex<Vec<String>>>,
    }

    impl MockProjector {
        fn new() -> Self {
            Self {
                projected: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn projected_events(&self) -> Vec<String> {
            self.projected.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ReadModelProjector for MockProjector {
        async fn project(&self, envelope: &EventEnvelope) -> Result<(), DomainError> {
            self.projected
                .lock()
                .unwrap()
                .push(envelope.payload.event_name().to_string());
            Ok(())
        }
    }

    // ── Mock ProjectionCheckpoint ────────────────────────────────────

    #[derive(Clone)]
    struct MockCheckpoint {
        position: Arc<Mutex<u64>>,
    }

    impl MockCheckpoint {
        fn new() -> Self {
            Self {
                position: Arc::new(Mutex::new(0)),
            }
        }

        fn set_position(&self, pos: u64) {
            *self.position.lock().unwrap() = pos;
        }

        fn get_position(&self) -> u64 {
            *self.position.lock().unwrap()
        }
    }

    #[async_trait]
    impl ProjectionCheckpoint for MockCheckpoint {
        async fn get_checkpoint(&self, _name: &str) -> Result<u64, DomainError> {
            Ok(*self.position.lock().unwrap())
        }

        async fn save_checkpoint(
            &self,
            _name: &str,
            global_position: u64,
        ) -> Result<(), DomainError> {
            *self.position.lock().unwrap() = global_position;
            Ok(())
        }
    }

    // ── Mock EventStore (for rebuild) ────────────────────────────────

    struct MockEventStore {
        events: Vec<EventEnvelope>,
    }

    impl MockEventStore {
        fn new(events: Vec<EventEnvelope>) -> Self {
            Self { events }
        }
    }

    #[async_trait]
    impl EventStore for MockEventStore {
        async fn append(
            &self,
            _aggregate_id: &str,
            _aggregate_type: AggregateType,
            _expected_sequence: u64,
            _events: Vec<DomainEvent>,
        ) -> Result<u64, DomainError> {
            unimplemented!()
        }

        async fn load_events(
            &self,
            _aggregate_id: &str,
            _from_sequence: u64,
        ) -> Result<Vec<EventEnvelope>, DomainError> {
            unimplemented!()
        }

        fn load_all_events(
            &self,
            _aggregate_type: Option<AggregateType>,
            from_global_position: u64,
        ) -> std::pin::Pin<Box<dyn futures::Stream<Item = Result<EventEnvelope, DomainError>> + Send + '_>> {
            let events: Vec<_> = self
                .events
                .iter()
                .filter(|e| e.global_position >= from_global_position)
                .cloned()
                .collect();
            Box::pin(futures::stream::iter(events.into_iter().map(Ok)))
        }

        fn subscribe(
            &self,
        ) -> std::pin::Pin<Box<dyn futures::Stream<Item = EventEnvelope> + Send + '_>> {
            unimplemented!()
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────

    fn make_envelope(global_position: u64, event: DomainEvent) -> EventEnvelope {
        let mut env = EventEnvelope::new(
            "acc-1".into(),
            AggregateType::Account,
            global_position,
            event,
        );
        env.global_position = global_position;
        env
    }

    fn account_added_event() -> DomainEvent {
        DomainEvent::AccountAdded {
            account_id: AccountId::new(),
            name: "Test".into(),
            email: EmailAddress::new("test@example.com").unwrap(),
            provider_type: ProviderType::Imap,
        }
    }

    // ── Tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn handle_projects_event_and_advances_checkpoint() {
        let projector = MockProjector::new();
        let checkpoint = MockCheckpoint::new();
        let handler = ProjectionEventHandler::new(projector.clone(), checkpoint.clone());

        let env = make_envelope(1, account_added_event());
        handler.handle(&env).await.unwrap();

        assert_eq!(projector.projected_events(), vec!["AccountAdded"]);
        assert_eq!(checkpoint.get_position(), 1);
    }

    #[tokio::test]
    async fn handle_skips_already_processed_events() {
        let projector = MockProjector::new();
        let checkpoint = MockCheckpoint::new();
        checkpoint.set_position(5);

        let handler = ProjectionEventHandler::new(projector.clone(), checkpoint.clone());

        // Events at positions <= 5 should be skipped.
        let env = make_envelope(3, account_added_event());
        handler.handle(&env).await.unwrap();

        let env = make_envelope(5, account_added_event());
        handler.handle(&env).await.unwrap();

        assert!(projector.projected_events().is_empty());

        // Event at position 6 should be processed.
        let env = make_envelope(6, account_added_event());
        handler.handle(&env).await.unwrap();

        assert_eq!(projector.projected_events(), vec!["AccountAdded"]);
        assert_eq!(checkpoint.get_position(), 6);
    }

    #[tokio::test]
    async fn rebuild_replays_all_events() {
        let projector = MockProjector::new();
        let checkpoint = MockCheckpoint::new();
        checkpoint.set_position(99); // simulate existing state

        let handler = ProjectionEventHandler::new(projector.clone(), checkpoint.clone());

        let store = MockEventStore::new(vec![
            make_envelope(1, account_added_event()),
            make_envelope(2, account_added_event()),
            make_envelope(3, account_added_event()),
        ]);

        let count = handler.rebuild(&store).await.unwrap();
        assert_eq!(count, 3);
        assert_eq!(projector.projected_events().len(), 3);
        assert_eq!(checkpoint.get_position(), 3);
    }

    #[tokio::test]
    async fn catch_up_processes_only_new_events() {
        let projector = MockProjector::new();
        let checkpoint = MockCheckpoint::new();
        checkpoint.set_position(2);

        let handler = ProjectionEventHandler::new(projector.clone(), checkpoint.clone());

        let store = MockEventStore::new(vec![
            make_envelope(1, account_added_event()),
            make_envelope(2, account_added_event()),
            make_envelope(3, account_added_event()),
            make_envelope(4, account_added_event()),
        ]);

        let count = handler.catch_up(&store).await.unwrap();
        assert_eq!(count, 2); // only positions 3 and 4
        assert_eq!(projector.projected_events().len(), 2);
        assert_eq!(checkpoint.get_position(), 4);
    }

    #[tokio::test]
    async fn catch_up_checkpoints_after_each_batch() {
        let projector = MockProjector::new();
        let checkpoint = MockCheckpoint::new();
        let handler = ProjectionEventHandler::new(projector.clone(), checkpoint.clone());
        let store = MockEventStore::new(
            (1..=2500).map(|p| make_envelope(p, account_added_event())).collect(),
        );

        assert_eq!(handler.catch_up(&store).await.unwrap(), 2500);
        assert_eq!(projector.projected_events().len(), 2500);
        assert_eq!(checkpoint.get_position(), 2500);
    }

    /// Stores the checkpoint itself, like the SQLite repository.
    #[derive(Clone, Default)]
    struct TxProjector {
        batches: Arc<Mutex<Vec<(String, usize, u64)>>>,
    }

    #[async_trait]
    impl ReadModelProjector for TxProjector {
        async fn project(&self, _envelope: &EventEnvelope) -> Result<(), DomainError> {
            unreachable!("catch-up must go through project_batch")
        }

        async fn project_batch(
            &self,
            checkpoint: &str,
            events: Vec<EventEnvelope>,
        ) -> Result<bool, DomainError> {
            let last = events.last().unwrap().global_position;
            self.batches.lock().unwrap().push((checkpoint.to_owned(), events.len(), last));
            Ok(true)
        }
    }

    #[tokio::test]
    async fn catch_up_hands_batches_to_a_transactional_projector() {
        let projector = TxProjector::default();
        let checkpoint = MockCheckpoint::new();
        checkpoint.set_position(2);
        let handler = ProjectionEventHandler::new(projector.clone(), checkpoint.clone());
        let store = MockEventStore::new(
            (1..=2002).map(|p| make_envelope(p, account_added_event())).collect(),
        );

        assert_eq!(handler.catch_up(&store).await.unwrap(), 2000);
        assert_eq!(
            *projector.batches.lock().unwrap(),
            [("read_model".to_owned(), 1000, 1002), ("read_model".to_owned(), 1000, 2002)]
        );
        assert_eq!(checkpoint.get_position(), 2, "the projector owns the checkpoint");
    }
}
