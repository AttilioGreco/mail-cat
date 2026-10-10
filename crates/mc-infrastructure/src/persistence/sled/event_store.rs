//! Sled-backed event store implementation.
//!
//! # Storage layout
//!
//! Uses three sled trees:
//!
//! - **`events_by_aggregate`**: `"{aggregate_id}:{sequence:016}"` → JSON(EventEnvelope)
//!   Per-aggregate event ordering. Sequence numbers are zero-padded for correct
//!   lexicographic sorting.
//!
//! - **`events_global`**: `"{global_position:016}"` → JSON(EventEnvelope)
//!   Total-ordered stream of all events. global_position is monotonically increasing.
//!
//! - **`aggregate_meta`**: `"{aggregate_id}"` → `"{last_sequence}"`
//!   Tracks the current sequence number for each aggregate (used for OCC).
//!
//! The global position counter is stored in sled using `fetch_and_update` for
//! atomic increments.

use async_trait::async_trait;
use std::pin::Pin;

use futures::Stream;
use sled::Db;
use tokio::sync::broadcast;
use tracing::{debug, warn};

use mc_domain::{AggregateType, DomainError, DomainEvent, EventEnvelope, EventStore};

const EVENTS_BY_AGGREGATE_TREE: &str = "events_by_aggregate";
const EVENTS_GLOBAL_TREE: &str = "events_global";
const AGGREGATE_META_TREE: &str = "aggregate_meta";
const GLOBAL_POSITION_KEY: &str = "__global_position__";

/// Sled-backed implementation of the [`EventStore`] domain trait.
pub struct SledEventStore {
    db: Db,
    /// Broadcast channel for live event subscriptions.
    notifier: broadcast::Sender<EventEnvelope>,
}

impl Clone for SledEventStore {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            notifier: self.notifier.clone(),
        }
    }
}

impl SledEventStore {
    /// Open or create an event store at the given sled database.
    pub fn new(db: Db) -> Self {
        let (notifier, _) = broadcast::channel(4096);
        Self { db, notifier }
    }

    /// Open a temporary event store (useful for testing).
    pub fn open_temp() -> Result<Self, DomainError> {
        let db = sled::Config::new()
            .temporary(true)
            .open()
            .map_err(|e| DomainError::EventStore(e.to_string()))?;
        Ok(Self::new(db))
    }

    fn events_by_aggregate_tree(&self) -> Result<sled::Tree, DomainError> {
        self.db
            .open_tree(EVENTS_BY_AGGREGATE_TREE)
            .map_err(|e| DomainError::EventStore(e.to_string()))
    }

    fn events_global_tree(&self) -> Result<sled::Tree, DomainError> {
        self.db
            .open_tree(EVENTS_GLOBAL_TREE)
            .map_err(|e| DomainError::EventStore(e.to_string()))
    }

    fn aggregate_meta_tree(&self) -> Result<sled::Tree, DomainError> {
        self.db
            .open_tree(AGGREGATE_META_TREE)
            .map_err(|e| DomainError::EventStore(e.to_string()))
    }

    /// Atomically increment the global position counter and return the new value.
    fn next_global_position(&self) -> Result<u64, DomainError> {
        let meta = self.aggregate_meta_tree()?;
        let result = meta
            .fetch_and_update(GLOBAL_POSITION_KEY, |old| {
                let current = old
                    .map(|bytes| {
                        let s = std::str::from_utf8(bytes).unwrap_or("0");
                        s.parse::<u64>().unwrap_or(0)
                    })
                    .unwrap_or(0);
                let next = current + 1;
                Some(next.to_string().into_bytes())
            })
            .map_err(|e| DomainError::EventStore(e.to_string()))?;

        // fetch_and_update returns the OLD value; we need old + 1.
        let old_val = result
            .map(|bytes| {
                let s = std::str::from_utf8(&bytes).unwrap_or("0");
                s.parse::<u64>().unwrap_or(0)
            })
            .unwrap_or(0);

        Ok(old_val + 1)
    }

    fn stored_sequence(&self, aggregate_id: &str) -> Result<u64, DomainError> {
        let meta = self.aggregate_meta_tree()?;
        let val = meta
            .get(aggregate_id)
            .map_err(|e| DomainError::EventStore(e.to_string()))?;

        match val {
            Some(bytes) => {
                let s = std::str::from_utf8(&bytes)
                    .map_err(|e| DomainError::EventStore(e.to_string()))?;
                s.parse::<u64>()
                    .map_err(|e| DomainError::EventStore(e.to_string()))
            }
            None => Ok(0),
        }
    }

    /// Format the per-aggregate key: `{aggregate_id}:{sequence:016}`.
    fn aggregate_key(aggregate_id: &str, sequence: u64) -> String {
        format!("{aggregate_id}:{sequence:016}")
    }

    /// Format the global key: `{global_position:016}`.
    fn global_key(global_position: u64) -> String {
        format!("{global_position:016}")
    }
}

#[async_trait]
impl EventStore for SledEventStore {
    async fn append(
        &self,
        aggregate_id: &str,
        aggregate_type: AggregateType,
        expected_sequence: u64,
        events: Vec<DomainEvent>,
    ) -> Result<u64, DomainError> {
        if events.is_empty() {
            return Ok(expected_sequence);
        }

        // OCC check: verify the current sequence matches expected.
        let current_seq = self.stored_sequence(aggregate_id)?;
        if current_seq != expected_sequence {
            return Err(DomainError::ConcurrencyConflict {
                expected: expected_sequence,
                actual: current_seq,
            });
        }

        let agg_tree = self.events_by_aggregate_tree()?;
        let global_tree = self.events_global_tree()?;
        let meta_tree = self.aggregate_meta_tree()?;

        let mut last_sequence = current_seq;

        for event in events {
            last_sequence += 1;
            let global_pos = self.next_global_position()?;

            let mut envelope = EventEnvelope::new(
                aggregate_id.to_string(),
                aggregate_type,
                last_sequence,
                event,
            );
            envelope.global_position = global_pos;

            let json = serde_json::to_vec(&envelope)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;

            // Write to per-aggregate tree.
            let agg_key = Self::aggregate_key(aggregate_id, last_sequence);
            agg_tree
                .insert(agg_key.as_bytes(), json.as_slice())
                .map_err(|e| DomainError::EventStore(e.to_string()))?;

            // Write to global tree.
            let global_key = Self::global_key(global_pos);
            global_tree
                .insert(global_key.as_bytes(), json.as_slice())
                .map_err(|e| DomainError::EventStore(e.to_string()))?;

            debug!(
                aggregate_id,
                sequence = last_sequence,
                global_position = global_pos,
                event = envelope.payload.event_name(),
                "event appended"
            );

            // Notify live subscribers (best-effort).
            let _ = self.notifier.send(envelope);
        }

        // Update aggregate meta with new last sequence.
        meta_tree
            .insert(
                aggregate_id.as_bytes(),
                last_sequence.to_string().as_bytes(),
            )
            .map_err(|e| DomainError::EventStore(e.to_string()))?;

        // No per-append fsync: everything here is re-fetchable from the provider,
        // and fsyncing every event made a sync thousands of fsyncs. sled's
        // background flush (every 500 ms) bounds a crash to the last half second,
        // and the clean-shutdown path in main.rs flushes explicitly.

        Ok(last_sequence)
    }

    async fn current_sequence(&self, aggregate_id: &str) -> Result<u64, DomainError> {
        self.stored_sequence(aggregate_id)
    }

    async fn load_events(
        &self,
        aggregate_id: &str,
        from_sequence: u64,
    ) -> Result<Vec<EventEnvelope>, DomainError> {
        let tree = self.events_by_aggregate_tree()?;

        let prefix = format!("{aggregate_id}:");
        let start_key = Self::aggregate_key(aggregate_id, from_sequence);

        let mut envelopes = Vec::new();
        for item in tree.range(start_key.as_bytes()..) {
            let (key, value) = item.map_err(|e| DomainError::EventStore(e.to_string()))?;

            // Stop when we leave this aggregate's prefix.
            let key_str =
                std::str::from_utf8(&key).map_err(|e| DomainError::EventStore(e.to_string()))?;
            if !key_str.starts_with(&prefix) {
                break;
            }

            let envelope: EventEnvelope = serde_json::from_slice(&value)
                .map_err(|e| DomainError::Serialization(e.to_string()))?;
            envelopes.push(envelope);
        }

        Ok(envelopes)
    }

    fn load_all_events(
        &self,
        aggregate_type: Option<AggregateType>,
        from_global_position: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<EventEnvelope, DomainError>> + Send + '_>> {
        let start_key = Self::global_key(from_global_position);

        let stream = async_stream::stream! {
            let tree = match self.events_global_tree() {
                Ok(tree) => tree,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };

            for item in tree.range(start_key.as_bytes()..) {
                let (_key, value) = match item {
                    Ok(kv) => kv,
                    Err(e) => {
                        yield Err(DomainError::EventStore(e.to_string()));
                        return;
                    }
                };

                let envelope: EventEnvelope = match serde_json::from_slice(&value) {
                    Ok(envelope) => envelope,
                    Err(e) => {
                        yield Err(DomainError::Serialization(e.to_string()));
                        return;
                    }
                };

                if let Some(ref filter_type) = aggregate_type
                    && &envelope.aggregate_type != filter_type
                {
                    continue;
                }

                yield Ok(envelope);
            }
        };
        Box::pin(stream)
    }

    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = EventEnvelope> + Send + '_>> {
        let mut receiver = self.notifier.subscribe();
        let stream = async_stream::stream! {
            loop {
                match receiver.recv().await {
                    Ok(envelope) => yield envelope,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(lagged = n, "event store subscriber lagged, skipped {n} events");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        };
        Box::pin(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use mc_domain::{AccountId, EmailAddress, ProviderType};

    fn make_event() -> DomainEvent {
        DomainEvent::AccountAdded {
            account_id: AccountId::new(),
            name: "Test".into(),
            email: EmailAddress::new("test@example.com").unwrap(),
            provider_type: ProviderType::Imap,
        }
    }

    #[tokio::test]
    async fn append_and_load_events() {
        let store = SledEventStore::open_temp().unwrap();

        let seq = store
            .append("acc-1", AggregateType::Account, 0, vec![make_event()])
            .await
            .unwrap();
        assert_eq!(seq, 1);

        let seq = store
            .append(
                "acc-1",
                AggregateType::Account,
                1,
                vec![make_event(), make_event()],
            )
            .await
            .unwrap();
        assert_eq!(seq, 3);

        let events = store.load_events("acc-1", 0).await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(store.current_sequence("acc-1").await.unwrap(), 3);
        assert_eq!(store.current_sequence("unknown").await.unwrap(), 0);
        assert_eq!(events[0].sequence_number, 1);
        assert_eq!(events[1].sequence_number, 2);
        assert_eq!(events[2].sequence_number, 3);
    }

    #[tokio::test]
    async fn load_events_from_sequence() {
        let store = SledEventStore::open_temp().unwrap();

        store
            .append(
                "acc-1",
                AggregateType::Account,
                0,
                vec![make_event(), make_event(), make_event()],
            )
            .await
            .unwrap();

        let events = store.load_events("acc-1", 2).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence_number, 2);
        assert_eq!(events[1].sequence_number, 3);
    }

    #[tokio::test]
    async fn concurrency_conflict() {
        let store = SledEventStore::open_temp().unwrap();

        store
            .append("acc-1", AggregateType::Account, 0, vec![make_event()])
            .await
            .unwrap();

        // Try appending with wrong expected sequence.
        let result = store
            .append("acc-1", AggregateType::Account, 0, vec![make_event()])
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            DomainError::ConcurrencyConflict { expected, actual } => {
                assert_eq!(expected, 0);
                assert_eq!(actual, 1);
            }
            other => panic!("expected ConcurrencyConflict, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn global_position_is_monotonic() {
        let store = SledEventStore::open_temp().unwrap();

        store
            .append("acc-1", AggregateType::Account, 0, vec![make_event()])
            .await
            .unwrap();
        store
            .append("acc-2", AggregateType::Account, 0, vec![make_event()])
            .await
            .unwrap();

        let all: Vec<EventEnvelope> = store.load_all_events(None, 0).try_collect().await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].global_position, 1);
        assert_eq!(all[1].global_position, 2);
        // Different aggregates but monotonic global order.
        assert_eq!(all[0].aggregate_id, "acc-1");
        assert_eq!(all[1].aggregate_id, "acc-2");
    }

    #[tokio::test]
    async fn load_all_events_filtered_by_type() {
        let store = SledEventStore::open_temp().unwrap();

        store
            .append("acc-1", AggregateType::Account, 0, vec![make_event()])
            .await
            .unwrap();

        let msg_event = DomainEvent::DraftDiscarded {
            message_id: mc_domain::MessageId::new(),
        };
        store
            .append("msg-1", AggregateType::Message, 0, vec![msg_event])
            .await
            .unwrap();

        let account_events: Vec<EventEnvelope> = store
            .load_all_events(Some(AggregateType::Account), 0)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(account_events.len(), 1);

        let message_events: Vec<EventEnvelope> = store
            .load_all_events(Some(AggregateType::Message), 0)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(message_events.len(), 1);

        let all_events: Vec<EventEnvelope> =
            store.load_all_events(None, 0).try_collect().await.unwrap();
        assert_eq!(all_events.len(), 2);
    }

    #[tokio::test]
    async fn empty_append_returns_expected_sequence() {
        let store = SledEventStore::open_temp().unwrap();

        let seq = store
            .append("acc-1", AggregateType::Account, 0, vec![])
            .await
            .unwrap();
        assert_eq!(seq, 0);
    }

    #[tokio::test]
    async fn separate_aggregates_have_independent_sequences() {
        let store = SledEventStore::open_temp().unwrap();

        store
            .append("acc-1", AggregateType::Account, 0, vec![make_event()])
            .await
            .unwrap();
        store
            .append("acc-2", AggregateType::Account, 0, vec![make_event()])
            .await
            .unwrap();

        let events_1 = store.load_events("acc-1", 0).await.unwrap();
        let events_2 = store.load_events("acc-2", 0).await.unwrap();

        assert_eq!(events_1.len(), 1);
        assert_eq!(events_2.len(), 1);
        assert_eq!(events_1[0].sequence_number, 1);
        assert_eq!(events_2[0].sequence_number, 1);
    }

    #[tokio::test]
    async fn subscribe_receives_new_events() {
        let store = SledEventStore::open_temp().unwrap();

        // Subscribe BEFORE appending so the receiver catches the broadcast.
        let mut sub = store.notifier.subscribe();

        store
            .append("acc-1", AggregateType::Account, 0, vec![make_event()])
            .await
            .unwrap();

        let envelope = sub.recv().await.expect("should receive event");
        assert_eq!(envelope.aggregate_id, "acc-1");
        assert_eq!(envelope.payload.event_name(), "AccountAdded");
    }
}
