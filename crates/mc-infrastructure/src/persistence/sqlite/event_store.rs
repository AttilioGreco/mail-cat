//! SQLite-backed [`EventStore`]: one `events` table, rowid = global position.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::DateTime;
use futures::Stream;
use rusqlite::{Connection, Params, Row, TransactionBehavior, params};
use tokio::sync::broadcast;
use tracing::warn;
use uuid::Uuid;

use mc_domain::{AggregateType, DomainError, DomainEvent, EventEnvelope, EventStore};

use super::SqliteDb;

/// Rows per `load_all_events` round-trip: catch-up over the whole log must
/// never hold more than one page in memory.
const STREAM_PAGE: i64 = 500;

const SELECT_ENVELOPE: &str = "SELECT event_id, aggregate_id, aggregate_type, sequence, \
     global_position, occurred_at, payload FROM events";

pub struct SqliteEventStore {
    db: Arc<SqliteDb>,
    notifier: broadcast::Sender<EventEnvelope>,
}

impl SqliteEventStore {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        let (notifier, _) = broadcast::channel(4096);
        Self { db, notifier }
    }
}

fn store_err(e: impl std::fmt::Display) -> DomainError {
    DomainError::EventStore(e.to_string())
}

fn type_code(t: AggregateType) -> i64 {
    match t {
        AggregateType::Account => 0,
        AggregateType::Mailbox => 1,
        AggregateType::Message => 2,
        AggregateType::Contact => 3,
    }
}

fn type_from_code(code: i64) -> Result<AggregateType, DomainError> {
    Ok(match code {
        0 => AggregateType::Account,
        1 => AggregateType::Mailbox,
        2 => AggregateType::Message,
        3 => AggregateType::Contact,
        other => return Err(store_err(format!("unknown aggregate type code {other}"))),
    })
}

fn envelope_from_row(row: &Row) -> Result<EventEnvelope, DomainError> {
    let event_id: Vec<u8> = row.get(0).map_err(store_err)?;
    let payload: Vec<u8> = row.get(6).map_err(store_err)?;
    Ok(EventEnvelope {
        event_id: Uuid::from_slice(&event_id).map_err(store_err)?,
        aggregate_id: row.get(1).map_err(store_err)?,
        aggregate_type: type_from_code(row.get(2).map_err(store_err)?)?,
        sequence_number: row.get::<_, i64>(3).map_err(store_err)? as u64,
        global_position: row.get::<_, i64>(4).map_err(store_err)? as u64,
        occurred_at: DateTime::from_timestamp_nanos(row.get(5).map_err(store_err)?),
        payload: serde_json::from_slice(&payload)
            .map_err(|e| DomainError::Serialization(e.to_string()))?,
    })
}

fn query_envelopes(
    conn: &Connection,
    sql: &str,
    params: impl Params,
) -> Result<Vec<EventEnvelope>, DomainError> {
    let mut stmt = conn.prepare_cached(sql).map_err(store_err)?;
    let mut rows = stmt.query(params).map_err(store_err)?;
    let mut envelopes = Vec::new();
    while let Some(row) = rows.next().map_err(store_err)? {
        envelopes.push(envelope_from_row(row)?);
    }
    Ok(envelopes)
}

fn max_sequence(conn: &Connection, aggregate_id: &str) -> Result<u64, DomainError> {
    conn.query_row(
        "SELECT COALESCE(MAX(sequence), 0) FROM events WHERE aggregate_id = ?1",
        [aggregate_id],
        |r| r.get::<_, i64>(0),
    )
    .map(|seq| seq as u64)
    .map_err(store_err)
}

#[async_trait]
impl EventStore for SqliteEventStore {
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
        let aggregate_id = aggregate_id.to_owned();
        let notifier = self.notifier.clone();
        self.db
            .write(move |conn| {
                // IMMEDIATE takes the write lock up front, so the sequence read
                // below cannot go stale before the inserts commit.
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(store_err)?;
                let current = max_sequence(&tx, &aggregate_id)?;
                if current != expected_sequence {
                    return Err(DomainError::ConcurrencyConflict {
                        expected: expected_sequence,
                        actual: current,
                    });
                }
                let mut envelopes = Vec::with_capacity(events.len());
                {
                    let mut insert = tx
                        .prepare_cached(
                            "INSERT INTO events (event_id, aggregate_id, aggregate_type, \
                             sequence, event_type, occurred_at, payload) \
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        )
                        .map_err(store_err)?;
                    for (sequence, event) in (current + 1..).zip(events) {
                        let mut envelope =
                            EventEnvelope::new(aggregate_id.clone(), aggregate_type, sequence, event);
                        let occurred_at = envelope
                            .occurred_at
                            .timestamp_nanos_opt()
                            .ok_or_else(|| store_err("occurred_at out of i64 nanosecond range"))?;
                        let payload = serde_json::to_vec(&envelope.payload)
                            .map_err(|e| DomainError::Serialization(e.to_string()))?;
                        insert
                            .execute(params![
                                &envelope.event_id.as_bytes()[..],
                                aggregate_id,
                                type_code(aggregate_type),
                                sequence as i64,
                                envelope.payload.event_name(),
                                occurred_at,
                                payload,
                            ])
                            .map_err(store_err)?;
                        envelope.global_position = tx.last_insert_rowid() as u64;
                        envelopes.push(envelope);
                    }
                }
                tx.commit().map_err(store_err)?;

                // Broadcast while still holding the writer lock: positions are
                // assigned under it, so subscribers (whose checkpoints skip
                // anything at or below the last position seen) get them in
                // order, and a caller cancelled after commit cannot swallow
                // committed events. No live subscriber is not an error.
                let last_sequence = envelopes.last().map_or(expected_sequence, |e| e.sequence_number);
                for envelope in envelopes {
                    let _ = notifier.send(envelope);
                }
                Ok(last_sequence)
            })
            .await
    }

    async fn current_sequence(&self, aggregate_id: &str) -> Result<u64, DomainError> {
        let aggregate_id = aggregate_id.to_owned();
        self.db.read(move |conn| max_sequence(conn, &aggregate_id)).await
    }

    async fn load_events(
        &self,
        aggregate_id: &str,
        from_sequence: u64,
    ) -> Result<Vec<EventEnvelope>, DomainError> {
        let aggregate_id = aggregate_id.to_owned();
        self.db
            .read(move |conn| {
                query_envelopes(
                    conn,
                    &format!(
                        "{SELECT_ENVELOPE} WHERE aggregate_id = ?1 AND sequence >= ?2 \
                         ORDER BY sequence"
                    ),
                    params![aggregate_id, from_sequence as i64],
                )
            })
            .await
    }

    fn load_all_events(
        &self,
        aggregate_type: Option<AggregateType>,
        from_global_position: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<EventEnvelope, DomainError>> + Send + '_>> {
        let type_filter = aggregate_type.map(type_code);
        let mut next = from_global_position as i64;
        Box::pin(async_stream::stream! {
            loop {
                let page = self
                    .db
                    .read(move |conn| {
                        query_envelopes(
                            conn,
                            &format!(
                                "{SELECT_ENVELOPE} WHERE global_position >= ?1 \
                                 AND (?2 IS NULL OR aggregate_type = ?2) \
                                 ORDER BY global_position LIMIT ?3"
                            ),
                            params![next, type_filter, STREAM_PAGE],
                        )
                    })
                    .await;
                let page = match page {
                    Ok(page) => page,
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                };
                let full_page = page.len() as i64 == STREAM_PAGE;
                for envelope in page {
                    next = envelope.global_position as i64 + 1;
                    yield Ok(envelope);
                }
                if !full_page {
                    return;
                }
            }
        })
    }

    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = EventEnvelope> + Send + '_>> {
        let mut receiver = self.notifier.subscribe();
        Box::pin(async_stream::stream! {
            loop {
                match receiver.recv().await {
                    Ok(envelope) => yield envelope,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(lagged = n, "event store subscriber lagged, skipped {n} events");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }
}

#[cfg(test)]
mod conformance {
    fn make() -> (tempfile::TempDir, super::SqliteEventStore) {
        let dir = tempfile::tempdir().unwrap();
        let db = super::super::SqliteDb::open(&dir.path().join("mail-cat.db")).unwrap();
        (dir, super::SqliteEventStore::new(db))
    }

    crate::persistence::event_store_conformance::event_store_conformance!(make());
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, TryStreamExt};
    use mc_domain::{AccountId, EmailAddress, ProviderType};

    fn open(dir: &tempfile::TempDir) -> SqliteEventStore {
        SqliteEventStore::new(SqliteDb::open(&dir.path().join("mail-cat.db")).unwrap())
    }

    fn event(name: &str) -> DomainEvent {
        DomainEvent::AccountAdded {
            account_id: AccountId::new(),
            name: name.into(),
            email: EmailAddress::new("test@example.com").unwrap(),
            provider_type: ProviderType::Imap,
        }
    }

    #[tokio::test]
    async fn concurrent_appends_never_share_a_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(open(&dir));

        let tasks = (0..2).map(|_| {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                store.append("acc-1", AggregateType::Account, 0, vec![event("x")]).await
            })
        });
        let results = futures::future::join_all(tasks).await;

        let won = results.iter().filter(|r| matches!(r, Ok(Ok(1)))).count();
        let lost = results
            .iter()
            .filter(|r| matches!(r, Ok(Err(DomainError::ConcurrencyConflict { .. }))))
            .count();
        assert_eq!((won, lost), (1, 1), "{results:?}");
        assert_eq!(store.load_events("acc-1", 0).await.unwrap().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_appends_broadcast_in_global_position_order() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(open(&dir));
        let mut live = store.subscribe();

        let appends = (0..64).map(|i| {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                store.append(&format!("acc-{i}"), AggregateType::Account, 0, vec![event("x")]).await
            })
        });
        for result in futures::future::join_all(appends).await {
            result.unwrap().unwrap();
        }

        let mut positions = Vec::new();
        for _ in 0..64 {
            positions.push(live.next().await.unwrap().global_position);
        }
        assert_eq!(positions, (1..=64).collect::<Vec<u64>>());
    }

    #[tokio::test]
    async fn load_all_events_streams_across_pages() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(&dir);
        let total = STREAM_PAGE as usize * 2 + 1;
        let batch = (0..total).map(|i| event(&i.to_string())).collect();
        store.append("acc-1", AggregateType::Account, 0, batch).await.unwrap();

        let all: Vec<EventEnvelope> = store.load_all_events(None, 0).try_collect().await.unwrap();
        let positions: Vec<u64> = all.iter().map(|e| e.global_position).collect();
        assert_eq!(positions, (1..=total as u64).collect::<Vec<_>>());

        let from = STREAM_PAGE as u64 + 7;
        let tail: Vec<EventEnvelope> =
            store.load_all_events(None, from).try_collect().await.unwrap();
        assert_eq!(tail.first().map(|e| e.global_position), Some(from));
        assert_eq!(tail.len() as u64, total as u64 - from + 1);
    }

    #[tokio::test]
    async fn reopen_keeps_events_and_continues_positions() {
        let dir = tempfile::tempdir().unwrap();
        open(&dir).append("acc-1", AggregateType::Account, 0, vec![event("a")]).await.unwrap();

        let store = open(&dir);
        assert_eq!(store.current_sequence("acc-1").await.unwrap(), 1);
        store.append("acc-2", AggregateType::Account, 0, vec![event("b")]).await.unwrap();

        let all: Vec<EventEnvelope> = store.load_all_events(None, 0).try_collect().await.unwrap();
        let positions: Vec<u64> = all.iter().map(|e| e.global_position).collect();
        assert_eq!(positions, [1, 2]);
    }

    #[tokio::test]
    async fn rejected_append_broadcasts_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(&dir);
        store.append("acc-1", AggregateType::Account, 0, vec![event("a")]).await.unwrap();

        let mut live = store.subscribe();
        store
            .append("acc-1", AggregateType::Account, 0, vec![event("stale")])
            .await
            .unwrap_err();
        store.append("acc-1", AggregateType::Account, 1, vec![event("b")]).await.unwrap();

        let first = live.next().await.unwrap();
        assert_eq!(first.sequence_number, 2, "only the committed append is broadcast");
    }

    #[tokio::test]
    async fn envelopes_round_trip_exactly() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(&dir);
        let mut live = store.subscribe();
        store
            .append("acc-1", AggregateType::Account, 0, vec![event("Zoë ✉ 日本")])
            .await
            .unwrap();

        let broadcast = live.next().await.unwrap();
        let loaded = store.load_events("acc-1", 0).await.unwrap().remove(0);
        assert_eq!(
            serde_json::to_value(&broadcast).unwrap(),
            serde_json::to_value(&loaded).unwrap()
        );
    }
}
