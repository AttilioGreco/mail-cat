# SQLite Migration — Plan 1: Event Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A production-ready `SqliteEventStore` behind the existing `EventStore` trait, proven by a backend-agnostic conformance suite that runs against both sled and SQLite. Production stays on sled.

**Architecture:** A small `SqliteDb` owns one writer connection (behind a mutex) and a lazily grown pool of read-only connections; all SQLite work runs on `spawn_blocking`. Schema is versioned with `PRAGMA user_version` and an append-only list of SQL migrations. The event log is one `events` table whose rowid is the global position; optimistic concurrency is checked inside an `IMMEDIATE` transaction and backed by `UNIQUE(aggregate_id, sequence)`.

**Tech Stack:** Rust 2024, tokio, rusqlite 0.40 (`bundled`), serde_json, async-stream, tempfile (tests).

**Spec:** `docs/superpowers/specs/2026-10-10-sqlite-migration-design.md` (§1 engine, §2 event store, §8 phases 0–1).

## Plan series

This spec ships as five plans, each written when the previous one is merged (the later ones depend on interfaces this one fixes):

| Plan | Spec phases | Scope |
|---|---|---|
| **1 (this)** | 0, 1 | Commit pending fixes, branch, `SqliteDb`, `SqliteEventStore`, conformance suite |
| 2 | 2 | mc-domain/mc-application: `MessageBodyStore`, body out of `MessageBodyFetched` (legacy payload kept readable for the migrator), status events bus-only, `update_server_counts`, transactional batch projection hook |
| 3 | 3 | `SqliteRepository` (read model schema = migration #2) + read-repository conformance suite incl. missing coverage |
| 4 | 4 | Index-only Tantivy schema, hit hydration + snippets, `suggest` and contact search in SQL, drop contact index |
| 5 | 5–7 | Wiring, `migrate` subcommand (needs an envelope-preserving append), cutover on a copy, delete sled |

## Global Constraints

- `rusqlite = { version = "0.40", features = ["bundled"] }` — no system SQLite dependency.
- Pragmas on the writer: `journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON`, `cache_size=-8000`, `temp_store=MEMORY` (spec §1).
- One writer connection behind `std::sync::Mutex`; readers are read-only connections; all DB calls via `tokio::task::spawn_blocking`. No r2d2, no migration crate.
- Migrations: ordered `&[&str]`, `PRAGMA user_version` = number applied. Append-only.
- Event payload column = `serde_json` of `DomainEvent` (not the envelope).
- `occurred_at` stored as **unix nanoseconds** (`i64`) so envelopes round-trip exactly.
- Production wiring (`mc-server`) is **not** touched in this plan.
- Commits in this repo carry **no** `Co-Authored-By` trailer.
- Comments explain *why*, never restate code (user global instructions).

## Review Focus

1. **Two tasks append to the same aggregate at once** — exactly one wins, the other gets `ConcurrencyConflict`; no duplicate sequence ever lands. (Task 4, `concurrent_appends_never_share_a_sequence`)
2. **Catch-up over a log larger than one page** — `load_all_events` streams every event once, in order, across page boundaries, starting inclusive at the requested position. (Task 4, `load_all_events_streams_across_pages`)
3. **Server restart** — events survive reopen and new global positions continue after the last one instead of restarting. (Task 4, `reopen_keeps_events_and_continues_positions`)
4. **A rejected append** — live subscribers see nothing from it; they only ever see committed events. (Task 4, `rejected_append_broadcasts_nothing`)
5. **Running an older binary on a newer database** — opening fails loudly instead of silently running against an unknown schema. (Task 3, `newer_schema_is_rejected`)

---

### Task 1: Land pending fixes on main and branch

Today's measured fixes are uncommitted on `main`. They are independent of the migration and must not ride along on the migration branch.

**Files:**
- Commit only (no edits): `crates/mc-infrastructure/src/email/imap/client.rs`, `Cargo.toml`, `justfile`, `crates/mc-domain/src/traits/event_store.rs`, `crates/mc-infrastructure/src/persistence/sled/event_store.rs`, `crates/mc-infrastructure/src/persistence/sled/read_repository.rs`, `crates/mc-application/src/services/sync_service.rs`, `crates/mc-server/src/oauth_refresh.rs`, `crates/mc-server/src/wiring.rs`

- [ ] **Step 1: Verify the tree is green**

Run: `cargo test --workspace 2>&1 | grep -E "test result|FAILED"`
Expected: every line `test result: ok`, no `FAILED`.

- [ ] **Step 2: Commit the IMAP pool**

```bash
git add crates/mc-infrastructure/src/email/imap/client.rs
git commit -m "perf(imap): pool connections per account instead of one TLS session per operation

Max 4 pooled sessions per account (semaphore), NOOP revalidation after 30s idle,
sessions returned only on success. IDLE watcher keeps its dedicated connection.
Measured: ~63 → 18 connections per 6 minutes."
```

- [ ] **Step 3: Commit the profiling build**

```bash
git add Cargo.toml justfile
git commit -m "build: add profiling profile and just install-profiling"
```

- [ ] **Step 4: Commit the persistence and sync fixes**

```bash
git add crates/mc-domain/src/traits/event_store.rs \
        crates/mc-infrastructure/src/persistence/sled/event_store.rs \
        crates/mc-infrastructure/src/persistence/sled/read_repository.rs \
        crates/mc-application/src/services/sync_service.rs \
        crates/mc-server/src/oauth_refresh.rs \
        crates/mc-server/src/wiring.rs
git commit -m "perf: stop replaying aggregate history on append; back off periodic sync on rejected credentials

- EventStore::current_sequence (O(1) on sled) replaces load_events(..).len() in
  4 callers; append used to deserialize every body of a message aggregate.
- sled page cache capped at 16 MB (was 1 GB default, ~90% of RSS).
- no fsync per appended event; sled's 500 ms background flush + shutdown flush.
- list_mailboxes keeps AuthenticationFailed typed; periodic sync backs off
  10/20/40 min then hourly per account instead of retrying every 5 min."
```

- [ ] **Step 5: Branch**

Run: `git switch -c feat/sqlite-migration && git status --short`
Expected: on `feat/sqlite-migration`; only `?? README.md` remains (untracked, not ours).

---

### Task 2: Backend-agnostic event store conformance suite

Move the 8 sled event-store tests into a macro that any backend instantiates. The subscribe test switches from the private `notifier` to the trait's `subscribe()` (the receiver is created eagerly inside `subscribe()`, so subscribing before appending still works).

**Files:**
- Create: `crates/mc-infrastructure/src/persistence/event_store_conformance.rs`
- Modify: `crates/mc-infrastructure/src/persistence/mod.rs`
- Modify: `crates/mc-infrastructure/src/persistence/sled/event_store.rs` (replace the `#[cfg(test)] mod tests` block at the end of the file)

**Interfaces:**
- Produces: `event_store_conformance!($make:expr)` — `$make` evaluates to `(guard, store)`; `guard` is kept alive for the test (e.g. a `TempDir`), `store: impl EventStore`. Generates 8 `#[tokio::test]` functions in the invoking module.

- [ ] **Step 1: Write the conformance macro**

`crates/mc-infrastructure/src/persistence/event_store_conformance.rs`:

```rust
//! `EventStore` behaviour every backend must share, instantiated per backend.

macro_rules! event_store_conformance {
    ($make:expr) => {
        use futures::{StreamExt, TryStreamExt};
        use mc_domain::{
            AccountId, AggregateType, DomainError, DomainEvent, EmailAddress, EventEnvelope,
            EventStore, ProviderType,
        };

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
            let (_guard, store) = $make;

            let seq = store
                .append("acc-1", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap();
            assert_eq!(seq, 1);
            let seq = store
                .append("acc-1", AggregateType::Account, 1, vec![make_event(), make_event()])
                .await
                .unwrap();
            assert_eq!(seq, 3);

            let events = store.load_events("acc-1", 0).await.unwrap();
            let sequences: Vec<u64> = events.iter().map(|e| e.sequence_number).collect();
            assert_eq!(sequences, [1, 2, 3]);
            assert_eq!(store.current_sequence("acc-1").await.unwrap(), 3);
            assert_eq!(store.current_sequence("unknown").await.unwrap(), 0);
        }

        #[tokio::test]
        async fn load_events_from_sequence() {
            let (_guard, store) = $make;
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
            let sequences: Vec<u64> = events.iter().map(|e| e.sequence_number).collect();
            assert_eq!(sequences, [2, 3]);
        }

        #[tokio::test]
        async fn concurrency_conflict() {
            let (_guard, store) = $make;
            store
                .append("acc-1", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap();

            let err = store
                .append("acc-1", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap_err();
            assert!(
                matches!(err, DomainError::ConcurrencyConflict { expected: 0, actual: 1 }),
                "got {err:?}"
            );
        }

        #[tokio::test]
        async fn global_position_is_monotonic() {
            let (_guard, store) = $make;
            store
                .append("acc-1", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap();
            store
                .append("acc-2", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap();

            let all: Vec<EventEnvelope> =
                store.load_all_events(None, 0).try_collect().await.unwrap();
            let order: Vec<(u64, &str)> =
                all.iter().map(|e| (e.global_position, e.aggregate_id.as_str())).collect();
            assert_eq!(order, [(1, "acc-1"), (2, "acc-2")]);
        }

        #[tokio::test]
        async fn load_all_events_filtered_by_type() {
            let (_guard, store) = $make;
            store
                .append("acc-1", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap();
            let discarded = DomainEvent::DraftDiscarded { message_id: mc_domain::MessageId::new() };
            store
                .append("msg-1", AggregateType::Message, 0, vec![discarded])
                .await
                .unwrap();

            for (filter, expected) in [
                (Some(AggregateType::Account), 1),
                (Some(AggregateType::Message), 1),
                (None, 2),
            ] {
                let events: Vec<EventEnvelope> =
                    store.load_all_events(filter, 0).try_collect().await.unwrap();
                assert_eq!(events.len(), expected, "filter {filter:?}");
            }
        }

        #[tokio::test]
        async fn empty_append_returns_expected_sequence() {
            let (_guard, store) = $make;
            let seq = store.append("acc-1", AggregateType::Account, 0, vec![]).await.unwrap();
            assert_eq!(seq, 0);
        }

        #[tokio::test]
        async fn separate_aggregates_have_independent_sequences() {
            let (_guard, store) = $make;
            for id in ["acc-1", "acc-2"] {
                store.append(id, AggregateType::Account, 0, vec![make_event()]).await.unwrap();
            }
            for id in ["acc-1", "acc-2"] {
                let events = store.load_events(id, 0).await.unwrap();
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].sequence_number, 1);
            }
        }

        #[tokio::test]
        async fn subscribe_receives_new_events() {
            let (_guard, store) = $make;
            let mut live = store.subscribe();

            store
                .append("acc-1", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap();

            let envelope = live.next().await.expect("should receive event");
            assert_eq!(envelope.aggregate_id, "acc-1");
            assert_eq!(envelope.payload.event_name(), "AccountAdded");
        }
    };
}

pub(crate) use event_store_conformance;
```

- [ ] **Step 2: Register the module**

In `crates/mc-infrastructure/src/persistence/mod.rs`, add at the top:

```rust
#[cfg(test)]
pub(crate) mod event_store_conformance;
```

- [ ] **Step 3: Replace the sled tests with the suite**

In `crates/mc-infrastructure/src/persistence/sled/event_store.rs`, delete the entire `#[cfg(test)] mod tests { … }` block (from `#[cfg(test)]` to the end of the file) and put in its place:

```rust
#[cfg(test)]
mod conformance {
    crate::persistence::event_store_conformance::event_store_conformance!((
        (),
        super::SledEventStore::open_temp().unwrap()
    ));
}
```

- [ ] **Step 4: Run the suite against sled**

Run: `cargo test -p mc-infrastructure sled::event_store 2>&1 | grep -E "test |test result"`
Expected: 8 tests under `persistence::sled::event_store::conformance::`, all `ok`.

- [ ] **Step 5: Lint and commit**

Run: `cargo clippy -p mc-infrastructure --all-targets --message-format=short 2>&1 | grep -E "^error|conformance|sled/event_store"`
Expected: no output.

```bash
git add crates/mc-infrastructure/src/persistence/
git commit -m "test(event-store): backend-agnostic conformance suite, run against sled"
```

---

### Task 3: `SqliteDb` — connection management and schema migrations

**Files:**
- Modify: `Cargo.toml` (workspace dependencies)
- Modify: `crates/mc-infrastructure/Cargo.toml`
- Create: `crates/mc-infrastructure/src/persistence/sqlite/mod.rs`
- Modify: `crates/mc-infrastructure/src/persistence/mod.rs`

**Interfaces:**
- Produces:
  - `pub struct SqliteDb`
  - `SqliteDb::open(path: &Path) -> Result<Arc<SqliteDb>, DomainError>` — creates the file if missing, applies pragmas and pending migrations.
  - `SqliteDb::write<T, F>(self: &Arc<Self>, f: F) -> Result<T, DomainError>` where `F: FnOnce(&mut rusqlite::Connection) -> Result<T, DomainError> + Send + 'static`, `T: Send + 'static`.
  - `SqliteDb::read<T, F>(self: &Arc<Self>, f: F) -> Result<T, DomainError>` where `F: FnOnce(&rusqlite::Connection) -> Result<T, DomainError> + Send + 'static`, `T: Send + 'static`.
  - `pub(crate) const MIGRATIONS: &[&str]` — index 0 creates `events`.
  - Infra errors (open, pragma, migration, join failure) map to `DomainError::Repository`.

- [ ] **Step 1: Add the dependency**

Root `Cargo.toml`, in `[workspace.dependencies]` next to `sled`:

```toml
rusqlite = { version = "0.40", features = ["bundled"] }
```

`crates/mc-infrastructure/Cargo.toml`, in `[dependencies]` after `sled`:

```toml
rusqlite = { workspace = true }
```

- [ ] **Step 2: Write the failing tests**

Create `crates/mc-infrastructure/src/persistence/sqlite/mod.rs` with only the test module for now:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn open_in(dir: &tempfile::TempDir) -> Result<Arc<SqliteDb>, DomainError> {
        SqliteDb::open(&dir.path().join("mail-cat.db"))
    }

    #[tokio::test]
    async fn open_applies_pragmas_and_all_migrations() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_in(&dir).unwrap();

        let (mode, version) = db
            .read(|conn| {
                let mode: String =
                    conn.query_row("PRAGMA journal_mode", [], |r| r.get(0)).map_err(repo_err)?;
                let version: i64 =
                    conn.query_row("PRAGMA user_version", [], |r| r.get(0)).map_err(repo_err)?;
                Ok((mode, version))
            })
            .await
            .unwrap();
        assert_eq!(mode, "wal");
        assert_eq!(version, MIGRATIONS.len() as i64);
    }

    #[tokio::test]
    async fn reopen_does_not_reapply_migrations() {
        let dir = tempfile::tempdir().unwrap();
        drop(open_in(&dir).unwrap());
        // A re-run of migration #1 would fail with "table events already exists".
        open_in(&dir).unwrap();
    }

    #[tokio::test]
    async fn newer_schema_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        drop(open_in(&dir).unwrap());
        rusqlite::Connection::open(dir.path().join("mail-cat.db"))
            .unwrap()
            .pragma_update(None, "user_version", 999)
            .unwrap();

        let err = open_in(&dir).err().expect("newer schema must not open");
        assert!(err.to_string().contains("newer"), "got {err}");
    }

    #[tokio::test]
    async fn write_then_read_from_pool() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_in(&dir).unwrap();
        db.write(|conn| {
            conn.execute_batch("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (42);")
                .map_err(repo_err)
        })
        .await
        .unwrap();

        let v: i64 = db
            .read(|conn| conn.query_row("SELECT v FROM t", [], |r| r.get(0)).map_err(repo_err))
            .await
            .unwrap();
        assert_eq!(v, 42);
    }
}
```

Register it in `crates/mc-infrastructure/src/persistence/mod.rs`:

```rust
pub mod sqlite;
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p mc-infrastructure persistence::sqlite 2>&1 | grep -E "^error" | head -3`
Expected: compile errors — `SqliteDb`, `repo_err`, `MIGRATIONS` not found.

- [ ] **Step 4: Implement `SqliteDb`**

Prepend to `crates/mc-infrastructure/src/persistence/sqlite/mod.rs` (above the test module):

```rust
//! SQLite persistence: one database file, one writer, a pool of readers.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use mc_domain::DomainError;
use rusqlite::{Connection, OpenFlags};

/// Schema migrations, applied in order; `PRAGMA user_version` counts how many
/// have run. Append only — a shipped migration is never edited.
pub(crate) const MIGRATIONS: &[&str] = &[
    // 1: event log. The rowid is the global position.
    "CREATE TABLE events (
        global_position INTEGER PRIMARY KEY,
        event_id        BLOB    NOT NULL,
        aggregate_id    TEXT    NOT NULL,
        aggregate_type  INTEGER NOT NULL,
        sequence        INTEGER NOT NULL,
        event_type      TEXT    NOT NULL,
        occurred_at     INTEGER NOT NULL,
        payload         BLOB    NOT NULL,
        UNIQUE (aggregate_id, sequence)
    );",
];

pub struct SqliteDb {
    path: PathBuf,
    writer: Mutex<Connection>,
    // ponytail: grows to peak read concurrency, which tokio's blocking pool
    // bounds; cap it if idle connections ever show up in memory profiles.
    readers: Mutex<Vec<Connection>>,
}

impl SqliteDb {
    pub fn open(path: &Path) -> Result<Arc<Self>, DomainError> {
        let mut writer = Connection::open(path).map_err(repo_err)?;
        // synchronous=NORMAL: in WAL mode a crash loses at most the last
        // transactions since the checkpoint, never consistency. Everything
        // stored here is re-fetchable from the provider.
        writer
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;
                 PRAGMA foreign_keys = ON;
                 PRAGMA cache_size = -8000;
                 PRAGMA temp_store = MEMORY;",
            )
            .map_err(repo_err)?;
        migrate(&mut writer)?;
        Ok(Arc::new(Self {
            path: path.to_owned(),
            writer: Mutex::new(writer),
            readers: Mutex::default(),
        }))
    }

    pub async fn write<T, F>(self: &Arc<Self>, f: F) -> Result<T, DomainError>
    where
        F: FnOnce(&mut Connection) -> Result<T, DomainError> + Send + 'static,
        T: Send + 'static,
    {
        let db = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            // A panic mid-closure rolls its transaction back on drop, so the
            // connection is still sound; don't let poisoning brick all writes.
            let mut conn = db.writer.lock().unwrap_or_else(PoisonError::into_inner);
            f(&mut conn)
        })
        .await
        .map_err(|e| DomainError::Repository(format!("sqlite write task failed: {e}")))?
    }

    pub async fn read<T, F>(self: &Arc<Self>, f: F) -> Result<T, DomainError>
    where
        F: FnOnce(&Connection) -> Result<T, DomainError> + Send + 'static,
        T: Send + 'static,
    {
        let db = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let conn = db.checkout_reader()?;
            let result = f(&conn);
            db.readers.lock().unwrap_or_else(PoisonError::into_inner).push(conn);
            result
        })
        .await
        .map_err(|e| DomainError::Repository(format!("sqlite read task failed: {e}")))?
    }

    fn checkout_reader(&self) -> Result<Connection, DomainError> {
        if let Some(conn) = self.readers.lock().unwrap_or_else(PoisonError::into_inner).pop() {
            return Ok(conn);
        }
        let conn = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(repo_err)?;
        conn.execute_batch("PRAGMA cache_size = -2000;").map_err(repo_err)?;
        Ok(conn)
    }
}

fn migrate(conn: &mut Connection) -> Result<(), DomainError> {
    let applied: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(repo_err)?;
    let known = MIGRATIONS.len() as i64;
    if applied > known {
        return Err(DomainError::Repository(format!(
            "database schema v{applied} is newer than this binary (v{known})"
        )));
    }
    let tx = conn.transaction().map_err(repo_err)?;
    for (version, sql) in (1..).zip(MIGRATIONS).skip(applied as usize) {
        tx.execute_batch(sql).map_err(repo_err)?;
        tx.pragma_update(None, "user_version", version).map_err(repo_err)?;
    }
    tx.commit().map_err(repo_err)
}

pub(crate) fn repo_err(e: impl std::fmt::Display) -> DomainError {
    DomainError::Repository(e.to_string())
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p mc-infrastructure persistence::sqlite 2>&1 | grep -E "test |test result"`
Expected: 4 tests `ok`.

- [ ] **Step 6: Lint and commit**

Run: `cargo clippy -p mc-infrastructure --all-targets --message-format=short 2>&1 | grep -E "^error|sqlite"`
Expected: no output.

```bash
git add Cargo.toml Cargo.lock crates/mc-infrastructure/Cargo.toml crates/mc-infrastructure/src/persistence/
git commit -m "feat(sqlite): SqliteDb with WAL, writer/reader split and versioned migrations"
```

---

### Task 4: `SqliteEventStore`

**Files:**
- Create: `crates/mc-infrastructure/src/persistence/sqlite/event_store.rs`
- Modify: `crates/mc-infrastructure/src/persistence/sqlite/mod.rs` (add `pub mod event_store;`)
- Modify: `crates/mc-infrastructure/src/persistence/mod.rs` (re-export)
- Modify: `crates/mc-infrastructure/src/lib.rs:14` (re-export next to `SledEventStore`)

**Interfaces:**
- Consumes: `SqliteDb::{open, read, write}`, `repo_err` (Task 3); `event_store_conformance!` (Task 2).
- Produces: `pub struct SqliteEventStore`; `SqliteEventStore::new(db: Arc<SqliteDb>) -> Self`; `impl EventStore for SqliteEventStore`. Exported as `mc_infrastructure::SqliteEventStore` and `mc_infrastructure::persistence::sqlite::SqliteDb`.

- [ ] **Step 1: Write the failing tests**

Create `crates/mc-infrastructure/src/persistence/sqlite/event_store.rs` with the tests only:

```rust
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
```

Add to `crates/mc-infrastructure/src/persistence/sqlite/mod.rs`, below the imports:

```rust
pub mod event_store;
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p mc-infrastructure sqlite::event_store 2>&1 | grep -E "^error" | head -3`
Expected: compile errors — `SqliteEventStore`, `STREAM_PAGE` not found.

- [ ] **Step 3: Implement `SqliteEventStore`**

Prepend to `crates/mc-infrastructure/src/persistence/sqlite/event_store.rs`:

```rust
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
        let envelopes = self
            .db
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
                Ok(envelopes)
            })
            .await?;

        let last_sequence = envelopes.last().map_or(expected_sequence, |e| e.sequence_number);
        for envelope in envelopes {
            // Best-effort: no live subscriber is not an error.
            let _ = self.notifier.send(envelope);
        }
        Ok(last_sequence)
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
```

- [ ] **Step 4: Export the types**

`crates/mc-infrastructure/src/persistence/mod.rs`, after the sled re-exports:

```rust
pub use self::sqlite::event_store::SqliteEventStore;
```

`crates/mc-infrastructure/src/lib.rs:14` — add `SqliteEventStore` to the existing `pub use persistence::{…}` list.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p mc-infrastructure persistence:: 2>&1 | grep -E "test result|FAILED|panicked"`
Expected: `test result: ok` — 8 sled conformance + 8 SQLite conformance + 5 SQLite event-store + 4 `SqliteDb` + the existing read-repository tests; no `FAILED`.

- [ ] **Step 6: Run the whole workspace and lint**

Run: `cargo test --workspace 2>&1 | grep -E "test result|FAILED" && cargo clippy --workspace --all-targets --message-format=short 2>&1 | grep -E "^error|sqlite"`
Expected: all `ok`; clippy prints nothing for `sqlite`.

- [ ] **Step 7: Commit**

```bash
git add crates/mc-infrastructure/src/
git commit -m "feat(sqlite): SqliteEventStore with atomic OCC and paged catch-up stream

Passes the shared EventStore conformance suite. One row per event (sled wrote
two); UNIQUE(aggregate_id, sequence) backs the IMMEDIATE-transaction OCC check;
load_all_events streams 500-row pages; broadcast only after commit."
```
