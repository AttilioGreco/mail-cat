# SQLite Migration — Plan 3: Read Repository Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A production-ready `SqliteRepository` (every read trait, both write repositories, `ReadModelProjector`, `ProjectionCheckpoint`, `BodyBackfillQueue`, `MessageBodyStore`) proven by a read-repository conformance suite that runs against **both** sled and SQLite, plus a transactional batch-projection hook so catch-up commits projection and checkpoint together. Production stays on sled.

**Architecture:** The read model is migration #2 in `MIGRATIONS` (spec §4). `sqlite/repository.rs` holds the query side, codecs, write repositories, checkpoints, bodies (zstd) and the backfill queue; `sqlite/projector.rs` holds one replay-safe arm per event, all running inside the caller's transaction. Conversations are recomputed from `messages` rows instead of counter-patched, so any replay converges. `ReadModelProjector` gains a defaulted `project_batch` that the SQLite backend overrides to write the batch and its checkpoint in one transaction; `ProjectionEventHandler` feeds it batches of ≤ 1000.

**Tech Stack:** Rust 2024, tokio, rusqlite 0.40 (`bundled`, `uuid`), zstd 0.13, serde_json, futures, sled (production backend until plan 5), tempfile (tests).

**Spec:** `docs/superpowers/specs/2026-10-10-sqlite-migration-design.md` (§3 bodies, §4 read model, §8 phase 3, §9 decisions, §10 deferred items).

## Plan series

| Plan | Scope | State |
|---|---|---|
| 1 | `SqliteDb`, `SqliteEventStore`, event-store conformance suite | done |
| 2 | Body out of events, `MessageBodyStore`, status events transient | done |
| **3 (this)** | `SqliteRepository`, read-repository conformance suite, transactional batch projection, replay-safe arms on both backends | |
| 4 | Index-only Tantivy, hit hydration, `suggest`/contacts in SQL | |
| 5 | Wiring, `migrate`, cutover, delete sled | |

## Rulings taken while writing this plan (spec amended in Task 5)

- **R4 — batch hook = defaulted `ReadModelProjector::project_batch(&self, checkpoint: &str, events: Vec<EventEnvelope>) -> Result<bool, DomainError>`.** Default projects one by one and returns `false` (caller saves the checkpoint, as today). SQLite overrides it: batch + checkpoint in one transaction, returns `true`. `ProjectionEventHandler` keeps its `new(projector, checkpoint)` signature, so `mc-server` wiring is untouched and the five existing mocks compile unchanged. Cost if wrong: plan 5 can replace the bool with a combined trait when it rewrites the wiring anyway.
- **R5 — live projection still does not checkpoint.** `append_and_project` and the bus subscriber live in `mc-application`/`mc-server` wiring; moving them onto checkpoints is plan 5's job. This plan closes the *harm* instead: every projector arm is replay-safe on both backends, proven by the conformance test `replaying_the_log_over_live_state_changes_nothing`.
- **R6 — `messages.flags` is JSON (`Vec<Flag>`) plus a `seen INTEGER` column, not a bitset.** `Flag::Custom(String)` (IMAP keywords) does not fit a bitset. `messages_unread` becomes `ON messages(mailbox_id) WHERE seen = 0`.
- **R7 — mailbox unread is maintained symmetrically.** A *newly inserted* unseen `MessageReceived` adds 1 to its mailbox's `unread_messages` (both backends). Today only delete/flag/move adjust it, so replaying `Received`+`Deleted` of the same unread message (routine after every sync's catch-up) undercounts the badge. Server counts still overwrite it after each poll (`update_mailbox_counts` runs after receiving/reconciling). Replaying `Received`/`Sent` for an existing row is a no-op on both backends; `MessageMoved` into the mailbox the row is already in is a no-op.
- **R8 — no `mailboxes.account_id → accounts` foreign key, `messages_mailbox_uid` not UNIQUE.** Catch-up replays events over rows in arbitrary later states; an FK turns replaying `MailboxDiscovered` of a removed account into a boot failure, and a UNIQUE `(mailbox, uid)` turns a UIDVALIDITY reset into one. FKs stay where the parent is checked first (`message_bodies`, `contact_emails`).
- **R9 — SQLite conversations are recomputed from `messages`** (`refresh_conversation`) on every change to a thread; subject from the thread's oldest message, sender/sample/date from the newest; RFC copies counted once. `rebuild_index` = recompute every thread, returns the number of conversations (the trait doc says "events replayed"; there are no thread events to replay when the thread id is a column).
- **R10 — timestamps are unix nanos** (like the event log; exact round-trip), clamped for dates outside 1677–2262. Drafts store `mailbox_id NULL` and read back as the nil uuid; `DraftSaved` uses the envelope's `occurred_at` instead of `Utc::now()` so replays are deterministic.
- **R11 — `SqliteRepository` also implements `AccountWriteRepository` and `ContactWriteRepository`** (owns an `Arc<SqliteEventStore>`), like `SledRepository`: the ported tests use them and plan 5 wires them.

## Global Constraints

- Production backend stays **sled**. No file under `crates/mc-server/` changes in this plan.
- Schema is migration **#2** appended to `MIGRATIONS` in `sqlite/mod.rs`; migration #1 is never edited.
- Schema follows spec §4 verbatim except rulings R6 and R8.
- Bodies: `message_bodies` table, zstd **level 3**, compression in the store, never in the domain. `messages.body_state` is `pending | stored | evicted` (`evicted` unused).
- `put_body` alone never drains the backfill queue; only projecting `MessageBodyFetched` does (plan 2 contract).
- Catch-up/rebuild: batches of **≤ 1000** events; on SQLite each batch and its checkpoint commit in **one** transaction.
- Every projector arm on both backends is replay-safe: projecting any suffix of the log again over live state leaves the read model unchanged.
- `rusqlite` features: `bundled`, `uuid`. IDs stored as 16-byte BLOBs.
- Commits carry **no** `Co-Authored-By` trailer.
- Comments explain *why*, never restate code (user global instructions).

## Review Focus

1. **Boot/post-sync catch-up replays events already projected live**, including `MessageReceived` of a message deleted since — the read model (rows, flags, unread badges, conversations, contacts, backfill) is byte-for-byte what it was. (Task 3, `replaying_the_log_over_live_state_changes_nothing`)
2. **Mailbox badges after a replay of `MailboxDiscovered`** — server counts survive instead of dropping to 0 until the next poll. (Task 3, `replaying_mailbox_discovered_keeps_server_counts`)
3. **Server killed mid catch-up** — a failed batch leaves neither its rows nor its checkpoint behind, so the next boot re-projects exactly that batch. (Task 4, `failed_batch_rolls_back_projection_and_checkpoint`)
4. **The same message in two mailboxes (Gmail labels share one RFC Message-ID)** — the conversation counts it once; on SQLite deleting one copy keeps the conversation with count 1. (Task 3, `rfc_copies_count_once_per_conversation`; Task 4, `deleting_one_rfc_copy_keeps_the_conversation`)
5. **The same UID reused in one mailbox (UIDVALIDITY reset)** — projection does not fail; lookup returns the newest. (Task 3, `find_by_provider_uid_is_scoped_and_survives_uid_reuse`)

---

### Task 1: Transactional batch-projection hook

**Files:**
- Modify: `crates/mc-domain/src/traits/read_model_projector.rs`
- Modify: `crates/mc-application/src/projections/read_model.rs`

**Interfaces:**
- Produces (in `mc_domain::ReadModelProjector`, defaulted):
  ```rust
  async fn project_batch(&self, _checkpoint: &str, events: Vec<EventEnvelope>) -> Result<bool, DomainError>;
  ```
  `true` = the backend committed checkpoint `_checkpoint` = last event's `global_position` together with the batch.
- Produces: `ProjectionEventHandler::{handle, rebuild, catch_up}` unchanged signatures; internally batch through `project_batch`, `BATCH_SIZE = 1000`.

- [ ] **Step 1: Write the failing tests**

Append to `mod tests` in `crates/mc-application/src/projections/read_model.rs`:

```rust
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p mc-application projections::read_model 2>&1 | tail -20`
Expected: compile error `method project_batch is not a member of trait ReadModelProjector`.

- [ ] **Step 3: Add the defaulted trait method**

In `crates/mc-domain/src/traits/read_model_projector.rs`, inside the trait after `project`:

```rust
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
```

- [ ] **Step 4: Batch in the handler**

In `crates/mc-application/src/projections/read_model.rs`:

Replace `use futures::StreamExt;` with `use futures::TryStreamExt;`. After `const PROJECTION_NAME` add:

```rust
/// Events per transaction during catch-up and rebuild (spec §4).
const BATCH_SIZE: usize = 1000;
```

In `handle`, replace the `self.projector.project(...)` call and the following `self.checkpoint.save_checkpoint(...)` call with:

```rust
        self.project_and_checkpoint(vec![envelope.clone()]).await?;
```

Replace the bodies of `rebuild` and `catch_up` and add two private methods:

```rust
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
```

Keep the existing doc comments on `rebuild`/`catch_up`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p mc-application projections::read_model 2>&1 | grep -E "test result|FAILED|panicked"`
Expected: `test result: ok` (6 tests), no `FAILED`.

Run: `cargo test --workspace 2>&1 | grep -E "test result|FAILED"`
Expected: every line `ok`.

- [ ] **Step 6: Commit**

```bash
git add crates/mc-domain/src/traits/read_model_projector.rs crates/mc-application/src/projections/read_model.rs
git commit -m "feat(projection): batch catch-up through ReadModelProjector::project_batch

Batches of 1000; a backend that stores the checkpoint in the same transaction
returns true and the handler skips its own save. Default keeps today's
behaviour, so sled and the mocks are unchanged."
```

---

### Task 2: Read-repository conformance suite (port the sled tests)

Pure move: the 17 backend-agnostic sled read-repository tests become a macro any backend instantiates. No behaviour change; sled must stay green.

**Files:**
- Create: `crates/mc-infrastructure/src/persistence/read_repository_conformance.rs`
- Modify: `crates/mc-infrastructure/src/persistence/mod.rs`
- Modify: `crates/mc-infrastructure/src/persistence/sled/read_repository.rs` (test module at the end, `:2260-3405`)

**Interfaces:**
- Produces: `read_repository_conformance!($make:expr)` — `$make` evaluates to `(guard, store, repo)`: `guard` kept alive for the test, `store: Arc<impl EventStore>` (the repo's own store), `repo` implementing `AccountWriteRepository`, `AccountReadRepository`, `MailboxReadRepository`, `MessageReadRepository`, `ConversationReadRepository`, `ContactWriteRepository`, `ContactReadRepository`, `ReadModelProjector`, `ProjectionCheckpoint`, `BodyBackfillQueue`, `MessageBodyStore`. Generates `#[tokio::test]` fns in the invoking module.

- [ ] **Step 1: Create the macro with the moved fixtures and tests**

`crates/mc-infrastructure/src/persistence/read_repository_conformance.rs`:

```rust
//! Read-model behaviour every backend must share, instantiated per backend.

macro_rules! read_repository_conformance {
    ($make:expr) => {
        use futures::TryStreamExt;
        use mc_domain::{
            AccountId, AccountReadRepository, AccountStatus, AccountWriteRepository, Address,
            AggregateType, AuthConfig, BodyBackfillQueue, ConnectionConfig, ContactEmail,
            ContactEmailLabel, ContactId, ContactReadRepository, ContactSource,
            ConversationReadRepository, DomainEvent, EmailAddress, Envelope, EventEnvelope,
            EventStore, Flag, Identity, MailboxId, MailboxPath, MailboxReadRepository,
            MailboxType, MessageBodyStore, MessageId, MessageReadRepository,
            ProjectionCheckpoint, ProviderType, ProviderUid, ReadModelProjector, ThreadId,
        };
        use mc_shared::pagination::Pagination;

        // <fixtures moved from sled/read_repository.rs, see Step 2>

        // <tests moved from sled/read_repository.rs, see Step 2>
    };
}

pub(crate) use read_repository_conformance;
```

Trim the `use` list to exactly what the moved code (and, after Task 3, the new tests) uses — the build must have no unused-import warnings.

- [ ] **Step 2: Move fixtures and tests into the macro body**

From the `#[cfg(test)] mod tests` of `sled/read_repository.rs`, **cut** these items and paste them into the macro body, unchanged except for the rewrite rules below:

Fixtures: `test_config` (:2278), `test_identity` (:2293), `test_envelope` (:2522), `make_event_envelope` (:2540), `received_event` (:3231), `body_fetched` (:3338), `body_text` (:3348).

Tests (17): `account_write_and_read`, `list_accounts_returns_all`, `load_nonexistent_account_returns_none`, `backfill_queue_newest_first_and_dequeues_on_body`, `account_stats_report_server_total_local_envelopes_and_queued_bodies`, `projection_checkpoint_roundtrip`, `project_account_lifecycle`, `project_mailbox_lifecycle`, `flagging_seen_maintains_mailbox_unread_count`, `project_message_lifecycle`, `project_message_deleted_cleans_conversation_and_thread`, `project_message_move_reindexes_summary`, `project_message_sent_persists_body_appears_in_list_and_threads`, `put_body_sets_body_on_existing_message`, `put_body_on_unknown_message_is_a_noop`, `put_body_alone_does_not_drain_backfill`, `body_fetched_event_keeps_stored_body_and_drains_backfill`, `replaying_received_and_body_fetched_keeps_stored_body`.

Rewrite rules (apply mechanically, nothing else):
1. `let (_es, repo) = make_store_and_repo();` → `let (_guard, _es, repo) = $make;`
2. `async fn body_text(repo: &SledRepository<SledEventStore>, id: &MessageId)` → `async fn body_text(repo: &impl MessageReadRepository, id: &MessageId)`
3. Paths only valid inside sled (`super::…`, `crate::persistence::sled::…`) → the `mc_domain::…` equivalent.

Stay in the sled `tests` module (they poke sled trees): `replaying_received_keeps_legacy_inline_body`, `body_survives_a_concurrent_row_rewrite`. Keep there `make_store_and_repo` and copies of the fixtures those two use (`make_event_envelope`, `received_event`, `body_text` with its sled signature) so the module still compiles.

- [ ] **Step 3: Register the macro and instantiate it for sled**

`crates/mc-infrastructure/src/persistence/mod.rs` — after the `event_store_conformance` line:

```rust
#[cfg(test)]
pub(crate) mod read_repository_conformance;
```

Append to `sled/read_repository.rs`:

```rust
#[cfg(test)]
mod conformance {
    use std::sync::Arc;

    use super::SledRepository;
    use crate::persistence::sled::event_store::SledEventStore;

    fn make() -> ((), Arc<SledEventStore>, SledRepository<SledEventStore>) {
        let db = sled::Config::new().temporary(true).open().unwrap();
        let store = Arc::new(SledEventStore::new(db.clone()));
        ((), store.clone(), SledRepository::new(db, store))
    }

    crate::persistence::read_repository_conformance::read_repository_conformance!(make());
}
```

- [ ] **Step 4: Run the suite on sled**

Run: `cargo test -p mc-infrastructure persistence::sled::read_repository 2>&1 | grep -E "test result|FAILED|warning: unused"`
Expected: `test result: ok`, 19 tests (17 under `conformance::`, 2 under `tests::`), no `FAILED`, no unused warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/mc-infrastructure/src/persistence/read_repository_conformance.rs \
        crates/mc-infrastructure/src/persistence/mod.rs \
        crates/mc-infrastructure/src/persistence/sled/read_repository.rs
git commit -m "test(read-model): backend-agnostic read repository conformance suite, run against sled"
```

---

### Task 3: Missing coverage + replay-safe sled arms

New conformance tests for the uncovered queries and for replay-safety; the replay tests fail on today's sled, so the same task makes the four sled arms replay-safe (R7).

**Files:**
- Modify: `crates/mc-infrastructure/src/persistence/read_repository_conformance.rs`
- Modify: `crates/mc-infrastructure/src/persistence/sled/read_repository.rs` (arms `MailboxDiscovered` :606-639, `MessageReceived` :724-920, `MessageMoved` :1021-1056, `MessageSent` :1109-1240)

**Interfaces:**
- Consumes: `read_repository_conformance!` (Task 2).
- Produces: sled projector semantics identical to the SQLite one (Task 4): `Received`/`Sent` on an existing row are no-ops; a new unseen `Received` adds 1 to its mailbox's unread; `MailboxDiscovered` keeps counts; a move into the current mailbox is a no-op.

- [ ] **Step 1: Add fixtures to the macro body**

```rust
        /// Fixed clock: minutes after an arbitrary epoch, so orderings are exact.
        fn at(minutes: i64) -> chrono::DateTime<chrono::Utc> {
            chrono::DateTime::from_timestamp(1_760_000_000 + minutes * 60, 0).unwrap()
        }

        /// Projection ignores aggregate id and sequence.
        fn env_of(event: DomainEvent) -> EventEnvelope {
            make_event_envelope(1, "agg", event.aggregate_type(), 1, event)
        }

        async fn project_all(
            repo: &impl ReadModelProjector,
            events: impl IntoIterator<Item = DomainEvent>,
        ) {
            for event in events {
                repo.project(&env_of(event)).await.unwrap();
            }
        }

        /// Appends to the real store (one aggregate per type) and returns the
        /// log as the store numbered it.
        async fn append_all<ES: EventStore>(store: &ES, events: Vec<DomainEvent>) -> Vec<EventEnvelope> {
            for event in events {
                let ty = event.aggregate_type();
                let id = ty.to_string();
                let seq = store.current_sequence(&id).await.unwrap();
                store.append(&id, ty, seq, vec![event]).await.unwrap();
            }
            store.load_all_events(None, 0).try_collect().await.unwrap()
        }

        fn received(
            account_id: AccountId,
            mailbox_id: MailboxId,
            uid: u32,
            rfc: Option<&str>,
            references: &[&str],
            seen: bool,
            received_at: chrono::DateTime<chrono::Utc>,
        ) -> (MessageId, DomainEvent) {
            let message_id = MessageId::new();
            let mut envelope = test_envelope();
            envelope.date = received_at;
            let event = DomainEvent::MessageReceived {
                message_id,
                account_id,
                mailbox_id,
                provider_uid: ProviderUid::new(uid),
                message_rfc_id: rfc.map(str::to_owned),
                in_reply_to: None,
                references: references.iter().map(|r| r.to_string()).collect(),
                envelope,
                flags: if seen { vec![Flag::Seen] } else { vec![] },
                size_bytes: 100,
                has_attachments: false,
                received_at,
            };
            (message_id, event)
        }

        fn discovered(account_id: AccountId, mailbox_id: MailboxId, path: &str) -> DomainEvent {
            DomainEvent::MailboxDiscovered {
                mailbox_id,
                account_id,
                path: MailboxPath::parse(path, '/').unwrap(),
                mailbox_type: MailboxType::Custom,
            }
        }

        fn flagged(message_id: MessageId, mailbox_id: MailboxId, add: Vec<Flag>, remove: Vec<Flag>) -> DomainEvent {
            DomainEvent::MessageFlagged { message_id, mailbox_id, flags_added: add, flags_removed: remove }
        }

        fn email(raw: &str) -> EmailAddress {
            EmailAddress::new(raw).unwrap()
        }

        fn first_page() -> Pagination {
            Pagination::new(1, 200)
        }

        fn ids<T>(items: &[T], id: impl Fn(&T) -> MessageId) -> Vec<MessageId> {
            items.iter().map(id).collect()
        }

        /// Everything a user can see of one account, rendered for equality.
        async fn snapshot<R>(repo: &R, account_id: &AccountId, mailboxes: &[MailboxId]) -> String
        where
            R: AccountReadRepository
                + MailboxReadRepository
                + MessageReadRepository
                + ConversationReadRepository
                + ContactReadRepository
                + BodyBackfillQueue,
        {
            let page = first_page();
            let mut out = format!("{:?}\n", repo.list_accounts().await.unwrap());
            for mailbox in mailboxes {
                out += &format!("{:?}\n", repo.get_mailbox(mailbox).await.unwrap());
                out += &format!("{:?}\n", repo.list_messages(mailbox, &page).await.unwrap().items);
                out += &format!("{:?}\n", repo.find_unread_by_mailbox(mailbox).await.unwrap());
                out += &format!(
                    "{:?}\n",
                    repo.list_conversations(account_id, Some(mailbox), &page).await.unwrap().items
                );
            }
            out += &format!("{:?}\n", repo.scan_all_messages(Some(account_id)).await.unwrap());
            out += &format!("{:?}\n", repo.list_conversations(account_id, None, &page).await.unwrap().items);
            out += &format!("{:?}\n", repo.list_contacts(&page).await.unwrap().items);
            out += &format!("{:?}\n", repo.count_pending_backfill(account_id).await.unwrap());
            out
        }
```

- [ ] **Step 2: Add the query-coverage tests**

```rust
        #[tokio::test]
        async fn list_messages_pages_newest_first() {
            let (_guard, _es, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            let mut msgs = Vec::new();
            for minute in 0..5 {
                let (id, event) = received(account, inbox, minute as u32 + 1, None, &[], false, at(minute));
                repo.project(&env_of(event)).await.unwrap();
                msgs.push(id);
            }

            let page = repo.list_messages(&inbox, &Pagination::new(1, 2)).await.unwrap();
            assert_eq!(page.total_count, 5);
            assert_eq!(ids(&page.items, |m| m.id), [msgs[4], msgs[3]]);
            let last = repo.list_messages(&inbox, &Pagination::new(3, 2)).await.unwrap();
            assert_eq!(ids(&last.items, |m| m.id), [msgs[0]]);
        }

        #[tokio::test]
        async fn get_thread_returns_the_whole_thread_oldest_first() {
            let (_guard, _es, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            let (root, e_root) = received(account, inbox, 1, Some("<root@x>"), &[], false, at(0));
            let (late, e_late) = received(account, inbox, 3, Some("<late@x>"), &["<root@x>"], false, at(20));
            let (mid, e_mid) = received(account, inbox, 2, Some("<mid@x>"), &["<root@x>"], false, at(10));
            let (_other, e_other) = received(account, inbox, 4, Some("<other@x>"), &[], false, at(5));
            project_all(&repo, [e_root, e_late, e_mid, e_other]).await;

            let thread = repo.get_thread(&late).await.unwrap();
            assert_eq!(ids(&thread, |m| m.id), [root, mid, late]);
            assert!(repo.get_thread(&MessageId::new()).await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn find_by_provider_uid_is_scoped_and_survives_uid_reuse() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, archive) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (old, e_old) = received(account, inbox, 7, None, &[], false, at(0));
            let (_elsewhere, e_elsewhere) = received(account, archive, 7, None, &[], false, at(1));
            // UIDVALIDITY reset: the server hands out uid 7 again in the same mailbox.
            let (new, e_new) = received(account, inbox, 7, None, &[], false, at(2));
            project_all(&repo, [e_old, e_elsewhere, e_new]).await;

            let found = repo.find_by_provider_uid(&inbox, 7).await.unwrap().unwrap();
            assert_eq!(found.id, new);
            assert_eq!(found.provider_uid, Some(ProviderUid::new(7)));
            assert!(repo.find_by_provider_uid(&inbox, 8).await.unwrap().is_none());
            assert_ne!(found.id, old);
        }

        #[tokio::test]
        async fn find_unread_by_mailbox_skips_seen_and_other_mailboxes() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, archive) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (unread, e1) = received(account, inbox, 1, None, &[], false, at(0));
            let (_read, e2) = received(account, inbox, 2, None, &[], true, at(1));
            let (_elsewhere, e3) = received(account, archive, 3, None, &[], false, at(2));
            project_all(&repo, [e1, e2, e3]).await;

            let found = repo.find_unread_by_mailbox(&inbox).await.unwrap();
            assert_eq!(ids(&found, |m| m.id), [unread]);

            project_all(&repo, [flagged(unread, inbox, vec![Flag::Seen], vec![])]).await;
            assert!(repo.find_unread_by_mailbox(&inbox).await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn list_conversations_scoped_to_a_mailbox() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, archive) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (root, e_root) = received(account, inbox, 1, Some("<root@x>"), &[], false, at(0));
            let (reply, e_reply) = received(account, archive, 2, Some("<reply@x>"), &["<root@x>"], true, at(10));
            let (solo, e_solo) = received(account, inbox, 3, Some("<solo@x>"), &[], true, at(5));
            project_all(&repo, [e_root, e_reply, e_solo]).await;
            let page = first_page();

            let all = repo.list_conversations(&account, None, &page).await.unwrap();
            assert_eq!(all.total_count, 2);
            let stats: Vec<_> = all
                .items
                .iter()
                .map(|c| (c.thread_id.clone(), c.message_count, c.unread_count, c.sample_message_id))
                .collect();
            assert_eq!(
                stats,
                [(ThreadId::new("<root@x>"), 2, 1, reply), (ThreadId::new("<solo@x>"), 1, 0, solo)]
            );

            let in_inbox = repo.list_conversations(&account, Some(&inbox), &page).await.unwrap();
            assert_eq!(in_inbox.total_count, 2);
            let stats: Vec<_> = in_inbox
                .items
                .iter()
                .map(|c| (c.thread_id.clone(), c.message_count, c.unread_count, c.sample_message_id))
                .collect();
            assert_eq!(
                stats,
                [(ThreadId::new("<solo@x>"), 1, 0, solo), (ThreadId::new("<root@x>"), 1, 1, root)]
            );
            assert_eq!(in_inbox.items[1].last_message_at, at(0));
            assert_eq!(in_inbox.items[1].subject, "Hello World");
            assert_eq!(in_inbox.items[1].latest_sender, "Sender");

            let in_archive = repo.list_conversations(&account, Some(&archive), &page).await.unwrap();
            assert_eq!(in_archive.total_count, 1);
            assert_eq!(in_archive.items[0].sample_message_id, reply);
            assert_eq!(in_archive.items[0].unread_count, 0);
        }

        #[tokio::test]
        async fn rfc_copies_count_once_per_conversation() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, all_mail) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (_a, e_a) = received(account, inbox, 1, Some("<same@x>"), &[], false, at(0));
            let (_b, e_b) = received(account, all_mail, 9, Some("<same@x>"), &[], false, at(0));
            project_all(&repo, [e_a, e_b]).await;

            let conv = repo
                .get_conversation(&ThreadId::new("<same@x>"), &account)
                .await
                .unwrap()
                .unwrap();
            assert_eq!((conv.message_count, conv.unread_count), (1, 1));
        }

        #[tokio::test]
        async fn rebuild_index_restores_conversations() {
            let (_guard, store, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            let (_root, e_root) = received(account, inbox, 1, Some("<root@x>"), &[], false, at(0));
            let (reply, e_reply) = received(account, inbox, 2, Some("<reply@x>"), &["<root@x>"], false, at(10));
            let log = append_all(
                store.as_ref(),
                vec![e_root, e_reply, flagged(reply, inbox, vec![Flag::Seen], vec![])],
            )
            .await;
            for event in &log {
                repo.project(event).await.unwrap();
            }
            let thread = ThreadId::new("<root@x>");
            let before = repo.get_conversation(&thread, &account).await.unwrap().unwrap();

            assert!(repo.rebuild_index().await.unwrap() > 0);

            let after = repo.get_conversation(&thread, &account).await.unwrap().unwrap();
            assert_eq!((after.message_count, after.unread_count), (2, 1));
            assert_eq!(format!("{after:?}"), format!("{before:?}"));
        }

        #[tokio::test]
        async fn drafts_stay_out_of_mailbox_views() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, draft) = (AccountId::new(), MailboxId::new(), MessageId::new());
            project_all(
                &repo,
                [DomainEvent::DraftSaved {
                    message_id: draft,
                    account_id: account,
                    envelope: test_envelope(),
                    body_text: Some("draft".into()),
                    body_html: None,
                }],
            )
            .await;

            let stored = repo.get_message(&draft).await.unwrap().unwrap();
            assert_eq!(stored.body_text.as_deref(), Some("draft"));
            assert!(stored.flags.contains(&Flag::Draft));
            assert_eq!(repo.list_messages(&inbox, &first_page()).await.unwrap().total_count, 0);
            assert_eq!(repo.count_pending_backfill(&account).await.unwrap(), 0);

            project_all(&repo, [DomainEvent::DraftDiscarded { message_id: draft }]).await;
            assert!(repo.get_message(&draft).await.unwrap().is_none());
        }

        #[tokio::test]
        async fn contact_lifecycle_and_lookup() {
            let (_guard, _es, repo) = $make;
            let (contact, account) = (ContactId::new(), AccountId::new());
            project_all(
                &repo,
                [DomainEvent::ContactAdded {
                    contact_id: contact,
                    display_name: "Ada Lovelace".into(),
                    emails: vec![
                        ContactEmail::primary(email("ada@example.com")),
                        ContactEmail::new(email("ada@work.example.com"), ContactEmailLabel::Work, false),
                    ],
                    source: ContactSource::Local,
                    external_id: None,
                    account_scope: Some(account),
                }],
            )
            .await;

            let summary = repo.get_contact(&contact).await.unwrap().unwrap();
            assert_eq!(summary.display_name, "Ada Lovelace");
            assert_eq!(summary.primary_email, email("ada@example.com"));
            assert_eq!(summary.emails.len(), 2);
            assert_eq!(summary.organization, None);
            assert_eq!(summary.account_scope, Some(account));
            let found = repo.find_by_email(&email("ada@work.example.com")).await.unwrap();
            assert_eq!(found.iter().map(|c| c.id).collect::<Vec<_>>(), [contact]);
            assert_eq!(repo.find_by_email_in_account(&email("ada@example.com"), &account).await.unwrap().len(), 1);
            assert!(repo.find_by_email_in_account(&email("ada@example.com"), &AccountId::new()).await.unwrap().is_empty());
            assert_eq!(repo.search_contacts("lovelace", 10).await.unwrap().len(), 1);
            assert_eq!(repo.search_contacts("ADA@EXAMPLE", 10).await.unwrap().len(), 1);
            assert!(repo.search_contacts("nobody", 10).await.unwrap().is_empty());

            project_all(
                &repo,
                [DomainEvent::ContactUpdated {
                    contact_id: contact,
                    display_name: Some("Ada King".into()),
                    emails: Some(vec![ContactEmail::primary(email("ada@king.example.com"))]),
                    phones: None,
                    organization: Some(Some("Analytical".into())),
                    notes: None,
                }],
            )
            .await;

            let summary = repo.get_contact(&contact).await.unwrap().unwrap();
            assert_eq!(summary.display_name, "Ada King");
            assert_eq!(summary.primary_email, email("ada@king.example.com"));
            assert_eq!(summary.organization.as_deref(), Some("Analytical"));
            assert!(repo.find_by_email(&email("ada@work.example.com")).await.unwrap().is_empty());
            assert_eq!(repo.find_by_email(&email("ada@king.example.com")).await.unwrap().len(), 1);
            assert_eq!(repo.list_contacts(&first_page()).await.unwrap().total_count, 1);

            project_all(&repo, [DomainEvent::ContactRemoved { contact_id: contact }]).await;
            assert!(repo.get_contact(&contact).await.unwrap().is_none());
            assert!(repo.find_by_email(&email("ada@king.example.com")).await.unwrap().is_empty());
            assert_eq!(repo.list_contacts(&first_page()).await.unwrap().total_count, 0);
        }

        #[tokio::test]
        async fn duplicate_candidates_and_merge() {
            let (_guard, _es, repo) = $make;
            let (account, other_account) = (AccountId::new(), AccountId::new());
            let (c1, c2, c3) = (ContactId::new(), ContactId::new(), ContactId::new());
            let added = |id: ContactId, emails: Vec<ContactEmail>, scope: AccountId| DomainEvent::ContactAdded {
                contact_id: id,
                display_name: id.to_string(),
                emails,
                source: ContactSource::LocalEmail,
                external_id: None,
                account_scope: Some(scope),
            };
            let shared = || ContactEmail::primary(email("shared@example.com"));
            project_all(
                &repo,
                [
                    added(c1, vec![shared(), ContactEmail::new(email("one@example.com"), ContactEmailLabel::Home, false)], account),
                    added(c2, vec![shared()], account),
                    added(c3, vec![shared()], other_account),
                ],
            )
            .await;
            let pair_ids = |pairs: Vec<(mc_domain::ContactSummary, mc_domain::ContactSummary)>| {
                let mut out: Vec<(String, String)> = pairs
                    .into_iter()
                    .map(|(a, b)| {
                        let (a, b) = (a.id.to_string(), b.id.to_string());
                        if a < b { (a, b) } else { (b, a) }
                    })
                    .collect();
                out.sort();
                out
            };
            let sorted = |a: ContactId, b: ContactId| {
                let (a, b) = (a.to_string(), b.to_string());
                if a < b { (a, b) } else { (b, a) }
            };

            let scoped = pair_ids(repo.find_duplicate_candidates(Some(&account)).await.unwrap());
            assert_eq!(scoped, [sorted(c1, c2)]);
            let mut everywhere = vec![sorted(c1, c2), sorted(c1, c3), sorted(c2, c3)];
            everywhere.sort();
            assert_eq!(pair_ids(repo.find_duplicate_candidates(None).await.unwrap()), everywhere);

            project_all(
                &repo,
                [DomainEvent::ContactMerged {
                    kept_id: c1,
                    discarded_id: c2,
                    merged_emails: vec![shared(), ContactEmail::new(email("one@example.com"), ContactEmailLabel::Home, false)],
                }],
            )
            .await;
            assert!(repo.get_contact(&c2).await.unwrap().is_none());
            let mut holders: Vec<String> = repo
                .find_by_email(&email("shared@example.com"))
                .await
                .unwrap()
                .iter()
                .map(|c| c.id.to_string())
                .collect();
            holders.sort();
            let mut expected = vec![c1.to_string(), c3.to_string()];
            expected.sort();
            assert_eq!(holders, expected);
            assert!(repo.find_duplicate_candidates(Some(&account)).await.unwrap().is_empty());
        }
```

- [ ] **Step 3: Add the replay-safety tests**

```rust
        #[tokio::test]
        async fn replaying_mailbox_discovered_keeps_server_counts() {
            let (_guard, _es, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            project_all(
                &repo,
                [
                    discovered(account, inbox, "INBOX"),
                    DomainEvent::MailboxCountsUpdated { mailbox_id: inbox, total_messages: 10, unread_messages: 4 },
                    discovered(account, inbox, "INBOX"),
                ],
            )
            .await;

            let mailbox = repo.get_mailbox(&inbox).await.unwrap().unwrap();
            assert_eq!((mailbox.total_messages, mailbox.unread_messages), (10, 4));
        }

        /// Boot and post-sync catch-up re-project everything since the last
        /// checkpoint on top of what the live path already projected.
        #[tokio::test]
        async fn replaying_the_log_over_live_state_changes_nothing() {
            let (_guard, store, repo) = $make;
            let (account, inbox, archive) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (root, e_root) = received(account, inbox, 1, Some("<root@x>"), &[], false, at(0));
            let (reply, e_reply) = received(account, inbox, 2, Some("<reply@x>"), &["<root@x>"], false, at(10));
            let (gone, e_gone) = received(account, inbox, 3, Some("<gone@x>"), &[], false, at(20));
            let (sent, contact) = (MessageId::new(), ContactId::new());
            let log = append_all(
                store.as_ref(),
                vec![
                    DomainEvent::AccountAdded {
                        account_id: account,
                        name: "Work".into(),
                        email: email("me@example.com"),
                        provider_type: ProviderType::Imap,
                    },
                    discovered(account, inbox, "INBOX"),
                    discovered(account, archive, "Archive"),
                    e_root,
                    e_reply,
                    e_gone,
                    DomainEvent::MessageBodyFetched { message_id: root, attachments: vec![] },
                    flagged(root, inbox, vec![Flag::Seen], vec![]),
                    DomainEvent::MessageMoved { message_id: reply, from_mailbox_id: inbox, to_mailbox_id: archive },
                    DomainEvent::MessageDeleted { message_id: gone, mailbox_id: inbox, permanently: false },
                    DomainEvent::MessageSent {
                        message_id: sent,
                        account_id: account,
                        mailbox_id: archive,
                        envelope: test_envelope(),
                        in_reply_to: Some("<root@x>".into()),
                        references: vec!["<root@x>".into()],
                        body_text: Some("thanks".into()),
                        body_html: None,
                        sent_at: at(30),
                    },
                    flagged(sent, archive, vec![], vec![Flag::Seen]),
                    DomainEvent::ContactAdded {
                        contact_id: contact,
                        display_name: "Ada".into(),
                        emails: vec![ContactEmail::primary(email("ada@example.com"))],
                        source: ContactSource::LocalEmail,
                        external_id: None,
                        account_scope: Some(account),
                    },
                    DomainEvent::ContactUpdated {
                        contact_id: contact,
                        display_name: Some("Ada L.".into()),
                        emails: None,
                        phones: None,
                        organization: Some(Some("Analytical".into())),
                        notes: None,
                    },
                ],
            )
            .await;
            for event in &log {
                repo.project(event).await.unwrap();
            }
            repo.put_body(&root, Some("hello".into()), None).await.unwrap();
            // Server counts are transient: they reach the read model only through project().
            project_all(
                &repo,
                [DomainEvent::MailboxCountsUpdated { mailbox_id: inbox, total_messages: 10, unread_messages: 4 }],
            )
            .await;
            let live = snapshot(&repo, &account, &[inbox, archive]).await;

            for event in &log {
                repo.project(event).await.unwrap();
            }

            assert_eq!(snapshot(&repo, &account, &[inbox, archive]).await, live);
            assert_eq!(
                repo.get_mailbox(&inbox).await.unwrap().unwrap().unread_messages,
                4,
                "replaying the received+deleted pair must not move the badge"
            );
        }
```

- [ ] **Step 4: Run the suite on sled to see the replay tests fail**

Run: `cargo test -p mc-infrastructure persistence::sled::read_repository::conformance 2>&1 | grep -E "test |test result"`
Expected: `replaying_mailbox_discovered_keeps_server_counts` FAILS (counts `(0, 0)`), `replaying_the_log_over_live_state_changes_nothing` FAILS (snapshot differs — flags/unread reset by the `MessageReceived` replay). The query-coverage tests pass. Any other failure is a test bug — fix the test, not sled.

- [ ] **Step 5: Make the sled arms replay-safe**

In `sled/read_repository.rs`, `ReadModelProjector::project`:

**`MailboxDiscovered`** — replace the `let mailbox = Mailbox { … };` literal and the `let mailbox_tree = self.tree(MAILBOXES_TREE)?;` line after it with:

```rust
                let mailbox_tree = self.tree(MAILBOXES_TREE)?;
                // Counts are transient (never in the log), so a replay must keep them.
                let previous: Option<Mailbox> = mailbox_tree
                    .get(mailbox_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                    .map(|bytes| serde_json::from_slice(&bytes))
                    .transpose()
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                let mut mailbox = previous.unwrap_or_else(|| Mailbox {
                    id: *mailbox_id,
                    account_id: *account_id,
                    path: path.clone(),
                    display_name: String::new(),
                    mailbox_type: *mailbox_type,
                    flags: Vec::new(),
                    total_messages: 0,
                    unread_messages: 0,
                    uid_validity: None,
                    uid_next: None,
                });
                mailbox.account_id = *account_id;
                mailbox.path = path.clone();
                mailbox.display_name = path.name().to_string();
                mailbox.mailbox_type = *mailbox_type;
```

**`MessageReceived`** — replace the block from `let msg_tree = self.tree(MESSAGES_TREE)?;` through the backfill `insert(...)` (just before `debug!(message_id = %message_id, "projected MessageReceived");`) so that the row, summary and backfill writes only happen for a new message, and a new unread message bumps its mailbox:

```rust
                let msg_tree = self.tree(MESSAGES_TREE)?;
                // Message ids are minted per receipt, so an existing row means a
                // replay: it already reflects every later event (flags, body,
                // attachments, mailbox) and must not be reset from this one.
                let existing: Option<Message> = msg_tree
                    .get(message_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                    .map(|bytes| serde_json::from_slice(&bytes))
                    .transpose()
                    .map_err(|e| DomainError::Serialization(e.to_string()))?;
                let is_unread = match &existing {
                    Some(message) => !message.flags.contains(&Flag::Seen),
                    None => !flags.contains(&Flag::Seen),
                };
                if existing.is_none() {
                    // <the existing `let message = Message { … }` literal, with
                    //  `body_text: None, body_html: None`, then the msg_tree insert,
                    //  the MessageSummary block and the backfill block — unchanged>

                    // Symmetric with MessageDeleted's decrement, so replaying a
                    // received+deleted pair nets zero (server counts overwrite
                    // this after each poll anyway).
                    if is_unread {
                        self.adjust_mailbox_unread(mailbox_id, 1)?;
                    }
                }
```

Delete the old `let (body_text, body_html) = match msg_tree.get(...) { … };` block (a replay no longer rewrites the row, so the legacy inline body is kept by construction — `replaying_received_keeps_legacy_inline_body` must stay green). In the conversation block below, delete the line `let is_unread = !flags.contains(&Flag::Seen);` — it now uses the `is_unread` computed above (the existing row's flags on replay, which is what `rebuild_index` needs).

**`MessageMoved`** — change the unread condition to skip a move into the mailbox the row is already in:

```rust
                    // A replay finds the row already in `to`: nothing moves.
                    if old_mailbox_id != *to_mailbox_id && !message.flags.contains(&Flag::Seen) {
```

**`MessageSent`** — wrap the `messages` row write and the `message_summaries` write in a guard (the conversation block is already guarded by `message_to_thread`):

```rust
                let tree = self.tree(MESSAGES_TREE)?;
                // Sent ids are fresh per send; an existing row means a replay
                // that must not reset flags changed since (e.g. marked unread).
                if !tree
                    .contains_key(message_id.to_string().as_bytes())
                    .map_err(|e| DomainError::Repository(e.to_string()))?
                {
                    // <existing Message literal + tree.insert + MessageSummary block, unchanged>
                }
```

- [ ] **Step 6: Fix the moved unread test's event order**

In `flagging_seen_maintains_mailbox_unread_count` (now in the macro), reorder the setup array to `MailboxDiscovered`, `MessageReceived`, `MailboxCountsUpdated` — the order sync emits them (`update_mailbox_counts` runs after receiving). With R7 the receipt adds 1, then the server count sets 1; the assertions stay unchanged.

- [ ] **Step 7: Run everything on sled**

Run: `cargo test -p mc-infrastructure persistence::sled 2>&1 | grep -E "test result|FAILED"`
Expected: all `ok`.

Run: `cargo test --workspace 2>&1 | grep -E "test result|FAILED"`
Expected: all `ok`.

- [ ] **Step 8: Commit**

```bash
git add crates/mc-infrastructure/src/persistence/read_repository_conformance.rs \
        crates/mc-infrastructure/src/persistence/sled/read_repository.rs
git commit -m "fix(sled): replay-safe projector arms; conformance coverage for queries and replays

Catch-up re-projects everything since the last checkpoint over live state.
MessageReceived/MessageSent replays no longer reset flags (badge and unread
drift), MailboxDiscovered keeps server counts, a received unread message
bumps its mailbox so received+deleted replays net zero.

New conformance tests: paging, thread order, uid lookup and reuse, unread
by mailbox, per-mailbox conversations, RFC copies, rebuild_index, drafts,
contacts, duplicates/merge, full-log replay."
```

---

### Task 4: `SqliteRepository`

**Files:**
- Modify: `Cargo.toml` (workspace deps), `crates/mc-infrastructure/Cargo.toml`
- Modify: `crates/mc-infrastructure/src/persistence/sqlite/mod.rs` (migration #2, modules)
- Create: `crates/mc-infrastructure/src/persistence/sqlite/repository.rs`
- Create: `crates/mc-infrastructure/src/persistence/sqlite/projector.rs`
- Modify: `crates/mc-infrastructure/src/persistence/mod.rs`, `crates/mc-infrastructure/src/lib.rs` (exports)

**Interfaces:**
- Consumes: `SqliteDb::{open, read, write}`, `repo_err`, `SqliteEventStore::new` (plan 1); `ReadModelProjector::project_batch` (Task 1); `read_repository_conformance!` (Tasks 2–3).
- Produces: `pub struct SqliteRepository` with `pub fn new(db: Arc<SqliteDb>, events: Arc<SqliteEventStore>) -> Self`, `Clone`, implementing every trait listed in Task 2's macro interface; exported as `mc_infrastructure::SqliteRepository`.

- [ ] **Step 1: Dependencies**

Root `Cargo.toml`, `[workspace.dependencies]`:

```toml
rusqlite = { version = "0.40", features = ["bundled", "uuid"] }
zstd = "0.13"
```

`crates/mc-infrastructure/Cargo.toml`, after `rusqlite`:

```toml
zstd = { workspace = true }
```

Run: `cargo build -p mc-infrastructure 2>&1 | tail -3`
Expected: builds.

- [ ] **Step 2: Schema as migration #2**

In `sqlite/mod.rs`, append to `MIGRATIONS` after migration 1:

```rust
    // 2: read model (spec §4). No FK from mailboxes to accounts and no UNIQUE
    // (mailbox, uid): catch-up replays events over rows in any later state,
    // and either constraint would turn a harmless replay into a boot failure.
    "CREATE TABLE accounts (
        id            BLOB PRIMARY KEY,
        name          TEXT NOT NULL,
        email         TEXT NOT NULL,
        provider_type TEXT NOT NULL,
        status        TEXT NOT NULL,
        total_unread  INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE mailboxes (
        id              BLOB PRIMARY KEY,
        account_id      BLOB NOT NULL,
        path            TEXT NOT NULL,
        delimiter       TEXT NOT NULL,
        display_name    TEXT NOT NULL,
        mailbox_type    TEXT NOT NULL,
        flags           TEXT NOT NULL,
        total_messages  INTEGER NOT NULL DEFAULT 0,
        unread_messages INTEGER NOT NULL DEFAULT 0,
        uid_validity    INTEGER,
        uid_next        INTEGER
    );
    CREATE INDEX mailboxes_account ON mailboxes(account_id);
    CREATE TABLE messages (
        id              BLOB PRIMARY KEY,
        account_id      BLOB NOT NULL,
        mailbox_id      BLOB,
        provider_uid    INTEGER,
        rfc_message_id  TEXT,
        in_reply_to     TEXT,
        refs            TEXT NOT NULL,
        thread_id       TEXT,
        subject         TEXT NOT NULL,
        from_name       TEXT,
        from_email      TEXT NOT NULL,
        addresses       TEXT NOT NULL,
        flags           TEXT NOT NULL,
        seen            INTEGER NOT NULL,
        size_bytes      INTEGER NOT NULL,
        has_attachments INTEGER NOT NULL,
        attachments     TEXT NOT NULL,
        sent_at         INTEGER NOT NULL,
        received_at     INTEGER NOT NULL,
        internal_date   INTEGER NOT NULL,
        body_state      TEXT NOT NULL DEFAULT 'pending'
    );
    CREATE INDEX messages_mailbox_date ON messages(mailbox_id, received_at DESC);
    CREATE INDEX messages_mailbox_uid ON messages(mailbox_id, provider_uid);
    CREATE INDEX messages_thread ON messages(account_id, thread_id, received_at);
    CREATE INDEX messages_rfc ON messages(account_id, rfc_message_id);
    CREATE INDEX messages_backfill ON messages(account_id, received_at DESC) WHERE body_state = 'pending';
    CREATE INDEX messages_unread ON messages(mailbox_id) WHERE seen = 0;
    CREATE TABLE message_bodies (
        message_id BLOB PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
        text_zstd  BLOB,
        html_zstd  BLOB,
        fetched_at INTEGER NOT NULL
    );
    CREATE TABLE conversations (
        account_id        BLOB NOT NULL,
        thread_id         TEXT NOT NULL,
        subject           TEXT NOT NULL,
        message_count     INTEGER NOT NULL,
        unread_count      INTEGER NOT NULL,
        last_message_at   INTEGER NOT NULL,
        latest_sender     TEXT NOT NULL,
        sample_message_id BLOB NOT NULL,
        PRIMARY KEY (account_id, thread_id)
    );
    CREATE INDEX conversations_recent ON conversations(account_id, last_message_at DESC);
    CREATE TABLE contacts (
        id            BLOB PRIMARY KEY,
        display_name  TEXT NOT NULL,
        primary_email TEXT NOT NULL,
        organization  TEXT,
        source        TEXT NOT NULL,
        account_scope BLOB,
        emails        TEXT NOT NULL
    );
    CREATE TABLE contact_emails (
        email      TEXT NOT NULL,
        contact_id BLOB NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
        PRIMARY KEY (email, contact_id)
    );
    CREATE TABLE projection_checkpoints (
        name     TEXT PRIMARY KEY,
        position INTEGER NOT NULL
    );",
```

Below `pub mod event_store;` add:

```rust
mod projector;
pub mod repository;
```

- [ ] **Step 3: Write `sqlite/repository.rs`**

```rust
//! SQLite read model: every read trait, the write repositories, checkpoints,
//! the body store and the backfill queue. Projection arms live in
//! `projector.rs`.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Params, Row, params};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use mc_domain::{
    Account, AccountId, AccountReadRepository, AccountSummary, AccountSyncStats,
    AccountWriteRepository, Address, AggregateType, BackfillEntry, BodyBackfillQueue, Contact,
    ContactId, ContactReadRepository, ContactSummary, ContactWriteRepository, Conversation,
    ConversationReadRepository, ConversationSummary, DomainError, DomainEvent, EmailAddress,
    Envelope, EventStore, Mailbox, MailboxId, MailboxPath, MailboxReadRepository, Message,
    MessageBodyStore, MessageId, MessageReadRepository, MessageSummary, ProjectionCheckpoint,
    ProviderUid, ThreadId,
};
use mc_shared::pagination::{PagedResult, Pagination};

use super::event_store::SqliteEventStore;
use super::projector::refresh_conversation;
use super::{SqliteDb, repo_err};

/// Spec §3.
const ZSTD_LEVEL: i32 = 3;

#[derive(Clone)]
pub struct SqliteRepository {
    pub(super) db: Arc<SqliteDb>,
    events: Arc<SqliteEventStore>,
}

impl SqliteRepository {
    pub fn new(db: Arc<SqliteDb>, events: Arc<SqliteEventStore>) -> Self {
        Self { db, events }
    }
}

// ── Codecs ──────────────────────────────────────────────────────────────

/// Unix nanoseconds, like the event log, so timestamps round-trip exactly.
/// Dates outside 1677–2262 only come from broken headers; they clamp.
pub(super) fn nanos(t: DateTime<Utc>) -> i64 {
    t.timestamp_nanos_opt()
        .unwrap_or(if t.timestamp() < 0 { i64::MIN } else { i64::MAX })
}

pub(super) fn sql_int(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

pub(super) fn json<T: Serialize + ?Sized>(value: &T) -> Result<String, DomainError> {
    serde_json::to_string(value).map_err(DomainError::from)
}

fn conversion_err(
    idx: usize,
    ty: Type,
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(idx, ty, e.into())
}

fn json_col<T: DeserializeOwned>(row: &Row, idx: usize) -> rusqlite::Result<T> {
    let text: String = row.get(idx)?;
    serde_json::from_str(&text).map_err(|e| conversion_err(idx, Type::Text, e))
}

fn email_col(row: &Row, idx: usize) -> rusqlite::Result<EmailAddress> {
    let text: String = row.get(idx)?;
    EmailAddress::new(&text).map_err(|e| conversion_err(idx, Type::Text, e))
}

fn time_col(row: &Row, idx: usize) -> rusqlite::Result<DateTime<Utc>> {
    Ok(DateTime::from_timestamp_nanos(row.get(idx)?))
}

fn compress(body: Option<&str>) -> Result<Option<Vec<u8>>, DomainError> {
    body.map(|b| zstd::encode_all(b.as_bytes(), ZSTD_LEVEL).map_err(repo_err))
        .transpose()
}

fn body_col(row: &Row, idx: usize) -> rusqlite::Result<Option<String>> {
    let Some(blob) = row.get::<_, Option<Vec<u8>>>(idx)? else {
        return Ok(None);
    };
    let bytes =
        zstd::decode_all(blob.as_slice()).map_err(|e| conversion_err(idx, Type::Blob, e))?;
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|e| conversion_err(idx, Type::Blob, e))
}

/// Display name for a sender, falling back to the address when the name is
/// absent or blank (some clients send an empty name).
pub(super) fn sender_display(name: Option<String>, email: String) -> String {
    name.filter(|n| !n.trim().is_empty()).unwrap_or(email)
}

/// The address lists of a message, one JSON object (spec §4 `addresses`).
#[derive(Serialize, Deserialize)]
pub(super) struct Addresses {
    from: Vec<Address>,
    to: Vec<Address>,
    cc: Vec<Address>,
    bcc: Vec<Address>,
    reply_to: Vec<Address>,
}

impl Addresses {
    pub(super) fn of(e: &Envelope) -> Self {
        Self {
            from: e.from.clone(),
            to: e.to.clone(),
            cc: e.cc.clone(),
            bcc: e.bcc.clone(),
            reply_to: e.reply_to.clone(),
        }
    }
}

// ── SQL helpers ─────────────────────────────────────────────────────────

pub(super) fn exec(conn: &Connection, sql: &str, params: impl Params) -> Result<usize, DomainError> {
    conn.prepare_cached(sql)
        .and_then(|mut stmt| stmt.execute(params))
        .map_err(repo_err)
}

pub(super) fn query_one<T>(
    conn: &Connection,
    sql: &str,
    params: impl Params,
    map: impl FnOnce(&Row) -> rusqlite::Result<T>,
) -> Result<Option<T>, DomainError> {
    conn.prepare_cached(sql)
        .and_then(|mut stmt| stmt.query_row(params, map).optional())
        .map_err(repo_err)
}

fn query_all<T>(
    conn: &Connection,
    sql: &str,
    params: impl Params,
    map: impl FnMut(&Row) -> rusqlite::Result<T>,
) -> Result<Vec<T>, DomainError> {
    let mut stmt = conn.prepare_cached(sql).map_err(repo_err)?;
    let rows = stmt.query_map(params, map).map_err(repo_err)?;
    rows.collect::<rusqlite::Result<_>>().map_err(repo_err)
}

fn count_rows(conn: &Connection, sql: &str, params: impl Params) -> Result<u64, DomainError> {
    conn.query_row(sql, params, |r| r.get::<_, i64>(0))
        .map(|n| n as u64)
        .map_err(repo_err)
}

/// Stores the body of an existing message; unknown ids are skipped (the
/// message may have been deleted while its body was in flight).
pub(super) fn store_body(
    conn: &Connection,
    id: &MessageId,
    text: Option<&str>,
    html: Option<&str>,
    fetched_at: DateTime<Utc>,
) -> Result<(), DomainError> {
    exec(
        conn,
        "INSERT INTO message_bodies (message_id, text_zstd, html_zstd, fetched_at) \
         SELECT ?1, ?2, ?3, ?4 WHERE EXISTS (SELECT 1 FROM messages WHERE id = ?1) \
         ON CONFLICT(message_id) DO UPDATE SET text_zstd = excluded.text_zstd, \
             html_zstd = excluded.html_zstd, fetched_at = excluded.fetched_at",
        params![id.as_uuid(), compress(text)?, compress(html)?, nanos(fetched_at)],
    )
    .map(drop)
}

pub(super) fn save_checkpoint_in(
    conn: &Connection,
    name: &str,
    position: u64,
) -> Result<(), DomainError> {
    exec(
        conn,
        "INSERT INTO projection_checkpoints (name, position) VALUES (?1, ?2) \
         ON CONFLICT(name) DO UPDATE SET position = excluded.position",
        params![name, sql_int(position)],
    )
    .map(drop)
}

// ── Row mapping ─────────────────────────────────────────────────────────

const MESSAGE_COLUMNS: &str = "m.id, m.account_id, m.mailbox_id, m.provider_uid, \
     m.rfc_message_id, m.in_reply_to, m.refs, m.subject, m.addresses, m.flags, m.size_bytes, \
     m.has_attachments, m.attachments, m.sent_at, m.received_at, m.internal_date";

fn select_messages(rest: &str) -> String {
    format!("SELECT {MESSAGE_COLUMNS} FROM messages m {rest}")
}

fn select_messages_with_body(rest: &str) -> String {
    format!(
        "SELECT {MESSAGE_COLUMNS}, b.text_zstd, b.html_zstd FROM messages m \
         LEFT JOIN message_bodies b ON b.message_id = m.id {rest}"
    )
}

fn message_from_row(row: &Row) -> rusqlite::Result<Message> {
    let addresses: Addresses = json_col(row, 8)?;
    let mailbox_id: Option<Uuid> = row.get(2)?;
    Ok(Message {
        id: MessageId::from_uuid(row.get(0)?),
        account_id: AccountId::from_uuid(row.get(1)?),
        // Drafts have no mailbox (NULL); the domain type still needs one.
        mailbox_id: MailboxId::from_uuid(mailbox_id.unwrap_or_default()),
        provider_uid: row.get::<_, Option<u32>>(3)?.map(ProviderUid::new),
        message_rfc_id: row.get(4)?,
        in_reply_to: row.get(5)?,
        references: json_col(row, 6)?,
        envelope: Envelope {
            from: addresses.from,
            to: addresses.to,
            cc: addresses.cc,
            bcc: addresses.bcc,
            reply_to: addresses.reply_to,
            subject: row.get(7)?,
            date: time_col(row, 13)?,
        },
        flags: json_col(row, 9)?,
        size_bytes: row.get::<_, i64>(10)? as u64,
        has_attachments: row.get(11)?,
        attachments: json_col(row, 12)?,
        body_text: None,
        body_html: None,
        received_at: time_col(row, 14)?,
        internal_date: time_col(row, 15)?,
    })
}

fn message_with_body(row: &Row) -> rusqlite::Result<Message> {
    let mut message = message_from_row(row)?;
    message.body_text = body_col(row, 16)?;
    message.body_html = body_col(row, 17)?;
    Ok(message)
}

fn summary_of(m: Message) -> MessageSummary {
    let Envelope { from, to, cc, subject, .. } = m.envelope;
    let sender = from.into_iter().next();
    MessageSummary {
        id: m.id,
        account_id: m.account_id,
        mailbox_id: m.mailbox_id,
        provider_uid: m.provider_uid,
        from_email: sender.as_ref().map(|a| a.email.to_string()).unwrap_or_default(),
        from_name: sender.and_then(|a| a.name),
        to: to.into_iter().map(|a| a.email.to_string()).collect(),
        cc: cc.into_iter().map(|a| a.email.to_string()).collect(),
        subject,
        flags: m.flags,
        size_bytes: m.size_bytes,
        has_attachments: m.has_attachments,
        received_at: m.received_at,
    }
}

fn summary_from_row(row: &Row) -> rusqlite::Result<MessageSummary> {
    message_from_row(row).map(summary_of)
}

const SELECT_ACCOUNT: &str =
    "SELECT id, name, email, provider_type, status, total_unread FROM accounts";

fn account_from_row(row: &Row) -> rusqlite::Result<AccountSummary> {
    Ok(AccountSummary {
        id: AccountId::from_uuid(row.get(0)?),
        name: row.get(1)?,
        email: email_col(row, 2)?,
        provider_type: json_col(row, 3)?,
        status: json_col(row, 4)?,
        total_unread: row.get(5)?,
    })
}

const SELECT_MAILBOX: &str = "SELECT id, account_id, path, delimiter, display_name, \
     mailbox_type, flags, total_messages, unread_messages, uid_validity, uid_next FROM mailboxes";

fn mailbox_from_row(row: &Row) -> rusqlite::Result<Mailbox> {
    let path: String = row.get(2)?;
    let delimiter = row.get::<_, String>(3)?.chars().next().unwrap_or('/');
    Ok(Mailbox {
        id: MailboxId::from_uuid(row.get(0)?),
        account_id: AccountId::from_uuid(row.get(1)?),
        path: MailboxPath::parse(&path, delimiter).map_err(|e| conversion_err(2, Type::Text, e))?,
        display_name: row.get(4)?,
        mailbox_type: json_col(row, 5)?,
        flags: json_col(row, 6)?,
        total_messages: row.get(7)?,
        unread_messages: row.get(8)?,
        uid_validity: row.get(9)?,
        uid_next: row.get(10)?,
    })
}

const SELECT_CONVERSATION: &str = "SELECT thread_id, account_id, subject, message_count, \
     unread_count, last_message_at, latest_sender, sample_message_id FROM conversations";

fn conversation_from_row(row: &Row) -> rusqlite::Result<ConversationSummary> {
    Ok(ConversationSummary {
        thread_id: ThreadId::new(row.get::<_, String>(0)?),
        account_id: AccountId::from_uuid(row.get(1)?),
        subject: row.get(2)?,
        message_count: row.get(3)?,
        unread_count: row.get(4)?,
        last_message_at: time_col(row, 5)?,
        latest_sender: row.get(6)?,
        sample_message_id: MessageId::from_uuid(row.get(7)?),
    })
}

/// Per-mailbox threads are computed, not stored (spec §4): stats count only
/// this mailbox's copies. SQLite fills the bare columns from the row that
/// holds MAX(received_at).
const MAILBOX_CONVERSATIONS: &str = "SELECT thread_id, account_id, subject, COUNT(*), \
         SUM(seen = 0), MAX(received_at), from_name, from_email, id \
     FROM messages WHERE account_id = ?1 AND mailbox_id = ?2 AND thread_id IS NOT NULL \
     GROUP BY thread_id ORDER BY MAX(received_at) DESC, thread_id LIMIT ?3 OFFSET ?4";

fn mailbox_conversation_from_row(row: &Row) -> rusqlite::Result<ConversationSummary> {
    let subject: String = row.get(2)?;
    Ok(ConversationSummary {
        thread_id: ThreadId::new(row.get::<_, String>(0)?),
        account_id: AccountId::from_uuid(row.get(1)?),
        subject: Conversation::normalize_subject(&subject),
        message_count: row.get(3)?,
        unread_count: row.get(4)?,
        last_message_at: time_col(row, 5)?,
        latest_sender: sender_display(row.get(6)?, row.get(7)?),
        sample_message_id: MessageId::from_uuid(row.get(8)?),
    })
}

const CONTACT_COLUMNS: &str = "c.id, c.display_name, c.primary_email, c.emails, \
     c.organization, c.source, c.account_scope";

fn select_contacts(rest: &str) -> String {
    format!("SELECT {CONTACT_COLUMNS} FROM contacts c {rest}")
}

/// `at` = index of the first contact column, so one row can carry two contacts.
fn contact_from_row(row: &Row, at: usize) -> rusqlite::Result<ContactSummary> {
    Ok(ContactSummary {
        id: ContactId::from_uuid(row.get(at)?),
        display_name: row.get(at + 1)?,
        primary_email: email_col(row, at + 2)?,
        emails: json_col(row, at + 3)?,
        organization: row.get(at + 4)?,
        source: json_col(row, at + 5)?,
        account_scope: row.get::<_, Option<Uuid>>(at + 6)?.map(AccountId::from_uuid),
    })
}

fn page_bounds(pagination: &Pagination) -> (i64, i64) {
    (i64::from(pagination.page_size), sql_int(pagination.offset()))
}

// ── Write side ──────────────────────────────────────────────────────────

#[async_trait]
impl AccountWriteRepository for SqliteRepository {
    async fn save(&self, account: &Account, events: Vec<DomainEvent>) -> Result<(), DomainError> {
        let aggregate_id = account.id.to_string();
        let expected = self.events.current_sequence(&aggregate_id).await?;
        self.events
            .append(&aggregate_id, AggregateType::Account, expected, events)
            .await?;

        // Same contract as the sled repository: the summary is current when
        // save returns, without waiting for the projector.
        let (id, name, email) =
            (*account.id.as_uuid(), account.name.clone(), account.email.as_str().to_owned());
        let (provider_type, status) = (json(&account.provider_type)?, json(&account.status)?);
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "INSERT INTO accounts (id, name, email, provider_type, status) \
                     VALUES (?1, ?2, ?3, ?4, ?5) \
                     ON CONFLICT(id) DO UPDATE SET name = excluded.name, email = excluded.email, \
                         provider_type = excluded.provider_type, status = excluded.status",
                    params![id, name, email, provider_type, status],
                )
                .map(drop)
            })
            .await
    }

    async fn load(&self, id: &AccountId) -> Result<Option<Account>, DomainError> {
        let events = self.events.load_events(&id.to_string(), 0).await?;
        if events.is_empty() {
            return Ok(None);
        }
        let events: Vec<DomainEvent> = events.into_iter().map(|e| e.payload).collect();
        Ok(Account::rehydrate(&events))
    }
}

#[async_trait]
impl ContactWriteRepository for SqliteRepository {
    async fn save(&self, contact: &Contact, events: Vec<DomainEvent>) -> Result<(), DomainError> {
        let aggregate_id = contact.id.to_string();
        let expected = self.events.current_sequence(&aggregate_id).await?;
        self.events
            .append(&aggregate_id, AggregateType::Contact, expected, events)
            .await
            .map(drop)
    }

    async fn load(&self, id: &ContactId) -> Result<Option<Contact>, DomainError> {
        let events = self.events.load_events(&id.to_string(), 0).await?;
        if events.is_empty() {
            return Ok(None);
        }
        let events: Vec<DomainEvent> = events.into_iter().map(|e| e.payload).collect();
        Ok(Contact::rehydrate(&events))
    }
}

#[async_trait]
impl ProjectionCheckpoint for SqliteRepository {
    async fn get_checkpoint(&self, projection_name: &str) -> Result<u64, DomainError> {
        let name = projection_name.to_owned();
        self.db
            .read(move |conn| {
                let position = query_one(
                    conn,
                    "SELECT position FROM projection_checkpoints WHERE name = ?1",
                    [name],
                    |r| r.get::<_, i64>(0),
                )?;
                Ok(position.map_or(0, |p| p as u64))
            })
            .await
    }

    async fn save_checkpoint(
        &self,
        projection_name: &str,
        global_position: u64,
    ) -> Result<(), DomainError> {
        let name = projection_name.to_owned();
        self.db
            .write(move |conn| save_checkpoint_in(conn, &name, global_position))
            .await
    }
}

#[async_trait]
impl MessageBodyStore for SqliteRepository {
    async fn put_body(
        &self,
        message_id: &MessageId,
        body_text: Option<String>,
        body_html: Option<String>,
    ) -> Result<(), DomainError> {
        let id = *message_id;
        self.db
            .write(move |conn| {
                store_body(conn, &id, body_text.as_deref(), body_html.as_deref(), Utc::now())
            })
            .await
    }
}

#[async_trait]
impl BodyBackfillQueue for SqliteRepository {
    async fn list_pending_backfill(
        &self,
        account_id: &AccountId,
        limit: usize,
    ) -> Result<Vec<BackfillEntry>, DomainError> {
        let (account, limit) = (*account_id.as_uuid(), sql_int(limit as u64));
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    "SELECT id, mailbox_id, provider_uid FROM messages \
                     WHERE account_id = ?1 AND body_state = 'pending' \
                     ORDER BY received_at DESC LIMIT ?2",
                    params![account, limit],
                    |r| {
                        Ok(BackfillEntry {
                            message_id: MessageId::from_uuid(r.get(0)?),
                            mailbox_id: MailboxId::from_uuid(r.get(1)?),
                            provider_uid: r.get(2)?,
                        })
                    },
                )
            })
            .await
    }

    async fn count_pending_backfill(&self, account_id: &AccountId) -> Result<u32, DomainError> {
        let account = *account_id.as_uuid();
        self.db
            .read(move |conn| {
                count_rows(
                    conn,
                    "SELECT COUNT(*) FROM messages WHERE account_id = ?1 AND body_state = 'pending'",
                    [account],
                )
                .map(|n| u32::try_from(n).unwrap_or(u32::MAX))
            })
            .await
    }
}

// ── Read side ───────────────────────────────────────────────────────────

#[async_trait]
impl AccountReadRepository for SqliteRepository {
    async fn get_account(&self, id: &AccountId) -> Result<Option<AccountSummary>, DomainError> {
        let id = *id.as_uuid();
        self.db
            .read(move |conn| {
                query_one(conn, &format!("{SELECT_ACCOUNT} WHERE id = ?1"), [id], account_from_row)
            })
            .await
    }

    async fn list_accounts(&self) -> Result<Vec<AccountSummary>, DomainError> {
        self.db
            .read(|conn| query_all(conn, &format!("{SELECT_ACCOUNT} ORDER BY id"), [], account_from_row))
            .await
    }

    async fn account_stats(&self, id: &AccountId) -> Result<AccountSyncStats, DomainError> {
        let id = *id.as_uuid();
        self.db
            .read(move |conn| {
                let (total, synced, pending): (i64, i64, i64) = conn
                    .query_row(
                        "SELECT \
                           (SELECT COALESCE(SUM(total_messages), 0) FROM mailboxes WHERE account_id = ?1), \
                           (SELECT COUNT(*) FROM messages WHERE mailbox_id IN \
                               (SELECT id FROM mailboxes WHERE account_id = ?1)), \
                           (SELECT COUNT(*) FROM messages WHERE account_id = ?1 AND body_state = 'pending')",
                        [id],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )
                    .map_err(repo_err)?;
                let clamp = |v: i64| u32::try_from(v).unwrap_or(u32::MAX);
                Ok(AccountSyncStats {
                    total_messages: clamp(total),
                    synced_messages: clamp(synced),
                    pending_bodies: clamp(pending),
                })
            })
            .await
    }
}

#[async_trait]
impl MailboxReadRepository for SqliteRepository {
    async fn get_mailbox(&self, id: &MailboxId) -> Result<Option<Mailbox>, DomainError> {
        let id = *id.as_uuid();
        self.db
            .read(move |conn| {
                query_one(conn, &format!("{SELECT_MAILBOX} WHERE id = ?1"), [id], mailbox_from_row)
            })
            .await
    }

    async fn list_mailboxes(&self, account_id: &AccountId) -> Result<Vec<Mailbox>, DomainError> {
        let account = *account_id.as_uuid();
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    &format!("{SELECT_MAILBOX} WHERE account_id = ?1 ORDER BY id"),
                    [account],
                    mailbox_from_row,
                )
            })
            .await
    }
}

#[async_trait]
impl MessageReadRepository for SqliteRepository {
    async fn get_message(&self, id: &MessageId) -> Result<Option<Message>, DomainError> {
        let id = *id.as_uuid();
        self.db
            .read(move |conn| {
                query_one(conn, &select_messages_with_body("WHERE m.id = ?1"), [id], message_with_body)
            })
            .await
    }

    async fn list_messages(
        &self,
        mailbox_id: &MailboxId,
        pagination: &Pagination,
    ) -> Result<PagedResult<MessageSummary>, DomainError> {
        let (mailbox, pagination) = (*mailbox_id.as_uuid(), pagination.clone());
        let (limit, offset) = page_bounds(&pagination);
        self.db
            .read(move |conn| {
                let total =
                    count_rows(conn, "SELECT COUNT(*) FROM messages WHERE mailbox_id = ?1", [mailbox])?;
                let items = query_all(
                    conn,
                    &select_messages(
                        "WHERE m.mailbox_id = ?1 ORDER BY m.received_at DESC, m.id LIMIT ?2 OFFSET ?3",
                    ),
                    params![mailbox, limit, offset],
                    summary_from_row,
                )?;
                Ok(PagedResult::new(items, total, &pagination))
            })
            .await
    }

    async fn get_thread(&self, message_id: &MessageId) -> Result<Vec<Message>, DomainError> {
        let id = *message_id.as_uuid();
        self.db
            .read(move |conn| {
                let Some((account, thread)) = query_one(
                    conn,
                    "SELECT account_id, thread_id FROM messages WHERE id = ?1",
                    [id],
                    |r| Ok((r.get::<_, Uuid>(0)?, r.get::<_, Option<String>>(1)?)),
                )?
                else {
                    return Ok(Vec::new());
                };
                match thread {
                    Some(thread) => query_all(
                        conn,
                        &select_messages_with_body(
                            "WHERE m.account_id = ?1 AND m.thread_id = ?2 ORDER BY m.received_at, m.id",
                        ),
                        params![account, thread],
                        message_with_body,
                    ),
                    None => query_all(conn, &select_messages_with_body("WHERE m.id = ?1"), [id], message_with_body),
                }
            })
            .await
    }

    async fn find_by_provider_uid(
        &self,
        mailbox_id: &MailboxId,
        provider_uid: u32,
    ) -> Result<Option<MessageSummary>, DomainError> {
        let mailbox = *mailbox_id.as_uuid();
        self.db
            .read(move |conn| {
                query_one(
                    conn,
                    &select_messages(
                        "WHERE m.mailbox_id = ?1 AND m.provider_uid = ?2 ORDER BY m.received_at DESC LIMIT 1",
                    ),
                    params![mailbox, provider_uid],
                    summary_from_row,
                )
            })
            .await
    }

    async fn find_unread_by_mailbox(
        &self,
        mailbox_id: &MailboxId,
    ) -> Result<Vec<MessageSummary>, DomainError> {
        let mailbox = *mailbox_id.as_uuid();
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    &select_messages("WHERE m.mailbox_id = ?1 AND m.seen = 0 ORDER BY m.received_at DESC, m.id"),
                    [mailbox],
                    summary_from_row,
                )
            })
            .await
    }

    async fn scan_all_messages(
        &self,
        account_id: Option<&AccountId>,
    ) -> Result<Vec<Message>, DomainError> {
        let account = account_id.map(|a| *a.as_uuid());
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    &select_messages("WHERE ?1 IS NULL OR m.account_id = ?1 ORDER BY m.id"),
                    [account],
                    message_from_row,
                )
            })
            .await
    }
}

#[async_trait]
impl ConversationReadRepository for SqliteRepository {
    async fn get_conversation(
        &self,
        thread_id: &ThreadId,
        account_id: &AccountId,
    ) -> Result<Option<ConversationSummary>, DomainError> {
        let (thread, account) = (thread_id.value().to_owned(), *account_id.as_uuid());
        self.db
            .read(move |conn| {
                query_one(
                    conn,
                    &format!("{SELECT_CONVERSATION} WHERE account_id = ?1 AND thread_id = ?2"),
                    params![account, thread],
                    conversation_from_row,
                )
            })
            .await
    }

    async fn list_conversations(
        &self,
        account_id: &AccountId,
        mailbox_id: Option<&MailboxId>,
        pagination: &Pagination,
    ) -> Result<PagedResult<ConversationSummary>, DomainError> {
        let (account, mailbox) = (*account_id.as_uuid(), mailbox_id.map(|m| *m.as_uuid()));
        let pagination = pagination.clone();
        let (limit, offset) = page_bounds(&pagination);
        self.db
            .read(move |conn| {
                let (total, items) = match mailbox {
                    None => (
                        count_rows(conn, "SELECT COUNT(*) FROM conversations WHERE account_id = ?1", [account])?,
                        query_all(
                            conn,
                            &format!(
                                "{SELECT_CONVERSATION} WHERE account_id = ?1 \
                                 ORDER BY last_message_at DESC, thread_id LIMIT ?2 OFFSET ?3"
                            ),
                            params![account, limit, offset],
                            conversation_from_row,
                        )?,
                    ),
                    Some(mailbox) => (
                        count_rows(
                            conn,
                            "SELECT COUNT(DISTINCT thread_id) FROM messages \
                             WHERE account_id = ?1 AND mailbox_id = ?2",
                            params![account, mailbox],
                        )?,
                        query_all(
                            conn,
                            MAILBOX_CONVERSATIONS,
                            params![account, mailbox, limit, offset],
                            mailbox_conversation_from_row,
                        )?,
                    ),
                };
                Ok(PagedResult::new(items, total, &pagination))
            })
            .await
    }

    /// Recomputes every conversation from `messages`; returns how many there are.
    async fn rebuild_index(&self) -> Result<u64, DomainError> {
        self.db
            .write(|conn| {
                let tx = conn.transaction().map_err(repo_err)?;
                exec(&tx, "DELETE FROM conversations", [])?;
                let threads = query_all(
                    &tx,
                    "SELECT DISTINCT account_id, thread_id FROM messages WHERE thread_id IS NOT NULL",
                    [],
                    |r| Ok((r.get::<_, Uuid>(0)?, r.get::<_, String>(1)?)),
                )?;
                for (account, thread) in &threads {
                    refresh_conversation(&tx, account, thread)?;
                }
                tx.commit().map_err(repo_err)?;
                Ok(threads.len() as u64)
            })
            .await
    }
}

#[async_trait]
impl ContactReadRepository for SqliteRepository {
    async fn get_contact(&self, id: &ContactId) -> Result<Option<ContactSummary>, DomainError> {
        let id = *id.as_uuid();
        self.db
            .read(move |conn| {
                query_one(conn, &select_contacts("WHERE c.id = ?1"), [id], |r| contact_from_row(r, 0))
            })
            .await
    }

    async fn list_contacts(
        &self,
        pagination: &Pagination,
    ) -> Result<PagedResult<ContactSummary>, DomainError> {
        let pagination = pagination.clone();
        let (limit, offset) = page_bounds(&pagination);
        self.db
            .read(move |conn| {
                let total = count_rows(conn, "SELECT COUNT(*) FROM contacts", [])?;
                let items = query_all(
                    conn,
                    &select_contacts("ORDER BY c.id LIMIT ?1 OFFSET ?2"),
                    params![limit, offset],
                    |r| contact_from_row(r, 0),
                )?;
                Ok(PagedResult::new(items, total, &pagination))
            })
            .await
    }

    async fn find_by_email(&self, email: &EmailAddress) -> Result<Vec<ContactSummary>, DomainError> {
        let email = email.as_str().to_owned();
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    &select_contacts(
                        "JOIN contact_emails e ON e.contact_id = c.id WHERE e.email = ?1 ORDER BY c.id",
                    ),
                    [email],
                    |r| contact_from_row(r, 0),
                )
            })
            .await
    }

    async fn find_by_email_in_account(
        &self,
        email: &EmailAddress,
        account_id: &AccountId,
    ) -> Result<Vec<ContactSummary>, DomainError> {
        let (email, account) = (email.as_str().to_owned(), *account_id.as_uuid());
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    &select_contacts(
                        "JOIN contact_emails e ON e.contact_id = c.id \
                         WHERE e.email = ?1 AND c.account_scope = ?2 ORDER BY c.id",
                    ),
                    params![email, account],
                    |r| contact_from_row(r, 0),
                )
            })
            .await
    }

    async fn search_contacts(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<ContactSummary>, DomainError> {
        // ponytail: LIKE folds ASCII case only (sled lowercased Unicode too);
        // add a lowered column if non-ASCII case-insensitive search matters.
        let escaped = query
            .to_lowercase()
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let (pattern, limit) = (format!("%{escaped}%"), sql_int(limit as u64));
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    &select_contacts(
                        "WHERE c.display_name LIKE ?1 ESCAPE '\\' OR c.primary_email LIKE ?1 ESCAPE '\\' \
                         ORDER BY c.id LIMIT ?2",
                    ),
                    params![pattern, limit],
                    |r| contact_from_row(r, 0),
                )
            })
            .await
    }

    async fn find_duplicate_candidates(
        &self,
        account_id: Option<&AccountId>,
    ) -> Result<Vec<(ContactSummary, ContactSummary)>, DomainError> {
        let scope = account_id.map(|a| *a.as_uuid());
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    &format!(
                        "SELECT DISTINCT {CONTACT_COLUMNS}, d.id, d.display_name, d.primary_email, \
                             d.emails, d.organization, d.source, d.account_scope \
                         FROM contact_emails a \
                         JOIN contact_emails b ON b.email = a.email AND b.contact_id > a.contact_id \
                         JOIN contacts c ON c.id = a.contact_id \
                         JOIN contacts d ON d.id = b.contact_id \
                         WHERE ?1 IS NULL OR (c.account_scope = ?1 AND d.account_scope = ?1) \
                         ORDER BY c.id, d.id"
                    ),
                    [scope],
                    |r| Ok((contact_from_row(r, 0)?, contact_from_row(r, 7)?)),
                )
            })
            .await
    }
}
```

- [ ] **Step 4: Write `sqlite/projector.rs`**

```rust
//! Projection of domain events into the SQLite read model. Every arm runs in
//! the caller's transaction and must be replay-safe: boot and post-sync
//! catch-up re-project events the live path already applied (spec §10).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use uuid::Uuid;

use mc_domain::{
    AccountId, AccountStatus, ContactEmail, ContactId, Conversation, DomainError, DomainEvent,
    Envelope, EventEnvelope, Flag, MailboxId, MessageId, ReadModelProjector,
};

use super::repo_err;
use super::repository::{
    Addresses, SqliteRepository, exec, json, nanos, query_one, save_checkpoint_in,
    sender_display, sql_int, store_body,
};

#[async_trait]
impl ReadModelProjector for SqliteRepository {
    async fn project(&self, event: &EventEnvelope) -> Result<(), DomainError> {
        let event = event.clone();
        self.db
            .write(move |conn| {
                let tx = conn.transaction().map_err(repo_err)?;
                apply(&tx, &event)?;
                tx.commit().map_err(repo_err)
            })
            .await
    }

    async fn project_batch(
        &self,
        checkpoint: &str,
        events: Vec<EventEnvelope>,
    ) -> Result<bool, DomainError> {
        let Some(position) = events.last().map(|e| e.global_position) else {
            return Ok(true);
        };
        let checkpoint = checkpoint.to_owned();
        self.db
            .write(move |conn| {
                let tx = conn.transaction().map_err(repo_err)?;
                for event in &events {
                    apply(&tx, event)?;
                }
                save_checkpoint_in(&tx, &checkpoint, position)?;
                tx.commit().map_err(repo_err)
            })
            .await?;
        Ok(true)
    }
}

fn apply(tx: &Connection, event: &EventEnvelope) -> Result<(), DomainError> {
    match &event.payload {
        DomainEvent::AccountAdded { account_id, name, email, provider_type } => {
            exec(
                tx,
                "INSERT INTO accounts (id, name, email, provider_type, status) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(id) DO UPDATE SET name = excluded.name, email = excluded.email, \
                     provider_type = excluded.provider_type, status = excluded.status",
                params![
                    account_id.as_uuid(),
                    name,
                    email.as_str(),
                    json(provider_type)?,
                    json(&AccountStatus::Active)?
                ],
            )?;
        }
        DomainEvent::AccountUpdated { account_id, name, .. } => {
            if let Some(name) = name {
                exec(tx, "UPDATE accounts SET name = ?2 WHERE id = ?1", params![account_id.as_uuid(), name])?;
            }
        }
        DomainEvent::AccountRemoved { account_id } => {
            exec(tx, "DELETE FROM accounts WHERE id = ?1", [account_id.as_uuid()])?;
        }
        DomainEvent::AccountAuthExpired { account_id, .. } => {
            set_account_status(tx, account_id, AccountStatus::NeedsReauth)?;
        }
        DomainEvent::AccountReauthenticated { account_id } => {
            set_account_status(tx, account_id, AccountStatus::Active)?;
        }
        DomainEvent::AccountSyncStarted { .. }
        | DomainEvent::AccountSyncCompleted { .. }
        | DomainEvent::AccountSyncFailed { .. } => {}

        DomainEvent::MailboxDiscovered { mailbox_id, account_id, path, mailbox_type } => {
            // Counts are transient (never in the log), so a replay must keep them.
            exec(
                tx,
                "INSERT INTO mailboxes (id, account_id, path, delimiter, display_name, mailbox_type, flags) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, '[]') \
                 ON CONFLICT(id) DO UPDATE SET account_id = excluded.account_id, path = excluded.path, \
                     delimiter = excluded.delimiter, display_name = excluded.display_name, \
                     mailbox_type = excluded.mailbox_type",
                params![
                    mailbox_id.as_uuid(),
                    account_id.as_uuid(),
                    path.full_path(),
                    path.delimiter().to_string(),
                    path.name(),
                    json(mailbox_type)?
                ],
            )?;
        }
        DomainEvent::MailboxTypeChanged { mailbox_id, mailbox_type } => {
            exec(
                tx,
                "UPDATE mailboxes SET mailbox_type = ?2 WHERE id = ?1",
                params![mailbox_id.as_uuid(), json(mailbox_type)?],
            )?;
        }
        DomainEvent::MailboxCountsUpdated { mailbox_id, total_messages, unread_messages } => {
            exec(
                tx,
                "UPDATE mailboxes SET total_messages = ?2, unread_messages = ?3 WHERE id = ?1",
                params![mailbox_id.as_uuid(), total_messages, unread_messages],
            )?;
        }
        DomainEvent::MailboxRenamed { mailbox_id, new_path, .. } => {
            exec(
                tx,
                "UPDATE mailboxes SET path = ?2, delimiter = ?3, display_name = ?4 WHERE id = ?1",
                params![
                    mailbox_id.as_uuid(),
                    new_path.full_path(),
                    new_path.delimiter().to_string(),
                    new_path.name()
                ],
            )?;
        }
        DomainEvent::MailboxDeleted { mailbox_id, .. } => {
            exec(tx, "DELETE FROM mailboxes WHERE id = ?1", [mailbox_id.as_uuid()])?;
        }

        DomainEvent::MessageReceived {
            message_id,
            account_id,
            mailbox_id,
            provider_uid,
            message_rfc_id,
            in_reply_to,
            references,
            envelope,
            flags,
            size_bytes,
            has_attachments,
            received_at,
        } => {
            let thread_id = Conversation::resolve_thread_id(
                message_rfc_id.as_deref(),
                in_reply_to.as_deref(),
                references,
                &message_id.to_string(),
            );
            let inserted = insert_message(
                tx,
                &NewMessage {
                    id: message_id,
                    account_id,
                    mailbox_id: Some(mailbox_id),
                    provider_uid: Some(provider_uid.value()),
                    rfc_message_id: message_rfc_id.as_deref(),
                    in_reply_to: in_reply_to.as_deref(),
                    references,
                    thread_id: Some(thread_id.value()),
                    envelope,
                    flags,
                    size_bytes: *size_bytes,
                    has_attachments: *has_attachments,
                    received_at: *received_at,
                    body_state: "pending",
                },
            )?;
            if inserted {
                // Symmetric with MessageDeleted's decrement, so replaying a
                // received+deleted pair nets zero.
                if !flags.contains(&Flag::Seen) {
                    adjust_unread(tx, mailbox_id.as_uuid(), 1)?;
                }
                refresh_conversation(tx, account_id.as_uuid(), thread_id.value())?;
            }
        }
        DomainEvent::MessageBodyFetched { message_id, attachments } => {
            exec(
                tx,
                "UPDATE messages SET attachments = ?2, has_attachments = ?3, body_state = 'stored' \
                 WHERE id = ?1",
                params![message_id.as_uuid(), json(attachments)?, !attachments.is_empty()],
            )?;
        }
        DomainEvent::MessageFlagged { message_id, flags_added, flags_removed, .. } => {
            flag_message(tx, message_id, flags_added, flags_removed)?;
        }
        DomainEvent::MessageMoved { message_id, to_mailbox_id, .. } => {
            move_message(tx, message_id, to_mailbox_id)?;
        }
        DomainEvent::MessageDeleted { message_id, .. } => delete_message(tx, message_id)?,
        DomainEvent::MessageSent {
            message_id,
            account_id,
            mailbox_id,
            envelope,
            in_reply_to,
            references,
            body_text,
            body_html,
            sent_at,
        } => {
            let thread_id = Conversation::resolve_thread_id(
                None,
                in_reply_to.as_deref(),
                references,
                &message_id.to_string(),
            );
            let inserted = insert_message(
                tx,
                &NewMessage {
                    id: message_id,
                    account_id,
                    mailbox_id: Some(mailbox_id),
                    provider_uid: None,
                    rfc_message_id: None,
                    in_reply_to: in_reply_to.as_deref(),
                    references,
                    thread_id: Some(thread_id.value()),
                    envelope,
                    flags: &[Flag::Seen],
                    size_bytes: 0,
                    has_attachments: false,
                    received_at: *sent_at,
                    body_state: "stored",
                },
            )?;
            if inserted {
                store_body(tx, message_id, body_text.as_deref(), body_html.as_deref(), *sent_at)?;
                refresh_conversation(tx, account_id.as_uuid(), thread_id.value())?;
            }
        }
        DomainEvent::DraftSaved { message_id, account_id, envelope, body_text, body_html } => {
            // Each save replaces the draft; the event's own time keeps replays deterministic.
            exec(tx, "DELETE FROM messages WHERE id = ?1", [message_id.as_uuid()])?;
            insert_message(
                tx,
                &NewMessage {
                    id: message_id,
                    account_id,
                    mailbox_id: None,
                    provider_uid: None,
                    rfc_message_id: None,
                    in_reply_to: None,
                    references: &[],
                    thread_id: None,
                    envelope,
                    flags: &[Flag::Draft],
                    size_bytes: 0,
                    has_attachments: false,
                    received_at: event.occurred_at,
                    body_state: "stored",
                },
            )?;
            store_body(tx, message_id, body_text.as_deref(), body_html.as_deref(), event.occurred_at)?;
        }
        DomainEvent::DraftDiscarded { message_id } => {
            exec(tx, "DELETE FROM messages WHERE id = ?1", [message_id.as_uuid()])?;
        }

        DomainEvent::ContactAdded { contact_id, display_name, emails, source, account_scope, .. } => {
            exec(
                tx,
                "INSERT INTO contacts (id, display_name, primary_email, source, account_scope, emails) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT(id) DO UPDATE SET display_name = excluded.display_name, \
                     primary_email = excluded.primary_email, source = excluded.source, \
                     account_scope = excluded.account_scope, emails = excluded.emails",
                params![
                    contact_id.as_uuid(),
                    display_name,
                    primary_email(emails)?,
                    json(source)?,
                    account_scope.map(|a| *a.as_uuid()),
                    json(emails)?
                ],
            )?;
            index_contact_emails(tx, contact_id, emails)?;
        }
        DomainEvent::ContactUpdated { contact_id, display_name, emails, organization, .. } => {
            if let Some(name) = display_name {
                exec(
                    tx,
                    "UPDATE contacts SET display_name = ?2 WHERE id = ?1",
                    params![contact_id.as_uuid(), name],
                )?;
            }
            if let Some(emails) = emails {
                set_contact_emails(tx, contact_id, emails)?;
            }
            if let Some(organization) = organization {
                exec(
                    tx,
                    "UPDATE contacts SET organization = ?2 WHERE id = ?1",
                    params![contact_id.as_uuid(), organization],
                )?;
            }
        }
        DomainEvent::ContactRemoved { contact_id } => {
            exec(tx, "DELETE FROM contacts WHERE id = ?1", [contact_id.as_uuid()])?;
        }
        DomainEvent::ContactMerged { kept_id, discarded_id, merged_emails } => {
            set_contact_emails(tx, kept_id, merged_emails)?;
            exec(tx, "DELETE FROM contacts WHERE id = ?1", [discarded_id.as_uuid()])?;
        }
    }
    Ok(())
}

fn set_account_status(
    tx: &Connection,
    account_id: &AccountId,
    status: AccountStatus,
) -> Result<(), DomainError> {
    exec(
        tx,
        "UPDATE accounts SET status = ?2 WHERE id = ?1",
        params![account_id.as_uuid(), json(&status)?],
    )
    .map(drop)
}

struct NewMessage<'a> {
    id: &'a MessageId,
    account_id: &'a AccountId,
    mailbox_id: Option<&'a MailboxId>,
    provider_uid: Option<u32>,
    rfc_message_id: Option<&'a str>,
    in_reply_to: Option<&'a str>,
    references: &'a [String],
    thread_id: Option<&'a str>,
    envelope: &'a Envelope,
    flags: &'a [Flag],
    size_bytes: u64,
    has_attachments: bool,
    received_at: DateTime<Utc>,
    body_state: &'static str,
}

/// Inserts unless the id exists and reports whether it did. Message ids are
/// minted per receipt/send, so an existing row means a replay: it already
/// reflects every later event and must not be reset.
fn insert_message(tx: &Connection, m: &NewMessage) -> Result<bool, DomainError> {
    let sender = m.envelope.from.first();
    let inserted = exec(
        tx,
        "INSERT INTO messages (id, account_id, mailbox_id, provider_uid, rfc_message_id, \
             in_reply_to, refs, thread_id, subject, from_name, from_email, addresses, flags, seen, \
             size_bytes, has_attachments, attachments, sent_at, received_at, internal_date, body_state) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, '[]', \
             ?17, ?18, ?18, ?19) \
         ON CONFLICT(id) DO NOTHING",
        params![
            m.id.as_uuid(),
            m.account_id.as_uuid(),
            m.mailbox_id.map(MailboxId::as_uuid),
            m.provider_uid,
            m.rfc_message_id,
            m.in_reply_to,
            json(m.references)?,
            m.thread_id,
            m.envelope.subject,
            sender.and_then(|a| a.name.as_deref()),
            sender.map_or("", |a| a.email.as_str()),
            json(&Addresses::of(m.envelope))?,
            json(m.flags)?,
            m.flags.contains(&Flag::Seen),
            sql_int(m.size_bytes),
            m.has_attachments,
            nanos(m.envelope.date),
            nanos(m.received_at),
            m.body_state
        ],
    )?;
    Ok(inserted == 1)
}

fn adjust_unread(tx: &Connection, mailbox_id: &Uuid, delta: i64) -> Result<(), DomainError> {
    exec(
        tx,
        "UPDATE mailboxes SET unread_messages = MAX(unread_messages + ?2, 0) WHERE id = ?1",
        params![mailbox_id, delta],
    )
    .map(drop)
}

fn flag_message(
    tx: &Connection,
    id: &MessageId,
    added: &[Flag],
    removed: &[Flag],
) -> Result<(), DomainError> {
    let Some((flags, mailbox_id, account_id, thread_id)) = query_one(
        tx,
        "SELECT flags, mailbox_id, account_id, thread_id FROM messages WHERE id = ?1",
        [id.as_uuid()],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<Uuid>>(1)?,
                r.get::<_, Uuid>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        },
    )?
    else {
        return Ok(());
    };
    let mut flags: Vec<Flag> = serde_json::from_str(&flags)?;
    let was_seen = flags.contains(&Flag::Seen);
    flags.retain(|f| !removed.contains(f));
    for flag in added {
        if !flags.contains(flag) {
            flags.push(flag.clone());
        }
    }
    let seen = flags.contains(&Flag::Seen);
    exec(
        tx,
        "UPDATE messages SET flags = ?2, seen = ?3 WHERE id = ?1",
        params![id.as_uuid(), json(&flags)?, seen],
    )?;
    // The delta comes from the row's before/after state, so re-applying a
    // flag change is a no-op.
    if seen != was_seen {
        if let Some(mailbox_id) = mailbox_id {
            adjust_unread(tx, &mailbox_id, if seen { -1 } else { 1 })?;
        }
        if let Some(thread_id) = thread_id {
            refresh_conversation(tx, &account_id, &thread_id)?;
        }
    }
    Ok(())
}

fn move_message(tx: &Connection, id: &MessageId, to: &MailboxId) -> Result<(), DomainError> {
    let Some((from, seen)) = query_one(
        tx,
        "SELECT mailbox_id, seen FROM messages WHERE id = ?1",
        [id.as_uuid()],
        |r| Ok((r.get::<_, Option<Uuid>>(0)?, r.get::<_, bool>(1)?)),
    )?
    else {
        return Ok(());
    };
    let to = *to.as_uuid();
    // A replay finds the row already in `to`: nothing moves, nothing to count.
    if from == Some(to) {
        return Ok(());
    }
    exec(tx, "UPDATE messages SET mailbox_id = ?2 WHERE id = ?1", params![id.as_uuid(), to])?;
    if !seen {
        if let Some(from) = from {
            adjust_unread(tx, &from, -1)?;
        }
        adjust_unread(tx, &to, 1)?;
    }
    Ok(())
}

fn delete_message(tx: &Connection, id: &MessageId) -> Result<(), DomainError> {
    let Some((mailbox_id, account_id, thread_id, seen)) = query_one(
        tx,
        "SELECT mailbox_id, account_id, thread_id, seen FROM messages WHERE id = ?1",
        [id.as_uuid()],
        |r| {
            Ok((
                r.get::<_, Option<Uuid>>(0)?,
                r.get::<_, Uuid>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, bool>(3)?,
            ))
        },
    )?
    else {
        return Ok(());
    };
    // The body row goes with it (ON DELETE CASCADE).
    exec(tx, "DELETE FROM messages WHERE id = ?1", [id.as_uuid()])?;
    if !seen && let Some(mailbox_id) = mailbox_id {
        adjust_unread(tx, &mailbox_id, -1)?;
    }
    if let Some(thread_id) = thread_id {
        refresh_conversation(tx, &account_id, &thread_id)?;
    }
    Ok(())
}

/// Recomputes a thread's row from its messages, so it converges after any
/// sequence of replays. Copies of one RFC message in several mailboxes
/// (Gmail labels) count once.
pub(super) fn refresh_conversation(
    tx: &Connection,
    account_id: &Uuid,
    thread_id: &str,
) -> Result<(), DomainError> {
    let (count, unread): (u32, u32) = tx
        .query_row(
            "SELECT COUNT(DISTINCT COALESCE(rfc_message_id, hex(id))), \
                    COUNT(DISTINCT CASE WHEN seen = 0 THEN COALESCE(rfc_message_id, hex(id)) END) \
             FROM messages WHERE account_id = ?1 AND thread_id = ?2",
            params![account_id, thread_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(repo_err)?;
    if count == 0 {
        exec(
            tx,
            "DELETE FROM conversations WHERE account_id = ?1 AND thread_id = ?2",
            params![account_id, thread_id],
        )?;
        return Ok(());
    }
    let (sample, last_at, from_name, from_email): (Uuid, i64, Option<String>, String) = tx
        .query_row(
            "SELECT id, received_at, from_name, from_email FROM messages \
             WHERE account_id = ?1 AND thread_id = ?2 ORDER BY received_at DESC, id DESC LIMIT 1",
            params![account_id, thread_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(repo_err)?;
    let subject: String = tx
        .query_row(
            "SELECT subject FROM messages WHERE account_id = ?1 AND thread_id = ?2 \
             ORDER BY received_at, id LIMIT 1",
            params![account_id, thread_id],
            |r| r.get(0),
        )
        .map_err(repo_err)?;
    exec(
        tx,
        "INSERT INTO conversations (account_id, thread_id, subject, message_count, unread_count, \
             last_message_at, latest_sender, sample_message_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
         ON CONFLICT(account_id, thread_id) DO UPDATE SET subject = excluded.subject, \
             message_count = excluded.message_count, unread_count = excluded.unread_count, \
             last_message_at = excluded.last_message_at, latest_sender = excluded.latest_sender, \
             sample_message_id = excluded.sample_message_id",
        params![
            account_id,
            thread_id,
            Conversation::normalize_subject(&subject),
            count,
            unread,
            last_at,
            sender_display(from_name, from_email),
            sample
        ],
    )
    .map(drop)
}

fn primary_email(emails: &[ContactEmail]) -> Result<&str, DomainError> {
    emails
        .iter()
        .find(|e| e.is_primary)
        .or_else(|| emails.first())
        .map(|e| e.address.as_str())
        .ok_or_else(|| {
            DomainError::Validation("a contact must have at least one email address".into())
        })
}

/// Skipped for unknown contacts, whose index rows the foreign key would reject.
fn set_contact_emails(
    tx: &Connection,
    contact_id: &ContactId,
    emails: &[ContactEmail],
) -> Result<(), DomainError> {
    let updated = exec(
        tx,
        "UPDATE contacts SET emails = ?2, primary_email = ?3 WHERE id = ?1",
        params![contact_id.as_uuid(), json(emails)?, primary_email(emails)?],
    )?;
    if updated == 1 {
        index_contact_emails(tx, contact_id, emails)?;
    }
    Ok(())
}

fn index_contact_emails(
    tx: &Connection,
    contact_id: &ContactId,
    emails: &[ContactEmail],
) -> Result<(), DomainError> {
    exec(tx, "DELETE FROM contact_emails WHERE contact_id = ?1", [contact_id.as_uuid()])?;
    for email in emails {
        exec(
            tx,
            "INSERT OR IGNORE INTO contact_emails (email, contact_id) VALUES (?1, ?2)",
            params![email.address.as_str(), contact_id.as_uuid()],
        )?;
    }
    Ok(())
}
```

If a borrow or trait-bound detail does not compile as written (e.g. `[u8;16]`/`Uuid` `ToSql` via the `uuid` feature, `params!` with `Option<&Uuid>`), fix it locally without changing the SQL or the semantics.

- [ ] **Step 5: Exports**

`crates/mc-infrastructure/src/persistence/mod.rs`, after the `SqliteEventStore` re-export:

```rust
pub use self::sqlite::repository::SqliteRepository;
```

`crates/mc-infrastructure/src/lib.rs:14` — add `SqliteRepository` to the `pub use persistence::{…}` list.

Run: `cargo build -p mc-infrastructure 2>&1 | grep -E "^(error|warning)" | head`
Expected: no errors, no new warnings.

- [ ] **Step 6: Instantiate the conformance suite and SQLite-only tests**

Append to `sqlite/repository.rs`:

```rust
#[cfg(test)]
mod conformance {
    use std::sync::Arc;

    use super::SqliteRepository;
    use crate::persistence::sqlite::SqliteDb;
    use crate::persistence::sqlite::event_store::SqliteEventStore;

    fn make() -> (tempfile::TempDir, Arc<SqliteEventStore>, SqliteRepository) {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::open(&dir.path().join("mail-cat.db")).unwrap();
        let store = Arc::new(SqliteEventStore::new(db.clone()));
        (dir, store.clone(), SqliteRepository::new(db, store))
    }

    crate::persistence::read_repository_conformance::read_repository_conformance!(make());
}

#[cfg(test)]
mod tests {
    use super::*;
    use mc_domain::{EventEnvelope, MailboxType, ProviderType, ReadModelProjector};

    fn open() -> (tempfile::TempDir, SqliteRepository) {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::open(&dir.path().join("mail-cat.db")).unwrap();
        let store = Arc::new(SqliteEventStore::new(db.clone()));
        (dir, SqliteRepository::new(db, store))
    }

    fn at_position(position: u64, event: DomainEvent) -> EventEnvelope {
        let mut envelope = EventEnvelope::new("agg".into(), event.aggregate_type(), 1, event);
        envelope.global_position = position;
        envelope
    }

    fn account_added() -> DomainEvent {
        DomainEvent::AccountAdded {
            account_id: AccountId::new(),
            name: "Work".into(),
            email: EmailAddress::new("me@example.com").unwrap(),
            provider_type: ProviderType::Imap,
        }
    }

    fn received(
        account_id: AccountId,
        mailbox_id: MailboxId,
        rfc: &str,
    ) -> (MessageId, DomainEvent) {
        let message_id = MessageId::new();
        let event = DomainEvent::MessageReceived {
            message_id,
            account_id,
            mailbox_id,
            provider_uid: ProviderUid::new(1),
            message_rfc_id: Some(rfc.into()),
            in_reply_to: None,
            references: vec![],
            envelope: Envelope {
                from: vec![],
                to: vec![],
                cc: vec![],
                bcc: vec![],
                reply_to: vec![],
                subject: "s".into(),
                date: Utc::now(),
            },
            flags: vec![],
            size_bytes: 1,
            has_attachments: false,
            received_at: Utc::now(),
        };
        (message_id, event)
    }

    #[tokio::test]
    async fn bodies_are_stored_compressed() {
        let (_dir, repo) = open();
        let (id, event) = received(AccountId::new(), MailboxId::new(), "<a@x>");
        repo.project(&at_position(1, event)).await.unwrap();
        let text = "the same line again\n".repeat(1000);

        repo.put_body(&id, Some(text.clone()), None).await.unwrap();

        let stored: i64 = repo
            .db
            .read(move |c| {
                c.query_row(
                    "SELECT length(text_zstd) FROM message_bodies WHERE message_id = ?1",
                    [id.as_uuid()],
                    |r| r.get(0),
                )
                .map_err(repo_err)
            })
            .await
            .unwrap();
        assert!(stored < 1000, "{stored} bytes for {} of text", text.len());
        let message = repo.get_message(&id).await.unwrap().unwrap();
        assert_eq!(message.body_text, Some(text));
    }

    #[tokio::test]
    async fn deleting_a_message_takes_its_body_along() {
        let (_dir, repo) = open();
        let (id, event) = received(AccountId::new(), MailboxId::new(), "<a@x>");
        repo.project(&at_position(1, event)).await.unwrap();
        repo.put_body(&id, Some("x".into()), None).await.unwrap();

        let deleted = DomainEvent::MessageDeleted {
            message_id: id,
            mailbox_id: MailboxId::new(),
            permanently: true,
        };
        repo.project(&at_position(2, deleted)).await.unwrap();

        let bodies: i64 = repo
            .db
            .read(|c| c.query_row("SELECT COUNT(*) FROM message_bodies", [], |r| r.get(0)).map_err(repo_err))
            .await
            .unwrap();
        assert_eq!(bodies, 0);
    }

    #[tokio::test]
    async fn project_batch_commits_projection_and_checkpoint_together() {
        let (_dir, repo) = open();

        let committed = repo
            .project_batch("read_model", vec![at_position(7, account_added()), at_position(8, account_added())])
            .await
            .unwrap();

        assert!(committed);
        assert_eq!(repo.get_checkpoint("read_model").await.unwrap(), 8);
        assert_eq!(repo.list_accounts().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn failed_batch_rolls_back_projection_and_checkpoint() {
        let (_dir, repo) = open();
        // Make the second event's arm fail inside the transaction.
        repo.db
            .write(|c| c.execute_batch("DROP TABLE contact_emails; DROP TABLE contacts;").map_err(repo_err))
            .await
            .unwrap();
        let contact = DomainEvent::ContactAdded {
            contact_id: ContactId::new(),
            display_name: "Ada".into(),
            emails: vec![mc_domain::ContactEmail::primary(EmailAddress::new("ada@example.com").unwrap())],
            source: mc_domain::ContactSource::Local,
            external_id: None,
            account_scope: None,
        };

        let result = repo
            .project_batch("read_model", vec![at_position(1, account_added()), at_position(2, contact)])
            .await;

        assert!(result.is_err());
        assert!(repo.list_accounts().await.unwrap().is_empty());
        assert_eq!(repo.get_checkpoint("read_model").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn deleting_one_rfc_copy_keeps_the_conversation() {
        let (_dir, repo) = open();
        let account = AccountId::new();
        let (inbox_copy, a) = received(account, MailboxId::new(), "<same@x>");
        let (_label_copy, b) = received(account, MailboxId::new(), "<same@x>");
        for (pos, event) in [a, b].into_iter().enumerate() {
            repo.project(&at_position(pos as u64 + 1, event)).await.unwrap();
        }

        let deleted = DomainEvent::MessageDeleted {
            message_id: inbox_copy,
            mailbox_id: MailboxId::new(),
            permanently: false,
        };
        repo.project(&at_position(3, deleted)).await.unwrap();

        let conv = repo
            .get_conversation(&ThreadId::new("<same@x>"), &account)
            .await
            .unwrap()
            .expect("the other copy still holds the thread");
        assert_eq!(conv.message_count, 1);
    }

    #[tokio::test]
    async fn mailbox_paths_round_trip() {
        let (_dir, repo) = open();
        let (account, mailbox) = (AccountId::new(), MailboxId::new());
        let path = MailboxPath::parse("Work.Projects.2026", '.').unwrap();
        let discovered = DomainEvent::MailboxDiscovered {
            mailbox_id: mailbox,
            account_id: account,
            path: path.clone(),
            mailbox_type: MailboxType::Custom,
        };
        repo.project(&at_position(1, discovered)).await.unwrap();

        let stored = repo.get_mailbox(&mailbox).await.unwrap().unwrap();
        assert_eq!(stored.path, path);
        assert_eq!(stored.display_name, "2026");
    }
}
```

- [ ] **Step 7: Run the SQLite suite**

Run: `cargo test -p mc-infrastructure persistence::sqlite 2>&1 | grep -E "test result|FAILED|panicked"`
Expected: `test result: ok` — the full read-repository conformance suite (Tasks 2–3), the 6 SQLite-only tests, plus the existing `SqliteDb`/event-store tests. A conformance failure means the SQLite semantics diverge from sled: fix SQLite (sled is the reference) unless the divergence is a ruling (R6–R10); report any such case.

Run: `cargo test --workspace 2>&1 | grep -E "test result|FAILED"`
Expected: all `ok`.

Run: `cargo clippy -p mc-infrastructure --all-targets 2>&1 | grep -E "sqlite/|read_repository_conformance" | head`
Expected: no output (no warnings in the new files).

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock crates/mc-infrastructure/Cargo.toml \
        crates/mc-infrastructure/src/lib.rs crates/mc-infrastructure/src/persistence/mod.rs \
        crates/mc-infrastructure/src/persistence/sqlite/
git commit -m "feat(sqlite): SqliteRepository — read model as migration #2, transactional batch projection

All read traits, both write repositories, checkpoints, zstd bodies, backfill
queue. Conversations are recomputed from messages rows, so replays converge.
Runs the read-repository conformance suite; catch-up batches commit together
with their checkpoint."
```

---

### Task 5: Spec bookkeeping and final verification

**Files:**
- Modify: `docs/superpowers/specs/2026-10-10-sqlite-migration-design.md` (§4 schema notes, §10)

- [ ] **Step 1: Amend §4**

Under the §4 "Notes" list, add:

```markdown
- Plan 3 rulings: `messages.flags` is JSON `Vec<Flag>` plus a `seen` column
  (IMAP keywords don't fit a bitset; `messages_unread` is `WHERE seen = 0`).
  No FK `mailboxes → accounts` and `messages_mailbox_uid` is not UNIQUE:
  catch-up replays over later state, and either constraint would turn a
  replay or a UIDVALIDITY reset into a boot failure. Conversations are
  recomputed from `messages` on every change to a thread; `rebuild_index`
  recomputes all of them. Timestamps are unix nanos.
```

- [ ] **Step 2: Update §10**

Tick, with the Task 3 commit hash:
- the `MailboxDiscovered` item (`- [x] … (plan 3, <hash>: both backends keep counts on replay)`).

Rewrite the live-projection item as ticked-in-part and keep the remainder open:

```markdown
- [x] Projector arms are replay-safe on both backends, proven by the conformance test
  `replaying_the_log_over_live_state_changes_nothing` (plan 3, <hash>). Catch-up batches
  commit with their checkpoint on SQLite (plan 3, <hash>).
- [ ] Live projection (`append_and_project`, bus subscriber `wiring.rs:506`) still doesn't
  checkpoint, so catch-up re-projects since the last catch-up — harmless now, wasted work.
  **Plan 5** (wiring).
```

Add under a new heading `**From writing plan 3:**`:

```markdown
- [ ] sled `MessageDeleted` leaves the `body_backfill` entry and drops the `rfc_id_seen`
  marker even if another copy exists (conversation undercount). SQLite is correct;
  vanishes with sled in plan 5.
- [ ] `ConversationReadRepository::rebuild_index` doc says "events replayed"; SQLite returns
  the number of conversations. Reword the trait doc when sled is deleted.
- [ ] `search_contacts` on SQLite folds ASCII case only (`LIKE`); sled lowercased Unicode.
```

- [ ] **Step 3: Final verification**

Run: `cargo test --workspace 2>&1 | grep -E "test result|FAILED"`
Expected: all `ok`.

Run: `git diff main --stat -- crates/mc-server`
Expected: empty (wiring untouched).

Run: `cargo clippy --workspace --all-targets 2>&1 | grep -E "sqlite/|read_repository_conformance|projections/read_model.rs|read_model_projector.rs"`
Expected: no output (pre-existing warnings elsewhere are tracked in spec §10).

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-10-10-sqlite-migration-design.md
git commit -m "docs(spec): record plan 3 rulings and deferred items"
```
