# SQLite Migration — Plan 2: Domain Events Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Message bodies leave the event log (stored once through a new `MessageBodyStore`), and server-reported status events (`AccountSyncStarted/Completed`, `MailboxCountsUpdated`) stop being appended while still reaching the read model and the in-process bus. Production stays on sled.

**Architecture:** `MessageBodyFetched` keeps `message_id` + `attachments` only. Every producer writes the body through `MessageBodyStore::put_body` *before* appending the event; the sled read model stores it in the existing `messages` tree, and the projector arm no longer touches body fields (so replaying old or new events never clobbers a stored body). Status events go through a new `SyncService::project_transient`: projected into the read model and published on the bus, never appended.

**Tech Stack:** Rust 2024, tokio, async-trait, serde_json, sled (production backend until plan 5).

**Spec:** `docs/superpowers/specs/2026-10-10-sqlite-migration-design.md` (§2 "Status events leave the log", §3 bodies, §8 phase 2, §10 deferred).

## Plan series

| Plan | Scope | State |
|---|---|---|
| 1 | `SqliteDb`, `SqliteEventStore`, conformance suite | done (`feat/sqlite-migration`) |
| **2 (this)** | Body out of events, `MessageBodyStore`, status events transient | |
| 3 | `SqliteRepository` + read-repository conformance suite + **transactional batch projection** (moved here from phase 2, see Ruling R2) | |
| 4 | Index-only Tantivy | |
| 5 | Wiring, `migrate`, cutover, delete sled | |

## Rulings taken while writing this plan (spec amended in the same commit)

- **R1 — no `MailboxReadRepository::update_server_counts`.** Status events are projected through the existing `ReadModelProjector` without being appended (`project_transient`). The sled `MailboxCountsUpdated` arm already applies counts; the gRPC mailbox stream already consumes the event from the bus (`message_service.rs:679-689`). A new trait method would duplicate that arm in 5 implementations for no behaviour. Cost if wrong: plan 3 adds the method if the SQLite projector needs it.
- **R2 — transactional batch projection moves to plan 3.** Its only consumer is the SQLite repository; designing the hook before that implementation exists would be guesswork. Cost if wrong: none for plan 2 (sled has no transactions to exploit).
- **R3 — `AccountSyncFailed` stays unemitted.** Nothing constructs it today (its desktop-notification arm in `wiring.rs:698-709` is dead). Emitting it is a feature, not part of this migration; noted in spec §10.

## Global Constraints

- Production backend stays **sled**; `mc-server` wiring is not touched except where a trait bound forces it to compile.
- `MessageSent` and `DraftSaved` **keep** their body fields (spec §3: user-authored).
- Old persisted `MessageBodyFetched` events (with `body_text`/`body_html`) must still deserialize — the sled store and the plan-5 migrator read them.
- Body is written **before** the event is appended (spec §3); the backfill queue is drained only by projecting `MessageBodyFetched`.
- Commits carry **no** `Co-Authored-By` trailer.
- Comments explain *why*, never restate code.

## Review Focus

1. **Old sled events still carry a body** — deserializing a legacy `MessageBodyFetched` JSON with `body_text`/`body_html` succeeds and yields the new shape. (Task 2, `legacy_body_fetched_event_still_deserializes`)
2. **Catch-up or rebuild replays `MessageBodyFetched` after the body was stored** — the stored body survives. (Task 2, `body_fetched_event_keeps_stored_body_and_drains_backfill`)
3. **Body stored but the append fails** — the message stays in the backfill queue and is fetched again, never silently marked done. (Task 1, `put_body_alone_does_not_drain_backfill`)
4. **Mailbox unread badges in the UI** — `MailboxCountsUpdated` still reaches the bus after it stops being appended. (Task 3, `status_events_reach_projector_and_bus_but_not_the_log`)
5. **Opening a message whose body was never fetched** (UI → `FetchMessageBody` command) — the body is stored and readable right after the command returns. (Task 2, `handle_fetch_body_stores_body_through_body_store`)

---

### Task 1: `MessageBodyStore` port and sled adapter

**Files:**
- Modify: `crates/mc-domain/src/traits/repository.rs` (after `BodyBackfillQueue`, ~line 175)
- Modify: `crates/mc-domain/src/lib.rs:32-37` (re-export)
- Modify: `crates/mc-infrastructure/src/persistence/sled/read_repository.rs` (new impl block near the `BodyBackfillQueue` impl; forwarding impl near `:2068`; tests at the end)

**Interfaces:**
- Produces:
  ```rust
  #[async_trait]
  pub trait MessageBodyStore: Send + Sync {
      async fn put_body(
          &self,
          message_id: &MessageId,
          body_text: Option<String>,
          body_html: Option<String>,
      ) -> Result<(), DomainError>;
  }
  ```
  exported as `mc_domain::MessageBodyStore`; implemented for `SledRepository<ES>` and `SharedSledRepository<ES>`.

- [ ] **Step 1: Write the failing tests**

Append inside the existing `#[cfg(test)] mod tests` of `read_repository.rs` (it already has `make_store_and_repo()` and `make_event_envelope(...)`; reuse the `MessageReceived` construction used by `backfill_queue_newest_first_and_dequeues_on_body` at ~:2301 — copy its received-event block):

```rust
    #[tokio::test]
    async fn put_body_sets_body_on_existing_message() {
        let (_es, repo) = make_store_and_repo();
        let account_id = AccountId::new();
        let message_id = MessageId::new();
        repo.project(&received_event(account_id, message_id, chrono::Utc::now()))
            .await
            .unwrap();

        repo.put_body(&message_id, Some("plain".into()), Some("<p>html</p>".into()))
            .await
            .unwrap();

        let msg = MessageReadRepository::get_message(&repo, &message_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.body_text.as_deref(), Some("plain"));
        assert_eq!(msg.body_html.as_deref(), Some("<p>html</p>"));
    }

    #[tokio::test]
    async fn put_body_on_unknown_message_is_a_noop() {
        let (_es, repo) = make_store_and_repo();
        repo.put_body(&MessageId::new(), Some("x".into()), None).await.unwrap();
    }

    #[tokio::test]
    async fn put_body_alone_does_not_drain_backfill() {
        let (_es, repo) = make_store_and_repo();
        let account_id = AccountId::new();
        let message_id = MessageId::new();
        repo.project(&received_event(account_id, message_id, chrono::Utc::now()))
            .await
            .unwrap();

        repo.put_body(&message_id, Some("plain".into()), None).await.unwrap();

        // Only the appended MessageBodyFetched marks the body done; if that
        // append failed, the message must still be fetched again.
        assert_eq!(repo.count_pending_backfill(&account_id).await.unwrap(), 1);
    }
```

And the helper, next to `make_event_envelope` in the same test module:

```rust
    fn received_event(
        account_id: AccountId,
        message_id: MessageId,
        received_at: chrono::DateTime<chrono::Utc>,
    ) -> EventEnvelope {
        make_event_envelope(
            1,
            &message_id.to_string(),
            AggregateType::Message,
            1,
            DomainEvent::MessageReceived {
                message_id,
                account_id,
                mailbox_id: MailboxId::new(),
                provider_uid: ProviderUid::new(1),
                message_rfc_id: None,
                in_reply_to: None,
                references: vec![],
                envelope: mc_domain::Envelope {
                    from: vec![],
                    to: vec![],
                    cc: vec![],
                    bcc: vec![],
                    reply_to: vec![],
                    subject: "s".into(),
                    date: received_at,
                },
                flags: vec![],
                size_bytes: 1,
                has_attachments: false,
                received_at,
            },
        )
    }
```

If `make_event_envelope`'s parameter order differs from `(global_position, aggregate_id, aggregate_type, sequence, event)`, match the existing helper — it is the one used at ~:2349.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p mc-infrastructure put_body 2>&1 | grep -E "^error" | head -3`
Expected: `no method named put_body` (trait not defined).

- [ ] **Step 3: Add the port**

`crates/mc-domain/src/traits/repository.rs`, after the `BodyBackfillQueue` trait:

```rust
/// Where fetched message bodies live. A body is a copy of server state, so it
/// is stored once here rather than in the event log; `MessageBodyFetched` only
/// records that it happened.
#[async_trait]
pub trait MessageBodyStore: Send + Sync {
    /// Store the body of an already-received message. Unknown ids are ignored:
    /// the message may have been deleted while its body was in flight.
    async fn put_body(
        &self,
        message_id: &MessageId,
        body_text: Option<String>,
        body_html: Option<String>,
    ) -> Result<(), DomainError>;
}
```

`crates/mc-domain/src/lib.rs`, add `MessageBodyStore` to the `pub use traits::repository::{…}` list.

- [ ] **Step 4: Implement it on sled**

`read_repository.rs`, add `MessageBodyStore` to the `use mc_domain::{…}` list and, next to the `BodyBackfillQueue` impl for `SledRepository`:

```rust
#[async_trait]
impl<ES: EventStore> MessageBodyStore for SledRepository<ES> {
    async fn put_body(
        &self,
        message_id: &MessageId,
        body_text: Option<String>,
        body_html: Option<String>,
    ) -> Result<(), DomainError> {
        let tree = self.tree(MESSAGES_TREE)?;
        let key = message_id.to_string();
        let Some(bytes) = tree
            .get(key.as_bytes())
            .map_err(|e| DomainError::Repository(e.to_string()))?
        else {
            return Ok(());
        };
        let mut message: Message = serde_json::from_slice(&bytes)
            .map_err(|e| DomainError::Serialization(e.to_string()))?;
        message.body_text = body_text;
        message.body_html = body_html;
        let json = serde_json::to_vec(&message)
            .map_err(|e| DomainError::Serialization(e.to_string()))?;
        tree.insert(key.as_bytes(), json)
            .map_err(|e| DomainError::Repository(e.to_string()))?;
        Ok(())
    }
}
```

Next to the other `SharedSledRepository` forwarding impls (~:2068):

```rust
#[async_trait]
impl<ES: EventStore> MessageBodyStore for SharedSledRepository<ES> {
    async fn put_body(
        &self,
        message_id: &MessageId,
        body_text: Option<String>,
        body_html: Option<String>,
    ) -> Result<(), DomainError> {
        self.0.put_body(message_id, body_text, body_html).await
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p mc-infrastructure put_body 2>&1 | grep -E "test |test result"`
Expected: 3 tests `ok`.

- [ ] **Step 6: Commit**

```bash
git add crates/mc-domain/src/ crates/mc-infrastructure/src/persistence/sled/read_repository.rs
git commit -m "feat(domain): MessageBodyStore port, sled adapter"
```

---

### Task 2: Body out of `MessageBodyFetched`

One atomic change: the event shape, every producer, every consumer and the affected tests, so the workspace compiles at the end.

**Files:**
- Modify: `crates/mc-domain/src/events/mod.rs:186-192` (variant) + its test module
- Modify: `crates/mc-domain/src/model/message.rs:107-126` (`set_body`), `:209-219` (`apply`), `:366-389` (test)
- Modify: `crates/mc-application/src/services/sync_service.rs` — bounds at `:97`, `:141`, `:1369`; producers at `:549`, `:689`, `:749`, `:1087`; test mocks + test `:2048`
- Modify: `crates/mc-application/src/handlers/message.rs` — bounds at `:30`, `:37`; `handle_fetch_body` `:54-85`; `MockMessageReadRepo` `:941`; new test
- Modify: `crates/mc-application/src/bus/in_process.rs:36,53,91` (bounds)
- Modify: `crates/mc-infrastructure/src/persistence/sled/read_repository.rs` — projector arm `:898-930`; tests at `:2349`, `:2432`, `:2837`

**Interfaces:**
- Consumes: `MessageBodyStore::put_body` (Task 1).
- Produces: `DomainEvent::MessageBodyFetched { message_id: MessageId, attachments: Vec<Attachment> }`; `SyncService::store_body(&self, message_id: &MessageId, body_text: Option<String>, body_html: Option<String>, attachments: Vec<Attachment>) -> Result<(), ApplicationError>` (private); `SyncService`'s `P` and `MessageCommandHandler`/`InProcessCommandBus`'s `MR` gain `+ MessageBodyStore`.

- [ ] **Step 1: Write the failing tests**

(a) `crates/mc-domain/src/events/mod.rs`, inside its `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn legacy_body_fetched_event_still_deserializes() {
        // Shape written to sled before bodies left the log; the store and the
        // sled → SQLite migrator still read these.
        let legacy = r#"{"MessageBodyFetched":{
            "message_id":"019f3880-7549-7310-8e77-f48d3b5a12e2",
            "body_text":"hello","body_html":"<p>hello</p>","attachments":[]}}"#;

        let event: DomainEvent = serde_json::from_str(legacy).unwrap();

        assert!(matches!(event, DomainEvent::MessageBodyFetched { ref attachments, .. } if attachments.is_empty()));
        let reserialized = serde_json::to_string(&event).unwrap();
        assert!(!reserialized.contains("body_text"), "{reserialized}");
    }
```

(b) `crates/mc-domain/src/model/message.rs`, replace the body of `set_body_produces_event` after `let event = msg.set_body(…);`:

```rust
        assert_eq!(msg.body_text.as_deref(), Some("Hello plain text"));
        assert_eq!(msg.body_html.as_deref(), Some("<p>Hello HTML</p>"));
        assert_eq!(event.event_name(), "MessageBodyFetched");
        let json = serde_json::to_string(&event).unwrap();
        assert!(!json.contains("Hello"), "body must not travel in the event: {json}");
```

(c) `read_repository.rs` test module:

```rust
    #[tokio::test]
    async fn body_fetched_event_keeps_stored_body_and_drains_backfill() {
        let (_es, repo) = make_store_and_repo();
        let account_id = AccountId::new();
        let message_id = MessageId::new();
        repo.project(&received_event(account_id, message_id, chrono::Utc::now()))
            .await
            .unwrap();
        repo.put_body(&message_id, Some("plain".into()), None).await.unwrap();

        let fetched = make_event_envelope(
            2,
            &message_id.to_string(),
            AggregateType::Message,
            2,
            DomainEvent::MessageBodyFetched { message_id, attachments: vec![] },
        );
        // Twice: catch-up and rebuild replay events that were already projected.
        repo.project(&fetched).await.unwrap();
        repo.project(&fetched).await.unwrap();

        let msg = MessageReadRepository::get_message(&repo, &message_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.body_text.as_deref(), Some("plain"));
        assert_eq!(repo.count_pending_backfill(&account_id).await.unwrap(), 0);
    }
```

(d) `sync_service.rs` — in `fetch_message_bodies_bulk_uses_one_session_for_whole_batch` (`:2048`), append at the end of the test:

```rust
        // Bodies went to the body store, once each, not into the events.
        let bodies = projector.stored_bodies();
        assert_eq!(bodies.len(), 3);
        for entry in &entries {
            let text = bodies.get(&entry.message_id).and_then(|(t, _)| t.clone());
            assert!(text.is_some_and(|t| t.starts_with("body-")), "{bodies:?}");
        }
        for (_, event) in &appended {
            let json = serde_json::to_string(event).unwrap();
            assert!(!json.contains("body-"), "body leaked into the log: {json}");
        }
```

(e) `handlers/message.rs` test module:

```rust
    #[tokio::test]
    async fn handle_fetch_body_stores_body_through_body_store() {
        let msg = make_test_message();
        let msg_id = msg.id;
        let provider = MockEmailProvider::new();
        *provider.detail_response.write().unwrap() = Some(Message {
            body_text: Some("fetched text".into()),
            body_html: Some("<p>fetched</p>".into()),
            ..msg.clone()
        });

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(provider)));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::new());
        let handler = MessageCommandHandler::new(es, registry, read_repo.clone(), bus.clone());

        handler.handle_fetch_body(msg_id, MailboxPath::inbox()).await.unwrap();

        assert_eq!(
            read_repo.stored_bodies.lock().unwrap().as_slice(),
            &[(msg_id, Some("fetched text".to_string()), Some("<p>fetched</p>".to_string()))]
        );
        assert_eq!(bus.last_event_name().unwrap(), "MessageBodyFetched");
    }
```

If `MockEmailProvider::detail_response` is not `RwLock<Option<Message>>`, use the setter the existing tests use for it (search `detail_response` in the test module).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --workspace 2>&1 | grep -E "^error" | sort | uniq -c | head`
Expected: compile errors only — `stored_bodies` and `MessageBodyStore` impls missing on the mocks. (The legacy-deserialization test compiles and passes already: it guards compatibility, it does not drive the change.)

- [ ] **Step 3: Change the event**

`crates/mc-domain/src/events/mod.rs`:

```rust
    /// A message body was fetched and stored (`MessageBodyStore`). The body
    /// itself is not in the event: it is re-fetchable server state, and keeping
    /// it here made the log the largest copy of every mailbox.
    MessageBodyFetched {
        message_id: MessageId,
        attachments: Vec<Attachment>,
    },
```

`crates/mc-domain/src/model/message.rs` — `set_body` returns the slimmer event (the aggregate still holds the body in memory for its caller):

```rust
        DomainEvent::MessageBodyFetched {
            message_id: self.id,
            attachments,
        }
```

and the `apply` arm:

```rust
            DomainEvent::MessageBodyFetched { attachments, .. } => {
                self.attachments = attachments.clone();
                self.has_attachments = !self.attachments.is_empty();
            }
```

- [ ] **Step 4: Sled projector arm stops writing bodies**

`read_repository.rs:898` — replace the destructuring and the two body assignments:

```rust
            DomainEvent::MessageBodyFetched {
                message_id,
                attachments,
            } => {
```

and delete the lines `message.body_text = body_text.clone();` and `message.body_html = body_html.clone();` (keep the attachments update and the backfill drain).

Update the three existing tests that build the old shape. At `:2349` and `:2432` replace the event literal with
`DomainEvent::MessageBodyFetched { message_id: newer, attachments: vec![] }` (resp. `message_id: stored`).
At `:2837` insert before `let env = make_event_envelope(` of that event:

```rust
        repo.put_body(&message_id, Some("Hello plain".into()), Some("<p>Hello</p>".into()))
            .await
            .unwrap();
```

and replace the literal with `DomainEvent::MessageBodyFetched { message_id, attachments: vec![] }`.

- [ ] **Step 5: SyncService writes the body through the store**

Add `MessageBodyStore` and `Attachment` to the `use mc_domain::{…}` list, then append `+ MessageBodyStore` to the `P:` bound at `:97`, `:141` and `:1369`:

```rust
    P: ReadModelProjector + MailboxReadRepository + MessageReadRepository + ProjectionCheckpoint + BodyBackfillQueue + MessageBodyStore,
```

Add next to `append_and_project`:

```rust
    /// Body first, then the event: if the append fails, the backfill queue (drained
    /// only by projecting `MessageBodyFetched`) still lists the message, so it is
    /// fetched again instead of being marked done without a record.
    async fn store_body(
        &self,
        message_id: &MessageId,
        body_text: Option<String>,
        body_html: Option<String>,
        attachments: Vec<Attachment>,
    ) -> Result<(), ApplicationError> {
        self.projector
            .put_body(message_id, body_text, body_html)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to store body: {e}")))?;
        let event = DomainEvent::MessageBodyFetched { message_id: *message_id, attachments };
        self.append_and_project(&message_id.to_string(), AggregateType::Message, event)
            .await
    }
```

Replace the four producers:

`:549` (start_sync eager bulk) — the `let event = …;` plus the `self.append_and_project(&message_id.to_string(), AggregateType::Message, event)` call inside `if let Err(e) = …` become:

```rust
                                if let Err(e) = self
                                    .store_body(message_id, result.body_text, result.body_html, vec![])
                                    .await
```

(keep the existing `warn!(…"Failed to project body"); continue;` body of the `if let`).

`:689` (`fetch_message_body`):

```rust
        self.store_body(
            message_id,
            fetched.body_text.clone(),
            fetched.body_html.clone(),
            fetched.attachments.clone(),
        )
        .await?;
```

`:749` (`fetch_message_bodies_bulk`):

```rust
            self.store_body(
                &entry.message_id,
                result.body_text.clone(),
                result.body_html.clone(),
                vec![],
            )
            .await?;
```

`:1087` (`sync_mailbox` eager) — same as `:549` with the existing `warn!(…"sync_mailbox: body projection failed"); continue;`:

```rust
                            if let Err(e) = self
                                .store_body(message_id, result.body_text, result.body_html, vec![])
                                .await
```

Test mock — `MockProjector` gains a field and an impl:

```rust
        bodies: Mutex<HashMap<MessageId, (Option<String>, Option<String>)>>,
```

(initialise with `bodies: Mutex::new(HashMap::new()),` in `new()`), plus

```rust
        fn stored_bodies(&self) -> HashMap<MessageId, (Option<String>, Option<String>)> {
            self.bodies.lock().unwrap().clone()
        }
```

and

```rust
    #[async_trait]
    impl MessageBodyStore for MockProjector {
        async fn put_body(
            &self,
            message_id: &MessageId,
            body_text: Option<String>,
            body_html: Option<String>,
        ) -> Result<(), DomainError> {
            self.bodies.lock().unwrap().insert(*message_id, (body_text, body_html));
            Ok(())
        }
    }
```

- [ ] **Step 6: The command handler writes the body through the store**

In `handlers/message.rs` (`:30`, `:37`) and `bus/in_process.rs` (`:36`, `:53`, `:91`), every `MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository` becomes `MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore` (add `MessageBodyStore` to each file's `mc_domain` imports).

`handle_fetch_body`, between `let event = message.set_body(…);` and `self.append_and_publish(…)`:

```rust
        // Body before the event, as in SyncService::store_body.
        self.message_read_repo
            .put_body(&message_id, detail.body_text.clone(), detail.body_html.clone())
            .await?;
```

`MockMessageReadRepo` gains `stored_bodies: Mutex<Vec<(MessageId, Option<String>, Option<String>)>>` (initialised to `Mutex::new(vec![])` in both `new()` and `with_mailboxes()`), plus:

```rust
    #[async_trait]
    impl MessageBodyStore for MockMessageReadRepo {
        async fn put_body(
            &self,
            message_id: &MessageId,
            body_text: Option<String>,
            body_html: Option<String>,
        ) -> Result<(), DomainError> {
            self.stored_bodies.lock().unwrap().push((*message_id, body_text, body_html));
            Ok(())
        }
    }
```

- [ ] **Step 7: Run the workspace**

Run: `cargo test --workspace 2>&1 | grep -E "^error|FAILED|panicked|test result" | sort | uniq -c`
Expected: no `error`, no `FAILED`; the five new tests pass. If `mc-server` fails to compile on a missing `MessageBodyStore` bound, the concrete type is `SledRepository`/`SharedSledRepository`, which implements it since Task 1 — add the import only.

- [ ] **Step 8: Lint and commit**

Run: `cargo clippy --workspace --all-targets --message-format=short 2>&1 | grep -E "^error|body"`
Expected: no new warnings.

```bash
git add crates/
git commit -m "refactor(events): MessageBodyFetched no longer carries the body

Producers store it through MessageBodyStore before appending; the sled
projector arm no longer writes body fields, so replaying any event leaves
the stored body intact. Old events with body fields still deserialize."
```

---

### Task 3: Status events are transient

**Files:**
- Modify: `crates/mc-application/src/services/sync_service.rs` — `append_and_project` `:1168-1204`, `start_sync` `:262-270` and `:623-633`, `update_mailbox_counts` `:1349-1362`; tests `:2224` and new test

**Interfaces:**
- Produces: `SyncService::project_transient(&self, aggregate_id: &str, aggregate_type: AggregateType, event: DomainEvent) -> Result<(), ApplicationError>` (private). Envelopes it emits have `sequence_number == 0` and `global_position == 0` (never stored).

- [ ] **Step 1: Write the failing test**

In the `sync_service.rs` test module:

```rust
    struct RecordingPublisher(Mutex<Vec<DomainEvent>>);

    impl EventPublisher for RecordingPublisher {
        fn publish(&self, envelope: EventEnvelope) {
            self.0.lock().unwrap().push(envelope.payload);
        }
    }

    #[tokio::test]
    async fn status_events_reach_projector_and_bus_but_not_the_log() {
        let account_id = AccountId::new();
        let (inbox, _) = Mailbox::discover(
            account_id,
            MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
            MailboxType::Inbox,
        );
        let provider = MockEmailProvider {
            mailboxes: vec![inbox],
            envelopes: vec![],
            bulk_fetch_calls: Mutex::new(vec![]),
            status: Some(MailboxStatus {
                uid_validity: 1,
                uid_next: 1,
                total_messages: 42,
                unseen_messages: 5,
            }),
        };
        let event_store = Arc::new(MockEventStore::new());
        let projector = Arc::new(MockProjector::new());
        let bus = Arc::new(RecordingPublisher(Mutex::new(vec![])));
        let service = make_service(provider, event_store.clone(), projector.clone())
            .with_event_publisher(bus.clone());

        service.start_sync(&account_id).await.unwrap();

        let is_status = |e: &DomainEvent| {
            matches!(
                e,
                DomainEvent::AccountSyncStarted { .. }
                    | DomainEvent::AccountSyncCompleted { .. }
                    | DomainEvent::MailboxCountsUpdated { .. }
            )
        };
        let appended: Vec<_> = event_store.appended_events();
        assert!(!appended.iter().any(|(_, e)| is_status(e)), "{appended:?}");
        let projected = projector.projected_events();
        assert!(projected.iter().any(|e| matches!(e, DomainEvent::AccountSyncStarted { .. })));
        assert!(projected.iter().any(|e| matches!(e, DomainEvent::AccountSyncCompleted { .. })));
        assert!(projected.iter().any(|e| matches!(e, DomainEvent::MailboxCountsUpdated { .. })));
        let published = bus.0.lock().unwrap();
        assert!(published.iter().any(|e| matches!(
            e,
            DomainEvent::MailboxCountsUpdated { total_messages: 42, unread_messages: 5, .. }
        )));
    }
```

If `MockEmailProvider` has more fields than the four above, copy the literal from `sync_updates_mailbox_counts` (`:2253`), which constructs the same provider.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p mc-application status_events_reach 2>&1 | grep -E "panicked|test result"`
Expected: FAIL — the `appended` assertion lists `AccountSyncStarted`.

- [ ] **Step 3: Add `project_transient` and route status events through it**

Split the tail of `append_and_project` (everything after the `append` call that builds the envelope, projects it and publishes it) into:

```rust
    /// Server-reported state (sync lifecycle, mailbox counts): applied to the
    /// read model and announced on the bus, but never appended — it is not a
    /// domain fact, and appending it every poll grew the log without bound.
    async fn project_transient(
        &self,
        aggregate_id: &str,
        aggregate_type: AggregateType,
        event: DomainEvent,
    ) -> Result<(), ApplicationError> {
        self.project_and_publish(EventEnvelope::new(aggregate_id.to_string(), aggregate_type, 0, event))
            .await
    }

    async fn project_and_publish(&self, envelope: EventEnvelope) -> Result<(), ApplicationError> {
        self.projector
            .project(&envelope)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to project event: {e}")))?;
        if let Some(publisher) = &self.event_publisher {
            publisher.publish(envelope);
        }
        Ok(())
    }
```

and make `append_and_project` end with:

```rust
        let envelope = EventEnvelope::new(aggregate_id.to_string(), aggregate_type, new_seq, event);
        self.project_and_publish(envelope).await
```

(keep its existing `current_sequence` + `append` part unchanged; drop the now-redundant `event.clone()` if clippy flags it).

Replace in `start_sync`:

```rust
        self.project_transient(&account_id.to_string(), AggregateType::Account, sync_started)
            .await?;
```

and

```rust
        self.project_transient(&account_id.to_string(), AggregateType::Account, sync_completed)
            .await?;
```

and in `update_mailbox_counts` (also fix its doc line to `/// Update mailbox message counts (transient: read model + bus, not the log).`):

```rust
        self.project_transient(&mailbox.id.to_string(), AggregateType::Mailbox, event)
            .await
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --workspace 2>&1 | grep -E "^error|FAILED|panicked|test result" | sort | uniq -c`
Expected: all `ok`, including `status_events_reach_projector_and_bus_but_not_the_log`, `sync_emits_account_lifecycle_events` and `sync_updates_mailbox_counts` (they read `projected_events`, which still sees all three).

- [ ] **Step 5: Lint and commit**

Run: `cargo clippy --workspace --all-targets --message-format=short 2>&1 | grep -E "^error|sync_service"`
Expected: nothing new.

```bash
git add crates/mc-application/src/services/sync_service.rs
git commit -m "refactor(sync): sync lifecycle and mailbox counts are transient, not appended

Projected into the read model and published on the bus (the UI's
CountsChanged stream still sees MailboxCountsUpdated), but no longer
appended on every sync and poll."
```
