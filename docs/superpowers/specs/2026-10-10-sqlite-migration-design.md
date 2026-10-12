# Persistence migration: sled → SQLite

**Date:** 2026-10-10
**Status:** Decisions resolved (§9) — pending spec review

## Problem

Measured on the live install (4 accounts, server up 3 days):

| Metric | Value | Cause |
|---|---|---|
| sled DB on disk | **53 GB** (50 GB blobs + 3 GB) | bodies stored uncompressed JSON, 4 times (below) |
| Tantivy index | 1.8 GB | stores `body_text` (STORED) + positions on bodies |
| Server RSS idle | 415–470 MB → **~210 MB** after today's fixes | heaptrack: 287 MB → 129 MB is sled |
| sled floor | ~100 MB | page table (58 MB, grows with DB size), metrics (21 MB), snapshot (14 MB), flusher |

After capping the sled cache (16 MB) and removing the full-history replay on
every append (`EventStore::current_sequence`), the remaining memory is sled's own
bookkeeping, which scales with DB size and has no knob. sled 0.34 is
effectively unmaintained. Target "a few tens of MB" is not reachable on sled.

Every fetched message body currently lives in **four** places:

1. `events_by_aggregate` — `MessageBodyFetched` payload (JSON)
2. `events_global` — same envelope, written again (sled has no secondary index)
3. `messages` read-model tree — full `Message` JSON incl. `body_text`/`body_html`
4. Tantivy — `body_text` STORED (+ `body_html` indexed)

Beyond storage, the sled read model hand-maintains 13 trees, 6 of which are
secondary indexes (`mailboxes_by_account`, `contacts_by_email`,
`message_to_thread`, `thread_messages`, `rfc_id_seen`, `body_backfill`), with
non-atomic read-modify-write across trees. Pagination deserializes the whole
mailbox per page (`list_messages`, `read_repository.rs:281`).

## Goal

- One SQLite file is the single source of truth. Each piece of data stored once.
- Bodies stored once, zstd-compressed, outside the event log.
- Tantivy stays as the search engine but becomes **index-only**: no stored
  content (§5).
- Idle RSS **< 60 MB**; disk a small fraction of today.
- Projection writes atomic (one transaction per event batch).
- Existing domain traits (`EventStore`, `*ReadRepository`, `ProjectionCheckpoint`,
  `BodyBackfillQueue`, `ReadModelProjector`) keep their signatures where possible,
  so `mc-application` changes stay small.
- Real data migration from the existing sled DB — no forced full re-sync.

## Non-goals

- Changing the gRPC API or the UI.
- Dropping event sourcing for messages (follow-up, §9 D4).
- New search features (query operators, filters, sort options) — follow-up
  spec. This design only lays out an index schema that supports them (§5).
- Connection pooling for IMAP (done separately, already on main).

## 1. Engine & access model

- `rusqlite` with `bundled` (SQLite version pinned by the crate, no system dependency).
- One file: `<data_dir>/mail-cat.db`. `PRAGMA journal_mode=WAL`,
  `synchronous=NORMAL` (fsync at checkpoint, not per commit — content is
  re-fetchable; agreed 2026-10-10), `foreign_keys=ON`, `cache_size=-8000` (8 MB),
  `temp_store=MEMORY`.
- **One writer connection** behind a `std::sync::Mutex`, used from
  `tokio::task::spawn_blocking`. SQLite serializes writers anyway.
- **Readers**: small fixed pool (4) of read-only connections, same pattern
  (`Mutex<Vec<Connection>>`, checkout/return). No r2d2 dependency.
- Schema versioned with `PRAGMA user_version`; migrations are an ordered
  `&[&str]` of SQL applied at open. No migration crate.

## 2. Event store schema

```sql
CREATE TABLE events (
  global_position INTEGER PRIMARY KEY,          -- rowid: monotonic, no counter key
  event_id        BLOB    NOT NULL,             -- uuid v7, 16 bytes
  aggregate_id    TEXT    NOT NULL,
  aggregate_type  INTEGER NOT NULL,
  sequence        INTEGER NOT NULL,
  event_type      TEXT    NOT NULL,             -- DomainEvent::event_name(), for filtering in SQL
  occurred_at     INTEGER NOT NULL,             -- unix nanos: envelopes round-trip exactly
  payload         BLOB    NOT NULL,             -- serde_json of DomainEvent
  UNIQUE (aggregate_id, sequence)
);
```

- One write per event (today: two). `load_all_events` = `WHERE global_position >= ?
  ORDER BY global_position`; type filter moves into SQL.
- OCC becomes **atomic**: `append` runs in one `IMMEDIATE` transaction;
  `current_sequence` = `SELECT MAX(sequence)`; `UNIQUE(aggregate_id, sequence)`
  turns a race into a constraint error mapped to `ConcurrencyConflict`.
  (Today the check is a non-atomic read-compare.)
- `subscribe()` keeps the in-process `broadcast` channel, sent **after commit**.
- Payload stays JSON (debuggable with `sqlite3`, `MessageSent`'s
  `#[serde(default)]` compatibility preserved). Events become small once bodies
  leave them (§3), so binary encoding is not worth it.

### Status events leave the log

`AccountSyncStarted/Completed/Failed` are no-ops in the projector but are
appended on every sync, forever, and the whole Account history is replayed by
`AccountWriteRepository::load` (10 MB retained in the heap profile).
`MailboxCountsUpdated` is a per-poll counter snapshot.

- Sync status and mailbox counts are **transient**: `SyncService` projects
  them into the read model and publishes them on the bus, but does not append
  them (plan 2, R1). The existing projector arm applies the counts; the gRPC
  mailbox stream keeps receiving `MailboxCountsUpdated` from the bus. No new
  repository method.
- The migration (§7) drops these events.

## 3. Bodies: content cache, not events

A fetched body is a copy of server state, re-downloadable at any time. It does
not belong in an append-only log.

```sql
CREATE TABLE message_bodies (
  message_id  BLOB PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
  text_zstd   BLOB,
  html_zstd   BLOB,
  fetched_at  INTEGER NOT NULL
);
```

- `MessageBodyFetched` keeps `message_id` + `attachments` (metadata) but **no
  body**. `SyncService` writes the body through a new `MessageBodyStore` trait
  (`put_body`, `get_body`) *then* appends the event. The read model
  `get_message` joins the body table.
- `MessageSent` and `DraftSaved` **keep** their body in the event: user-authored,
  low volume, not re-fetchable before the server has it.
- zstd level 3 via the `zstd` crate. Compression happens in the store, not the domain.
- Bodies are kept **locally, forever**, like today: one download per message,
  then always read from SQLite. "Cache" means *re-derivable*, not ephemeral —
  a lost row is recoverable by putting the message back in the backfill queue.
- `messages.body_state` is `pending | stored | evicted`. `evicted` is unused by
  this design; it exists so a later retention policy (by age or size, possibly
  HTML-only) is a setting, not a schema migration. Decide it after measuring the
  compressed size post-migration.

## 4. Read model schema

Indexes replace the hand-maintained trees.

```sql
CREATE TABLE accounts  (id BLOB PRIMARY KEY, name TEXT, email TEXT, provider_type TEXT,
                        status TEXT, total_unread INTEGER NOT NULL DEFAULT 0);

CREATE TABLE mailboxes (id BLOB PRIMARY KEY, account_id BLOB NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                        path TEXT NOT NULL, delimiter TEXT, display_name TEXT, mailbox_type TEXT,
                        flags TEXT, total_messages INTEGER, unread_messages INTEGER,
                        uid_validity INTEGER, uid_next INTEGER);
CREATE INDEX mailboxes_account ON mailboxes(account_id);          -- was mailboxes_by_account

CREATE TABLE messages (
  id BLOB PRIMARY KEY, account_id BLOB NOT NULL, mailbox_id BLOB,  -- NULL for drafts
  provider_uid INTEGER, rfc_message_id TEXT, in_reply_to TEXT, refs TEXT,  -- refs: JSON array
  thread_id TEXT,                                                            -- was message_to_thread + thread_messages
  subject TEXT, from_name TEXT, from_email TEXT,
  addresses TEXT NOT NULL,                                                   -- JSON {from,to,cc,bcc,reply_to}
  flags INTEGER NOT NULL,                                                    -- bitset; seen bit indexed below
  size_bytes INTEGER, has_attachments INTEGER, attachments TEXT,             -- JSON metadata
  sent_at INTEGER, received_at INTEGER NOT NULL, internal_date INTEGER,
  body_state TEXT NOT NULL DEFAULT 'pending'                                 -- pending|stored|evicted; was body_backfill
);
CREATE INDEX messages_mailbox_date ON messages(mailbox_id, received_at DESC);           -- list_messages
CREATE UNIQUE INDEX messages_mailbox_uid ON messages(mailbox_id, provider_uid);          -- find_by_provider_uid
CREATE INDEX messages_thread ON messages(account_id, thread_id, received_at);           -- get_thread
CREATE INDEX messages_rfc ON messages(account_id, rfc_message_id);                      -- was rfc_id_seen
CREATE INDEX messages_backfill ON messages(account_id, received_at DESC) WHERE body_state = 'pending';
CREATE INDEX messages_unread ON messages(mailbox_id) WHERE (flags & 1) = 0;             -- find_unread_by_mailbox

CREATE TABLE conversations (account_id BLOB NOT NULL, thread_id TEXT NOT NULL, subject TEXT,
                            message_count INTEGER, unread_count INTEGER, last_message_at INTEGER,
                            latest_sender TEXT, sample_message_id BLOB,
                            PRIMARY KEY (account_id, thread_id));
CREATE INDEX conversations_recent ON conversations(account_id, last_message_at DESC);

CREATE TABLE contacts (id BLOB PRIMARY KEY, display_name TEXT, primary_email TEXT,
                       organization TEXT, source TEXT, account_scope BLOB, emails TEXT);  -- emails: JSON
CREATE TABLE contact_emails (email TEXT NOT NULL, contact_id BLOB NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
                             PRIMARY KEY (email, contact_id));                            -- was contacts_by_email

CREATE TABLE projection_checkpoints (name TEXT PRIMARY KEY, position INTEGER NOT NULL);
```

Notes:
- Conversations stay a materialized table (list view needs counts without
  aggregating per request). `conversations_for_mailbox` becomes a join on
  `messages.thread_id`.
- `message_summaries` disappears: `MessageSummary` is a column projection of
  `messages`. The inverted-timestamp key trick is replaced by the index.
- Each projected event (or a catch-up batch of ≤ 1000) is applied in **one
  transaction** together with its checkpoint update. That removes the
  cross-tree inconsistency class entirely.
- Drafts: `DraftSaved` today writes `messages` with a random mailbox id and no
  summary. In SQLite `messages.mailbox_id` is **nullable** and drafts are stored
  with `NULL`; every mailbox-scoped query already filters by `mailbox_id`, so
  drafts stay out of lists exactly as today. Covered by a conformance test.
- IDs as 16-byte BLOBs (uuid), not TEXT: halves index size.
- Plan 3 rulings: `messages.flags` is JSON `Vec<Flag>` plus a `seen` column
  (IMAP keywords don't fit a bitset; `messages_unread` is `WHERE seen = 0`).
  No FK `mailboxes → accounts` and `messages_mailbox_uid` is not UNIQUE:
  catch-up replays over later state, and either constraint would turn a
  replay or a UIDVALIDITY reset into a boot failure. Conversations are
  recomputed from `messages` on every change to a thread; `rebuild_index`
  recomputes all of them. Timestamps are unix nanos.

## 5. Full-text search: Tantivy, index-only

Today Tantivy STORES subject, from, to, `body_text`, date, and the search
result list is rendered from those stored fields (`search_service.rs:50-80`;
`suggest` aggregates stored docs, `tantivy_index.rs:434-574`). Content is
therefore in the read model *and* in Tantivy's doc store.

Tantivy is kept — query power and fast sorting/filtering matter, and search
will be extended next (operators like `from:`, `is:unread`, `has:attachment`,
`in:`, date ranges). It stops being a second content store.

**Message index schema** (`tantivy_index.rs`, rebuilt once by the migration):

| Field | Options | Purpose |
|---|---|---|
| `message_id` | STRING, **STORED** | the only stored field; hit → SQLite row |
| `account_id`, `mailbox_id` | STRING, FAST | scope filters (`in:`) |
| `date` | i64, INDEXED, FAST | sort by date, `before:`/`after:` |
| `flags` | u64, FAST | `is:unread`, `is:flagged` |
| `has_attachment` | bool, FAST | `has:attachment` |
| `subject`, `from_name`, `body` | TEXT `en_stem`, positions | full text, phrases |
| `from_email`, `to`, `cc` | TEXT `email_tokenizer` | address search |

`body` = `body_text`, or tag-stripped `body_html` when there is no text part
(one field instead of two). FAST fields are columnar index data, not content.

**Read path:** `search` returns `message_id` + score (+ sort key). The handler
hydrates the visible page from SQLite (`SELECT … FROM messages WHERE id IN (…)`)
and builds snippets with `SnippetGenerator::snippet(&body)` on the decompressed
body of those rows only. `suggest` moves to SQL over `messages`
(subject / from / to prefix, `LIMIT`).

**Consistency:** the existing checkpointed `SearchProjectionHandler` stays
(it already reads content from the read model, `search_index.rs:57-92`). Only
the document builder and the hit type change.

**Contacts:** drop `tantivy_contact_index.rs`. A few thousand rows:
`search_contacts` = `LIKE` on name / emails; `find_duplicate_candidates` =
`GROUP BY email HAVING COUNT(*) > 1` on `contact_emails`. Removes the second
index, its handler-driven maintenance (`handlers/contact.rs`) and its
missing rebuild path.

## 6. Files involved

| Crate | File | Change |
|---|---|---|
| root | `Cargo.toml` | add `rusqlite` (bundled), `zstd`; remove `sled` at the end |
| mc-domain | `src/events/mod.rs` | `MessageBodyFetched` drops body fields; status events kept as bus-only variants (or moved to a progress type) |
| mc-domain | `src/traits/repository.rs` | add `MessageBodyStore`; `BodyBackfillQueue` unchanged |
| mc-domain | `src/traits/event_store.rs` | unchanged (already has `current_sequence`) |
| mc-domain | `src/traits/search_index.rs` | `SearchHit` → id + score; `suggest` leaves the trait; `ContactIndex` trait removed |
| mc-application | `src/services/sync_service.rs` | write body via `MessageBodyStore` before append; status events projected + published, not appended |
| mc-application | `src/projections/search_index.rs` | unchanged logic; new document shape |
| mc-application | `src/projections/read_model.rs` | batch + checkpoint in one transaction (needs a `project_batch` or transaction hook) |
| mc-application | `src/handlers/query.rs`, `handlers/contact.rs` | hit hydration + snippets; `suggest` and contact search → repository |
| mc-infrastructure | `src/persistence/sqlite/{mod,schema,event_store,repository}.rs` | **new** |
| mc-infrastructure | `src/persistence/sled/*` | kept read-only for the migrator, then deleted |
| mc-infrastructure | `src/search/tantivy_index.rs` | index-only schema (§5) |
| mc-infrastructure | `src/search/tantivy_contact_index.rs` | deleted |
| mc-infrastructure | `src/lib.rs`, `src/persistence/mod.rs` | exports |
| mc-interfaces | `src/grpc/services/search_service.rs`, `contact_service.rs` | map hydrated hits (proto unchanged) |
| mc-server | `src/wiring.rs` | open SQLite, type aliases `:39-56`, contact index removal, `AppContext.db` |
| mc-server | `src/main.rs` | shutdown: `PRAGMA wal_checkpoint(TRUNCATE)` instead of sled flush; `migrate` subcommand |
| mc-server | `src/config.rs`, `deploy/config.toml.example` | `db_path` → SQLite file |
| docs | `ARCHITECTURE.md`, `README.md` | storage section |

## 7. Data migration (sled → SQLite)

`mail-cat-server migrate --from <sled_dir> --to <sqlite_file>`, run with the
service stopped. Stream, never materialize.

1. Open sled read-only; open/create SQLite at the target with schema.
2. Stream `events_global` in order (`load_all_events(None, 0)`).
3. Per event:
   - Status events (`AccountSync*`, `MailboxCountsUpdated`) → skip; the latest
     `MailboxCountsUpdated` per mailbox is applied to `mailboxes` directly.
   - `MessageBodyFetched` → body to `message_bodies` (zstd), event appended without body.
   - Everything else → appended unchanged.
   - Re-number `sequence` per aggregate (dropping events leaves gaps);
     `global_position` assigned by SQLite.
4. Project the read model by replaying the new log (same projector code path
   as production — doubles as its test). Transactions of 1000 events.
5. Rebuild the Tantivy index into a **new** directory with the index-only
   schema (reset the `search_index` checkpoint, run catch-up); swap on success.
6. Verify, print a report: per-account message counts, per-mailbox unread,
   contact count, bodies stored vs pending — compared with the sled read model.
   Non-zero exit on mismatch.
7. `VACUUM`. Leave the sled directory untouched; the user deletes it after a
   few days of running on SQLite.

Rollback = point `db_path` / `index_path` back at the old dirs and reinstall
the previous binary.

## 8. Phases (one crate at a time, each ends green)

| # | Crate | Work | Done when |
|---|---|---|---|
| 0 | — | Commit today's fixes on main; branch `feat/sqlite-migration` | clean tree |
| 1 | mc-infrastructure | `sqlite/schema.rs`, `SqliteEventStore`. Port the 8 event-store tests as a backend-generic conformance suite run against **both** sled and SQLite | both pass |
| 2 | mc-domain + mc-application | `MessageBodyStore`, body out of `MessageBodyFetched`, status events transient | workspace tests pass on sled |
| 3 | mc-infrastructure | `SqliteRepository` (all read traits + projector + checkpoints + backfill + body store) + transactional batch projection hook (moved from phase 2). Port the 13 read-repository tests to the generic suite; add the missing ones (contacts, `rebuild_index`, `get_thread` order, `find_by_provider_uid`, `find_unread_by_mailbox`, `list_conversations` per mailbox) | suite green on both |
| 4 | mc-infrastructure + mc-application + mc-interfaces | Index-only Tantivy schema, hit hydration + snippets from SQLite, `suggest` and contacts search in SQL, drop contact index | search tests green; parity check on a fixed query set |
| 5 | mc-server | wiring on SQLite, `migrate` subcommand, shutdown, config | server runs on a migrated copy |
| 6 | — | Migrate a **copy** of the live DB; run report; compare RSS / disk / `list_messages` latency vs today | targets met (§Goal) |
| 7 | all | Delete sled code and dep; docs | `cargo tree` has no sled |

Phases 1–3 keep sled as the production backend, so main stays shippable at
every step.

## 9. Decisions (resolved 2026-10-10)

- **D1 — Search engine:** keep Tantivy, index-only (§5). FTS5 rejected: weaker
  query power and sorting, and search is the next thing to extend.
- **D2 — Search features:** separate follow-up spec. This one only ships the
  index schema that supports them.
- **D3 — Data:** migrate from sled (§7), no re-sync. Preserves local-only state
  and avoids a full re-download while Gmail is throttling two accounts.
- **D4 — Bodies:** out of the event log into `message_bodies`, zstd, kept
  forever (§3). Retention policy deferred until the compressed size is known.
- **D5 — Event sourcing for messages:** kept for now. Replacing message events
  with a plain state table (accounts, contacts, drafts stay event-sourced) is
  the planned **next step** after this migration, as its own spec.

## 10. Deferred items

Tracked here so later plans pick them up. Tick when done (with commit).

**From the plan 1 final review (minor, `SqliteEventStore` / `SqliteDb`):**
- [ ] Map `UNIQUE(aggregate_id, sequence)` violations to `ConcurrencyConflict` (§2 says so; today → `EventStore`). Only reachable with a second writer process, e.g. `migrate` against a live DB — **do in plan 5** alongside the migrator.
- [ ] `u64 → i64` casts (`from_global_position`, `from_sequence`, `sequence`) wrap above `i64::MAX`; use `i64::try_from(..).unwrap_or(i64::MAX)`.
- [ ] Reject a negative `PRAGMA user_version` at open (today it skips every migration).
- [ ] `SqliteDb::read/write` map spawn_blocking join errors to `Repository` even for the event store; consider an error-mapper parameter.
- [ ] Test `load_all_events` paging **with** an `aggregate_type` filter (mixed types, > `STREAM_PAGE` matches).
- [ ] Concurrency test exercises the writer mutex, not the `IMMEDIATE` transaction — acceptable while the mutex exists; revisit if writes ever go multi-process.

**From writing plan 2:**
- [x] Projector arms are replay-safe on both backends, proven by the conformance test
  `replaying_the_log_over_live_state_changes_nothing` (plan 3, 28a5dd8). Catch-up batches
  commit with their checkpoint on SQLite (plan 3, ed7af72).
- [ ] Live projection (`append_and_project`, bus subscriber `wiring.rs:506`) still doesn't
  checkpoint, so catch-up re-projects since the last catch-up — harmless now, wasted work.
  **Plan 5** (wiring).
- [x] Replaying `MailboxDiscovered` (full rebuild) resets mailbox counts to 0 until the next poll (plan 3, 28a5dd8: both backends keep counts on replay).
- [ ] sled `message_bodies` rows are not removed on `MessageDeleted` (orphans until sled is deleted in plan 5). SQLite uses `ON DELETE CASCADE`.
- [ ] Stale doc comments mentioning the body in the event: `sync_service.rs` `fetch_message_body` / `fetch_message_bodies_bulk` docs, mc-domain `traits/search_index.rs:14`.
- [ ] `AccountSyncFailed` is never emitted, so its desktop notification (`wiring.rs:698-709`) is dead. Emitting it (transient, from `start_sync`'s failure path) is a small feature, not migration work.

**From writing plan 3:**
- [ ] sled `MessageDeleted` leaves the `body_backfill` entry and drops the `rfc_id_seen`
  marker even if another copy exists (conversation undercount). SQLite is correct;
  vanishes with sled in plan 5.
- [ ] `ConversationReadRepository::rebuild_index` doc says "events replayed"; SQLite returns
  the number of conversations. Reword the trait doc when sled is deleted.
- [ ] `search_contacts` on SQLite folds ASCII case only (`LIKE`); sled lowercased Unicode.

**From the plan 3 final review (minor):**
- [ ] Flag order is not replay-stable (both backends): remove-then-re-add moves a flag to
  the end, so a replay can turn `[Seen, Flagged]` into `[Flagged, Seen]`. Equal as a set;
  keep flags in a canonical order if the order ever matters.
- [x] Conformance replay test only replays the whole log; add an every-suffix replay
  (thread with remaining messages, one RFC copy deleted, double move, rename, contact
  merge/remove) before plan 5 touches wiring.
- [ ] SQLite `ContactAdded` with no emails fails the batch (`primary_email`); sled falls back
  to `unknown@unknown`. Unreachable (domain validates since ddcd561), recheck in the plan 5 migrator.
- [x] `find_by_provider_uid` on SQLite: add `m.id DESC` tie-break for equal `received_at`
  (newest-minted wins; sled returns the older id on a tie, gone in plan 5).

**From phase 4 (implemented without a written plan, 2026-10-12):**
- [ ] Parity check of search results on a fixed query set against real data — do it on the
  migrated copy in plan 5 (the branch has no production index to compare against).
- [ ] sled `suggest` scans every summary per call; acceptable until sled is deleted.
- [x] `traits/search_index.rs` doc no longer describes the body-in-event indexing (e549b76..).

**Out of scope of the migration, decided 2026-10-10:**
- [ ] Search features (operators, filters, sort) — own spec; index schema ready after plan 4.
- [ ] Body retention policy (`evicted`) — decide after measuring compressed size (plan 5, step 6).
- [ ] Event sourcing for messages → plain state table — own spec, after plan 5.
- [ ] Gmail + 4sigma were throttled (`AUTHENTICATIONFAILED`) since 2026-10-06; periodic-sync backoff shipped 2026-10-10. Verify they recover; if not, redo OAuth login.
- [ ] Pre-existing clippy warnings (oauth/presets.rs, sled/read_repository.rs, tantivy_index.rs, wiring.rs) — sled ones vanish in plan 5.
