# Search index as an event-sourced projection

**Date:** 2026-07-07
**Status:** Approved design, pending implementation plan

## Problem

The Tantivy search index is a projection of domain events, but — unlike the
read model — it is **not** event-sourced with a checkpoint. It is maintained by
three disconnected paths:

1. `SyncService` indexes messages inline (`index_message`) during sync.
2. Ad-hoc event-bus subscribers in `wiring.rs` (de-index on `MessageDeleted`,
   re-index on `MessageMoved`).
3. A **full rebuild from the read model on every server boot**
   (`scan_all_messages` → `search_index.rebuild`).

Because the incremental coverage was never complete or reliable, the full
rebuild existed as a safety net. That rebuild loads every message into memory
and reindexes all documents on **each restart** — on a 70k+ message store this
spikes memory and burns CPU, and it is the reason the index "reindexes
everything from scratch" at startup.

A recent stop-gap added a count-equality guard (skip rebuild when index doc
count == live message count) plus an incremental re-index on `MessageMoved`.
The guard is a heuristic with a known blind spot (drift that preserves the total
count), and the dual-maintenance structure remains. This design replaces the
stop-gaps with a definitive fix.

## Goal

Make the search index a first-class event-sourced projection with its own
checkpoint, exactly like the read model. The index is brought up to date
**incrementally** from the event store; it is never fully rebuilt during normal
operation. Drift is impossible under normal operation, and crash recovery is
handled by idempotent replay from the last durable checkpoint.

## Existing patterns reused

- `ProjectionEventHandler` (`crates/mc-application/src/projections/read_model.rs`)
  — the read model's checkpointed projection handler, with `handle` (live,
  idempotent, checkpoint-advancing), `catch_up` (replay events after the
  checkpoint), and `rebuild` (reset to 0 and replay all). The new handler
  mirrors this shape.
- `ProjectionCheckpoint` (`crates/mc-domain/src/traits/repository.rs`) — a
  checkpoint store keyed by `projection_name`, backed by sled
  (`PROJECTION_CHECKPOINTS_TREE`). Reused verbatim with a new name
  `"search_index"`.
- The event store (`SledEventStore`) already assigns a monotonic
  `global_position` to every event, and `SyncService` appends all synced
  messages to it (`event_store.append`, then publishes to the bus). So every
  message — synced or command-driven — is visible to a checkpointed projection.

## Architecture

A new `SearchProjectionHandler` lives alongside `ProjectionEventHandler`
(same crate, `crates/mc-application/src/projections/`). It owns:

- a `SearchIndex` handle (the Tantivy index),
- a `MessageReadRepository` handle (to read current message content),
- a `ProjectionCheckpoint` handle (checkpoint name `"search_index"`).

### Core rule: refresh-by-id, not delta-apply

The handler does **not** interpret event bodies to mutate the index. For each
message-affecting event it takes the `message_id` and refreshes that one message
from the **read model** (the source of truth for message content):

| Event | Action |
|-------|--------|
| `MessageDeleted { message_id }` | `search.remove_message(id)` |
| `MessageReceived`, `MessageBodyFetched`, `MessageMoved`, `MessageFlagged` (all carry `message_id`) | read `Message` from read model; `Some(msg)` → `search.index_message(&msg)`; `None` → `search.remove_message(id)` |
| everything else | ignore |

Consequences:

- **Idempotent**: `index_message` deletes the prior doc by `message_id` term
  then adds one document; replaying the same event re-indexes the current state.
- **Self-healing on replay**: replaying an old `MessageReceived` for a message
  that has since been deleted reads `None` and removes it — the index converges
  to current truth regardless of replay position.
- **No stale mailbox/flags**: because the whole current message is re-indexed,
  the indexed `mailbox_id` always matches the read model (fixes the
  "stuck in trash / searchable in old folder forever" bug at the root).

Note: flags are not indexed fields, so `MessageFlagged` is effectively a no-op
for index contents; it is included for uniformity and harmless idempotency.

### Ordering requirement

Refresh-by-id reads the read model, so the read-model projection for a given
event must be applied **before** the search projection reads it. This is
guaranteed on every path:

- **Boot**: read-model `catch_up` runs to completion before search `catch_up`.
- **Live bus**: the subscriber applies the read-model projection, then the
  search projection, for each event in sequence (chained in one loop).

## The three paths (all incremental, zero full rebuild)

1. **Boot** — replace the full-rebuild block in `wiring.rs` with
   `search_handler.catch_up(event_store)`. On a fresh install (or after an index
   wipe) the checkpoint is 0, so `catch_up` indexes everything **once**; this is
   the natural migration with no special casing. Every subsequent boot processes
   only events appended since the last shutdown — effectively no work.

2. **After each sync** (initial sync + the existing periodic 5-minute re-sync
   loop) — call `search_handler.catch_up(event_store)`. Sync appends thousands
   of events in bulk; the in-process event bus (capacity 1000) drops events
   under that load, so the live path alone is not reliable during bulk. `catch_up`
   reads from the event store (lossless) and heals any gap incrementally from the
   checkpoint.

3. **Live bus** — the existing live projection subscriber is extended to drive
   the search projection after the read-model projection for each event. This
   gives instant search freshness for runtime commands (move/delete/flag) and
   trickle-in mail. It is best-effort for latency; correctness is guaranteed by
   the `catch_up` backstop.

## Durability (checkpoint vs. commit ordering)

Tantivy batches commits (auto-commit every 100 docs; `search()` flushes pending
before querying). The invariant: **the search checkpoint must never advance past
the last durable Tantivy commit.**

- In `catch_up`: process the batch, **commit the index, then save the
  checkpoint** to the last processed position. Requires exposing a commit/flush
  on the `SearchIndex` trait (the internal `commit_and_reload` already exists;
  add a public trait method, e.g. `flush()`).
- If a crash occurs between commit and checkpoint save, the checkpoint is behind
  the durable index; replay re-indexes those messages idempotently — safe.
- If work is done but not yet committed and a crash occurs, the checkpoint was
  not advanced (it only advances after commit), so those events replay — safe.

Because indexing is refresh-by-id from the current read model, over-replay is
always safe.

## Changes to existing code

**Added:**

- `crates/mc-application/src/projections/search_index.rs` —
  `SearchProjectionHandler` with `handle`, `catch_up`, and a private
  event→action mapping. Mirrors `read_model.rs`.
- `SearchIndex::flush()` (trait + Tantivy impl delegating to
  `commit_and_reload`).

**Removed (the stop-gaps this design subsumes):**

- The full-rebuild block in `wiring.rs` (`scan_all_messages` → `rebuild`) and
  the count-equality guard around it.
- The ad-hoc search de-index/move subscriber in `wiring.rs` (folded into the
  unified live subscriber).
- The three inline `index_message` calls in `SyncService` — indexing is now
  owned solely by the projection. (Sync still appends events to the event store
  and publishes to the bus; nothing else changes in sync.)
- `SearchIndex::count()` and `MessageReadRepository::count_messages()` — added
  only for the count-guard, now dead.

`SearchIndex::rebuild()` is no longer on the boot path (`catch_up` from
`checkpoint = 0` achieves the same first-time full index via per-message
`index_message`). The implementation plan will confirm whether any caller
remains (e.g. an admin rebuild command); if none, it is removed as dead code,
otherwise it is kept as an explicit recovery primitive.

**Modified:**

- `wiring.rs`: construct the `SearchProjectionHandler`; wire the three paths
  (boot catch_up, post-sync catch_up, live subscriber chaining).

## Error handling

Projection errors are logged and do not crash the server (a stale or unavailable
index must not block startup or sync). On a per-event failure the checkpoint is
not advanced past the failure, so the next `catch_up` retries it — same policy as
the read-model handler.

## Testing

Unit tests for `SearchProjectionHandler` (mirroring `read_model.rs` tests), with
mock `SearchIndex`, mock `MessageReadRepository`, and mock `ProjectionCheckpoint`:

- `catch_up` indexes present messages and removes absent ones, advancing the
  checkpoint to the last position.
- Idempotent replay: re-running `catch_up` over the same events produces the
  same index state and does not double-count.
- `MessageDeleted` removes; `MessageMoved` re-indexes with the current
  (moved) message; a message deleted after an earlier `MessageReceived` ends up
  removed when replayed.

Existing Tantivy index tests are unaffected except for the removal of the
`count()`-based test.

## Out of scope

- Reworking how the read model is maintained (its dual inline+bus paths stay).
- Changing the event schema or the indexed document fields.
- A user-facing "rebuild index" command (the `checkpoint = 0` catch_up covers
  recovery; an explicit admin command can be added later if wanted).
