# Contacts and calendars: bidirectional server sync

## Problem

mail-cat syncs mail only. Contacts are local (`ContactSource::{Local, LocalEmail}`,
`model/contact.rs:62`), harvested from mail by the `EnsureContact` subscriber
(`wiring.rs:372-419`). There are no calendars. Every provider the user has exposes
address books and calendars: Google and generic servers (Stalwart, Nextcloud, iCloud,
Fastmail) through CardDAV/CalDAV, and Microsoft only through Graph.

This phase is **server only**: fetching the data and syncing it in both directions,
solidly. The UI is a later phase.

## Goals

1. Two domain ports, `ContactsProvider` and `CalendarProvider`, that describe what the
   application needs and do not mention the protocol. They are shaped like
   `EmailProvider`: adapters live in `mc-infrastructure`, and an enum dispatches between
   them like `AnyProvider` (`email/any.rs`).
2. Bidirectional sync of every address book and calendar the account can see:
   - its own collections
   - collections shared by other users
   - subscriptions (holidays and similar), read-only where the server says so
3. Data survives conflicts and crashes. Conflicts are resolved last-write-wins (§4).
   The losing version stays in the event store.
4. Exposed over gRPC with a CLI command set, so the server can be exercised without a UI.
5. Providers, in delivery order:
   1. **Google**: CardDAV + CalDAV, using the existing OAuth with extra scopes.
   2. **Generic accounts** (IMAP/JMAP accounts on other servers): CardDAV + CalDAV with
      discovery.
   3. **Microsoft**: a Graph adapter behind the same ports.

   One DAV adapter covers 1 and 2. Only authentication and endpoint discovery differ.

## Non-goals (this phase)

- UI of any kind.
- Meetings: attendees, invitations (RFC 6638 / iMIP), free/busy, "find a time". These are
  the **next** phase. The ports must leave room for them (§6), but nothing is built now.
- 3-way merge.
- Reverting a lost version (recorded in memory for the UI phase).
- JMAP Contacts / JMAP Calendars adapters. Stalwart also speaks DAV, so phase 2 covers it.
- Data migration: the data folder is wiped on model changes (project rule). If a config
  or keyring rename is needed, tell the user old → new.

## Anti-goals

- **Losing a user edit silently.** A local change that never reached the server, and is
  then overwritten by a pull, is the failure this design exists to prevent.
- Retry storms against a provider. The Gmail lockout came from retry-hammering. DAV
  traffic goes through the same `auth_gate` backoff as mail.

## 1. Domain model

New aggregates (add to `AggregateType`, `events/mod.rs:56`):

| Aggregate | Id | Holds |
|---|---|---|
| `AddressBook` | account + collection href | name, description, read-only, sync cursor lives outside (like `SyncCursor`) |
| `Calendar` | account + collection href | name, color, timezone, read-only, owner (own / shared / subscription), component set (VEVENT/VTODO) |
| `Contact` (existing) | existing `ContactId` | gains a remote binding (below) |
| `CalendarEvent` | account + resource href | one iCalendar resource: the master plus its overrides (a single `UID`) |

**Raw payload is the source of truth.** Every remote-bound contact or event carries the
full vCard / iCalendar text. Fields the queries need are extracted alongside, into the
projection only:
- contacts: names, emails, phones, organization
- events: summary, start, end, all-day, rrule present, location, status

A field the model doesn't know is never dropped, because we always write back the raw
text with our edits applied to it. We never regenerate the text from the extracted
fields.

Remote binding, on `Contact` and `CalendarEvent`:
`{ account, collection, href, etag, remote_modified: Option<DateTime> }`.

`ContactSource` gains `CardDav` and `MicrosoftGraph`. Harvested `LocalEmail` contacts stay
separate entries in this phase. Autocomplete ranks provider contacts above harvested
ones for the same address. *(Builder's call; auto-merge via `ContactMerged` is a later
decision.)*

Recurrence: we store the raw `RRULE`/`EXDATE`/overrides and **expand occurrences at
read time** for a requested range. Occurrences are never persisted. There is one
`CalendarEvent` per **href**. If a server splits a series across hrefs with the same
`UID`, both are kept as they are, and expansion lets the override replace the master's
occurrence. *(My call.)*

`LocalEmail` contacts are never pushed. Creating a contact or an event requires an
explicit target collection in the API; there is no implicit default. *(My call.)*

VTODO and VJOURNAL resources in a calendar are synced **opaquely**: raw payload, href and
etag, no extracted fields and no API beyond listing. They round-trip untouched.
Collections that hold only VTODO are listed but not synced. *(My call, revisit with tasks.)*

Events, `<Aggregate><PastTense>` like the existing ones:
- `AddressBookDiscovered`, `AddressBookUpdated`, `AddressBookRemoved`, and the same for
  `Calendar`
- `ContactAdded`/`ContactUpdated`/`ContactRemoved` (existing, extended with raw payload
  and binding)
- `CalendarEventAdded`, `CalendarEventUpdated`, `CalendarEventRemoved`
- `…PushedToServer`: the server acknowledged a local version, with the new etag
- `…ConflictLost`: carries the local version that was discarded, so it stays in the
  history

Every `…Added`/`…Updated`/`…Removed` on a remote-bound resource carries
`origin: Local | Remote`. Pull-applied events are `Remote` and user edits are `Local`.
*Dirty* means the last `Local` event has no later `…PushedToServer`. Harvester
(`EnsureContact`) events never touch remote-bound contacts. *(My call.)*

## 2. Ports

```rust
#[async_trait]
pub trait ContactsProvider: Send + Sync {
    async fn list_address_books(&self) -> Result<Vec<AddressBook>, DomainError>;
    async fn address_book_changes(&self, book: &CollectionId, since: Option<&SyncCursor>)
        -> Result<CollectionChanges, DomainError>;
    async fn fetch_contacts(&self, book: &CollectionId, hrefs: &[Href])
        -> Result<Vec<RemoteResource>, DomainError>;
    async fn put_contact(&self, book: &CollectionId, put: ResourcePut)
        -> Result<PutOutcome, DomainError>;
    async fn delete_contact(&self, book: &CollectionId, href: &Href, if_match: &Etag)
        -> Result<DeleteOutcome, DomainError>;
}
```

`CalendarProvider` has the same shape (`list_calendars`, `calendar_changes`,
`fetch_events`, `put_event`, `delete_event`).

- `CollectionChanges`: `{ changed: Vec<(Href, Etag)>, removed: Vec<Href>, cursor,
  reset: bool }`, the same idea as `MailboxChanges`.
- `RemoteResource`: `{ href, etag, raw }`.
- `ResourcePut`: `{ href: Option<Href> /* None = create */, raw, if_match: Option<Etag> }`.
- `PutOutcome`: `Stored { href, etag }` or `PreconditionFailed` (412).
- `DeleteOutcome`: `Deleted`, `PreconditionFailed` or `Gone`.

Conflicts are an outcome the application handles, not an error. Signatures are
indicative: the builder may adjust names and types while keeping the semantics.

## 3. Pull (server → local)

Per collection, mirroring `sync_one` (`sync_service/mailbox.rs:69-137`):

1. **Cheap check**: compare the `getctag` (or the sync-token) with the stored one. If it
   is unchanged, stop.
2. **Delta**: `sync-collection` REPORT (RFC 6578) with the stored token returns changed
   (href + etag) and removed resources. No token, an unsupported server, or an
   invalid/expired token (`valid-sync-token` error) → `reset: true`: PROPFIND depth 1
   lists every href + etag, and we diff it against local.
3. **Fetch**: `addressbook-multiget` / `calendar-multiget`, only for hrefs whose etag
   differs from local. Batch in chunks.
4. **Apply**:
   - a resource with a pending local edit goes through the conflict rule (§4)
   - otherwise append `…Updated`/`…Added`/`…Removed`
5. **Persist the cursor last.** A crash redoes the pass from the old cursor. Every step
   is idempotent: an etag equal to local means no event.

Collection list: `list_*` on every pass. A new collection is discovered, a vanished one
is removed along with its resources, but first every dirty resource in it gets
`…ConflictLost`, so the unpushed edit stays in the history. A collection that becomes
read-only does the same for its dirty resources. An account that loses its scopes stops
its DAV task and nothing is removed. *(My call.)*

Ownership and permissions *(my call; the builder verifies per server)*:
- read-only comes from `DAV:current-user-privilege-set`: no `write-content` means
  read-only
- own vs. shared comes from comparing `DAV:owner` with `current-user-principal`
- subscription comes from the calendarserver `subscribed` resource type or `source`
  property, where present

## 4. Push (local → server) and conflicts

Mail writes go straight to the provider, and the next sync repairs any failure
(`handlers/message.rs:295-326`). **That model is not safe here**: a pull would overwrite
an edit that never reached the server. So contacts and events get a durable pending
state:

- A local edit appends `…Updated` with the new raw payload and marks the resource
  **dirty** in the projection (dirty = last local event not yet followed by
  `…PushedToServer`). Dirty state is derived from events, so it survives restarts.
- The handler tries the push immediately (fast path). Whether it succeeds or not, the
  sync pass **pushes every dirty resource first, then pulls**. Nothing is lost if the
  fast path fails or the process dies.
- The push is `PUT` with `If-Match: <etag>`, or `If-None-Match: *` for a create.
  Delete is `DELETE` with `If-Match`.
  - `Stored` → `…PushedToServer { etag, href }`.
  - `PreconditionFailed` → fetch the remote copy, then apply the **last-write-wins** rule:
    compare the local edit's `occurred_at` with the remote `REV` (vCard) or
    `LAST-MODIFIED` (iCal). Clock skew is accepted as inherent to last-write-wins.
    Every `ConflictLost` is counted in the sync status exposed over gRPC, so it is never
    invisible. *(My call.)*
    - Local is newer → re-PUT with the fresh etag. Repeat at most once, then defer to
      the next pass.
    - Remote is newer or has no timestamp → append `…ConflictLost { local_raw }`, then
      `…Updated` with the remote copy.
  - `Gone` on update → the remote deleted the resource. Apply the same last-write-wins
    rule, using the time of the remote removal as observed (now). In practice a local
    edit made before the pull wins and is re-created. *(Builder may refine.)*
- A write to a read-only collection is rejected in the handler before any event.
- Error classes reuse `stops_the_pass` and the `auth_gate` backoff:
  - auth, connection or throttling stops the pass
  - a 4xx on one resource marks it `push_failed` with the reason and skips it; it is
    retried on the next pass, never in a tight loop
  - a 5xx / timeout follows the backoff

When we change the raw payload, we bump `REV` / `LAST-MODIFIED` / `DTSTAMP` / `SEQUENCE`
ourselves, so the other clients' last-write-wins sees our edit correctly.

## 5. Runtime

- A **separate per-account task** (`DavSyncLoop`) under the same `supervise()` and the
  same auth gate as `SyncLoop` (`account_runtime/tasks.rs:90-147`). It has its own period
  (5 min, like `SYNC_PERIOD`), and the gate's `ready()`/`retry_due()` apply. Contacts and
  calendars run in one pass, push before pull. *(Builder's call: separate task vs. extra
  step in `SyncLoop`; separate keeps a slow DAV server from delaying mail.)*
- Providers are registered in their own registry, the same `InMemoryProviderRegistry`
  pattern (`provider_registry.rs`), keyed by account.
- An account without DAV config simply has no DAV task.

## 6. Room for meetings (next phase, not built)

The model keeps attendees and organizer from the raw payload, so nothing is lost. The
ports will later gain `free_busy(attendees, range)`. Google needs the Calendar API
(`freebusy.query`) for it, because Google CalDAV has none. Graph has `getSchedule`;
generic servers use the RFC 6638 outbox. The domain will compute common free slots.
Nothing in this phase may preclude:
- an adapter mixing DAV with a REST call
- an event carrying `ATTENDEE`/`ORGANIZER`

## 7. Configuration, auth, discovery

`ConnectionConfig` (`model/account.rs:169`) gains
`dav: Option<DavConfig { carddav: Option<Url>, caldav: Option<Url> }>`. These are the
principal or home-set URLs, discovered or set by hand.

- **Google**:
  - It is a *quirks profile* of the one DAV adapter. Known quirks:
    - some paths have no sync-token, so fall back to the reset path
    - the payload is rewritten on write, so always keep the etag and body the server
      returns
    - CardDAV has no shared address books

    The builder may split Google into its own adapter only if the quirks outgrow a
    profile.
  - Add scopes `https://www.googleapis.com/auth/carddav` and
    `https://www.googleapis.com/auth/calendar` to the preset (`oauth/presets.rs:52-90`).
  - Endpoints are fixed:
    - CardDAV: `https://www.googleapis.com/carddav/v1/principals/{email}/lists/default/`
    - CalDAV: `https://apidata.googleusercontent.com/caldav/v2/`
  - The builder verifies the CalDAV principal/calendar-home discovery against Google's
    docs.
  - **Existing Google accounts must re-consent** for the new scopes. An account whose
    token lacks them gets no DAV task and is reported as needing reauth for contacts and
    calendars. Mail keeps working. A refresh cannot widen scopes, so re-consent is an
    explicit user action through the existing `NeedsReauth` flow. Tell the user.
- **Generic**:
  - Discovery runs RFC 6764 in order:
    1. `https://{domain}/.well-known/carddav` / `caldav`
    2. if that fails, DNS SRV `_carddavs._tcp` / `_caldavs._tcp`
    3. then `current-user-principal`
    4. then `addressbook-home-set` / `calendar-home-set`
  - Credentials are the account's own (`AuthConfig` Plain / AppPassword / OAuth2).
  - A manual URL override is allowed.
- **Microsoft** (phase 3):
  - Graph scopes are `Contacts.ReadWrite` and `Calendars.ReadWrite`.
  - A Microsoft token is per resource: a Graph token is a separate refresh from the same
    refresh token, not the IMAP one.
  - Graph delta queries map onto `CollectionChanges`.
  - Graph is JSON, not vCard/iCal. The adapter converts to/from vCard/iCal so the domain
    sees one format. *(The builder plans phase 3 in detail when it starts.)*

## 8. Interfaces

- `proto/mail_cat/v1/contacts.proto` is extended:
  - address books
  - contact create/update/delete with raw payload and extracted fields
  - sync status: dirty / push_failed / last sync
- `calendars.proto` is new: calendars, events in a range (occurrences expanded), and
  event create/update/delete.
- Path as for contacts today: `grpc/services/…` → `mc-client` → `mc-cli` commands
  (`contacts`, `calendars`, `events`).
- No GraphQL in this phase.

## 9. Dependencies

There is no XML, vCard/iCal or DNS crate today. `reqwest` is already present.
- XML: `quick-xml`.
- vCard + iCalendar parsing and recurrence expansion: one crate if it covers both with
  round-trip fidelity (e.g. `calcard`). Otherwise a vCard/iCal parser plus `rrule`.
- DNS SRV: `hickory-resolver`.

The builder picks the final crates. The criteria: they must keep unknown properties on
round-trip, and handle timezones (`VTIMEZONE`, TZID).

## 10. Done criteria

- Integration tests against a real DAV server in a container (Radicale or Stalwart),
  for both contacts and calendars:
  - initial sync
  - delta sync
  - token-expired reset
  - local create/update/delete pushed
  - 412 conflict where local wins and where remote wins (lost version in history)
  - crash between push and cursor save, with no loss and no duplicate
  - read-only collection rejects writes
- A unit test of recurrence expansion with an override and an EXDATE, across a DST
  change.
- Google verified by hand on the user's Gmail and 4sigma accounts: sync, edit on the
  phone, edit in the CLI, both directions.
- A generic-server account verified by hand (crazylinux.it).
- Files are 400-600 lines, with tests in `foo/tests.rs` (project rule).
