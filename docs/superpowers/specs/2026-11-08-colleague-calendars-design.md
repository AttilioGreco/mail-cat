# Subscribing to colleagues' calendars

## Problem

Calendars already shared with the user (Workspace shared calendars, room resources) sync through
CalDAV. The user cannot add a colleague's calendar from mail-cat. Google needs the calendar to be in
the user's `calendarList`, and generic CalDAV servers only list what is in the user's own calendar
home.

## Scope

- **Google**: `calendarList.insert` / `calendarList.delete` through the Calendar REST API. The
  existing `auth/calendar` scope already allows it, so no re-auth is needed. Once a calendar is in
  the calendarList, Google's CalDAV home lists it, so the sync engine is unchanged.
- **Generic CalDAV**: look up the colleague's principal by email, list the calendars in their
  calendar home that we can read, and keep those hrefs in the account config. Each listing also
  PROPFINDs them.
- **Microsoft**: waits for the Graph adapter.
- **Free/busy only**: a colleague who shares only free/busy still appears. Their events show as
  "Occupato" blocks, read-only, with whatever detail the server returns. No special handling.

## Port

Two methods on `CalendarProvider` (`mc-domain/src/traits/dav_provider.rs`):

```rust
/// Adds the calendars `email` lets us read. `Remote` when the server remembers the
/// subscription (Google), `Local(hrefs)` when we must remember it ourselves (CalDAV).
async fn subscribe(&self, email: &str) -> Result<Subscribed>;
/// Removes a calendar added by `subscribe`. CalDAV: no-op on the server.
async fn unsubscribe(&self, calendar: &CollectionId) -> Result<()>;
```

`Subscribed::{Remote, Local(Vec<Href>)}`. Errors: `NotFound` (no such principal, or Google 404),
`Forbidden` (nothing shared, Google 403). The UI maps both to clear Italian messages.

## Adapter (`mc-infrastructure/src/dav`)

- **Google** (`Profile::Google`): a small `reqwest` call, using the same `LoginGate` bearer as
  `DavClient`, so token refresh comes for free.
  - subscribe: `POST https://www.googleapis.com/calendar/v3/users/me/calendarList` with
    `{"id": email}`.
  - unsubscribe: `DELETE …/calendarList/{calendarId}`. The calendarId is the path segment after
    `/caldav/v2/` in the collection href, percent-decoded.
- **Generic**:
  - subscribe: send a `REPORT DAV:principal-property-search` to the principal collection set
    (`DAV:principal-collection-set` on our principal), matching
    `C:calendar-user-address-set` = `mailto:{email}`. Take the first principal's
    `calendar-home-set`. PROPFIND depth 1 there, keeping calendar collections that return 200.
    Return them as `Local`.
  - Fallback when the server does not support the REPORT (Stalwart, Radicale): replace the last
    path segment of our own principal href with `{email}`, then with the local part. Use the first
    candidate that resolves a calendar home.
  - The listing (`collections.rs` `listing`) also PROPFINDs (depth 0) each configured extra href.
    A 403 or 404 drops that calendar from the listing (reconcile then removes it locally, as it
    does today).
- Ownership is already derived from `DAV:owner` vs `current-user-principal`, so these calendars
  come out `Shared`. Google calendarList entries do too.

## Config

`DavConfig` (`mc-domain/src/model/account.rs`) gains
`#[serde(default, skip_serializing_if = "Vec::is_empty")] subscribed: Vec<Url>`. `dav_setup` hands
it to the generic adapter. Additive field, so no config rename.

## Application

Handlers `handle_subscribe_calendar(account, email)` and `handle_unsubscribe_calendar(collection)`
in `handlers/dav.rs`:
- call the port
- for `Local`, persist the hrefs into the account's `DavConfig.subscribed` (unsubscribe removes the
  href) through the existing account-config update path
- trigger `DavSyncService::sync_account` so the calendar shows up or disappears right away

Unsubscribe is refused for `Ownership::Own`.

## gRPC / client / CLI

- `calendars.proto`: `SubscribeCalendar(account_id, email)` and
  `UnsubscribeCalendar(account_id, collection_id)`.
- mc-client methods.
- CLI: `calendars subscribe <account> <email>` and `calendars unsubscribe <account> <collection>`.

## UI

- `CalendarSidebar.tsx`: a "+" next to each account group opens "Aggiungi calendario di un
  collega". It has a single email field with contact autocomplete (`search_contacts`, as in
  `RecipientField`). Microsoft accounts hide the button.
- Right-click on a non-`Own` calendar: "Annulla iscrizione".
- Tauri commands `subscribe_calendar` and `unsubscribe_calendar` wrap the client.

## Tests

- Adapter: a Google subscribe/unsubscribe request shape against a mock server. Generic
  subscribe against the existing Radicale fixture (fallback path), including a listing that
  includes the extra href and drops it on 403.
- Handler: `Local` persists the hrefs into the config, and unsubscribing an `Own` calendar is
  refused.
- Live check on 4sigma: subscribe to a colleague, confirm events (or Occupato blocks) appear,
  then unsubscribe.
