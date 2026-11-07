# Contacts and calendars: UI

## Intent

mail-cat aims to be an alternative to Outlook, Apple Mail and Thunderbird. Users must not be
inconvenienced. Most Thunderbird users never use its calendar, so they lose nothing. The
target is to make **Apple Mail** and **Outlook** users feel at home.

Two habits have to be served at once:
- **Apple Mail**: the calendar is "invisible". It comes to you inside the mail as an invite card
  or a detected date, and you never have to open it.
- **Outlook**: Mail, Calendar and People are sections of the same app, and the calendar is a
  complete one.

The mockup the user approved is in `.superpowers/brainstorm/*/content/calendar-ui.html`. It is a
wireframe, not the final graphics.

Server side is done (spec `2026-11-07-contacts-calendars-sync-design.md`, branch
`feat/dav-sync`). It provides:
- bidirectional CardDAV/CalDAV
- gRPC `CalendarService` and the contacts RPCs
- per-feature permission status (`AccountAuthStatus.contacts` / `.calendar`)
- the always-available "Ri-autentica" button in the account settings

## Goals, in value order

1. **Section rail**: Posta / Calendario / Contatti icons at the top of the account rail, above
   the account avatars. Ctrl+1 / Ctrl+2 / Ctrl+3 switch between them, as in Outlook.
2. **Calendar section**:
   - the calendar list on the left: all accounts together, grouped by account, each calendar
     with its color and a show/hide checkbox, a 🔒 on read-only ones, and a mini month
   - views Day / Work week / Week / Month
   - double-click or drag on an empty slot to create an event
   - click an event to open it
   - drag to move it, and drag its edge to resize it, both on writable calendars only
3. **Agenda pane** on the right of the mail view: today, then the next 7 days, across every
   visible calendar. It can be opened and closed, and its state is remembered.
4. **Contact card on hover** over a sender or recipient name in the message pane:
   - name, organization, address book (and account), emails and phones
   - actions: Write, Recent mails (a search for `from:`), and later Schedule meeting (§Phase 2)
5. **Contacts section**:
   - a list across accounts, grouped by address book, with search
   - a detail card, plus create / edit / delete in a writable book
   - harvested-from-mail contacts appear in their own group "Dalle mail", read-only
6. **Permissions in place**:
   - a feature whose permission is missing shows an inline notice in the section where it
     would appear: the calendar list, the contacts list, or the agenda
   - the notice says what is missing and offers "Ri-autentica"
   - nothing else is blocked, and mail never is
7. **Event editor** (dialog):
   - title, calendar (writable only), start/end, all-day, location and description
   - delete, with confirmation
   - times are shown and entered in the system time zone; the server keeps the event's own
     TZID

## Phase 2: built together with the server's meetings phase, NOT now

- **Invite card inside the message** (iMIP `text/calendar` part):
  - Accept / Maybe / Decline / Propose new time
  - a mini-day strip showing conflicts with existing events
  - the invite shown dashed in the calendar until it is answered
- **Find a time / scheduling assistant**: attendees and free/busy, via the Calendar API and
  Graph.
- **Schedule meeting** from the contact card and the compose window.

## Phase 3: later

- **Dates detected in mail**: a "📅 data trovata" marker in the list and an "add to calendar"
  banner, as in Apple Mail.
- **Reminders as system notifications** (VALARM).
- Editing **one occurrence** of a recurring series, with the "this / this and following / all"
  choice. Until then, an edit applies to the whole series, and the editor says so.

## Non-goals

- Tasks (VTODO), creating, renaming or deleting calendars or address books, sharing
  management, printing, a year view, and per-event time-zone pickers.
- Any UI for Microsoft beyond the permission notice. Graph is server phase 3.
- Reverting a lost conflict version (recorded in memory, a separate later feature).

## Anti-goals

- **Forcing the calendar on a mail-only user.** With the agenda closed and the Calendar section
  never opened, the app must look and behave exactly as today. That means:
  - no prompts
  - no badges about missing calendar permissions on the mail view
- **Mail becoming slower.** The agenda and hover card load lazily and never delay the message
  list or the reading pane.
- A calendar that silently shows wrong times. All-day events, DST changes, and events in
  another TZID must display correctly in the system zone.

## Constraints and existing stack (build on it)

- Frontend: React 19, TanStack Query, zustand, Tailwind 4, date-fns and lucide-react. It
  follows the feature-sliced layout already in `crates/mc-ui/src-ui/src`: `app`, `pages`,
  `widgets`, `features`, `entities`, `shared`.
- Expected new slices:
  - `entities/calendar`, `entities/contact` (if no contact entity exists yet)
  - `pages/calendar`, `pages/contacts`
  - `widgets/agenda-pane`, `widgets/contact-card`
  - `features/edit-event`, `features/edit-contact`
- Tauri commands in `crates/mc-ui/src/commands.rs` call `mc-client`. Every new RPC needs a
  command, following the existing ones.
- Hardcoded Italian labels, like the rest of the app. There is no i18n mechanism.
- **No heavy calendar library.** The week/day grid is absolutely positioned blocks and the month
  view is a CSS grid, both built with date-fns. *(My call: this keeps the bundle small and the
  look consistent; revisit only if drag-and-drop becomes unmanageable.)*
- The same project file-size rule applies (400-600 lines); split components accordingly.

## Server gaps this UI needs (small, in scope)

- `EventFields` / `EventEdit` / the proto have no **description**. Add it, extracted from and
  written back to `DESCRIPTION`, raw-preserving like the other fields.
- **Live updates**:
  - when a DAV pass changes something, the UI must refresh without polling the user's patience
  - the sync service already publishes events on the bus
  - add a server stream, or extend an existing watch stream (`WatchAuthStatus` /
    `WatchSyncProgress`), with a "contacts/calendar changed for account X" signal
  - the UI invalidates the related TanStack queries on that signal
  - *(My call: a push signal, not polling, because the mail side already works by push.)*
- **Cross-account queries**: "events in range" and "contacts search" for all accounts at once,
  or the UI fans out per account in parallel. The builder picks the smaller change.
- **Lookup by email address** for the hover card: is there a remote or harvested contact for
  this address? Prefer the account the message belongs to. `SearchContacts` may already cover
  it; check.

## Behaviour details

- **Colors** come from the server's calendar color. A calendar without one gets a stable color
  derived from its id.
- **Calendar visibility** (checkboxes) and the agenda open/closed state are per user, kept in
  the UI's existing settings persistence.
- **Read-only calendars**:
  - they are shown, but cannot be chosen in the editor
  - drag and resize are disabled on them
  - their events open read-only
- **A pending local change** (`dirty`) shows a small "in sincronizzazione" marker on the event
  or contact. `push_failed` shows the reason on hover, and the item stays editable.
- **Empty states**:
  - Calendar section with no account having calendars: one line explaining it, plus the
    permission notice where relevant
  - Contacts section: the same
- **Section switch** keeps each section's own state: the selected message, the calendar
  date/view, and the selected contact.

## Decisions from the builder review *(all my calls)*

1. **Recurring events**:
   - drag and resize are **disabled** on recurring events, so a gesture can never silently move
     a whole series
   - the editor shows the series' own start/end (the master), with the note "modifica l'intera
     serie"
   - clicking an occurrence opens the series
2. **Times**:
   - a timed event's `start`/`end` are UTC instants, rendered in the system zone
   - an all-day event's are UTC midnight of its dates, with `end` exclusive (iCal `DTEND`); the
     UI reads the UTC date parts and never converts them, or a date would shift a day west of
     UTC
   - the server already normalises TZIDs, and floating times are taken as UTC by the parser
     (a known limit)
3. **Contacts**:
   - add one RPC, `LookupContact(email, account)`, returning the full card: name, org, emails,
     phones, address book and account, plus a `source` of remote or harvested
   - it prefers a remote contact of that account, then any remote one, then a harvested one
   - the "Dalle mail" group lists harvested contacts through the existing search/list path,
     extended if it lacks fields
   - harvested contacts have no edit or delete in the UI
4. **Live updates**:
   - a coarse signal "contacts or calendar changed for account X"
   - added to an existing watch stream if one fits (`WatchSyncProgress`); otherwise a new
     server stream
   - one app-wide Tauri listener, active whatever the view, invalidates the related queries
5. **Cross-account events**: the UI fans out `ListEvents` per account in parallel. Each account
   gets its own loading and error state, and one failing account never hides the others.
6. **Sync markers**: `EventOccurrence` gains `dirty` and `push_failed`, so the grid can show
   them. Contacts already carry them.
7. **Persistence**: calendar visibility and the agenda open/closed state live in a zustand
   persisted store (localStorage), like the session, theme and shortcut stores.
8. **Rail and shortcuts**:
   - the section icons go at the top of the existing account-rail column; no new column
   - Ctrl+1 / Ctrl+2 / Ctrl+3 are free today; the builder rechecks the shortcut store
   - Labels and Settings stay full pages as now, not sections

## Done criteria (UI phase 1, goals 1-7)

- `bun run build` (tsc + vite) and `bun run lint` pass, along with cargo check, clippy and the
  tests for the touched crates.
- By hand, on the user's Gmail and 4sigma accounts:
  - week, month and day views show the real events in the right local times, including
    all-day and recurring ones
  - creating, editing, moving and deleting an event from the UI shows up on Google Calendar,
    and a change made on the phone shows up in the UI after a sync, with no manual refresh
  - the hover card finds a synced contact
  - creating and editing a contact round-trips to Google
  - with the agenda closed and Calendar never opened, the mail view is unchanged
- The project rules hold: file size, WHY-only comments, no new heavy dependencies.

## Left to the builder

- Component breakdown, store shape, query keys, and exact drag-and-drop mechanics.
- Visual polish within the existing app theme (light and dark).
- Whether the agenda shows 7 days or "until N events".
