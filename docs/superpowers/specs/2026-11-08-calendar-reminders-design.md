# Calendar reminders (VALARM → desktop notifications)

Phase 3 item of `2026-11-07-contacts-calendars-ui-design.md`, pulled forward.

## Intent

Synced events already carry the reminders set in Google or another client, as VALARMs, and
today nobody reads them. The goal: a desktop notification at the right time, even with the UI
closed, and the option to add or remove reminders from mail-cat's own event editor.

## Goals

- Desktop notification for each VALARM of an event in **the user's own calendars**.
- Notifications work with the UI closed, because the server sends them, beside the mail
  notifications (`crates/mc-server/src/notifications.rs`).
- Reminders missed while the server was off fire at startup if the event **has not ended**.
- Notification actions:
  - **Apri evento**: the UI opens on the event's day and shows that occurrence.
  - **Unisciti**: present only when the event has a video-call link. It opens the link in
    the browser.
- Editor: a list of reminders ("10 minuti prima", …) to add and remove. They are written as
  VALARMs and pushed like any other edit.

## Non-goals

- Snooze.
- Writing `ACKNOWLEDGED` back to CalDAV: a reminder dismissed here can still ring on the phone.
- Reminders for subscribed colleague calendars (`DavConfig.subscribed`).
- A per-calendar on/off toggle.
- Sending `ACTION:EMAIL` alarms as mail. `EMAIL` and `AUDIO` alarms are shown as display
  notifications.
- VTODO alarms.

## Anti-goals

- **Double notifications.** A reminder fires once per (event, occurrence, trigger), including
  after a restart.
- **A wrong time.** All-day events, `RELATED=END`, absolute triggers, DST and TZIDs must give
  the same moment Google shows.
- **Mail-only users bothered.** No calendar permission or no DAV means no scheduler work and
  no prompts.

## Design

### Parsing (mc-domain `formats/icalendar.rs`)

- Each occurrence exposes its alarms. calcard already parses `VALARM` components, so this is
  reading, not a new parser.
- A recurrence override (RECURRENCE-ID) uses its own VALARMs if it has any, otherwise the
  master's (RFC 5545 semantics, as Google does).
- An alarm resolves to a fire instant per occurrence:
  - relative trigger (default `RELATED=START`): occurrence start + duration;
  - `RELATED=END`: occurrence end + duration;
  - absolute (`VALUE=DATE-TIME`): that instant, applying only to the occurrence it falls
    before. *(Builder: for a recurring series, an absolute trigger is effectively a
    one-shot; attach it to the first occurrence whose start is ≥ the trigger.)*
  - all-day event: the start is midnight **in the system zone** of that date. Google's
    all-day default `-PT15H` gives 09:00 the day before.
- Unparseable alarms are skipped with a `debug!` log, never an error for the event.

### Scheduler (mc-server)

- A task in the account runtime, alongside DAV sync, ticks every 60 s.
- Each tick scans the own (non-subscribed) calendars of every account with calendar
  permission:
  - occurrences overlapping `[now, now + 8 days]` (8 days covers a `-P1W` trigger);
  - plus occurrences already started but not yet ended (missed reminders).
- An alarm with `fire_at <= now`, its occurrence not yet ended, and not yet recorded fires,
  and is recorded.
- Fired reminders live in a new SQLite table `fired_reminders(account_id, href,
  occurrence_start, trigger_key, fired_at)` with a unique key on the first four. Rows whose
  occurrence ended more than a day ago are pruned each tick (or hourly). No migration: the
  data folder is wiped on model changes (pre-1.0).
- `trigger_key` is the normalized trigger text plus `RELATED`. If the user changes
  "10 min prima" to "30 min prima", the new reminder fires too.
- After a sync applies remote changes, the next tick sees them. No extra wiring is needed.
- *(Builder: reuse the existing occurrence-range query the `ListEvents` handler uses rather
  than a second expansion path.)*

### Notification

- Title: event summary. Body: local time range (e.g. "Oggi 14:30–15:00" / "Domani, tutto il
  giorno") plus location if any. Calendar name as app subtitle where the platform allows.
- Actions:
  - `open-event` → launches/focuses the UI with a deep link
    `--open-event <account>/<calendar-href-encoded>/<event-href-encoded>/<occurrence-start-unix>`.
  - `join` → `open` / `xdg-open` the meeting URL. Only present when a URL was found.
- Meeting URL detection, first match wins:
  1. `X-GOOGLE-CONFERENCE`
  2. `URL`
  3. the first `https://` link in `LOCATION`, then in `DESCRIPTION`, matching
     `meet.google.com`, `*.zoom.us`, `teams.microsoft.com`, `teams.live.com`, `*.webex.com`
- Use the same registry/D-Bus listener as mail notifications, so actions survive the banner
  leaving the screen. `NotifiedMessage` grows into an enum (message | event) rather than a
  parallel registry.

### UI deep link (mc-ui)

- `DeepLink` becomes an enum: `Message { … }` (today's `--open-message`) | `Event { account,
  calendar, href, start }`.
- On `Event`, the UI switches to the Calendar section, navigates to that day and opens the
  occurrence's popover or detail. If the event is gone (deleted since), it just opens the day.

### Editor

- `EventDetail` (proto) gains `repeated Reminder reminders`, where
  `Reminder { int64 before_seconds; }` is relative to start.
- `CreateEventRequest` and `UpdateEventRequest` gain `optional ReminderList reminders`:
  - absent: unchanged;
  - present: replace.
- `EventEdit.reminders: Option<Vec<Duration>>`. Replacing rewrites only the master's
  **relative-to-start** alarms (as `ACTION:DISPLAY`, `DESCRIPTION:Reminder`, a new UID per
  alarm). Absolute and `RELATED=END` alarms are kept untouched and not shown in the editor.
- UI: a "Promemoria" row in the event editor, as chips "10 min prima ✕" plus a "+ Aggiungi"
  menu: all'ora dell'evento, 5/10/15/30 min, 1/2 h, 1/2 giorni, 1 settimana prima, and a
  custom value in minutes. All-day events offer "il giorno stesso alle 9:00" (`PT9H`) and
  "il giorno prima alle 9:00" (`-PT15H`) instead of the minute list.
- New events are prefilled with **one reminder, 10 minutes before** (all-day: "il giorno
  prima alle 9:00"). *(My call, matches Google's default; removable.)*
- Read-only calendars (colleague) don't show the row.

## Constraints

- Build on what exists:
  - notify-rust plus the D-Bus listener in `notifications.rs`;
  - the deep-link plumbing (`tauri-plugin-single-instance`, `PendingDeepLink`,
    `useDeepLink.ts`);
  - `EventEdit` and the push path for edits;
  - calcard for iCal.
- No new dependencies expected beyond what's in the workspace.
- Tests:
  - fire-time resolution in mc-domain: relative, `RELATED=END`, absolute, all-day, DST day,
    override vs master;
  - scheduler dedupe and the missed-reminder rule in mc-server, with a fake clock;
  - VALARM round-trip in `EventEdit`.

## Left to the builder

- Tick placement: a per-account task or one global task.
- How the fake clock is injected.
- Exact notification text formatting.
- Where the editor row sits in the existing dialog.
