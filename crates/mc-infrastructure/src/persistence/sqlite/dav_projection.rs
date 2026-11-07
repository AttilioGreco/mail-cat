//! Projection of the DAV events: address books, calendars, the calendar
//! events and the remote half of contacts. Like the rest of the projector,
//! every arm is replay-safe.

use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};

use mc_domain::{
    AccountId, AggregateType, CalendarEvent, ContactId, DomainError, DomainEvent, EventEnvelope,
    Href, Origin, RemoteCard,
};

use super::repository::{exec, json, nanos};

fn event_key_of(account: &AccountId, href: &Href) -> String {
    AggregateType::CalendarEvent.path_id(*account, href.as_str())
}

/// Stamps the server half onto a contact row that `ContactAdded`/`Updated`
/// already wrote. A local edit makes it dirty; whatever the server says does
/// not.
pub(super) fn bind_contact(
    tx: &Connection,
    contact_id: &ContactId,
    card: &RemoteCard,
    at: DateTime<Utc>,
) -> Result<(), DomainError> {
    let b = &card.binding;
    let local = card.origin == Origin::Local;
    exec(
        tx,
        "UPDATE contacts SET raw = ?2, remote_account = ?3, remote_collection = ?4, \
             remote_href = ?5, remote_etag = ?6, remote_modified = ?7, dirty = ?8, \
             edited_at = CASE WHEN ?8 THEN ?9 ELSE edited_at END, push_error = NULL \
         WHERE id = ?1",
        params![
            contact_id.as_uuid(),
            card.raw,
            b.account.as_uuid(),
            b.collection.as_str(),
            b.href.as_str(),
            b.etag.as_ref().map(|e| e.as_str()),
            b.remote_modified.map(nanos),
            local,
            nanos(at),
        ],
    )
    .map(drop)
}

/// A local delete of a contact the server has leaves its row gone but the
/// DELETE still to send; a delete the server did, or of a contact it never
/// saw, leaves nothing.
pub(super) fn contact_removed(
    tx: &Connection,
    contact_id: &ContactId,
    origin: Origin,
    at: DateTime<Utc>,
) -> Result<(), DomainError> {
    let key = contact_id.to_string();
    if origin == Origin::Local {
        exec(
            tx,
            "INSERT OR REPLACE INTO pending_deletes \
                 (aggregate_id, kind, account_id, collection, href, etag, edited_at) \
             SELECT ?1, 'contact', remote_account, remote_collection, remote_href, remote_etag, ?3 \
             FROM contacts WHERE id = ?2 AND remote_etag IS NOT NULL",
            params![key, contact_id.as_uuid(), nanos(at)],
        )?;
    } else {
        exec(
            tx,
            "DELETE FROM pending_deletes WHERE aggregate_id = ?1",
            [key],
        )?;
    }
    Ok(())
}

pub(super) fn apply(tx: &Connection, envelope: &EventEnvelope) -> Result<(), DomainError> {
    let at = envelope.occurred_at;
    match &envelope.payload {
        DomainEvent::ContactPushedToServer {
            contact_id,
            href,
            etag,
        } => {
            exec(
                tx,
                "UPDATE contacts SET remote_href = ?2, remote_etag = ?3, dirty = 0, \
                     push_error = NULL WHERE id = ?1",
                params![contact_id.as_uuid(), href.as_str(), etag.as_str()],
            )?;
            exec(
                tx,
                "DELETE FROM pending_deletes WHERE aggregate_id = ?1",
                [contact_id.to_string()],
            )?;
        }
        DomainEvent::ContactConflictLost { contact_id, .. } => {
            // The row, or its pending delete when the user had deleted it.
            exec(
                tx,
                "INSERT OR IGNORE INTO conflicts_lost (event_id, account_id) \
                 SELECT ?1, remote_account FROM contacts \
                     WHERE id = ?2 AND remote_account IS NOT NULL \
                 UNION ALL \
                 SELECT ?1, account_id FROM pending_deletes WHERE aggregate_id = ?3",
                params![
                    envelope.event_id,
                    contact_id.as_uuid(),
                    contact_id.to_string()
                ],
            )?;
        }
        DomainEvent::ContactPushFailed { contact_id, reason } => {
            exec(
                tx,
                "UPDATE contacts SET push_error = ?2 WHERE id = ?1",
                params![contact_id.as_uuid(), reason],
            )?;
            exec(
                tx,
                "UPDATE pending_deletes SET push_error = ?2 WHERE aggregate_id = ?1",
                params![contact_id.to_string(), reason],
            )?;
        }

        DomainEvent::AddressBookDiscovered { book } | DomainEvent::AddressBookUpdated { book } => {
            exec(
                tx,
                "INSERT INTO address_books (account_id, collection, name, description, read_only) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(account_id, collection) DO UPDATE SET name = excluded.name, \
                     description = excluded.description, read_only = excluded.read_only",
                params![
                    book.account.as_uuid(),
                    book.collection.as_str(),
                    book.name,
                    book.description,
                    book.read_only
                ],
            )?;
        }
        DomainEvent::AddressBookRemoved {
            account_id,
            collection,
        } => {
            let (account, collection) = (account_id.as_uuid(), collection.as_str());
            exec(
                tx,
                "DELETE FROM address_books WHERE account_id = ?1 AND collection = ?2",
                params![account, collection],
            )?;
            // Contact emails follow by cascade.
            exec(
                tx,
                "DELETE FROM contacts WHERE remote_account = ?1 AND remote_collection = ?2",
                params![account, collection],
            )?;
            exec(
                tx,
                "DELETE FROM pending_deletes \
                 WHERE account_id = ?1 AND collection = ?2 AND kind = 'contact'",
                params![account, collection],
            )?;
        }

        DomainEvent::CalendarDiscovered { calendar: c }
        | DomainEvent::CalendarUpdated { calendar: c } => {
            exec(
                tx,
                "INSERT INTO calendars \
                     (account_id, collection, name, color, timezone, read_only, ownership, components) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
                 ON CONFLICT(account_id, collection) DO UPDATE SET name = excluded.name, \
                     color = excluded.color, timezone = excluded.timezone, \
                     read_only = excluded.read_only, ownership = excluded.ownership, \
                     components = excluded.components",
                params![
                    c.account.as_uuid(),
                    c.collection.as_str(),
                    c.name,
                    c.color,
                    c.timezone,
                    c.read_only,
                    json(&c.ownership)?,
                    json(&c.components)?
                ],
            )?;
        }
        DomainEvent::CalendarRemoved {
            account_id,
            collection,
        } => {
            let (account, collection) = (account_id.as_uuid(), collection.as_str());
            exec(
                tx,
                "DELETE FROM calendars WHERE account_id = ?1 AND collection = ?2",
                params![account, collection],
            )?;
            exec(
                tx,
                "DELETE FROM calendar_events WHERE account_id = ?1 AND collection = ?2",
                params![account, collection],
            )?;
            exec(
                tx,
                "DELETE FROM pending_deletes \
                 WHERE account_id = ?1 AND collection = ?2 AND kind = 'event'",
                params![account, collection],
            )?;
        }

        DomainEvent::CalendarEventAdded { event, origin }
        | DomainEvent::CalendarEventUpdated { event, origin } => {
            upsert_event(tx, event, *origin, at)?;
        }
        DomainEvent::CalendarEventRemoved {
            account_id,
            href,
            origin,
        } => {
            let key = event_key_of(account_id, href);
            if *origin == Origin::Local {
                exec(
                    tx,
                    "INSERT OR REPLACE INTO pending_deletes \
                         (aggregate_id, kind, account_id, collection, href, etag, edited_at) \
                     SELECT ?1, 'event', account_id, collection, href, etag, ?4 \
                     FROM calendar_events WHERE account_id = ?2 AND href = ?3 AND etag IS NOT NULL",
                    params![key, account_id.as_uuid(), href.as_str(), nanos(at)],
                )?;
            } else {
                exec(
                    tx,
                    "DELETE FROM pending_deletes WHERE aggregate_id = ?1",
                    [key],
                )?;
            }
            exec(
                tx,
                "DELETE FROM calendar_events WHERE account_id = ?1 AND href = ?2",
                params![account_id.as_uuid(), href.as_str()],
            )?;
        }
        DomainEvent::CalendarEventPushedToServer {
            account_id,
            href,
            etag,
        } => {
            exec(
                tx,
                "UPDATE calendar_events SET etag = ?3, dirty = 0, push_error = NULL \
                 WHERE account_id = ?1 AND href = ?2",
                params![account_id.as_uuid(), href.as_str(), etag.as_str()],
            )?;
            exec(
                tx,
                "DELETE FROM pending_deletes WHERE aggregate_id = ?1",
                [event_key_of(account_id, href)],
            )?;
        }
        DomainEvent::CalendarEventConflictLost { account_id, .. } => {
            exec(
                tx,
                "INSERT OR IGNORE INTO conflicts_lost (event_id, account_id) VALUES (?1, ?2)",
                params![envelope.event_id, account_id.as_uuid()],
            )?;
        }
        DomainEvent::CalendarEventPushFailed {
            account_id,
            href,
            reason,
        } => {
            exec(
                tx,
                "UPDATE calendar_events SET push_error = ?3 WHERE account_id = ?1 AND href = ?2",
                params![account_id.as_uuid(), href.as_str(), reason],
            )?;
            exec(
                tx,
                "UPDATE pending_deletes SET push_error = ?2 WHERE aggregate_id = ?1",
                params![event_key_of(account_id, href), reason],
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn upsert_event(
    tx: &Connection,
    event: &CalendarEvent,
    origin: Origin,
    at: DateTime<Utc>,
) -> Result<(), DomainError> {
    let b = &event.binding;
    let f = event.fields.as_ref();
    exec(
        tx,
        "INSERT INTO calendar_events \
             (account_id, href, collection, etag, remote_modified, raw, summary, start_at, end_at, \
              all_day, recurring, location, status, dirty, edited_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15) \
         ON CONFLICT(account_id, href) DO UPDATE SET collection = excluded.collection, \
             etag = excluded.etag, remote_modified = excluded.remote_modified, raw = excluded.raw, \
             summary = excluded.summary, start_at = excluded.start_at, end_at = excluded.end_at, \
             all_day = excluded.all_day, recurring = excluded.recurring, \
             location = excluded.location, status = excluded.status, dirty = excluded.dirty, \
             edited_at = CASE WHEN excluded.dirty THEN excluded.edited_at \
                              ELSE calendar_events.edited_at END, \
             push_error = NULL",
        params![
            b.account.as_uuid(),
            b.href.as_str(),
            b.collection.as_str(),
            b.etag.as_ref().map(|e| e.as_str()),
            b.remote_modified.map(nanos),
            event.raw,
            f.map(|f| f.summary.as_str()),
            f.map(|f| nanos(f.start)),
            f.map(|f| nanos(f.end)),
            f.is_some_and(|f| f.all_day),
            f.is_some_and(|f| f.recurring),
            f.and_then(|f| f.location.as_deref()),
            f.and_then(|f| f.status).map(|s| json(&s)).transpose()?,
            origin == Origin::Local,
            nanos(at),
        ],
    )
    .map(drop)
}

/// Everything an account's DAV data leaves in the read model.
pub(super) fn drop_account(tx: &Connection, account: &AccountId) -> Result<(), DomainError> {
    let id = account.as_uuid();
    for sql in [
        "DELETE FROM contacts WHERE remote_account = ?1",
        "DELETE FROM address_books WHERE account_id = ?1",
        "DELETE FROM calendars WHERE account_id = ?1",
        "DELETE FROM calendar_events WHERE account_id = ?1",
        "DELETE FROM pending_deletes WHERE account_id = ?1",
        "DELETE FROM conflicts_lost WHERE account_id = ?1",
    ] {
        exec(tx, sql, [id])?;
    }
    Ok(())
}
