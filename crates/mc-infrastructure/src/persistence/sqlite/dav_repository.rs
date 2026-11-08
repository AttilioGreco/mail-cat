//! `DavReadRepository` over SQLite.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::types::{Type, ValueRef};
use rusqlite::{Connection, Row, params};
use uuid::Uuid;

use mc_domain::{
    AccountId, AddressBook, AggregateType, Calendar, CalendarEvent, CollectionId, ContactId,
    DavReadRepository, DomainError, EmailAddress, Etag, EventFields, EventSync, Href, Occurrence,
    PendingChange, PendingPush, RemoteBinding, RemoteContact, formats,
};

use super::SqliteDb;
use super::repository::{
    SqliteRepository, conversion_err, exec, json_col, nanos, query_all, query_one, time_col,
};

fn opt_time(row: &Row, idx: usize) -> rusqlite::Result<Option<DateTime<Utc>>> {
    Ok(row
        .get::<_, Option<i64>>(idx)?
        .map(DateTime::from_timestamp_nanos))
}

/// Columns: account_id, collection, href, etag, remote_modified.
fn binding_from_row(row: &Row, at: usize) -> rusqlite::Result<RemoteBinding> {
    Ok(RemoteBinding {
        account: AccountId::from_uuid(row.get(at)?),
        collection: CollectionId::new(row.get::<_, String>(at + 1)?),
        href: Href::new(row.get::<_, String>(at + 2)?),
        etag: row.get::<_, Option<String>>(at + 3)?.map(Etag::new),
        remote_modified: opt_time(row, at + 4)?,
    })
}

const EVENT_COLUMNS: &str = "account_id, collection, href, etag, remote_modified, raw, \
     summary, start_at, end_at, all_day, recurring, location, status, description, \
     dirty, push_error";

fn fields_from_row(row: &Row, at: usize) -> rusqlite::Result<Option<EventFields>> {
    let Some(summary) = row.get::<_, Option<String>>(at)? else {
        return Ok(None);
    };
    Ok(Some(EventFields {
        summary,
        start: time_col(row, at + 1)?,
        end: time_col(row, at + 2)?,
        all_day: row.get(at + 3)?,
        recurring: row.get(at + 4)?,
        location: row.get(at + 5)?,
        status: match row.get::<_, Option<String>>(at + 6)? {
            Some(text) => Some(
                serde_json::from_str(&text).map_err(|e| conversion_err(at + 6, Type::Text, e))?,
            ),
            None => None,
        },
        description: row.get(at + 7)?,
    }))
}

fn event_from_row(row: &Row) -> rusqlite::Result<CalendarEvent> {
    Ok(CalendarEvent {
        binding: binding_from_row(row, 0)?,
        raw: row.get(5)?,
        fields: fields_from_row(row, 6)?,
    })
}

/// Columns after the event's: dirty, push_error.
fn sync_from_row(row: &Row) -> rusqlite::Result<EventSync> {
    Ok(EventSync {
        dirty: row.get(14)?,
        push_failed: row.get(15)?,
    })
}

const REMOTE_CONTACT_COLUMNS: &str = "id, remote_account, remote_collection, remote_href, \
     remote_etag, remote_modified, raw, dirty, push_error";

fn remote_contact_from_row(row: &Row) -> rusqlite::Result<RemoteContact> {
    Ok(RemoteContact {
        id: ContactId::from_uuid(row.get(0)?),
        binding: binding_from_row(row, 1)?,
        raw: row.get::<_, Option<String>>(6)?.unwrap_or_default(),
        dirty: row.get(7)?,
        push_error: row.get(8)?,
    })
}

fn pending(
    conn: &Connection,
    account: Uuid,
    collection: &str,
    kind: &str,
    dirty_sql: &str,
) -> Result<Vec<PendingPush>, DomainError> {
    // Columns: aggregate id, binding (5), raw, edited_at, push_error, local_seq.
    let map = |r: &Row, change: Option<PendingChange>| -> rusqlite::Result<PendingPush> {
        let binding = binding_from_row(r, 1)?;
        let aggregate_id = match r.get_ref(0)? {
            ValueRef::Blob(id) => Uuid::from_slice(id)
                .map_err(|e| conversion_err(0, Type::Blob, e))?
                .to_string(),
            ValueRef::Text(id) => String::from_utf8_lossy(id).into_owned(),
            _ => AggregateType::CalendarEvent.path_id(binding.account, binding.href.as_str()),
        };
        Ok(PendingPush {
            aggregate_id,
            binding,
            change: match change {
                Some(change) => change,
                None => PendingChange::Put { raw: r.get(6)? },
            },
            edited_at: time_col(r, 7)?,
            failure: r.get(8)?,
            sequence: r.get::<_, Option<i64>>(9)?.unwrap_or(0) as u64,
        })
    };
    let mut found = query_all(conn, dirty_sql, params![account, collection], |r| {
        map(r, None)
    })?;
    found.extend(query_all(
        conn,
        "SELECT aggregate_id, account_id, collection, href, etag, NULL, NULL, edited_at, push_error, \
             local_seq FROM pending_deletes WHERE account_id = ?1 AND collection = ?2 AND kind = ?3",
        params![account, collection, kind],
        |r| map(r, Some(PendingChange::Delete)),
    )?);
    found.sort_by_key(|p| p.edited_at);
    Ok(found)
}

fn etags(
    conn: &Connection,
    sql: &str,
    account: Uuid,
    collection: &str,
) -> Result<HashMap<Href, Etag>, DomainError> {
    Ok(query_all(conn, sql, params![account, collection], |r| {
        Ok((
            Href::new(r.get::<_, String>(0)?),
            Etag::new(r.get::<_, String>(1)?),
        ))
    })?
    .into_iter()
    .collect())
}

#[async_trait]
impl DavReadRepository for SqliteRepository {
    async fn list_address_books(
        &self,
        account: &AccountId,
    ) -> Result<Vec<AddressBook>, DomainError> {
        let account = *account.as_uuid();
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    "SELECT account_id, collection, name, description, read_only \
                     FROM address_books WHERE account_id = ?1 ORDER BY name, collection",
                    [account],
                    |r| {
                        Ok(AddressBook {
                            account: AccountId::from_uuid(r.get(0)?),
                            collection: CollectionId::new(r.get::<_, String>(1)?),
                            name: r.get(2)?,
                            description: r.get(3)?,
                            read_only: r.get(4)?,
                        })
                    },
                )
            })
            .await
    }

    async fn list_calendars(&self, account: &AccountId) -> Result<Vec<Calendar>, DomainError> {
        let account = *account.as_uuid();
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    "SELECT account_id, collection, name, color, timezone, read_only, ownership, \
                         components FROM calendars WHERE account_id = ?1 ORDER BY name, collection",
                    [account],
                    |r| {
                        Ok(Calendar {
                            account: AccountId::from_uuid(r.get(0)?),
                            collection: CollectionId::new(r.get::<_, String>(1)?),
                            name: r.get(2)?,
                            color: r.get(3)?,
                            timezone: r.get(4)?,
                            read_only: r.get(5)?,
                            ownership: json_col(r, 6)?,
                            components: json_col(r, 7)?,
                        })
                    },
                )
            })
            .await
    }

    async fn pending_contacts(
        &self,
        account: &AccountId,
        book: &CollectionId,
    ) -> Result<Vec<PendingPush>, DomainError> {
        let (account, book) = (*account.as_uuid(), book.to_string());
        self.db
            .read(move |conn| {
                pending(
                    conn,
                    account,
                    &book,
                    "contact",
                    "SELECT id, remote_account, remote_collection, remote_href, \
                         remote_etag, remote_modified, raw, edited_at, push_error, local_seq \
                     FROM contacts \
                     WHERE remote_account = ?1 AND remote_collection = ?2 AND dirty = 1",
                )
            })
            .await
    }

    async fn pending_events(
        &self,
        account: &AccountId,
        calendar: &CollectionId,
    ) -> Result<Vec<PendingPush>, DomainError> {
        let (account, calendar) = (*account.as_uuid(), calendar.to_string());
        self.db
            .read(move |conn| {
                pending(
                    conn,
                    account,
                    &calendar,
                    "event",
                    "SELECT NULL, account_id, collection, href, etag, remote_modified, raw, \
                         edited_at, push_error, local_seq \
                     FROM calendar_events WHERE account_id = ?1 AND collection = ?2 AND dirty = 1",
                )
            })
            .await
    }

    async fn contact_etags(
        &self,
        account: &AccountId,
        book: &CollectionId,
    ) -> Result<HashMap<Href, Etag>, DomainError> {
        let (account, book) = (*account.as_uuid(), book.to_string());
        self.db
            .read(move |conn| {
                etags(
                    conn,
                    "SELECT remote_href, remote_etag FROM contacts WHERE remote_account = ?1 \
                         AND remote_collection = ?2 AND remote_etag IS NOT NULL",
                    account,
                    &book,
                )
            })
            .await
    }

    async fn event_etags(
        &self,
        account: &AccountId,
        calendar: &CollectionId,
    ) -> Result<HashMap<Href, Etag>, DomainError> {
        let (account, calendar) = (*account.as_uuid(), calendar.to_string());
        self.db
            .read(move |conn| {
                etags(
                    conn,
                    "SELECT href, etag FROM calendar_events WHERE account_id = ?1 \
                         AND collection = ?2 AND etag IS NOT NULL",
                    account,
                    &calendar,
                )
            })
            .await
    }

    async fn remote_contacts(
        &self,
        account: &AccountId,
        book: Option<&CollectionId>,
    ) -> Result<Vec<RemoteContact>, DomainError> {
        let (account, book) = (*account.as_uuid(), book.map(ToString::to_string));
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    &format!(
                        "SELECT {REMOTE_CONTACT_COLUMNS} FROM contacts \
                         WHERE remote_account = ?1 AND remote_href IS NOT NULL \
                           AND (?2 IS NULL OR remote_collection = ?2) \
                         ORDER BY display_name, id"
                    ),
                    params![account, book],
                    remote_contact_from_row,
                )
            })
            .await
    }

    async fn remote_contact(&self, id: &ContactId) -> Result<Option<RemoteContact>, DomainError> {
        let id = *id.as_uuid();
        self.db
            .read(move |conn| {
                query_one(
                    conn,
                    &format!(
                        "SELECT {REMOTE_CONTACT_COLUMNS} FROM contacts \
                         WHERE id = ?1 AND remote_href IS NOT NULL"
                    ),
                    [id],
                    remote_contact_from_row,
                )
            })
            .await
    }

    async fn remote_contacts_by_email(
        &self,
        email: &EmailAddress,
    ) -> Result<Vec<RemoteContact>, DomainError> {
        let email = email.as_str().to_owned();
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    &format!(
                        "SELECT {REMOTE_CONTACT_COLUMNS} FROM contacts \
                         WHERE remote_href IS NOT NULL AND id IN \
                           (SELECT contact_id FROM contact_emails WHERE email = ?1) \
                         ORDER BY id"
                    ),
                    [email],
                    remote_contact_from_row,
                )
            })
            .await
    }

    async fn contact_at(
        &self,
        account: &AccountId,
        href: &Href,
    ) -> Result<Option<ContactId>, DomainError> {
        let (account, href) = (*account.as_uuid(), href.to_string());
        self.db
            .read(move |conn| {
                query_one(
                    conn,
                    "SELECT id FROM contacts WHERE remote_account = ?1 AND remote_href = ?2",
                    params![account, href],
                    |r| Ok(ContactId::from_uuid(r.get(0)?)),
                )
            })
            .await
    }

    async fn get_event(
        &self,
        account: &AccountId,
        href: &Href,
    ) -> Result<Option<CalendarEvent>, DomainError> {
        let (account, href) = (*account.as_uuid(), href.to_string());
        self.db
            .read(move |conn| {
                query_one(
                    conn,
                    &format!(
                        "SELECT {EVENT_COLUMNS} FROM calendar_events \
                         WHERE account_id = ?1 AND href = ?2"
                    ),
                    params![account, href],
                    event_from_row,
                )
            })
            .await
    }

    async fn event_sync(&self, account: &AccountId, href: &Href) -> Result<EventSync, DomainError> {
        let (account, href) = (*account.as_uuid(), href.to_string());
        self.db
            .read(move |conn| {
                query_one(
                    conn,
                    &format!(
                        "SELECT {EVENT_COLUMNS} FROM calendar_events \
                         WHERE account_id = ?1 AND href = ?2"
                    ),
                    params![account, href],
                    sync_from_row,
                )
                .map(Option::unwrap_or_default)
            })
            .await
    }

    async fn events_in_range(
        &self,
        account: &AccountId,
        calendars: &[CollectionId],
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<Occurrence>, DomainError> {
        let account = *account.as_uuid();
        let calendars = calendars.to_vec();
        self.db
            .read(move |conn| {
                // A recurring resource can reach into the range from any
                // start, so those are all loaded and expanded.
                // ponytail: bound them by their first start and UNTIL when
                // an account has enough recurring events for this to show.
                let rows = query_all(
                    conn,
                    &format!(
                        "SELECT {EVENT_COLUMNS} FROM calendar_events \
                         WHERE account_id = ?1 AND start_at IS NOT NULL AND (recurring = 1 \
                             OR (start_at < ?3 AND (end_at > ?2 OR start_at >= ?2)))"
                    ),
                    params![account, super::repository::nanos(from), super::repository::nanos(to)],
                    |r| Ok((event_from_row(r)?, sync_from_row(r)?)),
                )?;
                let mut found = Vec::new();
                for (event, sync) in rows {
                    let calendar = event.binding.collection.clone();
                    if !calendars.is_empty() && !calendars.contains(&calendar) {
                        continue;
                    }
                    let Some(fields) = event.fields else { continue };
                    let expanded = if fields.recurring {
                        match formats::occurrences(&event.raw, from, to) {
                            Ok(expanded) => expanded,
                            Err(e) => {
                                tracing::warn!(href = %event.binding.href, "skipping unreadable event: {e}");
                                continue;
                            }
                        }
                    } else {
                        vec![fields]
                    };
                    found.extend(expanded.into_iter().map(|event_fields| Occurrence {
                        calendar: calendar.clone(),
                        href: event.binding.href.clone(),
                        event: event_fields,
                        sync: sync.clone(),
                    }));
                }
                found.sort_by_key(|o| o.event.start);
                Ok(found)
            })
            .await
    }

    async fn conflicts_lost(&self, account: &AccountId) -> Result<u64, DomainError> {
        let account = *account.as_uuid();
        self.db
            .read(move |conn| {
                query_one(
                    conn,
                    "SELECT COUNT(*) FROM conflicts_lost WHERE account_id = ?1",
                    [account],
                    |r| r.get::<_, i64>(0),
                )
                .map(|n| n.unwrap_or(0) as u64)
            })
            .await
    }
}

/// The calendar reminders that already rang.
#[derive(Clone)]
pub struct ReminderLog {
    db: Arc<SqliteDb>,
}

impl ReminderLog {
    /// Kept this long after the occurrence ended, in case a sync moves it back.
    const RETENTION: chrono::TimeDelta = chrono::TimeDelta::days(1);

    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Records the reminder as shown. False when it already was: the caller
    /// must not show it again.
    pub async fn record(
        &self,
        account: &AccountId,
        href: &Href,
        occurrence: (DateTime<Utc>, DateTime<Utc>),
        trigger_key: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, DomainError> {
        let (account, href, key) = (*account.as_uuid(), href.to_string(), trigger_key.to_owned());
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "INSERT OR IGNORE INTO fired_reminders \
                     (account_id, href, occurrence_start, trigger_key, occurrence_end, fired_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        account,
                        href,
                        nanos(occurrence.0),
                        key,
                        nanos(occurrence.1),
                        nanos(now)
                    ],
                )
                .map(|inserted| inserted == 1)
            })
            .await
    }

    pub async fn prune(&self, now: DateTime<Utc>) -> Result<(), DomainError> {
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "DELETE FROM fired_reminders WHERE occurrence_end < ?1",
                    [nanos(now - Self::RETENTION)],
                )
                .map(drop)
            })
            .await
    }
}
