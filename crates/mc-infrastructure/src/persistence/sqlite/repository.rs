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
    ProviderUid, SuggestScope, Suggestion, ThreadId,
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

/// `%query%` for `LIKE … ESCAPE '\\'`.
// ponytail: LIKE folds ASCII case only (sled lowercased Unicode too); add a
// lowered column if non-ASCII case-insensitive search matters.
fn like_pattern(query: &str) -> String {
    let escaped = query
        .to_lowercase()
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    format!("%{escaped}%")
}

/// Per suggest field: its label and a query yielding `(value, count)` rows for
/// `?1` = LIKE pattern, `?2` = limit.
const SUGGEST_FROM_EMAIL: (&str, &str) = (
    "from_email",
    "SELECT from_email, COUNT(*) FROM messages WHERE from_email LIKE ?1 ESCAPE '\\' \
     GROUP BY from_email ORDER BY 2 DESC, 1 LIMIT ?2",
);
const SUGGEST_FROM_NAME: (&str, &str) = (
    "from_name",
    "SELECT from_name, COUNT(*) FROM messages WHERE from_name LIKE ?1 ESCAPE '\\' \
     GROUP BY from_name ORDER BY 2 DESC, 1 LIMIT ?2",
);
const SUGGEST_TO: (&str, &str) = (
    "to",
    "SELECT json_extract(t.value, '$.email') AS v, COUNT(*) \
     FROM messages m, json_each(m.addresses, '$.to') t \
     WHERE v LIKE ?1 ESCAPE '\\' GROUP BY v ORDER BY 2 DESC, 1 LIMIT ?2",
);
const SUGGEST_SUBJECT: (&str, &str) = (
    "subject",
    "SELECT subject, COUNT(*) FROM messages WHERE subject LIKE ?1 ESCAPE '\\' \
     GROUP BY subject ORDER BY 2 DESC, 1 LIMIT ?2",
);

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
                        "WHERE m.mailbox_id = ?1 AND m.provider_uid = ?2 ORDER BY m.received_at DESC, m.id DESC LIMIT 1",
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

    // ponytail: full scan of `messages` per keystroke (a few ms per 10k rows);
    // add an FTS5 prefix table if it shows up in latency.
    async fn suggest(
        &self,
        prefix: &str,
        scope: Option<SuggestScope>,
        max_per_field: usize,
    ) -> Result<Vec<Suggestion>, DomainError> {
        let fields: &[(&str, &str)] = match scope {
            _ if prefix.is_empty() => &[],
            None => &[SUGGEST_FROM_EMAIL, SUGGEST_FROM_NAME, SUGGEST_TO, SUGGEST_SUBJECT],
            Some(SuggestScope::From) => &[SUGGEST_FROM_EMAIL, SUGGEST_FROM_NAME],
            Some(SuggestScope::To) => &[SUGGEST_TO],
            Some(SuggestScope::Subject) => &[SUGGEST_SUBJECT],
            Some(SuggestScope::Body) => &[],
        };
        let (pattern, limit) = (like_pattern(prefix), sql_int(max_per_field as u64));
        self.db
            .read(move |conn| {
                let mut out = Vec::new();
                for (field, sql) in fields {
                    out.extend(query_all(conn, sql, params![pattern, limit], |r| {
                        Ok(Suggestion {
                            field: (*field).to_owned(),
                            value: r.get(0)?,
                            doc_count: r.get(1)?,
                        })
                    })?);
                }
                Ok(out)
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
        let (pattern, limit) = (like_pattern(query), sql_int(limit as u64));
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    &select_contacts(
                        "WHERE c.display_name LIKE ?1 ESCAPE '\\' OR EXISTS (SELECT 1 FROM contact_emails e \
                             WHERE e.contact_id = c.id AND e.email LIKE ?1 ESCAPE '\\') \
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
    async fn uid_lookup_breaks_received_at_ties_by_newest_id() {
        let (_dir, repo) = open();
        let (account, inbox, received_at) = (AccountId::new(), MailboxId::new(), Utc::now());
        let mut ids = Vec::new();
        for pos in 1..=2 {
            let (id, mut event) = received(account, inbox, &format!("<{pos}@x>"));
            if let DomainEvent::MessageReceived { received_at: at, .. } = &mut event {
                *at = received_at;
            }
            repo.project(&at_position(pos, event)).await.unwrap();
            ids.push(id);
        }

        let found = repo.find_by_provider_uid(&inbox, 1).await.unwrap().unwrap();
        assert_eq!(found.id, ids[1]);
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
