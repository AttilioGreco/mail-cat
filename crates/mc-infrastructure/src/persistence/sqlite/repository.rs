//! SQLite read model: every read trait, the write repositories, checkpoints,
//! the body store and the backfill queue. Projection arms live in
//! `projector.rs`.

use std::collections::{HashMap, HashSet};
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
    Envelope, EventEnvelope, EventStore, Mailbox, MailboxId, MailboxPath, MailboxReadRepository, Message,
    MessageBodyStore, MessageId, MessageReadRepository, MessageSummary, ProjectionCheckpoint,
    ReadModelProjector,
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

    /// Appends and applies the events to the read model before returning, so
    /// callers see their own writes and nothing depends on the event bus
    /// delivering them (it drops events when a subscriber lags).
    async fn append_and_project(
        &self,
        aggregate_id: String,
        aggregate_type: AggregateType,
        events: Vec<DomainEvent>,
    ) -> Result<(), DomainError> {
        let expected = self.events.current_sequence(&aggregate_id).await?;
        let count = events.len() as u64;
        let last = self
            .events
            .append(&aggregate_id, aggregate_type, expected, events.clone())
            .await?;
        for (sequence, event) in (last + 1 - count..).zip(events) {
            let envelope = EventEnvelope::new(aggregate_id.clone(), aggregate_type, sequence, event);
            ReadModelProjector::project(self, &envelope).await?;
        }
        Ok(())
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

pub(super) fn query_all<T>(
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
    truncated: bool,
    fetched_at: DateTime<Utc>,
) -> Result<(), DomainError> {
    exec(
        conn,
        "INSERT INTO message_bodies (message_id, text_zstd, html_zstd, truncated, fetched_at) \
         SELECT ?1, ?2, ?3, ?4, ?5 WHERE EXISTS (SELECT 1 FROM messages WHERE id = ?1) \
         ON CONFLICT(message_id) DO UPDATE SET text_zstd = excluded.text_zstd, \
             html_zstd = excluded.html_zstd, truncated = excluded.truncated, fetched_at = excluded.fetched_at",
        params![id.as_uuid(), compress(text)?, compress(html)?, truncated, nanos(fetched_at)],
    )
    .map(drop)
}

/// The copies of one message (`rfc_id`) in an account that lack a body the
/// best stored copy has, each given that body: blobs are copied, not shared,
/// so every copy keeps its own row and is deleted with it. Copies are of one
/// message only if size and sender agree too; anyone can reuse a Message-ID.
fn share_body(conn: &Connection, account: Uuid, rfc_id: &str) -> Result<Vec<(MessageId, bool)>, DomainError> {
    let rows = query_all(
        conn,
        "SELECT m.id, m.body_state = 'pending', b.truncated, m.size_bytes, m.from_email FROM messages m \
         LEFT JOIN message_bodies b ON b.message_id = m.id \
         WHERE m.account_id = ?1 AND m.rfc_message_id = ?2",
        params![account, rfc_id],
        |r| {
            let copy = (r.get::<_, Uuid>(0)?, r.get::<_, bool>(1)?, r.get::<_, Option<bool>>(2)?);
            Ok(((r.get::<_, i64>(3)?, r.get::<_, String>(4)?), copy))
        },
    )?;
    let mut messages: HashMap<(i64, String), Vec<_>> = HashMap::new();
    for (key, copy) in rows {
        messages.entry(key).or_default().push(copy);
    }
    let mut shared = Vec::new();
    for copies in messages.into_values() {
        let Some((source, truncated)) = copies
            .iter()
            .filter_map(|&(id, _, truncated)| Some((id, truncated?)))
            .min_by_key(|&(_, truncated)| truncated)
        else {
            continue;
        };
        for (id, pending, has) in copies {
            // A pending copy holding a body of its own lost its event: record it.
            let wants = pending || has.is_some_and(|t| t && !truncated);
            if !wants {
                continue;
            }
            if id != source {
                exec(
                    conn,
                    "INSERT INTO message_bodies (message_id, text_zstd, html_zstd, truncated, fetched_at) \
                     SELECT ?1, text_zstd, html_zstd, truncated, fetched_at FROM message_bodies WHERE message_id = ?2 \
                     ON CONFLICT(message_id) DO UPDATE SET text_zstd = excluded.text_zstd, \
                         html_zstd = excluded.html_zstd, truncated = excluded.truncated, fetched_at = excluded.fetched_at",
                    params![id, source],
                )?;
            }
            shared.push((MessageId::from_uuid(id), truncated));
        }
    }
    Ok(shared)
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
        "SELECT {MESSAGE_COLUMNS}, b.text_zstd, b.html_zstd, b.truncated FROM messages m \
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
        body_truncated: false,
        received_at: time_col(row, 14)?,
        internal_date: time_col(row, 15)?,
    })
}

fn message_with_body(row: &Row) -> rusqlite::Result<Message> {
    let mut message = message_from_row(row)?;
    message.body_text = body_col(row, 16)?;
    message.body_html = body_col(row, 17)?;
    message.body_truncated = row.get::<_, Option<bool>>(18)?.unwrap_or_default();
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
    "SELECT id, name, email, provider_type, status, total_unread, status_error, status_since FROM accounts";

fn account_from_row(row: &Row) -> rusqlite::Result<AccountSummary> {
    Ok(AccountSummary {
        id: AccountId::from_uuid(row.get(0)?),
        name: row.get(1)?,
        email: email_col(row, 2)?,
        provider_type: json_col(row, 3)?,
        status: json_col(row, 4)?,
        total_unread: row.get(5)?,
        status_error: row.get(6)?,
        status_since: row.get::<_, Option<i64>>(7)?.map(DateTime::from_timestamp_nanos),
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
// ponytail: LIKE folds ASCII case only (no Unicode lowercasing); add a
// lowered column if non-ASCII case-insensitive search matters.
fn like_pattern(query: &str) -> String {
    format!("%{}%", like_escape(query))
}

fn like_escape(query: &str) -> String {
    query
        .to_lowercase()
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// Autocomplete, one row per address: a person is picked by address, so a
/// name match yields each of their addresses, an address match only that one.
/// Copies of an address in one account (auto-created duplicates) collapse
/// onto the named one. Ranking, Apple Mail style: name-word / address /
/// domain prefix hits before mere substrings, then addresses you write to
/// (frecency), then ones that write to you, then name. `?1` = escaped query,
/// `?2` = now (ns). Sent frecency = count × 30/(30 + days since last send):
/// an address written to daily last month still beats a one-off from
/// yesterday, but recency wins between similar counts.
const SEARCH_CONTACTS_SQL: &str = r"
    SELECT h.id, h.display_name, h.address, h.emails, h.organization, h.source, h.account_scope
    FROM (
        SELECT c.*, e.email AS address,
            c.display_name LIKE ?1 || '%' ESCAPE '\'
                OR c.display_name LIKE '% ' || ?1 || '%' ESCAPE '\'
                OR e.email LIKE ?1 || '%' ESCAPE '\'
                OR e.email LIKE '%@' || ?1 || '%' ESCAPE '\' AS prefix_hit,
            ROW_NUMBER() OVER (PARTITION BY e.email, c.account_scope
                ORDER BY c.display_name = '', c.id) AS copy
        FROM contacts c
        JOIN contact_emails e ON e.contact_id = c.id
        WHERE c.display_name LIKE '%' || ?1 || '%' ESCAPE '\'
            OR e.email LIKE '%' || ?1 || '%' ESCAPE '\'
    ) h
    LEFT JOIN address_usage u ON u.email = h.address
    WHERE h.copy = 1
    ORDER BY
        h.prefix_hit DESC,
        u.sent_count * 30.0 / (30.0 + MAX(?2 - u.last_sent_at, 0) / 86400e9) DESC,
        u.last_received_at DESC,
        h.display_name COLLATE NOCASE, h.id, h.address
    LIMIT ?3";

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
        self.append_and_project(account.id.to_string(), AggregateType::Account, events).await
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
        self.append_and_project(contact.id.to_string(), AggregateType::Contact, events).await
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
        truncated: bool,
    ) -> Result<(), DomainError> {
        let id = *message_id;
        self.db
            .write(move |conn| {
                store_body(conn, &id, body_text.as_deref(), body_html.as_deref(), truncated, Utc::now())
            })
            .await
    }

    async fn share_bodies(
        &self,
        account_id: &AccountId,
        rfc_message_ids: &[String],
    ) -> Result<Vec<(MessageId, bool)>, DomainError> {
        // An empty `<>` would pair unrelated messages.
        let rfc_ids: Vec<String> =
            rfc_message_ids.iter().filter(|id| !id.trim_matches(['<', '>', ' ']).is_empty()).cloned().collect();
        if rfc_ids.is_empty() {
            return Ok(Vec::new());
        }
        let account = *account_id.as_uuid();
        self.db
            .write(move |conn| {
                let tx = conn.transaction().map_err(repo_err)?;
                let mut shared = Vec::new();
                for rfc_id in &rfc_ids {
                    shared.extend(share_body(&tx, account, rfc_id)?);
                }
                tx.commit().map_err(repo_err)?;
                Ok(shared)
            })
            .await
    }
}

/// Messages the backfill downloads: Trash and Spam only when opened, and
/// none without a UID to fetch it by (a renumbered mailbox mid-sync).
const BACKFILLABLE: &str = "FROM messages m LEFT JOIN mailboxes b ON b.id = m.mailbox_id \
     WHERE m.account_id = ?1 AND m.body_state = 'pending' AND m.provider_uid IS NOT NULL \
     AND IFNULL(b.mailbox_type, '') NOT IN ('\"Trash\"', '\"Spam\"')";

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
                // ponytail: sorts every pending row per call (~100k on a big
                // Gmail account, well under a batch's fetch); a class column
                // in the backfill index if it ever shows.
                query_all(
                    conn,
                    &format!(
                        "SELECT m.id, m.mailbox_id, m.provider_uid, m.rfc_message_id, \
                         m.size_bytes, m.from_email {BACKFILLABLE} \
                         ORDER BY CASE b.mailbox_type WHEN '\"Inbox\"' THEN 0 \
                             WHEN '\"All\"' THEN 2 WHEN '\"Important\"' THEN 2 ELSE 1 END, \
                             m.received_at DESC LIMIT ?2"
                    ),
                    params![account, limit],
                    |r| {
                        Ok(BackfillEntry {
                            message_id: MessageId::from_uuid(r.get(0)?),
                            mailbox_id: MailboxId::from_uuid(r.get(1)?),
                            provider_uid: r.get(2)?,
                            rfc_message_id: r.get(3)?,
                            size_bytes: r.get::<_, i64>(4)? as u64,
                            from_email: r.get(5)?,
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
                count_rows(conn, &format!("SELECT COUNT(*) {BACKFILLABLE}"), [account])
                .map(|n| u32::try_from(n).unwrap_or(u32::MAX))
            })
            .await
    }

    async fn downloaded_since(
        &self,
        account_id: &AccountId,
        since: DateTime<Utc>,
    ) -> Result<u64, DomainError> {
        let (account, since) = (*account_id.as_uuid(), nanos(since));
        self.db
            .read(move |conn| {
                count_rows(
                    conn,
                    "SELECT COALESCE(SUM(d.bytes), 0) FROM downloads d \
                     JOIN accounts a ON a.email = d.account_email \
                     WHERE a.id = ?1 AND d.at >= ?2",
                    params![account, since],
                )
            })
            .await
    }
}

/// Records what one account downloads from its server, read back through
/// `BodyBackfillQueue::downloaded_since`.
#[derive(Clone)]
pub struct DownloadMeter {
    db: Arc<SqliteDb>,
    account_email: String,
}

impl DownloadMeter {
    /// Kept a little past the 24 h window the cap is checked against.
    const RETENTION: chrono::TimeDelta = chrono::TimeDelta::hours(48);

    /// Keyed like the stored account, which `EmailAddress` lowercases.
    pub fn new(db: Arc<SqliteDb>, account_email: &str) -> Self {
        Self { db, account_email: account_email.trim().to_lowercase() }
    }

    /// Never fails the download it measures: a lost row only under-counts.
    pub async fn record(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let (email, now) = (self.account_email.clone(), Utc::now());
        let result = self
            .db
            .write(move |conn| {
                exec(
                    conn,
                    "INSERT INTO downloads (account_email, at, bytes) VALUES (?1, ?2, ?3)",
                    params![email, nanos(now), sql_int(bytes)],
                )?;
                exec(
                    conn,
                    "DELETE FROM downloads WHERE account_email = ?1 AND at < ?2",
                    params![email, nanos(now - Self::RETENTION)],
                )
            })
            .await;
        if let Err(e) = result {
            tracing::warn!(account = %self.account_email, error = %e, "download meter write failed");
        }
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
                        &format!("SELECT \
                           (SELECT COALESCE(SUM(total_messages), 0) FROM mailboxes WHERE account_id = ?1), \
                           (SELECT COUNT(*) FROM messages WHERE mailbox_id IN \
                               (SELECT id FROM mailboxes WHERE account_id = ?1)), \
                           (SELECT COUNT(*) {BACKFILLABLE})"),
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

    async fn held_uids(
        &self,
        mailbox_id: &MailboxId,
        uids: &[ProviderUid],
    ) -> Result<HashSet<ProviderUid>, DomainError> {
        let mailbox = *mailbox_id.as_uuid();
        // One JSON array parameter: a sync batch can be tens of thousands of
        // UIDs, past SQLite's bound-parameter limit.
        let uids = serde_json::to_string(&uids.iter().map(|u| u.value()).collect::<Vec<_>>())
            .map_err(repo_err)?;
        let held = self
            .db
            .read(move |conn| {
                query_all(
                    conn,
                    "SELECT DISTINCT provider_uid FROM messages \
                     WHERE mailbox_id = ?1 AND provider_uid IN (SELECT value FROM json_each(?2))",
                    params![mailbox, uids],
                    |r| r.get::<_, u32>(0),
                )
            })
            .await?;
        Ok(held.into_iter().map(ProviderUid::new).collect())
    }

    async fn uidless_messages(&self, mailbox_id: &MailboxId) -> Result<Vec<MessageId>, DomainError> {
        let mailbox = *mailbox_id.as_uuid();
        let ids = self
            .db
            .read(move |conn| {
                query_all(
                    conn,
                    "SELECT id FROM messages WHERE mailbox_id = ?1 AND provider_uid IS NULL",
                    [mailbox],
                    |r| r.get::<_, Uuid>(0),
                )
            })
            .await?;
        Ok(ids.into_iter().map(MessageId::from_uuid).collect())
    }

    async fn find_by_rfc_id(
        &self,
        account_id: &AccountId,
        rfc_id: &str,
    ) -> Result<Vec<MessageSummary>, DomainError> {
        let (account, rfc_id) = (*account_id.as_uuid(), rfc_id.to_owned());
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    &select_messages("WHERE m.account_id = ?1 AND m.rfc_message_id = ?2"),
                    params![account, rfc_id],
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
        let (query, limit) = (like_escape(query), sql_int(limit as u64));
        let now = nanos(Utc::now());
        self.db
            .read(move |conn| {
                query_all(conn, SEARCH_CONTACTS_SQL, params![query, now, limit], |r| {
                    // The row's primary_email column is the matched address.
                    let mut hit = contact_from_row(r, 0)?;
                    hit.emails.retain(|e| e.address == hit.primary_email);
                    Ok(hit)
                })
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
    use mc_domain::{AccountStatus, EventEnvelope, MailboxType, ProviderType, ReadModelProjector};

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
            restored: false,
        };
        (message_id, event)
    }

    #[tokio::test]
    async fn bodies_are_stored_compressed() {
        let (_dir, repo) = open();
        let (id, event) = received(AccountId::new(), MailboxId::new(), "<a@x>");
        repo.project(&at_position(1, event)).await.unwrap();
        let text = "the same line again\n".repeat(1000);

        repo.put_body(&id, Some(text.clone()), None, false).await.unwrap();

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
    async fn an_auth_status_keeps_its_reason_and_since_until_fixed() {
        let (_dir, repo) = open();
        let added = account_added();
        let DomainEvent::AccountAdded { account_id, .. } = added else { unreachable!() };
        repo.project(&at_position(1, added)).await.unwrap();

        let rejected = at_position(
            2,
            DomainEvent::AccountCredentialsRejected { account_id, error: "the server rejected the password".into() },
        );
        repo.project(&rejected).await.unwrap();
        let account = repo.get_account(&account_id).await.unwrap().unwrap();
        assert_eq!(account.status, AccountStatus::CredentialsRejected);
        assert_eq!(account.status_error.as_deref(), Some("the server rejected the password"));
        assert_eq!(account.status_since, Some(rejected.occurred_at));

        repo.project(&at_position(3, DomainEvent::AccountReauthenticated { account_id })).await.unwrap();
        let account = repo.get_account(&account_id).await.unwrap().unwrap();
        assert_eq!(account.status, AccountStatus::Active);
        assert_eq!(account.status_error, None);
    }

    #[tokio::test]
    async fn downloads_count_per_account_within_the_window() {
        let (_dir, repo) = open();
        let added = account_added();
        let DomainEvent::AccountAdded { account_id, .. } = added else { unreachable!() };
        repo.project(&at_position(1, added)).await.unwrap();
        let meter = DownloadMeter::new(repo.db.clone(), " Me@Example.com");
        let stranger = DownloadMeter::new(repo.db.clone(), "other@example.com");

        let before = Utc::now();
        meter.record(700).await;
        meter.record(300).await;
        stranger.record(5_000).await;

        let day_ago = before - chrono::TimeDelta::hours(24);
        assert_eq!(repo.downloaded_since(&account_id, day_ago).await.unwrap(), 1_000);
        let later = Utc::now() + chrono::TimeDelta::seconds(1);
        assert_eq!(repo.downloaded_since(&account_id, later).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn deleting_a_message_takes_its_body_along() {
        let (_dir, repo) = open();
        let (id, event) = received(AccountId::new(), MailboxId::new(), "<a@x>");
        repo.project(&at_position(1, event)).await.unwrap();
        repo.put_body(&id, Some("x".into()), None, false).await.unwrap();

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
    async fn saved_contact_events_reach_the_read_model_without_the_bus() {
        let (_dir, repo) = open();
        let (contact, added) = Contact::create(
            "Ada".into(),
            vec![mc_domain::ContactEmail::primary(EmailAddress::new("ada@example.com").unwrap())],
            vec![],
            None,
            None,
            mc_domain::ContactSource::Local,
            None,
            None,
        )
        .unwrap();

        ContactWriteRepository::save(&repo, &contact, vec![added]).await.unwrap();
        assert!(repo.get_contact(&contact.id).await.unwrap().is_some());

        ContactWriteRepository::save(&repo, &contact, vec![contact.remove()]).await.unwrap();
        assert!(repo.get_contact(&contact.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn address_usage_migration_seeds_from_stored_messages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mail-cat.db");
        let db = SqliteDb::open(&path).unwrap();
        let repo = SqliteRepository::new(db.clone(), Arc::new(SqliteEventStore::new(db)));
        let (account, inbox, sent) = (AccountId::new(), MailboxId::new(), MailboxId::new());
        let addr = |raw: &str| vec![Address { name: None, email: EmailAddress::new(raw).unwrap() }];
        let mut events = vec![];
        for (mailbox_id, mailbox_type) in [(inbox, MailboxType::Inbox), (sent, MailboxType::Sent)] {
            events.push(DomainEvent::MailboxDiscovered {
                mailbox_id,
                account_id: account,
                path: MailboxPath::parse(&format!("{mailbox_type:?}"), '/').unwrap(),
                mailbox_type,
            });
        }
        for (mailbox, from, to) in [
            (sent, "me@example.com", "ada@example.com"),
            (sent, "me@example.com", "ada@example.com"),
            (inbox, "bob@example.com", "me@example.com"),
        ] {
            let (_, mut event) = received(account, mailbox, "<x@y>");
            if let DomainEvent::MessageReceived { envelope, .. } = &mut event {
                envelope.from = addr(from);
                envelope.to = addr(to);
                envelope.cc = addr(to);
            }
            events.push(event);
        }
        for (pos, event) in (1..).zip(events) {
            repo.project(&at_position(pos, event)).await.unwrap();
        }
        // Back to a pre-usage database holding the same messages.
        repo.db
            .write(|c| c.execute_batch(
                "DROP TABLE address_usage; DROP TABLE sent_counted; DROP TABLE downloads; \
                 ALTER TABLE accounts DROP COLUMN status_error; ALTER TABLE accounts DROP COLUMN status_since; \
                 ALTER TABLE message_bodies DROP COLUMN truncated; \
                 PRAGMA user_version = 2;",
            ).map_err(repo_err))
            .await
            .unwrap();
        drop(repo);

        let usage: Vec<(String, i64, bool, bool)> = SqliteDb::open(&path)
            .unwrap()
            .read(|c| {
                query_all(
                    c,
                    "SELECT email, sent_count, last_sent_at IS NOT NULL, last_received_at IS NOT NULL \
                     FROM address_usage ORDER BY email",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
            })
            .await
            .unwrap();
        assert_eq!(
            usage,
            [("ada@example.com".into(), 2, true, false), ("bob@example.com".into(), 0, false, true)]
        );
    }

    #[tokio::test]
    async fn sent_message_syncing_back_is_one_message_counted_once() {
        let (_dir, repo) = open();
        let (account, sent) = (AccountId::new(), MailboxId::new());
        let rfc = "<sent@example.com>";
        let ada = vec![Address { name: None, email: EmailAddress::new("ada@example.com").unwrap() }];
        let placeholder = MessageId::new();
        let (server_copy, mut synced) = received(account, sent, rfc);
        if let DomainEvent::MessageReceived { envelope, .. } = &mut synced {
            envelope.to = ada.clone();
        }
        let events = [
            DomainEvent::MailboxDiscovered {
                mailbox_id: sent,
                account_id: account,
                path: MailboxPath::parse("Sent", '/').unwrap(),
                mailbox_type: MailboxType::Sent,
            },
            DomainEvent::MessageSent {
                message_id: placeholder,
                account_id: account,
                mailbox_id: sent,
                envelope: Envelope {
                    from: vec![],
                    to: ada,
                    cc: vec![],
                    bcc: vec![],
                    reply_to: vec![],
                    subject: "s".into(),
                    date: Utc::now(),
                },
                message_rfc_id: Some(rfc.into()),
                in_reply_to: None,
                references: vec![],
                body_text: Some("hi".into()),
                body_html: None,
                sent_at: Utc::now(),
            },
            // What SyncService emits when the server copy reaches Sent.
            synced,
            DomainEvent::MessageDeleted { message_id: placeholder, mailbox_id: sent, permanently: true },
        ];
        let events: Vec<_> = (1..).zip(events).map(|(pos, event)| at_position(pos, event)).collect();
        for event in &events {
            repo.project(event).await.unwrap();
        }
        assert_eq!(sent_state(&repo, sent).await, (vec![server_copy], 1, 1));

        // Boot catch-up replays what the live path already projected.
        repo.project_batch("read_model", events).await.unwrap();
        assert_eq!(sent_state(&repo, sent).await, (vec![server_copy], 1, 1));
    }

    #[tokio::test]
    async fn sent_placeholder_arriving_after_its_server_copy_is_not_shown() {
        let (_dir, repo) = open();
        let (account, sent) = (AccountId::new(), MailboxId::new());
        let rfc = "<gmail@example.com>";
        let ada = vec![Address { name: None, email: EmailAddress::new("ada@example.com").unwrap() }];
        let (server_copy, mut synced) = received(account, sent, rfc);
        if let DomainEvent::MessageReceived { envelope, .. } = &mut synced {
            envelope.to = ada.clone();
        }
        let placeholder = MessageId::new();
        let sent_event = |message_id| DomainEvent::MessageSent {
            message_id,
            account_id: account,
            mailbox_id: sent,
            envelope: Envelope {
                from: vec![],
                to: ada.clone(),
                cc: vec![],
                bcc: vec![],
                reply_to: vec![],
                subject: "s".into(),
                date: Utc::now(),
            },
            message_rfc_id: Some(rfc.into()),
            in_reply_to: None,
            references: vec![],
            body_text: Some("hi".into()),
            body_html: None,
            sent_at: Utc::now(),
        };
        let discovered = DomainEvent::MailboxDiscovered {
            mailbox_id: sent,
            account_id: account,
            path: MailboxPath::parse("Sent", '/').unwrap(),
            mailbox_type: MailboxType::Sent,
        };

        // Gmail files the copy during SMTP; a sync can store it first.
        let events = [discovered.clone(), synced.clone(), sent_event(placeholder)];
        for (pos, event) in (1..).zip(events) {
            repo.project(&at_position(pos, event)).await.unwrap();
        }
        assert_eq!(sent_state(&repo, sent).await, (vec![server_copy], 1, 1));

        // A sync that looked before the placeholder existed stores the copy after it.
        let (_dir, repo) = open();
        let events = [discovered, sent_event(placeholder), synced];
        for (pos, event) in (1..).zip(events) {
            repo.project(&at_position(pos, event)).await.unwrap();
        }
        assert_eq!(sent_state(&repo, sent).await, (vec![server_copy], 1, 1));
    }

    /// Messages listed in Sent, ada's sent_count, total conversation messages.
    async fn sent_state(repo: &SqliteRepository, sent: MailboxId) -> (Vec<MessageId>, i64, i64) {
        let listed = repo.list_messages(&sent, &Pagination { page: 1, page_size: 10 }).await.unwrap();
        let (sent_count, conversations) = repo
            .db
            .read(|c| {
                Ok((
                    c.query_row("SELECT sent_count FROM address_usage WHERE email = 'ada@example.com'", [], |r| {
                        r.get(0)
                    })
                    .map_err(repo_err)?,
                    c.query_row("SELECT SUM(message_count) FROM conversations", [], |r| r.get(0))
                        .map_err(repo_err)?,
                ))
            })
            .await
            .unwrap();
        (listed.items.iter().map(|m| m.id).collect(), sent_count, conversations)
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
