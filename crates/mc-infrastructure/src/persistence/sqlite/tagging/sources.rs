//! Messages as the classifier reads them. A labels account's labels are its
//! custom mailboxes; a folders account's are the keywords (tags) on the
//! message, `$`-prefixed ones being protocol markers rather than the user's.
//! Underscores read as spaces, since a keyword cannot hold one.

use chrono::DateTime;
use rusqlite::types::Type;
use rusqlite::{Connection, Row, params_from_iter};
use uuid::Uuid;

use mc_domain::model::message::strip_html;
use mc_domain::{
    AccountId, DomainError, ListHeaders, MailboxId, MessageId, SourceFilter, TagSource,
};

use crate::persistence::sqlite::repository::{body_col, conversion_err, json_col, query_all};

/// Parameters per statement stay far below SQLite's limit.
pub(super) fn placeholders(from: usize, n: usize) -> String {
    (from..from + n)
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// The keywords of message `m`, one row each, as `k.value`.
macro_rules! keywords {
    () => {
        "json_each(m.flags) f, \
         json_each(CASE WHEN f.type = 'object' THEN f.value ELSE '{}' END) k \
         WHERE k.key = 'Custom' AND k.value NOT LIKE '$%'"
    };
}

const SOURCE_COLUMNS: &str = concat!(
    "SELECT m.id, m.account_id, m.from_email, m.from_name, m.subject, \
     m.list_headers, b.text_zstd, b.html_zstd, \
     CASE WHEN a.mailbox_model = '\"Folders\"' THEN \
        (SELECT json_group_array(lower(replace(k.value, '_', ' '))) FROM ",
    keywords!(),
    ") ELSE (SELECT json_group_array(lower(mb.display_path)) FROM message_mailboxes x \
        JOIN mailboxes mb ON mb.id = x.mailbox_id \
        WHERE x.message_id = m.id AND mb.mailbox_type = '\"Custom\"') END, \
     EXISTS (SELECT 1 FROM message_mailboxes x JOIN mailboxes mb ON mb.id = x.mailbox_id \
        WHERE x.message_id = m.id AND mb.mailbox_type = '\"Inbox\"'), \
     m.received_at, a.mailbox_model = '\"Folders\"', \
     (SELECT x.mailbox_id FROM message_mailboxes x WHERE x.message_id = m.id LIMIT 1), \
     COALESCE(a.supports_tags, 0) \
     FROM messages m \
     JOIN accounts a ON a.id = m.account_id \
     LEFT JOIN message_bodies b ON b.message_id = m.id"
);

const LABELLED: &str = concat!(
    "CASE WHEN a.mailbox_model = '\"Folders\"' THEN EXISTS (SELECT 1 FROM ",
    keywords!(),
    ") ELSE EXISTS (SELECT 1 FROM message_mailboxes x \
     JOIN mailboxes mb ON mb.id = x.mailbox_id \
     WHERE x.message_id = m.id AND mb.mailbox_type = '\"Custom\"') END"
);

const UNREADABLE: &str = "NOT EXISTS (SELECT 1 FROM message_mailboxes x \
     JOIN mailboxes mb ON mb.id = x.mailbox_id WHERE x.message_id = m.id \
     AND mb.mailbox_type IN ('\"Trash\"', '\"Spam\"', '\"Drafts\"'))";

fn source_from_row(row: &Row) -> rusqlite::Result<TagSource> {
    let list_headers = row
        .get::<_, Option<String>>(5)?
        .map(|j| serde_json::from_str::<ListHeaders>(&j))
        .transpose()
        .map_err(|e| conversion_err(5, Type::Text, e))?
        .unwrap_or_default();
    let body = match body_col(row, 6)? {
        Some(text) => Some(text),
        None => body_col(row, 7)?.map(|html| strip_html(&html)),
    };
    Ok(TagSource {
        id: MessageId::from_uuid(row.get(0)?),
        account_id: AccountId::from_uuid(row.get(1)?),
        from_email: row.get(2)?,
        from_name: row.get(3)?,
        subject: row.get(4)?,
        list_headers,
        body,
        labels: json_col(row, 8)?,
        in_inbox: row.get(9)?,
        received_at: DateTime::from_timestamp_nanos(row.get(10)?),
        keywords: row.get(11)?,
        folder: row.get::<_, Option<Uuid>>(12)?.map(MailboxId::from_uuid),
        tags: row.get(13)?,
    })
}

pub(super) fn by_ids(conn: &Connection, ids: &[Uuid]) -> Result<Vec<TagSource>, DomainError> {
    let sql = format!(
        "{SOURCE_COLUMNS} WHERE m.id IN ({}) AND {UNREADABLE}",
        placeholders(1, ids.len())
    );
    query_all(conn, &sql, params_from_iter(ids.iter()), source_from_row)
}

/// Keyset page: ids are time-ordered, so `cursor` is a position, not an offset.
pub(super) fn page(
    conn: &Connection,
    accounts: &[Uuid],
    filter: SourceFilter,
    cursor: Uuid,
    newest_first: bool,
    limit: u32,
) -> Result<Vec<TagSource>, DomainError> {
    let (past, order) = if newest_first {
        ("<", "DESC")
    } else {
        (">", "")
    };
    let kind = match filter {
        SourceFilter::Labelled => format!("AND {LABELLED}"),
        SourceFilter::Unlabelled => format!("AND NOT ({LABELLED})"),
        SourceFilter::Any => String::new(),
    };
    let sql = format!(
        "{SOURCE_COLUMNS} WHERE m.account_id IN ({}) AND m.id {past} ?1 AND {UNREADABLE} {kind} \
         ORDER BY m.id {order} LIMIT {limit}",
        placeholders(2, accounts.len())
    );
    let args = std::iter::once(&cursor).chain(accounts.iter());
    query_all(conn, &sql, params_from_iter(args), source_from_row)
}
