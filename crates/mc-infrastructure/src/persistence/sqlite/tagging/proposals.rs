//! Discovery in SQLite: the points to cluster and the proposals made of them.
//! Members are packed 16 bytes each: a cluster is hundreds of ids.

use chrono::DateTime;
use rusqlite::types::Type;
use rusqlite::{Connection, Row, params, params_from_iter};
use uuid::Uuid;

use mc_domain::{
    DiscoveryPoint, DomainError, MessageId, Proposal, ProposalId, ProposalStatus, Scope,
};

use super::sources::{LABELLED, UNREADABLE, placeholders};
use super::vectors::decode;
use crate::persistence::sqlite::repository::{
    conversion_err, exec, json, json_col, nanos, query_all, query_one,
};

fn pack(ids: &[MessageId]) -> Vec<u8> {
    ids.iter()
        .flat_map(|id| id.as_uuid().into_bytes())
        .collect()
}

fn unpack(blob: &[u8]) -> Vec<MessageId> {
    blob.as_chunks::<16>()
        .0
        .iter()
        .map(|b| MessageId::from_uuid(Uuid::from_bytes(*b)))
        .collect()
}

const COLUMNS: &str =
    "id, scope, name, keywords, members, inbox, samples, status, created_at FROM tag_proposals";

fn proposal_of_row(row: &Row) -> rusqlite::Result<Proposal> {
    let scope: String = row.get(1)?;
    let status: String = row.get(7)?;
    Ok(Proposal {
        id: ProposalId::from_uuid(row.get(0)?),
        scope: Scope::from_key(&scope).ok_or_else(|| conversion_err(1, Type::Text, scope))?,
        name: row.get(2)?,
        keywords: json_col(row, 3)?,
        members: unpack(&row.get::<_, Vec<u8>>(4)?),
        inbox: row.get(5)?,
        samples: json_col(row, 6)?,
        status: status
            .parse()
            .map_err(|e: String| conversion_err(7, Type::Text, e))?,
        created_at: DateTime::from_timestamp_nanos(row.get(8)?),
    })
}

pub(super) fn all(conn: &Connection, status: ProposalStatus) -> Result<Vec<Proposal>, DomainError> {
    query_all(
        conn,
        &format!("SELECT {COLUMNS} WHERE status = ?1 ORDER BY inbox DESC, created_at, id"),
        [status.as_str()],
        proposal_of_row,
    )
}

pub(super) fn one(conn: &Connection, id: ProposalId) -> Result<Option<Proposal>, DomainError> {
    query_one(
        conn,
        &format!("SELECT {COLUMNS} WHERE id = ?1"),
        [id.as_uuid()],
        proposal_of_row,
    )
}

pub(super) fn put(conn: &Connection, p: &Proposal) -> Result<(), DomainError> {
    exec(
        conn,
        "INSERT OR REPLACE INTO tag_proposals \
         (id, scope, name, keywords, members, inbox, samples, status, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            p.id.as_uuid(),
            p.scope.key(),
            p.name,
            json(&p.keywords)?,
            pack(&p.members),
            p.inbox,
            json(&p.samples)?,
            p.status.as_str(),
            nanos(p.created_at),
        ],
    )
    .map(drop)
}

pub(super) fn delete(conn: &Connection, id: ProposalId) -> Result<(), DomainError> {
    exec(
        conn,
        "DELETE FROM tag_proposals WHERE id = ?1",
        [id.as_uuid()],
    )
    .map(drop)
}

/// Inbox mail first, as it is what the user wants cleaned; then the newest.
pub(super) fn points(
    conn: &Connection,
    accounts: &[Uuid],
    model: &str,
    limit: u32,
) -> Result<Vec<DiscoveryPoint>, DomainError> {
    let sql = format!(
        "SELECT m.id, v.vec, EXISTS (SELECT 1 FROM message_mailboxes x \
            JOIN mailboxes mb ON mb.id = x.mailbox_id \
            WHERE x.message_id = m.id AND mb.mailbox_type = '\"Inbox\"') AS in_inbox \
         FROM messages m JOIN accounts a ON a.id = m.account_id \
         JOIN tag_vectors v ON v.message_id = m.id AND v.model = ?1 \
         WHERE m.account_id IN ({}) AND {UNREADABLE} AND NOT ({LABELLED}) \
         AND (a.mailbox_model != '\"Folders\"' OR COALESCE(a.supports_tags, 0)) \
         ORDER BY in_inbox DESC, m.id DESC LIMIT {limit}",
        placeholders(2, accounts.len())
    );
    let args = std::iter::once(&model as &dyn rusqlite::ToSql)
        .chain(accounts.iter().map(|a| a as &dyn rusqlite::ToSql));
    let rows = query_all(conn, &sql, params_from_iter(args), |r| {
        Ok((
            MessageId::from_uuid(r.get(0)?),
            r.get::<_, Vec<u8>>(1)?,
            r.get::<_, bool>(2)?,
        ))
    })?;
    Ok(rows
        .into_iter()
        .filter_map(|(id, blob, in_inbox)| {
            Some(DiscoveryPoint {
                id,
                in_inbox,
                vector: decode(&blob)?,
            })
        })
        .collect())
}
