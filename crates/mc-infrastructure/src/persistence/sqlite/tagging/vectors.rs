//! Embeddings: stored as f32 little-endian BLOBs next to the model that made
//! them, so a model change leaves the old vectors unseen rather than wrong.

use std::collections::HashMap;

use rusqlite::{Connection, params, params_from_iter};
use uuid::Uuid;

use mc_domain::{BackgroundSpeed, DomainError, LabelledVector, MessageId, Scope};

use super::sources::placeholders;
use crate::persistence::sqlite::repository::{exec, json_col, query_all, query_one};

const CHUNK: usize = 500;

pub(super) fn encode(vector: &[f32]) -> Vec<u8> {
    vector.iter().flat_map(|x| x.to_le_bytes()).collect()
}

/// `None` for a blob that is not a whole number of floats.
pub(super) fn decode(blob: &[u8]) -> Option<Vec<f32>> {
    let (floats, rest) = blob.as_chunks::<4>();
    rest.is_empty()
        .then(|| floats.iter().map(|b| f32::from_le_bytes(*b)).collect())
}

pub(super) fn enabled(conn: &Connection) -> Result<bool, DomainError> {
    Ok(query_one(
        conn,
        "SELECT value FROM tag_prefs WHERE key = 'embeddings'",
        [],
        |r| r.get::<_, String>(0),
    )?
    .is_some_and(|v| v == "on"))
}

pub(super) fn set_enabled(conn: &Connection, on: bool) -> Result<(), DomainError> {
    exec(
        conn,
        "INSERT INTO tag_prefs (key, value) VALUES ('embeddings', ?1) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [if on { "on" } else { "off" }],
    )
    .map(drop)
}

pub(super) fn speed(conn: &Connection) -> Result<BackgroundSpeed, DomainError> {
    Ok(query_one(
        conn,
        "SELECT value FROM tag_prefs WHERE key = 'speed'",
        [],
        |r| r.get::<_, String>(0),
    )?
    .and_then(|v| v.parse().ok())
    .unwrap_or_default())
}

pub(super) fn set_speed(conn: &Connection, speed: BackgroundSpeed) -> Result<(), DomainError> {
    exec(
        conn,
        "INSERT INTO tag_prefs (key, value) VALUES ('speed', ?1) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [speed.as_str()],
    )
    .map(drop)
}

pub(super) fn put(
    conn: &mut Connection,
    model: &str,
    vectors: &[(MessageId, Vec<f32>)],
) -> Result<(), DomainError> {
    let tx = conn
        .transaction()
        .map_err(crate::persistence::sqlite::repo_err)?;
    for (id, vector) in vectors {
        // A message deleted meanwhile has no row to hang the vector on.
        exec(
            &tx,
            "INSERT OR REPLACE INTO tag_vectors (message_id, model, vec) \
             SELECT id, ?2, ?3 FROM messages WHERE id = ?1",
            params![id.as_uuid(), model, encode(vector)],
        )?;
    }
    tx.commit().map_err(crate::persistence::sqlite::repo_err)
}

pub(super) fn of(
    conn: &Connection,
    model: &str,
    ids: &[Uuid],
) -> Result<HashMap<MessageId, Vec<f32>>, DomainError> {
    let mut found = HashMap::with_capacity(ids.len());
    for chunk in ids.chunks(CHUNK) {
        let sql = format!(
            "SELECT message_id, vec FROM tag_vectors WHERE model = ?1 AND message_id IN ({})",
            placeholders(2, chunk.len())
        );
        let args = std::iter::once(&model as &dyn rusqlite::ToSql)
            .chain(chunk.iter().map(|i| i as &dyn rusqlite::ToSql));
        let rows = query_all(conn, &sql, params_from_iter(args), |r| {
            Ok((MessageId::from_uuid(r.get(0)?), r.get::<_, Vec<u8>>(1)?))
        })?;
        found.extend(
            rows.into_iter()
                .filter_map(|(id, blob)| Some((id, decode(&blob)?))),
        );
    }
    Ok(found)
}

/// What the scope's model was taught, with each message's vector. Rows of
/// weight 0 are rejections kept through a reset and still say "not this".
pub(super) fn scope(
    conn: &Connection,
    scope: Scope,
    model: &str,
) -> Result<Vec<LabelledVector>, DomainError> {
    let rows = query_all(
        conn,
        "SELECT e.message_id, v.vec, e.positives, e.negatives \
         FROM tag_examples e JOIN tag_vectors v ON v.message_id = e.message_id \
         WHERE e.scope = ?1 AND v.model = ?2",
        params![scope.key(), model],
        |r| {
            Ok(LabelledVector {
                message_id: MessageId::from_uuid(r.get(0)?),
                vector: decode(&r.get::<_, Vec<u8>>(1)?).unwrap_or_default(),
                positives: json_col(r, 2)?,
                negatives: json_col(r, 3)?,
            })
        },
    )?;
    Ok(rows.into_iter().filter(|v| !v.vector.is_empty()).collect())
}
