//! Threads as connected components of the Message-ID graph (own id,
//! In-Reply-To and every References entry are mutually linked), kept up to date
//! by union-find at write time so reads never walk the graph.

use std::collections::BTreeSet;

use rusqlite::{Connection, params};
use uuid::Uuid;

use mc_domain::{Conversation, DomainError, MailboxType};

use super::repo_err;
use super::repository::sender_display;
use super::repository::{exec, json, query_all};

#[cfg(test)]
mod tests;

/// `<id>` for any of `id`, `<id>`, ` <id> `; None when nothing is left. Ids
/// are stored with brackets everywhere, so the links match theirs.
fn normalize_id(raw: &str) -> Option<String> {
    let inner = raw
        .trim()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .trim();
    (!inner.is_empty()).then(|| format!("<{inner}>"))
}

/// Links every Message-ID the message mentions to one thread and returns it,
/// merging the threads they already belong to. Idempotent: a replay finds the
/// ids already linked together and changes nothing.
///
/// Ids of messages we never downloaded (a parent cited only in References) are
/// linked too: they are what joins two replies whose common parent we lack.
pub(super) fn assign_thread(
    tx: &Connection,
    account_id: &Uuid,
    own: Option<&str>,
    in_reply_to: Option<&str>,
    references: &[String],
) -> Result<Uuid, DomainError> {
    let ids: BTreeSet<String> = own
        .into_iter()
        .chain(in_reply_to)
        .chain(references.iter().map(String::as_str))
        .filter_map(normalize_id)
        .collect();
    if ids.is_empty() {
        return Ok(Uuid::now_v7());
    }
    let ids = json(&ids)?;
    // Smallest UUIDv7 = oldest thread wins, so the choice is the same on replay.
    let mut found = query_all(
        tx,
        "SELECT DISTINCT thread_id FROM thread_links \
         WHERE account_id = ?1 AND rfc_id IN (SELECT value FROM json_each(?2)) ORDER BY thread_id",
        params![account_id, ids],
        |r| r.get::<_, Uuid>(0),
    )?
    .into_iter();
    let winner = found.next().unwrap_or_else(Uuid::now_v7);
    for loser in found {
        for table in ["thread_links", "messages"] {
            exec(
                tx,
                &format!(
                    "UPDATE {table} SET thread_id = ?2 WHERE account_id = ?1 AND thread_id = ?3"
                ),
                params![account_id, winner, loser],
            )?;
        }
        exec(
            tx,
            "DELETE FROM conversations WHERE account_id = ?1 AND thread_id = ?2",
            params![account_id, loser],
        )?;
    }
    exec(
        tx,
        "INSERT INTO thread_links (account_id, rfc_id, thread_id) \
         SELECT ?1, value, ?3 FROM json_each(?2) WHERE true \
         ON CONFLICT(account_id, rfc_id) DO UPDATE SET thread_id = excluded.thread_id",
        params![account_id, ids, winner],
    )?;
    Ok(winner)
}

/// A message counts toward its conversation only while it sits in some mailbox
/// other than Trash or Junk (a mailbox we have no row for counts).
pub(super) const COUNTED: &str = "EXISTS (SELECT 1 FROM message_mailboxes mm \
     LEFT JOIN mailboxes mb ON mb.id = mm.mailbox_id \
     WHERE mm.message_id = m.id AND (mb.mailbox_type IS NULL OR mb.mailbox_type NOT IN (?3, ?4)))";

/// Recomputes a thread's row from its messages, so it converges after any
/// sequence of replays. Copies of one RFC message count once; a thread with
/// nothing countable has no row.
pub(super) fn refresh_conversation(
    tx: &Connection,
    account_id: &Uuid,
    thread_id: &Uuid,
) -> Result<(), DomainError> {
    let skipped = (json(&MailboxType::Trash)?, json(&MailboxType::Spam)?);
    let args = params![account_id, thread_id, skipped.0, skipped.1];
    let (count, unread): (u32, u32) = tx
        .query_row(
            &format!(
                "SELECT COUNT(DISTINCT COALESCE(rfc_message_id, hex(id))), \
                        COUNT(DISTINCT CASE WHEN seen = 0 THEN COALESCE(rfc_message_id, hex(id)) END) \
                 FROM messages m WHERE account_id = ?1 AND thread_id = ?2 AND {COUNTED}"
            ),
            args,
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(repo_err)?;
    if count == 0 {
        exec(
            tx,
            "DELETE FROM conversations WHERE account_id = ?1 AND thread_id = ?2",
            params![account_id, thread_id],
        )?;
        return Ok(());
    }
    let (sample, last_at, from_name, from_email): (Uuid, i64, Option<String>, String) = tx
        .query_row(
            &format!(
                "SELECT id, received_at, from_name, from_email FROM messages m \
                 WHERE account_id = ?1 AND thread_id = ?2 AND {COUNTED} \
                 ORDER BY received_at DESC, id DESC LIMIT 1"
            ),
            args,
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(repo_err)?;
    let subject: String = tx
        .query_row(
            &format!(
                "SELECT subject FROM messages m WHERE account_id = ?1 AND thread_id = ?2 AND {COUNTED} \
                 ORDER BY received_at, id LIMIT 1"
            ),
            args,
            |r| r.get(0),
        )
        .map_err(repo_err)?;
    exec(
        tx,
        "INSERT INTO conversations (account_id, thread_id, subject, message_count, unread_count, \
             last_message_at, latest_sender, sample_message_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
         ON CONFLICT(account_id, thread_id) DO UPDATE SET subject = excluded.subject, \
             message_count = excluded.message_count, unread_count = excluded.unread_count, \
             last_message_at = excluded.last_message_at, latest_sender = excluded.latest_sender, \
             sample_message_id = excluded.sample_message_id",
        params![
            account_id,
            thread_id,
            Conversation::normalize_subject(&subject),
            count,
            unread,
            last_at,
            sender_display(from_name, from_email),
            sample
        ],
    )
    .map(drop)
}

/// Threads with a message in `mailbox_id`: what to refresh when the mailbox
/// changes type or goes away.
pub(super) fn threads_in_mailbox(
    tx: &Connection,
    mailbox_id: &Uuid,
) -> Result<Vec<(Uuid, Uuid)>, DomainError> {
    query_all(
        tx,
        "SELECT DISTINCT m.account_id, m.thread_id FROM message_mailboxes mm \
         JOIN messages m ON m.id = mm.message_id WHERE mm.mailbox_id = ?1 AND m.thread_id IS NOT NULL",
        [mailbox_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
}
