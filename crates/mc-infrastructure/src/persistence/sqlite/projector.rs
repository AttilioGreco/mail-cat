//! Projection of domain events into the SQLite read model. Every arm runs in
//! the caller's transaction and must be replay-safe: boot and post-sync
//! catch-up re-project events the live path already applied (spec §10).

use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use uuid::Uuid;

use mc_domain::{
    AccountId, AccountStatus, ContactEmail, ContactId, DomainError, DomainEvent,
    Envelope, EventEnvelope, Flag, MailboxId, MailboxType, Membership, MessageId, RemoteId, ReadModelProjector,
};

use super::repo_err;
use super::threading::{assign_thread, refresh_conversation, threads_in_mailbox};
use super::repository::{
    Addresses, SqliteRepository, exec, json, nanos, query_all, query_one, save_checkpoint_in,
    sql_int, store_body,
};

#[async_trait]
impl ReadModelProjector for SqliteRepository {
    async fn project(&self, event: &EventEnvelope) -> Result<(), DomainError> {
        let event = event.clone();
        self.db
            .write(move |conn| {
                let tx = conn.transaction().map_err(repo_err)?;
                apply(&tx, &event)?;
                tx.commit().map_err(repo_err)
            })
            .await
    }

    async fn project_batch(
        &self,
        checkpoint: &str,
        events: Vec<EventEnvelope>,
    ) -> Result<bool, DomainError> {
        let Some(position) = events.last().map(|e| e.global_position) else {
            return Ok(true);
        };
        let checkpoint = checkpoint.to_owned();
        self.db
            .write(move |conn| {
                let tx = conn.transaction().map_err(repo_err)?;
                for event in &events {
                    apply(&tx, event)?;
                }
                save_checkpoint_in(&tx, &checkpoint, position)?;
                tx.commit().map_err(repo_err)
            })
            .await?;
        Ok(true)
    }
}

fn apply(tx: &Connection, event: &EventEnvelope) -> Result<(), DomainError> {
    match &event.payload {
        DomainEvent::AccountAdded { account_id, name, email, provider_type } => {
            exec(
                tx,
                "INSERT INTO accounts (id, name, email, provider_type, status) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(id) DO UPDATE SET name = excluded.name, email = excluded.email, \
                     provider_type = excluded.provider_type, status = excluded.status",
                params![
                    account_id.as_uuid(),
                    name,
                    email.as_str(),
                    json(provider_type)?,
                    json(&AccountStatus::Active)?
                ],
            )?;
        }
        DomainEvent::AccountUpdated { account_id, name, .. } => {
            if let Some(name) = name {
                exec(tx, "UPDATE accounts SET name = ?2 WHERE id = ?1", params![account_id.as_uuid(), name])?;
            }
        }
        DomainEvent::AccountRemoved { account_id } => {
            exec(tx, "DELETE FROM accounts WHERE id = ?1", [account_id.as_uuid()])?;
            exec(tx, "DELETE FROM sync_cursors WHERE account_id = ?1", [account_id.as_uuid()])?;
        }
        DomainEvent::AccountAuthExpired { account_id, error } => {
            set_account_status(tx, account_id, AccountStatus::NeedsReauth, Some(error), event.occurred_at)?;
        }
        DomainEvent::AccountCredentialsRejected { account_id, error } => {
            set_account_status(tx, account_id, AccountStatus::CredentialsRejected, Some(error), event.occurred_at)?;
        }
        DomainEvent::AccountReauthenticated { account_id } => {
            set_account_status(tx, account_id, AccountStatus::Active, None, event.occurred_at)?;
        }
        DomainEvent::AccountSyncStarted { .. }
        | DomainEvent::AccountSyncCompleted { .. }
        | DomainEvent::AccountSyncFailed { .. } => {}

        DomainEvent::MailboxDiscovered { mailbox_id, account_id, path, mailbox_type } => {
            // Counts are transient (never in the log), so a replay must keep them.
            exec(
                tx,
                "INSERT INTO mailboxes (id, account_id, path, delimiter, display_name, mailbox_type, flags) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, '[]') \
                 ON CONFLICT(id) DO UPDATE SET account_id = excluded.account_id, path = excluded.path, \
                     delimiter = excluded.delimiter, display_name = excluded.display_name, \
                     mailbox_type = excluded.mailbox_type",
                params![
                    mailbox_id.as_uuid(),
                    account_id.as_uuid(),
                    path.full_path(),
                    path.delimiter().to_string(),
                    path.name(),
                    json(mailbox_type)?
                ],
            )?;
        }
        DomainEvent::MailboxTypeChanged { mailbox_id, mailbox_type } => {
            exec(
                tx,
                "UPDATE mailboxes SET mailbox_type = ?2 WHERE id = ?1",
                params![mailbox_id.as_uuid(), json(mailbox_type)?],
            )?;
            refresh_threads(tx, &threads_in_mailbox(tx, mailbox_id.as_uuid())?)?;
        }
        DomainEvent::MailboxCountsUpdated { mailbox_id, total_messages, unread_messages } => {
            exec(
                tx,
                "UPDATE mailboxes SET total_messages = ?2, unread_messages = ?3 WHERE id = ?1",
                params![mailbox_id.as_uuid(), total_messages, unread_messages],
            )?;
        }
        DomainEvent::MailboxRenamed { mailbox_id, new_path, .. } => {
            exec(
                tx,
                "UPDATE mailboxes SET path = ?2, delimiter = ?3, display_name = ?4 WHERE id = ?1",
                params![
                    mailbox_id.as_uuid(),
                    new_path.full_path(),
                    new_path.delimiter().to_string(),
                    new_path.name()
                ],
            )?;
        }
        DomainEvent::MailboxDeleted { mailbox_id, .. } => {
            // Messages that were only here go with it.
            let only_here = query_all(
                tx,
                "SELECT message_id FROM message_mailboxes mm WHERE mailbox_id = ?1 AND NOT EXISTS \
                     (SELECT 1 FROM message_mailboxes o WHERE o.message_id = mm.message_id AND o.mailbox_id != ?1)",
                [mailbox_id.as_uuid()],
                |r| r.get::<_, Uuid>(0),
            )?;
            let threads = threads_in_mailbox(tx, mailbox_id.as_uuid())?;
            for id in only_here {
                delete_message(tx, &MessageId::from_uuid(id))?;
            }
            exec(tx, "DELETE FROM message_mailboxes WHERE mailbox_id = ?1", [mailbox_id.as_uuid()])?;
            refresh_threads(tx, &threads)?;
            exec(tx, "DELETE FROM mailboxes WHERE id = ?1", [mailbox_id.as_uuid()])?;
            exec(tx, "DELETE FROM sync_cursors WHERE collection = ?1", [mailbox_id.to_string()])?;
        }

        DomainEvent::MessageReceived {
            message_id,
            account_id,
            mailbox_id,
            remote_id,
            message_rfc_id,
            in_reply_to,
            references,
            envelope,
            flags,
            size_bytes,
            has_attachments,
            received_at,
            ..
        } => {
            let thread_id =
                assign_thread(tx, account_id.as_uuid(), message_rfc_id.as_deref(), in_reply_to.as_deref(), references)?;
            let inserted = insert_message(
                tx,
                &NewMessage {
                    id: message_id,
                    account_id,
                    rfc_message_id: message_rfc_id.as_deref(),
                    in_reply_to: in_reply_to.as_deref(),
                    references,
                    thread_id: Some(thread_id),
                    envelope,
                    flags,
                    size_bytes: *size_bytes,
                    has_attachments: *has_attachments,
                    received_at: *received_at,
                    body_state: "pending",
                },
            )?;
            if inserted {
                upsert_membership(tx, message_id, mailbox_id, Some(remote_id))?;
                // Symmetric with MessageDeleted's decrement, so replaying a
                // received+deleted pair nets zero.
                if !flags.contains(&Flag::Seen) {
                    adjust_unread(tx, mailbox_id.as_uuid(), 1)?;
                }
                refresh_conversation(tx, account_id.as_uuid(), &thread_id)?;
                note_synced_message(tx, account_id, mailbox_id, message_id, message_rfc_id.as_deref(), envelope, *received_at)?;
            }
        }
        DomainEvent::MessageBodyFetched { message_id, attachments } => {
            // A text-only fetch (backfill, a shared body) knows nothing of the
            // attachments: the envelope's flag stays.
            if attachments.is_empty() {
                exec(tx, "UPDATE messages SET body_state = 'stored' WHERE id = ?1", params![message_id.as_uuid()])?;
            } else {
                exec(
                    tx,
                    "UPDATE messages SET attachments = ?2, has_attachments = 1, body_state = 'stored' \
                     WHERE id = ?1",
                    params![message_id.as_uuid(), json(attachments)?],
                )?;
            }
        }
        DomainEvent::MessageFlagged { message_id, flags_added, flags_removed, .. } => {
            flag_message(tx, message_id, flags_added, flags_removed)?;
        }
        DomainEvent::MessageMailboxesChanged { message_id, added, removed } => {
            change_mailboxes(tx, message_id, added, removed)?;
        }
        DomainEvent::MessageDeleted { message_id, .. } => delete_message(tx, message_id)?,
        DomainEvent::MessageSent {
            message_id,
            account_id,
            mailbox_id,
            envelope,
            message_rfc_id,
            in_reply_to,
            references,
            body_text,
            body_html,
            sent_at,
        } => {
            let thread_id =
                assign_thread(tx, account_id.as_uuid(), message_rfc_id.as_deref(), in_reply_to.as_deref(), references)?;
            // The server copy can sync into Sent before this lands (Gmail files
            // it during SMTP): it already is the message, a placeholder would
            // only duplicate it.
            let inserted = !server_copy_exists(tx, account_id, mailbox_id, message_rfc_id.as_deref())?
                && insert_message(
                tx,
                &NewMessage {
                    id: message_id,
                    account_id,
                    rfc_message_id: message_rfc_id.as_deref(),
                    in_reply_to: in_reply_to.as_deref(),
                    references,
                    thread_id: Some(thread_id),
                    envelope,
                    flags: &[Flag::Seen],
                    size_bytes: 0,
                    has_attachments: false,
                    received_at: *sent_at,
                    body_state: "stored",
                },
            )?;
            if inserted {
                upsert_membership(tx, message_id, mailbox_id, None)?;
                store_body(tx, message_id, body_text.as_deref(), body_html.as_deref(), false, *sent_at)?;
                refresh_conversation(tx, account_id.as_uuid(), &thread_id)?;
            }
            note_sent_once(tx, account_id, message_id, message_rfc_id.as_deref(), envelope, *sent_at)?;
        }
        DomainEvent::DraftSaved { message_id, account_id, envelope, body_text, body_html } => {
            // Each save replaces the draft; the event's own time keeps replays deterministic.
            exec(tx, "DELETE FROM messages WHERE id = ?1", [message_id.as_uuid()])?;
            insert_message(
                tx,
                &NewMessage {
                    id: message_id,
                    account_id,
                    rfc_message_id: None,
                    in_reply_to: None,
                    references: &[],
                    thread_id: None,
                    envelope,
                    flags: &[Flag::Draft],
                    size_bytes: 0,
                    has_attachments: false,
                    received_at: event.occurred_at,
                    body_state: "stored",
                },
            )?;
            store_body(tx, message_id, body_text.as_deref(), body_html.as_deref(), false, event.occurred_at)?;
        }
        DomainEvent::DraftDiscarded { message_id } => {
            exec(tx, "DELETE FROM messages WHERE id = ?1", [message_id.as_uuid()])?;
        }

        DomainEvent::ContactAdded { contact_id, display_name, emails, source, account_scope, .. } => {
            exec(
                tx,
                "INSERT INTO contacts (id, display_name, primary_email, source, account_scope, emails) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT(id) DO UPDATE SET display_name = excluded.display_name, \
                     primary_email = excluded.primary_email, source = excluded.source, \
                     account_scope = excluded.account_scope, emails = excluded.emails",
                params![
                    contact_id.as_uuid(),
                    display_name,
                    primary_email(emails)?,
                    json(source)?,
                    account_scope.map(|a| *a.as_uuid()),
                    json(emails)?
                ],
            )?;
            index_contact_emails(tx, contact_id, emails)?;
        }
        DomainEvent::ContactUpdated { contact_id, display_name, emails, organization, .. } => {
            if let Some(name) = display_name {
                exec(
                    tx,
                    "UPDATE contacts SET display_name = ?2 WHERE id = ?1",
                    params![contact_id.as_uuid(), name],
                )?;
            }
            if let Some(emails) = emails {
                set_contact_emails(tx, contact_id, emails)?;
            }
            if let Some(organization) = organization {
                exec(
                    tx,
                    "UPDATE contacts SET organization = ?2 WHERE id = ?1",
                    params![contact_id.as_uuid(), organization],
                )?;
            }
        }
        DomainEvent::ContactRemoved { contact_id } => {
            exec(tx, "DELETE FROM contacts WHERE id = ?1", [contact_id.as_uuid()])?;
        }
        DomainEvent::ContactMerged { kept_id, discarded_id, merged_emails } => {
            set_contact_emails(tx, kept_id, merged_emails)?;
            exec(tx, "DELETE FROM contacts WHERE id = ?1", [discarded_id.as_uuid()])?;
        }
    }
    Ok(())
}

fn set_account_status(
    tx: &Connection,
    account_id: &AccountId,
    status: AccountStatus,
    error: Option<&String>,
    since: DateTime<Utc>,
) -> Result<(), DomainError> {
    exec(
        tx,
        "UPDATE accounts SET status = ?2, status_error = ?3, status_since = ?4 WHERE id = ?1",
        params![account_id.as_uuid(), json(&status)?, error, nanos(since)],
    )
    .map(drop)
}

struct NewMessage<'a> {
    id: &'a MessageId,
    account_id: &'a AccountId,
    rfc_message_id: Option<&'a str>,
    in_reply_to: Option<&'a str>,
    references: &'a [String],
    thread_id: Option<Uuid>,
    envelope: &'a Envelope,
    flags: &'a [Flag],
    size_bytes: u64,
    has_attachments: bool,
    received_at: DateTime<Utc>,
    body_state: &'static str,
}

/// Inserts unless the id exists and reports whether it did. Message ids are
/// minted per receipt/send, so an existing row means a replay: it already
/// reflects every later event and must not be reset.
fn insert_message(tx: &Connection, m: &NewMessage) -> Result<bool, DomainError> {
    let sender = m.envelope.from.first();
    let inserted = exec(
        tx,
        "INSERT INTO messages (id, account_id, rfc_message_id, \
             in_reply_to, refs, thread_id, subject, from_name, from_email, addresses, flags, seen, \
             size_bytes, has_attachments, attachments, sent_at, received_at, internal_date, body_state) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, '[]', \
             ?15, ?16, ?16, ?17) \
         ON CONFLICT(id) DO NOTHING",
        params![
            m.id.as_uuid(),
            m.account_id.as_uuid(),
            m.rfc_message_id,
            m.in_reply_to,
            json(m.references)?,
            m.thread_id,
            m.envelope.subject,
            sender.and_then(|a| a.name.as_deref()),
            sender.map_or("", |a| a.email.as_str()),
            json(&Addresses::of(m.envelope))?,
            json(m.flags)?,
            m.flags.contains(&Flag::Seen),
            sql_int(m.size_bytes),
            m.has_attachments,
            nanos(m.envelope.date),
            nanos(m.received_at),
            m.body_state
        ],
    )?;
    Ok(inserted == 1)
}

/// `received_at` is copied from the message (it never changes) so a mailbox
/// lists by date from `message_mailboxes_date` alone. An existing membership
/// only takes the new remote id.
fn upsert_membership(
    tx: &Connection,
    id: &MessageId,
    mailbox_id: &MailboxId,
    remote_id: Option<&RemoteId>,
) -> Result<(), DomainError> {
    exec(
        tx,
        "INSERT INTO message_mailboxes (message_id, mailbox_id, remote_id, received_at) \
         SELECT id, ?2, ?3, received_at FROM messages WHERE id = ?1 \
         ON CONFLICT(message_id, mailbox_id) DO UPDATE SET remote_id = excluded.remote_id",
        params![id.as_uuid(), mailbox_id.as_uuid(), remote_id.map(RemoteId::as_str)],
    )
    .map(drop)
}

/// Received-side usage is a MAX, so replays are harmless; sent-side counting
/// goes through `note_sent_once`.
fn note_synced_message(
    tx: &Connection,
    account_id: &AccountId,
    mailbox_id: &MailboxId,
    message_id: &MessageId,
    rfc_message_id: Option<&str>,
    envelope: &Envelope,
    at: DateTime<Utc>,
) -> Result<(), DomainError> {
    let mailbox_type = query_one(
        tx,
        "SELECT mailbox_type FROM mailboxes WHERE id = ?1",
        [mailbox_id.as_uuid()],
        |r| r.get::<_, String>(0),
    )?;
    match mailbox_type.as_deref() {
        Some(t) if t == json(&MailboxType::Sent)? => {
            drop_sent_placeholders(tx, account_id, mailbox_id, rfc_message_id)?;
            note_sent_once(tx, account_id, message_id, rfc_message_id, envelope, at)
        }
        Some(t) if t == json(&MailboxType::Drafts)? => Ok(()),
        _ => envelope.from.iter().try_for_each(|a| {
            exec(
                tx,
                "INSERT INTO address_usage (email, last_received_at) VALUES (?1, ?2) \
                 ON CONFLICT(email) DO UPDATE SET \
                     last_received_at = MAX(IFNULL(last_received_at, ?2), ?2)",
                params![a.email.as_str(), nanos(at)],
            )
            .map(drop)
        }),
    }
}

fn server_copy_exists(
    tx: &Connection,
    account_id: &AccountId,
    mailbox_id: &MailboxId,
    rfc_message_id: Option<&str>,
) -> Result<bool, DomainError> {
    let Some(rfc_message_id) = rfc_message_id else {
        return Ok(false);
    };
    Ok(query_one(
        tx,
        "SELECT 1 FROM messages m JOIN message_mailboxes mm ON mm.message_id = m.id \
         WHERE m.account_id = ?1 AND m.rfc_message_id = ?2 \
             AND mm.mailbox_id = ?3 AND mm.remote_id IS NOT NULL LIMIT 1",
        params![account_id.as_uuid(), rfc_message_id, mailbox_id.as_uuid()],
        |_| Ok(()),
    )?
    .is_some())
}

/// The server copy of a message we sent replaces its `MessageSent`
/// placeholder. SyncService supersedes it with an event, but a sync that
/// looked before the placeholder existed can't; this closes that window.
/// Projection-only, yet replay-safe: it reads only rows earlier events
/// projected, so a replay deletes exactly the same ones.
///
/// Only in `sent`: elsewhere a UID-less copy of the same Message-ID is a
/// received message waiting for its UID, not a placeholder.
fn drop_sent_placeholders(
    tx: &Connection,
    account_id: &AccountId,
    sent: &MailboxId,
    rfc_message_id: Option<&str>,
) -> Result<(), DomainError> {
    let Some(rfc_message_id) = rfc_message_id else {
        return Ok(());
    };
    let placeholders = query_all(
        tx,
        "SELECT m.id FROM messages m JOIN message_mailboxes mm ON mm.message_id = m.id \
         WHERE m.account_id = ?1 AND m.rfc_message_id = ?2 AND mm.mailbox_id = ?3 AND mm.remote_id IS NULL",
        params![account_id.as_uuid(), rfc_message_id, sent.as_uuid()],
        |r| r.get::<_, Uuid>(0),
    )?;
    placeholders.into_iter().try_for_each(|id| delete_message(tx, &MessageId::from_uuid(id)))
}

/// Counts a sent message's recipients once per Message-ID, however many
/// times its placeholder or server copy is (re)projected.
fn note_sent_once(
    tx: &Connection,
    account_id: &AccountId,
    message_id: &MessageId,
    rfc_message_id: Option<&str>,
    envelope: &Envelope,
    at: DateTime<Utc>,
) -> Result<(), DomainError> {
    let first = exec(
        tx,
        "INSERT OR IGNORE INTO sent_counted (account_id, message_key) VALUES (?1, COALESCE(?2, hex(?3)))",
        params![account_id.as_uuid(), rfc_message_id, message_id.as_uuid()],
    )?;
    if first == 0 {
        return Ok(());
    }
    let recipients: BTreeSet<&str> =
        envelope.to.iter().chain(&envelope.cc).chain(&envelope.bcc).map(|a| a.email.as_str()).collect();
    for email in recipients {
        exec(
            tx,
            "INSERT INTO address_usage (email, sent_count, last_sent_at) VALUES (?1, 1, ?2) \
             ON CONFLICT(email) DO UPDATE SET sent_count = sent_count + 1, \
                 last_sent_at = MAX(IFNULL(last_sent_at, ?2), ?2)",
            params![email, nanos(at)],
        )?;
    }
    Ok(())
}

fn adjust_unread(tx: &Connection, mailbox_id: &Uuid, delta: i64) -> Result<(), DomainError> {
    exec(
        tx,
        "UPDATE mailboxes SET unread_messages = MAX(unread_messages + ?2, 0) WHERE id = ?1",
        params![mailbox_id, delta],
    )
    .map(drop)
}

fn flag_message(
    tx: &Connection,
    id: &MessageId,
    added: &[Flag],
    removed: &[Flag],
) -> Result<(), DomainError> {
    let Some((flags, account_id, thread_id)) = query_one(
        tx,
        "SELECT flags, account_id, thread_id FROM messages WHERE id = ?1",
        [id.as_uuid()],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, Uuid>(1)?, r.get::<_, Option<Uuid>>(2)?)),
    )?
    else {
        return Ok(());
    };
    let mut flags: Vec<Flag> = serde_json::from_str(&flags)?;
    let was_seen = flags.contains(&Flag::Seen);
    flags.retain(|f| !removed.contains(f));
    for flag in added {
        if !flags.contains(flag) {
            flags.push(flag.clone());
        }
    }
    let seen = flags.contains(&Flag::Seen);
    exec(
        tx,
        "UPDATE messages SET flags = ?2, seen = ?3 WHERE id = ?1",
        params![id.as_uuid(), json(&flags)?, seen],
    )?;
    // The delta comes from the row's before/after state, so re-applying a
    // flag change is a no-op.
    if seen != was_seen {
        exec(
            tx,
            "UPDATE mailboxes SET unread_messages = MAX(unread_messages + ?2, 0) \
             WHERE id IN (SELECT mailbox_id FROM message_mailboxes WHERE message_id = ?1)",
            params![id.as_uuid(), if seen { -1 } else { 1 }],
        )?;
        if let Some(thread_id) = thread_id {
            refresh_conversation(tx, &account_id, &thread_id)?;
        }
    }
    Ok(())
}

/// Replay-safe: each membership is counted only when this call is what adds
/// or removes its row.
fn change_mailboxes(
    tx: &Connection,
    id: &MessageId,
    added: &[Membership],
    removed: &[MailboxId],
) -> Result<(), DomainError> {
    let Some(seen) = query_one(tx, "SELECT seen FROM messages WHERE id = ?1", [id.as_uuid()], |r| {
        r.get::<_, bool>(0)
    })?
    else {
        return Ok(());
    };
    for mailbox_id in removed {
        let gone = exec(
            tx,
            "DELETE FROM message_mailboxes WHERE message_id = ?1 AND mailbox_id = ?2",
            params![id.as_uuid(), mailbox_id.as_uuid()],
        )?;
        if gone == 1 && !seen {
            adjust_unread(tx, mailbox_id.as_uuid(), -1)?;
        }
    }
    for Membership { mailbox_id, remote_id } in added {
        let existed = query_one(
            tx,
            "SELECT 1 FROM message_mailboxes WHERE message_id = ?1 AND mailbox_id = ?2",
            params![id.as_uuid(), mailbox_id.as_uuid()],
            |_| Ok(()),
        )?
        .is_some();
        upsert_membership(tx, id, mailbox_id, remote_id.as_ref())?;
        if !existed && !seen {
            adjust_unread(tx, mailbox_id.as_uuid(), 1)?;
        }
    }
    // The domain never removes the last membership here, but a sync acting on a
    // stale summary can: a message in no mailbox is gone, not an orphan row.
    let orphan = query_one(
        tx,
        "SELECT 1 FROM message_mailboxes WHERE message_id = ?1 LIMIT 1",
        [id.as_uuid()],
        |_| Ok(()),
    )?
    .is_none();
    if orphan {
        return delete_message(tx, id);
    }
    // Moving to or from Trash changes what counts.
    if let Some((account, thread)) = thread_of(tx, id)? {
        refresh_conversation(tx, &account, &thread)?;
    }
    Ok(())
}

fn thread_of(tx: &Connection, id: &MessageId) -> Result<Option<(Uuid, Uuid)>, DomainError> {
    query_one(
        tx,
        "SELECT account_id, thread_id FROM messages WHERE id = ?1 AND thread_id IS NOT NULL",
        [id.as_uuid()],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
}

fn refresh_threads(tx: &Connection, threads: &[(Uuid, Uuid)]) -> Result<(), DomainError> {
    threads.iter().try_for_each(|(account, thread)| refresh_conversation(tx, account, thread))
}

fn delete_message(tx: &Connection, id: &MessageId) -> Result<(), DomainError> {
    let Some((account_id, thread_id, seen)) = query_one(
        tx,
        "SELECT account_id, thread_id, seen FROM messages WHERE id = ?1",
        [id.as_uuid()],
        |r| Ok((r.get::<_, Uuid>(0)?, r.get::<_, Option<Uuid>>(1)?, r.get::<_, bool>(2)?)),
    )?
    else {
        return Ok(());
    };
    if !seen {
        exec(
            tx,
            "UPDATE mailboxes SET unread_messages = MAX(unread_messages - 1, 0) \
             WHERE id IN (SELECT mailbox_id FROM message_mailboxes WHERE message_id = ?1)",
            [id.as_uuid()],
        )?;
    }
    // Memberships and the body row go with it (ON DELETE CASCADE).
    exec(tx, "DELETE FROM messages WHERE id = ?1", [id.as_uuid()])?;
    if let Some(thread_id) = thread_id {
        refresh_conversation(tx, &account_id, &thread_id)?;
    }
    Ok(())
}

fn primary_email(emails: &[ContactEmail]) -> Result<&str, DomainError> {
    emails
        .iter()
        .find(|e| e.is_primary)
        .or_else(|| emails.first())
        .map(|e| e.address.as_str())
        .ok_or_else(|| {
            DomainError::Validation("a contact must have at least one email address".into())
        })
}

/// Skipped for unknown contacts, whose index rows the foreign key would reject.
fn set_contact_emails(
    tx: &Connection,
    contact_id: &ContactId,
    emails: &[ContactEmail],
) -> Result<(), DomainError> {
    let updated = exec(
        tx,
        "UPDATE contacts SET emails = ?2, primary_email = ?3 WHERE id = ?1",
        params![contact_id.as_uuid(), json(emails)?, primary_email(emails)?],
    )?;
    if updated == 1 {
        index_contact_emails(tx, contact_id, emails)?;
    }
    Ok(())
}

fn index_contact_emails(
    tx: &Connection,
    contact_id: &ContactId,
    emails: &[ContactEmail],
) -> Result<(), DomainError> {
    exec(tx, "DELETE FROM contact_emails WHERE contact_id = ?1", [contact_id.as_uuid()])?;
    for email in emails {
        exec(
            tx,
            "INSERT OR IGNORE INTO contact_emails (email, contact_id) VALUES (?1, ?2)",
            params![email.address.as_str(), contact_id.as_uuid()],
        )?;
    }
    Ok(())
}
