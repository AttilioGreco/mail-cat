//! Projection of domain events into the SQLite read model. Every arm runs in
//! the caller's transaction and must be replay-safe: boot and post-sync
//! catch-up re-project events the live path already applied (spec §10).

use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use uuid::Uuid;

use mc_domain::{
    AccountId, AccountStatus, ContactEmail, ContactId, Conversation, DomainError, DomainEvent,
    Envelope, EventEnvelope, Flag, MailboxId, MailboxType, MessageId, ReadModelProjector,
};

use super::repo_err;
use super::repository::{
    Addresses, SqliteRepository, exec, json, nanos, query_one, save_checkpoint_in,
    sender_display, sql_int, store_body,
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
        }
        DomainEvent::AccountAuthExpired { account_id, .. } => {
            set_account_status(tx, account_id, AccountStatus::NeedsReauth)?;
        }
        DomainEvent::AccountReauthenticated { account_id } => {
            set_account_status(tx, account_id, AccountStatus::Active)?;
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
            exec(tx, "DELETE FROM mailboxes WHERE id = ?1", [mailbox_id.as_uuid()])?;
        }

        DomainEvent::MessageReceived {
            message_id,
            account_id,
            mailbox_id,
            provider_uid,
            message_rfc_id,
            in_reply_to,
            references,
            envelope,
            flags,
            size_bytes,
            has_attachments,
            received_at,
        } => {
            let thread_id = Conversation::resolve_thread_id(
                message_rfc_id.as_deref(),
                in_reply_to.as_deref(),
                references,
                &message_id.to_string(),
            );
            let inserted = insert_message(
                tx,
                &NewMessage {
                    id: message_id,
                    account_id,
                    mailbox_id: Some(mailbox_id),
                    provider_uid: Some(provider_uid.value()),
                    rfc_message_id: message_rfc_id.as_deref(),
                    in_reply_to: in_reply_to.as_deref(),
                    references,
                    thread_id: Some(thread_id.value()),
                    envelope,
                    flags,
                    size_bytes: *size_bytes,
                    has_attachments: *has_attachments,
                    received_at: *received_at,
                    body_state: "pending",
                },
            )?;
            if inserted {
                // Symmetric with MessageDeleted's decrement, so replaying a
                // received+deleted pair nets zero.
                if !flags.contains(&Flag::Seen) {
                    adjust_unread(tx, mailbox_id.as_uuid(), 1)?;
                }
                refresh_conversation(tx, account_id.as_uuid(), thread_id.value())?;
                note_synced_message(tx, mailbox_id, envelope, *received_at)?;
            }
        }
        DomainEvent::MessageBodyFetched { message_id, attachments } => {
            exec(
                tx,
                "UPDATE messages SET attachments = ?2, has_attachments = ?3, body_state = 'stored' \
                 WHERE id = ?1",
                params![message_id.as_uuid(), json(attachments)?, !attachments.is_empty()],
            )?;
        }
        DomainEvent::MessageFlagged { message_id, flags_added, flags_removed, .. } => {
            flag_message(tx, message_id, flags_added, flags_removed)?;
        }
        DomainEvent::MessageMoved { message_id, to_mailbox_id, .. } => {
            move_message(tx, message_id, to_mailbox_id)?;
        }
        DomainEvent::MessageDeleted { message_id, .. } => delete_message(tx, message_id)?,
        DomainEvent::MessageSent {
            message_id,
            account_id,
            mailbox_id,
            envelope,
            in_reply_to,
            references,
            body_text,
            body_html,
            sent_at,
        } => {
            let thread_id = Conversation::resolve_thread_id(
                None,
                in_reply_to.as_deref(),
                references,
                &message_id.to_string(),
            );
            let inserted = insert_message(
                tx,
                &NewMessage {
                    id: message_id,
                    account_id,
                    mailbox_id: Some(mailbox_id),
                    provider_uid: None,
                    rfc_message_id: None,
                    in_reply_to: in_reply_to.as_deref(),
                    references,
                    thread_id: Some(thread_id.value()),
                    envelope,
                    flags: &[Flag::Seen],
                    size_bytes: 0,
                    has_attachments: false,
                    received_at: *sent_at,
                    body_state: "stored",
                },
            )?;
            if inserted {
                store_body(tx, message_id, body_text.as_deref(), body_html.as_deref(), *sent_at)?;
                refresh_conversation(tx, account_id.as_uuid(), thread_id.value())?;
                note_sent(tx, envelope, *sent_at)?;
            }
        }
        DomainEvent::DraftSaved { message_id, account_id, envelope, body_text, body_html } => {
            // Each save replaces the draft; the event's own time keeps replays deterministic.
            exec(tx, "DELETE FROM messages WHERE id = ?1", [message_id.as_uuid()])?;
            insert_message(
                tx,
                &NewMessage {
                    id: message_id,
                    account_id,
                    mailbox_id: None,
                    provider_uid: None,
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
            store_body(tx, message_id, body_text.as_deref(), body_html.as_deref(), event.occurred_at)?;
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
) -> Result<(), DomainError> {
    exec(
        tx,
        "UPDATE accounts SET status = ?2 WHERE id = ?1",
        params![account_id.as_uuid(), json(&status)?],
    )
    .map(drop)
}

struct NewMessage<'a> {
    id: &'a MessageId,
    account_id: &'a AccountId,
    mailbox_id: Option<&'a MailboxId>,
    provider_uid: Option<u32>,
    rfc_message_id: Option<&'a str>,
    in_reply_to: Option<&'a str>,
    references: &'a [String],
    thread_id: Option<&'a str>,
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
        "INSERT INTO messages (id, account_id, mailbox_id, provider_uid, rfc_message_id, \
             in_reply_to, refs, thread_id, subject, from_name, from_email, addresses, flags, seen, \
             size_bytes, has_attachments, attachments, sent_at, received_at, internal_date, body_state) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, '[]', \
             ?17, ?18, ?18, ?19) \
         ON CONFLICT(id) DO NOTHING",
        params![
            m.id.as_uuid(),
            m.account_id.as_uuid(),
            m.mailbox_id.map(MailboxId::as_uuid),
            m.provider_uid,
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

/// Callers only note a message the first time its row is inserted, so replays
/// don't recount. A message mail-cat sent is counted twice once the Sent folder
/// syncs back (`MessageSent`, then `MessageReceived` there); that inflates
/// every such recipient alike, so the ranking order holds.
fn note_synced_message(
    tx: &Connection,
    mailbox_id: &MailboxId,
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
        Some(t) if t == json(&MailboxType::Sent)? => note_sent(tx, envelope, at),
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

fn note_sent(tx: &Connection, envelope: &Envelope, at: DateTime<Utc>) -> Result<(), DomainError> {
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
    let Some((flags, mailbox_id, account_id, thread_id)) = query_one(
        tx,
        "SELECT flags, mailbox_id, account_id, thread_id FROM messages WHERE id = ?1",
        [id.as_uuid()],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<Uuid>>(1)?,
                r.get::<_, Uuid>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        },
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
        if let Some(mailbox_id) = mailbox_id {
            adjust_unread(tx, &mailbox_id, if seen { -1 } else { 1 })?;
        }
        if let Some(thread_id) = thread_id {
            refresh_conversation(tx, &account_id, &thread_id)?;
        }
    }
    Ok(())
}

fn move_message(tx: &Connection, id: &MessageId, to: &MailboxId) -> Result<(), DomainError> {
    let Some((from, seen)) = query_one(
        tx,
        "SELECT mailbox_id, seen FROM messages WHERE id = ?1",
        [id.as_uuid()],
        |r| Ok((r.get::<_, Option<Uuid>>(0)?, r.get::<_, bool>(1)?)),
    )?
    else {
        return Ok(());
    };
    let to = *to.as_uuid();
    // A replay finds the row already in `to`: nothing moves, nothing to count.
    if from == Some(to) {
        return Ok(());
    }
    exec(tx, "UPDATE messages SET mailbox_id = ?2 WHERE id = ?1", params![id.as_uuid(), to])?;
    if !seen {
        if let Some(from) = from {
            adjust_unread(tx, &from, -1)?;
        }
        adjust_unread(tx, &to, 1)?;
    }
    Ok(())
}

fn delete_message(tx: &Connection, id: &MessageId) -> Result<(), DomainError> {
    let Some((mailbox_id, account_id, thread_id, seen)) = query_one(
        tx,
        "SELECT mailbox_id, account_id, thread_id, seen FROM messages WHERE id = ?1",
        [id.as_uuid()],
        |r| {
            Ok((
                r.get::<_, Option<Uuid>>(0)?,
                r.get::<_, Uuid>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, bool>(3)?,
            ))
        },
    )?
    else {
        return Ok(());
    };
    // The body row goes with it (ON DELETE CASCADE).
    exec(tx, "DELETE FROM messages WHERE id = ?1", [id.as_uuid()])?;
    if !seen && let Some(mailbox_id) = mailbox_id {
        adjust_unread(tx, &mailbox_id, -1)?;
    }
    if let Some(thread_id) = thread_id {
        refresh_conversation(tx, &account_id, &thread_id)?;
    }
    Ok(())
}

/// Recomputes a thread's row from its messages, so it converges after any
/// sequence of replays. Copies of one RFC message in several mailboxes
/// (Gmail labels) count once.
pub(super) fn refresh_conversation(
    tx: &Connection,
    account_id: &Uuid,
    thread_id: &str,
) -> Result<(), DomainError> {
    let (count, unread): (u32, u32) = tx
        .query_row(
            "SELECT COUNT(DISTINCT COALESCE(rfc_message_id, hex(id))), \
                    COUNT(DISTINCT CASE WHEN seen = 0 THEN COALESCE(rfc_message_id, hex(id)) END) \
             FROM messages WHERE account_id = ?1 AND thread_id = ?2",
            params![account_id, thread_id],
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
            "SELECT id, received_at, from_name, from_email FROM messages \
             WHERE account_id = ?1 AND thread_id = ?2 ORDER BY received_at DESC, id DESC LIMIT 1",
            params![account_id, thread_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(repo_err)?;
    let subject: String = tx
        .query_row(
            "SELECT subject FROM messages WHERE account_id = ?1 AND thread_id = ?2 \
             ORDER BY received_at, id LIMIT 1",
            params![account_id, thread_id],
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
