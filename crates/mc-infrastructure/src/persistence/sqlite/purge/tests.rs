use std::sync::Arc;

use chrono::Utc;
use futures::TryStreamExt;
use rusqlite::params;

use mc_domain::{
    AccountId, AggregateType, ContactEmail, ContactId, ContactSource, DomainEvent, EmailAddress,
    Envelope, EventStore, Flag, MailboxId, MailboxPath, MailboxType, MessageId, ProviderType,
    ReadModelProjector, RemoteId,
};

use super::*;
use crate::persistence::sqlite::SqliteDb;
use crate::persistence::sqlite::event_store::SqliteEventStore;

struct Fixture {
    _dir: tempfile::TempDir,
    store: Arc<SqliteEventStore>,
    repo: SqliteRepository,
}

fn open() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteDb::open(&dir.path().join("mail-cat.db")).unwrap();
    let store = Arc::new(SqliteEventStore::new(db.clone()));
    let repo = SqliteRepository::new(db, store.clone());
    Fixture {
        _dir: dir,
        store,
        repo,
    }
}

impl Fixture {
    /// Appends to the log and projects, as `append_and_project` does.
    async fn record(&self, aggregate: String, kind: AggregateType, event: DomainEvent) {
        let sequence = self.store.current_sequence(&aggregate).await.unwrap();
        self.store
            .append(&aggregate, kind, sequence, vec![event])
            .await
            .unwrap();
        let envelope = self
            .store
            .load_events(&aggregate, sequence + 1)
            .await
            .unwrap()
            .remove(0);
        self.repo.project(&envelope).await.unwrap();
    }

    /// An account with an inbox, a threaded message and its flag and body
    /// events, a cursor, a sent counter and a download row. Returns the
    /// account, its inbox and its message.
    async fn account(&self, address: &str) -> (AccountId, MailboxId, MessageId) {
        let (account, inbox, message) = (AccountId::new(), MailboxId::new(), MessageId::new());
        self.record(
            account.to_string(),
            AggregateType::Account,
            DomainEvent::AccountAdded {
                account_id: account,
                name: address.into(),
                email: EmailAddress::new(address).unwrap(),
                provider_type: ProviderType::Imap,
            },
        )
        .await;
        self.record(
            inbox.to_string(),
            AggregateType::Mailbox,
            DomainEvent::MailboxDiscovered {
                mailbox_id: inbox,
                account_id: account,
                path: MailboxPath::inbox(),
                display_path: "INBOX".into(),
                mailbox_type: MailboxType::Inbox,
            },
        )
        .await;
        self.record(
            message.to_string(),
            AggregateType::Message,
            DomainEvent::MessageReceived {
                message_id: message,
                account_id: account,
                mailbox_id: inbox,
                remote_id: RemoteId::from(1),
                message_rfc_id: Some("<a@x>".into()),
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
                provider_key: None,
            },
        )
        .await;
        // Neither names the account: only the aggregate does.
        for event in [
            DomainEvent::MessageFlagged {
                message_id: message,
                flags_added: vec![Flag::Seen],
                flags_removed: vec![],
            },
            DomainEvent::MessageBodyFetched {
                message_id: message,
                attachments: vec![],
            },
        ] {
            self.record(message.to_string(), AggregateType::Message, event)
                .await;
        }
        let (id, address) = (*account.as_uuid(), address.to_owned());
        self.repo
            .db
            .write(move |c| {
                c.execute(
                    "INSERT INTO sync_cursors VALUES (?1, 'INBOX', 'c')",
                    params![id],
                )
                .and_then(|_| c.execute("INSERT INTO sent_counted VALUES (?1, 'k')", params![id]))
                .and_then(|_| {
                    c.execute("INSERT INTO downloads VALUES (?1, 1, 10)", params![address])
                })
                .map(drop)
                .map_err(repo_err)
            })
            .await
            .unwrap();
        (account, inbox, message)
    }

    async fn remove(&self, account: AccountId) {
        self.record(
            account.to_string(),
            AggregateType::Account,
            DomainEvent::AccountRemoved {
                account_id: account,
            },
        )
        .await;
    }

    /// Rows holding anything of `account`, per table, in one list.
    async fn rows_of(&self, account: AccountId, address: &str) -> Vec<(&'static str, i64)> {
        let (id, address, text) = (*account.as_uuid(), address.to_owned(), account.to_string());
        self.repo
            .db
            .read(move |c| {
                let count = |sql: &str, p: &dyn rusqlite::ToSql| {
                    c.query_row(sql, [p], |r| r.get::<_, i64>(0))
                        .map_err(repo_err)
                };
                let by_account = |table: &str| {
                    count(
                        &format!("SELECT count(*) FROM {table} WHERE account_id = ?1"),
                        &id,
                    )
                };
                Ok(vec![
                    (
                        "accounts",
                        count("SELECT count(*) FROM accounts WHERE id = ?1", &id)?,
                    ),
                    ("mailboxes", by_account("mailboxes")?),
                    ("messages", by_account("messages")?),
                    ("conversations", by_account("conversations")?),
                    ("thread_links", by_account("thread_links")?),
                    ("sync_cursors", by_account("sync_cursors")?),
                    ("sent_counted", by_account("sent_counted")?),
                    (
                        "downloads",
                        count(
                            "SELECT count(*) FROM downloads WHERE account_email = ?1",
                            &address,
                        )?,
                    ),
                    (
                        "events",
                        count("SELECT count(*) FROM events WHERE account_id = ?1", &text)?,
                    ),
                    (
                        "message_mailboxes",
                        count(
                            "SELECT count(*) FROM message_mailboxes WHERE mailbox_id IN \
                             (SELECT id FROM mailboxes WHERE account_id = ?1)",
                            &id,
                        )?,
                    ),
                ])
            })
            .await
            .unwrap()
    }

    async fn replay(&self) {
        let log: Vec<_> = self
            .store
            .load_all_events(None, 0)
            .try_collect()
            .await
            .unwrap();
        for envelope in &log {
            self.repo.project(envelope).await.unwrap();
        }
    }
}

#[tokio::test]
async fn purge_leaves_no_row_of_the_account_and_touches_nothing_else() {
    let f = open();
    let (gone, ..) = f.account("gone@example.com").await;
    let (kept, ..) = f.account("kept@example.com").await;
    let contact = ContactId::new();
    f.record(
        contact.to_string(),
        AggregateType::Contact,
        DomainEvent::ContactAdded {
            contact_id: contact,
            display_name: "Ada".into(),
            emails: vec![ContactEmail::primary(
                EmailAddress::new("ada@x.org").unwrap(),
            )],
            source: ContactSource::Local,
            external_id: None,
            account_scope: None,
        },
    )
    .await;
    let before = f.rows_of(kept, "kept@example.com").await;
    for (table, rows) in &before {
        assert!(*rows > 0, "{table} is not exercised");
    }
    f.remove(gone).await;
    assert_eq!(
        f.repo.pending_removals().await.unwrap(),
        vec![(gone, Some("gone@example.com".to_owned()))]
    );

    f.repo
        .purge_account(gone, Some("gone@example.com".into()))
        .await
        .unwrap();

    for (table, rows) in f.rows_of(gone, "gone@example.com").await {
        assert_eq!(rows, 0, "{table}");
    }
    assert_eq!(f.rows_of(kept, "kept@example.com").await, before);
    assert!(f.repo.pending_removals().await.unwrap().is_empty());
    let contacts: i64 = f
        .repo
        .db
        .read(|c| {
            c.query_row("SELECT count(*) FROM contacts", [], |r| r.get(0))
                .map_err(repo_err)
        })
        .await
        .unwrap();
    assert_eq!(contacts, 1);
    let contact_events = f.store.load_events(&contact.to_string(), 0).await.unwrap();
    assert_eq!(contact_events.len(), 1);
}

#[tokio::test]
async fn replay_after_a_purge_brings_nothing_back_and_new_events_are_not_skipped() {
    let f = open();
    let (gone, ..) = f.account("gone@example.com").await;
    let (kept, ..) = f.account("kept@example.com").await;
    let before = f.rows_of(kept, "kept@example.com").await;
    f.remove(gone).await;
    // The removed account owns the log's last position.
    let last = f
        .store
        .load_all_events(None, 0)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .last()
        .unwrap()
        .global_position;
    f.repo.purge_account(gone, None).await.unwrap();

    f.replay().await;

    for (table, rows) in f.rows_of(gone, "none").await {
        assert_eq!(rows, 0, "{table}");
    }
    assert_eq!(f.rows_of(kept, "kept@example.com").await, before);
    let (again, ..) = f.account("gone@example.com").await;
    let fresh = f
        .store
        .load_events(&again.to_string(), 0)
        .await
        .unwrap()
        .remove(0);
    assert!(fresh.global_position > last);
}

#[tokio::test]
async fn purge_keeps_the_download_log_when_the_address_is_in_use() {
    let f = open();
    let (gone, ..) = f.account("same@example.com").await;
    f.remove(gone).await;

    f.repo.purge_account(gone, None).await.unwrap();

    let downloads = f.rows_of(gone, "same@example.com").await;
    assert_eq!(
        downloads.iter().find(|(t, _)| *t == "downloads").unwrap().1,
        1
    );
}
