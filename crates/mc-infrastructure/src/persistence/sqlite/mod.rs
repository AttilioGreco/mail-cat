//! SQLite persistence: one database file, one writer, a pool of readers.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use mc_domain::DomainError;
use rusqlite::{Connection, OpenFlags};

pub mod event_store;
mod projector;
pub mod repository;

/// Schema migrations, applied in order; `PRAGMA user_version` counts how many
/// have run. Append only — a shipped migration is never edited.
pub(crate) const MIGRATIONS: &[&str] = &[
    // 1: event log. The rowid is the global position.
    "CREATE TABLE events (
        global_position INTEGER PRIMARY KEY,
        event_id        BLOB    NOT NULL,
        aggregate_id    TEXT    NOT NULL,
        aggregate_type  INTEGER NOT NULL,
        sequence        INTEGER NOT NULL,
        event_type      TEXT    NOT NULL,
        occurred_at     INTEGER NOT NULL,
        payload         BLOB    NOT NULL,
        UNIQUE (aggregate_id, sequence)
    );",
    // 2: read model (spec §4). No FK from mailboxes to accounts and no UNIQUE
    // (mailbox, uid): catch-up replays events over rows in any later state,
    // and either constraint would turn a harmless replay into a boot failure.
    "CREATE TABLE accounts (
        id            BLOB PRIMARY KEY,
        name          TEXT NOT NULL,
        email         TEXT NOT NULL,
        provider_type TEXT NOT NULL,
        status        TEXT NOT NULL,
        total_unread  INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE mailboxes (
        id              BLOB PRIMARY KEY,
        account_id      BLOB NOT NULL,
        path            TEXT NOT NULL,
        delimiter       TEXT NOT NULL,
        display_name    TEXT NOT NULL,
        mailbox_type    TEXT NOT NULL,
        flags           TEXT NOT NULL,
        total_messages  INTEGER NOT NULL DEFAULT 0,
        unread_messages INTEGER NOT NULL DEFAULT 0,
        uid_validity    INTEGER,
        uid_next        INTEGER
    );
    CREATE INDEX mailboxes_account ON mailboxes(account_id);
    CREATE TABLE messages (
        id              BLOB PRIMARY KEY,
        account_id      BLOB NOT NULL,
        mailbox_id      BLOB,
        provider_uid    INTEGER,
        rfc_message_id  TEXT,
        in_reply_to     TEXT,
        refs            TEXT NOT NULL,
        thread_id       TEXT,
        subject         TEXT NOT NULL,
        from_name       TEXT,
        from_email      TEXT NOT NULL,
        addresses       TEXT NOT NULL,
        flags           TEXT NOT NULL,
        seen            INTEGER NOT NULL,
        size_bytes      INTEGER NOT NULL,
        has_attachments INTEGER NOT NULL,
        attachments     TEXT NOT NULL,
        sent_at         INTEGER NOT NULL,
        received_at     INTEGER NOT NULL,
        internal_date   INTEGER NOT NULL,
        body_state      TEXT NOT NULL DEFAULT 'pending'
    );
    CREATE INDEX messages_mailbox_date ON messages(mailbox_id, received_at DESC);
    CREATE INDEX messages_mailbox_uid ON messages(mailbox_id, provider_uid);
    CREATE INDEX messages_thread ON messages(account_id, thread_id, received_at);
    CREATE INDEX messages_rfc ON messages(account_id, rfc_message_id);
    CREATE INDEX messages_backfill ON messages(account_id, received_at DESC) WHERE body_state = 'pending';
    CREATE INDEX messages_unread ON messages(mailbox_id) WHERE seen = 0;
    CREATE TABLE message_bodies (
        message_id BLOB PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
        text_zstd  BLOB,
        html_zstd  BLOB,
        fetched_at INTEGER NOT NULL
    );
    CREATE TABLE conversations (
        account_id        BLOB NOT NULL,
        thread_id         TEXT NOT NULL,
        subject           TEXT NOT NULL,
        message_count     INTEGER NOT NULL,
        unread_count      INTEGER NOT NULL,
        last_message_at   INTEGER NOT NULL,
        latest_sender     TEXT NOT NULL,
        sample_message_id BLOB NOT NULL,
        PRIMARY KEY (account_id, thread_id)
    );
    CREATE INDEX conversations_recent ON conversations(account_id, last_message_at DESC);
    CREATE TABLE contacts (
        id            BLOB PRIMARY KEY,
        display_name  TEXT NOT NULL,
        primary_email TEXT NOT NULL,
        organization  TEXT,
        source        TEXT NOT NULL,
        account_scope BLOB,
        emails        TEXT NOT NULL
    );
    CREATE TABLE contact_emails (
        email      TEXT NOT NULL,
        contact_id BLOB NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
        PRIMARY KEY (email, contact_id)
    );
    CREATE TABLE projection_checkpoints (
        name     TEXT PRIMARY KEY,
        position INTEGER NOT NULL
    );",
    // 3: per-address correspondence, ranking recipient autocomplete. Keyed by
    // address, not contact: it outlives contact edits and merges. Seeded from
    // the messages already stored so existing databases rank immediately.
    r#"CREATE TABLE address_usage (
        email            TEXT PRIMARY KEY,
        sent_count       INTEGER NOT NULL DEFAULT 0,
        last_sent_at     INTEGER,
        last_received_at INTEGER
    ) WITHOUT ROWID;
    INSERT INTO address_usage (email, sent_count, last_sent_at)
        SELECT json_extract(a.value, '$.email'), COUNT(DISTINCT m.id), MAX(m.received_at)
        FROM messages m
        JOIN mailboxes b ON b.id = m.mailbox_id AND b.mailbox_type = '"Sent"',
             json_each(m.addresses) k, json_each(k.value) a
        WHERE k.key IN ('to', 'cc', 'bcc')
        GROUP BY 1;
    INSERT INTO address_usage (email, last_received_at)
        SELECT m.from_email, MAX(m.received_at)
        FROM messages m
        JOIN mailboxes b ON b.id = m.mailbox_id AND b.mailbox_type NOT IN ('"Sent"', '"Drafts"')
        WHERE m.from_email != ''
        GROUP BY 1
        ON CONFLICT(email) DO UPDATE SET last_received_at = excluded.last_received_at;"#,
    // 4: the sent messages address_usage has counted, keyed by Message-ID
    // (row id when there is none). Outlives the rows: replaying a superseded
    // Sent placeholder's MessageSent re-inserts a deleted row, and the server
    // copy shares its Message-ID, so neither may count again.
    r#"CREATE TABLE sent_counted (
        account_id  BLOB NOT NULL,
        message_key TEXT NOT NULL,
        PRIMARY KEY (account_id, message_key)
    ) WITHOUT ROWID;
    INSERT OR IGNORE INTO sent_counted (account_id, message_key)
        SELECT m.account_id, COALESCE(m.rfc_message_id, hex(m.id))
        FROM messages m
        JOIN mailboxes b ON b.id = m.mailbox_id AND b.mailbox_type = '"Sent"';"#,
    // 5: bytes pulled from each account's server, for its daily download cap
    // (Gmail: 2.5 GB/day). Keyed by address: the provider meters before the
    // account id exists. Operational data, not event-sourced; rows older
    // than the window are pruned as new ones arrive.
    "CREATE TABLE downloads (
        account_email TEXT NOT NULL,
        at            INTEGER NOT NULL,
        bytes         INTEGER NOT NULL
    );
    CREATE INDEX downloads_recent ON downloads(account_email, at);",
    // 6: why and since when an account is in its status (re-login needed,
    // password rejected), so a restart keeps the reason.
    "ALTER TABLE accounts ADD COLUMN status_error TEXT;
    ALTER TABLE accounts ADD COLUMN status_since INTEGER;",
    // 7: a body the backfill cut at its size cap; opening the message fetches
    // the rest. Bodies stored before are whole.
    "ALTER TABLE message_bodies ADD COLUMN truncated INTEGER NOT NULL DEFAULT 0;",
];

/// Idle read connections kept for reuse (spec §1: pool of 4). Each holds its
/// own page cache, so a read burst must not leave them all resident.
const MAX_IDLE_READERS: usize = 4;

pub struct SqliteDb {
    path: PathBuf,
    writer: Mutex<Connection>,
    readers: Mutex<Vec<Connection>>,
}

impl SqliteDb {
    pub fn open(path: &Path) -> Result<Arc<Self>, DomainError> {
        let mut writer = Connection::open(path).map_err(repo_err)?;
        // synchronous=NORMAL: in WAL mode a crash loses at most the last
        // transactions since the checkpoint, never consistency. Everything
        // stored here is re-fetchable from the provider.
        writer
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;
                 PRAGMA foreign_keys = ON;
                 PRAGMA cache_size = -8000;
                 PRAGMA temp_store = MEMORY;",
            )
            .map_err(repo_err)?;
        migrate(&mut writer)?;
        Ok(Arc::new(Self {
            path: path.to_owned(),
            writer: Mutex::new(writer),
            readers: Mutex::default(),
        }))
    }

    pub async fn write<T, F>(self: &Arc<Self>, f: F) -> Result<T, DomainError>
    where
        F: FnOnce(&mut Connection) -> Result<T, DomainError> + Send + 'static,
        T: Send + 'static,
    {
        let db = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            // A panic mid-closure rolls its transaction back on drop, so the
            // connection is still sound; don't let poisoning brick all writes.
            let mut conn = db.writer.lock().unwrap_or_else(PoisonError::into_inner);
            f(&mut conn)
        })
        .await
        .map_err(|e| DomainError::Repository(format!("sqlite write task failed: {e}")))?
    }

    pub async fn read<T, F>(self: &Arc<Self>, f: F) -> Result<T, DomainError>
    where
        F: FnOnce(&Connection) -> Result<T, DomainError> + Send + 'static,
        T: Send + 'static,
    {
        let db = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let conn = db.checkout_reader()?;
            let result = f(&conn);
            let mut idle = db.readers.lock().unwrap_or_else(PoisonError::into_inner);
            if idle.len() < MAX_IDLE_READERS {
                idle.push(conn);
            }
            result
        })
        .await
        .map_err(|e| DomainError::Repository(format!("sqlite read task failed: {e}")))?
    }

    fn checkout_reader(&self) -> Result<Connection, DomainError> {
        if let Some(conn) = self.readers.lock().unwrap_or_else(PoisonError::into_inner).pop() {
            return Ok(conn);
        }
        let conn = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(repo_err)?;
        conn.execute_batch("PRAGMA cache_size = -2000;").map_err(repo_err)?;
        Ok(conn)
    }
}

fn migrate(conn: &mut Connection) -> Result<(), DomainError> {
    let applied: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(repo_err)?;
    let known = MIGRATIONS.len() as i64;
    if applied > known {
        return Err(DomainError::Repository(format!(
            "database schema v{applied} is newer than this binary (v{known})"
        )));
    }
    let tx = conn.transaction().map_err(repo_err)?;
    for (version, sql) in (1..).zip(MIGRATIONS).skip(applied as usize) {
        tx.execute_batch(sql).map_err(repo_err)?;
        tx.pragma_update(None, "user_version", version).map_err(repo_err)?;
    }
    tx.commit().map_err(repo_err)
}

pub(crate) fn repo_err(e: impl std::fmt::Display) -> DomainError {
    DomainError::Repository(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_in(dir: &tempfile::TempDir) -> Result<Arc<SqliteDb>, DomainError> {
        SqliteDb::open(&dir.path().join("mail-cat.db"))
    }

    #[tokio::test]
    async fn open_applies_pragmas_and_all_migrations() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_in(&dir).unwrap();

        let (mode, version) = db
            .read(|conn| {
                let mode: String =
                    conn.query_row("PRAGMA journal_mode", [], |r| r.get(0)).map_err(repo_err)?;
                let version: i64 =
                    conn.query_row("PRAGMA user_version", [], |r| r.get(0)).map_err(repo_err)?;
                Ok((mode, version))
            })
            .await
            .unwrap();
        assert_eq!(mode, "wal");
        assert_eq!(version, MIGRATIONS.len() as i64);
    }

    #[tokio::test]
    async fn reopen_does_not_reapply_migrations() {
        let dir = tempfile::tempdir().unwrap();
        drop(open_in(&dir).unwrap());
        // A re-run of migration #1 would fail with "table events already exists".
        open_in(&dir).unwrap();
    }

    #[tokio::test]
    async fn newer_schema_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        drop(open_in(&dir).unwrap());
        rusqlite::Connection::open(dir.path().join("mail-cat.db"))
            .unwrap()
            .pragma_update(None, "user_version", 999)
            .unwrap();

        let err = open_in(&dir).err().expect("newer schema must not open");
        assert!(err.to_string().contains("newer"), "got {err}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_burst_leaves_at_most_max_idle_readers() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_in(&dir).unwrap();

        let reads = (0..16).map(|_| {
            db.read(|_| {
                std::thread::sleep(std::time::Duration::from_millis(50));
                Ok(())
            })
        });
        for result in futures::future::join_all(reads).await {
            result.unwrap();
        }

        assert!(db.readers.lock().unwrap().len() <= MAX_IDLE_READERS);
    }

    #[tokio::test]
    async fn write_then_read_from_pool() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_in(&dir).unwrap();
        db.write(|conn| {
            conn.execute_batch("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (42);")
                .map_err(repo_err)
        })
        .await
        .unwrap();

        let v: i64 = db
            .read(|conn| conn.query_row("SELECT v FROM t", [], |r| r.get(0)).map_err(repo_err))
            .await
            .unwrap();
        assert_eq!(v, 42);
    }
}
