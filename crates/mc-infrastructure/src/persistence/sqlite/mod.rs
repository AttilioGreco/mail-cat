//! SQLite persistence: one database file, one writer, a pool of readers.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use mc_domain::DomainError;
use rusqlite::{Connection, OpenFlags};

mod dav_projection;
mod dav_repository;
pub use dav_repository::ReminderLog;
pub mod event_store;
mod pending_rename;
mod projector;
mod purge;
pub mod repository;
pub mod tagging;
mod threading;

/// Schema migrations, applied in order; `PRAGMA user_version` counts how many
/// have run. Append only once the first release ships; until then the schema
/// is edited in place and the data folder wiped.
pub(crate) const MIGRATIONS: &[&str] = &[
    // 1: event log. The rowid is the global position.
    // AUTOINCREMENT: purging an account deletes events, and a position that
    // is reused would sit below the projections' checkpoints and be skipped.
    // `account_id` is what a purge deletes by; contact events have none.
    "CREATE TABLE events (
        global_position INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id        BLOB    NOT NULL,
        aggregate_id    TEXT    NOT NULL,
        aggregate_type  INTEGER NOT NULL,
        account_id      TEXT,
        sequence        INTEGER NOT NULL,
        event_type      TEXT    NOT NULL,
        occurred_at     INTEGER NOT NULL,
        payload         BLOB    NOT NULL,
        UNIQUE (aggregate_id, sequence)
    );
    CREATE INDEX events_account ON events(account_id);",
    // 2: read model (spec §4). No FK from mailboxes to accounts or from
    // memberships to mailboxes, and no UNIQUE (mailbox, uid): catch-up replays
    // events over rows in any later state, and any of them would turn a
    // harmless replay into a boot failure.
    "CREATE TABLE accounts (
        id            BLOB PRIMARY KEY,
        name          TEXT NOT NULL,
        email         TEXT NOT NULL,
        provider_type TEXT NOT NULL,
        mailbox_model TEXT NOT NULL,
        supports_tags INTEGER,
        status        TEXT NOT NULL,
        total_unread  INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE mailboxes (
        id              BLOB PRIMARY KEY,
        account_id      BLOB NOT NULL,
        path            TEXT NOT NULL,
        path_opaque     INTEGER NOT NULL DEFAULT 0,
        delimiter       TEXT NOT NULL,
        display_path    TEXT NOT NULL,
        display_name    TEXT NOT NULL,
        mailbox_type    TEXT NOT NULL,
        flags           TEXT NOT NULL,
        total_messages  INTEGER NOT NULL DEFAULT 0,
        unread_messages INTEGER NOT NULL DEFAULT 0,
        subscribed      INTEGER NOT NULL DEFAULT 1,
        subscription_explicit INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX mailboxes_account ON mailboxes(account_id);
    CREATE TABLE messages (
        id              BLOB PRIMARY KEY,
        account_id      BLOB NOT NULL,
        rfc_message_id  TEXT,
        in_reply_to     TEXT,
        refs            TEXT NOT NULL,
        thread_id       BLOB,
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
        body_state      TEXT NOT NULL DEFAULT 'pending',
        provider_key    TEXT
    );
    CREATE TABLE message_mailboxes (
        message_id   BLOB NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
        mailbox_id   BLOB NOT NULL,
        remote_id    TEXT,
        received_at  INTEGER NOT NULL,
        PRIMARY KEY (message_id, mailbox_id)
    ) WITHOUT ROWID;
    CREATE INDEX message_mailboxes_date ON message_mailboxes(mailbox_id, received_at DESC, message_id);
    CREATE INDEX message_mailboxes_remote ON message_mailboxes(mailbox_id, remote_id);
    CREATE INDEX messages_thread ON messages(account_id, thread_id, received_at);
    CREATE INDEX messages_rfc ON messages(account_id, rfc_message_id);
    CREATE INDEX messages_key ON messages(account_id, provider_key);
    CREATE INDEX messages_backfill ON messages(account_id, received_at DESC) WHERE body_state = 'pending';
    CREATE TABLE message_bodies (
        message_id BLOB PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
        text_zstd  BLOB,
        html_zstd  BLOB,
        fetched_at INTEGER NOT NULL
    );
    CREATE TABLE conversations (
        account_id        BLOB NOT NULL,
        thread_id         BLOB NOT NULL,
        subject           TEXT NOT NULL,
        message_count     INTEGER NOT NULL,
        unread_count      INTEGER NOT NULL,
        last_message_at   INTEGER NOT NULL,
        latest_sender     TEXT NOT NULL,
        sample_message_id BLOB NOT NULL,
        PRIMARY KEY (account_id, thread_id)
    );
    -- Every Message-ID seen in a header (own, In-Reply-To, References) -> its
    -- thread. No FK to messages: ids of messages we never downloaded link too.
    CREATE TABLE thread_links (
        account_id BLOB NOT NULL,
        rfc_id     TEXT NOT NULL,
        thread_id  BLOB NOT NULL,
        PRIMARY KEY (account_id, rfc_id)
    ) WITHOUT ROWID;
    CREATE INDEX thread_links_thread ON thread_links(account_id, thread_id);
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
    );
    CREATE TABLE sync_cursors (
        account_id BLOB NOT NULL,
        collection TEXT NOT NULL,
        cursor     TEXT NOT NULL,
        PRIMARY KEY (account_id, collection)
    );",
    // 3: per-address correspondence, ranking recipient autocomplete. Keyed by
    // address, not contact: it outlives contact edits and merges.
    r#"CREATE TABLE address_usage (
        email            TEXT PRIMARY KEY,
        sent_count       INTEGER NOT NULL DEFAULT 0,
        last_sent_at     INTEGER,
        last_received_at INTEGER
    ) WITHOUT ROWID;"#,
    // 4: the sent messages address_usage has counted, keyed by Message-ID
    // (row id when there is none). Outlives the rows: replaying a superseded
    // Sent placeholder's MessageSent re-inserts a deleted row, and the server
    // copy shares its Message-ID, so neither may count again.
    r#"CREATE TABLE sent_counted (
        account_id  BLOB NOT NULL,
        message_key TEXT NOT NULL,
        PRIMARY KEY (account_id, message_key)
    ) WITHOUT ROWID;"#,
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
    // 8: label suggestion. Mailing-list headers of messages synced from now
    // on (older rows have none until the data folder is wiped and resynced),
    // training groups (the default one has a fixed id; an account with no
    // row is in it, a NULL group means in none), per-label settings, and a
    // one-vs-rest naive Bayes: `tag_corpus` counts every trained message,
    // `tag_label_tokens` those under a label (`positive`) or rejected for it
    // (`negative`), so a label's negatives are the corpus minus its positives.
    // `tag_examples` remembers what each message taught, to withdraw it.
    "ALTER TABLE messages ADD COLUMN list_headers TEXT;
    CREATE TABLE tag_groups (id BLOB PRIMARY KEY, name TEXT NOT NULL);
    INSERT INTO tag_groups VALUES (x'00000000000000000000000000000001', 'Tutti');
    CREATE TABLE tag_members (account_id BLOB PRIMARY KEY, group_id BLOB) WITHOUT ROWID;
    CREATE TABLE tag_settings (
        scope TEXT NOT NULL, label TEXT NOT NULL, mode TEXT NOT NULL, threshold REAL NOT NULL,
        PRIMARY KEY (scope, label)
    ) WITHOUT ROWID;
    CREATE TABLE tag_scopes (
        scope TEXT PRIMARY KEY, documents REAL NOT NULL DEFAULT 0, trained INTEGER NOT NULL DEFAULT 0
    ) WITHOUT ROWID;
    CREATE TABLE tag_corpus (
        scope TEXT NOT NULL, token TEXT NOT NULL, weight REAL NOT NULL,
        PRIMARY KEY (scope, token)
    ) WITHOUT ROWID;
    CREATE TABLE tag_labels (
        scope TEXT NOT NULL, label TEXT NOT NULL,
        positive_docs REAL NOT NULL DEFAULT 0, negative_docs REAL NOT NULL DEFAULT 0,
        PRIMARY KEY (scope, label)
    ) WITHOUT ROWID;
    CREATE TABLE tag_label_tokens (
        scope TEXT NOT NULL, label TEXT NOT NULL, token TEXT NOT NULL,
        positive REAL NOT NULL DEFAULT 0, negative REAL NOT NULL DEFAULT 0,
        PRIMARY KEY (scope, label, token)
    ) WITHOUT ROWID;
    CREATE TABLE tag_examples (
        message_id BLOB PRIMARY KEY, account_id BLOB NOT NULL, scope TEXT NOT NULL,
        positives TEXT NOT NULL, negatives TEXT NOT NULL, with_body INTEGER NOT NULL,
        weight REAL NOT NULL
    );
    CREATE INDEX tag_examples_account ON tag_examples(account_id);
    CREATE TABLE tag_auto (
        message_id BLOB NOT NULL, label TEXT NOT NULL, score REAL NOT NULL,
        archived INTEGER NOT NULL, at INTEGER NOT NULL,
        PRIMARY KEY (message_id, label)
    ) WITHOUT ROWID;",
    // 9: sentence embeddings for label suggestion. One vector per message
    // (f32 little-endian, unit length); `model` names the model and revision
    // that made it, and a vector of any other is ignored and made again.
    // `tag_prefs` holds the user's consent to download the model and embed.
    "CREATE TABLE tag_vectors (
        message_id BLOB PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
        model TEXT NOT NULL, vec BLOB NOT NULL
    );
    CREATE TABLE tag_prefs (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;",
    // 10: label discovery. A proposal is a cluster of unlabelled mail with a
    // name to give it; `members` packs the 16-byte ids of its messages and is
    // what a later run matches against, so a rejected cluster stays rejected.
    // `confirmed` marks labels applied on the user's say-so (accepted
    // proposals): the model learns them as the user's own.
    "CREATE TABLE tag_proposals (
        id BLOB PRIMARY KEY, scope TEXT NOT NULL, name TEXT NOT NULL,
        keywords TEXT NOT NULL, members BLOB NOT NULL, inbox INTEGER NOT NULL,
        samples TEXT NOT NULL, status TEXT NOT NULL, created_at INTEGER NOT NULL
    );
    CREATE INDEX tag_proposals_status ON tag_proposals(status, scope);
    ALTER TABLE tag_auto ADD COLUMN confirmed INTEGER NOT NULL DEFAULT 0;",
    // 11: curating proposals. `curated` proposals (renamed or merged by a
    // person) survive discovery's refresh; `created` lists the label mailboxes
    // accepting one made (JSON), and `proposal_id` ties the labels an
    // acceptance applied to it, so the acceptance can be undone.
    "ALTER TABLE tag_proposals ADD COLUMN curated INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE tag_proposals ADD COLUMN created TEXT NOT NULL DEFAULT '[]';
    ALTER TABLE tag_auto ADD COLUMN proposal_id BLOB;
    CREATE INDEX tag_auto_proposal ON tag_auto(proposal_id) WHERE proposal_id IS NOT NULL;",
    // 12: mailbox renames sent to the server before the local record follows;
    // sync settles what a crash left. Not event-sourced: it is how the
    // events get written, and holds nothing once they are.
    "CREATE TABLE pending_renames (
        mailbox_id  BLOB PRIMARY KEY,
        account_id  BLOB NOT NULL,
        old_display TEXT NOT NULL,
        new_display TEXT NOT NULL
    );
    CREATE INDEX pending_renames_account ON pending_renames(account_id);",
    // 13: a label's color (a palette key), kept here because mail servers
    // have none. Shared by the label's whole training group.
    "ALTER TABLE tag_settings ADD COLUMN color TEXT;",
    // 14: server-synced contacts and calendars. A contact bound to a server
    // keeps its whole vCard and the place it lives; `dirty` is derived from
    // the events (last local one not yet pushed), `edited_at` is when it
    // happened and `local_seq` is its sequence, which an acknowledgement
    // must match to clear it. A resource the user deleted has no row left to be dirty, so
    // its pending DELETE waits in `pending_deletes` until the server
    // acknowledges it. `conflicts_lost` is keyed by event id so that
    // re-projecting an event cannot count it twice.
    "ALTER TABLE contacts ADD COLUMN raw TEXT;
    ALTER TABLE contacts ADD COLUMN remote_account BLOB;
    ALTER TABLE contacts ADD COLUMN remote_collection TEXT;
    ALTER TABLE contacts ADD COLUMN remote_href TEXT;
    ALTER TABLE contacts ADD COLUMN remote_etag TEXT;
    ALTER TABLE contacts ADD COLUMN remote_modified INTEGER;
    ALTER TABLE contacts ADD COLUMN dirty INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE contacts ADD COLUMN edited_at INTEGER;
    ALTER TABLE contacts ADD COLUMN local_seq INTEGER;
    ALTER TABLE contacts ADD COLUMN push_error TEXT;
    CREATE INDEX contacts_remote ON contacts(remote_account, remote_collection)
        WHERE remote_href IS NOT NULL;
    CREATE TABLE address_books (
        account_id  BLOB NOT NULL,
        collection  TEXT NOT NULL,
        name        TEXT NOT NULL,
        description TEXT,
        read_only   INTEGER NOT NULL,
        PRIMARY KEY (account_id, collection)
    ) WITHOUT ROWID;
    CREATE TABLE calendars (
        account_id BLOB NOT NULL,
        collection TEXT NOT NULL,
        name       TEXT NOT NULL,
        color      TEXT,
        timezone   TEXT,
        read_only  INTEGER NOT NULL,
        ownership  TEXT NOT NULL,
        components TEXT NOT NULL,
        PRIMARY KEY (account_id, collection)
    ) WITHOUT ROWID;
    CREATE TABLE calendar_events (
        account_id      BLOB NOT NULL,
        href            TEXT NOT NULL,
        collection      TEXT NOT NULL,
        etag            TEXT,
        remote_modified INTEGER,
        raw             TEXT NOT NULL,
        summary         TEXT,
        start_at        INTEGER,
        end_at          INTEGER,
        all_day         INTEGER NOT NULL DEFAULT 0,
        recurring       INTEGER NOT NULL DEFAULT 0,
        location        TEXT,
        status          TEXT,
        dirty           INTEGER NOT NULL DEFAULT 0,
        edited_at       INTEGER,
        local_seq       INTEGER,
        push_error      TEXT,
        PRIMARY KEY (account_id, href)
    ) WITHOUT ROWID;
    CREATE INDEX calendar_events_collection ON calendar_events(account_id, collection, start_at);
    CREATE TABLE pending_deletes (
        aggregate_id TEXT PRIMARY KEY,
        kind         TEXT NOT NULL,
        account_id   BLOB NOT NULL,
        collection   TEXT NOT NULL,
        href         TEXT NOT NULL,
        etag         TEXT NOT NULL,
        edited_at    INTEGER NOT NULL,
        local_seq    INTEGER NOT NULL,
        push_error   TEXT
    ) WITHOUT ROWID;
    CREATE INDEX pending_deletes_collection ON pending_deletes(account_id, collection, kind);
    CREATE TABLE conflicts_lost (
        event_id   BLOB PRIMARY KEY,
        account_id BLOB NOT NULL
    ) WITHOUT ROWID;
    CREATE INDEX conflicts_lost_account ON conflicts_lost(account_id);",
    // 15: an event's DESCRIPTION, for the editor.
    "ALTER TABLE calendar_events ADD COLUMN description TEXT;",
    // 16: reminders already shown, so a restart does not ring them again.
    // `occurrence_end` keeps a long event's rows from being pruned mid-event.
    "CREATE TABLE fired_reminders (
        account_id       BLOB    NOT NULL,
        href             TEXT    NOT NULL,
        occurrence_start INTEGER NOT NULL,
        trigger_key      TEXT    NOT NULL,
        occurrence_end   INTEGER NOT NULL,
        fired_at         INTEGER NOT NULL,
        PRIMARY KEY (account_id, href, occurrence_start, trigger_key)
    ) WITHOUT ROWID;",
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
        if let Some(conn) = self
            .readers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
        {
            return Ok(conn);
        }
        let conn = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(repo_err)?;
        conn.execute_batch("PRAGMA cache_size = -2000;")
            .map_err(repo_err)?;
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
        tx.pragma_update(None, "user_version", version)
            .map_err(repo_err)?;
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
                let mode: String = conn
                    .query_row("PRAGMA journal_mode", [], |r| r.get(0))
                    .map_err(repo_err)?;
                let version: i64 = conn
                    .query_row("PRAGMA user_version", [], |r| r.get(0))
                    .map_err(repo_err)?;
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
            .read(|conn| {
                conn.query_row("SELECT v FROM t", [], |r| r.get(0))
                    .map_err(repo_err)
            })
            .await
            .unwrap();
        assert_eq!(v, 42);
    }
}
