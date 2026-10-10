//! SQLite persistence: one database file, one writer, a pool of readers.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use mc_domain::DomainError;
use rusqlite::{Connection, OpenFlags};

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
];

pub struct SqliteDb {
    path: PathBuf,
    writer: Mutex<Connection>,
    // ponytail: grows to peak read concurrency, which tokio's blocking pool
    // bounds; cap it if idle connections ever show up in memory profiles.
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
            db.readers.lock().unwrap_or_else(PoisonError::into_inner).push(conn);
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
