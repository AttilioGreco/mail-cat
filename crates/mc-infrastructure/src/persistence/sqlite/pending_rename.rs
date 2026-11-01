//! Renames sent to the server whose local record is not written yet.

use async_trait::async_trait;
use rusqlite::params;

use mc_domain::{AccountId, DomainError, MailboxId, PendingRename, PendingRenameStore};

use super::repository::{SqliteRepository, exec, query_all};

#[async_trait]
impl PendingRenameStore for SqliteRepository {
    async fn put_pending_rename(&self, rename: &PendingRename) -> Result<(), DomainError> {
        let r = rename.clone();
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "INSERT OR REPLACE INTO pending_renames \
                     (mailbox_id, account_id, old_display, new_display) VALUES (?1, ?2, ?3, ?4)",
                    params![
                        r.mailbox_id.as_uuid(),
                        r.account_id.as_uuid(),
                        r.old_display,
                        r.new_display
                    ],
                )
                .map(drop)
            })
            .await
    }

    async fn pending_renames(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<PendingRename>, DomainError> {
        let account = *account_id.as_uuid();
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    "SELECT mailbox_id, old_display, new_display FROM pending_renames \
                     WHERE account_id = ?1",
                    [account],
                    |r| {
                        Ok(PendingRename {
                            mailbox_id: MailboxId::from_uuid(r.get(0)?),
                            account_id: AccountId::from_uuid(account),
                            old_display: r.get(1)?,
                            new_display: r.get(2)?,
                        })
                    },
                )
            })
            .await
    }

    async fn clear_pending_rename(&self, mailbox_id: &MailboxId) -> Result<(), DomainError> {
        let id = *mailbox_id.as_uuid();
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "DELETE FROM pending_renames WHERE mailbox_id = ?1",
                    [id],
                )
                .map(drop)
            })
            .await
    }
}
