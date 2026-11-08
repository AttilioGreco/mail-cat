//! Removing an account: everything the server keeps about it goes in one
//! transaction. Contacts and address usage span accounts and stay.

use rusqlite::params;
use uuid::Uuid;

use mc_domain::{AccountId, DomainError, DomainEvent, EventStore, GroupId, Scope};

use super::repo_err;
use super::repository::{SqliteRepository, exec, query_all, query_one};
use super::tagging::delete_scope_rows;

impl SqliteRepository {
    /// Accounts with an `AccountRemoved` event still in the log. The purge
    /// deletes that event with the rest, so whatever is listed is unfinished:
    /// the server died between recording the removal and purging. The address
    /// comes from the account's first event: the log is all that is left of it.
    pub async fn pending_removals(&self) -> Result<Vec<(AccountId, Option<String>)>, DomainError> {
        let ids = self
            .db
            .read(|conn| {
                query_all(
                    conn,
                    "SELECT DISTINCT account_id FROM events WHERE event_type = 'AccountRemoved'",
                    [],
                    |r| r.get::<_, String>(0),
                )
            })
            .await?;
        let mut pending = Vec::with_capacity(ids.len());
        for id in ids {
            let account_id = AccountId::from_uuid(
                Uuid::parse_str(&id).map_err(|e| DomainError::Repository(e.to_string()))?,
            );
            let email = self
                .events
                .load_events(&id, 0)
                .await?
                .into_iter()
                .find_map(|e| match e.payload {
                    DomainEvent::AccountAdded { email, .. } => Some(email.as_str().to_owned()),
                    _ => None,
                });
            pending.push((account_id, email));
        }
        Ok(pending)
    }

    /// Deletes the account's events and every row projected from them.
    /// `email` is `None` when another account now uses the address, whose
    /// download log must stay. Idempotent.
    pub async fn purge_account(
        &self,
        account_id: AccountId,
        email: Option<String>,
    ) -> Result<(), DomainError> {
        self.db
            .write(move |conn| {
                let tx = conn.transaction().map_err(repo_err)?;
                let id = account_id.as_uuid();
                exec(
                    &tx,
                    "DELETE FROM tag_auto WHERE message_id IN \
                     (SELECT id FROM messages WHERE account_id = ?1)",
                    [id],
                )?;
                // Its share of the model goes: a group's counts are rebuilt
                // from the remaining members, a lone account's all go.
                let scope = match query_one(
                    &tx,
                    "SELECT group_id FROM tag_members WHERE account_id = ?1",
                    [id],
                    |r| r.get::<_, Option<Uuid>>(0),
                )? {
                    None => Scope::Group(GroupId::DEFAULT),
                    Some(None) => Scope::Alone(account_id),
                    Some(Some(group)) => Scope::Group(GroupId::from_uuid(group)),
                };
                exec(&tx, "DELETE FROM tag_examples WHERE account_id = ?1", [id])?;
                delete_scope_rows(&tx, &scope.key())?;
                if matches!(scope, Scope::Alone(_)) {
                    exec(
                        &tx,
                        "DELETE FROM tag_settings WHERE scope = ?1",
                        [scope.key()],
                    )?;
                }
                // Bodies and memberships follow their message by cascade.
                for table in [
                    "messages",
                    "mailboxes",
                    "conversations",
                    "thread_links",
                    "sync_cursors",
                    "sent_counted",
                    "pending_renames",
                    "fired_reminders",
                ] {
                    exec(
                        &tx,
                        &format!("DELETE FROM {table} WHERE account_id = ?1"),
                        [id],
                    )?;
                }
                exec(&tx, "DELETE FROM tag_members WHERE account_id = ?1", [id])?;
                super::dav_projection::drop_account(&tx, &account_id)?;
                exec(&tx, "DELETE FROM accounts WHERE id = ?1", [id])?;
                if let Some(email) = email {
                    exec(
                        &tx,
                        "DELETE FROM downloads WHERE account_email = ?1",
                        params![email],
                    )?;
                }
                exec(
                    &tx,
                    "DELETE FROM events WHERE account_id = ?1",
                    [account_id.to_string()],
                )?;
                tx.commit().map_err(repo_err)
            })
            .await
    }
}

#[cfg(test)]
mod tests;
