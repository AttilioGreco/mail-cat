//! SQLite home of the label classifier: groups, settings, token counts,
//! what each message taught, and the labels it applied by itself.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::DateTime;
use rusqlite::types::Type;
use rusqlite::{Connection, Row, params, params_from_iter};
use uuid::Uuid;

use mc_domain::{
    AccountId, AutoApplied, BackgroundSpeed, DiscoveryPoint, DomainError, Example, GroupId,
    LabelExamples, LabelMode, LabelSettings, LabelStats, LabelledVector, Lesson, MessageId,
    ModelProfile, ModelStats, Proposal, ProposalId, ProposalStatus, Scope, SourceFilter, TagSource,
    TaggingStore, TrainingGroup,
};

use super::repository::{conversion_err, exec, json, json_col, nanos, query_all, query_one};
use super::{SqliteDb, repo_err};

mod proposals;
mod sources;
mod vectors;
use sources::placeholders;

pub struct SqliteTaggingStore {
    db: Arc<SqliteDb>,
}

impl SqliteTaggingStore {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }
}

/// Parameters per statement stay far below SQLite's limit.
const MAX_TOKENS: usize = 2000;
/// Bumped when what tokens or labels mean changes, so scopes trained under
/// the old meaning (stored as 1) retrain on the next start.
const TRAINING_VERSION: u32 = 3;

const AUTO_COLUMNS: &str =
    "SELECT message_id, label, score, archived, at, confirmed, proposal_id FROM tag_auto";

fn auto_of_row(r: &Row) -> rusqlite::Result<AutoApplied> {
    Ok(AutoApplied {
        message_id: MessageId::from_uuid(r.get(0)?),
        label: r.get(1)?,
        score: r.get(2)?,
        archived: r.get(3)?,
        at: DateTime::from_timestamp_nanos(r.get(4)?),
        confirmed: r.get(5)?,
        proposal_id: r.get::<_, Option<Uuid>>(6)?.map(ProposalId::from_uuid),
    })
}

fn group_of_row(row: &Row) -> rusqlite::Result<(GroupId, String)> {
    Ok((GroupId::from_uuid(row.get(0)?), row.get(1)?))
}

#[async_trait]
impl TaggingStore for SqliteTaggingStore {
    async fn list_groups(&self) -> Result<Vec<TrainingGroup>, DomainError> {
        self.db
            .read(|conn| {
                let groups = query_all(
                    conn,
                    "SELECT id, name FROM tag_groups ORDER BY id",
                    [],
                    group_of_row,
                )?;
                // An account with no row is in the default group.
                let members = query_all(
                    conn,
                    "SELECT a.id, m.group_id, m.account_id IS NOT NULL FROM accounts a \
                     LEFT JOIN tag_members m ON m.account_id = a.id ORDER BY a.email",
                    [],
                    |r| {
                        Ok((
                            AccountId::from_uuid(r.get(0)?),
                            r.get::<_, Option<Uuid>>(1)?.map(GroupId::from_uuid),
                            r.get::<_, bool>(2)?,
                        ))
                    },
                )?;
                Ok(groups
                    .into_iter()
                    .map(|(id, name)| TrainingGroup {
                        id,
                        name,
                        accounts: members
                            .iter()
                            .filter(|(_, group, assigned)| {
                                if *assigned {
                                    *group == Some(id)
                                } else {
                                    id == GroupId::DEFAULT
                                }
                            })
                            .map(|(account, ..)| *account)
                            .collect(),
                    })
                    .collect())
            })
            .await
    }

    async fn create_group(&self, name: &str) -> Result<TrainingGroup, DomainError> {
        let (id, name) = (GroupId::new(), name.trim().to_owned());
        let stored = name.clone();
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "INSERT INTO tag_groups (id, name) VALUES (?1, ?2)",
                    params![id.as_uuid(), stored],
                )
                .map(drop)
            })
            .await?;
        Ok(TrainingGroup {
            id,
            name,
            accounts: Vec::new(),
        })
    }

    async fn rename_group(&self, id: GroupId, name: &str) -> Result<(), DomainError> {
        let name = name.trim().to_owned();
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "UPDATE tag_groups SET name = ?2 WHERE id = ?1",
                    params![id.as_uuid(), name],
                )
                .map(drop)
            })
            .await
    }

    async fn delete_group(&self, id: GroupId) -> Result<(), DomainError> {
        if id == GroupId::DEFAULT {
            return Err(DomainError::Validation(
                "the default group cannot be deleted".into(),
            ));
        }
        self.db
            .write(move |conn| {
                let tx = conn.transaction().map_err(repo_err)?;
                // Members fall back to the default group (no row).
                exec(
                    &tx,
                    "DELETE FROM tag_members WHERE group_id = ?1",
                    [id.as_uuid()],
                )?;
                exec(&tx, "DELETE FROM tag_groups WHERE id = ?1", [id.as_uuid()])?;
                let key = Scope::Group(id).key();
                delete_scope_rows(&tx, &key)?;
                // Nobody is left to retrain it, so nothing of it stays.
                exec(&tx, "DELETE FROM tag_settings WHERE scope = ?1", [&key])?;
                exec(&tx, "DELETE FROM tag_examples WHERE scope = ?1", [&key])?;
                tx.commit().map_err(repo_err)
            })
            .await
    }

    async fn assign_account(
        &self,
        account: AccountId,
        group: Option<GroupId>,
    ) -> Result<(), DomainError> {
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "INSERT INTO tag_members (account_id, group_id) VALUES (?1, ?2) \
                     ON CONFLICT(account_id) DO UPDATE SET group_id = excluded.group_id",
                    params![account.as_uuid(), group.as_ref().map(GroupId::as_uuid)],
                )
                .map(drop)
            })
            .await
    }

    async fn scope_of(&self, account: AccountId) -> Result<Scope, DomainError> {
        self.db
            .read(move |conn| {
                let member = query_one(
                    conn,
                    "SELECT group_id FROM tag_members WHERE account_id = ?1",
                    [account.as_uuid()],
                    |r| r.get::<_, Option<Uuid>>(0),
                )?;
                Ok(match member {
                    None => Scope::Group(GroupId::DEFAULT),
                    Some(None) => Scope::Alone(account),
                    Some(Some(group)) => Scope::Group(GroupId::from_uuid(group)),
                })
            })
            .await
    }

    async fn scope_accounts(&self, scope: Scope) -> Result<Vec<AccountId>, DomainError> {
        match scope {
            Scope::Alone(account) => Ok(vec![account]),
            Scope::Group(id) => Ok(self
                .list_groups()
                .await?
                .into_iter()
                .find(|g| g.id == id)
                .map(|g| g.accounts)
                .unwrap_or_default()),
        }
    }

    async fn scopes(&self) -> Result<Vec<Scope>, DomainError> {
        let groups = self.list_groups().await?;
        let mut scopes: Vec<Scope> = groups
            .iter()
            .filter(|g| !g.accounts.is_empty())
            .map(|g| Scope::Group(g.id))
            .collect();
        let in_a_group: Vec<AccountId> = groups.iter().flat_map(|g| g.accounts.clone()).collect();
        let accounts = self
            .db
            .read(|conn| {
                query_all(conn, "SELECT id FROM accounts", [], |r| {
                    Ok(AccountId::from_uuid(r.get(0)?))
                })
            })
            .await?;
        scopes.extend(
            accounts
                .into_iter()
                .filter(|a| !in_a_group.contains(a))
                .map(Scope::Alone),
        );
        Ok(scopes)
    }

    async fn settings(&self, scope: Scope) -> Result<Vec<LabelSettings>, DomainError> {
        let key = scope.key();
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    "SELECT label, mode, threshold FROM tag_settings WHERE scope = ?1",
                    [key],
                    |r| {
                        let mode: String = r.get(1)?;
                        Ok(LabelSettings {
                            label: r.get(0)?,
                            mode: mode
                                .parse::<LabelMode>()
                                .map_err(|e| conversion_err(1, Type::Text, e))?,
                            threshold: r.get(2)?,
                        })
                    },
                )
            })
            .await
    }

    async fn put_settings(
        &self,
        scope: Scope,
        settings: &LabelSettings,
    ) -> Result<(), DomainError> {
        let (key, s) = (scope.key(), settings.clone());
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "INSERT INTO tag_settings (scope, label, mode, threshold) VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(scope, label) DO UPDATE SET mode = excluded.mode, \
                         threshold = excluded.threshold",
                    params![key, s.label, s.mode.as_str(), s.threshold],
                )
                .map(drop)
            })
            .await
    }

    async fn stats(&self, scope: Scope, tokens: &[String]) -> Result<ModelStats, DomainError> {
        let key = scope.key();
        let tokens: Vec<String> = tokens.iter().take(MAX_TOKENS).cloned().collect();
        self.db
            .read(move |conn| {
                let mut stats = ModelStats {
                    documents: query_one(
                        conn,
                        "SELECT documents FROM tag_scopes WHERE scope = ?1",
                        [&key],
                        |r| r.get(0),
                    )?
                    .unwrap_or_default(),
                    ..ModelStats::default()
                };
                for (label, positive_docs, negative_docs) in query_all(
                    conn,
                    "SELECT label, positive_docs, negative_docs FROM tag_labels WHERE scope = ?1",
                    [&key],
                    |r| Ok((r.get::<_, String>(0)?, r.get(1)?, r.get(2)?)),
                )? {
                    stats.labels.insert(
                        label,
                        LabelStats {
                            positive_docs,
                            negative_docs,
                            tokens: HashMap::new(),
                        },
                    );
                }
                if tokens.is_empty() {
                    return Ok(stats);
                }
                let marks = placeholders(2, tokens.len());
                let args = || {
                    std::iter::once(key.as_str()).chain(tokens.iter().map(String::as_str))
                };
                stats.corpus = query_all(
                    conn,
                    &format!("SELECT token, weight FROM tag_corpus WHERE scope = ?1 AND token IN ({marks})"),
                    params_from_iter(args()),
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?
                .into_iter()
                .collect();
                for (label, token, positive, negative) in query_all(
                    conn,
                    &format!(
                        "SELECT label, token, positive, negative FROM tag_label_tokens \
                         WHERE scope = ?1 AND token IN ({marks})"
                    ),
                    params_from_iter(args()),
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get(2)?, r.get(3)?)),
                )? {
                    stats
                        .labels
                        .entry(label)
                        .or_default()
                        .tokens
                        .insert(token, (positive, negative));
                }
                Ok(stats)
            })
            .await
    }

    async fn teach(&self, scope: Scope, lesson: Lesson<'_>) -> Result<(), DomainError> {
        let key = scope.key();
        let (tokens, positives, negatives) = (
            lesson.tokens.to_vec(),
            lesson.positives.to_vec(),
            lesson.negatives.to_vec(),
        );
        let w = lesson.weight;
        let explicit = w.signum();
        self.db
            .write(move |conn| {
                let tx = conn.transaction().map_err(repo_err)?;
                exec(
                    &tx,
                    "INSERT INTO tag_scopes (scope, documents) VALUES (?1, ?2) \
                     ON CONFLICT(scope) DO UPDATE SET documents = documents + excluded.documents",
                    params![key, w],
                )?;
                for token in &tokens {
                    exec(
                        &tx,
                        "INSERT INTO tag_corpus (scope, token, weight) VALUES (?1, ?2, ?3) \
                         ON CONFLICT(scope, token) DO UPDATE SET weight = weight + excluded.weight",
                        params![key, token, w],
                    )?;
                }
                for (labels, positive) in [(&positives, true), (&negatives, false)] {
                    let amount = if positive { w } else { explicit };
                    for label in labels {
                        exec(
                            &tx,
                            if positive {
                                "INSERT INTO tag_labels (scope, label, positive_docs) VALUES (?1, ?2, ?3) \
                                 ON CONFLICT(scope, label) DO UPDATE SET positive_docs = positive_docs + excluded.positive_docs"
                            } else {
                                "INSERT INTO tag_labels (scope, label, negative_docs) VALUES (?1, ?2, ?3) \
                                 ON CONFLICT(scope, label) DO UPDATE SET negative_docs = negative_docs + excluded.negative_docs"
                            },
                            params![key, label, amount],
                        )?;
                        for token in &tokens {
                            exec(
                                &tx,
                                if positive {
                                    "INSERT INTO tag_label_tokens (scope, label, token, positive) VALUES (?1, ?2, ?3, ?4) \
                                     ON CONFLICT(scope, label, token) DO UPDATE SET positive = positive + excluded.positive"
                                } else {
                                    "INSERT INTO tag_label_tokens (scope, label, token, negative) VALUES (?1, ?2, ?3, ?4) \
                                     ON CONFLICT(scope, label, token) DO UPDATE SET negative = negative + excluded.negative"
                                },
                                params![key, label, token, amount],
                            )?;
                        }
                    }
                }
                tx.commit().map_err(repo_err)
            })
            .await
    }

    async fn label_examples(
        &self,
        scope: Scope,
        label: &str,
        limit: u32,
    ) -> Result<LabelExamples, DomainError> {
        let (key, label) = (scope.key(), label.to_owned());
        self.db
            .read(move |conn| {
                let from = "FROM tag_examples WHERE scope = ?1 AND weight > 0 AND EXISTS \
                     (SELECT 1 FROM json_each(positives) WHERE value = ?2)";
                let recent = query_all(
                    conn,
                    &format!("SELECT message_id {from} ORDER BY message_id DESC LIMIT {limit}"),
                    params![key, label],
                    |r| Ok(MessageId::from_uuid(r.get(0)?)),
                )?;
                let per_account = query_all(
                    conn,
                    &format!("SELECT account_id, COUNT(*) {from} GROUP BY account_id"),
                    params![key, label],
                    |r| Ok((AccountId::from_uuid(r.get(0)?), r.get(1)?)),
                )?;
                Ok(LabelExamples {
                    recent,
                    per_account,
                })
            })
            .await
    }

    async fn positives(&self, scope: Scope) -> Result<Vec<(String, u32)>, DomainError> {
        let key = scope.key();
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    "SELECT label, positive_docs FROM tag_labels WHERE scope = ?1 AND positive_docs > 0.5",
                    [key],
                    |r| Ok((r.get(0)?, r.get::<_, f64>(1)?.round() as u32)),
                )
            })
            .await
    }

    async fn reset(&self, scope: Scope) -> Result<(), DomainError> {
        let key = scope.key();
        self.db
            .write(move |conn| {
                let tx = conn.transaction().map_err(repo_err)?;
                delete_scope_rows(&tx, &key)?;
                tx.commit().map_err(repo_err)
            })
            .await
    }

    async fn is_trained(&self, scope: Scope) -> Result<bool, DomainError> {
        let key = scope.key();
        self.db
            .read(move |conn| {
                Ok(query_one(
                    conn,
                    "SELECT trained FROM tag_scopes WHERE scope = ?1",
                    [key],
                    |r| r.get::<_, u32>(0),
                )?
                .is_some_and(|v| v >= TRAINING_VERSION))
            })
            .await
    }

    async fn mark_trained(&self, scope: Scope) -> Result<(), DomainError> {
        let key = scope.key();
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "INSERT INTO tag_scopes (scope, trained) VALUES (?1, ?2) \
                     ON CONFLICT(scope) DO UPDATE SET trained = ?2",
                    params![key, TRAINING_VERSION],
                )
                .map(drop)
            })
            .await
    }

    async fn mark_untrained(&self, scope: Scope) -> Result<(), DomainError> {
        let key = scope.key();
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "UPDATE tag_scopes SET trained = 0 WHERE scope = ?1",
                    [key],
                )
                .map(drop)
            })
            .await
    }

    async fn example(&self, message: MessageId) -> Result<Option<Example>, DomainError> {
        self.db
            .read(move |conn| {
                query_one(
                    conn,
                    "SELECT account_id, scope, positives, negatives, with_body, weight \
                     FROM tag_examples WHERE message_id = ?1",
                    [message.as_uuid()],
                    |r| {
                        Ok(Example {
                            message_id: message,
                            account_id: AccountId::from_uuid(r.get(0)?),
                            scope: r.get(1)?,
                            positives: json_col(r, 2)?,
                            negatives: json_col(r, 3)?,
                            with_body: r.get(4)?,
                            weight: r.get(5)?,
                        })
                    },
                )
            })
            .await
    }

    async fn put_example(&self, e: &Example) -> Result<(), DomainError> {
        let e = e.clone();
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "INSERT OR REPLACE INTO tag_examples \
                     (message_id, account_id, scope, positives, negatives, with_body, weight) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        e.message_id.as_uuid(),
                        e.account_id.as_uuid(),
                        e.scope,
                        json(&e.positives)?,
                        json(&e.negatives)?,
                        e.with_body,
                        e.weight
                    ],
                )
                .map(drop)
            })
            .await
    }

    async fn sources(&self, ids: &[MessageId]) -> Result<Vec<TagSource>, DomainError> {
        let ids: Vec<Uuid> = ids.iter().map(|i| *i.as_uuid()).collect();
        self.db.read(move |conn| sources::by_ids(conn, &ids)).await
    }

    async fn source_page(
        &self,
        accounts: &[AccountId],
        filter: SourceFilter,
        cursor: Option<MessageId>,
        newest_first: bool,
        limit: u32,
    ) -> Result<Vec<TagSource>, DomainError> {
        if accounts.is_empty() {
            return Ok(Vec::new());
        }
        let accounts: Vec<Uuid> = accounts.iter().map(|a| *a.as_uuid()).collect();
        let cursor = cursor.map_or_else(
            || {
                if newest_first {
                    Uuid::max()
                } else {
                    Uuid::nil()
                }
            },
            |m| *m.as_uuid(),
        );
        self.db
            .read(move |conn| sources::page(conn, &accounts, filter, cursor, newest_first, limit))
            .await
    }

    async fn record_auto(&self, applied: &AutoApplied) -> Result<(), DomainError> {
        let a = applied.clone();
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "INSERT OR REPLACE INTO tag_auto \
                     (message_id, label, score, archived, at, confirmed, proposal_id) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        a.message_id.as_uuid(),
                        a.label,
                        a.score,
                        a.archived,
                        nanos(a.at),
                        a.confirmed,
                        a.proposal_id.map(|p| *p.as_uuid()),
                    ],
                )
                .map(drop)
            })
            .await
    }

    async fn autos(&self, message: MessageId) -> Result<Vec<AutoApplied>, DomainError> {
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    &format!("{AUTO_COLUMNS} WHERE message_id = ?1"),
                    [message.as_uuid()],
                    auto_of_row,
                )
            })
            .await
    }

    async fn proposal_autos(&self, proposal: ProposalId) -> Result<Vec<AutoApplied>, DomainError> {
        self.db
            .read(move |conn| {
                query_all(
                    conn,
                    &format!("{AUTO_COLUMNS} WHERE proposal_id = ?1"),
                    [proposal.as_uuid()],
                    auto_of_row,
                )
            })
            .await
    }

    async fn take_auto(&self, message: MessageId, label: &str) -> Result<bool, DomainError> {
        let label = label.to_owned();
        self.db
            .write(move |conn| {
                exec(
                    conn,
                    "DELETE FROM tag_auto WHERE message_id = ?1 AND label = ?2",
                    params![message.as_uuid(), label],
                )
                .map(|n| n > 0)
            })
            .await
    }
    async fn embeddings_enabled(&self) -> Result<bool, DomainError> {
        self.db.read(vectors::enabled).await
    }

    async fn set_embeddings_enabled(&self, enabled: bool) -> Result<(), DomainError> {
        self.db
            .write(move |conn| vectors::set_enabled(conn, enabled))
            .await
    }

    async fn background_speed(&self) -> Result<BackgroundSpeed, DomainError> {
        self.db.read(vectors::speed).await
    }

    async fn set_background_speed(&self, speed: BackgroundSpeed) -> Result<(), DomainError> {
        self.db
            .write(move |conn| vectors::set_speed(conn, speed))
            .await
    }

    async fn model_profile(&self) -> Result<ModelProfile, DomainError> {
        self.db.read(vectors::profile).await
    }

    async fn set_model_profile(&self, profile: ModelProfile) -> Result<(), DomainError> {
        self.db
            .write(move |conn| vectors::set_profile(conn, profile))
            .await
    }

    async fn execution_target(&self) -> Result<String, DomainError> {
        self.db.read(vectors::target).await
    }

    async fn set_execution_target(&self, target: &str) -> Result<(), DomainError> {
        let target = target.to_owned();
        self.db
            .write(move |conn| vectors::set_target(conn, &target))
            .await
    }

    async fn put_vectors(
        &self,
        model: &str,
        batch: &[(MessageId, Vec<f32>)],
    ) -> Result<(), DomainError> {
        let (model, batch) = (model.to_owned(), batch.to_vec());
        self.db
            .write(move |conn| vectors::put(conn, &model, &batch))
            .await
    }

    async fn vectors(
        &self,
        model: &str,
        ids: &[MessageId],
    ) -> Result<HashMap<MessageId, Vec<f32>>, DomainError> {
        let (model, ids): (String, Vec<Uuid>) =
            (model.to_owned(), ids.iter().map(|i| *i.as_uuid()).collect());
        self.db
            .read(move |conn| vectors::of(conn, &model, &ids))
            .await
    }

    async fn scope_vectors(
        &self,
        scope: Scope,
        model: &str,
    ) -> Result<Vec<LabelledVector>, DomainError> {
        let model = model.to_owned();
        self.db
            .read(move |conn| vectors::scope(conn, scope, &model))
            .await
    }

    async fn count_sources(
        &self,
        accounts: &[AccountId],
        filter: SourceFilter,
    ) -> Result<u64, DomainError> {
        if accounts.is_empty() {
            return Ok(0);
        }
        let accounts: Vec<Uuid> = accounts.iter().map(|a| *a.as_uuid()).collect();
        self.db
            .read(move |conn| sources::count(conn, &accounts, filter))
            .await
    }

    async fn embedding_counts(&self, model: &str) -> Result<(u64, u64), DomainError> {
        let model = model.to_owned();
        self.db
            .read(move |conn| sources::embedding_counts(conn, &model))
            .await
    }

    async fn unembedded(
        &self,
        model: &str,
        before: Option<MessageId>,
        limit: u32,
    ) -> Result<Vec<TagSource>, DomainError> {
        let model = model.to_owned();
        let before = before.map_or_else(Uuid::max, |m| *m.as_uuid());
        self.db
            .read(move |conn| sources::unembedded(conn, &model, before, limit))
            .await
    }

    async fn unlabelled_points(
        &self,
        accounts: &[AccountId],
        model: &str,
        limit: u32,
    ) -> Result<Vec<DiscoveryPoint>, DomainError> {
        if accounts.is_empty() {
            return Ok(Vec::new());
        }
        let model = model.to_owned();
        let accounts: Vec<Uuid> = accounts.iter().map(|a| *a.as_uuid()).collect();
        self.db
            .read(move |conn| proposals::points(conn, &accounts, &model, limit))
            .await
    }

    async fn proposals(&self, status: ProposalStatus) -> Result<Vec<Proposal>, DomainError> {
        self.db.read(move |conn| proposals::all(conn, status)).await
    }

    async fn proposal(&self, id: ProposalId) -> Result<Option<Proposal>, DomainError> {
        self.db.read(move |conn| proposals::one(conn, id)).await
    }

    async fn put_proposal(&self, proposal: &Proposal) -> Result<(), DomainError> {
        let proposal = proposal.clone();
        self.db
            .write(move |conn| proposals::put(conn, &proposal))
            .await
    }

    async fn delete_proposal(&self, id: ProposalId) -> Result<(), DomainError> {
        self.db.write(move |conn| proposals::delete(conn, id)).await
    }
}

/// Counts and the trained mark of a scope; its settings stay. Examples that
/// hold a rejection stay too, emptied of what they taught (weight 0), as
/// rejections are the user's and cannot be learnt back from the mail.
pub(super) fn delete_scope_rows(tx: &Connection, key: &str) -> Result<(), DomainError> {
    for table in ["tag_scopes", "tag_corpus", "tag_labels", "tag_label_tokens"] {
        exec(tx, &format!("DELETE FROM {table} WHERE scope = ?1"), [key])?;
    }
    exec(
        tx,
        "DELETE FROM tag_examples WHERE scope = ?1 AND negatives = '[]'",
        [key],
    )?;
    exec(
        tx,
        "UPDATE tag_examples SET positives = '[]', weight = 0 WHERE scope = ?1",
        [key],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests;
