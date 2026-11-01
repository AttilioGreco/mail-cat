use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Value};
use tantivy::snippet::SnippetGenerator;
use tantivy::tokenizer::TextAnalyzer;
use tantivy::{Index, IndexReader, IndexWriter, Order, ReloadPolicy, TantivyDocument};
use tracing::{debug, info, warn};

use mc_domain::error::DomainError;
use mc_domain::model::account::AccountId;
use mc_domain::model::mailbox::MailboxId;
use mc_domain::model::message::Message;
use mc_domain::traits::search_index::{
    IndexExtras, IndexHit, IndexResults, SearchDirectory, SearchIndex,
};
use mc_domain::value_objects::message_id::MessageId;
use mc_shared::pagination::Pagination;

use super::builder::QueryBuilder;
use super::query::{self, Expr};
use super::schema::{IndexFields, build_schema, message_to_document};

/// Documents buffered before an automatic commit. Every commit writes a new
/// segment that the merge thread must later fold in, so committing often
/// (it was 100) kept a core busy merging during sync. `search` commits
/// whatever is pending first, so results never miss buffered writes.
const AUTO_COMMIT_THRESHOLD: u32 = 1000;

/// Tantivy's minimum per-thread arena; one indexing thread is plenty for a
/// mailbox and keeps the writer's memory at this floor.
const WRITER_MEMORY_BYTES: usize = 15_000_000;

/// Tantivy search index for email messages (spec §5). Stores only
/// `message_id`; everything shown to the user is hydrated from the read model.
/// The FAST fields (scope, date, flags, attachments) are columnar index data
/// for filters and sorting, not content.
pub struct TantivySearchIndex {
    index: Index,
    reader: IndexReader,
    writer: parking_lot::Mutex<IndexWriter>,
    fields: IndexFields,
    pending_count: AtomicU32,
    recreated: bool,
}

impl TantivySearchIndex {
    /// Opens the index at `index_path`, creating it — or recreating it when it
    /// was built with another schema. Check [`Self::was_recreated`] afterwards:
    /// a new index is empty and must be refilled from the event log.
    pub fn new<P: AsRef<Path>>(index_path: P) -> Result<Self, DomainError> {
        let path = index_path.as_ref();
        let schema = build_schema();
        let existing = if path.join("meta.json").exists() {
            Some(Index::open_in_dir(path).map_err(|e| search_err("open index", e))?)
        } else {
            None
        };
        let (index, recreated) = match existing {
            Some(index) if index.schema() == schema => (index, false),
            stale => {
                if stale.is_some() {
                    warn!(path = %path.display(), "search index schema changed, recreating it");
                    std::fs::remove_dir_all(path)
                        .map_err(|e| search_err("remove stale index", e))?;
                }
                std::fs::create_dir_all(path).map_err(|e| search_err("create index dir", e))?;
                let index = Index::create_in_dir(path, schema.clone())
                    .map_err(|e| search_err("create index", e))?;
                (index, true)
            }
        };

        // "email_tokenizer": splits on @, dots, whitespace — good for email addresses
        let email_analyzer = TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
            .filter(tantivy::tokenizer::LowerCaser)
            .build();
        index
            .tokenizers()
            .register("email_tokenizer", email_analyzer);

        let writer = index
            .writer_with_num_threads(1, WRITER_MEMORY_BYTES)
            .map_err(|e| search_err("create writer", e))?;
        // Manual reload: the reader is reloaded explicitly after each commit.
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| search_err("create reader", e))?;
        let fields = IndexFields::of(&schema);

        info!(recreated, "TantivySearchIndex initialized");
        Ok(Self {
            index,
            reader,
            writer: parking_lot::Mutex::new(writer),
            fields,
            pending_count: AtomicU32::new(0),
            recreated,
        })
    }

    /// True when [`Self::new`] created an empty index.
    pub fn was_recreated(&self) -> bool {
        self.recreated
    }

    fn parse_query(
        &self,
        query: &str,
        account_id: Option<&AccountId>,
        mailbox_id: Option<&MailboxId>,
        directory: &SearchDirectory,
    ) -> Result<(Expr, Box<dyn Query>), DomainError> {
        let expr = query::parse(query, Utc::now()).map_err(DomainError::Validation)?;
        let builder = QueryBuilder {
            index: &self.index,
            fields: &self.fields,
            directory,
        };
        let mut clauses = vec![(Occur::Must, builder.build(&expr))];
        let scopes = [
            (self.fields.account_id, account_id.map(ToString::to_string)),
            (self.fields.mailbox_id, mailbox_id.map(ToString::to_string)),
        ];
        for (field, id) in scopes {
            if let Some(id) = id {
                let term = tantivy::Term::from_field_text(field, &id);
                clauses.push((
                    Occur::Must,
                    Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
                ));
            }
        }
        Ok((expr, Box::new(BooleanQuery::new(clauses))))
    }

    fn commit_and_reload(&self) -> Result<(), DomainError> {
        self.writer
            .lock()
            .commit()
            .map_err(|e| search_err("commit", e))?;
        self.pending_count.store(0, Ordering::Relaxed);
        self.reader
            .reload()
            .map_err(|e| search_err("reload reader", e))
    }
}

#[async_trait]
impl SearchIndex for TantivySearchIndex {
    async fn index_message(
        &self,
        message: &Message,
        extras: &IndexExtras,
    ) -> Result<(), DomainError> {
        debug!("Indexing message: {}", message.id);
        let term = tantivy::Term::from_field_text(self.fields.message_id, &message.id.to_string());
        {
            let writer = self.writer.lock();
            writer.delete_term(term);
            writer
                .add_document(message_to_document(&self.fields, message, extras))
                .map_err(|e| search_err("add document", e))?;
        }
        // Batch commits during sync.
        if self.pending_count.fetch_add(1, Ordering::Relaxed) + 1 >= AUTO_COMMIT_THRESHOLD {
            self.commit_and_reload()?;
        }
        Ok(())
    }

    async fn remove_message(&self, message_id: &MessageId) -> Result<(), DomainError> {
        debug!("Removing message: {}", message_id);
        let term = tantivy::Term::from_field_text(self.fields.message_id, &message_id.to_string());
        self.writer.lock().delete_term(term);
        self.pending_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn remove_account(&self, account_id: &AccountId) -> Result<(), DomainError> {
        let term = tantivy::Term::from_field_text(self.fields.account_id, &account_id.to_string());
        self.writer.lock().delete_term(term);
        self.commit_and_reload()
    }

    async fn search(
        &self,
        query: &str,
        account_id: Option<&AccountId>,
        mailbox_id: Option<&MailboxId>,
        directory: &SearchDirectory,
        pagination: &Pagination,
    ) -> Result<IndexResults, DomainError> {
        if self.pending_count.load(Ordering::Relaxed) > 0 {
            self.commit_and_reload()?;
        }
        info!(query, ?account_id, ?mailbox_id, "searching");

        let searcher = self.reader.searcher();
        let (expr, parsed) = self.parse_query(query, account_id, mailbox_id, directory)?;
        let offset = ((pagination.page.max(1) - 1) * pagination.page_size) as usize;
        let wanted = offset + pagination.page_size as usize;
        let mut words = Vec::new();
        expr.positive_text(&mut words);
        let (found, total) = if words.is_empty() {
            // Filters alone give every hit the same score: newest first is
            // what a mailbox view means.
            let (top, total) = searcher
                .search(
                    &parsed,
                    &(
                        TopDocs::with_limit(wanted).order_by_fast_field::<i64>("date", Order::Desc),
                        Count,
                    ),
                )
                .map_err(|e| search_err("search", e))?;
            (
                top.into_iter().map(|(_, a)| (0.0, a)).collect::<Vec<_>>(),
                total,
            )
        } else {
            searcher
                .search(
                    &parsed,
                    &(TopDocs::with_limit(wanted).order_by_score(), Count),
                )
                .map_err(|e| search_err("search", e))?
        };

        let hits = found
            .into_iter()
            .skip(offset)
            .filter_map(|(score, address)| {
                let doc: TantivyDocument = searcher.doc(address).ok()?;
                let id = doc.get_first(self.fields.message_id)?.as_str()?;
                let message_id = MessageId::from_uuid(uuid::Uuid::parse_str(id).ok()?);
                Some(IndexHit { message_id, score })
            })
            .collect();
        Ok(IndexResults {
            hits,
            total_count: total as u64,
        })
    }

    // ponytail: parses the query and builds a generator per message; batch it
    // if result pages grow past a few dozen hits.
    fn snippet(&self, query: &str, message: &Message) -> Option<String> {
        let body = message.plain_body()?;
        let expr = query::parse(query, Utc::now()).ok()?;
        let mut words = Vec::new();
        expr.positive_text(&mut words);
        // Only the free text is highlighted; filters have nothing to show.
        let text = Expr::And(
            words
                .into_iter()
                .map(|w| Expr::Text(w.to_owned()))
                .collect(),
        );
        let directory = SearchDirectory::default();
        let builder = QueryBuilder {
            index: &self.index,
            fields: &self.fields,
            directory: &directory,
        };
        let parsed = builder.build(&text);
        let generator =
            SnippetGenerator::create(&self.reader.searcher(), &*parsed, self.fields.body).ok()?;
        let html = generator.snippet(&body).to_html();
        (!html.trim().is_empty()).then_some(html)
    }

    async fn flush(&self) -> Result<(), DomainError> {
        self.commit_and_reload()
    }
}

fn search_err(what: &str, e: impl std::fmt::Display) -> DomainError {
    DomainError::SearchIndex(format!("{what} failed: {e}"))
}

#[cfg(test)]
mod tests;
