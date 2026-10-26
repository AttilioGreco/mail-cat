use std::borrow::Cow;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{Query, QueryParser};
use tantivy::schema::{
    FAST, Field, INDEXED, IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing,
    TextOptions, Value,
};
use tantivy::snippet::SnippetGenerator;
use tantivy::tokenizer::TextAnalyzer;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};
use tracing::{debug, info, warn};

use mc_domain::error::DomainError;
use mc_domain::model::account::AccountId;
use mc_domain::model::mailbox::MailboxId;
use mc_domain::model::message::Message;
use mc_domain::traits::search_index::{IndexHit, IndexResults, SearchIndex};
use mc_domain::value_objects::flag::Flag;
use mc_domain::value_objects::message_id::MessageId;
use mc_shared::pagination::Pagination;

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

struct IndexFields {
    message_id: Field,
    account_id: Field,
    mailbox_id: Field,
    date: Field,
    flags: Field,
    has_attachment: Field,
    subject: Field,
    from_name: Field,
    body: Field,
    from_email: Field,
    to: Field,
    cc: Field,
}

impl TantivySearchIndex {
    /// Opens the index at `index_path`, creating it — or recreating it when it
    /// was built with another schema. Check [`Self::was_recreated`] afterwards:
    /// a new index is empty and must be refilled from the event log.
    pub fn new<P: AsRef<Path>>(index_path: P) -> Result<Self, DomainError> {
        let path = index_path.as_ref();
        let schema = Self::build_schema();
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
                    std::fs::remove_dir_all(path).map_err(|e| search_err("remove stale index", e))?;
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
        index.tokenizers().register("email_tokenizer", email_analyzer);

        let writer = index
            .writer_with_num_threads(1, WRITER_MEMORY_BYTES)
            .map_err(|e| search_err("create writer", e))?;
        // Manual reload: the reader is reloaded explicitly after each commit.
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| search_err("create reader", e))?;
        let field = |name| schema.get_field(name).expect("field is in build_schema");
        let fields = IndexFields {
            message_id: field("message_id"),
            account_id: field("account_id"),
            mailbox_id: field("mailbox_id"),
            date: field("date"),
            flags: field("flags"),
            has_attachment: field("has_attachment"),
            subject: field("subject"),
            from_name: field("from_name"),
            body: field("body"),
            from_email: field("from_email"),
            to: field("to"),
            cc: field("cc"),
        };

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

    fn build_schema() -> Schema {
        let mut builder = Schema::builder();
        let text = |tokenizer| {
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(tokenizer)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
        };

        builder.add_text_field("message_id", STRING | STORED);
        builder.add_text_field("account_id", STRING | FAST);
        builder.add_text_field("mailbox_id", STRING | FAST);
        builder.add_i64_field("date", INDEXED | FAST);
        builder.add_u64_field("flags", FAST);
        builder.add_bool_field("has_attachment", FAST);
        for name in ["subject", "from_name", "body"] {
            builder.add_text_field(name, text("en_stem"));
        }
        for name in ["from_email", "to", "cc"] {
            builder.add_text_field(name, text("email_tokenizer"));
        }
        builder.build()
    }

    fn message_to_document(&self, message: &Message) -> TantivyDocument {
        let f = &self.fields;
        let e = &message.envelope;
        let join = |names: Vec<&str>| names.join(" ");
        let mut doc = TantivyDocument::new();
        doc.add_text(f.message_id, message.id.to_string());
        doc.add_text(f.account_id, message.account_id.to_string());
        for membership in &message.memberships {
            doc.add_text(f.mailbox_id, membership.mailbox_id.to_string());
        }
        doc.add_i64(f.date, e.date.timestamp());
        doc.add_u64(f.flags, flag_bits(&message.flags));
        doc.add_bool(f.has_attachment, message.has_attachments);
        doc.add_text(f.subject, &e.subject);
        doc.add_text(f.from_name, join(e.from.iter().filter_map(|a| a.name.as_deref()).collect()));
        doc.add_text(f.from_email, join(e.from.iter().map(|a| a.email.as_str()).collect()));
        doc.add_text(f.to, join(e.to.iter().map(|a| a.email.as_str()).collect()));
        doc.add_text(f.cc, join(e.cc.iter().map(|a| a.email.as_str()).collect()));
        if let Some(body) = body_of(message) {
            doc.add_text(f.body, body);
        }
        doc
    }

    fn parse_query(
        &self,
        query: &str,
        account_id: Option<&AccountId>,
        mailbox_id: Option<&MailboxId>,
    ) -> Result<Box<dyn Query>, DomainError> {
        let f = &self.fields;
        let parser = QueryParser::for_index(
            &self.index,
            vec![f.subject, f.from_name, f.from_email, f.to, f.cc, f.body],
        );
        // Parentheses keep the user's ORs from swallowing the scope filters.
        let mut query = format!("({})", rewrite_scoped_query(query));
        if let Some(account_id) = account_id {
            query.push_str(&format!(" AND account_id:\"{account_id}\""));
        }
        if let Some(mailbox_id) = mailbox_id {
            query.push_str(&format!(" AND mailbox_id:\"{mailbox_id}\""));
        }
        parser.parse_query(&query).map_err(|e| search_err("parse query", e))
    }

    fn commit_and_reload(&self) -> Result<(), DomainError> {
        self.writer.lock().commit().map_err(|e| search_err("commit", e))?;
        self.pending_count.store(0, Ordering::Relaxed);
        self.reader.reload().map_err(|e| search_err("reload reader", e))
    }
}

#[async_trait]
impl SearchIndex for TantivySearchIndex {
    async fn index_message(&self, message: &Message) -> Result<(), DomainError> {
        debug!("Indexing message: {}", message.id);
        let term = tantivy::Term::from_field_text(self.fields.message_id, &message.id.to_string());
        {
            let writer = self.writer.lock();
            writer.delete_term(term);
            writer
                .add_document(self.message_to_document(message))
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

    async fn search(
        &self,
        query: &str,
        account_id: Option<&AccountId>,
        mailbox_id: Option<&MailboxId>,
        pagination: &Pagination,
    ) -> Result<IndexResults, DomainError> {
        if self.pending_count.load(Ordering::Relaxed) > 0 {
            self.commit_and_reload()?;
        }
        info!(query, ?account_id, ?mailbox_id, "searching");

        let searcher = self.reader.searcher();
        let parsed = self.parse_query(query, account_id, mailbox_id)?;
        let offset = ((pagination.page.max(1) - 1) * pagination.page_size) as usize;
        let limit = pagination.page_size as usize;
        let (top_docs, total) = searcher
            .search(&parsed, &(TopDocs::with_limit(offset + limit).order_by_score(), Count))
            .map_err(|e| search_err("search", e))?;

        let hits = top_docs
            .into_iter()
            .skip(offset)
            .filter_map(|(score, address)| {
                let doc: TantivyDocument = searcher.doc(address).ok()?;
                let id = doc.get_first(self.fields.message_id)?.as_str()?;
                let message_id = MessageId::from_uuid(uuid::Uuid::parse_str(id).ok()?);
                Some(IndexHit { message_id, score })
            })
            .collect();
        Ok(IndexResults { hits, total_count: total as u64 })
    }

    // ponytail: parses the query and builds a generator per message; batch it
    // if result pages grow past a few dozen hits.
    fn snippet(&self, query: &str, message: &Message) -> Option<String> {
        let body = body_of(message)?;
        let parsed = self.parse_query(query, None, None).ok()?;
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

/// The searchable body: the text part, or the tag-stripped HTML part when the
/// message has no text part (spec §5: one field instead of two).
fn body_of(message: &Message) -> Option<Cow<'_, str>> {
    match (&message.body_text, &message.body_html) {
        (Some(text), _) => Some(Cow::Borrowed(text)),
        (None, Some(html)) => Some(Cow::Owned(strip_html_for_index(html))),
        (None, None) => None,
    }
}

/// System flags as bits for the `flags` FAST field (`is:unread`, …). IMAP
/// keywords have no bit; filter on them in the read model if ever needed.
fn flag_bits(flags: &[Flag]) -> u64 {
    flags.iter().fold(0, |bits, flag| {
        bits | match flag {
            Flag::Seen => 1,
            Flag::Answered => 1 << 1,
            Flag::Flagged => 1 << 2,
            Flag::Deleted => 1 << 3,
            Flag::Draft => 1 << 4,
            Flag::Recent => 1 << 5,
            Flag::Custom(_) => 0,
        }
    })
}

/// Rewrite user-facing scope prefixes into Tantivy field queries.
///
/// - `from: segreteria@corona.it`  → `from_email:"segreteria@corona.it" OR from_name:"segreteria@corona.it"`
/// - `to: alice@example.com`       → `to:"alice@example.com"`
/// - `sbj: fattura`                → `subject:"fattura"`
/// - `hello world` (no prefix)     → `hello world` (searches all default fields)
fn rewrite_scoped_query(query: &str) -> String {
    let trimmed = query.trim();
    let lower = trimmed.to_lowercase();

    // "from: value" → search both from_email and from_name
    if lower.starts_with("from:") {
        let value = trimmed[5..].trim();
        if !value.is_empty() {
            return format!(
                "from_email:\"{}\" OR from_name:\"{}\"",
                value, value
            );
        }
    }
    // "to: value" → search to field
    if lower.starts_with("to:") {
        let value = trimmed[3..].trim();
        if !value.is_empty() {
            return format!("to:\"{}\"", value);
        }
    }
    // "sbj: value" → search subject field
    if lower.starts_with("sbj:") {
        let value = trimmed[4..].trim();
        if !value.is_empty() {
            return format!("subject:\"{}\"", value);
        }
    }

    // No known prefix — return as-is for full-text across all default fields
    trimmed.to_string()
}

/// Strip HTML tags for indexing (simple tag removal).
fn strip_html_for_index(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    for ch in html.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests;
