use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, QueryParser, RegexQuery};
use tantivy::schema::{Field, IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing, TextOptions, Value};
use tantivy::snippet::SnippetGenerator;
use tantivy::tokenizer::TextAnalyzer;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};
use tracing::{debug, info};

use mc_domain::error::DomainError;
use mc_domain::model::account::AccountId;
use mc_domain::model::mailbox::MailboxId;
use mc_domain::model::message::Message;
use mc_domain::traits::search_index::{SearchHit, SearchIndex, SearchResults};
use mc_domain::value_objects::message_id::MessageId;
use mc_shared::pagination::Pagination;

/// How many documents to buffer before auto-committing.
const AUTO_COMMIT_THRESHOLD: u32 = 100;

/// Tantivy-based search index for email messages.
///
/// Schema fields:
/// - message_id: UUID (stored, exact match)
/// - account_id: UUID (stored, exact match for filtering)
/// - mailbox_id: UUID (stored, exact match for filtering)
/// - subject: text (full-text with stemming, stored for display)
/// - from_name: text (full-text with stemming, stored)
/// - from_email: text (full-text, stored)
/// - to: text (full-text, stored)
/// - body_text: text (full-text with stemming, stored for snippets)
/// - body_html: text (full-text with stemming, not stored)
/// - date: i64 timestamp (stored, for sorting)
pub struct TantivySearchIndex {
    index: Arc<Index>,
    reader: IndexReader,
    writer: Arc<parking_lot::Mutex<IndexWriter>>,
    fields: IndexFields,
    pending_count: AtomicU32,
}

#[derive(Clone)]
struct IndexFields {
    message_id: Field,
    account_id: Field,
    mailbox_id: Field,
    subject: Field,
    from_name: Field,
    from_email: Field,
    to: Field,
    body_text: Field,
    body_html: Field,
    date: Field,
}

impl TantivySearchIndex {
    /// Create a new Tantivy search index at the given path.
    pub fn new<P: AsRef<Path>>(index_path: P) -> Result<Self, DomainError> {
        let schema = Self::build_schema();

        // Check if index already exists (meta.json is the marker file)
        let meta_path = index_path.as_ref().join("meta.json");
        let index = if meta_path.exists() {
            Index::open_in_dir(index_path)
                .map_err(|e| DomainError::SearchIndex(format!("Failed to open index: {}", e)))?
        } else {
            std::fs::create_dir_all(&index_path).map_err(|e| {
                DomainError::SearchIndex(format!("Failed to create index dir: {}", e))
            })?;
            Index::create_in_dir(index_path, schema.clone())
                .map_err(|e| DomainError::SearchIndex(format!("Failed to create index: {}", e)))?
        };

        // Register the custom "email_tokenizer" for indexing email-like content
        Self::register_tokenizers(&index);

        // Create writer with 50MB heap
        let writer = index
            .writer(50_000_000)
            .map_err(|e| DomainError::SearchIndex(format!("Failed to create writer: {}", e)))?;

        // Create reader with manual reload (we'll reload explicitly after commits)
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| DomainError::SearchIndex(format!("Failed to create reader: {}", e)))?;

        let fields = IndexFields {
            message_id: schema.get_field("message_id").unwrap(),
            account_id: schema.get_field("account_id").unwrap(),
            mailbox_id: schema.get_field("mailbox_id").unwrap(),
            subject: schema.get_field("subject").unwrap(),
            from_name: schema.get_field("from_name").unwrap(),
            from_email: schema.get_field("from_email").unwrap(),
            to: schema.get_field("to").unwrap(),
            body_text: schema.get_field("body_text").unwrap(),
            body_html: schema.get_field("body_html").unwrap(),
            date: schema.get_field("date").unwrap(),
        };

        info!("TantivySearchIndex initialized");

        Ok(Self {
            index: Arc::new(index),
            reader,
            writer: Arc::new(parking_lot::Mutex::new(writer)),
            fields,
            pending_count: AtomicU32::new(0),
        })
    }

    /// Register custom tokenizers on the index.
    fn register_tokenizers(index: &Index) {
        // "email_tokenizer": splits on @, dots, whitespace — good for email addresses
        let email_analyzer = TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
            .filter(tantivy::tokenizer::LowerCaser)
            .build();
        index
            .tokenizers()
            .register("email_tokenizer", email_analyzer);
    }

    /// Build the Tantivy schema for email messages.
    fn build_schema() -> Schema {
        let mut schema_builder = Schema::builder();

        // IDs — stored and indexed as exact (non-tokenized) strings for filtering
        schema_builder.add_text_field("message_id", STRING | STORED);
        schema_builder.add_text_field("account_id", STRING | STORED);
        schema_builder.add_text_field("mailbox_id", STRING | STORED);

        // Subject — full text search with default tokenizer (lowercase + stemming via en_stem)
        let text_opts = TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("en_stem")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored();
        schema_builder.add_text_field("subject", text_opts.clone());

        // From name — full text search with stemming, stored
        schema_builder.add_text_field("from_name", text_opts.clone());

        // From email — tokenized for email-style search, stored
        let email_opts = TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("email_tokenizer")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored();
        schema_builder.add_text_field("from_email", email_opts.clone());

        // To — email addresses
        schema_builder.add_text_field("to", email_opts);

        // Body text — full text with stemming, STORED for snippet generation
        schema_builder.add_text_field("body_text", text_opts.clone());

        // Body HTML — full text with stemming, NOT stored (just searchable)
        let text_not_stored = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("en_stem")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        );
        schema_builder.add_text_field("body_html", text_not_stored);

        // Date for sorting
        schema_builder.add_i64_field("date", STORED);

        schema_builder.build()
    }

    /// Convert a Message to a Tantivy document.
    fn message_to_document(&self, message: &Message) -> TantivyDocument {
        let mut doc = TantivyDocument::new();

        // IDs
        doc.add_text(self.fields.message_id, message.id.to_string());
        doc.add_text(self.fields.account_id, message.account_id.to_string());
        doc.add_text(self.fields.mailbox_id, message.mailbox_id.to_string());

        // Subject
        doc.add_text(self.fields.subject, &message.envelope.subject);

        // From — name and email separately for better search
        let from_names: Vec<String> = message
            .envelope
            .from
            .iter()
            .filter_map(|addr| addr.name.clone())
            .collect();
        doc.add_text(self.fields.from_name, from_names.join(" "));

        let from_emails: Vec<&str> = message
            .envelope
            .from
            .iter()
            .map(|addr| addr.email.as_str())
            .collect();
        doc.add_text(self.fields.from_email, from_emails.join(" "));

        // To
        let to_text: Vec<&str> = message
            .envelope
            .to
            .iter()
            .map(|addr| addr.email.as_str())
            .collect();
        doc.add_text(self.fields.to, to_text.join(" "));

        // Body
        if let Some(ref body_text) = message.body_text {
            doc.add_text(self.fields.body_text, body_text);
        }
        if let Some(ref body_html) = message.body_html {
            // Strip HTML tags before indexing for cleaner full-text search
            let stripped = strip_html_for_index(body_html);
            doc.add_text(self.fields.body_html, stripped);
        }

        // Date
        doc.add_i64(self.fields.date, message.envelope.date.timestamp());

        doc
    }

    /// Commit pending changes and reload reader.
    fn commit_and_reload(&self) -> Result<(), DomainError> {
        let mut writer = self.writer.lock();
        writer
            .commit()
            .map_err(|e| DomainError::SearchIndex(format!("Commit failed: {}", e)))?;
        drop(writer);

        self.pending_count.store(0, Ordering::Relaxed);

        self.reader
            .reload()
            .map_err(|e| DomainError::SearchIndex(format!("Reader reload failed: {}", e)))?;
        Ok(())
    }
}

#[async_trait]
impl SearchIndex for TantivySearchIndex {
    async fn index_message(&self, message: &Message) -> Result<(), DomainError> {
        debug!("Indexing message: {}", message.id);

        // Delete existing document (if any) for idempotency
        let term = tantivy::Term::from_field_text(self.fields.message_id, &message.id.to_string());

        let writer = self.writer.lock();
        writer.delete_term(term);

        // Add new document
        let doc = self.message_to_document(message);
        writer
            .add_document(doc)
            .map_err(|e| DomainError::SearchIndex(format!("Add document failed: {}", e)))?;
        drop(writer);

        // Auto-commit every N documents for batching efficiency during sync
        let pending = self.pending_count.fetch_add(1, Ordering::Relaxed) + 1;
        if pending >= AUTO_COMMIT_THRESHOLD {
            self.commit_and_reload()?;
            debug!("Auto-committed after {} documents", pending);
        }

        Ok(())
    }

    async fn remove_message(&self, message_id: &MessageId) -> Result<(), DomainError> {
        debug!("Removing message: {}", message_id);
        let mut writer = self.writer.lock();
        let term = tantivy::Term::from_field_text(self.fields.message_id, &message_id.to_string());
        writer.delete_term(term);
        writer
            .commit()
            .map_err(|e| DomainError::SearchIndex(format!("Commit failed: {}", e)))?;
        drop(writer);
        self.reader
            .reload()
            .map_err(|e| DomainError::SearchIndex(format!("Reader reload failed: {}", e)))?;
        Ok(())
    }

    async fn search(
        &self,
        query: &str,
        account_id: Option<&AccountId>,
        mailbox_id: Option<&MailboxId>,
        pagination: &Pagination,
    ) -> Result<SearchResults, DomainError> {
        // Flush any pending documents before searching
        if self.pending_count.load(Ordering::Relaxed) > 0 {
            self.commit_and_reload()?;
        }

        info!(
            "Searching: query='{}', account_id={:?}, mailbox_id={:?}",
            query, account_id, mailbox_id
        );

        let searcher = self.reader.searcher();

        // Rewrite user-facing scope prefixes (from:, to:, sbj:) into Tantivy field names.
        // "from: foo" → search from_email AND from_name for "foo"
        // "to: foo"   → search to for "foo"
        // "sbj: foo"  → search subject for "foo"
        // No prefix   → search all default fields
        let rewritten = rewrite_scoped_query(query);

        // Build query parser for searchable fields
        let query_parser = QueryParser::for_index(
            &self.index,
            vec![
                self.fields.subject,
                self.fields.from_name,
                self.fields.from_email,
                self.fields.to,
                self.fields.body_text,
                self.fields.body_html,
            ],
        );

        // Build query with optional account/mailbox filters.
        // Wrap the user query in parentheses to preserve OR precedence
        // e.g. (from_email:"x" OR from_name:"x") AND account_id:"y"
        let query_str = if account_id.is_some() || mailbox_id.is_some() {
            let mut q = format!("({})", rewritten);
            if let Some(account_id) = account_id {
                q.push_str(&format!(" AND account_id:\"{}\"", account_id));
            }
            if let Some(mailbox_id) = mailbox_id {
                q.push_str(&format!(" AND mailbox_id:\"{}\"", mailbox_id));
            }
            q
        } else {
            rewritten
        };

        let parsed_query = query_parser
            .parse_query(&query_str)
            .map_err(|e| DomainError::SearchIndex(format!("Query parse failed: {}", e)))?;

        // Calculate offset and limit
        let offset = ((pagination.page - 1) * pagination.page_size) as usize;
        let limit = pagination.page_size as usize;

        // Search with TopDocs collector
        let top_docs = searcher
            .search(&parsed_query, &TopDocs::with_limit(offset + limit).order_by_score())
            .map_err(|e| DomainError::SearchIndex(format!("Search failed: {}", e)))?;

        // Count total hits
        let total_count = searcher
            .search(&parsed_query, &tantivy::collector::Count)
            .map_err(|e| DomainError::SearchIndex(format!("Count failed: {}", e)))?;

        // Create snippet generator from body_text field
        let snippet_generator =
            SnippetGenerator::create(&searcher, &parsed_query, self.fields.body_text)
                .ok();

        // Extract results with pagination
        let hits: Vec<SearchHit> = top_docs
            .iter()
            .skip(offset)
            .take(limit)
            .filter_map(|(score, doc_address)| {
                let doc: TantivyDocument = searcher.doc(*doc_address).ok()?;

                let message_id_str = doc.get_first(self.fields.message_id)?.as_str()?;
                let uuid = uuid::Uuid::parse_str(message_id_str).ok()?;
                let message_id = MessageId::from_uuid(uuid);

                // Read stored envelope fields
                let subject = doc.get_first(self.fields.subject)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let from_name = doc.get_first(self.fields.from_name)
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string());
                let from_email = doc.get_first(self.fields.from_email)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let date = doc.get_first(self.fields.date)
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);

                // Generate snippet from body_text
                let snippet = snippet_generator
                    .as_ref()
                    .and_then(|sg| {
                        let s = sg.snippet_from_doc(&doc);
                        let html = s.to_html();
                        if html.trim().is_empty() { None } else { Some(html) }
                    });

                Some(SearchHit {
                    message_id,
                    score: *score,
                    snippet,
                    subject,
                    from_name,
                    from_email,
                    date,
                })
            })
            .collect();

        info!(
            "Search returned {} hits (total: {})",
            hits.len(),
            total_count
        );

        Ok(SearchResults {
            hits,
            total_count: total_count as u64,
        })
    }

    async fn suggest(
        &self,
        prefix: &str,
        scope: Option<mc_domain::SuggestScope>,
        max_per_field: usize,
    ) -> Result<Vec<mc_domain::Suggestion>, DomainError> {
        use mc_domain::SuggestScope;
        use std::collections::HashMap;

        if prefix.is_empty() {
            return Ok(vec![]);
        }

        // Flush pending writes
        if self.pending_count.load(Ordering::Relaxed) > 0 {
            self.commit_and_reload()?;
        }

        let searcher = self.reader.searcher();
        let prefix_lower = prefix.to_lowercase();

        // Determine which fields to scan based on scope
        let fields_to_scan: Vec<(&str, Field)> = match scope {
            Some(SuggestScope::From) => vec![
                ("from_email", self.fields.from_email),
                ("from_name", self.fields.from_name),
            ],
            Some(SuggestScope::To) => vec![("to", self.fields.to)],
            Some(SuggestScope::Subject) => vec![("subject", self.fields.subject)],
            Some(SuggestScope::Body) => vec![("body_text", self.fields.body_text)],
            None => vec![
                ("from_email", self.fields.from_email),
                ("from_name", self.fields.from_name),
                ("to", self.fields.to),
                ("subject", self.fields.subject),
            ],
        };

        // Build a bool prefix query (like Elasticsearch match_bool_prefix):
        // For each field, create a RegexQuery matching terms that start with the prefix.
        // The tokenizers split emails into tokens ("meroni", "virgilio", "gmail", "com")
        // so the regex matches against individual tokens in the term dictionary.
        let escaped = regex_escape_prefix(&prefix_lower);
        let mut subqueries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();

        for (_, field) in &fields_to_scan {
            if let Ok(rq) = RegexQuery::from_pattern(&format!("{escaped}.*"), *field) {
                subqueries.push((Occur::Should, Box::new(rq)));
            }
        }

        if subqueries.is_empty() {
            return Ok(vec![]);
        }

        let bool_query = BooleanQuery::new(subqueries);

        // Fetch top N docs matching the prefix
        let top_docs = searcher
            .search(&bool_query, &TopDocs::with_limit(200).order_by_score())
            .map_err(|e| DomainError::SearchIndex(format!("Suggest search failed: {}", e)))?;

        // Aggregate individual values (not full stored text) from matching docs.
        // Email fields (from_email, to) store space-joined addresses — split them.
        // Subject is a single value per doc — keep as-is.
        // Body text is too long for suggestions — skip unless explicitly scoped.
        // key: (field_label, individual_value) → doc count
        let mut aggregations: HashMap<(String, String), u32> = HashMap::new();

        for (_score, doc_address) in &top_docs {
            let doc: TantivyDocument = searcher
                .doc(*doc_address)
                .map_err(|e| DomainError::SearchIndex(format!("Doc fetch failed: {}", e)))?;

            for (label, field) in &fields_to_scan {
                for value in doc.get_all(*field) {
                    if let Some(text) = value.as_str() {
                        if text.trim().is_empty() {
                            continue;
                        }

                        // Extract individual values depending on field type
                        let individual_values: Vec<&str> = match *label {
                            // Email fields: stored as "a@b.com c@d.com" — split on whitespace
                            "from_email" | "to" => text.split_whitespace().collect(),
                            // Name field: stored as "John Doe Jane Smith" — but names
                            // contain spaces so we can't split. Keep the whole value.
                            // Each doc typically has one sender anyway.
                            "from_name" => vec![text],
                            // Subject: single value per doc, keep as-is
                            "subject" => vec![text],
                            // Body: too long. Extract nothing for aggregation
                            // (body search works via full-text, not autocomplete)
                            "body_text" => continue,
                            _ => vec![text],
                        };

                        for val in individual_values {
                            let val_trimmed = val.trim();
                            if val_trimmed.is_empty() {
                                continue;
                            }
                            let val_lower = val_trimmed.to_lowercase();
                            if val_lower.contains(&prefix_lower) {
                                let key = (label.to_string(), val_trimmed.to_string());
                                *aggregations.entry(key).or_insert(0) += 1;
                            }
                        }
                    }
                }
            }
        }

        // Convert to Suggestion vec, sorted by count desc, limited per field
        let mut by_field: HashMap<String, Vec<mc_domain::Suggestion>> = HashMap::new();
        let mut all: Vec<((String, String), u32)> = aggregations.into_iter().collect();
        all.sort_by(|a, b| b.1.cmp(&a.1));

        for ((field, value), count) in all {
            let group = by_field.entry(field.clone()).or_default();
            if group.len() < max_per_field {
                group.push(mc_domain::Suggestion {
                    field,
                    value,
                    doc_count: count,
                });
            }
        }

        // Flatten in display order
        let field_order = ["from_email", "from_name", "to", "subject", "body_text"];
        let mut result = Vec::new();
        for f in &field_order {
            if let Some(mut suggestions) = by_field.remove(*f) {
                result.append(&mut suggestions);
            }
        }

        debug!("Suggest '{}': {} suggestions", prefix, result.len());
        Ok(result)
    }

    async fn flush(&self) -> Result<(), DomainError> {
        self.commit_and_reload()
    }

    async fn rebuild(&self, messages: Vec<Message>) -> Result<u64, DomainError> {
        info!("Rebuilding index with {} messages", messages.len());

        let mut writer = self.writer.lock();

        // Delete all documents
        writer
            .delete_all_documents()
            .map_err(|e| DomainError::SearchIndex(format!("Delete all failed: {}", e)))?;

        // Add all messages
        let mut count = 0u64;
        for message in messages {
            let doc = self.message_to_document(&message);
            writer
                .add_document(doc)
                .map_err(|e| DomainError::SearchIndex(format!("Add document failed: {}", e)))?;
            count += 1;

            // Commit every 1000 documents
            if count.is_multiple_of(1000) {
                writer
                    .commit()
                    .map_err(|e| DomainError::SearchIndex(format!("Commit failed: {}", e)))?;
                debug!("Indexed {} messages", count);
            }
        }

        // Final commit
        writer
            .commit()
            .map_err(|e| DomainError::SearchIndex(format!("Final commit failed: {}", e)))?;

        // Release lock before reloading reader
        drop(writer);

        // Force reader reload
        self.reader
            .reload()
            .map_err(|e| DomainError::SearchIndex(format!("Reader reload failed: {}", e)))?;

        info!("Rebuild complete: {} messages indexed", count);
        Ok(count)
    }
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

/// Escape regex metacharacters in a user-provided prefix so it can be
/// safely embedded in a `RegexQuery` pattern like `{prefix}.*`.
fn regex_escape_prefix(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for ch in s.chars() {
        if "\\^$.|?*+()[]{}".contains(ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
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
mod tests {
    use super::*;
    use chrono::Utc;
    use mc_domain::model::message::{Address, Envelope};
    use mc_domain::value_objects::email_address::EmailAddress;
    use tempfile::TempDir;

    fn create_test_message(id: &str, subject: &str, from: &str, body: &str) -> Message {
        let uuid = uuid::Uuid::parse_str(id).unwrap();
        Message {
            id: MessageId::from_uuid(uuid),
            account_id: AccountId::new(),
            mailbox_id: MailboxId::new(),
            provider_uid: None,
            message_rfc_id: None,
            in_reply_to: None,
            references: vec![],
            envelope: Envelope {
                subject: subject.to_string(),
                from: vec![Address {
                    name: Some("Test User".to_string()),
                    email: EmailAddress::new(from).unwrap(),
                }],
                to: vec![],
                cc: vec![],
                bcc: vec![],
                date: Utc::now(),
                reply_to: vec![],
            },
            body_text: Some(body.to_string()),
            body_html: None,
            flags: vec![],
            size_bytes: 1024,
            has_attachments: false,
            attachments: vec![],
            received_at: Utc::now(),
            internal_date: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_index_and_search() {
        let temp_dir = TempDir::new().unwrap();
        let index = TantivySearchIndex::new(temp_dir.path()).unwrap();

        let msg = create_test_message(
            "01234567-89ab-cdef-0123-456789abcdef",
            "Test Subject",
            "test@example.com",
            "This is a test message body",
        );

        // Index message
        index.index_message(&msg).await.unwrap();

        // Search
        let results = index
            .search(
                "test",
                None,
                None,
                &Pagination {
                    page: 1,
                    page_size: 10,
                },
            )
            .await
            .unwrap();

        assert_eq!(results.total_count, 1);
        assert_eq!(results.hits.len(), 1);
        assert_eq!(results.hits[0].message_id, msg.id);
    }

    #[tokio::test]
    async fn test_search_by_sender_name() {
        let temp_dir = TempDir::new().unwrap();
        let index = TantivySearchIndex::new(temp_dir.path()).unwrap();

        let msg = create_test_message(
            "01234567-89ab-cdef-0123-456789abcdef",
            "Hello",
            "john@example.com",
            "Some content",
        );

        index.index_message(&msg).await.unwrap();

        // Search by sender name (from_name field)
        let results = index
            .search("Test User", None, None, &Pagination::default())
            .await
            .unwrap();
        assert_eq!(results.total_count, 1);
    }

    #[tokio::test]
    async fn test_stemming_search() {
        let temp_dir = TempDir::new().unwrap();
        let index = TantivySearchIndex::new(temp_dir.path()).unwrap();

        let msg = create_test_message(
            "01234567-89ab-cdef-0123-456789abcdef",
            "Meeting invitation",
            "test@example.com",
            "We are running the weekly meetings every Tuesday",
        );

        index.index_message(&msg).await.unwrap();

        // "run" should match "running" via stemming
        let results = index
            .search("run", None, None, &Pagination::default())
            .await
            .unwrap();
        assert_eq!(results.total_count, 1);

        // "meeting" should match "meetings" via stemming
        let results = index
            .search("meeting", None, None, &Pagination::default())
            .await
            .unwrap();
        assert_eq!(results.total_count, 1);
    }

    #[tokio::test]
    async fn test_remove_message() {
        let temp_dir = TempDir::new().unwrap();
        let index = TantivySearchIndex::new(temp_dir.path()).unwrap();

        let msg1 = create_test_message(
            "01234567-89ab-cdef-0123-456789abcdef",
            "Test Subject",
            "test@example.com",
            "Test body",
        );

        let msg2 = create_test_message(
            "11234567-89ab-cdef-0123-456789abcdef",
            "Another Subject",
            "another@example.com",
            "Another body",
        );

        // Index two messages
        index.index_message(&msg1).await.unwrap();
        index.index_message(&msg2).await.unwrap();

        let results = index
            .search("Subject", None, None, &Pagination::default())
            .await
            .unwrap();
        assert_eq!(results.hits.len(), 2);

        // Remove first message
        index.remove_message(&msg1.id).await.unwrap();

        let results = index
            .search("Subject", None, None, &Pagination::default())
            .await
            .unwrap();

        // Should only find msg2
        assert_eq!(results.hits.len(), 1);
        assert_eq!(results.hits[0].message_id, msg2.id);
    }

    fn create_test_message_full(
        id: &str,
        subject: &str,
        from_name: &str,
        from_email: &str,
        to_emails: &[&str],
        body: &str,
    ) -> Message {
        let uuid = uuid::Uuid::parse_str(id).unwrap();
        Message {
            id: MessageId::from_uuid(uuid),
            account_id: AccountId::new(),
            mailbox_id: MailboxId::new(),
            provider_uid: None,
            message_rfc_id: None,
            in_reply_to: None,
            references: vec![],
            envelope: Envelope {
                subject: subject.to_string(),
                from: vec![Address {
                    name: Some(from_name.to_string()),
                    email: EmailAddress::new(from_email).unwrap(),
                }],
                to: to_emails
                    .iter()
                    .map(|e| Address {
                        name: None,
                        email: EmailAddress::new(e).unwrap(),
                    })
                    .collect(),
                cc: vec![],
                bcc: vec![],
                date: Utc::now(),
                reply_to: vec![],
            },
            body_text: Some(body.to_string()),
            body_html: None,
            flags: vec![],
            size_bytes: 1024,
            has_attachments: false,
            attachments: vec![],
            received_at: Utc::now(),
            internal_date: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_suggest_aggregates_individual_emails() {
        let temp_dir = TempDir::new().unwrap();
        let index = TantivySearchIndex::new(temp_dir.path()).unwrap();

        // 3 messages from meroni, 1 from another person
        let msg1 = create_test_message_full(
            "01234567-89ab-cdef-0123-456789abcdef",
            "Fattura di gennaio",
            "Virgilio Meroni",
            "meroni.virgilio@gmail.com",
            &["me@example.com"],
            "In allegato la fattura",
        );
        let msg2 = create_test_message_full(
            "11234567-89ab-cdef-0123-456789abcdef",
            "Re: Fattura di gennaio",
            "Virgilio Meroni",
            "meroni.virgilio@gmail.com",
            &["me@example.com"],
            "Confermo ricezione",
        );
        let msg3 = create_test_message_full(
            "21234567-89ab-cdef-0123-456789abcdef",
            "Meeting domani",
            "Virgilio Meroni",
            "meroni.virgilio@gmail.com",
            &["me@example.com", "altro@azienda.it"],
            "Ci vediamo domani alle 10",
        );
        let msg4 = create_test_message_full(
            "31234567-89ab-cdef-0123-456789abcdef",
            "Progetto nuovo",
            "Marco Rossi",
            "marco.rossi@company.com",
            &["meroni.virgilio@gmail.com"],
            "Ciao Virgilio, ti scrivo per il progetto",
        );

        for msg in [&msg1, &msg2, &msg3, &msg4] {
            index.index_message(msg).await.unwrap();
        }

        // Search "mero" — should find meroni.virgilio@gmail.com as a single suggestion
        let suggestions = index.suggest("mero", None, 5).await.unwrap();

        // Should have from_email suggestion for "meroni.virgilio@gmail.com"
        let from_emails: Vec<_> = suggestions
            .iter()
            .filter(|s| s.field == "from_email")
            .collect();
        assert!(
            !from_emails.is_empty(),
            "Should have from_email suggestions, got: {:?}",
            suggestions
        );
        assert_eq!(from_emails[0].value, "meroni.virgilio@gmail.com");
        assert_eq!(from_emails[0].doc_count, 3); // 3 messages from meroni

        // Should also find it in "to" field (msg4 sends TO meroni)
        let to_results: Vec<_> = suggestions.iter().filter(|s| s.field == "to").collect();
        assert!(
            !to_results.is_empty(),
            "Should have 'to' suggestions, got: {:?}",
            suggestions
        );
        assert_eq!(to_results[0].value, "meroni.virgilio@gmail.com");
        assert_eq!(to_results[0].doc_count, 1);

        // Should have from_name suggestion for "Virgilio Meroni"
        let from_names: Vec<_> = suggestions
            .iter()
            .filter(|s| s.field == "from_name")
            .collect();
        assert!(
            !from_names.is_empty(),
            "Should have from_name suggestions, got: {:?}",
            suggestions
        );
        assert_eq!(from_names[0].value, "Virgilio Meroni");
    }

    #[tokio::test]
    async fn test_suggest_scoped_to_subject() {
        let temp_dir = TempDir::new().unwrap();
        let index = TantivySearchIndex::new(temp_dir.path()).unwrap();

        let msg1 = create_test_message_full(
            "01234567-89ab-cdef-0123-456789abcdef",
            "Fattura di gennaio",
            "Test User",
            "test@example.com",
            &["me@example.com"],
            "body content",
        );
        let msg2 = create_test_message_full(
            "11234567-89ab-cdef-0123-456789abcdef",
            "Fattura di febbraio",
            "Test User",
            "test@example.com",
            &["me@example.com"],
            "body content",
        );

        index.index_message(&msg1).await.unwrap();
        index.index_message(&msg2).await.unwrap();

        // Scoped to subject: "fattur" should find both subject lines
        let suggestions = index
            .suggest("fattur", Some(mc_domain::SuggestScope::Subject), 5)
            .await
            .unwrap();

        assert_eq!(suggestions.len(), 2, "Should find 2 subject suggestions: {:?}", suggestions);
        assert!(suggestions.iter().all(|s| s.field == "subject"));
    }

    #[tokio::test]
    async fn test_rebuild_index() {
        let temp_dir = TempDir::new().unwrap();
        let index = TantivySearchIndex::new(temp_dir.path()).unwrap();

        let messages = vec![
            create_test_message(
                "01234567-89ab-cdef-0123-456789abcdef",
                "First",
                "test1@example.com",
                "Body 1",
            ),
            create_test_message(
                "11234567-89ab-cdef-0123-456789abcdef",
                "Second",
                "test2@example.com",
                "Body 2",
            ),
            create_test_message(
                "21234567-89ab-cdef-0123-456789abcdef",
                "Third",
                "test3@example.com",
                "Body 3",
            ),
        ];

        let count = index.rebuild(messages).await.unwrap();
        assert_eq!(count, 3);

        let results = index
            .search("Body", None, None, &Pagination::default())
            .await
            .unwrap();
        assert_eq!(results.total_count, 3);
    }
}
