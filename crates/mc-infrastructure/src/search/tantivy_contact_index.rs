//! Tantivy-backed contact search index for autocomplete.
//!
//! # Schema
//! - `contact_id`    : STRING | STORED — UUID, used for upsert/delete
//! - `display_name`  : text (en_stem, STORED) — "Mario Rossi"
//! - `primary_email` : text (email_tokenizer, STORED) — "mario.rossi@example.com"
//! - `all_emails`    : text (email_tokenizer, STORED) — space-joined all addresses
//! - `organization`  : text (en_stem, STORED) — "Acme Corp"
//! - `account_scope` : STRING | STORED — UUID of the owning account, empty = shared
//!
//! # Autocomplete strategy
//! A regex query `{prefix}.*` runs against the tokenised term dictionary of
//! `display_name`, `primary_email`, and `all_emails`. This gives sub-millisecond
//! prefix matching without an additional ngram index.
//!
//! # Account scoping
//! When `requesting_account_id` is given, a two-phase search is performed:
//! 1. Fetch up to `limit * 4` candidates via tantivy.
//! 2. Partition into own-account and cross-account.
//! 3. Return own-account first; append cross-account only if own results are
//!    insufficient to fill `limit`.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use tantivy::collector::TopDocs;
use tantivy::query::{AllQuery, BooleanQuery, Occur, RegexQuery};
use tantivy::schema::{Field, IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing, TextOptions, Value};
use tantivy::tokenizer::TextAnalyzer;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};
use tracing::{debug, info};

use mc_domain::error::DomainError;
use mc_domain::model::account::AccountId;
use mc_domain::traits::contact_index::{ContactIndex, ContactSearchHit};
use mc_domain::traits::repository::ContactSummary;
use mc_domain::value_objects::contact_id::ContactId;

const AUTO_COMMIT_THRESHOLD: u32 = 50;

/// Tantivy-backed contact search/autocomplete index.
pub struct TantivyContactIndex {
    #[allow(dead_code)]
    index: Arc<Index>,
    reader: IndexReader,
    writer: Arc<parking_lot::Mutex<IndexWriter>>,
    fields: ContactFields,
    pending_count: AtomicU32,
}

#[derive(Clone)]
struct ContactFields {
    contact_id: Field,
    display_name: Field,
    primary_email: Field,
    all_emails: Field,
    organization: Field,
    account_scope: Field,
}

impl TantivyContactIndex {
    pub fn new<P: AsRef<Path>>(index_path: P) -> Result<Self, DomainError> {
        let schema = Self::build_schema();
        let meta_path = index_path.as_ref().join("meta.json");
        let index = if meta_path.exists() {
            Index::open_in_dir(&index_path)
                .map_err(|e| DomainError::SearchIndex(format!("open contact index: {e}")))?
        } else {
            std::fs::create_dir_all(&index_path).map_err(|e| {
                DomainError::SearchIndex(format!("create contact index dir: {e}"))
            })?;
            Index::create_in_dir(&index_path, schema.clone())
                .map_err(|e| DomainError::SearchIndex(format!("create contact index: {e}")))?
        };

        // Register "email_tokenizer": splits on @, dots, whitespace, lowercased
        let email_analyzer = TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
            .filter(tantivy::tokenizer::LowerCaser)
            .build();
        index.tokenizers().register("email_tokenizer", email_analyzer);

        let writer = index
            .writer(15_000_000)
            .map_err(|e| DomainError::SearchIndex(format!("contact index writer: {e}")))?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| DomainError::SearchIndex(format!("contact index reader: {e}")))?;

        let fields = ContactFields {
            contact_id: schema.get_field("contact_id").unwrap(),
            display_name: schema.get_field("display_name").unwrap(),
            primary_email: schema.get_field("primary_email").unwrap(),
            all_emails: schema.get_field("all_emails").unwrap(),
            organization: schema.get_field("organization").unwrap(),
            account_scope: schema.get_field("account_scope").unwrap(),
        };

        info!("TantivyContactIndex initialized");

        Ok(Self {
            index: Arc::new(index),
            reader,
            writer: Arc::new(parking_lot::Mutex::new(writer)),
            fields,
            pending_count: AtomicU32::new(0),
        })
    }

    fn build_schema() -> Schema {
        let mut b = Schema::builder();

        b.add_text_field("contact_id", STRING | STORED);
        b.add_text_field("account_scope", STRING | STORED);

        let text_opts = TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("en_stem")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored();

        let email_opts = TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("email_tokenizer")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored();

        b.add_text_field("display_name", text_opts.clone());
        b.add_text_field("primary_email", email_opts.clone());
        b.add_text_field("all_emails", email_opts);
        b.add_text_field("organization", text_opts);

        b.build()
    }

    fn summary_to_doc(&self, summary: &ContactSummary) -> TantivyDocument {
        let mut doc = TantivyDocument::new();
        doc.add_text(self.fields.contact_id, summary.id.to_string());
        doc.add_text(
            self.fields.account_scope,
            summary.account_scope.as_ref().map(|a| a.to_string()).unwrap_or_default(),
        );
        doc.add_text(self.fields.display_name, &summary.display_name);
        doc.add_text(self.fields.primary_email, summary.primary_email.as_str());

        let all = summary
            .emails
            .iter()
            .map(|e| e.address.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        doc.add_text(self.fields.all_emails, &all);

        if let Some(ref org) = summary.organization {
            doc.add_text(self.fields.organization, org.as_str());
        }

        doc
    }

    fn commit_and_reload(&self) -> Result<(), DomainError> {
        let mut w = self.writer.lock();
        w.commit()
            .map_err(|e| DomainError::SearchIndex(format!("contact index commit: {e}")))?;
        drop(w);
        self.pending_count.store(0, Ordering::Relaxed);
        self.reader
            .reload()
            .map_err(|e| DomainError::SearchIndex(format!("contact index reload: {e}")))?;
        Ok(())
    }

    /// Read all stored fields from a tantivy document into a `ContactSearchHit`.
    /// `requesting_scope` is the account_scope string of the requester (used to set
    /// `is_cross_account`).
    fn doc_to_hit(
        &self,
        doc: &TantivyDocument,
        requesting_scope: Option<&str>,
    ) -> Option<ContactSearchHit> {
        let id_str = doc.get_first(self.fields.contact_id).and_then(|v| v.as_str())?;
        let id = uuid::Uuid::parse_str(id_str)
            .map(mc_domain::ContactId::from_uuid)
            .ok()?;

        let display_name = doc
            .get_first(self.fields.display_name)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let primary_email = doc
            .get_first(self.fields.primary_email)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let all_emails: Vec<String> = doc
            .get_first(self.fields.all_emails)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .split_whitespace()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        let organization = doc
            .get_first(self.fields.organization)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        let scope_str = doc
            .get_first(self.fields.account_scope)
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let account_scope = if scope_str.is_empty() {
            None
        } else {
            uuid::Uuid::parse_str(scope_str)
                .map(AccountId::from_uuid)
                .ok()
        };

        let is_cross_account = match requesting_scope {
            Some(req) if !req.is_empty() => scope_str != req,
            _ => false,
        };

        Some(ContactSearchHit {
            id,
            display_name,
            primary_email,
            all_emails,
            organization,
            account_scope,
            is_cross_account,
        })
    }
}

#[async_trait]
impl ContactIndex for TantivyContactIndex {
    async fn index_contact(&self, summary: &ContactSummary) -> Result<(), DomainError> {
        // Delete existing document for this contact (upsert)
        let term = tantivy::Term::from_field_text(
            self.fields.contact_id,
            &summary.id.to_string(),
        );
        {
            let w = self.writer.lock();
            w.delete_term(term);
            let doc = self.summary_to_doc(summary);
            w.add_document(doc)
                .map_err(|e| DomainError::SearchIndex(format!("contact index add: {e}")))?;
        }

        let pending = self.pending_count.fetch_add(1, Ordering::Relaxed) + 1;
        if pending >= AUTO_COMMIT_THRESHOLD {
            self.commit_and_reload()?;
        }
        Ok(())
    }

    async fn remove_contact(&self, id: &ContactId) -> Result<(), DomainError> {
        let term = tantivy::Term::from_field_text(self.fields.contact_id, &id.to_string());
        {
            let mut w = self.writer.lock();
            w.delete_term(term);
            w.commit()
                .map_err(|e| DomainError::SearchIndex(format!("contact index commit: {e}")))?;
        }
        self.reader
            .reload()
            .map_err(|e| DomainError::SearchIndex(format!("contact index reload: {e}")))?;
        Ok(())
    }

    async fn search_contacts(
        &self,
        query: &str,
        limit: usize,
        requesting_account_id: Option<&AccountId>,
    ) -> Result<Vec<ContactSearchHit>, DomainError> {
        if query.trim().is_empty() {
            return Ok(vec![]);
        }

        // Flush pending before searching
        if self.pending_count.load(Ordering::Relaxed) > 0 {
            self.commit_and_reload()?;
        }

        let searcher = self.reader.searcher();
        let prefix = regex_escape(&query.trim().to_lowercase());

        let mut subqueries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();
        for field in [
            self.fields.display_name,
            self.fields.primary_email,
            self.fields.all_emails,
        ] {
            if let Ok(rq) = RegexQuery::from_pattern(&format!("{prefix}.*"), field) {
                subqueries.push((Occur::Should, Box::new(rq)));
            }
        }

        if subqueries.is_empty() {
            return Ok(vec![]);
        }

        let bool_query = BooleanQuery::new(subqueries);

        // Fetch more candidates than needed so we can do account-scoped partitioning
        let fetch_limit = limit * 4;
        let top_docs = searcher
            .search(&bool_query, &TopDocs::with_limit(fetch_limit).order_by_score())
            .map_err(|e| DomainError::SearchIndex(format!("contact search failed: {e}")))?;

        let requesting_scope = requesting_account_id.map(|a| a.to_string());
        let req_scope_str = requesting_scope.as_deref();

        let mut own_hits: Vec<ContactSearchHit> = Vec::new();
        let mut cross_hits: Vec<ContactSearchHit> = Vec::new();

        for (_score, addr) in &top_docs {
            let doc: TantivyDocument = searcher
                .doc(*addr)
                .map_err(|e| DomainError::SearchIndex(format!("contact doc fetch: {e}")))?;

            if let Some(mut hit) = self.doc_to_hit(&doc, req_scope_str) {
                if hit.is_cross_account {
                    cross_hits.push(hit);
                } else {
                    hit.is_cross_account = false;
                    own_hits.push(hit);
                }
            }
        }

        // Own-account first; cross-account only if we have room left
        let mut results = Vec::with_capacity(limit);
        results.extend(own_hits.into_iter().take(limit));
        if results.len() < limit {
            let remaining = limit - results.len();
            results.extend(cross_hits.into_iter().take(remaining));
        }

        debug!("contact search '{}': {} hits", query, results.len());
        Ok(results)
    }

    async fn find_duplicate_candidates(
        &self,
        account_id: Option<&AccountId>,
    ) -> Result<Vec<(ContactSearchHit, ContactSearchHit)>, DomainError> {
        // Flush pending
        if self.pending_count.load(Ordering::Relaxed) > 0 {
            self.commit_and_reload()?;
        }

        let searcher = self.reader.searcher();
        let top_docs = searcher
            .search(&AllQuery, &TopDocs::with_limit(10_000).order_by_score())
            .map_err(|e| DomainError::SearchIndex(format!("contact dup scan failed: {e}")))?;

        let scope_filter = account_id.map(|a| a.to_string());

        // Collect all hits (no cross-account marking needed here)
        let mut all_hits: Vec<ContactSearchHit> = Vec::new();
        for (_score, addr) in &top_docs {
            let doc: TantivyDocument = searcher
                .doc(*addr)
                .map_err(|e| DomainError::SearchIndex(format!("contact doc fetch: {e}")))?;

            if let Some(hit) = self.doc_to_hit(&doc, None) {
                // Filter by account if requested
                if let Some(ref scope) = scope_filter {
                    let hit_scope = hit.account_scope.as_ref().map(|a| a.to_string());
                    if hit_scope.as_deref() != Some(scope.as_str()) {
                        continue;
                    }
                }
                all_hits.push(hit);
            }
        }

        // Build email → Vec<index> map
        use std::collections::HashMap;
        let mut email_map: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, hit) in all_hits.iter().enumerate() {
            for email in &hit.all_emails {
                email_map.entry(email.clone()).or_default().push(i);
            }
        }

        // Collect unique pairs (avoid duplicates with a BTreeSet of sorted id pairs)
        use std::collections::BTreeSet;
        let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
        let mut pairs: Vec<(ContactSearchHit, ContactSearchHit)> = Vec::new();

        for indices in email_map.values() {
            if indices.len() < 2 {
                continue;
            }
            for i in 0..indices.len() {
                for j in (i + 1)..indices.len() {
                    let a = &all_hits[indices[i]];
                    let b = &all_hits[indices[j]];
                    let key = if a.id.to_string() < b.id.to_string() {
                        (a.id.to_string(), b.id.to_string())
                    } else {
                        (b.id.to_string(), a.id.to_string())
                    };
                    if seen.insert(key) {
                        pairs.push((a.clone(), b.clone()));
                    }
                }
            }
        }

        Ok(pairs)
    }
}

fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for ch in s.chars() {
        if "\\^$.|?*+()[]{}".contains(ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}
