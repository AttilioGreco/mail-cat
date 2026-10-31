//! Embeddings in the background and the neighbours they give. Nothing here
//! runs unless the user agreed to it: the model is not even downloaded until
//! then.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use mc_domain::{Embedder, MailboxReadRepository, MessageId, Scope, TagSource, TaggingStore};

use super::TaggingService;
use super::knn::{Nearest, Pool};
use crate::bus::CommandBus;
use crate::error::ApplicationError;

/// Messages per batch, and the pause after one: the backlog is low priority.
const BATCH: u32 = 16;
const PAUSE: Duration = Duration::from_secs(2);
/// Wait before trying again after a failure (offline, disk full, ...).
const RETRY: Duration = Duration::from_secs(600);
/// A scope's vectors are read again after this; new examples show up then.
const POOL_TTL: Duration = Duration::from_secs(60);
/// Characters of the body that are embedded, about the model's token window.
const BODY_CHARS: usize = 1200;

/// What is embedded of a message: its subject and the start of its body.
pub(super) fn embedding_text(source: &TagSource) -> Option<String> {
    let body = source.body.as_deref().unwrap_or_default();
    let head = body
        .char_indices()
        .nth(BODY_CHARS)
        .map_or(body, |(i, _)| &body[..i]);
    let text = format!("{}\n{head}", source.subject);
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    (!text.is_empty()).then_some(text)
}

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    /// Whether the user agreed to download the model and embed their mail.
    pub fn embeddings_enabled(&self) -> bool {
        self.embedder.is_some() && self.embedding_on.load(Ordering::Relaxed)
    }

    /// The model's name, for display.
    pub fn embedding_model(&self) -> Option<&str> {
        self.embedder.as_deref().map(Embedder::model)
    }

    pub async fn set_embeddings(&self, enabled: bool) -> Result<(), ApplicationError> {
        self.store.set_embeddings_enabled(enabled).await?;
        self.embedding_on.store(enabled, Ordering::Relaxed);
        self.forget_pools();
        self.wake.notify_one();
        Ok(())
    }

    pub(super) fn forget_pools(&self) {
        self.pools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    pub(super) async fn load_consent(&self) -> Result<(), ApplicationError> {
        let on = self.store.embeddings_enabled().await?;
        self.embedding_on.store(on, Ordering::Relaxed);
        Ok(())
    }

    /// Embed the mail that has a body and no vector yet, newest first, a
    /// batch at a time. Woken when a body arrives or consent is given.
    pub(super) async fn run_embedder(&self, stop: CancellationToken) {
        let Some(embedder) = self.embedder.clone() else {
            return;
        };
        let mut before = None;
        loop {
            let (worked, failed) = if self.embeddings_enabled() {
                match self.embed_next(embedder.as_ref(), &mut before).await {
                    Ok(worked) => (worked, false),
                    Err(e) => {
                        warn!(error = %e, "embeddings: batch failed, trying again later");
                        (false, true)
                    }
                }
            } else {
                (false, false)
            };
            if !worked {
                before = None;
            }
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(if failed { RETRY } else { PAUSE }), if worked || failed => {}
                _ = self.wake.notified(), if !worked && !failed => {}
            }
        }
    }

    /// `false` when there was nothing left to embed.
    async fn embed_next(
        &self,
        embedder: &dyn Embedder,
        before: &mut Option<MessageId>,
    ) -> Result<bool, ApplicationError> {
        let page = self
            .store
            .unembedded(embedder.model(), *before, BATCH)
            .await?;
        let Some(last) = page.last().map(|s| s.id) else {
            return Ok(false);
        };
        *before = Some(last);
        let (ids, texts): (Vec<_>, Vec<_>) = page
            .iter()
            .filter_map(|s| Some((s.id, embedding_text(s)?)))
            .unzip();
        if texts.is_empty() {
            return Ok(true);
        }
        let vectors = embedder.embed(texts).await?;
        let batch: Vec<_> = ids.into_iter().zip(vectors).collect();
        self.store.put_vectors(embedder.model(), &batch).await?;
        debug!(count = batch.len(), "embeddings: batch stored");
        Ok(true)
    }

    /// The neighbours of each of `ids` that has a vector among the scope's
    /// labelled mail; empty while embeddings are off.
    pub(super) async fn nearest(
        &self,
        scope: Scope,
        ids: &[MessageId],
    ) -> Result<HashMap<MessageId, Nearest>, ApplicationError> {
        let Some(embedder) = self.embedder.as_ref().filter(|_| self.embeddings_enabled()) else {
            return Ok(HashMap::new());
        };
        let queries = self.store.vectors(embedder.model(), ids).await?;
        if queries.is_empty() {
            return Ok(HashMap::new());
        }
        let pool = self.pool(scope, embedder.model()).await?;
        // 200 messages against 20k vectors is a second of arithmetic.
        tokio::task::spawn_blocking(move || {
            queries
                .into_iter()
                .map(|(id, query)| (id, pool.nearest(&query, id)))
                .collect()
        })
        .await
        .map_err(|e| ApplicationError::Internal(format!("neighbour search: {e}")))
    }

    async fn pool(&self, scope: Scope, model: &str) -> Result<Arc<Pool>, ApplicationError> {
        let fresh = |pools: &super::Pools| {
            pools
                .get(&scope)
                .filter(|(at, _)| at.elapsed() < POOL_TTL)
                .map(|(_, pool)| Arc::clone(pool))
        };
        if let Some(pool) = fresh(
            &self
                .pools
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        ) {
            return Ok(pool);
        }
        let pool = Arc::new(Pool::new(self.store.scope_vectors(scope, model).await?));
        let mut pools = self
            .pools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Other scopes' vectors are not kept once stale.
        pools.retain(|_, (at, _)| at.elapsed() < POOL_TTL);
        pools.insert(scope, (Instant::now(), Arc::clone(&pool)));
        Ok(pool)
    }
}
