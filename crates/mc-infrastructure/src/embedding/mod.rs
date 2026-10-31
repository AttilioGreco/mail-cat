//! The embedding model, kept in memory only while it is being used.
//!
//! `LazyEmbedder` loads an engine on the first request and drops it after
//! `idle` without one; inference runs on a blocking thread. The state lock is
//! never held across an await: two first requests may both load, and the
//! second engine is simply discarded.

mod e5;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use tokio::time::Instant;

use mc_domain::{DomainError, Embedder};

pub use e5::multilingual_e5_small;

/// Dropped (and so freed) when the embedder lets go of it.
pub trait Engine: Send + 'static {
    fn embed(&mut self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, DomainError>;
}

/// Builds `workers` engines, each running inference on `threads` threads.
type Loader<E> =
    Box<dyn Fn(usize, usize) -> BoxFuture<'static, Result<Vec<E>, DomainError>> + Send + Sync>;

type Engines<E> = Vec<Arc<Mutex<E>>>;

struct State<E> {
    /// Empty while unloaded.
    engines: Engines<E>,
    last_used: Instant,
    watching: bool,
    workers: usize,
    threads: usize,
    worker_bytes: Option<u64>,
}

pub struct LazyEmbedder<E: Engine> {
    model: String,
    load: Loader<E>,
    idle: Duration,
    state: Arc<Mutex<State<E>>>,
}

impl<E: Engine> LazyEmbedder<E> {
    pub fn new(model: &str, idle: Duration, load: Loader<E>) -> Self {
        Self {
            model: model.to_owned(),
            load,
            idle,
            state: Arc::new(Mutex::new(State {
                engines: Vec::new(),
                last_used: Instant::now(),
                watching: false,
                workers: 1,
                threads: 1,
                worker_bytes: None,
            })),
        }
    }

    /// Whether the engines are in memory.
    pub fn loaded(&self) -> bool {
        !self.state.lock().engines.is_empty()
    }

    async fn engines(&self) -> Result<Engines<E>, DomainError> {
        if let Some(engines) = self.touch() {
            return Ok(engines);
        }
        loop {
            let (workers, threads) = {
                let state = self.state.lock();
                (state.workers, state.threads)
            };
            let before = mc_shared::memory::resident_bytes();
            let loaded: Engines<E> = (self.load)(workers, threads)
                .await?
                .into_iter()
                .map(|e| Arc::new(Mutex::new(e)))
                .collect();
            let grown = before
                .zip(mc_shared::memory::resident_bytes())
                .map(|(b, a)| a.saturating_sub(b) / loaded.len().max(1) as u64);
            let mut state = self.state.lock();
            if (state.workers, state.threads) != (workers, threads) {
                // Changed while loading: these are already out of date.
                continue;
            }
            if state.engines.is_empty() {
                state.engines = loaded;
                state.worker_bytes = grown;
            }
            state.last_used = Instant::now();
            if !std::mem::replace(&mut state.watching, true) {
                tokio::spawn(unload_when_idle(self.state.clone(), self.idle));
            }
            return Ok(state.engines.clone());
        }
    }

    fn touch(&self) -> Option<Engines<E>> {
        let mut state = self.state.lock();
        if state.engines.is_empty() {
            return None;
        }
        state.last_used = Instant::now();
        Some(state.engines.clone())
    }
}

async fn unload_when_idle<E>(state: Arc<Mutex<State<E>>>, idle: Duration) {
    loop {
        let wait = {
            let mut state = state.lock();
            let wait = idle.saturating_sub(state.last_used.elapsed());
            if wait.is_zero() {
                state.engines.clear();
                state.watching = false;
                return;
            }
            wait
        };
        tokio::time::sleep(wait).await;
    }
}

#[async_trait]
impl<E: Engine> Embedder for LazyEmbedder<E> {
    fn model(&self) -> &str {
        &self.model
    }

    fn loaded(&self) -> bool {
        LazyEmbedder::loaded(self)
    }

    fn worker_bytes(&self) -> Option<u64> {
        self.state.lock().worker_bytes
    }

    fn set_parallelism(&self, workers: usize, threads: usize) {
        let (workers, threads) = (workers.max(1), threads.max(1));
        let mut state = self.state.lock();
        if (state.workers, state.threads) != (workers, threads) {
            (state.workers, state.threads) = (workers, threads);
            // Rebuilt with the new shape on the next request.
            state.engines.clear();
        }
    }

    /// The texts are shared out among the workers, each on a niced thread of
    /// its own, and come back in order.
    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, DomainError> {
        let engines = self.engines().await?;
        let chunk = texts.len().div_ceil(engines.len()).max(1);
        let runs = texts
            .chunks(chunk)
            .map(<[String]>::to_vec)
            .zip(engines)
            .map(|(part, engine)| niced(move || engine.lock().embed(part)));
        let parts = futures::future::try_join_all(runs).await?;
        // Inference may have outlasted the idle time.
        self.touch();
        parts.into_iter().try_fold(Vec::new(), |mut all, part| {
            all.extend(part?);
            Ok(all)
        })
    }
}

/// Runs `f` on a thread of its own at background priority. Nice cannot be
/// raised back, so a pooled thread that may later serve a request must not
/// take it; the thread that builds the ONNX session also hands the priority
/// to the session's worker threads.
async fn niced<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<T, DomainError> {
    // The pool thread only waits: tokio's paused test clock does not skip
    // ahead while a blocking task is in flight.
    tokio::task::spawn_blocking(move || {
        std::thread::spawn(move || {
            if let Err(e) = mc_shared::priority::lower_thread_priority() {
                tracing::debug!(error = %e, "embeddings: could not lower the thread priority");
            }
            f()
        })
        .join()
    })
    .await
    .ok()
    .and_then(Result::ok)
    .ok_or_else(|| DomainError::Provider("embedding thread ended unexpectedly".into()))
}
