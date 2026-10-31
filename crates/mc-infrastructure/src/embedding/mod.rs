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

type Loader<E> = Box<dyn Fn() -> BoxFuture<'static, Result<E, DomainError>> + Send + Sync>;

struct State<E> {
    engine: Option<Arc<Mutex<E>>>,
    last_used: Instant,
    watching: bool,
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
                engine: None,
                last_used: Instant::now(),
                watching: false,
            })),
        }
    }

    /// Whether the engine is in memory.
    pub fn loaded(&self) -> bool {
        self.state.lock().engine.is_some()
    }

    async fn engine(&self) -> Result<Arc<Mutex<E>>, DomainError> {
        if let Some(engine) = self.touch() {
            return Ok(engine);
        }
        let loaded = Arc::new(Mutex::new((self.load)().await?));
        let mut state = self.state.lock();
        let engine = state.engine.get_or_insert(loaded).clone();
        state.last_used = Instant::now();
        if !std::mem::replace(&mut state.watching, true) {
            tokio::spawn(unload_when_idle(self.state.clone(), self.idle));
        }
        Ok(engine)
    }

    fn touch(&self) -> Option<Arc<Mutex<E>>> {
        let mut state = self.state.lock();
        let engine = state.engine.clone()?;
        state.last_used = Instant::now();
        Some(engine)
    }
}

async fn unload_when_idle<E>(state: Arc<Mutex<State<E>>>, idle: Duration) {
    loop {
        let wait = {
            let mut state = state.lock();
            let wait = idle.saturating_sub(state.last_used.elapsed());
            if wait.is_zero() {
                state.engine = None;
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

    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, DomainError> {
        let engine = self.engine().await?;
        let out = tokio::task::spawn_blocking(move || engine.lock().embed(texts))
            .await
            .map_err(|e| DomainError::Provider(format!("embedding task: {e}")))?;
        // Inference may have outlasted the idle time.
        self.touch();
        out
    }
}
