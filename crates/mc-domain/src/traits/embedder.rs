//! Turns text into vectors where similar meaning is close, locally.

use async_trait::async_trait;

use crate::error::DomainError;

#[async_trait]
pub trait Embedder: Send + Sync {
    /// Names the model and its revision: vectors of another id are not comparable.
    fn model(&self) -> &str;

    /// One unit-length vector per text, in order. May download and load the
    /// model first, so it can take long the first time.
    /// How many copies of the model run side by side, with how many threads
    /// each, from now on; a loaded model is rebuilt to that shape. Models
    /// that do not run locally ignore it.
    fn set_parallelism(&self, _workers: usize, _threads: usize) {}

    /// Whether the model is in memory right now.
    fn loaded(&self) -> bool {
        false
    }

    /// Roughly what each worker holds in memory, measured when it was loaded.
    fn worker_bytes(&self) -> Option<u64> {
        None
    }

    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, DomainError>;
}
