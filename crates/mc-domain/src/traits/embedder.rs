//! Turns text into vectors where similar meaning is close, locally.

use async_trait::async_trait;

use crate::error::DomainError;

#[async_trait]
pub trait Embedder: Send + Sync {
    /// Names the model and its revision: vectors of another id are not comparable.
    fn model(&self) -> &str;

    /// One unit-length vector per text, in order. May download and load the
    /// model first, so it can take long the first time.
    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, DomainError>;
}
