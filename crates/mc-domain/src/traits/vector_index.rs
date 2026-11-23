//! Nearest-neighbour search over message embeddings, kept outside the
//! database so it can be memory-mapped instead of loaded.

use async_trait::async_trait;

use crate::error::DomainError;
use crate::value_objects::message_id::MessageId;

/// The index key of a message.
///
/// ponytail: xor of the UUID's two halves. Ids are time-ordered UUIDv7, whose
/// low half is random, so a collision is ~1e-7 likely among 2M messages, and
/// it only matters when both are labelled examples. Widen the key if the
/// index ever has to hold far more than that.
pub fn vector_key(id: &MessageId) -> u64 {
    let n = id.as_uuid().as_u128();
    (n >> 64) as u64 ^ n as u64
}

/// Where an index is rebuilt from: the stored vectors.
#[async_trait]
pub trait VectorSource: Send + Sync {
    /// Vectors of `model` for messages after `after` (keyset), in id order.
    async fn vector_page(
        &self,
        model: &str,
        after: Option<MessageId>,
        limit: u32,
    ) -> Result<Vec<(MessageId, Vec<f32>)>, DomainError>;

    /// How many messages have a vector of `model`.
    async fn vector_count(&self, model: &str) -> Result<u64, DomainError>;
}

#[async_trait]
pub trait VectorIndex: Send + Sync {
    /// Messages that already have a vector get this one instead.
    fn add(&self, model: &str, vectors: &[(MessageId, Vec<f32>)]);

    /// The message is dropped from the index of every model.
    fn remove(&self, id: &MessageId);

    /// Up to `k` of the vectors closest to `query` whose [`vector_key`] passes
    /// `keep`, as (key, cosine similarity), closest first. Empty for a model
    /// with nothing indexed or a query of another size.
    fn nearest(
        &self,
        model: &str,
        query: &[f32],
        k: usize,
        keep: &dyn Fn(u64) -> bool,
    ) -> Vec<(u64, f32)>;

    /// Make sure the index of `model` matches the stored vectors, rebuilding
    /// it from them if not. Cheap once it has been checked.
    async fn ensure(&self, model: &str) -> Result<(), DomainError>;
}
