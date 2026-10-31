//! Turns text into vectors where similar meaning is close, locally.

use async_trait::async_trait;

use crate::error::DomainError;
use crate::model::embedding::{EmbedPurpose, ModelId, ModelOption, ModelProfile, TargetOption};

#[async_trait]
pub trait Embedder: Send + Sync {
    /// The model in use now: vectors of another id are not comparable.
    fn model(&self) -> ModelId;

    fn dims(&self) -> usize;

    /// The models on offer; empty where there is no choice.
    fn options(&self) -> Vec<ModelOption> {
        Vec::new()
    }

    /// Where inference can run; empty where there is no choice.
    fn targets(&self) -> Vec<TargetOption> {
        Vec::new()
    }

    /// Use another model and/or execution target (by id) from the next
    /// request; the old one is let go of. Fails, changing nothing, for a
    /// target that is not available here.
    fn select(&self, _profile: ModelProfile, _target: &str) -> Result<(), DomainError> {
        Ok(())
    }

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

    /// One unit-length vector per text, in order. May download and load the
    /// model first, so it can take long the first time.
    async fn embed(
        &self,
        texts: Vec<String>,
        purpose: EmbedPurpose,
    ) -> Result<Vec<Vec<f32>>, DomainError>;
}
