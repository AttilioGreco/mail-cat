//! Nearest neighbours over message embeddings: what the messages most like
//! this one were labelled. The vectors live in a `VectorIndex`; the `Pool`
//! only knows the labels of the scope's examples.
//!
//! ponytail: the index holds every embedded message and the pool's examples
//! are a filter over it, so a sparse scope makes the filtered search walk far.
//! Past what that costs, index the examples apart from the rest.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use mc_domain::{DomainError, LabelledExample, MessageId, VectorIndex, vector_key};

use super::training::UNLABELLED_WEIGHT;

const K: usize = 15;
/// Multilingual e5 puts unrelated mail at ~0.82, translations of one text at
/// ~0.9 and near-duplicates at ~0.95, so similarity counts from this floor,
/// not from zero. Measured on a handful of sentences, not on real mail:
/// revisit with `mc labels eval`.
const FLOOR: f32 = 0.8;

struct Item {
    positives: Vec<String>,
    /// Unlabelled mail only says "probably none of the labels".
    weight: f64,
}

/// The labelled mail of one scope. A message rejected for a label is kept
/// with no positives, so it counts against it.
pub struct Pool {
    items: Vec<Item>,
    /// Index key to position in `items`.
    by_key: HashMap<u64, usize>,
}

impl Pool {
    pub fn new(examples: impl IntoIterator<Item = LabelledExample>) -> Self {
        let mut pool = Self {
            items: Vec::new(),
            by_key: HashMap::new(),
        };
        for e in examples {
            pool.by_key
                .insert(vector_key(&e.message_id), pool.items.len());
            pool.items.push(Item {
                weight: if e.positives.is_empty() && e.negatives.is_empty() {
                    UNLABELLED_WEIGHT
                } else {
                    1.0
                },
                positives: e.positives,
            });
        }
        pool
    }

    /// The `K` most similar examples to `query`, `me` excluded.
    pub fn nearest(
        self: &Arc<Self>,
        index: &dyn VectorIndex,
        model: &str,
        query: &[f32],
        me: MessageId,
    ) -> Nearest {
        let me = vector_key(&me);
        let found = index.nearest(model, query, K, &|key| {
            key != me && self.by_key.contains_key(&key)
        });
        Nearest {
            pool: Arc::clone(self),
            hits: found
                .into_iter()
                .map(|(key, sim)| {
                    let i = self.by_key[&key];
                    let closeness = f64::from(((sim - FLOOR) / (1.0 - FLOOR)).clamp(0.0, 1.0));
                    (closeness * self.items[i].weight, i)
                })
                .collect(),
        }
    }
}

/// Exact search over vectors held in memory, for the held-out evaluation and
/// the tests, where there is no database to build a file from.
#[derive(Default)]
pub struct ExactIndex(std::sync::Mutex<Vec<(u64, Vec<f32>)>>);

#[async_trait]
impl VectorIndex for ExactIndex {
    fn add(&self, _model: &str, vectors: &[(MessageId, Vec<f32>)]) {
        let mut all = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        all.extend(vectors.iter().map(|(id, v)| (vector_key(id), v.clone())));
    }

    fn remove(&self, id: &MessageId) {
        let key = vector_key(id);
        let mut all = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        all.retain(|(k, _)| *k != key);
    }

    fn nearest(
        &self,
        _model: &str,
        query: &[f32],
        k: usize,
        keep: &dyn Fn(u64) -> bool,
    ) -> Vec<(u64, f32)> {
        let all = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut sims: Vec<(u64, f32)> = all
            .iter()
            .filter(|(key, v)| v.len() == query.len() && keep(*key))
            .map(|(key, v)| (*key, v.iter().zip(query).map(|(a, b)| a * b).sum()))
            .collect();
        sims.sort_by(|a, b| b.1.total_cmp(&a.1));
        sims.truncate(k);
        sims
    }

    async fn ensure(&self, _model: &str) -> Result<(), DomainError> {
        Ok(())
    }
}

pub struct Nearest {
    pool: Arc<Pool>,
    /// (weight, index into the pool).
    hits: Vec<(f64, usize)>,
}

impl Nearest {
    /// The weighted share of the neighbours labelled `label`. A neighbour that
    /// was rejected for it, or carries other labels, counts against; `None`
    /// when nothing is close enough to say anything.
    pub fn share(&self, label: &str) -> Option<f64> {
        let total: f64 = self.hits.iter().map(|(w, _)| w).sum();
        if total <= 0.0 {
            return None;
        }
        let with: f64 = self
            .hits
            .iter()
            .filter(|(_, i)| self.pool.items[*i].positives.iter().any(|l| l == label))
            .map(|(w, _)| w)
            .sum();
        Some(with / total)
    }
}
