//! Nearest neighbours over message embeddings: what the messages most like
//! this one were labelled. Brute force over unit vectors (cosine is a dot
//! product).
//!
//! ponytail: a linear scan, a few ms at 20k vectors (see the ignored timing
//! test). Past ~200k, put an ANN index (HNSW) behind `Pool::nearest`.

use std::sync::Arc;

use mc_domain::{LabelledVector, MessageId};

use super::training::UNLABELLED_WEIGHT;

const K: usize = 15;
/// Multilingual e5 puts unrelated mail at ~0.82, translations of one text at
/// ~0.9 and near-duplicates at ~0.95, so similarity counts from this floor,
/// not from zero. Measured on a handful of sentences, not on real mail:
/// revisit with `mc labels eval`.
const FLOOR: f32 = 0.8;

struct Item {
    id: MessageId,
    positives: Vec<String>,
    /// Unlabelled mail only says "probably none of the labels".
    weight: f64,
}

/// The labelled vectors of one scope, in one allocation. A message rejected
/// for a label is kept with no positives, so it counts against it.
pub struct Pool {
    dim: usize,
    data: Vec<f32>,
    items: Vec<Item>,
}

impl Pool {
    pub fn new(vectors: impl IntoIterator<Item = LabelledVector>) -> Self {
        let mut pool = Self {
            dim: 0,
            data: Vec::new(),
            items: Vec::new(),
        };
        for v in vectors {
            if pool.dim == 0 {
                pool.dim = v.vector.len();
            }
            if v.vector.len() != pool.dim {
                continue;
            }
            pool.data.extend_from_slice(&v.vector);
            pool.items.push(Item {
                weight: if v.positives.is_empty() && v.negatives.is_empty() {
                    UNLABELLED_WEIGHT
                } else {
                    1.0
                },
                id: v.message_id,
                positives: v.positives,
            });
        }
        pool
    }

    /// The `K` most similar messages to `query`, `me` excluded.
    pub fn nearest(self: &Arc<Self>, query: &[f32], me: MessageId) -> Nearest {
        let mut sims: Vec<(f32, usize)> = Vec::new();
        if query.len() == self.dim && self.dim > 0 {
            sims = self
                .data
                .chunks_exact(self.dim)
                .zip(&self.items)
                .enumerate()
                .filter(|(_, (_, item))| item.id != me)
                .map(|(i, (v, _))| (v.iter().zip(query).map(|(a, b)| a * b).sum(), i))
                .collect();
        }
        if sims.len() > K {
            sims.select_nth_unstable_by(K, |a, b| b.0.total_cmp(&a.0));
            sims.truncate(K);
        }
        Nearest {
            pool: Arc::clone(self),
            hits: sims
                .into_iter()
                .map(|(sim, i)| {
                    let closeness = f64::from(((sim - FLOOR) / (1.0 - FLOOR)).clamp(0.0, 1.0));
                    (closeness * self.items[i].weight, i)
                })
                .collect(),
        }
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
