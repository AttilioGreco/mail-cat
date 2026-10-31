//! Offline evaluation: hold out part of the labelled mail, teach a model
//! from the rest, and see what each threshold would have done to the
//! held-out mail. Used to pick thresholds; changes nothing.

use std::collections::HashMap;
use std::sync::Arc;

use mc_domain::{
    AccountId, LabelSettings, LabelledVector, Lesson, MailboxReadRepository, MessageId, ModelStats,
    Score, SourceFilter, TagSource, TaggingStore,
};

use super::knn::{Nearest, Pool};
use super::training::{UNLABELLED_SAMPLE, UNLABELLED_WEIGHT};
use super::{TaggingService, bayes, features};
use crate::bus::CommandBus;
use crate::error::ApplicationError;

const PAGE: u32 = 500;
/// One message in this many is held out.
const HOLDOUT_EVERY: u128 = 5;
pub const THRESHOLDS: [f64; 5] = [0.6, 0.7, 0.8, 0.9, 0.95];

#[derive(Debug, Clone)]
pub struct EvalRow {
    pub threshold: f64,
    /// Held-out messages scored at or above the threshold.
    pub flagged: u32,
    pub correct: u32,
    pub precision: f64,
    pub recall: f64,
}

#[derive(Debug, Clone)]
pub struct LabelEval {
    pub label: String,
    /// Held-out messages that carry the label.
    pub held_out: u32,
    /// Of those, how many have an embedding to be judged by neighbours.
    pub embedded: u32,
    /// Words only.
    pub bayes_rows: Vec<EvalRow>,
    /// As the classifier scores: words and neighbours where a vector exists.
    pub rows: Vec<EvalRow>,
}

fn held_out(source: &TagSource) -> bool {
    source.id.as_uuid().as_u128().is_multiple_of(HOLDOUT_EVERY)
}

/// Per label of `labels`, how each threshold does on the held-out sources,
/// by words alone and with neighbours from `vectors` (by message).
pub(super) fn evaluate(
    train: &[TagSource],
    test: &[TagSource],
    labels: &[String],
    vectors: &HashMap<MessageId, Vec<f32>>,
) -> Vec<LabelEval> {
    let mut stats = ModelStats::default();
    for source in train {
        let tokens = features::tokens(source, true);
        let weight = if source.labels.is_empty() {
            UNLABELLED_WEIGHT
        } else {
            1.0
        };
        bayes::learn(
            &mut stats,
            &Lesson {
                tokens: &tokens,
                positives: &source.labels,
                negatives: &[],
                weight,
            },
        );
    }
    let pool = Arc::new(Pool::new(train.iter().filter_map(|s| {
        Some(LabelledVector {
            message_id: s.id,
            vector: vectors.get(&s.id)?.clone(),
            positives: s.labels.clone(),
            negatives: Vec::new(),
        })
    })));
    let neighbours: Vec<Option<Nearest>> = test
        .iter()
        .map(|s| vectors.get(&s.id).map(|v| pool.nearest(v, s.id)))
        .collect();
    let scored = |with_neighbours: bool| -> Vec<(Vec<Score>, &TagSource)> {
        test.iter()
            .zip(&neighbours)
            .map(|(s, near)| {
                let near = near.as_ref().filter(|_| with_neighbours);
                let tokens = features::tokens(s, true);
                let scores =
                    bayes::scores(&stats, labels.iter().map(String::as_str), &tokens, near);
                (scores, s)
            })
            .collect()
    };
    let (alone, combined) = (scored(false), scored(true));
    labels
        .iter()
        .map(|label| {
            let held = |s: &&TagSource| s.labels.contains(label);
            LabelEval {
                label: label.clone(),
                held_out: test.iter().filter(held).count() as u32,
                embedded: test
                    .iter()
                    .filter(held)
                    .filter(|s| vectors.contains_key(&s.id))
                    .count() as u32,
                bayes_rows: rows(label, &alone),
                rows: rows(label, &combined),
            }
        })
        .collect()
}

fn rows(label: &str, scored: &[(Vec<Score>, &TagSource)]) -> Vec<EvalRow> {
    let held = scored
        .iter()
        .filter(|(_, s)| s.labels.iter().any(|l| l == label))
        .count() as u32;
    THRESHOLDS
        .iter()
        .map(|&threshold| {
            let flagged_by =
                |s: &[Score]| s.iter().any(|x| x.label == label && x.score >= threshold);
            let flagged = scored.iter().filter(|(s, _)| flagged_by(s)).count() as u32;
            let correct = scored
                .iter()
                .filter(|(s, t)| flagged_by(s) && t.labels.iter().any(|l| l == label))
                .count() as u32;
            EvalRow {
                threshold,
                flagged,
                correct,
                precision: ratio(correct, flagged),
                recall: ratio(correct, held),
            }
        })
        .collect()
}

fn ratio(n: u32, d: u32) -> f64 {
    if d == 0 {
        0.0
    } else {
        f64::from(n) / f64::from(d)
    }
}

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    /// Evaluate the account's group (or the account alone). Mail under a
    /// label the user never applied counts against precision.
    pub async fn eval(&self, account: AccountId) -> Result<Vec<LabelEval>, ApplicationError> {
        let scope = self.store.scope_of(account).await?;
        let accounts = self.store.scope_accounts(scope).await?;
        let mut sources = Vec::new();
        for (filter, newest_first, cap) in [
            (SourceFilter::Labelled, false, usize::MAX),
            (SourceFilter::Unlabelled, true, UNLABELLED_SAMPLE),
        ] {
            let mut cursor = None;
            let mut taken = 0;
            while taken < cap {
                let page = self
                    .store
                    .source_page(&accounts, filter, cursor, newest_first, PAGE)
                    .await?;
                let Some(last) = page.last().map(|s| s.id) else {
                    break;
                };
                cursor = Some(last);
                taken += page.len();
                sources.extend(page);
            }
        }
        let (test, train): (Vec<_>, Vec<_>) = sources.into_iter().partition(held_out);
        let mut labels: Vec<String> = train
            .iter()
            .flat_map(|s| s.labels.iter().cloned())
            .collect();
        labels.sort();
        labels.dedup();
        labels.retain(|l| {
            train.iter().filter(|s| s.labels.contains(l)).count() as u32
                >= LabelSettings::MIN_POSITIVES
        });
        let ids: Vec<MessageId> = train.iter().chain(&test).map(|s| s.id).collect();
        let vectors = match self.embedder.as_ref().filter(|_| self.embeddings_enabled()) {
            Some(embedder) => {
                self.store
                    .vectors(&embedder.model().to_string(), &ids)
                    .await?
            }
            None => HashMap::new(),
        };
        Ok(evaluate(&train, &test, &labels, &vectors))
    }
}
