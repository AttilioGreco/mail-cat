//! Offline evaluation: hold out part of the labelled mail, teach a model
//! from the rest, and see what each threshold would have done to the
//! held-out mail. Used to pick thresholds; changes nothing.

use mc_domain::{
    AccountId, LabelSettings, Lesson, MailboxReadRepository, ModelStats, SourceFilter, TagSource,
    TaggingStore,
};

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
    pub rows: Vec<EvalRow>,
}

fn held_out(source: &TagSource) -> bool {
    source.id.as_uuid().as_u128().is_multiple_of(HOLDOUT_EVERY)
}

/// Per label of `labels`, how each threshold does on the held-out sources.
pub(super) fn evaluate(
    train: &[TagSource],
    test: &[TagSource],
    labels: &[String],
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
    let scored: Vec<(Vec<mc_domain::Score>, &TagSource)> = test
        .iter()
        .map(|s| {
            (
                bayes::scores(
                    &stats,
                    labels.iter().map(String::as_str),
                    &features::tokens(s, true),
                ),
                s,
            )
        })
        .collect();
    labels
        .iter()
        .map(|label| {
            let truth = |s: &TagSource| s.labels.contains(label);
            let held = scored.iter().filter(|(_, s)| truth(s)).count() as u32;
            let rows = THRESHOLDS
                .iter()
                .map(|&threshold| {
                    let hits = |s: &[mc_domain::Score]| {
                        s.iter().any(|x| &x.label == label && x.score >= threshold)
                    };
                    let flagged = scored.iter().filter(|(s, _)| hits(s)).count() as u32;
                    let correct = scored.iter().filter(|(s, t)| hits(s) && truth(t)).count() as u32;
                    EvalRow {
                        threshold,
                        flagged,
                        correct,
                        precision: ratio(correct, flagged),
                        recall: ratio(correct, held),
                    }
                })
                .collect();
            LabelEval {
                label: label.clone(),
                held_out: held,
                rows,
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
        Ok(evaluate(&train, &test, &labels))
    }
}
