//! One-vs-rest naive Bayes over presence of tokens, on the counts the
//! store keeps (see migration 8). A token's evidence is shrunk towards "no
//! information" when it has been seen few times, so a token that appeared
//! once under another label cannot vouch for this one.

use mc_domain::{LabelStats, Lesson, ModelStats, Score};

/// Pseudo-documents pulling a token's rate towards the label's base rate.
const SHRINK: f64 = 2.0;
/// Most evidence one token may carry (log-odds) and how many tokens count:
/// words of one message are correlated, and uncapped naive Bayes is
/// overconfident on long mail.
const MAX_EVIDENCE: f64 = 3.0;
const TOP_TOKENS: usize = 15;

/// What each signal says about a (message, label); signals added later (e.g.
/// nearest neighbours) join here and in [`combine`].
pub struct Signals {
    pub bayes: f64,
}

/// The confidence in [0, 1] from the available signals.
pub fn combine(signals: &Signals) -> f64 {
    signals.bayes
}

/// Apply a lesson to in-memory counts: the arithmetic the store performs in
/// SQL, so a model can be built and tested without a database.
pub fn learn(stats: &mut ModelStats, lesson: &Lesson) {
    let w = lesson.weight;
    stats.documents += w;
    for token in lesson.tokens {
        *stats.corpus.entry(token.clone()).or_default() += w;
    }
    for (labels, positive) in [(lesson.positives, true), (lesson.negatives, false)] {
        let amount = if positive { w } else { w.signum() };
        for label in labels {
            let counts = stats.labels.entry(label.clone()).or_default();
            if positive {
                counts.positive_docs += amount;
            } else {
                counts.negative_docs += amount;
            }
            for token in lesson.tokens {
                let cell = counts.tokens.entry(token.clone()).or_default();
                if positive {
                    cell.0 += amount;
                } else {
                    cell.1 += amount;
                }
            }
        }
    }
}

fn logit(p: f64) -> f64 {
    (p / (1.0 - p)).ln()
}

fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

/// How strongly the tokens say the message belongs under `label`; `None`
/// while the label has no positive example.
pub fn bayes_score(stats: &ModelStats, label: &str, tokens: &[String]) -> Option<f64> {
    let counts: &LabelStats = stats.labels.get(label).filter(|c| c.positive_docs >= 1.0)?;
    let negative_docs = (stats.documents - counts.positive_docs + counts.negative_docs).max(0.0);
    // Clamped away from 0 and 1 so the logits below are finite.
    let base =
        (counts.positive_docs / (counts.positive_docs + negative_docs)).clamp(1e-4, 1.0 - 1e-4);
    let prior = logit(base);

    let mut evidence: Vec<f64> = tokens
        .iter()
        .filter_map(|token| {
            let corpus = *stats.corpus.get(token)?;
            let (positive, explicit_negative) =
                counts.tokens.get(token).copied().unwrap_or_default();
            let negative = (corpus - positive).max(0.0) + explicit_negative;
            let rate = (positive + SHRINK * base) / (positive + negative + SHRINK);
            let e = logit(rate.clamp(1e-4, 1.0 - 1e-4)) - prior;
            Some(e.clamp(-MAX_EVIDENCE, MAX_EVIDENCE))
        })
        .collect();
    evidence.sort_by(|a, b| b.abs().total_cmp(&a.abs()));
    Some(sigmoid(
        prior + evidence.iter().take(TOP_TOKENS).sum::<f64>(),
    ))
}

/// Combined scores of the message for each of `labels`, best first.
pub fn scores<'a>(
    stats: &ModelStats,
    labels: impl IntoIterator<Item = &'a str>,
    tokens: &[String],
) -> Vec<Score> {
    let mut scored: Vec<Score> = labels
        .into_iter()
        .filter_map(|label| {
            let bayes = bayes_score(stats, label, tokens)?;
            Some(Score {
                label: label.to_owned(),
                score: combine(&Signals { bayes }),
            })
        })
        .collect();
    scored.sort_by(|a, b| b.score.total_cmp(&a.score));
    scored
}
