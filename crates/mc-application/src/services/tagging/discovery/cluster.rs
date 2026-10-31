//! The numbers of discovery: reduce the vectors, find the dense groups among
//! them, and say which words set a group apart.

use std::collections::HashMap;

use hdbscan::{Hdbscan, HdbscanHyperParams};

/// Components kept. Embeddings carry their meaning in far fewer directions
/// than their 384: clustering 32 gives the same groups as clustering all of
/// them, about 40 times faster (measured on 25k real messages: 26 s against
/// several minutes), as the kd-tree HDBSCAN uses is no better than a linear
/// scan in 384 dimensions.
const COMPONENTS: usize = 32;
/// Messages the components are fitted on; the rest are only projected.
const FIT_ON: usize = 4000;
const POWER_ROUNDS: usize = 30;
/// A term must be in this share of a cluster to name it.
const MIN_SHARE: f64 = 0.3;
const KEYWORDS: usize = 12;
const CENTRAL: usize = 5;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct Cluster {
    /// Indices into the vectors that were clustered, ascending.
    pub members: Vec<usize>,
    /// Members nearest the cluster's centre, nearest first.
    pub central: Vec<usize>,
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn normalize(v: &mut [f32]) {
    let norm = dot(v, v).sqrt();
    if norm > 0.0 {
        v.iter_mut().for_each(|x| *x /= norm);
    }
}

/// Subtract the mean of the vectors: e5 puts all mail in one narrow cone
/// (unrelated mail at cosine ~0.8), and what tells messages apart is the
/// spread around it.
fn centered(vectors: &[&[f32]]) -> Vec<Vec<f32>> {
    let dims = vectors[0].len();
    let mut mean = vec![0.0; dims];
    for v in vectors {
        mean.iter_mut().zip(*v).for_each(|(m, x)| *m += x);
    }
    mean.iter_mut().for_each(|m| *m /= vectors.len() as f32);
    vectors
        .iter()
        .map(|v| v.iter().zip(&mean).map(|(x, m)| x - m).collect())
        .collect()
}

/// The `COMPONENTS` directions of most variance (subspace iteration on the
/// covariance of a sample), and every vector projected on them, unit length.
fn reduced(data: &[Vec<f32>]) -> Vec<Vec<f32>> {
    let dims = data[0].len();
    let k = COMPONENTS.min(dims);
    let step = data.len().div_ceil(FIT_ON);
    let mut cov = vec![0.0f32; dims * dims];
    for v in data.iter().step_by(step) {
        for i in 0..dims {
            let (vi, row) = (v[i], &mut cov[i * dims..(i + 1) * dims]);
            for j in i..dims {
                row[j] += vi * v[j];
            }
        }
    }
    for i in 0..dims {
        for j in 0..i {
            cov[i * dims + j] = cov[j * dims + i];
        }
    }
    // A fixed start, so the same mail always gives the same groups.
    let mut seed = 0x2545_f491u32;
    let mut basis: Vec<Vec<f32>> = (0..k)
        .map(|_| {
            (0..dims)
                .map(|_| {
                    seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    (seed >> 8) as f32 / (1 << 24) as f32 - 0.5
                })
                .collect()
        })
        .collect();
    for _ in 0..POWER_ROUNDS {
        for a in 0..k {
            let mut z: Vec<f32> = (0..dims)
                .map(|i| dot(&cov[i * dims..(i + 1) * dims], &basis[a]))
                .collect();
            for earlier in &basis[..a] {
                let d = dot(&z, earlier);
                z.iter_mut().zip(earlier).for_each(|(x, e)| *x -= d * e);
            }
            normalize(&mut z);
            basis[a] = z;
        }
    }
    data.iter()
        .map(|v| {
            let mut out: Vec<f32> = basis.iter().map(|b| dot(b, v)).collect();
            normalize(&mut out);
            out
        })
        .collect()
}

/// Groups of at least `min_size` similar vectors; those that fit no group are
/// left out. Largest first.
pub(super) fn find(vectors: &[&[f32]], min_size: usize, min_samples: usize) -> Vec<Cluster> {
    let dims = vectors.first().map_or(0, |v| v.len());
    if dims == 0 || vectors.len() < 2 * min_size || vectors.iter().any(|v| v.len() != dims) {
        return Vec::new();
    }
    let data = reduced(&centered(vectors));
    let params = HdbscanHyperParams::builder()
        .min_cluster_size(min_size)
        .min_samples(min_samples)
        .build();
    let Ok(labels) = Hdbscan::new(&data, params).cluster() else {
        return Vec::new();
    };
    let mut groups: HashMap<i32, Vec<usize>> = HashMap::new();
    for (i, &label) in labels.iter().enumerate().filter(|(_, l)| **l >= 0) {
        groups.entry(label).or_default().push(i);
    }
    let mut clusters: Vec<Cluster> = groups
        .into_values()
        .map(|members| {
            let mut centre = vec![0.0; data[0].len()];
            for &i in &members {
                centre.iter_mut().zip(&data[i]).for_each(|(c, x)| *c += x);
            }
            let mut near: Vec<(f32, usize)> = members
                .iter()
                .map(|&i| (dot(&centre, &data[i]), i))
                .collect();
            near.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
            Cluster {
                central: near.into_iter().take(CENTRAL).map(|(_, i)| i).collect(),
                members,
            }
        })
        .collect();
    clusters.sort_by(|a, b| {
        b.members
            .len()
            .cmp(&a.members.len())
            .then(a.members[0].cmp(&b.members[0]))
    });
    clusters
}

/// In how many messages each term occurs.
#[derive(Debug, Default)]
struct Counts {
    docs: u32,
    df: HashMap<String, u32>,
}

impl Counts {
    fn add(&mut self, tokens: &[String]) {
        self.docs += 1;
        for token in tokens {
            *self.df.entry(token.clone()).or_default() += 1;
        }
    }
}

/// Document frequencies of every cluster and of all the mail looked at, for
/// class-based TF-IDF: how common a term is in the cluster (tf), weighed by
/// how rare it is outside it (idf).
#[derive(Debug)]
pub(super) struct Terms {
    all: Counts,
    classes: Vec<Counts>,
}

impl Terms {
    pub fn new(clusters: usize) -> Self {
        Self {
            all: Counts::default(),
            classes: (0..clusters).map(|_| Counts::default()).collect(),
        }
    }

    /// One message's tokens, in `class` or among the rest.
    pub fn add(&mut self, class: Option<usize>, tokens: &[String]) {
        self.all.add(tokens);
        if let Some(c) = class {
            self.classes[c].add(tokens);
        }
    }

    /// The words that set `class` apart, most distinctive first. Only the
    /// words of the subject, the sender's name and the body are topics.
    pub fn keywords(&self, class: usize) -> Vec<(String, f64)> {
        let this = &self.classes[class];
        let outside = f64::from(self.all.docs - this.docs);
        let mut best: HashMap<&str, f64> = HashMap::new();
        for (token, &inside) in &this.df {
            let Some(word) = ["s:", "n:", "b:"]
                .iter()
                .find_map(|prefix| token.strip_prefix(prefix))
            else {
                continue;
            };
            let share = f64::from(inside) / f64::from(this.docs);
            let elsewhere = f64::from(self.all.df[token] - inside);
            let rest_share = elsewhere / outside.max(1.0);
            if share < MIN_SHARE || rest_share >= share {
                continue;
            }
            let score = share * (1.0 + outside / (elsewhere + 1.0)).ln();
            let slot = best.entry(word).or_default();
            *slot = slot.max(score);
        }
        let mut words: Vec<(String, f64)> =
            best.into_iter().map(|(w, s)| (w.to_owned(), s)).collect();
        words.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        words.truncate(KEYWORDS);
        words
    }
}
