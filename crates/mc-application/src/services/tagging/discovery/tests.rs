use std::collections::HashSet;

use mc_domain::MessageId;

use super::cluster::{Terms, find};
use super::jaccard;

/// A deterministic spread in [-0.5, 0.5).
fn noise(seed: &mut u32) -> f32 {
    *seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
    (*seed >> 8) as f32 / (1 << 24) as f32 - 0.5
}

fn unit(mut v: Vec<f32>) -> Vec<f32> {
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    v.iter_mut().for_each(|x| *x /= n);
    v
}

const SCATTER: usize = 100;

/// Three tight groups of 40 and 100 points spread over the whole space, all in
/// one cone like real embeddings: everything shares a large common part.
fn planted() -> Vec<Vec<f32>> {
    let mut seed = 7;
    let dims = 6;
    let common: Vec<f32> = (0..dims).map(|_| 3.0 + noise(&mut seed)).collect();
    let centres: Vec<Vec<f32>> = (0..3)
        .map(|_| (0..dims).map(|_| noise(&mut seed) * 4.0).collect())
        .collect();
    let mut out = Vec::new();
    for centre in &centres {
        for _ in 0..40 {
            out.push(unit(
                (0..dims)
                    .map(|d| common[d] + centre[d] + noise(&mut seed) * 0.2)
                    .collect(),
            ));
        }
    }
    for _ in 0..SCATTER {
        out.push(unit(
            (0..dims)
                .map(|d| common[d] + noise(&mut seed) * 8.0)
                .collect(),
        ));
    }
    out
}

#[test]
fn planted_groups_are_found_and_the_scatter_is_left_out() {
    let data = planted();
    let vectors: Vec<&[f32]> = data.iter().map(Vec::as_slice).collect();
    let clusters = find(&vectors, 15, 5);

    assert_eq!(clusters.len(), 3, "{clusters:?}");
    let mut groups = HashSet::new();
    let mut scatter = 0;
    for cluster in &clusters {
        let own = cluster.members.iter().filter(|&&i| i < 120).count();
        let group = cluster.members.iter().find(|&&i| i < 120).map(|i| i / 40);
        assert!(own >= 38, "{own} of a planted group");
        assert!(
            cluster
                .members
                .iter()
                .filter(|&&i| i < 120)
                .all(|i| Some(i / 40) == group)
        );
        groups.insert(group);
        scatter += cluster.members.len() - own;
        assert!(
            cluster.central.len() == 5
                && cluster.central.iter().all(|i| cluster.members.contains(i))
        );
    }
    assert_eq!(groups.len(), 3, "one cluster per planted group");
    assert!(
        scatter < SCATTER / 2,
        "{scatter} of {SCATTER} scattered points were put in a cluster"
    );
}

#[test]
fn too_little_mail_or_odd_vectors_make_no_clusters() {
    let data = planted();
    let few: Vec<&[f32]> = data.iter().take(20).map(Vec::as_slice).collect();
    assert!(find(&few, 15, 5).is_empty());
    assert!(find(&[], 15, 5).is_empty());
    let mut ragged: Vec<&[f32]> = data.iter().map(Vec::as_slice).collect();
    ragged.push(&data[0][..3]);
    assert!(find(&ragged, 15, 5).is_empty());
}

fn tokens(words: &[&str]) -> Vec<String> {
    words.iter().map(|w| (*w).to_owned()).collect()
}

#[test]
fn keywords_are_the_words_common_inside_and_rare_outside() {
    let mut terms = Terms::new(1);
    for _ in 0..10 {
        terms.add(
            Some(0),
            &tokens(&[
                "s:ordine",
                "s:spedizione",
                "b:grazie",
                "d:amazon.it",
                "h:bulk",
            ]),
        );
    }
    for i in 0..40 {
        let rest = tokens(&[
            "b:grazie",
            if i % 2 == 0 {
                "s:riunione"
            } else {
                "s:fattura"
            },
        ]);
        terms.add(None, &rest);
    }
    // Once in ten: too rare in the cluster to name it.
    terms.add(Some(0), &tokens(&["s:ordine", "s:raro"]));

    let words: Vec<String> = terms.keywords(0).into_iter().map(|(w, _)| w).collect();
    assert_eq!(words, ["ordine", "spedizione"], "{words:?}");
}

#[test]
fn a_word_equally_common_outside_is_not_distinctive_and_the_better_term_ranks_first() {
    let mut terms = Terms::new(1);
    for i in 0..10 {
        let mut t = tokens(&["s:ordine", "b:grazie"]);
        if i < 5 {
            t.push("s:amazon".into());
        }
        terms.add(Some(0), &t);
    }
    for _ in 0..30 {
        terms.add(None, &tokens(&["b:grazie"]));
    }
    terms.add(None, &tokens(&["s:ordine", "b:grazie"]));

    let keywords = terms.keywords(0);
    let words: Vec<&str> = keywords.iter().map(|(w, _)| w.as_str()).collect();
    assert_eq!(
        words,
        ["ordine", "amazon"],
        "ordine is in all of them: {keywords:?}"
    );
    assert!(keywords[0].1 > keywords[1].1);
}

#[test]
fn clusters_are_matched_by_their_overlap() {
    let ids: Vec<MessageId> = (0..10).map(|_| MessageId::new()).collect();
    let set = |range: std::ops::Range<usize>| ids[range].iter().copied().collect::<HashSet<_>>();
    assert_eq!(jaccard(&set(0..10), &set(0..10)), 1.0);
    assert_eq!(jaccard(&set(0..4), &set(4..8)), 0.0);
    assert!((jaccard(&set(0..6), &set(3..9)) - 3.0 / 9.0).abs() < 1e-9);
    assert_eq!(jaccard(&HashSet::new(), &HashSet::new()), 0.0);
}
