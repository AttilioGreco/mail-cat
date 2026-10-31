//! A message as the classifier sees it: a set of prefixed tokens, so the
//! same word in the subject and in the body are different evidence.

use std::collections::{BTreeSet, HashSet};

use mc_domain::TagSource;

/// Characters of the body that are read, and distinct words kept from them.
const BODY_CHARS: usize = 4000;
const BODY_WORDS: usize = 300;

/// Hashes, tracking ids and counters carry no topic and never recur: hex runs
/// with a digit ("003aa5", "0000563a225cd6a5"), and long words mostly digits.
fn is_noise(word: &str) -> bool {
    let len = word.chars().count();
    let digits = word.chars().filter(char::is_ascii_digit).count();
    digits == len
        || len >= 6 && digits > 0 && word.chars().all(|c| c.is_ascii_hexdigit())
        || len >= 8 && digits * 2 >= len
}

fn words(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|w| (3..=24).contains(&w.chars().count()))
        .map(str::to_lowercase)
        .filter(|w| !is_noise(w))
}

/// Distinct tokens, sorted so the same message always yields the same list.
/// `with_body` false reads the envelope only, as a lesson taught before the
/// body arrived must be withdrawn with exactly the tokens it added.
pub fn tokens(source: &TagSource, with_body: bool) -> Vec<String> {
    let mut set = BTreeSet::new();
    let from = source.from_email.to_lowercase();
    if let Some((_, domain)) = from.rsplit_once('@') {
        set.insert(format!("d:{domain}"));
    }
    set.insert(format!("f:{from}"));
    if let Some(name) = &source.from_name {
        set.extend(words(name).map(|w| format!("n:{w}")));
    }
    set.extend(words(&source.subject).map(|w| format!("s:{w}")));

    let list = &source.list_headers;
    if list.unsubscribe {
        set.insert("h:unsubscribe".into());
    }
    if let Some(id) = &list.list_id {
        set.insert(format!("h:list:{}", id.to_lowercase()));
    }
    if list.bulk {
        set.insert("h:bulk".into());
    }
    if list.auto_submitted {
        set.insert("h:auto".into());
    }

    if let Some(body) = source.body.as_ref().filter(|_| with_body) {
        let head = body
            .char_indices()
            .nth(BODY_CHARS)
            .map_or(body.as_str(), |(i, _)| &body[..i]);
        let mut seen = HashSet::new();
        set.extend(
            words(head)
                .filter(|w| seen.insert(w.clone()))
                .take(BODY_WORDS)
                .map(|w| format!("b:{w}")),
        );
    }
    set.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::is_noise;

    #[test]
    fn ids_are_noise_and_words_are_not() {
        for noise in [
            "0000563a225cd6a5",
            "003aa5",
            "007c89",
            "2024",
            "a1b2c3d4e5",
            "x9y8z7w6v5",
        ] {
            assert!(is_noise(noise), "{noise}");
        }
        for word in [
            "decade", "facade", "invoice", "fattura", "q3", "covid19", "ipv6", "b2b",
        ] {
            assert!(!is_noise(word), "{word}");
        }
    }
}
