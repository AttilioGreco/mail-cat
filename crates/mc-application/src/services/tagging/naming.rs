//! Names a cluster of similar mail from its distinctive words and senders.

use async_trait::async_trait;

use mc_domain::{ClusterSummary, LabelNamer, ProposedName};

/// A sender sending this share of the cluster is its brand: "Amazon" says more
/// than any word the mail contains.
const BRAND_SHARE: f64 = 0.6;
/// Terms shown next to the name.
const SHOWN: usize = 3;
const FALLBACK: &str = "Messaggi simili";

/// Words that tell nothing about a topic, Italian and English.
const STOP_WORDS: &[&str] = &[
    "alla",
    "alle",
    "agli",
    "allo",
    "che",
    "chi",
    "con",
    "come",
    "contro",
    "dei",
    "del",
    "della",
    "delle",
    "dello",
    "dal",
    "dalla",
    "degli",
    "dopo",
    "gli",
    "per",
    "tra",
    "fra",
    "una",
    "uno",
    "nel",
    "nella",
    "nei",
    "nelle",
    "non",
    "più",
    "sono",
    "sul",
    "sulla",
    "suo",
    "sua",
    "tuo",
    "tua",
    "tuoi",
    "tue",
    "questo",
    "questa",
    "questi",
    "queste",
    "essere",
    "stato",
    "stati",
    "ciao",
    "grazie",
    "cordiali",
    "saluti",
    "gentile",
    "buongiorno",
    "salve",
    "oggi",
    "ancora",
    "anche",
    "poi",
    "solo",
    "molto",
    "tutto",
    "tutti",
    "tutte",
    "the",
    "and",
    "for",
    "you",
    "your",
    "with",
    "from",
    "this",
    "that",
    "are",
    "was",
    "has",
    "have",
    "our",
    "not",
    "all",
    "will",
    "can",
    "new",
    "hello",
    "thanks",
    "regards",
    "dear",
    "http",
    "https",
    "www",
    "com",
    "mail",
    "email",
    "re",
    "fwd",
];

/// Mailbox providers: the domain says nothing about the topic.
const FREE_MAIL: &[&str] = &[
    "gmail",
    "googlemail",
    "outlook",
    "hotmail",
    "live",
    "yahoo",
    "icloud",
    "libero",
    "virgilio",
    "tiscali",
    "alice",
    "msn",
    "proton",
    "protonmail",
    "gmx",
    "aol",
];

/// Second-level labels that are part of the suffix ("co.uk", "com.au").
const SUFFIXES: &[&str] = &["co", "com", "org", "net", "gov", "edu", "ac"];

#[derive(Debug, Clone, Copy, Default)]
pub struct KeywordNamer;

#[async_trait]
impl LabelNamer for KeywordNamer {
    async fn name(&self, cluster: &ClusterSummary) -> ProposedName {
        name(cluster)
    }
}

fn name(cluster: &ClusterSummary) -> ProposedName {
    let mut terms: Vec<_> = cluster
        .keywords
        .iter()
        .filter(|(term, _)| is_topic(term))
        .collect();
    // Ties by term, so the same cluster always gets the same name.
    terms.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let mut keywords: Vec<String> = terms.iter().map(|(t, _)| t.to_lowercase()).collect();
    keywords.dedup();

    let brand = dominant_brand(cluster);
    let name = match (&brand, keywords.first()) {
        (Some(brand), _) => capitalize(brand),
        (None, Some(top)) => capitalize(top),
        (None, None) => {
            return ProposedName {
                name: FALLBACK.into(),
                keywords: Vec::new(),
            };
        }
    };
    if let Some(brand) = brand {
        keywords.retain(|k| *k != brand);
        keywords.insert(0, brand);
    }
    keywords.truncate(SHOWN);
    ProposedName { name, keywords }
}

fn is_topic(term: &str) -> bool {
    let term = term.to_lowercase();
    term.chars().count() >= 3
        && term.chars().all(char::is_alphabetic)
        && !STOP_WORDS.contains(&term.as_str())
}

/// The brand of the sender that wrote most of the cluster, if it wrote enough.
fn dominant_brand(cluster: &ClusterSummary) -> Option<String> {
    let (sender, count) = cluster
        .senders
        .iter()
        .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0)))?;
    if (*count as f64) < BRAND_SHARE * cluster.size as f64 {
        return None;
    }
    brand(sender).filter(|b| !FREE_MAIL.contains(&b.as_str()))
}

/// "mail.amazon.co.uk" and "orders@amazon.it" are both "amazon".
fn brand(sender: &str) -> Option<String> {
    let domain = sender.rsplit('@').next()?.to_lowercase();
    let mut labels: Vec<&str> = domain.split('.').filter(|l| !l.is_empty()).collect();
    labels.pop()?;
    if labels.len() > 1 && labels.last().is_some_and(|l| SUFFIXES.contains(l)) {
        labels.pop();
    }
    let brand = labels.pop()?;
    (brand.chars().count() >= 3 && brand.chars().all(char::is_alphanumeric))
        .then(|| brand.to_owned())
}

fn capitalize(word: &str) -> String {
    let mut chars = word.chars();
    chars
        .next()
        .map(|c| c.to_uppercase().chain(chars).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster(size: usize, keywords: &[(&str, f64)], senders: &[(&str, usize)]) -> ClusterSummary {
        ClusterSummary {
            size,
            keywords: keywords.iter().map(|(t, w)| ((*t).into(), *w)).collect(),
            senders: senders.iter().map(|(s, n)| ((*s).into(), *n)).collect(),
            sample_subjects: Vec::new(),
        }
    }

    #[test]
    fn the_most_distinctive_term_names_a_mixed_cluster() {
        let c = cluster(
            40,
            &[
                ("spedizione", 0.5),
                ("ordine", 0.9),
                ("tracking", 0.3),
                ("pacco", 0.2),
            ],
            &[("a.example", 10), ("b.example", 9), ("c.example", 8)],
        );
        let named = name(&c);
        assert_eq!(named.name, "Ordine");
        assert_eq!(named.keywords, ["ordine", "spedizione", "tracking"]);
    }

    #[test]
    fn a_dominant_sender_domain_is_the_name_and_leads_the_keywords() {
        let c = cluster(
            43,
            &[("amazon", 0.8), ("ordine", 0.7), ("spedizione", 0.6)],
            &[("mail.amazon.it", 30), ("altro.example", 13)],
        );
        let named = name(&c);
        assert_eq!(named.name, "Amazon");
        assert_eq!(named.keywords, ["amazon", "ordine", "spedizione"]);
    }

    #[test]
    fn a_domain_with_a_minority_share_or_a_free_mailbox_is_not_a_brand() {
        let spread = cluster(
            10,
            &[("ordine", 1.0)],
            &[("shop.example", 5), ("x.example", 5)],
        );
        assert_eq!(name(&spread).name, "Ordine");
        let personal = cluster(10, &[("viaggio", 1.0)], &[("gmail.com", 9)]);
        assert_eq!(name(&personal).name, "Viaggio");
    }

    #[test]
    fn stop_words_numbers_and_short_terms_are_ignored() {
        let c = cluster(
            10,
            &[
                ("della", 3.0),
                ("the", 2.0),
                ("2024", 2.0),
                ("ok", 2.0),
                ("fattura", 0.1),
            ],
            &[],
        );
        let named = name(&c);
        assert_eq!(named.name, "Fattura");
        assert_eq!(named.keywords, ["fattura"]);
    }

    #[test]
    fn brands_are_read_from_addresses_and_suffixes() {
        assert_eq!(brand("orders@amazon.co.uk").as_deref(), Some("amazon"));
        assert_eq!(brand("news.paypal.com").as_deref(), Some("paypal"));
        assert_eq!(brand("localhost"), None);
    }

    #[test]
    fn the_same_cluster_always_gets_the_same_name() {
        let c = cluster(5, &[("beta", 1.0), ("alfa", 1.0), ("gamma", 1.0)], &[]);
        let first = name(&c);
        assert_eq!(first.name, "Alfa");
        assert_eq!(first.keywords, ["alfa", "beta", "gamma"]);
        let mut shuffled = c.clone();
        shuffled.keywords.reverse();
        assert_eq!(name(&shuffled), first);
    }

    #[test]
    fn nothing_to_go_on_gets_a_plain_name() {
        let named = name(&cluster(5, &[("e", 1.0)], &[("gmail.com", 5)]));
        assert_eq!(named.name, FALLBACK);
        assert!(named.keywords.is_empty());
    }

    #[tokio::test]
    async fn it_is_a_label_namer() {
        let c = cluster(5, &[("fattura", 1.0)], &[]);
        assert_eq!(KeywordNamer.name(&c).await.name, "Fattura");
    }
}
