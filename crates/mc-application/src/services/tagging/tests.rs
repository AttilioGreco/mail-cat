use chrono::Utc;
use mc_domain::{
    AccountId, LabelMode, Lesson, ListHeaders, Mailbox, MailboxPath, MailboxType, MessageId,
    ModelStats, TagSource,
};

use super::placement::{GroupMailboxes, Home, Placement};
use super::retro::Plan;
use super::{bayes, eval, features, label_key, label_mailbox};

fn source(from: &str, subject: &str, labels: &[&str]) -> TagSource {
    TagSource {
        id: MessageId::new(),
        account_id: AccountId::new(),
        from_email: from.into(),
        from_name: None,
        subject: subject.into(),
        list_headers: ListHeaders::default(),
        body: None,
        labels: labels.iter().map(|l| l.to_string()).collect(),
        keywords: false,
        tags: true,
        folder: None,
        in_inbox: true,
        received_at: Utc::now(),
    }
}

fn teach(stats: &mut ModelStats, s: &TagSource, negatives: &[String], weight: f64) {
    let tokens = features::tokens(s, true);
    bayes::learn(
        stats,
        &Lesson {
            tokens: &tokens,
            positives: &s.labels,
            negatives,
            weight,
        },
    );
}

fn score(stats: &ModelStats, label: &str, s: &TagSource) -> Option<f64> {
    bayes::bayes_score(stats, label, &features::tokens(s, true))
}

fn trained() -> ModelStats {
    let mut stats = ModelStats::default();
    for i in 0..10 {
        teach(
            &mut stats,
            &source(
                "news@shop.example",
                &format!("offerte della settimana {i}"),
                &["newsletter"],
            ),
            &[],
            1.0,
        );
        teach(
            &mut stats,
            &source("anna@family.example", &format!("cena domenica {i}"), &[]),
            &[],
            0.3,
        );
        teach(
            &mut stats,
            &source(
                "boss@work.example",
                &format!("riunione progetto {i}"),
                &["lavoro"],
            ),
            &[],
            1.0,
        );
    }
    stats
}

#[test]
fn tokens_are_prefixed_distinct_and_lowercase() {
    let mut s = source("News@Shop.Example", "Offerte OFFERTE 2024", &[]);
    s.list_headers = ListHeaders {
        unsubscribe: true,
        bulk: true,
        ..ListHeaders::default()
    };
    s.body = Some("ciao ciao mondo".into());
    let tokens = features::tokens(&s, true);
    for want in [
        "f:news@shop.example",
        "d:shop.example",
        "s:offerte",
        "h:unsubscribe",
        "h:bulk",
        "b:mondo",
    ] {
        assert!(
            tokens.contains(&want.to_string()),
            "missing {want}: {tokens:?}"
        );
    }
    assert_eq!(tokens.iter().filter(|t| *t == "s:offerte").count(), 1);
    assert!(
        !tokens.iter().any(|t| t == "s:2024"),
        "numbers are not evidence"
    );
    assert!(
        !features::tokens(&s, false)
            .iter()
            .any(|t| t.starts_with("b:"))
    );
}

#[test]
fn scores_the_label_the_sender_belongs_to() {
    let stats = trained();
    let news = score(
        &stats,
        "newsletter",
        &source("news@shop.example", "offerte speciali", &[]),
    )
    .unwrap();
    let family = score(
        &stats,
        "newsletter",
        &source("anna@family.example", "cena", &[]),
    )
    .unwrap();
    assert!(news > 0.9, "{news}");
    assert!(family < 0.1, "{family}");
}

#[test]
fn unseen_tokens_and_unknown_labels_say_nothing() {
    let stats = trained();
    assert!(score(&stats, "mai-visto", &source("a@b.example", "x", &[])).is_none());
    let blank = score(
        &stats,
        "newsletter",
        &source("new@zzz.example", "qqqq", &[]),
    )
    .unwrap();
    assert!(blank < 0.5, "a stranger is not a newsletter: {blank}");
}

#[test]
fn withdrawing_a_lesson_restores_the_counts() {
    let mut stats = trained();
    let before = score(
        &stats,
        "newsletter",
        &source("news@shop.example", "offerte", &[]),
    )
    .unwrap();
    let extra = source(
        "news@shop.example",
        "offerte straordinarie",
        &["newsletter"],
    );
    teach(&mut stats, &extra, &[], 1.0);
    teach(&mut stats, &extra, &[], -1.0);
    let after = score(
        &stats,
        "newsletter",
        &source("news@shop.example", "offerte", &[]),
    )
    .unwrap();
    assert!((before - after).abs() < 1e-9);
}

#[test]
fn explicit_negatives_pull_the_label_down() {
    let mut stats = trained();
    let probe = source("news@shop.example", "offerte speciali", &[]);
    let before = score(&stats, "newsletter", &probe).unwrap();
    let rejected = source("news@shop.example", "offerte speciali", &[]);
    for _ in 0..6 {
        teach(&mut stats, &rejected, &["newsletter".to_string()], 0.3);
    }
    assert!(score(&stats, "newsletter", &probe).unwrap() < before);
}

#[test]
fn learning_incrementally_equals_learning_at_once() {
    let a = source("x@y.example", "alfa beta", &["uno"]);
    let b = source("z@y.example", "gamma", &[]);
    let mut one = ModelStats::default();
    teach(&mut one, &a, &[], 1.0);
    teach(&mut one, &b, &[], 0.3);
    let mut two = ModelStats::default();
    teach(&mut two, &b, &[], 0.3);
    teach(&mut two, &a, &[], 1.0);
    let probe = source("x@y.example", "alfa", &[]);
    assert_eq!(score(&one, "uno", &probe), score(&two, "uno", &probe));
}

fn mailbox(path: &str, kind: MailboxType) -> Mailbox {
    let account = AccountId::new();
    Mailbox::discover(
        account,
        MailboxPath::new(vec![path.into()], '/').unwrap(),
        kind,
    )
    .0
}

#[test]
fn labels_match_by_name_regardless_of_case() {
    let boxes = vec![
        mailbox("INBOX", MailboxType::Inbox),
        mailbox("Newsletter", MailboxType::Custom),
    ];
    assert!(label_mailbox(&boxes, &label_key(" NEWSLETTER ")).is_some());
    assert!(
        label_mailbox(&boxes, "inbox").is_none(),
        "only custom mailboxes are labels"
    );
}

fn group_of(account: AccountId, boxes: Vec<Mailbox>) -> GroupMailboxes {
    GroupMailboxes::from([(account, boxes)])
}

#[test]
fn preview_counts_and_samples_only_what_would_change() {
    let account = AccountId::new();
    let group = group_of(
        account,
        vec![
            mailbox("INBOX", MailboxType::Inbox),
            mailbox("Newsletter", MailboxType::Custom),
        ],
    );
    let mut plan = Plan::default();
    let mut apply = |subject: &str, labels: &[&str], in_inbox, score, mode| {
        let mut s = source("a@x.example", subject, labels);
        (s.account_id, s.in_inbox) = (account, in_inbox);
        plan.consider(&s, score, 0.9, mode, &group, "newsletter");
    };
    apply("uno", &[], true, 0.95, LabelMode::ApplyArchive);
    apply("due", &[], false, 0.99, LabelMode::ApplyArchive);
    apply("tre", &["newsletter"], true, 0.99, LabelMode::Apply);
    apply("quattro", &[], true, 0.5, LabelMode::Apply);
    let preview = plan.preview();
    assert_eq!((preview.to_label, preview.to_archive), (2, 1));
    assert_eq!(preview.sample[0].subject, "due", "surest first");
}

#[test]
fn a_missing_label_is_created_under_the_groups_spelling() {
    let (a, b) = (AccountId::new(), AccountId::new());
    let mut group = group_of(a, vec![mailbox("Newsletter", MailboxType::Custom)]);
    group.insert(b, vec![mailbox("INBOX", MailboxType::Inbox)]);
    let mut s = source("a@x.example", "x", &[]);
    s.account_id = b;
    let place = Placement::of(&s, &group, "newsletter", LabelMode::ApplyArchive);
    assert_eq!(place.home, Home::Create("Newsletter".into()));
    assert!(
        place.remove.is_some(),
        "the Inbox is dropped in the same change"
    );
}

#[test]
fn a_folders_account_labels_with_a_keyword_and_archives_by_move() {
    let account = AccountId::new();
    let group = group_of(account, vec![mailbox("INBOX", MailboxType::Inbox)]);
    let mut s = source("a@x.example", "x", &[]);
    (s.account_id, s.keywords) = (account, true);
    let place = Placement::of(&s, &group, "my label", LabelMode::ApplyArchive);
    assert_eq!(place.home, Home::Keyword("my_label".into()));
    assert!((place.remove, place.archive) == (None, true));
}

#[test]
fn eval_reports_precision_and_recall_on_held_out_mail() {
    let mut sources = Vec::new();
    for i in 0..60 {
        sources.push(source(
            "news@shop.example",
            &format!("offerte {i}"),
            &["newsletter"],
        ));
        sources.push(source("anna@family.example", &format!("cena {i}"), &[]));
    }
    let (test, train): (Vec<_>, Vec<_>) = sources
        .into_iter()
        .enumerate()
        .partition(|(i, _)| i % 5 == 0);
    let (test, train): (Vec<_>, Vec<_>) = (
        test.into_iter().map(|x| x.1).collect(),
        train.into_iter().map(|x| x.1).collect(),
    );
    let report = eval::evaluate(&train, &test, &["newsletter".to_string()]);
    let at_90 = report[0].rows.iter().find(|r| r.threshold == 0.9).unwrap();
    assert!(report[0].held_out > 0);
    assert_eq!(at_90.precision, 1.0);
    assert_eq!(at_90.recall, 1.0);
}
