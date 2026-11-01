use super::*;
use chrono::TimeZone;

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 1, 12, 0, 0).unwrap()
}

fn parsed(input: &str) -> Expr {
    parse(input, now()).unwrap()
}

fn word(w: &str) -> Expr {
    Expr::Text(w.into())
}

#[test]
fn words_are_an_implicit_and_and_phrases_stay_whole() {
    assert_eq!(
        parsed(r#"hello "big world""#),
        Expr::And(vec![word("hello"), word("big world")])
    );
}

#[test]
fn or_binds_tighter_than_the_implicit_and() {
    assert_eq!(
        parsed("a b OR c"),
        Expr::And(vec![word("a"), Expr::Or(vec![word("b"), word("c")])])
    );
}

#[test]
fn parentheses_group_and_a_leading_minus_negates() {
    assert_eq!(
        parsed("(a b) OR -c"),
        Expr::And(vec![Expr::Or(vec![
            Expr::And(vec![word("a"), word("b")]),
            Expr::Not(Box::new(word("c")))
        ])])
    );
    assert_eq!(
        parsed("-(a b)"),
        Expr::And(vec![Expr::Not(Box::new(Expr::And(vec![
            word("a"),
            word("b")
        ])))])
    );
}

#[test]
fn operators_take_quoted_values_and_negate() {
    assert_eq!(
        parsed(r#"label:"Newsletter/The Space Cinema" -label:Dev"#),
        Expr::And(vec![
            Expr::Label("Newsletter/The Space Cinema".into()),
            Expr::Not(Box::new(Expr::Label("Dev".into()))),
        ])
    );
}

#[test]
fn a_space_after_the_colon_still_binds_the_value() {
    assert_eq!(
        parsed("from: segreteria@corona.it"),
        Expr::And(vec![Expr::Field(
            TextField::From,
            "segreteria@corona.it".into()
        )])
    );
}

#[test]
fn sbj_is_an_alias_of_subject() {
    assert_eq!(parsed("sbj:fattura"), parsed("subject:fattura"));
}

#[test]
fn unknown_operators_and_values_are_plain_text() {
    assert_eq!(parsed("foo:bar"), Expr::And(vec![word("foo:bar")]));
    assert_eq!(parsed("is:maybe"), Expr::And(vec![word("is:maybe")]));
    assert_eq!(parsed("from:"), Expr::And(vec![word("from:")]));
}

#[test]
fn states_and_attachments() {
    assert_eq!(
        parsed("is:unread has:attachment filename:fattura"),
        Expr::And(vec![
            Expr::State(State::Unread),
            Expr::State(State::Attachment),
            Expr::Field(TextField::Filename, "fattura".into()),
        ])
    );
}

#[test]
fn dates_are_utc_midnights_and_relative_spans_count_back_from_now() {
    let ts = |y, m, d| Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap().timestamp();
    assert_eq!(
        parsed("after:2026-01-31 before:2026/02/01"),
        Expr::And(vec![
            Expr::Since(ts(2026, 1, 31)),
            Expr::Until(ts(2026, 2, 1))
        ])
    );
    let at = |y, m, d| Utc.with_ymd_and_hms(y, m, d, 12, 0, 0).unwrap().timestamp();
    assert_eq!(
        parsed("newer_than:7d older_than:2m"),
        Expr::And(vec![
            Expr::Since(at(2026, 10, 25)),
            Expr::Until(at(2026, 9, 1))
        ])
    );
    assert_eq!(
        parsed("newer_than:1y"),
        Expr::And(vec![Expr::Since(at(2025, 11, 1))])
    );
}

#[test]
fn malformed_dates_are_errors_that_name_the_operator() {
    for bad in [
        "after:yesterday",
        "before:2026-13-01",
        "newer_than:soon",
        "older_than:7x",
    ] {
        let err = parse(bad, now()).unwrap_err();
        assert!(err.starts_with(bad.split(':').next().unwrap()), "{err}");
    }
}

#[test]
fn half_typed_queries_do_not_fail() {
    for partial in ["(a", "a)", "a OR", "\"open", "-", "label:\"Dev"] {
        assert!(parse(partial, now()).is_ok(), "{partial}");
    }
}

#[test]
fn only_positive_text_is_highlighted() {
    let (expr, mut words) = (parsed("a -b (c OR d) from:x"), Vec::new());
    expr.positive_text(&mut words);
    assert_eq!(words, ["a", "c", "d"]);
}
