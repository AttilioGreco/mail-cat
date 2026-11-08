use super::{DeepLink, INITIAL_WATCH_BACKOFF, MAX_WATCH_BACKOFF, next_backoff};

fn link(a: &str, m: &str, msg: &str) -> DeepLink {
    DeepLink::Message {
        account_id: a.into(),
        mailbox_id: m.into(),
        message_id: msg.into(),
    }
}

#[test]
fn watch_backoff_doubles_and_caps() {
    let mut d = INITIAL_WATCH_BACKOFF;
    let mut seen = vec![d];
    for _ in 0..10 {
        d = next_backoff(d);
        seen.push(d);
    }
    assert_eq!(seen[1], INITIAL_WATCH_BACKOFF * 2, "the delay doubles");
    assert_eq!(d, MAX_WATCH_BACKOFF, "and stops at the ceiling");
    assert!(
        seen.windows(2).all(|w| w[1] >= w[0]),
        "never goes backwards: {seen:?}"
    );
}

#[test]
fn parses_both_flag_spellings() {
    let expected = Some(link("acc", "mb", "msg"));
    assert_eq!(
        DeepLink::from_args(["--open-message", "acc/mb/msg"]),
        expected
    );
    assert_eq!(DeepLink::from_args(["--open-message=acc/mb/msg"]), expected);
    // Must still be found after unrelated arguments.
    assert_eq!(
        DeepLink::from_args(["--other", "x", "--open-message", "acc/mb/msg"]),
        expected
    );
}

#[test]
fn round_trips_through_the_wire_form() {
    let original = link("acc", "mb", "msg");
    let parsed = DeepLink::from_args(["--open-message", &original.to_arg_value()]);
    assert_eq!(parsed, Some(original));
}

/// A malformed link must degrade to "just open the app", never panic or
/// half-navigate — the notification path has no way to report an error.
#[test]
fn malformed_links_are_ignored() {
    assert_eq!(DeepLink::from_args(Vec::<String>::new()), None);
    assert_eq!(
        DeepLink::from_args(["--open-message"]),
        None,
        "flag with no value"
    );
    assert_eq!(
        DeepLink::from_args(["--open-message", "acc/mb"]),
        None,
        "too few parts"
    );
    assert_eq!(
        DeepLink::from_args(["--open-message", "a/b/c/d"]),
        None,
        "too many parts"
    );
    assert_eq!(
        DeepLink::from_args(["--open-message", "acc//msg"]),
        None,
        "empty part"
    );
    assert_eq!(DeepLink::from_args(["--open-message", "//"]), None);
    assert_eq!(
        DeepLink::from_args(["--open-messageXYZ", "a/b/c"]),
        None,
        "flag typo"
    );
}

#[test]
fn parses_an_event_link_with_encoded_slashes() {
    let expected = Some(DeepLink::Event {
        account_id: "acc".into(),
        calendar: "/dav/cal/".into(),
        href: "/dav/cal/a b.ics".into(),
        start: 1_793_000_000,
    });
    let value = "acc/%2Fdav%2Fcal%2F/%2Fdav%2Fcal%2Fa%20b%2Eics/1793000000";
    assert_eq!(DeepLink::from_args(["--open-event", value]), expected);
    assert_eq!(
        DeepLink::from_args([format!("--open-event={value}")]),
        expected
    );
    assert_eq!(
        DeepLink::from_args(["--open-event", &expected.clone().unwrap().to_arg_value()]),
        expected
    );
}

#[test]
fn malformed_event_links_are_ignored() {
    for value in [
        "acc/cal/href",
        "acc/cal/href/soon",
        "acc//href/1",
        "a/b/c/1/extra",
        "acc/%FF/href/1",
    ] {
        assert_eq!(
            DeepLink::from_args(["--open-event", value]),
            None,
            "{value}"
        );
    }
}

#[test]
fn the_json_shape_is_tagged_by_kind() {
    let event = DeepLink::Event {
        account_id: "acc".into(),
        calendar: "c".into(),
        href: "h".into(),
        start: 7,
    };
    assert_eq!(
        serde_json::to_string(&event).unwrap(),
        r#"{"kind":"event","accountId":"acc","calendar":"c","href":"h","start":7}"#
    );
    assert_eq!(
        serde_json::to_string(&link("a", "m", "x")).unwrap(),
        r#"{"kind":"message","accountId":"a","mailboxId":"m","messageId":"x"}"#
    );
}
