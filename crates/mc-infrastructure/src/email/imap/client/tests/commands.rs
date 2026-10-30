use super::*;

#[tokio::test]
async fn move_uses_uid_move_and_reports_the_new_uid() {
    let (mut session, commands) = scripted(server_reply).await;
    let caps = ServerCaps {
        move_cmd: true,
        uidplus: true,
        gmail: false,
    };

    let moved = move_uids(&mut session, caps, &[5], "Archive")
        .await
        .unwrap();

    assert_eq!(moved, [(5, 77)], "COPYUID arrives untagged with MOVE");
    assert_eq!(uid_commands(&commands), ["UID MOVE 5 \"Archive\""]);
}

#[tokio::test]
async fn move_without_move_expunges_only_its_own_uids() {
    let (mut session, commands) = scripted(server_reply).await;
    let caps = ServerCaps {
        move_cmd: false,
        uidplus: true,
        gmail: false,
    };

    let moved = move_uids(&mut session, caps, &[5], "Archive")
        .await
        .unwrap();

    assert_eq!(moved, [(5, 78)]);
    assert_eq!(
        uid_commands(&commands),
        [
            "UID COPY 5 \"Archive\"",
            "UID STORE 5 +FLAGS.SILENT (\\Deleted)",
            "UID EXPUNGE 5"
        ]
    );
}

/// A plain EXPUNGE would purge mail another client merely marked.
#[tokio::test]
async fn without_uidplus_nothing_is_ever_expunged() {
    let caps = ServerCaps::default();

    let (mut session, commands) = scripted(server_reply).await;
    delete_uids(&mut session, caps, &[6], true).await.unwrap();
    assert_eq!(
        uid_commands(&commands),
        ["UID STORE 6 +FLAGS.SILENT (\\Deleted)"]
    );

    let (mut session, commands) = scripted(server_reply).await;
    move_uids(&mut session, caps, &[5], "Archive")
        .await
        .unwrap();
    assert_eq!(
        uid_commands(&commands),
        [
            "UID COPY 5 \"Archive\"",
            "UID STORE 5 +FLAGS.SILENT (\\Deleted)"
        ]
    );
}

#[tokio::test]
async fn unknown_capabilities_are_asked_again_and_refuse_when_still_unknown() {
    let (mut session, _) = scripted(|command| {
        if command == "CAPABILITY" {
            "* CAPABILITY IMAP4rev1 MOVE UIDPLUS\r\n$T OK Done\r\n"
        } else {
            "$T OK Done\r\n"
        }
    })
    .await;
    let cell = OnceLock::new();
    let caps = known_caps(&cell, &mut session).await.unwrap();
    assert!(caps.move_cmd && caps.uidplus);
    assert!(cell.get().is_some(), "learned once");

    let (mut session, commands) = scripted(|_| "$T BAD no\r\n").await;
    let err = known_caps(&OnceLock::new(), &mut session)
        .await
        .unwrap_err();
    assert!(matches!(err, DomainError::Provider(_)), "{err:?}");
    assert_eq!(
        uid_commands(&commands),
        ["CAPABILITY"],
        "nothing moved or deleted on a guess"
    );
}

#[tokio::test]
async fn uid_flags_are_fetched_in_bounded_batches() {
    let (mut session, commands) = scripted(|command| {
        if command == "UID SEARCH ALL" {
            // Two batches' worth: 1..=5000 and 5001..=5002.
            Box::leak(
                format!(
                    "* SEARCH {}\r\n$T OK Done\r\n",
                    (1..=5002)
                        .map(|u| u.to_string())
                        .collect::<Vec<_>>()
                        .join(" ")
                )
                .into_boxed_str(),
            )
        } else {
            "$T OK Done\r\n"
        }
    })
    .await;

    uid_flags(&mut session, false).await.unwrap();

    assert_eq!(
        uid_commands(&commands),
        [
            "UID SEARCH ALL",
            "UID FETCH 1:5000 (FLAGS)",
            "UID FETCH 5001:5002 (FLAGS)"
        ]
    );
}

#[test]
fn append_time_grows_with_the_message() {
    assert_eq!(append_timeout(1024), COMMAND_TIMEOUT);
    assert_eq!(
        append_timeout(30 * 1024 * 1024),
        COMMAND_TIMEOUT + Duration::from_secs(480)
    );
}

#[test]
fn an_empty_message_id_is_never_searched() {
    assert!(message_id_search("<>").unwrap().is_none());
    assert!(message_id_search("  ").unwrap().is_none());
    assert_eq!(
        message_id_search("<a@b>").unwrap().as_deref(),
        Some("UNDELETED HEADER Message-ID \"<a@b>\"")
    );
}

#[test]
fn imap4rev2_implies_move_and_uidplus() {
    let rev2 = ServerCaps::from_names(|name| name == "IMAP4rev2").unwrap();
    assert!(rev2.move_cmd && rev2.uidplus);
    let rev1 = ServerCaps::from_names(|name| ["IMAP4rev1", "UIDPLUS"].contains(&name)).unwrap();
    assert!(!rev1.move_cmd && rev1.uidplus);
    assert!(
        ServerCaps::from_names(|_| false).is_none(),
        "an empty answer is no answer"
    );
}

#[test]
fn copy_reports_the_destination_uid_only_with_uidplus() {
    use async_imap::imap_proto::{Response, parser::parse_response};

    let code_of = |line: &'static [u8]| match parse_response(line).unwrap().1 {
        Response::Done { code, .. } => code,
        other => panic!("not a tagged reply: {other:?}"),
    };
    assert_eq!(
        copied_uids(code_of(b"A3 OK [COPYUID 38505 304 3956] Done\r\n").as_ref()),
        [(304, 3956)]
    );
    assert_eq!(
        copied_uids(code_of(b"A3 OK [COPYUID 38505 304,319:320 3956:3958] Done\r\n").as_ref()),
        [(304, 3956), (319, 3957), (320, 3958)]
    );
    assert!(copied_uids(code_of(b"A3 OK Done\r\n").as_ref()).is_empty());
}

#[test]
fn test_imap_provider_creation() {
    let provider = ImapEmailProvider::new(
        "imap.example.com".to_string(),
        993,
        "user@example.com".to_string(),
        Arc::new(LoginSecret::Password("password".to_string())),
        true,
        None, // no SMTP sender in unit test
    );

    let (host, port, username, use_tls) = provider.config();
    assert_eq!(host, "imap.example.com");
    assert_eq!(port, 993);
    assert_eq!(username, "user@example.com");
    assert!(use_tls);
}

#[test]
fn xoauth2_sasl_string_has_expected_format() {
    let mut auth = XOAuth2Authenticator {
        user: "me@gmail.com".into(),
        access_token: "ya29.abc".into(),
        error: None,
    };
    let response = async_imap::Authenticator::process(&mut &mut auth, &[]);
    assert_eq!(
        response,
        "user=me@gmail.com\x01auth=Bearer ya29.abc\x01\x01"
    );

    // The error challenge is answered empty, never with the token again.
    let error = br#"{"status":"400","schemes":"Bearer","scope":"https://mail.google.com/"}"#;
    assert_eq!(
        async_imap::Authenticator::process(&mut &mut auth, error),
        ""
    );
    assert!(xoauth2_rejects_token(auth.error.as_deref().unwrap()));
    assert!(!xoauth2_rejects_token(br#"{"status":"503"}"#));
}

#[test]
fn gmail_extension_is_read_from_capabilities() {
    let gmail = ServerCaps::from_names(|name| ["IMAP4rev1", "X-GM-EXT-1"].contains(&name)).unwrap();
    assert!(gmail.gmail);
    assert!(
        !ServerCaps::from_names(|name| name == "IMAP4rev1")
            .unwrap()
            .gmail
    );
}

#[tokio::test]
async fn gmail_listing_asks_for_the_message_id_and_parses_it() {
    let (mut session, commands) = scripted(|command| {
        if command == "UID SEARCH ALL" {
            "* SEARCH 4\r\n$T OK Done\r\n"
        } else {
            "* 1 FETCH (UID 4 FLAGS (\\Seen) X-GM-MSGID 1278455344230334865)\r\n$T OK Done\r\n"
        }
    })
    .await;

    let fetches = uid_flags(&mut session, true).await.unwrap();

    assert_eq!(
        uid_commands(&commands),
        ["UID SEARCH ALL", "UID FETCH 4 (FLAGS X-GM-MSGID)"]
    );
    assert_eq!(fetches[0].gmail_msg_id(), Some(&1278455344230334865));
}

#[test]
fn envelope_fetch_asks_for_the_message_id_only_on_gmail() {
    assert!(super::fetch::envelope_items(true).contains("X-GM-MSGID"));
    assert_eq!(
        super::fetch::envelope_items(false),
        "(ENVELOPE FLAGS INTERNALDATE BODY.PEEK[HEADER])"
    );
}

#[tokio::test]
async fn subscribing_names_the_mailbox_as_the_server_spelled_it() {
    use super::super::mailbox::set_subscribed_on;
    let (mut session, commands) = scripted(server_reply).await;

    set_subscribed_on(&mut session, "Clienti/Caf&AOk-", true)
        .await
        .unwrap();
    set_subscribed_on(&mut session, "Say \"hi\"", false)
        .await
        .unwrap();

    assert_eq!(
        uid_commands(&commands),
        [
            "SUBSCRIBE \"Clienti/Caf&AOk-\"",
            "UNSUBSCRIBE \"Say \\\"hi\\\"\""
        ]
    );
}

#[tokio::test]
async fn a_refused_subscription_is_an_error() {
    use super::super::mailbox::set_subscribed_on;
    let (mut session, _) = scripted(|_| "$T NO [CANNOT] not now\r\n").await;

    assert!(set_subscribed_on(&mut session, "Work", true).await.is_err());
}

#[tokio::test]
async fn lsub_lists_the_subscribed_names_and_nothing_means_everything() {
    use super::super::mailbox::lsub_names;
    let (mut session, commands) = scripted(
        |_| "* LSUB (\\HasNoChildren) \"/\" \"Work\"\r\n* LSUB () \"/\" INBOX\r\n$T OK Done\r\n",
    )
    .await;
    let names = lsub_names(&mut session).await.unwrap();
    assert_eq!(uid_commands(&commands), ["LSUB \"\" \"*\""]);
    assert!(names.contains("Work") && names.contains("INBOX") && names.len() == 2);

    // A \Noselect parent only stands in for a subscribed child.
    let (mut session, _) = scripted(
        |_| "* LSUB (\\Noselect) \"/\" \"Parent\"\r\n* LSUB () \"/\" \"Parent/Kid\"\r\n$T OK Done\r\n",
    )
    .await;
    let names = lsub_names(&mut session).await.unwrap();
    assert!(names.contains("Parent/Kid") && !names.contains("Parent"));

    let (mut session, _) = scripted(|_| "$T OK Done\r\n").await;
    assert!(
        lsub_names(&mut session).await.is_none(),
        "empty: no subscriptions were ever made"
    );

    let (mut session, _) = scripted(|_| "$T BAD unknown command\r\n").await;
    assert!(lsub_names(&mut session).await.is_none(), "unsupported");
}

#[test]
fn initial_subscription_rules() {
    use super::super::mailbox::{Listed, is_subscribed};
    use mc_domain::{AccountId, Mailbox, MailboxType};
    use std::collections::HashSet;

    let listed = |name: &str, kind| Listed {
        name: name.to_owned(),
        no_select: false,
        mailbox: Mailbox::discover(
            AccountId::new(),
            mc_domain::MailboxPath::parse(name, '/').unwrap(),
            kind,
        )
        .0,
    };
    let lsub: HashSet<String> = ["Work".to_owned()].into();
    let some = Some(&lsub);

    // Generic IMAP: LSUB decides, except the Inbox; no LSUB means all subscribed.
    assert!(is_subscribed(
        &listed("Work", MailboxType::Custom),
        some,
        false
    ));
    assert!(!is_subscribed(
        &listed("Other", MailboxType::Custom),
        some,
        false
    ));
    for (name, kind) in [
        ("Sent", MailboxType::Sent),
        ("Drafts", MailboxType::Drafts),
        ("All", MailboxType::All),
        ("Archive", MailboxType::Archive),
    ] {
        assert!(is_subscribed(&listed(name, kind), some, false), "{name}");
        assert!(is_subscribed(&listed(name, kind), some, true), "{name}");
    }
    assert!(!is_subscribed(
        &listed("Junk", MailboxType::Spam),
        some,
        false
    ));
    assert!(is_subscribed(
        &listed("INBOX", MailboxType::Inbox),
        some,
        false
    ));
    assert!(is_subscribed(
        &listed("Other", MailboxType::Custom),
        None,
        false
    ));
    assert!(is_subscribed(
        &listed("Trash", MailboxType::Trash),
        None,
        false
    ));

    // Gmail layout: Spam and Trash are off whatever LSUB says; views follow it.
    let everything: HashSet<String> = ["Trash".to_owned(), "Spam".to_owned()].into();
    assert!(!is_subscribed(
        &listed("Trash", MailboxType::Trash),
        Some(&everything),
        true
    ));
    assert!(!is_subscribed(
        &listed("Spam", MailboxType::Spam),
        None,
        true
    ));
    assert!(is_subscribed(
        &listed("Work", MailboxType::Custom),
        None,
        true
    ));
    assert!(!is_subscribed(
        &listed("Other", MailboxType::Custom),
        some,
        true
    ));
    // The \Noselect "[Gmail]" container is hidden on Gmail only.
    let container = Listed {
        no_select: true,
        ..listed("[Gmail]", MailboxType::Custom)
    };
    assert!(!is_subscribed(&container, None, true));
    assert!(is_subscribed(&container, None, false));
}
