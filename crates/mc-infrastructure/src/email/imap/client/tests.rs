use super::*;

fn structure_of(response: &[u8]) -> Vec<TextPart> {
    use async_imap::imap_proto::types::{AttributeValue, Response};
    let (_, Response::Fetch(_, attrs)) =
        async_imap::imap_proto::parser::parse_response(response).unwrap()
    else {
        panic!("not a FETCH response")
    };
    let structure = attrs
        .iter()
        .find_map(|a| match a {
            AttributeValue::BodyStructure(s) => Some(s),
            _ => None,
        })
        .unwrap();
    text_parts(structure)
}

#[test]
fn text_parts_skip_attachments_and_keep_the_first_of_each_kind() {
    // mixed( alternative(plain qp, html base64), plain attachment, pdf )
    let parts = structure_of(
        b"* 1 FETCH (UID 7 BODYSTRUCTURE (((\"TEXT\" \"PLAIN\" (\"CHARSET\" \"iso-8859-1\") NIL NIL \"QUOTED-PRINTABLE\" 12 1 NIL NIL NIL)\
(\"TEXT\" \"HTML\" (\"CHARSET\" \"utf-8\") NIL NIL \"BASE64\" 40 1 NIL NIL NIL) \"ALTERNATIVE\" (\"BOUNDARY\" \"b2\") NIL NIL)\
(\"TEXT\" \"PLAIN\" (\"NAME\" \"notes.txt\") NIL NIL \"7BIT\" 5000 80 NIL (\"ATTACHMENT\" (\"FILENAME\" \"notes.txt\")) NIL)\
(\"APPLICATION\" \"PDF\" (\"NAME\" \"big.pdf\") NIL NIL \"BASE64\" 9000000 NIL (\"ATTACHMENT\" (\"FILENAME\" \"big.pdf\")) NIL) \"MIXED\" (\"BOUNDARY\" \"b1\") NIL NIL))\r\n",
    );
    let sections: Vec<_> = parts.iter().map(|p| (p.section.clone(), p.html)).collect();
    assert_eq!(sections, [(vec![1, 1], false), (vec![1, 2], true)]);

    assert_eq!(
        parts[0].decode(b"caff=E8 =\r\nok").as_deref(),
        Some("caff\u{e8} ok")
    );
    assert_eq!(
        parts[1].decode(b"PHA+Y2lhbzwvcD4=").as_deref(),
        Some("<p>ciao</p>")
    );
}

#[test]
fn whole_message_fallback_skips_text_attachments() {
    let raw = b"Content-Type: multipart/mixed; boundary=b\r\n\r\n\
--b\r\nContent-Type: text/plain\r\nContent-Disposition: attachment; filename=log.txt\r\n\r\nlog\r\n\
--b\r\nContent-Type: text/plain\r\n\r\nciao\r\n\
--b\r\nContent-Type: text/html\r\n\r\n<p>ciao</p>\r\n--b--\r\n";
    let (text, html) = whole_message_text(raw);
    assert_eq!(text.as_deref().map(str::trim), Some("ciao"));
    assert_eq!(html.as_deref().map(str::trim), Some("<p>ciao</p>"));
}

#[test]
fn a_single_part_message_is_part_one() {
    let parts = structure_of(
        b"* 1 FETCH (UID 8 BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"utf-8\") NIL NIL \"7BIT\" 3 1 NIL NIL NIL))\r\n",
    );
    assert_eq!(
        parts.iter().map(|p| p.section.clone()).collect::<Vec<_>>(),
        [vec![1]]
    );
}

type FakeSession = async_imap::Session<tokio_util::compat::Compat<tokio::io::DuplexStream>>;

/// A logged-in session against an in-memory server that answers each
/// command with `reply(command)` (`$T` = its tag) and records the commands.
async fn scripted(reply: fn(&str) -> &'static str) -> (FakeSession, Arc<Mutex<Vec<String>>>) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio_util::compat::TokioAsyncReadCompatExt;

    let (client_end, server_end) = tokio::io::duplex(64 * 1024);
    let commands = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&commands);
    tokio::spawn(async move {
        let (read, mut write) = tokio::io::split(server_end);
        write.write_all(b"* OK ready\r\n").await.unwrap();
        let mut lines = BufReader::new(read).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let (tag, command) = line.split_once(' ').unwrap();
            seen.lock().unwrap().push(command.to_owned());
            let answer = if command.starts_with("LOGIN") {
                "$T OK logged in\r\n"
            } else {
                reply(command)
            };
            write
                .write_all(answer.replace("$T", tag).as_bytes())
                .await
                .unwrap();
        }
    });

    let mut client = async_imap::Client::new(client_end.compat());
    client.read_response().await.unwrap();
    let session = client.login("user", "pass").await.map_err(|e| e.0).unwrap();
    (session, commands)
}

fn uid_commands(commands: &Mutex<Vec<String>>) -> Vec<String> {
    commands
        .lock()
        .unwrap()
        .iter()
        .filter(|c| !c.starts_with("LOGIN"))
        .cloned()
        .collect()
}

fn server_reply(command: &str) -> &'static str {
    if command.starts_with("UID MOVE") {
        "* OK [COPYUID 7 5 77] Moved\r\n* 3 EXPUNGE\r\n$T OK Done\r\n"
    } else if command.starts_with("UID COPY") {
        "$T OK [COPYUID 7 5 78] Done\r\n"
    } else if command.contains("EXPUNGE") {
        "* 3 EXPUNGE\r\n$T OK Done\r\n"
    } else {
        "$T OK Done\r\n"
    }
}

#[tokio::test]
async fn move_uses_uid_move_and_reports_the_new_uid() {
    let (mut session, commands) = scripted(server_reply).await;
    let caps = ServerCaps {
        move_cmd: true,
        uidplus: true,
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

    uid_flags(&mut session).await.unwrap();

    assert_eq!(
        uid_commands(&commands),
        [
            "UID SEARCH ALL",
            "UID FETCH 1:5000 (FLAGS)",
            "UID FETCH 5001:5002 (FLAGS)"
        ]
    );
}

/// The UIDs of a `UID FETCH <set> ...` command.
fn fetched_uids(command: &str) -> Vec<u32> {
    let set = command.split(' ').nth(2).unwrap();
    set.split(',')
        .flat_map(|range| match range.split_once(':') {
            Some((a, b)) => (a.parse().unwrap()..=b.parse().unwrap()).collect::<Vec<u32>>(),
            None => vec![range.parse().unwrap()],
        })
        .collect()
}

fn leaked(reply: String) -> &'static str {
    Box::leak(reply.into_boxed_str())
}

/// One plain-text part of `octets` bytes per UID, except UID 3, whose
/// structure the parser rejects. Bodies are `body-<uid>`.
fn backfill_server(command: &str) -> &'static str {
    let uids = || fetched_uids(command);
    let mut reply = String::new();
    if command.ends_with("(UID BODYSTRUCTURE)") {
        for uid in uids() {
            reply += &if uid == 3 {
                "* 3 FETCH (UID 3 BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"utf-8\") NIL NIL \"7BIT\" x))\r\n".to_owned()
            } else {
                format!(
                    "* {uid} FETCH (UID {uid} BODYSTRUCTURE (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"utf-8\") NIL NIL \"7BIT\" 900000 1 NIL NIL NIL))\r\n"
                )
            };
        }
    } else if command.contains("BODY.PEEK[1]") {
        for uid in uids() {
            let body = format!("body-{uid}\r\ncut");
            reply += &format!(
                "* {uid} FETCH (UID {uid} BODY[1]<0> {{{}}}\r\n{body})\r\n",
                body.len()
            );
        }
    } else if command.contains("BODY.PEEK[]") {
        let raw = "Subject: odd\r\nContent-Type: text/plain\r\n\r\nhello\r\n";
        reply += &format!("* 3 FETCH (UID 3 BODY[]<0> {{{}}}\r\n{raw})\r\n", raw.len());
    }
    leaked(reply + "$T OK Done\r\n")
}

#[tokio::test]
async fn text_parts_are_fetched_up_to_the_cap_and_marked_truncated() {
    let (mut session, commands) = scripted(backfill_server).await;

    let (found, bytes) = text_bodies(&mut session, &[7], Some(TEXT_PART_CAP))
        .await
        .unwrap();

    assert_eq!(
        uid_commands(&commands),
        [
            "UID FETCH 7 (UID BODYSTRUCTURE)",
            "UID FETCH 7 (UID BODY.PEEK[1]<0.262144>)"
        ]
    );
    assert!(found[0].truncated, "a 900 kB part past the cap");
    // Up to its last whole line: the rest is fetched when it is opened.
    assert_eq!(found[0].body_text.as_deref(), Some("body-7\r\n"));
    assert_eq!(bytes, 1024 + 11);
}

#[tokio::test]
async fn without_a_cap_the_whole_part_is_fetched() {
    let (mut session, commands) = scripted(backfill_server).await;

    let (found, _) = text_bodies(&mut session, &[7], None).await.unwrap();

    assert_eq!(uid_commands(&commands)[1], "UID FETCH 7 (UID BODY.PEEK[1])");
    assert!(!found[0].truncated);
}

#[tokio::test]
async fn a_message_with_only_attachments_gets_an_empty_body() {
    let (mut session, commands) = scripted(|command| {
        if command.ends_with("(UID BODYSTRUCTURE)") {
            "* 1 FETCH (UID 9 BODYSTRUCTURE ((\"APPLICATION\" \"PDF\" (\"NAME\" \"big.pdf\") NIL NIL \"BASE64\" 9000000 NIL (\"ATTACHMENT\" (\"FILENAME\" \"big.pdf\")) NIL) \"MIXED\" (\"BOUNDARY\" \"b1\") NIL NIL))\r\n$T OK Done\r\n"
        } else {
            "$T OK Done\r\n"
        }
    })
    .await;

    let (found, _) = text_bodies(&mut session, &[9], Some(TEXT_PART_CAP))
        .await
        .unwrap();

    // A result all the same, so the message is stored empty and dequeued.
    assert_eq!(found.len(), 1);
    assert!(found[0].body_text.is_none() && found[0].body_html.is_none());
    assert_eq!(uid_commands(&commands).len(), 1, "nothing to download");
}

#[tokio::test]
async fn one_unreadable_structure_costs_only_its_own_message() {
    let sessions = Arc::new(Mutex::new(Vec::new()));
    let connect = || {
        let sessions = Arc::clone(&sessions);
        async move {
            let (session, commands) = scripted(backfill_server).await;
            sessions.lock().unwrap().push(commands);
            session
        }
    };

    let found = isolating(
        &[1, 2, 3, 4, 5],
        |set| {
            let session = connect();
            async move {
                Ok(text_bodies(&mut session.await, set, Some(TEXT_PART_CAP))
                    .await
                    .map(|(r, _)| r))
            }
        },
        |uid| {
            let session = connect();
            async move {
                Ok(message_start(&mut session.await, uid, TEXT_PART_CAP)
                    .await
                    .map(|(r, _)| r))
            }
        },
    )
    .await
    .unwrap();

    let mut texts: Vec<_> = found
        .iter()
        .map(|r| (r.remote_id.as_str().parse::<u32>().unwrap(), r.body_text.clone().unwrap()))
        .collect();
    texts.sort();
    assert_eq!(texts[2], (3, "hello\r\n".to_owned()));
    assert_eq!(
        texts.iter().filter(|(_, t)| t.starts_with("body-")).count(),
        4
    );

    let commands: Vec<String> = sessions
        .lock()
        .unwrap()
        .iter()
        .flat_map(|c| uid_commands(c))
        .collect();
    let raw: Vec<&String> = commands
        .iter()
        .filter(|c| c.contains("BODY.PEEK[]"))
        .collect();
    // Only the bad one goes raw, and never past the cap.
    assert_eq!(raw, ["UID FETCH 3 (UID BODY.PEEK[]<0.262144>)"]);
    // Halving, not one retry per message: 1:5, 1:2, 3:5, 3, 4:5.
    assert_eq!(
        commands
            .iter()
            .filter(|c| c.ends_with("(UID BODYSTRUCTURE)"))
            .count(),
        5,
        "{commands:#?}"
    );
}

/// `isolating` over sessions that all answer with `reply`; the commands
/// the server saw are returned with the outcome.
async fn isolated_against(
    reply: fn(&str) -> &'static str,
) -> (Result<usize, DomainError>, Vec<String>) {
    let sessions = Arc::new(Mutex::new(Vec::new()));
    let connect = || {
        let sessions = Arc::clone(&sessions);
        async move {
            let (session, commands) = scripted(reply).await;
            sessions.lock().unwrap().push(commands);
            session
        }
    };
    let found = isolating(
        &[1, 2, 3, 4, 5],
        |set| {
            let session = connect();
            async move {
                Ok(text_bodies(&mut session.await, set, Some(TEXT_PART_CAP))
                    .await
                    .map(|(r, _)| r))
            }
        },
        |uid| {
            let session = connect();
            async move {
                Ok(message_start(&mut session.await, uid, TEXT_PART_CAP)
                    .await
                    .map(|(r, _)| r))
            }
        },
    )
    .await;
    let commands = sessions
        .lock()
        .unwrap()
        .iter()
        .flat_map(|c| uid_commands(c))
        .collect();
    (found.map(|r| r.len()), commands)
}

#[tokio::test]
async fn a_server_refusing_every_fetch_is_asked_once() {
    let (found, commands) = isolated_against(|_| "$T NO [SERVERBUG] try later\r\n").await;

    assert!(matches!(found, Err(DomainError::Provider(_))), "{found:?}");
    assert_eq!(commands.len(), 1, "{commands:#?}");
}

#[tokio::test]
async fn an_exhausted_quota_is_a_throttle() {
    let (found, commands) =
        isolated_against(|_| "$T NO [OVERQUOTA] Account exceeded bandwidth limits.\r\n").await;

    assert!(matches!(found, Err(DomainError::Throttled(_))), "{found:?}");
    assert_eq!(commands.len(), 1);
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

/// Accepts the connection, then never says a word.
async fn silent_server() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _connection = listener.accept().await;
        std::future::pending::<()>().await;
    });
    port
}

#[tokio::test(start_paused = true)]
async fn a_server_that_never_answers_times_out_instead_of_hanging() {
    let port = silent_server().await;
    let provider = ImapEmailProvider::new(
        "127.0.0.1".into(),
        port,
        "user".into(),
        Arc::new(LoginSecret::Password("pass".into())),
        true,
        None,
    );

    let err = provider
        .mailbox_changes(&MailboxPath::inbox(), None)
        .await
        .unwrap_err();

    assert!(matches!(err, DomainError::ConnectionTimeout(_)), "{err:?}");
}

#[tokio::test(start_paused = true)]
async fn a_command_left_unanswered_times_out() {
    let (mut session, _) = scripted(|command| {
        if command.starts_with("UID MOVE") {
            ""
        } else {
            "$T OK Done\r\n"
        }
    })
    .await;
    let caps = ServerCaps {
        move_cmd: true,
        uidplus: true,
    };

    let err = timed(
        COMMAND_TIMEOUT,
        "Move",
        move_uids(&mut session, caps, &[5], "Archive"),
    )
    .await
    .unwrap_err();

    assert!(matches!(err, DomainError::ConnectionTimeout(_)), "{err:?}");
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

/// Logs in through the real async-imap client against a server that greets,
/// then answers the login with `script` (raw lines; `$T` is the tag,
/// `<` waits for a client line). Returns how the login failed.
async fn login_against(secret: LoginSecret, script: &'static [&'static str]) -> DomainError {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio_util::compat::TokioAsyncReadCompatExt;

    let (client_end, server_end) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let (read, mut write) = tokio::io::split(server_end);
        write
            .write_all(b"* OK [CAPABILITY IMAP4rev1 AUTH=XOAUTH2] ready\r\n")
            .await
            .unwrap();
        let mut lines = BufReader::new(read).lines();
        let first = lines.next_line().await.unwrap().unwrap();
        let tag = first.split_once(' ').unwrap().0.to_owned();
        for step in script {
            if *step == "<" {
                lines.next_line().await.unwrap();
            } else {
                write
                    .write_all(format!("{}\r\n", step.replace("$T", &tag)).as_bytes())
                    .await
                    .unwrap();
            }
        }
    });

    let mut client = async_imap::Client::new(client_end.compat());
    client.read_response().await.unwrap();
    match log_in(client, "me@example.com", secret).await {
        Ok(_) => panic!("the login succeeded"),
        Err(e) => e,
    }
}

fn password() -> LoginSecret {
    LoginSecret::Password("hunter2".into())
}

#[tokio::test]
async fn login_replies_from_real_servers_are_classified_from_raw_bytes() {
    fn kind(e: &DomainError) -> &'static str {
        match e {
            DomainError::AuthenticationFailed(_) => "refused",
            DomainError::Throttled(_) => "throttled",
            DomainError::ConnectionFailed(_) => "transient",
            _ => "other",
        }
    }
    let cases: [(&'static str, &str); 13] = [
        // Gmail
        (
            "$T NO [AUTHENTICATIONFAILED] Invalid credentials (Failure)",
            "refused",
        ),
        (
            "$T NO [ALERT] Application-specific password required: https://support.google.com/accounts/answer/185833 (Failure)",
            "refused",
        ),
        (
            "$T NO [ALERT] Please log in via your web browser: https://support.google.com/mail/accounts/answer/78754 (Failure)",
            "refused",
        ),
        (
            "$T NO [WEBALERT https://accounts.google.com/signin/continue?sarp=1] Web login required.",
            "refused",
        ),
        (
            "$T NO [ALERT] Too many simultaneous connections. (Failure)",
            "throttled",
        ),
        (
            "$T NO [ALERT] Account exceeded command or bandwidth limits. (Failure)",
            "throttled",
        ),
        ("$T NO [UNAVAILABLE] Temporary System Error", "transient"),
        // Dovecot
        (
            "$T NO [AUTHENTICATIONFAILED] Authentication failed.",
            "refused",
        ),
        (
            "$T NO [UNAVAILABLE] Temporary authentication failure. [mx1:2026-10-23 10:00:00]",
            "transient",
        ),
        ("$T NO [INUSE] Mailbox is locked.", "transient"),
        // Stalwart
        (
            "$T NO [LIMIT] Too many authentication requests from this IP address.",
            "transient",
        ),
        // Old servers: a bare NO
        ("$T NO LOGIN failed.", "refused"),
        ("$T BAD Invalid SASL argument.", "transient"),
    ];
    for (reply, expected) in cases {
        let script: &'static [&'static str] = Box::leak(Box::new([reply]));
        let e = login_against(password(), script).await;
        assert_eq!(kind(&e), expected, "{reply} → {e:?}");
    }

    // The reason the user sees is the server's own text, not async-imap's rendering.
    let e = login_against(
        password(),
        &["$T NO [ALERT] Please log in via your web browser: https://x (Failure)"],
    )
    .await;
    assert_eq!(
        e.to_string(),
        "Authentication failed: Please log in via your web browser: https://x (Failure)"
    );
}

#[tokio::test]
async fn an_xoauth2_error_challenge_refuses_the_token() {
    // Gmail: `+ ` for the token, then the base64 JSON error, then the tagged NO.
    // {"status":"400","schemes":"Bearer","scope":"https://mail.google.com/"}
    let script = &[
        "+ ",
        "<",
        "+ eyJzdGF0dXMiOiI0MDAiLCJzY2hlbWVzIjoiQmVhcmVyIiwic2NvcGUiOiJodHRwczovL21haWwuZ29vZ2xlLmNvbS8ifQ==",
        "<",
        "$T NO [ALERT] Invalid credentials (Failure)",
    ];
    let e = login_against(LoginSecret::OAuthToken("ya29.stale".into()), script).await;
    assert!(matches!(e, DomainError::AuthenticationFailed(_)), "{e:?}");
    assert!(!e.to_string().contains("ya29"), "never echoes the token");
}

#[test]
fn a_validate_error_never_echoes_the_password() {
    let invalid = login_error(async_imap::error::ValidateError('§').into(), false);
    assert!(!invalid.to_string().contains('§'));
}
