use std::sync::{Arc, Mutex};

use super::super::tests::{FakeSession, serving, uid_commands};
use super::*;

fn entry(name: &str, kind: MailboxType) -> Entry<'_> {
    Entry {
        name,
        delimiter: '/',
        kind,
    }
}

fn gmail_layout() -> Layout {
    Layout::from_list([
        entry("INBOX", MailboxType::Inbox),
        entry("[Gmail]", MailboxType::Custom),
        entry("[Gmail]/All Mail", MailboxType::All),
        entry("[Gmail]/Sent Mail", MailboxType::Sent),
        entry("[Gmail]/Drafts", MailboxType::Drafts),
        entry("[Gmail]/Important", MailboxType::Important),
        entry("[Gmail]/Spam", MailboxType::Spam),
        entry("[Gmail]/Trash", MailboxType::Trash),
        entry("[Gmail]/Starred", MailboxType::Starred),
        entry("Work", MailboxType::Custom),
        entry("Clienti/Caf&AOk-", MailboxType::Custom),
        entry("Say \"hi\"", MailboxType::Custom),
    ])
    .expect("has All Mail")
}

fn path(name: &str) -> MailboxPath {
    parse_mailbox_path(name, '/').unwrap()
}

fn labels(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| n.to_string()).collect()
}

#[test]
fn only_all_spam_and_trash_are_real() {
    let layout = gmail_layout();
    for real in ["[Gmail]/All Mail", "[Gmail]/Spam", "[Gmail]/Trash"] {
        assert!(!layout.is_view(real), "{real}");
        assert_eq!(layout.backing(real), real);
    }
    for view in ["INBOX", "[Gmail]/Sent Mail", "Work", "Clienti/Caf&AOk-"] {
        assert!(layout.is_view(view), "{view}");
        assert_eq!(layout.backing(view), "[Gmail]/All Mail");
        assert_eq!(
            layout.backed_by(&path(view)),
            Some(path("[Gmail]/All Mail"))
        );
    }
    assert_eq!(layout.backed_by(&path("[Gmail]/Trash")), None);
    assert!(layout.holds_all_mail_ids("INBOX") && layout.holds_all_mail_ids("[Gmail]/All Mail"));
    assert!(!layout.holds_all_mail_ids("[Gmail]/Trash"));
}

#[test]
fn a_gmail_without_all_mail_is_plain_imap() {
    assert!(Layout::from_list([entry("INBOX", MailboxType::Inbox)]).is_none());
}

#[test]
fn labels_map_to_views_by_role_name_encoding_and_hierarchy() {
    let layout = gmail_layout();
    let got = layout.paths_of(&labels(&[
        "\\Inbox",
        "\\Sent",
        "\\Draft",
        "\\Important",
        "\\Starred",
        "Work",
        "work",
        "Clienti/Caf&AOk-",
        "Clienti/Café",
        "Say \"hi\"",
        "Unknown",
    ]));
    let names: Vec<String> = got.iter().map(mailbox_path_to_string).collect();
    assert_eq!(
        names,
        [
            "INBOX",
            "[Gmail]/Sent Mail",
            "[Gmail]/Drafts",
            "[Gmail]/Important",
            "[Gmail]/Starred",
            "Work",
            "Work",
            "Clienti/Caf&AOk-",
            "Clienti/Caf&AOk-",
            "Say \"hi\"",
        ]
    );
}

#[test]
fn store_names_use_system_atoms_and_quote_the_rest() {
    let layout = gmail_layout();
    let stored = layout
        .store_labels(&[
            path("INBOX"),
            path("Work"),
            path("Say \"hi\""),
            path("Clienti/Caf&AOk-"),
        ])
        .unwrap();
    assert_eq!(
        stored,
        [
            "\\Inbox",
            "\"Work\"",
            "\"Say \\\"hi\\\"\"",
            "\"Clienti/Caf&AOk-\""
        ]
    );
    for not_a_label in ["[Gmail]/All Mail", "[Gmail]/Trash", "Nope"] {
        assert!(matches!(
            layout.store_labels(&[path(not_a_label)]),
            Err(DomainError::NotSupported(_))
        ));
    }
}

#[test]
fn the_fingerprint_is_stable_across_releases() {
    assert_eq!(gmail_layout().fingerprint, 4_226_677_756_450_693_453);
}

#[test]
fn labels_the_last_list_did_not_show_are_reported() {
    let rows = [row(1, vec![], &["\\Inbox", "Work", "Old Name", "New"], 1)];
    let unlisted: Vec<_> = gmail_layout().unlisted(&rows).into_iter().collect();
    assert_eq!(unlisted, ["New", "Old Name"]);
}

#[test]
fn the_fingerprint_follows_the_label_set() {
    let layout = gmail_layout();
    assert_eq!(layout.fingerprint, gmail_layout().fingerprint);
    let renamed = Layout::from_list([
        entry("[Gmail]/All Mail", MailboxType::All),
        entry("Work2", MailboxType::Custom),
    ])
    .unwrap();
    assert_ne!(layout.fingerprint, renamed.fingerprint);
}

// ── cursor and plan ──────────────────────────────────────────────────────

fn at(uid_validity: u32, uid_next: u32, modseq: u64, views: u64) -> Position {
    Position {
        uid_validity,
        uid_next,
        modseq,
        views,
    }
}

#[test]
fn cursor_round_trips_and_rejects_other_formats() {
    let position = at(7, 42, 900, 5);
    assert_eq!(position.cursor().as_str(), "gm1:7:42:900:5");
    assert_eq!(Position::parse(&position.cursor()), Some(position));
    for foreign in [
        "",
        "imap1:7:42",
        "gm1:7:42:900",
        "gm1:7:42:900:5:1",
        "gm1:x:1:1:1",
    ] {
        assert_eq!(
            Position::parse(&SyncCursor::new(foreign)),
            None,
            "{foreign}"
        );
    }
}

#[test]
fn the_plan_reads_everything_unless_the_cursor_is_current() {
    let cursor = at(7, 42, 900, 5).cursor();
    let delta = Plan {
        reset: None,
        last_next: 42,
        changed_since: Some(900),
    };
    assert_eq!(plan(Some(&cursor), 7, 950, 5), delta);
    let full = |reset, last_next| Plan {
        reset,
        last_next,
        changed_since: None,
    };
    assert_eq!(plan(None, 7, 950, 5), full(None, 1));
    assert_eq!(
        plan(Some(&cursor), 8, 950, 5),
        full(Some(Reset::IdsInvalidated), 1),
        "UIDVALIDITY changed"
    );
    assert_eq!(
        plan(Some(&SyncCursor::new("imap1:7:42")), 7, 950, 5),
        full(Some(Reset::IdsInvalidated), 1),
        "a cursor of another shape"
    );
    assert_eq!(
        plan(Some(&cursor), 7, 950, 6),
        full(None, 42),
        "labels changed"
    );
    assert_eq!(plan(Some(&cursor), 7, 0, 5), full(None, 42), "no CONDSTORE");
    assert_eq!(
        plan(Some(&at(7, 42, 0, 5).cursor()), 7, 950, 5),
        full(None, 42)
    );
}

// ── changes ──────────────────────────────────────────────────────────────

const COUNTS: MailboxCounts = MailboxCounts {
    total: 0,
    unread: 0,
};

fn row(uid: u32, flags: Vec<Flag>, label_names: &[&str], key: u64) -> Row {
    Row {
        uid,
        flags,
        labels: labels(label_names),
        key: Some(key),
    }
}

fn ids(ids: &[u32]) -> Vec<RemoteId> {
    ids.iter().copied().map(RemoteId::from).collect()
}

fn listed(changes: MailboxChanges) -> (Vec<RemoteId>, Vec<(RemoteId, Vec<Flag>)>) {
    match changes.existing {
        Existing::Listed { present, changed } => (present, changed),
        other => panic!("expected a listing, got {other:?}"),
    }
}

#[test]
fn a_first_sync_adds_everything_newest_first_with_keys_and_labels() {
    let layout = gmail_layout();
    let rows = vec![
        row(1, vec![Flag::Seen], &["\\Inbox", "Work"], 11),
        row(3, vec![], &["\\Sent"], 33),
        row(2, vec![], &[], 22),
    ];
    let first = plan(None, 7, 500, layout.fingerprint);
    let changes = changes_from(&layout, &first, 7, 500, COUNTS, vec![1, 2, 3], rows);
    assert_eq!(changes.added, ids(&[3, 2, 1]));
    assert_eq!(changes.keys[&RemoteId::from(3)], MessageKey::new("33"));
    let names = |uid: u32| -> Vec<String> {
        changes.labels[&RemoteId::from(uid)]
            .iter()
            .map(mailbox_path_to_string)
            .collect()
    };
    assert_eq!(names(1), ["INBOX", "Work"]);
    assert_eq!(names(3), ["[Gmail]/Sent Mail"]);
    assert!(
        names(2).is_empty(),
        "a message with no label is still reported"
    );
    assert_eq!(
        Position::parse(&changes.cursor),
        Some(at(7, 4, 500, layout.fingerprint))
    );
    let (present, changed) = listed(changes);
    assert_eq!(present, ids(&[3, 2, 1]));
    assert_eq!(changed.len(), 3);
}

#[test]
fn a_delta_reports_new_and_relabelled_messages_and_what_is_still_there() {
    let layout = gmail_layout();
    let cursor = at(7, 4, 500, layout.fingerprint).cursor();
    let delta = plan(Some(&cursor), 7, 520, layout.fingerprint);
    assert_eq!(delta.changed_since, Some(500));
    // 1 lost Inbox (archived), 4 is new, 2 was expunged, 3 is unchanged.
    let rows = vec![
        row(1, vec![Flag::Seen], &["Work"], 11),
        row(4, vec![], &["\\Inbox"], 44),
    ];
    let changes = changes_from(&layout, &delta, 7, 520, COUNTS, vec![1, 3, 4], rows);
    assert_eq!(changes.reset, None);
    assert_eq!(changes.added, ids(&[4]));
    assert_eq!(
        changes.keys.keys().collect::<Vec<_>>(),
        [&RemoteId::from(4)],
        "keys only for the new"
    );
    assert_eq!(changes.labels.len(), 2, "the unchanged 3 is not mentioned");
    assert_eq!(
        mailbox_path_to_string(&changes.labels[&RemoteId::from(1)][0]),
        "Work"
    );
    assert_eq!(Position::parse(&changes.cursor).unwrap().uid_next, 5);
    let (present, changed) = listed(changes);
    assert_eq!(present, ids(&[4, 3, 1]), "2 is absent: gone");
    assert_eq!(
        changed.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
        ids(&[1, 4])
    );
}

#[test]
fn a_message_flagged_deleted_is_gone() {
    let layout = gmail_layout();
    let first = plan(None, 7, 500, layout.fingerprint);
    let rows = vec![row(2, vec![Flag::Deleted], &["\\Inbox"], 22)];
    let changes = changes_from(&layout, &first, 7, 500, COUNTS, vec![1, 2], rows);
    assert_eq!(changes.added, ids(&[1]));
    assert!(!changes.labels.contains_key(&RemoteId::from(2)));
    assert_eq!(listed(changes).0, ids(&[1]));
}

// ── against a server that speaks X-GM-LABELS ─────────────────────────────

struct Msg {
    uid: u32,
    labels: Vec<String>,
    modseq: u64,
}

struct Account {
    messages: Vec<Msg>,
    modseq: u64,
}

fn label_list(labels: &[String]) -> String {
    labels
        .iter()
        .map(|l| {
            if l.starts_with('\\') {
                l.to_string()
            } else {
                format!("\"{}\"", l.replace('\\', "\\\\").replace('"', "\\\""))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn uids_in(set: &str, all: &[u32]) -> Vec<u32> {
    let mut out = Vec::new();
    for part in set.split(',') {
        let (from, to) = part.split_once(':').unwrap_or((part, part));
        let from: u32 = from.parse().unwrap();
        let to: u32 = to.parse().unwrap_or(u32::MAX);
        out.extend(all.iter().copied().filter(|u| (from..=to).contains(u)));
    }
    out
}

/// A Gmail's All Mail: CONDSTORE FETCH, `STORE ±X-GM-LABELS`, nothing else.
async fn gmail(
    messages: Vec<(u32, Vec<&'static str>)>,
) -> (FakeSession, Arc<Mutex<Vec<String>>>, Arc<Mutex<Account>>) {
    let account = Arc::new(Mutex::new(Account {
        modseq: 100,
        messages: messages
            .into_iter()
            .map(|(uid, labels)| Msg {
                uid,
                labels: labels.into_iter().map(String::from).collect(),
                modseq: 100,
            })
            .collect(),
    }));
    let state = Arc::clone(&account);
    let (session, commands) = serving(move |command| {
        let mut account = state.lock().unwrap();
        let mut parts = command.splitn(4, ' ');
        let (_, verb, set, rest) = (
            parts.next(),
            parts.next().unwrap_or(""),
            parts.next().unwrap_or(""),
            parts.next().unwrap_or(""),
        );
        let all: Vec<u32> = account.messages.iter().map(|m| m.uid).collect();
        match verb {
            "FETCH" => {
                let since: u64 = rest
                    .split("CHANGEDSINCE ")
                    .nth(1)
                    .and_then(|s| s.trim_end_matches(')').parse().ok())
                    .unwrap_or(0);
                let wanted = uids_in(set, &all);
                let mut out = String::new();
                for (seq, m) in account.messages.iter().enumerate() {
                    if wanted.contains(&m.uid) && m.modseq > since {
                        out += &format!(
                            "* {} FETCH (UID {} FLAGS (\\Seen) X-GM-LABELS ({}) X-GM-MSGID {} MODSEQ ({}))\r\n",
                            seq + 1,
                            m.uid,
                            label_list(&m.labels),
                            1000 + m.uid,
                            m.modseq
                        );
                    }
                }
                out + "$T OK Fetch done\r\n"
            }
            "STORE" => {
                account.modseq += 1;
                let modseq = account.modseq;
                let remove = rest.starts_with('-');
                let named: Vec<String> = rest
                    .split_once('(')
                    .map(|(_, l)| l.trim_end_matches(')'))
                    .unwrap_or("")
                    .split(' ')
                    .map(|l| l.trim_matches('"').to_owned())
                    .collect();
                for m in account.messages.iter_mut().filter(|m| uids_in(set, &all).contains(&m.uid)) {
                    m.labels.retain(|l| !(remove && named.contains(l)));
                    if !remove {
                        for n in &named {
                            if !m.labels.contains(n) {
                                m.labels.push(n.clone());
                            }
                        }
                    }
                    m.modseq = modseq;
                }
                "$T OK Stored\r\n".to_owned()
            }
            _ => "$T BAD unsupported\r\n".to_owned(),
        }
    })
    .await;
    (session, commands, account)
}

#[tokio::test]
async fn fetch_parses_system_atoms_quoted_escaped_and_empty_labels() {
    let (mut session, commands, _) = gmail(vec![
        (
            1,
            vec!["\\Inbox", "\\Important", "Work", "Say \"hi\"", "Caf&AOk-"],
        ),
        (2, vec![]),
    ])
    .await;
    let rows = labelled_rows(&mut session, &[1, 2], None).await.unwrap();
    assert_eq!(
        uid_commands(&commands),
        ["UID FETCH 1:2 (FLAGS X-GM-LABELS X-GM-MSGID)"]
    );
    assert_eq!(
        rows[0].labels,
        ["\\Inbox", "\\Important", "Work", "Say \"hi\"", "Caf&AOk-"]
    );
    assert_eq!(rows[0].key, Some(1001));
    assert_eq!(rows[0].flags, [Flag::Seen]);
    assert!(rows[1].labels.is_empty());
}

#[tokio::test]
async fn an_unsolicited_flags_update_is_not_a_message_without_labels() {
    let (mut session, _) = serving(|_| {
        "* 1 FETCH (UID 1 FLAGS (\\Seen))\r\n\
         * 2 FETCH (UID 2 FLAGS () X-GM-LABELS () X-GM-MSGID 5)\r\n\
         $T OK done\r\n"
            .to_owned()
    })
    .await;
    let rows = labelled_rows(&mut session, &[1, 2], None).await.unwrap();
    assert_eq!(rows.iter().map(|r| r.uid).collect::<Vec<_>>(), [2]);
    assert!(rows[0].labels.is_empty());
}

/// imap-proto has no NIL for a label list and, being a streaming parser,
/// waits for more input: the command's timeout is what ends it. Gmail sends
/// `()` for no labels, so this is not expected; what matters is that no
/// message comes out label-less by accident.
#[tokio::test]
async fn a_nil_label_list_never_yields_a_label_less_row() {
    let (mut session, _) = serving(|_| {
        "* 2 FETCH (UID 2 FLAGS () X-GM-LABELS NIL X-GM-MSGID 5)\r\n$T OK done\r\n".to_owned()
    })
    .await;
    let read = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        labelled_rows(&mut session, &[2], None),
    )
    .await;
    match read {
        Err(_elapsed) => {}
        Ok(Ok(rows)) => assert!(rows.is_empty(), "{rows:?}"),
        Ok(Err(DomainError::Serialization(_))) => {}
        Ok(Err(e)) => panic!("unexpected {e}"),
    }
}

#[tokio::test]
async fn a_delta_fetch_asks_changedsince_and_sees_only_the_changed() {
    let (mut session, commands, account) =
        gmail(vec![(1, vec!["\\Inbox", "Work"]), (2, vec!["\\Inbox"])]).await;
    let first = labelled_rows(&mut session, &[1, 2], None).await.unwrap();
    assert_eq!(first.len(), 2);
    let seen_up_to = account.lock().unwrap().modseq;

    // Archive message 1 and label message 2, as the handlers would.
    let layout = gmail_layout();
    ImapEmailProvider::move_by_labels(&mut session, &layout, &[1], "INBOX", "[Gmail]/All Mail")
        .await
        .unwrap();
    store_labels(&mut session, &[2], '+', &["\"Work\"".to_owned()])
        .await
        .unwrap();

    let rows = labelled_rows(&mut session, &[1, 2], Some(seen_up_to))
        .await
        .unwrap();
    assert_eq!(
        uid_commands(&commands),
        [
            "UID FETCH 1:2 (FLAGS X-GM-LABELS X-GM-MSGID)",
            "UID STORE 1 -X-GM-LABELS (\\Inbox)",
            "UID STORE 2 +X-GM-LABELS (\"Work\")",
            &format!("UID FETCH 1:* (FLAGS X-GM-LABELS X-GM-MSGID) (CHANGEDSINCE {seen_up_to})"),
        ]
    );
    let by_uid = |uid| rows.iter().find(|r| r.uid == uid).unwrap().labels.clone();
    assert_eq!(by_uid(1), ["Work"]);
    assert_eq!(by_uid(2), ["\\Inbox", "Work"]);

    let now = account.lock().unwrap().modseq;
    let nothing = labelled_rows(&mut session, &[1, 2], Some(now))
        .await
        .unwrap();
    assert!(nothing.is_empty());
}

#[tokio::test]
async fn relabelling_commands_between_views() {
    let (mut session, commands, _) = gmail(vec![(5, vec!["\\Inbox"])]).await;
    let layout = gmail_layout();
    for (from, to) in [
        ("INBOX", "Work"),
        ("Work", "[Gmail]/All Mail"),
        ("[Gmail]/All Mail", "Work"),
        ("Work", "Work"),
    ] {
        ImapEmailProvider::move_by_labels(&mut session, &layout, &[5, 6, 7, 9], from, to)
            .await
            .unwrap();
    }
    assert_eq!(
        uid_commands(&commands),
        [
            "UID STORE 5:7,9 +X-GM-LABELS (\"Work\")",
            "UID STORE 5:7,9 -X-GM-LABELS (\\Inbox)",
            "UID STORE 5:7,9 -X-GM-LABELS (\"Work\")",
            "UID STORE 5:7,9 +X-GM-LABELS (\"Work\")",
            "UID STORE 5:7,9 +X-GM-LABELS (\"Work\")",
        ]
    );
}

#[tokio::test]
async fn deleting_for_good_from_all_mail_goes_through_trash() {
    let (mut session, commands) = serving(|c| {
        match c {
            _ if c.starts_with("UID MOVE") => "* OK [COPYUID 7 5:6 77:78] Moved\r\n$T OK Done\r\n",
            _ if c.starts_with("SELECT") => "* 2 EXISTS\r\n$T OK [READ-WRITE] Selected\r\n",
            _ if c.contains("EXPUNGE") => "* 1 EXPUNGE\r\n$T OK Done\r\n",
            _ => "$T OK Done\r\n",
        }
        .to_owned()
    })
    .await;
    let caps = ServerCaps {
        move_cmd: true,
        uidplus: true,
        gmail: true,
    };
    ImapEmailProvider::expunge_via_trash(&mut session, &gmail_layout(), caps, &[5, 6])
        .await
        .unwrap();
    assert_eq!(
        uid_commands(&commands),
        [
            "UID MOVE 5:6 \"[Gmail]/Trash\"",
            "SELECT \"[Gmail]/Trash\"",
            "UID STORE 77:78 +FLAGS.SILENT (\\Deleted)",
            "UID EXPUNGE 77:78",
        ]
    );
}

/// LIST sends names as quoted strings, escapes and modified UTF-7 included.
#[tokio::test]
async fn listed_names_with_escapes_and_utf7_match_their_labels() {
    let (mut session, _) = serving(|_| {
        "* LIST (\\HasNoChildren) \"/\" INBOX\r\n\
         * LIST (\\HasNoChildren \\All) \"/\" \"[Gmail]/All Mail\"\r\n\
         * LIST (\\HasNoChildren) \"/\" \"mc-itest/Clienti/Caf&AOk-\"\r\n\
         * LIST (\\HasNoChildren) \"/\" \"mc-itest/Say \\\"hi\\\"\"\r\n\
         * LIST (\\HasNoChildren) \"/\" \"mc-itest/back\\\\slash\"\r\n\
         $T OK done\r\n"
            .to_owned()
    })
    .await;
    let listed = list_on(&mut session, true).await.unwrap();
    let layout = Layout::from_list(listed.iter().map(|l| Entry {
        name: &l.name,
        delimiter: '/',
        kind: l.mailbox.mailbox_type,
    }))
    .unwrap();
    let wire = labels(&[
        "mc-itest/Clienti/Caf&AOk-",
        "mc-itest/Say \"hi\"",
        "mc-itest/back\\slash",
    ]);
    assert_eq!(layout.paths_of(&wire).len(), 3);
    let say = listed.iter().find(|l| l.name.contains("Say")).unwrap();
    assert_eq!(say.mailbox.display_path, "mc-itest/Say \"hi\"");
    assert_eq!(
        layout.store_label(&say.name).unwrap(),
        "\"mc-itest/Say \\\"hi\\\"\""
    );
}
