use std::sync::Arc;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use mc_domain::traits::login_gate::LoginSecret;
use mc_domain::{
    EmailProvider, Existing, Flag, MailboxPath, MailboxType, RemoteId, Reset, SyncCursor,
};

use super::JmapEmailProvider;
use super::client::session_url;
use super::mapper;

#[test]
fn roles_map_to_mailbox_types() {
    let cases = [
        (Some("inbox"), MailboxType::Inbox),
        (Some("sent"), MailboxType::Sent),
        (Some("drafts"), MailboxType::Drafts),
        (Some("trash"), MailboxType::Trash),
        (Some("junk"), MailboxType::Spam),
        (Some("archive"), MailboxType::Archive),
        (Some("all"), MailboxType::All),
        (Some("important"), MailboxType::Important),
        (Some("shared-by-nobody"), MailboxType::Custom),
        (None, MailboxType::Custom),
    ];
    for (role, expected) in cases {
        assert_eq!(mapper::mailbox_type(role), expected, "{role:?}");
    }
}

#[test]
fn keywords_and_flags_round_trip() {
    let flags = mapper::flags(
        &json!({"keywords": {"$seen": true, "$flagged": true, "$work": true, "$draft": false}}),
    );
    assert!(flags.contains(&Flag::Seen));
    assert!(flags.contains(&Flag::Flagged));
    assert!(flags.contains(&Flag::Custom("$work".into())));
    assert!(!flags.contains(&Flag::Draft));
    for flag in &flags {
        let keyword = mapper::keyword_from_flag(flag).unwrap();
        assert_eq!(&mapper::flag_from_keyword(&keyword), flag);
    }
    assert_eq!(mapper::keyword_from_flag(&Flag::Deleted), None);
}

#[test]
fn display_path_follows_the_parent_chain() {
    let paths = mapper::display_paths(&[
        json!({"id": "a", "name": "Work"}),
        json!({"id": "b", "name": "Clients", "parentId": "a"}),
        json!({"id": "c", "name": "Acme", "parentId": "b"}),
    ]);
    assert_eq!(paths["c"], "Work/Clients/Acme");
    assert_eq!(paths["a"], "Work");
}

#[test]
fn bare_domains_get_the_well_known_path() {
    assert_eq!(
        session_url("mail.example.com").unwrap().as_str(),
        "https://mail.example.com/.well-known/jmap"
    );
    assert_eq!(
        session_url("http://127.0.0.1:18080").unwrap().as_str(),
        "http://127.0.0.1:18080/.well-known/jmap"
    );
    assert_eq!(
        session_url("https://jmap.example.com/session")
            .unwrap()
            .path(),
        "/session"
    );
}

#[test]
fn envelope_restores_angle_brackets_and_falls_back_to_received_date() {
    let envelope = mapper::envelope(&json!({
        "id": "e1", "messageId": ["a@x"], "inReplyTo": ["b@x"], "references": ["b@x", "c@x"],
        "from": [{"name": "Al", "email": "al@x.org"}], "subject": "Hi",
        "receivedAt": "2026-01-02T03:04:05Z", "size": 10, "hasAttachment": true,
        "keywords": {"$seen": true},
    }))
    .unwrap();
    assert_eq!(envelope.message_rfc_id.as_deref(), Some("<a@x>"));
    assert_eq!(envelope.in_reply_to.as_deref(), Some("<b@x>"));
    assert_eq!(envelope.references, ["<b@x>", "<c@x>"]);
    assert_eq!(envelope.envelope.date, envelope.internal_date);
    assert!(envelope.has_attachments);
    assert_eq!(envelope.key.unwrap().as_str(), "e1");
}

#[test]
fn html_only_mail_has_no_text_body() {
    // JMAP lists the HTML part under textBody too when there is no plain one.
    let body = mapper::bodies(&json!({
        "id": "e1",
        "textBody": [{"partId": "1", "type": "text/html"}],
        "htmlBody": [{"partId": "1", "type": "text/html"}],
        "bodyValues": {"1": {"value": "<p>hi</p>", "isTruncated": true}},
    }))
    .unwrap();
    assert_eq!(body.body_text, None);
    assert_eq!(body.body_html.as_deref(), Some("<p>hi</p>"));
    assert!(body.truncated);
}

/// A throwaway JMAP server: `handler` answers each method call, back-references
/// to an earlier call's `ids` are resolved before it sees them.
async fn serve(handler: impl Fn(&str, &Value) -> Value + Send + Sync + 'static) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let handler = Arc::new(handler);
    let session_base = base.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (handler, base) = (handler.clone(), session_base.clone());
            tokio::spawn(async move {
                let (path, body) = read_request(&mut socket).await;
                let reply = if path.starts_with("/.well-known/jmap") {
                    session_json(&base)
                } else if path.starts_with("/up/") {
                    json!({"blobId": "blob1"})
                } else {
                    answer(&*handler, &body)
                };
                let reply = reply.to_string();
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    reply.len()
                );
                socket.write_all(head.as_bytes()).await.unwrap();
                socket.write_all(reply.as_bytes()).await.unwrap();
            });
        }
    });
    base
}

fn session_json(base: &str) -> Value {
    json!({
        "primaryAccounts": {"urn:ietf:params:jmap:mail": "acc"},
        "capabilities": {"urn:ietf:params:jmap:core": {"maxObjectsInGet": 2, "maxObjectsInSet": 2}},
        "state": "ss1",
        // Advertised on a host nobody can reach: the client must stay on `base`.
        "apiUrl": "https://unreachable.invalid/jmap/api",
        "downloadUrl": format!("{base}/dl/{{accountId}}/{{blobId}}/{{name}}?accept={{type}}"),
        "uploadUrl": "https://unreachable.invalid/up/{accountId}/",
        "eventSourceUrl": "https://unreachable.invalid/es/?types={types}&closeafter={closeafter}&ping={ping}",
    })
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> (String, Value) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = socket.read(&mut chunk).await.unwrap();
        buf.extend_from_slice(&chunk[..n]);
        let text = String::from_utf8_lossy(&buf).into_owned();
        let Some((head, body)) = text.split_once("\r\n\r\n") else {
            continue;
        };
        let length = head
            .lines()
            .find_map(|l| {
                l.to_ascii_lowercase()
                    .strip_prefix("content-length:")?
                    .trim()
                    .parse()
                    .ok()
            })
            .unwrap_or(0usize);
        if body.len() >= length {
            let path = head.split_whitespace().nth(1).unwrap().to_owned();
            return (path, serde_json::from_str(body).unwrap_or(Value::Null));
        }
    }
}

fn answer(handler: &(impl Fn(&str, &Value) -> Value + ?Sized), request: &Value) -> Value {
    let mut results: Vec<Value> = Vec::new();
    let mut responses = Vec::new();
    for call in request["methodCalls"].as_array().unwrap() {
        let (name, id) = (call[0].as_str().unwrap(), call[2].clone());
        let mut args = call[1].clone();
        if let Some(reference) = args.get("#ids") {
            let index: usize = reference["resultOf"].as_str().unwrap()[1..]
                .parse()
                .unwrap();
            args["ids"] = results[index]["ids"].clone();
        }
        let result = handler(name, &args);
        responses.push(match result.get("error") {
            Some(kind) => json!(["error", {"type": kind}, id]),
            None => json!([name, result, id]),
        });
        // A server-run follow-up (`onSuccess*`) answers under the same call id.
        if let Some(extra) = result.get("implicit") {
            responses.push(json!(["Email/set", extra, id]));
        }
        results.push(result);
    }
    json!({"methodResponses": responses})
}

fn provider(base: &str) -> JmapEmailProvider {
    JmapEmailProvider::new(
        base,
        "u".into(),
        Arc::new(LoginSecret::Password("pw".into())),
    )
    .unwrap()
}

fn inbox() -> MailboxPath {
    MailboxPath::new(vec!["mb1".into()], '/').unwrap()
}

/// Three messages, newest first, paged two at a time.
fn snapshot_server(base_state: &'static str) -> impl Fn(&str, &Value) -> Value {
    move |name, args| match name {
        "Email/get" if args["ids"] == json!([]) => json!({"state": base_state, "list": []}),
        "Mailbox/get" => json!({"list": [{"id": "mb1", "totalEmails": 3, "unreadEmails": 1}]}),
        "Email/query" => {
            let all = ["e3", "e2", "e1"];
            let from = args["position"].as_u64().unwrap() as usize;
            json!({"ids": all[from..all.len().min(from + 2)]})
        }
        "Email/get" => {
            let list: Vec<Value> = args["ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|id| json!({"id": id, "keywords": if id == "e2" {json!({"$seen": true})} else {json!({})}}))
                .collect();
            json!({"list": list})
        }
        other => panic!("unexpected {other}"),
    }
}

#[tokio::test]
async fn first_sync_pages_newest_first_and_stays_on_the_session_origin() {
    let base = serve(snapshot_server("s1")).await;
    let changes = provider(&base)
        .mailbox_changes(&inbox(), None)
        .await
        .unwrap();

    assert_eq!(changes.cursor, SyncCursor::new("s1"));
    assert_eq!(changes.reset, None);
    let ids: Vec<&str> = changes.added.iter().map(RemoteId::as_str).collect();
    assert_eq!(ids, ["e3", "e2", "e1"]);
    assert_eq!(changes.keys[&RemoteId::new("e2")].as_str(), "e2");
    assert_eq!((changes.counts.total, changes.counts.unread), (3, 1));
    let Existing::Snapshot(entries) = changes.existing else {
        panic!("a first sync lists everything");
    };
    assert_eq!(entries[1], (RemoteId::new("e2"), vec![Flag::Seen]));
}

#[tokio::test]
async fn find_by_message_id_scans_and_ignores_brackets_and_empty_ids() {
    // Paged two at a time, so the scan must walk past the first page.
    let base = serve(|name, args| match name {
        "Email/query" => {
            let all = ["e1", "e2", "e3"];
            let from = args["position"].as_u64().unwrap() as usize;
            json!({"ids": all[from..all.len().min(from + 2)]})
        }
        "Email/get" => {
            let message_id = |id: &str| if id == "e2" { "b@x" } else { "a@x" };
            let list: Vec<Value> = args["ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|id| json!({"id": id, "messageId": [message_id(id.as_str().unwrap())]}))
                .collect();
            json!({"list": list})
        }
        other => panic!("unexpected {other}"),
    })
    .await;
    let p = provider(&base);

    let found = p.find_by_message_id(&inbox(), "<a@x>").await.unwrap();
    let found: Vec<&str> = found.iter().map(RemoteId::as_str).collect();
    assert_eq!(found, ["e1", "e3"]);
    // Empty ids never query (the panic arm above) nor match everything.
    for empty in ["", "<>", " "] {
        assert!(
            p.find_by_message_id(&inbox(), empty)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn invalid_arguments_rescans_like_cannot_calculate_changes() {
    let snapshot = snapshot_server("s9");
    let base = serve(move |name, args| match name {
        "Email/changes" => json!({"error": "invalidArguments"}),
        _ => snapshot(name, args),
    })
    .await;

    let changes = provider(&base)
        .mailbox_changes(&inbox(), Some(&SyncCursor::new("bad")))
        .await
        .unwrap();

    assert_eq!(changes.reset, Some(Reset::Rescan));
}

#[tokio::test]
async fn cannot_calculate_changes_rescans() {
    let snapshot = snapshot_server("s9");
    let base = serve(move |name, args| match name {
        "Email/changes" => json!({"error": "cannotCalculateChanges"}),
        _ => snapshot(name, args),
    })
    .await;

    let changes = provider(&base)
        .mailbox_changes(&inbox(), Some(&SyncCursor::new("old")))
        .await
        .unwrap();

    assert_eq!(changes.reset, Some(Reset::Rescan));
    assert_eq!(changes.cursor, SyncCursor::new("s9"));
    assert_eq!(changes.added.len(), 3);
    assert!(matches!(changes.existing, Existing::Snapshot(_)));
}

#[tokio::test]
async fn delta_sorts_what_arrived_and_drops_what_moved_out() {
    let base = serve(|name, args| match name {
        "Email/changes" => json!({
            "newState": "s2", "hasMoreChanges": false,
            "created": ["n1", "n2", "other"], "updated": ["u1", "gone"], "destroyed": ["d1"],
        }),
        "Mailbox/get" => json!({"list": [{"id": "mb1", "totalEmails": 5, "unreadEmails": 0}]}),
        "Email/get" => {
            let email = |id: &str, mailboxes: Value, at: &str| {
                json!({"id": id, "mailboxIds": mailboxes, "receivedAt": at, "keywords": {"$seen": true}})
            };
            let wanted = args["ids"].as_array().unwrap();
            let all = [
                email("n1", json!({"mb1": true}), "2026-01-01T00:00:00Z"),
                email("n2", json!({"mb1": true}), "2026-01-02T00:00:00Z"),
                email("other", json!({"mb2": true}), "2026-01-03T00:00:00Z"),
                email("u1", json!({"mb1": true}), "2026-01-01T00:00:00Z"),
                email("gone", json!({"mb2": true}), "2026-01-01T00:00:00Z"),
            ];
            json!({"list": all.into_iter().filter(|e| wanted.contains(&e["id"])).collect::<Vec<_>>()})
        }
        other => panic!("unexpected {other}"),
    })
    .await;

    let changes = provider(&base)
        .mailbox_changes(&inbox(), Some(&SyncCursor::new("s1")))
        .await
        .unwrap();

    assert_eq!(changes.cursor, SyncCursor::new("s2"));
    assert_eq!(changes.reset, None);
    let added: Vec<&str> = changes.added.iter().map(RemoteId::as_str).collect();
    assert_eq!(added, ["n2", "n1"]);
    let Existing::Delta { changed, removed } = changes.existing else {
        panic!("a later sync lists changes");
    };
    assert_eq!(changed, [(RemoteId::new("u1"), vec![Flag::Seen])]);
    let removed: Vec<&str> = removed.iter().map(RemoteId::as_str).collect();
    assert_eq!(removed, ["d1", "other", "gone"]);
}

#[tokio::test]
async fn mailboxes_are_opaque_ids_with_a_readable_path() {
    let base = serve(|_, _| {
        json!({"list": [
            {"id": "mb1", "name": "Inbox", "role": "inbox", "totalEmails": 4, "unreadEmails": 2},
            {"id": "mb2", "name": "Acme", "parentId": "mb1", "totalEmails": 0, "unreadEmails": 0},
        ]})
    })
    .await;

    let mailboxes = provider(&base).list_mailboxes().await.unwrap();

    assert_eq!(mailboxes[0].mailbox_type, MailboxType::Inbox);
    assert_eq!(mailboxes[0].path.full_path(), "mb1");
    assert_eq!(mailboxes[1].display_path, "Inbox/Acme");
    assert_eq!(mailboxes[1].display_name, "Acme");
}

type Calls = Arc<std::sync::Mutex<Vec<(String, Value)>>>;

/// A server that records every call and answers `/set` calls with `reply`.
async fn recording(
    reply: impl Fn(&str, &Value) -> Value + Send + Sync + 'static,
) -> (String, Calls) {
    let calls = Calls::default();
    let seen = calls.clone();
    let base = serve(move |name, args| {
        seen.lock().unwrap().push((name.to_owned(), args.clone()));
        reply(name, args)
    })
    .await;
    (base, calls)
}

fn ids(list: &[&str]) -> Vec<RemoteId> {
    list.iter().map(|id| RemoteId::new(*id)).collect()
}

#[tokio::test]
async fn flags_become_keyword_patches_in_chunks_of_the_set_limit() {
    let (base, calls) = recording(|_, _| json!({})).await;
    provider(&base)
        .set_flags_bulk(
            &inbox(),
            &ids(&["a", "b", "c"]),
            &[Flag::Seen, Flag::Custom("$work".into())],
            &[Flag::Flagged, Flag::Deleted],
        )
        .await
        .unwrap();

    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2, "three ids at two per request");
    let patch = json!({"keywords/$seen": true, "keywords/$work": true, "keywords/$flagged": null});
    assert_eq!(calls[0].1["update"], json!({"a": patch, "b": patch}));
    assert_eq!(calls[1].1["update"], json!({"c": patch}));
}

#[tokio::test]
async fn refused_ids_are_named_after_every_chunk_was_tried() {
    let (base, calls) = recording(|_, args| {
        let refused = |id: &str| {
            args["update"]
                .get(id)
                .map(|_| json!({id: {"type": "forbidden"}}))
        };
        json!({"notUpdated": refused("a").or(refused("c")).unwrap()})
    })
    .await;
    let err = provider(&base)
        .set_flags_bulk(&inbox(), &ids(&["a", "b", "c"]), &[Flag::Seen], &[])
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("a: forbidden") && err.contains("c: forbidden"),
        "{err}"
    );
    assert_eq!(calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn move_swaps_the_mailbox_and_keeps_the_ids() {
    let (base, calls) = recording(|_, _| json!({})).await;
    let to = MailboxPath::new(vec!["mb2".into()], '/').unwrap();
    let moved = provider(&base)
        .move_messages(&ids(&["a"]), &inbox(), &to)
        .await
        .unwrap();

    assert_eq!(moved[&RemoteId::new("a")], Some(RemoteId::new("a")));
    assert_eq!(
        calls.lock().unwrap()[0].1["update"],
        json!({"a": {"mailboxIds/mb1": null, "mailboxIds/mb2": true}})
    );
}

#[tokio::test]
async fn delete_destroys_or_files_into_trash() {
    let (base, calls) = recording(|name, _| match name {
        "Mailbox/get" => json!({"list": [
            {"id": "mb1", "role": "inbox"}, {"id": "mbt", "role": "trash"}]}),
        "Email/get" => json!({"list": [{"id": "a", "mailboxIds": {"mb1": true}}]}),
        _ => json!({}),
    })
    .await;
    let p = provider(&base);

    p.delete_messages(&inbox(), &ids(&["a"]), true)
        .await
        .unwrap();
    p.delete_messages(&inbox(), &ids(&["b"]), false)
        .await
        .unwrap();

    let calls = calls.lock().unwrap();
    assert_eq!(calls[1].1["destroy"], json!(["a"]));
    assert_eq!(
        calls.last().unwrap().1["update"],
        json!({"b": {"mailboxIds/mb1": null, "mailboxIds/mbt": true}})
    );
}

#[tokio::test]
async fn delete_only_unfiles_an_email_that_sits_in_other_mailboxes() {
    let (base, calls) = recording(|name, _| match name {
        "Email/get" => json!({"list": [
            {"id": "a", "mailboxIds": {"mb1": true, "mb9": true}},
            {"id": "b", "mailboxIds": {"mb1": true}}]}),
        _ => json!({}),
    })
    .await;
    provider(&base)
        .delete_messages(&inbox(), &ids(&["a", "b"]), true)
        .await
        .unwrap();

    let set = &calls.lock().unwrap()[1].1;
    assert_eq!(set["update"], json!({"a": {"mailboxIds/mb1": null}}));
    assert_eq!(set["destroy"], json!(["b"]));
}

#[tokio::test]
async fn vanished_ids_are_no_error_but_move_reports_them() {
    let (base, _) = recording(|name, args| match name {
        "Email/get" => json!({"list": [], "notFound": ["a"]}),
        _ => json!({"notUpdated": {"a": {"type": "notFound"}}, "update": args["update"]}),
    })
    .await;
    let p = provider(&base);
    p.delete_messages(&inbox(), &ids(&["a"]), true)
        .await
        .unwrap();
    let to = MailboxPath::new(vec!["mb2".into()], '/').unwrap();
    let moved = p.move_messages(&ids(&["a"]), &inbox(), &to).await.unwrap();
    assert_eq!(moved[&RemoteId::new("a")], None);
}

#[tokio::test]
async fn soft_delete_without_a_trash_mailbox_is_an_error() {
    let base = serve(|_, _| json!({"list": [{"id": "mb1", "role": "inbox"}]})).await;
    let err = provider(&base)
        .delete_messages(&inbox(), &ids(&["a"]), false)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no Trash"), "{err}");
}

fn outgoing() -> mc_domain::OutgoingMessage {
    let address = |email: &str| mc_domain::Address {
        name: None,
        email: mc_domain::EmailAddress::new(email).unwrap(),
    };
    mc_domain::OutgoingMessage {
        from: address("bob@x.org"),
        to: vec![address("alice@x.org")],
        cc: vec![],
        bcc: vec![address("carol@x.org")],
        subject: "Hi".into(),
        body_text: Some("hello".into()),
        body_html: None,
        in_reply_to: None,
        references: vec![],
        attachments: vec![],
        message_rfc_id: Some("<m1@x.org>".into()),
        extra_headers: vec![],
    }
}

fn mail_server() -> impl Fn(&str, &Value) -> Value {
    |name, _| match name {
        "Mailbox/get" => {
            json!({"list": [{"id": "mbd", "role": "drafts"}, {"id": "mbs", "role": "sent"}]})
        }
        "Identity/get" => json!({"list": [{"id": "i1", "email": "Bob@x.org"}]}),
        "Email/import" => json!({"created": {"d": {"id": "new1"}}}),
        "Email/set" => json!({}),
        "EmailSubmission/set" => json!({"created": {"s": {"id": "sub1"}}}),
        other => panic!("unexpected {other}"),
    }
}

#[tokio::test]
async fn draft_is_created_with_draft_keyword_and_replaces_the_old_one() {
    let (base, calls) = recording(mail_server()).await;
    let drafts = MailboxPath::new(vec!["mbd".into()], '/').unwrap();
    let id = provider(&base)
        .save_draft(&drafts, &outgoing(), Some(&RemoteId::new("old")))
        .await
        .unwrap();

    assert_eq!(id, RemoteId::new("new1"));
    let calls = calls.lock().unwrap();
    assert_eq!(
        calls[0].1["emails"]["d"],
        json!({"blobId": "blob1", "mailboxIds": {"mbd": true}, "keywords": {"$draft": true, "$seen": true}})
    );
    assert_eq!(calls[1].1["destroy"], json!(["old"]));
}

#[tokio::test]
async fn send_submits_with_an_envelope_and_files_the_copy_in_sent() {
    let (base, calls) = recording(mail_server()).await;
    let sent = MailboxPath::new(vec!["mbs".into()], '/').unwrap();
    let outcome = provider(&base)
        .send_message(&outgoing(), Some(&sent))
        .await
        .unwrap();

    assert_eq!(outcome.sent_copy, mc_domain::SentCopy::ServerFiled);
    let calls = calls.lock().unwrap();
    let submission = &calls
        .iter()
        .find(|(n, _)| n == "EmailSubmission/set")
        .unwrap()
        .1;
    let create = &submission["create"]["s"];
    assert_eq!(create["identityId"], "i1");
    assert_eq!(create["emailId"], "#d");
    assert_eq!(create["envelope"]["mailFrom"]["email"], "bob@x.org");
    assert_eq!(
        create["envelope"]["rcptTo"],
        json!([{"email": "alice@x.org"}, {"email": "carol@x.org"}])
    );
    assert_eq!(
        submission["onSuccessUpdateEmail"]["#s"],
        json!({"keywords/$draft": null, "mailboxIds/mbd": null, "mailboxIds/mbs": true})
    );
}

#[tokio::test]
async fn a_failed_followup_leaves_no_draft_and_no_claim_of_a_sent_copy() {
    let (base, calls) = recording(|name, args| match name {
        "EmailSubmission/set" => json!({
            "created": {"s": {"id": "sub1"}},
            "implicit": {"notUpdated": {"new1": {"type": "invalidProperties"}}},
        }),
        _ => mail_server()(name, args),
    })
    .await;
    let sent = MailboxPath::new(vec!["mbs".into()], '/').unwrap();
    let outcome = provider(&base)
        .send_message(&outgoing(), Some(&sent))
        .await
        .unwrap();

    assert_eq!(outcome.sent_copy, mc_domain::SentCopy::NotStored);
    let calls = calls.lock().unwrap();
    assert_eq!(calls.last().unwrap().1["destroy"], json!(["new1"]));
}

#[tokio::test]
async fn a_refused_submission_destroys_the_imported_draft() {
    let (base, calls) = recording(|name, args| match name {
        "EmailSubmission/set" => {
            json!({"notCreated": {"s": {"type": "forbiddenFrom"}}})
        }
        _ => mail_server()(name, args),
    })
    .await;
    let err = provider(&base)
        .send_message(&outgoing(), None)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("forbiddenFrom"), "{err}");
    assert_eq!(
        calls.lock().unwrap().last().unwrap().1["destroy"],
        json!(["new1"])
    );
}

#[tokio::test]
async fn send_without_a_matching_identity_fails() {
    let base = serve(|name, _| match name {
        "Identity/get" => json!({"list": [{"id": "i1", "email": "other@x.org"}]}),
        _ => json!({"list": []}),
    })
    .await;
    let err = provider(&base)
        .send_message(&outgoing(), None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("bob@x.org"), "{err}");
}

#[test]
fn event_parser_joins_split_chunks_and_crlf() {
    let mut parser = super::push::EventParser::default();
    assert!(parser.feed(b"event: state\r\nda").is_empty());
    let events = parser.feed(b"ta: {\"a\":1}\r\n\r\n: ping\n\nevent: state\ndata: 2\n\n");
    assert_eq!(events[0], ("state".into(), "{\"a\":1}".into()));
    assert_eq!(events[2], ("state".into(), "2".into()));
}

/// Answers the session, then each event-source connection with the next of
/// `streams`: `(status, body)`; the connection closes after the body.
async fn serve_events(streams: Vec<(u16, String)>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let session = session_json(&base).to_string();
    let session = session.replace("https://unreachable.invalid/es/", &format!("{base}/es/"));
    tokio::spawn(async move {
        let mut streams = streams.into_iter();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (path, _) = read_request(&mut socket).await;
            let (status, kind, body) = if path.starts_with("/es/") {
                let (status, body) = streams.next().unwrap_or((200, String::new()));
                (status, "text/event-stream", body)
            } else {
                (200, "application/json", session.clone())
            };
            let head = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: {kind}\r\nConnection: close\r\n\r\n{body}"
            );
            socket.write_all(head.as_bytes()).await.unwrap();
        }
    });
    base
}

fn state_event(email: &str) -> String {
    format!("event: state\ndata: {{\"changed\": {{\"acc\": {{\"Email\": \"{email}\"}}}}}}\n\n")
}

#[tokio::test]
async fn state_events_notify_once_per_change_and_survive_reconnects() {
    use futures::StreamExt;
    let first = format!(
        "{}{}{}",
        state_event("e1"),
        state_event("e1"),
        state_event("e2")
    );
    let base = serve_events(vec![(200, first), (200, state_event("e3"))]).await;
    let p = provider(&base);
    let mut stream = p.watch_mailbox(&inbox());

    // The third notification only comes after the 2 s reconnect.
    for _ in 0..3 {
        let notification = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next())
            .await
            .expect("a notification")
            .unwrap()
            .unwrap();
        assert!(matches!(
            notification,
            mc_domain::ProviderNotification::MailboxCountChanged { .. }
        ));
    }
}

/// Refuses the credential and, like a paused account, never lets the
/// watcher connect again.
#[derive(Default)]
struct PausingGate {
    failures: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl mc_domain::traits::login_gate::LoginGate for PausingGate {
    async fn secret(&self) -> Result<LoginSecret, mc_domain::error::DomainError> {
        Ok(LoginSecret::Password("pw".into()))
    }
    async fn login_failed(
        &self,
        _: &LoginSecret,
        _: &mc_domain::error::DomainError,
        _: bool,
    ) -> bool {
        self.failures
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        false
    }
    fn login_succeeded(&self) {}
    async fn ready(&self) {
        if self.failures.load(std::sync::atomic::Ordering::SeqCst) > 0 {
            std::future::pending::<()>().await;
        }
    }
}

#[tokio::test]
async fn a_rejected_login_pauses_the_watcher() {
    use futures::StreamExt;
    let base = serve_events(vec![(401, String::new())]).await;
    let gate = Arc::new(PausingGate::default());
    let p = JmapEmailProvider::new(&base, "u".into(), gate.clone()).unwrap();
    let mut stream = p.watch_mailbox(&inbox());

    let next = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next()).await;
    assert!(next.is_err(), "nothing is emitted while paused");
    assert_eq!(gate.failures.load(std::sync::atomic::Ordering::SeqCst), 1);
}
