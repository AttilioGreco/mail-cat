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
                    json!({
                        "primaryAccounts": {"urn:ietf:params:jmap:mail": "acc"},
                        "capabilities": {"urn:ietf:params:jmap:core": {"maxObjectsInGet": 2}},
                        // Advertised on a host nobody can reach: the client must stay on `base`.
                        "apiUrl": "https://unreachable.invalid/jmap/api",
                        "downloadUrl": format!("{base}/dl/{{accountId}}/{{blobId}}/{{name}}?accept={{type}}"),
                    })
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

#[tokio::test]
async fn write_side_is_refused_for_now() {
    let base = serve(|_, _| json!({})).await;
    let err = provider(&base)
        .delete_messages(&inbox(), &[RemoteId::new("e1")], false)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not implemented yet"), "{err}");
}
