//! The whole path against a real Radicale in Docker: service, SQLite store and
//! the CardDAV/CalDAV adapter. The slice B harness is private to
//! mc-infrastructure's tests, and mc-infrastructure cannot depend on this
//! crate, so a minimal copy of its container start-up lives here. Run with
//! `cargo test -p mc-application radicale -- --ignored`.

use std::process::Command;
use std::time::Duration;

use mc_domain::formats::EventWhen;
use mc_domain::model::account::DavConfig;
use mc_domain::{CalendarProvider, ContactsProvider, LoginSecret, PutOutcome, ResourcePut};
use mc_infrastructure::{DavProfile, DavProvider};
use url::Url;

use super::*;
use crate::commands::{CommandResult, ContactFields};

const IMAGE: &str = "ghcr.io/kozea/radicale:3.5.4";

const CONFIG: &str = "[server]\nhosts = 0.0.0.0:5232\n[auth]\ntype = none\n\
                      [rights]\ntype = owner_only\n";

struct Radicale {
    id: String,
    base: Url,
    _config: tempfile::TempDir,
}

impl Radicale {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("config"), CONFIG).unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            let open = |p: &std::path::Path, mode| {
                std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode)).unwrap()
            };
            open(dir.path(), 0o755);
            open(&dir.path().join("config"), 0o644);
        }
        let out = Command::new("docker")
            .args(["run", "-d", "--rm", "-p", "127.0.0.1::5232", "-v"])
            .arg(format!("{}:/config:ro", dir.path().display()))
            .arg(IMAGE)
            .args(["--config", "/config/config"])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let id = String::from_utf8(out.stdout).unwrap().trim().to_owned();
        let port = Command::new("docker")
            .args(["port", &id, "5232/tcp"])
            .output()
            .unwrap();
        let port = String::from_utf8(port.stdout).unwrap();
        let port = port.lines().next().unwrap().rsplit(':').next().unwrap();
        let server = Self {
            id,
            base: Url::parse(&format!("http://127.0.0.1:{port}/")).unwrap(),
            _config: dir,
        };
        for _ in 0..100 {
            if reqwest::get(server.base.clone()).await.is_ok() {
                return server;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!("Radicale did not come up");
    }

    async fn make(&self, path: &str, method: &str, body: String) {
        let reply = reqwest::Client::new()
            .request(method.parse().unwrap(), self.base.join(path).unwrap())
            .basic_auth("alice", Some("x"))
            .body(body)
            .send()
            .await
            .unwrap();
        assert!(
            reply.status().is_success(),
            "{method} {path}: {}",
            reply.status()
        );
    }
}

impl Drop for Radicale {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).output();
    }
}

#[tokio::test]
#[ignore = "needs docker"]
async fn contacts_and_events_round_trip_through_the_service() {
    let server = Radicale::start().await;
    server
        .make(
            "alice/friends/",
            "MKCOL",
            r#"<mkcol xmlns="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><set><prop>
            <resourcetype><collection/><C:addressbook/></resourcetype>
            <displayname>Friends</displayname></prop></set></mkcol>"#
                .into(),
        )
        .await;
    server
        .make(
            "alice/work/",
            "MKCALENDAR",
            r#"<C:mkcalendar xmlns="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><set><prop>
            <displayname>Work</displayname>
            <C:supported-calendar-component-set><C:comp name="VEVENT"/>
            </C:supported-calendar-component-set></prop></set></C:mkcalendar>"#
                .into(),
        )
        .await;

    let env = env();
    let home = server.base.join("alice/").unwrap();
    let provider = Arc::new(
        DavProvider::new(
            env.account,
            "alice@127.0.0.1",
            "alice".into(),
            Arc::new(LoginSecret::Password("x".into())),
            Some(&DavConfig {
                carddav: Some(home.clone()),
                caldav: Some(home),
            }),
            DavProfile::Generic,
        )
        .unwrap(),
    );
    env.svc
        .providers
        .register(env.account, provider.clone(), DavStatus::all_enabled());
    let book = CollectionId::new("/alice/friends/");

    // Discovery and the first pull, then a contact created here.
    env.sync().await;
    assert_eq!(
        env.repo
            .list_address_books(&env.account)
            .await
            .unwrap()
            .len(),
        1
    );
    let CommandResult::ContactAdded { contact_id } = env
        .handler
        .handle_add_contact(
            env.account,
            book.clone(),
            ContactFields {
                display_name: Some("Zed".into()),
                ..ContactFields::default()
            },
        )
        .await
        .unwrap()
    else {
        panic!("expected the contact");
    };
    for _ in 0..100 {
        if env.pending_in(&book).await.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let bound = env.contact(contact_id).await.remote.unwrap();
    assert!(bound.etag.is_some(), "the fast path reached the server");

    // Another client writes a contact; the next pass brings it in.
    let theirs = ResourcePut {
        href: Href::new("/alice/friends/theirs.vcf"),
        raw: vcard("theirs", "Theirs", None),
        if_match: None,
    };
    assert!(matches!(
        provider.put_contact(&book, theirs).await.unwrap(),
        PutOutcome::Stored { .. }
    ));
    env.sync().await;
    env.contact_at("/alice/friends/theirs.vcf").await;

    // Both sides edit `theirs`: the server's copy is the newer.
    let id = env.contact_at("/alice/friends/theirs.vcf").await;
    let etag = bound_etag(&env, id).await;
    env.edit_local(id, "Mine").await;
    let newer = ResourcePut {
        href: Href::new("/alice/friends/theirs.vcf"),
        raw: vcard("theirs", "Newer", Some(FAR_AHEAD)),
        if_match: Some(etag),
    };
    provider.put_contact(&book, newer).await.unwrap();
    env.sync().await;
    assert_eq!(env.lost().await, 1);
    assert!(env.contact(id).await.raw.unwrap().contains("FN:Newer"));

    // An event created here and a local delete, both pushed by the pass.
    let (start, end) = (
        EventWhen::At("2026-11-12T09:00:00Z".parse().unwrap()),
        EventWhen::At("2026-11-12T10:00:00Z".parse().unwrap()),
    );
    env.handler
        .handle_add_event(
            env.account,
            CollectionId::new("/alice/work/"),
            "Lunch".into(),
            (start, end),
            None,
        )
        .await
        .unwrap();
    env.handler.handle_remove_contact(contact_id).await.unwrap();
    env.sync().await;
    env.sync().await;

    let work = CollectionId::new("/alice/work/");
    let changes = provider.calendar_changes(&work, None).await.unwrap();
    assert_eq!(changes.changed.len(), 1, "the event is on the server");
    let books = provider.address_book_changes(&book, None).await.unwrap();
    assert_eq!(books.changed.len(), 1, "only `theirs` is left");
}

async fn bound_etag(env: &Env, id: ContactId) -> Etag {
    env.contact(id).await.remote.unwrap().etag.unwrap()
}

impl Env {
    async fn pending_in(&self, book: &CollectionId) -> Vec<PendingPush> {
        self.repo
            .pending_contacts(&self.account, book)
            .await
            .unwrap()
    }
}
