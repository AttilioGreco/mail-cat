//! Against a real Radicale in Docker. Ignored by default like the repo's
//! other container harnesses: run with `cargo test -p mc-infrastructure dav::
//! -- --ignored`. Each test gets its own throwaway container.

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use url::Url;

use mc_domain::error::DomainError;
use mc_domain::model::account::{AccountId, DavConfig};
use mc_domain::model::collection::{CalendarComponent, Ownership};
use mc_domain::traits::dav_provider::{
    CalendarProvider, ContactsProvider, DeleteOutcome, PutOutcome, ResourcePut, Subscribed,
};
use mc_domain::traits::login_gate::LoginSecret;
use mc_domain::value_objects::remote::{CollectionId, Href};
use mc_domain::value_objects::sync_cursor::SyncCursor;

use crate::dav::{DavProvider, Profile};

const IMAGE: &str = "ghcr.io/kozea/radicale:3.5.4";

const CONFIG: &str = "[server]\nhosts = 0.0.0.0:5232\n[auth]\ntype = none\n\
                      [rights]\ntype = from_file\nfile = /config/rights\n";

/// Owners have everything on their own tree; alice may only read bob's.
const RIGHTS: &str = "[root]\nuser: .+\ncollection:\npermissions: R\n\n\
                      [own]\nuser: .+\ncollection: {user}(/.*)?\npermissions: RrWw\n\n\
                      [alice-reads-bob]\nuser: alice\ncollection: bob(/.*)?\npermissions: Rr\n";

struct Radicale {
    id: String,
    base: Url,
    _config: tempfile::TempDir,
}

impl Radicale {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("config"), CONFIG).unwrap();
        std::fs::write(dir.path().join("rights"), RIGHTS).unwrap();
        make_readable(dir.path());
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
        let port = String::from_utf8(port.stdout)
            .unwrap()
            .lines()
            .next()
            .unwrap()
            .rsplit(':')
            .next()
            .unwrap()
            .to_owned();
        let base = Url::parse(&format!("http://127.0.0.1:{port}/")).unwrap();
        let server = Self {
            id,
            base,
            _config: dir,
        };
        server.wait_ready().await;
        server
    }

    async fn wait_ready(&self) {
        for _ in 0..100 {
            if reqwest::get(self.base.clone()).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!("Radicale did not come up");
    }

    /// Creates a collection the way a client would: MKCOL / MKCALENDAR.
    async fn make(&self, user: &str, path: &str, method: &str, body: &str) {
        let reply = reqwest::Client::new()
            .request(method.parse().unwrap(), self.base.join(path).unwrap())
            .basic_auth(user, Some("x"))
            .body(body.to_owned())
            .send()
            .await
            .unwrap();
        assert!(
            reply.status().is_success(),
            "{method} {path}: {}",
            reply.status()
        );
    }

    async fn address_book(&self, user: &str, name: &str) {
        let body = r#"<mkcol xmlns="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><set><prop>
            <resourcetype><collection/><C:addressbook/></resourcetype>
            <displayname>Friends</displayname></prop></set></mkcol>"#;
        self.make(user, &format!("{user}/{name}/"), "MKCOL", body)
            .await;
    }

    async fn calendar(&self, user: &str, name: &str, comps: &[&str]) {
        let comps: String = comps
            .iter()
            .map(|c| format!(r#"<C:comp name="{c}"/>"#))
            .collect();
        let body = format!(
            r#"<C:mkcalendar xmlns="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><set><prop>
            <displayname>Work</displayname>
            <C:supported-calendar-component-set>{comps}</C:supported-calendar-component-set>
            </prop></set></C:mkcalendar>"#
        );
        self.make(user, &format!("{user}/{name}/"), "MKCALENDAR", &body)
            .await;
    }

    fn provider(&self, user: &str, dav: Option<&DavConfig>) -> DavProvider {
        DavProvider::with_origin(
            AccountId::new(),
            self.base.clone(),
            &format!("{user}@127.0.0.1"),
            user.to_owned(),
            Arc::new(LoginSecret::Password("x".into())),
            dav,
            Profile::Generic,
        )
        .unwrap()
    }
}

impl Drop for Radicale {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).output();
    }
}

/// The container user is not the test's: let it read the mount.
fn make_readable(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    for f in ["config", "rights"] {
        std::fs::set_permissions(dir.join(f), std::fs::Permissions::from_mode(0o644)).unwrap();
    }
}

fn vcard(uid: &str, name: &str) -> String {
    format!("BEGIN:VCARD\r\nVERSION:3.0\r\nUID:{uid}\r\nFN:{name}\r\nN:{name};;;;\r\nEND:VCARD\r\n")
}

fn event(uid: &str, summary: &str) -> String {
    format!(
        "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//mail-cat//test//EN\r\nBEGIN:VEVENT\r\n\
         UID:{uid}\r\nDTSTAMP:20261107T100000Z\r\nDTSTART:20261110T090000Z\r\n\
         DTEND:20261110T100000Z\r\nSUMMARY:{summary}\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
    )
}

fn create(href: &str, raw: String) -> ResourcePut {
    ResourcePut {
        href: Href::new(href),
        raw,
        if_match: None,
    }
}

fn stored(outcome: PutOutcome) -> (Href, mc_domain::value_objects::remote::Etag) {
    match outcome {
        PutOutcome::Stored { href, etag, .. } => (href, etag),
        PutOutcome::PreconditionFailed => panic!("the put was refused"),
    }
}

#[tokio::test]
#[ignore = "needs docker"]
async fn discovery_finds_the_collections_of_each_kind() {
    let server = Radicale::start().await;
    server.address_book("alice", "friends").await;
    server.calendar("alice", "work", &["VEVENT"]).await;
    let provider = server.provider("alice", None);

    let books = provider.list_address_books().await.unwrap();
    assert_eq!(books.len(), 1, "{books:?}");
    assert_eq!(books[0].name, "Friends");
    assert_eq!(books[0].collection.as_str(), "/alice/friends/");
    assert!(!books[0].read_only);

    let calendars = provider.list_calendars().await.unwrap();
    assert_eq!(calendars.len(), 1, "{calendars:?}");
    assert_eq!(calendars[0].name, "Work");
    assert_eq!(calendars[0].components, [CalendarComponent::Event]);
    assert_eq!(calendars[0].ownership, Ownership::Own);
}

#[tokio::test]
#[ignore = "needs docker"]
async fn a_manual_url_skips_discovery() {
    let server = Radicale::start().await;
    server.address_book("alice", "friends").await;
    let dav = DavConfig {
        carddav: Some(server.base.join("/alice/").unwrap()),
        caldav: None,
        ..DavConfig::default()
    };
    // The origin is unreachable on purpose: only the override may be used.
    let provider = DavProvider::with_origin(
        AccountId::new(),
        Url::parse("http://127.0.0.1:1/").unwrap(),
        "alice@127.0.0.1",
        "alice".into(),
        Arc::new(LoginSecret::Password("x".into())),
        Some(&dav),
        Profile::Generic,
    )
    .unwrap();
    assert_eq!(provider.list_address_books().await.unwrap().len(), 1);
}

#[tokio::test]
#[ignore = "needs docker"]
async fn contacts_sync_initially_then_by_delta() {
    let server = Radicale::start().await;
    server.address_book("alice", "friends").await;
    let provider = server.provider("alice", None);
    let book = CollectionId::new("/alice/friends/");

    let (a, a_etag) = stored(
        provider
            .put_contact(&book, create("/alice/friends/a.vcf", vcard("a", "Ann")))
            .await
            .unwrap(),
    );
    let (b, _) = stored(
        provider
            .put_contact(&book, create("/alice/friends/b.vcf", vcard("b", "Bob")))
            .await
            .unwrap(),
    );

    let first = provider.address_book_changes(&book, None).await.unwrap();
    assert!(first.reset);
    assert_eq!(first.changed.len(), 2);
    assert!(first.removed.is_empty());

    let same = provider
        .address_book_changes(&book, Some(&first.cursor))
        .await
        .unwrap();
    assert!(!same.reset && same.changed.is_empty() && same.removed.is_empty());
    assert_eq!(
        same.cursor, first.cursor,
        "an unchanged collection keeps its cursor"
    );

    let (c, _) = stored(
        provider
            .put_contact(&book, create("/alice/friends/c.vcf", vcard("c", "Cy")))
            .await
            .unwrap(),
    );
    let updated = ResourcePut {
        href: a.clone(),
        raw: vcard("a", "Anna"),
        if_match: Some(a_etag),
    };
    let (_, a_new) = stored(provider.put_contact(&book, updated).await.unwrap());
    assert_eq!(
        provider
            .delete_contact(&book, &b, &stored_etag(&provider, &book, &b).await)
            .await
            .unwrap(),
        DeleteOutcome::Deleted
    );

    let delta = provider
        .address_book_changes(&book, Some(&first.cursor))
        .await
        .unwrap();
    assert!(!delta.reset, "the token served the delta");
    let mut changed: Vec<_> = delta.changed.to_vec();
    changed.sort();
    assert_eq!(
        changed,
        [
            (a.clone(), a_new.clone()),
            (c.clone(), changed[1].1.clone())
        ]
    );
    assert_eq!(delta.removed, [b]);

    let fetched = provider
        .fetch_contacts(&book, &[a, Href::new("/alice/friends/none.vcf")])
        .await
        .unwrap();
    assert_eq!(fetched.len(), 1, "absent resources are simply missing");
    assert_eq!(fetched[0].etag, a_new);
    assert!(fetched[0].raw.contains("FN:Anna"));
}

async fn stored_etag(
    p: &DavProvider,
    book: &CollectionId,
    href: &Href,
) -> mc_domain::value_objects::remote::Etag {
    p.fetch_contacts(book, std::slice::from_ref(href))
        .await
        .unwrap()
        .remove(0)
        .etag
}

#[tokio::test]
#[ignore = "needs docker"]
async fn an_expired_token_falls_back_to_a_full_listing() {
    let server = Radicale::start().await;
    server.address_book("alice", "friends").await;
    let provider = server.provider("alice", None);
    let book = CollectionId::new("/alice/friends/");
    provider
        .put_contact(&book, create("/alice/friends/a.vcf", vcard("a", "Ann")))
        .await
        .unwrap();

    let stale = SyncCursor::new(r#"{"ctag":"old","token":"http://radicale.org/ns/sync/deadbeef"}"#);
    let changes = provider
        .address_book_changes(&book, Some(&stale))
        .await
        .unwrap();
    assert!(changes.reset);
    assert_eq!(changes.changed.len(), 1);
}

#[tokio::test]
#[ignore = "needs docker"]
async fn events_are_created_updated_and_deleted_with_preconditions() {
    let server = Radicale::start().await;
    server.calendar("alice", "work", &["VEVENT"]).await;
    let provider = server.provider("alice", None);
    let cal = CollectionId::new("/alice/work/");
    let href = "/alice/work/e1.ics";

    let (href, etag) = stored(
        provider
            .put_event(&cal, create(href, event("e1", "Standup")))
            .await
            .unwrap(),
    );

    // A create never overwrites.
    let again = provider
        .put_event(&cal, create(href.as_str(), event("e1", "Other")))
        .await
        .unwrap();
    assert_eq!(again, PutOutcome::PreconditionFailed);

    let edit = ResourcePut {
        href: href.clone(),
        raw: event("e1", "Standup v2"),
        if_match: Some(etag.clone()),
    };
    let (_, newer) = stored(provider.put_event(&cal, edit).await.unwrap());
    assert_ne!(newer, etag);

    // The old etag is stale now: update and delete both lose.
    let stale = ResourcePut {
        href: href.clone(),
        raw: event("e1", "Lost"),
        if_match: Some(etag.clone()),
    };
    assert_eq!(
        provider.put_event(&cal, stale).await.unwrap(),
        PutOutcome::PreconditionFailed
    );
    assert_eq!(
        provider.delete_event(&cal, &href, &etag).await.unwrap(),
        DeleteOutcome::PreconditionFailed
    );

    assert_eq!(
        provider.delete_event(&cal, &href, &newer).await.unwrap(),
        DeleteOutcome::Deleted
    );
    assert_eq!(
        provider.delete_event(&cal, &href, &newer).await.unwrap(),
        DeleteOutcome::Gone
    );
    let revive = ResourcePut {
        href,
        raw: event("e1", "Back"),
        if_match: Some(newer),
    };
    assert_eq!(
        provider.put_event(&cal, revive).await.unwrap(),
        PutOutcome::PreconditionFailed
    );

    let delta = provider.calendar_changes(&cal, None).await.unwrap();
    assert!(delta.changed.is_empty());
}

#[tokio::test]
#[ignore = "needs docker"]
async fn a_task_only_calendar_is_listed_as_not_holding_events() {
    let server = Radicale::start().await;
    server.calendar("alice", "tasks", &["VTODO"]).await;
    let calendars = server
        .provider("alice", None)
        .list_calendars()
        .await
        .unwrap();
    assert_eq!(calendars[0].components, [CalendarComponent::Todo]);
    assert!(!calendars[0].holds_events());
}

#[tokio::test]
#[ignore = "needs docker"]
async fn a_collection_shared_read_only_reports_it_and_refuses_writes() {
    let server = Radicale::start().await;
    server.calendar("bob", "team", &["VEVENT"]).await;
    let dav = DavConfig {
        carddav: None,
        caldav: Some(server.base.join("/bob/").unwrap()),
        ..DavConfig::default()
    };
    let provider = server.provider("alice", Some(&dav));

    let calendars = provider.list_calendars().await.unwrap();
    assert_eq!(calendars.len(), 1, "{calendars:?}");
    assert!(calendars[0].read_only);
    assert_eq!(calendars[0].ownership, Ownership::Shared);

    let err = provider
        .put_event(
            &calendars[0].collection,
            create("/bob/team/x.ics", event("x", "No")),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, DomainError::Provider(_)), "{err}");
}

#[tokio::test]
#[ignore = "needs docker"]
async fn subscribing_finds_a_colleagues_calendars_and_a_refused_one_drops_out() {
    let server = Radicale::start().await;
    server.calendar("alice", "work", &["VEVENT"]).await;
    server.calendar("bob", "team", &["VEVENT"]).await;
    server.calendar("carol", "secret", &["VEVENT"]).await;
    let provider = server.provider("alice", None);

    // Radicale cannot search principals: `bob@127.0.0.1` is refused, `bob` found.
    let Subscribed::Local(added) = provider.subscribe("bob@127.0.0.1").await.unwrap() else {
        panic!("a generic server does not remember subscriptions");
    };
    assert_eq!(added, [server.base.join("/bob/team/").unwrap()]);

    let calendars = provider.list_calendars().await.unwrap();
    let team = calendars
        .iter()
        .find(|c| c.collection.as_str() == "/bob/team/")
        .unwrap_or_else(|| panic!("{calendars:?}"));
    assert_eq!(team.ownership, Ownership::Shared);
    assert!(team.read_only);
    assert!(
        calendars
            .iter()
            .any(|c| c.collection.as_str() == "/alice/work/")
    );

    let err = provider.subscribe("carol@127.0.0.1").await.unwrap_err();
    assert!(matches!(err, DomainError::Forbidden(_)), "{err}");

    // A configured href the server now refuses is left out of the listing.
    let dav = DavConfig {
        subscribed: vec![
            server.base.join("/bob/team/").unwrap(),
            server.base.join("/carol/secret/").unwrap(),
        ],
        ..DavConfig::default()
    };
    let listed = server
        .provider("alice", Some(&dav))
        .list_calendars()
        .await
        .unwrap();
    let paths: Vec<_> = listed.iter().map(|c| c.collection.as_str()).collect();
    assert!(paths.contains(&"/bob/team/"), "{paths:?}");
    assert!(!paths.contains(&"/carol/secret/"), "{paths:?}");

    provider
        .unsubscribe(&CollectionId::new("/bob/team/"))
        .await
        .unwrap();
    let calendars = provider.list_calendars().await.unwrap();
    assert!(
        calendars
            .iter()
            .all(|c| c.collection.as_str() != "/bob/team/")
    );
}
