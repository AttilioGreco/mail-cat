use std::sync::Arc;

use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};
use url::Url;

use mc_domain::error::DomainError;
use mc_domain::traits::dav_provider::{CalendarProvider, Subscribed};

use super::client::{DavView, classify, etag};
use super::xml::parse_multistatus;
use super::{Cursor, xml};
use super::{DavProvider, Profile};

mod radicale;

/// Radicale answers with `ns0:` prefixes and quoted etags.
const RADICALE_LISTING: &str = r#"<?xml version='1.0' encoding='utf-8'?>
<multistatus xmlns="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:CS="http://calendarserver.org/ns/" xmlns:ICAL="http://apple.com/ns/ical/">
<response><href>/alice/work/</href><propstat><prop>
<resourcetype><collection /><C:calendar /></resourcetype>
<displayname>Work</displayname>
<ICAL:calendar-color>#ff8800ff</ICAL:calendar-color>
<C:supported-calendar-component-set><C:comp name="VEVENT" /><C:comp name="VTODO" /></C:supported-calendar-component-set>
<CS:getctag>"ctag-1"</CS:getctag>
<sync-token>http://radicale.org/ns/sync/abc</sync-token>
<current-user-privilege-set><privilege><read /></privilege></current-user-privilege-set>
</prop><status>HTTP/1.1 200 OK</status></propstat>
<propstat><prop><owner /></prop><status>HTTP/1.1 404 Not Found</status></propstat></response>
</multistatus>"#;

/// Google wraps everything in `D:` and sends payloads with entity-encoded
/// line breaks.
const GOOGLE_MULTIGET: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
<D:response><D:href>/caldav/v2/me%40example.com/events/abc.ics</D:href>
<D:propstat><D:prop><D:getetag>"63812345678"</D:getetag>
<C:calendar-data>BEGIN:VCALENDAR&#13;
SUMMARY:Lunch &amp; learn&#13;
END:VCALENDAR&#13;
</C:calendar-data></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
<D:response><D:href>/caldav/v2/me%40example.com/events/gone.ics</D:href>
<D:status>HTTP/1.1 404 Not Found</D:status></D:response>
</D:multistatus>"#;

/// Stalwart reports removals in sync-collection as bare 404 responses.
const STALWART_SYNC: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<D:multistatus xmlns:D="DAV:">
<D:response><D:href>/dav/card/alice/default/a.vcf</D:href><D:propstat><D:prop><D:getetag>"5"</D:getetag></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
<D:response><D:href>/dav/card/alice/default/b.vcf</D:href><D:status>HTTP/1.1 404 Not Found</D:status></D:response>
<D:sync-token>urn:stalwart:davsync:7</D:sync-token>
</D:multistatus>"#;

#[test]
fn radicale_collection_properties_flatten() {
    let ms = parse_multistatus(RADICALE_LISTING.as_bytes()).unwrap();
    let r = &ms.responses[0];
    assert_eq!(r.href, "/alice/work/");
    assert!(r.is_collection());
    assert!(r.props["resourcetype"].has("calendar"));
    assert_eq!(r.text("displayname"), Some("Work"));
    assert_eq!(r.text("calendar-color"), Some("#ff8800ff"));
    assert_eq!(r.text("getctag"), Some("\"ctag-1\""));
    assert_eq!(
        r.props["supported-calendar-component-set"].names,
        ["VEVENT", "VTODO"]
    );
    assert!(r.props["current-user-privilege-set"].has("read"));
    assert!(!r.props["current-user-privilege-set"].has("write-content"));
    assert!(!r.props.contains_key("owner"), "a 404 propstat is no value");
}

#[test]
fn google_payload_keeps_entities_and_line_breaks() {
    let ms = parse_multistatus(GOOGLE_MULTIGET.as_bytes()).unwrap();
    assert_eq!(ms.responses.len(), 2);
    let data = ms.responses[0].text("calendar-data").unwrap();
    assert_eq!(
        data,
        "BEGIN:VCALENDAR\r\nSUMMARY:Lunch & learn\r\nEND:VCALENDAR\r\n"
    );
    assert_eq!(ms.responses[1].status, Some(404));
    assert!(ms.responses[1].props.is_empty());
}

#[test]
fn sync_collection_yields_changes_removals_and_token() {
    let ms = parse_multistatus(STALWART_SYNC.as_bytes()).unwrap();
    assert_eq!(ms.sync_token.as_deref(), Some("urn:stalwart:davsync:7"));
    assert_eq!(ms.responses[0].text("getetag"), Some("\"5\""));
    assert_eq!(ms.responses[1].status, Some(404));
}

#[test]
fn principal_and_home_set_hrefs_are_reachable() {
    let body = r#"<d:multistatus xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav"><d:response><d:href>/</d:href><d:propstat><d:prop>
        <d:current-user-principal><d:href>/principals/alice/</d:href></d:current-user-principal>
        <card:addressbook-home-set><d:href>/dav/alice/</d:href></card:addressbook-home-set>
        </d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>"#;
    let r = &parse_multistatus(body.as_bytes()).unwrap().responses[0];
    assert_eq!(
        r.props["current-user-principal"].hrefs,
        ["/principals/alice/"]
    );
    assert_eq!(r.props["addressbook-home-set"].hrefs, ["/dav/alice/"]);
}

#[test]
fn malformed_xml_is_a_provider_error() {
    let err = parse_multistatus(b"<multistatus><response></multistatus>").unwrap_err();
    assert!(matches!(err, DomainError::Provider(_)), "{err}");
}

#[test]
fn etags_are_one_strong_quoted_form() {
    for raw in ["abc", "\"abc\"", "W/\"abc\"", " W/abc "] {
        assert_eq!(etag(raw).as_str(), "\"abc\"", "{raw}");
    }
}

#[test]
fn cursor_round_trips_and_garbage_is_no_cursor() {
    let cursor = Cursor {
        ctag: Some("c".into()),
        token: None,
    };
    assert_eq!(cursor.encode().as_str(), r#"{"ctag":"c"}"#);
    assert_eq!(Cursor::parse(&cursor.encode()), Some(cursor));
    assert_eq!(Cursor::parse(&mc_domain::SyncCursor::new("not json")), None);
}

#[test]
fn request_bodies_escape_what_the_server_sent_back() {
    let sync = xml::sync_collection("a<b&c");
    assert!(sync.contains("a&lt;b&amp;c"));
    let multiget = xml::multiget(super::Kind::Contacts, ["/x/a&b.vcf"].into_iter());
    assert!(multiget.contains("<D:href>/x/a&amp;b.vcf</D:href>"));
}

#[test]
fn status_classes_match_what_stops_a_pass() {
    let with_retry = {
        let mut h = HeaderMap::new();
        h.insert(RETRY_AFTER, HeaderValue::from_static("120"));
        h
    };
    let none = HeaderMap::new();
    let check = |s: StatusCode, h: &HeaderMap, body: &[u8]| classify(s, h, body);

    assert!(matches!(
        check(StatusCode::UNAUTHORIZED, &none, b""),
        Err(DomainError::AuthenticationFailed(_))
    ));
    assert!(matches!(
        check(StatusCode::TOO_MANY_REQUESTS, &none, b""),
        Err(DomainError::Throttled(_))
    ));
    let throttled = check(StatusCode::SERVICE_UNAVAILABLE, &with_retry, b"");
    assert!(matches!(&throttled, Err(DomainError::Throttled(m)) if m.contains("retry after 120")));
    assert!(matches!(
        check(StatusCode::SERVICE_UNAVAILABLE, &none, b""),
        Err(DomainError::ConnectionFailed(_))
    ));
    assert!(matches!(
        check(StatusCode::BAD_GATEWAY, &none, b""),
        Err(DomainError::ConnectionFailed(_))
    ));
    assert!(matches!(
        check(
            StatusCode::FORBIDDEN,
            &none,
            br#"{"reason":"rateLimitExceeded"}"#
        ),
        Err(DomainError::Throttled(_))
    ));
    for ok in [
        StatusCode::FORBIDDEN,
        StatusCode::PRECONDITION_FAILED,
        StatusCode::NOT_FOUND,
        StatusCode::CONFLICT,
    ] {
        assert!(check(ok, &none, b"").is_ok(), "{ok} belongs to the call");
    }
}

/// Records what reaches the account's gate.
#[derive(Default)]
struct Recording(std::sync::Mutex<Vec<&'static str>>);

#[async_trait::async_trait]
impl mc_domain::LoginGate for Recording {
    async fn secret(&self) -> Result<mc_domain::LoginSecret, DomainError> {
        Ok(mc_domain::LoginSecret::OAuthToken("t".into()))
    }
    async fn login_failed(&self, _: &mc_domain::LoginSecret, _: &DomainError, _: bool) -> bool {
        self.0.lock().unwrap().push("login_failed");
        false
    }
    fn login_succeeded(&self) {
        self.0.lock().unwrap().push("login_succeeded");
    }
    async fn smtp_token_refused(&self, _: &mc_domain::LoginSecret) -> bool {
        self.0.lock().unwrap().push("refresh");
        true
    }
    async fn ready(&self) {}
}

#[tokio::test]
async fn dav_failures_never_reach_the_account_state() {
    use mc_domain::LoginGate;
    let gate = Recording::default();
    let view = DavView(&gate);
    let token = mc_domain::LoginSecret::OAuthToken("t".into());

    for e in [
        DomainError::ConnectionFailed("521".into()),
        DomainError::Throttled("slow".into()),
    ] {
        assert!(!view.login_failed(&token, &e, false).await);
    }
    view.login_succeeded();
    assert!(
        gate.0.lock().unwrap().is_empty(),
        "the mail's state is IMAP's"
    );

    let refused = DomainError::AuthenticationFailed("401".into());
    assert!(
        view.login_failed(&token, &refused, false).await,
        "one refresh"
    );
    assert!(!view.login_failed(&token, &refused, true).await);
    assert_eq!(*gate.0.lock().unwrap(), ["refresh"]);
}

/// Answers each request with the next of `statuses` and keeps what it was
/// sent: request line, headers and body as text.
async fn google_api(statuses: Vec<u16>) -> (Url, Arc<std::sync::Mutex<Vec<String>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!(
        "http://{}/calendar/v3/",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let log = seen.clone();
    tokio::spawn(async move {
        for status in statuses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0; 4096];
            // One read holds a request this small.
            let n = socket.read(&mut chunk).await.unwrap();
            request.extend_from_slice(&chunk[..n]);
            log.lock()
                .unwrap()
                .push(String::from_utf8_lossy(&request).into_owned());
            let reply =
                format!("HTTP/1.1 {status} X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            socket.write_all(reply.as_bytes()).await.unwrap();
        }
    });
    (url, seen)
}

fn google(api: Url) -> DavProvider {
    let mut provider = DavProvider::with_origin(
        mc_domain::AccountId::new(),
        Url::parse("https://example.com/").unwrap(),
        "me@example.com",
        "me@example.com".into(),
        Arc::new(mc_domain::LoginSecret::OAuthToken("tok".into())),
        None,
        Profile::Google,
    )
    .unwrap();
    provider.api = api;
    provider
}

#[tokio::test]
async fn google_subscribe_inserts_into_the_calendar_list_and_maps_refusals() {
    let (api, seen) = google_api(vec![200, 409, 404, 403]).await;
    let provider = google(api);

    assert_eq!(
        provider.subscribe("bob@example.com").await.unwrap(),
        Subscribed::Local(vec![
            Url::parse("https://apidata.googleusercontent.com/caldav/v2/bob%40example.com/events/")
                .unwrap()
        ])
    );
    let request = seen.lock().unwrap()[0].to_lowercase();
    assert!(
        request.starts_with("post /calendar/v3/users/me/calendarlist "),
        "{request}"
    );
    assert!(request.contains("authorization: bearer tok"), "{request}");
    assert!(
        request.ends_with(r#"{"id":"bob@example.com"}"#),
        "{request}"
    );

    assert_eq!(
        provider.subscribe("bob@example.com").await.unwrap(),
        Subscribed::Local(vec![
            Url::parse("https://apidata.googleusercontent.com/caldav/v2/bob%40example.com/events/")
                .unwrap()
        ]),
        "already in the list"
    );
    let err = provider.subscribe("nobody@example.com").await.unwrap_err();
    assert!(matches!(err, DomainError::NotFound(_)), "{err}");
    let err = provider.subscribe("private@example.com").await.unwrap_err();
    assert!(matches!(err, DomainError::Forbidden(_)), "{err}");
    assert_eq!(
        provider.subscribed.lock().unwrap().len(),
        1,
        "remembered once"
    );
}

#[tokio::test]
async fn google_unsubscribe_deletes_the_decoded_calendar_id() {
    let (api, seen) = google_api(vec![204]).await;
    let provider = google(api);
    let collection = mc_domain::CollectionId::new("/caldav/v2/bob%40example.com/events/");

    provider.subscribed.lock().unwrap().push(
        Url::parse("https://apidata.googleusercontent.com/caldav/v2/bob%40example.com/events/")
            .unwrap(),
    );
    provider.unsubscribe(&collection).await.unwrap();
    assert!(provider.subscribed.lock().unwrap().is_empty());

    let request = seen.lock().unwrap()[0].clone();
    assert!(
        request.starts_with("DELETE /calendar/v3/users/me/calendarList/bob@example.com "),
        "{request}"
    );
    let err = provider
        .unsubscribe(&mc_domain::CollectionId::new("/elsewhere/"))
        .await
        .unwrap_err();
    assert!(matches!(err, DomainError::Validation(_)), "{err}");
}

#[test]
fn a_google_calendar_id_is_the_decoded_segment_after_caldav_v2() {
    use super::collections::google_calendar_id;
    assert_eq!(
        google_calendar_id("/caldav/v2/a%2Bb%40x.com/events/").as_deref(),
        Some("a+b@x.com")
    );
    assert_eq!(google_calendar_id("/caldav/v2/id").as_deref(), Some("id"));
    assert_eq!(google_calendar_id("/caldav/v2//events/"), None);
}
