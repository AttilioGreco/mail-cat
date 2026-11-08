//! RFC 6764 service discovery, in the order the spec fixes: well-known URI,
//! DNS SRV, then `current-user-principal` and the home-set under it.

use hickory_resolver::TokioResolver;
use hickory_resolver::proto::rr::RData;
use reqwest::StatusCode;
use url::{Host, Url};

use mc_domain::error::DomainError;

use super::Kind;
use super::client::DavClient;
use super::xml::{self, Response};

const MAX_REDIRECTS: usize = 5;

/// Where an account's collections live.
#[derive(Debug, Clone)]
pub(super) struct Home {
    pub url: Url,
    /// Path of `current-user-principal`, to tell own collections from shared.
    pub principal: Option<String>,
}

fn home_prop(kind: Kind) -> &'static str {
    match kind {
        Kind::Contacts => "addressbook-home-set",
        Kind::Calendar => "calendar-home-set",
    }
}

pub(super) fn first_href(response: &Response, prop: &str) -> Option<String> {
    response.props.get(prop)?.hrefs.first().cloned()
}

/// A manual URL, or one discovery found: the principal, the home-set, or the
/// collection itself (Google's fixed CardDAV URL is an address book).
pub(super) async fn resolve_home(
    client: &DavClient,
    kind: Kind,
    start: &Url,
) -> Result<Home, DomainError> {
    let ask = |url: Url| async move {
        let ms = client
            .propfind(&url, "0", xml::principal_and_home(kind))
            .await?;
        let found = ms.responses.into_iter().next().unwrap_or_default();
        Ok::<_, DomainError>((
            first_href(&found, home_prop(kind)),
            first_href(&found, "current-user-principal"),
        ))
    };
    let (home, principal) = ask(start.clone()).await?;
    let principal = principal.and_then(|p| start.join(&p).ok());
    if let Some(home) = home {
        return Ok(Home {
            url: start.join(&home).map_err(bad_href)?,
            principal: principal.map(|p| p.path().to_owned()),
        });
    }
    let Some(principal) = principal else {
        return Ok(Home {
            url: start.clone(),
            principal: None,
        });
    };
    // The start URL answered no home-set but named a principal: ask there. A
    // principal that cannot be read is not fatal, the start URL still works.
    let home = match ask(principal.clone()).await {
        Ok((Some(home), _)) => principal.join(&home).map_err(bad_href)?,
        Ok(_) => start.clone(),
        Err(e) if stops_discovery(&e) => return Err(e),
        Err(_) => start.clone(),
    };
    Ok(Home {
        url: home,
        principal: Some(principal.path().to_owned()),
    })
}

fn bad_href(e: url::ParseError) -> DomainError {
    DomainError::Provider(format!("DAV server sent an invalid href: {e}"))
}

/// Failures that say something about the account, not about this step.
pub(super) fn stops_discovery(e: &DomainError) -> bool {
    matches!(
        e,
        DomainError::AuthenticationFailed(_) | DomainError::Throttled(_)
    )
}

pub(super) async fn discover(
    client: &DavClient,
    kind: Kind,
    origin: &Url,
) -> Result<Home, DomainError> {
    let service = match kind {
        Kind::Contacts => "carddav",
        Kind::Calendar => "caldav",
    };
    match well_known(client, kind, origin, service).await {
        Ok(Some(url)) => return resolve_home(client, kind, &url).await,
        Ok(None) => {}
        Err(e) if stops_discovery(&e) => return Err(e),
        Err(_) => {}
    }
    if let Some(srv) = srv_origin(origin, service).await {
        match well_known(client, kind, &srv, service).await {
            Ok(Some(url)) => return resolve_home(client, kind, &url).await,
            Err(e) if stops_discovery(&e) => return Err(e),
            _ => {}
        }
        if let Ok(home) = resolve_home(client, kind, &srv).await {
            return Ok(home);
        }
    }
    resolve_home(client, kind, origin).await
}

/// PROPFINDs `/.well-known/{service}` and follows its redirects by hand.
/// The URL that finally answers 207 is where the service lives.
async fn well_known(
    client: &DavClient,
    kind: Kind,
    origin: &Url,
    service: &str,
) -> Result<Option<Url>, DomainError> {
    let mut url = origin
        .join(&format!("/.well-known/{service}"))
        .map_err(bad_href)?;
    for _ in 0..=MAX_REDIRECTS {
        let reply = client
            .request(
                "PROPFIND",
                &url,
                &[("Depth", "0")],
                Some(super::client::xml_body(xml::principal_and_home(kind))),
            )
            .await?;
        if reply.status == StatusCode::MULTI_STATUS {
            return Ok(Some(url));
        }
        if !reply.is_redirect() {
            return Ok(None);
        }
        let next = url
            .join(reply.location().unwrap_or_default())
            .map_err(bad_href)?;
        // Credentials follow the redirect only to the same host or over TLS.
        if next.host() != url.host() && next.scheme() != "https" {
            return Ok(None);
        }
        url = next;
    }
    Ok(None)
}

/// `_carddavs._tcp.{domain}`: lowest priority, then highest weight.
async fn srv_origin(origin: &Url, service: &str) -> Option<Url> {
    let Some(Host::Domain(domain)) = origin.host() else {
        return None;
    };
    let resolver = TokioResolver::builder_tokio().ok()?.build().ok()?;
    let lookup = resolver
        .srv_lookup(format!("_{service}s._tcp.{domain}."))
        .await
        .ok()?;
    let srv = lookup
        .answers()
        .iter()
        .filter_map(|r| match &r.data {
            RData::SRV(srv) => Some(srv),
            _ => None,
        })
        .min_by_key(|s| (s.priority, std::cmp::Reverse(s.weight)))?;
    let host = srv.target.to_utf8();
    let host = host.trim_end_matches('.');
    Url::parse(&format!("https://{host}:{}/", srv.port)).ok()
}
