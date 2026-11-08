//! The collection list: names, permissions and ownership from one PROPFIND.

use reqwest::StatusCode;
use serde_json::json;
use url::Url;

use mc_domain::error::DomainError;
use mc_domain::model::collection::{AddressBook, Calendar, CalendarComponent, Ownership};
use mc_domain::traits::dav_provider::Subscribed;
use mc_domain::value_objects::remote::CollectionId;

use super::client::xml_body;
use super::discovery::{first_href, stops_discovery};
use super::xml::{self, Multistatus, Response};
use super::{DavProvider, Kind};

fn same_path(a: &str, b: &str) -> bool {
    a.trim_end_matches('/') == b.trim_end_matches('/')
}

/// `current-user-privilege-set` lists what the user may do. Without
/// `write-content` (or an aggregate containing it) the collection is read-only;
/// a server that does not report privileges is taken as writable.
fn read_only(response: &Response) -> bool {
    response
        .props
        .get("current-user-privilege-set")
        .is_some_and(|p| !(p.has("all") || p.has("write") || p.has("write-content")))
}

fn display_name(response: &Response, path: &str) -> String {
    response.text("displayname").map_or_else(
        || {
            path.trim_end_matches('/')
                .rsplit('/')
                .next()
                .unwrap_or(path)
                .to_owned()
        },
        str::to_owned,
    )
}

/// The `calendar-timezone` property is a VTIMEZONE; the name is its TZID.
fn timezone(response: &Response) -> Option<String> {
    response
        .text("calendar-timezone")?
        .lines()
        .find_map(|l| l.trim().strip_prefix("TZID:"))
        .map(str::to_owned)
}

/// The collections of `kind` in a listing, by their path on the server.
fn collections_of(ms: Multistatus, base: &Url, kind: Kind) -> Vec<(String, Response)> {
    let wanted = match kind {
        Kind::Contacts => ["addressbook"].as_slice(),
        Kind::Calendar => ["calendar", "subscribed"].as_slice(),
    };
    ms.responses
        .into_iter()
        .filter(|r| {
            r.props
                .get("resourcetype")
                .is_some_and(|t| wanted.iter().any(|w| t.has(w)))
        })
        .filter_map(|r| Some((base.join(&r.href).ok()?.path().to_owned(), r)))
        .collect()
}

impl DavProvider {
    async fn listing(&self, kind: Kind) -> Result<Vec<(String, Response)>, DomainError> {
        let home = self.home(kind).await?;
        let ms = self
            .client
            .propfind(&home.url, "1", xml::collection_listing(kind))
            .await?;
        let mut listed = collections_of(ms, &home.url, kind);
        if kind == Kind::Calendar {
            listed = self.with_subscribed(listed).await?;
        }
        Ok(listed)
    }

    /// Calendars subscribed on a generic server sit outside our home. One the
    /// server now refuses or no longer has is dropped, so reconcile removes it.
    async fn with_subscribed(
        &self,
        mut listed: Vec<(String, Response)>,
    ) -> Result<Vec<(String, Response)>, DomainError> {
        let urls = self
            .subscribed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        for url in urls {
            let ms = match self
                .propfind_checked(&url, "0", xml::collection_listing(Kind::Calendar))
                .await
            {
                Ok(ms) => ms,
                Err(DomainError::NotFound(_) | DomainError::Forbidden(_)) => continue,
                Err(e) => return Err(e),
            };
            for found in collections_of(ms, &url, Kind::Calendar) {
                if listed.iter().all(|(path, _)| !same_path(path, &found.0)) {
                    listed.push(found);
                }
            }
        }
        Ok(listed)
    }

    /// A PROPFIND whose 404 and 403 say something about the target itself.
    async fn propfind_checked(
        &self,
        url: &Url,
        depth: &str,
        body: String,
    ) -> Result<Multistatus, DomainError> {
        let reply = self
            .client
            .request("PROPFIND", url, &[("Depth", depth)], Some(xml_body(body)))
            .await?;
        match reply.status {
            StatusCode::NOT_FOUND => Err(DomainError::NotFound(url.path().to_owned())),
            StatusCode::FORBIDDEN => Err(DomainError::Forbidden(url.path().to_owned())),
            _ => reply.multistatus("PROPFIND"),
        }
    }

    pub(super) async fn address_books(&self) -> Result<Vec<AddressBook>, DomainError> {
        Ok(self
            .listing(Kind::Contacts)
            .await?
            .into_iter()
            .map(|(path, r)| AddressBook {
                account: self.account,
                name: display_name(&r, &path),
                description: r.text("addressbook-description").map(str::to_owned),
                read_only: read_only(&r),
                collection: CollectionId::new(path),
            })
            .collect())
    }

    pub(super) async fn calendars(&self) -> Result<Vec<Calendar>, DomainError> {
        let home = self.home(Kind::Calendar).await?;
        let listed = self.listing(Kind::Calendar).await?;
        let subscribed: Vec<String> = self
            .subscribed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|u| u.path().to_owned())
            .collect();
        let mut calendars = Vec::new();
        for (path, r) in listed {
            let own_principal = home.principal.clone().or_else(|| {
                let href = r.props.get("current-user-principal")?.hrefs.first()?;
                Some(home.url.join(href).ok()?.path().to_owned())
            });
            let owner = r
                .props
                .get("owner")
                .and_then(|p| p.hrefs.first())
                .and_then(|h| home.url.join(h).ok())
                .map(|u| u.path().to_owned());
            // Subscriptions are told by the calendarserver resource type or a
            // `source` with a URL (Radicale sends an empty one on every
            // calendar); owner against principal separates own from shared.
            let ownership = if r
                .props
                .get("resourcetype")
                .is_some_and(|t| t.has("subscribed"))
                || r.props.get("source").is_some_and(|p| !p.hrefs.is_empty())
            {
                Ownership::Subscription
            } else if matches!((&owner, &own_principal), (Some(o), Some(p)) if !same_path(o, p))
                // Google sends no `DAV:owner`, so a calendar we subscribed to would read as ours.
                || subscribed.iter().any(|s| same_path(s, &path))
            {
                Ownership::Shared
            } else {
                Ownership::Own
            };
            let components = r
                .props
                .get("supported-calendar-component-set")
                .map(|p| {
                    p.names
                        .iter()
                        .filter_map(|n| match n.to_ascii_uppercase().as_str() {
                            "VEVENT" => Some(CalendarComponent::Event),
                            "VTODO" => Some(CalendarComponent::Todo),
                            _ => None,
                        })
                        .collect()
                })
                .unwrap_or_default();
            calendars.push(Calendar {
                account: self.account,
                name: display_name(&r, &path),
                color: r.text("calendar-color").map(str::to_owned),
                timezone: timezone(&r),
                read_only: read_only(&r),
                ownership,
                components,
                collection: CollectionId::new(path),
            });
        }
        Ok(calendars)
    }
}

impl DavProvider {
    pub(super) async fn google_subscribe(&self, email: &str) -> Result<Subscribed, DomainError> {
        let url = self.calendar_list_url(&[])?;
        let body = json!({ "id": email }).to_string();
        let reply = self
            .client
            .request("POST", &url, &[], Some(("application/json", body)))
            .await?;
        match reply.status {
            // 409: already in the list.
            s if s.is_success() || s == StatusCode::CONFLICT => {}
            StatusCode::NOT_FOUND => return Err(DomainError::NotFound(email.to_owned())),
            StatusCode::FORBIDDEN => return Err(DomainError::Forbidden(email.to_owned())),
            _ => return Err(reply.unexpected("calendarList.insert")),
        }
        // Google's CalDAV home lists only the calendars ticked in syncselect, and a
        // new calendarList entry is not, so we also remember its CalDAV URL. The
        // `%40` matches the hrefs Google's own listing uses.
        let url = self
            .caldav
            .as_ref()
            .ok_or_else(|| DomainError::Provider("no CalDAV URL for Google".into()))?
            .join(&format!("../{}/events/", email.replace('@', "%40")))
            .map_err(bad_url)?;
        let mut subscribed = self.subscribed.lock().unwrap_or_else(|e| e.into_inner());
        if !subscribed.contains(&url) {
            subscribed.push(url.clone());
        }
        Ok(Subscribed::Local(vec![url]))
    }

    pub(super) async fn google_unsubscribe(
        &self,
        calendar: &CollectionId,
    ) -> Result<(), DomainError> {
        let id = google_calendar_id(calendar.as_str())
            .ok_or_else(|| DomainError::Validation(format!("not a Google calendar: {calendar}")))?;
        let url = self.calendar_list_url(&[&id])?;
        let reply = self.client.request("DELETE", &url, &[], None).await?;
        match reply.status {
            // Already gone from the list.
            s if s.is_success() || s == StatusCode::NOT_FOUND => Ok(()),
            StatusCode::FORBIDDEN => Err(DomainError::Forbidden(id)),
            _ => Err(reply.unexpected("calendarList.delete")),
        }
    }

    fn calendar_list_url(&self, tail: &[&str]) -> Result<Url, DomainError> {
        let mut url = self.api.clone();
        url.path_segments_mut()
            .map_err(|()| DomainError::Provider("Calendar API URL cannot be a base".into()))?
            .pop_if_empty()
            .extend(["users", "me", "calendarList"])
            .extend(tail);
        Ok(url)
    }

    pub(super) async fn caldav_subscribe(&self, email: &str) -> Result<Subscribed, DomainError> {
        let home = self.home(Kind::Calendar).await?;
        let own = home.principal.as_deref().unwrap_or(home.url.path());
        let own = home.url.join(own).map_err(bad_url)?;
        let colleague = match self.search_principal(&own, email).await {
            Ok(Some(home)) => home,
            Ok(None) => self.guess_home(&own, email).await?,
            Err(e) if stops_discovery(&e) => return Err(e),
            Err(_) => self.guess_home(&own, email).await?,
        };
        let calendars = self.readable_calendars(&colleague).await?;
        let mut subscribed = self.subscribed.lock().unwrap_or_else(|e| e.into_inner());
        for url in &calendars {
            if !subscribed.contains(url) {
                subscribed.push(url.clone());
            }
        }
        Ok(Subscribed::Local(calendars))
    }

    /// Radicale and Stalwart cannot search principals, but name them after the
    /// email or its local part, as ours is.
    async fn guess_home(&self, own: &Url, email: &str) -> Result<Url, DomainError> {
        let local = email.split('@').next().unwrap_or(email);
        let mut failure = DomainError::NotFound(email.to_owned());
        for name in [email, local] {
            match self.calendar_home_of(&sibling(own, name)?).await {
                Ok(home) => return Ok(home),
                Err(e @ DomainError::Forbidden(_)) => failure = e,
                Err(DomainError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Err(failure)
    }

    /// The calendar home of the principal whose address set holds `email`,
    /// asked of the server's principal collections (RFC 3744 §9.4).
    async fn search_principal(&self, own: &Url, email: &str) -> Result<Option<Url>, DomainError> {
        let ms = self
            .propfind_checked(own, "0", xml::principal_collections())
            .await?;
        let sets = ms
            .responses
            .first()
            .and_then(|r| r.props.get("principal-collection-set"));
        for set in sets.into_iter().flat_map(|p| &p.hrefs) {
            let url = own.join(set).map_err(bad_url)?;
            let reply = self
                .client
                .request(
                    "REPORT",
                    &url,
                    &[("Depth", "0")],
                    Some(xml_body(xml::principal_search(email))),
                )
                .await?;
            if reply.status != StatusCode::MULTI_STATUS {
                continue;
            }
            let found = reply.multistatus("REPORT")?;
            if let Some(home) = found
                .responses
                .iter()
                .find_map(|r| first_href(r, "calendar-home-set"))
            {
                return Ok(Some(url.join(&home).map_err(bad_url)?));
            }
        }
        Ok(None)
    }

    async fn calendar_home_of(&self, principal: &Url) -> Result<Url, DomainError> {
        let ms = self
            .propfind_checked(principal, "0", xml::principal_and_home(Kind::Calendar))
            .await?;
        let home = ms
            .responses
            .first()
            .and_then(|r| first_href(r, "calendar-home-set"));
        match home {
            Some(home) => principal.join(&home).map_err(bad_url),
            None => Ok(principal.clone()),
        }
    }

    /// The calendars of `home` the server shows us. None means it shares
    /// nothing with us, whatever it says about the home itself.
    async fn readable_calendars(&self, home: &Url) -> Result<Vec<Url>, DomainError> {
        let ms = self
            .propfind_checked(home, "1", xml::collection_listing(Kind::Calendar))
            .await?;
        let calendars: Vec<Url> = collections_of(ms, home, Kind::Calendar)
            .into_iter()
            .filter_map(|(path, _)| home.join(&path).ok())
            .collect();
        if calendars.is_empty() {
            return Err(DomainError::Forbidden(home.path().to_owned()));
        }
        Ok(calendars)
    }
}

fn bad_url(e: url::ParseError) -> DomainError {
    DomainError::Provider(format!("DAV server sent an invalid href: {e}"))
}

/// `principal` with its last path segment replaced by `name`.
fn sibling(principal: &Url, name: &str) -> Result<Url, DomainError> {
    let mut url = principal.clone();
    url.path_segments_mut()
        .map_err(|()| DomainError::Provider("principal URL cannot be a base".into()))?
        .pop_if_empty()
        .pop()
        .push(name)
        .push("");
    Ok(url)
}

/// The segment after `/caldav/v2/` is the calendar id, percent-encoded.
pub(super) fn google_calendar_id(collection: &str) -> Option<String> {
    let encoded = collection
        .split_once("/caldav/v2/")?
        .1
        .split('/')
        .next()
        .filter(|id| !id.is_empty())?;
    let bytes = encoded.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok());
        match (bytes[i], hex.and_then(|h| u8::from_str_radix(h, 16).ok())) {
            (b'%', Some(byte)) => {
                decoded.push(byte);
                i += 3;
            }
            (byte, _) => {
                decoded.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8(decoded).ok()
}
