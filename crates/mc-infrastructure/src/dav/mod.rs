//! CardDAV and CalDAV adapter: one client behind both the `ContactsProvider`
//! and `CalendarProvider` ports. Providers differ only in credentials,
//! endpoint discovery and a few quirks, which [`Profile`] captures.

mod client;
mod collections;
mod discovery;
mod sync;
#[cfg(test)]
mod tests;
mod write;
mod xml;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use url::Url;

use mc_domain::error::DomainError;
use mc_domain::model::account::{AccountId, DavConfig};
use mc_domain::model::collection::{AddressBook, Calendar};
use mc_domain::traits::dav_provider::{
    CalendarProvider, CollectionChanges, ContactsProvider, DeleteOutcome, PutOutcome,
    RemoteResource, ResourcePut, Subscribed,
};
use mc_domain::traits::login_gate::LoginGate;
use mc_domain::value_objects::remote::{CollectionId, Etag, Href};
use mc_domain::value_objects::sync_cursor::SyncCursor;

use client::DavClient;
use discovery::Home;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Contacts,
    Calendar,
}

impl Kind {
    fn content_type(self) -> &'static str {
        match self {
            Self::Contacts => "text/vcard; charset=utf-8",
            Self::Calendar => "text/calendar; charset=utf-8",
        }
    }

    fn data_prop(self) -> &'static str {
        match self {
            Self::Contacts => "address-data",
            Self::Calendar => "calendar-data",
        }
    }
}

/// Server-specific behavior, kept as a switch on the one adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Profile {
    #[default]
    Generic,
    /// Google CardDAV/CalDAV: fixed endpoints, no `If-None-Match`, contacts
    /// created by POST, bodies rewritten on write, sync-token not guaranteed.
    Google,
}

impl Profile {
    fn guards_creates(self) -> bool {
        self != Self::Google
    }

    fn posts_creates(self, kind: Kind) -> bool {
        self == Self::Google && kind == Kind::Contacts
    }

    fn rewrites_on_write(self) -> bool {
        self == Self::Google
    }
}

/// The sync cursor, stored as JSON: `{"ctag": "...", "token": "..."}`.
///
/// `ctag` is the collection's `getctag`, the cheap "anything changed?" check.
/// `token` is the RFC 6578 `sync-token`, the delta. Each may be absent: a
/// server without sync-token (some Google paths) yields a ctag-only cursor,
/// whose every change is a full relisting (`reset`).
#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Cursor {
    #[serde(skip_serializing_if = "Option::is_none")]
    ctag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,
}

impl Cursor {
    /// An unreadable cursor is a missing one: the pass starts over.
    fn parse(cursor: &SyncCursor) -> Option<Self> {
        serde_json::from_str(cursor.as_str()).ok()
    }

    fn encode(&self) -> SyncCursor {
        SyncCursor::new(serde_json::to_string(self).unwrap_or_default())
    }
}

pub struct DavProvider {
    client: DavClient,
    account: AccountId,
    profile: Profile,
    /// `https://{email domain}/`, where discovery starts.
    origin: Url,
    carddav: Option<Url>,
    caldav: Option<Url>,
    contacts_home: OnceCell<Home>,
    calendar_home: OnceCell<Home>,
    /// Google's Calendar REST API, where its calendar list is edited.
    api: Url,
    /// Calendars of other principals, listed besides our own home. Starts from
    /// the account config; `subscribe` and `unsubscribe` keep it current so the
    /// next listing needs no restart.
    subscribed: Mutex<Vec<Url>>,
}

impl DavProvider {
    /// `dav` URLs skip discovery for their protocol; the Google profile has
    /// its own when none is given. The credential comes from `gate`, which
    /// also refreshes an OAuth token after a 401.
    pub fn new(
        account: AccountId,
        email: &str,
        username: String,
        gate: Arc<dyn LoginGate>,
        dav: Option<&DavConfig>,
        profile: Profile,
    ) -> Result<Self, DomainError> {
        let domain = email.rsplit_once('@').map_or(email, |(_, d)| d);
        let origin = Url::parse(&format!("https://{domain}/"))
            .map_err(|e| DomainError::Validation(format!("invalid DAV domain '{domain}': {e}")))?;
        Self::with_origin(account, origin, email, username, gate, dav, profile)
    }

    pub(super) fn with_origin(
        account: AccountId,
        origin: Url,
        email: &str,
        username: String,
        gate: Arc<dyn LoginGate>,
        dav: Option<&DavConfig>,
        profile: Profile,
    ) -> Result<Self, DomainError> {
        let mut carddav = dav.and_then(|d| d.carddav.clone());
        let mut caldav = dav.and_then(|d| d.caldav.clone());
        if profile == Profile::Google {
            // The address book is the collection itself; the CalDAV principal
            // answers the calendar-home-set (Google's guide gives these two as
            // the only starting points, `.well-known` is not documented for CalDAV).
            carddav = carddav.or_else(|| {
                Url::parse(&format!(
                    "https://www.googleapis.com/carddav/v1/principals/{email}/lists/default/"
                ))
                .ok()
            });
            caldav = caldav.or_else(|| {
                Url::parse(&format!(
                    "https://apidata.googleusercontent.com/caldav/v2/{email}/user"
                ))
                .ok()
            });
        }
        Ok(Self {
            client: DavClient::new(username, gate)?,
            account,
            profile,
            origin,
            carddav,
            caldav,
            contacts_home: OnceCell::new(),
            calendar_home: OnceCell::new(),
            api: Url::parse("https://www.googleapis.com/calendar/v3/")
                .map_err(|e| DomainError::Provider(format!("Calendar API URL: {e}")))?,
            subscribed: Mutex::new(dav.map(|d| d.subscribed.clone()).unwrap_or_default()),
        })
    }

    /// Whether the server offers CardDAV or CalDAV. Only a failure that says
    /// something about the account or the connection is an error: the rest
    /// means the server has no such service.
    pub async fn is_available(&self) -> Result<bool, DomainError> {
        let (contacts, calendar) =
            tokio::join!(self.home(Kind::Contacts), self.home(Kind::Calendar));
        if contacts.is_ok() || calendar.is_ok() {
            return Ok(true);
        }
        match (contacts, calendar) {
            (Err(e), _) | (_, Err(e))
                if matches!(
                    e,
                    DomainError::AuthenticationFailed(_)
                        | DomainError::ConnectionFailed(_)
                        | DomainError::ConnectionTimeout(_)
                        | DomainError::Throttled(_)
                ) =>
            {
                Err(e)
            }
            _ => Ok(false),
        }
    }

    /// Resolves once per provider; a failure is retried by the next pass.
    async fn home(&self, kind: Kind) -> Result<&Home, DomainError> {
        let (cell, manual) = match kind {
            Kind::Contacts => (&self.contacts_home, &self.carddav),
            Kind::Calendar => (&self.calendar_home, &self.caldav),
        };
        cell.get_or_try_init(|| async {
            match manual {
                Some(url) => discovery::resolve_home(&self.client, kind, url).await,
                None => discovery::discover(&self.client, kind, &self.origin).await,
            }
        })
        .await
    }

    /// Collection and resource paths are those the server sent; resolving
    /// them against the home keeps every request on the server's own origin.
    async fn url(&self, kind: Kind, path: &str) -> Result<Url, DomainError> {
        self.home(kind)
            .await?
            .url
            .join(path)
            .map_err(|e| DomainError::Validation(format!("invalid DAV path '{path}': {e}")))
    }
}

#[async_trait]
impl ContactsProvider for DavProvider {
    fn creates_by_post(&self) -> bool {
        self.profile.posts_creates(Kind::Contacts)
    }

    async fn list_address_books(&self) -> Result<Vec<AddressBook>, DomainError> {
        self.address_books().await
    }

    async fn address_book_changes(
        &self,
        book: &CollectionId,
        since: Option<&SyncCursor>,
    ) -> Result<CollectionChanges, DomainError> {
        self.changes(Kind::Contacts, book, since).await
    }

    async fn fetch_contacts(
        &self,
        book: &CollectionId,
        hrefs: &[Href],
    ) -> Result<Vec<RemoteResource>, DomainError> {
        self.fetch(Kind::Contacts, book, hrefs).await
    }

    async fn put_contact(
        &self,
        book: &CollectionId,
        put: ResourcePut,
    ) -> Result<PutOutcome, DomainError> {
        self.put(Kind::Contacts, book, put).await
    }

    async fn delete_contact(
        &self,
        _book: &CollectionId,
        href: &Href,
        if_match: &Etag,
    ) -> Result<DeleteOutcome, DomainError> {
        self.delete(Kind::Contacts, href, if_match).await
    }
}

#[async_trait]
impl CalendarProvider for DavProvider {
    async fn list_calendars(&self) -> Result<Vec<Calendar>, DomainError> {
        self.calendars().await
    }

    async fn calendar_changes(
        &self,
        calendar: &CollectionId,
        since: Option<&SyncCursor>,
    ) -> Result<CollectionChanges, DomainError> {
        self.changes(Kind::Calendar, calendar, since).await
    }

    async fn fetch_events(
        &self,
        calendar: &CollectionId,
        hrefs: &[Href],
    ) -> Result<Vec<RemoteResource>, DomainError> {
        self.fetch(Kind::Calendar, calendar, hrefs).await
    }

    async fn put_event(
        &self,
        calendar: &CollectionId,
        put: ResourcePut,
    ) -> Result<PutOutcome, DomainError> {
        self.put(Kind::Calendar, calendar, put).await
    }

    async fn delete_event(
        &self,
        _calendar: &CollectionId,
        href: &Href,
        if_match: &Etag,
    ) -> Result<DeleteOutcome, DomainError> {
        self.delete(Kind::Calendar, href, if_match).await
    }

    async fn subscribe(&self, email: &str) -> Result<Subscribed, DomainError> {
        if self.profile == Profile::Google {
            return self.google_subscribe(email).await;
        }
        self.caldav_subscribe(email).await
    }

    async fn unsubscribe(&self, calendar: &CollectionId) -> Result<(), DomainError> {
        if self.profile == Profile::Google {
            self.google_unsubscribe(calendar).await?;
        }
        self.subscribed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|u| u.path().trim_end_matches('/') != calendar.as_str().trim_end_matches('/'));
        Ok(())
    }
}
