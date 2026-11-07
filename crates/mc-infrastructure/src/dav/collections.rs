//! The collection list: names, permissions and ownership from one PROPFIND.

use mc_domain::error::DomainError;
use mc_domain::model::collection::{AddressBook, Calendar, CalendarComponent, Ownership};
use mc_domain::value_objects::remote::CollectionId;

use super::xml::{self, Response};
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

impl DavProvider {
    async fn listing(&self, kind: Kind) -> Result<Vec<(String, Response)>, DomainError> {
        let home = self.home(kind).await?;
        let ms = self
            .client
            .propfind(&home.url, "1", xml::collection_listing(kind))
            .await?;
        let wanted = match kind {
            Kind::Contacts => ["addressbook"].as_slice(),
            Kind::Calendar => ["calendar", "subscribed"].as_slice(),
        };
        Ok(ms
            .responses
            .into_iter()
            .filter(|r| {
                r.props
                    .get("resourcetype")
                    .is_some_and(|t| wanted.iter().any(|w| t.has(w)))
            })
            .filter_map(|r| Some((home.url.join(&r.href).ok()?.path().to_owned(), r)))
            .collect())
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
        let mut calendars = Vec::new();
        for (path, r) in self.listing(Kind::Calendar).await? {
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
            } else if matches!((&owner, &own_principal), (Some(o), Some(p)) if !same_path(o, p)) {
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
