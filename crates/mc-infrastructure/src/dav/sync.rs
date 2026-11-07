//! Pull side: the cheap check, the delta (RFC 6578) with its relisting
//! fallback, and multiget fetches.

use reqwest::StatusCode;
use url::Url;

use mc_domain::error::DomainError;
use mc_domain::traits::dav_provider::{CollectionChanges, RemoteResource};
use mc_domain::value_objects::remote::{CollectionId, Href};
use mc_domain::value_objects::sync_cursor::SyncCursor;

use super::client::{etag, xml_body};
use super::xml::{self, Multistatus};
use super::{Cursor, DavProvider, Kind};

/// Hrefs per multiget: keeps one request line-sized for strict servers.
const MULTIGET_CHUNK: usize = 50;

/// Answers a server gives when it cannot serve the token: the RFC's
/// `valid-sync-token` precondition (403 or 409 by implementation), a stale
/// state (412), or no support for the report at all.
fn token_refused(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::FORBIDDEN
            | StatusCode::CONFLICT
            | StatusCode::PRECONDITION_FAILED
            | StatusCode::BAD_REQUEST
            | StatusCode::METHOD_NOT_ALLOWED
            | StatusCode::NOT_IMPLEMENTED
            | StatusCode::INSUFFICIENT_STORAGE
    )
}

fn same_path(a: &str, b: &str) -> bool {
    a.trim_end_matches('/') == b.trim_end_matches('/')
}

impl DavProvider {
    pub(super) async fn changes(
        &self,
        kind: Kind,
        collection: &CollectionId,
        since: Option<&SyncCursor>,
    ) -> Result<CollectionChanges, DomainError> {
        let url = self.url(kind, collection.as_str()).await?;
        let head = self
            .client
            .propfind(&url, "0", xml::head_props())
            .await?
            .responses
            .into_iter()
            .next()
            .unwrap_or_default();
        // Read before anything is listed: a change that lands in between
        // shows up on the next pass instead of being skipped.
        let now = Cursor {
            ctag: head.text("getctag").map(str::to_owned),
            token: head.text("sync-token").map(str::to_owned),
        };
        let old = since.and_then(Cursor::parse);
        if let (Some(old), Some(since)) = (&old, since) {
            let unchanged = if now.ctag.is_some() {
                old.ctag == now.ctag
            } else {
                now.token.is_some() && old.token == now.token
            };
            if unchanged {
                return Ok(CollectionChanges {
                    cursor: since.clone(),
                    changed: Vec::new(),
                    removed: Vec::new(),
                    reset: false,
                });
            }
        }

        if let Some(token) = old.as_ref().and_then(|o| o.token.as_deref())
            && let Some(delta) = self.delta(&url, token).await?
        {
            return Ok(self.delta_changes(&url, delta, now.ctag));
        }
        self.relist(&url, now).await
    }

    /// `None` when the server cannot serve the token.
    async fn delta(&self, url: &Url, token: &str) -> Result<Option<Multistatus>, DomainError> {
        let reply = self
            .client
            .request(
                "REPORT",
                url,
                &[("Depth", "0")],
                Some(xml_body(xml::sync_collection(token))),
            )
            .await?;
        if token_refused(reply.status) {
            return Ok(None);
        }
        reply.multistatus("sync-collection").map(Some)
    }

    fn delta_changes(
        &self,
        url: &Url,
        delta: Multistatus,
        ctag: Option<String>,
    ) -> CollectionChanges {
        let mut changed = Vec::new();
        let mut removed = Vec::new();
        for r in delta.responses {
            let Some(path) = url.join(&r.href).ok().map(|u| u.path().to_owned()) else {
                continue;
            };
            if same_path(&path, url.path()) {
                continue;
            }
            if r.status == Some(404) {
                removed.push(Href::new(path));
            } else if let Some(tag) = r.text("getetag").filter(|_| !r.is_collection()) {
                changed.push((Href::new(path), etag(tag)));
            }
        }
        CollectionChanges {
            cursor: Cursor {
                ctag,
                token: delta.sync_token,
            }
            .encode(),
            changed,
            removed,
            reset: false,
        }
    }

    /// Every href and etag of the collection, for the caller to diff.
    async fn relist(&self, url: &Url, now: Cursor) -> Result<CollectionChanges, DomainError> {
        let listing = self.client.propfind(url, "1", xml::etag_listing()).await?;
        let changed = listing
            .responses
            .iter()
            .filter(|r| !r.is_collection())
            .filter_map(|r| {
                let path = url.join(&r.href).ok()?.path().to_owned();
                Some((Href::new(path), etag(r.text("getetag")?)))
            })
            .collect();
        Ok(CollectionChanges {
            cursor: now.encode(),
            changed,
            removed: Vec::new(),
            reset: true,
        })
    }

    pub(super) async fn fetch(
        &self,
        kind: Kind,
        collection: &CollectionId,
        hrefs: &[Href],
    ) -> Result<Vec<RemoteResource>, DomainError> {
        let url = self.url(kind, collection.as_str()).await?;
        let mut found = Vec::with_capacity(hrefs.len());
        for chunk in hrefs.chunks(MULTIGET_CHUNK) {
            let body = xml::multiget(kind, chunk.iter().map(Href::as_str));
            let reply = self
                .client
                .request("REPORT", &url, &[("Depth", "1")], Some(xml_body(body)))
                .await?;
            for r in reply.multistatus("multiget")?.responses {
                // A 404 response, or one without data, is a resource that is gone.
                let (Some(tag), Some(raw), Ok(href)) = (
                    r.text("getetag"),
                    r.text(kind.data_prop()),
                    url.join(&r.href),
                ) else {
                    continue;
                };
                found.push(RemoteResource {
                    href: Href::new(href.path()),
                    etag: etag(tag),
                    raw: raw.to_owned(),
                });
            }
        }
        Ok(found)
    }
}
