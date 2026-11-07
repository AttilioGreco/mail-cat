//! Push side: conditional PUT and DELETE, and the copy the server holds
//! after a write.

use reqwest::StatusCode;

use mc_domain::error::DomainError;
use mc_domain::traits::dav_provider::{DeleteOutcome, PutOutcome, ResourcePut};
use mc_domain::value_objects::remote::{CollectionId, Etag, Href};

use super::{DavProvider, Kind};

fn same_body(a: &str, b: &str) -> bool {
    a.replace("\r\n", "\n").trim_end() == b.replace("\r\n", "\n").trim_end()
}

impl DavProvider {
    pub(super) async fn put(
        &self,
        kind: Kind,
        collection: &CollectionId,
        put: ResourcePut,
    ) -> Result<PutOutcome, DomainError> {
        let collection_url = self.url(kind, collection.as_str()).await?;
        let posted = put.if_match.is_none() && self.profile.posts_creates(kind);
        let (method, url) = if posted {
            ("POST", collection_url.clone())
        } else {
            ("PUT", self.url(kind, put.href.as_str()).await?)
        };
        let guard = match &put.if_match {
            Some(etag) => Some(("If-Match", etag.as_str())),
            None if self.profile.guards_creates() => Some(("If-None-Match", "*")),
            None => None,
        };
        let reply = self
            .client
            .request(
                method,
                &url,
                guard.as_slice(),
                Some((kind.content_type(), put.raw.clone())),
            )
            .await?;
        match reply.status {
            StatusCode::OK | StatusCode::CREATED | StatusCode::NO_CONTENT => {}
            StatusCode::PRECONDITION_FAILED => return Ok(PutOutcome::PreconditionFailed),
            // An update of a resource that vanished: the caller refetches and
            // finds it absent, the same path as a stale etag.
            StatusCode::NOT_FOUND | StatusCode::GONE if put.if_match.is_some() => {
                return Ok(PutOutcome::PreconditionFailed);
            }
            _ => return Err(reply.unexpected("PUT")),
        }

        let href = match reply.location().filter(|_| posted) {
            Some(location) => Href::new(
                collection_url
                    .join(location)
                    .map_err(|e| {
                        DomainError::Provider(format!(
                            "DAV created resource at an invalid Location: {e}"
                        ))
                    })?
                    .path(),
            ),
            None => put.href.clone(),
        };
        // RFC 4791 5.3.4: a server that changed the body must not send an
        // ETag, so a missing one means the stored copy differs. Google
        // rewrites without saying so, hence the unconditional fetch there.
        if let Some(etag) = reply.etag().filter(|_| !self.profile.rewrites_on_write()) {
            return Ok(PutOutcome::Stored {
                href,
                etag,
                raw: None,
            });
        }
        let stored = self
            .fetch(kind, collection, std::slice::from_ref(&href))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                DomainError::Provider(format!(
                    "DAV resource {href} is missing right after its write"
                ))
            })?;
        Ok(PutOutcome::Stored {
            href: stored.href,
            etag: stored.etag,
            raw: (!same_body(&stored.raw, &put.raw)).then_some(stored.raw),
        })
    }

    pub(super) async fn delete(
        &self,
        kind: Kind,
        href: &Href,
        if_match: &Etag,
    ) -> Result<DeleteOutcome, DomainError> {
        let url = self.url(kind, href.as_str()).await?;
        let reply = self
            .client
            .request("DELETE", &url, &[("If-Match", if_match.as_str())], None)
            .await?;
        match reply.status {
            StatusCode::PRECONDITION_FAILED => Ok(DeleteOutcome::PreconditionFailed),
            StatusCode::NOT_FOUND | StatusCode::GONE => Ok(DeleteOutcome::Gone),
            _ => reply
                .expect("DELETE", &[StatusCode::OK, StatusCode::NO_CONTENT])
                .map(|_| DeleteOutcome::Deleted),
        }
    }
}
