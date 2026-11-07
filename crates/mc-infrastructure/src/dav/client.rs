//! HTTP transport for WebDAV: credentials through the login gate, status
//! classes mapped to the errors the sync pass reacts to.

use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{CONTENT_TYPE, HeaderMap, LOCATION, RETRY_AFTER};
use reqwest::{Method, StatusCode};
use url::Url;

use mc_domain::error::DomainError;
use mc_domain::traits::login_gate::{LoginGate, LoginSecret, login_with};
use mc_domain::value_objects::remote::Etag;

use super::xml::{Multistatus, parse_multistatus};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

pub(super) struct Reply {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

impl Reply {
    pub fn etag(&self) -> Option<Etag> {
        self.headers.get("etag")?.to_str().ok().map(etag)
    }

    pub fn location(&self) -> Option<&str> {
        self.headers.get(LOCATION)?.to_str().ok()
    }

    pub fn is_redirect(&self) -> bool {
        self.status.is_redirection() && self.location().is_some()
    }

    /// Any answer other than the expected one is a failure of this call only:
    /// auth, throttling and outages were already told apart by `request`.
    pub fn expect(self, what: &str, ok: &[StatusCode]) -> Result<Self, DomainError> {
        if ok.contains(&self.status) {
            return Ok(self);
        }
        Err(self.unexpected(what))
    }

    /// Carries the start of the body: a bare status hides why a server
    /// refused (Google names the disabled API or missing scope there).
    pub fn unexpected(&self, what: &str) -> DomainError {
        let body = String::from_utf8_lossy(&self.body);
        let body: String = body.split_whitespace().collect::<Vec<_>>().join(" ");
        let body: String = body.chars().take(300).collect();
        DomainError::Provider(format!(
            "DAV {what}: the server answered {}: {body}",
            self.status
        ))
    }

    pub fn multistatus(self, what: &str) -> Result<Multistatus, DomainError> {
        parse_multistatus(&self.expect(what, &[StatusCode::MULTI_STATUS])?.body)
    }
}

/// Servers disagree on quoting and weakness (`"x"`, `x`, `W/"x"`). One form
/// everywhere keeps the etag a listing returns equal to the one a write
/// returns. Weak tags are normalized to strong because `If-Match` compares
/// strongly, and a tag we could never send back would make every edit a conflict.
pub(super) fn etag(raw: &str) -> Etag {
    let raw = raw.trim();
    let raw = raw.strip_prefix("W/").unwrap_or(raw).trim_matches('"');
    Etag::new(format!("\"{raw}\""))
}

pub(super) struct DavClient {
    http: reqwest::Client,
    username: String,
    gate: Arc<dyn LoginGate>,
}

impl DavClient {
    pub fn new(username: String, gate: Arc<dyn LoginGate>) -> Result<Self, DomainError> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            // Discovery follows redirects by hand: reqwest turns a redirected
            // PROPFIND into a GET and would hand our credentials to any host.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| DomainError::Provider(format!("HTTP client: {e}")))?;
        Ok(Self {
            http,
            username,
            gate,
        })
    }

    pub async fn request(
        &self,
        method: &str,
        url: &Url,
        headers: &[(&str, &str)],
        body: Option<(&str, String)>,
    ) -> Result<Reply, DomainError> {
        let method = Method::from_bytes(method.as_bytes())
            .map_err(|e| DomainError::Validation(format!("HTTP method {method}: {e}")))?;
        login_with(&DavView(self.gate.as_ref()), |secret| {
            let mut request = self.http.request(method.clone(), url.clone());
            for (name, value) in headers {
                request = request.header(*name, *value);
            }
            if let Some((content_type, body)) = &body {
                request = request
                    .header(CONTENT_TYPE, *content_type)
                    .body(body.clone());
            }
            request = match secret {
                LoginSecret::Password(password) => {
                    request.basic_auth(&self.username, Some(password))
                }
                LoginSecret::OAuthToken(token) => request.bearer_auth(token),
            };
            async move {
                let response = request.send().await.map_err(transport_error)?;
                let status = response.status();
                let headers = response.headers().clone();
                let body = response.bytes().await.map_err(transport_error)?.to_vec();
                classify(status, &headers, &body)?;
                Ok(Reply {
                    status,
                    headers,
                    body,
                })
            }
        })
        .await
    }

    pub async fn propfind(
        &self,
        url: &Url,
        depth: &str,
        body: String,
    ) -> Result<Multistatus, DomainError> {
        self.request("PROPFIND", url, &[("Depth", depth)], Some(xml_body(body)))
            .await?
            .multistatus("PROPFIND")
    }
}

pub(super) fn xml_body(body: String) -> (&'static str, String) {
    ("application/xml; charset=utf-8", body)
}

fn transport_error(e: reqwest::Error) -> DomainError {
    if e.is_timeout() {
        DomainError::ConnectionTimeout(e.to_string())
    } else {
        DomainError::ConnectionFailed(e.to_string())
    }
}

/// The account's gate as DAV may use it: the secret and one refresh of a
/// refused token, nothing more. The account's state belongs to IMAP, so a DAV
/// server that is down or refusing never pauses the mail; the DAV loop keeps
/// its own backoff.
pub(super) struct DavView<'a>(pub(super) &'a dyn LoginGate);

#[allow(clippy::double_must_use)] // async_trait's expansion
#[async_trait::async_trait]
impl LoginGate for DavView<'_> {
    async fn secret(&self) -> Result<LoginSecret, DomainError> {
        self.0.secret().await
    }

    async fn login_failed(&self, secret: &LoginSecret, error: &DomainError, retried: bool) -> bool {
        matches!(error, DomainError::AuthenticationFailed(_))
            && !retried
            && self.0.smtp_token_refused(secret).await
    }

    fn login_succeeded(&self) {}

    async fn ready(&self) {
        self.0.ready().await;
    }
}

/// Only what stops a whole pass is an error here; a 4xx about one resource
/// (412, 404, a refused write) is for the caller to interpret.
pub(super) fn classify(
    status: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(), DomainError> {
    let retry_after = headers
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(|v| format!(", retry after {v}"))
        .unwrap_or_default();
    match status {
        StatusCode::UNAUTHORIZED => Err(DomainError::AuthenticationFailed(format!(
            "the DAV server answered {status}"
        ))),
        StatusCode::TOO_MANY_REQUESTS => Err(DomainError::Throttled(format!(
            "the DAV server answered {status}{retry_after}"
        ))),
        StatusCode::SERVICE_UNAVAILABLE if !retry_after.is_empty() => Err(DomainError::Throttled(
            format!("the DAV server answered {status}{retry_after}"),
        )),
        // Google reports its quota as a 403 with a rateLimitExceeded reason.
        StatusCode::FORBIDDEN
            if String::from_utf8_lossy(body)
                .to_lowercase()
                .contains("ratelimitexceeded") =>
        {
            Err(DomainError::Throttled(
                "the DAV server answered 403 rateLimitExceeded".into(),
            ))
        }
        s if s.is_server_error() => Err(DomainError::ConnectionFailed(format!(
            "the DAV server answered {s}"
        ))),
        _ => Ok(()),
    }
}
