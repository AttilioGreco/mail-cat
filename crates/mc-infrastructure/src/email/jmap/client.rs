//! JMAP transport: session discovery, authenticated method calls, blob download.

use std::sync::Arc;
use std::time::Duration;

use reqwest::{RequestBuilder, StatusCode};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use url::Url;

use mc_domain::error::DomainError;
use mc_domain::traits::login_gate::{LoginGate, LoginSecret, login_with};

use crate::persistence::DownloadMeter;

const CORE: &str = "urn:ietf:params:jmap:core";
const MAIL: &str = "urn:ietf:params:jmap:mail";
const SUBMISSION: &str = "urn:ietf:params:jmap:submission";
/// Outlives any push connection: a dead one is noticed by the missing pings.
const STREAM_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
/// A server may advertise more, but a page this size keeps one reply bounded.
const MAX_OBJECTS_CEILING: usize = 500;

/// A method-level failure: the request went through, this call did not.
#[derive(Debug)]
pub(super) struct MethodError {
    pub kind: String,
    description: String,
}

impl From<MethodError> for DomainError {
    fn from(e: MethodError) -> Self {
        Self::Provider(format!("JMAP {}: {}", e.kind, e.description))
    }
}

pub(super) struct Session {
    pub account_id: String,
    /// Objects per `/get` or `/query` call the server accepts.
    pub max_objects: usize,
    /// Objects per `/set` call.
    pub max_set: usize,
    /// Changes whenever the server's capabilities or accounts do.
    state: String,
    api_url: Url,
    download_template: String,
    upload_template: String,
    event_template: String,
    origin: Url,
}

pub(super) struct JmapClient {
    http: reqwest::Client,
    session_url: Url,
    username: String,
    gate: Arc<dyn LoginGate>,
    /// Dropped when the server reports a newer session state.
    session: RwLock<Option<Arc<Session>>>,
    meter: Option<DownloadMeter>,
}

/// `example.com` and `https://example.com` become the RFC 8620 §2.2
/// well-known URL; a full session URL is kept as given.
pub(super) fn session_url(input: &str) -> Result<Url, DomainError> {
    let input = input.trim();
    let with_scheme = if input.contains("://") {
        input.to_owned()
    } else {
        format!("https://{input}")
    };
    let mut url = Url::parse(&with_scheme)
        .map_err(|e| DomainError::Validation(format!("invalid JMAP URL '{input}': {e}")))?;
    if url.path() == "/" {
        url.set_path("/.well-known/jmap");
    }
    Ok(url)
}

/// The advertised URL's path and query on the session's own origin. Servers
/// behind a proxy advertise their internal hostname, and credentials must not
/// follow a server-supplied host anyway.
fn on_origin(origin: &Url, advertised: &str) -> Result<Url, DomainError> {
    let advertised = Url::parse(advertised)
        .map_err(|e| DomainError::Provider(format!("JMAP session URL '{advertised}': {e}")))?;
    let mut url = origin.clone();
    url.set_path(advertised.path());
    url.set_query(advertised.query());
    Ok(url)
}

impl JmapClient {
    pub fn new(
        session_url: &str,
        username: String,
        gate: Arc<dyn LoginGate>,
    ) -> Result<Self, DomainError> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| DomainError::Provider(format!("HTTP client: {e}")))?;
        Ok(Self {
            http,
            session_url: self::session_url(session_url)?,
            username,
            gate,
            session: RwLock::new(None),
            meter: None,
        })
    }

    pub fn with_download_meter(mut self, meter: DownloadMeter) -> Self {
        self.meter = Some(meter);
        self
    }

    pub async fn ready(&self) {
        self.gate.ready().await;
    }

    async fn record(&self, bytes: usize) {
        if let Some(meter) = &self.meter {
            meter.record(bytes as u64).await;
        }
    }

    /// Fetches the session object; also the login test, as nothing else
    /// answers 401 for a wrong password before any mail is touched.
    pub async fn session(&self) -> Result<Arc<Session>, DomainError> {
        if let Some(session) = self.session.read().await.as_ref() {
            return Ok(session.clone());
        }
        let mut cached = self.session.write().await;
        if let Some(session) = cached.as_ref() {
            return Ok(session.clone());
        }
        let body: Value = self
            .send(|| self.http.get(self.session_url.clone()))
            .await?
            .json()
            .await
            .map_err(|e| DomainError::Provider(format!("JMAP session: {e}")))?;
        let session = Arc::new(self.parse_session(&body)?);
        *cached = Some(session.clone());
        Ok(session)
    }

    fn parse_session(&self, body: &Value) -> Result<Session, DomainError> {
        let text = |key: &str| {
            body[key].as_str().ok_or_else(|| {
                DomainError::Provider(format!("JMAP session has no {key}: not a JMAP server?"))
            })
        };
        let account_id = body["primaryAccounts"][MAIL]
            .as_str()
            .ok_or_else(|| DomainError::Provider("JMAP session has no mail account".into()))?
            .to_owned();
        let limit = |key: &str| {
            body["capabilities"][CORE][key]
                .as_u64()
                .map_or(MAX_OBJECTS_CEILING, |n| n as usize)
                .clamp(1, MAX_OBJECTS_CEILING)
        };
        Ok(Session {
            account_id,
            max_objects: limit("maxObjectsInGet"),
            max_set: limit("maxObjectsInSet"),
            state: body["state"].as_str().unwrap_or_default().to_owned(),
            api_url: on_origin(&self.session_url, text("apiUrl")?)?,
            download_template: text("downloadUrl")?.to_owned(),
            upload_template: text("uploadUrl")?.to_owned(),
            event_template: text("eventSourceUrl")?.to_owned(),
            origin: self.session_url.clone(),
        })
    }

    /// One request carrying `calls`; the replies come back in call order.
    /// `accountId` is added to every call.
    pub async fn call(
        &self,
        calls: Vec<(&str, Value)>,
    ) -> Result<Vec<Result<Value, MethodError>>, DomainError> {
        let session = self.session().await?;
        let names: Vec<&str> = calls.iter().map(|(name, _)| *name).collect();
        let method_calls: Vec<Value> = calls
            .into_iter()
            .enumerate()
            .map(|(i, (name, mut args))| {
                args["accountId"] = json!(session.account_id);
                json!([name, args, format!("c{i}")])
            })
            .collect();
        let count = method_calls.len();
        let mut using = vec![CORE, MAIL];
        if names
            .iter()
            .any(|n| n.starts_with("EmailSubmission/") || n.starts_with("Identity/"))
        {
            using.push(SUBMISSION);
        }
        let request = json!({"using": using, "methodCalls": method_calls});
        let bytes = self
            .send(|| self.http.post(session.api_url.clone()).json(&request))
            .await?
            .bytes()
            .await
            .map_err(|e| DomainError::ConnectionFailed(format!("JMAP response: {e}")))?;
        self.record(bytes.len()).await;
        let reply: Value = serde_json::from_slice(&bytes)
            .map_err(|e| DomainError::Provider(format!("JMAP response: {e}")))?;

        let mut results: Vec<Option<Result<Value, MethodError>>> =
            std::iter::repeat_with(|| None).take(count).collect();
        for response in reply["methodResponses"].as_array().into_iter().flatten() {
            let Some(index) = response[2]
                .as_str()
                .and_then(|id| id.strip_prefix('c')?.parse::<usize>().ok())
                .filter(|i| *i < count)
            else {
                continue;
            };
            let args = response[1].clone();
            // `EmailSubmission/set` is followed by the `Email/set` it implied,
            // under the same call id: the call's own answer comes first.
            if results[index].is_some() {
                continue;
            }
            results[index] = Some(if response[0] == "error" {
                Err(MethodError {
                    kind: args["type"].as_str().unwrap_or("unknown").to_owned(),
                    description: args["description"].as_str().unwrap_or_default().to_owned(),
                })
            } else {
                Ok(args)
            });
        }
        let stale = reply["sessionState"]
            .as_str()
            .is_some_and(|state| state != session.state);
        let unknown_method = results
            .iter()
            .flatten()
            .any(|r| r.as_ref().is_err_and(|e| e.kind == "unknownMethod"));
        if stale || unknown_method {
            *self.session.write().await = None;
        }
        results
            .into_iter()
            .map(|r| r.ok_or_else(|| DomainError::Provider("JMAP reply misses a call".into())))
            .collect()
    }

    pub async fn download(&self, blob_id: &str) -> Result<Vec<u8>, DomainError> {
        let session = self.session().await?;
        let expanded = session
            .download_template
            .replace("{accountId}", &session.account_id)
            .replace("{blobId}", blob_id)
            .replace("{name}", "message.eml")
            .replace("{type}", "message%2Frfc822");
        let url = on_origin(&session.origin, &expanded)?;
        let response = self.send(|| self.http.get(url.clone())).await?;
        let bytes = response
            .bytes()
            .await
            .map_err(|e| DomainError::ConnectionFailed(format!("JMAP download: {e}")))?;
        self.record(bytes.len()).await;
        Ok(bytes.to_vec())
    }

    /// Uploads a message and returns its blob id.
    pub async fn upload(&self, data: Vec<u8>) -> Result<String, DomainError> {
        let session = self.session().await?;
        let expanded = session
            .upload_template
            .replace("{accountId}", &session.account_id);
        let url = on_origin(&session.origin, &expanded)?;
        let reply: Value = self
            .send(|| {
                self.http
                    .post(url.clone())
                    .header(reqwest::header::CONTENT_TYPE, "message/rfc822")
                    .body(data.clone())
            })
            .await?
            .json()
            .await
            .map_err(|e| DomainError::Provider(format!("JMAP upload: {e}")))?;
        reply["blobId"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| DomainError::Provider("JMAP upload answered no blobId".into()))
    }

    /// Opens the push stream for mail and mailbox changes; the account id
    /// its `state` events are keyed by comes along.
    pub async fn event_stream(&self) -> Result<(reqwest::Response, String), DomainError> {
        let session = self.session().await?;
        let expanded = session
            .event_template
            .replace("{types}", "Email,Mailbox")
            .replace("{closeafter}", "no")
            .replace("{ping}", "60");
        let url = on_origin(&session.origin, &expanded)?;
        let response = self
            .send(|| {
                self.http
                    .get(url.clone())
                    .header(reqwest::header::ACCEPT, "text/event-stream")
                    .timeout(STREAM_TIMEOUT)
            })
            .await?;
        Ok((response, session.account_id.clone()))
    }

    /// Sends the request built by `build` with the account's credential,
    /// through the gate so a rejected login pauses the account like IMAP's.
    async fn send(
        &self,
        build: impl Fn() -> RequestBuilder,
    ) -> Result<reqwest::Response, DomainError> {
        login_with(self.gate.as_ref(), |secret| {
            let request = match secret {
                LoginSecret::Password(password) => {
                    build().basic_auth(&self.username, Some(password))
                }
                LoginSecret::OAuthToken(token) => build().bearer_auth(token),
            };
            async move {
                let response = request.send().await.map_err(transport_error)?;
                match status_error(response.status()) {
                    Some(e) => Err(e),
                    None => Ok(response),
                }
            }
        })
        .await
    }
}

fn transport_error(e: reqwest::Error) -> DomainError {
    if e.is_timeout() {
        DomainError::ConnectionTimeout(e.to_string())
    } else {
        DomainError::ConnectionFailed(e.to_string())
    }
}

pub(super) fn status_error(status: StatusCode) -> Option<DomainError> {
    match status {
        s if s.is_success() => None,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Some(
            DomainError::AuthenticationFailed(format!("the JMAP server answered {status}")),
        ),
        StatusCode::TOO_MANY_REQUESTS => Some(DomainError::Throttled(format!(
            "the JMAP server answered {status}"
        ))),
        s if s.is_server_error() => Some(DomainError::ConnectionFailed(format!(
            "the JMAP server answered {status}"
        ))),
        s => Some(DomainError::Provider(format!(
            "the JMAP server answered {s}"
        ))),
    }
}
