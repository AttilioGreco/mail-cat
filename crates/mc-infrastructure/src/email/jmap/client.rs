//! JMAP transport: session discovery, authenticated method calls, blob download.

use std::sync::Arc;
use std::time::Duration;

use reqwest::{RequestBuilder, StatusCode};
use serde_json::{Value, json};
use tokio::sync::OnceCell;
use url::Url;

use mc_domain::error::DomainError;
use mc_domain::traits::login_gate::{LoginGate, LoginSecret, login_with};

const CORE: &str = "urn:ietf:params:jmap:core";
const MAIL: &str = "urn:ietf:params:jmap:mail";
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
    api_url: Url,
    download_template: String,
    origin: Url,
}

pub(super) struct JmapClient {
    http: reqwest::Client,
    session_url: Url,
    username: String,
    gate: Arc<dyn LoginGate>,
    session: OnceCell<Session>,
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
            session: OnceCell::new(),
        })
    }

    /// Fetches the session object; also the login test, as nothing else
    /// answers 401 for a wrong password before any mail is touched.
    pub async fn session(&self) -> Result<&Session, DomainError> {
        self.session
            .get_or_try_init(|| async {
                let body: Value = self
                    .send(|| self.http.get(self.session_url.clone()))
                    .await?
                    .json()
                    .await
                    .map_err(|e| DomainError::Provider(format!("JMAP session: {e}")))?;
                self.parse_session(&body)
            })
            .await
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
        let max_objects = body["capabilities"][CORE]["maxObjectsInGet"]
            .as_u64()
            .map_or(MAX_OBJECTS_CEILING, |n| n as usize)
            .clamp(1, MAX_OBJECTS_CEILING);
        Ok(Session {
            account_id,
            max_objects,
            api_url: on_origin(&self.session_url, text("apiUrl")?)?,
            download_template: text("downloadUrl")?.to_owned(),
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
        let method_calls: Vec<Value> = calls
            .into_iter()
            .enumerate()
            .map(|(i, (name, mut args))| {
                args["accountId"] = json!(session.account_id);
                json!([name, args, format!("c{i}")])
            })
            .collect();
        let count = method_calls.len();
        let request = json!({"using": [CORE, MAIL], "methodCalls": method_calls});
        let reply: Value = self
            .send(|| self.http.post(session.api_url.clone()).json(&request))
            .await?
            .json()
            .await
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
            results[index] = Some(if response[0] == "error" {
                Err(MethodError {
                    kind: args["type"].as_str().unwrap_or("unknown").to_owned(),
                    description: args["description"].as_str().unwrap_or_default().to_owned(),
                })
            } else {
                Ok(args)
            });
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
        Ok(bytes.to_vec())
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
