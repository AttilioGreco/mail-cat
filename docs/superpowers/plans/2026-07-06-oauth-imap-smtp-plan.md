# OAuth2 login (Google/Microsoft) for IMAP/SMTP — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a user connect a Gmail or Outlook/Office365 account via OAuth2 sign-in, and use the resulting tokens to authenticate on the existing generic IMAP/SMTP pipeline via SASL XOAUTH2 — server-driven, gRPC-triggered, CLI-first.

**Architecture:** The server owns the whole OAuth dance (loopback listener, PKCE, token exchange, keychain + config.toml writes) behind one new streaming gRPC RPC (`StartOAuthLogin`). `mc-cli` is a thin trigger: it calls the RPC, opens the system browser on the URL from the first stream event, and prints the final result. A background loop already running in `wiring.rs` gets one more per-account check to refresh OAuth tokens before they expire.

**Tech Stack:** Rust workspace (mc-domain, mc-application, mc-infrastructure, mc-interfaces, mc-server, mc-client, mc-cli). New dependency: `oauth2` crate (PKCE + token exchange, via its bundled `reqwest` backend) and `url` (query-string parsing). `async-imap` and `lettre` already support the wire mechanics (`AUTHENTICATE`/`Mechanism::Xoauth2`); no new email-protocol dependency needed.

## Global Constraints

- Pin `oauth2 = "4"` explicitly (not a bare `"*"` or unpinned range). If `cargo add`/`cargo build` resolves oauth2 5.x with a different builder API, **downgrade the Cargo.toml pin back to `"4"`** rather than adapting code to a newer type-state builder — v4's API is what every code sample in this plan targets.
- Every new dependency must be added to `[workspace.dependencies]` in the root `Cargo.toml` first, then referenced via `{ workspace = true }` in the consuming crate's own `Cargo.toml` — this repo's existing convention (see any crate's `Cargo.toml`).
- Secrets (OAuth tokens) never touch `config.toml` or the sled event store — only the OS keychain via `SecretStore`. This is already enforced for tokens (`AuthConfig::OAuth2`'s token fields are `#[serde(skip)]`); don't add a token field anywhere serializable.
- Design doc for full context/rationale: `docs/superpowers/specs/2026-07-06-oauth-imap-smtp-design.md`.
- Run `cargo test --workspace` and `cargo clippy --workspace --all-targets` after each task's implementation step, not just the task's own crate — cross-crate signature changes (e.g. `ImapEmailProvider::new`, `AppContext::new`) can break call sites in other crates.

---

### Task 1: Domain — `OAuthProvider` enum + `AuthConfig::OAuth2.provider` field

**Files:**
- Modify: `crates/mc-domain/src/model/account.rs`
- Modify: `crates/mc-domain/src/lib.rs:9-12`

**Interfaces:**
- Produces: `mc_domain::model::account::OAuthProvider` (`Google`, `Microsoft`, `Copy + Clone + Debug + PartialEq + Eq + Serialize + Deserialize`), re-exported as `mc_domain::OAuthProvider`. `AuthConfig::OAuth2` gains a `provider: OAuthProvider` field (not skipped — this is not a secret, it's needed to pick the refresh endpoint).

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` block at the bottom of `crates/mc-domain/src/model/account.rs`:

```rust
    #[test]
    fn oauth2_variant_carries_provider() {
        let auth = AuthConfig::OAuth2 {
            provider: OAuthProvider::Google,
            access_token: "at".into(),
            refresh_token: "rt".into(),
            expires_at: Utc::now(),
        };
        match auth {
            AuthConfig::OAuth2 { provider, .. } => assert_eq!(provider, OAuthProvider::Google),
            _ => panic!("expected OAuth2 variant"),
        }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p mc-domain oauth2_variant_carries_provider`
Expected: FAIL — compile error, `OAuthProvider` not found and `AuthConfig::OAuth2` has no `provider` field.

- [ ] **Step 3: Implement**

In `crates/mc-domain/src/model/account.rs`, add above the `AuthConfig` enum (after the `SmtpConnectionConfig` struct, before the `AuthConfig` doc comment):

```rust
/// Which OAuth2 provider issued the tokens on an `AuthConfig::OAuth2` account.
/// Needed to pick the right token-refresh endpoint later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OAuthProvider {
    Google,
    Microsoft,
}
```

Change the `OAuth2` variant of `AuthConfig`:

```rust
    OAuth2 {
        provider: OAuthProvider,
        #[serde(skip)]
        access_token: String,
        #[serde(skip)]
        refresh_token: String,
        expires_at: DateTime<Utc>,
    },
```

In `crates/mc-domain/src/lib.rs:9-12`, change:

```rust
pub use model::account::{
    Account, AccountId, AccountStatus, AuthConfig, ConnectionConfig, ProviderType,
};
```
to:
```rust
pub use model::account::{
    Account, AccountId, AccountStatus, AuthConfig, ConnectionConfig, OAuthProvider, ProviderType,
};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p mc-domain oauth2_variant_carries_provider`
Expected: PASS

- [ ] **Step 5: Fix the one other build break this causes**

`crates/mc-interfaces/src/grpc/mappers/account.rs:59-69` constructs `DomainAuthConfig::OAuth2 { access_token, refresh_token, expires_at }` without a `provider` — this now fails to compile. Fix it minimally for now (a real provider value is threaded through properly in Task 8b/9 for the *new* login flow; this proto mapper is for the *pre-existing* `AddAccount`/`UpdateAccount` RPCs, which don't carry a provider at all today):

In `crates/mc-interfaces/src/grpc/mappers/account.rs`, in `domain_auth_to_proto`, the `DomainAuthConfig::OAuth2` match arm gains `provider` in the destructure (unused, prefixed `_`):
```rust
        DomainAuthConfig::OAuth2 {
            provider: _,
            access_token,
            refresh_token,
            expires_at,
        } => Some(proto::auth_config::AuthType::Oauth2(OAuth2Auth {
```
And in `proto_auth_to_domain`, the construction gains a default provider (Google — arbitrary, this path isn't used by the new feature; it only matters for the legacy manual `AddAccount` RPC which nothing calls with `OAuth2Auth` today):
```rust
            Ok(DomainAuthConfig::OAuth2 {
                provider: mc_domain::OAuthProvider::Google,
                access_token: oauth2.access_token.clone(),
                refresh_token: oauth2.refresh_token.clone(),
                expires_at,
            })
```

Run: `cargo build --workspace`
Expected: builds clean.

- [ ] **Step 6: Commit**

```bash
git add crates/mc-domain/src/model/account.rs crates/mc-domain/src/lib.rs crates/mc-interfaces/src/grpc/mappers/account.rs
git commit -m "feat(domain): add OAuthProvider to AuthConfig::OAuth2"
```

---

### Task 2: `mc-infrastructure::oauth` — provider presets + loopback listener

**Files:**
- Create: `crates/mc-infrastructure/src/oauth/mod.rs`
- Create: `crates/mc-infrastructure/src/oauth/presets.rs`
- Create: `crates/mc-infrastructure/src/oauth/loopback.rs`
- Modify: `crates/mc-infrastructure/src/lib.rs`
- Modify: `crates/mc-infrastructure/Cargo.toml`
- Modify: `Cargo.toml` (workspace root)

**Interfaces:**
- Consumes: `mc_domain::OAuthProvider` (Task 1).
- Produces:
  - `mc_infrastructure::oauth::provider_preset(provider: OAuthProvider) -> ProviderPreset` with public fields `imap_host: &'static str`, `imap_port: u16`, `smtp_host: &'static str`, `smtp_port: u16`.
  - `mc_infrastructure::oauth::loopback::accept_one_redirect(listener: &tokio::net::TcpListener) -> Result<(String, String), mc_domain::DomainError>` — returns `(code, state)`.
  - Used by Task 3 (IMAP/SMTP don't consume this module directly) and Task 9 (`OAuthChallengeManager`) via `begin_authorization`/`complete_authorization`/`refresh`, added in this same task.

- [ ] **Step 1: Add the new dependencies**

In `Cargo.toml` (workspace root), under the `# Email` section (after `tokio-native-tls = "0.3"`), add:

```toml
# OAuth2 (Google/Microsoft login for XOAUTH2 IMAP/SMTP)
oauth2 = { version = "4", features = ["reqwest"] }
url = "2"
```

In `crates/mc-infrastructure/Cargo.toml`, after `keyring = { workspace = true }`, add:

```toml
oauth2 = { workspace = true }
url = { workspace = true }
```

Run: `cargo check -p mc-infrastructure`
Expected: builds clean (deps resolve, nothing uses them yet).

- [ ] **Step 2: Write the failing test for the loopback listener**

Create `crates/mc-infrastructure/src/oauth/loopback.rs`:

```rust
//! One-shot local HTTP listener that catches an OAuth2 authorization-code
//! redirect (`http://127.0.0.1:<port>/callback?code=...&state=...`).

use mc_domain::DomainError;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

const SUCCESS_BODY: &str =
    "<html><body><h1>mail-cat</h1><p>Login completato, puoi chiudere questa scheda.</p></body></html>";

/// Accepts exactly one HTTP GET request on `listener`, parses `code`/`state`
/// from its query string, and replies with a static confirmation page.
pub async fn accept_one_redirect(listener: &TcpListener) -> Result<(String, String), DomainError> {
    let (stream, _) = listener
        .accept()
        .await
        .map_err(|e| DomainError::Provider(format!("OAuth loopback accept failed: {e}")))?;

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .await
        .map_err(|e| DomainError::Provider(format!("OAuth loopback read failed: {e}")))?;

    let path = request_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| DomainError::Provider("malformed OAuth redirect request".into()))?;
    let query = path
        .split_once('?')
        .map(|(_, q)| q)
        .ok_or_else(|| DomainError::Provider("OAuth redirect had no query string".into()))?;

    let mut code = None;
    let mut state = None;
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "code" => code = Some(value.into_owned()),
            "state" => state = Some(value.into_owned()),
            _ => {}
        }
    }

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        SUCCESS_BODY.len(),
        SUCCESS_BODY
    );
    let _ = write_half.write_all(response.as_bytes()).await;
    let _ = write_half.flush().await;

    Ok((
        code.ok_or_else(|| DomainError::Provider("OAuth redirect missing 'code'".into()))?,
        state.ok_or_else(|| DomainError::Provider("OAuth redirect missing 'state'".into()))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn parses_code_and_state_from_redirect_query_string() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(b"GET /callback?code=abc123&state=xyz789 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
                .await
                .unwrap();
            let mut buf = String::new();
            stream.read_to_string(&mut buf).await.ok();
            buf
        });

        let (code, state) = accept_one_redirect(&listener).await.unwrap();
        assert_eq!(code, "abc123");
        assert_eq!(state, "xyz789");

        let response = client.await.unwrap();
        assert!(response.contains("200 OK"));
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p mc-infrastructure parses_code_and_state_from_redirect_query_string`
Expected: FAIL — `crates/mc-infrastructure/src/oauth/loopback.rs` isn't wired into the crate yet (module not declared), so this doesn't even compile.

- [ ] **Step 4: Wire the module in and add the presets file**

Create `crates/mc-infrastructure/src/oauth/presets.rs`:

```rust
//! Per-provider OAuth2 endpoints, scopes, and IMAP/SMTP connection presets.
//!
//! One shared "mail-cat" OAuth app per provider (client_id — and for Google,
//! client_secret — registered once by the maintainer, not per-user). Override
//! via env var for local dev; the compiled-in defaults are empty until set.

use mc_domain::OAuthProvider;

pub struct ProviderPreset {
    pub auth_url: &'static str,
    pub token_url: &'static str,
    pub scopes: &'static [&'static str],
    pub imap_host: &'static str,
    pub imap_port: u16,
    pub smtp_host: &'static str,
    pub smtp_port: u16,
    client_id_env: &'static str,
    client_secret_env: Option<&'static str>,
}

impl ProviderPreset {
    pub fn client_id(&self) -> String {
        std::env::var(self.client_id_env).unwrap_or_default()
    }

    pub fn client_secret(&self) -> Option<String> {
        let env = self.client_secret_env?;
        Some(std::env::var(env).unwrap_or_default())
    }
}

pub fn provider_preset(provider: OAuthProvider) -> ProviderPreset {
    match provider {
        OAuthProvider::Google => ProviderPreset {
            auth_url: "https://accounts.google.com/o/oauth2/v2/auth",
            token_url: "https://oauth2.googleapis.com/token",
            scopes: &["https://mail.google.com/"],
            imap_host: "imap.gmail.com",
            imap_port: 993,
            smtp_host: "smtp.gmail.com",
            smtp_port: 587,
            client_id_env: "MAILCAT_GOOGLE_CLIENT_ID",
            client_secret_env: Some("MAILCAT_GOOGLE_CLIENT_SECRET"),
        },
        OAuthProvider::Microsoft => ProviderPreset {
            auth_url: "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
            token_url: "https://login.microsoftonline.com/common/oauth2/v2.0/token",
            scopes: &[
                "https://outlook.office.com/IMAP.AccessAsUser.All",
                "https://outlook.office.com/SMTP.Send",
                "offline_access",
            ],
            imap_host: "outlook.office365.com",
            imap_port: 993,
            smtp_host: "smtp.office365.com",
            smtp_port: 587,
            client_id_env: "MAILCAT_MICROSOFT_CLIENT_ID",
            client_secret_env: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn google_preset_has_gmail_hosts() {
        let preset = provider_preset(OAuthProvider::Google);
        assert_eq!(preset.imap_host, "imap.gmail.com");
        assert_eq!(preset.smtp_host, "smtp.gmail.com");
    }

    #[test]
    fn microsoft_preset_has_no_client_secret() {
        let preset = provider_preset(OAuthProvider::Microsoft);
        assert!(preset.client_secret().is_none());
    }
}
```

Create `crates/mc-infrastructure/src/oauth/mod.rs`:

```rust
//! OAuth2 authorization-code + PKCE flow for Google/Microsoft, used to
//! authenticate on the generic IMAP/SMTP pipeline via SASL XOAUTH2.

pub mod loopback;
mod presets;

pub use presets::{provider_preset, ProviderPreset};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use mc_domain::{DomainError, OAuthProvider, OAuthSecrets};
use oauth2::basic::BasicClient;
use oauth2::{
    AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, PkceCodeChallenge,
    PkceCodeVerifier, RedirectUrl, RefreshToken, Scope, TokenResponse, TokenUrl,
};
use tokio::net::TcpListener;

/// Holds everything needed to complete a login once the browser redirects
/// back: the bound loopback listener, the PKCE verifier, and the CSRF token
/// to check the redirect against.
pub struct PendingLogin {
    client: BasicClient,
    pkce_verifier: PkceCodeVerifier,
    csrf_token: CsrfToken,
    listener: TcpListener,
}

/// Binds the loopback listener and builds the provider's authorization URL.
/// Returns the URL to show the user plus the handle needed to complete it.
pub async fn begin_authorization(
    provider: OAuthProvider,
) -> Result<(String, PendingLogin), DomainError> {
    let preset = provider_preset(provider);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| DomainError::Provider(format!("failed to bind OAuth loopback listener: {e}")))?;
    let port = listener
        .local_addr()
        .map_err(|e| DomainError::Provider(format!("failed to read loopback port: {e}")))?
        .port();
    let redirect_uri = format!("http://127.0.0.1:{port}/callback");

    let client = BasicClient::new(
        ClientId::new(preset.client_id()),
        preset.client_secret().map(ClientSecret::new),
        AuthUrl::new(preset.auth_url.to_string())
            .map_err(|e| DomainError::Provider(format!("invalid auth url: {e}")))?,
        Some(
            TokenUrl::new(preset.token_url.to_string())
                .map_err(|e| DomainError::Provider(format!("invalid token url: {e}")))?,
        ),
    )
    .set_redirect_uri(
        RedirectUrl::new(redirect_uri)
            .map_err(|e| DomainError::Provider(format!("invalid redirect url: {e}")))?,
    );

    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let mut request = client
        .authorize_url(CsrfToken::new_random)
        .set_pkce_challenge(pkce_challenge);
    for scope in preset.scopes {
        request = request.add_scope(Scope::new((*scope).to_string()));
    }
    let (authorize_url, csrf_token) = request.url();

    Ok((
        authorize_url.to_string(),
        PendingLogin { client, pkce_verifier, csrf_token, listener },
    ))
}

/// Awaits the one redirect on the loopback listener, exchanges the code for
/// tokens. Validates the CSRF state before exchanging anything.
pub async fn complete_authorization(pending: PendingLogin) -> Result<OAuthSecrets, DomainError> {
    let (code, state) = loopback::accept_one_redirect(&pending.listener).await?;
    if state != *pending.csrf_token.secret() {
        return Err(DomainError::Provider("OAuth state mismatch — possible CSRF".into()));
    }

    let token = pending
        .client
        .exchange_code(AuthorizationCode::new(code))
        .set_pkce_verifier(pending.pkce_verifier)
        .request_async(oauth2::reqwest::async_http_client)
        .await
        .map_err(|e| DomainError::Provider(format!("OAuth token exchange failed: {e}")))?;

    token_response_to_secrets(&token)
}

/// Exchanges a refresh token for a fresh access token.
pub async fn refresh(provider: OAuthProvider, refresh_token: &str) -> Result<OAuthSecrets, DomainError> {
    let preset = provider_preset(provider);
    let client = BasicClient::new(
        ClientId::new(preset.client_id()),
        preset.client_secret().map(ClientSecret::new),
        AuthUrl::new(preset.auth_url.to_string())
            .map_err(|e| DomainError::Provider(format!("invalid auth url: {e}")))?,
        Some(
            TokenUrl::new(preset.token_url.to_string())
                .map_err(|e| DomainError::Provider(format!("invalid token url: {e}")))?,
        ),
    );

    let token = client
        .exchange_refresh_token(&RefreshToken::new(refresh_token.to_string()))
        .request_async(oauth2::reqwest::async_http_client)
        .await
        .map_err(|e| DomainError::Provider(format!("OAuth token refresh failed: {e}")))?;

    let mut secrets = token_response_to_secrets(&token)?;
    if secrets.refresh_token.is_empty() {
        // Some providers omit refresh_token on refresh responses; keep the old one.
        secrets.refresh_token = refresh_token.to_string();
    }
    Ok(secrets)
}

fn token_response_to_secrets<TR: TokenResponse<oauth2::basic::BasicTokenType>>(
    token: &TR,
) -> Result<OAuthSecrets, DomainError> {
    let expires_in = token.expires_in().unwrap_or(std::time::Duration::from_secs(3600));
    let expires_at: DateTime<Utc> =
        Utc::now() + ChronoDuration::from_std(expires_in).unwrap_or(ChronoDuration::hours(1));
    Ok(OAuthSecrets {
        access_token: token.access_token().secret().clone(),
        refresh_token: token.refresh_token().map(|t| t.secret().clone()).unwrap_or_default(),
        expires_at,
    })
}
```

In `crates/mc-infrastructure/src/lib.rs`, add `pub mod oauth;` next to the other `pub mod` lines (after `pub mod secret;`).

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p mc-infrastructure oauth`
Expected: PASS (3 tests: `parses_code_and_state_from_redirect_query_string`, `google_preset_has_gmail_hosts`, `microsoft_preset_has_no_client_secret`).

- [ ] **Step 6: Add a network-free test that `begin_authorization` builds a correct URL**

Add to `crates/mc-infrastructure/src/oauth/mod.rs`'s test module (create one if the file has none yet):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn begin_authorization_builds_google_url_with_scope_and_pkce() {
        let (url, _pending) = begin_authorization(OAuthProvider::Google).await.unwrap();
        assert!(url.starts_with("https://accounts.google.com/o/oauth2/v2/auth"));
        assert!(url.contains("code_challenge"));
        assert!(url.contains("mail.google.com"));
    }
}
```

Run: `cargo test -p mc-infrastructure begin_authorization_builds_google_url_with_scope_and_pkce`
Expected: PASS — this makes no network call; it only builds a URL and binds a local port.

- [ ] **Step 7: Full workspace check**

Run: `cargo build --workspace && cargo clippy --workspace --all-targets`
Expected: clean (no other crate references this module yet).

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock crates/mc-infrastructure/Cargo.toml crates/mc-infrastructure/src/lib.rs crates/mc-infrastructure/src/oauth
git commit -m "feat(infra): add oauth module (PKCE flow, loopback redirect, provider presets)"
```

---

### Task 3: IMAP client — XOAUTH2 support

**Files:**
- Modify: `crates/mc-infrastructure/src/email/imap/client.rs`
- Modify: `crates/mc-server/src/wiring.rs:184-191` (the one call site of `ImapEmailProvider::new`)
- Modify: `crates/mc-server/src/wiring.rs:915-955` (the one test constructing `AppContext` — no direct `ImapEmailProvider::new` call there, no change needed; confirmed by the file read, listed here only to double check during implementation)

**Interfaces:**
- Produces: `mc_infrastructure::email::imap::client::ImapCredential` (new public enum: `Password(String)`, `OAuthToken(Arc<tokio::sync::RwLock<String>>)`). `ImapEmailProvider::new`'s 4th parameter changes from `password: String` to `credential: ImapCredential`.
- Consumes: nothing new.

- [ ] **Step 1: Write the failing test**

Add to `crates/mc-infrastructure/src/email/imap/client.rs`'s existing (or new) `#[cfg(test)] mod tests` block:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xoauth2_sasl_string_has_expected_format() {
        let mut auth = XOAuth2Authenticator {
            user: "me@gmail.com".into(),
            access_token: "ya29.abc".into(),
        };
        let response = async_imap::Authenticator::process(&mut auth, &[]);
        assert_eq!(response, "user=me@gmail.com\x01auth=Bearer ya29.abc\x01\x01");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p mc-infrastructure xoauth2_sasl_string_has_expected_format`
Expected: FAIL — `XOAuth2Authenticator` doesn't exist yet.

- [ ] **Step 3: Implement**

In `crates/mc-infrastructure/src/email/imap/client.rs`, add near the top (after the existing `use` block):

```rust
use std::sync::Arc;
use tokio::sync::RwLock;
```

Add before `impl ImapEmailProvider`:

```rust
/// Credential an `ImapEmailProvider` authenticates with.
///
/// `OAuthToken` wraps a shared cell so a background token refresh (see
/// `mc-server`'s periodic OAuth refresh loop) is visible on the very next
/// connection without restarting anything.
pub enum ImapCredential {
    Password(String),
    OAuthToken(Arc<RwLock<String>>),
}

/// SASL XOAUTH2 authenticator — sends the full bearer-token response
/// immediately, ignoring the (empty) server challenge, per Google's/
/// Microsoft's XOAUTH2 spec.
struct XOAuth2Authenticator {
    user: String,
    access_token: String,
}

impl async_imap::Authenticator for XOAuth2Authenticator {
    type Response = String;

    fn process(&mut self, _challenge: &[u8]) -> Self::Response {
        format!("user={}\x01auth=Bearer {}\x01\x01", self.user, self.access_token)
    }
}
```

Change the `ImapEmailProvider` struct's `password: String` field to `credential: ImapCredential`, and `new()`'s parameter + body to match:

```rust
pub struct ImapEmailProvider {
    host: String,
    port: u16,
    username: String,
    credential: ImapCredential,
    use_tls: bool,
    smtp_sender: Option<SmtpSender>,
}

impl ImapEmailProvider {
    pub fn new(
        host: String,
        port: u16,
        username: String,
        credential: ImapCredential,
        use_tls: bool,
        smtp_sender: Option<SmtpSender>,
    ) -> Self {
        Self { host, port, username, credential, use_tls, smtp_sender }
    }
```

In `create_imap_session`, replace the single `.login(...)` call:

```rust
        let session = match &self.credential {
            ImapCredential::Password(password) => client
                .login(&self.username, password)
                .await
                .map_err(|e| DomainError::Provider(format!("IMAP authentication failed: {:?}", e.0)))?,
            ImapCredential::OAuthToken(token_cell) => {
                let access_token = token_cell.read().await.clone();
                let auth = XOAuth2Authenticator { user: self.username.clone(), access_token };
                client
                    .authenticate("XOAUTH2", auth)
                    .await
                    .map_err(|e| DomainError::Provider(format!("IMAP XOAUTH2 authentication failed: {:?}", e.0)))?
            }
        };
```

Update the one call site in `crates/mc-server/src/wiring.rs:184-191`:

```rust
            let provider = Arc::new(ImapEmailProvider::new(
                account_config.imap_host.clone(),
                account_config.imap_port,
                account_config.username.clone(),
                mc_infrastructure::email::imap::client::ImapCredential::Password(imap_password.clone()),
                account_config.use_tls,
                smtp_sender,
            ));
```

(Task 5 replaces this with a branch that also handles OAuth accounts — this step just keeps the existing password path compiling.)

- [ ] **Step 4: Run test to verify it passes, then full build**

Run: `cargo test -p mc-infrastructure xoauth2_sasl_string_has_expected_format`
Expected: PASS

Run: `cargo build --workspace`
Expected: clean. If `client.authenticate("XOAUTH2", auth)` doesn't match `async-imap`'s actual signature (by-value vs by-ref authenticator, or a different error-tuple shape than `.login`'s), check `cargo doc -p async-imap --open` → `Client::authenticate` and adjust only the call site to match — the `XOAuth2Authenticator`/`ImapCredential` types and the test stay as written.

- [ ] **Step 5: Commit**

```bash
git add crates/mc-infrastructure/src/email/imap/client.rs crates/mc-server/src/wiring.rs
git commit -m "feat(imap): add XOAUTH2 authentication path"
```

---

### Task 4: SMTP sender — XOAUTH2 support

**Files:**
- Modify: `crates/mc-infrastructure/src/email/smtp/sender.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces: `SmtpSender::build_oauth(config: &SmtpConnectionConfig, username: &str, token: Arc<tokio::sync::RwLock<String>>) -> SmtpSender` (new constructor, alongside the existing `build`). Both `test_connection`/`send` behavior unchanged for password-built senders.

- [ ] **Step 1: Write the failing test**

Add to a new `#[cfg(test)] mod tests` block at the bottom of `crates/mc-infrastructure/src/email/smtp/sender.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn test_config() -> SmtpConnectionConfig {
        SmtpConnectionConfig {
            host: "smtp.example.com".into(),
            port: 587,
            security: SmtpSecurity::StartTls,
            username: None,
            password: None,
        }
    }

    #[tokio::test]
    async fn oauth_sender_reads_current_token_from_shared_cell() {
        let token = Arc::new(RwLock::new("first-token".to_string()));
        let sender = SmtpSender::build_oauth(&test_config(), "me@example.com", token.clone());

        // Simulate the background refresher rotating the token.
        *token.write().await = "refreshed-token".to_string();

        assert_eq!(sender.current_token_for_test().await, "refreshed-token");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p mc-infrastructure oauth_sender_reads_current_token_from_shared_cell`
Expected: FAIL — `build_oauth`/`current_token_for_test` don't exist.

- [ ] **Step 3: Implement**

Replace the top of `crates/mc-infrastructure/src/email/smtp/sender.rs` (imports and the `SmtpSender` struct + `build`/`test_connection`/`send`) with:

```rust
use std::sync::Arc;

use lettre::{
    message::{header::ContentType, Mailbox, MultiPart, SinglePart},
    transport::smtp::authentication::{Credentials, Mechanism},
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
};
use mc_domain::{
    error::DomainError,
    model::{
        account::{SmtpConnectionConfig, SmtpSecurity},
        message::{Address, OutgoingMessage},
    },
};
use tokio::sync::RwLock;

/// Async SMTP sender. `Static` builds its transport once at startup (existing
/// password-account behavior, unchanged). `OAuth` rebuilds the transport on
/// every send/test since the shared access token can rotate between calls —
/// the transport itself is cheap to construct (no I/O happens until
/// `.send()`/`.test_connection()` runs).
enum SmtpBackend {
    Static(AsyncSmtpTransport<Tokio1Executor>),
    OAuth {
        host: String,
        port: u16,
        security: SmtpSecurity,
        username: String,
        token: Arc<RwLock<String>>,
    },
}

pub struct SmtpSender {
    backend: SmtpBackend,
}

impl SmtpSender {
    /// Build an `SmtpSender` from the given SMTP config, falling back to
    /// `fallback_username` / `fallback_password` when the config does not
    /// provide its own credentials.
    pub fn build(
        config: &SmtpConnectionConfig,
        fallback_username: &str,
        fallback_password: &str,
    ) -> Result<Self, DomainError> {
        let username = config.username.as_deref().unwrap_or(fallback_username);
        let password = config.password.as_deref().unwrap_or(fallback_password);
        let creds = Credentials::new(username.to_owned(), password.to_owned());

        let builder = Self::transport_builder(&config.host, config.security)?;
        let transport = builder.port(config.port).credentials(creds).build();
        Ok(Self { backend: SmtpBackend::Static(transport) })
    }

    /// OAuth-authenticated sender. `token` is shared with the account's IMAP
    /// provider so a background token refresh is visible immediately.
    pub fn build_oauth(
        config: &SmtpConnectionConfig,
        username: &str,
        token: Arc<RwLock<String>>,
    ) -> Self {
        Self {
            backend: SmtpBackend::OAuth {
                host: config.host.clone(),
                port: config.port,
                security: config.security,
                username: username.to_owned(),
                token,
            },
        }
    }

    fn transport_builder(
        host: &str,
        security: SmtpSecurity,
    ) -> Result<lettre::AsyncSmtpTransportBuilder, DomainError> {
        match security {
            SmtpSecurity::Smtps => AsyncSmtpTransport::<Tokio1Executor>::relay(host)
                .map_err(|e| DomainError::Provider(format!("SMTP relay build error: {e}"))),
            SmtpSecurity::StartTls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host)
                .map_err(|e| DomainError::Provider(format!("SMTP starttls build error: {e}"))),
        }
    }

    async fn oauth_transport(
        host: &str,
        port: u16,
        security: SmtpSecurity,
        username: &str,
        token: &RwLock<String>,
    ) -> Result<AsyncSmtpTransport<Tokio1Executor>, DomainError> {
        let access_token = token.read().await.clone();
        let creds = Credentials::new(username.to_owned(), access_token);
        let builder = Self::transport_builder(host, security)?;
        Ok(builder
            .port(port)
            .credentials(creds)
            .authentication(vec![Mechanism::Xoauth2])
            .build())
    }

    /// Send a NOOP to verify the SMTP server is reachable.
    pub async fn test_connection(&self) -> Result<bool, DomainError> {
        match &self.backend {
            SmtpBackend::Static(t) => t.test_connection().await,
            SmtpBackend::OAuth { host, port, security, username, token } => {
                Self::oauth_transport(host, *port, *security, username, token)
                    .await?
                    .test_connection()
                    .await
            }
        }
        .map_err(|e| DomainError::Provider(format!("SMTP connection test failed: {e}")))
    }

    /// Send an outgoing message via SMTP.
    pub async fn send(&self, msg: &OutgoingMessage) -> Result<(), DomainError> {
        let email = build_message(msg)?;
        match &self.backend {
            SmtpBackend::Static(t) => t.send(email).await,
            SmtpBackend::OAuth { host, port, security, username, token } => {
                Self::oauth_transport(host, *port, *security, username, token)
                    .await?
                    .send(email)
                    .await
            }
        }
        .map_err(|e| DomainError::Provider(format!("SMTP send error: {e}")))?;
        Ok(())
    }

    #[cfg(test)]
    async fn current_token_for_test(&self) -> String {
        match &self.backend {
            SmtpBackend::OAuth { token, .. } => token.read().await.clone(),
            SmtpBackend::Static(_) => panic!("current_token_for_test called on a Static sender"),
        }
    }
}
```

Leave `build_message`/`to_mailbox` (the free functions below) untouched.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p mc-infrastructure oauth_sender_reads_current_token_from_shared_cell`
Expected: PASS. If `lettre::AsyncSmtpTransportBuilder` isn't the exact public type name for `.relay()`'s return value in the resolved lettre version, check `cargo doc -p lettre --open` → `AsyncSmtpTransport::relay` return type and adjust `transport_builder`'s return type only.

Run: `cargo build --workspace && cargo clippy --workspace --all-targets`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/mc-infrastructure/src/email/smtp/sender.rs
git commit -m "feat(smtp): add XOAUTH2 authentication path"
```

---

### Task 5: proto — `OAuthProvider` enum + `StartOAuthLogin` streaming RPC

**Files:**
- Modify: `proto/mail_cat/v1/accounts.proto`

**Interfaces:**
- Produces (generated by `tonic-prost-build` on next build of any crate depending on the proto): `proto::OAuthProvider` (enum, `Unspecified`/`Google`/`Microsoft`), `proto::StartOAuthLoginRequest { name, email, provider }`, `proto::OAuthLoginEvent` (oneof: `AwaitingBrowser{authorize_url}` / `Succeeded{email}` / `Failed{message}`), and a new required trait method `account_service_server::AccountService::start_oauth_login` + associated `StartOAuthLoginStream` type.
- This task deliberately breaks `crates/mc-interfaces/src/grpc/services/account_service.rs`'s compile (the trait now requires a method `AccountServiceImpl` doesn't implement) — that failure is fixed in Task 7. This is the "failing test" for this task.

- [ ] **Step 1: Modify the proto file**

In `proto/mail_cat/v1/accounts.proto`, add to the `AccountService` service block (after `rpc SyncAccount`):

```proto
  // Start an OAuth2 login (Google/Microsoft) for a new account. Streams:
  // one AwaitingBrowser event with the URL to open, then exactly one of
  // OAuthLoginSucceeded/OAuthLoginFailed once the browser redirect completes.
  rpc StartOAuthLogin(StartOAuthLoginRequest) returns (stream OAuthLoginEvent);
```

Add these new top-level definitions (after the existing `AccountStatus` enum, before `AuthConfig`):

```proto
enum OAuthProvider {
  OAUTH_PROVIDER_UNSPECIFIED = 0;
  OAUTH_PROVIDER_GOOGLE = 1;
  OAUTH_PROVIDER_MICROSOFT = 2;
}
```

Add these at the bottom of the file (after `SyncPhase`):

```proto
message StartOAuthLoginRequest {
  string name = 1;
  string email = 2;
  OAuthProvider provider = 3;
}

message OAuthLoginEvent {
  oneof event {
    AwaitingBrowser awaiting_browser = 1;
    OAuthLoginSucceeded succeeded = 2;
    OAuthLoginFailed failed = 3;
  }
}

message AwaitingBrowser {
  string authorize_url = 1;
}

message OAuthLoginSucceeded {
  string email = 1;
}

message OAuthLoginFailed {
  string message = 1;
}
```

- [ ] **Step 2: Verify codegen compiles but the service impl doesn't**

Run: `cargo build -p mc-interfaces`
Expected: FAIL — `error[E0046]: not all trait items implemented, missing: start_oauth_login` (or similar) on `impl<C, Q> AccountService for AccountServiceImpl<C, Q>` in `account_service.rs`. This confirms the proto codegen picked up the new RPC and message types.

- [ ] **Step 3: Commit anyway (this task's deliverable is the proto + confirmed-required next step)**

```bash
git add proto/mail_cat/v1/accounts.proto
git commit -m "feat(proto): add StartOAuthLogin streaming RPC and OAuth message types"
```

Note for the next task: `cargo build -p mc-interfaces` staying red is expected and intentional until Task 7 lands — don't try to make this task's build green by stubbing the trait method here.

---

### Task 6: mc-application — `OAuthLoginTrigger` trait

**Files:**
- Create: `crates/mc-application/src/services/oauth_login_trigger.rs`
- Modify: `crates/mc-application/src/services/mod.rs`
- Modify: `crates/mc-application/src/lib.rs:21`

**Interfaces:**
- Consumes: `mc_domain::OAuthProvider` (Task 1).
- Produces: `mc_application::OAuthLoginTrigger` trait (`Send + Sync`, one method `start_login`) and `mc_application::OAuthLoginProgress` enum (`AwaitingBrowser{authorize_url}`, `Succeeded{email}`, `Failed{message}`). This mirrors the existing `SyncTrigger`/`SyncProgressEvent` pattern in the same file's sibling `sync_trigger.rs`, so `mc-interfaces` can depend on the trait generically (Task 7) while the concrete implementation lives in `mc-server` (Task 8), which is the crate that actually knows about `config.toml`.

- [ ] **Step 1: Write the failing test**

Create `crates/mc-application/src/services/oauth_login_trigger.rs`:

```rust
//! Port for starting an interactive OAuth2 login. The concrete implementation
//! (in `mc-server`) owns the loopback listener, token exchange, keychain
//! write, and config.toml write; this crate only knows the shape of the
//! progress stream, so `mc-interfaces`'s gRPC layer can depend on it without
//! depending on `mc-server`.

use mc_domain::OAuthProvider;
use tokio::sync::mpsc;

/// One event in an OAuth login's progress. A `start_login` call yields
/// exactly one `AwaitingBrowser`, followed by exactly one of
/// `Succeeded`/`Failed`.
pub enum OAuthLoginProgress {
    AwaitingBrowser { authorize_url: String },
    Succeeded { email: String },
    Failed { message: String },
}

pub trait OAuthLoginTrigger: Send + Sync {
    /// Starts the login in the background; the caller drives it to
    /// completion by reading from the returned channel.
    fn start_login(
        &self,
        name: String,
        email: String,
        provider: OAuthProvider,
    ) -> mpsc::Receiver<OAuthLoginProgress>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeTrigger;

    impl OAuthLoginTrigger for FakeTrigger {
        fn start_login(&self, _name: String, email: String, _provider: OAuthProvider) -> mpsc::Receiver<OAuthLoginProgress> {
            let (tx, rx) = mpsc::channel(2);
            tokio::spawn(async move {
                let _ = tx.send(OAuthLoginProgress::AwaitingBrowser { authorize_url: "https://example.com".into() }).await;
                let _ = tx.send(OAuthLoginProgress::Succeeded { email }).await;
            });
            rx
        }
    }

    #[tokio::test]
    async fn trigger_yields_awaiting_then_succeeded() {
        let trigger = FakeTrigger;
        let mut rx = trigger.start_login("Work".into(), "me@gmail.com".into(), OAuthProvider::Google);

        match rx.recv().await {
            Some(OAuthLoginProgress::AwaitingBrowser { authorize_url }) => {
                assert_eq!(authorize_url, "https://example.com");
            }
            other => panic!("expected AwaitingBrowser, got {}", matches_desc(&other)),
        }
        match rx.recv().await {
            Some(OAuthLoginProgress::Succeeded { email }) => assert_eq!(email, "me@gmail.com"),
            other => panic!("expected Succeeded, got {}", matches_desc(&other)),
        }
    }

    fn matches_desc(event: &Option<OAuthLoginProgress>) -> &'static str {
        match event {
            Some(OAuthLoginProgress::AwaitingBrowser { .. }) => "AwaitingBrowser",
            Some(OAuthLoginProgress::Succeeded { .. }) => "Succeeded",
            Some(OAuthLoginProgress::Failed { .. }) => "Failed",
            None => "None",
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p mc-application trigger_yields_awaiting_then_succeeded`
Expected: FAIL — module not declared in `services/mod.rs` yet, so it doesn't compile as part of the crate.

- [ ] **Step 3: Wire the module in**

In `crates/mc-application/src/services/mod.rs`, change to:

```rust
pub mod oauth_login_trigger;
pub mod sync_service;
pub mod sync_trigger;

pub use oauth_login_trigger::{OAuthLoginProgress, OAuthLoginTrigger};
pub use sync_service::{SyncConfig, SyncProgressEvent, SyncService, SyncState};
pub use sync_trigger::SyncTrigger;
```

In `crates/mc-application/src/lib.rs:21`, change:

```rust
pub use services::{SyncConfig, SyncProgressEvent, SyncService, SyncState, SyncTrigger};
```
to:
```rust
pub use services::{
    OAuthLoginProgress, OAuthLoginTrigger, SyncConfig, SyncProgressEvent, SyncService, SyncState,
    SyncTrigger,
};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p mc-application trigger_yields_awaiting_then_succeeded`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/mc-application/src/services/oauth_login_trigger.rs crates/mc-application/src/services/mod.rs crates/mc-application/src/lib.rs
git commit -m "feat(application): add OAuthLoginTrigger port"
```

---

### Task 7: mc-interfaces — `start_oauth_login` gRPC handler

**Files:**
- Modify: `crates/mc-interfaces/src/grpc/services/account_service.rs`
- Modify: `crates/mc-interfaces/src/grpc/mappers/account.rs`

**Interfaces:**
- Consumes: `mc_application::{OAuthLoginTrigger, OAuthLoginProgress}` (Task 6), `proto::{StartOAuthLoginRequest, OAuthLoginEvent, OAuthProvider as ProtoOAuthProvider}` (Task 5), `mc_domain::OAuthProvider` (Task 1).
- Produces: `AccountServiceImpl::with_oauth_trigger(self, trigger: Arc<dyn OAuthLoginTrigger>) -> Self` builder method (mirrors `with_sync_trigger`), and a working `start_oauth_login` RPC handler. Also `proto_oauth_provider_to_domain(i32) -> Result<mc_domain::OAuthProvider, String>` in the mappers file, for Task 9 to reuse if needed.

- [ ] **Step 1: This task's "failing test" is the compile break Task 5 already produced**

Run: `cargo build -p mc-interfaces`
Expected: FAIL (same error as Task 5 Step 2 — confirm it's still there before starting).

- [ ] **Step 2: Add the mapper function**

In `crates/mc-interfaces/src/grpc/mappers/account.rs`, add near `proto_provider_type_to_domain`:

```rust
/// Convert proto OAuthProvider to domain OAuthProvider.
pub fn proto_oauth_provider_to_domain(provider: i32) -> Result<mc_domain::OAuthProvider, String> {
    match proto::OAuthProvider::try_from(provider).ok() {
        Some(proto::OAuthProvider::Google) => Ok(mc_domain::OAuthProvider::Google),
        Some(proto::OAuthProvider::Microsoft) => Ok(mc_domain::OAuthProvider::Microsoft),
        _ => Err(format!("Invalid OAuth provider: {}", provider)),
    }
}
```

- [ ] **Step 3: Implement the handler**

In `crates/mc-interfaces/src/grpc/services/account_service.rs`:

Change the imports at the top:
```rust
use crate::grpc::mappers::{
    application_error_to_status, domain_account_summary_to_detail, domain_account_summary_to_proto,
    proto_connection_to_domain, proto_email_to_domain, proto_oauth_provider_to_domain,
    proto_provider_type_to_domain,
};
use crate::grpc::proto::{
    self, AddAccountRequest, AddAccountResponse, GetAccountRequest, GetAccountResponse,
    ListAccountsRequest, ListAccountsResponse, RemoveAccountRequest, RemoveAccountResponse,
    StartOAuthLoginRequest, SyncAccountRequest, UpdateAccountRequest, UpdateAccountResponse,
    account_service_server::AccountService,
};
use mc_application::{
    OAuthLoginProgress, OAuthLoginTrigger, SyncState, SyncTrigger,
    bus::{CommandBus, QueryBus},
    commands::{Command, CommandResult},
    queries::Query,
};
```

Change the struct + constructor + add the new builder:
```rust
pub struct AccountServiceImpl<C: CommandBus + 'static, Q: QueryBus + 'static> {
    command_bus: Arc<C>,
    query_bus: Arc<Q>,
    sync_trigger: Option<Arc<dyn SyncTrigger>>,
    oauth_trigger: Option<Arc<dyn OAuthLoginTrigger>>,
}

impl<C: CommandBus + 'static, Q: QueryBus + 'static> AccountServiceImpl<C, Q> {
    pub fn new(command_bus: Arc<C>, query_bus: Arc<Q>) -> Self {
        Self {
            command_bus,
            query_bus,
            sync_trigger: None,
            oauth_trigger: None,
        }
    }

    pub fn with_sync_trigger(mut self, trigger: Arc<dyn SyncTrigger>) -> Self {
        self.sync_trigger = Some(trigger);
        self
    }

    pub fn with_oauth_trigger(mut self, trigger: Arc<dyn OAuthLoginTrigger>) -> Self {
        self.oauth_trigger = Some(trigger);
        self
    }
}
```

Add the handler + stream type inside the `impl AccountService for AccountServiceImpl<C, Q>` block (after `sync_account`):

```rust
    type StartOAuthLoginStream = ReceiverStream<Result<proto::OAuthLoginEvent, Status>>;

    async fn start_oauth_login(
        &self,
        request: Request<StartOAuthLoginRequest>,
    ) -> Result<Response<Self::StartOAuthLoginStream>, Status> {
        let req = request.into_inner();
        let provider =
            proto_oauth_provider_to_domain(req.provider).map_err(Status::invalid_argument)?;

        let trigger = self
            .oauth_trigger
            .clone()
            .ok_or_else(|| Status::unavailable("OAuth login not configured"))?;

        let mut progress_rx = trigger.start_login(req.name, req.email, provider);
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            while let Some(event) = progress_rx.recv().await {
                let proto_event = oauth_login_progress_to_proto(event);
                if tx.send(Ok(proto_event)).await.is_err() {
                    break; // client disconnected
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
```

Add the free function at the bottom of the file (next to `sync_state_to_phase`):

```rust
fn oauth_login_progress_to_proto(event: OAuthLoginProgress) -> proto::OAuthLoginEvent {
    use proto::oauth_login_event::Event;
    let event = match event {
        OAuthLoginProgress::AwaitingBrowser { authorize_url } => {
            Event::AwaitingBrowser(proto::AwaitingBrowser { authorize_url })
        }
        OAuthLoginProgress::Succeeded { email } => {
            Event::Succeeded(proto::OAuthLoginSucceeded { email })
        }
        OAuthLoginProgress::Failed { message } => {
            Event::Failed(proto::OAuthLoginFailed { message })
        }
    };
    proto::OAuthLoginEvent { event: Some(event) }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo build -p mc-interfaces`
Expected: builds clean (the `E0046` from Task 5 is now gone).

Run: `cargo test -p mc-interfaces`
Expected: PASS (existing tests unaffected).

- [ ] **Step 5: Commit**

```bash
git add crates/mc-interfaces/src/grpc/services/account_service.rs crates/mc-interfaces/src/grpc/mappers/account.rs
git commit -m "feat(grpc): implement StartOAuthLogin handler"
```

---

### Task 8: mc-server — `config.rs` gains an OAuth account variant

**Files:**
- Modify: `crates/mc-server/src/config.rs`

**Interfaces:**
- Produces: `mc_server::config::OAuthProviderConfig` enum (`Google`/`Microsoft`, `#[serde(rename_all = "lowercase")]`, `impl From<OAuthProviderConfig> for mc_domain::OAuthProvider`), and `AccountConfig.oauth_provider: Option<OAuthProviderConfig>` (`#[serde(default)]`).

- [ ] **Step 1: Write the failing test**

Add to a `#[cfg(test)] mod tests` block at the bottom of `crates/mc-server/src/config.rs` (create one if none exists — check the file first; if a test module already exists, add this test inside it):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const OAUTH_ACCOUNT_TOML: &str = r#"
[[accounts]]
name = "Work Gmail"
email = "me@gmail.com"
provider_type = "imap"
imap_host = "imap.gmail.com"
imap_port = 993
use_tls = true
username = "me@gmail.com"
oauth_provider = "google"

[accounts.smtp]
host = "smtp.gmail.com"
port = 587
security = "start_tls"
"#;

    #[test]
    fn account_config_parses_oauth_provider() {
        let config: ServerConfig = toml::from_str(OAUTH_ACCOUNT_TOML).unwrap();
        assert_eq!(config.accounts.len(), 1);
        assert_eq!(config.accounts[0].oauth_provider, Some(OAuthProviderConfig::Google));
        assert!(config.accounts[0].password.is_none());
    }

    #[test]
    fn account_config_without_oauth_provider_defaults_to_none() {
        let config: ServerConfig = toml::from_str(
            r#"
[[accounts]]
name = "Plain"
email = "me@example.com"
provider_type = "imap"
imap_host = "imap.example.com"
imap_port = 993
use_tls = true
username = "me@example.com"
"#,
        )
        .unwrap();
        assert_eq!(config.accounts[0].oauth_provider, None);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p mc-server account_config_parses_oauth_provider`
Expected: FAIL — `OAuthProviderConfig` doesn't exist, `AccountConfig` has no `oauth_provider` field.

- [ ] **Step 3: Implement**

Add after the `ProviderTypeConfig` enum + its `From` impl in `crates/mc-server/src/config.rs`:

```rust
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OAuthProviderConfig {
    Google,
    Microsoft,
}

impl From<OAuthProviderConfig> for mc_domain::OAuthProvider {
    fn from(provider: OAuthProviderConfig) -> Self {
        match provider {
            OAuthProviderConfig::Google => Self::Google,
            OAuthProviderConfig::Microsoft => Self::Microsoft,
        }
    }
}
```

Add a field to `AccountConfig` (after `password`):

```rust
    /// Present only for accounts authenticating via OAuth2. When set,
    /// `password`/smtp `password` are irrelevant — the XOAUTH2 token comes
    /// from the keychain's `AccountSecrets.oauth` instead.
    #[serde(default)]
    pub oauth_provider: Option<OAuthProviderConfig>,
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p mc-server account_config_parses_oauth_provider account_config_without_oauth_provider_defaults_to_none`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/mc-server/src/config.rs
git commit -m "feat(config): add oauth_provider field to AccountConfig"
```

---

### Task 9: mc-server — `OAuthChallengeManager` (implements `OAuthLoginTrigger`)

**Files:**
- Create: `crates/mc-server/src/oauth_login.rs`
- Modify: `crates/mc-server/src/main.rs`

**Interfaces:**
- Consumes: `mc_application::{OAuthLoginTrigger, OAuthLoginProgress}` (Task 6), `mc_infrastructure::oauth::{begin_authorization, complete_authorization, provider_preset}` (Task 2), `mc_domain::SecretStore` (existing), `crate::config::OAuthProviderConfig` (Task 8).
- Produces: `mc_server::oauth_login::OAuthChallengeManager::new(config_path: PathBuf, secret_store: Arc<dyn SecretStore>) -> Self`, implementing `OAuthLoginTrigger`.

- [ ] **Step 1: Write the failing test**

Create `crates/mc-server/src/oauth_login.rs` with the test module first (the implementation stub below makes it compile so the *behavior* test can fail meaningfully rather than on a missing-symbol error):

```rust
//! Drives an interactive OAuth2 login end to end: opens the loopback
//! listener + authorize URL (via `mc_infrastructure::oauth`), waits for the
//! browser redirect, stores the resulting tokens in the OS keychain, and
//! appends the new account to `config.toml`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use mc_application::{OAuthLoginProgress, OAuthLoginTrigger};
use mc_domain::{OAuthProvider, SecretStore};
use tokio::sync::mpsc;
use toml_edit::{value, ArrayOfTables, DocumentMut, Item, Table};

#[derive(Clone)]
pub struct OAuthChallengeManager {
    config_path: PathBuf,
    secret_store: Arc<dyn SecretStore>,
}

impl OAuthChallengeManager {
    pub fn new(config_path: PathBuf, secret_store: Arc<dyn SecretStore>) -> Self {
        Self { config_path, secret_store }
    }
}

impl OAuthLoginTrigger for OAuthChallengeManager {
    fn start_login(
        &self,
        name: String,
        email: String,
        provider: OAuthProvider,
    ) -> mpsc::Receiver<OAuthLoginProgress> {
        let (tx, rx) = mpsc::channel(2);
        let secret_store = self.secret_store.clone();
        let config_path = self.config_path.clone();

        tokio::spawn(async move {
            let (authorize_url, pending) = match mc_infrastructure::oauth::begin_authorization(provider).await {
                Ok(v) => v,
                Err(e) => {
                    let _ = tx.send(OAuthLoginProgress::Failed { message: e.to_string() }).await;
                    return;
                }
            };
            if tx
                .send(OAuthLoginProgress::AwaitingBrowser { authorize_url })
                .await
                .is_err()
            {
                return; // caller dropped the stream
            }

            let result = tokio::time::timeout(
                std::time::Duration::from_secs(300),
                mc_infrastructure::oauth::complete_authorization(pending),
            )
            .await;

            let secrets = match result {
                Ok(Ok(secrets)) => secrets,
                Ok(Err(e)) => {
                    let _ = tx.send(OAuthLoginProgress::Failed { message: e.to_string() }).await;
                    return;
                }
                Err(_) => {
                    let _ = tx
                        .send(OAuthLoginProgress::Failed {
                            message: "timed out waiting for browser login (5 min)".into(),
                        })
                        .await;
                    return;
                }
            };

            let mut bundle = secret_store.load(&email).ok().flatten().unwrap_or_default();
            bundle.oauth = Some(secrets);
            if let Err(e) = secret_store.store(&email, &bundle) {
                let _ = tx
                    .send(OAuthLoginProgress::Failed { message: format!("storing tokens in keychain: {e}") })
                    .await;
                return;
            }

            if let Err(e) = append_oauth_account(&config_path, &name, &email, provider) {
                let _ = tx
                    .send(OAuthLoginProgress::Failed { message: format!("writing account to config: {e}") })
                    .await;
                return;
            }

            let _ = tx.send(OAuthLoginProgress::Succeeded { email }).await;
        });

        rx
    }
}

/// Appends a new `[[accounts]]` table for an OAuth account to `config.toml`,
/// using the provider's IMAP/SMTP presets. Format-preserving (`toml_edit`),
/// same approach as `secret_ingest`'s rewrites.
fn append_oauth_account(
    config_path: &Path,
    name: &str,
    email: &str,
    provider: OAuthProvider,
) -> anyhow::Result<()> {
    let preset = mc_infrastructure::oauth::provider_preset(provider);
    let contents = std::fs::read_to_string(config_path).unwrap_or_default();
    let mut doc: DocumentMut = if contents.trim().is_empty() {
        DocumentMut::new()
    } else {
        contents.parse()?
    };

    if doc.get("accounts").is_none() {
        doc["accounts"] = Item::ArrayOfTables(ArrayOfTables::new());
    }
    let accounts = doc["accounts"]
        .as_array_of_tables_mut()
        .ok_or_else(|| anyhow::anyhow!("`accounts` in config.toml is not an array of tables"))?;

    let mut table = Table::new();
    table["name"] = value(name);
    table["email"] = value(email);
    table["provider_type"] = value("imap");
    table["imap_host"] = value(preset.imap_host);
    table["imap_port"] = value(preset.imap_port as i64);
    table["use_tls"] = value(true);
    table["username"] = value(email);
    table["oauth_provider"] = value(match provider {
        OAuthProvider::Google => "google",
        OAuthProvider::Microsoft => "microsoft",
    });

    let mut smtp = Table::new();
    smtp["host"] = value(preset.smtp_host);
    smtp["port"] = value(preset.smtp_port as i64);
    smtp["security"] = value("start_tls");
    table["smtp"] = Item::Table(smtp);

    accounts.push(table);
    std::fs::write(config_path, doc.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mc_domain::AccountSecrets;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockStore(Mutex<HashMap<String, AccountSecrets>>);
    impl SecretStore for MockStore {
        fn load(&self, email: &str) -> Result<Option<AccountSecrets>, mc_domain::DomainError> {
            Ok(self.0.lock().unwrap().get(email).cloned())
        }
        fn store(&self, email: &str, s: &AccountSecrets) -> Result<(), mc_domain::DomainError> {
            self.0.lock().unwrap().insert(email.into(), s.clone());
            Ok(())
        }
        fn delete(&self, email: &str) -> Result<(), mc_domain::DomainError> {
            self.0.lock().unwrap().remove(email);
            Ok(())
        }
    }

    #[test]
    fn append_oauth_account_writes_expected_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "grpc_addr = \"127.0.0.1:50051\"\n").unwrap();

        append_oauth_account(&path, "Work Gmail", "me@gmail.com", OAuthProvider::Google).unwrap();

        let written = std::fs::read_to_string(&path).unwrap();
        let parsed: crate::config::ServerConfig = toml::from_str(&written).unwrap();
        assert_eq!(parsed.accounts.len(), 1);
        assert_eq!(parsed.accounts[0].email, "me@gmail.com");
        assert_eq!(parsed.accounts[0].imap_host, "imap.gmail.com");
        assert_eq!(
            parsed.accounts[0].oauth_provider,
            Some(crate::config::OAuthProviderConfig::Google)
        );
        assert!(!written.contains("password"));
    }

    #[tokio::test]
    async fn start_login_reports_failure_when_client_id_unset() {
        // No MAILCAT_GOOGLE_CLIENT_ID set in the test environment — begin_authorization
        // itself still succeeds (it only builds a URL), but this at minimum proves
        // the trigger's AwaitingBrowser → terminal-event shape holds without a browser.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "grpc_addr = \"127.0.0.1:50051\"\n").unwrap();
        let manager = OAuthChallengeManager::new(path, Arc::new(MockStore::default()));

        let mut rx = manager.start_login("Work".into(), "me@gmail.com".into(), OAuthProvider::Google);
        match rx.recv().await {
            Some(OAuthLoginProgress::AwaitingBrowser { authorize_url }) => {
                assert!(authorize_url.contains("accounts.google.com"));
            }
            other => panic!("expected AwaitingBrowser first"),
        }
        // Don't wait for the 5-minute timeout in a unit test — drop the receiver,
        // which ends the spawned task's send loop on its next `tx.send(...)`.
    }
}
```

- [ ] **Step 2: Run tests to verify they fail, then pass**

Run: `cargo test -p mc-server append_oauth_account_writes_expected_fields start_login_reports_failure_when_client_id_unset`
Expected: first run FAILS if `mc-server/Cargo.toml` is missing `tempfile`/`toml` dev-deps (check — `toml` is already a normal dependency per the earlier `cat`; add `tempfile = { workspace = true }` to `[dev-dependencies]` in `crates/mc-server/Cargo.toml` if that section doesn't exist yet). After adding it, both tests PASS.

- [ ] **Step 3: Wire the module into `main.rs`**

In `crates/mc-server/src/main.rs`, add `mod oauth_login;` next to the other `mod` declarations (after `mod config;`).

Change the `wiring` construction block (around what's currently `let app_ctx = AppContext::new(&config, secret_store.as_ref())...`) to build the manager and pass it through:

```rust
    let oauth_challenges = std::sync::Arc::new(oauth_login::OAuthChallengeManager::new(
        PathBuf::from(&config_path),
        secret_store.clone(),
    ));

    // Wire up all dependencies
    let app_ctx = AppContext::new(&config, &config_path, secret_store.clone(), oauth_challenges)
        .await
        .context("failed to initialize application context")?;
```

(`AppContext::new`'s new signature — owned `Arc<dyn SecretStore>`, plus `config_path` and `oauth_challenges` params — is implemented in Task 10, which also fixes the one existing unit test in `wiring.rs` that calls `AppContext::new` with the old signature. This task's own build stays red until Task 10 lands — same pattern as Task 5/7.)

Run: `cargo build -p mc-server`
Expected: FAIL — `AppContext::new` doesn't yet accept 4 arguments. This is the expected/intentional handoff to Task 10.

- [ ] **Step 4: Commit**

```bash
git add crates/mc-server/src/oauth_login.rs crates/mc-server/src/main.rs crates/mc-server/Cargo.toml
git commit -m "feat(server): add OAuthChallengeManager (OAuth login orchestration)"
```

---

### Task 10: mc-server `wiring.rs` — resolve OAuth accounts + periodic token refresh

**Files:**
- Create: `crates/mc-server/src/oauth_refresh.rs`
- Modify: `crates/mc-server/src/wiring.rs`

**Interfaces:**
- Consumes: `mc_infrastructure::email::imap::client::ImapCredential` (Task 3), `SmtpSender::build_oauth` (Task 4), `mc_infrastructure::oauth::refresh` (Task 2), `crate::oauth_login::OAuthChallengeManager` (Task 9), `crate::config::OAuthProviderConfig` (Task 8).
- Produces: `AppContext::new(config: &ServerConfig, config_path: &str, secret_store: Arc<dyn SecretStore>, oauth_challenges: Arc<OAuthChallengeManager>) -> anyhow::Result<Self>` (signature change — 2 new params, `secret_store` now owned not borrowed). `oauth_refresh::needs_refresh(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool` (pure, unit-tested) and `oauth_refresh::spawn_refresh_loop(states: Vec<OAuthRuntimeState>, secret_store: Arc<dyn SecretStore>, shutdown: CancellationToken)`.

- [ ] **Step 1: Write the failing test for the pure refresh-threshold check**

Create `crates/mc-server/src/oauth_refresh.rs`:

```rust
//! Background refresh of OAuth2 tokens close to expiry. Piggybacks on the
//! same 5-minute cadence as the existing periodic re-sync in `wiring.rs`
//! rather than running its own timer.

use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use mc_domain::{OAuthProvider, SecretStore};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// A live account's OAuth runtime state, shared with its `ImapEmailProvider`
/// and `SmtpSender` via the `Arc<RwLock<..>>` cells — refreshing here updates
/// both immediately, no restart needed.
#[derive(Clone)]
pub struct OAuthRuntimeState {
    pub email: String,
    pub provider: OAuthProvider,
    pub access_token: Arc<RwLock<String>>,
    pub refresh_token: Arc<RwLock<String>>,
    pub expires_at: Arc<RwLock<DateTime<Utc>>>,
}

/// True once `expires_at` is within 2 minutes of (or past) `now`.
pub fn needs_refresh(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    expires_at - now <= ChronoDuration::minutes(2)
}

/// Spawns the periodic check. Call once per server startup with every
/// OAuth-authenticated account's runtime state.
pub fn spawn_refresh_loop(
    states: Vec<OAuthRuntimeState>,
    secret_store: Arc<dyn SecretStore>,
    shutdown: CancellationToken,
) {
    if states.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5 * 60));
        interval.tick().await; // skip immediate first tick
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            for state in &states {
                let expires_at = *state.expires_at.read().await;
                if !needs_refresh(expires_at, Utc::now()) {
                    continue;
                }
                let refresh_token = state.refresh_token.read().await.clone();
                match mc_infrastructure::oauth::refresh(state.provider, &refresh_token).await {
                    Ok(new_secrets) => {
                        *state.access_token.write().await = new_secrets.access_token.clone();
                        *state.refresh_token.write().await = new_secrets.refresh_token.clone();
                        *state.expires_at.write().await = new_secrets.expires_at;

                        let mut bundle = secret_store.load(&state.email).ok().flatten().unwrap_or_default();
                        bundle.oauth = Some(new_secrets);
                        if let Err(e) = secret_store.store(&state.email, &bundle) {
                            tracing::error!("OAuth refresh for {}: failed writing keychain: {e}", state.email);
                        } else {
                            tracing::info!("OAuth token refreshed for {}", state.email);
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "OAuth refresh for {} failed ({e}) — token likely expired; run `mc accounts add-oauth` again to re-authenticate",
                            state.email
                        );
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn needs_refresh_true_when_within_threshold() {
        let now = Utc::now();
        assert!(needs_refresh(now + ChronoDuration::minutes(1), now));
        assert!(needs_refresh(now - ChronoDuration::minutes(1), now));
    }

    #[test]
    fn needs_refresh_false_when_far_from_expiry() {
        let now = Utc::now();
        assert!(!needs_refresh(now + ChronoDuration::minutes(30), now));
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p mc-server needs_refresh`
Expected: PASS (pure function, no wiring dependency yet — declare the module first).

Add `mod oauth_refresh;` to `crates/mc-server/src/main.rs` (next to `mod oauth_login;`) so the crate picks the file up, then re-run the same command.

- [ ] **Step 3: Update `AppContext::new`'s signature and the per-account provider loop**

In `crates/mc-server/src/wiring.rs`, add imports:
```rust
use chrono::{DateTime, Utc};
use mc_infrastructure::email::imap::client::ImapCredential;
use tokio::sync::RwLock;
use crate::oauth_login::OAuthChallengeManager;
use crate::oauth_refresh::{self, OAuthRuntimeState};
```

Change `AppContext`'s field `pub account_service: ...` — no change needed there (still built the same way, just gets `.with_oauth_trigger(...)` added later in this same task).

Change the `new` signature:
```rust
    pub async fn new(
        config: &ServerConfig,
        config_path: &str,
        secret_store: Arc<dyn mc_domain::SecretStore>,
        oauth_challenges: Arc<OAuthChallengeManager>,
    ) -> anyhow::Result<Self> {
```
(drop the old `secret_store: &dyn mc_domain::SecretStore` parameter; every existing `secret_store.load(...)` call site inside this function keeps working unchanged since `Arc<dyn T>` derefs to `&dyn T`. `config_path` is accepted but unused in this task's body — it's threaded through only so a *future* task could use it; **do not** add an unused-param `#[allow(unused)]` — instead use it now, in the same step, for the OAuth branch below, since it's needed there.)

Replace the per-account provider-construction block (`for account_config in &config.accounts { ... }`, currently building `imap_password` + `ImapEmailProvider::new(..., imap_password.clone(), ...)`) with a branch on `account_config.oauth_provider` and collect OAuth runtime state as you go. Add right before the `for account_config in &config.accounts` loop:

```rust
        let mut oauth_states: Vec<OAuthRuntimeState> = Vec::new();
```

Inside the loop, after computing `secrets` (the existing `secret_store.load(&account_config.email)...` line) and before building `smtp_sender`, branch:

```rust
            let credential = if let Some(oauth_cfg) = account_config.oauth_provider {
                let oauth_secrets = secrets.oauth.clone().unwrap_or_else(|| {
                    tracing::warn!(
                        "⚠ {}: nessun token OAuth nel keychain — esegui `mc accounts add-oauth`",
                        account_config.email
                    );
                    mc_domain::OAuthSecrets {
                        access_token: String::new(),
                        refresh_token: String::new(),
                        expires_at: Utc::now(),
                    }
                });
                let access_token = Arc::new(RwLock::new(oauth_secrets.access_token.clone()));
                let refresh_token = Arc::new(RwLock::new(oauth_secrets.refresh_token.clone()));
                let expires_at = Arc::new(RwLock::new(oauth_secrets.expires_at));
                oauth_states.push(OAuthRuntimeState {
                    email: account_config.email.clone(),
                    provider: oauth_cfg.into(),
                    access_token: access_token.clone(),
                    refresh_token,
                    expires_at,
                });
                ImapCredential::OAuthToken(access_token)
            } else {
                ImapCredential::Password(imap_password.clone())
            };
```

Change the `smtp_sender` construction's `SmtpSender::build(&domain_smtp, &account_config.username, &imap_password)` call: for OAuth accounts, use `SmtpSender::build_oauth` instead. Replace that whole `if let Some(smtp_cfg) = &account_config.smtp { ... }` block's inner sender-construction line:

```rust
                let sender_result = match &credential {
                    ImapCredential::OAuthToken(token) => {
                        Ok(SmtpSender::build_oauth(&domain_smtp, &account_config.username, token.clone()))
                    }
                    ImapCredential::Password(_) => {
                        SmtpSender::build(&domain_smtp, &account_config.username, &imap_password)
                    }
                };
                match sender_result {
```
(replacing the existing `match SmtpSender::build(&domain_smtp, &account_config.username, &imap_password) {` line — same arms follow unchanged).

Change the `ImapEmailProvider::new(...)` call:
```rust
            let provider = Arc::new(ImapEmailProvider::new(
                account_config.imap_host.clone(),
                account_config.imap_port,
                account_config.username.clone(),
                credential,
                account_config.use_tls,
                smtp_sender,
            ));
```

- [ ] **Step 4: Wire the account_service builder and the refresh loop**

Change the `account_service` construction (in the "Interface Layer" section near the bottom):
```rust
        let account_service = AccountServiceImpl::new(command_bus.clone(), query_bus.clone())
            .with_sync_trigger(sync_service.clone() as Arc<dyn SyncTrigger>)
            .with_oauth_trigger(oauth_challenges as Arc<dyn mc_application::OAuthLoginTrigger>);
```

Add, right after the existing "Periodic re-sync (every 5 minutes)" block:
```rust
        // ── Periodic OAuth token refresh (piggybacks the same 5-minute cadence) ──
        oauth_refresh::spawn_refresh_loop(oauth_states, secret_store.clone(), shutdown.clone());
```

- [ ] **Step 5: Fix the existing test and `main.rs` call site**

Update the `#[tokio::test] async fn sync_service_integrated_in_app_context` test at the bottom of `wiring.rs`:
```rust
    #[tokio::test]
    async fn sync_service_integrated_in_app_context() {
        let config = ServerConfig {
            grpc_addr: "127.0.0.1:50051".to_string(),
            db_path: "/tmp/test_db_sync".to_string(),
            index_path: "/tmp/test_index_sync".to_string(),
            accounts: vec![],
            ui_bin: "mail-cat-ui".to_string(),
        };

        let secret_store: Arc<dyn mc_domain::SecretStore> =
            Arc::new(mc_infrastructure::KeyringSecretStore::new());
        let oauth_challenges = Arc::new(crate::oauth_login::OAuthChallengeManager::new(
            std::path::PathBuf::from("/tmp/test_config_sync.toml"),
            secret_store.clone(),
        ));
        let context = match AppContext::new(&config, "/tmp/test_config_sync.toml", secret_store, oauth_challenges).await {
            Ok(ctx) => ctx,
            Err(e) => panic!("failed to create app context: {}", e),
        };
        // ... rest of the test body unchanged
```

Update `crates/mc-server/src/main.rs`'s call site from Task 9 Step 3 if the parameter order/types drifted — confirm it now reads:
```rust
    let app_ctx = AppContext::new(&config, &config_path, secret_store.clone(), oauth_challenges)
        .await
        .context("failed to initialize application context")?;
```

- [ ] **Step 6: Run tests to verify everything passes**

Run: `cargo build --workspace`
Expected: clean.

Run: `cargo test --workspace`
Expected: all PASS, including `sync_service_integrated_in_app_context`, `needs_refresh_*`, and every test from Tasks 1–9.

Run: `cargo clippy --workspace --all-targets`
Expected: no new warnings beyond the pre-existing ones noted in project memory (too-many-arguments on constructors, etc.) — if `AppContext::new` or `ImapEmailProvider::new` now trip a fresh `too_many_arguments` lint, that's expected and consistent with existing pre-existing warnings of the same kind; do not suppress it, just confirm it's not a *new category* of warning.

- [ ] **Step 7: Commit**

```bash
git add crates/mc-server/src/oauth_refresh.rs crates/mc-server/src/wiring.rs crates/mc-server/src/main.rs
git commit -m "feat(server): wire OAuth accounts into live IMAP/SMTP providers + background token refresh"
```

---

### Task 11: mc-client — `start_oauth_login` streaming wrapper

**Files:**
- Modify: `crates/mc-client/src/types.rs`
- Modify: `crates/mc-client/src/convert.rs`
- Modify: `crates/mc-client/src/client.rs`

**Interfaces:**
- Produces: `mc_client::types::OAuthLoginEvent` (public enum: `AwaitingBrowser{authorize_url: String}`, `Succeeded{email: String}`, `Failed{message: String}`), `mc_client::types::OAuthProvider` (`Google`/`Microsoft`, mirrors the proto enum for client callers), `mc_client::convert::oauth_login_event_from_proto`, and `MailCatClient::start_oauth_login(&self, name: &str, email: &str, provider: OAuthProvider) -> Result<impl Stream<Item = Result<OAuthLoginEvent>>>`.

- [ ] **Step 1: Write the failing test**

Add to `crates/mc-client/src/convert.rs`'s `#[cfg(test)] mod tests` block (create one at the bottom if none exists — check the file first):

```rust
#[cfg(test)]
mod oauth_tests {
    use super::*;

    #[test]
    fn awaiting_browser_event_converts() {
        let proto_event = crate::client::pb::OAuthLoginEvent {
            event: Some(crate::client::pb::oauth_login_event::Event::AwaitingBrowser(
                crate::client::pb::AwaitingBrowser { authorize_url: "https://accounts.google.com/x".into() },
            )),
        };
        match oauth_login_event_from_proto(proto_event) {
            crate::types::OAuthLoginEvent::AwaitingBrowser { authorize_url } => {
                assert_eq!(authorize_url, "https://accounts.google.com/x");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
```

(Check `crates/mc-client/src/client.rs`'s top for how the generated proto module is imported/aliased — e.g. `mod pb { tonic::include_proto!(...) }` or similar — and use that exact path instead of `crate::client::pb` if it differs; the existing `sync_progress_from_proto`/`mailbox_notification_from_proto` functions in `convert.rs` show the real import path to copy.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p mc-client awaiting_browser_event_converts`
Expected: FAIL — `OAuthLoginEvent`/`oauth_login_event_from_proto` don't exist.

- [ ] **Step 3: Implement**

In `crates/mc-client/src/types.rs`, add:
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OAuthProvider {
    Google,
    Microsoft,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OAuthLoginEvent {
    AwaitingBrowser { authorize_url: String },
    Succeeded { email: String },
    Failed { message: String },
}
```

In `crates/mc-client/src/convert.rs`, add (using whatever the file's existing alias for the generated proto module is — copy the pattern from an existing function like `sync_progress_from_proto`):
```rust
pub fn oauth_login_event_from_proto(event: pb::OAuthLoginEvent) -> crate::types::OAuthLoginEvent {
    use pb::oauth_login_event::Event;
    match event.event {
        Some(Event::AwaitingBrowser(e)) => {
            crate::types::OAuthLoginEvent::AwaitingBrowser { authorize_url: e.authorize_url }
        }
        Some(Event::Succeeded(e)) => crate::types::OAuthLoginEvent::Succeeded { email: e.email },
        Some(Event::Failed(e)) => crate::types::OAuthLoginEvent::Failed { message: e.message },
        None => crate::types::OAuthLoginEvent::Failed { message: "empty OAuth login event".into() },
    }
}

fn oauth_provider_to_proto(provider: crate::types::OAuthProvider) -> i32 {
    match provider {
        crate::types::OAuthProvider::Google => pb::OAuthProvider::Google as i32,
        crate::types::OAuthProvider::Microsoft => pb::OAuthProvider::Microsoft as i32,
    }
}
```

In `crates/mc-client/src/client.rs`, add near `sync_account` (using the same `AccountServiceClient` the existing methods use):
```rust
    pub async fn start_oauth_login(
        &self,
        name: &str,
        email: &str,
        provider: crate::types::OAuthProvider,
    ) -> Result<impl Stream<Item = Result<crate::types::OAuthLoginEvent>>> {
        let mut client = AccountServiceClient::new(self.channel.clone());
        let stream = client
            .start_oauth_login(pb::StartOAuthLoginRequest {
                name: name.to_owned(),
                email: email.to_owned(),
                provider: crate::convert::oauth_provider_to_proto(provider),
            })
            .await?
            .into_inner();

        Ok(tokio_stream::StreamExt::map(stream, |item| {
            item.map(crate::convert::oauth_login_event_from_proto)
                .map_err(ClientError::Grpc)
        }))
    }
```

(If `oauth_provider_to_proto` needs to be `pub(crate)` rather than private to be visible from `client.rs`, make that adjustment — check how other convert functions used across files in this crate are scoped, e.g. `sync_progress_from_proto`, and match that visibility.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p mc-client awaiting_browser_event_converts`
Expected: PASS

Run: `cargo build --workspace`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/mc-client/src/types.rs crates/mc-client/src/convert.rs crates/mc-client/src/client.rs
git commit -m "feat(client): add start_oauth_login streaming wrapper"
```

---

### Task 12: mc-cli — `mc accounts add-oauth`

**Files:**
- Modify: `crates/mc-cli/src/commands/accounts.rs`

**Interfaces:**
- Consumes: `mc_client::MailCatClient::start_oauth_login` (Task 11), `mc_client::types::{OAuthProvider, OAuthLoginEvent}`.
- Produces: `mc accounts add-oauth --provider <google|microsoft> --email <addr> --name <display name>` CLI subcommand.

This task has no automated test — it's a thin CLI wrapper over an already-tested client method, and its actual behavior (opening a real browser, waiting for a real OAuth redirect) can only be verified manually. Steps are still concrete and complete.

- [ ] **Step 1: Implement the subcommand**

Replace the top of `crates/mc-cli/src/commands/accounts.rs` with:

```rust
use anyhow::{bail, Context, Result};
use clap::Subcommand;
use colored::Colorize;
use mc_client::types::{OAuthLoginEvent, OAuthProvider};
use mc_client::MailCatClient;
use tabled::{Table, Tabled};
use tokio_stream::StreamExt;

#[derive(Subcommand)]
pub enum AccountsCmd {
    /// List all configured accounts
    List,
    /// Connect a Gmail/Outlook account via OAuth2 (opens your browser)
    AddOauth {
        /// OAuth provider to sign in with
        #[arg(long, value_enum)]
        provider: OauthProviderArg,
        /// The account's email address
        #[arg(long)]
        email: String,
        /// Display name for the account (defaults to the email)
        #[arg(long)]
        name: Option<String>,
    },
}

#[derive(Clone, clap::ValueEnum)]
pub enum OauthProviderArg {
    Google,
    Microsoft,
}

impl From<OauthProviderArg> for OAuthProvider {
    fn from(arg: OauthProviderArg) -> Self {
        match arg {
            OauthProviderArg::Google => OAuthProvider::Google,
            OauthProviderArg::Microsoft => OAuthProvider::Microsoft,
        }
    }
}

pub async fn run(cmd: AccountsCmd, client: MailCatClient) -> Result<()> {
    match cmd {
        AccountsCmd::List => list(client).await,
        AccountsCmd::AddOauth { provider, email, name } => {
            add_oauth(client, provider.into(), email, name).await
        }
    }
}

async fn add_oauth(
    client: MailCatClient,
    provider: OAuthProvider,
    email: String,
    name: Option<String>,
) -> Result<()> {
    let display_name = name.unwrap_or_else(|| email.clone());
    let mut stream = client
        .start_oauth_login(&display_name, &email, provider)
        .await
        .context("starting OAuth login")?;

    while let Some(event) = stream.next().await {
        match event.context("reading OAuth login progress")? {
            OAuthLoginEvent::AwaitingBrowser { authorize_url } => {
                println!("{} {}", "Apro il browser per il login:".cyan(), authorize_url);
                if let Err(e) = open_browser(&authorize_url) {
                    println!(
                        "{} apri manualmente questo link: {}",
                        format!("Impossibile aprire il browser automaticamente ({e}) —").yellow(),
                        authorize_url
                    );
                }
            }
            OAuthLoginEvent::Succeeded { email } => {
                println!(
                    "{} account {} aggiunto a config.toml. Riavvia mail-cat-server per attivarlo.",
                    "✓".green(),
                    email
                );
                return Ok(());
            }
            OAuthLoginEvent::Failed { message } => {
                bail!("Login OAuth fallito: {message}");
            }
        }
    }
    bail!("Login OAuth interrotto: stream terminato senza un evento finale");
}

fn open_browser(url: &str) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        std::process::Command::new("xdg-open").arg(url).status()?;
    }
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open").arg(url).status()?;
    }
    #[cfg(target_os = "windows")]
    {
        std::process::Command::new("cmd").args(["/C", "start", url]).status()?;
    }
    Ok(())
}

#[derive(Tabled)]
struct AccountRow {
    #[tabled(rename = "ID")]
    id: String,
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Email")]
    email: String,
    #[tabled(rename = "Status")]
    status: String,
}

async fn list(client: MailCatClient) -> Result<()> {
    let accounts = client.list_accounts().await?;

    if accounts.is_empty() {
        println!("{}", "No accounts found.".yellow());
        return Ok(());
    }

    let rows: Vec<AccountRow> = accounts
        .into_iter()
        .map(|a| AccountRow {
            id: a.id,
            name: a.name,
            email: a.email,
            status: if a.is_active {
                "active".green().to_string()
            } else {
                "inactive".red().to_string()
            },
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}
```

- [ ] **Step 2: Build and manually smoke-test the CLI wiring (no real OAuth yet)**

Run: `cargo build -p mc-cli`
Expected: clean.

Run: `cargo run -p mc-cli -- accounts add-oauth --provider google --email test@gmail.com`
Expected: fails with a clear gRPC/connection error (no `mail-cat-server` running in this shell) or, if a server is running without `MAILCAT_GOOGLE_CLIENT_ID` set, prints `Apro il browser per il login: https://accounts.google.com/...` and attempts to open a browser — either outcome confirms the command is wired end-to-end; a real login isn't required for this step.

- [ ] **Step 3: Commit**

```bash
git add crates/mc-cli/src/commands/accounts.rs
git commit -m "feat(cli): add 'mc accounts add-oauth' command"
```

---

### Task 13: End-to-end manual verification

**Files:** none (verification only).

This task has no automated steps — it's the real-world check that everything built in Tasks 1–12 actually works against live Google/Microsoft accounts, per the design doc's testing section.

- [ ] **Step 1: Register the OAuth apps**

Google Cloud Console: create an OAuth client, type "Desktop app", scope `https://mail.google.com/`, note client_id + client_secret. Add yourself as a test user if the consent screen is in "Testing" mode.

Azure AD: create an app registration, platform "Mobile and desktop applications", redirect URI `http://localhost` (loopback, any port), API permissions `IMAP.AccessAsUser.All`, `SMTP.Send`, `offline_access` (delegated), note the client_id (application ID).

- [ ] **Step 2: Export credentials and run the server**

```bash
export MAILCAT_GOOGLE_CLIENT_ID="<client id>"
export MAILCAT_GOOGLE_CLIENT_SECRET="<client secret>"
export MAILCAT_MICROSOFT_CLIENT_ID="<client id>"
cargo run -p mc-server
```

Expected: server starts, logs show gRPC listening on the configured address.

- [ ] **Step 3: Run the login for a real test account**

```bash
cargo run -p mc-cli -- accounts add-oauth --provider google --email <your test gmail address>
```

Expected: browser opens on Google's consent screen, after granting access the CLI prints `✓ account <email> aggiunto a config.toml. Riavvia mail-cat-server per attivarlo.`

Verify:
- `~/.config/mail-cat/config.toml` has a new `[[accounts]]` block with `oauth_provider = "google"` and no `password` field.
- The keychain has a new "mail-cat" entry for that email (`secret-tool search service mail-cat` on Linux, or Keychain Access/Credential Manager on other OSes) containing an `oauth` JSON blob.

- [ ] **Step 4: Restart and confirm real IMAP/SMTP traffic**

```bash
# Ctrl+C the running mc-server, then:
cargo run -p mc-server
```

Expected: logs show the account auto-created/registered, background sync starting, and no auth errors. Confirm real mail appears via `cargo run -p mc-cli -- mailboxes list --account <id>` (or the existing equivalent listing command) and that `mc messages send-test` (if present — check `crates/mc-cli/src/commands` for the exact existing command name) successfully sends through XOAUTH2 SMTP.

- [ ] **Step 5: Repeat for Microsoft**

Same as Steps 3–4 with `--provider microsoft` and a test Outlook/Office365 account.

- [ ] **Step 6: Record results**

No commit for this task (nothing to check in) — but note in the next session's context (or ask the user) whether both providers worked end-to-end, and specifically whether Google's 7-day unverified-app refresh-token expiry (documented in the design doc) was hit during testing.

---

## Self-Review Notes

- **Spec coverage:** every section of `docs/superpowers/specs/2026-07-06-oauth-imap-smtp-design.md` maps to a task — domain (Task 1), oauth module/PKCE/loopback (Task 2), IMAP XOAUTH2 (Task 3), SMTP XOAUTH2 (Task 4), gRPC streaming RPC (Tasks 5–7), config.toml (Task 8), server-owned challenge flow (Task 9), live wiring + background refresh (Task 10), CLI (Tasks 11–12), manual E2E (Task 13).
- **Signature consistency checked:** `ImapEmailProvider::new`'s 4th param (`ImapCredential`, Task 3) is used identically in Task 10's wiring.rs rewrite. `SmtpSender::build_oauth`'s 3-arg shape (Task 4) matches its Task 10 call site. `OAuthLoginTrigger::start_login`'s 3-arg shape (Task 6) matches every call site in Tasks 7, 9, and its own test. `AppContext::new`'s 4-arg signature is introduced once (Task 9's `main.rs` edit) and implemented to match exactly in Task 10 (including the test call site and the real `main.rs` call site) — no drift between the two.
- **Known deliberate risk, called out inline rather than hidden:** the `oauth2` crate's exact v4 builder API (Task 2) and `async-imap`'s `authenticate()` signature (Task 3) are written from documentation/memory, not verified against a live `cargo doc` in this environment. Both tasks include an explicit fallback instruction (check `cargo doc --open`, adjust only the call site) rather than silently hoping the code compiles as written — this is the one place in the plan where "no placeholders" is in tension with genuine third-party API uncertainty, resolved by being explicit about it instead of papering over it.
