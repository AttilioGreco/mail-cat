# Expired IMAP OAuth Token Detection + Re-auth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When an account's OAuth refresh token is revoked/invalid, clear the dead tokens from the keychain, flag the account, and let the user re-authenticate from a desktop notification without restarting the server.

**Architecture:** The existing 5-minute `oauth_refresh` loop becomes the sole detector: it classifies `invalid_grant` responses as `DomainError::AuthenticationFailed` (vs. transient `DomainError::Provider` errors), clears the account's OAuth keychain entry, and appends a new `AccountAuthExpired` domain event. The existing desktop-notification event-bus subscriber gets a new case for that event with an action button that drives the existing `OAuthChallengeManager::start_login()` in-process, then hot-swaps the fresh tokens into the same `Arc<RwLock<_>>` cells the running IMAP/SMTP providers already share with the refresh loop — no restart required.

**Tech Stack:** Rust, `oauth2` v4 (already a dependency), `notify-rust` v4 (already a dependency), the project's existing event-sourced domain model (`mc-domain`) and sled-backed event store (`mc-infrastructure`).

## Global Constraints

- No new crate dependencies — everything is built from `oauth2`, `notify-rust`, `tokio`, and the existing domain/repository traits already in the workspace.
- Follow the existing pattern in this codebase: mutate an aggregate via `Account::apply(&event)`, persist via `AccountWriteRepository::save(&account, vec![event])`, and separately publish the same event to `Arc<dyn EventPublisher>` for in-process subscribers (repository `save()` does **not** publish to the bus itself — see `sync_service.rs`'s `append_and_project` for the reference pattern).
- Design doc: `docs/superpowers/specs/2026-07-20-oauth-token-reauth-design.md`. Out-of-scope items listed there (no `sync_service.rs` changes, no CLI status surfacing, no early-revocation detection) stay out of scope here too.

---

### Task 1: Domain model — `NeedsReauth` status and auth-expiry events

**Files:**
- Modify: `crates/mc-domain/src/model/account.rs`
- Modify: `crates/mc-domain/src/events/mod.rs`

**Interfaces:**
- Produces: `mc_domain::AccountStatus::NeedsReauth`, `mc_domain::DomainEvent::AccountAuthExpired { account_id: AccountId, error: String }`, `mc_domain::DomainEvent::AccountReauthenticated { account_id: AccountId }`. `Account::apply()` sets `status = NeedsReauth` on the former, `status = Active` on the latter. Later tasks depend on these three names existing exactly as written.

- [ ] **Step 1: Write the failing tests**

In `crates/mc-domain/src/events/mod.rs`, inside the existing `#[cfg(test)] mod tests` block (find it via the existing `aggregate_type_display`/`event_name` tests near the bottom of the file), add:

```rust
    #[test]
    fn account_auth_expired_aggregate_type_and_name() {
        let event = DomainEvent::AccountAuthExpired {
            account_id: AccountId::new(),
            error: "invalid_grant".into(),
        };
        assert_eq!(event.aggregate_type(), AggregateType::Account);
        assert_eq!(event.event_name(), "AccountAuthExpired");
    }

    #[test]
    fn account_reauthenticated_aggregate_type_and_name() {
        let event = DomainEvent::AccountReauthenticated {
            account_id: AccountId::new(),
        };
        assert_eq!(event.aggregate_type(), AggregateType::Account);
        assert_eq!(event.event_name(), "AccountReauthenticated");
    }
```

In `crates/mc-domain/src/model/account.rs`, inside the existing `#[cfg(test)] mod tests` block, add (after `rehydrate_removed_account`):

```rust
    #[test]
    fn rehydrate_account_needs_reauth() {
        let id = AccountId::new();
        let events = vec![
            DomainEvent::AccountAdded {
                account_id: id,
                name: "Test".into(),
                email: EmailAddress::new("test@example.com").unwrap(),
                provider_type: ProviderType::Imap,
            },
            DomainEvent::AccountAuthExpired {
                account_id: id,
                error: "invalid_grant".into(),
            },
        ];

        let account = Account::rehydrate(&events).unwrap();
        assert_eq!(account.status, AccountStatus::NeedsReauth);
    }

    #[test]
    fn reauthenticated_returns_account_to_active() {
        let id = AccountId::new();
        let events = vec![
            DomainEvent::AccountAdded {
                account_id: id,
                name: "Test".into(),
                email: EmailAddress::new("test@example.com").unwrap(),
                provider_type: ProviderType::Imap,
            },
            DomainEvent::AccountAuthExpired {
                account_id: id,
                error: "invalid_grant".into(),
            },
            DomainEvent::AccountReauthenticated { account_id: id },
        ];

        let account = Account::rehydrate(&events).unwrap();
        assert_eq!(account.status, AccountStatus::Active);
    }
```

- [ ] **Step 2: Run tests to verify they fail (compile error — types don't exist yet)**

Run: `cargo test -p mc-domain --lib`
Expected: FAIL to compile — `no variant named AccountAuthExpired found for enum DomainEvent` (and similar for `AccountReauthenticated`, `AccountStatus::NeedsReauth`).

- [ ] **Step 3: Add the new event variants**

In `crates/mc-domain/src/events/mod.rs`, in the `DomainEvent` enum, right after the existing `AccountSyncFailed` variant:

```rust
    /// A background token refresh discovered the account's OAuth refresh
    /// token is dead (revoked/invalid). Tokens have already been cleared
    /// from the keychain by the time this is appended.
    AccountAuthExpired {
        account_id: AccountId,
        error: String,
    },

    /// The user completed a fresh OAuth login for an account that was
    /// previously flagged `AccountAuthExpired`.
    AccountReauthenticated { account_id: AccountId },
```

In the same file, update `DomainEvent::aggregate_type()`:

```rust
            Self::AccountAdded { .. }
            | Self::AccountUpdated { .. }
            | Self::AccountRemoved { .. }
            | Self::AccountSyncStarted { .. }
            | Self::AccountSyncCompleted { .. }
            | Self::AccountSyncFailed { .. }
            | Self::AccountAuthExpired { .. }
            | Self::AccountReauthenticated { .. } => AggregateType::Account,
```

And `DomainEvent::event_name()`, right after the `AccountSyncFailed` arm:

```rust
            Self::AccountAuthExpired { .. } => "AccountAuthExpired",
            Self::AccountReauthenticated { .. } => "AccountReauthenticated",
```

- [ ] **Step 4: Add the `NeedsReauth` status and wire `Account::apply()`**

In `crates/mc-domain/src/model/account.rs`, update `AccountStatus`:

```rust
pub enum AccountStatus {
    Active,
    Disabled,
    Error,
    NeedsReauth,
}
```

In `Account::apply()`, replace:

```rust
            DomainEvent::AccountRemoved { .. } => {
                self.status = AccountStatus::Disabled;
            }
            DomainEvent::AccountSyncStarted { .. }
            | DomainEvent::AccountSyncCompleted { .. }
            | DomainEvent::AccountSyncFailed { .. } => {
                // Sync events don't mutate aggregate state.
                // They're tracked for audit/projection purposes.
            }
```

with:

```rust
            DomainEvent::AccountRemoved { .. } => {
                self.status = AccountStatus::Disabled;
            }
            DomainEvent::AccountAuthExpired { .. } => {
                self.status = AccountStatus::NeedsReauth;
            }
            DomainEvent::AccountReauthenticated { .. } => {
                self.status = AccountStatus::Active;
            }
            DomainEvent::AccountSyncStarted { .. }
            | DomainEvent::AccountSyncCompleted { .. }
            | DomainEvent::AccountSyncFailed { .. } => {
                // Sync events don't mutate aggregate state.
                // They're tracked for audit/projection purposes.
            }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p mc-domain --lib`
Expected: PASS — all tests including the 4 new ones.

- [ ] **Step 6: Commit**

```bash
git add crates/mc-domain/src/model/account.rs crates/mc-domain/src/events/mod.rs
git commit -m "feat(domain): add AccountStatus::NeedsReauth and auth-expiry events"
```

---

### Task 2: Classify OAuth refresh errors (dead token vs. transient)

**Files:**
- Modify: `crates/mc-infrastructure/src/oauth/mod.rs`

**Interfaces:**
- Consumes: `mc_domain::DomainError::{Provider, AuthenticationFailed}` (both already exist in `crates/mc-domain/src/error.rs`).
- Produces: `refresh()` (existing public fn, signature unchanged: `pub async fn refresh(provider: OAuthProvider, refresh_token: &str) -> Result<OAuthSecrets, DomainError>`) now returns `DomainError::AuthenticationFailed(_)` specifically when the provider reports `invalid_grant`, and `DomainError::Provider(_)` for every other failure. Task 4 matches on this distinction.

- [ ] **Step 1: Write the failing tests**

In `crates/mc-infrastructure/src/oauth/mod.rs`, inside the existing `#[cfg(test)] mod tests` block (starts at line 133), add:

```rust
    #[test]
    fn classify_invalid_grant_as_authentication_failed() {
        let resp = oauth2::StandardErrorResponse::new(
            oauth2::basic::BasicErrorResponseType::InvalidGrant,
            None,
            None,
        );
        let err: oauth2::RequestTokenError<
            std::io::Error,
            oauth2::StandardErrorResponse<oauth2::basic::BasicErrorResponseType>,
        > = oauth2::RequestTokenError::ServerResponse(resp);

        let classified = classify_refresh_error(err);

        assert!(matches!(classified, DomainError::AuthenticationFailed(_)));
    }

    #[test]
    fn classify_other_server_response_as_provider_error() {
        let resp = oauth2::StandardErrorResponse::new(
            oauth2::basic::BasicErrorResponseType::InvalidRequest,
            None,
            None,
        );
        let err: oauth2::RequestTokenError<
            std::io::Error,
            oauth2::StandardErrorResponse<oauth2::basic::BasicErrorResponseType>,
        > = oauth2::RequestTokenError::ServerResponse(resp);

        let classified = classify_refresh_error(err);

        assert!(matches!(classified, DomainError::Provider(_)));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p mc-infrastructure --lib oauth::`
Expected: FAIL to compile — `cannot find function classify_refresh_error in this scope`.

- [ ] **Step 3: Implement `classify_refresh_error` and wire it into `refresh()`**

In `crates/mc-infrastructure/src/oauth/mod.rs`, replace the `refresh()` function's error mapping:

```rust
    let token = client
        .exchange_refresh_token(&RefreshToken::new(refresh_token.to_string()))
        .request_async(oauth2::reqwest::async_http_client)
        .await
        .map_err(|e| DomainError::Provider(format!("OAuth token refresh failed: {e}")))?;
```

with:

```rust
    let token = client
        .exchange_refresh_token(&RefreshToken::new(refresh_token.to_string()))
        .request_async(oauth2::reqwest::async_http_client)
        .await
        .map_err(classify_refresh_error)?;
```

Then add this new function right after `refresh()` (before `token_response_to_secrets`):

```rust
/// Distinguishes a dead refresh token (revoked/invalid — the account needs a
/// full user re-auth) from a transient failure (network hiccup, timeout,
/// provider 5xx — safe to retry next cycle).
fn classify_refresh_error<RE: std::error::Error + 'static>(
    e: oauth2::RequestTokenError<RE, oauth2::StandardErrorResponse<oauth2::basic::BasicErrorResponseType>>,
) -> DomainError {
    match e {
        oauth2::RequestTokenError::ServerResponse(resp)
            if matches!(resp.error(), oauth2::basic::BasicErrorResponseType::InvalidGrant) =>
        {
            DomainError::AuthenticationFailed(format!("refresh token invalid or revoked: {resp}"))
        }
        other => DomainError::Provider(format!("OAuth token refresh failed: {other}")),
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p mc-infrastructure --lib oauth::`
Expected: PASS — all tests including the 2 new ones.

- [ ] **Step 5: Commit**

```bash
git add crates/mc-infrastructure/src/oauth/mod.rs
git commit -m "feat(oauth): classify invalid_grant refresh errors as AuthenticationFailed"
```

---

### Task 3: Idempotent `append_oauth_account` + browser-open helper for re-auth

**Files:**
- Modify: `crates/mc-server/src/oauth_login.rs`

**Interfaces:**
- Produces: `append_oauth_account()` (existing private fn, unchanged signature) becomes a no-op when the given `email` already has a `[[accounts]]` entry in `config.toml`. New `pub(crate) fn open_browser(url: &str) -> std::io::Result<()>` for Task 5 to call when prompting re-auth from the desktop notification.

- [ ] **Step 1: Write the failing test**

In `crates/mc-server/src/oauth_login.rs`, in the existing `#[cfg(test)] mod tests` block, add (after `append_oauth_account_writes_expected_fields`):

```rust
    #[test]
    fn append_oauth_account_is_idempotent_for_existing_email() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "grpc_addr = \"127.0.0.1:50051\"\n").unwrap();

        append_oauth_account(&path, "Work Gmail", "me@gmail.com", OAuthProvider::Google).unwrap();
        append_oauth_account(&path, "Work Gmail", "me@gmail.com", OAuthProvider::Google).unwrap();

        let written = std::fs::read_to_string(&path).unwrap();
        let parsed: crate::config::ServerConfig = toml::from_str(&written).unwrap();
        assert_eq!(
            parsed.accounts.len(),
            1,
            "re-auth of an existing account must not duplicate its config entry"
        );
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p mc-server --lib oauth_login::tests::append_oauth_account_is_idempotent`
Expected: FAIL — `assertion 'left == right' failed ... left: 2, right: 1`.

- [ ] **Step 3: Make `append_oauth_account` idempotent and add `open_browser`**

In `crates/mc-server/src/oauth_login.rs`, in `append_oauth_account`, right after the block that ensures `doc["accounts"]` is an `ArrayOfTables` (right before `let mut table = Table::new();`), insert:

```rust
    let already_present = accounts
        .iter()
        .any(|table| table.get("email").and_then(|v| v.as_str()) == Some(email));
    if already_present {
        // Re-auth of an existing account: the caller has already refreshed
        // and stored the tokens in the keychain — the config entry itself
        // hasn't changed, so appending here would create a duplicate
        // [[accounts]] table.
        return Ok(());
    }

```

Then, right after `append_oauth_account`'s closing `}` and before `#[cfg(test)]`, add:

```rust
/// Opens `url` in the OS default browser. Best-effort: callers should also
/// show the URL so the user can open it manually if this fails (e.g. no
/// desktop session).
// ponytail: duplicates mc-cli's private open_browser (crates/mc-cli/src/commands/accounts.rs) —
// extract to a shared location if a third caller needs it.
pub(crate) fn open_browser(url: &str) -> std::io::Result<()> {
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p mc-server --lib oauth_login::`
Expected: PASS — all tests including the new idempotency test.

- [ ] **Step 5: Commit**

```bash
git add crates/mc-server/src/oauth_login.rs
git commit -m "fix(oauth): make append_oauth_account idempotent, add open_browser for re-auth"
```

---

### Task 4: `oauth_refresh.rs` — detect dead tokens, clear keychain, emit `AccountAuthExpired`

**Files:**
- Modify: `crates/mc-server/src/oauth_refresh.rs`

**Interfaces:**
- Consumes: `mc_domain::DomainEvent::AccountAuthExpired` (Task 1), `mc_infrastructure::oauth::refresh()`'s new `DomainError::AuthenticationFailed` classification (Task 2), `mc_domain::{AccountWriteRepository, EventStore}`, `mc_application::EventPublisher`.
- Produces:
  - `OAuthRuntimeState` gains three fields: `account_id: mc_domain::AccountId`, `name: String`, `needs_reauth: Arc<RwLock<bool>>`.
  - `spawn_refresh_loop` gains three parameters (inserted after `secret_store`): `event_store: Arc<dyn mc_domain::EventStore>`, `account_repository: Arc<dyn mc_domain::AccountWriteRepository>`, `event_publisher: Arc<dyn mc_application::EventPublisher>`.
  - New `pub(crate) async fn append_account_event(account_id: mc_domain::AccountId, event: mc_domain::DomainEvent, event_store: &Arc<dyn mc_domain::EventStore>, account_repository: &Arc<dyn mc_domain::AccountWriteRepository>, event_publisher: &Arc<dyn mc_application::EventPublisher>) -> Result<(), mc_domain::DomainError>` — Task 5 reuses this for the re-auth-succeeded path.
  - New pure fn `fn should_attempt_refresh(needs_reauth: bool, expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool`.

- [ ] **Step 1: Write the failing test**

In `crates/mc-server/src/oauth_refresh.rs`, in the existing `#[cfg(test)] mod tests` block, add:

```rust
    #[test]
    fn should_attempt_refresh_false_once_flagged_needs_reauth() {
        let now = Utc::now();
        // Within the refresh threshold, but already flagged dead.
        assert!(!should_attempt_refresh(true, now + ChronoDuration::minutes(1), now));
    }

    #[test]
    fn should_attempt_refresh_true_when_due_and_not_flagged() {
        let now = Utc::now();
        assert!(should_attempt_refresh(false, now - ChronoDuration::minutes(1), now));
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p mc-server --lib oauth_refresh::tests::should_attempt_refresh`
Expected: FAIL to compile — `cannot find function should_attempt_refresh in this scope`.

- [ ] **Step 3: Update imports and `OAuthRuntimeState`**

At the top of `crates/mc-server/src/oauth_refresh.rs`, replace:

```rust
use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use mc_domain::{OAuthProvider, SecretStore};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
```

with:

```rust
use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use mc_application::EventPublisher;
use mc_domain::{
    AccountId, AccountWriteRepository, AggregateType, DomainError, DomainEvent, EventEnvelope,
    EventStore, OAuthProvider, SecretStore,
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
```

Replace the `OAuthRuntimeState` struct:

```rust
#[derive(Clone)]
pub struct OAuthRuntimeState {
    pub email: String,
    pub provider: OAuthProvider,
    pub access_token: Arc<RwLock<String>>,
    pub refresh_token: Arc<RwLock<String>>,
    pub expires_at: Arc<RwLock<DateTime<Utc>>>,
}
```

with:

```rust
#[derive(Clone)]
pub struct OAuthRuntimeState {
    pub account_id: AccountId,
    pub name: String,
    pub email: String,
    pub provider: OAuthProvider,
    pub access_token: Arc<RwLock<String>>,
    pub refresh_token: Arc<RwLock<String>>,
    pub expires_at: Arc<RwLock<DateTime<Utc>>>,
    /// Set once a refresh attempt proves the refresh token is dead
    /// (revoked/invalid). While true, the loop stops retrying this account.
    /// Cleared back to false once the user successfully re-authenticates.
    pub needs_reauth: Arc<RwLock<bool>>,
}
```

- [ ] **Step 4: Add `should_attempt_refresh` and `append_account_event`, rewrite the loop body**

Replace the `needs_refresh` function's neighbor — the whole `spawn_refresh_loop` function — with:

```rust
/// True once `expires_at` is within 10 minutes of (or past) `now`.
pub fn needs_refresh(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    expires_at - now <= ChronoDuration::minutes(10)
}

/// True when a live token cell should be refreshed: not already flagged
/// dead by a previous cycle, and within 10 minutes of (or past) expiry.
fn should_attempt_refresh(needs_reauth: bool, expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    !needs_reauth && needs_refresh(expires_at, now)
}

/// Appends a single event to an `Account` aggregate and publishes it to the
/// in-process event bus, mirroring `SyncService::append_and_project`'s
/// append-then-publish pattern (repository `save()` alone does not publish).
pub(crate) async fn append_account_event(
    account_id: AccountId,
    event: DomainEvent,
    event_store: &Arc<dyn EventStore>,
    account_repository: &Arc<dyn AccountWriteRepository>,
    event_publisher: &Arc<dyn EventPublisher>,
) -> Result<(), DomainError> {
    let mut account = account_repository
        .load(&account_id)
        .await?
        .ok_or_else(|| DomainError::AccountNotFound(account_id.to_string()))?;
    let current_events = event_store.load_events(&account_id.to_string(), 0).await?;
    let sequence_number = current_events.len() as u64 + 1;

    account.apply(&event);
    account_repository.save(&account, vec![event.clone()]).await?;

    event_publisher.publish(EventEnvelope::new(
        account_id.to_string(),
        AggregateType::Account,
        sequence_number,
        event,
    ));
    Ok(())
}

/// Spawns the periodic check. Call once per server startup with every
/// OAuth-authenticated account's runtime state.
pub fn spawn_refresh_loop(
    states: Vec<OAuthRuntimeState>,
    secret_store: Arc<dyn SecretStore>,
    event_store: Arc<dyn EventStore>,
    account_repository: Arc<dyn AccountWriteRepository>,
    event_publisher: Arc<dyn EventPublisher>,
    shutdown: CancellationToken,
) {
    if states.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5 * 60));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            for state in &states {
                let expires_at = *state.expires_at.read().await;
                let dead = *state.needs_reauth.read().await;
                if !should_attempt_refresh(dead, expires_at, Utc::now()) {
                    continue;
                }
                let refresh_token = state.refresh_token.read().await.clone();
                match mc_infrastructure::oauth::refresh(state.provider, &refresh_token).await {
                    Ok(new_secrets) => {
                        *state.access_token.write().await = new_secrets.access_token.clone();
                        *state.refresh_token.write().await = new_secrets.refresh_token.clone();
                        *state.expires_at.write().await = new_secrets.expires_at;

                        // Don't let a real Err (e.g. a corrupt/undeserializable
                        // keychain entry) silently collapse to "no entry" — that
                        // would overwrite the keychain with only the new OAuth
                        // tokens, discarding any existing imap/smtp passwords for
                        // this account. Skip this account's refresh persistence
                        // (not the whole loop) on a real read error.
                        let mut bundle = match secret_store.load(&state.email) {
                            Ok(existing) => existing.unwrap_or_default(),
                            Err(e) => {
                                tracing::error!(
                                    "OAuth refresh for {}: failed reading keychain before update: {e}",
                                    state.email
                                );
                                continue;
                            }
                        };
                        bundle.oauth = Some(new_secrets);
                        if let Err(e) = secret_store.store(&state.email, &bundle) {
                            tracing::error!("OAuth refresh for {}: failed writing keychain: {e}", state.email);
                        } else {
                            tracing::info!("OAuth token refreshed for {}", state.email);
                        }
                    }
                    Err(DomainError::AuthenticationFailed(msg)) => {
                        tracing::error!(
                            "OAuth refresh token for {} is invalid or revoked ({msg}) — \
                             clearing tokens, account needs re-authentication",
                            state.email
                        );
                        *state.needs_reauth.write().await = true;

                        let mut bundle = match secret_store.load(&state.email) {
                            Ok(existing) => existing.unwrap_or_default(),
                            Err(e) => {
                                tracing::error!(
                                    "OAuth de-auth for {}: failed reading keychain before clearing: {e}",
                                    state.email
                                );
                                continue;
                            }
                        };
                        bundle.oauth = None;
                        if let Err(e) = secret_store.store(&state.email, &bundle) {
                            tracing::error!("OAuth de-auth for {}: failed clearing keychain: {e}", state.email);
                        }

                        if let Err(e) = append_account_event(
                            state.account_id,
                            DomainEvent::AccountAuthExpired {
                                account_id: state.account_id,
                                error: msg,
                            },
                            &event_store,
                            &account_repository,
                            &event_publisher,
                        )
                        .await
                        {
                            tracing::error!(
                                "OAuth de-auth for {}: failed recording AccountAuthExpired event: {e}",
                                state.email
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "OAuth refresh for {} failed ({e}) — will retry next cycle",
                            state.email
                        );
                    }
                }
            }
        }
    });
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p mc-server --lib oauth_refresh::`
Expected: PASS — `needs_refresh_*` tests unchanged, plus the 2 new `should_attempt_refresh_*` tests. (This will not yet compile the crate as a whole — `wiring.rs`'s call site is fixed in Task 5. If you want a green build at this checkpoint, run `cargo check -p mc-server --lib oauth_refresh` instead of a full `cargo test -p mc-server`, since the latter also compiles `wiring.rs`.)

- [ ] **Step 6: Commit**

```bash
git add crates/mc-server/src/oauth_refresh.rs
git commit -m "feat(oauth): detect dead refresh tokens, clear keychain, emit AccountAuthExpired"
```

---

### Task 5: `wiring.rs` — thread account identity through, notify + re-auth action

**Files:**
- Modify: `crates/mc-server/src/wiring.rs`

**Interfaces:**
- Consumes: `OAuthRuntimeState { account_id, name, email, provider, access_token, refresh_token, expires_at, needs_reauth }` (Task 4), `spawn_refresh_loop(states, secret_store, event_store, account_repository, event_publisher, shutdown)` (Task 4), `append_account_event` (Task 4), `open_browser` (Task 3), `DomainEvent::{AccountAuthExpired, AccountReauthenticated}` (Task 1).
- Produces: no new public interface — this is the final wiring task. Verified by build + the existing integration test + manual run.

- [ ] **Step 1: Restructure OAuth cell construction so `account_id`/`name` are known before `OAuthRuntimeState` is built**

`OAuthRuntimeState` is currently pushed inside the first per-account loop (building `providers_by_email`), but `account_id` is only resolved later, in the second per-account loop (the "Auto-create accounts from config" section). Change the first loop to stash the raw cells keyed by email, and build the real `OAuthRuntimeState` values in the second loop once the id is known.

Find this line (before the first `for account_config in &config.accounts` loop, near where `providers_by_email` is declared):

```rust
        let mut oauth_states: Vec<OAuthRuntimeState> = Vec::new();
```

Replace it with:

```rust
        let mut oauth_cells_by_email: HashMap<
            String,
            (
                Arc<RwLock<String>>,
                Arc<RwLock<String>>,
                Arc<RwLock<chrono::DateTime<Utc>>>,
                mc_domain::OAuthProvider,
            ),
        > = HashMap::new();
```

In the same loop, find:

```rust
                oauth_states.push(OAuthRuntimeState {
                    email: account_config.email.clone(),
                    provider: oauth_cfg.into(),
                    access_token: access_token.clone(),
                    refresh_token,
                    expires_at,
                });
```

Replace it with:

```rust
                oauth_cells_by_email.insert(
                    account_config.email.clone(),
                    (access_token.clone(), refresh_token, expires_at, oauth_cfg.into()),
                );
```

Then, just before the second loop (find `let mut registered_ids: Vec<(AccountId, String)> = Vec::new();`), add right after it:

```rust
        let mut oauth_states: Vec<OAuthRuntimeState> = Vec::new();
```

In the second loop's "already in DB" branch, right after `registered_ids.push((summary.id, summary.name.clone()));`, add:

```rust
                if let Some((access_token, refresh_token, expires_at, provider)) =
                    oauth_cells_by_email.remove(&account_config.email)
                {
                    oauth_states.push(OAuthRuntimeState {
                        account_id: summary.id,
                        name: summary.name.clone(),
                        email: account_config.email.clone(),
                        provider,
                        access_token,
                        refresh_token,
                        expires_at,
                        needs_reauth: Arc::new(RwLock::new(false)),
                    });
                }
```

And in the "not in DB" (auto-create) branch, right after `registered_ids.push((account_id, account_config.name.clone()));`, add:

```rust
                if let Some((access_token, refresh_token, expires_at, provider)) =
                    oauth_cells_by_email.remove(&account_config.email)
                {
                    oauth_states.push(OAuthRuntimeState {
                        account_id,
                        name: account_config.name.clone(),
                        email: account_config.email.clone(),
                        provider,
                        access_token,
                        refresh_token,
                        expires_at,
                        needs_reauth: Arc::new(RwLock::new(false)),
                    });
                }
```

- [ ] **Step 2: Update the `spawn_refresh_loop` call site**

Find:

```rust
        oauth_refresh::spawn_refresh_loop(oauth_states, secret_store.clone(), shutdown.clone());
```

Replace with:

```rust
        oauth_refresh::spawn_refresh_loop(
            oauth_states,
            secret_store.clone(),
            event_store.clone() as Arc<dyn mc_domain::EventStore>,
            repository_arc.clone() as Arc<dyn mc_domain::AccountWriteRepository>,
            event_bus.clone() as Arc<dyn mc_application::EventPublisher>,
            shutdown.clone(),
        );
```

- [ ] **Step 3: Run `cargo check` to catch any remaining wiring mismatches**

Run: `cargo check -p mc-server`
Expected: builds clean (or reports missing `use` statements for `mc_domain::OAuthProvider` / `chrono` — both are already imported at the top of `wiring.rs` as `chrono::Utc` and via `mc_domain::{..}`; add `mc_domain::OAuthProvider` to the existing `use mc_domain::{...}` block if the compiler flags it unresolved).

- [ ] **Step 4: Add the `AccountAuthExpired` notification case with the re-auth action**

Find the desktop notification subscriber block (`// ── Desktop notification subscriber`). Just before it, capture the extra clones it will need:

```rust
        {
            let mut sub = event_bus.subscribe();
            let notif_cmd_bus = command_bus.clone();
            let notif_q_bus = query_bus.clone();
            let notif_ui_bin = config.ui_bin.clone();
            let sd = shutdown.clone();
```

becomes:

```rust
        {
            let mut sub = event_bus.subscribe();
            let notif_cmd_bus = command_bus.clone();
            let notif_q_bus = query_bus.clone();
            let notif_ui_bin = config.ui_bin.clone();
            let notif_oauth_states = oauth_states.clone();
            let notif_oauth_challenges = oauth_challenges.clone();
            let notif_secret_store = secret_store.clone();
            let notif_repository = repository_arc.clone() as Arc<dyn mc_domain::AccountWriteRepository>;
            let notif_event_store = event_store.clone() as Arc<dyn mc_domain::EventStore>;
            let notif_event_publisher = event_bus.clone() as Arc<dyn mc_application::EventPublisher>;
            let sd = shutdown.clone();
```

Then, in the `match envelope.payload { ... }` block inside that subscriber, right after the existing `AccountSyncFailed` arm and before the trailing `_ => {}`, add:

```rust
                        mc_domain::DomainEvent::AccountAuthExpired { account_id, error } => {
                            let Some(state) = notif_oauth_states
                                .iter()
                                .find(|s| s.account_id == account_id)
                                .cloned()
                            else {
                                tracing::warn!(
                                    "AccountAuthExpired for unknown account {}",
                                    account_id.as_uuid()
                                );
                                continue;
                            };
                            let challenges = notif_oauth_challenges.clone();
                            let secret_store = notif_secret_store.clone();
                            let repository = notif_repository.clone();
                            let event_store = notif_event_store.clone();
                            let publisher = notif_event_publisher.clone();
                            tokio::task::spawn_blocking(move || {
                                let handle = match notify_rust::Notification::new()
                                    .summary(&format!("{} richiede una nuova autenticazione", state.email))
                                    .body(&error)
                                    .icon("dialog-error")
                                    .action("reauth", "Ri-autentica")
                                    .show()
                                {
                                    Ok(h) => h,
                                    Err(e) => {
                                        tracing::warn!("Desktop notification failed: {e}");
                                        return;
                                    }
                                };
                                let rt = tokio::runtime::Handle::current();
                                handle.wait_for_action(move |action| {
                                    if action != "reauth" {
                                        return;
                                    }
                                    rt.block_on(async move {
                                        let mut progress = challenges.start_login(
                                            state.name.clone(),
                                            state.email.clone(),
                                            state.provider,
                                        );
                                        while let Some(update) = progress.recv().await {
                                            match update {
                                                mc_application::OAuthLoginProgress::AwaitingBrowser {
                                                    authorize_url,
                                                } => {
                                                    if let Err(e) =
                                                        crate::oauth_login::open_browser(&authorize_url)
                                                    {
                                                        tracing::warn!(
                                                            "Failed to open browser for re-auth ({e}) — \
                                                             apri manualmente: {authorize_url}"
                                                        );
                                                    }
                                                }
                                                mc_application::OAuthLoginProgress::Succeeded { .. } => {
                                                    match secret_store.load(&state.email) {
                                                        Ok(Some(bundle)) => {
                                                            if let Some(oauth) = bundle.oauth {
                                                                *state.access_token.write().await =
                                                                    oauth.access_token;
                                                                *state.refresh_token.write().await =
                                                                    oauth.refresh_token;
                                                                *state.expires_at.write().await =
                                                                    oauth.expires_at;
                                                            }
                                                        }
                                                        Ok(None) => tracing::warn!(
                                                            "Re-auth succeeded for {} but keychain entry is missing",
                                                            state.email
                                                        ),
                                                        Err(e) => tracing::warn!(
                                                            "Re-auth succeeded for {} but re-reading keychain failed: {e}",
                                                            state.email
                                                        ),
                                                    }
                                                    *state.needs_reauth.write().await = false;

                                                    if let Err(e) = oauth_refresh::append_account_event(
                                                        state.account_id,
                                                        mc_domain::DomainEvent::AccountReauthenticated {
                                                            account_id: state.account_id,
                                                        },
                                                        &event_store,
                                                        &repository,
                                                        &publisher,
                                                    )
                                                    .await
                                                    {
                                                        tracing::warn!(
                                                            "Failed to record AccountReauthenticated for {}: {e}",
                                                            state.email
                                                        );
                                                    }

                                                    let _ = notify_rust::Notification::new()
                                                        .summary(&format!("{} ri-autenticato", state.email))
                                                        .body("Account riattivato con successo.")
                                                        .icon("dialog-information")
                                                        .show();
                                                }
                                                mc_application::OAuthLoginProgress::Failed { message } => {
                                                    tracing::warn!(
                                                        "Re-auth failed for {}: {message}",
                                                        state.email
                                                    );
                                                    let _ = notify_rust::Notification::new()
                                                        .summary(&format!(
                                                            "Ri-autenticazione fallita per {}",
                                                            state.email
                                                        ))
                                                        .body(&message)
                                                        .icon("dialog-error")
                                                        .show();
                                                }
                                            }
                                        }
                                    });
                                });
                            });
                        }
```

Also update the doc comment just above the subscriber block that lists handled events:

```rust
        //   • AccountSyncFailed – error alert (no actions)
```

becomes:

```rust
        //   • AccountSyncFailed – error alert (no actions)
        //   • AccountAuthExpired – error alert with "Ri-autentica" action:
        //       drives OAuthChallengeManager::start_login() in-process, opens the
        //       system browser, and on success hot-swaps the new tokens into the
        //       live OAuthRuntimeState cells (no server restart needed)
```

- [ ] **Step 5: Run the existing integration test and full build**

Run: `cargo test -p mc-server --lib`
Expected: PASS — `sync_service_integrated_in_app_context` still passes (it configures `accounts: vec![]`, so none of the new OAuth code paths run), and the crate builds without warnorrs about unused imports.

Run: `cargo build --workspace`
Expected: builds clean across all crates (`mc-domain`, `mc-infrastructure`, `mc-server`, and anything downstream that references `AccountStatus` or `DomainEvent` exhaustively — if any other crate has a non-exhaustive `match` over `AccountStatus` or `DomainEvent` that the compiler flags, add a `NeedsReauth` / `AccountAuthExpired` / `AccountReauthenticated` arm there matching the surrounding style before proceeding).

- [ ] **Step 6: Manual smoke test**

This path can't be fully exercised by automated tests (it needs a live OAuth provider round-trip). Manually verify the wiring is sound:

1. `cargo run -p mc-server` with a config containing at least one `oauth_provider`-configured account.
2. Confirm the log line `Initialized event bus` still appears before any OAuth-related logs (ordering sanity check — `event_bus` must exist before `spawn_refresh_loop` is called).
3. Temporarily set that account's keychain `refresh_token` to an obviously invalid string (e.g. via `secret-tool` on Linux, matching whatever the `KeyringSecretStore` uses) and wait up to 5 minutes (or temporarily shrink the interval while testing locally) — confirm a desktop notification titled "... richiede una nuova autenticazione" appears with a "Ri-autentica" button, and that clicking it opens a browser to the provider's consent screen.
4. Complete the consent flow and confirm a second notification ("... ri-autenticato") appears, and that IMAP sync resumes without restarting `mc-server`.

- [ ] **Step 7: Commit**

```bash
git add crates/mc-server/src/wiring.rs
git commit -m "feat(oauth): wire dead-token notification with in-process re-auth action"
```

---

## Self-Review Notes

- **Spec coverage:** detection (Task 2), token removal + status (Tasks 1, 4), notification + re-auth UX (Task 5), `append_oauth_account` idempotency fix (Task 3) — all design-doc sections have a task. Non-goals (no `sync_service.rs` change, no CLI surfacing, no early-revocation detection) are intentionally untouched by every task above.
- **Type consistency:** `OAuthRuntimeState`'s new fields (`account_id`, `name`, `needs_reauth`) are defined once in Task 4 and consumed with matching names in Task 5. `append_account_event`'s signature (Task 4) matches its two call sites (Task 4's own `AccountAuthExpired` append, Task 5's `AccountReauthenticated` append) exactly. `classify_refresh_error` (Task 2) and `should_attempt_refresh` (Task 4) are both free functions with no cross-task signature drift.
- **No placeholders:** every step includes complete code, not descriptions of code.
