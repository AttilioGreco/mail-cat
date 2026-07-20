# Design: expired/revoked IMAP OAuth token → de-auth + re-auth prompt

## Problem

`sync_service` backfill (and other IMAP calls) fail with `Provider error: IMAP
XOAUTH2 authentication failed: ... AUTHENTICATIONFAILED` when an account's
OAuth2 refresh token has expired or been revoked at the provider. Today the
app has no way to detect this class of failure at the account level: it just
logs an error, forever, while `oauth_refresh.rs`'s periodic refresh attempt
(every 5 min) also silently fails and logs. Nothing clears the dead tokens
and nothing tells the user the account needs attention.

## Goals

- Detect, at the account level, when an OAuth2 refresh token is dead
  (revoked/invalid) as opposed to a transient provider/network error.
- Clear the dead tokens from the keychain so they stop being retried forever.
- Mark the account's status so this state is visible in the domain model.
- Prompt the user to re-authenticate via a desktop notification with a direct
  action, and make the resulting new tokens actually usable by the running
  server without a restart.

## Non-goals (explicitly out of scope for this change)

- Changing `sync_service.rs` to skip sync attempts for accounts already
  flagged `NeedsReauth`. It keeps failing/logging every cycle until the next
  refresh-loop tick (≤5 min) catches it. Accepted noise window — add a
  status check there later if the log noise becomes a real problem.
- Detecting revocation that happens well before the token's recorded
  `expires_at`. The refresh loop only acts within 10 minutes of expiry
  (existing `needs_refresh` threshold), so an early revocation can go
  undetected for up to that long. Known limitation, not fixed here.
- Surfacing `NeedsReauth` status anywhere other than the desktop
  notification (no CLI/`mc accounts list` display changes).
- SMTP-specific detection — SMTP shares the same `OAuthRuntimeState` cells,
  so it benefits from the same fix, but no SMTP-specific error classification
  is added.

## Detection

`mc-infrastructure/src/oauth/mod.rs::refresh()` currently maps every error
from `exchange_refresh_token(...).request_async(...)` to
`DomainError::Provider(format!("OAuth token refresh failed: {e}"))`.

It changes to inspect the `oauth2` crate's `RequestTokenError` before
formatting:

- `RequestTokenError::ServerResponse(resp)` where `resp.error()` is
  `invalid_grant` (or the equivalent revoked/invalid-token error the
  provider returns) → return `DomainError::AuthenticationFailed(msg)`. This
  is the "refresh token is dead, needs full re-auth" signal.
- Anything else (network error, timeout, 5xx, parse error) → stays
  `DomainError::Provider(msg)`, unchanged behavior (log and retry next
  cycle).

This is the **only** trigger for the flow below — no changes to how
`sync_service` or the IMAP client classify their own auth errors. A one-off
IMAP `AUTHENTICATIONFAILED` (e.g. from a stale in-memory access token that
just hasn't been refreshed yet) does not, by itself, flag the account;
only a confirmed-dead refresh token does.

## Domain model changes

- `AccountStatus` (`mc-domain/src/model/account.rs`) gains a `NeedsReauth`
  variant alongside `Active`/`Disabled`/`Error`.
- New event `DomainEvent::AccountAuthExpired { account_id, error }`.
  Applied in `Account::apply()` by setting `status = AccountStatus::NeedsReauth`,
  mirroring the existing `AccountRemoved → Disabled` transition.
- A second small event/transition returns the account to `Active` on
  successful re-auth (reusing `AccountUpdated` or adding
  `AccountReauthenticated` — left to the implementation plan to decide
  based on what's cheaper against the existing event/status machinery).

## Token removal

`oauth_refresh.rs`, on receiving `DomainError::AuthenticationFailed` from
`refresh()`:

1. Load the account's `AccountSecrets` bundle from the keychain, set
   `bundle.oauth = None` (preserving `imap_password`/`smtp_password`/
   `app_password` if present — only the OAuth tokens are known-dead),
   `store()` the bundle back.
2. Append `AccountAuthExpired { account_id, error }` via the repository.
   Requires `OAuthRuntimeState` (`oauth_refresh.rs`) to carry `account_id`
   in addition to its existing `email`/`provider` fields — populated once at
   construction time in the account-resolution loop in `wiring.rs` (the
   `account_id` is already resolved a bit further down that same loop, just
   needs to be threaded into the `OAuthRuntimeState` push).
3. Set a new per-state flag (e.g. `needs_reauth: Arc<RwLock<bool>>` on
   `OAuthRuntimeState`) so the refresh loop stops attempting refresh for
   that account every 5 minutes against a token it already proved dead.
   Cleared on successful re-auth (see below).

## Re-auth UX

The existing notify-rust desktop notification subscriber in `wiring.rs`
(handles `MessageReceived` and `AccountSyncFailed` today) gets a new arm for
`AccountAuthExpired`:

- Fires a notification with one action button ("Ri-autentica").
- On click: calls `OAuthChallengeManager::start_login(name, email, provider)`
  in-process (already constructed and available at that point in `wiring.rs`),
  opens the returned `authorize_url` via the system's default opener, and
  awaits the progress stream.
- On `OAuthLoginProgress::Succeeded`:
  - clears the account's `needs_reauth` flag,
  - writes the fresh tokens into the matching `OAuthRuntimeState`'s
    `access_token`/`refresh_token`/`expires_at` cells (looked up by
    `account_id`), so the already-running `ImapEmailProvider`/`SmtpSender`
    pick them up immediately — the same mechanism the periodic refresh loop
    already uses to hot-swap tokens without a restart, just invoked manually
    here instead of on the 5-minute timer.
  - appends the event that flips `AccountStatus` back to `Active`.
- On `OAuthLoginProgress::Failed`: fires a second notification showing the
  error, so the user isn't left wondering whether it worked.

### Required fix: `append_oauth_account` must become idempotent

`OAuthChallengeManager::start_login()` (`mc-server/src/oauth_login.rs`)
currently always calls `append_oauth_account()` on success, which appends a
new `[[accounts]]` table to `config.toml`. That's correct for onboarding a
brand-new account but wrong for re-auth, where the account already has a
config entry — appending would create a duplicate. `append_oauth_account`
changes to check whether an entry for that email already exists in
`config.toml` and skip the append if so (updating nothing else there, since
the account's config didn't change — only its keychain tokens did).

## Data flow (happy path failure → recovery)

```
oauth_refresh loop (5 min tick)
  → refresh() returns AuthenticationFailed(invalid_grant)
  → keychain: oauth = None (other secrets kept)
  → event store: AccountAuthExpired { account_id, error }
  → AccountStatus → NeedsReauth
  → OAuthRuntimeState.needs_reauth = true  (loop stops retrying this account)
  → desktop notification subscriber receives AccountAuthExpired
  → notify-rust popup: "Account needs re-authentication" [Ri-autentica]

user clicks [Ri-autentica]
  → OAuthChallengeManager::start_login(name, email, provider)
  → system browser opens to authorize_url
  → user completes provider consent
  → OAuthLoginProgress::Succeeded
  → keychain: oauth = Some(new tokens)
  → append_oauth_account: no-op (email already in config.toml)
  → OAuthRuntimeState cells updated in place (IMAP/SMTP pick up new token)
  → OAuthRuntimeState.needs_reauth = false
  → event store: status → Active
```

## Error handling

- Keychain read/write failures during token removal or re-auth follow the
  existing pattern in `oauth_refresh.rs` (log and skip that account's
  update, don't crash the loop — see the existing comment there about not
  collapsing a real `Err` into "no entry").
- `start_login()` failures at any stage (auth URL build, browser timeout
  5 min, token exchange, keychain write, config write) already produce
  `OAuthLoginProgress::Failed { message }` — the notification handler's
  `Failed` arm surfaces that message directly, no new error taxonomy needed.

## Testing

- `refresh()`: unit test that an `invalid_grant` `ServerResponse` maps to
  `DomainError::AuthenticationFailed`, and that other error shapes still map
  to `DomainError::Provider` (existing `oauth_refresh.rs` tests already cover
  `needs_refresh`; add refresh-classification tests alongside `refresh()`
  itself in `mc-infrastructure`).
- `Account::apply()`: test `AccountAuthExpired` sets `NeedsReauth`, and the
  recovery event sets `Active` again (follows the existing
  `rehydrate_removed_account` test pattern in `account.rs`).
- `append_oauth_account`: test that a second call for an already-present
  email is a no-op (no duplicate `[[accounts]]` table), alongside the
  existing `append_oauth_account_writes_expected_fields` test.
- Manual/integration: cannot fully automate the browser-consent leg: the
  in-process `start_login` → keychain → `OAuthRuntimeState` hot-swap path
  should get a test with a fake `SecretStore`/`OAuthLoginTrigger` verifying
  the state cells are updated and `needs_reauth` clears, without a real
  browser round-trip.
