# OAuth2 login for Google/Microsoft, XOAUTH2 on IMAP/SMTP — design

## Goal

Let a user connect a Gmail or Outlook/Office365 account by signing in with
OAuth2 instead of typing a password/app-password, and use the resulting
tokens to authenticate on the **existing generic IMAP/SMTP pipeline** via
SASL XOAUTH2. Builds directly on the OS-keychain secret storage shipped in
the previous session (`docs/superpowers/specs/2026-07-06-secrets-keychain-design.md`).

## Explicitly out of scope (this pass)

- Native Gmail API / Microsoft Graph API clients. `ProviderType::GmailApi`/
  `OutlookApi` stay unused placeholders; every OAuth account is transport-wise
  a normal `ProviderType::Imap` account, just authenticated differently.
- Calendars.
- Any UI (Tauri) work — the gRPC endpoint added here is designed to be
  reusable 1:1 by a future "Sign in with Google/Microsoft" button, but no
  frontend code is written now.
- Live re-wiring without a server restart. Adding an OAuth account still
  requires restarting `mail-cat-server`, identical to today's only working
  path for password accounts (config.toml + restart). Extracting
  `wiring.rs`'s per-account registration into a reusable "hot add" function is
  a natural follow-up, not built here unless asked.
- Handling Google's unverified-app 7-day refresh-token expiry beyond a clear
  error message telling the user to redo the login. No auto-reverification,
  no fallback flow.

## Google/Microsoft app registration (out of code, but required before testing)

- One shared OAuth app per provider, registered by the user, client_id (and
  for Google, client_secret — not treated as confidential for installed apps
  per RFC 8252, but still required in the token-exchange request) baked into
  the binary, overridable via env vars for local dev:
  `MAILCAT_GOOGLE_CLIENT_ID` / `MAILCAT_GOOGLE_CLIENT_SECRET` /
  `MAILCAT_MICROSOFT_CLIENT_ID`.
- Google: OAuth client type "Desktop app", scope `https://mail.google.com/`
  (the only scope that unlocks Gmail's IMAP/SMTP bridge — narrower Gmail API
  scopes don't apply here). Consent screen can stay in "Testing" mode
  (no review) — tradeoff already discussed with the user: refresh tokens then
  expire after 7 days until the app is verified.
- Microsoft: Azure AD app registration, platform "Mobile and desktop
  applications", scopes `https://outlook.office.com/IMAP.AccessAsUser.All`,
  `https://outlook.office.com/SMTP.Send`, `offline_access`. No verification
  needed for these scopes.
- Redirect URI for both: a loopback address whose port is chosen at runtime
  (`http://127.0.0.1:<port>/callback`) — both providers support the
  loopback-any-port exception for installed apps (RFC 8252); the redirect URI
  registered in each console only needs to match the scheme/host/path, not a
  fixed port, for this client type.

## Domain (`mc-domain`)

- New `OAuthProvider { Google, Microsoft }` enum (`model/account.rs`), `Copy`,
  serializable.
- `AuthConfig::OAuth2` gains a `provider: OAuthProvider` field (needed to pick
  the right token endpoint on refresh). `access_token`/`refresh_token` stay
  `#[serde(skip)]` as already established.

## `mc-infrastructure::oauth` (new module)

- Per-provider preset table: authorize URL, token URL, scopes, client_id/secret
  resolution (env override → compiled-in constant), IMAP host/port, SMTP
  host/port + security mode.
- Depends on the `oauth2` crate (PKCE, CSRF state, authorization-code exchange,
  refresh-token exchange — hand-rolling PKCE/token-exchange crypto is a
  security-sensitive protocol implementation, not worth reinventing) using its
  `reqwest` backend feature. `reqwest` and `oauth2` are new workspace
  dependencies — the only new ones this feature needs.
- Loopback listener: hand-rolled on `std::net::TcpListener` (bind
  `127.0.0.1:0`, accept one connection, parse the GET request line's query
  string for `code`/`state`, write back a static "you can close this tab" HTML
  response). A full HTTP server crate would be overkill for one fixed
  request/response.
- Two entry points used by the server:
  - `begin_authorization(provider) -> (authorize_url, PendingLogin)` — binds
    the listener, builds the PKCE verifier/challenge + CSRF token + redirect
    URI, returns the URL to show the user and a handle holding everything
    needed to complete the exchange.
  - `complete_authorization(PendingLogin) -> OAuthSecrets` — async, awaits the
    one redirect on the listener, validates state, exchanges the code for
    tokens.
  - `refresh(provider, refresh_token) -> OAuthSecrets` — token refresh call.

## IMAP (`imap/client.rs`) / SMTP (`smtp/sender.rs`)

- `ImapEmailProvider`'s credential field becomes an enum:
  `ImapCredential::Password(String) | ImapCredential::OAuthToken(Arc<RwLock<String>>)`.
  `create_imap_session` matches on it: `Password` → existing `.login(...)`;
  `OAuthToken` → build the XOAUTH2 SASL string
  (`user=<email>\x01auth=Bearer <token>\x01\x01`) via a small custom
  `Authenticator` impl and call `.authenticate("XOAUTH2", ...)` (the mechanism
  `async-imap` already exposes, just unused until now).
- `SmtpSender` gets the equivalent: for OAuth accounts, credentials are
  rebuilt per send from the shared token cell (`Credentials::new(email,
  current_token)`, `.authentication(vec![Mechanism::Xoauth2])`); for password
  accounts nothing changes (transport built once at startup, as today).
- The `Arc<RwLock<String>>` token cell is shared between the account's IMAP
  provider and SMTP sender and the background refresher, so a refresh is
  visible everywhere immediately without restart.

## gRPC (new streaming RPC on `AccountService`)

```proto
enum OAuthProvider {
  OAUTH_PROVIDER_UNSPECIFIED = 0;
  OAUTH_PROVIDER_GOOGLE = 1;
  OAUTH_PROVIDER_MICROSOFT = 2;
}

rpc StartOAuthLogin(StartOAuthLoginRequest) returns (stream OAuthLoginEvent);

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
message AwaitingBrowser { string authorize_url = 1; }
message OAuthLoginSucceeded { string email = 1; }
message OAuthLoginFailed { string message = 1; }
```

Same shape as the existing `SyncAccount` streaming RPC (`accounts.proto`), so
the pattern is already established in this codebase.

**Server-side handler** (`mc-interfaces` grpc service → new
`OAuthChallengeManager` living on `AppContext`):
1. Calls `oauth::begin_authorization(provider)`.
2. Sends `AwaitingBrowser{authorize_url}` on the stream immediately.
3. Awaits `oauth::complete_authorization(...)` in the background.
4. On success: stores `OAuthSecrets` via `SecretStore::store`, appends the
   account to `config.toml` via `toml_edit` (reusing the same
   format-preserving-edit approach `secret_ingest.rs` already uses) with the
   provider's IMAP/SMTP host/port presets, sends `Succeeded{email}`. No
   `AccountId` exists yet at this point — it's only assigned once the server
   restarts and `wiring.rs`'s existing config-reconciliation loop picks up the
   new `config.toml` entry, exactly like it does for password accounts today.
5. On any failure (timeout, denied consent, token exchange error): sends
   `Failed{message}`, cleans up the listener.

**Client side (`mc-cli`)**: `mc accounts add-oauth --provider google|microsoft
--email x@gmail.com --name "Work Gmail"` calls the stream, opens the system
browser on the URL from the first event (`xdg-open`/`open`/`start` via
`std::process::Command`, matched on `cfg(target_os)` — no new dependency),
then blocks until the terminal event and prints success/failure. `mc-cli`
stays a pure gRPC client — no filesystem/keychain access added to it.

## Config file (`config.toml`)

`AccountConfig` (mc-server `config.rs`) gains an auth variant for OAuth
accounts: provider marker only, no tokens (same principle as the existing
`password: Option<String>` — secrets never touch the file). Read at startup by
the existing reconciliation loop in `wiring.rs`, which already knows how to
turn a `config.toml` entry into a live `ImapEmailProvider`/`SmtpSender` and an
`Account` aggregate — it just needs a branch for resolving `OAuthSecrets` from
the keychain instead of a plain password, and constructing the shared token
cell described above.

## Background token refresh

No new scheduler. The existing 5-minute reconcile loop in `wiring.rs` (already
iterating accounts for mailbox reconciliation) gets one more per-account
check: if the account is OAuth-authenticated and `expires_at` is within a
threshold (e.g. 2 minutes), call `oauth::refresh`, write the new tokens to the
keychain, update the shared token cell. If refresh fails (e.g. Google's
7-day-unverified-app expiry), log a clear error naming the account and
instructing the user to redo `add-oauth` — no retry loop, no fallback.

## Testing

- Unit: PKCE/state roundtrip is exercised through the `oauth2` crate itself
  (trust the library); our own code gets unit tests for the loopback
  listener's query-string parsing and for the config.toml OAuth entry
  read/write roundtrip.
- Integration: a `#[ignore]` test (like the existing `keychain_roundtrip`)
  that runs the interactive flow end-to-end against a real Google or
  Microsoft test account, run manually, not in CI.
- Manual verification: `mc accounts add-oauth --provider google --email
  <real test account>`, confirm browser opens, consent, tokens land in
  keychain (`secret-tool` / Keychain Access / Credential Manager), account
  appears in `config.toml`, restart `mail-cat-server`, confirm IMAP sync and
  a test send both work via XOAUTH2.

## Known limitations carried forward

- Restart required after `add-oauth` for the account to actually start
  syncing (matches today's behavior for password accounts).
- Google apps left in "Testing" publishing mode force a re-login every 7
  days — accepted tradeoff, revisit only if it becomes annoying enough to
  justify going through Google's app verification.
- `ProviderType::GmailApi`/`OutlookApi` remain unused; a later "native API"
  pass would give them real meaning without touching anything built here.
