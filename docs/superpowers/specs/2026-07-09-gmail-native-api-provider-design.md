# Gmail native API provider (replace IMAP for Google accounts)

**Date:** 2026-07-09
**Status:** Approved design, pending implementation plan

## Problem

Google OAuth accounts today connect via IMAP (`imap.gmail.com`), because IMAP
is Gmail's *emulation* layer over its native label model — Gmail has no real
folders. A message with multiple labels (e.g. `INBOX` + a user label) is
exposed as if it existed independently in each IMAP "folder", with its own
UID per folder. Syncing multiple mailboxes therefore re-downloads the same
message multiple times and inflates UID/message counts relative to what the
account actually contains.

The domain already anticipated a native backend: `ProviderType::GmailApi`
exists in `mc-domain::model::account`, `provider_type = "gmail_api"` is a
documented option in `deploy/config.toml.example`, and
`EmailProvider`/`AccountEventSource` doc comments explicitly call out Gmail
API as a future implementor. None of this is wired up — `mc-server` is
monomorphized to a single concrete provider
(`InMemoryProviderRegistry<ImapEmailProvider>`).

## Goal

Add a `GmailApiEmailProvider` that talks to the Gmail REST API directly,
selectable per-account via `provider_type = "gmail_api"`. It coexists with
the existing IMAP provider — Microsoft and generic IMAP accounts are
unaffected, and no automatic migration of existing Google/IMAP accounts
occurs. A message is fetched exactly once regardless of how many labels it
carries, and mailbox change detection continues to work close to real time
without any public-facing infrastructure (no Pub/Sub webhook, no relay
server) — mail-cat remains a per-user local service with no central backend.

## Explicitly out of scope

- Microsoft/Outlook native API (`ProviderType::OutlookApi`). `OAuthProvider`
  already separates the token-refresh concern from `ProviderType`, so this is
  a natural future addition, but it is not part of this design.
- True push notifications via Cloud Pub/Sub. Considered and rejected for v1:
  a pull subscription avoids a public webhook, but Pub/Sub topics/subscriptions
  belong to a GCP project, not to the end user's machine. Routing account-scoped
  notifications to the right local instance would require either a maintainer-run
  relay (a new backend component mail-cat doesn't otherwise have) or each user
  provisioning their own GCP project (unacceptable UX for a mail client). May be
  revisited later if polling+IDLE-wake latency proves insufficient in practice.
- Automatic migration/conversion of existing IMAP-configured Google accounts.
  Switching an account to `gmail_api` is a config change the user makes
  explicitly (re-add the account, or edit `provider_type` in config).

## Architecture

### Two independent concerns, two components

**Data path — `GmailApiEmailProvider`** (`mc-infrastructure/src/email/gmail/`,
implements `mc_domain::traits::email_provider::EmailProvider`):

- `MailboxPath` maps to a Gmail label (`INBOX`, `SENT`, a user label, etc.).
  `list_mailboxes` calls `users.labels.list`.
- `ProviderUid` is the Gmail **message id** (global, not per-label) — not an
  IMAP-style per-mailbox UID. This is what eliminates the duplicate-download
  problem: a message with N labels is one id, fetched once;
  `fetch_message_envelopes` for a given mailbox is `users.messages.list`
  filtered with `q=label:<name>`, returning ids that already exist in memory
  if fetched via another label.
- `fetch_message_detail` / `fetch_raw_message` / `fetch_bodies_bulk` use
  `users.messages.get` (format `full` / `raw`), batched via Gmail's batch
  HTTP endpoint where possible.
- `set_flags` maps IMAP-style flags to Gmail's label operations
  (`users.messages.modify` add/removeLabelIds — `\Seen` ↔ absence of
  `UNREAD`, `\Flagged` ↔ `STARRED`, etc.).
- `move_message` changes label membership (add destination label, remove
  source label — Gmail has no true move). `delete_message` maps to
  `trash` (soft) or `messages.delete` (permanent).
- `send_message` uses `users.messages.send`.
- `mailbox_status` / `fetch_uid_flags` have no UIDVALIDITY/UIDNEXT
  equivalent. Status is derived from `users.labels.get` (message/thread
  counts) and a stored `historyId` watermark; `fetch_uid_flags` diffs against
  that watermark via `users.history.list` instead of a full `UID FETCH
  FLAGS`.

**Notification path — `GmailAccountEventSource`** (implements
`mc_domain::traits::account_event_source::AccountEventSource`):

- Opens a plain IMAP IDLE connection (same OAuth2/XOAUTH2 plumbing already
  used today) against a single mailbox — `[Gmail]/All Mail` — purely as a
  wake-up signal. It does not parse or act on FETCH/EXISTS payloads.
- On wake (or on a coarse safety-net timer, e.g. every few minutes, in case
  an EXISTS is missed), it calls `users.history.list(startHistoryId=...)`
  and translates the returned history records into the existing
  `ProviderNotification` variants (`NewMessage`, `FlagsChanged`,
  `MailboxCountChanged`), then persists the new `historyId`.
- Structurally mirrors `ImapAccountEventSource`
  (`mc-infrastructure/src/email/imap/account_event_source.rs`): one
  background task, outer reconnect loop, notifications funneled through an
  mpsc channel into the returned stream.
- OAuth scope is unchanged from today (`https://mail.google.com/`) because
  IDLE still requires full IMAP access. Moving to the native API does not
  narrow the consent screen — the win is entirely on the data-fetching side,
  not the OAuth side.

### Registry becomes multi-provider

`mc-server/src/wiring.rs` currently hardcodes
`type ProviderRegistryImpl = InMemoryProviderRegistry<ImapEmailProvider>`.
With two concrete provider types coexisting, this needs an enum wrapper:

```rust
enum AnyEmailProvider {
    Imap(ImapEmailProvider),
    GmailApi(GmailApiEmailProvider),
}
```

implementing `EmailProvider` by delegating each method to the active variant
(`async_trait` methods forward via a `match`). `AccountEventSource` gets the
same treatment. Account wiring picks the variant from `account_config.provider_type`
at startup, same place it already reads `provider_type` today
(`wiring.rs:367`).

## Error handling

- **`historyId` expired** (Gmail discards history after ~7 days of
  inactivity; server down for a long stretch): `history.list` returns 404.
  Fall back to a full resync via `messages.list`, the same "rebuild from
  scratch" path already needed for IMAP's UIDVALIDITY-changed case.
- **Rate limiting / quota** (Gmail API has per-user and per-project quota):
  standard exponential backoff on 429/403 `rateLimitExceeded`, consistent
  with the existing queue/rate-limit handling mentioned for IMAP download
  speed.
- **IDLE connection drop**: reuses the existing outer reconnect loop pattern
  from `ImapAccountEventSource` (5s pause, restart).

## Testing

- Unit tests for label↔flag mapping (`\Seen`/`\Flagged` ↔ `UNREAD`/`STARRED`
  label add/remove) and for `MailboxPath` ↔ label name conversion.
- Unit tests for `history.list` record translation into
  `ProviderNotification` variants, including the 404-triggers-full-resync
  path.
- Integration test (behind a real-credentials feature flag, matching however
  the existing IMAP integration tests are gated) exercising fetch → modify →
  history diff against a real Gmail test account.

## Out of scope (deferred, not forgotten)

- Cloud Pub/Sub relay for true push (see "Explicitly out of scope" above).
- `OutlookApi` native provider.
- Automatic conversion of existing IMAP-configured accounts to `gmail_api`.
