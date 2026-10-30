# itest: local IMAP/JMAP integration harness

A throwaway [Stalwart](https://stalw.art) (`v0.16.25`) in Docker plus an
isolated `mail-cat-server` built from this tree, driven by `mc`, `stalwart-cli`
and stdlib Python. Needs docker, python3, openssl, dbus-run-session and
gnome-keyring-daemon.

```sh
just itest-reset          # wipe + recreate everything (also runs cargo build)
just itest-seed -n 20     # APPEND generated messages (default INBOX/Archive/Projects)
just itest                # all scenarios, each from reset+seed; exit 1 on any FAIL
just itest flags reappear # just some
just itest-down           # stop the server, drop Stalwart and its volumes
```

`MAIL_CAT_NO_IDLE=1 just itest-reset` starts the server without the INBOX
IDLE watchers, so syncs happen only when a test runs `mc sync` (the
`phone_moved_first` scenario needs IDLE and will fail in that mode).
`mc` takes a global `--json` for list/show output, `mc sync ID --mailbox X` for
a single mailbox and `mc accounts add ... --password-stdin`.

## Never touch the real mail-cat

The user's server runs on `127.0.0.1:50051` with real accounts. The harness
server listens on **`127.0.0.1:50061`**, reads `tests/itest/.state/config.toml`,
keeps its DB and index in `.state/mc/`, runs with `HOME` set to `.state/home`,
and gets a private D-Bus session with its own unlocked gnome-keyring, so the
password it moves into "the keychain" never reaches the real one. `reset.sh`
only kills the pid it started, and only if that pid runs the binary it built.
Always pass `--server http://127.0.0.1:50061` to `mc` by hand.

## Ports and credentials

| what                  | where                     |
|-----------------------|---------------------------|
| IMAPS (self-signed CA)| `localhost:19993`         |
| SMTP submission (TLS) | `localhost:19465`         |
| HTTP: JMAP, admin API | `http://127.0.0.1:18080`  |
| mail-cat gRPC         | `127.0.0.1:50061`         |
| Stalwart admin        | `admin` / `itest-admin-horse-battery` |
| users                 | `alice@itest.local` / `alice-itest-horse-battery`, `bob@itest.local` / `bob-itest-horse-battery` |

mail-cat always speaks TLS, so `reset.sh` makes a private CA in `.state/certs/`,
installs a `localhost` certificate in Stalwart and starts the server with
`SSL_CERT_FILE` pointing at the CA. Mailboxes: INBOX, Archive, Trash, Sent,
Drafts, Junk (special-use) and Projects. alice is configured in mail-cat over IMAP, bob over JMAP (`jmap_sync` scenario).

## Tools

- `./stalwart-cli <args>`: stalwart-cli from its image, logged in as admin
  (e.g. `./stalwart-cli query Account`).
- `./imapctl.py`: the "other client". `list INBOX` (UID, flags, Message-ID),
  `move|copy MAILBOX MSG DEST`, `store MAILBOX MSG +\Seen -\Flagged`,
  `delete MAILBOX MSG` (\Deleted + UID EXPUNGE), `expunge`, `mailboxes`, `caps`.
  MSG is a UID or `tag:N`, the `[itest-NNN]` tag every seeded subject carries.
- `./seed.py`: deterministic messages with threads, missing Message-IDs,
  attachments and mixed flags; see its `--help`.
- `./scenarios.py`: the assertions. Server logs of each run land in `.logs/`.

## Gmail scenarios (real mailbox, Gmail API as oracle)

`gmail_*` scenarios in `gmail_scenarios.py` sync a **dedicated, throwaway Gmail
mailbox** with mail-cat (IMAP + X-GM-LABELS) and compare the result with what
the Gmail REST API (`gmail/api.py`, stdlib `urllib`) reports, so the code is
not checked through its own IMAP view. Without the setup below they print
`SKIP` and never fail.

One-time setup:

1. Use a dedicated Gmail account (see below), and the Google OAuth
   client the app uses (`MAILCAT_GOOGLE_CLIENT_ID` / `MAILCAT_GOOGLE_CLIENT_SECRET`
   exported in your shell). The Gmail API must be enabled in that Cloud project;
   the scope `https://mail.google.com/` already covers both IMAP and REST.
2. Sign that mailbox in once with the normal app: `mc accounts add-oauth google
   test@gmail.com`. The harness reuses the refresh token the app put in your
   keychain (`secret-tool lookup service mail-cat username <address>`). Google
   refresh tokens do not rotate, so the app is undisturbed. No token file exists
   outside the keychain; access tokens live in memory only and are never printed.
3. `export MAILCAT_ITEST_GMAIL_CONFIRM=test@gmail.com` (the exact address).
4. `python3 tests/itest/gmail/selftest.py` (offline checks of the helpers and guards),
   then `tests/itest/scenarios.py gmail_sync gmail_labels_mc_to_server gmail_server_to_mc gmail_delete_permanent gmail_model_default`
   (plain `scenarios.py` runs them after the 11 others).

Each Gmail scenario reseeds the sandbox, then runs `reset.sh` with
`ITEST_GMAIL=1`, which adds the account to the isolated server's config
(`oauth_refresh_token`, read from your keychain, is moved into the private
keyring at startup by the server's secret ingest and the config backup is
deleted) and trusts the system CA store next to the private CA.
`gmail/seed.py` creates 12 messages `<mcitest-NNN@mc-itest.local>` and labels
including `mc-itest/Clienti/Café`, `mc-itest/A/B/C`, `mc-itest/Presentations`,
`mc-itest/Old Trash`, `mc-itest/Inbox Zero`, `work`/`Work` twins, a quote and a
backslash name (Gmail may refuse the last few; they are skipped then).

**Cost.** Every scenario is a fresh full IMAP sync; Gmail caps IMAP download at
~2.5 GB/day and throttles repeated logins. Keep the fixture small.

**Sandbox and guards.** All test messages carry the label `mc-itest`, all test
labels are `mc-itest/<name>`; the harness never creates, renames or deletes any
other label.

- Nothing runs unless `MAILCAT_ITEST_GMAIL_CONFIRM` is set. Only that address is
  looked up in the keychain; entries are never enumerated.
- Every change goes through `Gmail.mutate`, which re-reads `users.getProfile`
  (must equal the confirmed address) and then refuses unless each target message
  carries `mc-itest` (so `batchDelete`/`batchModify` with any other id is refused),
  each target label is under `mc-itest/`, new labels are under that prefix, and
  inserted messages carry `mc-itest`. The label can't be removed from a message.
- `seed.py` resets only messages carrying `mc-itest` and labels under `mc-itest/`
  (Spam/Trash/Drafts fixtures carry the label too). It prints the address first.
- Scenarios call mail-cat operations (labels, archive, delete, permanent delete)
  only on messages with the seeded `mcitest-` Message-ID prefix.
- `gmail/selftest.py` checks all of this offline against a fake transport.

**What is still true.** mail-cat syncs the account's *whole* All Mail: a
read-only download that spends the daily bandwidth, and every message lands in
the local test DB under `.state/`. The sandbox protects other mail from
destructive changes, not from exposure, so the mailbox should still be dedicated.
