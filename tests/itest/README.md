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
Drafts, Junk (special-use) and Projects. Only alice is configured in mail-cat.

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
