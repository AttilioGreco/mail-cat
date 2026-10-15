# mail-cat

A local-first desktop email client written in Rust: a headless gRPC daemon that owns
sync, storage and search, plus a Tauri/React UI and a CLI that both talk to it.

Mail lives on your machine — an event-sourced sled store, a Tantivy full-text index,
and credentials in the OS keychain. Nothing is sent anywhere except to your mail provider.

**Status:** alpha, actively developed. Linux is the only packaged target so far.
195 tests pass on `main`.

---

## Features

- **IMAP + SMTP** sync with UID-based incremental fetch, IMAP IDLE live updates, and
  resumable checkpoints.
- **OAuth2 login** for Gmail and Outlook (XOAUTH2, PKCE, loopback redirect), with
  background token refresh and an in-app re-auth prompt when a refresh token dies.
- **Password accounts** too — drop a password in `config.toml` and the daemon moves it
  into the OS keychain and rewrites the file without it.
- **Full-text search** over subject, addresses and bodies (Tantivy), kept up to date as
  a projection of the event stream.
- **Conversations** (threading), contacts index with autocomplete, compose/reply with
  Markdown, flags, move, trash and delete synced back to the server.
- **Lazy body backfill**: initial sync pulls envelopes only, bodies stream in afterwards
  at a configurable rate so large mailboxes don't blow a provider's daily bandwidth cap.
- **Desktop notifications**, keyboard shortcuts, light/dark theme, in-app account
  management and settings.
- **CLI** (`mc`) against the same gRPC API — useful for debugging and scripting.

## Architecture

DDD with CQRS and event sourcing. Domain is pure Rust with no I/O; everything concrete
sits behind traits (ports) implemented in the infrastructure crate.

| Crate | Role |
|---|---|
| `mc-shared` | Cross-cutting types: pagination, errors, telemetry |
| `mc-domain` | Aggregates, value objects, domain events, ports. Zero I/O deps |
| `mc-application` | Commands, queries, buses, handlers, projections, sync service |
| `mc-infrastructure` | IMAP/SMTP, OAuth2, sled event store, Tantivy, keychain |
| `mc-interfaces` | gRPC services and mappers |
| `mc-server` | The daemon: wiring, config, OAuth login/refresh, notifications |
| `mc-client` | Typed async gRPC client |
| `mc-cli` | `mc` — command-line client |
| `mc-ui` | Tauri v2 shell + React 19 frontend (Feature-Sliced Design) |

Dependency flow is one-way: `shared → domain → application → infrastructure/interfaces → server`.
The UI never links the backend; it speaks gRPC through `mc-client`.

Writes go through the command bus, land in the sled event store, and fan out to read-model
and search-index projections via an in-process event bus. Reads hit the projections, never
the event log.

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full design and
[`AdditionalConsideration.md`](AdditionalConsideration.md) for the architectural review
that shaped it.

## Requirements

- Rust (2024 edition — 1.85+; developed on 1.97)
- [`just`](https://github.com/casey/just) and [`bun`](https://bun.sh)
- `protoc` (protobuf compiler)
- Tauri v2 [system dependencies](https://tauri.app/start/prerequisites/) —
  on Linux: `webkit2gtk`, `libsoup3`, `openssl`, `librsvg`

## Quick start

```sh
just ui-install          # frontend deps
just build               # cargo build --workspace
just dev                 # server + UI together
```

Or run the pieces separately: `just server`, then `just ui`.

## Configuration

The server reads `~/.config/mail-cat/config.toml`. Start from
[`deploy/config.toml.example`](deploy/config.toml.example), which documents every key:

```toml
grpc_addr = "127.0.0.1:50051"
# db_path    = "~/.local/share/mail-cat/db"      # sled event store
# index_path = "~/.local/share/mail-cat/index"   # Tantivy index

[[accounts]]
name          = "Personal"
email         = "you@example.com"
provider_type = "imap"
imap_host     = "imap.example.com"
imap_port     = 993
use_tls       = true
smtp_host     = "smtp.example.com"
smtp_port     = 587
username      = "you@example.com"
password      = "app-password"   # moved to the keychain on first read
# daily_download_mb = 2000       # default for Gmail (2.5 GB/day cap); 0 = unlimited
```

`RUST_LOG` always overrides `log_level` from the file.

### OAuth accounts

Gmail and Outlook accounts are added interactively — no password in the config:

```sh
just cli -- accounts add-oauth --provider google you@gmail.com
```

The browser opens, you sign in, and the tokens go straight to the keychain. The same flow
is available from the UI's account settings, which is also where a re-auth prompt appears
if a refresh token is revoked.

## CLI

```sh
just cli -- accounts list
just cli -- mailboxes list <account-id>
just cli -- messages list <mailbox-id>
just cli -- conversations list <account-id>
just cli -- contacts search "sara"
just cli -- sync <account-id> --full
just cli -- watch <mailbox-id>        # live IMAP IDLE events
```

`mc` connects to `http://localhost:50051` by default (`--server` or `MAILCAT_SERVER_URL`).

## Install

```sh
just install    # ~/.local/bin + systemd user service + desktop entry + icons
just package    # deb, rpm and AppImage in target/release/bundle/
```

`just install` needs no root: it installs `mail-cat-server`, `mail-cat-ui` and `mc`, enables
the `mail-cat-server` user service, and drops a config template if you don't have one.

## Development

| Command | What it does |
|---|---|
| `just test` | `cargo test --workspace` |
| `just check` | fast type check |
| `just lint` | clippy, warnings denied |
| `just ui-dev` | Vite dev server only, no Tauri window |
| `just ui-build` | type-check + build the frontend |
| `just version-sync` | align `tauri.conf.json` with the workspace version |
| `just watch` | stream live INBOX events from the server |

Design specs and implementation plans live in [`docs/superpowers/`](docs/superpowers/) —
one document per feature (FSD migration, OAuth, keychain, search projection, packaging,
Gmail native API, token re-auth). `Progess.md` is the early day-by-day build log and stops
at Day 13; the git history is the accurate record after that.

## Known gaps

- Gmail/Outlook go through IMAP with OAuth2; the native API providers are
  [specced](docs/superpowers/specs/2026-07-09-gmail-native-api-provider-design.md) but not built.
- IMAP opens a connection per operation — no pooling yet.
- The GraphQL and HTTP interface modules are empty placeholders; gRPC is the only API.
- Only Linux packaging exists; macOS and Windows builds are untested.

## License

MIT
