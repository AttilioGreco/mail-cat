# Release for people who know nothing about the project — design

**Date:** 2026-11-06
**Status:** approved in chat, implementing
**Supersedes:** `2026-07-07-ci-tauri-packaging-design.md` (approved, never implemented; its
"out of scope" list no longer holds: the audience changed).

## Intent

A git tag `v*` produces a real release on GitHub that someone who has never heard of
mail-cat can download, install with a double click, and keep up to date without ever
opening a terminal. The server's background sync with the window closed is the main
feature: closing the window stops nothing, and there is no tray icon.

## Decisions (the user's)

- Audience: people who know nothing about the project.
- **No code signing** (no Apple Developer ID, no Windows certificate). Users have to trust
  the first launch; a page next to the downloads explains how.
- **Auto-update in the first release.**
- **Published on GitHub** (`origin`, GitHub Actions + GitHub Releases), not Gitea.
- Closing the window: nothing visible happens, the server keeps syncing. No tray icon.

## Calls made by Claude (reviewable)

- macOS: Apple Silicon (`aarch64-apple-darwin`) only. Ad-hoc signing (`signingIdentity: "-"`),
  required for arm64 binaries to run at all.
- Windows: NSIS installer in per-user mode (`installMode: "currentUser"`, no admin) — the
  updater and the per-user autostart both fit it better than MSI. MSI is not produced.
- Linux: AppImage (auto-updates), deb and rpm (update through the package manager or a new
  download if the Tauri updater does not support them in the version in use — verify).
- Updates are never silent: the UI shows "version X available", the user confirms.
- Uninstall removes the autostart and keeps database, index, config and keyring secrets.
- CI on every push/PR (fmt, clippy `-D warnings`, tests, frontend typecheck + lint) so a
  tag never builds a broken tree.
- No downgrade path; database migrations run at server start as today.

## Parts

### 1. `mail-cat-server service …` (single source of truth for the autostart)

Subcommands: `install`, `uninstall`, `start`, `stop`, `status`; no subcommand = run the
server as today. All per-user, no admin/root, idempotent (install twice = ok, uninstall
when absent = ok). `install` registers *and* starts. The registered command is the
absolute path of the running executable (`std::env::current_exe()`), so it points into
the installed bundle wherever it is.

- **Linux:** systemd user unit `~/.config/systemd/user/mail-cat-server.service`
  (content as `packaging/systemd/mail-cat-server.service`, ExecStart = current exe,
  `Restart=on-failure`, `MALLOC_ARENA_MAX=2`), `systemctl --user daemon-reload`,
  `enable --now`. A unit already shipped in `/usr/lib/systemd/user` by deb/rpm is
  respected (enable it rather than writing a second one).
- **macOS:** LaunchAgent `~/Library/LaunchAgents/com.mailcat.server.plist` with
  `RunAtLoad`, `KeepAlive` (restart on crash), stdout/stderr to
  `~/Library/Logs/mail-cat/`, loaded with `launchctl bootstrap gui/$UID`
  (`bootout` to remove).
- **Windows:** Scheduled Task "at log on" for the current user, hidden, no admin, restart
  on failure, started immediately by `install`. Not a Windows Service: a service runs in
  another session and cannot see the user's Credential Manager, where the OAuth tokens
  live. The server must not open a console window (`windows_subsystem = "windows"` or
  equivalent for the server binary when launched by the task; logs go to a file under
  the local app data dir).
- `stop`/`start` exist for the updater (Windows cannot overwrite a running exe).
- The justfile `install` calls `mail-cat-server service install` instead of copying the
  unit by hand.

### 2. Cross-platform paths

Today config, database, index and the UI path are built from `$HOME` with XDG layout,
which is wrong on macOS and broken on Windows (no `HOME`, falls back to `./data`). Use
the `dirs` crate (already a workspace dependency): config dir, data dir, local data dir
per OS. Linux paths must stay exactly as today (`~/.config/mail-cat/config.toml`,
`~/.local/share/mail-cat/…`) so existing installs keep their data. `MAIL_CAT_CONFIG`
still overrides. A missing config file means defaults (first run needs no config.toml;
accounts are added from the UI).

### 3. Bundling (Tauri)

- Server and CLI ship inside every bundle, next to the UI, and are found at runtime
  relative to the UI executable / resource dir (not `~/.local/bin`).
- ONNX Runtime: fastembed uses `ort-download-binaries`; make sure the native runtime the
  server needs is inside each bundle (or statically linked) on every OS.
- deb/rpm keep their current `files` mapping (binaries in `/usr/bin`, user unit in
  `/usr/lib/systemd/user`); postinst does not touch user sessions — the UI registers the
  autostart on first launch for every install type.
- First launch (all OS): the UI checks `service status` and runs `service install` if the
  autostart is missing or points to another executable (moved/updated app). One call,
  idempotent. If the server is not reachable the UI tries `service start` once and shows
  a clear message if that fails.

### 4. Auto-update

- `tauri-plugin-updater` (+ `tauri-plugin-process` for relaunch), endpoint
  `https://github.com/AttilioGreco/mail-cat/releases/latest/download/latest.json`,
  update artifacts signed with a Tauri updater key (minisign) whose private part lives in
  GitHub secrets (`TAURI_SIGNING_PRIVATE_KEY`, `TAURI_SIGNING_PRIVATE_KEY_PASSWORD`); the
  public key is in `tauri.conf.json`. Generating the key pair is a step for the user,
  documented in `docs/release.md`.
- Check at startup and every few hours; a small non-modal banner "Versione X disponibile —
  Aggiorna"; on confirm: download → verify → `service stop` → install → relaunch (the
  relaunched UI re-runs the first-launch check, which re-registers and starts the server
  from the new executable).
- Server, CLI and UI always update together because they live in one bundle.

### 5. GitHub Actions

- `ci.yml` (push + PR): Linux job with Tauri system deps, frontend build (bun), `cargo
  fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test
  --workspace`; frontend job: typecheck, eslint, `lint:fsd`.
- `release.yml` (tag `v*`): matrix `ubuntu-24.04` (22.04 is too old for the static ONNX Runtime: glibc 2.38+),
  `macos-latest` (arm64), `windows-latest`; build server + CLI release, then
  `tauri-apps/tauri-action` to bundle, sign updater artifacts and upload to a **draft**
  GitHub Release with `latest.json`. Version comes from the tag; the workflow fails if
  the tag does not match the version in `tauri.conf.json` / Cargo (or sets it — builder's
  choice, documented).
- Caching for cargo and bun.

### 6. Docs

`docs/release.md`: how to cut a release (bump, tag, publish the draft), how to create the
updater key and the secrets. `docs/install.md` (linked from the release body): download
per OS, first launch without signing (macOS: System Settings → Privacy & Security → Open
anyway; Windows: "More info → Run anyway"), where data lives, how to uninstall.

## Non-goals

Code signing and notarization, Intel macOS, Linux arm64, Flatpak/Snap (sandboxing fights
a systemd user unit), MSI, silent updates, downgrades, a tray icon, Windows Service.

## Anti-goals

- An install that needs a terminal or a hand-written config to get mail syncing.
- Closing the window stopping the sync.
- An update leaving UI and server on different versions, or failing on Windows because
  the server exe is locked.
- Breaking existing Linux installs (paths, the unit name `mail-cat-server.service`).

## Left to the builder

Exact Tauri config keys, how the UI locates the bundled server per OS, NSIS hooks, how
ORT is bundled, logging location details, the version-sync mechanism, test strategy
(a runnable self-check for `service install → status → uninstall` on the current OS).
