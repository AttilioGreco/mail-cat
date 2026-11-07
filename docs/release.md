# Cutting a release

A tag `vX.Y.Z` makes GitHub Actions (`.github/workflows/release.yml`) build installers on Linux (AppImage, deb, rpm), macOS arm64 (app, dmg) and Windows (NSIS, per-user), sign the update artifacts, and upload everything to a draft GitHub Release together with `latest.json`, the manifest the in-app updater reads. When all three succeed, a last job publishes it: `vX.Y.Z` as the latest release, a tag with a suffix (`vX.Y.Z-rc1`) as a pre-release. CI (`ci.yml`) runs fmt, clippy `-D warnings`, tests and the frontend checks on every PR and push to `main`.

## One-time setup

1. Generate the updater key pair (minisign). Keep the private key safe: losing it means installed apps can no longer be updated.

   ```
   cd crates/mc-ui/src-ui
   bun tauri signer generate -w ~/.tauri/mail-cat.key
   ```

   (`cargo tauri signer generate -w ...` does the same.) Choose a password.
2. Put the **public** key (the `.key.pub` file's content) in `crates/mc-ui/tauri.conf.json`, replacing the `pubkey` placeholder under `plugins.updater`. Commit it. Until then the in-app update check fails quietly and `tauri build` cannot sign updates.
3. In GitHub, Settings, Secrets and variables, Actions, add:
   - `TAURI_SIGNING_PRIVATE_KEY`: the content of the private key file (`~/.tauri/mail-cat.key`);
   - `TAURI_SIGNING_PRIVATE_KEY_PASSWORD`: its password.

## Each release

1. Write the notes under `## [Unreleased]` in `CHANGELOG.md` and commit them (`just release-check` shows them).
2. `just release-create X.Y.Z` (from a terminal, on `main`). `scripts/release.sh` refuses a dirty tree or a version that is not newer than both `Cargo.toml` and the highest tag, then bumps `Cargo.toml`, `Cargo.lock` and `tauri.conf.json`, dates the changelog, runs the same checks as CI, and after a confirmation commits `chore: release vX.Y.Z`, creates a GPG-signed tag (`git tag -s`, your `user.signingkey`) and, after a second confirmation, pushes branch and tag. The very first release may use the version already in `Cargo.toml`. `just release-prepare X.Y.Z` stops before the push; `just release-undo` removes a local tag and its release commit.
3. `just release-watch` follows the build. The workflow fails immediately, with a message, if the tag differs from either version.
4. Nothing to click: the `publish` job runs once the three builds are green. If one fails, the release stays a draft (visible only to the owner, under Releases); fix it and release the next version. Installed apps are offered only the latest non-pre-release, because the updater endpoint is `releases/latest/download/latest.json`, so an `-rc` is for testing by hand.

The release body links to `docs/install.md` (first launch without code signing).

## How the pieces fit

- Every bundle carries `mail-cat-server` and `mc` next to the UI executable: `/usr/bin` for deb/rpm, `usr/bin` inside the AppImage, `Contents/MacOS` in the .app, the install folder on Windows. On macOS and Windows they are Tauri `externalBin` entries (`tauri.macos.conf.json`, `tauri.windows.conf.json`), so the workflow copies them to `crates/mc-ui/binaries/<name>-<target-triple>` before bundling. Linux uses explicit `files` mappings from `target/release`. A local macOS/Windows build needs the same `binaries/` files first (`cargo check` there fails without them). The per-OS files are merged over `tauri.conf.json` automatically.
- ONNX Runtime is statically linked into `mail-cat-server` (the `ort` download is a static library), so no extra library ships.
- At launch the UI runs `mail-cat-server service status` and installs or starts the autostart when needed. Debug builds skip this unless `MAILCAT_MANAGE_SERVICE=1`; a custom `MAILCAT_SERVER_URL` also skips it.
- An update stops the server (`service stop`), installs, relaunches; the relaunched UI re-registers the server from the new files.

## Not covered

No code signing or notarization (macOS is ad-hoc signed), no Intel macOS, no MSI.
