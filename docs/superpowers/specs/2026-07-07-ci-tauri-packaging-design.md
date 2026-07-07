# CI + packaging Tauri multi-OS — design

**Data:** 2026-07-07
**Stato:** approvato, pronto per il piano

## Obiettivo

Portare mail-cat a distribuzione automatica:

1. **CI** su ogni push/PR che tiene verde il repo (lint + test Rust e frontend).
2. **Release automatica** su tag `v*` che produce installer nativi per **Linux, Windows, macOS** via Tauri e li carica su una GitHub Release.
3. Ogni installer contiene **UI + server + CLI** e registra l'**autostart per-utente** del server (background sync a UI chiusa).

## Contesto attuale

Workspace Rust con crate rilevanti:

- `mc-server` → bin `mail-cat-server`, gRPC. Oggi gira come **systemd *user* service** (per-utente, no root); tiene i segreti nel keychain dell'utente.
- `mc-ui` → bin `mail-cat-ui`, **Tauri 2** + React/Vite/bun (`crates/mc-ui/src-ui`), parla col server via gRPC (`mc-client`).
- `mc-cli` → bin `mc`.

`crates/mc-ui/tauri.conf.json` esiste ma ha solo target Linux `deb`+`appimage`. Nessuna CI. La registrazione dell'autostart vive nel `justfile` target `install` ed è solo-Linux (copia il unit file, `systemctl --user enable`).

**Decisione architetturale (confermata):** il server resta un **processo separato** dalla UI (non sidecar, non in-process), così il sync continua a UI chiusa. L'autostart è **per-utente** su tutti gli OS — coerente col systemd *user* attuale e col fatto che il server è legato al keychain/sessione dell'utente, quindi niente servizi machine-wide/admin.

## Componenti

### 1. Sottocomando autostart nel server

`mail-cat-server` guadagna: `service install | uninstall | run`.

- **Cosa fa:** unico punto di verità per registrare/rimuovere l'autostart per-utente del server. `run` è il normale avvio (comportamento attuale del binario).
- **Rami per OS:**
  - **Linux:** scrive+abilita systemd *user* unit in `~/.config/systemd/user/mail-cat-server.service`, `systemctl --user daemon-reload && enable --now`. (Sposta qui la logica oggi nel justfile.)
  - **Windows:** registra Scheduled Task al logon (nascosto, per-utente, no admin) via `schtasks`; `install` lo avvia subito.
  - **macOS:** scrive LaunchAgent plist in `~/Library/LaunchAgents/com.mailcat.server.plist`, `launchctl load`.
- **Dipendenze:** filesystem utente per-OS + tool di sistema (`systemctl`/`schtasks`/`launchctl`).
- **Idempotenza:** `install` non deve fallire se già installato; `uninstall` non deve fallire se assente.
- **Il justfile `install`** chiama `mail-cat-server service install` invece di copiare a mano il unit file.

### 2. Impacchettare server+CLI nell'installer

Il server non è un sidecar → in `tauri.conf.json` si aggiunge `bundle.resources` con `mail-cat-server` e `mc`, così finiscono dentro ogni installer accanto a `mail-cat-ui`.

- Il job di release compila prima `cargo build --release -p mc-server -p mc-cli`, copia i binari dove Tauri li cerca come resources, poi lancia `tauri build`.
- **Target bundle per OS** (`tauri.conf.json` / override CLI):
  - Linux: `deb`, `appimage`, `rpm`
  - Windows: `msi` (WiX)
  - macOS: `dmg` (+ `app`)

### 3. Registrazione autostart in post-install (per installer)

Ogni installer, dopo aver copiato i file, invoca `mail-cat-server service install` (e `uninstall` in rimozione):

- **deb:** script `postinst`/`prerm`. Tauri: `bundle.linux.deb.files`/hooks o maintainer scripts.
- **rpm:** scriptlet `%post`/`%preun`, stessa chiamata.
- **msi:** fragment WiX minimale (`bundle.windows.wix.fragmentPaths`) con una *deferred custom action* che esegue l'exe con args; niente logica dentro WiX.
- **dmg:** il `.dmg` non ha hook di install → l'autostart si registra al **primo avvio della UI**: al lancio la UI chiama `service install` se il LaunchAgent non è ancora presente (chiamata singola e idempotente, distinta dalla robustezza "rilancia se irraggiungibile" che resta fuori scope).

### 4. CI — due workflow GitHub Actions

**`ci.yml`** (push + PR):

- Job *rust*: install deps di sistema Tauri (webkit2gtk ecc.) su Linux, `bun install && bun run build` del frontend (serve `src-ui/dist` per compilare `mc-ui`), poi `cargo fmt --check`, `cargo clippy --workspace -- -D warnings`, `cargo test --workspace`.
- Job *frontend*: `bun install`, `tsc && vite build`, `eslint`.

**`release.yml`** (tag `v*`):

- Matrice: `ubuntu-latest`, `windows-latest`, `macos-latest`.
- Setup: deps di sistema Tauri; su Ubuntu anche `rpm`/`rpmbuild` per il bundle rpm.
- Step: `cargo build --release -p mc-server -p mc-cli` → posiziona i binari come resources → `tauri-apps/tauri-action` (builda frontend + bundle) → carica gli artefatti su una **GitHub Release draft**.

## Error handling

- `service install/uninstall` idempotenti; loggano ma non abortiscono l'installer su tool mancante (l'app resta usabile, l'utente può avviare il server a mano).
- CI: `-D warnings` fa fallire su clippy; fmt check fallisce su diff.

## Testing

- Un self-check runnable per il sottocomando `service`: install→verifica presenza artefatto (unit/plist/task)→uninstall→verifica rimozione, sul solo ramo dell'OS corrente (gli altri rami non sono eseguibili in CI cross-OS). Niente framework.
- CI stessa è la verifica dei workflow (dry-run su un tag di prova).

## Fuori scope (esplicito)

- **Code signing Windows** (cert a pagamento) → senza firma, avviso SmartScreen.
- **Firma + notarizzazione macOS** (Apple Developer ID) → senza, Gatekeeper mette in quarantena il dmg.
- **Universal/Intel macOS** → si builda per l'arch del runner (arm64); Intel dopo se serve.
- **UI che rilancia il server se non raggiungibile** → robustezza, non necessaria per la CI base.
