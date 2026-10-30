set dotenv-load := true
set shell := ["fish", "-c"]

# mostra i comandi disponibili
default:
    @just --list --unsorted

# ── Dev ──────────────────────────────────────────────────────────────────────

# avvia tutto: server gRPC + UI Tauri (due processi in parallelo)
dev:
    @echo "→ avvio server + UI..."
    cargo run -p mc-server & sleep 3; and just ui

# avvia solo il server gRPC
server:
    cargo run -p mc-server

# avvia solo la UI Tauri (richiede il server già in ascolto)
[working-directory: 'crates/mc-ui']
ui:
    cargo tauri dev

# ── Build ─────────────────────────────────────────────────────────────────────

# compila tutto il workspace (Rust)
build:
    cargo build --workspace

# compila i tre binari in release, senza generare pacchetti
[working-directory: 'crates/mc-ui']
build-release:
    # --no-bundle salta deb/rpm/AppImage ma esegue comunque il build del frontend.
    cargo build --workspace --release --exclude mc-ui
    NO_STRIP=1 cargo tauri build --no-bundle

# genera i pacchetti deb + rpm + AppImage, con server e CLI inclusi
[working-directory: 'crates/mc-ui']
package: build-release
    # I binari non-UI devono già esistere: il bundler li copia, non li compila.
    NO_STRIP=1 cargo tauri build
    @echo ""
    @echo "→ pacchetti in target/release/bundle/:"
    @ls -1 ../../target/release/bundle/*/* 2>/dev/null || true

# ── Versione ─────────────────────────────────────────────────────────────────

# mostra la versione corrente (workspace Cargo.toml = unica sorgente di verità)
version:
    @grep -m1 '^version' Cargo.toml | cut -d'"' -f2

# allinea tauri.conf.json alla versione del workspace
version-sync:
    #!/usr/bin/env bash
    # Due file separati che divergono in silenzio: senza questo il pacchetto
    # finisce etichettato con una versione diversa dai binari che contiene.
    set -euo pipefail
    V=$(grep -m1 '^version' Cargo.toml | cut -d'"' -f2)
    CUR=$(jq -r .version crates/mc-ui/tauri.conf.json)
    if [ "$V" = "$CUR" ]; then
        echo "✓ versione allineata: $V"
        exit 0
    fi
    jq --arg v "$V" '.version = $v' crates/mc-ui/tauri.conf.json > /tmp/tauri.conf.json.$$
    mv /tmp/tauri.conf.json.$$ crates/mc-ui/tauri.conf.json
    echo "→ tauri.conf.json: $CUR → $V"

# ── Test & Check ──────────────────────────────────────────────────────────────

# esegui tutti i test
test:
    cargo test --workspace

# check rapido senza build artifact
check:
    cargo check --workspace

# clippy su tutto
lint:
    cargo clippy --workspace -- -D warnings

# ── Integration test (Stalwart locale, vedi tests/itest/README.md) ─────────

# avvia lo Stalwart di test senza azzerarlo
itest-up:
    docker compose -f tests/itest/compose.yaml up -d

# azzera Stalwart (alice/bob, mailbox standard) e riavvia il mail-cat-server isolato su :50061
itest-reset:
    tests/itest/reset.sh

# aggiunge messaggi generati (es: just itest-seed -n 50 INBOX Projects)
itest-seed *args:
    tests/itest/seed.py {{args}}

# esegue gli scenari, ognuno da reset+seed (es: just itest flags reappear)
itest *args:
    tests/itest/scenarios.py {{args}}

# scenari Gmail veri contro la casella di test (tests/itest/README.md, "Gmail scenarios"):
# richiede MAILCAT_ITEST_GMAIL_CONFIRM e MAILCAT_GOOGLE_CLIENT_ID/SECRET (anche da .env) e il login `mc accounts add-oauth`
gmail-integration-test:
    #!/usr/bin/env fish
    for v in MAILCAT_ITEST_GMAIL_CONFIRM MAILCAT_GOOGLE_CLIENT_ID MAILCAT_GOOGLE_CLIENT_SECRET
        set -q $v; or begin; echo "manca $v (vedi tests/itest/README.md)"; exit 1; end
    end
    python3 tests/itest/gmail/selftest.py; or exit 1
    tests/itest/scenarios.py gmail_sync gmail_labels_mc_to_server gmail_server_to_mc gmail_delete_permanent gmail_model_default

# benchmark di sync con corpus da ~3000 messaggi e proxy che conta i byte
# (es: just itest-bench branch; just itest-bench main --root ../mail-cat-bench-main)
itest-bench label *args:
    tests/itest/bench.py {{label}} {{args}}

# ferma il server isolato e distrugge Stalwart con i suoi volumi
itest-down:
    tests/itest/reset.sh --stop
    docker compose -f tests/itest/compose.yaml down -v

# ── CLI ──────────────────────────────────────────────────────────────────────

# esegui il CLI (es: just cli -- accounts list)
cli *args:
    cargo run -p mc-cli -- {{args}}

# watch live: mostra eventi INBOX in tempo reale (richiede il server in ascolto)
watch:
    cargo run -p mc-client --example watch

# ── Frontend ─────────────────────────────────────────────────────────────────

# installa dipendenze npm del frontend
[working-directory: 'crates/mc-ui/src-ui']
ui-install:
    bun install

# type-check + build frontend (senza Tauri)
[working-directory: 'crates/mc-ui/src-ui']
ui-build:
    bun run build

# avvia solo il dev server Vite (senza finestra Tauri, utile per sviluppo UI puro)
[working-directory: 'crates/mc-ui/src-ui']
ui-dev:
    bun run dev

# ── Profiling ─────────────────────────────────────────────────────────────────

# sostituisce il server installato con una build release + simboli di debug e lo riavvia
install-profiling:
    cargo build -p mc-server --profile profiling
    systemctl stop --user mail-cat-server.service
    cp target/profiling/mail-cat-server ~/.local/bin/
    systemctl --user restart mail-cat-server
    @echo "✓ server di profiling attivo — es: samply record -p (pgrep -x mail-cat-server)"

# ── Install ───────────────────────────────────────────────────────────────────

# installa server + UI + CLI in ~/.local (no root) — per i pacchetti usa `just package`
install: build-release
    @echo "→ binari..."
    mkdir -p ~/.local/bin
    systemctl stop --user mail-cat-server.service
    cp target/release/mail-cat-server ~/.local/bin/
    cp target/release/mail-cat-ui ~/.local/bin/
    cp target/release/mc ~/.local/bin/

    @echo "→ config template (non sovrascrive se esiste già)..."
    mkdir -p ~/.config/mail-cat
    test -f ~/.config/mail-cat/config.toml; or cp deploy/config.toml.example ~/.config/mail-cat/config.toml
    grep -Eq '^(db_path|index_path)\s*=\s*"\./' ~/.config/mail-cat/config.toml; and echo "⚠ config.toml usa un path relativo per db_path/index_path: con systemd si risolve contro \$HOME, non contro il repo. Usa un path assoluto (es. ~/.local/share/mail-cat/db)." ; or true

    @echo "→ systemd user service..."
    mkdir -p ~/.config/systemd/user
    cp deploy/mail-cat-server.service ~/.config/systemd/user/
    systemctl --user daemon-reload
    systemctl --user enable mail-cat-server
    systemctl --user restart mail-cat-server

    @echo "→ icona..."
    mkdir -p ~/.local/share/icons/hicolor/32x32/apps
    mkdir -p ~/.local/share/icons/hicolor/64x64/apps
    mkdir -p ~/.local/share/icons/hicolor/128x128/apps
    mkdir -p ~/.local/share/icons/hicolor/256x256/apps
    cp crates/mc-ui/icons/32x32.png   ~/.local/share/icons/hicolor/32x32/apps/mail-cat.png
    cp crates/mc-ui/icons/64x64.png   ~/.local/share/icons/hicolor/64x64/apps/mail-cat.png
    cp crates/mc-ui/icons/128x128.png ~/.local/share/icons/hicolor/128x128/apps/mail-cat.png
    cp crates/mc-ui/icons/icon.png    ~/.local/share/icons/hicolor/256x256/apps/mail-cat.png
    gtk-update-icon-cache -f -t ~/.local/share/icons/hicolor; or true

    @echo "→ desktop entry..."
    mkdir -p ~/.local/share/applications
    cp deploy/mail-cat.desktop ~/.local/share/applications/
    update-desktop-database ~/.local/share/applications; or true

    @echo "✓ mail-cat installato — server attivo via systemd user service"
