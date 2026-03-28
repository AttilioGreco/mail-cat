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

# compila in release
[working-directory: 'crates/mc-ui']
build-release:
    cargo build --workspace --release
    NO_STRIP=1 cargo tauri build

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

# ── Install ───────────────────────────────────────────────────────────────────

# compila release e installa server + UI in ~/.local (no root)
install: build-release
    @echo "→ binari..."
    mkdir -p ~/.local/bin
    cp target/release/mail-cat-server ~/.local/bin/
    cp target/release/mail-cat-ui ~/.local/bin/

    @echo "→ config template (non sovrascrive se esiste già)..."
    mkdir -p ~/.config/mail-cat
    test -f ~/.config/mail-cat/config.toml; or cp deploy/config.toml.example ~/.config/mail-cat/config.toml

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
