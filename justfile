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
