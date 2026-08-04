#!/usr/bin/env bash
# Build the Tauri frontend, but only when its sources actually changed.
#
# Why: vite rewrites src-ui/dist on every run, even when the output bytes are
# identical. Tauri's build script watches that directory, so a fresh mtime alone
# forces a full recompile *and relink* of mc-ui — roughly 30s added to every
# `just install`, for no change at all.
#
# We hash the inputs instead of trusting mtimes, and skip the build when the
# hash matches the stamp written by the previous run.
#
# Correctness note: the hash must cover EVERY input that can change the bundle.
# Missing one would ship a stale UI, which is far worse than a slow build — so
# this walks the whole src-ui directory and excludes only generated/vendored
# paths, rather than listing the inputs it knows about.
set -euo pipefail

cd "$(dirname "$0")/../crates/mc-ui/src-ui"

STAMP="dist/.sources-hash"

current_hash() {
    # -print0/-z keeps filenames with spaces intact; sort makes the walk order
    # stable so the hash does not depend on the filesystem's readdir order.
    find . \
        \( -name node_modules -o -name dist -o -name .git \) -prune -o \
        -type f -print0 \
    | sort -z \
    | xargs -0 sha256sum \
    | sha256sum \
    | cut -d' ' -f1
}

HASH="$(current_hash)"

if [ -f "$STAMP" ] && [ "$(cat "$STAMP")" = "$HASH" ]; then
    echo "frontend: sorgenti invariati, build saltata ($(echo "$HASH" | cut -c1-12))"
    exit 0
fi

bun run build

# Written only after a successful build: a failed build must not mark the tree
# as up to date, or the next run would skip it and package a stale dist.
echo "$HASH" > "$STAMP"
echo "frontend: build completata ($(echo "$HASH" | cut -c1-12))"
