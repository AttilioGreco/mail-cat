#!/usr/bin/env bash
# Bump version (Cargo.toml + tauri.conf.json), date the CHANGELOG, run the CI
# checks, commit, tag and push. The tag triggers .github/workflows/release.yml,
# which builds a DRAFT GitHub Release (see docs/release.md).
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info() { echo -e "${BLUE}[i]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

[ $# -ge 1 ] || error "Usage: $0 <version> [--skip-tests] [--no-push]"
VERSION="${1#v}"
TAG="v${VERSION}"
shift
SKIP_TESTS=false
NO_PUSH=false
for arg in "$@"; do
    case "$arg" in
        --skip-tests) SKIP_TESTS=true ;;
        --no-push) NO_PUSH=true ;;
        *) warn "Unknown flag: $arg" ;;
    esac
done

# The prompts treat an empty answer as "yes": a piped run would approve the
# commit, the tag and the push without anyone agreeing.
[ -t 0 ] || error "Run this from a terminal: the confirmation prompts read as 'yes' on empty input."

[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]] || error "Invalid version. Expected X.Y.Z or X.Y.Z-prerelease"

cd "$(git rev-parse --show-toplevel)"
TAURI_CONF=crates/mc-ui/tauri.conf.json
FILES=(Cargo.toml Cargo.lock "$TAURI_CONF" CHANGELOG.md)

# ── Preconditions: reject a release that cannot succeed before touching anything.
git diff-index --quiet HEAD -- || error "Uncommitted changes. Commit or stash them first."

BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$BRANCH" != "main" ]; then
    warn "Not on 'main' (current: ${BRANCH})"
    read -p "Continue anyway? [y/N] " -n 1 -r; echo
    [[ $REPLY =~ ^[Yy]$ ]] || error "Release cancelled"
fi

CURRENT=$(grep -m1 '^version = ' Cargo.toml | cut -d'"' -f2)
[ -n "$CURRENT" ] || error "Could not read the version from Cargo.toml"

git rev-parse -q --verify "refs/tags/${TAG}" > /dev/null && error "Tag ${TAG} already exists locally. Pick a higher version."
git ls-remote --exit-code --tags origin "refs/tags/${TAG}" > /dev/null 2>&1 && error "Tag ${TAG} already exists on origin. Pick a higher version."

# Compare with the highest tag too: a failed release can leave Cargo.toml rewound,
# and then an old number looks new. `sort -V` gets prereleases wrong (rc1 > final),
# good enough to catch re-releasing an old number.
HIGHEST_TAG=$(git tag --list 'v*' | sed 's/^v//' | sort -V | tail -1)
newer() { [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -1)" = "$1" ]; }
if [ -n "$HIGHEST_TAG" ]; then
    newer "$VERSION" "$HIGHEST_TAG" || error "${VERSION} is not newer than the highest released ${HIGHEST_TAG}."
fi
# The very first release may tag the version already in Cargo.toml.
BUMP=true
if [ "$VERSION" = "$CURRENT" ] && [ -z "$HIGHEST_TAG" ]; then
    BUMP=false
elif ! newer "$VERSION" "$CURRENT"; then
    error "${VERSION} is not newer than the current ${CURRENT}."
fi

grep -q '^## \[Unreleased\]' CHANGELOG.md || error "CHANGELOG.md has no '## [Unreleased]' section."
grep -q "^## \[${VERSION}\]" CHANGELOG.md && error "CHANGELOG.md already has an entry for ${VERSION}."
command -v jq > /dev/null || error "jq is required"

success "Preconditions passed (current ${CURRENT}, releasing ${TAG})"

# Undo the release commit if anything fails between committing and tagging.
RELEASE_COMMIT=""
TAGGED=false
rollback() {
    [ -n "$RELEASE_COMMIT" ] && [ "$TAGGED" = false ] && [ "$(git rev-parse HEAD)" = "$RELEASE_COMMIT" ] || return 0
    warn "Release failed after committing: removing the release commit"
    git reset --hard HEAD~1 > /dev/null
}
trap rollback EXIT
revert_files() { warn "Reverting file changes..."; git checkout -- "${FILES[@]}" 2>/dev/null || true; }

# ── 1. Version
if [ "$BUMP" = true ]; then
    info "Bumping version ${CURRENT} → ${VERSION}..."
    sed -i "0,/^version = \".*\"$/s//version = \"${VERSION}\"/" Cargo.toml
    jq --arg v "$VERSION" '.version = $v' "$TAURI_CONF" > "${TAURI_CONF}.tmp" && mv "${TAURI_CONF}.tmp" "$TAURI_CONF"
    cargo check --workspace --quiet || { revert_files; error "cargo check failed after the bump"; }
    success "Cargo.toml, Cargo.lock, tauri.conf.json at ${VERSION}"
fi
[ "$(jq -r .version "$TAURI_CONF")" = "$VERSION" ] || { revert_files; error "tauri.conf.json version differs from ${VERSION} (just version-sync)"; }

# ── 2. CHANGELOG: the Unreleased notes become this version's notes.
info "Dating CHANGELOG.md..."
awk -v ver="$VERSION" -v date="$(date +%Y-%m-%d)" '
    /^## \[Unreleased\]/ { print "## [Unreleased]\n"; print "## [" ver "] - " date; next }
    { print }
' CHANGELOG.md > CHANGELOG.md.tmp && mv CHANGELOG.md.tmp CHANGELOG.md
success "CHANGELOG.md updated"

# ── 3. Same gates as ci.yml, so the tag never builds a tree CI would reject.
if [ "$SKIP_TESTS" = false ]; then
    info "Running fmt, clippy, tests and frontend checks..."
    if cargo fmt --all --check \
        && cargo clippy --workspace --all-targets --quiet -- -D warnings \
        && cargo test --workspace --quiet \
        && (cd crates/mc-ui/src-ui && bunx tsc --noEmit && bun run lint && bun run lint:fsd); then
        success "All checks passed"
    else
        revert_files
        error "Checks failed. Fix them or use --skip-tests."
    fi
else
    warn "Skipping checks (--skip-tests)"
fi

# ── 4. Review and commit
echo
info "Changes to be committed:"
git diff --color=always -- "${FILES[@]}" | head -80
echo
read -p "$(echo -e "${GREEN}Commit and tag ${TAG}?${NC} [Y/n] ")" -n 1 -r; echo
if [[ $REPLY =~ ^[Nn]$ ]]; then
    revert_files
    error "Release cancelled"
fi
git add -- "${FILES[@]}"
git commit -q -m "chore: release ${TAG}"
RELEASE_COMMIT=$(git rev-parse HEAD)
success "Commit created"

git tag -s "$TAG" -m "Release ${TAG}"
TAGGED=true
success "Tag created: ${TAG}"

# ── 5. Push
PUSH_HINT="  git push origin ${BRANCH} && git push origin ${TAG}"
if [ "$NO_PUSH" = true ]; then
    warn "Skipping push (--no-push). To push:"; echo "$PUSH_HINT"
    exit 0
fi
echo
info "Pushing ${BRANCH} and ${TAG} starts the Release workflow (draft GitHub Release)."
read -p "$(echo -e "${GREEN}Push to origin?${NC} [Y/n] ")" -n 1 -r; echo
if [[ $REPLY =~ ^[Nn]$ ]]; then
    warn "Push cancelled. To push later:"; echo "$PUSH_HINT"
    exit 0
fi
git push origin "$BRANCH" && git push origin "$TAG" || error "Push failed"
success "Pushed ${TAG}"
echo
info "Next steps:"
echo "  1. Watch the build: just release-watch"
echo "  2. Check the draft and its assets (latest.json included), then Publish:"
echo "     https://github.com/AttilioGreco/mail-cat/releases"
