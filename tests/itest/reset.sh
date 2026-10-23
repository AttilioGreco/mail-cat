#!/usr/bin/env bash
# Wipe and rebuild the whole harness: a fresh Stalwart (volumes dropped) with
# alice/bob, and a fresh isolated mail-cat-server on :50061 syncing alice.
#
# Never touches the real mail-cat (127.0.0.1:50051, ~/.config/mail-cat,
# ~/.local/share/mail-cat): the server gets its own config, data dir, HOME and
# a private D-Bus session with a throwaway gnome-keyring, so the password it
# ingests never reaches the user's real keychain.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
STATE="$HERE/.state"
GRPC_PORT=50061
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
BIN="$TARGET/debug/mail-cat-server"
MC="$TARGET/debug/mc"
CLI="$HERE/stalwart-cli"
ALICE_PW=alice-itest-horse-battery
BOB_PW=bob-itest-horse-battery

log() { printf '\033[1;34m→\033[0m %s\n' "$*"; }

wait_for() { # wait_for <seconds> <cmd...>
    local t=$1; shift
    for _ in $(seq "$t"); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done
    echo "timeout waiting for: $*" >&2; return 1
}

stop_server() {
    local pidfile="$STATE/server.pid" pid
    [[ -f $pidfile ]] || return 0
    pid=$(<"$pidfile")
    # Only kill the pid we started, recognised by our config in its environment
    # (the binary path is no proof: CARGO_TARGET_DIR may differ between runs).
    if tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | grep -qxF "MAIL_CAT_CONFIG=$STATE/config.toml"; then
        kill "$pid"
        wait_for 10 bash -c "! kill -0 $pid"
    fi
    rm -f "$pidfile"
}

if [[ ${1:-} == --stop ]]; then stop_server; exit 0; fi

log "building mail-cat-server + mc"
(cd "$ROOT" && cargo build -q -p mc-server -p mc-cli)

stop_server
if ss -ltn | grep -q "127.0.0.1:$GRPC_PORT "; then
    echo "port $GRPC_PORT is busy and not by our server: stop it first" >&2; exit 1
fi

log "resetting Stalwart (volumes wiped)"
docker compose -f "$HERE/compose.yaml" down -v --remove-orphans >/dev/null 2>&1
docker compose -f "$HERE/compose.yaml" up -d --quiet-pull stalwart >/dev/null 2>&1
wait_for 60 curl -fs http://127.0.0.1:18080/healthz/live

# First boot is bootstrap mode: finish setup, then restart into normal mode.
"$CLI" update Bootstrap --json '{
  "serverHostname": "mail.itest.local", "defaultDomain": "itest.local",
  "requestTlsCertificate": false, "generateDkimKeys": false,
  "tracer": {"@type": "Stdout", "ansi": false, "enable": true, "level": "info",
             "lossy": false, "multiline": false, "buffered": false,
             "events": {}, "eventsPolicy": "exclude"}}' >/dev/null
# ponytail: stop+start, not restart: `restart` trips a stale-endpoint bug in Docker 29.
docker compose -f "$HERE/compose.yaml" stop stalwart >/dev/null 2>&1
docker compose -f "$HERE/compose.yaml" start stalwart >/dev/null 2>&1
wait_for 60 curl -fs http://127.0.0.1:18080/healthz/live

log "TLS certificate (private CA, trusted by mail-cat via SSL_CERT_FILE)"
rm -rf "$STATE" && mkdir -p "$STATE/certs" "$STATE/mc" "$STATE/home" "$STATE/run"
chmod 700 "$STATE/run"
(
    cd "$STATE/certs"
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
        -subj /CN=mailcat-itest-ca -addext basicConstraints=critical,CA:TRUE \
        -addext keyUsage=critical,keyCertSign,cRLSign -keyout ca.key -out ca.pem 2>/dev/null
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=localhost \
        -keyout server.key -out server.csr 2>/dev/null
    # Python 3.13+ verifies strictly: key usage and key identifiers are required.
    cat > ext.cnf <<'EXT'
subjectAltName=DNS:localhost,IP:127.0.0.1
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid
EXT
    openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 3650 \
        -extfile ext.cnf -out server.pem 2>/dev/null
)
python3 - "$STATE/certs" > "$STATE/certs/stalwart.json" <<'EOF'
import json, sys
d = sys.argv[1]
print(json.dumps({
    "certificate": {"@type": "Text", "value": open(f"{d}/server.pem").read()},
    "privateKey": {"@type": "Text", "secret": open(f"{d}/server.key").read()},
}))
EOF
"$CLI" create Certificate --json "$(<"$STATE/certs/stalwart.json")" >/dev/null
"$CLI" create Action/ReloadTlsCertificates >/dev/null

log "users alice, bob"
DOMAIN_ID=$("$CLI" query Domain --json | python3 -c 'import json,sys; print(json.loads(sys.stdin.readline())["id"])')
for u in alice:$ALICE_PW bob:$BOB_PW; do
    "$CLI" create Account/User --json "{\"name\": \"${u%%:*}\", \"domainId\": \"$DOMAIN_ID\",
        \"credentials\": {\"0\": {\"@type\": \"Password\", \"secret\": \"${u#*:}\"}}}" >/dev/null
done

wait_for 30 python3 "$HERE/imapctl.py" caps
for u in alice bob; do
    python3 "$HERE/imapctl.py" --user "$u" ensure-mailboxes
done

log "isolated mail-cat-server on 127.0.0.1:$GRPC_PORT"
cat > "$STATE/config.toml" <<EOF
grpc_addr  = "127.0.0.1:$GRPC_PORT"
db_path    = "$STATE/mc/mail-cat.db"
index_path = "$STATE/mc/index"
ui_bin     = "/bin/false"

[[accounts]]
name          = "alice"
email         = "alice@itest.local"
provider_type = "imap"
imap_host     = "localhost"
imap_port     = 19993
use_tls       = true
username      = "alice@itest.local"
password      = "$ALICE_PW"

[accounts.smtp]
host     = "localhost"
port     = 19465
security = "smtps"
EOF

# The private bus has no service dirs, so nothing gets D-Bus-activated
# (a desktop notification would otherwise spawn the user's notification daemon).
# The child records its own pid so stop_server kills the server, not the
# dbus-run-session wrapper; the bus and keyring die with it.
env HOME="$STATE/home" XDG_DATA_HOME="$STATE/home/.local/share" \
    XDG_CONFIG_HOME="$STATE/home/.config" XDG_RUNTIME_DIR="$STATE/run" \
    MAIL_CAT_CONFIG="$STATE/config.toml" NO_COLOR=1 SSL_CERT_FILE="$STATE/certs/ca.pem" \
    RUST_LOG="${RUST_LOG:-mc_server=info,mc_application=debug,mc_infrastructure=debug}" \
    setsid dbus-run-session --config-file="$HERE/dbus-session.conf" -- sh -c '
        echo itest | gnome-keyring-daemon --unlock --components=secrets >/dev/null
        echo $$ > "$0"
        exec "$1"' "$STATE/server.pid" "$BIN" > "$STATE/server.log" 2>&1 < /dev/null &

wait_for 30 "$MC" --server "http://127.0.0.1:$GRPC_PORT" accounts list
kill -0 "$(<"$STATE/server.pid")" || { tail "$STATE/server.log"; exit 1; }
log "ready — server log: $STATE/server.log"
