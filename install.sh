#!/bin/sh
# observant-agent installer for systemd Linux (amd64/arm64).
# Usage: OBSERVANT_URL=... OBSERVANT_TOKEN=... sh install.sh [path-to-binary]
# With no binary argument, the script expects observant-agent in the
# current directory (release download comes later in M7).
set -eu

[ "$(id -u)" = 0 ] || { echo "run as root (sudo sh install.sh)"; exit 1; }
: "${OBSERVANT_URL:?set OBSERVANT_URL}"
: "${OBSERVANT_TOKEN:?set OBSERVANT_TOKEN}"
BIN="${1:-./observant-agent}"
[ -f "$BIN" ] || { echo "binary not found: $BIN"; exit 1; }

# Find the nologin shell. The path differs between distributions.
NOLOGIN=""
for p in /usr/sbin/nologin /sbin/nologin /usr/bin/nologin; do
    [ -x "$p" ] && { NOLOGIN="$p"; break; }
done
[ -n "$NOLOGIN" ] || NOLOGIN="$(command -v nologin 2>/dev/null || true)"
[ -n "$NOLOGIN" ] || NOLOGIN=/bin/false

id -u observant >/dev/null 2>&1 || useradd --system --no-create-home --shell "$NOLOGIN" observant
if [ -S /var/run/docker.sock ]; then
    echo
    echo "WARNING: docker group access is equivalent to root on this host."
    echo "A member of the docker group can start a container that mounts / and"
    echo "gain full control of the machine. The installer is about to add the"
    echo "observant user to the docker group so that the agent can read"
    echo "container stats. Set OBSERVANT_DOCKER=off and skip this step if that"
    echo "trade is not acceptable."
    echo
    usermod -aG docker observant 2>/dev/null || true
fi

install -m 755 "$BIN" /usr/local/bin/observant-agent
mkdir -p /etc/observant
# umask 077 sets the mode of a new file. A truncating write keeps the mode of
# an existing file, so remove the old file first.
rm -f /etc/observant/agent.env
(
    umask 077
    cat > /etc/observant/agent.env <<EOF
OBSERVANT_URL=$OBSERVANT_URL
OBSERVANT_TOKEN=$OBSERVANT_TOKEN
OBSERVANT_INTERVAL=${OBSERVANT_INTERVAL:-15s}
EOF
)

cat > /etc/systemd/system/observant-agent.service <<'EOF'
[Unit]
Description=observant.computer monitoring agent
After=network-online.target
Wants=network-online.target

[Service]
User=observant
EnvironmentFile=/etc/observant/agent.env
ExecStart=/usr/local/bin/observant-agent
Restart=always
RestartSec=5
# Hardening: the agent only reads /proc, /sys, and the docker socket.
# ProtectSystem=strict already mounts the whole file system read-only, so a
# ReadOnlyPaths=/ line adds nothing.
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now observant-agent

# Remove the staged copies. A binary and a script left in /tmp are world
# readable and easy to replace.
case "$BIN" in
    /tmp/*) rm -f "$BIN" ;;
esac
SELF=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")
case "$SELF" in
    /tmp/*) rm -f "$SELF" ;;
esac

sleep 2
systemctl --no-pager --lines 5 status observant-agent
