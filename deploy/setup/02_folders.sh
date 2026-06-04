#!/bin/bash
# =============================================================================
# 02_folders.sh
# Creates data directories and prepares deploy/.env for a user stack.
# Run from inside the cloned repo (e.g. /opt/<username>):
#   sudo bash deploy/setup/02_folders.sh
# The username is derived from the repo directory name — no arguments needed.
# =============================================================================
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
USERNAME="$(basename "$REPO_DIR")"
BASE="$REPO_DIR"   # data dirs live alongside the repo at /opt/<username>/

echo "==> Setting up directories for user: $USERNAME"
echo "    Base path: $BASE"
echo ""

echo "==> Creating data directories..."
mkdir -p "$BASE/data/influxdb"
mkdir -p "$BASE/data/grafana"
mkdir -p "$BASE/data/mcp-server"
mkdir -p "$BASE/data/mcp-server-gpt"
mkdir -p "$BASE/garminconnect-tokens"
mkdir -p "$BASE/whoop-tokens"
mkdir -p "$BASE/backups"
mkdir -p "$BASE/wireguard/wg_confs"

echo "==> Setting permissions..."
# Grafana runs as uid 472
chown -R 472:472 "$BASE/data/grafana"
# InfluxDB runs as uid 1500 (confirmed from influxdb:1.11 image)
chown -R 1500:1500 "$BASE/data/influxdb"
# garmin-fetch-data, whoop-fetch-data, and mcp-server run as appuser (uid 1000)
chown -R 1000:1000 "$BASE/garminconnect-tokens" "$BASE/whoop-tokens"
# MCP servers: restrict to owner — stores OAuth tokens
chown -R 1000:1000 "$BASE/data/mcp-server" "$BASE/data/mcp-server-gpt"
chmod 700 "$BASE/data/mcp-server" "$BASE/data/mcp-server-gpt"

echo "==> Patching Grafana dashboard datasource reference..."
DASHBOARD_JSON="$BASE/Grafana_Dashboard/Garmin-Grafana-Dashboard.json"
if [ -f "$DASHBOARD_JSON" ]; then
    sed -i 's/\${DS_GARMIN_STATS}/garmin_influxdb/g' "$DASHBOARD_JSON"
    echo "    Patched: $DASHBOARD_JSON"
else
    echo "    Skipped: $DASHBOARD_JSON not found"
fi

echo "==> Creating deploy/.env from template..."
ENV_FILE="$BASE/deploy/.env"
TEMPLATE="$BASE/deploy/template/.env.template"
if [ ! -f "$ENV_FILE" ]; then
    if [ -f "$TEMPLATE" ]; then
        sed "s/{{USERNAME}}/$USERNAME/g" "$TEMPLATE" > "$ENV_FILE"
    else
        cp "$BASE/deploy/.env.example" "$ENV_FILE"
    fi
    chmod 600 "$ENV_FILE"
    chown root:root "$ENV_FILE"
    echo "    Created: $ENV_FILE"
    echo "    Fill in all values before starting the stack:"
    echo "    nano $ENV_FILE"
else
    echo "    deploy/.env already exists — skipping (delete to reset)"
fi

echo ""
echo "==> Setup complete for '$USERNAME'."
echo "    Next steps:"
echo "    1. Fill in deploy/.env"
echo "    2. ./deploy/stack.sh $USERNAME generate"
echo "    3. Start the stack: ./deploy/stack.sh $USERNAME up"
echo "    4. Create DB users: sudo bash deploy/setup/03_influxdb_users.sh"
