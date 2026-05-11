#!/bin/bash
# =============================================================================
# 02_folders.sh
# Creates data directories and prepares deploy/.env for the physiology stack.
# The repo must be cloned to /opt/physiology-repo before running this.
# Run as root: bash deploy/setup/02_folders.sh
# =============================================================================
set -euo pipefail

BASE=/opt/physiology          # data directory (influxdb, grafana, tokens, backups)
REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"  # repo root (e.g. /opt/physiology-repo)

echo "==> Creating data directories..."
mkdir -p $BASE/data/influxdb
mkdir -p $BASE/data/grafana
mkdir -p $BASE/garminconnect-tokens
mkdir -p $BASE/backups

echo "==> Setting permissions..."
# Grafana runs as uid 472
chown -R 472:472 $BASE/data/grafana
# InfluxDB runs as uid 1500 (confirmed from influxdb:1.11 image)
chown -R 1500:1500 $BASE/data/influxdb
# garmin-fetch-data runs as appuser (uid 1000)
chown -R 1000:1000 $BASE/garminconnect-tokens

echo "==> Patching Grafana dashboard datasource reference..."
sed -i 's/\${DS_GARMIN_STATS}/garmin_influxdb/g' \
    "$REPO_DIR/Grafana_Dashboard/Garmin-Grafana-Dashboard.json"

echo "==> Creating deploy/.env from template..."
if [ ! -f "$REPO_DIR/deploy/.env" ]; then
    cp "$REPO_DIR/deploy/.env.example" "$REPO_DIR/deploy/.env"
    chmod 600 "$REPO_DIR/deploy/.env"
    chown root:root "$REPO_DIR/deploy/.env"
    echo "    .env created at $REPO_DIR/deploy/.env"
    echo "    Fill in all values before starting the stack:"
    echo "    nano $REPO_DIR/deploy/.env"
else
    echo "    deploy/.env already exists — skipping (delete to reset)"
fi

echo ""
echo "==> Setup complete."
echo "    Next: fill in deploy/.env then start the stack:"
echo "    cd $REPO_DIR/deploy && docker compose up -d"
