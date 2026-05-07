#!/bin/bash
# =============================================================================
# 02_folders.sh
# Creates /opt/physiology directory structure with correct permissions
# Run as root after 01_server_setup.sh
# =============================================================================
set -euo pipefail

BASE=/opt/physiology
REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

echo "==> Creating directory structure at $BASE..."
mkdir -p $BASE/{data/influxdb,data/grafana,mcp_server,backups}

echo "==> Setting permissions..."
# Grafana runs as uid 472
chown -R 472:472 $BASE/data/grafana

# InfluxDB runs as uid 1500 (confirmed from influxdb:1.11 image)
chown -R 1500:1500 $BASE/data/influxdb

echo "==> Copying deployment files..."
cp $REPO_DIR/deploy/docker-compose.yml $BASE/docker-compose.yml
cp -r $REPO_DIR/deploy/mcp_server/. $BASE/mcp_server/

echo "==> Creating .env from template..."
if [ ! -f $BASE/.env ]; then
    cp $REPO_DIR/deploy/.env.example $BASE/.env
    chmod 600 $BASE/.env
    chown root:root $BASE/.env
    echo "    .env created - fill in all values before starting stack"
    echo "    nano $BASE/.env"
else
    echo "    .env already exists - skipping (delete it to reset)"
fi

echo ""
echo "==> Folder setup complete."
echo "    Next: fill in $BASE/.env then run: docker compose up -d"
echo "    Then run 03_influxdb_users.sh once InfluxDB is healthy"
