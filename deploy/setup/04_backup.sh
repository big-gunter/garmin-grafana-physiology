#!/bin/bash
# =============================================================================
# 04_backup.sh
# Daily backup of InfluxDB and Grafana data for a user stack.
# The username is derived from the repo directory — no arguments needed.
# Add to cron (adjust path to match your clone location):
#   0 2 * * * bash /opt/<username>/deploy/setup/04_backup.sh >> /var/log/<username>-backup.log 2>&1
# =============================================================================
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
USERNAME="$(basename "$REPO_DIR")"
DEPLOY_DIR="$REPO_DIR/deploy"
ENV_FILE="$DEPLOY_DIR/.env"
COMPOSE_FILE="$DEPLOY_DIR/docker-compose.$USERNAME.yml"
BASE="$REPO_DIR"
BACKUP_DIR="$BASE/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RETAIN_DAYS=7

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: $ENV_FILE not found"
    exit 1
fi

if [ ! -f "$COMPOSE_FILE" ]; then
    echo "ERROR: $COMPOSE_FILE not found — run './deploy/stack.sh $USERNAME generate' first"
    exit 1
fi

export $(grep -v '^#' "$ENV_FILE" | grep -v '^$' | xargs)

echo "==> Starting backup $TIMESTAMP for stack: $USERNAME"
mkdir -p "$BACKUP_DIR"

# InfluxDB backup
echo "==> Backing up InfluxDB..."
docker compose -f "$COMPOSE_FILE" exec influxdb \
    influxd backup -portable "/tmp/influx_$TIMESTAMP"
CONTAINER_ID=$(docker compose -f "$COMPOSE_FILE" ps -q influxdb)
docker cp "$CONTAINER_ID:/tmp/influx_$TIMESTAMP" "$BACKUP_DIR/influx_$TIMESTAMP"
docker compose -f "$COMPOSE_FILE" exec influxdb \
    rm -rf "/tmp/influx_$TIMESTAMP"

# Grafana backup
echo "==> Backing up Grafana..."
tar -czf "$BACKUP_DIR/grafana_$TIMESTAMP.tar.gz" \
    -C "$BASE/data" grafana

# Cleanup old backups
echo "==> Removing backups older than $RETAIN_DAYS days..."
find "$BACKUP_DIR" -mtime +"$RETAIN_DAYS" -delete

echo "==> Backup complete"
ls -lh "$BACKUP_DIR" | tail -10
