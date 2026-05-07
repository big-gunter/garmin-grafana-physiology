#!/bin/bash
# =============================================================================
# 04_backup.sh
# Daily backup of InfluxDB and Grafana data
# Add to cron: 0 2 * * * /opt/physiology/deploy/setup/04_backup.sh
# =============================================================================
set -euo pipefail

BASE=/opt/physiology
BACKUP_DIR=$BASE/backups
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RETAIN_DAYS=7

export $(grep -v '^#' $BASE/.env | grep -v '^$' | xargs)

echo "==> Starting backup $TIMESTAMP..."

# InfluxDB backup
echo "==> Backing up InfluxDB..."
docker exec physiology-influxdb-1 influxd backup \
    -portable /tmp/influx_$TIMESTAMP
docker cp physiology-influxdb-1:/tmp/influx_$TIMESTAMP \
    $BACKUP_DIR/influx_$TIMESTAMP
docker exec physiology-influxdb-1 rm -rf /tmp/influx_$TIMESTAMP

# Grafana backup
echo "==> Backing up Grafana..."
tar -czf $BACKUP_DIR/grafana_$TIMESTAMP.tar.gz \
    -C $BASE/data grafana

# Cleanup
echo "==> Removing backups older than $RETAIN_DAYS days..."
find $BACKUP_DIR -mtime +$RETAIN_DAYS -delete

echo "==> Backup complete"
ls -lh $BACKUP_DIR | tail -10
