#!/bin/bash
# =============================================================================
# 03_influxdb_users.sh
# Creates read-only mcp_reader user in InfluxDB
# Run AFTER docker compose up -d and InfluxDB shows healthy
# =============================================================================
set -euo pipefail

BASE=/opt/physiology
CONTAINER="physiology-influxdb-1"

# Load env
if [ -f $BASE/.env ]; then
    export $(grep -v '^#' $BASE/.env | grep -v '^$' | xargs)
else
    echo "ERROR: $BASE/.env not found"
    exit 1
fi

DB="${INFLUX_DATABASE:-garmin}"
MCP_PASSWORD="${INFLUX_MCP_PASSWORD:-}"
ADMIN_PASSWORD="${INFLUX_PASSWORD:-}"

if [ -z "$MCP_PASSWORD" ]; then
    echo "ERROR: INFLUX_MCP_PASSWORD not set in .env"
    exit 1
fi

echo "==> Waiting for InfluxDB to be healthy..."
for i in {1..30}; do
    if docker exec "$CONTAINER" influx \
        -username admin \
        -password "$ADMIN_PASSWORD" \
        -execute "SHOW DATABASES" &>/dev/null; then
        echo "    InfluxDB is ready"
        break
    fi
    echo "    Waiting... ($i/30)"
    sleep 3
done

echo "==> Creating mcp_reader user..."
docker exec "$CONTAINER" influx \
    -username admin \
    -password "$ADMIN_PASSWORD" \
    -execute "CREATE USER mcp_reader WITH PASSWORD '$MCP_PASSWORD'" 2>/dev/null || \
    echo "    User may already exist, continuing..."

echo "==> Granting READ access on $DB..."
docker exec "$CONTAINER" influx \
    -username admin \
    -password "$ADMIN_PASSWORD" \
    -execute "GRANT READ ON \"$DB\" TO mcp_reader"

echo "==> Verifying grants..."
docker exec "$CONTAINER" influx \
    -username admin \
    -password "$ADMIN_PASSWORD" \
    -execute "SHOW GRANTS FOR mcp_reader"

echo ""
echo "==> Done. mcp_reader has READ-ONLY access to $DB"
