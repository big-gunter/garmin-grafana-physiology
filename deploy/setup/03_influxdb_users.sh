#!/bin/bash
# =============================================================================
# 03_influxdb_users.sh
# Creates InfluxDB users:
#   - mcp_reader: READ-ONLY access for MCP server
#   - garmin_writer: WRITE access for Garmin ingest script
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

DB="${INFLUX_DATABASE:-GarminStats}"
ADMIN_PASSWORD="${INFLUX_PASSWORD:-}"
MCP_PASSWORD="${INFLUX_MCP_PASSWORD:-}"
WRITER_PASSWORD="${INFLUX_WRITER_PASSWORD:-}"

if [ -z "$ADMIN_PASSWORD" ]; then
    echo "ERROR: INFLUX_PASSWORD not set in .env"
    exit 1
fi
if [ -z "$MCP_PASSWORD" ]; then
    echo "ERROR: INFLUX_MCP_PASSWORD not set in .env"
    exit 1
fi
if [ -z "$WRITER_PASSWORD" ]; then
    echo "ERROR: INFLUX_WRITER_PASSWORD not set in .env"
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

echo "==> Creating mcp_reader user (READ-ONLY)..."
docker exec "$CONTAINER" influx \
    -username admin \
    -password "$ADMIN_PASSWORD" \
    -execute "CREATE USER mcp_reader WITH PASSWORD '$MCP_PASSWORD'" 2>/dev/null || \
    echo "    User may already exist, continuing..."

docker exec "$CONTAINER" influx \
    -username admin \
    -password "$ADMIN_PASSWORD" \
    -execute "GRANT READ ON \"$DB\" TO mcp_reader"

echo "==> Creating garmin_writer user (WRITE)..."
docker exec "$CONTAINER" influx \
    -username admin \
    -password "$ADMIN_PASSWORD" \
    -execute "CREATE USER garmin_writer WITH PASSWORD '$WRITER_PASSWORD'" 2>/dev/null || \
    echo "    User may already exist, continuing..."

docker exec "$CONTAINER" influx \
    -username admin \
    -password "$ADMIN_PASSWORD" \
    -execute "GRANT ALL ON \"$DB\" TO garmin_writer"

echo "==> Verifying grants..."
echo "--- mcp_reader ---"
docker exec "$CONTAINER" influx \
    -username admin \
    -password "$ADMIN_PASSWORD" \
    -execute "SHOW GRANTS FOR mcp_reader"

echo "--- garmin_writer ---"
docker exec "$CONTAINER" influx \
    -username admin \
    -password "$ADMIN_PASSWORD" \
    -execute "SHOW GRANTS FOR garmin_writer"

echo ""
echo "==> Done."
echo "    mcp_reader  → READ-ONLY on $DB"
echo "    garmin_writer → ALL on $DB"