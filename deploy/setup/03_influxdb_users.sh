#!/bin/bash
# =============================================================================
# 03_influxdb_users.sh
# Creates InfluxDB users:
#   - mcp_reader: READ + WRITE access for MCP server and Grafana datasource
#   - garmin_writer: WRITE access for Garmin/WHOOP ingest containers
# Run AFTER the stack is up and InfluxDB shows healthy.
# Run from inside the cloned repo (e.g. /opt/<username>):
#   sudo bash deploy/setup/03_influxdb_users.sh
# =============================================================================
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
USERNAME="$(basename "$REPO_DIR")"
DEPLOY_DIR="$REPO_DIR/deploy"
ENV_FILE="$DEPLOY_DIR/.env"
COMPOSE_FILE="$DEPLOY_DIR/docker-compose.$USERNAME.yml"

echo "==> Creating InfluxDB users for stack: $USERNAME"

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: $ENV_FILE not found — run 02_folders.sh first"
    exit 1
fi

if [ ! -f "$COMPOSE_FILE" ]; then
    echo "ERROR: $COMPOSE_FILE not found — run './deploy/stack.sh $USERNAME generate' first"
    exit 1
fi

export $(grep -v '^#' "$ENV_FILE" | grep -v '^$' | xargs)

DB="${INFLUX_DATABASE:-GarminStats}"
ADMIN_PASSWORD="${INFLUX_PASSWORD:-}"
MCP_PASSWORD="${INFLUX_MCP_PASSWORD:-}"
WRITER_PASSWORD="${INFLUX_WRITER_PASSWORD:-}"

if [ -z "$ADMIN_PASSWORD" ]; then
    echo "ERROR: INFLUX_PASSWORD not set in deploy/.env"
    exit 1
fi
if [ -z "$MCP_PASSWORD" ]; then
    echo "ERROR: INFLUX_MCP_PASSWORD not set in deploy/.env"
    exit 1
fi
if [ -z "$WRITER_PASSWORD" ]; then
    echo "ERROR: INFLUX_WRITER_PASSWORD not set in deploy/.env"
    exit 1
fi

influx_exec() {
    docker compose -f "$COMPOSE_FILE" exec influxdb influx "$@"
}

echo "==> Waiting for InfluxDB to be healthy..."
for i in {1..30}; do
    if influx_exec -username admin -password "$ADMIN_PASSWORD" -execute "SHOW DATABASES" &>/dev/null; then
        echo "    InfluxDB is ready"
        break
    fi
    echo "    Waiting... ($i/30)"
    sleep 3
done

echo "==> Creating mcp_reader user (ALL access)..."
influx_exec -username admin -password "$ADMIN_PASSWORD" \
    -execute "CREATE USER mcp_reader WITH PASSWORD '$MCP_PASSWORD'" 2>/dev/null || \
    echo "    User may already exist, continuing..."

influx_exec -username admin -password "$ADMIN_PASSWORD" \
    -execute "GRANT ALL ON \"$DB\" TO mcp_reader"

echo "==> Creating garmin_writer user (ALL access)..."
influx_exec -username admin -password "$ADMIN_PASSWORD" \
    -execute "CREATE USER garmin_writer WITH PASSWORD '$WRITER_PASSWORD'" 2>/dev/null || \
    echo "    User may already exist, continuing..."

influx_exec -username admin -password "$ADMIN_PASSWORD" \
    -execute "GRANT ALL ON \"$DB\" TO garmin_writer"

echo "==> Verifying grants..."
echo "--- mcp_reader ---"
influx_exec -username admin -password "$ADMIN_PASSWORD" \
    -execute "SHOW GRANTS FOR mcp_reader"

echo "--- garmin_writer ---"
influx_exec -username admin -password "$ADMIN_PASSWORD" \
    -execute "SHOW GRANTS FOR garmin_writer"

echo ""
echo "==> Done."
echo "    mcp_reader    → ALL on $DB"
echo "    garmin_writer → ALL on $DB"
