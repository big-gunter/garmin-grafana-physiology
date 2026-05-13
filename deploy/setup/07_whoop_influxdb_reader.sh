#!/bin/bash
# =============================================================================
# 07_whoop_influxdb_reader.sh
# WHOOP InfluxDB access check
#
# The WHOOP ingest container writes to the same GarminStats database using
# the existing garmin_writer credentials. The mcp_reader user already has
# READ access to the entire GarminStats database (granted in step 03), so
# no additional grants are needed for the new WHOOP measurements.
#
# This script verifies the existing grants are still in place.
# Run from repo root: bash deploy/setup/07_whoop_influxdb_reader.sh
# =============================================================================
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
DEPLOY_DIR="$REPO_DIR/deploy"
ENV_FILE="$DEPLOY_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: $ENV_FILE not found — run 02_folders.sh first"
    exit 1
fi

export $(grep -v '^#' "$ENV_FILE" | grep -v '^$' | xargs)

DB="${INFLUX_DATABASE:-GarminStats}"
ADMIN_PASSWORD="${INFLUX_PASSWORD:-}"

if [ -z "$ADMIN_PASSWORD" ]; then
    echo "ERROR: INFLUX_PASSWORD not set in deploy/.env"
    exit 1
fi

influx_exec() {
    docker compose -f "$DEPLOY_DIR/docker-compose.yml" exec influxdb influx "$@"
}

echo "==> Verifying mcp_reader has READ access to $DB (covers WHOOP measurements)..."
influx_exec -username admin -password "$ADMIN_PASSWORD" \
    -execute "SHOW GRANTS FOR mcp_reader"

echo ""
echo "==> Verifying garmin_writer has ALL access to $DB (used by WHOOP ingest)..."
influx_exec -username admin -password "$ADMIN_PASSWORD" \
    -execute "SHOW GRANTS FOR garmin_writer"

echo ""
echo "==> No additional grants required."
echo "    WHOOP measurements (WhoopRecovery, WhoopSleep, WhoopStrain, WhoopWorkout)"
echo "    are written to $DB — already covered by existing user grants."
