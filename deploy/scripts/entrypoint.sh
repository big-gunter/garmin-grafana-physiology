#!/bin/bash
set -e

# Start supercronic with the app crontab in the background.
# supercronic inherits the Docker process environment, so all
# INFLUXDB_* and other container env vars are available to cron jobs.
supercronic /app/crontab &

# Hand off to the main container process (replaces this shell).
exec "$@"
