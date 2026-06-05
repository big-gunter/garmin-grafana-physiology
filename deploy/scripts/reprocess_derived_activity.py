#!/usr/bin/env python3
"""
Reprocess DerivedActivity for activities where cs_mps / rTSS_ts are null.

Reads ActivityGPS from InfluxDB, reconstructs FIT-like records, and re-runs
derive_and_write_activity_metrics_v1 with the fixed best_rolling_mean
(min_periods=90% of window instead of 100%).

Root cause: best_rolling_mean used min_periods=w (all samples must be non-NaN).
GPS speed has 3-8% gaps from signal dropouts. The first trail run had 0.19%
gaps and succeeded; all subsequent activities had 3-8% gaps, making every
rolling window NaN → fit_cs_from_gap_speed returned NaN → cs_mps/rTSS null.

Run inside the garmin-fetch-data container:
  docker compose exec garmin-fetch-data \\
      python /app/scripts/reprocess_derived_activity.py [--dry-run]

Or via docker run:
  docker compose -f deploy/docker-compose.<user>.yml run --rm garmin-fetch-data \\
      python /app/scripts/reprocess_derived_activity.py
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

import influxdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ---------------------------------------------------------------------------
# InfluxDB connection (same env vars as the main pipeline)
# ---------------------------------------------------------------------------
INFLUXDB_HOST     = os.environ.get("INFLUXDB_HOST", "influxdb")
INFLUXDB_PORT     = int(os.environ.get("INFLUXDB_PORT", "8086"))
INFLUXDB_USERNAME = os.environ.get("INFLUXDB_USERNAME", "garmin_writer")
INFLUXDB_PASSWORD = os.environ.get("INFLUXDB_PASSWORD", "")
INFLUXDB_DATABASE = os.environ.get("INFLUXDB_DATABASE", "GarminStats")
GARMIN_DEVICENAME = os.environ.get("GARMIN_DEVICENAME", "")

client = influxdb.InfluxDBClient(
    host=INFLUXDB_HOST,
    port=INFLUXDB_PORT,
    username=INFLUXDB_USERNAME,
    password=INFLUXDB_PASSWORD,
    database=INFLUXDB_DATABASE,
)


def query(q: str) -> list[dict]:
    result = client.query(q, database=INFLUXDB_DATABASE)
    return list(result.get_points())


def get_failing_activity_ids() -> list[tuple[int, str, datetime]]:
    """Return (activity_id, activity_type, start_time) for activities missing cs_mps."""
    # Select both Activity_ID (field) and cs_mps together — InfluxDB returns None
    # for cs_mps when not written, so we can filter in Python.
    all_derived = query(
        'SELECT "Activity_ID", "cs_mps", "hrTSS_ts", time '
        'FROM "DerivedActivity" ORDER BY time ASC'
    )

    # ActivitySummary for sport type and canonical start time
    summaries = query(
        'SELECT "Activity_ID", "activityType", time FROM "ActivitySummary" '
        'WHERE "activityName" != \'END\' ORDER BY time ASC'
    )
    summary_map = {
        int(r["Activity_ID"]): (r.get("activityType", ""), r["time"])
        for r in summaries
        if r.get("Activity_ID") is not None
    }

    results = []
    for r in all_derived:
        act_id = r.get("Activity_ID")
        if act_id is None:
            continue
        act_id = int(act_id)
        if r.get("cs_mps") is not None:
            continue  # already computed
        if act_id not in summary_map:
            continue
        activity_type, ts_str = summary_map[act_id]
        start_time = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        results.append((act_id, activity_type, start_time))

    return results


def get_gps_records(activity_id: int) -> list[dict]:
    """Read ActivityGPS from InfluxDB and return FIT-compatible record dicts."""
    rows = query(
        f'SELECT * FROM "ActivityGPS" WHERE "ActivityID"=\'{activity_id}\' ORDER BY time ASC'
    )
    records = []
    for r in rows:
        ts = datetime.fromisoformat(r["time"].replace("Z", "+00:00"))
        records.append({
            "timestamp":        ts,
            "distance":         r.get("Distance"),
            "enhanced_altitude":r.get("Altitude"),
            "enhanced_speed":   r.get("Speed"),
            "heart_rate":       r.get("HeartRate"),
            "power":            r.get("Power"),
        })
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be reprocessed without writing")
    args = parser.parse_args()

    # Import here so env vars are loaded first
    sys.path.insert(0, "/app")
    try:
        from garmin_grafana.garmin_fetch import derive_and_write_activity_metrics_v1
    except Exception as e:
        logging.error(f"Failed to import derive_and_write_activity_metrics_v1: {e}")
        logging.error("Make sure this script runs inside the garmin-fetch-data container.")
        sys.exit(1)

    failing = get_failing_activity_ids()

    if not failing:
        logging.info("No activities with missing cs_mps found — nothing to reprocess.")
        return

    logging.info(f"Found {len(failing)} activities to reprocess:")
    for act_id, activity_type, start_time in failing:
        logging.info(f"  {act_id}  {activity_type}  {start_time.strftime('%Y-%m-%d %H:%M UTC')}")

    if args.dry_run:
        logging.info("Dry run — exiting without writing.")
        return

    success = 0
    failed = 0
    for act_id, activity_type, start_time in failing:
        records = get_gps_records(act_id)
        if not records:
            logging.warning(f"  {act_id}: no ActivityGPS data found — skipping")
            continue

        logging.info(f"  Reprocessing {act_id} ({activity_type}, {len(records)} GPS pts)...")
        try:
            derive_and_write_activity_metrics_v1(
                activity_id=act_id,
                activity_type=activity_type,
                activity_start_time=start_time,
                all_records_list=records,
            )
            logging.info(f"    ✓ Done")
            success += 1
        except Exception:
            logging.exception(f"    ✗ Failed")
            failed += 1

    logging.info(f"\nReprocessing complete: {success} succeeded, {failed} failed.")

    if success > 0:
        logging.info(
            "Run backfill_training_load.py to update TrainingLoadDaily "
            "with the newly written cs_mps / rTSS_ts values:\n"
            "  python /app/scripts/backfill_training_load.py --from 2026-05-18 --yes"
        )


if __name__ == "__main__":
    main()
