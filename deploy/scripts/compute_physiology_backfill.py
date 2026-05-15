#!/usr/bin/env python3
"""
compute_physiology_backfill.py — Recompute PhysiologyDaily records

Rewrites PhysiologyDaily (HRmax_est, HRR, Karvonen zones, RHR_used) for a
date range using the same compute_and_write_physiology() function as the
live garmin-fetch-data container.

Run this after writing or updating the AthleteProfile record in InfluxDB to
propagate corrected zone values across historic dates.

Modes:
  --backfill            Last 90 days (tag-matched overwrite; skips already-updated)
  --date YYYY-MM-DD     Single date (overwrites)
  (no args)             Yesterday and today (overwrites)

Flags:
  --clean               After processing, delete any orphaned PhysiologyDaily
                        records with Device='Unknown' or Device='backfill'

Env vars (same as the garmin-fetch-data container):
  INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD
  INFLUXDB_DATABASE       (default: GarminStats)

Athlete constants are read from the AthleteProfile measurement in InfluxDB
(most recent record at startup).  See write_athlete_profile.py to seed or
update the profile.

Run inside the garmin-fetch-data container:
  docker compose exec garmin-fetch-data \\
    python /app/scripts/compute_physiology_backfill.py --backfill

Or directly (with env loaded):
  source <(grep -v '^#' deploy/.env | sed 's/^/export /') \\
    && python deploy/scripts/compute_physiology_backfill.py --backfill

InfluxDB 1.x tag-key principle
-------------------------------
In InfluxDB 1.x, tags are part of the series primary key.  A write with a
*different* tag value is always an INSERT into a new series — it never
overwrites an existing point.  Backfill scripts must read the existing tags
first and echo them back on the write; otherwise you end up with silent
duplicates at the same timestamp under different tag sets.

This script implements that pattern:
  1. Query the existing PhysiologyDaily record for each date (GROUP BY "Device").
  2. Copy the Device tag value from the existing record.
  3. Pass that device name into compute_and_write_physiology() so the new
     write lands on the same series and silently overwrites the old point.
  4. If no existing record exists, write with Device="backfill" so it is
     clearly labelled and removable via --clean.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

# garmin_grafana.garmin_fetch initialises the InfluxDB client and loads
# AthleteProfile from InfluxDB at import time.  No Garmin API calls.
from garmin_grafana import garmin_fetch  # noqa: E402


# ---------------------------------------------------------------------------
# InfluxDB helpers
# ---------------------------------------------------------------------------

def _query_existing(influx_db: str, asof_date: str) -> tuple[str | None, str | None]:
    """Return (device_tag, HRmax_est_source) for the existing PhysiologyDaily
    record on *asof_date*, or (None, None) if no record exists.

    Uses GROUP BY "Device" so that the tag value surfaces in the series key
    rather than being lost in the field projection.
    """
    d = date.fromisoformat(asof_date)
    start_z = f"{asof_date}T00:00:00Z"
    end_z = (d + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    q = (
        f'SELECT "HRmax_est_source" FROM "PhysiologyDaily" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f'GROUP BY "Device" LIMIT 1'
    )
    client = garmin_fetch.influxdbclient
    result = client.query(q, database=influx_db)
    for (_, tags), points in result.items():
        device = (tags or {}).get("Device") or None
        for row in points:
            return device, row.get("HRmax_est_source") or ""
    return None, None


def _delete_orphaned(influx_db: str, sentinels: list[str]) -> int:
    """Delete PhysiologyDaily records whose Device tag matches any sentinel.

    Returns total rows affected (InfluxDB 1.x DELETE does not report row
    counts, so we count the rows that existed *before* deleting them).
    """
    client = garmin_fetch.influxdbclient
    deleted = 0
    for sentinel in sentinels:
        count_q = (
            f'SELECT COUNT("HRmax_est") FROM "PhysiologyDaily" '
            f"WHERE \"Device\" = '{sentinel}'"
        )
        rows = list(client.query(count_q, database=influx_db).get_points())
        n = int(rows[0]["count"]) if rows else 0
        if n > 0:
            del_q = f'DELETE FROM "PhysiologyDaily" WHERE "Device" = \'{sentinel}\''
            client.query(del_q, database=influx_db)
            logging.info(f"--clean: deleted {n} record(s) with Device='{sentinel}'")
            deleted += n
        else:
            logging.info(f"--clean: no records with Device='{sentinel}' — nothing to remove")
    return deleted


# ---------------------------------------------------------------------------
# Date range helper
# ---------------------------------------------------------------------------

def _date_range(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recompute PhysiologyDaily records (HRmax, zones, HRR)"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--backfill", action="store_true", help="Recompute last 90 days"
    )
    group.add_argument(
        "--date", metavar="YYYY-MM-DD", help="Recompute a single date"
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help=(
            "After processing, delete orphaned PhysiologyDaily records "
            "with Device='Unknown' or Device='backfill'"
        ),
    )
    args = parser.parse_args()

    today = date.today()

    if args.backfill:
        start = today - timedelta(days=89)
        end = today
        logging.info(f"Backfill mode: {start} → {end} (90 days)")
    elif args.date:
        start = end = date.fromisoformat(args.date)
        logging.info(f"Single-date mode: {start}")
    else:
        start = today - timedelta(days=1)
        end = today
        logging.info(f"Default mode: {start} → {end} (yesterday + today)")

    profile = garmin_fetch._athlete_profile
    influx_db = garmin_fetch.INFLUXDB_DATABASE
    logging.info(
        f"Athlete profile: hrmax={profile.hrmax_bpm}, rhr_floor={profile.rhr_floor_bpm}, "
        f"lthr={profile.lthr_bpm}, version={profile.version}, source={profile.hrmax_source}"
    )

    ok = skipped = err = 0
    for ds in _date_range(start, end):
        try:
            existing_device, existing_src = _query_existing(influx_db, ds)

            # Idempotency: record already reflects the current AthleteProfile.
            if existing_device is not None and "athlete_profile" in (existing_src or ""):
                logging.debug(
                    f"{ds}: already up-to-date (Device='{existing_device}', "
                    f"HRmax_est_source='{existing_src}') — skipping"
                )
                skipped += 1
                continue

            # Use the existing Device tag so the write lands on the same series
            # and silently overwrites rather than creating a duplicate series.
            # Fall back to "backfill" (not "Unknown") if no record exists yet.
            device = existing_device if existing_device is not None else "backfill"

            if existing_device is None:
                logging.info(f"{ds}: no existing record — writing with Device='backfill'")
            else:
                logging.info(
                    f"{ds}: overwriting Device='{device}' "
                    f"(was HRmax_est_source='{existing_src}')"
                )

            garmin_fetch.compute_and_write_physiology(ds, device_name=device)
            ok += 1
        except Exception:
            logging.exception(f"Failed for {ds}")
            err += 1

    logging.info(f"Done — {ok} written, {skipped} skipped (already up-to-date), {err} errors")

    if args.clean:
        logging.info("Running --clean: removing orphaned Device='Unknown' and Device='backfill' records")
        n_deleted = _delete_orphaned(influx_db, ["Unknown", "backfill"])
        logging.info(f"--clean complete: {n_deleted} total orphaned record(s) removed")

    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
