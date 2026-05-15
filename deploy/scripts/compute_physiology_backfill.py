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
  --days N              Last N days (same behaviour as --backfill with custom window)
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
  1. Query the existing PhysiologyDaily record for each date (GROUP BY *).
  2. Copy ALL tag values (Device, User_ID, Database_Name) from the existing
     record — every tag must match for InfluxDB to treat the write as an
     overwrite rather than a new series.
  3. Pass device_name and user_id overrides into compute_and_write_physiology()
     so the new write lands on the same series.
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

_ExistingRecord = tuple[str | None, str | None, str | None, str | None]
# (device, user_id, database_name, hrmax_est_source)


def _query_existing(influx_db: str, asof_date: str) -> _ExistingRecord:
    """Return (device, user_id, database_name, HRmax_est_source) for the
    existing PhysiologyDaily record on *asof_date*, or (None, None, None, None)
    if no record exists.

    Uses GROUP BY * so that ALL tag values surface in the series key rather
    than being lost in the field projection.  In InfluxDB 1.x a write must
    match every tag to be treated as an overwrite rather than a new series.
    """
    d = date.fromisoformat(asof_date)
    start_z = f"{asof_date}T00:00:00Z"
    end_z = (d + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    q = (
        f'SELECT "HRmax_est_source" FROM "PhysiologyDaily" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f"GROUP BY * LIMIT 1"
    )
    client = garmin_fetch.influxdbclient
    result = client.query(q, database=influx_db)
    for (_, tags), points in result.items():
        t = tags or {}
        device    = t.get("Device")    or None
        user_id   = t.get("User_ID")   or None
        db_name   = t.get("Database_Name") or None
        for row in points:
            return device, user_id, db_name, row.get("HRmax_est_source") or ""
    return None, None, None, None


def _count_series(influx_db: str, where: str) -> int:
    """Return the point count for a WHERE clause (used before DELETE)."""
    client = garmin_fetch.influxdbclient
    q = f'SELECT COUNT("HRmax_est") FROM "PhysiologyDaily" WHERE {where}'
    rows = list(client.query(q, database=influx_db).get_points())
    return int(rows[0]["count"]) if rows else 0


def _delete_orphaned(influx_db: str) -> int:
    """Delete PhysiologyDaily records that are clearly backfill artefacts.

    Removes records where:
      - Device = 'Unknown'   (written before the tag-matching fix)
      - Device = 'backfill'  (written as new-record sentinel)
      - User_ID = 'Unknown'  (written before the User_ID override fix)

    Returns total point count removed.  InfluxDB 1.x DELETE does not report
    row counts, so we query counts before issuing each DELETE.
    """
    client = garmin_fetch.influxdbclient
    deleted = 0

    device_sentinels = ["Unknown", "backfill"]
    for sentinel in device_sentinels:
        where = f'"Device" = \'{sentinel}\''
        n = _count_series(influx_db, where)
        if n > 0:
            client.query(f'DELETE FROM "PhysiologyDaily" WHERE {where}', database=influx_db)
            logging.info(f"--clean: deleted {n} record(s) with Device='{sentinel}'")
            deleted += n
        else:
            logging.info(f"--clean: no records with Device='{sentinel}'")

    # Remove records injected by earlier backfill runs before User_ID fix,
    # but only where User_ID is 'Unknown' (the garmin_obj=None fallback).
    uid_where = '"User_ID" = \'Unknown\''
    n = _count_series(influx_db, uid_where)
    if n > 0:
        client.query(f'DELETE FROM "PhysiologyDaily" WHERE {uid_where}', database=influx_db)
        logging.info(f"--clean: deleted {n} record(s) with User_ID='Unknown'")
        deleted += n
    else:
        logging.info("--clean: no records with User_ID='Unknown'")

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
        "--days", metavar="N", type=int, help="Recompute the last N days"
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
    elif args.days:
        start = today - timedelta(days=args.days - 1)
        end = today
        logging.info(f"Days mode: {start} → {end} ({args.days} days)")
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
            existing_device, existing_user_id, _existing_db, existing_src = _query_existing(
                influx_db, ds
            )

            # Idempotency: record already reflects the current AthleteProfile
            # AND all tags match what we would write — nothing to do.
            if existing_device is not None and "athlete_profile" in (existing_src or ""):
                logging.debug(
                    f"{ds}: already up-to-date "
                    f"(Device='{existing_device}', User_ID='{existing_user_id}', "
                    f"HRmax_est_source='{existing_src}') — skipping"
                )
                skipped += 1
                continue

            # Match ALL existing tag values so InfluxDB treats the write as an
            # overwrite of the same series rather than inserting a new one.
            device  = existing_device  if existing_device  is not None else "backfill"
            user_id = existing_user_id if existing_user_id is not None else None

            if existing_device is None:
                logging.info(f"{ds}: no existing record — writing with Device='backfill'")
            else:
                logging.info(
                    f"{ds}: overwriting Device='{device}', User_ID='{user_id}' "
                    f"(was HRmax_est_source='{existing_src}')"
                )

            garmin_fetch.compute_and_write_physiology(ds, device_name=device, user_id=user_id)
            ok += 1
        except Exception:
            logging.exception(f"Failed for {ds}")
            err += 1

    logging.info(f"Done — {ok} written, {skipped} skipped (already up-to-date), {err} errors")

    if args.clean:
        logging.info(
            "--clean: removing orphaned records "
            "(Device='Unknown', Device='backfill', User_ID='Unknown')"
        )
        n_deleted = _delete_orphaned(influx_db)
        logging.info(f"--clean complete: {n_deleted} total orphaned record(s) removed")

    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
