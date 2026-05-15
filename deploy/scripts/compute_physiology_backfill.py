#!/usr/bin/env python3
"""
compute_physiology_backfill.py — Recompute PhysiologyDaily records

Rewrites PhysiologyDaily (HRmax_est, HRR, Karvonen zones, RHR_used) for a
date range using the same compute_and_write_physiology() function as the
live garmin-fetch-data container.

Run this after writing or updating the AthleteProfile record in InfluxDB to
propagate corrected zone values across historic dates.

Modes:
  --backfill            Last 90 days (always overwrites existing records)
  --date YYYY-MM-DD     Single date (overwrites)
  (no args)             Yesterday and today (overwrites)

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

Note: importing garmin_grafana.garmin_fetch initialises the InfluxDB client
and loads AthleteProfile from env vars (lazy TCP connection; no Garmin auth).
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


def _date_range(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)


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
    logging.info(
        f"Athlete profile: hrmax={profile.hrmax_bpm}, rhr_floor={profile.rhr_floor_bpm}, "
        f"lthr={profile.lthr_bpm}, version={profile.version}, source={profile.hrmax_source}"
    )

    ok = err = 0
    for ds in _date_range(start, end):
        try:
            garmin_fetch.compute_and_write_physiology(ds)
            ok += 1
        except Exception:
            logging.exception(f"Failed for {ds}")
            err += 1

    logging.info(f"Done — {ok} written, {err} errors")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
