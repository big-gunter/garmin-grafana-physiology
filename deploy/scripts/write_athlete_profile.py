#!/usr/bin/env python3
"""
write_athlete_profile.py — Write an AthleteProfile record to InfluxDB.

Writes physiological constants for an athlete.  Run this once after deploying
a new stack, or whenever constants change (new observed HRmax, updated LTHR,
revised RHR floor, etc.).  Version is auto-incremented from the existing record
so the history stays auditable.

The ingest system reads the most recent AthleteProfile record at startup.
After writing a new record, restart the garmin-fetch-data container to pick it
up, then run compute_physiology_backfill.py --backfill to recompute historic
Karvonen zones with the updated values.

Idempotent: if a record with the same hrmax_bpm already exists in InfluxDB,
prints a confirmation and skips writing.  Pass --force to overwrite anyway.

Usage (inside container):
  docker compose exec garmin-fetch-data \\
    python /app/scripts/write_athlete_profile.py \\
      --athlete-id connor_cooper --hrmax 200 --rhr-floor 59 --lthr 170 \\
      --hrmax-source observed_200bpm

Usage (local, with env loaded):
  source <(grep -v '^#' deploy/.env | sed 's/^/export /') \\
    && python deploy/scripts/write_athlete_profile.py \\
         --athlete-id jon_cooper --hrmax 183 --rhr-floor 47 --lthr 136

Env vars required: INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME,
                   INFLUXDB_PASSWORD, INFLUXDB_DATABASE
"""
from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

# Importing garmin_fetch initialises the InfluxDB client from env vars.
# No Garmin API auth is triggered.
from garmin_grafana import garmin_fetch  # noqa: E402
from garmin_grafana.athlete_profile import (  # noqa: E402
    AthleteProfile,
    load_athlete_profile,
    write_athlete_profile,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Write an AthleteProfile record to InfluxDB")
    parser.add_argument("--athlete-id",   required=True, help="Athlete identifier (e.g. jon_cooper)")
    parser.add_argument("--hrmax",        required=True, type=int, help="Maximum heart rate (bpm)")
    parser.add_argument("--rhr-floor",    required=True, type=int, help="Resting HR floor for Karvonen zones (bpm)")
    parser.add_argument("--lthr",         required=True, type=int, help="Lactate threshold HR (bpm, reference only)")
    parser.add_argument("--hrmax-source", default="", help="Label for HRmax provenance (e.g. observed_200bpm)")
    parser.add_argument("--notes",        default="", help="Free-text notes stored with the record")
    parser.add_argument(
        "--force", action="store_true",
        help="Write even if a record with the same hrmax_bpm already exists",
    )
    args = parser.parse_args()

    existing = load_athlete_profile(garmin_fetch.influxdbclient, garmin_fetch.INFLUXDB_DATABASE)

    if existing.version > 0 and existing.hrmax_bpm == args.hrmax and not args.force:
        print(
            f"AthleteProfile already exists with hrmax_bpm={existing.hrmax_bpm} "
            f"(version={existing.version}, source={existing.hrmax_source!r}) — skipping.\n"
            f"Pass --force to overwrite."
        )
        return 0

    next_version = existing.version + 1

    profile = AthleteProfile.build(
        hrmax_bpm=args.hrmax,
        rhr_floor_bpm=args.rhr_floor,
        lthr_bpm=args.lthr,
        hrmax_source=args.hrmax_source,
        notes=args.notes,
        version=next_version,
        athlete_id=args.athlete_id,
    )

    write_athlete_profile(
        garmin_fetch.influxdbclient,
        profile,
        influx_version=garmin_fetch.INFLUXDB_VERSION,
    )
    print(
        f"\nAthleteProfile written successfully:\n"
        f"  athlete_id    = {profile.athlete_id}\n"
        f"  hrmax_bpm     = {profile.hrmax_bpm}\n"
        f"  rhr_floor_bpm = {profile.rhr_floor_bpm}\n"
        f"  lthr_bpm      = {profile.lthr_bpm}\n"
        f"  hrr_bpm       = {profile.hrr_bpm}\n"
        f"  hrmax_source  = {profile.hrmax_source}\n"
        f"  version       = {profile.version}\n"
        f"  Z1:  {profile.karvonen_z1_low}–{profile.karvonen_z1_high} bpm\n"
        f"  Z2:  {profile.karvonen_z2_low}–{profile.karvonen_z2_high} bpm\n"
        f"  Z3:  {profile.karvonen_z3_low}–{profile.karvonen_z3_high} bpm\n"
        f"  Z4:  {profile.karvonen_z4_low}–{profile.karvonen_z4_high} bpm\n"
        f"  Z5:  {profile.karvonen_z5_low}–{profile.karvonen_z5_high} bpm\n"
        f"\nNext steps:\n"
        f"  1. Restart the garmin-fetch-data container to load the new profile.\n"
        f"  2. Run: python /app/scripts/compute_physiology_backfill.py --backfill\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
