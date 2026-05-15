#!/usr/bin/env python3
"""
write_athlete_profile.py — Seed the AthleteProfile record in InfluxDB.

Writes validated physiological constants for the athlete.  Run this once
after deploying a new stack, or whenever constants change (new observed HRmax,
updated LTHR, revised RHR floor, etc.).

The ingest system reads the most recent AthleteProfile record at startup.
After writing a new record, restart the garmin-fetch-data container to pick it
up, then run compute_physiology_backfill.py --backfill to recompute historic
Karvonen zones with the updated values.

Idempotent: if a record with the same hrmax_bpm already exists in InfluxDB,
prints a confirmation and skips writing.  Pass --force to overwrite anyway.

Usage (inside container):
  docker compose exec garmin-fetch-data \\
    python /app/scripts/write_athlete_profile.py

Usage (local, with env loaded):
  source <(grep -v '^#' deploy/.env | sed 's/^/export /') \\
    && python deploy/scripts/write_athlete_profile.py

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

# ── Athlete constants ──────────────────────────────────────────────────────────
# Jon Cooper validated values — update this block when values change, then
# increment version so the history is auditable in InfluxDB.
PROFILE = AthleteProfile.build(
    hrmax_bpm=183,
    rhr_floor_bpm=47,
    lthr_bpm=136,
    hrmax_source="observed_184bpm_2025-02-05",
    notes=(
        "atenolol 25mg suppresses HR; validated max 184 bpm observed 5 Feb 2025 "
        "trail run; zones Karvonen HRR method"
    ),
    version=1,
    athlete_id="jon_cooper",
)
# ──────────────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed AthleteProfile in InfluxDB")
    parser.add_argument(
        "--force", action="store_true",
        help="Write even if a record with the same hrmax_bpm already exists"
    )
    args = parser.parse_args()

    existing = load_athlete_profile(garmin_fetch.influxdbclient, garmin_fetch.INFLUXDB_DATABASE)

    if existing.version > 0 and existing.hrmax_bpm == PROFILE.hrmax_bpm and not args.force:
        print(
            f"AthleteProfile already exists with hrmax_bpm={existing.hrmax_bpm} "
            f"(version={existing.version}, source={existing.hrmax_source!r}) — skipping.\n"
            f"Pass --force to overwrite."
        )
        return 0

    write_athlete_profile(
        garmin_fetch.influxdbclient,
        PROFILE,
        influx_version=garmin_fetch.INFLUXDB_VERSION,
    )
    print(
        f"\nAthleteProfile written successfully:\n"
        f"  athlete_id    = {PROFILE.athlete_id}\n"
        f"  hrmax_bpm     = {PROFILE.hrmax_bpm}\n"
        f"  rhr_floor_bpm = {PROFILE.rhr_floor_bpm}\n"
        f"  lthr_bpm      = {PROFILE.lthr_bpm}\n"
        f"  hrr_bpm       = {PROFILE.hrr_bpm}\n"
        f"  hrmax_source  = {PROFILE.hrmax_source}\n"
        f"  version       = {PROFILE.version}\n"
        f"  Z1:  {PROFILE.karvonen_z1_low}–{PROFILE.karvonen_z1_high} bpm\n"
        f"  Z2:  {PROFILE.karvonen_z2_low}–{PROFILE.karvonen_z2_high} bpm\n"
        f"  Z3:  {PROFILE.karvonen_z3_low}–{PROFILE.karvonen_z3_high} bpm\n"
        f"  Z4:  {PROFILE.karvonen_z4_low}–{PROFILE.karvonen_z4_high} bpm\n"
        f"  Z5:  {PROFILE.karvonen_z5_low}–{PROFILE.karvonen_z5_high} bpm\n"
        f"\nNext steps:\n"
        f"  1. Restart the garmin-fetch-data container to load the new profile.\n"
        f"  2. Run: python /app/scripts/compute_physiology_backfill.py --backfill\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
