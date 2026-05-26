#!/usr/bin/env python3
"""
backfill_training_load.py — Recompute TrainingLoadDaily records

Rewrites TrainingLoadDaily (TRIMP, TSS, ATL, CTL, TSB) for a date range using
the same compute_and_write_training_load() function as the live pipeline.

Run this after fixing the Device-filter bug in DerivedActivity queries to correct
historical records where activities with TRIMP_Banister_ts=null were being missed
or incorrectly calculated due to device-name drift causing Device-tag mismatches.

IMPORTANT — ATL/CTL chain dependency
--------------------------------------
ATL/CTL are exponentially weighted running averages. Each day's values depend on
the previous day's. Always process dates in ascending chronological order and
always start from the EARLIEST affected date — never backfill a later date in
isolation or the chain will be broken.

Modes:
  --diagnose            Dry-run: print proposed changes without writing anything
  --date YYYY-MM-DD     Recompute a single date (also recomputes forward to today
                        to maintain ATL/CTL chain continuity)
  --from YYYY-MM-DD     Recompute from this date to today (inclusive)
  --days N              Recompute the last N days

Flags:
  --yes                 Skip the "proceed?" confirmation prompt
  --delete-first        DELETE existing TrainingLoadDaily rows before rewriting
                        (required to avoid duplicate series if device tag changed)

Env vars (same as garmin-fetch-data container):
  INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD
  INFLUXDB_DATABASE  (default: GarminStats)

Run inside the garmin-fetch-data container:
  docker compose exec garmin-fetch-data \\
    python /app/scripts/backfill_training_load.py --from 2026-05-18 --diagnose

Then once satisfied:
  docker compose exec garmin-fetch-data \\
    python /app/scripts/backfill_training_load.py --from 2026-05-18 --delete-first

InfluxDB 1.x tag-series note
------------------------------
In InfluxDB 1.x, tags are part of the series primary key. A write with a DIFFERENT
Device tag does NOT overwrite the existing series — it creates a new duplicate.
Use --delete-first to remove the existing series before rewriting, or ensure the
Device tag used in the rewrite matches all existing series for that date.
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


def _date_range(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)


def _query_existing_series(client, influx_db: str, date_str: str) -> list[dict]:
    """Return one dict per existing Device series for TrainingLoadDaily on date_str."""
    d = date.fromisoformat(date_str)
    start_z = f"{date_str}T00:00:00Z"
    end_z = (d + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    q = (
        'SELECT "TRIMP_Banister", "hrTSS", "rTSS", "ATL_7_TRIMP", "CTL_42_TRIMP", '
        '"ATL_7_TSS", "CTL_42_TSS", "activities_used" '
        'FROM "TrainingLoadDaily" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "GROUP BY *"
    )
    result = client.query(q, database=influx_db)
    rows = []
    for (_, tags), points in result.items():
        t = tags or {}
        for row in points:
            rows.append({**t, **row})
    return rows


def _delete_date(client, influx_db: str, date_str: str) -> None:
    d = date.fromisoformat(date_str)
    start_z = f"{date_str}T00:00:00Z"
    end_z = (d + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    q = f"DELETE FROM \"TrainingLoadDaily\" WHERE time >= '{start_z}' AND time < '{end_z}'"
    client.query(q, database=influx_db)


def _diagnose_date(client, influx_db: str, date_str: str) -> None:
    """Print current TrainingLoadDaily and DerivedActivity data for a date."""
    d = date.fromisoformat(date_str)
    start_z = f"{date_str}T00:00:00Z"
    end_z = (d + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

    # Existing TrainingLoadDaily
    existing = _query_existing_series(client, influx_db, date_str)
    if existing:
        for row in existing:
            device = row.get("Device", "?")
            atl = row.get("ATL_7_TRIMP")
            atl_str = f"{float(atl):.4f}" if atl is not None else "None"
            logging.info(
                f"  [existing TrainingLoadDaily] {date_str} Device='{device}': "
                f"TRIMP_Banister={row.get('TRIMP_Banister')}, hrTSS={row.get('hrTSS')}, "
                f"rTSS={row.get('rTSS')}, ATL_TRIMP={atl_str}, "
                f"ATL_TSS={row.get('ATL_7_TSS')}, activities_used={row.get('activities_used')}"
            )
    else:
        logging.info(f"  [existing TrainingLoadDaily] {date_str}: no record")

    # ActivitySummary for the date
    q_acts = (
        'SELECT last("Activity_ID") AS act_id, last("averageHR") AS avgHR, '
        '       last("elapsedDuration") AS dur, last("activityName") AS act_name '
        'FROM "ActivitySummary" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "AND activityName != 'END' "
        f"AND \"Database_Name\"='{influx_db}' "
        'GROUP BY "ActivityID"'
    )
    try:
        res = client.query(q_acts, database=influx_db)
        acts = list(res.get_points())
        if not acts:
            logging.info(f"  [ActivitySummary] {date_str}: no activities")
        for a in acts:
            act_id = a.get("act_id")
            if act_id is None:
                continue
            act_id_i = int(float(act_id))
            dur_val = a.get("dur") or 0
            logging.info(
                f"  [ActivitySummary] {date_str} act_id={act_id_i}: "
                f"act_name={a.get('act_name')}, avgHR={a.get('avgHR')}, dur={float(dur_val):.0f}s"
            )

            # DerivedActivity (no Device filter — show ALL series)
            q_da = (
                'SELECT max("TRIMP_Banister_ts") AS b, max("TRIMP_Edwards_ts") AS e, '
                '       max("hrTSS_ts") AS hr, max("rTSS_ts") AS r, max("bikeTSS_ts") AS bike '
                'FROM "DerivedActivity" '
                f"WHERE time >= '{start_z}' AND time < '{end_z}' "
                f"AND \"ActivityID\"='{act_id_i}' "
                f"AND \"Database_Name\"='{influx_db}'"
            )
            da_res = client.query(q_da, database=influx_db)
            da_pts = list(da_res.get_points())
            if da_pts:
                dp = da_pts[0]
                logging.info(
                    f"  [DerivedActivity] act_id={act_id_i}: "
                    f"TRIMP_Banister={dp.get('b')}, TRIMP_Edwards={dp.get('e')}, "
                    f"hrTSS={dp.get('hr')}, rTSS={dp.get('r')}, bikeTSS={dp.get('bike')}"
                )
            else:
                logging.info(f"  [DerivedActivity] act_id={act_id_i}: no record (will use GPS/summary fallback)")

            # DerivedActivity by Device (show what each device series has)
            q_da_dev = (
                'SELECT last("TRIMP_Banister_ts") AS b, last("hrTSS_ts") AS hr '
                'FROM "DerivedActivity" '
                f"WHERE time >= '{start_z}' AND time < '{end_z}' "
                f"AND \"ActivityID\"='{act_id_i}' "
                f"AND \"Database_Name\"='{influx_db}' "
                'GROUP BY "Device"'
            )
            da_dev_res = client.query(q_da_dev, database=influx_db)
            for (_, tags), pts in da_dev_res.items():
                dev = (tags or {}).get("Device", "?")
                for p in pts:
                    logging.info(
                        f"  [DerivedActivity per-Device] act_id={act_id_i} Device='{dev}': "
                        f"TRIMP_Banister={p.get('b')}, hrTSS={p.get('hr')}"
                    )
    except Exception:
        logging.exception(f"Diagnosis query failed for {date_str}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recompute TrainingLoadDaily records"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--from", dest="from_date", metavar="YYYY-MM-DD",
                       help="Recompute from this date to today (recommended: earliest affected date)")
    group.add_argument("--date", dest="single_date", metavar="YYYY-MM-DD",
                       help="Single date (also recomputes forward to maintain ATL/CTL chain)")
    group.add_argument("--days", metavar="N", type=int,
                       help="Recompute the last N days")
    group.add_argument("--diagnose-only", dest="diagnose_only", metavar="YYYY-MM-DD",
                       help="Diagnose a single date without computing anything")
    parser.add_argument("--diagnose", action="store_true",
                        help="Dry-run: print proposed changes without writing")
    parser.add_argument("--delete-first", action="store_true",
                        help="DELETE existing TrainingLoadDaily rows before rewriting")
    parser.add_argument("--yes", action="store_true",
                        help="Skip confirmation prompt")
    args = parser.parse_args()

    # garmin_grafana.garmin_fetch initialises the InfluxDB client at import time.
    from garmin_grafana import garmin_fetch

    client = garmin_fetch.influxdbclient
    influx_db = garmin_fetch.INFLUXDB_DATABASE
    today = date.today()

    if args.diagnose_only:
        ds = args.diagnose_only
        logging.info(f"=== Diagnosis for {ds} ===")
        _diagnose_date(client, influx_db, ds)
        return 0

    # Determine date range
    if args.from_date:
        start = date.fromisoformat(args.from_date)
        end = today
    elif args.single_date:
        start = date.fromisoformat(args.single_date)
        end = today
    else:
        start = today - timedelta(days=args.days - 1)
        end = today

    date_list = list(_date_range(start, end))
    logging.info(f"Date range: {start} → {end} ({len(date_list)} days)")

    # Diagnose each date
    logging.info("=== Current state (before any changes) ===")
    for ds in date_list:
        _diagnose_date(client, influx_db, ds)

    if args.diagnose:
        logging.info("=== --diagnose mode: no changes made ===")
        return 0

    # Confirmation
    if not args.yes:
        print()
        print(f"About to recompute TrainingLoadDaily for {len(date_list)} days: {start} → {end}")
        if args.delete_first:
            print("WARNING: --delete-first will DELETE existing records before rewriting")
        ans = input("Proceed? [y/N] ").strip().lower()
        if ans != "y":
            print("Aborted.")
            return 1

    # Process
    ok = err = 0
    for ds in date_list:
        try:
            if args.delete_first:
                logging.info(f"{ds}: deleting existing TrainingLoadDaily series...")
                _delete_date(client, influx_db, ds)

            logging.info(f"{ds}: computing TrainingLoadDaily...")
            garmin_fetch.compute_and_write_training_load(ds)
            ok += 1
        except Exception:
            logging.exception(f"Failed for {ds}")
            err += 1
            if err > 3:
                logging.error("Too many consecutive errors; aborting")
                break

    logging.info(f"Done — {ok} written, {err} errors")

    # Final state
    logging.info("=== Final state (after recompute) ===")
    for ds in date_list:
        existing = _query_existing_series(client, influx_db, ds)
        for row in existing:
            device = row.get("Device", "?")
            logging.info(
                f"  {ds} Device='{device}': "
                f"TRIMP_Banister={row.get('TRIMP_Banister')}, hrTSS={row.get('hrTSS')}, "
                f"ATL_7_TSS={row.get('ATL_7_TSS')}, CTL_42_TSS={row.get('CTL_42_TSS')}, "
                f"activities_used={row.get('activities_used')}"
            )

    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
