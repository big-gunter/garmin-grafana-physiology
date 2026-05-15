#!/usr/bin/env python3
"""
compute_readiness.py — CoachReadiness composite score

Writes one CoachReadiness record per day to InfluxDB.

Modes:
  --backfill            Last 90 days, skip existing records (idempotent)
  --date YYYY-MM-DD     Single date (overwrites if exists)
  (no args)             Yesterday and today (overwrites if exists)

Env vars (same as the rest of the stack):
  INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD
  INFLUXDB_DATABASE  (default: GarminStats)
  The configured user must have WRITE access to the database.

Run from the repo root with .env loaded:
  source <(grep -v '^#' deploy/.env | sed 's/^/export /') \\
    && python deploy/scripts/compute_readiness.py --backfill

Or inside the garmin-fetch-data container:
  docker compose exec garmin-fetch-data \\
    python /app/deploy/scripts/compute_readiness.py --backfill

Requires: influxdb>=5.3.2  (already installed in garmin-fetch-data image)
          python-dotenv     (optional — auto-loads deploy/.env when present)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Auto-load deploy/.env when running directly on the host (not in Docker).
try:
    from dotenv import load_dotenv
    _env = Path(__file__).resolve().parent.parent / ".env"
    if _env.exists():
        load_dotenv(_env)
except ImportError:
    pass

from influxdb import InfluxDBClient


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

INFLUX_HOST = os.environ.get("INFLUXDB_HOST", "localhost")
INFLUX_PORT = int(os.environ.get("INFLUXDB_PORT", "8086"))
INFLUX_USER = os.environ.get("INFLUXDB_USERNAME", "admin")
INFLUX_PASS = os.environ.get("INFLUXDB_PASSWORD", "")
INFLUX_DB   = os.environ.get("INFLUXDB_DATABASE", "GarminStats")

WHOOP_START   = date(2023, 10, 13)
LOOKBACK_DAYS = 30
MIN_HRV_DAYS  = 7
VERSION       = "v1.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# InfluxDB helpers
# ---------------------------------------------------------------------------

def make_client() -> InfluxDBClient:
    client = InfluxDBClient(
        host=INFLUX_HOST, port=INFLUX_PORT,
        username=INFLUX_USER, password=INFLUX_PASS,
    )
    client.switch_database(INFLUX_DB)
    return client


def _q(client: InfluxDBClient, query: str) -> list[dict]:
    return list(client.query(query).get_points())


def _day_z(d: date) -> tuple[str, str]:
    """UTC [start, end) bounds for a single calendar day."""
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return (
        start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        (start + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def _range_z(start: date, end: date) -> tuple[str, str]:
    """UTC bounds covering [start_day, end_day] inclusive."""
    s, _ = _day_z(start)
    _, e = _day_z(end)
    return s, e


def _f(v: object) -> float | None:
    try:
        return float(v) if v is not None else None  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _date_of(row: dict) -> str | None:
    t = row.get("time")
    return t[:10] if t else None


# ---------------------------------------------------------------------------
# Bulk data fetchers  (single query per measurement, keyed by YYYY-MM-DD)
# ---------------------------------------------------------------------------

def fetch_whoop_recovery(client: InfluxDBClient, start: date, end: date) -> dict[str, dict]:
    s_z, e_z = _range_z(start, end)
    rows = _q(client, (
        f'SELECT "hrv_rmssd_milli","resting_heart_rate","recovery_score" '
        f'FROM "WhoopRecovery" '
        f"WHERE time >= '{s_z}' AND time < '{e_z}' ORDER BY time ASC"
    ))
    out: dict[str, dict] = {}
    for r in rows:
        d = _date_of(r)
        if d and d not in out:
            out[d] = r
    return out


def fetch_sleep_summary(client: InfluxDBClient, start: date, end: date) -> dict[str, dict]:
    s_z, e_z = _range_z(start, end)
    rows = _q(client, (
        f'SELECT "sleepScore","sleepTimeSeconds","restingHeartRate","avgOvernightHrv" '
        f'FROM "SleepSummary" '
        f"WHERE time >= '{s_z}' AND time < '{e_z}' ORDER BY time ASC"
    ))
    out: dict[str, dict] = {}
    for r in rows:
        d = _date_of(r)
        if d and d not in out:
            out[d] = r
    return out


def fetch_daily_stats(client: InfluxDBClient, start: date, end: date) -> dict[str, dict]:
    s_z, e_z = _range_z(start, end)
    rows = _q(client, (
        f'SELECT "restingHeartRate","bodyBatteryHighestValue" '
        f'FROM "DailyStats" '
        f"WHERE time >= '{s_z}' AND time < '{e_z}' ORDER BY time ASC"
    ))
    out: dict[str, dict] = {}
    for r in rows:
        d = _date_of(r)
        if d and d not in out:
            out[d] = r
    return out


def fetch_training_load(client: InfluxDBClient, start: date, end: date) -> dict[str, dict]:
    """TrainingLoadDaily holds project-computed ATL/CTL/TSB (not Garmin-native TrainingLoad)."""
    s_z, e_z = _range_z(start, end)
    rows = _q(client, (
        f'SELECT "TSB_TSS","TSB_TRIMP" '
        f'FROM "TrainingLoadDaily" '
        f"WHERE time >= '{s_z}' AND time < '{e_z}' ORDER BY time ASC"
    ))
    out: dict[str, dict] = {}
    for r in rows:
        d = _date_of(r)
        if d and d not in out:
            out[d] = r
    return out


def fetch_existing_readiness(client: InfluxDBClient, start: date, end: date) -> set[str]:
    """Return set of YYYY-MM-DD strings that already have a CoachReadiness record."""
    s_z, e_z = _range_z(start, end)
    rows = _q(client, (
        f'SELECT "coach_readiness" FROM "CoachReadiness" '
        f"WHERE time >= '{s_z}' AND time < '{e_z}'"
    ))
    return {_date_of(r) for r in rows if _date_of(r)}


# ---------------------------------------------------------------------------
# Component formulae
# ---------------------------------------------------------------------------

def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _hrv_component(current: float, history: list[float]) -> tuple[float, float]:
    """Returns (component 0–100, hrv_baseline_30d)."""
    baseline = sum(history) / len(history)
    ratio = _clamp(current / baseline, 0.4, 1.5) if baseline > 0 else 0.4
    return ((ratio - 0.4) / 1.1) * 100.0, baseline


def _sleep_component(score: float | None, secs: float | None) -> float:
    quality  = _clamp((_f(score) or 0.0) / 100.0, 0.0, 1.0)
    duration = min((_f(secs) or 0.0) / 28800.0, 1.0)
    return (quality * 0.6 + duration * 0.4) * 100.0


def _rhr_component(current: float, history: list[float]) -> tuple[float, float]:
    """Returns (component 0–100, rhr_baseline_30d_min)."""
    baseline_min = min(history)
    ratio = _clamp(baseline_min / current, 0.7, 1.15) if current > 0 else 0.7
    return ((ratio - 0.7) / 0.45) * 100.0, baseline_min


def _form_component(tsb: float | None) -> float:
    if tsb is None:
        return 50.0
    return _clamp((tsb + 30.0) / 60.0 * 100.0, 0.0, 100.0)


# ---------------------------------------------------------------------------
# Per-day computation
# ---------------------------------------------------------------------------

def compute_day(
    d: date,
    whoop_data: dict[str, dict],
    sleep_data: dict[str, dict],
    daily_data: dict[str, dict],
    tl_data:    dict[str, dict],
) -> dict | None:
    """Return an InfluxDB point dict, or None if the day must be skipped."""
    ds = d.isoformat()

    # --- HRV: WHOOP preferred, Garmin fallback ---
    w_row = whoop_data.get(ds) or {}
    s_row = sleep_data.get(ds) or {}

    whoop_hrv  = _f(w_row.get("hrv_rmssd_milli"))
    garmin_hrv = _f(s_row.get("avgOvernightHrv"))

    if whoop_hrv is not None:
        hrv_value, hrv_source = whoop_hrv, "whoop"
    elif garmin_hrv is not None:
        hrv_value, hrv_source = garmin_hrv, "garmin"
    else:
        log.info("skip %s — no HRV (WHOOP or Garmin)", ds)
        return None

    # --- HRV 30-day rolling history (prior days only, same source preference) ---
    hrv_hist: list[float] = []
    for i in range(1, LOOKBACK_DAYS + 1):
        pds = (d - timedelta(days=i)).isoformat()
        w = _f((whoop_data.get(pds) or {}).get("hrv_rmssd_milli"))
        g = _f((sleep_data.get(pds) or {}).get("avgOvernightHrv"))
        v = w if w is not None else g
        if v is not None:
            hrv_hist.append(v)

    if len(hrv_hist) < MIN_HRV_DAYS:
        log.info("skip %s — only %d days HRV history (need ≥%d)", ds, len(hrv_hist), MIN_HRV_DAYS)
        return None

    hrv_comp, hrv_baseline = _hrv_component(hrv_value, hrv_hist)

    # --- Sleep ---
    sleep_score = _f(s_row.get("sleepScore"))
    sleep_secs  = _f(s_row.get("sleepTimeSeconds"))
    sleep_comp  = _sleep_component(sleep_score, sleep_secs)

    # --- RHR: SleepSummary preferred, DailyStats fallback ---
    def _rhr_for(date_s: str) -> float | None:
        v = _f((sleep_data.get(date_s) or {}).get("restingHeartRate"))
        return v if v is not None else _f((daily_data.get(date_s) or {}).get("restingHeartRate"))

    rhr_value = _rhr_for(ds)
    rhr_hist: list[float] = []
    for i in range(1, LOOKBACK_DAYS + 1):
        v = _rhr_for((d - timedelta(days=i)).isoformat())
        if v is not None:
            rhr_hist.append(v)

    if rhr_value is not None and rhr_hist:
        rhr_comp, rhr_baseline_min = _rhr_component(rhr_value, rhr_hist)
    else:
        rhr_comp, rhr_baseline_min = 50.0, None

    # --- Form / TSB: most recent TrainingLoadDaily within 3 days ---
    # Spec says "DerivedActivity" but project-computed TSB lives in TrainingLoadDaily.
    # Prefer TSS-based TSB when LTHR is known, fall back to TRIMP-based.
    tsb_val: float | None = None
    for i in range(3):
        row = tl_data.get((d - timedelta(days=i)).isoformat()) or {}
        v = _f(row.get("TSB_TSS")) if row.get("TSB_TSS") is not None else _f(row.get("TSB_TRIMP"))
        if v is not None:
            tsb_val = v
            break

    form_comp = _form_component(tsb_val)

    # --- Composite ---
    readiness = (
        hrv_comp   * 0.40 +
        sleep_comp * 0.30 +
        rhr_comp   * 0.20 +
        form_comp  * 0.10
    )

    # --- Comparison / diagnostic fields ---
    garmin_bb = _f((daily_data.get(ds) or {}).get("bodyBatteryHighestValue"))
    whoop_rec = _f(w_row.get("recovery_score"))

    midnight_z = datetime(d.year, d.month, d.day, tzinfo=timezone.utc).isoformat()

    fields: dict = {
        "coach_readiness":      round(readiness, 2),
        "hrv_component":        round(hrv_comp, 2),
        "sleep_component":      round(sleep_comp, 2),
        "rhr_component":        round(rhr_comp, 2),
        "form_component":       round(form_comp, 2),
        "hrv_value":            round(hrv_value, 3),
        "hrv_baseline_30d":     round(hrv_baseline, 3),
        "hrv_source":           hrv_source,
        "sleep_score_raw":      sleep_score,
        "sleep_secs_raw":       sleep_secs,
        "rhr_value":            round(rhr_value, 1) if rhr_value is not None else None,
        "rhr_baseline_30d_min": round(rhr_baseline_min, 1) if rhr_baseline_min is not None else None,
        "tsb_value":            round(tsb_val, 2) if tsb_val is not None else None,
        "whoop_recovery":       whoop_rec,
        "whoop_hrv":            whoop_hrv,
        "garmin_hrv":           garmin_hrv,
        "garmin_body_battery":  garmin_bb,
        "notes":                VERSION,
    }

    # InfluxDB v1 rejects None fields — strip them so the write succeeds
    fields = {k: v for k, v in fields.items() if v is not None}

    return {
        "measurement": "CoachReadiness",
        "time": midnight_z,
        "tags": {"Database_Name": INFLUX_DB},
        "fields": fields,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute CoachReadiness composite score and write to InfluxDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--backfill", action="store_true",
                       help="Process last 90 days, skip existing records (idempotent)")
    group.add_argument("--date", metavar="YYYY-MM-DD",
                       help="Process a single date (overwrites existing)")
    args = parser.parse_args()

    today = date.today()

    if args.backfill:
        target_dates = sorted(today - timedelta(days=i) for i in range(90))
        skip_existing = True
    elif args.date:
        try:
            target_dates = [date.fromisoformat(args.date)]
        except ValueError:
            sys.exit(f"ERROR: invalid date '{args.date}' — expected YYYY-MM-DD")
        skip_existing = False
    else:
        target_dates = sorted({today - timedelta(days=1), today})
        skip_existing = False

    # Hard lower bound — no data before WHOOP started
    target_dates = [d for d in target_dates if d >= WHOOP_START]
    if not target_dates:
        log.info("No dates to process (all before WHOOP start %s)", WHOOP_START)
        return

    mode = "backfill" if args.backfill else ("--date" if args.date else "default")
    log.info(
        "Mode: %s | %s → %s (%d dates)",
        mode, target_dates[0], target_dates[-1], len(target_dates),
    )

    client = make_client()

    # Bulk data fetch: target range + 30-day lookback window
    fetch_start = target_dates[0] - timedelta(days=LOOKBACK_DAYS)
    fetch_end   = target_dates[-1]

    log.info(
        "Fetching measurements %s → %s (includes %d-day lookback) …",
        fetch_start, fetch_end, LOOKBACK_DAYS,
    )
    whoop_data  = fetch_whoop_recovery(client, fetch_start, fetch_end)
    sleep_data  = fetch_sleep_summary(client, fetch_start, fetch_end)
    daily_data  = fetch_daily_stats(client, fetch_start, fetch_end)
    tl_data     = fetch_training_load(client, fetch_start, fetch_end)

    log.info(
        "Records fetched: %d WhoopRecovery, %d SleepSummary, %d DailyStats, %d TrainingLoadDaily",
        len(whoop_data), len(sleep_data), len(daily_data), len(tl_data),
    )

    # Pre-fetch existing CoachReadiness dates (avoids N queries in backfill mode)
    existing: set[str] = set()
    if skip_existing:
        existing = fetch_existing_readiness(client, target_dates[0], target_dates[-1])
        log.info("Existing CoachReadiness records in range: %d", len(existing))

    n_written        = 0
    n_skipped_exists = 0
    n_skipped_data   = 0
    points_batch: list[dict] = []

    for d in target_dates:
        ds = d.isoformat()

        if skip_existing and ds in existing:
            n_skipped_exists += 1
            continue

        point = compute_day(d, whoop_data, sleep_data, daily_data, tl_data)
        if point is None:
            n_skipped_data += 1
            continue

        points_batch.append(point)
        n_written += 1

    if points_batch:
        client.write_points(points_batch)
        log.info("Wrote %d CoachReadiness records to InfluxDB", len(points_batch))

    date_range = (
        f"{target_dates[0]} → {target_dates[-1]}"
        if len(target_dates) > 1
        else str(target_dates[0])
    )
    print(f"\n{'='*52}")
    print(f"  CoachReadiness — done")
    print(f"  Date range      : {date_range}")
    print(f"  Written         : {n_written}")
    print(f"  Skipped (exists): {n_skipped_exists}")
    print(f"  Skipped (no HRV): {n_skipped_data}")
    print(f"  Total considered: {len(target_dates)}")
    print(f"{'='*52}\n")


if __name__ == "__main__":
    main()
