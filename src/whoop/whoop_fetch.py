from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

from whoop import config as cfg
from whoop.whoop_auth import WhoopAuth

WHOOP_API_BASE = "https://api.prod.whoop.com/developer/v2"

# --- module-level config (mirrors garmin_fetch.py pattern) ---
INFLUXDB_HOST = cfg.INFLUXDB_HOST
INFLUXDB_PORT = cfg.INFLUXDB_PORT
INFLUXDB_USERNAME = cfg.INFLUXDB_USERNAME
INFLUXDB_PASSWORD = cfg.INFLUXDB_PASSWORD
INFLUXDB_DATABASE = cfg.INFLUXDB_DATABASE
LOG_LEVEL = cfg.LOG_LEVEL
UPDATE_INTERVAL_SECONDS = cfg.UPDATE_INTERVAL_SECONDS
SKIP_EXISTING_DAILY = cfg.SKIP_EXISTING_DAILY
FETCH_CHUNK_DAYS = 30      # days per API batch
FETCH_DELAY_SECONDS = 2    # pause between chunks
TAG_MEASUREMENTS_WITH_USER_EMAIL = cfg.TAG_MEASUREMENTS_WITH_USER_EMAIL
MANUAL_START_DATE = cfg.MANUAL_START_DATE
MANUAL_END_DATE = cfg.MANUAL_END_DATE

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# InfluxDB client — module-level, same pattern as garmin_fetch.py
influxdbclient = InfluxDBClient(
    host=INFLUXDB_HOST,
    port=INFLUXDB_PORT,
    username=INFLUXDB_USERNAME,
    password=INFLUXDB_PASSWORD,
    database=INFLUXDB_DATABASE,
)

auth = WhoopAuth(
    client_id=cfg.WHOOP_CLIENT_ID,
    client_secret=cfg.WHOOP_CLIENT_SECRET,
    token_dir=cfg.WHOOP_TOKEN_DIR,
)

# Cached WHOOP user ID for tagging (populated in main())
_whoop_user_id: str = "unknown"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _base_tags() -> dict:
    tags: dict = {
        "Device": "WHOOP",
        "Database_Name": INFLUXDB_DATABASE,
    }
    if TAG_MEASUREMENTS_WITH_USER_EMAIL:
        tags["User_ID"] = _whoop_user_id
    return tags


def _api_get(path: str, params: dict | None = None) -> dict:
    token = auth.get_access_token()

    def _do_get() -> requests.Response:
        return requests.get(
            f"{WHOOP_API_BASE}{path}",
            headers={"Authorization": f"Bearer {token}"},
            params=params or {},
            timeout=30,
        )

    resp = _do_get()
    if resp.status_code == 429:
        retry_after = resp.headers.get("Retry-After")
        sleep_secs = int(retry_after) + 5 if retry_after else 60
        logging.warning("Rate limited (429); sleeping %ds before retry", sleep_secs)
        time.sleep(sleep_secs)
        resp = _do_get()
    resp.raise_for_status()
    return resp.json()


def _paginate(path: str, start_iso: str, end_iso: str) -> list[dict]:
    records: list[dict] = []
    next_token: str | None = None
    while True:
        params: dict = {"start": start_iso, "end": end_iso, "limit": 25}
        if next_token:
            params["nextToken"] = next_token
        data = _api_get(path, params)
        records.extend(data.get("records", []))
        next_token = data.get("next_token")
        if not next_token:
            break
    return records


def _day_iso(date_str: str) -> tuple[str, str]:
    """Return (start_iso, end_iso) in RFC 3339 UTC for the given YYYY-MM-DD day."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = dt + timedelta(days=1)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"), end.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _day_bounds_influx(date_str: str) -> tuple[str, str]:
    """Return (start_z, end_z) suitable for InfluxDB WHERE time clauses."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = dt + timedelta(days=1)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean_fields(fields: dict) -> dict:
    """Strip None values — same as influx.clean_point logic."""
    return {k: v for k, v in fields.items() if v is not None}


def write_points(points: list[dict] | None) -> None:
    if not points:
        return
    cleaned = []
    for p in points:
        fields = _clean_fields(p.get("fields") or {})
        if not fields:
            continue
        cleaned.append({**p, "fields": fields})
    if not cleaned:
        return
    try:
        influxdbclient.write_points(cleaned)
        logging.info("Success : wrote %d point(s) to InfluxDB", len(cleaned))
    except InfluxDBClientError:
        logging.exception("Failed to write %d points to InfluxDB", len(cleaned))


# ---------------------------------------------------------------------------
# Skip checks — one per measurement, independent (same pattern as Garmin
# intraday checks so a partial ingest failure only affects that measurement)
# ---------------------------------------------------------------------------

def _measurement_exists_for_day(measurement: str, field: str, date_str: str) -> bool:
    try:
        start_z, end_z = _day_bounds_influx(date_str)
        q = (
            f'SELECT count("{field}") AS c '
            f'FROM "{measurement}" '
            f"WHERE time >= '{start_z}' AND time < '{end_z}' "
            f"AND \"Database_Name\"='{INFLUXDB_DATABASE}'"
        )
        res = influxdbclient.query(q)
        pts = list(res.get_points())
        if not pts:
            return False
        c = pts[0].get("c")
        return (c is not None) and (float(c) > 0)
    except Exception:
        logging.exception("%s existence query failed", measurement)
        return False


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------

def fetch_recovery(start_date: str, end_date: str) -> list[dict]:
    start_iso, end_iso = _day_iso(start_date)
    # end_date is inclusive — advance end by 1 day
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    records = _paginate("/recovery", start_iso, end_iso)
    points: list[dict] = []
    for r in records:
        if r.get("score_state") != "SCORED":
            continue
        score = r.get("score") or {}
        created_at = r.get("created_at")
        if not created_at or not score:
            continue
        points.append({
            "measurement": "WhoopRecovery",
            "time": created_at,
            "tags": _base_tags(),
            "fields": {
                "recovery_score": score.get("recovery_score"),
                "hrv_rmssd_milli": score.get("hrv_rmssd_milli"),
                "resting_heart_rate": score.get("resting_heart_rate"),
                "spo2_percentage": score.get("spo2_percentage"),
                "skin_temp_celsius": score.get("skin_temp_celsius"),
                "sleep_need_baseline_milli": (score.get("sleep_needed") or {}).get("baseline_milli"),
            },
        })
    if points:
        logging.info("Success : fetched %d WhoopRecovery record(s) for %s→%s", len(points), start_date, end_date)
    return points


def fetch_sleep(start_date: str, end_date: str) -> list[dict]:
    start_iso, _ = _day_iso(start_date)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    records = _paginate("/activity/sleep", start_iso, end_iso)
    points: list[dict] = []
    for r in records:
        if r.get("score_state") != "SCORED":
            continue
        score = r.get("score") or {}
        start = r.get("start")
        if not start or not score:
            continue
        stage = score.get("stage_summary") or {}
        points.append({
            "measurement": "WhoopSleep",
            "time": start,
            "tags": _base_tags(),
            "fields": {
                "score_total": score.get("sleep_performance_percentage"),
                "total_light_sleep_milli": stage.get("total_light_sleep_time_milli"),
                "total_slow_wave_sleep_milli": stage.get("total_slow_wave_sleep_time_milli"),
                "total_rem_sleep_milli": stage.get("total_rem_sleep_time_milli"),
                "total_awake_milli": stage.get("total_awake_time_milli"),
                "time_in_bed_millis": stage.get("total_in_bed_time_milli"),
                "disturbance_count": stage.get("disturbance_count"),
                "respiratory_rate": score.get("respiratory_rate"),
                "sleep_efficiency_percentage": score.get("sleep_efficiency_percentage"),
            },
        })
    if points:
        logging.info("Success : fetched %d WhoopSleep record(s) for %s→%s", len(points), start_date, end_date)
    return points


def fetch_strain(start_date: str, end_date: str) -> list[dict]:
    start_iso, _ = _day_iso(start_date)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    records = _paginate("/cycle", start_iso, end_iso)
    points: list[dict] = []
    for r in records:
        if r.get("score_state") != "SCORED":
            continue
        score = r.get("score") or {}
        start = r.get("start")
        if not start or not score:
            continue
        points.append({
            "measurement": "WhoopStrain",
            "time": start,
            "tags": _base_tags(),
            "fields": {
                "day_strain": score.get("strain"),
                "kilojoule": score.get("kilojoule"),
                "average_heart_rate": score.get("average_heart_rate"),
                "max_heart_rate": score.get("max_heart_rate"),
            },
        })
    if points:
        logging.info("Success : fetched %d WhoopStrain record(s) for %s→%s", len(points), start_date, end_date)
    return points


def fetch_workouts(start_date: str, end_date: str, sport_map: dict[int, str]) -> list[dict]:
    start_iso, _ = _day_iso(start_date)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    records = _paginate("/activity/workout", start_iso, end_iso)
    points: list[dict] = []
    for r in records:
        if r.get("score_state") != "SCORED":
            continue
        score = r.get("score") or {}
        start = r.get("start")
        sport_id = r.get("sport_id")
        if not start or not score:
            continue
        sport_name = sport_map.get(sport_id, f"sport_{sport_id}") if sport_id is not None else "unknown"
        zones = score.get("zone_duration") or {}
        tags = {**_base_tags(), "sport_name": sport_name}
        points.append({
            "measurement": "WhoopWorkout",
            "time": start,
            "tags": tags,
            "fields": {
                "score_strain": score.get("strain"),
                "average_heart_rate": score.get("average_heart_rate"),
                "max_heart_rate": score.get("max_heart_rate"),
                "kilojoule": score.get("kilojoule"),
                "distance_meter": score.get("distance_meter"),
                "altitude_gain_meter": score.get("altitude_gain_meter"),
                "zone_zero_milli": zones.get("zone_zero_milli"),
                "zone_one_milli": zones.get("zone_one_milli"),
                "zone_two_milli": zones.get("zone_two_milli"),
                "zone_three_milli": zones.get("zone_three_milli"),
                "zone_four_milli": zones.get("zone_four_milli"),
                "zone_five_milli": zones.get("zone_five_milli"),
            },
        })
    if points:
        logging.info("Success : fetched %d WhoopWorkout record(s) for %s→%s", len(points), start_date, end_date)
    return points


# ---------------------------------------------------------------------------
# Sport map
# ---------------------------------------------------------------------------

def get_sport_map() -> dict[int, str]:
    try:
        data = _api_get("/sport")
        return {s["id"]: s["name"] for s in data.get("sports", [])}
    except Exception:
        logging.warning("Failed to fetch WHOOP sport list; sport IDs will be used as names")
        return {}


# ---------------------------------------------------------------------------
# Chunk ingest — fetches a date range in one API call per measurement.
# Skip check uses start_date (earliest day) as the proxy: a chunk is only
# skipped if the first day already exists, so a partially-ingested chunk
# will be retried in full rather than silently dropped.
# ---------------------------------------------------------------------------

def fetch_and_write_chunk(start_date: str, end_date: str, sport_map: dict[int, str]) -> None:
    logging.info("Fetching WHOOP data for %s → %s", start_date, end_date)
    check_day = start_date  # conservative proxy: skip only if earliest day exists

    if SKIP_EXISTING_DAILY and _measurement_exists_for_day("WhoopRecovery", "recovery_score", check_day):
        logging.info("WhoopRecovery already present from %s; skipping chunk", check_day)
    else:
        write_points(fetch_recovery(start_date, end_date))

    if SKIP_EXISTING_DAILY and _measurement_exists_for_day("WhoopSleep", "score_total", check_day):
        logging.info("WhoopSleep already present from %s; skipping chunk", check_day)
    else:
        write_points(fetch_sleep(start_date, end_date))

    if SKIP_EXISTING_DAILY and _measurement_exists_for_day("WhoopStrain", "day_strain", check_day):
        logging.info("WhoopStrain already present from %s; skipping chunk", check_day)
    else:
        write_points(fetch_strain(start_date, end_date))

    if SKIP_EXISTING_DAILY and _measurement_exists_for_day("WhoopWorkout", "score_strain", check_day):
        logging.info("WhoopWorkout already present from %s; skipping chunk", check_day)
    else:
        write_points(fetch_workouts(start_date, end_date, sport_map))


# ---------------------------------------------------------------------------
# Chunk iteration — reverse chronological (same direction as old iter_days)
# ---------------------------------------------------------------------------

def iter_chunks(start_date: str, end_date: str, chunk_days: int = FETCH_CHUNK_DAYS):
    """Yield (chunk_start, chunk_end) pairs in reverse-chronological order."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    chunk_end = end
    while chunk_end >= start:
        chunk_start = max(start, chunk_end - timedelta(days=chunk_days - 1))
        yield chunk_start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        chunk_end = chunk_start - timedelta(days=1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    global _whoop_user_id

    if not cfg.WHOOP_CLIENT_ID or not cfg.WHOOP_CLIENT_SECRET:
        logging.error("WHOOP_CLIENT_ID and WHOOP_CLIENT_SECRET must be set")
        return 1

    if not auth.load_tokens():
        logging.info("No WHOOP tokens found — starting initial authorization flow...")
        try:
            auth.run_initial_auth_flow()
        except Exception:
            logging.exception("Initial WHOOP auth failed")
            return 1

    # Fetch user profile for tagging
    try:
        profile = _api_get("/user/profile/basic")
        _whoop_user_id = str(profile.get("user_id", "unknown"))
        logging.info("Authenticated as WHOOP user %s (%s %s)",
                     _whoop_user_id,
                     profile.get("first_name", ""),
                     profile.get("last_name", ""))
    except Exception:
        logging.warning("Could not fetch WHOOP user profile")

    sport_map = get_sport_map()
    logging.info("Loaded %d WHOOP sport types", len(sport_map))

    # --- manual backfill ---
    if MANUAL_START_DATE:
        end = MANUAL_END_DATE or datetime.today().strftime("%Y-%m-%d")
        logging.info("Manual backfill: %s → %s", MANUAL_START_DATE, end)
        for chunk_start, chunk_end in iter_chunks(MANUAL_START_DATE, end):
            try:
                fetch_and_write_chunk(chunk_start, chunk_end, sport_map)
                time.sleep(FETCH_DELAY_SECONDS)
            except Exception:
                logging.exception("Failed to fetch WHOOP chunk %s → %s", chunk_start, chunk_end)
        logging.info("Backfill complete")
        return 0

    # --- live update loop (same pattern as garmin_fetch.py) ---
    last_date = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")
    try:
        res = influxdbclient.query('SELECT * FROM "WhoopRecovery" ORDER BY time DESC LIMIT 1')
        pts = list(res.get_points())
        if pts:
            last_date = pts[0]["time"][:10]
            logging.info("Resuming from last WHOOP sync date: %s", last_date)
    except Exception:
        logging.warning("Could not determine last WHOOP sync date; defaulting to 90 days ago")

    while True:
        today = datetime.today().strftime("%Y-%m-%d")
        logging.info("Checking WHOOP data from %s to %s", last_date, today)
        for chunk_start, chunk_end in iter_chunks(last_date, today):
            try:
                fetch_and_write_chunk(chunk_start, chunk_end, sport_map)
                time.sleep(FETCH_DELAY_SECONDS)
            except Exception:
                logging.exception("Failed to fetch WHOOP chunk %s → %s", chunk_start, chunk_end)
        last_date = today
        logging.info("Waiting %ds before next WHOOP check", UPDATE_INTERVAL_SECONDS)
        time.sleep(UPDATE_INTERVAL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
