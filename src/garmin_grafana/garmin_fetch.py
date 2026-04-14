# %%
import traceback
import requests, time, pytz, logging, os, sys, io, zipfile
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
from influxdb_client_3 import InfluxDBClient3, InfluxDBError
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
from garth.exc import GarthHTTPError
from garminconnect import (
    Garmin,
    GarminConnectAuthenticationError,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError,
)
from garmin_grafana.activity_gps import ActivityGPSContext, fetch_activity_GPS as _fetch_activity_GPS
from garmin_grafana.garmin_client import garmin_login as _garmin_login
from garmin_grafana.influx import InfluxConfig, create_influx_client, write_points as _write_points_to_influx
from garmin_grafana import config as cfg
from garmin_grafana.fetchers.training import TrainingFetchContext as _TrainingFetchContext
from garmin_grafana.fetchers import training as _training_fetchers
from garmin_grafana.fetchers.health import HealthFetchContext as _HealthFetchContext
from garmin_grafana.fetchers import health as _health_fetchers
from garmin_grafana.fetchers.activity import ActivityFetchContext as _ActivityFetchContext
from garmin_grafana.fetchers import activity as _activity_fetchers
from garmin_grafana.fetchers.intraday import IntradayFetchContext as _IntradayFetchContext
from garmin_grafana.fetchers import intraday as _intraday_fetchers
from garmin_grafana.fetchers.daily import DailyFetchContext as _DailyFetchContext
from garmin_grafana.fetchers import daily as _daily_fetchers
from garmin_grafana.fetchers.device import DeviceFetchContext as _DeviceFetchContext
from garmin_grafana.fetchers import device as _device_fetchers
from garmin_grafana.orchestrator import OrchestratorContext as _OrchestratorContext
from garmin_grafana.orchestrator import fetch_write_bulk as _fetch_write_bulk
from garmin_grafana.orchestrator import compute_rollups_range as _compute_rollups_range
from garmin_grafana.orchestrator import DailyFetchWriteContext as _DailyFetchWriteContext
from garmin_grafana.orchestrator import daily_fetch_write as _daily_fetch_write
from garmin_grafana.orchestrator import run_rollups_for_range as _run_rollups_for_range
from garmin_grafana.rollups import RollupContext as _RollupContext
from garmin_grafana import rollups as _rollups
from garmin_grafana.influx_queries import InfluxQueryContext as _InfluxQueryContext
from garmin_grafana import influx_queries as _influxq
from garmin_grafana.influx_v1_queries import InfluxV1QueryContext as _InfluxV1QueryContext
from garmin_grafana import influx_v1_queries as _influxv1

garmin_obj = None
banner_text = """

*****  █▀▀ ▄▀█ █▀█ █▀▄▀█ █ █▄ █    █▀▀ █▀█ ▄▀█ █▀▀ ▄▀█ █▄ █ ▄▀█  *****
*****  █▄█ █▀█ █▀▄ █ ▀ █ █ █ ▀█    █▄█ █▀▄ █▀█ █▀  █▀█ █ ▀█ █▀█  *****

______________________________________________________________________

By Arpan Ghosh | Please consider supporting the project if you love it
______________________________________________________________________

"""
print(banner_text)
USER_GENDER_OVERRIDE = cfg.USER_GENDER_OVERRIDE

# %%
def _norm_tag_value(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip().lower()
    if not s:
        return None
    s = s.replace(" ", "_")
    # common normalizations
    if s in {"inline_skating", "inline-skating", "skating", "inlineskating"}:
        return "inline_skating"
    if s in {"bike", "biking", "cycling"}:
        return "cycling"
    return s

def _is_run(activity_type: str) -> bool:
    t = _norm_tag_value(activity_type or "")
    return t in {
        "running", "trail_running", "treadmill_running", "track_running",
        "ultra_run", "virtual_run",
    }

def _is_ride(activity_type: str) -> bool:
    t = _norm_tag_value(activity_type or "")
    return t in {
        "cycling", "road_biking", "mountain_biking", "gravel_cycling",
        "virtual_ride", "indoor_cycling", "bike",
    }

USERPROFILE_WRITE_ONCE_PER_DAY = cfg.USERPROFILE_WRITE_ONCE_PER_DAY
INFLUXDB_VERSION = cfg.INFLUXDB_VERSION
INFLUXDB_HOST = cfg.INFLUXDB_HOST
INFLUXDB_PORT = cfg.INFLUXDB_PORT
INFLUXDB_USERNAME = cfg.INFLUXDB_USERNAME
INFLUXDB_PASSWORD = cfg.INFLUXDB_PASSWORD
INFLUXDB_DATABASE = cfg.INFLUXDB_DATABASE
INFLUXDB_V3_ACCESS_TOKEN = cfg.INFLUXDB_V3_ACCESS_TOKEN

TOKEN_DIR = cfg.TOKEN_DIR
GARMINCONNECT_EMAIL = cfg.GARMINCONNECT_EMAIL
GARMINCONNECT_PASSWORD = cfg.GARMINCONNECT_PASSWORD
GARMINCONNECT_IS_CN = cfg.GARMINCONNECT_IS_CN
GARMIN_DEVICENAME = cfg.GARMIN_DEVICENAME
GARMIN_DEVICEID = cfg.GARMIN_DEVICEID

AUTO_DATE_RANGE = cfg.AUTO_DATE_RANGE
MANUAL_START_DATE = cfg.MANUAL_START_DATE
MANUAL_END_DATE = cfg.MANUAL_END_DATE

LOG_LEVEL = cfg.LOG_LEVEL
FETCH_FAILED_WAIT_SECONDS = cfg.FETCH_FAILED_WAIT_SECONDS
RATE_LIMIT_CALLS_SECONDS = cfg.RATE_LIMIT_CALLS_SECONDS
MAX_CONSECUTIVE_500_ERRORS = cfg.MAX_CONSECUTIVE_500_ERRORS
INFLUXDB_ENDPOINT_IS_HTTP = cfg.INFLUXDB_ENDPOINT_IS_HTTP

GARMIN_DEVICENAME_AUTOMATIC = cfg.GARMIN_DEVICENAME_AUTOMATIC
UPDATE_INTERVAL_SECONDS = cfg.UPDATE_INTERVAL_SECONDS

FETCH_SELECTION = cfg.FETCH_SELECTION
LACTATE_THRESHOLD_SPORTS = cfg.LACTATE_THRESHOLD_SPORTS

KEEP_FIT_FILES = cfg.KEEP_FIT_FILES
FIT_FILE_STORAGE_LOCATION = cfg.FIT_FILE_STORAGE_LOCATION
ALWAYS_PROCESS_FIT_FILES = cfg.ALWAYS_PROCESS_FIT_FILES
REQUEST_INTRADAY_DATA_REFRESH = cfg.REQUEST_INTRADAY_DATA_REFRESH
IGNORE_INTRADAY_DATA_REFRESH_DAYS = cfg.IGNORE_INTRADAY_DATA_REFRESH_DAYS
TAG_MEASUREMENTS_WITH_USER_EMAIL = cfg.TAG_MEASUREMENTS_WITH_USER_EMAIL

FORCE_REPROCESS_ACTIVITIES = cfg.FORCE_REPROCESS_ACTIVITIES
ROLLUPS_AFTER_INGEST = cfg.ROLLUPS_AFTER_INGEST

USER_TIMEZONE = cfg.USER_TIMEZONE
PARSED_ACTIVITY_ID_LIST = []
# --- gender write guard ---
# ensures UserProfile is written only once per day
USER_PROFILE_WRITTEN_DATES: set[str] = set()
# FIT-derived gender overrides profile gender
FIT_GENDER_CACHE: dict[str, str] = {}
IGNORE_ERRORS = cfg.IGNORE_ERRORS

# %%
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# %%
influxdbclient = create_influx_client(
    InfluxConfig(
        version=INFLUXDB_VERSION,
        host=INFLUXDB_HOST,
        port=INFLUXDB_PORT,
        username=INFLUXDB_USERNAME,
        password=INFLUXDB_PASSWORD,
        database=INFLUXDB_DATABASE,
        v3_access_token=INFLUXDB_V3_ACCESS_TOKEN,
        endpoint_is_http=INFLUXDB_ENDPOINT_IS_HTTP,
    )
)

# %%
def iter_days(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = end
    while current >= start:
        yield current.strftime("%Y-%m-%d")
        current -= timedelta(days=1)

# %%
def garmin_login():
    return _garmin_login(
        token_dir=TOKEN_DIR,
        garminconnect_email=GARMINCONNECT_EMAIL,
        garminconnect_password=GARMINCONNECT_PASSWORD,
        garminconnect_is_cn=GARMINCONNECT_IS_CN,
    )

# %%
def _norm_gender(v: object) -> str:
    if v is None:
        return "unknown"
    s = str(v).strip().lower()
    if s in {"m", "male", "man"}:
        return "male"
    if s in {"f", "female", "woman"}:
        return "female"
    return "unknown"

def _gender_code(g: str) -> int:
    return 1 if g == "male" else 2 if g == "female" else 0

def _get_user_gender_from_garmin() -> str:
    if USER_GENDER_OVERRIDE:
        return _norm_gender(USER_GENDER_OVERRIDE)

    try:
        # garminconnect library commonly exposes get_user_profile()
        prof = garmin_obj.get_user_profile() if garmin_obj is not None else {}
        if isinstance(prof, dict):
            # try common keys
            for key in ("gender", "sex"):
                if key in prof:
                    return _norm_gender(prof.get(key))
            if "userProfile" in prof and isinstance(prof["userProfile"], dict):
                return _norm_gender(prof["userProfile"].get("gender") or prof["userProfile"].get("sex"))
    except Exception:
        logging.exception("Unable to extract gender from Garmin user profile")

    return "unknown"

def write_user_profile_point(date_str: str, gender: str | None = None, birth_year: int | None = None) -> list[dict]:
    """
    Canonical daily UserProfile writer.

    Critical rules:
      - No 'source' tag (prevents duplicate daily series).
      - Same timestamp/tagset => later writes overwrite earlier values.
      - 'gender' string field only written when known.
      - 'birth_year' stored as field when provided.
    """
    g = _norm_gender(gender) if gender is not None else _get_user_gender_from_garmin()

    tags = {
        "Device": GARMIN_DEVICENAME,
        "Database_Name": INFLUXDB_DATABASE,
    }

    fields: dict[str, object] = {
        "gender_code": _gender_code(g),
        "gender_is_known": 0 if g == "unknown" else 1,
    }

    if g != "unknown":
        fields["gender"] = g

    if birth_year is not None:
        try:
            fields["birth_year"] = int(birth_year)
        except Exception:
            pass

    return [
        {
            "measurement": "UserProfile",
            "time": datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(),
            "tags": tags,
            "fields": fields,
        }
    ]
    
def write_points_to_influxdb(points):
    def _get_user_id() -> str:
        try:
            if garmin_obj is not None and isinstance(garmin_obj.garth.profile, dict):
                return garmin_obj.garth.profile.get("userName", "Unknown") or "Unknown"
        except Exception:
            pass
        return "Unknown"

    _write_points_to_influx(
        client=influxdbclient,
        influx_version=INFLUXDB_VERSION,
        points=points,
        tag_measurements_with_user_email=TAG_MEASUREMENTS_WITH_USER_EMAIL,
        get_user_id=_get_user_id,
    )

# %%
from datetime import date as _date

def _dt_utc(s: str) -> datetime:
    return _influxq.dt_utc(s)

def _range_utc(start_date: str, days: int) -> tuple[str, str]:
    return _influxq.range_utc(start_date, days)

def _today_local_date_str(local_timediff: timedelta) -> str:
    # local_timediff is the offset you already computed in __main__
    return (datetime.now(pytz.UTC) + local_timediff).strftime("%Y-%m-%d")

def _should_rollups_after_ingest(start_date: str, end_date: str, local_timediff: timedelta) -> bool:
    """
    AUTO mode:
      - If range spans > 1 day => treat as backfill (rollups after ingest)
      - If end_date is before local 'today' => backfill (rollups after ingest)
      - Otherwise (typically 1 day, newest) => live mode (rollups inline)
    """
    if start_date != end_date:
        return True

    today_local = _today_local_date_str(local_timediff)
    return end_date < today_local

def _query_scalar_influx_v1(q: str) -> float | None:
    return _influxq.query_scalar_influx_v1(q, _InfluxQueryContext(influxdbclient=influxdbclient))

import math

USER_AGE = int(os.getenv("USER_AGE", "0"))  # 0 = auto/unknown

def _get_age_years(date_str: str | None = None) -> int:
    # 1) explicit env override
    if USER_AGE and USER_AGE > 0:
        return USER_AGE
    
    # 2) master age (best available if you've populated it from unknown_79)
    if INFLUXDB_VERSION == "1":
        a = _stored_age_years_v1()
        if a and a > 0:
            return a
    
    # 3) if a day is provided, try birth_year stored in Influx UserProfile/master
    if date_str and INFLUXDB_VERSION == "1":
        by = _get_birth_year_for_day_v1(date_str)
        if not by:
            by = _stored_birth_year_v1()
        if by:
            return _age_from_birth_year(by, date_str)
    # 3) fall back to Garmin profile birthDate
    try:
        prof = (garmin_obj.garth.profile or {}) if garmin_obj is not None else {}
        b = None
        if isinstance(prof, dict):
            b = prof.get("birthDate") or prof.get("birthdate") or prof.get("dateOfBirth")
            if not b and isinstance(prof.get("userProfile"), dict):
                b = prof["userProfile"].get("birthDate") or prof["userProfile"].get("birthdate")
        if b:
            y, m, d = [int(x) for x in str(b).split("-")[:3]]
            today = _date.today()
            age = today.year - y - ((today.month, today.day) < (m, d))
            return max(age, 0)
    except Exception:
        logging.exception("Unable to derive age from Garmin profile")

    return 0

def _hrmax_age_based(age_years: int, method: str = "tanaka") -> float | None:
    """
    Age-based HRmax fallback.
    tanaka: 208 - 0.7*age (often better than 220-age)
    fox:    220 - age
    """
    if age_years <= 0:
        return None
    method = (method or "tanaka").strip().lower()
    if method == "fox":
        return float(220 - age_years)
    return float(208 - 0.7 * age_years)  # tanaka default

HRMAX_FALLBACK_METHOD = os.getenv("HRMAX_FALLBACK_METHOD", "tanaka")  # tanaka|fox

def _query_last_row_influx_v1(q: str) -> dict | None:
    return _influxq.query_last_row_influx_v1(q, _InfluxQueryContext(influxdbclient=influxdbclient))

def _get_gender_for_day_v1(date_str: str) -> str:
    ctx = _InfluxV1QueryContext(
        influxdbclient=influxdbclient,
        influxdb_database=INFLUXDB_DATABASE,
        garmin_devicename=GARMIN_DEVICENAME,
        day_bounds_z=_day_bounds_z,
        dt_utc=_dt_utc,
        query_last_row=_query_last_row_influx_v1,
    )
    return _influxv1.get_gender_for_day(date_str, ctx)

def _get_userprofile_master_v1() -> dict:
    ctx = _InfluxV1QueryContext(
        influxdbclient=influxdbclient,
        influxdb_database=INFLUXDB_DATABASE,
        garmin_devicename=GARMIN_DEVICENAME,
        day_bounds_z=_day_bounds_z,
        dt_utc=_dt_utc,
        query_last_row=_query_last_row_influx_v1,
    )
    return _influxv1.get_userprofile_master(ctx)

def _stored_age_years_v1() -> int:
    ctx = _InfluxV1QueryContext(
        influxdbclient=influxdbclient,
        influxdb_database=INFLUXDB_DATABASE,
        garmin_devicename=GARMIN_DEVICENAME,
        day_bounds_z=_day_bounds_z,
        dt_utc=_dt_utc,
        query_last_row=_query_last_row_influx_v1,
    )
    return _influxv1.stored_age_years(ctx)

def _stored_birth_year_v1() -> int | None:
    ctx = _InfluxV1QueryContext(
        influxdbclient=influxdbclient,
        influxdb_database=INFLUXDB_DATABASE,
        garmin_devicename=GARMIN_DEVICENAME,
        day_bounds_z=_day_bounds_z,
        dt_utc=_dt_utc,
        query_last_row=_query_last_row_influx_v1,
    )
    return _influxv1.stored_birth_year(ctx)

def _birth_year_from_any(v: object) -> int | None:
    """
    Accepts:
      - int year (e.g., 1984)
      - string '1984'
      - string 'YYYY-MM-DD'
    Returns birth_year or None.
    """
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None

    # YYYY-MM-DD
    if "-" in s:
        parts = s.split("-")
        if len(parts) >= 1 and parts[0].isdigit():
            y = int(parts[0])
            return y if 1900 <= y <= _date.today().year else None

    if s.isdigit():
        y = int(s)
        return y if 1900 <= y <= _date.today().year else None

    return None

def _stored_lthr_bpm_v1() -> int | None:
    ctx = _InfluxV1QueryContext(
        influxdbclient=influxdbclient,
        influxdb_database=INFLUXDB_DATABASE,
        garmin_devicename=GARMIN_DEVICENAME,
        day_bounds_z=_day_bounds_z,
        dt_utc=_dt_utc,
        query_last_row=_query_last_row_influx_v1,
    )
    return _influxv1.stored_lthr_bpm(ctx)

def _age_from_birth_year(birth_year: int, date_str: str | None = None) -> int:
    if date_str:
        yr = int(str(date_str)[:4])   # YYYY from YYYY-MM-DD
    else:
        yr = _date.today().year
    return max(yr - int(birth_year), 0)

def _smooth_series(x: np.ndarray, window_samples: int) -> np.ndarray:
    w = max(3, int(window_samples))
    return pd.Series(x).rolling(w, center=True, min_periods=max(1, w//2)).mean().to_numpy()

def vertical_speed_mps(ts_epoch: np.ndarray, elev_m: np.ndarray, smooth_s: float = 10.0) -> np.ndarray:
    """
    Vertical speed in m/s using smoothed elevation and variable dt.
    Positive-only is handled later (we keep signed here).
    """
    t = np.asarray(ts_epoch, dtype=float)
    z = np.asarray(elev_m, dtype=float)

    # smooth elevation using ~smooth_s worth of samples (approx via median dt)
    dt = np.diff(t)
    dt = dt[(dt > 0) & np.isfinite(dt)]
    med_dt = float(np.median(dt)) if dt.size else 1.0
    sr = float(np.clip(1.0 / med_dt, 0.2, 5.0))
    w = int(max(3, smooth_s * sr))
    z_sm = _smooth_series(z, w)

    dz = np.diff(z_sm)
    dt_full = np.diff(t)
    dt_full = np.where(np.isfinite(dt_full) & (dt_full > 0), dt_full, np.nan)

    vz = dz / dt_full
    vz = np.r_[vz, vz[-1] if vz.size else 0.0]  # pad to length n
    # guard spikes
    vz = np.where(np.isfinite(vz), vz, 0.0)
    vz = np.clip(vz, -5.0, 5.0)  # ±5 m/s is already extreme
    return vz

def best_rolling_mean_masked(series: np.ndarray, ts_epoch: np.ndarray, window_s: float, mask: np.ndarray | None = None) -> float:
    """
    Best rolling mean over variable dt by resampling to 1 Hz-ish index.
    Since your FIT data is ~1 Hz, we can approximate with sample_rate derived from ts_epoch.
    Masked samples are excluded by setting them to NaN.
    """
    x = np.asarray(series, dtype=float)
    if mask is not None:
        m = np.asarray(mask, dtype=bool)
        x = np.where(m, x, np.nan)

    # derive sample rate from timestamps
    dt = np.diff(np.asarray(ts_epoch, dtype=float))
    dt = dt[(dt > 0) & np.isfinite(dt)]
    med_dt = float(np.median(dt)) if dt.size else 1.0
    sr = float(np.clip(1.0 / med_dt, 0.2, 5.0))

    w = max(3, int(window_s * sr))
    rm = pd.Series(x).rolling(w, min_periods=w).mean()
    if rm.dropna().empty:
        return np.nan
    return float(rm.max())

def _best_window_index(series: np.ndarray, ts_epoch: np.ndarray, window_s: float, mask: np.ndarray | None = None) -> tuple[int | None, int | None]:
    """
    Returns (start_idx, end_idx_inclusive) for the window achieving best rolling mean.
    Uses same approximation as best_rolling_mean_masked.
    """
    x = np.asarray(series, dtype=float)
    if mask is not None:
        m = np.asarray(mask, dtype=bool)
        x = np.where(m, x, np.nan)

    dt = np.diff(np.asarray(ts_epoch, dtype=float))
    dt = dt[(dt > 0) & np.isfinite(dt)]
    med_dt = float(np.median(dt)) if dt.size else 1.0
    sr = float(np.clip(1.0 / med_dt, 0.2, 5.0))

    w = max(3, int(window_s * sr))
    rm = pd.Series(x).rolling(w, min_periods=w).mean()
    if rm.dropna().empty:
        return (None, None)

    end_i = int(rm.idxmax())
    start_i = end_i - w + 1
    if start_i < 0:
        return (None, None)
    return (start_i, end_i)

def maybe_update_userprofile_master(*, birth_year=None, age_years=None, gender=None, lthr=None, source="unknown", activity_id=None):
    def _to_int(x):
        try:
            return int(float(x))
        except Exception:
            return None

    row = _get_userprofile_master_v1()

    stored_by   = _to_int(row.get("birth_year"))
    stored_age  = _to_int(row.get("age_years"))
    stored_gc   = _to_int(row.get("gender_code")) or 0
    stored_lthr = _to_int(row.get("lthr_bpm"))

    stored_gender_known = stored_gc in (1, 2)

    new_by   = _to_int(birth_year)
    new_age  = _to_int(age_years)
    new_lthr = _to_int(lthr)

    # if we have birth_year but no age, compute an approximate age (year-only)
    if new_age is None and new_by is not None:
        new_age = _age_from_birth_year(new_by, None)

    g = gender

    write_birth  = (new_by is not None) and (new_by != stored_by)
    write_age    = (new_age is not None) and (new_age != stored_age)
    write_gender = (g in {"male", "female"}) and (not stored_gender_known)
    write_lthr   = (new_lthr is not None) and (new_lthr > 0) and (new_lthr != stored_lthr)

    if not (write_birth or write_age or write_gender or write_lthr):
        return

    fields: dict[str, object] = {}

    if write_birth:
        fields["birth_year"] = int(new_by)
    if write_age:
        fields["age_years"] = int(new_age)

    if write_gender:
        fields["gender_code"] = _gender_code(g)
        fields["gender"] = g
        fields["gender_is_known"] = 1

    if write_lthr:
        fields["lthr_bpm"] = int(new_lthr)

    fields["source"] = source
    if activity_id is not None:
        fields["activity_id"] = int(activity_id)

    point = {
        "measurement": "UserProfileMaster",
        "time": datetime.now(pytz.UTC).isoformat(),
        "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
        "fields": fields,
    }
    write_points_to_influxdb([point])
    
def _iso_z(dt: datetime) -> str:
    return _influxq.iso_z(dt)

def _day_bounds_z(date_str: str) -> tuple[str, str]:
    return _influxq.day_bounds_z(date_str)

def _get_physiology_for_day_v1(date_str: str) -> tuple[float | None, float | None]:
    ctx = _InfluxV1QueryContext(
        influxdbclient=influxdbclient,
        influxdb_database=INFLUXDB_DATABASE,
        garmin_devicename=GARMIN_DEVICENAME,
        day_bounds_z=_day_bounds_z,
        dt_utc=_dt_utc,
        query_last_row=_query_last_row_influx_v1,
    )
    return _influxv1.get_physiology_for_day(date_str, ctx)


def _get_activities_for_day_v1(date_str: str) -> list[dict]:
    ctx = _InfluxV1QueryContext(
        influxdbclient=influxdbclient,
        influxdb_database=INFLUXDB_DATABASE,
        garmin_devicename=GARMIN_DEVICENAME,
        day_bounds_z=_day_bounds_z,
        dt_utc=_dt_utc,
        query_last_row=_query_last_row_influx_v1,
    )
    return _influxv1.get_activities_for_day(date_str, ctx)

def _get_trainingload_prev_day_v1(date_str: str) -> dict:
    ctx = _InfluxV1QueryContext(
        influxdbclient=influxdbclient,
        influxdb_database=INFLUXDB_DATABASE,
        garmin_devicename=GARMIN_DEVICENAME,
        day_bounds_z=_day_bounds_z,
        dt_utc=_dt_utc,
        query_last_row=_query_last_row_influx_v1,
    )
    return _influxv1.get_trainingload_prev_day(date_str, ctx)

def rolling_mean(x: np.ndarray, window: int) -> np.ndarray:
    if window <= 1:
        return x
    s = pd.Series(x)
    return s.rolling(window, center=True, min_periods=max(1, window//2)).mean().to_numpy()

def compute_grade(dist_m: np.ndarray, elev_m: np.ndarray, window_s: int, sample_rate_hz: float) -> np.ndarray:
    """
    Compute grade using smoothed elevation and distance over a time window.
    window_s: smoothing/derivative window in seconds
    """
    w = max(3, int(window_s * sample_rate_hz))
    elev_sm = rolling_mean(elev_m, w)

    # finite difference over window
    dd = np.roll(dist_m, -w) - dist_m
    dz = np.roll(elev_sm, -w) - elev_sm

    grade = np.zeros_like(dist_m, dtype=float)
    valid = (dd > 1.0)  # avoid divide-by-near-zero
    grade[valid] = dz[valid] / dd[valid]

    # last w points invalid due to roll
    grade[-w:] = grade[-w-1] if len(grade) > w+1 else 0.0

    # clamp insane grades (GPS/elev noise)
    grade = np.clip(grade, -0.30, 0.30)
    return grade

def minetti_cost_factor(grade: np.ndarray) -> np.ndarray:
    """
    Minetti-style polynomial for cost of running vs slope.
    Returns relative energy cost (dimensionless).
    Common form used in GAP implementations.
    """
    g = grade
    # polynomial (one widely-used approximation)
    C = (155.4*(g**5) - 30.4*(g**4) - 43.3*(g**3) + 46.3*(g**2) + 19.5*g + 3.6)
    # Normalize so flat cost is 1.0
    C0 = 3.6
    return C / C0

def gap_speed_mps(speed_mps: np.ndarray, grade: np.ndarray) -> np.ndarray:
    """
    Convert observed speed on grade to equivalent flat speed via cost ratio.
    """
    cf = minetti_cost_factor(grade)
    # If cost is higher than flat, equivalent flat speed is slower for same metabolic cost:
    # v_flat = v / cf
    v = np.clip(speed_mps, 0.0, None)
    v_gap = np.where(cf > 0, v / cf, v)
    # cap to avoid spikes
    return np.clip(v_gap, 0.0, 8.0)  # 8 m/s = 2:05/km (cap)

def acsm_vo2_running(speed_mps: np.ndarray, grade: np.ndarray) -> np.ndarray:
    """
    ACSM running metabolic equation.
    speed in m/s -> convert to m/min
    """
    v_m_min = speed_mps * 60.0
    g = grade
    vo2 = 0.2 * v_m_min + 0.9 * v_m_min * g + 3.5
    return np.clip(vo2, 0.0, 90.0)

def best_rolling_mean(series: np.ndarray, window_s: int, sample_rate_hz: float) -> float:
    w = max(3, int(window_s * sample_rate_hz))
    s = pd.Series(series)
    rm = s.rolling(w, min_periods=w).mean()
    if rm.dropna().empty:
        return np.nan
    return float(rm.max())

def fit_cs_from_gap_speed(gap_speed: np.ndarray, durations_s: list[int], sample_rate_hz: float) -> tuple[float, float]:
    """
    Returns (CS_mps, Dprime_m).
    """
    xs = []
    ys = []
    for t in durations_s:
        v_best = best_rolling_mean(gap_speed, t, sample_rate_hz)
        if np.isnan(v_best):
            continue
        D = v_best * t
        xs.append(t)
        ys.append(D)
    if len(xs) < 3:
        return (np.nan, np.nan)
    x = np.array(xs, dtype=float)
    y = np.array(ys, dtype=float)
    # linear regression y = a*x + b
    a, b = np.polyfit(x, y, 1)
    return (float(a), float(b))

def fit_cp_from_power(power_w: np.ndarray, durations_s: list[int], sample_rate_hz: float) -> tuple[float, float]:
    """
    Returns (CP_watts, Wprime_joules).
    """
    xs = []
    ys = []
    for t in durations_s:
        p_best = best_rolling_mean(power_w, t, sample_rate_hz)
        if np.isnan(p_best):
            continue
        W = p_best * t
        xs.append(t)
        ys.append(W)
    if len(xs) < 3:
        return (np.nan, np.nan)
    x = np.array(xs, dtype=float)
    y = np.array(ys, dtype=float)
    a, b = np.polyfit(x, y, 1)
    return (float(a), float(b))

# --- replace estimate_lthr_from_target(...) with this version ---
def estimate_lthr_from_target_contiguous(
    hr_bpm: np.ndarray,
    target_series: np.ndarray,
    target_value: float,
    sample_rate_hz: float,
    band: float = 0.05,              # loosened to ±5%
    min_contig_s: int = 10 * 60,     # require 10 min continuous in-band
    warmup_exclude_s: int = 10 * 60, # ignore first 10 min
    min_hr_bpm: float = 30.0,
) -> float:
    """
    Returns median HR over the LONGEST CONTIGUOUS segment where target_series
    is within ±band of target_value (after warmup). Requires that segment be
    at least min_contig_s long. Otherwise returns np.nan.

    This makes "steady state near threshold" explicit: one continuous block.
    """
    if target_value is None or not np.isfinite(target_value) or target_value <= 0:
        return np.nan
    if hr_bpm is None or target_series is None:
        return np.nan
    if len(hr_bpm) == 0 or len(target_series) == 0:
        return np.nan

    n = min(len(hr_bpm), len(target_series))
    hr = np.asarray(hr_bpm[:n], dtype=float)
    ts = np.asarray(target_series[:n], dtype=float)

    # time axis (seconds from start)
    t = np.arange(n, dtype=float) / float(sample_rate_hz if sample_rate_hz and sample_rate_hz > 0 else 1.0)

    lo = target_value * (1.0 - band)
    hi = target_value * (1.0 + band)

    mask = (
        (t >= warmup_exclude_s)
        & np.isfinite(ts)
        & (ts >= lo) & (ts <= hi)
        & np.isfinite(hr)
        & (hr >= min_hr_bpm)
    )

    if not mask.any():
        return np.nan

    # find longest contiguous True run
    idx = np.flatnonzero(mask)
    if idx.size == 0:
        return np.nan

    # breaks where difference > 1 index
    breaks = np.where(np.diff(idx) > 1)[0]
    # segments are [start_i, end_i] over idx array
    seg_starts = np.r_[0, breaks + 1]
    seg_ends   = np.r_[breaks, idx.size - 1]

    # choose longest by length in samples
    seg_lengths = (seg_ends - seg_starts + 1)
    best_seg = int(np.argmax(seg_lengths))

    s = int(idx[seg_starts[best_seg]])
    e = int(idx[seg_ends[best_seg]])  # inclusive
    contig_s = (e - s + 1) / float(sample_rate_hz if sample_rate_hz and sample_rate_hz > 0 else 1.0)

    if contig_s < float(min_contig_s):
        return np.nan

    # robust statistic
    return float(np.nanmedian(hr[s : e + 1]))

def _pace_s_per_km_from_mps(v_mps: float) -> float | None:
    try:
        v = float(v_mps)
        if v <= 0:
            return None
        return 1000.0 / v
    except Exception:
        return None

def derive_and_write_activity_metrics_v1(
    *,
    activity_id: int,
    activity_type: str,
    activity_start_time: datetime,
    all_records_list: list[dict],
) -> None:
    """
    Writes ONE point per activity into measurement DerivedActivity.

    Running:
      - grade from distance+elevation
      - GAP speed (Minetti)
      - CS from best 3/6/12/20/30min GAP speeds (distance-time model)
      - VO2 demand from ACSM using GAP (flat)
      - VO2max_est = best 5min VO2 demand
      - LTHR_est = median HR when GAP speed ~ CS (±2%) after 10 min warmup

    Cycling:
      - CP from best 3/6/12/20/30min power (work-time model)
      - LTHR_est = median HR when power ~ CP (±2%) after 10 min warmup
    """
    if INFLUXDB_VERSION != "1":
        return  # keep v1-only for now

    # decide sport
    is_run = _is_run(activity_type)
    is_ride = _is_ride(activity_type)
    if not (is_run or is_ride):
        return

    # extract series
    ts_list = []
    dist = []
    elev = []
    speed = []
    hr = []
    power = []

    for r in all_records_list:
        ts = r.get("timestamp")
        if ts is None:
            continue
        ts = ts.replace(tzinfo=pytz.UTC)
        ts_list.append(ts)

        dist.append(r.get("distance"))
        elev.append(r.get("enhanced_altitude") or r.get("altitude"))
        speed.append(r.get("enhanced_speed") or r.get("speed"))
        hr.append(r.get("heart_rate"))
        power.append(r.get("power"))

    if len(ts_list) < 300:  # <~5 minutes at 1 Hz
        return

    # numpy arrays + cleaning

    def _to_float_arr(x):
        return np.array([np.nan if v is None else float(v) for v in x], dtype=float)

    # 1) timestamps first
    ts_epoch = np.array([t.timestamp() for t in ts_list], dtype=float)  # seconds since epoch (UTC)

    # 2) convert lists -> arrays
    dist_m    = _to_float_arr(dist)
    elev_m    = _to_float_arr(elev)
    speed_mps = _to_float_arr(speed)
    hr_bpm    = _to_float_arr(hr)
    power_w   = _to_float_arr(power)

    # 3) sort everything by timestamp
    order = np.argsort(ts_epoch)
    ts_epoch  = ts_epoch[order]
    dist_m    = dist_m[order]
    elev_m    = elev_m[order]
    speed_mps = speed_mps[order]
    hr_bpm    = hr_bpm[order]
    power_w   = power_w[order]

    # 4) drop duplicate timestamps (optional but helps stability)
    #    keep first occurrence
    if ts_epoch.size > 1:
        keep = np.concatenate(([True], np.diff(ts_epoch) > 0))
        ts_epoch  = ts_epoch[keep]
        dist_m    = dist_m[keep]
        elev_m    = elev_m[keep]
        speed_mps = speed_mps[keep]
        hr_bpm    = hr_bpm[keep]
        power_w   = power_w[keep]

    # basic validity
    if not np.isfinite(speed_mps).any() or np.nanmax(speed_mps) <= 0:
        return

    # 5) compute sample rate from *sorted, de-duped* timestamps
    dt_s = np.diff(ts_epoch)
    dt_s = dt_s[(dt_s > 0) & np.isfinite(dt_s)]

    med_dt = float(np.median(dt_s)) if dt_s.size else 1.0
    if not (med_dt > 0):
        med_dt = 1.0
    sample_rate_hz = float(np.clip(1.0 / med_dt, 0.2, 5.0))

    # tags (match your conventions)
    selector = activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + str(activity_type)

    tags = {
        "Device": GARMIN_DEVICENAME,
        "Database_Name": INFLUXDB_DATABASE,
        "ActivityID": str(activity_id),
        "ActivitySelector": selector,
        "sport_tag": _norm_tag_value(activity_type),
    }

    fields: dict[str, object] = {
        "Activity_ID": int(activity_id),
    }

    durations = [180, 360, 720, 1200, 1800]  # 3/6/12/20/30 min

    # --- helpers ---
    def _sum_distance_from_speed(speed_mps: np.ndarray, sample_rate_hz: float) -> float | None:
        try:
            if speed_mps is None or len(speed_mps) == 0:
                return None
            sr = float(sample_rate_hz) if sample_rate_hz and sample_rate_hz > 0 else 1.0
            dt = 1.0 / sr
            v = np.asarray(speed_mps, dtype=float)
            v = np.where(np.isfinite(v) & (v > 0), v, 0.0)
            return float(np.sum(v) * dt)
        except Exception:
            return None

    def _sum_distance_from_speed_var_dt(speed_mps: np.ndarray, ts_epoch: np.ndarray) -> float | None:
        try:
            v = np.asarray(speed_mps, dtype=float)
            t = np.asarray(ts_epoch, dtype=float)
            if len(v) < 2 or len(t) != len(v):
                return None
            dt = np.diff(t)
            dt = np.where(np.isfinite(dt) & (dt > 0), dt, 0.0)
            v_mid = np.where(np.isfinite(v[:-1]) & (v[:-1] > 0), v[:-1], 0.0)
            return float(np.sum(v_mid * dt))
        except Exception:
            return None
    
    if is_run:
        # require dist+elev for grade; if missing, fall back to grade=0 and GAP=raw speed
        if np.isfinite(dist_m).any():
            dist_m = np.maximum.accumulate(np.where(np.isfinite(dist_m), dist_m, -np.inf))   
        if np.isfinite(dist_m).sum() > 100 and np.isfinite(elev_m).sum() > 100:
            grade = compute_grade(dist_m, elev_m, window_s=20, sample_rate_hz=sample_rate_hz)
            v_gap = gap_speed_mps(speed_mps, grade)
        else:
            grade = np.zeros_like(speed_mps)
            v_gap = np.clip(speed_mps, 0.0, 8.0)

        # effective distance on flat from GAP
        #gap_dist_m = _sum_distance_from_speed(v_gap, sample_rate_hz)
        gap_dist_m = _sum_distance_from_speed_var_dt(v_gap, ts_epoch)
        raw_dist_m = _sum_distance_from_speed_var_dt(np.clip(speed_mps, 0.0, None), ts_epoch)
        
        fields["gap_distance_m"] = gap_dist_m
        fields["raw_distance_m_from_fit"] = float(np.nanmax(dist_m)) if np.isfinite(dist_m).any() else None
        fields["raw_distance_m_from_speed"] = raw_dist_m  # optional sanity check
        fields["gap_distance_km"] = (gap_dist_m / 1000.0) if gap_dist_m is not None else None
        
        cs_mps, dprime_m = fit_cs_from_gap_speed(v_gap, durations, sample_rate_hz)
        fields["cs_mps"] = float(cs_mps) if np.isfinite(cs_mps) else None
        fields["dprime_m"] = float(dprime_m) if np.isfinite(dprime_m) else None

        cs_pace = _pace_s_per_km_from_mps(cs_mps) if np.isfinite(cs_mps) else None
        fields["cs_pace_s_per_km"] = cs_pace
        fields["lt_pace_s_per_km"] = cs_pace  # LT pace proxy = CS pace

        # VO2 demand using raw speed + grade (ACSM), mountain-safe.
        # Apply conservative mask so downhill/noise doesn't dominate.
        vo2_raw = acsm_vo2_running(np.clip(speed_mps, 0.0, 8.0), grade)
        
        vo2_valid = (
            np.isfinite(vo2_raw)
            & np.isfinite(speed_mps)
            & np.isfinite(grade)
            & (speed_mps >= float(os.getenv("VO2_SPEED_FLOOR_MPS", "1.2")))  # hiking? drop to 0.8–1.0
            & (grade >= float(os.getenv("VO2_GRADE_FLOOR", "0.0")))
            & (grade <= float(os.getenv("VO2_GRADE_CEIL", "0.30")))
        )
        
        vo2_f = np.where(vo2_valid, vo2_raw, np.nan)
        
        best5_vo2 = best_rolling_mean(vo2_f, 300, sample_rate_hz)
        fields["best5m_vo2_masked"] = float(best5_vo2) if np.isfinite(best5_vo2) else None
        fields["vo2max_est"] = fields["best5m_vo2_masked"]

        best5_all = best_rolling_mean(vo2_raw, 300, sample_rate_hz)
        fields["best5m_vo2_all"] = float(best5_all) if np.isfinite(best5_all) else None

        fields["vo2_mask_speed_floor_mps"] = float(os.getenv("VO2_SPEED_FLOOR_MPS", "1.2"))
        fields["vo2_mask_grade_floor"] = float(os.getenv("VO2_GRADE_FLOOR", "0.0"))
        fields["vo2_mask_grade_ceil"] = float(os.getenv("VO2_GRADE_CEIL", "0.30"))
        
        # optional diagnostics
        best5_speed = best_rolling_mean(np.clip(speed_mps, 0.0, None), 300, sample_rate_hz)
        fields["best5m_speed_mps_raw"] = float(best5_speed) if np.isfinite(best5_speed) else None
        fields["best5m_grade_median"] = float(np.nanmedian(grade)) if np.isfinite(grade).any() else None

        # LTHR estimate from near-CS segments
        if np.isfinite(cs_mps) and np.isfinite(hr_bpm).sum() > 100:
            lthr_est = estimate_lthr_from_target_contiguous(
                hr_bpm=hr_bpm,
                target_series=v_gap,
                target_value=cs_mps,
                sample_rate_hz=sample_rate_hz,
                band=0.05,              # ±5%
                min_contig_s=10 * 60,   # 10 min continuous
                warmup_exclude_s=10*60
            )
            fields["lthr_bpm_est"] = float(lthr_est) if np.isfinite(lthr_est) else None
            # add near where you set lthr_bpm_est (after computing lthr_est)
            fields["lthr_target_mps"] = float(cs_mps)
            fields["lthr_band"] = float(0.05)
            fields["lthr_min_contig_s"] = int(10 * 60)

        # --- Mountain-native fitness: Vertical Ascent Rate (VAM) ---
        # Need elevation
        if np.isfinite(elev_m).sum() > 100:
            vz_mps = vertical_speed_mps(ts_epoch, elev_m, smooth_s=10.0)

            # positive-only climb rate
            vz_up = np.where(vz_mps > 0, vz_mps, 0.0)

            # Gating to avoid "best" being dominated by coasting / pauses / nonsense
            # Tune hr_floor for beta blocker reality (110–120 often works better than 130+)
            hr_floor = float(os.getenv("VAM_HR_FLOOR", "115"))
            speed_floor = float(os.getenv("VAM_SPEED_FLOOR_MPS", "1.0"))  # ~3:20/km; adjust if needed
            warmup_exclude_s = int(os.getenv("VAM_WARMUP_EXCLUDE_S", str(10 * 60)))

            # time axis for warmup exclusion
            t_rel = ts_epoch - ts_epoch[0]
            valid = (
                np.isfinite(vz_up)
                & np.isfinite(speed_mps)
                & (speed_mps >= speed_floor)
                & np.isfinite(hr_bpm)
                & (hr_bpm >= hr_floor)
                & (t_rel >= warmup_exclude_s)
            )

            # Best sustained climb rate over 20 and 30 minutes
            best20_vz = best_rolling_mean_masked(vz_up, ts_epoch, window_s=20*60, mask=valid)
            best30_vz = best_rolling_mean_masked(vz_up, ts_epoch, window_s=30*60, mask=valid)

            # Store as VAM (m/h) and m/min too (handy)
            if np.isfinite(best20_vz):
                fields["best20m_vam_m_per_h"] = float(best20_vz * 3600.0)
                fields["best20m_vert_m_per_min"] = float(best20_vz * 60.0)
            if np.isfinite(best30_vz):
                fields["best30m_vam_m_per_h"] = float(best30_vz * 3600.0)
                fields["best30m_vert_m_per_min"] = float(best30_vz * 60.0)

            # Optional: store the paired HR and distance for the best-20m window
            s_i, e_i = _best_window_index(vz_up, ts_epoch, window_s=20*60, mask=valid)
            if s_i is not None and e_i is not None:
                fields["best20m_hr_median"] = float(np.nanmedian(hr_bpm[s_i:e_i+1]))
                # distance in that window (from raw speed variable dt)
                try:
                    t_win = ts_epoch[s_i:e_i+1]
                    v_win = np.clip(speed_mps[s_i:e_i+1], 0.0, None)
                    fields["best20m_distance_m"] = float(_sum_distance_from_speed_var_dt(v_win, t_win))
                except Exception:
                    pass

            # Record gating params for auditability
            fields["vam_hr_floor"] = float(hr_floor)
            fields["vam_speed_floor_mps"] = float(speed_floor)
            fields["vam_warmup_exclude_s"] = int(warmup_exclude_s)
    

    if is_ride:
        # require power for CP
        if np.isfinite(power_w).sum() > 100 and np.nanmax(power_w) > 0:
            cp_w, wprime_j = fit_cp_from_power(power_w, durations, sample_rate_hz)
            fields["cp_watts"] = float(cp_w) if np.isfinite(cp_w) else None
            fields["wprime_j"] = float(wprime_j) if np.isfinite(wprime_j) else None

            if np.isfinite(cp_w) and np.isfinite(hr_bpm).sum() > 100:
                lthr_est = estimate_lthr_from_target(hr_bpm, power_w, cp_w, sample_rate_hz)
                fields["lthr_bpm_est"] = float(lthr_est) if np.isfinite(lthr_est) else None
    
    # write point (ONE per activity)
    point = {
        "measurement": "DerivedActivity",
        "time": activity_start_time.replace(tzinfo=pytz.UTC).isoformat(),
        "tags": tags,
        "fields": fields,
    }
    write_points_to_influxdb([point])

def _bannister_trimp(dur_seconds: float, hr_avg: float, rhr: float, hrmax: float, gender: str) -> float:
    if dur_seconds <= 0 or hr_avg <= 0 or hrmax <= rhr:
        return 0.0
    hrr = hrmax - rhr
    hrratio = (hr_avg - rhr) / hrr
    # clamp
    if hrratio < 0:
        hrratio = 0.0
    elif hrratio > 1:
        hrratio = 1.0

    dur_min = dur_seconds / 60.0

    g = _norm_gender(gender)
    if g not in {"male","female"}:
        return 0.0
    if g == "female":
        k, b = 0.86, 1.67
    else:
        #hard-fail instead
        k, b = 0.64, 1.92

    return dur_min * hrratio * k * math.exp(b * hrratio)

def _hr_tss(dur_seconds: float, hr_avg: float, hr_thresh: float) -> float:
    """
    HR-based TSS-like score. Requires an estimated threshold HR (LTHR).
    Uses intensity factor IF = hr_avg / hr_thresh, then TSS = hours * IF^2 * 100.

    This is deliberately analogous to power TSS (hours * IF^2 * 100).
    """
    if dur_seconds <= 0 or hr_avg <= 0 or hr_thresh <= 0:
        return 0.0
    if hr_avg < 30:  # guard
        return 0.0

    hours = dur_seconds / 3600.0
    IF = hr_avg / hr_thresh
    # clamp to avoid absurd spikes from bad hr_thresh
    IF = max(0.3, min(IF, 1.5))
    return 100.0 * hours * (IF ** 2)

def compute_and_write_training_load(date_str: str) -> None:
    ctx = _RollupContext(
        influxdb_version=INFLUXDB_VERSION,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
        write_points_to_influxdb=write_points_to_influxdb,
        dt_utc=_dt_utc,
        norm_gender=_norm_gender,
        gender_code=_gender_code,
        median_rhr_7d=_median_rhr_7d,
        estimate_hrmax_activity_backoff=_estimate_hrmax_activity_backoff,
        hrr_zones=_hrr_zones,
        get_gender_for_day_v1=_get_gender_for_day_v1,
        get_userprofile_master_v1=_get_userprofile_master_v1,
        get_physiology_for_day_v1=_get_physiology_for_day_v1,
        stored_lthr_bpm_v1=_stored_lthr_bpm_v1,
        get_activities_for_day_v1=_get_activities_for_day_v1,
        get_trainingload_prev_day_v1=_get_trainingload_prev_day_v1,
        iso_z=_iso_z,
        query_scalar_influx_v1=_query_scalar_influx_v1,
        query_last_row_influx_v1=_query_last_row_influx_v1,
    )
    return _rollups.compute_and_write_training_load(date_str, ctx)

def run_rollups_for_range(start_date: str, end_date: str) -> None:
    return _run_rollups_for_range(
        start_date=start_date,
        end_date=end_date,
        influxdb_version=INFLUXDB_VERSION,
        compute_and_write_physiology=compute_and_write_physiology,
        compute_and_write_training_load=compute_and_write_training_load,
        compute_and_write_performance_daily=compute_and_write_performance_daily,
    )

def _userprofile_exists_for_day_v1(date_str: str) -> bool:
    if INFLUXDB_VERSION != "1":
        return False
    ctx = _InfluxV1QueryContext(
        influxdbclient=influxdbclient,
        influxdb_database=INFLUXDB_DATABASE,
        garmin_devicename=GARMIN_DEVICENAME,
        day_bounds_z=_day_bounds_z,
        dt_utc=_dt_utc,
        query_last_row=_query_last_row_influx_v1,
    )
    return _influxv1.userprofile_exists_for_day(date_str, ctx)

def _userprofile_known_for_day_v1(date_str: str) -> bool:
    ctx = _InfluxV1QueryContext(
        influxdbclient=influxdbclient,
        influxdb_database=INFLUXDB_DATABASE,
        garmin_devicename=GARMIN_DEVICENAME,
        day_bounds_z=_day_bounds_z,
        dt_utc=_dt_utc,
        query_last_row=_query_last_row_influx_v1,
    )
    return _influxv1.userprofile_known_for_day(date_str, ctx)
    
def _percentile_activity_maxhr_42d(asof_date: str) -> float | None:
    start = (_dt_utc(asof_date) - timedelta(days=41)).strftime("%Y-%m-%d")
    start_dt = _dt_utc(start)
    end_dt = start_dt + timedelta(days=42)
    start_z, end_z = _iso_z(start_dt), _iso_z(end_dt)

    q = (
        'SELECT percentile("maxHR", 95) '
        'FROM "ActivitySummary" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        'AND "maxHR" > 0 '
        "AND activityName != 'END' "
        f"AND \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}'"
    )
    return _query_scalar_influx_v1(q)

def _count_activity_maxhr_window(asof_date: str, window_days: int) -> int:
    start = (_dt_utc(asof_date) - timedelta(days=window_days - 1)).strftime("%Y-%m-%d")
    start_iso, end_iso = _range_utc(start, window_days)
    q = (
        'SELECT count("maxHR") AS c '
        'FROM "ActivitySummary" '
        f"WHERE time >= '{start_iso}' AND time < '{end_iso}' "
        'AND "maxHR" > 0 AND activityName != \'END\' '
        f"AND \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}'"
    )
    v = _query_scalar_influx_v1(q)
    return int(v) if v is not None else 0

def _p95_activity_maxhr_window(asof_date: str, window_days: int) -> float | None:
    start = (_dt_utc(asof_date) - timedelta(days=window_days - 1)).strftime("%Y-%m-%d")
    start_iso, end_iso = _range_utc(start, window_days)
    q = (
        'SELECT percentile("maxHR", 95) '
        'FROM "ActivitySummary" '
        f"WHERE time >= '{start_iso}' AND time < '{end_iso}' "
        'AND "maxHR" > 0 AND activityName != \'END\' '
        f"AND \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}'"
    )
    return _query_scalar_influx_v1(q)

def _estimate_hrmax_activity_backoff(asof_date: str, windows: list[int] = [42, 84], min_points: int = 5) -> tuple[float | None, str]:
    """
    Returns (hrmax_est, source_label)
    - Uses activity-only p95 maxHR over 42d, then 84d if needed.
    - Only accepts a window if it has at least min_points maxHR samples.
    - If no activity samples over the largest window, returns age-based fallback.
    """
    for w in windows:
        n = _count_activity_maxhr_window(asof_date, w)
        if n >= min_points:
            p95 = _p95_activity_maxhr_window(asof_date, w)
            if p95 is not None and p95 > 0:
                return float(p95), f"activity_p95_{w}d(n={n})"

    # No sufficient activity samples in any window -> age-based fallback
    age = _get_age_years(asof_date)
    hrmax_f = _hrmax_age_based(age, method=HRMAX_FALLBACK_METHOD)
    if hrmax_f is not None:
        return float(hrmax_f), f"age_fallback_{HRMAX_FALLBACK_METHOD}(age={age})"

    return None, "no_hrmax_available"

def _median_rhr_7d(asof_date: str) -> float | None:
    start = (_dt_utc(asof_date) - timedelta(days=6)).strftime("%Y-%m-%d")
    start_iso, end_iso = _range_utc(start, 7)
    q = (
        f'SELECT median("restingHeartRate") '
        f'FROM "DailyStats" '
        f"WHERE time >= '{start_iso}' AND time < '{end_iso}' AND \"restingHeartRate\" > 0"
    )
    return _query_scalar_influx_v1(q)

def _hrr_zones(rhr: float, hrmax: float) -> dict[str, float]:
    hrr = hrmax - rhr

    def bpm(p: float) -> float:
        return rhr + p * hrr

    return {
        "HRR": hrr,
        "Z1_Low": bpm(0.50),
        "Z1_High": bpm(0.60),
        "Z2_Low": bpm(0.60),
        "Z2_High": bpm(0.70),
        "Z3_Low": bpm(0.70),
        "Z3_High": bpm(0.80),
        "Z4_Low": bpm(0.80),
        "Z4_High": bpm(0.90),
        "Z5_Low": bpm(0.90),
        "Z5_High": bpm(1.00),
    }

def _get_birth_year_for_day_v1(date_str: str) -> int | None:
    ctx = _InfluxV1QueryContext(
        influxdbclient=influxdbclient,
        influxdb_database=INFLUXDB_DATABASE,
        garmin_devicename=GARMIN_DEVICENAME,
        day_bounds_z=_day_bounds_z,
        dt_utc=_dt_utc,
        query_last_row=_query_last_row_influx_v1,
    )
    return _influxv1.get_birth_year_for_day(date_str, ctx)

def _get_birth_year_from_garmin_profile() -> int | None:
    try:
        prof = garmin_obj.get_user_profile() if garmin_obj is not None else {}
        b = None
        if isinstance(prof, dict):
            b = prof.get("birthDate") or prof.get("birthdate") or prof.get("dateOfBirth")
            if not b and isinstance(prof.get("userProfile"), dict):
                b = prof["userProfile"].get("birthDate") or prof["userProfile"].get("birthdate")
        return _birth_year_from_any(b)
    except Exception:
        logging.exception("Unable to extract birth year from Garmin user profile")
        return None

def fitness_age_from_vo2(vo2: float, gender: str) -> float | None:
    """
    Returns an approximate 'fitness age' from VO2max.
    Replace the coefficients/table with your preferred normative source.
    """
    if vo2 is None or not np.isfinite(vo2) or vo2 <= 0:
        return None
    g = _norm_gender(gender)
    # placeholder: very rough mapping bands (replace with real normative mapping)
    # higher VO2 => younger age-equivalent
    if g == "male":
        # crude piecewise mapping
        if vo2 >= 60: return 25.0
        if vo2 >= 55: return 30.0
        if vo2 >= 50: return 35.0
        if vo2 >= 45: return 45.0
        if vo2 >= 40: return 55.0
        return 65.0
    if g == "female":
        if vo2 >= 55: return 25.0
        if vo2 >= 50: return 30.0
        if vo2 >= 45: return 35.0
        if vo2 >= 40: return 45.0
        if vo2 >= 35: return 55.0
        return 65.0
    return None

def compute_and_write_physiology(asof_date: str) -> None:
    ctx = _RollupContext(
        influxdb_version=INFLUXDB_VERSION,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
        write_points_to_influxdb=write_points_to_influxdb,
        dt_utc=_dt_utc,
        norm_gender=_norm_gender,
        gender_code=_gender_code,
        median_rhr_7d=_median_rhr_7d,
        estimate_hrmax_activity_backoff=_estimate_hrmax_activity_backoff,
        hrr_zones=_hrr_zones,
        get_gender_for_day_v1=_get_gender_for_day_v1,
        get_userprofile_master_v1=_get_userprofile_master_v1,
        get_physiology_for_day_v1=_get_physiology_for_day_v1,
        stored_lthr_bpm_v1=_stored_lthr_bpm_v1,
        get_activities_for_day_v1=_get_activities_for_day_v1,
        get_trainingload_prev_day_v1=_get_trainingload_prev_day_v1,
        iso_z=_iso_z,
        query_scalar_influx_v1=_query_scalar_influx_v1,
        query_last_row_influx_v1=_query_last_row_influx_v1,
    )
    return _rollups.compute_and_write_physiology(asof_date, ctx)

# %%
def get_daily_stats(date_str):
    ctx = _DailyFetchContext(garmin_obj=garmin_obj, garmin_devicename=GARMIN_DEVICENAME, influxdb_database=INFLUXDB_DATABASE)
    return _daily_fetchers.get_daily_stats(date_str, ctx)

# %%
def get_last_sync():
    global GARMIN_DEVICENAME
    global GARMIN_DEVICEID
    ctx = _DeviceFetchContext(
        garmin_obj=garmin_obj,
        influxdb_database=INFLUXDB_DATABASE,
        garmin_devicename_automatic=GARMIN_DEVICENAME_AUTOMATIC,
        current_garmin_devicename=GARMIN_DEVICENAME,
    )
    points_list, dev_name, dev_id = _device_fetchers.get_last_sync(ctx)
    if GARMIN_DEVICENAME_AUTOMATIC:
        GARMIN_DEVICENAME = dev_name
        GARMIN_DEVICEID = dev_id
    return points_list

# %%
def get_sleep_data(date_str):
    ctx = _DailyFetchContext(garmin_obj=garmin_obj, garmin_devicename=GARMIN_DEVICENAME, influxdb_database=INFLUXDB_DATABASE)
    return _daily_fetchers.get_sleep_data(date_str, ctx)

# %%
def get_intraday_hr(date_str):
    ctx = _IntradayFetchContext(garmin_obj=garmin_obj, garmin_devicename=GARMIN_DEVICENAME, influxdb_database=INFLUXDB_DATABASE)
    return _intraday_fetchers.get_intraday_hr(date_str, ctx)

# %%
def get_intraday_steps(date_str):
    ctx = _IntradayFetchContext(garmin_obj=garmin_obj, garmin_devicename=GARMIN_DEVICENAME, influxdb_database=INFLUXDB_DATABASE)
    return _intraday_fetchers.get_intraday_steps(date_str, ctx)

# %%
def get_intraday_stress(date_str):
    ctx = _IntradayFetchContext(garmin_obj=garmin_obj, garmin_devicename=GARMIN_DEVICENAME, influxdb_database=INFLUXDB_DATABASE)
    return _intraday_fetchers.get_intraday_stress(date_str, ctx)

# %%
def get_intraday_br(date_str):
    ctx = _IntradayFetchContext(garmin_obj=garmin_obj, garmin_devicename=GARMIN_DEVICENAME, influxdb_database=INFLUXDB_DATABASE)
    return _intraday_fetchers.get_intraday_br(date_str, ctx)

# %%
def get_intraday_hrv(date_str):
    ctx = _IntradayFetchContext(garmin_obj=garmin_obj, garmin_devicename=GARMIN_DEVICENAME, influxdb_database=INFLUXDB_DATABASE)
    return _intraday_fetchers.get_intraday_hrv(date_str, ctx)

# %%
def get_body_composition(date_str):
    ctx = _HealthFetchContext(
        garmin_obj=garmin_obj,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
    )
    return _health_fetchers.get_body_composition(date_str, ctx)

# %%
def get_activity_summary(date_str):
    ctx = _ActivityFetchContext(
        garmin_obj=garmin_obj,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
        always_process_fit_files=ALWAYS_PROCESS_FIT_FILES,
        norm_tag_value=_norm_tag_value,
    )
    return _activity_fetchers.get_activity_summary(date_str, ctx)

# %%
def fetch_activity_GPS(activityIDdict):  # Uses FIT file by default, falls back to TCX
    ctx = ActivityGPSContext(
        garmin_obj=garmin_obj,
        parsed_activity_id_list=PARSED_ACTIVITY_ID_LIST,
        force_reprocess_activities=FORCE_REPROCESS_ACTIVITIES,
        keep_fit_files=KEEP_FIT_FILES,
        fit_file_storage_location=FIT_FILE_STORAGE_LOCATION,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
        norm_tag_value=_norm_tag_value,
        norm_gender=_norm_gender,
        birth_year_from_any=_birth_year_from_any,
        stored_birth_year_v1=_stored_birth_year_v1,
        get_birth_year_from_garmin_profile=_get_birth_year_from_garmin_profile,
        write_points_to_influxdb=write_points_to_influxdb,
        write_user_profile_point=write_user_profile_point,
        maybe_update_userprofile_master=maybe_update_userprofile_master,
        derive_and_write_activity_metrics_v1=derive_and_write_activity_metrics_v1,
        fit_gender_cache=FIT_GENDER_CACHE,
    )
    return _fetch_activity_GPS(activityIDdict, ctx)

# %%
def get_lactate_threshold(date_str):
    ctx = _TrainingFetchContext(
        garmin_obj=garmin_obj,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
        lactate_threshold_sports=LACTATE_THRESHOLD_SPORTS,
    )
    return _training_fetchers.get_lactate_threshold(date_str, ctx)

# %%
def get_training_status(date_str):
    ctx = _TrainingFetchContext(
        garmin_obj=garmin_obj,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
        lactate_threshold_sports=LACTATE_THRESHOLD_SPORTS,
    )
    return _training_fetchers.get_training_status(date_str, ctx)

# %%
def get_training_readiness(date_str):
    ctx = _TrainingFetchContext(
        garmin_obj=garmin_obj,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
        lactate_threshold_sports=LACTATE_THRESHOLD_SPORTS,
    )
    return _training_fetchers.get_training_readiness(date_str, ctx)

# %%
def get_hillscore(date_str):
    ctx = _TrainingFetchContext(
        garmin_obj=garmin_obj,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
        lactate_threshold_sports=LACTATE_THRESHOLD_SPORTS,
    )
    return _training_fetchers.get_hillscore(date_str, ctx)

# %%
def get_race_predictions(date_str):
    ctx = _TrainingFetchContext(
        garmin_obj=garmin_obj,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
        lactate_threshold_sports=LACTATE_THRESHOLD_SPORTS,
    )
    return _training_fetchers.get_race_predictions(date_str, ctx)

# %%
def get_fitness_age(date_str):
    ctx = _TrainingFetchContext(
        garmin_obj=garmin_obj,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
        lactate_threshold_sports=LACTATE_THRESHOLD_SPORTS,
    )
    return _training_fetchers.get_fitness_age(date_str, ctx)

# %%
def get_vo2_max(date_str):
    ctx = _TrainingFetchContext(
        garmin_obj=garmin_obj,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
        lactate_threshold_sports=LACTATE_THRESHOLD_SPORTS,
    )
    return _training_fetchers.get_vo2_max(date_str, ctx)

# %%
def get_endurance_score(date_str):
    ctx = _TrainingFetchContext(
        garmin_obj=garmin_obj,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
        lactate_threshold_sports=LACTATE_THRESHOLD_SPORTS,
    )
    return _training_fetchers.get_endurance_score(date_str, ctx)

# %%
def get_blood_pressure(date_str):
    ctx = _HealthFetchContext(
        garmin_obj=garmin_obj,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
    )
    return _health_fetchers.get_blood_pressure(date_str, ctx)

# %%
def get_hydration(date_str):
    ctx = _HealthFetchContext(
        garmin_obj=garmin_obj,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
    )
    return _health_fetchers.get_hydration(date_str, ctx)

# %%
def get_solar_intensity(date_str):
    points_list = []

    if not GARMIN_DEVICEID:
        logging.warning("Skipping Solar Intensity data fetch as GARMIN_DEVICEID is not set.")
        return points_list

    si_all = garmin_obj.get_device_solar_data(GARMIN_DEVICEID, date_str) or {}
    daily = si_all.get("solarDailyDataDTOs", []) or []
    if len(daily) > 0:
        si_list = (daily[0] or {}).get("solarInputReadings", []) or []
        for si_measurement in si_list:
            data_fields = {
                "solarUtilization": si_measurement.get("solarUtilization", None),
                "activityTimeGainMs": si_measurement.get("activityTimeGainMs", None),
            }
            ts = si_measurement.get("readingTimestampGmt")
            if ts and (not all(v is None for v in data_fields.values())):
                points_list.append(
                    {
                        "measurement": "SolarIntensity",
                        "time": pytz.UTC.localize(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                        "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                        "fields": data_fields,
                    }
                )
        logging.info(f"Success : Fetching Solar Intensity data for date {date_str}")

    if len(points_list) == 0:
        logging.warning(f"No Solar Intensity data available for date {date_str}")
    return points_list

# %%
def get_lifestyle_data(date_str):
    points_list = []
    try:
        logging.info(f"Fetching Lifestyle Journaling data for date {date_str}")
        journal_data = garmin_obj.get_lifestyle_logging_data(date_str) or {}
        daily_logs = journal_data.get("dailyLogsReport", []) or []

        for log in daily_logs:
            behavior_name = log.get("name") or log.get("behavior")
            if not behavior_name:
                continue

            category = log.get("category", "UNKNOWN")
            log_status = log.get("logStatus")
            details = log.get("details", []) or []

            status = 1 if log_status == "YES" else 0

            value = 0.0
            for detail in details:
                amount = detail.get("amount")
                if amount is not None:
                    value += float(amount)

            points_list.append(
                {
                    "measurement": "LifestyleJournal",
                    "time": pytz.timezone("UTC").localize(datetime.strptime(date_str, "%Y-%m-%d")).isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE,
                        "behavior": behavior_name,
                        "category": category,
                    },
                    "fields": {"status": status, "value": value},
                }
            )

        logging.info(f"Success : Fetching Lifestyle Journaling data for date {date_str}")

    except Exception as e:
        logging.warning(f"Failed to fetch Lifestyle Journaling data for date {date_str}: {e}")

    return points_list

def compute_and_write_performance_daily(asof_date: str) -> None:
    ctx = _RollupContext(
        influxdb_version=INFLUXDB_VERSION,
        garmin_devicename=GARMIN_DEVICENAME,
        influxdb_database=INFLUXDB_DATABASE,
        write_points_to_influxdb=write_points_to_influxdb,
        dt_utc=_dt_utc,
        norm_gender=_norm_gender,
        gender_code=_gender_code,
        median_rhr_7d=_median_rhr_7d,
        estimate_hrmax_activity_backoff=_estimate_hrmax_activity_backoff,
        hrr_zones=_hrr_zones,
        get_gender_for_day_v1=_get_gender_for_day_v1,
        get_userprofile_master_v1=_get_userprofile_master_v1,
        get_physiology_for_day_v1=_get_physiology_for_day_v1,
        stored_lthr_bpm_v1=_stored_lthr_bpm_v1,
        get_activities_for_day_v1=_get_activities_for_day_v1,
        get_trainingload_prev_day_v1=_get_trainingload_prev_day_v1,
        iso_z=_iso_z,
        query_scalar_influx_v1=_query_scalar_influx_v1,
        query_last_row_influx_v1=_query_last_row_influx_v1,
    )
    return _rollups.compute_and_write_performance_daily(asof_date, ctx)

# %%
def daily_fetch_write(date_str, *, run_rollups_inline: bool = True):
    ctx = _DailyFetchWriteContext(
        garmin_obj=garmin_obj,
        request_intraday_data_refresh=REQUEST_INTRADAY_DATA_REFRESH,
        ignore_intraday_data_refresh_days=IGNORE_INTRADAY_DATA_REFRESH_DAYS,
        fetch_selection=FETCH_SELECTION,
        userprofile_write_once_per_day=USERPROFILE_WRITE_ONCE_PER_DAY,
        userprofile_exists_for_day_v1=_userprofile_exists_for_day_v1,
        get_user_gender_from_garmin=_get_user_gender_from_garmin,
        stored_birth_year_v1=_stored_birth_year_v1,
        get_birth_year_from_garmin_profile=_get_birth_year_from_garmin_profile,
        write_user_profile_point=write_user_profile_point,
        fit_gender_cache=FIT_GENDER_CACHE,
        write_points_to_influxdb=write_points_to_influxdb,
        get_daily_stats=get_daily_stats,
        get_sleep_data=get_sleep_data,
        get_intraday_steps=get_intraday_steps,
        get_intraday_hr=get_intraday_hr,
        get_intraday_stress=get_intraday_stress,
        get_intraday_br=get_intraday_br,
        get_intraday_hrv=get_intraday_hrv,
        get_fitness_age=get_fitness_age,
        get_vo2_max=get_vo2_max,
        get_race_predictions=get_race_predictions,
        get_body_composition=get_body_composition,
        get_lactate_threshold=get_lactate_threshold,
        get_training_status=get_training_status,
        get_training_readiness=get_training_readiness,
        get_hillscore=get_hillscore,
        get_endurance_score=get_endurance_score,
        get_blood_pressure=get_blood_pressure,
        get_hydration=get_hydration,
        get_activity_summary=get_activity_summary,
        fetch_activity_GPS=fetch_activity_GPS,
        get_solar_intensity=get_solar_intensity,
        get_lifestyle_data=get_lifestyle_data,
        compute_and_write_physiology=compute_and_write_physiology,
        compute_and_write_training_load=compute_and_write_training_load,
        compute_and_write_performance_daily=compute_and_write_performance_daily,
        rate_limit_calls_seconds=RATE_LIMIT_CALLS_SECONDS,
    )
    return _daily_fetch_write(date_str, run_rollups_inline=run_rollups_inline, ctx=ctx)

def compute_rollups_range(start_date: str, end_date: str) -> None:
    return _compute_rollups_range(
        start_date=start_date,
        end_date=end_date,
        compute_and_write_physiology=compute_and_write_physiology,
        compute_and_write_training_load=compute_and_write_training_load,
        compute_and_write_performance_daily=compute_and_write_performance_daily,
    )
                
# %%
def fetch_write_bulk(start_date_str, end_date_str, *, local_timediff: timedelta):
    global garmin_obj

    def _reauth():
        global garmin_obj
        garmin_obj = garmin_login()
        return garmin_obj

    ctx = _OrchestratorContext(
        garmin_obj=garmin_obj,
        write_points_to_influxdb=write_points_to_influxdb,
        get_last_sync=get_last_sync,
        daily_fetch_write=daily_fetch_write,
        iter_days=iter_days,
        should_rollups_after_ingest=_should_rollups_after_ingest,
        run_rollups_for_range=run_rollups_for_range,
        rate_limit_calls_seconds=RATE_LIMIT_CALLS_SECONDS,
        fetch_failed_wait_seconds=FETCH_FAILED_WAIT_SECONDS,
        max_consecutive_500_errors=MAX_CONSECUTIVE_500_ERRORS,
        ignore_errors=IGNORE_ERRORS,
        reauth=_reauth,
    )

    return _fetch_write_bulk(
        start_date_str=start_date_str,
        end_date_str=end_date_str,
        local_timediff=local_timediff,
        ctx=ctx,
    )

# %%
def main() -> int:
    garmin_obj = garmin_login()

    # determine local_timediff once for both MANUAL and LIVE paths
    try:
        if USER_TIMEZONE:
            local_timediff = datetime.now(tz=pytz.timezone(USER_TIMEZONE)).utcoffset()
        else:
            last_activity_dict = garmin_obj.get_last_activity()
            local_timediff = datetime.strptime(last_activity_dict["startTimeLocal"], "%Y-%m-%d %H:%M:%S") - datetime.strptime(
                last_activity_dict["startTimeGMT"], "%Y-%m-%d %H:%M:%S"
            )
    
        if local_timediff >= timedelta(0):
            logging.info("Using user's local timezone as UTC+" + str(local_timediff))
        else:
            logging.info("Using user's local timezone as UTC-" + str(-local_timediff))
    
    except (KeyError, TypeError):
        logging.warning("Unable to determine user's timezone - Defaulting to UTC. Consider providing TZ identifier with USER_TIMEZONE environment variable")
        local_timediff = timedelta(hours=0)
    
    if MANUAL_START_DATE:
        # local_timediff exists later in your code; for MANUAL path you should compute it before calling
        fetch_write_bulk(MANUAL_START_DATE, MANUAL_END_DATE, local_timediff=local_timediff)
        logging.info(f"Bulk update success : Fetched all available health metrics for date range {MANUAL_START_DATE} to {MANUAL_END_DATE}")
        return 0
    else:
        try:
            if INFLUXDB_VERSION == "1":
                last_influxdb_sync_time_UTC = pytz.utc.localize(
                    datetime.strptime(
                        list(influxdbclient.query("SELECT * FROM HeartRateIntraday ORDER BY time DESC LIMIT 1").get_points())[0]["time"],
                        "%Y-%m-%dT%H:%M:%SZ",
                    )
                )
            else:
                last_influxdb_sync_time_UTC = pytz.utc.localize(
                    influxdbclient.query(query="SELECT * FROM HeartRateIntraday ORDER BY time DESC LIMIT 1", language="influxql").to_pylist()[0][
                        "time"
                    ]
                )
        except Exception as err:
            logging.error(err)
            logging.warning(
                "No previously synced data found in local InfluxDB database, defaulting to 90 day initial fetching. Use specific start date ENV variable to bulk update past data"
            )
            last_influxdb_sync_time_UTC = (datetime.today() - timedelta(days=90)).astimezone(pytz.timezone("UTC"))

        while True:
            last_watch_sync_time_UTC = datetime.fromtimestamp(int((garmin_obj.get_device_last_used() or {}).get("lastUsedDeviceUploadTime") / 1000)).astimezone(
                pytz.timezone("UTC")
            )
            if last_influxdb_sync_time_UTC < last_watch_sync_time_UTC:
                logging.info(f"Update found : Current watch sync time is {last_watch_sync_time_UTC} UTC")
                fetch_write_bulk(
                    (last_influxdb_sync_time_UTC + local_timediff).strftime("%Y-%m-%d"),
                    (last_watch_sync_time_UTC + local_timediff).strftime("%Y-%m-%d"),
                    local_timediff=local_timediff,
                )
                last_influxdb_sync_time_UTC = last_watch_sync_time_UTC
            else:
                logging.info(f"No new data found : Current watch and influxdb sync time is {last_watch_sync_time_UTC} UTC")
            logging.info(f"waiting for {UPDATE_INTERVAL_SECONDS} seconds before next automatic update calls")
            time.sleep(UPDATE_INTERVAL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())