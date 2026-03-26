# %%
import traceback
import base64, requests, time, pytz, logging, os, sys, dotenv, io, zipfile
from fitparse import FitFile, FitParseError
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

garmin_obj = None
banner_text = """

*****  █▀▀ ▄▀█ █▀█ █▀▄▀█ █ █▄ █    █▀▀ █▀█ ▄▀█ █▀▀ ▄▀█ █▄ █ ▄▀█  *****
*****  █▄█ █▀█ █▀▄ █ ▀ █ █ █ ▀█    █▄█ █▀▄ █▀█ █▀  █▀█ █ ▀█ █▀█  *****

______________________________________________________________________

By Arpan Ghosh | Please consider supporting the project if you love it
______________________________________________________________________

"""
print(banner_text)

USER_GENDER_OVERRIDE = os.getenv("USER_GENDER", "").strip().lower()  # male|female|m|f|unknown|"" (auto)
env_override = dotenv.load_dotenv("override-default-vars.env", override=True)
if env_override:
    logging.warning("System ENV variables are overridden with override-default-vars.env")

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

def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"true", "t", "1", "yes", "y"}

USERPROFILE_WRITE_ONCE_PER_DAY = _bool_env("USERPROFILE_WRITE_ONCE_PER_DAY", default=True)

def _csv_env(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    return [s.strip() for s in str(raw).split(",") if s.strip()]

def _safe_b64decode_env(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    try:
        return base64.b64decode(raw).decode("utf-8")
    except Exception:
        logging.exception(f"Failed to base64 decode env var {name}")
        return None

USERPROFILE_WRITE_ONCE_PER_DAY = _bool_env("USERPROFILE_WRITE_ONCE_PER_DAY", default=True)

INFLUXDB_VERSION = os.getenv("INFLUXDB_VERSION", "1")  # accepted values are '1' or '3'
assert INFLUXDB_VERSION in ["1", "3"], "Only InfluxDB version 1 or 3 is allowed - please ensure to set this value to either 1 or 3"
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST", "your.influxdb.hostname")  # Required
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT", 8086))  # Required
INFLUXDB_USERNAME = os.getenv("INFLUXDB_USERNAME", "influxdb_username")  # Required
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD", "influxdb_access_password")  # Required
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE", "GarminStats")  # Required
INFLUXDB_V3_ACCESS_TOKEN = os.getenv("INFLUXDB_V3_ACCESS_TOKEN", "")  # required only for InfluxDB V3

TOKEN_DIR = os.path.expanduser(os.getenv("TOKEN_DIR", "~/.garminconnect"))  # optional
GARMINCONNECT_EMAIL = os.environ.get("GARMINCONNECT_EMAIL", None)  # optional

GARMINCONNECT_PASSWORD = _safe_b64decode_env("GARMINCONNECT_BASE64_PASSWORD")

GARMINCONNECT_IS_CN = _bool_env("GARMINCONNECT_IS_CN", default=False)  # optional if Chinese account
GARMIN_DEVICENAME = os.getenv("GARMIN_DEVICENAME", "Unknown")  # optional, attempts to set automatically if not given
GARMIN_DEVICEID = os.getenv("GARMIN_DEVICEID", None)  # optional, attempts to set automatically if not given

AUTO_DATE_RANGE = _bool_env("AUTO_DATE_RANGE", default=True)  # optional
MANUAL_START_DATE = os.getenv("MANUAL_START_DATE", None)  # optional, YYYY-MM-DD
MANUAL_END_DATE = os.getenv("MANUAL_END_DATE", datetime.today().strftime("%Y-%m-%d"))  # optional, YYYY-MM-DD

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")  # optional
FETCH_FAILED_WAIT_SECONDS = int(os.getenv("FETCH_FAILED_WAIT_SECONDS", 1800))  # optional
RATE_LIMIT_CALLS_SECONDS = int(os.getenv("RATE_LIMIT_CALLS_SECONDS", 5))  # optional
MAX_CONSECUTIVE_500_ERRORS = int(os.getenv("MAX_CONSECUTIVE_500_ERRORS", 10))  # optional
INFLUXDB_ENDPOINT_IS_HTTP = _bool_env("INFLUXDB_ENDPOINT_IS_HTTP", default=True)  # optional

GARMIN_DEVICENAME_AUTOMATIC = False if GARMIN_DEVICENAME != "Unknown" else True  # optional
UPDATE_INTERVAL_SECONDS = int(os.getenv("UPDATE_INTERVAL_SECONDS", 300))  # optional

# Selection parsing hardened: always a list of tokens
FETCH_SELECTION = _csv_env(
    "FETCH_SELECTION",
    "daily_avg,sleep,steps,heartrate,stress,breathing,hrv,fitness_age,vo2,activity,race_prediction,body_composition,lifestyle",
)

LACTATE_THRESHOLD_SPORTS = _csv_env("LACTATE_THRESHOLD_SPORTS", "RUNNING")
LACTATE_THRESHOLD_SPORTS = [s.upper() for s in LACTATE_THRESHOLD_SPORTS]

KEEP_FIT_FILES = _bool_env("KEEP_FIT_FILES", default=False)  # optional
FIT_FILE_STORAGE_LOCATION = os.getenv("FIT_FILE_STORAGE_LOCATION", os.path.join(os.path.expanduser("~"), "fit_filestore"))
ALWAYS_PROCESS_FIT_FILES = _bool_env("ALWAYS_PROCESS_FIT_FILES", default=False)  # optional
REQUEST_INTRADAY_DATA_REFRESH = _bool_env("REQUEST_INTRADAY_DATA_REFRESH", default=False)  # optional
IGNORE_INTRADAY_DATA_REFRESH_DAYS = int(os.getenv("IGNORE_INTRADAY_DATA_REFRESH_DAYS", 30))  # optional
TAG_MEASUREMENTS_WITH_USER_EMAIL = _bool_env("TAG_MEASUREMENTS_WITH_USER_EMAIL", default=False)  # optional

# Keep existing default behaviour: env unset => True
FORCE_REPROCESS_ACTIVITIES = _bool_env("FORCE_REPROCESS_ACTIVITIES", default=True)  # optional
ROLLUPS_AFTER_INGEST = _bool_env("ROLLUPS_AFTER_INGEST", default=True)

USER_TIMEZONE = os.getenv("USER_TIMEZONE", "")  # optional
PARSED_ACTIVITY_ID_LIST = []
# --- gender write guard ---
# ensures UserProfile is written only once per day
USER_PROFILE_WRITTEN_DATES: set[str] = set()
# FIT-derived gender overrides profile gender
FIT_GENDER_CACHE: dict[str, str] = {}
IGNORE_ERRORS = _bool_env("IGNORE_ERRORS", default=False)

# %%
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# %%
try:
    if INFLUXDB_ENDPOINT_IS_HTTP:
        if INFLUXDB_VERSION == "1":
            influxdbclient = InfluxDBClient(
                host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD
            )
            influxdbclient.switch_database(INFLUXDB_DATABASE)
        else:
            influxdbclient = InfluxDBClient3(
                host=f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}",
                token=INFLUXDB_V3_ACCESS_TOKEN,
                database=INFLUXDB_DATABASE,
            )
    else:
        if INFLUXDB_VERSION == "1":
            influxdbclient = InfluxDBClient(
                host=INFLUXDB_HOST,
                port=INFLUXDB_PORT,
                username=INFLUXDB_USERNAME,
                password=INFLUXDB_PASSWORD,
                ssl=True,
                verify_ssl=True,
            )
            influxdbclient.switch_database(INFLUXDB_DATABASE)
        else:
            influxdbclient = InfluxDBClient3(
                host=f"https://{INFLUXDB_HOST}:{INFLUXDB_PORT}",
                token=INFLUXDB_V3_ACCESS_TOKEN,
                database=INFLUXDB_DATABASE,
            )

    demo_point = {
        "measurement": "DemoPoint",
        "time": (datetime.now(pytz.utc) - timedelta(minutes=1)).isoformat(timespec="seconds"),
        "tags": {"DemoTag": "DemoTagValue"},
        "fields": {"DemoField": 0},
    }
    # test connection by writing/overwriting demo point
    if INFLUXDB_VERSION == "1":
        influxdbclient.write_points([demo_point])
    else:
        influxdbclient.write(record=[demo_point])

except (InfluxDBClientError, InfluxDBError) as err:
    logging.error("Unable to connect with influxdb database! Aborted")
    raise InfluxDBClientError("InfluxDB connection failed:" + str(err))

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
    try:
        logging.info(f"Trying to login to Garmin Connect using token data from directory '{TOKEN_DIR}'...")
        garmin = Garmin()
        garmin.login(TOKEN_DIR)
        logging.info("login to Garmin Connect successful using stored session tokens.")
    except (FileNotFoundError, GarthHTTPError, GarminConnectAuthenticationError):
        logging.warning(
            "Session is expired or login information not present/incorrect. You'll need to log in again...login with your Garmin Connect credentials to generate them."
        )
        try:
            user_email = GARMINCONNECT_EMAIL or input("Enter Garminconnect Login e-mail: ")
            user_password = GARMINCONNECT_PASSWORD or input("Enter Garminconnect password (characters will be visible): ")
            garmin = Garmin(email=user_email, password=user_password, is_cn=GARMINCONNECT_IS_CN, return_on_mfa=True)
            result1, result2 = garmin.login()
            if result1 == "needs_mfa":  # MFA is required
                mfa_code = input("MFA one-time code (via email or SMS): ")
                garmin.resume_login(result2, mfa_code)

            garmin.garth.dump(TOKEN_DIR)
            logging.info(f"Oauth tokens stored in '{TOKEN_DIR}' directory for future use")

            garmin.login(TOKEN_DIR)
            logging.info(
                "login to Garmin Connect successful using stored session tokens. Please restart the script. Saved logins will be used automatically"
            )
            sys.exit(0)  # terminating script

        except (FileNotFoundError, GarthHTTPError, GarminConnectAuthenticationError, requests.exceptions.HTTPError) as err:
            logging.error(str(err))
            raise Exception("Session is expired : please login again and restart the script")
    return garmin

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
    
def _clean_point(point: dict) -> dict | None:
    """
    Guardrails for Influx writes:
      - ensure tags/fields exist
      - drop None fields
      - drop point if fields become empty
    """
    if not isinstance(point, dict):
        return None
    fields = point.get("fields") or {}
    if not isinstance(fields, dict):
        fields = {}
    fields = {k: v for k, v in fields.items() if v is not None}
    if not fields:
        return None
    point["fields"] = fields

    tags = point.get("tags") or {}
    if not isinstance(tags, dict):
        tags = {}
    # ensure tag values are strings (Influx expects tag values as strings)
    tags = {str(k): ("" if v is None else str(v)) for k, v in tags.items()}
    point["tags"] = tags
    return point

def write_points_to_influxdb(points):
    write_chunk_size = 20000
    try:
        if not points:
            return

        # clean & de-null
        cleaned = []
        for p in points:
            cp = _clean_point(p)
            if cp is not None:
                cleaned.append(cp)

        if not cleaned:
            return

        if TAG_MEASUREMENTS_WITH_USER_EMAIL:
            user_id = "Unknown"
            try:
                if garmin_obj is not None and isinstance(garmin_obj.garth.profile, dict):
                    user_id = garmin_obj.garth.profile.get("userName", "Unknown")
            except Exception:
                pass
            for item in cleaned:
                item.setdefault("tags", {})
                item["tags"].update({"User_ID": user_id})

        for i in range(0, len(cleaned), write_chunk_size):
            batch = cleaned[i : i + write_chunk_size]
            if INFLUXDB_VERSION == "1":
                influxdbclient.write_points(batch)
            else:
                influxdbclient.write(record=batch)

        logging.info("Success : updated influxDB database with new points")
    except (InfluxDBClientError, InfluxDBError) as err:
        logging.error("Write failed : Unable to connect with database! " + str(err))

# %%
from datetime import date as _date

def _dt_utc(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=pytz.UTC)

def _range_utc(start_date: str, days: int) -> tuple[str, str]:
    start = _dt_utc(start_date)
    end = start + timedelta(days=days)
    return start.isoformat(), end.isoformat()

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
    try:
        res = influxdbclient.query(q)
        pts = list(res.get_points())
        if not pts:
            return None
        row = pts[0]
        for k in ("percentile", "max", "median", "mean", "value"):
            if k in row and row[k] is not None:
                return float(row[k])
        for v in row.values():
            if isinstance(v, (int, float)) and v is not None:
                return float(v)
        return None
    except Exception:
        logging.exception(f"Influx scalar query failed: {q}")
        return None

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
    try:
        res = influxdbclient.query(q)
        pts = list(res.get_points())
        return pts[0] if pts else None
    except Exception:
        logging.exception(f"Influx last-row query failed: {q}")
        return None

def _get_gender_for_day_v1(date_str: str) -> str:
    start_z, end_z = _day_bounds_z(date_str)
    q = (
        'SELECT last("gender_code") AS gc '
        'FROM "UserProfile" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "AND gender_is_known = 1 "
        f"AND \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}'"
    )
    row = _query_last_row_influx_v1(q) or {}
    gc = row.get("gc")

    try:
        gc_i = int(float(gc)) if gc is not None else 0
    except Exception:
        gc_i = 0

    if gc_i == 1:
        return "male"
    if gc_i == 2:
        return "female"
    return "unknown"

def _get_userprofile_master_v1() -> dict:
    q = (
        'SELECT last("age_years") AS age_years, '
        '       last("birth_year") AS birth_year, '
        '       last("gender_code") AS gender_code, '
        '       last("lthr_bpm") AS lthr_bpm '
        'FROM "UserProfileMaster" '
        f"WHERE \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}'"
    )
    return _query_last_row_influx_v1(q) or {}

def _stored_age_years_v1() -> int:
    row = _get_userprofile_master_v1()
    v = row.get("age_years")
    try:
        return int(float(v)) if v is not None else 0
    except Exception:
        return 0

def _stored_birth_year_v1() -> int | None:
    row = _get_userprofile_master_v1()
    v = row.get("birth_year")
    try:
        by = int(float(v)) if v is not None else 0
        return by if by > 0 else None
    except Exception:
        return None

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
    row = _get_userprofile_master_v1()
    v = row.get("lthr_bpm")
    try:
        x = int(float(v)) if v is not None else 0
        return x if x > 0 else None
    except Exception:
        return None

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
    # InfluxQL-friendly RFC3339 with Z (avoid +00:00)
    return dt.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

def _day_bounds_z(date_str: str) -> tuple[str, str]:
    day_start = _dt_utc(date_str)
    day_end = day_start + timedelta(days=1)
    return _iso_z(day_start), _iso_z(day_end)

def _get_physiology_for_day_v1(date_str: str) -> tuple[float | None, float | None]:
    # returns (rhr, hrmax_est)
    start_z, end_z = _day_bounds_z(date_str)
    q = (
        'SELECT last("RHR_7d_median") AS rhr, last("HRmax_est") AS hrmax '
        'FROM "PhysiologyDaily" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f"AND \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}'"
    )
    row = _query_last_row_influx_v1(q) or {}
    rhr = row.get("rhr")
    hrmax = row.get("hrmax")
    return (float(rhr) if rhr is not None else None, float(hrmax) if hrmax is not None else None)


def _get_activities_for_day_v1(date_str: str) -> list[dict]:
    """
    One row per activity_id (deduped) using last() per Activity_ID.
    Avoids any chance of counting multiple points for the same activity.
    """
    start_z, end_z = _day_bounds_z(date_str)

    q = (
        'SELECT last("elapsedDuration") AS elapsedDuration, '
        '       last("movingDuration")  AS movingDuration, '
        '       last("averageHR")       AS averageHR, '
        '       last("activityName")    AS activityName, '
        '       last("Activity_ID")     AS Activity_ID '
        'FROM "ActivitySummary" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "AND activityName != 'END' "
        "AND elapsedDuration > 0 "
        "AND averageHR > 0 "
        f"AND \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}' "
        'GROUP BY "Activity_ID"'
    )

    try:
        res = influxdbclient.query(q)
        return list(res.get_points())
    except Exception:
        logging.exception("ActivitySummary day query failed")
        return []

def _get_trainingload_prev_day_v1(date_str: str) -> dict:
    yday = (_dt_utc(date_str) - timedelta(days=1)).strftime("%Y-%m-%d")
    start_z, end_z = _day_bounds_z(yday)
    q = (
        'SELECT last("ATL_7_TRIMP") AS atl_trimp, last("CTL_42_TRIMP") AS ctl_trimp, '
        '       last("ATL_7_TSS")   AS atl_tss,   last("CTL_42_TSS")   AS ctl_tss '
        'FROM "TrainingLoadDaily" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f"AND \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}'"
    )
    row = _query_last_row_influx_v1(q) or {}
    def f(x):
        try:
            return float(x) if x is not None else None
        except Exception:
            return None
    return {
        "atl_trimp": f(row.get("atl_trimp")),
        "ctl_trimp": f(row.get("ctl_trimp")),
        "atl_tss": f(row.get("atl_tss")),
        "ctl_tss": f(row.get("ctl_tss")),
    }

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
    if INFLUXDB_VERSION != "1":
        logging.warning("Training load computation currently implemented for InfluxDB v1 only.")
        return

    # gender resolution
    gender = _norm_gender(_get_gender_for_day_v1(date_str))
    if gender not in {"male", "female"}:
        row = _get_userprofile_master_v1()
        gc = row.get("gender_code")
        try:
            gc_i = int(float(gc)) if gc is not None else 0
        except Exception:
            gc_i = 0
        gender = "male" if gc_i == 1 else "female" if gc_i == 2 else "unknown"

    if gender not in {"male", "female"}:
        logging.info(f"TrainingLoadDaily: gender unknown for {date_str}; skipping write (Bannister requires male/female).")
        return

    # physiology inputs
    rhr, hrmax = _get_physiology_for_day_v1(date_str)
    if rhr is None or hrmax is None:
        logging.info(f"TrainingLoadDaily: missing PhysiologyDaily inputs for {date_str} (rhr={rhr}, hrmax={hrmax})")
        return

    try:
        rhr_f = float(rhr)
        hrmax_f = float(hrmax)
    except Exception:
        logging.info(f"TrainingLoadDaily: invalid PhysiologyDaily inputs for {date_str} (rhr={rhr}, hrmax={hrmax})")
        return

    if rhr_f <= 0 or hrmax_f <= 0 or hrmax_f <= rhr_f:
        logging.info(f"TrainingLoadDaily: nonsensical physiology for {date_str} (rhr={rhr_f}, hrmax={hrmax_f})")
        return

    # threshold HR (optional for TSS track)
    lthr_bpm = _stored_lthr_bpm_v1()
    if lthr_bpm is None:
        logging.info(f"TrainingLoadDaily: missing LTHR (UserProfileMaster.lthr_bpm); TSS track will be null for {date_str}")

    acts = _get_activities_for_day_v1(date_str)

    trimp_total = 0.0
    tss_total = 0.0
    act_count = 0

    for a in acts:
        if str(a.get("activityName", "")).upper() == "END":
            continue

        dur = a.get("elapsedDuration")
        hr  = a.get("averageHR")
        if dur is None or hr is None:
            continue

        try:
            dur_f = float(dur)
            hr_f  = float(hr)
        except Exception:
            continue

        # TRIMP
        t = _bannister_trimp(dur_f, hr_f, rhr_f, hrmax_f, gender)
        if t > 0:
            trimp_total += t
            act_count += 1

        # TSS (only if LTHR available)
        if lthr_bpm is not None:
            tss_total += _hr_tss(dur_f, hr_f, float(lthr_bpm))

    # IMPORTANT: no early-return on rest days
    # If act_count==0, trimp_total/tss_total stay 0.0, which drives decay correctly.

    if act_count == 0:
        logging.info(f"TrainingLoadDaily: no usable activities for {date_str}; writing zero-load day")
        trimp_total = 0.0
        tss_total = 0.0
        # DO NOT return

    prev = _get_trainingload_prev_day_v1(date_str)

    a7  = 1.0 - math.exp(-1.0 / 7.0)
    a42 = 1.0 - math.exp(-1.0 / 42.0)

    atl_trimp = trimp_total if prev["atl_trimp"] is None else (prev["atl_trimp"] + a7  * (trimp_total - prev["atl_trimp"]))
    ctl_trimp = trimp_total if prev["ctl_trimp"] is None else (prev["ctl_trimp"] + a42 * (trimp_total - prev["ctl_trimp"]))
    tsb_trimp = ctl_trimp - atl_trimp

    if lthr_bpm is not None:
        atl_tss = tss_total if prev["atl_tss"] is None else (prev["atl_tss"] + a7  * (tss_total - prev["atl_tss"]))
        ctl_tss = tss_total if prev["ctl_tss"] is None else (prev["ctl_tss"] + a42 * (tss_total - prev["ctl_tss"]))
        tsb_tss = ctl_tss - atl_tss
    else:
        atl_tss = ctl_tss = tsb_tss = None

    point = {
        "measurement": "TrainingLoadDaily",
        "time": _dt_utc(date_str).isoformat(),
        "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
        "fields": {
            "TRIMP": float(trimp_total),
            "TSS": float(tss_total) if lthr_bpm is not None else None,

            "ATL_7_TRIMP": float(atl_trimp),
            "CTL_42_TRIMP": float(ctl_trimp),
            "TSB_TRIMP": float(tsb_trimp),

            "ATL_7_TSS": float(atl_tss) if atl_tss is not None else None,
            "CTL_42_TSS": float(ctl_tss) if ctl_tss is not None else None,
            "TSB_TSS": float(tsb_tss) if tsb_tss is not None else None,

            "lthr_used": float(lthr_bpm) if lthr_bpm is not None else None,
            "RHR_used": float(rhr_f),
            "HRmax_used": float(hrmax_f),
            "gender_code_used": int(_gender_code(gender)),
            "gender_is_known_used": 1,
            "activities_used": int(act_count),
        },
    }

    write_points_to_influxdb([point])
    logging.info(
        f"TrainingLoadDaily written for {date_str}: "
        f"TRIMP={trimp_total:.2f}, ATL_TRIMP={atl_trimp:.2f}, CTL_TRIMP={ctl_trimp:.2f}, TSB_TRIMP={tsb_trimp:.2f}; "
        f"TSS={tss_total:.1f}, ATL_TSS={(atl_tss if atl_tss is not None else float('nan')):.1f}, "
        f"CTL_TSS={(ctl_tss if ctl_tss is not None else float('nan')):.1f}, "
        f"TSB_TSS={(tsb_tss if tsb_tss is not None else float('nan')):.1f}; "
        f"acts={act_count}"
    )

def run_rollups_for_range(start_date: str, end_date: str) -> None:
    """
    Run rollups in chronological order so:
      - PhysiologyDaily gets correct trailing 7d RHR median
      - TrainingLoadDaily ATL/CTL/TSB get correct EWMAs (yesterday -> today)
    """
    if INFLUXDB_VERSION != "1":
        logging.warning("Rollups currently implemented for InfluxDB v1 only.")
        return

    logging.info(f"Rollups: running physiology + training load for {start_date} -> {end_date} (chronological)")

    # chronological loop
    d0 = datetime.strptime(start_date, "%Y-%m-%d")
    d1 = datetime.strptime(end_date, "%Y-%m-%d")
    cur = d0
    while cur <= d1:
        ds = cur.strftime("%Y-%m-%d")

        try:
            compute_and_write_physiology(ds)
        except Exception:
            logging.exception(f"PhysiologyDaily computation failed for {ds}")

        try:
            compute_and_write_training_load(ds)
        except Exception:
            logging.exception(f"TrainingLoadDaily computation failed for {ds}")

        try:
            compute_and_write_performance_daily(ds)
        except Exception:
            logging.exception(f"PerformanceDaily computation failed for {ds}")

        cur += timedelta(days=1)

def _userprofile_exists_for_day_v1(date_str: str) -> bool:
    """
    True if any UserProfile point exists for that UTC day (for this DB+Device).
    InfluxDB v1 InfluxQL.
    """
    if INFLUXDB_VERSION != "1":
        return False

    try:
        start_z, end_z = _day_bounds_z(date_str)
        q = (
            'SELECT count("gender_is_known") AS c '
            'FROM "UserProfile" '
            f"WHERE time >= '{start_z}' AND time < '{end_z}' "
            f"AND \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}'"
        )
        res = influxdbclient.query(q)
        pts = list(res.get_points())
        if not pts:
            return False
        c = pts[0].get("c")
        return (c is not None) and (float(c) > 0)
    except Exception:
        logging.exception("UserProfile existence query failed")
        return False

def _userprofile_known_for_day_v1(date_str: str) -> bool:
    """
    True if a UserProfile point exists for that UTC day with gender_is_known = 1.
    """
    try:
        start_z, end_z = _day_bounds_z(date_str)
        q = (
            'SELECT count("gender_is_known") AS c '
            'FROM "UserProfile" '
            f"WHERE time >= '{start_z}' AND time < '{end_z}' "
            f'AND "gender_is_known" = 1 '
            f"AND \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}'"
        )
        res = influxdbclient.query(q)
        pts = list(res.get_points())
        if not pts:
            return False
        c = pts[0].get("c")
        return (c is not None) and (float(c) > 0)
    except Exception:
        logging.exception("UserProfile known-gender existence query failed")
        return False
    
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
    start_z, end_z = _day_bounds_z(date_str)
    q = (
        'SELECT last("birth_year") '
        'FROM "UserProfile" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f"AND \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}'"
    )
    row = _query_last_row_influx_v1(q) or {}
    v = row.get("last")  # Influx returns column name "last" when no alias is used
    if v is None:
        return None
    try:
        return int(float(v))
    except Exception:
        return None

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
    if INFLUXDB_VERSION != "1":
        logging.warning("PhysiologyDaily computation currently implemented for InfluxDB v1 only.")
        return

    rhr7 = _median_rhr_7d(asof_date)
    if rhr7 is None:
        logging.info(f"PhysiologyDaily: insufficient RHR data for {asof_date}")
        return

    hrmax_est, hrmax_src = _estimate_hrmax_activity_backoff(asof_date, windows=[42, 84], min_points=5)
    if hrmax_est is None:
        logging.info(f"PhysiologyDaily: no HRmax estimate available for {asof_date} (source={hrmax_src})")
        return

    zones = _hrr_zones(rhr7, hrmax_est)

    fields = {
        "HRmax_est": float(hrmax_est),
        "HRmax_est_source": str(hrmax_src),
        "RHR_7d_median": float(rhr7),
        **{k: float(v) for k, v in zones.items()},
    }

    point = {
        "measurement": "PhysiologyDaily",
        "time": _dt_utc(asof_date).isoformat(),
        "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
        "fields": fields,
    }

    write_points_to_influxdb([point])
    logging.info(f"PhysiologyDaily written for {asof_date}: HRmax_est={hrmax_est:.1f} ({hrmax_src}), RHR_7d_median={rhr7:.1f}")

# %%
def get_daily_stats(date_str):
    points_list = []
    stats_json = garmin_obj.get_stats(date_str) or {}
    wellness_start = stats_json.get("wellnessStartTimeGmt")

    # keep original behavior (skip today)
    if wellness_start and datetime.strptime(date_str, "%Y-%m-%d") < datetime.today():
        points_list.append(
            {
                "measurement": "DailyStats",
                "time": pytz.timezone("UTC")
                .localize(datetime.strptime(wellness_start, "%Y-%m-%dT%H:%M:%S.%f"))
                .isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {
                    "activeKilocalories": stats_json.get("activeKilocalories"),
                    "bmrKilocalories": stats_json.get("bmrKilocalories"),
                    "totalSteps": stats_json.get("totalSteps"),
                    "totalDistanceMeters": stats_json.get("totalDistanceMeters"),
                    "highlyActiveSeconds": stats_json.get("highlyActiveSeconds"),
                    "activeSeconds": stats_json.get("activeSeconds"),
                    "sedentarySeconds": stats_json.get("sedentarySeconds"),
                    "sleepingSeconds": stats_json.get("sleepingSeconds"),
                    "moderateIntensityMinutes": stats_json.get("moderateIntensityMinutes"),
                    "vigorousIntensityMinutes": stats_json.get("vigorousIntensityMinutes"),
                    "floorsAscendedInMeters": stats_json.get("floorsAscendedInMeters"),
                    "floorsDescendedInMeters": stats_json.get("floorsDescendedInMeters"),
                    "floorsAscended": stats_json.get("floorsAscended"),
                    "floorsDescended": stats_json.get("floorsDescended"),
                    "minHeartRate": stats_json.get("minHeartRate"),
                    "maxHeartRate": stats_json.get("maxHeartRate"),
                    "restingHeartRate": stats_json.get("restingHeartRate"),
                    "minAvgHeartRate": stats_json.get("minAvgHeartRate"),
                    "maxAvgHeartRate": stats_json.get("maxAvgHeartRate"),
                    "avgSkinTempDeviationC": stats_json.get("avgSkinTempDeviationC"),
                    "avgSkinTempDeviationF": stats_json.get("avgSkinTempDeviationF"),
                    "stressDuration": stats_json.get("stressDuration"),
                    "restStressDuration": stats_json.get("restStressDuration"),
                    "activityStressDuration": stats_json.get("activityStressDuration"),
                    "uncategorizedStressDuration": stats_json.get("uncategorizedStressDuration"),
                    "totalStressDuration": stats_json.get("totalStressDuration"),
                    "lowStressDuration": stats_json.get("lowStressDuration"),
                    "mediumStressDuration": stats_json.get("mediumStressDuration"),
                    "highStressDuration": stats_json.get("highStressDuration"),
                    "stressPercentage": stats_json.get("stressPercentage"),
                    "restStressPercentage": stats_json.get("restStressPercentage"),
                    "activityStressPercentage": stats_json.get("activityStressPercentage"),
                    "uncategorizedStressPercentage": stats_json.get("uncategorizedStressPercentage"),
                    "lowStressPercentage": stats_json.get("lowStressPercentage"),
                    "mediumStressPercentage": stats_json.get("mediumStressPercentage"),
                    "highStressPercentage": stats_json.get("highStressPercentage"),
                    "bodyBatteryChargedValue": stats_json.get("bodyBatteryChargedValue"),
                    "bodyBatteryDrainedValue": stats_json.get("bodyBatteryDrainedValue"),
                    "bodyBatteryHighestValue": stats_json.get("bodyBatteryHighestValue"),
                    "bodyBatteryLowestValue": stats_json.get("bodyBatteryLowestValue"),
                    "bodyBatteryDuringSleep": stats_json.get("bodyBatteryDuringSleep"),
                    "bodyBatteryAtWakeTime": stats_json.get("bodyBatteryAtWakeTime"),
                    "averageSpo2": stats_json.get("averageSpo2"),
                    "lowestSpo2": stats_json.get("lowestSpo2"),
                },
            }
        )
        if points_list:
            logging.info(f"Success : Fetching daily metrics for date {date_str}")
        return points_list
    else:
        logging.debug("No daily stat data available for the give date " + date_str)
        return []

# %%
def get_last_sync():
    global GARMIN_DEVICENAME
    global GARMIN_DEVICEID
    points_list = []
    sync_data = garmin_obj.get_device_last_used() or {}
    if GARMIN_DEVICENAME_AUTOMATIC:
        GARMIN_DEVICENAME = sync_data.get("lastUsedDeviceName") or "Unknown"
        GARMIN_DEVICEID = sync_data.get("userDeviceId") or None

    ts = sync_data.get("lastUsedDeviceUploadTime")
    if ts:
        points_list.append(
            {
                "measurement": "DeviceSync",
                "time": datetime.fromtimestamp(ts / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"imageUrl": sync_data.get("imageUrl"), "Device_Name": GARMIN_DEVICENAME},
            }
        )

    if points_list:
        logging.info("Success : Updated device last sync time")
    else:
        logging.warning("No associated/synced Garmin device found with your account")
    return points_list

# %%
def get_sleep_data(date_str):
    points_list = []
    all_sleep_data = garmin_obj.get_sleep_data(date_str) or {}
    sleep_json = all_sleep_data.get("dailySleepDTO") or {}

    if sleep_json.get("sleepEndTimestampGMT"):
        sleep_scores = (sleep_json.get("sleepScores") or {}).get("overall") or {}
        points_list.append(
            {
                "measurement": "SleepSummary",
                "time": datetime.fromtimestamp(sleep_json["sleepEndTimestampGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {
                    "sleepTimeSeconds": sleep_json.get("sleepTimeSeconds"),
                    "deepSleepSeconds": sleep_json.get("deepSleepSeconds"),
                    "lightSleepSeconds": sleep_json.get("lightSleepSeconds"),
                    "remSleepSeconds": sleep_json.get("remSleepSeconds"),
                    "awakeSleepSeconds": sleep_json.get("awakeSleepSeconds"),
                    "averageSpO2Value": sleep_json.get("averageSpO2Value"),
                    "lowestSpO2Value": sleep_json.get("lowestSpO2Value"),
                    "highestSpO2Value": sleep_json.get("highestSpO2Value"),
                    "averageRespirationValue": sleep_json.get("averageRespirationValue"),
                    "lowestRespirationValue": sleep_json.get("lowestRespirationValue"),
                    "highestRespirationValue": sleep_json.get("highestRespirationValue"),
                    "awakeCount": sleep_json.get("awakeCount"),
                    "avgSleepStress": sleep_json.get("avgSleepStress"),
                    "sleepScore": sleep_scores.get("value"),
                    "restlessMomentsCount": all_sleep_data.get("restlessMomentsCount"),
                    "avgOvernightHrv": all_sleep_data.get("avgOvernightHrv"),
                    "bodyBatteryChange": all_sleep_data.get("bodyBatteryChange"),
                    "restingHeartRate": all_sleep_data.get("restingHeartRate"),
                    "avgSkinTempDeviationC": all_sleep_data.get("avgSkinTempDeviationC"),
                    "avgSkinTempDeviationF": all_sleep_data.get("avgSkinTempDeviationF"),
                },
            }
        )

    sleep_movement_intraday = all_sleep_data.get("sleepMovement") or []
    for entry in sleep_movement_intraday:
        start_gmt = entry.get("startGMT")
        end_gmt = entry.get("endGMT")
        if not start_gmt or not end_gmt:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(start_gmt, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {
                    "SleepMovementActivityLevel": entry.get("activityLevel", -1),
                    "SleepMovementActivitySeconds": int(
                        (datetime.strptime(end_gmt, "%Y-%m-%dT%H:%M:%S.%f") - datetime.strptime(start_gmt, "%Y-%m-%dT%H:%M:%S.%f")).total_seconds()
                    ),
                },
            }
        )

    sleep_levels_intraday = all_sleep_data.get("sleepLevels") or []
    last_level_entry = None
    for entry in sleep_levels_intraday:
        last_level_entry = entry
        start_gmt = entry.get("startGMT")
        end_gmt = entry.get("endGMT")
        if not start_gmt or not end_gmt:
            continue
        if entry.get("activityLevel") is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(start_gmt, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {
                    "SleepStageLevel": entry.get("activityLevel"),
                    "SleepStageSeconds": int(
                        (datetime.strptime(end_gmt, "%Y-%m-%dT%H:%M:%S.%f") - datetime.strptime(start_gmt, "%Y-%m-%dT%H:%M:%S.%f")).total_seconds()
                    ),
                },
            }
        )

    # Add additional duplicate terminal data point (issue #127)
    if last_level_entry and last_level_entry.get("endGMT"):
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(last_level_entry["endGMT"], "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"SleepStageLevel": last_level_entry.get("activityLevel")},
            }
        )

    sleep_restlessness_intraday = all_sleep_data.get("sleepRestlessMoments") or []
    for entry in sleep_restlessness_intraday:
        v = entry.get("value")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": datetime.fromtimestamp(entry["startGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"sleepRestlessValue": v},
            }
        )

    sleep_spo2_intraday = all_sleep_data.get("wellnessEpochSPO2DataDTOList") or []
    for entry in sleep_spo2_intraday:
        v = entry.get("spo2Reading")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(entry["epochTimestamp"], "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"spo2Reading": v},
            }
        )

    sleep_respiration_intraday = all_sleep_data.get("wellnessEpochRespirationDataDTOList") or []
    for entry in sleep_respiration_intraday:
        v = entry.get("respirationValue")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": datetime.fromtimestamp(entry["startTimeGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"respirationValue": v},
            }
        )

    sleep_heart_rate_intraday = all_sleep_data.get("sleepHeartRate") or []
    for entry in sleep_heart_rate_intraday:
        v = entry.get("value")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": datetime.fromtimestamp(entry["startGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"heartRate": v},
            }
        )

    sleep_stress_intraday = all_sleep_data.get("sleepStress") or []
    for entry in sleep_stress_intraday:
        v = entry.get("value")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": datetime.fromtimestamp(entry["startGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"stressValue": v},
            }
        )

    sleep_bb_intraday = all_sleep_data.get("sleepBodyBattery") or []
    for entry in sleep_bb_intraday:
        v = entry.get("value")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": datetime.fromtimestamp(entry["startGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"bodyBattery": v},
            }
        )

    sleep_hrv_intraday = all_sleep_data.get("hrvData") or []
    for entry in sleep_hrv_intraday:
        v = entry.get("value")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": datetime.fromtimestamp(entry["startGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"hrvData": v},
            }
        )

    if points_list:
        logging.info(f"Success : Fetching intraday sleep metrics for date {date_str}")
    return points_list

# %%
def get_intraday_hr(date_str):
    points_list = []
    hr_list = (garmin_obj.get_heart_rates(date_str) or {}).get("heartRateValues") or []
    for entry in hr_list:
        if len(entry) < 2:
            continue
        ts_ms, hr = entry[0], entry[1]
        if hr is None:
            continue
        points_list.append(
            {
                "measurement": "HeartRateIntraday",
                "time": datetime.fromtimestamp(ts_ms / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"HeartRate": hr},
            }
        )
    if points_list:
        logging.info(f"Success : Fetching intraday Heart Rate for date {date_str}")
    return points_list

# %%
def get_intraday_steps(date_str):
    points_list = []
    steps_list = garmin_obj.get_steps_data(date_str) or []
    for entry in steps_list:
        steps = entry.get("steps")
        if steps is None:
            continue
        start_gmt = entry.get("startGMT")
        if not start_gmt:
            continue
        points_list.append(
            {
                "measurement": "StepsIntraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(start_gmt, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"StepsCount": steps},
            }
        )
    if points_list:
        logging.info(f"Success : Fetching intraday steps for date {date_str}")
    return points_list

# %%
def get_intraday_stress(date_str):
    points_list = []
    stress_payload = garmin_obj.get_stress_data(date_str) or {}

    stress_list = stress_payload.get("stressValuesArray") or []
    for entry in stress_list:
        if len(entry) < 2:
            continue
        ts_ms, stress_val = entry[0], entry[1]
        if stress_val is None:
            continue
        points_list.append(
            {
                "measurement": "StressIntraday",
                "time": datetime.fromtimestamp(ts_ms / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"stressLevel": stress_val},
            }
        )

    bb_list = stress_payload.get("bodyBatteryValuesArray") or []
    for entry in bb_list:
        if len(entry) < 3:
            continue
        ts_ms, _, bb_val = entry[0], entry[1], entry[2]
        if bb_val is None:
            continue
        points_list.append(
            {
                "measurement": "BodyBatteryIntraday",
                "time": datetime.fromtimestamp(ts_ms / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"BodyBatteryLevel": bb_val},
            }
        )

    if points_list:
        logging.info(f"Success : Fetching intraday stress and Body Battery values for date {date_str}")
    return points_list

# %%
def get_intraday_br(date_str):
    points_list = []
    br_list = (garmin_obj.get_respiration_data(date_str) or {}).get("respirationValuesArray") or []
    for entry in br_list:
        if len(entry) < 2:
            continue
        ts_ms, br = entry[0], entry[1]
        if br is None:
            continue
        points_list.append(
            {
                "measurement": "BreathingRateIntraday",
                "time": datetime.fromtimestamp(ts_ms / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"BreathingRate": br},
            }
        )
    if points_list:
        logging.info(f"Success : Fetching intraday Breathing Rate for date {date_str}")
    return points_list

# %%
def get_intraday_hrv(date_str):
    points_list = []
    hrv_list = (garmin_obj.get_hrv_data(date_str) or {}).get("hrvReadings") or []
    for entry in hrv_list:
        v = entry.get("hrvValue")
        ts = entry.get("readingTimeGMT")
        if v is None or not ts:
            continue
        points_list.append(
            {
                "measurement": "HRV_Intraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"hrvValue": v},
            }
        )
    if points_list:
        logging.info(f"Success : Fetching intraday HRV for date {date_str}")
    return points_list

# %%
def get_body_composition(date_str):
    points_list = []
    payload = garmin_obj.get_weigh_ins(date_str, date_str) or {}
    weight_list_all = payload.get("dailyWeightSummaries", []) or []
    if weight_list_all:
        weight_list = (weight_list_all[0] or {}).get("allWeightMetrics", []) or []
        for weight_dict in weight_list:
            data_fields = {
                "weight": weight_dict.get("weight"),
                "bmi": weight_dict.get("bmi"),
                "bodyFat": weight_dict.get("bodyFat"),
                "bodyWater": weight_dict.get("bodyWater"),
                "boneMass": weight_dict.get("boneMass"),
                "muscleMass": weight_dict.get("muscleMass"),
                "physiqueRating": weight_dict.get("physiqueRating"),
                "visceralFat": weight_dict.get("visceralFat"),
            }
            if all(value is None for value in data_fields.values()):
                continue

            ts_gmt = weight_dict.get("timestampGMT")
            if ts_gmt:
                t = datetime.fromtimestamp((ts_gmt / 1000), tz=pytz.timezone("UTC")).isoformat()
            else:
                t = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat()

            points_list.append(
                {
                    "measurement": "BodyComposition",
                    "time": t,
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE,
                        "Frequency": "Intraday",
                        "SourceType": weight_dict.get("sourceType", "Unknown"),
                    },
                    "fields": data_fields,
                }
            )
        logging.info(f"Success : Fetching intraday Body Composition (Weight, BMI etc) for date {date_str}")
    return points_list

# %%
def get_activity_summary(date_str):
    points_list = []
    activity_with_gps_id_dict = {}
    activity_list = garmin_obj.get_activities_by_date(date_str, date_str) or []
    for activity in activity_list:
        act_id = activity.get("activityId")
        act_type = (activity.get("activityType") or {}).get("typeKey", "Unknown")

        if activity.get("hasPolyline") or ALWAYS_PROCESS_FIT_FILES:
            if not activity.get("hasPolyline"):
                logging.warning(
                    f"Activity ID {act_id} got no GPS data - yet, activity FIT file data will be processed as ALWAYS_PROCESS_FIT_FILES is on"
                )
            activity_with_gps_id_dict[act_id] = act_type

        if "startTimeGMT" in activity:
            start_dt = datetime.strptime(activity["startTimeGMT"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
            selector = start_dt.strftime("%Y%m%dT%H%M%SUTC-") + act_type

            points_list.append(
                {
                    "measurement": "ActivitySummary",
                    "time": start_dt.isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE,
                        "ActivityID": act_id,
                        "ActivitySelector": selector,
                        "activity_type_tag": _norm_tag_value(act_type),
                    },
                    "fields": {
                        "Activity_ID": act_id,
                        "Device_ID": activity.get("deviceId"),
                        "activityName": activity.get("activityName"),
                        "description": activity.get("description"),
                        "activityType": act_type if act_type != "Unknown" else None,
                        "distance": activity.get("distance"),
                        "elapsedDuration": activity.get("elapsedDuration"),
                        "movingDuration": activity.get("movingDuration"),
                        "averageSpeed": activity.get("averageSpeed"),
                        "maxSpeed": activity.get("maxSpeed"),
                        "calories": activity.get("calories"),
                        "bmrCalories": activity.get("bmrCalories"),
                        "averageHR": activity.get("averageHR"),
                        "maxHR": activity.get("maxHR"),
                        "locationName": activity.get("locationName"),
                        "lapCount": activity.get("lapCount"),
                        "hrTimeInZone_1": int(val) if (val := activity.get("hrTimeInZone_1")) is not None else None,
                        "hrTimeInZone_2": int(val) if (val := activity.get("hrTimeInZone_2")) is not None else None,
                        "hrTimeInZone_3": int(val) if (val := activity.get("hrTimeInZone_3")) is not None else None,
                        "hrTimeInZone_4": int(val) if (val := activity.get("hrTimeInZone_4")) is not None else None,
                        "hrTimeInZone_5": int(val) if (val := activity.get("hrTimeInZone_5")) is not None else None,
                    },
                }
            )

            # terminal END marker
            end_dt = start_dt + timedelta(seconds=int(activity.get("elapsedDuration", 0) or 0))
            points_list.append(
                {
                    "measurement": "ActivitySummary",
                    "time": end_dt.isoformat(),
                    "tags": {
                        "Device": GARMIN_DEVICENAME,
                        "Database_Name": INFLUXDB_DATABASE,
                        "ActivityID": act_id,
                        "ActivitySelector": selector,
                        "activity_type_tag": _norm_tag_value(act_type),
                    },
                    "fields": {"Activity_ID": act_id, "Device_ID": activity.get("deviceId"), "activityName": "END", "activityType": "No Activity"},
                }
            )

            logging.info(f"Success : Fetching Activity summary with id {act_id} for date {date_str}")
        else:
            logging.warning(f"Skipped : Start Timestamp missing for activity id {act_id} for date {date_str}")
    return points_list, activity_with_gps_id_dict

# %%
def fetch_activity_GPS(activityIDdict):  # Uses FIT file by default, falls back to TCX
    points_list = []
    for activityID in activityIDdict.keys():
        activity_type = activityIDdict[activityID]

        if (activityID in PARSED_ACTIVITY_ID_LIST) and (not FORCE_REPROCESS_ACTIVITIES):
            logging.info(f"Skipping : Activity ID {activityID} has already been processed within current runtime")
            continue
        if (activityID in PARSED_ACTIVITY_ID_LIST) and (FORCE_REPROCESS_ACTIVITIES):
            logging.info(f"Re-processing : Activity ID {activityID} (FORCE_REPROCESS_ACTIVITIES is on)")

        try:
            zip_data = garmin_obj.download_activity(activityID, dl_fmt=garmin_obj.ActivityDownloadFormat.ORIGINAL)
            logging.info(f"Processing : Activity ID {activityID} FIT file data - this may take a while...")
            zip_buffer = io.BytesIO(zip_data)

            with zipfile.ZipFile(zip_buffer) as zip_ref:
                fit_filename = next((f for f in zip_ref.namelist() if f.endswith(".fit")), None)
                if not fit_filename:
                    raise FileNotFoundError(f"No FIT file found in the downloaded zip archive for Activity ID {activityID}")

                fit_data = zip_ref.read(fit_filename)
                fit_file_buffer = io.BytesIO(fit_data)

                fitfile = FitFile(fit_file_buffer)
                fitfile.parse()

                all_records_list = [record.get_values() for record in fitfile.get_messages("record")]
                all_sessions_list = [record.get_values() for record in fitfile.get_messages("session")]
                all_lengths_list = [record.get_values() for record in fitfile.get_messages("length")]
                all_laps_list = [record.get_values() for record in fitfile.get_messages("lap")]

                fit_age_years = None
                for msg in fitfile.get_messages("user_metrics"):
                    # pull fields robustly (same approach as user_profile)
                    d = {f.name: f.value for f in msg.fields}
                    a = d.get("age")
                    if a is None:
                        continue
                    try:
                        fit_age_years = int(float(a))
                        break
                    except Exception:
                        pass

                if len(all_records_list) == 0:
                    raise FileNotFoundError(f"No records found in FIT file for Activity ID {activityID} - Discarding FIT file")

                # Guardrail: determine activity_start_time before using it anywhere
                ts0 = all_records_list[0].get("timestamp")
                if not ts0:
                    raise FileNotFoundError(f"First record missing timestamp in FIT for Activity ID {activityID} - Discarding FIT file")
                activity_start_time = ts0.replace(tzinfo=pytz.UTC)

                fit_lthr = None
                fit_ltpower = None
                # do NOT reset fit_age_years here
                
                try:
                    for msg in fitfile.get_messages("unknown_79"):
                        by_def = {getattr(f, "def_num", None): getattr(f, "value", None) for f in msg.fields}
                
                        if fit_age_years is None and by_def.get(1) is not None:
                            fit_age_years = int(float(by_def[1]))
                        if fit_lthr is None and by_def.get(11) is not None:
                            fit_lthr = int(float(by_def[11]))
                        if fit_ltpower is None and by_def.get(12) is not None:
                            fit_ltpower = int(float(by_def[12]))
                
                        if fit_age_years is not None and fit_lthr is not None and fit_ltpower is not None:
                            break
                
                    logging.info(f"FIT unknown_79 derived: age={fit_age_years}, lthr={fit_lthr}, ltpower={fit_ltpower}")
                except Exception:
                    logging.exception("FIT unknown_79 extraction failed")

                # --- NEW: derived performance metrics (run/ride) ---
                try:
                    derive_and_write_activity_metrics_v1(
                        activity_id=int(activityID),
                        activity_type=str(activity_type),
                        activity_start_time=activity_start_time,
                        all_records_list=all_records_list,
                    )
                except Exception:
                    logging.exception(f"DerivedActivity computation failed for Activity ID {activityID}")
                
                # FIT user_profile extraction (gender + birth year + birthdate)
                all_user_list = []
                for msg in fitfile.get_messages("user_profile"):
                    d = {}
                    for f in msg.fields:
                        # f.name is the decoded field name; f.value is the decoded value
                        d[f.name] = f.value
                    all_user_list.append(d)
                
                fit_gender = "unknown"
                fit_birth_year = None
                fit_birthdate = None  # if present as YYYY-MM-DD or similar
                
                if all_user_list:
                    for up in all_user_list:
                        g = up.get("gender") or up.get("sex")
                        if g:
                            fit_gender = _norm_gender(g)
                        # Try common keys first
                        yob = (
                            up.get("year_of_birth")
                            or up.get("birth_year")
                            or up.get("yearofbirth")
                            or up.get("birthyear")
                            or up.get("yob")
                        )
                        
                        bd = (
                            up.get("birthdate")
                            or up.get("birth_date")
                            or up.get("date_of_birth")
                            or up.get("dob")
                        )
                        
                        # If still missing, search keys heuristically (FIT fields vary by device/SDK)
                        if yob is None:
                            for k, v in up.items():
                                ks = str(k).lower()
                                if "birth" in ks and "year" in ks:
                                    yob = v
                                    break
                        
                        if bd is None:
                            for k, v in up.items():
                                ks = str(k).lower()
                                if "birth" in ks and ("date" in ks or "dob" in ks):
                                    bd = v
                                    break
                        
                        if fit_birth_year is None:
                            fit_birth_year = _birth_year_from_any(yob)
                        
                        if fit_birth_year is None and fit_birthdate is None:
                            fit_birthdate = bd
                
                        # if we got something usable, stop
                        if fit_gender in {"male", "female"} and fit_birth_year is not None:
                            break
                
                # If we only got a birthdate, derive birth_year
                if fit_birth_year is None:
                    fit_birth_year = _birth_year_from_any(fit_birthdate)

                # --- NEW: use FIT user_profile to populate master + per-day gender cache ---
                try:
                    activity_day = activity_start_time.strftime("%Y-%m-%d")

                    # If FIT did not provide birth_year, approximate from FIT age + activity year
                    if fit_birth_year is None and fit_age_years is not None:
                        # Approx birth year from FIT age + activity date (birthday-aware)
                        # If birthday has NOT happened yet as of activity date, birth year is (year - age - 1)
                        # If birthday HAS happened, birth year is (year - age)
                        try:
                            y = activity_start_time.year
                            a = int(fit_age_years)
                        
                            # If FIT user_profile contains birthdate, use it (best)
                            # Otherwise, assume birthday not yet occurred to avoid off-by-one (more correct for most of the year)
                            if fit_birthdate:
                                yy, mm, dd = [int(x) for x in str(fit_birthdate).split("-")[:3]]
                                had_bday = (activity_start_time.month, activity_start_time.day) >= (mm, dd)
                                fit_birth_year = y - a if had_bday else (y - a - 1)
                            else:
                                fit_birth_year = y - a - 1
                        except Exception:
                            pass
                
                    # cache gender by day (used by daily_fetch_write when garmin profile gender is missing)
                    if fit_gender in {"male", "female"}:
                        FIT_GENDER_CACHE[activity_day] = fit_gender
                        
                        # NEW: also update the day's UserProfile measurement in Influx
                        try:
                            by = (
                                fit_birth_year
                                or _stored_birth_year_v1()
                                or _get_birth_year_from_garmin_profile()
                            )
                            write_points_to_influxdb(
                                write_user_profile_point(
                                    activity_day,
                                    gender=fit_gender,
                                    birth_year=by,
                                )
                            )
                            logging.info(f"UserProfile updated from FIT for {activity_day}: gender={fit_gender}, birth_year={by}")
                        except Exception:
                            logging.exception("Failed writing UserProfile from FIT gender")
                
                    # persist to UserProfileMaster (birth_year drives age; gender drives TRIMP/Bannister)
                    maybe_update_userprofile_master(
                        birth_year=fit_birth_year,
                        age_years=fit_age_years,
                        gender=fit_gender if fit_gender in {"male", "female"} else None,
                        lthr=fit_lthr,
                        source="fit_unknown_79",
                        activity_id=int(activityID),
                    )
                except Exception:
                    logging.exception("UserProfileMaster update from FIT user_profile failed")

                for parsed_record in all_records_list:
                    ts = parsed_record.get("timestamp")
                    if not ts:
                        continue

                    hr = parsed_record.get("heart_rate", None)
                    u140 = parsed_record.get("unknown_140")
                    gas = (u140 / 1000.0) if u140 else None

                    point = {
                        "measurement": "ActivityGPS",
                        "time": ts.replace(tzinfo=pytz.UTC).isoformat(),
                        "tags": {
                            "Device": GARMIN_DEVICENAME,
                            "Database_Name": INFLUXDB_DATABASE,
                            "ActivityID": activityID,
                            "ActivitySelector": activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type,
                            "sport_tag": _norm_tag_value(activity_type),
                        },
                        "fields": {
                            "ActivityName": activity_type,
                            "Activity_ID": activityID,
                            "Latitude": int(parsed_record["position_lat"]) * (180 / 2**31) if parsed_record.get("position_lat") else None,
                            "Longitude": int(parsed_record["position_long"]) * (180 / 2**31) if parsed_record.get("position_long") else None,
                            "Altitude": parsed_record.get("enhanced_altitude", None) or parsed_record.get("altitude", None),
                            "Distance": parsed_record.get("distance", None),
                            "DurationSeconds": (ts.replace(tzinfo=pytz.UTC) - activity_start_time).total_seconds(),
                            "HeartRate": float(hr) if hr is not None else None,
                            "Speed": parsed_record.get("enhanced_speed", None) or parsed_record.get("speed", None),
                            "GradeAdjustedSpeed": gas,
                            "RunningEfficiency": (gas / hr) if (gas is not None and hr) else None,
                            "Cadence": parsed_record.get("cadence", None),
                            "Fractional_Cadence": parsed_record.get("fractional_cadence", None),
                            "Temperature": parsed_record.get("temperature", None),
                            "Accumulated_Power": parsed_record.get("accumulated_power", None),
                            "Power": parsed_record.get("power", None),
                            "Vertical_Oscillation": parsed_record.get("vertical_oscillation", None),
                            "Stance_Time": parsed_record.get("stance_time", None),
                            "Vertical_Ratio": parsed_record.get("vertical_ratio", None),
                            "Step_Length": parsed_record.get("step_length", None),
                        },
                    }
                    points_list.append(point)

                for session_record in all_sessions_list:
                    t = session_record.get("start_time") or session_record.get("timestamp")
                    if not t:
                        continue
                    t_iso = t.replace(tzinfo=pytz.UTC).isoformat()

                    point = {
                        "measurement": "ActivitySession",
                        "time": t_iso,
                        "tags": {
                            "Device": GARMIN_DEVICENAME,
                            "Database_Name": INFLUXDB_DATABASE,
                            "ActivityID": activityID,
                            "ActivitySelector": activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type,
                            "sport_tag": _norm_tag_value(session_record.get("sport")),
                            "sub_sport_tag": _norm_tag_value(session_record.get("sub_sport")),
                        },
                        "fields": {
                            "Index": int(session_record.get("message_index", -1)) + 1,
                            "ActivityName": activity_type,
                            "Activity_ID": activityID,
                            "Sport": str(session_record.get("sport", None)),
                            "Sub_Sport": session_record.get("sub_sport", None),
                            "Pool_Length": session_record.get("pool_length", None),
                            "Pool_Length_Unit": session_record.get("pool_length_unit", None),
                            "Lengths": session_record.get("num_laps", None),
                            "Laps": session_record.get("num_lengths", None),
                            "Aerobic_Training": session_record.get("total_training_effect", None),
                            "Anaerobic_Training": session_record.get("total_anaerobic_training_effect", None),
                            "Primary_Benefit": session_record.get("primary_benefit", None),
                            "Recovery_Time": session_record.get("recovery_time", None),
                        },
                    }
                    points_list.append(point)

                for length_record in all_lengths_list:
                    t = length_record.get("start_time") or length_record.get("timestamp")
                    if not t:
                        continue
                    t_iso = t.replace(tzinfo=pytz.UTC).isoformat()

                    point = {
                        "measurement": "ActivityLength",
                        "time": t_iso,
                        "tags": {
                            "Device": GARMIN_DEVICENAME,
                            "Database_Name": INFLUXDB_DATABASE,
                            "ActivityID": activityID,
                            "ActivitySelector": activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type,
                        },
                        "fields": {
                            "Index": int(length_record.get("message_index", -1)) + 1,
                            "ActivityName": activity_type,
                            "Activity_ID": activityID,
                            "Elapsed_Time": length_record.get("total_elapsed_time", None),
                            "Strokes": length_record.get("total_strokes", None),
                            "Swim_Stroke": length_record.get("swim_stroke", None),
                            "Avg_Speed": length_record.get("avg_speed", None),
                            "Calories": length_record.get("total_calories", None),
                            "Avg_Cadence": length_record.get("avg_swimming_cadence", None),
                        },
                    }
                    points_list.append(point)

                for lap_record in all_laps_list:
                    t = lap_record.get("start_time") or lap_record.get("timestamp")
                    if not t:
                        continue
                    t_iso = t.replace(tzinfo=pytz.UTC).isoformat()

                    point = {
                        "measurement": "ActivityLap",
                        "time": t_iso,
                        "tags": {
                            "Device": GARMIN_DEVICENAME,
                            "Database_Name": INFLUXDB_DATABASE,
                            "ActivityID": activityID,
                            "ActivitySelector": activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type,
                            "sport_tag": _norm_tag_value(lap_record.get("sport")),
                        },
                        "fields": {
                            "Index": int(lap_record.get("message_index", -1)) + 1,
                            "ActivityName": activity_type,
                            "Activity_ID": activityID,
                            "Elapsed_Time": lap_record.get("total_elapsed_time", None),
                            "Sport": lap_record.get("sport", None),
                            "Lengths": lap_record.get("num_lengths", None),
                            "Length_Index": lap_record.get("first_length_index", None),
                            "Distance": lap_record.get("total_distance", None),
                            "Cycles": lap_record.get("total_cycles", None),
                            "Avg_Stroke_Distance": lap_record.get("avg_stroke_distance", None),
                            "Moving_Duration": lap_record.get("total_moving_time", None),
                            "Standing_Duration": lap_record.get("time_standing", None),
                            "Avg_Speed": lap_record.get("enhanced_avg_speed", None),
                            "Max_Speed": lap_record.get("enhanced_max_speed", None),
                            "Calories": lap_record.get("total_calories", None),
                            "Avg_Power": lap_record.get("avg_power", None),
                            "Avg_HR": lap_record.get("avg_heart_rate", None),
                            "Max_HR": lap_record.get("max_heart_rate", None),
                            "Avg_Cadence": lap_record.get("avg_cadence", None),
                            "Avg_Temperature": lap_record.get("avg_temperature", None),
                            "Avg_Vertical_Oscillation": lap_record.get("avg_vertical_oscillation", None),
                            "Avg_Stance_Time": lap_record.get("avg_stance_time", None),
                            "Avg_Vertical_Ratio": lap_record.get("avg_vertical_ratio", None),
                            "Avg_Step_Length": lap_record.get("avg_step_length", None),
                        },
                    }
                    points_list.append(point)

                if KEEP_FIT_FILES:
                    os.makedirs(FIT_FILE_STORAGE_LOCATION, exist_ok=True)
                    fit_path = os.path.join(
                        FIT_FILE_STORAGE_LOCATION, activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type + ".fit"
                    )
                    with open(fit_path, "wb") as f:
                        f.write(fit_data)
                    logging.info(f"Success : Activity ID {activityID} stored in output file {fit_path}")

        except (FileNotFoundError, FitParseError) as err:
            logging.error(err)
            logging.warning(f"Fallback : Failed to use FIT file for activityID {activityID} - Trying TCX file...")

            ns = {
                "tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2",
                "ns3": "http://www.garmin.com/xmlschemas/ActivityExtension/v2",
            }
            try:
                tcx_file_data = garmin_obj.download_activity(activityID, dl_fmt=garmin_obj.ActivityDownloadFormat.TCX).decode("UTF-8")
                root = ET.fromstring(tcx_file_data)

                if KEEP_FIT_FILES:
                    os.makedirs(FIT_FILE_STORAGE_LOCATION, exist_ok=True)
                    act0 = root.findall("tcx:Activities/tcx:Activity", ns)
                    if act0:
                        activity_start_time = datetime.fromisoformat(act0[0].find("tcx:Id", ns).text.strip("Z"))
                        tcx_path = os.path.join(
                            FIT_FILE_STORAGE_LOCATION, activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type + ".tcx"
                        )
                        with open(tcx_path, "w") as f:
                            f.write(tcx_file_data)
                        logging.info(f"Success : Activity ID {activityID} stored in output file {tcx_path}")

            except requests.exceptions.Timeout:
                logging.warning(f"Request timeout for fetching large activity record {activityID} - skipping record")
                return []
            except Exception:
                logging.exception(f"Unable to fetch TCX for activity record {activityID} : skipping record")
                return []

            for activity in root.findall("tcx:Activities/tcx:Activity", ns):
                activity_start_time = datetime.fromisoformat(activity.find("tcx:Id", ns).text.strip("Z"))
                lap_index = 1
                for lap in activity.findall("tcx:Lap", ns):
                    for tp in lap.findall(".//tcx:Trackpoint", ns):
                        t_txt = tp.findtext("tcx:Time", default=None, namespaces=ns)
                        if not t_txt:
                            continue
                        time_obj = datetime.fromisoformat(t_txt.strip("Z"))

                        lat = tp.findtext("tcx:Position/tcx:LatitudeDegrees", default=None, namespaces=ns)
                        lon = tp.findtext("tcx:Position/tcx:LongitudeDegrees", default=None, namespaces=ns)
                        alt = tp.findtext("tcx:AltitudeMeters", default=None, namespaces=ns)
                        dist = tp.findtext("tcx:DistanceMeters", default=None, namespaces=ns)
                        hr = tp.findtext("tcx:HeartRateBpm/tcx:Value", default=None, namespaces=ns)
                        speed = tp.findtext("tcx:Extensions/ns3:TPX/ns3:Speed", default=None, namespaces=ns)

                        def _to_f(x):
                            try:
                                return float(x)
                            except Exception:
                                return None

                        point = {
                            "measurement": "ActivityGPS",
                            "time": time_obj.isoformat(),
                            "tags": {
                                "Device": GARMIN_DEVICENAME,
                                "Database_Name": INFLUXDB_DATABASE,
                                "ActivityID": activityID,
                                "ActivitySelector": activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type,
                            },
                            "fields": {
                                "ActivityName": activity_type,
                                "Activity_ID": activityID,
                                "Latitude": _to_f(lat),
                                "Longitude": _to_f(lon),
                                "Altitude": _to_f(alt),
                                "Distance": _to_f(dist),
                                "DurationSeconds": (time_obj - activity_start_time).total_seconds(),
                                "HeartRate": _to_f(hr),
                                "Speed": _to_f(speed),
                                "lap": lap_index,
                            },
                        }
                        points_list.append(point)
                    lap_index += 1

        logging.info(f"Success : Fetching detailed activity for Activity ID {activityID}")
        PARSED_ACTIVITY_ID_LIST.append(activityID)

    return points_list

# %%
def get_lactate_threshold(date_str):
    points_list = []
    endpoints = {}

    for ltsport in LACTATE_THRESHOLD_SPORTS:
        endpoints[f"SpeedThreshold_{ltsport}"] = (
            f"/biometric-service/stats/lactateThresholdSpeed/range/{date_str}/{date_str}?aggregation=daily&sport={ltsport}"
        )
        endpoints[f"HeartRateThreshold_{ltsport}"] = (
            f"/biometric-service/stats/lactateThresholdHeartRate/range/{date_str}/{date_str}?aggregation=daily&sport={ltsport}"
        )

    for label, endpoint in endpoints.items():
        lt_list_all = garmin_obj.connectapi(endpoint) or []
        for lt_dict in lt_list_all:
            value = lt_dict.get("value")
            if value is None:
                continue
            points_list.append(
                {
                    "measurement": "LactateThreshold",
                    "time": datetime.fromtimestamp(datetime.strptime(date_str, "%Y-%m-%d").timestamp(), tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                    "fields": {f"{label}": value},
                }
            )
            logging.info(f"Success : Fetching {label} for date {date_str}")
    return points_list

# %%
def get_training_status(date_str):
    points_list = []
    ts_list_all = garmin_obj.get_training_status(date_str) or {}
    ts_training_data_all = (ts_list_all.get("mostRecentTrainingStatus") or {}).get("latestTrainingStatusData", {}) or {}

    for device_id, ts_dict in ts_training_data_all.items():
        logging.info(f"Success : Processing Training Status for Device {device_id}")
        acute = ts_dict.get("acuteTrainingLoadDTO") or {}
        data_fields = {
            "trainingStatus": ts_dict.get("trainingStatus"),
            "trainingStatusFeedbackPhrase": ts_dict.get("trainingStatusFeedbackPhrase"),
            "weeklyTrainingLoad": ts_dict.get("weeklyTrainingLoad"),
            "fitnessTrend": ts_dict.get("fitnessTrend"),
            "acwrPercent": acute.get("acwrPercent"),
            "dailyTrainingLoadAcute": acute.get("dailyTrainingLoadAcute"),
            "dailyTrainingLoadChronic": acute.get("dailyTrainingLoadChronic"),
            "maxTrainingLoadChronic": acute.get("maxTrainingLoadChronic"),
            "minTrainingLoadChronic": acute.get("minTrainingLoadChronic"),
            "dailyAcuteChronicWorkloadRatio": acute.get("dailyAcuteChronicWorkloadRatio"),
        }
        ts = ts_dict.get("timestamp")
        if ts and any(v is not None for v in data_fields.values()):
            points_list.append(
                {
                    "measurement": "TrainingStatus",
                    "time": datetime.fromtimestamp(ts / 1000, tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                    "fields": data_fields,
                }
            )
            logging.info(f"Success : Fetching Training Status for date {date_str}")
    return points_list

# %%
def get_training_readiness(date_str):
    points_list = []
    tr_list_all = garmin_obj.get_training_readiness(date_str) or []
    for tr_dict in tr_list_all:
        data_fields = {
            "level": tr_dict.get("level"),
            "score": tr_dict.get("score"),
            "sleepScore": tr_dict.get("sleepScore"),
            "sleepScoreFactorPercent": tr_dict.get("sleepScoreFactorPercent"),
            "recoveryTime": tr_dict.get("recoveryTime"),
            "recoveryTimeFactorPercent": tr_dict.get("recoveryTimeFactorPercent"),
            "acwrFactorPercent": tr_dict.get("acwrFactorPercent"),
            "acuteLoad": tr_dict.get("acuteLoad"),
            "stressHistoryFactorPercent": tr_dict.get("stressHistoryFactorPercent"),
            "hrvFactorPercent": tr_dict.get("hrvFactorPercent"),
        }
        ts = tr_dict.get("timestamp")
        if ts and (not all(v is None for v in data_fields.values())):
            points_list.append(
                {
                    "measurement": "TrainingReadiness",
                    "time": pytz.timezone("UTC").localize(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                    "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                    "fields": data_fields,
                }
            )
            logging.info(f"Success : Fetching Training Readiness for date {date_str}")
    return points_list

# %%
def get_hillscore(date_str):
    points_list = []
    hill = garmin_obj.get_hill_score(date_str) or {}
    data_fields = {
        "strengthScore": hill.get("strengthScore"),
        "enduranceScore": hill.get("enduranceScore"),
        "hillScoreClassificationId": hill.get("hillScoreClassificationId"),
        "overallScore": hill.get("overallScore"),
        "hillScoreFeedbackPhraseId": hill.get("hillScoreFeedbackPhraseId"),
        "vo2MaxPreciseValue": hill.get("vo2MaxPreciseValue"),
    }
    if not all(v is None for v in data_fields.values()):
        points_list.append(
            {
                "measurement": "HillScore",
                "time": datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": data_fields,
            }
        )
        logging.info(f"Success : Fetching Hill Score for date {date_str}")
    return points_list

# %%
def get_race_predictions(date_str):
    points_list = []
    rp_all_list = garmin_obj.get_race_predictions(startdate=date_str, enddate=date_str, _type="daily") or []
    rp_all = rp_all_list[0] if len(rp_all_list) > 0 else {}
    if rp_all:
        data_fields = {
            "time5K": rp_all.get("time5K"),
            "time10K": rp_all.get("time10K"),
            "timeHalfMarathon": rp_all.get("timeHalfMarathon"),
            "timeMarathon": rp_all.get("timeMarathon"),
        }
        if not all(v is None for v in data_fields.values()):
            points_list.append(
                {
                    "measurement": "RacePredictions",
                    "time": datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(),
                    "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                    "fields": data_fields,
                }
            )
            logging.info(f"Success : Fetching Race Predictions for date {date_str}")
    return points_list

# %%
def get_fitness_age(date_str):
    points_list = []
    fitness_age = garmin_obj.get_fitnessage_data(date_str) or {}
    if fitness_age:
        data_fields = {
            "chronologicalAge": float(fitness_age.get("chronologicalAge")) if fitness_age.get("chronologicalAge") else None,
            "fitnessAge": fitness_age.get("fitnessAge"),
            "achievableFitnessAge": fitness_age.get("achievableFitnessAge"),
        }
        if not all(v is None for v in data_fields.values()):
            points_list.append(
                {
                    "measurement": "FitnessAge",
                    "time": datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(),
                    "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                    "fields": data_fields,
                }
            )
            logging.info(f"Success : Fetching Fitness Age for date {date_str}")
    return points_list

# %%
def get_vo2_max(date_str):
    points_list = []
    max_metrics = garmin_obj.get_max_metrics(date_str)
    try:
        if max_metrics:
            vo2_max_value = (max_metrics[0].get("generic") or {}).get("vo2MaxPreciseValue", None)
            vo2_max_value_cycling = (max_metrics[0].get("cycling") or {}).get("vo2MaxPreciseValue", None)
            if (vo2_max_value is not None) or (vo2_max_value_cycling is not None):
                points_list.append(
                    {
                        "measurement": "VO2_Max",
                        "time": datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(),
                        "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                        "fields": {"VO2_max_value": vo2_max_value, "VO2_max_value_cycling": vo2_max_value_cycling},
                    }
                )
                logging.info(f"Success : Fetching VO2-max for date {date_str}")
        return points_list
    except AttributeError:
        return []

# %%
def get_endurance_score(date_str):
    points_list = []
    endurance_dict = garmin_obj.get_endurance_score(date_str) or {}
    if endurance_dict.get("overallScore") is not None:
        points_list.append(
            {
                "measurement": "EnduranceScore",
                "time": pytz.timezone("UTC").localize(datetime.strptime(date_str, "%Y-%m-%d")).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": {"EnduranceScore": endurance_dict.get("overallScore")},
            }
        )
        logging.info(f"Success : Fetching Endurance Score for date {date_str}")
    return points_list

# %%
def get_blood_pressure(date_str):
    points_list = []
    bp_all = (garmin_obj.get_blood_pressure(date_str, date_str) or {}).get("measurementSummaries", []) or []
    if len(bp_all) > 0:
        bp_list = (bp_all[0] or {}).get("measurements", []) or []
        for bp_measurement in bp_list:
            data_fields = {
                "Systolic": bp_measurement.get("systolic", None),
                "Diastolic": bp_measurement.get("diastolic", None),
                "Pulse": bp_measurement.get("pulse", None),
            }
            ts = bp_measurement.get("measurementTimestampGMT")
            if ts and (not all(v is None for v in data_fields.values())):
                points_list.append(
                    {
                        "measurement": "BloodPressure",
                        "time": pytz.UTC.localize(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                        "tags": {
                            "Device": GARMIN_DEVICENAME,
                            "Database_Name": INFLUXDB_DATABASE,
                            "Source": bp_measurement.get("sourceType", None),
                        },
                        "fields": data_fields,
                    }
                )
        logging.info(f"Success : Fetching Blood Pressure for date {date_str}")
    return points_list

# %%
def get_hydration(date_str):
    points_list = []
    hydration_dict = garmin_obj.get_hydration_data(date_str) or {}
    data_fields = {
        "ValueInML": hydration_dict.get("valueInML", None),
        "SweatLossInML": hydration_dict.get("sweatLossInML", None),
        "GoalInML": hydration_dict.get("goalInML", None),
        "ActivityIntakeInML": hydration_dict.get("activityIntakeInML", None),
    }
    if not all(v is None for v in data_fields.values()):
        points_list.append(
            {
                "measurement": "Hydration",
                "time": datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(),
                "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
                "fields": data_fields,
            }
        )
        logging.info(f"Success : Fetching Hydration data for date {date_str}")
    return points_list

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
    if INFLUXDB_VERSION != "1":
        return

    end_dt = _dt_utc(asof_date) + timedelta(days=1)
    start_dt = end_dt - timedelta(days=42)
    start_z, end_z = _iso_z(start_dt), _iso_z(end_dt)

    # --- RUN: mountain fitness via VAM (best sustained climb rate) ---
    q_vam20 = (
        'SELECT max("best20m_vam_m_per_h") AS vam20 '
        'FROM "DerivedActivity" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "AND sport_tag =~ /running/ "
        f'AND "Database_Name"=\'{INFLUXDB_DATABASE}\' AND "Device"=\'{GARMIN_DEVICENAME}\''
    )
    vam20 = _query_scalar_influx_v1(q_vam20)

    q_vam30 = (
        'SELECT max("best30m_vam_m_per_h") AS vam30 '
        'FROM "DerivedActivity" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "AND sport_tag =~ /running/ "
        f'AND "Database_Name"=\'{INFLUXDB_DATABASE}\' AND "Device"=\'{GARMIN_DEVICENAME}\''
    )
    vam30 = _query_scalar_influx_v1(q_vam30)

    # --- RUN: VO2 (use your canonical field) ---
    # If DerivedActivity sets vo2max_est = best5m_vo2_masked, this is correct.
    q_run_vo2 = (
        'SELECT max("vo2max_est") AS vo2 '
        'FROM "DerivedActivity" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "AND sport_tag =~ /running/ "
        f'AND "Database_Name"=\'{INFLUXDB_DATABASE}\' AND "Device"=\'{GARMIN_DEVICENAME}\''
    )
    vo2 = _query_scalar_influx_v1(q_run_vo2)

    # --- RUN: most recent derived metrics in 42d ---
    # IMPORTANT: use the actual field names you write in DerivedActivity.
    # Replace best5m_vo2_masked with best5m_vo2_raw if you kept that naming.
    q_run_last = (
        'SELECT '
        '  last("cs_pace_s_per_km") AS cs_pace, '
        '  last("cs_mps")          AS cs_mps, '
        '  last("dprime_m")        AS dprime_m, '
        '  last("best5m_vo2_masked") AS best5m_vo2_last, '
        '  last("gap_distance_km") AS gap_km, '
        '  last("raw_distance_m_from_speed") AS raw_dist_m_speed, '
        '  last("raw_distance_m_from_fit")   AS raw_dist_m_fit, '
        '  last("lthr_bpm_est")    AS lthr '
        'FROM "DerivedActivity" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "AND sport_tag =~ /running/ "
        f'AND "Database_Name"=\'{INFLUXDB_DATABASE}\' AND "Device"=\'{GARMIN_DEVICENAME}\''
    )
    run_last = _query_last_row_influx_v1(q_run_last) or {}

    # --- BIKE: most recent derived metrics in 42d ---
    q_bike_last = (
        'SELECT '
        '  last("cp_watts")     AS cp, '
        '  last("wprime_j")     AS wprime_j, '
        '  last("lthr_bpm_est") AS lthr '
        'FROM "DerivedActivity" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "AND sport_tag =~ /cycl|bike|ride/ "
        f'AND "Database_Name"=\'{INFLUXDB_DATABASE}\' AND "Device"=\'{GARMIN_DEVICENAME}\''
    )
    bike_last = _query_last_row_influx_v1(q_bike_last) or {}

    # --- gender for fitness age model ---
    gender = _get_gender_for_day_v1(asof_date)
    if gender not in {"male", "female"}:
        row = _get_userprofile_master_v1()
        gc = row.get("gender_code")
        try:
            gc_i = int(float(gc)) if gc is not None else 0
        except Exception:
            gc_i = 0
        gender = "male" if gc_i == 1 else "female" if gc_i == 2 else "unknown"

    fa = fitness_age_from_vo2(vo2, gender)

    rhr, _ = _get_physiology_for_day_v1(asof_date)
    if fa is not None and rhr is not None:
        try:
            r = float(rhr)
            fa = fa + np.clip((r - 50.0) / 5.0, -2.0, 2.0)
        except Exception:
            pass

    def f(x):
        try:
            return float(x) if x is not None else None
        except Exception:
            return None

    point = {
        "measurement": "PerformanceDaily",
        "time": _dt_utc(asof_date).isoformat(),
        "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
        "fields": {
            # Mountain fitness
            "VAM20_best_42d_m_per_h": f(vam20),
            "VAM30_best_42d_m_per_h": f(vam30),
            "MountainFitness_source": "DerivedActivity.max(best20m/best30m)_vam_m_per_h over 42d",

            # VO2-based
            "VO2max_est_run": f(vo2),
            "FitnessAge_model": float(fa) if fa is not None else None,
            "FitnessAge_model_source": "vo2_run(+rhr_adj)" if fa is not None else None,

            # RUN details
            "CS_pace_s_per_km": f(run_last.get("cs_pace")),
            "CS_mps": f(run_last.get("cs_mps")),
            "Dprime_m": f(run_last.get("dprime_m")),
            "best5m_vo2_last": f(run_last.get("best5m_vo2_last")),
            "gap_distance_km_last": f(run_last.get("gap_km")),
            "raw_distance_m_from_speed_last": f(run_last.get("raw_dist_m_speed")),
            "raw_distance_m_from_fit_last": f(run_last.get("raw_dist_m_fit")),
            "LTHR_run_bpm_est": f(run_last.get("lthr")),

            # BIKE details
            "CP_watts": f(bike_last.get("cp")),
            "Wprime_j": f(bike_last.get("wprime_j")),
            "LTHR_bike_bpm_est": f(bike_last.get("lthr")),
        },
    }

    write_points_to_influxdb([point])

# %%
def daily_fetch_write(date_str, *, run_rollups_inline: bool = True):
    if REQUEST_INTRADAY_DATA_REFRESH and (
        datetime.strptime(date_str, "%Y-%m-%d") <= (datetime.today() - timedelta(days=IGNORE_INTRADAY_DATA_REFRESH_DAYS))
    ):
        data_refresh_response = (garmin_obj.connectapi(f"wellness-service/wellness/epoch/request/{date_str}", method="POST") or {}).get(
            "status", "Unknown"
        )
        logging.info(f"Intraday data refresh request status: {data_refresh_response}")
        if data_refresh_response == "SUBMITTED":
            logging.info("Waiting 10 seconds for refresh request to process...")
            time.sleep(10)
        elif data_refresh_response == "COMPLETE":
            logging.info(f"Data for date {date_str} is already available")
        elif data_refresh_response == "NO_FILES_FOUND":
            logging.info(f"No Data is available for date {date_str} to refresh")
            return None
        elif data_refresh_response == "DENIED":
            logging.info(
                "Daily refresh limit reached. Pausing script for 24 hours to ensure Intraday data fetching. Disable REQUEST_INTRADAY_DATA_REFRESH to avoid this!"
            )
            time.sleep(86500)
            data_refresh_response = (garmin_obj.connectapi(f"wellness-service/wellness/epoch/request/{date_str}", method="POST") or {}).get(
                "status", "Unknown"
            )
            logging.info(f"Intraday data refresh request status: {data_refresh_response}")
            logging.info("Waiting 10 seconds...")
            time.sleep(10)
        else:
            logging.info("Refresh response is unknown!")
            time.sleep(5)

    # --- user profile metadata (gender etc) ---
    try:
        if USERPROFILE_WRITE_ONCE_PER_DAY and _userprofile_exists_for_day_v1(date_str):
            logging.info(f"UserProfile already exists for {date_str}; skipping daily write")
        else:
            g = _get_user_gender_from_garmin()
            if g == "unknown":
                g = FIT_GENDER_CACHE.get(date_str, "unknown")
    
            by = _stored_birth_year_v1() or _get_birth_year_from_garmin_profile()
    
            if g != "unknown" or by is not None:
                write_points_to_influxdb(
                    write_user_profile_point(
                        date_str,
                        gender=None if g == "unknown" else g,
                        birth_year=by,
                    )
                )
            else:
                logging.info(f"{date_str} - UserProfile: not available yet (will be updated from FIT if an activity is processed)")
    except Exception:
        logging.exception(f"UserProfile write failed for {date_str}")

    if "daily_avg" in FETCH_SELECTION:
        write_points_to_influxdb(get_daily_stats(date_str))
    if "sleep" in FETCH_SELECTION:
        write_points_to_influxdb(get_sleep_data(date_str))
    if "steps" in FETCH_SELECTION:
        write_points_to_influxdb(get_intraday_steps(date_str))
    if "heartrate" in FETCH_SELECTION:
        write_points_to_influxdb(get_intraday_hr(date_str))
    if "stress" in FETCH_SELECTION:
        write_points_to_influxdb(get_intraday_stress(date_str))
    if "breathing" in FETCH_SELECTION:
        write_points_to_influxdb(get_intraday_br(date_str))
    if "hrv" in FETCH_SELECTION:
        write_points_to_influxdb(get_intraday_hrv(date_str))
    if "fitness_age" in FETCH_SELECTION:
        write_points_to_influxdb(get_fitness_age(date_str))
    if "vo2" in FETCH_SELECTION:
        write_points_to_influxdb(get_vo2_max(date_str))
    if "race_prediction" in FETCH_SELECTION:
        write_points_to_influxdb(get_race_predictions(date_str))
    if "body_composition" in FETCH_SELECTION:
        write_points_to_influxdb(get_body_composition(date_str))
    if "lactate_threshold" in FETCH_SELECTION:
        write_points_to_influxdb(get_lactate_threshold(date_str))
    if "training_status" in FETCH_SELECTION:
        write_points_to_influxdb(get_training_status(date_str))
    if "training_readiness" in FETCH_SELECTION:
        write_points_to_influxdb(get_training_readiness(date_str))
    if "hill_score" in FETCH_SELECTION:
        write_points_to_influxdb(get_hillscore(date_str))
    if "endurance_score" in FETCH_SELECTION:
        write_points_to_influxdb(get_endurance_score(date_str))
    if "blood_pressure" in FETCH_SELECTION:
        write_points_to_influxdb(get_blood_pressure(date_str))
    if "hydration" in FETCH_SELECTION:
        write_points_to_influxdb(get_hydration(date_str))
    if "activity" in FETCH_SELECTION:
        activity_summary_points_list, activity_with_gps_id_dict = get_activity_summary(date_str)
        write_points_to_influxdb(activity_summary_points_list)
        write_points_to_influxdb(fetch_activity_GPS(activity_with_gps_id_dict))
    if "solar_intensity" in FETCH_SELECTION:
        write_points_to_influxdb(get_solar_intensity(date_str))
    if "lifestyle" in FETCH_SELECTION:
        write_points_to_influxdb(get_lifestyle_data(date_str))

    # --- physiology/training rollups ---
    if run_rollups_inline:
        try:
            compute_and_write_physiology(date_str)
        except Exception:
            logging.exception(f"PhysiologyDaily computation failed for {date_str}")

        try:
            compute_and_write_training_load(date_str)
        except Exception:
            logging.exception(f"TrainingLoadDaily computation failed for {date_str}")

        try:
            compute_and_write_performance_daily(date_str)
        except Exception:
            logging.exception(f"PerformanceDaily computation failed for {date_str}")

def compute_rollups_range(start_date: str, end_date: str) -> None:
    """
    Compute rollups in chronological order so:
      - HRmax windows have historical data present
      - ATL/CTL can use yesterday's value
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    while current <= end:
        d = current.strftime("%Y-%m-%d")
    
        try:
            compute_and_write_physiology(d)
        except Exception:
            logging.exception(f"PhysiologyDaily computation failed for {d}")
    
        try:
            compute_and_write_training_load(d)
        except Exception:
            logging.exception(f"TrainingLoadDaily computation failed for {d}")

        try:
            compute_and_write_performance_daily(d)
        except Exception:
            logging.exception(f"PerformanceDaily computation failed for {d}")
    
        current += timedelta(days=1)
                
# %%
def fetch_write_bulk(start_date_str, end_date_str, *, local_timediff: timedelta):
    global garmin_obj
    consecutive_500_errors = 0
    logging.info("Fetching data for the given period in reverse chronological order")
    time.sleep(3)
    write_points_to_influxdb(get_last_sync())

    rollups_after = _should_rollups_after_ingest(start_date_str, end_date_str, local_timediff)
    if rollups_after:
        logging.info("Mode: backfill (rollups after ingest)")
    else:
        logging.info("Mode: live (rollups inline)")

    try:
        for current_date in iter_days(start_date_str, end_date_str):
            repeat_loop = True
            while repeat_loop:
                try:
                    daily_fetch_write(current_date, run_rollups_inline=(not rollups_after))
                    if consecutive_500_errors > 0:
                        logging.info(
                            f"Successfully fetched data after {consecutive_500_errors} consecutive 500 errors - resetting error counter"
                        )
                        consecutive_500_errors = 0
    
                    logging.info(f"Success : Fetched all available health metrics for date {current_date} (skipped any if unavailable)")
                    if RATE_LIMIT_CALLS_SECONDS > 0:
                        logging.info(f"Waiting : for {RATE_LIMIT_CALLS_SECONDS} seconds")
                        time.sleep(RATE_LIMIT_CALLS_SECONDS)
                    repeat_loop = False
    
                except GarminConnectTooManyRequestsError as err:
                    logging.error(err)
                    logging.info(f"Too many requests (429) : Failed to fetch one or more metrics - will retry for date {current_date}")
                    logging.info(f"Waiting : for {FETCH_FAILED_WAIT_SECONDS} seconds")
                    time.sleep(FETCH_FAILED_WAIT_SECONDS)
                    repeat_loop = True
    
                except (requests.exceptions.HTTPError, GarthHTTPError) as err:
                    is_500_error = False
                    if isinstance(err, requests.exceptions.HTTPError):
                        if hasattr(err, "response") and err.response is not None and err.response.status_code == 500:
                            is_500_error = True
                    elif isinstance(err, GarthHTTPError):
                        if getattr(err, "status_code", None) == 500:
                            is_500_error = True
                        elif hasattr(err, "response") and err.response is not None and err.response.status_code == 500:
                            is_500_error = True
    
                    if is_500_error:
                        consecutive_500_errors += 1
                        logging.error(f"HTTP 500 error ({consecutive_500_errors}/{MAX_CONSECUTIVE_500_ERRORS}) for date {current_date}: {err}")
                        if consecutive_500_errors >= MAX_CONSECUTIVE_500_ERRORS:
                            logging.warning(
                                f"Received {consecutive_500_errors} consecutive HTTP 500 errors. Logging error and continuing backward in time to fetch remaining data."
                            )
                            logging.warning(f"Skipping date {current_date} due to persistent 500 errors from Garmin API")
                            logging.info(f"Waiting : for {RATE_LIMIT_CALLS_SECONDS} seconds before continuing")
                            time.sleep(RATE_LIMIT_CALLS_SECONDS)
                            repeat_loop = False
                        else:
                            logging.info(
                                f"HTTP 500 error encountered - will retry for date {current_date} (attempt {consecutive_500_errors}/{MAX_CONSECUTIVE_500_ERRORS})"
                            )
                            logging.info(f"Waiting : for {RATE_LIMIT_CALLS_SECONDS} seconds before retry")
                            time.sleep(RATE_LIMIT_CALLS_SECONDS)
                            repeat_loop = True
                    else:
                        logging.error(err)
                        logging.info(f"HTTP Error (non-500) : Failed to fetch one or more metrics - skipping date {current_date}")
                        logging.info(f"Waiting : for {RATE_LIMIT_CALLS_SECONDS} seconds")
                        time.sleep(RATE_LIMIT_CALLS_SECONDS)
                        repeat_loop = False
    
                except (GarminConnectConnectionError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as err:
                    logging.error(err)
                    logging.info(f"Connection Error : Failed to fetch one or more metrics - skipping date {current_date}")
                    logging.info(f"Waiting : for {RATE_LIMIT_CALLS_SECONDS} seconds")
                    time.sleep(RATE_LIMIT_CALLS_SECONDS)
                    repeat_loop = False
    
                except GarminConnectAuthenticationError as err:
                    logging.error(err)
                    logging.info("Authentication Failed : Retrying login with given credentials (won't work automatically for MFA/2FA enabled accounts)")
                    garmin_obj = garmin_login()
                    time.sleep(5)
                    repeat_loop = True
    
                except Exception as err:
                    if IGNORE_ERRORS:
                        logging.warning("IGNORE_ERRORS Enabled >> Failed to process %s:", current_date)
                        logging.exception(err)
                        repeat_loop = False
                    else:
                        raise err
    finally:
        if rollups_after:
            run_rollups_for_range(start_date_str, end_date_str)

# %%
if __name__ == "__main__":
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
        sys.exit(0)
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