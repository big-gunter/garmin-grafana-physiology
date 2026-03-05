# %%
import traceback
import base64, requests, time, pytz, logging, os, sys, dotenv, io, zipfile
from fitparse import FitFile, FitParseError
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
from influxdb_client_3 import InfluxDBClient3, InfluxDBError
import xml.etree.ElementTree as ET
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

def _get_age_years(asof_date: str | None = None) -> int:
    """
    Returns age in years.

    Priority:
      1) USER_AGE env (if > 0)
      2) Garmin profile birthDate (YYYY-MM-DD)
      3) UserProfile.birth_year from InfluxDB (daily point; FIT-derived if present)
      4) 0 (unknown)

    Notes:
      - If asof_date is provided, age is calculated as-of that date (UTC day).
      - If asof_date is omitted, uses today's local date.
    """
    # 1) Explicit override
    if USER_AGE and USER_AGE > 0:
        return USER_AGE

    # Choose "today" reference
    try:
        ref = _date.today() if not asof_date else datetime.strptime(asof_date, "%Y-%m-%d").date()
    except Exception:
        ref = _date.today()

    # 2) Garmin profile birthDate
    try:
        prof = (garmin_obj.garth.profile or {}) if garmin_obj is not None else {}
        b = None
        if isinstance(prof, dict):
            b = prof.get("birthDate") or prof.get("birthdate") or prof.get("dateOfBirth")
            if not b and isinstance(prof.get("userProfile"), dict):
                b = prof["userProfile"].get("birthDate") or prof["userProfile"].get("birthdate") or prof["userProfile"].get("dateOfBirth")

        if b:
            y, m, d = [int(x) for x in str(b).split("-")[:3]]
            age = ref.year - y - ((ref.month, ref.day) < (m, d))
            return max(age, 0)
    except Exception:
        logging.exception("Unable to derive age from Garmin profile")

    # 3) FIT/UserProfile birth_year (Influx v1)
    try:
        if INFLUXDB_VERSION == "1":
            # Prefer the as-of day; if missing, fall back to "most recent known birth_year".
            by = None
            if asof_date:
                by = _get_birth_year_for_day_v1(asof_date)

            if by is None:
                q = (
                    'SELECT last("birth_year") AS by '
                    'FROM "UserProfile" '
                    f'WHERE "birth_year" > 0 '
                    f"AND \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}'"
                )
                row = _query_last_row_influx_v1(q) or {}
                by = row.get("by")
                if by is not None:
                    by = int(float(by))

            if by:
                age = ref.year - int(by)
                return max(age, 0)
    except Exception:
        logging.exception("Unable to derive age from UserProfile.birth_year")

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


def _get_trainingload_prev_day_v1(date_str: str) -> tuple[float | None, float | None]:
    # Returns (ATL_7_yday, CTL_42_yday)
    yday = (_dt_utc(date_str) - timedelta(days=1)).strftime("%Y-%m-%d")
    start_z, end_z = _day_bounds_z(yday)
    q = (
        'SELECT last("ATL_7") AS atl, last("CTL_42") AS ctl '
        'FROM "TrainingLoadDaily" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f"AND \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}'"
    )
    row = _query_last_row_influx_v1(q) or {}
    atl = row.get("atl")
    ctl = row.get("ctl")
    return (float(atl) if atl is not None else None, float(ctl) if ctl is not None else None)

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

def compute_and_write_training_load(date_str: str) -> None:
    if INFLUXDB_VERSION != "1":
        logging.warning("Training load computation currently implemented for InfluxDB v1 only.")
        return

    gender = _norm_gender(_get_gender_for_day_v1(date_str))
    if gender not in {"male", "female"}:
        logging.info(f"TrainingLoadDaily: gender unknown for {date_str}; skipping write (Bannister requires male/female).")
        return

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

    acts = _get_activities_for_day_v1(date_str)

    trimp_total = 0.0
    act_count = 0
    for a in acts:
        # guardrail: don't ever count the terminal END marker if it comes through
        if str(a.get("activityName", "")).upper() == "END":
            continue

        dur = a.get("elapsedDuration")
        hr = a.get("averageHR")
        if dur is None or hr is None:
            continue

        try:
            dur_f = float(dur)
            hr_f = float(hr)
        except Exception:
            continue

        t = _bannister_trimp(dur_f, hr_f, rhr_f, hrmax_f, gender)
        if t > 0:
            trimp_total += t
            act_count += 1

    if act_count == 0 and trimp_total == 0.0:
        logging.info(f"TrainingLoadDaily: no usable activities for {date_str}; skipping write")
        return

    # EWMA (yesterday -> today)
    atl_prev, ctl_prev = _get_trainingload_prev_day_v1(date_str)

    a7 = 1.0 - math.exp(-1.0 / 7.0)
    a42 = 1.0 - math.exp(-1.0 / 42.0)

    atl_prev = float(atl_prev) if atl_prev is not None else None
    ctl_prev = float(ctl_prev) if ctl_prev is not None else None

    atl = trimp_total if atl_prev is None else (atl_prev + a7 * (trimp_total - atl_prev))
    ctl = trimp_total if ctl_prev is None else (ctl_prev + a42 * (trimp_total - ctl_prev))
    tsb = ctl - atl

    point = {
        "measurement": "TrainingLoadDaily",
        "time": _dt_utc(date_str).isoformat(),
        "tags": {"Device": GARMIN_DEVICENAME, "Database_Name": INFLUXDB_DATABASE},
        "fields": {
            "TRIMP": float(trimp_total),
            "ATL_7": float(atl),
            "CTL_42": float(ctl),
            "TSB": float(tsb),
            "RHR_used": float(rhr_f),
            "HRmax_used": float(hrmax_f),
            "gender_code_used": int(_gender_code(gender)),
            "gender_is_known_used": 1,
            "activities_used": int(act_count),
        },
    }

    write_points_to_influxdb([point])
    logging.info(
        f"TrainingLoadDaily written for {date_str}: TRIMP={trimp_total:.2f}, ATL_7={atl:.2f}, CTL_42={ctl:.2f}, TSB={tsb:.2f} (acts={act_count})"
    )

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
        'SELECT last("birth_year") AS by '
        'FROM "UserProfile" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f"AND \"Database_Name\"='{INFLUXDB_DATABASE}' AND \"Device\"='{GARMIN_DEVICENAME}'"
    )
    row = _query_last_row_influx_v1(q) or {}
    by = row.get("by")
    if by is None:
        return None
    try:
        return int(float(by))
    except Exception:
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

                if len(all_records_list) == 0:
                    raise FileNotFoundError(f"No records found in FIT file for Activity ID {activityID} - Discarding FIT file")

                # Guardrail: determine activity_start_time before using it anywhere
                ts0 = all_records_list[0].get("timestamp")
                if not ts0:
                    raise FileNotFoundError(f"First record missing timestamp in FIT for Activity ID {activityID} - Discarding FIT file")
                activity_start_time = ts0.replace(tzinfo=pytz.UTC)

                # FIT user_profile extraction (gender + birth year)
                all_user_list = [m.get_values() for m in fitfile.get_messages("user_profile")]
                
                fit_gender = "unknown"
                fit_birth_year = None
                
                if all_user_list:
                    for up in all_user_list:
                        g = up.get("gender") or up.get("sex")
                        yob = up.get("birth_year") or up.get("year_of_birth")
                
                        if g is not None and fit_gender == "unknown":
                            fit_gender = _norm_gender(g)
                
                        if yob is not None and fit_birth_year is None:
                            try:
                                fit_birth_year = int(yob)
                            except Exception:
                                fit_birth_year = None
                
                        # stop once we have both
                        if fit_gender in {"male", "female"} and fit_birth_year is not None:
                            break

                # Write UserProfile from FIT (daily point; overwrites same-day point because tagset+time match)
                try:
                    act_date = activity_start_time.strftime("%Y-%m-%d")
                    if fit_gender != "unknown" or fit_birth_year is not None:
                        write_points_to_influxdb(
                            write_user_profile_point(
                                act_date,
                                gender=fit_gender if fit_gender != "unknown" else None,
                                birth_year=fit_birth_year,
                            )
                        )
                except Exception:
                    logging.exception("Failed writing UserProfile (gender/birth_year) from FIT")

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

# %%
def daily_fetch_write(date_str):
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
                logging.info(f"UserProfile gender unknown from garmin_profile for {date_str}; skipping write")
            else:
                write_points_to_influxdb(write_user_profile_point(date_str, gender=g))
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

    # --- physiology rollups (after ingestion for the day) ---
    try:
        compute_and_write_physiology(date_str)
    except Exception:
        logging.exception(f"PhysiologyDaily computation failed for {date_str}")
        
    try:
        compute_and_write_training_load(date_str)
    except Exception:
        logging.exception(f"TrainingLoadDaily computation failed for {date_str}")

# %%
def fetch_write_bulk(start_date_str, end_date_str):
    global garmin_obj
    consecutive_500_errors = 0
    logging.info("Fetching data for the given period in reverse chronological order")
    time.sleep(3)
    write_points_to_influxdb(get_last_sync())

    for current_date in iter_days(start_date_str, end_date_str):
        repeat_loop = True
        while repeat_loop:
            try:
                daily_fetch_write(current_date)
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

# %%
if __name__ == "__main__":
    garmin_obj = garmin_login()

    if MANUAL_START_DATE:
        fetch_write_bulk(MANUAL_START_DATE, MANUAL_END_DATE)
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
                "No previously synced data found in local InfluxDB database, defaulting to 7 day initial fetching. Use specific start date ENV variable to bulk update past data"
            )
            last_influxdb_sync_time_UTC = (datetime.today() - timedelta(days=7)).astimezone(pytz.timezone("UTC"))

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

        while True:
            last_watch_sync_time_UTC = datetime.fromtimestamp(int((garmin_obj.get_device_last_used() or {}).get("lastUsedDeviceUploadTime") / 1000)).astimezone(
                pytz.timezone("UTC")
            )
            if last_influxdb_sync_time_UTC < last_watch_sync_time_UTC:
                logging.info(f"Update found : Current watch sync time is {last_watch_sync_time_UTC} UTC")
                fetch_write_bulk(
                    (last_influxdb_sync_time_UTC + local_timediff).strftime("%Y-%m-%d"),
                    (last_watch_sync_time_UTC + local_timediff).strftime("%Y-%m-%d"),
                )
                last_influxdb_sync_time_UTC = last_watch_sync_time_UTC
            else:
                logging.info(f"No new data found : Current watch and influxdb sync time is {last_watch_sync_time_UTC} UTC")
            logging.info(f"waiting for {UPDATE_INTERVAL_SECONDS} seconds before next automatic update calls")
            time.sleep(UPDATE_INTERVAL_SECONDS)