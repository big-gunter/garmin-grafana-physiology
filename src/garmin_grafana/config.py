from __future__ import annotations

import base64
import logging
import os
from datetime import datetime

import dotenv


def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"true", "t", "1", "yes", "y"}


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


USER_GENDER_OVERRIDE = os.getenv("USER_GENDER", "").strip().lower()  # male|female|m|f|unknown|"" (auto)

env_override = dotenv.load_dotenv("override-default-vars.env", override=True)
if env_override:
    logging.warning("System ENV variables are overridden with override-default-vars.env")


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

FETCH_SELECTION = _csv_env(
    "FETCH_SELECTION",
    "daily_avg,sleep,steps,heartrate,stress,breathing,hrv,fitness_age,vo2,activity,race_prediction,body_composition,lifestyle",
)

LACTATE_THRESHOLD_SPORTS = [s.upper() for s in _csv_env("LACTATE_THRESHOLD_SPORTS", "RUNNING")]

KEEP_FIT_FILES = _bool_env("KEEP_FIT_FILES", default=False)  # optional
FIT_FILE_STORAGE_LOCATION = os.getenv("FIT_FILE_STORAGE_LOCATION", os.path.join(os.path.expanduser("~"), "fit_filestore"))
ALWAYS_PROCESS_FIT_FILES = _bool_env("ALWAYS_PROCESS_FIT_FILES", default=False)  # optional
REQUEST_INTRADAY_DATA_REFRESH = _bool_env("REQUEST_INTRADAY_DATA_REFRESH", default=False)  # optional
IGNORE_INTRADAY_DATA_REFRESH_DAYS = int(os.getenv("IGNORE_INTRADAY_DATA_REFRESH_DAYS", 30))  # optional
TAG_MEASUREMENTS_WITH_USER_EMAIL = _bool_env("TAG_MEASUREMENTS_WITH_USER_EMAIL", default=False)  # optional

FORCE_REPROCESS_ACTIVITIES = _bool_env("FORCE_REPROCESS_ACTIVITIES", default=True)  # optional
ROLLUPS_AFTER_INGEST = _bool_env("ROLLUPS_AFTER_INGEST", default=True)

USER_TIMEZONE = os.getenv("USER_TIMEZONE", "")  # optional
IGNORE_ERRORS = _bool_env("IGNORE_ERRORS", default=False)

