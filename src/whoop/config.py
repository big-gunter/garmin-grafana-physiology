from __future__ import annotations

import os


def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"true", "t", "1", "yes", "y"}


WHOOP_CLIENT_ID = os.getenv("WHOOP_CLIENT_ID", "")
WHOOP_CLIENT_SECRET = os.getenv("WHOOP_CLIENT_SECRET", "")
WHOOP_TOKEN_DIR = os.path.expanduser(os.getenv("WHOOP_TOKEN_DIR", "~/.whoop"))

INFLUXDB_HOST = os.getenv("INFLUXDB_HOST", "localhost")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT", "8086"))
INFLUXDB_USERNAME = os.getenv("INFLUXDB_USERNAME", "")
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD", "")
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE", "GarminStats")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
UPDATE_INTERVAL_SECONDS = int(os.getenv("UPDATE_INTERVAL_SECONDS", "300"))
SKIP_EXISTING_DAILY = _bool_env("SKIP_EXISTING_DAILY", default=True)
TAG_MEASUREMENTS_WITH_USER_EMAIL = _bool_env("TAG_MEASUREMENTS_WITH_USER_EMAIL", default=False)

MANUAL_START_DATE = os.getenv("MANUAL_START_DATE", None)
MANUAL_END_DATE = os.getenv("MANUAL_END_DATE", None)
