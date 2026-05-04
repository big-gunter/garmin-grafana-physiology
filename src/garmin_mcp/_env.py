from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


def _get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_influx_settings():
    """Load Influx connection settings from environment (.env optional)."""
    load_dotenv(override=False)
    return dict(
        version=os.getenv("INFLUXDB_VERSION", "1"),
        host=os.getenv("INFLUXDB_HOST", "localhost"),
        port=int(os.getenv("INFLUXDB_PORT", "8086")),
        username=os.getenv("INFLUXDB_USERNAME", ""),
        password=os.getenv("INFLUXDB_PASSWORD", ""),
        database=os.getenv("INFLUXDB_DATABASE", "GarminStats"),
        v3_access_token=os.getenv("INFLUXDB_V3_ACCESS_TOKEN", ""),
        endpoint_is_http=_get_bool("INFLUXDB_ENDPOINT_IS_HTTP", True),
    )


def docs_dir() -> Path:
    raw = (os.getenv("GARMIN_DOCS_DIR") or "").strip()
    if raw:
        return Path(raw).resolve()
    here = Path(__file__).resolve().parent
    app_root = here.parent
    # Docker: /app/docs (see Dockerfile COPY docs /app/docs)
    cand = app_root / "docs"
    if cand.is_dir():
        return cand.resolve()
    # Local dev: repo/docs when packages live under src/
    cand2 = app_root.parent / "docs"
    if cand2.is_dir():
        return cand2.resolve()
    return cand.resolve()

