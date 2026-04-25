from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytz
from influxdb import InfluxDBClient


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def create_influx_v1_writer(*, host: str, port: int, username: str, password: str, database: str) -> InfluxDBClient:
    client = InfluxDBClient(host=host, port=port, username=username, password=password)
    client.switch_database(database)
    return client


def write_agent_insights(
    *,
    client: InfluxDBClient,
    readiness_score: float,
    window_days: int,
    insights_text: str,
    prompt: str | None,
) -> None:
    point: dict[str, Any] = {
        "measurement": "AgentInsights",
        "time": datetime.now(pytz.utc).isoformat(timespec="seconds"),
        "tags": {
            "source": "ai-agent",
        },
        "fields": {
            "readiness_score": float(readiness_score),
            "window_days": int(window_days),
            "insights": str(insights_text),
            "prompt": "" if prompt is None else str(prompt),
        },
    }
    client.write_points([point])

