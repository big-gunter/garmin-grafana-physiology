from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pytz


@dataclass(frozen=True, slots=True)
class InfluxQueryContext:
    influxdbclient: Any


def dt_utc(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def day_bounds_z(date_str: str) -> tuple[str, str]:
    day_start = dt_utc(date_str)
    day_end = day_start + timedelta(days=1)
    return iso_z(day_start), iso_z(day_end)


def range_utc(start_date: str, days: int) -> tuple[str, str]:
    start = dt_utc(start_date)
    end = start + timedelta(days=days)
    return start.isoformat(), end.isoformat()


def query_scalar_influx_v1(q: str, ctx: InfluxQueryContext) -> float | None:
    try:
        res = ctx.influxdbclient.query(q)
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


def query_last_row_influx_v1(q: str, ctx: InfluxQueryContext) -> dict | None:
    try:
        res = ctx.influxdbclient.query(q)
        pts = list(res.get_points())
        return pts[0] if pts else None
    except Exception:
        logging.exception(f"Influx last-row query failed: {q}")
        return None

