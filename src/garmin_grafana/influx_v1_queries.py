from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable


@dataclass(frozen=True, slots=True)
class InfluxV1QueryContext:
    influxdbclient: Any
    influxdb_database: str
    garmin_devicename: str
    day_bounds_z: Callable[[str], tuple[str, str]]
    dt_utc: Callable[[str], Any]
    query_last_row: Callable[[str], dict | None]


def get_gender_for_day(date_str: str, ctx: InfluxV1QueryContext) -> str:
    start_z, end_z = ctx.day_bounds_z(date_str)
    q = (
        'SELECT last("gender_code") AS gc '
        'FROM "UserProfile" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "AND gender_is_known = 1 "
        f"AND \"Database_Name\"='{ctx.influxdb_database}' AND \"Device\"='{ctx.garmin_devicename}'"
    )
    row = ctx.query_last_row(q) or {}
    gc = row.get("gc")
    try:
        gc_i = int(float(gc)) if gc is not None else 0
    except Exception:
        gc_i = 0
    return "male" if gc_i == 1 else "female" if gc_i == 2 else "unknown"


def get_userprofile_master(ctx: InfluxV1QueryContext) -> dict:
    q = (
        'SELECT last("age_years") AS age_years, '
        '       last("birth_year") AS birth_year, '
        '       last("gender_code") AS gender_code, '
        '       last("lthr_bpm") AS lthr_bpm '
        'FROM "UserProfileMaster" '
        f"WHERE \"Database_Name\"='{ctx.influxdb_database}' AND \"Device\"='{ctx.garmin_devicename}'"
    )
    return ctx.query_last_row(q) or {}


def stored_age_years(ctx: InfluxV1QueryContext) -> int:
    v = get_userprofile_master(ctx).get("age_years")
    try:
        return int(float(v)) if v is not None else 0
    except Exception:
        return 0


def stored_birth_year(ctx: InfluxV1QueryContext) -> int | None:
    v = get_userprofile_master(ctx).get("birth_year")
    try:
        by = int(float(v)) if v is not None else 0
        return by if by > 0 else None
    except Exception:
        return None


def stored_lthr_bpm(ctx: InfluxV1QueryContext) -> int | None:
    v = get_userprofile_master(ctx).get("lthr_bpm")
    try:
        x = int(float(v)) if v is not None else 0
        return x if x > 0 else None
    except Exception:
        return None


def get_physiology_for_day(date_str: str, ctx: InfluxV1QueryContext) -> tuple[float | None, float | None]:
    start_z, end_z = ctx.day_bounds_z(date_str)
    q = (
        'SELECT last("RHR_7d_median") AS rhr, last("HRmax_est") AS hrmax '
        'FROM "PhysiologyDaily" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f"AND \"Database_Name\"='{ctx.influxdb_database}' AND \"Device\"='{ctx.garmin_devicename}'"
    )
    row = ctx.query_last_row(q) or {}
    rhr = row.get("rhr")
    hrmax = row.get("hrmax")
    return (float(rhr) if rhr is not None else None, float(hrmax) if hrmax is not None else None)


def get_activities_for_day(date_str: str, ctx: InfluxV1QueryContext) -> list[dict]:
    start_z, end_z = ctx.day_bounds_z(date_str)
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
        f"AND \"Database_Name\"='{ctx.influxdb_database}' AND \"Device\"='{ctx.garmin_devicename}' "
        'GROUP BY "Activity_ID"'
    )
    try:
        res = ctx.influxdbclient.query(q)
        return list(res.get_points())
    except Exception:
        logging.exception("ActivitySummary day query failed")
        return []


def get_trainingload_prev_day(date_str: str, ctx: InfluxV1QueryContext) -> dict:
    yday = (ctx.dt_utc(date_str) - timedelta(days=1)).strftime("%Y-%m-%d")
    start_z, end_z = ctx.day_bounds_z(yday)
    q = (
        'SELECT last("ATL_7_TRIMP") AS atl_trimp, last("CTL_42_TRIMP") AS ctl_trimp, '
        '       last("ATL_7_TSS")   AS atl_tss,   last("CTL_42_TSS")   AS ctl_tss '
        'FROM "TrainingLoadDaily" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f"AND \"Database_Name\"='{ctx.influxdb_database}' AND \"Device\"='{ctx.garmin_devicename}'"
    )
    row = ctx.query_last_row(q) or {}

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


def userprofile_exists_for_day(date_str: str, ctx: InfluxV1QueryContext) -> bool:
    try:
        start_z, end_z = ctx.day_bounds_z(date_str)
        q = (
            'SELECT count("gender_is_known") AS c '
            'FROM "UserProfile" '
            f"WHERE time >= '{start_z}' AND time < '{end_z}' "
            f"AND \"Database_Name\"='{ctx.influxdb_database}' AND \"Device\"='{ctx.garmin_devicename}'"
        )
        res = ctx.influxdbclient.query(q)
        pts = list(res.get_points())
        if not pts:
            return False
        c = pts[0].get("c")
        return (c is not None) and (float(c) > 0)
    except Exception:
        logging.exception("UserProfile existence query failed")
        return False


def userprofile_known_for_day(date_str: str, ctx: InfluxV1QueryContext) -> bool:
    try:
        start_z, end_z = ctx.day_bounds_z(date_str)
        q = (
            'SELECT count("gender_is_known") AS c '
            'FROM "UserProfile" '
            f"WHERE time >= '{start_z}' AND time < '{end_z}' "
            'AND "gender_is_known" = 1 '
            f"AND \"Database_Name\"='{ctx.influxdb_database}' AND \"Device\"='{ctx.garmin_devicename}'"
        )
        res = ctx.influxdbclient.query(q)
        pts = list(res.get_points())
        if not pts:
            return False
        c = pts[0].get("c")
        return (c is not None) and (float(c) > 0)
    except Exception:
        logging.exception("UserProfile known-gender existence query failed")
        return False


def get_birth_year_for_day(date_str: str, ctx: InfluxV1QueryContext) -> int | None:
    start_z, end_z = ctx.day_bounds_z(date_str)
    q = (
        'SELECT last("birth_year") '
        'FROM "UserProfile" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f"AND \"Database_Name\"='{ctx.influxdb_database}' AND \"Device\"='{ctx.garmin_devicename}'"
    )
    row = ctx.query_last_row(q) or {}
    v = row.get("last")
    if v is None:
        return None
    try:
        return int(float(v))
    except Exception:
        return None

