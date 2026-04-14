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

def get_hr_zones_for_day(date_str: str, ctx: InfluxV1QueryContext) -> dict[str, float] | None:
    """
    Returns absolute BPM zone bounds from PhysiologyDaily (Z1..Z5).
    Keys: Z1_Low, Z1_High, ..., Z5_Low, Z5_High.
    """
    start_z, end_z = ctx.day_bounds_z(date_str)
    q = (
        "SELECT "
        '  last("Z1_Low")  AS Z1_Low,  last("Z1_High") AS Z1_High, '
        '  last("Z2_Low")  AS Z2_Low,  last("Z2_High") AS Z2_High, '
        '  last("Z3_Low")  AS Z3_Low,  last("Z3_High") AS Z3_High, '
        '  last("Z4_Low")  AS Z4_Low,  last("Z4_High") AS Z4_High, '
        '  last("Z5_Low")  AS Z5_Low,  last("Z5_High") AS Z5_High '
        'FROM "PhysiologyDaily" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f"AND \"Database_Name\"='{ctx.influxdb_database}' AND \"Device\"='{ctx.garmin_devicename}'"
    )
    row = ctx.query_last_row(q) or {}

    def f(k: str) -> float | None:
        v = row.get(k)
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    out = {k: f(k) for k in (
        "Z1_Low","Z1_High","Z2_Low","Z2_High","Z3_Low","Z3_High","Z4_Low","Z4_High","Z5_Low","Z5_High"
    )}
    if any(v is None for v in out.values()):
        return None
    return {k: float(v) for k, v in out.items() if v is not None}


def get_activity_gps_points_for_day(date_str: str, activity_id: int, ctx: InfluxV1QueryContext) -> list[dict]:
    """
    Returns raw ActivityGPS points for a given day + Activity_ID.
    Expected fields: HeartRate, Power, Speed, GradeAdjustedSpeed, time.
    """
    start_z, end_z = ctx.day_bounds_z(date_str)
    q = (
        "SELECT "
        '  "HeartRate" AS HeartRate, '
        '  "Power" AS Power, '
        '  "Speed" AS Speed, '
        '  "GradeAdjustedSpeed" AS GradeAdjustedSpeed '
        'FROM "ActivityGPS" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f"AND \"Activity_ID\" = {int(activity_id)} "
        f"AND \"Database_Name\"='{ctx.influxdb_database}' AND \"Device\"='{ctx.garmin_devicename}'"
    )
    try:
        res = ctx.influxdbclient.query(q)
        return list(res.get_points())
    except Exception:
        logging.exception(f"ActivityGPS query failed for Activity_ID={activity_id} day={date_str}")
        return []


def get_derived_activity_thresholds_for_day(date_str: str, activity_id: int, ctx: InfluxV1QueryContext) -> dict:
    """
    Returns thresholds derived during FIT parsing for the activity (if present):
      - cs_mps (running critical speed from GAP)
      - cp_watts (cycling critical power)
    """
    start_z, end_z = ctx.day_bounds_z(date_str)
    q = (
        "SELECT "
        '  last("cs_mps") AS cs_mps, '
        '  last("cp_watts") AS cp_watts '
        'FROM "DerivedActivity" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f"AND \"ActivityID\"='{int(activity_id)}' "
        f"AND \"Database_Name\"='{ctx.influxdb_database}' AND \"Device\"='{ctx.garmin_devicename}'"
    )
    return ctx.query_last_row(q) or {}


def get_derived_activity_loads_for_day(date_str: str, activity_id: int, ctx: InfluxV1QueryContext) -> dict:
    """
    Returns per-activity training load fields computed during FIT parsing (if present).
    """
    start_z, end_z = ctx.day_bounds_z(date_str)
    q = (
        "SELECT "
        '  last("TRIMP_Banister_ts") AS TRIMP_Banister_ts, '
        '  last("TRIMP_Edwards_ts")  AS TRIMP_Edwards_ts, '
        '  last("hrTSS_ts")          AS hrTSS_ts, '
        '  last("rTSS_ts")           AS rTSS_ts, '
        '  last("bikeTSS_ts")        AS bikeTSS_ts '
        'FROM "DerivedActivity" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        f"AND \"ActivityID\"='{int(activity_id)}' "
        f"AND \"Database_Name\"='{ctx.influxdb_database}' AND \"Device\"='{ctx.garmin_devicename}'"
    )
    return ctx.query_last_row(q) or {}


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

