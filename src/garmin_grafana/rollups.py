from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

import numpy as np
import pytz


@dataclass(frozen=True, slots=True)
class RollupContext:
    influxdb_version: str
    garmin_devicename: str
    influxdb_database: str
    write_points_to_influxdb: Callable[[list[dict]], None]

    # shared helpers from the original module
    dt_utc: Callable[[str], datetime]
    norm_gender: Callable[[object], str]
    gender_code: Callable[[str], int]

    # physiology helpers
    median_rhr_7d: Callable[[str], float | None]
    estimate_hrmax_activity_backoff: Callable[..., tuple[float | None, str]]
    hrr_zones: Callable[[float, float], dict[str, float]]

    # training load helpers
    get_gender_for_day_v1: Callable[[str], str]
    get_userprofile_master_v1: Callable[[], dict]
    get_physiology_for_day_v1: Callable[[str], tuple[object | None, object | None]]
    stored_lthr_bpm_v1: Callable[[], object | None]
    get_activities_for_day_v1: Callable[[str], list[dict]]
    get_trainingload_prev_day_v1: Callable[[str], dict]

    # performance helpers
    iso_z: Callable[[datetime], str]
    query_scalar_influx_v1: Callable[[str], float | None]
    query_last_row_influx_v1: Callable[[str], dict | None]


def _bannister_trimp(dur_seconds: float, hr_avg: float, rhr: float, hrmax: float, gender: str, norm_gender: Callable[[object], str]) -> float:
    if dur_seconds <= 0 or hr_avg <= 0 or hrmax <= rhr:
        return 0.0
    hrr = hrmax - rhr
    hrratio = (hr_avg - rhr) / hrr
    if hrratio < 0:
        hrratio = 0.0
    elif hrratio > 1:
        hrratio = 1.0

    dur_min = dur_seconds / 60.0

    g = norm_gender(gender)
    if g not in {"male", "female"}:
        return 0.0
    if g == "female":
        k, b = 0.86, 1.67
    else:
        k, b = 0.64, 1.92

    return dur_min * hrratio * k * math.exp(b * hrratio)


def _hr_tss(dur_seconds: float, hr_avg: float, hr_thresh: float) -> float:
    if dur_seconds <= 0 or hr_avg <= 0 or hr_thresh <= 0:
        return 0.0
    if hr_avg < 30:
        return 0.0
    hours = dur_seconds / 3600.0
    IF = hr_avg / hr_thresh
    IF = max(0.3, min(IF, 1.5))
    return 100.0 * hours * (IF**2)


def compute_and_write_training_load(date_str: str, ctx: RollupContext) -> None:
    if ctx.influxdb_version != "1":
        logging.warning("Training load computation currently implemented for InfluxDB v1 only.")
        return

    gender = ctx.norm_gender(ctx.get_gender_for_day_v1(date_str))
    if gender not in {"male", "female"}:
        row = ctx.get_userprofile_master_v1()
        gc = row.get("gender_code")
        try:
            gc_i = int(float(gc)) if gc is not None else 0
        except Exception:
            gc_i = 0
        gender = "male" if gc_i == 1 else "female" if gc_i == 2 else "unknown"

    if gender not in {"male", "female"}:
        logging.info(
            f"TrainingLoadDaily: gender unknown for {date_str}; skipping write (Bannister requires male/female)."
        )
        return

    rhr, hrmax = ctx.get_physiology_for_day_v1(date_str)
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

    lthr_bpm = ctx.stored_lthr_bpm_v1()
    if lthr_bpm is None:
        logging.info(
            f"TrainingLoadDaily: missing LTHR (UserProfileMaster.lthr_bpm); TSS track will be null for {date_str}"
        )

    acts = ctx.get_activities_for_day_v1(date_str)

    trimp_total = 0.0
    tss_total = 0.0
    act_count = 0

    for a in acts:
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

        t = _bannister_trimp(dur_f, hr_f, rhr_f, hrmax_f, gender, ctx.norm_gender)
        if t > 0:
            trimp_total += t
            act_count += 1

        if lthr_bpm is not None:
            try:
                tss_total += _hr_tss(dur_f, hr_f, float(lthr_bpm))
            except Exception:
                pass

    if act_count == 0:
        logging.info(f"TrainingLoadDaily: no usable activities for {date_str}; writing zero-load day")
        trimp_total = 0.0
        tss_total = 0.0

    prev = ctx.get_trainingload_prev_day_v1(date_str)

    a7 = 1.0 - math.exp(-1.0 / 7.0)
    a42 = 1.0 - math.exp(-1.0 / 42.0)

    atl_trimp = trimp_total if prev["atl_trimp"] is None else (prev["atl_trimp"] + a7 * (trimp_total - prev["atl_trimp"]))
    ctl_trimp = trimp_total if prev["ctl_trimp"] is None else (prev["ctl_trimp"] + a42 * (trimp_total - prev["ctl_trimp"]))
    tsb_trimp = ctl_trimp - atl_trimp

    if lthr_bpm is not None:
        atl_tss = tss_total if prev["atl_tss"] is None else (prev["atl_tss"] + a7 * (tss_total - prev["atl_tss"]))
        ctl_tss = tss_total if prev["ctl_tss"] is None else (prev["ctl_tss"] + a42 * (tss_total - prev["ctl_tss"]))
        tsb_tss = ctl_tss - atl_tss
    else:
        atl_tss = ctl_tss = tsb_tss = None

    point = {
        "measurement": "TrainingLoadDaily",
        "time": ctx.dt_utc(date_str).isoformat(),
        "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
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
            "gender_code_used": int(ctx.gender_code(gender)),
            "gender_is_known_used": 1,
            "activities_used": int(act_count),
        },
    }

    ctx.write_points_to_influxdb([point])
    logging.info(
        f"TrainingLoadDaily written for {date_str}: "
        f"TRIMP={trimp_total:.2f}, ATL_TRIMP={atl_trimp:.2f}, CTL_TRIMP={ctl_trimp:.2f}, TSB_TRIMP={tsb_trimp:.2f}; "
        f"TSS={tss_total:.1f}, ATL_TSS={(atl_tss if atl_tss is not None else float('nan')):.1f}, "
        f"CTL_TSS={(ctl_tss if ctl_tss is not None else float('nan')):.1f}, "
        f"TSB_TSS={(tsb_tss if tsb_tss is not None else float('nan')):.1f}; "
        f"acts={act_count}"
    )


def compute_and_write_physiology(asof_date: str, ctx: RollupContext) -> None:
    if ctx.influxdb_version != "1":
        logging.warning("PhysiologyDaily computation currently implemented for InfluxDB v1 only.")
        return

    rhr7 = ctx.median_rhr_7d(asof_date)
    if rhr7 is None:
        logging.info(f"PhysiologyDaily: insufficient RHR data for {asof_date}")
        return

    hrmax_est, hrmax_src = ctx.estimate_hrmax_activity_backoff(asof_date, windows=[42, 84], min_points=5)
    if hrmax_est is None:
        logging.info(f"PhysiologyDaily: no HRmax estimate available for {asof_date} (source={hrmax_src})")
        return

    zones = ctx.hrr_zones(rhr7, float(hrmax_est))

    fields = {
        "HRmax_est": float(hrmax_est),
        "HRmax_est_source": str(hrmax_src),
        "RHR_7d_median": float(rhr7),
        **{k: float(v) for k, v in zones.items()},
    }

    point = {
        "measurement": "PhysiologyDaily",
        "time": ctx.dt_utc(asof_date).isoformat(),
        "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
        "fields": fields,
    }
    ctx.write_points_to_influxdb([point])
    logging.info(
        f"PhysiologyDaily written for {asof_date}: HRmax_est={float(hrmax_est):.1f} ({hrmax_src}), RHR_7d_median={float(rhr7):.1f}"
    )


def fitness_age_from_vo2(vo2: float, gender: str, norm_gender: Callable[[object], str]) -> float | None:
    if vo2 is None or not np.isfinite(vo2) or vo2 <= 0:
        return None
    g = norm_gender(gender)
    if g == "male":
        if vo2 >= 60:
            return 25.0
        if vo2 >= 55:
            return 30.0
        if vo2 >= 50:
            return 35.0
        if vo2 >= 45:
            return 45.0
        if vo2 >= 40:
            return 55.0
        return 65.0
    if g == "female":
        if vo2 >= 55:
            return 25.0
        if vo2 >= 50:
            return 30.0
        if vo2 >= 45:
            return 35.0
        if vo2 >= 40:
            return 45.0
        if vo2 >= 35:
            return 55.0
        return 65.0
    return None


def compute_and_write_performance_daily(asof_date: str, ctx: RollupContext) -> None:
    if ctx.influxdb_version != "1":
        return

    end_dt = ctx.dt_utc(asof_date) + timedelta(days=1)
    start_dt = end_dt - timedelta(days=42)
    start_z, end_z = ctx.iso_z(start_dt), ctx.iso_z(end_dt)

    q_vam20 = (
        'SELECT max("best20m_vam_m_per_h") AS vam20 '
        'FROM "DerivedActivity" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "AND sport_tag =~ /running/ "
        f'AND "Database_Name"=\'{ctx.influxdb_database}\' AND "Device"=\'{ctx.garmin_devicename}\''
    )
    vam20 = ctx.query_scalar_influx_v1(q_vam20)

    q_vam30 = (
        'SELECT max("best30m_vam_m_per_h") AS vam30 '
        'FROM "DerivedActivity" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "AND sport_tag =~ /running/ "
        f'AND "Database_Name"=\'{ctx.influxdb_database}\' AND "Device"=\'{ctx.garmin_devicename}\''
    )
    vam30 = ctx.query_scalar_influx_v1(q_vam30)

    q_run_vo2 = (
        'SELECT max("vo2max_est") AS vo2 '
        'FROM "DerivedActivity" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "AND sport_tag =~ /running/ "
        f'AND "Database_Name"=\'{ctx.influxdb_database}\' AND "Device"=\'{ctx.garmin_devicename}\''
    )
    vo2 = ctx.query_scalar_influx_v1(q_run_vo2)

    q_run_last = (
        "SELECT "
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
        f'AND "Database_Name"=\'{ctx.influxdb_database}\' AND "Device"=\'{ctx.garmin_devicename}\''
    )
    run_last = ctx.query_last_row_influx_v1(q_run_last) or {}

    q_bike_last = (
        "SELECT "
        '  last("cp_watts")     AS cp, '
        '  last("wprime_j")     AS wprime_j, '
        '  last("lthr_bpm_est") AS lthr '
        'FROM "DerivedActivity" '
        f"WHERE time >= '{start_z}' AND time < '{end_z}' "
        "AND sport_tag =~ /cycl|bike|ride/ "
        f'AND "Database_Name"=\'{ctx.influxdb_database}\' AND "Device"=\'{ctx.garmin_devicename}\''
    )
    bike_last = ctx.query_last_row_influx_v1(q_bike_last) or {}

    gender = ctx.get_gender_for_day_v1(asof_date)
    if gender not in {"male", "female"}:
        row = ctx.get_userprofile_master_v1()
        gc = row.get("gender_code")
        try:
            gc_i = int(float(gc)) if gc is not None else 0
        except Exception:
            gc_i = 0
        gender = "male" if gc_i == 1 else "female" if gc_i == 2 else "unknown"

    fa = fitness_age_from_vo2(vo2, gender, ctx.norm_gender) if vo2 is not None else None

    rhr, _ = ctx.get_physiology_for_day_v1(asof_date)
    if fa is not None and rhr is not None:
        try:
            r = float(rhr)
            fa = fa + float(np.clip((r - 50.0) / 5.0, -2.0, 2.0))
        except Exception:
            pass

    def f(x):
        try:
            return float(x) if x is not None else None
        except Exception:
            return None

    point = {
        "measurement": "PerformanceDaily",
        "time": ctx.dt_utc(asof_date).isoformat(),
        "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
        "fields": {
            "VAM20_best_42d_m_per_h": f(vam20),
            "VAM30_best_42d_m_per_h": f(vam30),
            "MountainFitness_source": "DerivedActivity.max(best20m/best30m)_vam_m_per_h over 42d",
            "VO2max_est_run": f(vo2),
            "FitnessAge_model": float(fa) if fa is not None else None,
            "FitnessAge_model_source": "vo2_run(+rhr_adj)" if fa is not None else None,
            "CS_pace_s_per_km": f(run_last.get("cs_pace")),
            "CS_mps": f(run_last.get("cs_mps")),
            "Dprime_m": f(run_last.get("dprime_m")),
            "best5m_vo2_last": f(run_last.get("best5m_vo2_last")),
            "gap_distance_km_last": f(run_last.get("gap_km")),
            "raw_distance_m_from_speed_last": f(run_last.get("raw_dist_m_speed")),
            "raw_distance_m_from_fit_last": f(run_last.get("raw_dist_m_fit")),
            "LTHR_run_bpm_est": f(run_last.get("lthr")),
            "CP_watts": f(bike_last.get("cp")),
            "Wprime_j": f(bike_last.get("wprime_j")),
            "LTHR_bike_bpm_est": f(bike_last.get("lthr")),
        },
    }

    ctx.write_points_to_influxdb([point])

