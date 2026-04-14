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
    get_hr_zones_for_day_v1: Callable[[str], dict[str, float] | None]
    get_activity_gps_points_for_day_v1: Callable[[str, int], list[dict]]
    get_derived_activity_thresholds_for_day_v1: Callable[[str, int], dict]
    get_derived_activity_loads_for_day_v1: Callable[[str, int], dict]

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

def _edwards_zone_weight(hr_bpm: float, zones: dict[str, float]) -> int:
    """
    Edwards TRIMP uses integer weights 1..5 based on HR zones.
    Here we map HR to your PhysiologyDaily Z1..Z5 absolute BPM boundaries.
    """
    try:
        hr = float(hr_bpm)
    except Exception:
        return 0
    if not (hr > 0):
        return 0

    z1l, z1h = zones["Z1_Low"], zones["Z1_High"]
    z2l, z2h = zones["Z2_Low"], zones["Z2_High"]
    z3l, z3h = zones["Z3_Low"], zones["Z3_High"]
    z4l, z4h = zones["Z4_Low"], zones["Z4_High"]
    z5l, z5h = zones["Z5_Low"], zones["Z5_High"]

    if hr < z1l:
        return 0
    if hr <= z1h:
        return 1
    if hr <= z2h and hr >= z2l:
        return 2
    if hr <= z3h and hr >= z3l:
        return 3
    if hr <= z4h and hr >= z4l:
        return 4
    # Above Z5_Low counts as zone 5 (cap)
    if hr >= z5l:
        return 5
    return 0


def _bin_time_series(ts_s: np.ndarray, values: np.ndarray, bin_s: float) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Returns (bin_dur_s, bin_mean, bin_has_data).
    Assumes ts_s is monotonic increasing (seconds).
    """
    if ts_s.size < 2:
        return (np.array([], dtype=float), np.array([], dtype=float), np.array([], dtype=bool))
    t0 = float(ts_s[0])
    rel = ts_s - t0
    b = np.floor(rel / float(bin_s)).astype(int)
    nb = int(b.max()) + 1 if b.size else 0
    if nb <= 0:
        return (np.array([], dtype=float), np.array([], dtype=float), np.array([], dtype=bool))

    # dt per sample
    dt = np.diff(ts_s)
    dt = np.where(np.isfinite(dt) & (dt > 0), dt, 0.0)
    med_dt = float(np.median(dt[dt > 0])) if np.any(dt > 0) else 1.0
    dt = np.r_[dt, med_dt]  # last sample

    bin_dur = np.zeros(nb, dtype=float)
    bin_sum = np.zeros(nb, dtype=float)
    bin_cnt = np.zeros(nb, dtype=float)
    for i in range(values.size):
        bi = int(b[i])
        if bi < 0 or bi >= nb:
            continue
        di = float(dt[i])
        if di <= 0:
            continue
        bin_dur[bi] += di
        v = values[i]
        if np.isfinite(v):
            # time-weighted mean
            bin_sum[bi] += float(v) * di
            bin_cnt[bi] += di

    mean = np.where(bin_cnt > 0, bin_sum / bin_cnt, np.nan)
    has = bin_cnt > 0
    return (bin_dur, mean, has)


def _np_from_power_bins(bin_dur_s: np.ndarray, bin_power_w: np.ndarray) -> float | None:
    """
    Approximate Normalized Power from fixed-duration bins:
      NP ≈ ( time-weighted mean( P^4 ) )^(1/4)
    This skips the canonical 30s rolling average step; with 30s bins it is a close proxy.
    """
    if bin_dur_s.size == 0 or bin_power_w.size == 0:
        return None
    dur = np.asarray(bin_dur_s, dtype=float)
    p = np.asarray(bin_power_w, dtype=float)
    m = np.isfinite(dur) & (dur > 0) & np.isfinite(p) & (p >= 0)
    if not m.any():
        return None
    w = dur[m]
    x = p[m] ** 4
    try:
        mean4 = float(np.sum(x * w) / np.sum(w))
        return float(mean4 ** 0.25) if mean4 > 0 else 0.0
    except Exception:
        return None


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

    zones = ctx.get_hr_zones_for_day_v1(date_str)
    zones_source = "PhysiologyDaily(Z1..Z5)"
    if zones is None:
        # Fallback to canonical Edwards (%HRmax) zones.
        # zone 1: 50-60% HRmax, ..., zone 5: 90-100% HRmax
        zones = {
            "Z1_Low": 0.50 * hrmax_f, "Z1_High": 0.60 * hrmax_f,
            "Z2_Low": 0.60 * hrmax_f, "Z2_High": 0.70 * hrmax_f,
            "Z3_Low": 0.70 * hrmax_f, "Z3_High": 0.80 * hrmax_f,
            "Z4_Low": 0.80 * hrmax_f, "Z4_High": 0.90 * hrmax_f,
            "Z5_Low": 0.90 * hrmax_f, "Z5_High": 1.00 * hrmax_f,
        }
        zones_source = "%HRmax(Edwards)"
        logging.info(f"TrainingLoadDaily: missing PhysiologyDaily HR zones for {date_str}; using {zones_source} fallback")

    acts = ctx.get_activities_for_day_v1(date_str)

    trimp_banister_total = 0.0
    trimp_edwards_total = 0.0
    tss_hr_total = 0.0
    tss_run_total = 0.0
    tss_bike_total = 0.0
    act_count = 0
    act_used_ts = 0

    for a in acts:
        if str(a.get("activityName", "")).upper() == "END":
            continue

        dur = a.get("elapsedDuration")
        hr = a.get("averageHR")
        act_id = a.get("Activity_ID")
        if dur is None or hr is None or act_id is None:
            continue

        try:
            dur_f = float(dur)
            hr_f = float(hr)
            act_id_i = int(float(act_id))
        except Exception:
            continue

        # Prefer DerivedActivity per-activity computed loads (computed at FIT parse time)
        da_loads = ctx.get_derived_activity_loads_for_day_v1(date_str, act_id_i) or {}
        def _ff(k: str) -> float | None:
            v = da_loads.get(k)
            try:
                return float(v) if v is not None else None
            except Exception:
                return None

        b_da = _ff("TRIMP_Banister_ts")
        e_da = _ff("TRIMP_Edwards_ts")
        hr_da = _ff("hrTSS_ts")
        r_da = _ff("rTSS_ts")
        bike_da = _ff("bikeTSS_ts")

        if b_da is not None or e_da is not None or hr_da is not None or r_da is not None or bike_da is not None:
            if b_da is not None and b_da > 0:
                trimp_banister_total += float(b_da)
            if e_da is not None and e_da > 0:
                trimp_edwards_total += float(e_da)
            if hr_da is not None and hr_da > 0:
                tss_hr_total += float(hr_da)
            if r_da is not None and r_da > 0:
                tss_run_total += float(r_da)
            if bike_da is not None and bike_da > 0:
                tss_bike_total += float(bike_da)
            act_count += 1
            act_used_ts += 1
            continue

        # Prefer time-series computations when ActivityGPS is available.
        points = ctx.get_activity_gps_points_for_day_v1(date_str, act_id_i)
        used_ts = False
        if points and len(points) >= 60:
            try:
                # extract arrays
                ts_s = []
                hr_s = []
                pwr_s = []
                spd_s = []
                gap_s = []
                for p in points:
                    t = p.get("time")
                    if t is None:
                        continue
                    # Influx returns RFC3339 strings; datetime parsing is expensive.
                    # Use pandas-like parsing avoidance by relying on numpy datetime64.
                    try:
                        ts = np.datetime64(t).astype("datetime64[ns]").astype(np.int64) / 1e9
                    except Exception:
                        continue
                    ts_s.append(float(ts))
                    hr_s.append(p.get("HeartRate"))
                    pwr_s.append(p.get("Power"))
                    spd_s.append(p.get("Speed"))
                    gap_s.append(p.get("GradeAdjustedSpeed"))

                if len(ts_s) >= 60:
                    ts = np.asarray(ts_s, dtype=float)
                    order = np.argsort(ts)
                    ts = ts[order]
                    # de-dup
                    if ts.size > 1:
                        keep = np.r_[True, np.diff(ts) > 0]
                    else:
                        keep = np.array([True])
                    ts = ts[keep]

                    hr_arr = np.asarray([np.nan if v is None else float(v) for v in np.asarray(hr_s, dtype=object)[order][keep]], dtype=float)
                    pwr_arr = np.asarray([np.nan if v is None else float(v) for v in np.asarray(pwr_s, dtype=object)[order][keep]], dtype=float)
                    spd_arr = np.asarray([np.nan if v is None else float(v) for v in np.asarray(spd_s, dtype=object)[order][keep]], dtype=float)
                    gap_arr = np.asarray([np.nan if v is None else float(v) for v in np.asarray(gap_s, dtype=object)[order][keep]], dtype=float)

                    # fill GAP from Speed if missing
                    gap_use = np.where(np.isfinite(gap_arr) & (gap_arr > 0), gap_arr, spd_arr)

                    bin_s = float(np.clip(float(getattr(ctx, "training_load_bin_seconds", 30.0) if hasattr(ctx, "training_load_bin_seconds") else 30.0), 10.0, 120.0))
                    bin_dur, bin_hr, hr_has = _bin_time_series(ts, hr_arr, bin_s=bin_s)

                    if bin_dur.size and np.isfinite(bin_hr).any():
                        # Banister TRIMP summed per-bin
                        t_b = 0.0
                        for dsec, h in zip(bin_dur, bin_hr):
                            if not (np.isfinite(dsec) and dsec > 0 and np.isfinite(h) and h > 0):
                                continue
                            t_b += _bannister_trimp(float(dsec), float(h), rhr_f, hrmax_f, gender, ctx.norm_gender)
                        if t_b > 0:
                            trimp_banister_total += float(t_b)

                        # Edwards TRIMP (zone-weighted minutes)
                        if zones is not None:
                            t_e = 0.0
                            for dsec, h in zip(bin_dur, bin_hr):
                                if not (np.isfinite(dsec) and dsec > 0 and np.isfinite(h) and h > 0):
                                    continue
                                w = _edwards_zone_weight(float(h), zones)
                                if w <= 0:
                                    continue
                                t_e += (float(dsec) / 60.0) * float(w)
                            if t_e > 0:
                                trimp_edwards_total += float(t_e)

                        # hrTSS (windowed)
                        if lthr_bpm is not None:
                            try:
                                thr = float(lthr_bpm)
                                t_hr = 0.0
                                for dsec, h in zip(bin_dur, bin_hr):
                                    if not (np.isfinite(dsec) and dsec > 0 and np.isfinite(h) and h > 0):
                                        continue
                                    t_hr += _hr_tss(float(dsec), float(h), thr)
                                tss_hr_total += float(t_hr)
                            except Exception:
                                pass

                        # rTSS (GAP speed vs CS)
                        thr_row = ctx.get_derived_activity_thresholds_for_day_v1(date_str, act_id_i) or {}
                        cs = thr_row.get("cs_mps")
                        try:
                            cs_f = float(cs) if cs is not None else np.nan
                        except Exception:
                            cs_f = np.nan
                        if np.isfinite(cs_f) and cs_f > 0:
                            _, bin_gap, _ = _bin_time_series(ts, gap_use, bin_s=bin_s)
                            t_run = 0.0
                            for dsec, v in zip(bin_dur, bin_gap):
                                if not (np.isfinite(dsec) and dsec > 0 and np.isfinite(v) and v > 0):
                                    continue
                                hours = float(dsec) / 3600.0
                                IF = float(v) / float(cs_f)
                                IF = max(0.3, min(IF, 1.5))
                                t_run += 100.0 * hours * (IF ** 2)
                            tss_run_total += float(t_run)

                        # bike TSS (NP proxy vs CP)
                        cp = thr_row.get("cp_watts")
                        try:
                            cp_f = float(cp) if cp is not None else np.nan
                        except Exception:
                            cp_f = np.nan
                        if np.isfinite(cp_f) and cp_f > 0:
                            _, bin_p, _ = _bin_time_series(ts, pwr_arr, bin_s=bin_s)
                            np_est = _np_from_power_bins(bin_dur, bin_p)
                            if np_est is not None and np.isfinite(np_est):
                                total_hours = float(np.sum(bin_dur)) / 3600.0
                                IF = float(np_est) / float(cp_f)
                                IF = max(0.3, min(IF, 1.5))
                                tss_bike_total += 100.0 * total_hours * (IF ** 2)

                        used_ts = True
            except Exception:
                logging.exception(f"TrainingLoadDaily: time-series load failed for Activity_ID={act_id_i}")

        if not used_ts:
            # Fallback: summary-based
            t = _bannister_trimp(dur_f, hr_f, rhr_f, hrmax_f, gender, ctx.norm_gender)
            if t > 0:
                trimp_banister_total += t

            if zones is not None:
                # crude Edwards fallback: weight from avgHR only
                w = _edwards_zone_weight(hr_f, zones)
                if w > 0:
                    trimp_edwards_total += (dur_f / 60.0) * float(w)

            if lthr_bpm is not None:
                try:
                    tss_hr_total += _hr_tss(dur_f, hr_f, float(lthr_bpm))
                except Exception:
                    pass

        act_count += 1
        if used_ts:
            act_used_ts += 1

    if act_count == 0:
        logging.info(f"TrainingLoadDaily: no usable activities for {date_str}; writing zero-load day")
        trimp_banister_total = 0.0
        trimp_edwards_total = 0.0
        tss_hr_total = 0.0
        tss_run_total = 0.0
        tss_bike_total = 0.0

    prev = ctx.get_trainingload_prev_day_v1(date_str)

    a7 = 1.0 - math.exp(-1.0 / 7.0)
    a42 = 1.0 - math.exp(-1.0 / 42.0)

    atl_trimp = trimp_banister_total if prev["atl_trimp"] is None else (prev["atl_trimp"] + a7 * (trimp_banister_total - prev["atl_trimp"]))
    ctl_trimp = trimp_banister_total if prev["ctl_trimp"] is None else (prev["ctl_trimp"] + a42 * (trimp_banister_total - prev["ctl_trimp"]))
    tsb_trimp = ctl_trimp - atl_trimp

    if lthr_bpm is not None:
        atl_tss = tss_hr_total if prev["atl_tss"] is None else (prev["atl_tss"] + a7 * (tss_hr_total - prev["atl_tss"]))
        ctl_tss = tss_hr_total if prev["ctl_tss"] is None else (prev["ctl_tss"] + a42 * (tss_hr_total - prev["ctl_tss"]))
        tsb_tss = ctl_tss - atl_tss
    else:
        atl_tss = ctl_tss = tsb_tss = None

    point = {
        "measurement": "TrainingLoadDaily",
        "time": ctx.dt_utc(date_str).isoformat(),
        "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
        "fields": {
            # Back-compat fields
            "TRIMP": float(trimp_banister_total),
            "TSS": float(tss_hr_total) if lthr_bpm is not None else None,

            # Explicit standards/variants
            "TRIMP_Banister": float(trimp_banister_total),
            "TRIMP_Edwards": float(trimp_edwards_total) if zones is not None else None,
            "TRIMP_Edwards_zones_source": str(zones_source) if zones is not None else None,
            "hrTSS": float(tss_hr_total) if lthr_bpm is not None else None,
            "rTSS": float(tss_run_total) if tss_run_total > 0 else None,
            "bikeTSS": float(tss_bike_total) if tss_bike_total > 0 else None,

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
            "activities_used_timeseries": int(act_used_ts),
        },
    }

    ctx.write_points_to_influxdb([point])
    logging.info(
        f"TrainingLoadDaily written for {date_str}: "
        f"bTRIMP={trimp_banister_total:.2f}, eTRIMP={trimp_edwards_total:.2f}, "
        f"ATL_TRIMP={atl_trimp:.2f}, CTL_TRIMP={ctl_trimp:.2f}, TSB_TRIMP={tsb_trimp:.2f}; "
        f"hrTSS={tss_hr_total:.1f}, rTSS={tss_run_total:.1f}, bikeTSS={tss_bike_total:.1f}, "
        f"ATL_TSS={(atl_tss if atl_tss is not None else float('nan')):.1f}, "
        f"CTL_TSS={(ctl_tss if ctl_tss is not None else float('nan')):.1f}, "
        f"TSB_TSS={(tsb_tss if tsb_tss is not None else float('nan')):.1f}; "
        f"acts={act_count}, ts_acts={act_used_ts}"
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

