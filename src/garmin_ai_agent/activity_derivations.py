from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True, slots=True)
class DerivedActivityMetrics:
    activity_id: str
    sport_tag: str | None
    activity_type: str | None
    start_time_utc: str
    fields: dict[str, Any]


def _to_dt_utc(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=True)


def _rolling_best_mean(values: np.ndarray, times_s: np.ndarray, window_s: float) -> tuple[float | None, int | None, int | None]:
    """
    Variable-dt best mean over a window, using simple per-sample dt weights.
    Returns (best_mean, start_idx, end_idx).
    """
    if values.size < 2 or times_s.size != values.size:
        return (None, None, None)
    v = np.asarray(values, dtype=float)
    t = np.asarray(times_s, dtype=float)
    m = np.isfinite(v) & np.isfinite(t)
    if m.sum() < 2:
        return (None, None, None)
    v = v[m]
    t = t[m]
    # ensure sorted by time
    order = np.argsort(t)
    v = v[order]
    t = t[order]
    dt = np.diff(t)
    dt = np.where(np.isfinite(dt) & (dt > 0), dt, 0.0)
    if not np.any(dt > 0):
        return (None, None, None)
    # extend last dt with median dt
    med_dt = float(np.median(dt[dt > 0]))
    dt = np.r_[dt, med_dt]

    best = None
    best_s = None
    best_e = None
    j = 0
    acc_t = 0.0
    acc_vt = 0.0
    for i in range(v.size):
        # add i
        acc_t += float(dt[i])
        acc_vt += float(v[i]) * float(dt[i])
        # shrink from left
        while j < i and acc_t - float(dt[j]) >= window_s:
            acc_t -= float(dt[j])
            acc_vt -= float(v[j]) * float(dt[j])
            j += 1
        if acc_t >= window_s * 0.98:  # allow small dt rounding error
            mean = acc_vt / acc_t if acc_t > 0 else np.nan
            if np.isfinite(mean) and (best is None or mean > best):
                best = float(mean)
                best_s = int(j)
                best_e = int(i)
    return (best, best_s, best_e)


def _grade_from_dist_alt(dist_m: np.ndarray, alt_m: np.ndarray) -> np.ndarray:
    d = np.asarray(dist_m, dtype=float)
    a = np.asarray(alt_m, dtype=float)
    n = min(d.size, a.size)
    d = d[:n]
    a = a[:n]
    # enforce monotonic distance when possible
    d = np.where(np.isfinite(d), d, np.nan)
    if np.isfinite(d).any():
        d = np.maximum.accumulate(np.nan_to_num(d, nan=-np.inf))
    # derivative
    dd = np.diff(d)
    da = np.diff(a)
    dd = np.where(np.isfinite(dd) & (dd > 0), dd, np.nan)
    g = np.where(np.isfinite(dd), da / dd, np.nan)
    # pad to length n
    g = np.r_[g, np.nan]
    # smooth a bit (median filter-ish via rolling mean)
    s = pd.Series(g).rolling(9, min_periods=3, center=True).mean().to_numpy()
    return np.clip(s, -0.35, 0.35)


def _acsm_vo2_running(speed_mps: np.ndarray, grade: np.ndarray) -> np.ndarray:
    v = np.asarray(speed_mps, dtype=float)
    g = np.asarray(grade, dtype=float)
    v = np.where(np.isfinite(v) & (v > 0), v, np.nan)
    g = np.where(np.isfinite(g), g, 0.0)
    # avoid downhill inflating "easy" VO2; clamp at 0 for demand proxy
    g = np.clip(g, 0.0, 0.35)
    v_m_min = v * 60.0
    vo2 = 0.2 * v_m_min + 0.9 * v_m_min * g + 3.5
    return np.where(np.isfinite(vo2), np.clip(vo2, 0.0, 90.0), np.nan)


def _banister_trimp_series(dt_s: np.ndarray, hr: np.ndarray, rhr: float, hrmax: float, gender: str) -> float | None:
    if hrmax <= rhr:
        return None
    h = np.asarray(hr, dtype=float)
    dt = np.asarray(dt_s, dtype=float)
    m = np.isfinite(h) & np.isfinite(dt) & (dt > 0) & (h > 0)
    if not m.any():
        return None
    h = h[m]
    dt = dt[m]
    hrr = (h - float(rhr)) / (float(hrmax) - float(rhr))
    hrr = np.clip(hrr, 0.0, 1.2)
    if gender == "female":
        y = 0.86 * np.exp(1.67 * hrr)
    else:
        y = 0.64 * np.exp(1.92 * hrr)
    # TRIMP = sum(minutes * HRR * y)
    minutes = dt / 60.0
    return float(np.sum(minutes * hrr * y))


def _edwards_trimp_series(dt_s: np.ndarray, hr: np.ndarray, hrmax: float) -> float | None:
    h = np.asarray(hr, dtype=float)
    dt = np.asarray(dt_s, dtype=float)
    m = np.isfinite(h) & np.isfinite(dt) & (dt > 0) & (h > 0)
    if not m.any() or hrmax <= 0:
        return None
    h = h[m]
    dt = dt[m]
    z = h / float(hrmax)
    # zones (%HRmax): 50-60,60-70,70-80,80-90,90-100
    weights = np.zeros_like(z)
    weights[(z >= 0.50) & (z < 0.60)] = 1
    weights[(z >= 0.60) & (z < 0.70)] = 2
    weights[(z >= 0.70) & (z < 0.80)] = 3
    weights[(z >= 0.80) & (z < 0.90)] = 4
    weights[z >= 0.90] = 5
    minutes = dt / 60.0
    return float(np.sum(minutes * weights))


def derive_activity_metrics_from_streams(
    *,
    activity_id: str,
    sport_tag: str | None,
    activity_type: str | None,
    start_time_utc: str,
    df_stream: pd.DataFrame,
    hrmax_bpm: float | None,
    rhr_bpm: float | None,
    gender: str | None,
    weight_kg: float | None,
    cycling_gross_eff: float = 0.23,
) -> DerivedActivityMetrics:
    """
    Compute VO2max estimate and TRIMP from raw Garmin-imported activity streams (ActivityGPS).
    """
    fields: dict[str, Any] = {}
    d = df_stream.copy()
    if d.empty:
        return DerivedActivityMetrics(activity_id=activity_id, sport_tag=sport_tag, activity_type=activity_type, start_time_utc=start_time_utc, fields=fields)

    if "time" in d.columns:
        d["time"] = _to_dt_utc(d["time"])
        d = d.dropna(subset=["time"]).sort_values("time")
        t_s = d["time"].astype("int64").to_numpy(dtype=float) / 1e9
    else:
        t_s = np.arange(len(d), dtype=float)

    # dt
    dt = np.diff(t_s)
    dt = np.where(np.isfinite(dt) & (dt > 0), dt, 0.0)
    if dt.size:
        med_dt = float(np.median(dt[dt > 0])) if np.any(dt > 0) else 1.0
    else:
        med_dt = 1.0
    dt = np.r_[dt, med_dt]

    hr = d["HeartRate"].to_numpy(dtype=float) if "HeartRate" in d.columns else np.array([], dtype=float)
    speed = d["Speed"].to_numpy(dtype=float) if "Speed" in d.columns else np.array([], dtype=float)
    dist = d["Distance"].to_numpy(dtype=float) if "Distance" in d.columns else np.array([], dtype=float)
    alt = d["Altitude"].to_numpy(dtype=float) if "Altitude" in d.columns else np.array([], dtype=float)
    power = d["Power"].to_numpy(dtype=float) if "Power" in d.columns else np.array([], dtype=float)

    # VO2max estimate (running only; needs speed+grade)
    is_run = (sport_tag or "").lower().startswith("running") or (activity_type or "").lower().find("run") >= 0
    if is_run and speed.size and dist.size and alt.size:
        grade = _grade_from_dist_alt(dist, alt)
        vo2 = _acsm_vo2_running(speed, grade)
        best5, s_i, e_i = _rolling_best_mean(vo2, t_s, window_s=300.0)
        fields["vo2_demand_best5m"] = best5
        # Estimate VO2max from demand / relative intensity using HR fraction of HRmax in the same window
        if best5 is not None and s_i is not None and e_i is not None and hr.size:
            hr_win = hr[s_i : e_i + 1]
            hr_win = hr_win[np.isfinite(hr_win) & (hr_win > 0)]
            hr_med = float(np.median(hr_win)) if hr_win.size else None
            fields["hr_best5m_median_bpm"] = hr_med
            if hr_med is not None and hrmax_bpm is not None and hrmax_bpm > 0:
                frac = float(np.clip(hr_med / float(hrmax_bpm), 0.30, 1.05))
                fields["vo2max_est"] = float(best5) / frac if frac > 0 else None
                fields["vo2max_est_method"] = "ACSM(grade+speed) best5m / (HR_best5m/HRmax)"
            else:
                fields["vo2max_est"] = None
                fields["vo2max_est_method"] = "insufficient_HRmax"

    # VO2max estimate (cycling): from raw power + assumed gross efficiency + weight.
    # Convert mechanical watts -> metabolic VO2 using ~20.9 kJ per liter O2.
    is_ride = (sport_tag or "").lower().find("cycling") >= 0 or (activity_type or "").lower().find("cycle") >= 0 or (activity_type or "").lower().find("ride") >= 0
    if is_ride and power.size and weight_kg is not None and weight_kg > 0:
        eff = float(cycling_gross_eff) if cycling_gross_eff and cycling_gross_eff > 0 else 0.23
        eff = float(np.clip(eff, 0.15, 0.30))
        w = np.asarray(power, dtype=float)
        w = np.where(np.isfinite(w) & (w > 0), w, np.nan)
        # metabolic watts
        met_w = w / eff
        # L/min = (J/s * 60 s/min) / (20900 J/L)
        vo2_l_min = (met_w * 60.0) / 20900.0
        vo2_ml_kg_min = (vo2_l_min * 1000.0) / float(weight_kg)
        best5_cyc, s_i, e_i = _rolling_best_mean(vo2_ml_kg_min, t_s, window_s=300.0)
        fields["vo2_demand_best5m_cycling"] = best5_cyc
        fields["cycling_gross_eff_used"] = eff
        fields["weight_kg_used"] = float(weight_kg)
        if best5_cyc is not None and s_i is not None and e_i is not None and hr.size:
            hr_win = hr[s_i : e_i + 1]
            hr_win = hr_win[np.isfinite(hr_win) & (hr_win > 0)]
            hr_med = float(np.median(hr_win)) if hr_win.size else None
            fields["hr_best5m_cycling_median_bpm"] = hr_med
            if hr_med is not None and hrmax_bpm is not None and hrmax_bpm > 0:
                frac = float(np.clip(hr_med / float(hrmax_bpm), 0.30, 1.05))
                fields["vo2max_est_cycling"] = float(best5_cyc) / frac if frac > 0 else None
                fields["vo2max_est_cycling_method"] = "Power/eff->VO2 best5m / (HR_best5m/HRmax)"
            else:
                fields["vo2max_est_cycling"] = None
                fields["vo2max_est_cycling_method"] = "insufficient_HRmax"

        # Cycling load metrics (TSS-style) using power time series.
        # This is intentionally self-contained per activity, so it can be written to AgentDerivedActivity
        # and visualized in Grafana without additional configuration.
        try:
            p = np.asarray(power, dtype=float)
            p = np.where(np.isfinite(p) & (p > 0), p, 0.0)  # coasting/missing as 0W
            # FTP proxy from this activity (best 20-min mean)
            p20, _, _ = _rolling_best_mean(p, t_s, window_s=1200.0)
            ftp_w = float(p20) * 0.95 if (p20 is not None and np.isfinite(p20) and p20 > 0) else None
            fields["ftp_est_w_activity"] = ftp_w
            fields["ftp_est_method_activity"] = "0.95 * best 20-min mean power (within activity)"

            # Normalized Power (NP): 30s rolling mean, 4th-power average, 4th-root
            if p.size >= 10 and np.any(p > 0):
                # approximate dt-weighting by resampling to median dt via rolling on samples
                # Use time-based rolling via pandas over the existing sample spacing.
                ps = pd.Series(p)
                roll = ps.rolling(window=30, min_periods=10, center=True).mean()
                p30 = roll.to_numpy(dtype=float)
                p30 = np.where(np.isfinite(p30) & (p30 > 0), p30, 0.0)
                np_w = float(np.power(np.mean(np.power(p30, 4.0)), 0.25)) if np.any(p30 > 0) else None
                fields["np_w"] = np_w

                if ftp_w is not None and np_w is not None and ftp_w > 0:
                    intensity = float(np_w) / float(ftp_w)
                    fields["if"] = float(np.clip(intensity, 0.0, 2.0))
                    dur_s = float(fields.get("duration_s_stream") or np.sum(dt[np.isfinite(dt) & (dt > 0)]))
                    # Standard cycling TSS definition:
                    # TSS = (sec * NP * IF) / (FTP * 3600) * 100
                    fields["tss"] = float((dur_s * float(np_w) * float(fields["if"])) / (float(ftp_w) * 3600.0) * 100.0) if dur_s > 0 else None
        except Exception:
            pass

    # TRIMP (raw HR time series)
    if hr.size and hrmax_bpm is not None and rhr_bpm is not None:
        g = (gender or "male").strip().lower()
        if g not in {"male", "female"}:
            g = "male"
        fields["trimp_banister"] = _banister_trimp_series(dt, hr, float(rhr_bpm), float(hrmax_bpm), g)
        fields["trimp_edwards"] = _edwards_trimp_series(dt, hr, float(hrmax_bpm))

    # Basic totals
    if dt.size:
        fields["duration_s_stream"] = float(np.sum(dt[np.isfinite(dt) & (dt > 0)]))
    if dist.size and np.isfinite(dist).any():
        fields["distance_m_stream_end"] = float(np.nanmax(dist))

    return DerivedActivityMetrics(
        activity_id=str(activity_id),
        sport_tag=sport_tag,
        activity_type=activity_type,
        start_time_utc=start_time_utc,
        fields=fields,
    )

