from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd

from .influx_ro import InfluxRO, query_influxql_df


@dataclass(frozen=True, slots=True)
class Snapshot:
    generated_at_utc: str
    window_days: int
    metrics: dict[str, Any]
    debug: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_float(x: Any) -> float | None:
    try:
        f = float(x)
        return f if np.isfinite(f) else None
    except Exception:
        return None


def _last_value(df: pd.DataFrame, col: str) -> float | None:
    if df is None or df.empty or col not in df.columns:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.iloc[-1])


def _mean_value(df: pd.DataFrame, col: str) -> float | None:
    if df is None or df.empty or col not in df.columns:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.mean())


def _std_value(df: pd.DataFrame, col: str) -> float | None:
    if df is None or df.empty or col not in df.columns:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if s.size < 2:
        return None
    return float(s.std(ddof=1))


def build_snapshot(ro: InfluxRO, *, window_days: int = 42) -> Snapshot:
    """
    Produces a physiology/training-load snapshot from the DB (Influx v1).

    This intentionally queries the DB directly rather than relying on any in-process
    computed values from fetch scripts.
    """
    now = _utc_now()
    since = now - timedelta(days=int(window_days))
    since_iso = _iso(since)

    # IMPORTANT constraint: only use fields directly imported from Garmin.
    # Do NOT use rollups/derived measurements such as PhysiologyDaily, TrainingLoadDaily, DerivedActivity.
    #
    # Raw Garmin-imported sources we rely on:
    # - DailyStats: restingHeartRate, stress, body battery, activity seconds, etc.
    # - SleepSummary: sleepScore, avgOvernightHrv, restingHeartRate, sleepTimeSeconds, etc.
    # - HRV_Intraday: hrvValue (time series)
    # - ActivitySummary: movingDuration, distance, averageHR, etc.

    q_daily = f'SELECT * FROM "DailyStats" WHERE time >= \'{since_iso}\' ORDER BY time ASC'
    q_sleep = f'SELECT * FROM "SleepSummary" WHERE time >= \'{since_iso}\' ORDER BY time ASC'
    q_hrv_i = f'SELECT * FROM "HRV_Intraday" WHERE time >= \'{since_iso}\' ORDER BY time ASC'
    q_act = f'SELECT * FROM "ActivitySummary" WHERE time >= \'{since_iso}\' ORDER BY time ASC'
    q_vo2 = f'SELECT * FROM "VO2_Max" WHERE time >= \'{since_iso}\' ORDER BY time ASC'
    q_race = f'SELECT * FROM "RacePredictions" WHERE time >= \'{since_iso}\' ORDER BY time ASC'

    df_daily = query_influxql_df(ro, q_daily)
    df_sleep = query_influxql_df(ro, q_sleep)
    df_hrv_i = query_influxql_df(ro, q_hrv_i)
    df_act = query_influxql_df(ro, q_act)
    df_vo2 = query_influxql_df(ro, q_vo2)
    df_race = query_influxql_df(ro, q_race)

    # Resting HR: prefer DailyStats.restingHeartRate, fall back to SleepSummary.restingHeartRate
    rhr = _last_value(df_daily, "restingHeartRate") or _last_value(df_sleep, "restingHeartRate")

    # HRV: prefer SleepSummary.avgOvernightHrv (Garmin field), else last HRV_Intraday.hrvValue
    hrv = _last_value(df_sleep, "avgOvernightHrv") or _last_value(df_hrv_i, "hrvValue")

    sleep_score = _last_value(df_sleep, "sleepScore")
    sleep_time_s = _last_value(df_sleep, "sleepTimeSeconds")

    vo2_run = _last_value(df_vo2, "VO2_max_value")
    vo2_cyc = _last_value(df_vo2, "VO2_max_value_cycling")

    # Simple load from raw ActivitySummary: acute (7d) vs chronic (28d) moving duration
    acute_7d_s = None
    chronic_28d_s = None
    load_ratio = None
    if df_act is not None and not df_act.empty and "time" in df_act.columns:
        d = df_act.copy()
        d["time"] = pd.to_datetime(d["time"], errors="coerce", utc=True)
        d = d.dropna(subset=["time"])
        if "movingDuration" in d.columns:
            d["movingDuration"] = pd.to_numeric(d["movingDuration"], errors="coerce")
            d = d.dropna(subset=["movingDuration"])
            if not d.empty:
                t_end = d["time"].iloc[-1]
                d7 = d[d["time"] >= (t_end - pd.Timedelta(days=7))]
                d28 = d[d["time"] >= (t_end - pd.Timedelta(days=28))]
                acute_7d_s = float(d7["movingDuration"].sum()) if not d7.empty else 0.0
                chronic_28d_s = float(d28["movingDuration"].sum()) if not d28.empty else 0.0
                if chronic_28d_s and chronic_28d_s > 0:
                    load_ratio = acute_7d_s / (chronic_28d_s / 4.0)  # compare 7d to avg-week in last 28d

    # Trends (simple): compare last 7d mean vs prior 21d mean for HRV/RHR when present (raw fields only).
    debug: dict[str, Any] = {}

    def _trend(df: pd.DataFrame, col: str) -> dict[str, float | None]:
        if df is None or df.empty or col not in df.columns:
            return {"mean_7d": None, "mean_prev": None, "delta": None, "z_7d": None}
        d = df.copy()
        d["time"] = pd.to_datetime(d["time"], errors="coerce", utc=True)
        d = d.dropna(subset=["time"])
        d[col] = pd.to_numeric(d[col], errors="coerce")
        d = d.dropna(subset=[col]).sort_values("time")
        if d.empty:
            return {"mean_7d": None, "mean_prev": None, "delta": None, "z_7d": None}
        t_end = d["time"].iloc[-1]
        d7 = d[d["time"] >= (t_end - pd.Timedelta(days=7))]
        dprev = d[(d["time"] < (t_end - pd.Timedelta(days=7))) & (d["time"] >= (t_end - pd.Timedelta(days=28)))]
        mean_7d = float(d7[col].mean()) if not d7.empty else None
        mean_prev = float(dprev[col].mean()) if not dprev.empty else None
        delta = (mean_7d - mean_prev) if (mean_7d is not None and mean_prev is not None) else None
        z_7d = None
        if not dprev.empty and mean_7d is not None:
            sd_prev = float(dprev[col].std(ddof=1)) if dprev[col].size >= 2 else None
            if sd_prev and sd_prev > 0:
                z_7d = (mean_7d - mean_prev) / sd_prev if mean_prev is not None else None
        return {"mean_7d": mean_7d, "mean_prev": mean_prev, "delta": delta, "z_7d": z_7d}

    # Find actual column names for trend calculation if available
    hrv_trend = None
    rhr_trend = None
    if df_sleep is not None and "avgOvernightHrv" in df_sleep.columns:
        hrv_trend = _trend(df_sleep, "avgOvernightHrv")
    elif df_hrv_i is not None and "hrvValue" in df_hrv_i.columns:
        hrv_trend = _trend(df_hrv_i, "hrvValue")

    if df_daily is not None and "restingHeartRate" in df_daily.columns:
        rhr_trend = _trend(df_daily, "restingHeartRate")
    elif df_sleep is not None and "restingHeartRate" in df_sleep.columns:
        rhr_trend = _trend(df_sleep, "restingHeartRate")

    debug["hrv_trend"] = hrv_trend
    debug["rhr_trend"] = rhr_trend
    debug["load"] = {
        "acute_7d_moving_s": acute_7d_s,
        "chronic_28d_moving_s": chronic_28d_s,
        "load_ratio": load_ratio,
        "notes": "Computed from ActivitySummary.movingDuration only (raw Garmin import).",
    }
    debug["available_signals"] = {
        "has_DailyStats": bool(df_daily is not None and not df_daily.empty),
        "has_SleepSummary": bool(df_sleep is not None and not df_sleep.empty),
        "has_HRV_Intraday": bool(df_hrv_i is not None and not df_hrv_i.empty),
        "has_ActivitySummary": bool(df_act is not None and not df_act.empty),
        "has_VO2_Max": bool(df_vo2 is not None and not df_vo2.empty),
        "has_RacePredictions": bool(df_race is not None and not df_race.empty),
        "note": "Signals listed here are raw Garmin-imported measurements the agent is allowed to use.",
    }

    metrics = {
        "rhr_bpm": _safe_float(rhr),
        "hrv": _safe_float(hrv),
        "sleep_score": _safe_float(sleep_score),
        "sleep_time_s": _safe_float(sleep_time_s),
        "vo2max_run": _safe_float(vo2_run),
        "vo2max_cycling": _safe_float(vo2_cyc),
        "acute_7d_moving_s": _safe_float(acute_7d_s),
        "chronic_28d_moving_s": _safe_float(chronic_28d_s),
        "load_ratio": _safe_float(load_ratio),
    }

    return Snapshot(
        generated_at_utc=_iso(now),
        window_days=int(window_days),
        metrics=metrics,
        debug=debug,
    )


def readiness_score(snapshot: Snapshot) -> dict[str, Any]:
    """
    Deterministic readiness score (0..100) derived from DB values.

    Uses Garmin-provided TrainingReadiness when present as an anchor, but will still
    produce a score if it is missing by combining HRV/RHR/Sleep and load balance.
    """
    m = snapshot.metrics

    # No derived readiness fields are used; start neutral and adjust from raw Garmin imports.
    score = 60.0
    reasons: list[str] = []

    # Sleep influence
    sleep = m.get("sleep_score")
    if sleep is not None:
        if sleep < 55:
            score -= 12
            reasons.append("Low sleep score")
        elif sleep < 70:
            score -= 5
            reasons.append("Moderate sleep score")
        elif sleep > 85:
            score += 3
            reasons.append("High sleep score")

    # Load balance (raw): acute:chronic ratio based on ActivitySummary movingDuration
    lr = m.get("load_ratio")
    if lr is not None:
        if lr >= 1.5:
            score -= 8
            reasons.append("Acute load high vs recent baseline (7d vs 28d)")
        elif lr >= 1.2:
            score -= 4
            reasons.append("Acute load moderately elevated (7d vs 28d)")
        elif lr <= 0.7:
            score += 2
            reasons.append("Acute load low vs baseline (more freshness)")

    # Trend signals (if present)
    hrvz = snapshot.debug.get("hrv_trend", {}) or {}
    rhrz = snapshot.debug.get("rhr_trend", {}) or {}
    hrv_delta = hrvz.get("delta")
    rhr_delta = rhrz.get("delta")
    if isinstance(hrv_delta, (int, float)) and hrv_delta is not None and hrv_delta < -2:
        score -= 6
        reasons.append("HRV down vs baseline")
    if isinstance(rhr_delta, (int, float)) and rhr_delta is not None and rhr_delta > 2:
        score -= 6
        reasons.append("Resting HR up vs baseline")

    score = float(np.clip(score, 0.0, 100.0))

    return {
        "score": score,
        "reasons": reasons,
        "inputs_used": {
            "sleep_score": m.get("sleep_score"),
            "load_ratio": m.get("load_ratio"),
            "hrv_trend": snapshot.debug.get("hrv_trend"),
            "rhr_trend": snapshot.debug.get("rhr_trend"),
        },
    }

