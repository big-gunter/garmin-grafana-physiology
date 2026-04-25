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

    # These measurements exist in this repo for InfluxDB v1 rollups / fetchers.
    # We keep queries narrow (last N days) and only select fields the agent needs.
    q_phys = (
        'SELECT * FROM "PhysiologyDaily" '
        f"WHERE time >= '{since_iso}' ORDER BY time ASC"
    )
    q_load = (
        'SELECT * FROM "TrainingLoadDaily" '
        f"WHERE time >= '{since_iso}' ORDER BY time ASC"
    )
    q_sleep = (
        'SELECT * FROM "SleepSummary" '
        f"WHERE time >= '{since_iso}' ORDER BY time ASC"
    )
    q_readiness = (
        'SELECT * FROM "TrainingReadiness" '
        f"WHERE time >= '{since_iso}' ORDER BY time ASC"
    )

    df_phys = query_influxql_df(ro, q_phys)
    df_load = query_influxql_df(ro, q_load)
    df_sleep = query_influxql_df(ro, q_sleep)
    df_tr = query_influxql_df(ro, q_readiness)

    # Common field names can vary by Garmin endpoints / mapping.
    # We defensively try several likely columns.
    rhr = _last_value(df_phys, "restingHeartRate") or _last_value(df_phys, "rhr_bpm") or _last_value(df_phys, "RHR")
    hrv = _last_value(df_phys, "hrv_rmssd") or _last_value(df_phys, "rmssd") or _last_value(df_phys, "HRV")

    ctl = _last_value(df_load, "ctl") or _last_value(df_load, "CTL")
    atl = _last_value(df_load, "atl") or _last_value(df_load, "ATL")
    tsb = _last_value(df_load, "tsb") or _last_value(df_load, "TSB")

    sleep_score = (
        _last_value(df_sleep, "sleepScore")
        or _last_value(df_sleep, "overallSleepScore")
        or _last_value(df_sleep, "score")
    )
    tr_score = _last_value(df_tr, "score") or _last_value(df_tr, "trainingReadinessScore")

    # Trends (simple): compare last 7d mean vs prior 21d mean for HRV/RHR when present.
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
    if "hrv_rmssd" in df_phys.columns:
        hrv_trend = _trend(df_phys, "hrv_rmssd")
    elif "rmssd" in df_phys.columns:
        hrv_trend = _trend(df_phys, "rmssd")

    if "rhr_bpm" in df_phys.columns:
        rhr_trend = _trend(df_phys, "rhr_bpm")
    elif "restingHeartRate" in df_phys.columns:
        rhr_trend = _trend(df_phys, "restingHeartRate")

    debug["hrv_trend"] = hrv_trend
    debug["rhr_trend"] = rhr_trend

    metrics = {
        "rhr_bpm": _safe_float(rhr),
        "hrv_rmssd": _safe_float(hrv),
        "sleep_score": _safe_float(sleep_score),
        "training_readiness_garmin": _safe_float(tr_score),
        "ctl": _safe_float(ctl),
        "atl": _safe_float(atl),
        "tsb": _safe_float(tsb),
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

    # Start with Garmin readiness if present, else neutral 60.
    base = m.get("training_readiness_garmin")
    score = float(base) if isinstance(base, (int, float)) and base is not None else 60.0
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

    # Load balance (TSB)
    tsb = m.get("tsb")
    if tsb is not None:
        if tsb < -15:
            score -= 10
            reasons.append("High accumulated fatigue (TSB low)")
        elif tsb < -5:
            score -= 5
            reasons.append("Some fatigue (TSB mildly negative)")
        elif tsb > 10:
            score += 3
            reasons.append("Fresh (TSB positive)")

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
            "training_readiness_garmin": m.get("training_readiness_garmin"),
            "sleep_score": m.get("sleep_score"),
            "tsb": m.get("tsb"),
            "hrv_trend": snapshot.debug.get("hrv_trend"),
            "rhr_trend": snapshot.debug.get("rhr_trend"),
        },
    }

