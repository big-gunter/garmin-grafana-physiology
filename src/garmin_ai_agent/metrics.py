from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import numpy as np
import pandas as pd

from .activity_derivations import _acsm_vo2_running, _grade_from_dist_alt, _rolling_best_mean
from .influx_ro import InfluxRO, query_influxql_df


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


def _to_dt_utc(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=True)


def _activity_summary_df(ro: InfluxRO, *, since_iso: str, sport: str | None = None) -> pd.DataFrame:
    q = f'SELECT * FROM "ActivitySummary" WHERE time >= \'{since_iso}\' ORDER BY time DESC'
    df = query_influxql_df(ro, q)
    if df is None or df.empty:
        return pd.DataFrame()
    # Normalize time
    if "time" in df.columns:
        df["time"] = _to_dt_utc(df["time"])
        df = df.dropna(subset=["time"]).sort_values("time", ascending=False)

    if sport:
        s = str(sport).strip().lower()
        # Upstream tags/fields vary, we try several columns.
        cols = [c for c in ["activity_type_tag", "activityType", "activityTypeName"] if c in df.columns]
        if cols:
            m = pd.Series([False] * len(df), index=df.index)
            for c in cols:
                m = m | df[c].astype(str).str.lower().str.contains(s, na=False)
            df = df[m]
    return df


def _pick_activity_id(row: pd.Series) -> str | None:
    for k in ["Activity_ID", "ActivityId", "activityId", "ActivityID", "ActivityId"]:
        if k in row and row.get(k) is not None:
            v = row.get(k)
            try:
                if str(v).isdigit():
                    return str(int(v))
                return str(v)
            except Exception:
                return str(v)
    return None


def _fetch_activity_stream(ro: InfluxRO, *, activity_id: str, since_iso: str) -> pd.DataFrame:
    # Raw streams (as imported by garmin-fetch)
    q = (
        'SELECT "Speed","HeartRate","Altitude","Distance","Power","GradeAdjustedSpeed" '
        f'FROM "ActivityGPS" WHERE "ActivityID" = \'{activity_id}\' AND time >= \'{since_iso}\' ORDER BY time ASC'
    )
    df = query_influxql_df(ro, q)
    if df is None or df.empty:
        return pd.DataFrame()
    if "time" in df.columns:
        df["time"] = _to_dt_utc(df["time"])
        df = df.dropna(subset=["time"]).sort_values("time")
    return df


def _dt_seconds(t_s: np.ndarray) -> np.ndarray:
    dt = np.diff(t_s)
    dt = np.where(np.isfinite(dt) & (dt > 0), dt, 0.0)
    if dt.size:
        med_dt = float(np.median(dt[dt > 0])) if np.any(dt > 0) else 1.0
    else:
        med_dt = 1.0
    return np.r_[dt, med_dt]


def _rolling_best_mean_simple(values: np.ndarray, t_s: np.ndarray, window_s: float) -> float | None:
    best, _, _ = _rolling_best_mean(values, t_s, window_s=window_s)
    return best


def _power_curve(df_stream: pd.DataFrame, durations_s: list[int]) -> dict[str, float | None]:
    if df_stream is None or df_stream.empty or "Power" not in df_stream.columns or "time" not in df_stream.columns:
        return {}
    d = df_stream.copy()
    # IMPORTANT: treat missing/coasting power as 0 (do NOT drop), otherwise rolling windows
    # get artificially inflated by excluding low/empty samples.
    p = pd.to_numeric(d["Power"], errors="coerce").to_numpy(dtype=float)
    p = np.where(np.isfinite(p) & (p > 0), p, 0.0)
    t_s = d["time"].astype("int64").to_numpy(dtype=float) / 1e9
    out: dict[str, float | None] = {}
    for dur in durations_s:
        best = _rolling_best_mean_simple(p, t_s, float(dur))
        out[f"best_mean_power_{dur}s_w"] = _safe_float(best)
    return out


def _estimate_ftp_from_power_curve(curve: dict[str, float | None]) -> dict[str, Any]:
    """
    Very common heuristic: FTP ≈ 0.95 * best 20-min mean power.
    If 20-min missing, falls back to CP-style 2-point approximation if possible.
    """
    p20 = curve.get("best_mean_power_1200s_w")
    if p20 is not None:
        return {"ftp_w": float(p20) * 0.95, "method": "0.95 * best 20-min mean power"}

    # Fallback: CP from (3min, 12min) using work model W = CP*t + W'
    p180 = curve.get("best_mean_power_180s_w")
    p720 = curve.get("best_mean_power_720s_w")
    if p180 is not None and p720 is not None:
        # Work at each duration
        w1 = float(p180) * 180.0
        w2 = float(p720) * 720.0
        # CP = (W2 - W1) / (t2 - t1)
        cp = (w2 - w1) / (720.0 - 180.0) if (720.0 - 180.0) > 0 else None
        if cp is not None and np.isfinite(cp) and cp > 0:
            return {"ftp_w": float(cp), "method": "CP estimate from best 3-min and 12-min mean power"}

    return {"ftp_w": None, "method": "insufficient power curve data (need at least 20-min, or 3-min+12-min)"}


def _estimate_lthr(df_stream: pd.DataFrame) -> dict[str, Any]:
    """
    LTHR proxy from the best 30-minute mean HR (Joe Friel-style field test approximation).
    Returns median pace/speed in that window when speed is present.
    """
    if df_stream is None or df_stream.empty or "time" not in df_stream.columns or "HeartRate" not in df_stream.columns:
        return {"lthr_bpm": None, "method": "missing HeartRate stream"}
    d = df_stream.copy()
    hr = pd.to_numeric(d["HeartRate"], errors="coerce").to_numpy(dtype=float)
    t_s = d["time"].astype("int64").to_numpy(dtype=float) / 1e9
    best30, s_i, e_i = _rolling_best_mean(hr, t_s, window_s=1800.0)
    if best30 is None or s_i is None or e_i is None:
        return {"lthr_bpm": None, "method": "insufficient data for best 30-min HR window"}

    out: dict[str, Any] = {
        "lthr_bpm": _safe_float(best30),
        "method": "best 30-minute mean HeartRate (field-test proxy)",
    }

    # pace proxy (running): use Speed if present; also grade-adjusted speed if present
    if "Speed" in d.columns:
        sp = pd.to_numeric(d["Speed"], errors="coerce").to_numpy(dtype=float)
        sp_win = sp[s_i : e_i + 1]
        sp_win = sp_win[np.isfinite(sp_win) & (sp_win > 0)]
        if sp_win.size:
            out["speed_mps_at_lthr_window_median"] = float(np.median(sp_win))
    if "GradeAdjustedSpeed" in d.columns:
        gas = pd.to_numeric(d["GradeAdjustedSpeed"], errors="coerce").to_numpy(dtype=float)
        gas_win = gas[s_i : e_i + 1]
        gas_win = gas_win[np.isfinite(gas_win) & (gas_win > 0)]
        if gas_win.size:
            out["grade_adjusted_speed_mps_at_lthr_window_median"] = float(np.median(gas_win))

    out["window_start_utc"] = str(pd.to_datetime(d["time"].iloc[int(s_i)], utc=True).to_pydatetime().astimezone(timezone.utc).isoformat().replace("+00:00", "Z"))
    out["window_end_utc"] = str(pd.to_datetime(d["time"].iloc[int(e_i)], utc=True).to_pydatetime().astimezone(timezone.utc).isoformat().replace("+00:00", "Z"))
    return out


def _estimate_running_vo2(df_stream: pd.DataFrame) -> dict[str, Any]:
    """
    VO₂ demand from ACSM running equation using speed+grade.
    Includes best 5-minute mean and summary stats.
    """
    if df_stream is None or df_stream.empty or "time" not in df_stream.columns:
        return {"ok": False, "note": "missing stream/time"}
    need = {"Speed", "Distance", "Altitude"}
    if not need.issubset(set(df_stream.columns)):
        return {"ok": False, "note": "missing required streams for running VO₂ (need Speed+Distance+Altitude)"}
    d = df_stream.copy()
    speed = pd.to_numeric(d["Speed"], errors="coerce").to_numpy(dtype=float)
    dist = pd.to_numeric(d["Distance"], errors="coerce").to_numpy(dtype=float)
    alt = pd.to_numeric(d["Altitude"], errors="coerce").to_numpy(dtype=float)
    t_s = d["time"].astype("int64").to_numpy(dtype=float) / 1e9

    grade = _grade_from_dist_alt(dist, alt)

    # Guardrail against "free speed" / downhill artefacts:
    # - ACSM helper clamps downhill grade to 0 for VO2 demand.
    # - Additionally prefer samples likely reflecting *effort* if HR exists.
    hr = None
    if "HeartRate" in d.columns:
        hr = pd.to_numeric(d["HeartRate"], errors="coerce").to_numpy(dtype=float)
    effort_mask = np.ones_like(speed, dtype=bool)
    if hr is not None and hr.size:
        hr_f = hr[np.isfinite(hr) & (hr > 0)]
        if hr_f.size >= 30:
            thr = 0.60 * float(np.nanmax(hr_f))
            effort_mask = np.isfinite(hr) & (hr >= thr)

    vo2 = _acsm_vo2_running(speed, grade)
    vo2_eff = np.where(effort_mask, vo2, np.nan)
    best5, s_i, e_i = _rolling_best_mean(vo2_eff, t_s, window_s=300.0)

    out: dict[str, Any] = {"ok": True}
    out["vo2_demand_best5m_ml_kg_min"] = _safe_float(best5)
    if np.isfinite(vo2).any():
        v = vo2[np.isfinite(vo2)]
        out["vo2_demand_mean_ml_kg_min"] = float(np.mean(v))
        out["vo2_demand_p95_ml_kg_min"] = float(np.percentile(v, 95))
    if np.isfinite(vo2_eff).any():
        v = vo2_eff[np.isfinite(vo2_eff)]
        out["vo2_demand_effort_filtered_mean_ml_kg_min"] = float(np.mean(v))
        out["vo2_demand_effort_filtered_p95_ml_kg_min"] = float(np.percentile(v, 95))
        out["effort_filter"] = "HeartRate >= 60% of activity max HR (if HR present); otherwise none"

    # If Garmin provides GradeAdjustedSpeed, compute a grade-normalised VO2 proxy too.
    if "GradeAdjustedSpeed" in d.columns:
        gas = pd.to_numeric(d["GradeAdjustedSpeed"], errors="coerce").to_numpy(dtype=float)
        gas = np.where(np.isfinite(gas) & (gas > 0), gas, np.nan)
        vo2_gap = _acsm_vo2_running(gas, np.zeros_like(gas))
        vo2_gap_eff = np.where(effort_mask, vo2_gap, np.nan)
        best5_gap, _, _ = _rolling_best_mean(vo2_gap_eff, t_s, window_s=300.0)
        out["vo2_demand_gap_best5m_ml_kg_min"] = _safe_float(best5_gap)
        if np.isfinite(vo2_gap_eff).any():
            v = vo2_gap_eff[np.isfinite(vo2_gap_eff)]
            out["vo2_demand_gap_effort_filtered_mean_ml_kg_min"] = float(np.mean(v))
    if s_i is not None and e_i is not None:
        out["best5m_window_start_utc"] = str(pd.to_datetime(d["time"].iloc[int(s_i)], utc=True).to_pydatetime().astimezone(timezone.utc).isoformat().replace("+00:00", "Z"))
        out["best5m_window_end_utc"] = str(pd.to_datetime(d["time"].iloc[int(e_i)], utc=True).to_pydatetime().astimezone(timezone.utc).isoformat().replace("+00:00", "Z"))
    return out


def _estimate_cycling_lthr(df_stream: pd.DataFrame) -> dict[str, Any]:
    """
    Cycling LTHR proxy: best 30-minute mean HR on the ride.
    (Still a proxy; best used on steady efforts, so we also compute a power stability indicator when power exists.)
    """
    out = _estimate_lthr(df_stream)
    out["sport"] = "cycling"
    if df_stream is None or df_stream.empty or "Power" not in df_stream.columns:
        return out
    try:
        p = pd.to_numeric(df_stream["Power"], errors="coerce").to_numpy(dtype=float)
        p = np.where(np.isfinite(p) & (p > 0), p, np.nan)
        if np.isfinite(p).any():
            out["power_p95_w"] = float(np.nanpercentile(p, 95))
            out["power_mean_w"] = float(np.nanmean(p))
    except Exception:
        pass
    return out


def _activities_in_window(ro: InfluxRO, *, window_days: int, sport: str | None, limit: int) -> list[dict[str, Any]]:
    since_iso = _iso(_utc_now() - timedelta(days=int(window_days)))
    df = _activity_summary_df(ro, since_iso=since_iso, sport=sport)
    if df.empty:
        return []
    out: list[dict[str, Any]] = []
    for _, row in df.head(int(limit)).iterrows():
        act_id = _pick_activity_id(row)
        if not act_id:
            continue
        t = row.get("time")
        t_iso = _iso(t.to_pydatetime()) if t is not None and pd.notna(t) else ""
        out.append({"activity_id": act_id, "activity_time_utc": t_iso})
    return out


def _aggregate_numeric(values: list[float]) -> dict[str, float]:
    v = np.asarray(values, dtype=float)
    v = v[np.isfinite(v)]
    if v.size == 0:
        return {}
    return {
        "count": int(v.size),
        "mean": float(np.mean(v)),
        "best": float(np.max(v)),
        "p50": float(np.percentile(v, 50)),
        "p90": float(np.percentile(v, 90)),
    }


def _pace_min_per_km(speed_mps: float | None) -> float | None:
    if speed_mps is None or not np.isfinite(speed_mps) or speed_mps <= 0:
        return None
    return float((1000.0 / float(speed_mps)) / 60.0)


def _best_mean_speed(df_stream: pd.DataFrame, duration_s: int) -> float | None:
    if df_stream is None or df_stream.empty or "time" not in df_stream.columns:
        return None

    d = df_stream.copy()
    t_s = d["time"].astype("int64").to_numpy(dtype=float) / 1e9

    # Prefer GAP if available (terrain-normalised). Otherwise use speed.
    if "GradeAdjustedSpeed" in d.columns:
        sp = pd.to_numeric(d["GradeAdjustedSpeed"], errors="coerce").to_numpy(dtype=float)
    elif "Speed" in d.columns:
        sp = pd.to_numeric(d["Speed"], errors="coerce").to_numpy(dtype=float)
    else:
        return None
    sp = np.where(np.isfinite(sp) & (sp > 0), sp, np.nan)

    # Effort filter using HR, if present (avoid downhill coasting / sprint artefacts)
    if "HeartRate" in d.columns:
        hr = pd.to_numeric(d["HeartRate"], errors="coerce").to_numpy(dtype=float)
        hr_f = hr[np.isfinite(hr) & (hr > 0)]
        if hr_f.size >= 30:
            thr = 0.60 * float(np.nanmax(hr_f))
            sp = np.where(np.isfinite(hr) & (hr >= thr), sp, np.nan)

    best, _, _ = _rolling_best_mean(sp, t_s, window_s=float(duration_s))
    return _safe_float(best)


@dataclass(frozen=True, slots=True)
class MetricResult:
    name: str
    ok: bool
    data: dict[str, Any]
    notes: list[str]


MetricFn = Callable[[InfluxRO, dict[str, Any]], MetricResult]


def catalog() -> list[dict[str, Any]]:
    return [
        {
            "name": "vo2_last_run",
            "description": "Agent-calculated running VO₂ demand for the most recent running activity (ACSM speed+grade). Includes effort-filtering and grade-adjusted-speed (if available) to reduce downhill/terrain bias.",
            "requires": ["ActivitySummary", "ActivityGPS.Speed", "ActivityGPS.Distance", "ActivityGPS.Altitude"],
        },
        {
            "name": "vo2_window_run",
            "description": "Running VO₂ demand summary over a time window across recent runs (effort-filtered; includes GAP variant if present).",
            "requires": ["ActivitySummary", "ActivityGPS.Speed", "ActivityGPS.Distance", "ActivityGPS.Altitude", "ActivityGPS.HeartRate (recommended)"],
        },
        {
            "name": "vo2_last_ride",
            "description": "Agent-calculated cycling VO₂ demand proxy for most recent cycling activity (power→VO₂).",
            "requires": ["ActivitySummary", "ActivityGPS.Power", "BodyComposition.weight (optional)"],
        },
        {
            "name": "vo2_window_ride",
            "description": "Cycling VO₂ demand summary over a time window across recent rides (power→VO₂; coasting handled).",
            "requires": ["ActivitySummary", "ActivityGPS.Power", "BodyComposition.weight"],
        },
        {
            "name": "vo2_window_all",
            "description": "Combined VO₂ summary: runs + rides in window (returns separate blocks and combined counts).",
            "requires": ["ActivitySummary", "ActivityGPS streams as per run/ride"],
        },
        {
            "name": "power_curve_last_ride",
            "description": "Best mean power curve for durations on the most recent ride (coasting treated as 0W; missing samples do not inflate results).",
            "requires": ["ActivityGPS.Power"],
        },
        {
            "name": "power_curve_window_ride",
            "description": "Best mean power curve aggregated across multiple rides in the window (uses max of per-ride best means).",
            "requires": ["ActivitySummary", "ActivityGPS.Power"],
        },
        {
            "name": "ftp_est_last_ride",
            "description": "FTP/CP proxy estimate from last ride power curve (20-min * 0.95 fallback CP).",
            "requires": ["ActivityGPS.Power"],
        },
        {
            "name": "ftp_est_window_ride",
            "description": "FTP/CP proxy estimate over window using aggregated power curve across rides.",
            "requires": ["ActivitySummary", "ActivityGPS.Power"],
        },
        {
            "name": "threshold_window_run",
            "description": "Running threshold pace proxy via Critical Speed (CS) estimated from aggregated best 3-min and 12-min mean speed (prefers GradeAdjustedSpeed when present; effort-filtered).",
            "requires": ["ActivitySummary", "ActivityGPS.Speed or ActivityGPS.GradeAdjustedSpeed", "ActivityGPS.HeartRate (recommended)"],
        },
        {
            "name": "threshold_window_ride",
            "description": "Cycling threshold power proxy (FTP/CP) over window (alias of ftp_est_window_ride).",
            "requires": ["ActivitySummary", "ActivityGPS.Power"],
        },
        {
            "name": "threshold_window_all",
            "description": "Combined threshold summary (running CS + cycling FTP/CP over window).",
            "requires": ["ActivitySummary", "ActivityGPS streams"],
        },
        {
            "name": "lthr_last_run",
            "description": "LTHR proxy from best 30-minute mean HR in the most recent run; includes speed/grade-adjusted speed if available.",
            "requires": ["ActivityGPS.HeartRate", "ActivityGPS.Speed (optional)", "ActivityGPS.GradeAdjustedSpeed (optional)"],
        },
        {
            "name": "lthr_window_run",
            "description": "Running LTHR summary over window across runs (best 30-min mean HR per run; returns distribution).",
            "requires": ["ActivitySummary", "ActivityGPS.HeartRate"],
        },
        {
            "name": "lthr_last_ride",
            "description": "Cycling LTHR proxy from best 30-minute mean HR in the most recent ride.",
            "requires": ["ActivitySummary", "ActivityGPS.HeartRate"],
        },
        {
            "name": "lthr_window_ride",
            "description": "Cycling LTHR summary over window across rides.",
            "requires": ["ActivitySummary", "ActivityGPS.HeartRate"],
        },
        {
            "name": "lthr_window_all",
            "description": "Combined LTHR summary over window (runs + rides).",
            "requires": ["ActivitySummary", "ActivityGPS.HeartRate"],
        },
    ]


def _last_activity(ro: InfluxRO, *, window_days: int, sport: str) -> dict[str, Any]:
    since_iso = _iso(_utc_now() - timedelta(days=int(window_days)))
    df = _activity_summary_df(ro, since_iso=since_iso, sport=sport)
    if df.empty:
        return {"ok": False, "note": f"No ActivitySummary rows found for sport filter '{sport}' in last {window_days} days."}
    row = df.iloc[0]
    act_id = _pick_activity_id(row)
    if not act_id:
        return {"ok": False, "note": "Could not determine ActivityID from ActivitySummary."}
    return {"ok": True, "activity_id": act_id, "activity_time_utc": str(_iso(row["time"].to_pydatetime()) if "time" in row and pd.notna(row["time"]) else "")}


def _weight_kg_latest(ro: InfluxRO, *, since_iso: str) -> float | None:
    df = query_influxql_df(ro, f'SELECT * FROM "BodyComposition" WHERE time >= \'{since_iso}\' ORDER BY time DESC LIMIT 1')
    if df is None or df.empty or "weight" not in df.columns:
        return None
    s = pd.to_numeric(df["weight"], errors="coerce").dropna()
    if s.empty:
        return None
    w_raw = float(s.iloc[0])
    if w_raw > 500:  # grams
        w_raw = w_raw / 1000.0
    return w_raw if 20.0 <= w_raw <= 250.0 else None


def compute_metrics(
    ro: InfluxRO,
    *,
    metrics: list[str],
    window_days: int = 30,
    activity_limit: int = 20,
    cycling_gross_eff: float = 0.23,
) -> dict[str, Any]:
    ctx: dict[str, Any] = {
        "window_days": int(window_days),
        "activity_limit": int(activity_limit),
        "cycling_gross_eff": float(cycling_gross_eff),
    }
    out: dict[str, Any] = {"generated_at_utc": _iso(_utc_now()), "window_days": int(window_days), "results": []}
    for m in metrics:
        out["results"].append(_compute_one(ro, m, ctx).data_with_meta())
    return out


def _compute_one(ro: InfluxRO, name: str, ctx: dict[str, Any]) -> "MetricResultExt":
    n = str(name or "").strip().lower()
    window_days = int(ctx.get("window_days", 30))
    since_iso = _iso(_utc_now() - timedelta(days=int(window_days)))
    notes: list[str] = []
    limit = int(ctx.get("activity_limit", 20))

    if n == "vo2_last_run":
        last = _last_activity(ro, window_days=window_days, sport="run")
        if not last.get("ok"):
            return MetricResultExt(name=n, ok=False, data={"detail": last.get("note")}, notes=[])
        df_stream = _fetch_activity_stream(ro, activity_id=last["activity_id"], since_iso=since_iso)
        v = _estimate_running_vo2(df_stream)
        v["activity_id"] = last["activity_id"]
        v["activity_time_utc"] = last.get("activity_time_utc")
        return MetricResultExt(name=n, ok=bool(v.get("ok")), data=v, notes=notes)

    if n == "vo2_window_run":
        acts = _activities_in_window(ro, window_days=window_days, sport="run", limit=limit)
        if not acts:
            return MetricResultExt(name=n, ok=False, data={"detail": f"No runs found in last {window_days} days."}, notes=[])
        vals: list[float] = []
        vals_gap: list[float] = []
        used = 0
        for a in acts:
            df_stream = _fetch_activity_stream(ro, activity_id=a["activity_id"], since_iso=since_iso)
            v = _estimate_running_vo2(df_stream)
            x = v.get("vo2_demand_best5m_ml_kg_min")
            if x is not None:
                vals.append(float(x))
                used += 1
            xg = v.get("vo2_demand_gap_best5m_ml_kg_min")
            if xg is not None:
                vals_gap.append(float(xg))
        out = {
            "sport": "running",
            "activities_considered": int(len(acts)),
            "activities_used": int(used),
            "vo2_demand_best5m_summary": _aggregate_numeric(vals),
            "vo2_demand_gap_best5m_summary": _aggregate_numeric(vals_gap),
            "notes": "Per-activity best 5-min VO₂ demand; downhill clamped; effort-filtered when HR present; GAP variant if available.",
        }
        ok = bool(out["vo2_demand_best5m_summary"])
        return MetricResultExt(name=n, ok=ok, data=out, notes=notes)

    if n == "lthr_last_run":
        last = _last_activity(ro, window_days=window_days, sport="run")
        if not last.get("ok"):
            return MetricResultExt(name=n, ok=False, data={"detail": last.get("note")}, notes=[])
        df_stream = _fetch_activity_stream(ro, activity_id=last["activity_id"], since_iso=since_iso)
        v = _estimate_lthr(df_stream)
        v["activity_id"] = last["activity_id"]
        v["activity_time_utc"] = last.get("activity_time_utc")
        ok = v.get("lthr_bpm") is not None
        return MetricResultExt(name=n, ok=bool(ok), data=v, notes=notes)

    if n == "lthr_window_run":
        acts = _activities_in_window(ro, window_days=window_days, sport="run", limit=limit)
        if not acts:
            return MetricResultExt(name=n, ok=False, data={"detail": f"No runs found in last {window_days} days."}, notes=[])
        vals: list[float] = []
        used = 0
        for a in acts:
            df_stream = _fetch_activity_stream(ro, activity_id=a["activity_id"], since_iso=since_iso)
            v = _estimate_lthr(df_stream)
            if v.get("lthr_bpm") is not None:
                vals.append(float(v["lthr_bpm"]))
                used += 1
        out = {"sport": "running", "activities_considered": int(len(acts)), "activities_used": int(used), "lthr_bpm_summary": _aggregate_numeric(vals)}
        ok = bool(out["lthr_bpm_summary"])
        return MetricResultExt(name=n, ok=ok, data=out, notes=notes)

    if n == "lthr_last_ride":
        last = _last_activity(ro, window_days=window_days, sport="cycle|ride|bike|cycling")
        if not last.get("ok"):
            return MetricResultExt(name=n, ok=False, data={"detail": last.get("note")}, notes=[])
        df_stream = _fetch_activity_stream(ro, activity_id=last["activity_id"], since_iso=since_iso)
        v = _estimate_cycling_lthr(df_stream)
        v["activity_id"] = last["activity_id"]
        v["activity_time_utc"] = last.get("activity_time_utc")
        ok = v.get("lthr_bpm") is not None
        return MetricResultExt(name=n, ok=bool(ok), data=v, notes=notes)

    if n == "lthr_window_ride":
        acts = _activities_in_window(ro, window_days=window_days, sport="cycle|ride|bike|cycling", limit=limit)
        if not acts:
            return MetricResultExt(name=n, ok=False, data={"detail": f"No rides found in last {window_days} days."}, notes=[])
        vals: list[float] = []
        used = 0
        for a in acts:
            df_stream = _fetch_activity_stream(ro, activity_id=a["activity_id"], since_iso=since_iso)
            v = _estimate_cycling_lthr(df_stream)
            if v.get("lthr_bpm") is not None:
                vals.append(float(v["lthr_bpm"]))
                used += 1
        out = {"sport": "cycling", "activities_considered": int(len(acts)), "activities_used": int(used), "lthr_bpm_summary": _aggregate_numeric(vals)}
        ok = bool(out["lthr_bpm_summary"])
        return MetricResultExt(name=n, ok=ok, data=out, notes=notes)

    if n == "lthr_window_all":
        run = _compute_one(ro, "lthr_window_run", {**ctx, "window_days": window_days, "activity_limit": limit}).data
        ride = _compute_one(ro, "lthr_window_ride", {**ctx, "window_days": window_days, "activity_limit": limit}).data
        out = {"running": run, "cycling": ride, "note": "Combined report; see per-sport summaries."}
        ok = bool((run or {}).get("lthr_bpm_summary")) or bool((ride or {}).get("lthr_bpm_summary"))
        return MetricResultExt(name=n, ok=ok, data=out, notes=notes)

    if n in {"power_curve_last_ride", "ftp_est_last_ride", "vo2_last_ride"}:
        last = _last_activity(ro, window_days=window_days, sport="cycle|ride|bike|cycling")
        if not last.get("ok"):
            return MetricResultExt(name=n, ok=False, data={"detail": last.get("note")}, notes=[])
        df_stream = _fetch_activity_stream(ro, activity_id=last["activity_id"], since_iso=since_iso)
        if df_stream.empty:
            return MetricResultExt(name=n, ok=False, data={"detail": "No ActivityGPS stream found for that activity."}, notes=[])

        if n == "power_curve_last_ride":
            curve = _power_curve(df_stream, durations_s=[5, 15, 30, 60, 180, 300, 600, 1200, 1800, 3600])
            return MetricResultExt(
                name=n,
                ok=bool(curve),
                data={"activity_id": last["activity_id"], "activity_time_utc": last.get("activity_time_utc"), "curve": curve},
                notes=notes,
            )

        if n == "ftp_est_last_ride":
            curve = _power_curve(df_stream, durations_s=[180, 720, 1200])
            ftp = _estimate_ftp_from_power_curve(curve)
            return MetricResultExt(
                name=n,
                ok=ftp.get("ftp_w") is not None,
                data={"activity_id": last["activity_id"], "activity_time_utc": last.get("activity_time_utc"), **ftp, "power_curve_used": curve},
                notes=notes,
            )

        # vo2_last_ride (power→VO2 proxy)
        w = _weight_kg_latest(ro, since_iso=since_iso)
        if w is None:
            notes.append("BodyComposition.weight not available; cycling VO₂ demand needs weight (kg).")
            return MetricResultExt(
                name=n,
                ok=False,
                data={"activity_id": last["activity_id"], "activity_time_utc": last.get("activity_time_utc"), "detail": "Missing weight (kg) for cycling VO₂ conversion."},
                notes=notes,
            )

        p = pd.to_numeric(df_stream.get("Power"), errors="coerce").to_numpy(dtype=float)
        t_s = df_stream["time"].astype("int64").to_numpy(dtype=float) / 1e9
        # gross efficiency: use default (config value is applied when deriving AgentDerivedActivity; here we keep simple)
        eff = float(np.clip(float(ctx.get("cycling_gross_eff", 0.23)), 0.15, 0.30))
        p = np.where(np.isfinite(p) & (p > 0), p, np.nan)
        met_w = p / eff
        vo2_l_min = (met_w * 60.0) / 20900.0
        vo2_ml_kg_min = (vo2_l_min * 1000.0) / float(w)
        best5 = _rolling_best_mean_simple(vo2_ml_kg_min, t_s, 300.0)
        out = {
            "activity_id": last["activity_id"],
            "activity_time_utc": last.get("activity_time_utc"),
            "weight_kg_used": float(w),
            "cycling_gross_eff_used": eff,
            "vo2_demand_best5m_ml_kg_min": _safe_float(best5),
        }
        return MetricResultExt(name=n, ok=out["vo2_demand_best5m_ml_kg_min"] is not None, data=out, notes=notes)

    if n == "power_curve_window_ride":
        acts = _activities_in_window(ro, window_days=window_days, sport="cycle|ride|bike|cycling", limit=limit)
        if not acts:
            return MetricResultExt(name=n, ok=False, data={"detail": f"No rides found in last {window_days} days."}, notes=[])
        durations = [5, 15, 30, 60, 180, 300, 600, 1200, 1800, 3600]
        best_by_dur: dict[str, float] = {}
        used = 0
        for a in acts:
            df_stream = _fetch_activity_stream(ro, activity_id=a["activity_id"], since_iso=since_iso)
            curve = _power_curve(df_stream, durations_s=durations)
            if not curve:
                continue
            used += 1
            for k, v in curve.items():
                if v is None:
                    continue
                best_by_dur[k] = max(float(best_by_dur.get(k, 0.0)), float(v))
        out = {"sport": "cycling", "activities_considered": int(len(acts)), "activities_used": int(used), "curve": best_by_dur, "note": "Aggregated by taking max of per-ride best means; coasting treated as 0W."}
        ok = bool(best_by_dur)
        return MetricResultExt(name=n, ok=ok, data=out, notes=notes)

    if n == "ftp_est_window_ride":
        curve_res = _compute_one(ro, "power_curve_window_ride", {**ctx, "window_days": window_days, "activity_limit": limit})
        curve = (curve_res.data or {}).get("curve") if isinstance(curve_res.data, dict) else None
        if not isinstance(curve, dict) or not curve:
            return MetricResultExt(name=n, ok=False, data={"detail": "No usable power curve in window."}, notes=curve_res.notes)
        # ensure keys for ftp estimator
        ftp = _estimate_ftp_from_power_curve(
            {
                "best_mean_power_180s_w": curve.get("best_mean_power_180s_w"),
                "best_mean_power_720s_w": curve.get("best_mean_power_720s_w"),
                "best_mean_power_1200s_w": curve.get("best_mean_power_1200s_w"),
            }
        )
        out = {"sport": "cycling", **ftp, "power_curve_used": {k: curve.get(k) for k in ["best_mean_power_180s_w", "best_mean_power_720s_w", "best_mean_power_1200s_w"]}}
        ok = ftp.get("ftp_w") is not None
        return MetricResultExt(name=n, ok=ok, data=out, notes=notes)

    if n == "threshold_window_ride":
        # Alias: cycling threshold proxy is ftp_est_window_ride
        res = _compute_one(ro, "ftp_est_window_ride", ctx)
        return MetricResultExt(name=n, ok=res.ok, data=res.data, notes=res.notes)

    if n == "threshold_window_run":
        acts = _activities_in_window(ro, window_days=window_days, sport="run", limit=limit)
        if not acts:
            return MetricResultExt(name=n, ok=False, data={"detail": f"No runs found in last {window_days} days."}, notes=[])
        # Aggregate best mean speeds across runs by taking the max per duration
        best_180 = None
        best_720 = None
        used = 0
        for a in acts:
            df_stream = _fetch_activity_stream(ro, activity_id=a["activity_id"], since_iso=since_iso)
            v180 = _best_mean_speed(df_stream, 180)
            v720 = _best_mean_speed(df_stream, 720)
            if v180 is None and v720 is None:
                continue
            used += 1
            if v180 is not None:
                best_180 = v180 if best_180 is None else max(best_180, v180)
            if v720 is not None:
                best_720 = v720 if best_720 is None else max(best_720, v720)

        if best_180 is None or best_720 is None:
            return MetricResultExt(
                name=n,
                ok=False,
                data={"detail": "Insufficient stream data to estimate Critical Speed (need best 3-min and 12-min speeds).", "activities_considered": len(acts), "activities_used": used},
                notes=notes,
            )

        # CS estimate from work-distance model: D = CS*t + D'
        # Using v*t as distance proxy for each duration.
        d1 = float(best_180) * 180.0
        d2 = float(best_720) * 720.0
        cs = (d2 - d1) / (720.0 - 180.0) if (720.0 - 180.0) > 0 else None
        cs = _safe_float(cs)
        out = {
            "sport": "running",
            "activities_considered": int(len(acts)),
            "activities_used": int(used),
            "best_mean_speed_180s_mps": float(best_180),
            "best_mean_speed_720s_mps": float(best_720),
            "critical_speed_mps": cs,
            "critical_speed_pace_min_per_km": _pace_min_per_km(cs) if cs is not None else None,
            "method": "CS from aggregated best 3-min and 12-min mean speed (prefers GradeAdjustedSpeed; effort-filtered by HR if present).",
        }
        return MetricResultExt(name=n, ok=cs is not None, data=out, notes=notes)

    if n == "threshold_window_all":
        run = _compute_one(ro, "threshold_window_run", {**ctx, "window_days": window_days, "activity_limit": limit}).data
        ride = _compute_one(ro, "threshold_window_ride", {**ctx, "window_days": window_days, "activity_limit": limit}).data
        out = {"running": run, "cycling": ride, "note": "Combined threshold report; see per-sport blocks."}
        ok = bool((run or {}).get("critical_speed_mps")) or bool((ride or {}).get("ftp_w"))
        return MetricResultExt(name=n, ok=ok, data=out, notes=notes)

    if n == "vo2_window_ride":
        acts = _activities_in_window(ro, window_days=window_days, sport="cycle|ride|bike|cycling", limit=limit)
        if not acts:
            return MetricResultExt(name=n, ok=False, data={"detail": f"No rides found in last {window_days} days."}, notes=[])
        w = _weight_kg_latest(ro, since_iso=since_iso)
        if w is None:
            return MetricResultExt(name=n, ok=False, data={"detail": "Missing BodyComposition.weight (kg) for cycling VO₂ conversion."}, notes=[])
        eff = float(np.clip(float(ctx.get("cycling_gross_eff", 0.23)), 0.15, 0.30))
        vals: list[float] = []
        used = 0
        for a in acts:
            df_stream = _fetch_activity_stream(ro, activity_id=a["activity_id"], since_iso=since_iso)
            if df_stream.empty or "Power" not in df_stream.columns:
                continue
            p = pd.to_numeric(df_stream.get("Power"), errors="coerce").to_numpy(dtype=float)
            p = np.where(np.isfinite(p) & (p > 0), p, 0.0)  # coasting as 0
            t_s = df_stream["time"].astype("int64").to_numpy(dtype=float) / 1e9
            met_w = p / eff
            vo2_l_min = (met_w * 60.0) / 20900.0
            vo2_ml_kg_min = (vo2_l_min * 1000.0) / float(w)
            best5 = _rolling_best_mean_simple(vo2_ml_kg_min, t_s, 300.0)
            if best5 is not None:
                vals.append(float(best5))
                used += 1
        out = {"sport": "cycling", "activities_considered": int(len(acts)), "activities_used": int(used), "weight_kg_used": float(w), "cycling_gross_eff_used": eff, "vo2_demand_best5m_summary": _aggregate_numeric(vals)}
        ok = bool(out["vo2_demand_best5m_summary"])
        return MetricResultExt(name=n, ok=ok, data=out, notes=notes)

    if n == "vo2_window_all":
        run = _compute_one(ro, "vo2_window_run", {**ctx, "window_days": window_days, "activity_limit": limit}).data
        ride = _compute_one(ro, "vo2_window_ride", {**ctx, "window_days": window_days, "activity_limit": limit}).data
        out = {"running": run, "cycling": ride, "note": "Combined report; see per-sport summaries."}
        ok = bool((run or {}).get("vo2_demand_best5m_summary")) or bool((ride or {}).get("vo2_demand_best5m_summary"))
        return MetricResultExt(name=n, ok=ok, data=out, notes=notes)

    return MetricResultExt(name=n, ok=False, data={"detail": f"Unknown metric '{name}'. See /metrics/catalog."}, notes=[])


@dataclass(frozen=True, slots=True)
class MetricResultExt:
    name: str
    ok: bool
    data: dict[str, Any]
    notes: list[str]

    def data_with_meta(self) -> dict[str, Any]:
        return {"name": self.name, "ok": bool(self.ok), "notes": list(self.notes or []), "data": self.data}


def render_markdown(result: dict[str, Any]) -> str:
    """
    Render a human-readable markdown summary (no tables).
    """
    lines: list[str] = []
    lines.append("## Metrics")
    lines.append("")

    def _render_dict_block(title: str, d: dict[str, Any], indent: str = "") -> None:
        """
        Render a nested dict (used for combined running/cycling blocks).
        Keep it compact, prefer summaries and headline values.
        """
        lines.append(f"{indent}#### {title}")
        if not isinstance(d, dict) or not d:
            lines.append(f"{indent}- **detail**: missing")
            return
        # headline keys
        for k in ["sport", "activities_considered", "activities_used", "weight_kg_used", "cycling_gross_eff_used", "method", "detail", "note", "notes"]:
            if k in d and d.get(k) is not None and not isinstance(d.get(k), (dict, list)):
                lines.append(f"{indent}- **{k}**: {d.get(k)}")
        # summaries
        for sk in ["vo2_demand_best5m_summary", "vo2_demand_gap_best5m_summary", "lthr_bpm_summary"]:
            if sk in d and isinstance(d[sk], dict) and d[sk]:
                lines.append(f"{indent}- **{sk}**:")
                for kk, vv in d[sk].items():
                    lines.append(f"{indent}  - **{kk}**: {vv}")
        # threshold headline
        for k in ["ftp_w", "critical_speed_mps", "critical_speed_pace_min_per_km"]:
            if k in d and d.get(k) is not None:
                lines.append(f"{indent}- **{k}**: {d.get(k)}")

    for r in result.get("results", []):
        name = r.get("name")
        ok = r.get("ok")
        lines.append(f"### {name}")
        lines.append(f"- **ok**: {bool(ok)}")
        for n in (r.get("notes") or [])[:6]:
            lines.append(f"- **note**: {n}")
        data = r.get("data") or {}
        if isinstance(data, dict):
            # show key high-signal fields only (keep it readable, no tables)
            keys = [
                "sport",
                "activity_time_utc",
                "activity_id",
                "vo2_demand_best5m_ml_kg_min",
                "vo2_demand_gap_best5m_ml_kg_min",
                "lthr_bpm",
                "ftp_w",
                "critical_speed_mps",
                "critical_speed_pace_min_per_km",
                "method",
                "detail",
            ]
            for k in keys:
                if k in data and data.get(k) is not None and k not in {"curve", "power_curve_used"}:
                    lines.append(f"- **{k}**: {data.get(k)}")

            # summary dicts
            for sk in ["vo2_demand_best5m_summary", "vo2_demand_gap_best5m_summary", "lthr_bpm_summary"]:
                if sk in data and isinstance(data[sk], dict) and data[sk]:
                    lines.append(f"- **{sk}**:")
                    for kk, vv in data[sk].items():
                        lines.append(f"  - **{kk}**: {vv}")
            if "curve" in data and isinstance(data["curve"], dict):
                lines.append("- **power_curve**:")
                for kk, vv in data["curve"].items():
                    if vv is not None:
                        lines.append(f"  - **{kk}**: {vv:.1f} W" if isinstance(vv, (int, float)) else f"  - **{kk}**: {vv}")
            if "running" in data or "cycling" in data:
                # combined blocks: expand inline
                if isinstance(data.get("running"), dict):
                    _render_dict_block("Running", data["running"], indent="")
                if isinstance(data.get("cycling"), dict):
                    _render_dict_block("Cycling", data["cycling"], indent="")
        lines.append("")
    return "\n".join(lines).strip()

