"""
athlete_profile.py — Persistent athlete physiological constants stored in InfluxDB.

AthleteProfile stores validated HR constants (HRmax, RHR floor, LTHR, and
pre-computed Karvonen zone boundaries) as versioned InfluxDB records.  The
ingest system always reads the most recent record.

To update athlete constants: write a new AthleteProfile record via
deploy/scripts/write_athlete_profile.py and restart the garmin-fetch-data
container to pick up the new values.

Future consideration (not implemented): when HRmax is revised (new observed max
during a race), write a new AthleteProfile record with version+1. The ingest
always reads the latest. Backfill scripts could optionally accept a
--profile-date flag to use the profile that was current at that specific date.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

# ── Hardcoded cold-start defaults ─────────────────────────────────────────────
# Used when no AthleteProfile record exists in InfluxDB (first deploy / empty DB).
# Jon Cooper validated values:
#   HRmax 183 bpm  — best credible observed: 184 bpm, 5 Feb 2025 trail run
#   RHR floor 47   — well-rested Garmin baseline; atenolol 25 mg suppresses below this
#   LTHR 136 bpm   — reference field; not used in zone boundary calculation
_DEFAULT_HRMAX: int = 183
_DEFAULT_RHR_FLOOR: int = 47
_DEFAULT_LTHR: int = 136


def _karvonen_zones(hrmax: int, rhr_floor: int) -> dict[str, int]:
    hrr = hrmax - rhr_floor

    def bpm(pct: float) -> int:
        return round(rhr_floor + pct * hrr)

    return {
        "karvonen_z1_low":  bpm(0.50),
        "karvonen_z1_high": bpm(0.60),
        "karvonen_z2_low":  bpm(0.60),
        "karvonen_z2_high": bpm(0.70),
        "karvonen_z3_low":  bpm(0.70),
        "karvonen_z3_high": bpm(0.80),
        "karvonen_z4_low":  bpm(0.80),
        "karvonen_z4_high": bpm(0.90),
        "karvonen_z5_low":  bpm(0.90),
        "karvonen_z5_high": bpm(1.00),
    }


@dataclass
class AthleteProfile:
    hrmax_bpm: int
    rhr_floor_bpm: int
    lthr_bpm: int
    hrr_bpm: int
    karvonen_z1_low: int
    karvonen_z1_high: int
    karvonen_z2_low: int
    karvonen_z2_high: int
    karvonen_z3_low: int
    karvonen_z3_high: int
    karvonen_z4_low: int
    karvonen_z4_high: int
    karvonen_z5_low: int
    karvonen_z5_high: int
    hrmax_source: str = ""
    notes: str = ""
    version: int = 1
    athlete_id: str = "default"

    @classmethod
    def build(
        cls,
        hrmax_bpm: int,
        rhr_floor_bpm: int,
        lthr_bpm: int,
        *,
        hrmax_source: str = "",
        notes: str = "",
        version: int = 1,
        athlete_id: str = "default",
    ) -> "AthleteProfile":
        """Construct an AthleteProfile, computing HRR and Karvonen zones automatically."""
        zones = _karvonen_zones(hrmax_bpm, rhr_floor_bpm)
        return cls(
            hrmax_bpm=hrmax_bpm,
            rhr_floor_bpm=rhr_floor_bpm,
            lthr_bpm=lthr_bpm,
            hrr_bpm=hrmax_bpm - rhr_floor_bpm,
            karvonen_z1_low=zones["karvonen_z1_low"],
            karvonen_z1_high=zones["karvonen_z1_high"],
            karvonen_z2_low=zones["karvonen_z2_low"],
            karvonen_z2_high=zones["karvonen_z2_high"],
            karvonen_z3_low=zones["karvonen_z3_low"],
            karvonen_z3_high=zones["karvonen_z3_high"],
            karvonen_z4_low=zones["karvonen_z4_low"],
            karvonen_z4_high=zones["karvonen_z4_high"],
            karvonen_z5_low=zones["karvonen_z5_low"],
            karvonen_z5_high=zones["karvonen_z5_high"],
            hrmax_source=hrmax_source,
            notes=notes,
            version=version,
            athlete_id=athlete_id,
        )

    @property
    def hrmax_est_source_label(self) -> str:
        """Label written to PhysiologyDaily.HRmax_est_source."""
        if self.hrmax_source:
            return f"athlete_profile({self.hrmax_source})"
        return "athlete_profile"


# Cold-start fallback — version=0 signals "not yet seeded in InfluxDB".
DEFAULT_ATHLETE_PROFILE = AthleteProfile.build(
    hrmax_bpm=_DEFAULT_HRMAX,
    rhr_floor_bpm=_DEFAULT_RHR_FLOOR,
    lthr_bpm=_DEFAULT_LTHR,
    hrmax_source="observed_184bpm_2025-02-05",
    notes=(
        "Cold-start default (no AthleteProfile record in InfluxDB). "
        "Seed with deploy/scripts/write_athlete_profile.py. "
        "atenolol 25mg suppresses HR; validated max 184 bpm observed 5 Feb 2025 "
        "trail run; zones Karvonen HRR method."
    ),
    version=0,
    athlete_id="default",
)


def load_athlete_profile(influx_client: Any, influx_database: str) -> AthleteProfile:
    """
    Load the most recent AthleteProfile record from InfluxDB.

    Falls back to DEFAULT_ATHLETE_PROFILE (version=0) if no record exists,
    logging a warning so cold-start deployments still function.
    """
    try:
        q = 'SELECT * FROM "AthleteProfile" ORDER BY time DESC LIMIT 1'
        result = influx_client.query(q)
        points = list(result.get_points())
        if not points:
            log.warning(
                "AthleteProfile: no record in InfluxDB — using built-in defaults "
                "(hrmax=%d, rhr_floor=%d). Run write_athlete_profile.py to seed.",
                DEFAULT_ATHLETE_PROFILE.hrmax_bpm,
                DEFAULT_ATHLETE_PROFILE.rhr_floor_bpm,
            )
            return DEFAULT_ATHLETE_PROFILE

        row = points[0]
        profile = AthleteProfile.build(
            hrmax_bpm=int(row.get("hrmax_bpm") or _DEFAULT_HRMAX),
            rhr_floor_bpm=int(row.get("rhr_floor_bpm") or _DEFAULT_RHR_FLOOR),
            lthr_bpm=int(row.get("lthr_bpm") or _DEFAULT_LTHR),
            hrmax_source=str(row.get("hrmax_source") or ""),
            notes=str(row.get("notes") or ""),
            version=int(row.get("version") or 1),
            athlete_id=str(row.get("athlete_id") or "default"),
        )
        log.info(
            "AthleteProfile loaded: hrmax=%d, rhr_floor=%d, lthr=%d, version=%d, "
            "source=%s, athlete_id=%s",
            profile.hrmax_bpm, profile.rhr_floor_bpm, profile.lthr_bpm,
            profile.version, profile.hrmax_source, profile.athlete_id,
        )
        return profile
    except Exception:
        log.exception(
            "AthleteProfile: failed to load from InfluxDB — using built-in defaults "
            "(hrmax=%d, rhr_floor=%d).",
            DEFAULT_ATHLETE_PROFILE.hrmax_bpm,
            DEFAULT_ATHLETE_PROFILE.rhr_floor_bpm,
        )
        return DEFAULT_ATHLETE_PROFILE


def write_athlete_profile(
    influx_client: Any,
    profile: AthleteProfile,
    influx_version: str = "1",
) -> None:
    """Write a new AthleteProfile record to InfluxDB."""
    fields: dict = {
        "hrmax_bpm":        profile.hrmax_bpm,
        "rhr_floor_bpm":    profile.rhr_floor_bpm,
        "lthr_bpm":         profile.lthr_bpm,
        "hrr_bpm":          profile.hrr_bpm,
        "karvonen_z1_low":  profile.karvonen_z1_low,
        "karvonen_z1_high": profile.karvonen_z1_high,
        "karvonen_z2_low":  profile.karvonen_z2_low,
        "karvonen_z2_high": profile.karvonen_z2_high,
        "karvonen_z3_low":  profile.karvonen_z3_low,
        "karvonen_z3_high": profile.karvonen_z3_high,
        "karvonen_z4_low":  profile.karvonen_z4_low,
        "karvonen_z4_high": profile.karvonen_z4_high,
        "karvonen_z5_low":  profile.karvonen_z5_low,
        "karvonen_z5_high": profile.karvonen_z5_high,
        "hrmax_source":     profile.hrmax_source,
        "notes":            profile.notes,
        "version":          profile.version,
    }
    point = {
        "measurement": "AthleteProfile",
        "tags": {"athlete_id": profile.athlete_id},
        "time": datetime.now(timezone.utc).isoformat(),
        "fields": fields,
    }
    if influx_version == "1":
        influx_client.write_points([point])
    else:
        influx_client.write(record=[point])
    log.info(
        "AthleteProfile written: hrmax=%d, rhr_floor=%d, lthr=%d, version=%d, athlete_id=%s",
        profile.hrmax_bpm, profile.rhr_floor_bpm, profile.lthr_bpm,
        profile.version, profile.athlete_id,
    )
