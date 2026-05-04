from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .influx_ro import InfluxRO, query_influxql_df


@dataclass(frozen=True, slots=True)
class MeasurementSchema:
    name: str
    tag_keys: list[str]
    field_keys: list[tuple[str, str]]  # (fieldKey, fieldType)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _df_col(df: pd.DataFrame, name: str) -> list[str]:
    if df is None or df.empty or name not in df.columns:
        return []
    return [str(x) for x in df[name].dropna().tolist()]


def export_schema_influxql(ro: InfluxRO) -> list[MeasurementSchema]:
    if ro.version != "1":
        raise NotImplementedError("Schema export currently supports InfluxDB v1 only.")

    meas_df = query_influxql_df(ro, "SHOW MEASUREMENTS")
    measurements = _df_col(meas_df, "name")

    schemas: list[MeasurementSchema] = []
    for m in sorted(set(measurements)):
        fk = query_influxql_df(ro, f'SHOW FIELD KEYS FROM "{m}"')
        field_keys: list[tuple[str, str]] = []
        if fk is not None and not fk.empty:
            if "fieldKey" in fk.columns and "fieldType" in fk.columns:
                for a, b in zip(fk["fieldKey"].tolist(), fk["fieldType"].tolist()):
                    if a is None or b is None:
                        continue
                    field_keys.append((str(a), str(b)))

        tk = query_influxql_df(ro, f'SHOW TAG KEYS FROM "{m}"')
        tag_keys = []
        if tk is not None and not tk.empty and "tagKey" in tk.columns:
            tag_keys = [str(x) for x in tk["tagKey"].dropna().tolist()]

        schemas.append(MeasurementSchema(name=m, tag_keys=sorted(set(tag_keys)), field_keys=sorted(set(field_keys))))

    return schemas


def write_schema_markdown(*, schemas: list[MeasurementSchema], out_path: str) -> None:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append("## InfluxDB schema export (auto-generated)")
    lines.append("")
    lines.append(f"- Generated: `{_utc_now_iso()}`")
    lines.append("- Source: InfluxDB v1 introspection (`SHOW MEASUREMENTS/FIELD KEYS/TAG KEYS`)")
    lines.append("")

    for s in schemas:
        lines.append(f"### `{s.name}`")
        lines.append("")
        if s.tag_keys:
            lines.append("**Tag keys**")
            for t in s.tag_keys:
                lines.append(f"- `{t}`")
            lines.append("")
        if s.field_keys:
            lines.append("**Field keys**")
            for fk, ft in s.field_keys:
                lines.append(f"- `{fk}`: `{ft}`")
            lines.append("")
        if not s.tag_keys and not s.field_keys:
            lines.append("_No keys returned (measurement may be empty)._")
            lines.append("")

    p.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
