"""Register read-only InfluxQL tools on a FastMCP instance."""
from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP

from garmin_integration.influx_ro import create_influx_ro, query_influxql_df

from ._env import load_influx_settings
from ._influx_guard import assert_readonly_influxql, clamp_rows


def register_influx_tools(mcp: FastMCP) -> None:
    def _ro():
        s = load_influx_settings()
        return create_influx_ro(**s)

    @mcp.tool()
    def influxql_query(query: str, max_rows: int = 500) -> str:
        """Run a read-only InfluxQL SELECT or SHOW query against GarminStats. Writes/admin queries are rejected."""
        assert_readonly_influxql(query)
        mr = clamp_rows(max_rows)
        ro = _ro()
        df = query_influxql_df(ro, query.strip())
        if df is None or df.empty:
            return json.dumps({"rows": [], "row_count": 0})
        out = df.head(mr)
        return json.dumps(
            {
                "rows": out.to_dict(orient="records"),
                "row_count": int(len(out)),
                "truncated": len(df) > mr,
            },
            default=str,
        )

    @mcp.tool()
    def list_measurements() -> str:
        """List all InfluxDB measurement names (InfluxDB 1.x)."""
        ro = _ro()
        df = query_influxql_df(ro, "SHOW MEASUREMENTS")
        if df is None or df.empty or "name" not in df.columns:
            return json.dumps({"measurements": []})
        names = [str(x) for x in df["name"].dropna().tolist()]
        return json.dumps({"measurements": sorted(set(names))})

    @mcp.tool()
    def describe_measurement(measurement: str) -> str:
        """Return tag keys and field keys for one measurement."""
        m = (measurement or "").strip()
        if not m:
            return json.dumps({"error": "measurement required"})
        ro = _ro()
        fk = query_influxql_df(ro, f'SHOW FIELD KEYS FROM "{m}"')
        tk = query_influxql_df(ro, f'SHOW TAG KEYS FROM "{m}"')
        fields = []
        if fk is not None and not fk.empty and "fieldKey" in fk.columns:
            for a, b in zip(fk.get("fieldKey", []), fk.get("fieldType", [])):
                fields.append({"field": str(a), "type": str(b)})
        tags = []
        if tk is not None and not tk.empty and "tagKey" in tk.columns:
            tags = [str(x) for x in tk["tagKey"].dropna().tolist()]
        return json.dumps({"measurement": m, "field_keys": fields, "tag_keys": tags})
