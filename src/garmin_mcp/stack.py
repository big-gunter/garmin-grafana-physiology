"""
Unified MCP server: read-only InfluxQL + SQLite agent memory + agent-domain doc.

Recommended for Claude Desktop (stdio). InfluxDB stays read-only; findings never touch Influx.

Run:
  uv run garmin-mcp
  # or: python -m garmin_mcp.stack

Http gateway (remote clients, TLS in front):
  uv run garmin-mcp-http

Env:
  INFLUXDB_* — same as garmin-fetch
  GARMIN_AGENT_MEMORY_DB — optional explicit path to findings.sqlite
  GARMIN_AGENT_MEMORY_DIR — optional directory (default ./data/agent-memory/findings.sqlite)
"""
from __future__ import annotations

from .stack_build import create_garmin_mcp

mcp = create_garmin_mcp()


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
