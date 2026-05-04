"""
MCP server: read-only InfluxDB only (no agent memory).

Prefer ``garmin-mcp`` (stack) for daily use: Influx + SQLite memory + domain doc.
"""
from __future__ import annotations

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from ._env import docs_dir
from .influx_tools import register_influx_tools

load_dotenv(override=False)

mcp = FastMCP("Garmin Influx (read-only)")
register_influx_tools(mcp)


@mcp.resource("docs://agent-domain")
def agent_domain_doc() -> str:
    """Sports-science domain spec for reasoning about Garmin data (markdown)."""
    p = docs_dir() / "agent-domain.md"
    if not p.is_file():
        return f"(missing file: {p})"
    return p.read_text(encoding="utf-8", errors="replace")


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
