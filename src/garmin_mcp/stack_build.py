"""Shared FastMCP factory for stdio and HTTP transports."""

from __future__ import annotations

from typing import Any

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from ._env import docs_dir
from .influx_tools import register_influx_tools
from .memory_store import AgentMemoryStore, default_memory_db_path
from .memory_tools import register_memory_tools

load_dotenv(override=False)

_store: AgentMemoryStore | None = None


def _get_store() -> AgentMemoryStore:
    global _store
    if _store is None:
        _store = AgentMemoryStore(default_memory_db_path())
    return _store


def _register_docs_resource(mcp: FastMCP) -> None:
    @mcp.resource("docs://agent-domain")
    def agent_domain_doc() -> str:
        """Sports-science domain spec for reasoning about Garmin data (markdown)."""
        p = docs_dir() / "agent-domain.md"
        if not p.is_file():
            return f"(missing file: {p})"
        return p.read_text(encoding="utf-8", errors="replace")


def create_garmin_mcp(**fastmcp_kwargs: Any) -> FastMCP:
    """Build the unified MCP app (read-only Influx + SQLite memory + domain doc resource).

    Pass any ``FastMCP`` constructor arguments (e.g. ``host``, ``port``,
    ``streamable_http_path``, ``transport_security``) for HTTP deployments.
    """
    mcp = FastMCP("Garmin data + agent memory", **fastmcp_kwargs)
    register_influx_tools(mcp)
    register_memory_tools(mcp, _get_store())
    _register_docs_resource(mcp)
    return mcp
