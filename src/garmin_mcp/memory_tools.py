"""Register agent memory (SQLite sidecar) tools on a FastMCP instance."""
from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP

from .memory_store import AgentMemoryStore


def register_memory_tools(mcp: FastMCP, store: AgentMemoryStore) -> None:
    @mcp.tool()
    def remember_finding(summary: str, detail: str = "", tags: str = "") -> str:
        """Save a short conclusion or note for later (stored locally in SQLite, not Influx). Use for prior insights the agent should recall."""
        try:
            fid = store.append(summary=summary, detail=detail, tags=tags or None)
            return json.dumps({"ok": True, "id": fid, "note": "stored in agent memory sidecar"})
        except ValueError as e:
            return json.dumps({"ok": False, "error": str(e)})

    @mcp.tool()
    def search_past_findings(query: str, limit: int = 20) -> str:
        """Search saved findings by keyword in summary, detail, or tags."""
        rows = store.search(query, limit=limit)
        return json.dumps(
            {
                "findings": [
                    {
                        "id": r.id,
                        "created_at": r.created_at,
                        "summary": r.summary,
                        "detail": r.detail,
                        "tags": r.tags,
                    }
                    for r in rows
                ],
                "count": len(rows),
            },
            default=str,
        )

    @mcp.tool()
    def list_recent_findings(limit: int = 15) -> str:
        """List the most recent saved findings (newest first)."""
        rows = store.recent(limit=limit)
        return json.dumps(
            {
                "findings": [
                    {
                        "id": r.id,
                        "created_at": r.created_at,
                        "summary": r.summary,
                        "detail": r.detail,
                        "tags": r.tags,
                    }
                    for r in rows
                ],
                "count": len(rows),
            },
            default=str,
        )

    @mcp.tool()
    def get_finding(finding_id: int) -> str:
        """Fetch one saved finding by id."""
        r = store.get(int(finding_id))
        if r is None:
            return json.dumps({"error": "not found", "id": finding_id})
        return json.dumps(
            {
                "id": r.id,
                "created_at": r.created_at,
                "summary": r.summary,
                "detail": r.detail,
                "tags": r.tags,
            },
            default=str,
        )
