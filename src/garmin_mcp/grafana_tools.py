"""
MCP server: Grafana HTTP API (dashboards). No Influx credentials.

Run (stdio):
  GRAFANA_URL=http://localhost:3000 GRAFANA_API_TOKEN=... python -m garmin_mcp.grafana_tools
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import httpx
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from garmin_integration.grafana_api import push_dashboard_json

load_dotenv(override=False)

mcp = FastMCP("Grafana (dashboard API)")


def _url() -> str:
    u = (os.getenv("GRAFANA_URL") or "http://localhost:3000").rstrip("/")
    return u


def _headers() -> dict[str, str]:
    tok = (os.getenv("GRAFANA_API_TOKEN") or "").strip()
    if not tok:
        raise RuntimeError("GRAFANA_API_TOKEN is not set")
    return {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}


@mcp.tool()
def grafana_search_dashboards(query: str = "") -> str:
    """Search dashboards. Optional query string matches title/tags (Grafana /api/search)."""
    with httpx.Client(timeout=30.0) as client:
        r = client.get(
            f"{_url()}/api/search",
            params={"query": query or ""},
            headers=_headers(),
        )
        if r.status_code >= 400:
            return json.dumps({"error": r.text, "status": r.status_code})
        return json.dumps(r.json(), default=str)


@mcp.tool()
def grafana_get_dashboard_uid(uid: str) -> str:
    """Fetch dashboard JSON by uid (for edit/clone)."""
    u = (uid or "").strip()
    if not u:
        return json.dumps({"error": "uid required"})
    with httpx.Client(timeout=30.0) as client:
        r = client.get(f"{_url()}/api/dashboards/uid/{u}", headers=_headers())
        if r.status_code >= 400:
            return json.dumps({"error": r.text, "status": r.status_code})
        return json.dumps(r.json(), default=str)


@mcp.tool()
def grafana_push_dashboard_file(
    dashboard_path: str,
    folder_id: int = 0,
    overwrite: bool = True,
) -> str:
    """POST a dashboard JSON file from disk into Grafana (path must be readable by this process)."""
    p = (dashboard_path or "").strip()
    if not p:
        return json.dumps({"error": "dashboard_path required"})
    tok = (os.getenv("GRAFANA_API_TOKEN") or "").strip()
    if not tok:
        return json.dumps({"error": "GRAFANA_API_TOKEN is not set"})
    try:
        out = push_dashboard_json(
            grafana_url=_url(),
            api_token=tok,
            dashboard_path=p,
            folder_id=folder_id,
            overwrite=overwrite,
        )
        return json.dumps(out, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def grafana_provisioned_dashboards_dir() -> str:
    """List JSON files in GARMIN_GRAFANA_DASHBOARD_DIR (default ./Grafana_Dashboard)."""
    root = Path(os.getenv("GARMIN_GRAFANA_DASHBOARD_DIR", "Grafana_Dashboard")).resolve()
    if not root.is_dir():
        return json.dumps({"path": str(root), "files": [], "error": "not a directory"})
    files = sorted([p.name for p in root.glob("*.json")])
    return json.dumps({"path": str(root), "files": files})


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
