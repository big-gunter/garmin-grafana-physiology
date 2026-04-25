from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx


def _normalize_url(url: str) -> str:
    return url.rstrip("/")


def push_dashboard_json(
    *,
    grafana_url: str,
    api_token: str,
    dashboard_path: str,
    folder_id: int = 0,
    overwrite: bool = True,
) -> dict[str, Any]:
    """
    Pushes a dashboard JSON file to Grafana via HTTP API.

    Uses POST /api/dashboards/db with:
      { dashboard: <json>, folderId, overwrite }
    """
    p = Path(dashboard_path)
    if not p.exists():
        raise FileNotFoundError(str(p))

    dashboard = json.loads(p.read_text(encoding="utf-8"))
    # Grafana recommends null id on import/update payload
    if "id" in dashboard:
        dashboard["id"] = None

    payload = {"dashboard": dashboard, "folderId": int(folder_id), "overwrite": bool(overwrite)}

    url = _normalize_url(grafana_url) + "/api/dashboards/db"
    headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}

    def _post(u: str) -> dict[str, Any]:
        with httpx.Client(timeout=20.0) as client:
            r = client.post(u, headers=headers, json=payload)
            if r.status_code >= 400:
                raise RuntimeError(f"Grafana API error {r.status_code}: {r.text}")
            return r.json()

    try:
        return _post(url)
    except httpx.ConnectError:
        # Common docker-compose pitfall: using http://localhost:3000 inside a container.
        # If the configured URL targets localhost, retry with the compose service name.
        if _normalize_url(grafana_url).startswith(("http://localhost", "http://127.0.0.1", "https://localhost", "https://127.0.0.1")):
            fallback = "http://grafana:3000/api/dashboards/db"
            return _post(fallback)
        raise

