from __future__ import annotations

import json

import typer
from rich import print

from .config import load_config
from .influx_ro import create_influx_ro
from .readiness import build_snapshot, readiness_score


app = typer.Typer(add_completion=False)


def _client():
    cfg = load_config()
    ro = create_influx_ro(
        version=cfg.influx_version,
        host=cfg.influx_host,
        port=cfg.influx_port,
        username=cfg.influx_username,
        password=cfg.influx_password,
        database=cfg.influx_database,
        endpoint_is_http=cfg.influx_endpoint_is_http,
    )
    return cfg, ro


@app.command()
def snapshot(window_days: int = typer.Option(42, min=1, max=365)):
    """Print a physiology/training-load snapshot (JSON) derived from the DB."""
    _, ro = _client()
    snap = build_snapshot(ro, window_days=window_days)
    print(json.dumps(snap.to_dict(), indent=2, sort_keys=True))


@app.command()
def readiness(window_days: int = typer.Option(42, min=1, max=365)):
    """Print readiness score (JSON) derived from the DB."""
    _, ro = _client()
    snap = build_snapshot(ro, window_days=window_days)
    out = {"snapshot": snap.to_dict(), "readiness": readiness_score(snap)}
    print(json.dumps(out, indent=2, sort_keys=True))


@app.command()
def serve():
    """Run the agent HTTP API server."""
    from .server import main

    main()


if __name__ == "__main__":
    app()

