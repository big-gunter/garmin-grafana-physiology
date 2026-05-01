from __future__ import annotations

import json

import typer
from rich import print

from .config import load_config
from .influx_ro import create_influx_ro
from .readiness import build_snapshot, readiness_score
from .anthropic_client import LLMConfig, create_client, summarize_readiness


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


@app.command()
def insights(
    window_days: int = typer.Option(42, min=1, max=365),
    prompt: str = typer.Option("", help="Optional context/question"),
    json_out: bool = typer.Option(False, "--json", help="Print full JSON payload (snapshot + readiness + insights)"),
):
    """Print a concise AI summary from DB-derived snapshot + readiness."""
    cfg, ro = _client()
    if not cfg.anthropic_api_key:
        raise typer.BadParameter("ANTHROPIC_API_KEY is not configured")
    snap = build_snapshot(ro, window_days=window_days)
    score = readiness_score(snap)
    client = create_client(
        LLMConfig(
            api_key=cfg.anthropic_api_key,
            analysis_model=cfg.analysis_model,
            planning_model=cfg.planning_model,
        )
    )
    text = summarize_readiness(
        client=client,
        model=cfg.analysis_model,
        snapshot=snap.to_dict(),
        readiness=score,
        user_prompt=prompt or None,
    )
    if json_out:
        out = {"snapshot": snap.to_dict(), "readiness": score, "insights": text}
        print(json.dumps(out, indent=2, sort_keys=True))
    else:
        # Default: print only the narrative to avoid UI truncation.
        print(text)


@app.command()
def export_schema(out: str = typer.Option("docs/schema.influxql.md", help="Output markdown path")):
    """Export InfluxDB v1 measurement/tag/field keys to markdown."""
    _, ro = _client()
    from .schema_export import export_schema_influxql, write_schema_markdown

    schemas = export_schema_influxql(ro)
    write_schema_markdown(schemas=schemas, out_path=out)
    print(f"Wrote schema to {out}")


if __name__ == "__main__":
    app()

