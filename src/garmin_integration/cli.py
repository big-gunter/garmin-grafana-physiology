from __future__ import annotations

import os

import typer
from dotenv import load_dotenv

from .influx_ro import create_influx_ro
from .schema_export import export_schema_influxql, write_schema_markdown


def _get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _ro_from_env():
    load_dotenv(override=False)
    return create_influx_ro(
        version=os.getenv("INFLUXDB_VERSION", "1"),
        host=os.getenv("INFLUXDB_HOST", "localhost"),
        port=int(os.getenv("INFLUXDB_PORT", "8086")),
        username=os.getenv("INFLUXDB_USERNAME", ""),
        password=os.getenv("INFLUXDB_PASSWORD", ""),
        database=os.getenv("INFLUXDB_DATABASE", "GarminStats"),
        v3_access_token=os.getenv("INFLUXDB_V3_ACCESS_TOKEN", ""),
        endpoint_is_http=_get_bool("INFLUXDB_ENDPOINT_IS_HTTP", True),
    )


def main(
    out: str = typer.Option("docs/schema.influxql.md", help="Output markdown path"),
) -> None:
    """Export InfluxDB v1 measurement/tag/field keys to markdown."""
    ro = _ro_from_env()
    schemas = export_schema_influxql(ro)
    write_schema_markdown(schemas=schemas, out_path=out)
    typer.echo(f"Wrote schema to {out}")


def cli() -> None:
    typer.run(main)


if __name__ == "__main__":
    cli()
