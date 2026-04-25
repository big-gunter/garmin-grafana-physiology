from __future__ import annotations

from dataclasses import dataclass
import os

from dotenv import load_dotenv


def _get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True, slots=True)
class AgentConfig:
    # Agent server
    host: str
    port: int
    auth_token: str | None

    # Influx
    influx_version: str
    influx_host: str
    influx_port: int
    influx_username: str
    influx_password: str
    influx_database: str
    influx_endpoint_is_http: bool
    allow_db_write: bool
    cycling_gross_efficiency: float

    # Anthropic
    anthropic_api_key: str | None
    analysis_model: str
    planning_model: str

    # Grafana
    grafana_url: str
    grafana_api_token: str | None


def load_config() -> AgentConfig:
    # allow local dev via dotenv; Docker compose can still pass env directly
    load_dotenv(override=False)

    host = os.getenv("AGENT_HOST", "0.0.0.0")
    port = int(os.getenv("AGENT_PORT", "8000"))

    auth_token = os.getenv("AGENT_AUTH_TOKEN") or None

    influx_version = os.getenv("INFLUXDB_VERSION", "1")
    influx_host = os.getenv("INFLUXDB_HOST", "influxdb")
    influx_port = int(os.getenv("INFLUXDB_PORT", "8086"))
    influx_username = os.getenv("INFLUXDB_USERNAME", "")
    influx_password = os.getenv("INFLUXDB_PASSWORD", "")
    influx_database = os.getenv("INFLUXDB_DATABASE", "GarminStats")
    influx_endpoint_is_http = _get_bool("INFLUXDB_ENDPOINT_IS_HTTP", True)
    allow_db_write = _get_bool("AI_ALLOW_DB_WRITE", False)
    cycling_gross_efficiency = float(os.getenv("AI_CYCLING_GROSS_EFF", "0.23"))

    anthropic_api_key = os.getenv("ANTHROPIC_API_KEY") or None
    analysis_model = os.getenv("ANALYSIS_MODEL", "claude-sonnet-4-6")
    planning_model = os.getenv("PLANNING_MODEL", "claude-opus-4-6")

    grafana_url = os.getenv("GRAFANA_URL", "http://grafana:3000")
    grafana_api_token = os.getenv("GRAFANA_API_TOKEN") or None

    if influx_version not in {"1", "3"}:
        raise ValueError("INFLUXDB_VERSION must be '1' or '3'")
    if not influx_host:
        raise ValueError("INFLUXDB_HOST is required")
    if not influx_database:
        raise ValueError("INFLUXDB_DATABASE is required")

    return AgentConfig(
        host=host,
        port=port,
        auth_token=auth_token,
        influx_version=influx_version,
        influx_host=influx_host,
        influx_port=influx_port,
        influx_username=influx_username,
        influx_password=influx_password,
        influx_database=influx_database,
        influx_endpoint_is_http=influx_endpoint_is_http,
        allow_db_write=allow_db_write,
        cycling_gross_efficiency=cycling_gross_efficiency,
        anthropic_api_key=anthropic_api_key,
        analysis_model=analysis_model,
        planning_model=planning_model,
        grafana_url=grafana_url,
        grafana_api_token=grafana_api_token,
    )

