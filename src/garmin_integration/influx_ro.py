from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from garmin_grafana.influx import InfluxConfig, create_influx_client


@dataclass(frozen=True, slots=True)
class InfluxRO:
    client: Any
    version: str


def create_influx_ro(
    *,
    version: str,
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
    v3_access_token: str = "",
    endpoint_is_http: bool = True,
) -> InfluxRO:
    cfg = InfluxConfig(
        version=version,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        v3_access_token=v3_access_token,
        endpoint_is_http=endpoint_is_http,
    )
    client = create_influx_client(cfg)
    return InfluxRO(client=client, version=version)


def query_influxql_df(ro: InfluxRO, influxql: str) -> pd.DataFrame:
    """InfluxDB v1: execute InfluxQL and return a DataFrame (read-only)."""
    if ro.version != "1":
        raise NotImplementedError("Read-only query currently supports InfluxDB v1 (InfluxQL) only.")
    res = ro.client.query(influxql)
    if not res:
        return pd.DataFrame()
    try:
        points = list(res.get_points())
    except Exception:
        points = list(res)
    return pd.DataFrame(points)
