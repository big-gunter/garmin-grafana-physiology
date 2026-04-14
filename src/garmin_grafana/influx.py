from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

import pytz
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
from influxdb_client_3 import InfluxDBClient3, InfluxDBError


@dataclass(frozen=True, slots=True)
class InfluxConfig:
    version: str  # "1" or "3"
    host: str
    port: int
    username: str
    password: str
    database: str
    v3_access_token: str
    endpoint_is_http: bool


def create_influx_client(cfg: InfluxConfig) -> Any:
    try:
        if cfg.endpoint_is_http:
            if cfg.version == "1":
                client = InfluxDBClient(host=cfg.host, port=cfg.port, username=cfg.username, password=cfg.password)
                client.switch_database(cfg.database)
            else:
                client = InfluxDBClient3(
                    host=f"http://{cfg.host}:{cfg.port}",
                    token=cfg.v3_access_token,
                    database=cfg.database,
                )
        else:
            if cfg.version == "1":
                client = InfluxDBClient(
                    host=cfg.host,
                    port=cfg.port,
                    username=cfg.username,
                    password=cfg.password,
                    ssl=True,
                    verify_ssl=True,
                )
                client.switch_database(cfg.database)
            else:
                client = InfluxDBClient3(
                    host=f"https://{cfg.host}:{cfg.port}",
                    token=cfg.v3_access_token,
                    database=cfg.database,
                )

        demo_point = {
            "measurement": "DemoPoint",
            "time": (datetime.now(pytz.utc) - timedelta(minutes=1)).isoformat(timespec="seconds"),
            "tags": {"DemoTag": "DemoTagValue"},
            "fields": {"DemoField": 0},
        }
        if cfg.version == "1":
            client.write_points([demo_point])
        else:
            client.write(record=[demo_point])
        return client
    except (InfluxDBClientError, InfluxDBError) as err:
        logging.error("Unable to connect with influxdb database! Aborted")
        raise InfluxDBClientError("InfluxDB connection failed:" + str(err))


def clean_point(point: dict) -> dict | None:
    if not isinstance(point, dict):
        return None
    fields = point.get("fields") or {}
    if not isinstance(fields, dict):
        fields = {}
    fields = {k: v for k, v in fields.items() if v is not None}
    if not fields:
        return None
    point["fields"] = fields

    tags = point.get("tags") or {}
    if not isinstance(tags, dict):
        tags = {}
    tags = {str(k): ("" if v is None else str(v)) for k, v in tags.items()}
    point["tags"] = tags
    return point


def write_points(
    *,
    client: Any,
    influx_version: str,
    points: list[dict] | None,
    tag_measurements_with_user_email: bool,
    get_user_id: Callable[[], str],
    write_chunk_size: int = 20000,
) -> None:
    try:
        if not points:
            return

        cleaned: list[dict] = []
        for p in points:
            cp = clean_point(p)
            if cp is not None:
                cleaned.append(cp)
        if not cleaned:
            return

        if tag_measurements_with_user_email:
            user_id = "Unknown"
            try:
                user_id = get_user_id() or "Unknown"
            except Exception:
                pass
            for item in cleaned:
                item.setdefault("tags", {})
                item["tags"].update({"User_ID": user_id})

        for i in range(0, len(cleaned), write_chunk_size):
            batch = cleaned[i : i + write_chunk_size]
            if influx_version == "1":
                client.write_points(batch)
            else:
                client.write(record=batch)

        logging.info("Success : updated influxDB database with new points")
    except (InfluxDBClientError, InfluxDBError) as err:
        logging.error("Write failed : Unable to connect with database! " + str(err))

