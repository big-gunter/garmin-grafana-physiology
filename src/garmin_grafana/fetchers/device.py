from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pytz


@dataclass(frozen=True, slots=True)
class DeviceFetchContext:
    garmin_obj: Any
    influxdb_database: str
    garmin_devicename_automatic: bool
    current_garmin_devicename: str


def get_last_sync(ctx: DeviceFetchContext) -> tuple[list[dict], str, str | None]:
    """
    Returns (points, updated_device_name, updated_device_id).
    Mirrors the original behavior: when devicename automatic is enabled, update name/id from sync payload.
    """
    points_list: list[dict] = []
    sync_data = ctx.garmin_obj.get_device_last_used() or {}

    device_name = ctx.current_garmin_devicename
    device_id: str | None = None
    if ctx.garmin_devicename_automatic:
        device_name = sync_data.get("lastUsedDeviceName") or "Unknown"
        device_id = sync_data.get("userDeviceId") or None

    ts = sync_data.get("lastUsedDeviceUploadTime")
    if ts:
        points_list.append(
            {
                "measurement": "DeviceSync",
                "time": datetime.fromtimestamp(ts / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": device_name, "Database_Name": ctx.influxdb_database},
                "fields": {"imageUrl": sync_data.get("imageUrl"), "Device_Name": device_name},
            }
        )

    if points_list:
        logging.info("Success : Updated device last sync time")
    else:
        logging.warning("No associated/synced Garmin device found with your account")

    return points_list, device_name, device_id

