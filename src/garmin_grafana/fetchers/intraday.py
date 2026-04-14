from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pytz


@dataclass(frozen=True, slots=True)
class IntradayFetchContext:
    garmin_obj: Any
    garmin_devicename: str
    influxdb_database: str


def get_intraday_hr(date_str: str, ctx: IntradayFetchContext) -> list[dict]:
    points_list: list[dict] = []
    hr_list = (ctx.garmin_obj.get_heart_rates(date_str) or {}).get("heartRateValues") or []
    for entry in hr_list:
        if len(entry) < 2:
            continue
        ts_ms, hr = entry[0], entry[1]
        if hr is None:
            continue
        points_list.append(
            {
                "measurement": "HeartRateIntraday",
                "time": datetime.fromtimestamp(ts_ms / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"HeartRate": hr},
            }
        )
    if points_list:
        logging.info(f"Success : Fetching intraday Heart Rate for date {date_str}")
    return points_list


def get_intraday_steps(date_str: str, ctx: IntradayFetchContext) -> list[dict]:
    points_list: list[dict] = []
    steps_list = ctx.garmin_obj.get_steps_data(date_str) or []
    for entry in steps_list:
        steps = entry.get("steps")
        if steps is None:
            continue
        start_gmt = entry.get("startGMT")
        if not start_gmt:
            continue
        points_list.append(
            {
                "measurement": "StepsIntraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(start_gmt, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"StepsCount": steps},
            }
        )
    if points_list:
        logging.info(f"Success : Fetching intraday steps for date {date_str}")
    return points_list


def get_intraday_stress(date_str: str, ctx: IntradayFetchContext) -> list[dict]:
    points_list: list[dict] = []
    stress_payload = ctx.garmin_obj.get_stress_data(date_str) or {}

    stress_list = stress_payload.get("stressValuesArray") or []
    for entry in stress_list:
        if len(entry) < 2:
            continue
        ts_ms, stress_val = entry[0], entry[1]
        if stress_val is None:
            continue
        points_list.append(
            {
                "measurement": "StressIntraday",
                "time": datetime.fromtimestamp(ts_ms / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"stressLevel": stress_val},
            }
        )

    bb_list = stress_payload.get("bodyBatteryValuesArray") or []
    for entry in bb_list:
        if len(entry) < 3:
            continue
        ts_ms, _, bb_val = entry[0], entry[1], entry[2]
        if bb_val is None:
            continue
        points_list.append(
            {
                "measurement": "BodyBatteryIntraday",
                "time": datetime.fromtimestamp(ts_ms / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"BodyBatteryLevel": bb_val},
            }
        )

    if points_list:
        logging.info(f"Success : Fetching intraday stress and Body Battery values for date {date_str}")
    return points_list


def get_intraday_br(date_str: str, ctx: IntradayFetchContext) -> list[dict]:
    points_list: list[dict] = []
    br_list = (ctx.garmin_obj.get_respiration_data(date_str) or {}).get("respirationValuesArray") or []
    for entry in br_list:
        if len(entry) < 2:
            continue
        ts_ms, br = entry[0], entry[1]
        if br is None:
            continue
        points_list.append(
            {
                "measurement": "BreathingRateIntraday",
                "time": datetime.fromtimestamp(ts_ms / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"BreathingRate": br},
            }
        )
    if points_list:
        logging.info(f"Success : Fetching intraday Breathing Rate for date {date_str}")
    return points_list


def get_intraday_hrv(date_str: str, ctx: IntradayFetchContext) -> list[dict]:
    points_list: list[dict] = []
    hrv_list = (ctx.garmin_obj.get_hrv_data(date_str) or {}).get("hrvReadings") or []
    for entry in hrv_list:
        v = entry.get("hrvValue")
        ts = entry.get("readingTimeGMT")
        if v is None or not ts:
            continue
        points_list.append(
            {
                "measurement": "HRV_Intraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"hrvValue": v},
            }
        )
    if points_list:
        logging.info(f"Success : Fetching intraday HRV for date {date_str}")
    return points_list

