from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pytz


@dataclass(frozen=True, slots=True)
class HealthFetchContext:
    garmin_obj: Any
    garmin_devicename: str
    influxdb_database: str


def get_body_composition(date_str: str, ctx: HealthFetchContext) -> list[dict]:
    points_list: list[dict] = []
    payload = ctx.garmin_obj.get_weigh_ins(date_str, date_str) or {}
    weight_list_all = payload.get("dailyWeightSummaries", []) or []
    if weight_list_all:
        weight_list = (weight_list_all[0] or {}).get("allWeightMetrics", []) or []
        for weight_dict in weight_list:
            data_fields = {
                "weight": weight_dict.get("weight"),
                "bmi": weight_dict.get("bmi"),
                "bodyFat": weight_dict.get("bodyFat"),
                "bodyWater": weight_dict.get("bodyWater"),
                "boneMass": weight_dict.get("boneMass"),
                "muscleMass": weight_dict.get("muscleMass"),
                "physiqueRating": weight_dict.get("physiqueRating"),
                "visceralFat": weight_dict.get("visceralFat"),
            }
            if all(value is None for value in data_fields.values()):
                continue

            ts_gmt = weight_dict.get("timestampGMT")
            if ts_gmt:
                t = datetime.fromtimestamp((ts_gmt / 1000), tz=pytz.timezone("UTC")).isoformat()
            else:
                t = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat()

            points_list.append(
                {
                    "measurement": "BodyComposition",
                    "time": t,
                    "tags": {
                        "Device": ctx.garmin_devicename,
                        "Database_Name": ctx.influxdb_database,
                        "Frequency": "Intraday",
                        "SourceType": weight_dict.get("sourceType", "Unknown"),
                    },
                    "fields": data_fields,
                }
            )
        logging.info(f"Success : Fetching intraday Body Composition (Weight, BMI etc) for date {date_str}")
    return points_list


def get_blood_pressure(date_str: str, ctx: HealthFetchContext) -> list[dict]:
    points_list: list[dict] = []
    bp_all = (ctx.garmin_obj.get_blood_pressure(date_str, date_str) or {}).get("measurementSummaries", []) or []
    if len(bp_all) > 0:
        bp_list = (bp_all[0] or {}).get("measurements", []) or []
        for bp_measurement in bp_list:
            data_fields = {
                "Systolic": bp_measurement.get("systolic", None),
                "Diastolic": bp_measurement.get("diastolic", None),
                "Pulse": bp_measurement.get("pulse", None),
            }
            ts = bp_measurement.get("measurementTimestampGMT")
            if ts and (not all(v is None for v in data_fields.values())):
                points_list.append(
                    {
                        "measurement": "BloodPressure",
                        "time": pytz.UTC.localize(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                        "tags": {
                            "Device": ctx.garmin_devicename,
                            "Database_Name": ctx.influxdb_database,
                            "Source": bp_measurement.get("sourceType", None),
                        },
                        "fields": data_fields,
                    }
                )
        logging.info(f"Success : Fetching Blood Pressure for date {date_str}")
    return points_list


def get_hydration(date_str: str, ctx: HealthFetchContext) -> list[dict]:
    points_list: list[dict] = []
    hydration_dict = ctx.garmin_obj.get_hydration_data(date_str) or {}
    data_fields = {
        "ValueInML": hydration_dict.get("valueInML", None),
        "SweatLossInML": hydration_dict.get("sweatLossInML", None),
        "GoalInML": hydration_dict.get("goalInML", None),
        "ActivityIntakeInML": hydration_dict.get("activityIntakeInML", None),
    }
    if not all(v is None for v in data_fields.values()):
        points_list.append(
            {
                "measurement": "Hydration",
                "time": datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": data_fields,
            }
        )
        logging.info(f"Success : Fetching Hydration for date {date_str}")
    return points_list

