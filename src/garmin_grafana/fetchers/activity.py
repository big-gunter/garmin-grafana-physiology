from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

import pytz


@dataclass(frozen=True, slots=True)
class ActivityFetchContext:
    garmin_obj: Any
    garmin_devicename: str
    influxdb_database: str
    always_process_fit_files: bool
    norm_tag_value: Callable[[object], str | None]


def get_activity_summary(date_str: str, ctx: ActivityFetchContext) -> tuple[list[dict], dict]:
    points_list: list[dict] = []
    activity_with_gps_id_dict: dict = {}
    activity_list = ctx.garmin_obj.get_activities_by_date(date_str, date_str) or []
    for activity in activity_list:
        act_id = activity.get("activityId")
        act_type = (activity.get("activityType") or {}).get("typeKey", "Unknown")

        if activity.get("hasPolyline") or ctx.always_process_fit_files:
            if not activity.get("hasPolyline"):
                logging.warning(
                    f"Activity ID {act_id} got no GPS data - yet, activity FIT file data will be processed as ALWAYS_PROCESS_FIT_FILES is on"
                )
            activity_with_gps_id_dict[act_id] = act_type

        if "startTimeGMT" in activity:
            start_dt = datetime.strptime(activity["startTimeGMT"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
            selector = start_dt.strftime("%Y%m%dT%H%M%SUTC-") + act_type

            points_list.append(
                {
                    "measurement": "ActivitySummary",
                    "time": start_dt.isoformat(),
                    "tags": {
                        "Device": ctx.garmin_devicename,
                        "Database_Name": ctx.influxdb_database,
                        "ActivityID": act_id,
                        "ActivitySelector": selector,
                        "activity_type_tag": ctx.norm_tag_value(act_type),
                    },
                    "fields": {
                        "Activity_ID": act_id,
                        "Device_ID": activity.get("deviceId"),
                        "activityName": activity.get("activityName"),
                        "description": activity.get("description"),
                        "activityType": act_type if act_type != "Unknown" else None,
                        "distance": activity.get("distance"),
                        "elapsedDuration": activity.get("elapsedDuration"),
                        "movingDuration": activity.get("movingDuration"),
                        "averageSpeed": activity.get("averageSpeed"),
                        "maxSpeed": activity.get("maxSpeed"),
                        "calories": activity.get("calories"),
                        "bmrCalories": activity.get("bmrCalories"),
                        "averageHR": activity.get("averageHR"),
                        "maxHR": activity.get("maxHR"),
                        "locationName": activity.get("locationName"),
                        "lapCount": activity.get("lapCount"),
                        "hrTimeInZone_1": int(val) if (val := activity.get("hrTimeInZone_1")) is not None else None,
                        "hrTimeInZone_2": int(val) if (val := activity.get("hrTimeInZone_2")) is not None else None,
                        "hrTimeInZone_3": int(val) if (val := activity.get("hrTimeInZone_3")) is not None else None,
                        "hrTimeInZone_4": int(val) if (val := activity.get("hrTimeInZone_4")) is not None else None,
                        "hrTimeInZone_5": int(val) if (val := activity.get("hrTimeInZone_5")) is not None else None,
                    },
                }
            )

            end_dt = start_dt + timedelta(seconds=int(activity.get("elapsedDuration", 0) or 0))
            points_list.append(
                {
                    "measurement": "ActivitySummary",
                    "time": end_dt.isoformat(),
                    "tags": {
                        "Device": ctx.garmin_devicename,
                        "Database_Name": ctx.influxdb_database,
                        "ActivityID": act_id,
                        "ActivitySelector": selector,
                        "activity_type_tag": ctx.norm_tag_value(act_type),
                    },
                    "fields": {
                        "Activity_ID": act_id,
                        "Device_ID": activity.get("deviceId"),
                        "activityName": "END",
                        "activityType": "No Activity",
                    },
                }
            )

            logging.info(f"Success : Fetching Activity summary with id {act_id} for date {date_str}")
        else:
            logging.warning(f"Skipped : Start Timestamp missing for activity id {act_id} for date {date_str}")

    return points_list, activity_with_gps_id_dict

