from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pytz


@dataclass(frozen=True, slots=True)
class DailyFetchContext:
    garmin_obj: Any
    garmin_devicename: str
    influxdb_database: str


def get_daily_stats(date_str: str, ctx: DailyFetchContext) -> list[dict]:
    points_list: list[dict] = []
    stats_json = ctx.garmin_obj.get_stats(date_str) or {}
    wellness_start = stats_json.get("wellnessStartTimeGmt")

    if wellness_start and datetime.strptime(date_str, "%Y-%m-%d") < datetime.today():
        points_list.append(
            {
                "measurement": "DailyStats",
                "time": pytz.timezone("UTC").localize(datetime.strptime(wellness_start, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {
                    "activeKilocalories": stats_json.get("activeKilocalories"),
                    "bmrKilocalories": stats_json.get("bmrKilocalories"),
                    "totalSteps": stats_json.get("totalSteps"),
                    "totalDistanceMeters": stats_json.get("totalDistanceMeters"),
                    "highlyActiveSeconds": stats_json.get("highlyActiveSeconds"),
                    "activeSeconds": stats_json.get("activeSeconds"),
                    "sedentarySeconds": stats_json.get("sedentarySeconds"),
                    "sleepingSeconds": stats_json.get("sleepingSeconds"),
                    "moderateIntensityMinutes": stats_json.get("moderateIntensityMinutes"),
                    "vigorousIntensityMinutes": stats_json.get("vigorousIntensityMinutes"),
                    "floorsAscendedInMeters": stats_json.get("floorsAscendedInMeters"),
                    "floorsDescendedInMeters": stats_json.get("floorsDescendedInMeters"),
                    "floorsAscended": stats_json.get("floorsAscended"),
                    "floorsDescended": stats_json.get("floorsDescended"),
                    "minHeartRate": stats_json.get("minHeartRate"),
                    "maxHeartRate": stats_json.get("maxHeartRate"),
                    "restingHeartRate": stats_json.get("restingHeartRate"),
                    "minAvgHeartRate": stats_json.get("minAvgHeartRate"),
                    "maxAvgHeartRate": stats_json.get("maxAvgHeartRate"),
                    "avgSkinTempDeviationC": stats_json.get("avgSkinTempDeviationC"),
                    "avgSkinTempDeviationF": stats_json.get("avgSkinTempDeviationF"),
                    "stressDuration": stats_json.get("stressDuration"),
                    "restStressDuration": stats_json.get("restStressDuration"),
                    "activityStressDuration": stats_json.get("activityStressDuration"),
                    "uncategorizedStressDuration": stats_json.get("uncategorizedStressDuration"),
                    "totalStressDuration": stats_json.get("totalStressDuration"),
                    "lowStressDuration": stats_json.get("lowStressDuration"),
                    "mediumStressDuration": stats_json.get("mediumStressDuration"),
                    "highStressDuration": stats_json.get("highStressDuration"),
                    "stressPercentage": stats_json.get("stressPercentage"),
                    "restStressPercentage": stats_json.get("restStressPercentage"),
                    "activityStressPercentage": stats_json.get("activityStressPercentage"),
                    "uncategorizedStressPercentage": stats_json.get("uncategorizedStressPercentage"),
                    "lowStressPercentage": stats_json.get("lowStressPercentage"),
                    "mediumStressPercentage": stats_json.get("mediumStressPercentage"),
                    "highStressPercentage": stats_json.get("highStressPercentage"),
                    "bodyBatteryChargedValue": stats_json.get("bodyBatteryChargedValue"),
                    "bodyBatteryDrainedValue": stats_json.get("bodyBatteryDrainedValue"),
                    "bodyBatteryHighestValue": stats_json.get("bodyBatteryHighestValue"),
                    "bodyBatteryLowestValue": stats_json.get("bodyBatteryLowestValue"),
                    "bodyBatteryDuringSleep": stats_json.get("bodyBatteryDuringSleep"),
                    "bodyBatteryAtWakeTime": stats_json.get("bodyBatteryAtWakeTime"),
                    "averageSpo2": stats_json.get("averageSpo2"),
                    "lowestSpo2": stats_json.get("lowestSpo2"),
                },
            }
        )
        if points_list:
            logging.info(f"Success : Fetching daily metrics for date {date_str}")
        return points_list

    logging.debug("No daily stat data available for the give date " + date_str)
    return []


def get_sleep_data(date_str: str, ctx: DailyFetchContext) -> list[dict]:
    points_list: list[dict] = []
    all_sleep_data = ctx.garmin_obj.get_sleep_data(date_str) or {}
    sleep_json = all_sleep_data.get("dailySleepDTO") or {}

    if sleep_json.get("sleepEndTimestampGMT"):
        sleep_scores = (sleep_json.get("sleepScores") or {}).get("overall") or {}
        points_list.append(
            {
                "measurement": "SleepSummary",
                "time": datetime.fromtimestamp(sleep_json["sleepEndTimestampGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {
                    "sleepTimeSeconds": sleep_json.get("sleepTimeSeconds"),
                    "deepSleepSeconds": sleep_json.get("deepSleepSeconds"),
                    "lightSleepSeconds": sleep_json.get("lightSleepSeconds"),
                    "remSleepSeconds": sleep_json.get("remSleepSeconds"),
                    "awakeSleepSeconds": sleep_json.get("awakeSleepSeconds"),
                    "averageSpO2Value": sleep_json.get("averageSpO2Value"),
                    "lowestSpO2Value": sleep_json.get("lowestSpO2Value"),
                    "highestSpO2Value": sleep_json.get("highestSpO2Value"),
                    "averageRespirationValue": sleep_json.get("averageRespirationValue"),
                    "lowestRespirationValue": sleep_json.get("lowestRespirationValue"),
                    "highestRespirationValue": sleep_json.get("highestRespirationValue"),
                    "awakeCount": sleep_json.get("awakeCount"),
                    "avgSleepStress": sleep_json.get("avgSleepStress"),
                    "sleepScore": sleep_scores.get("value"),
                    "restlessMomentsCount": all_sleep_data.get("restlessMomentsCount"),
                    "avgOvernightHrv": all_sleep_data.get("avgOvernightHrv"),
                    "bodyBatteryChange": all_sleep_data.get("bodyBatteryChange"),
                    "restingHeartRate": all_sleep_data.get("restingHeartRate"),
                    "avgSkinTempDeviationC": all_sleep_data.get("avgSkinTempDeviationC"),
                    "avgSkinTempDeviationF": all_sleep_data.get("avgSkinTempDeviationF"),
                },
            }
        )

    sleep_movement_intraday = all_sleep_data.get("sleepMovement") or []
    for entry in sleep_movement_intraday:
        start_gmt = entry.get("startGMT")
        end_gmt = entry.get("endGMT")
        if not start_gmt or not end_gmt:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(start_gmt, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {
                    "SleepMovementActivityLevel": entry.get("activityLevel", -1),
                    "SleepMovementActivitySeconds": int(
                        (
                            datetime.strptime(end_gmt, "%Y-%m-%dT%H:%M:%S.%f")
                            - datetime.strptime(start_gmt, "%Y-%m-%dT%H:%M:%S.%f")
                        ).total_seconds()
                    ),
                },
            }
        )

    sleep_levels_intraday = all_sleep_data.get("sleepLevels") or []
    last_level_entry = None
    for entry in sleep_levels_intraday:
        last_level_entry = entry
        start_gmt = entry.get("startGMT")
        end_gmt = entry.get("endGMT")
        if not start_gmt or not end_gmt:
            continue
        if entry.get("activityLevel") is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(start_gmt, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {
                    "SleepStageLevel": entry.get("activityLevel"),
                    "SleepStageSeconds": int(
                        (
                            datetime.strptime(end_gmt, "%Y-%m-%dT%H:%M:%S.%f")
                            - datetime.strptime(start_gmt, "%Y-%m-%dT%H:%M:%S.%f")
                        ).total_seconds()
                    ),
                },
            }
        )

    if last_level_entry and last_level_entry.get("endGMT"):
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(last_level_entry["endGMT"], "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"SleepStageLevel": last_level_entry.get("activityLevel")},
            }
        )

    sleep_restlessness_intraday = all_sleep_data.get("sleepRestlessMoments") or []
    for entry in sleep_restlessness_intraday:
        v = entry.get("value")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": datetime.fromtimestamp(entry["startGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"sleepRestlessValue": v},
            }
        )

    sleep_spo2_intraday = all_sleep_data.get("wellnessEpochSPO2DataDTOList") or []
    for entry in sleep_spo2_intraday:
        v = entry.get("spo2Reading")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": pytz.timezone("UTC").localize(datetime.strptime(entry["epochTimestamp"], "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"spo2Reading": v},
            }
        )

    sleep_respiration_intraday = all_sleep_data.get("wellnessEpochRespirationDataDTOList") or []
    for entry in sleep_respiration_intraday:
        v = entry.get("respirationValue")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": datetime.fromtimestamp(entry["startTimeGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"respirationValue": v},
            }
        )

    sleep_heart_rate_intraday = all_sleep_data.get("sleepHeartRate") or []
    for entry in sleep_heart_rate_intraday:
        v = entry.get("value")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": datetime.fromtimestamp(entry["startGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"heartRate": v},
            }
        )

    sleep_stress_intraday = all_sleep_data.get("sleepStress") or []
    for entry in sleep_stress_intraday:
        v = entry.get("value")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": datetime.fromtimestamp(entry["startGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"stressValue": v},
            }
        )

    sleep_bb_intraday = all_sleep_data.get("sleepBodyBattery") or []
    for entry in sleep_bb_intraday:
        v = entry.get("value")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": datetime.fromtimestamp(entry["startGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"bodyBattery": v},
            }
        )

    sleep_hrv_intraday = all_sleep_data.get("hrvData") or []
    for entry in sleep_hrv_intraday:
        v = entry.get("value")
        if v is None:
            continue
        points_list.append(
            {
                "measurement": "SleepIntraday",
                "time": datetime.fromtimestamp(entry["startGMT"] / 1000, tz=pytz.timezone("UTC")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"hrvData": v},
            }
        )

    if points_list:
        logging.info(f"Success : Fetching intraday sleep metrics for date {date_str}")
    return points_list

