from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pytz


@dataclass(frozen=True, slots=True)
class TrainingFetchContext:
    garmin_obj: Any
    garmin_devicename: str
    influxdb_database: str
    lactate_threshold_sports: list[str]


def get_lactate_threshold(date_str: str, ctx: TrainingFetchContext) -> list[dict]:
    points_list: list[dict] = []
    endpoints: dict[str, str] = {}

    for ltsport in ctx.lactate_threshold_sports:
        endpoints[f"SpeedThreshold_{ltsport}"] = (
            f"/biometric-service/stats/lactateThresholdSpeed/range/{date_str}/{date_str}?aggregation=daily&sport={ltsport}"
        )
        endpoints[f"HeartRateThreshold_{ltsport}"] = (
            f"/biometric-service/stats/lactateThresholdHeartRate/range/{date_str}/{date_str}?aggregation=daily&sport={ltsport}"
        )

    for label, endpoint in endpoints.items():
        lt_list_all = ctx.garmin_obj.connectapi(endpoint) or []
        for lt_dict in lt_list_all:
            value = lt_dict.get("value")
            if value is None:
                continue
            points_list.append(
                {
                    "measurement": "LactateThreshold",
                    "time": datetime.fromtimestamp(datetime.strptime(date_str, "%Y-%m-%d").timestamp(), tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                    "fields": {f"{label}": value},
                }
            )
            logging.info(f"Success : Fetching {label} for date {date_str}")
    return points_list


def get_training_status(date_str: str, ctx: TrainingFetchContext) -> list[dict]:
    points_list: list[dict] = []
    ts_list_all = ctx.garmin_obj.get_training_status(date_str) or {}
    ts_training_data_all = (ts_list_all.get("mostRecentTrainingStatus") or {}).get("latestTrainingStatusData", {}) or {}

    for device_id, ts_dict in ts_training_data_all.items():
        logging.info(f"Success : Processing Training Status for Device {device_id}")
        acute = ts_dict.get("acuteTrainingLoadDTO") or {}
        data_fields = {
            "trainingStatus": ts_dict.get("trainingStatus"),
            "trainingStatusFeedbackPhrase": ts_dict.get("trainingStatusFeedbackPhrase"),
            "weeklyTrainingLoad": ts_dict.get("weeklyTrainingLoad"),
            "fitnessTrend": ts_dict.get("fitnessTrend"),
            "acwrPercent": acute.get("acwrPercent"),
            "dailyTrainingLoadAcute": acute.get("dailyTrainingLoadAcute"),
            "dailyTrainingLoadChronic": acute.get("dailyTrainingLoadChronic"),
            "maxTrainingLoadChronic": acute.get("maxTrainingLoadChronic"),
            "minTrainingLoadChronic": acute.get("minTrainingLoadChronic"),
            "dailyAcuteChronicWorkloadRatio": acute.get("dailyAcuteChronicWorkloadRatio"),
        }
        ts = ts_dict.get("timestamp")
        if ts and any(v is not None for v in data_fields.values()):
            points_list.append(
                {
                    "measurement": "TrainingStatus",
                    "time": datetime.fromtimestamp(ts / 1000, tz=pytz.timezone("UTC")).isoformat(),
                    "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                    "fields": data_fields,
                }
            )
            logging.info(f"Success : Fetching Training Status for date {date_str}")
    return points_list


def get_training_readiness(date_str: str, ctx: TrainingFetchContext) -> list[dict]:
    points_list: list[dict] = []
    tr_list_all = ctx.garmin_obj.get_training_readiness(date_str) or []
    for tr_dict in tr_list_all:
        data_fields = {
            "level": tr_dict.get("level"),
            "score": tr_dict.get("score"),
            "sleepScore": tr_dict.get("sleepScore"),
            "sleepScoreFactorPercent": tr_dict.get("sleepScoreFactorPercent"),
            "recoveryTime": tr_dict.get("recoveryTime"),
            "recoveryTimeFactorPercent": tr_dict.get("recoveryTimeFactorPercent"),
            "acwrFactorPercent": tr_dict.get("acwrFactorPercent"),
            "acuteLoad": tr_dict.get("acuteLoad"),
            "stressHistoryFactorPercent": tr_dict.get("stressHistoryFactorPercent"),
            "hrvFactorPercent": tr_dict.get("hrvFactorPercent"),
        }
        ts = tr_dict.get("timestamp")
        if ts and (not all(v is None for v in data_fields.values())):
            points_list.append(
                {
                    "measurement": "TrainingReadiness",
                    "time": pytz.timezone("UTC").localize(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")).isoformat(),
                    "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                    "fields": data_fields,
                }
            )
            logging.info(f"Success : Fetching Training Readiness for date {date_str}")
    return points_list


def get_hillscore(date_str: str, ctx: TrainingFetchContext) -> list[dict]:
    points_list: list[dict] = []
    hill = ctx.garmin_obj.get_hill_score(date_str) or {}
    data_fields = {
        "strengthScore": hill.get("strengthScore"),
        "enduranceScore": hill.get("enduranceScore"),
        "hillScoreClassificationId": hill.get("hillScoreClassificationId"),
        "overallScore": hill.get("overallScore"),
        "hillScoreFeedbackPhraseId": hill.get("hillScoreFeedbackPhraseId"),
        "vo2MaxPreciseValue": hill.get("vo2MaxPreciseValue"),
    }
    if not all(v is None for v in data_fields.values()):
        points_list.append(
            {
                "measurement": "HillScore",
                "time": datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": data_fields,
            }
        )
        logging.info(f"Success : Fetching Hill Score for date {date_str}")
    return points_list


def get_race_predictions(date_str: str, ctx: TrainingFetchContext) -> list[dict]:
    points_list: list[dict] = []
    rp_all_list = ctx.garmin_obj.get_race_predictions(startdate=date_str, enddate=date_str, _type="daily") or []
    rp_all = rp_all_list[0] if len(rp_all_list) > 0 else {}
    if rp_all:
        data_fields = {
            "time5K": rp_all.get("time5K"),
            "time10K": rp_all.get("time10K"),
            "timeHalfMarathon": rp_all.get("timeHalfMarathon"),
            "timeMarathon": rp_all.get("timeMarathon"),
        }
        if not all(v is None for v in data_fields.values()):
            points_list.append(
                {
                    "measurement": "RacePredictions",
                    "time": datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(),
                    "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                    "fields": data_fields,
                }
            )
            logging.info(f"Success : Fetching Race Predictions for date {date_str}")
    return points_list


def get_fitness_age(date_str: str, ctx: TrainingFetchContext) -> list[dict]:
    points_list: list[dict] = []
    fitness_age = ctx.garmin_obj.get_fitnessage_data(date_str) or {}
    if fitness_age:
        data_fields = {
            "chronologicalAge": float(fitness_age.get("chronologicalAge")) if fitness_age.get("chronologicalAge") else None,
            "fitnessAge": fitness_age.get("fitnessAge"),
            "achievableFitnessAge": fitness_age.get("achievableFitnessAge"),
        }
        if not all(v is None for v in data_fields.values()):
            points_list.append(
                {
                    "measurement": "FitnessAge",
                    "time": datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(),
                    "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                    "fields": data_fields,
                }
            )
            logging.info(f"Success : Fetching Fitness Age for date {date_str}")
    return points_list


def get_vo2_max(date_str: str, ctx: TrainingFetchContext) -> list[dict]:
    points_list: list[dict] = []
    max_metrics = ctx.garmin_obj.get_max_metrics(date_str)
    try:
        if max_metrics:
            vo2_max_value = (max_metrics[0].get("generic") or {}).get("vo2MaxPreciseValue", None)
            vo2_max_value_cycling = (max_metrics[0].get("cycling") or {}).get("vo2MaxPreciseValue", None)
            if (vo2_max_value is not None) or (vo2_max_value_cycling is not None):
                points_list.append(
                    {
                        "measurement": "VO2_Max",
                        "time": datetime.strptime(date_str, "%Y-%m-%d").replace(hour=0, tzinfo=pytz.UTC).isoformat(),
                        "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                        "fields": {"VO2_max_value": vo2_max_value, "VO2_max_value_cycling": vo2_max_value_cycling},
                    }
                )
                logging.info(f"Success : Fetching VO2-max for date {date_str}")
        return points_list
    except AttributeError:
        return []


def get_endurance_score(date_str: str, ctx: TrainingFetchContext) -> list[dict]:
    points_list: list[dict] = []
    endurance_dict = ctx.garmin_obj.get_endurance_score(date_str) or {}
    if endurance_dict.get("overallScore") is not None:
        points_list.append(
            {
                "measurement": "EnduranceScore",
                "time": pytz.timezone("UTC").localize(datetime.strptime(date_str, "%Y-%m-%d")).isoformat(),
                "tags": {"Device": ctx.garmin_devicename, "Database_Name": ctx.influxdb_database},
                "fields": {"EnduranceScore": endurance_dict.get("overallScore")},
            }
        )
        logging.info(f"Success : Fetching Endurance Score for date {date_str}")
    return points_list

