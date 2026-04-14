from __future__ import annotations

import io
import logging
import os
import zipfile
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable
import xml.etree.ElementTree as ET

import pytz
import requests
from fitparse import FitFile, FitParseError


@dataclass(slots=True)
class ActivityGPSContext:
    garmin_obj: Any
    parsed_activity_id_list: list[str]
    force_reprocess_activities: bool
    keep_fit_files: bool
    fit_file_storage_location: str

    garmin_devicename: str
    influxdb_database: str

    norm_tag_value: Callable[[object], str | None]
    norm_gender: Callable[[object], str]
    birth_year_from_any: Callable[[object], int | None]
    stored_birth_year_v1: Callable[[], int | None]
    get_birth_year_from_garmin_profile: Callable[[], int | None]

    write_points_to_influxdb: Callable[[list[dict]], None]
    write_user_profile_point: Callable[[str, str | None, int | None], list[dict]]
    maybe_update_userprofile_master: Callable[..., None]
    derive_and_write_activity_metrics_v1: Callable[..., None]

    fit_gender_cache: dict[str, str]


def fetch_activity_GPS(activityIDdict: dict[str, str], ctx: ActivityGPSContext) -> list[dict]:
    """
    Fetch and parse detailed Activity GPS data.

    Prefers FIT download; falls back to TCX when FIT unavailable or parse fails.
    Returns a list of Influx points.
    """
    points_list: list[dict] = []

    for activityID, activity_type in activityIDdict.items():
        if (activityID in ctx.parsed_activity_id_list) and (not ctx.force_reprocess_activities):
            logging.info(f"Skipping : Activity ID {activityID} has already been processed within current runtime")
            continue
        if (activityID in ctx.parsed_activity_id_list) and ctx.force_reprocess_activities:
            logging.info(f"Re-processing : Activity ID {activityID} (FORCE_REPROCESS_ACTIVITIES is on)")

        try:
            zip_data = ctx.garmin_obj.download_activity(activityID, dl_fmt=ctx.garmin_obj.ActivityDownloadFormat.ORIGINAL)
            logging.info(f"Processing : Activity ID {activityID} FIT file data - this may take a while...")
            zip_buffer = io.BytesIO(zip_data)

            with zipfile.ZipFile(zip_buffer) as zip_ref:
                fit_filename = next((f for f in zip_ref.namelist() if f.endswith(".fit")), None)
                if not fit_filename:
                    raise FileNotFoundError(f"No FIT file found in the downloaded zip archive for Activity ID {activityID}")

                fit_data = zip_ref.read(fit_filename)
                fit_file_buffer = io.BytesIO(fit_data)

                fitfile = FitFile(fit_file_buffer)
                fitfile.parse()

                all_records_list = [record.get_values() for record in fitfile.get_messages("record")]
                all_sessions_list = [record.get_values() for record in fitfile.get_messages("session")]
                all_lengths_list = [record.get_values() for record in fitfile.get_messages("length")]
                all_laps_list = [record.get_values() for record in fitfile.get_messages("lap")]

                fit_age_years = None
                for msg in fitfile.get_messages("user_metrics"):
                    d = {f.name: f.value for f in msg.fields}
                    a = d.get("age")
                    if a is None:
                        continue
                    try:
                        fit_age_years = int(float(a))
                        break
                    except Exception:
                        pass

                if len(all_records_list) == 0:
                    raise FileNotFoundError(f"No records found in FIT file for Activity ID {activityID} - Discarding FIT file")

                ts0 = all_records_list[0].get("timestamp")
                if not ts0:
                    raise FileNotFoundError(f"First record missing timestamp in FIT for Activity ID {activityID} - Discarding FIT file")
                activity_start_time = ts0.replace(tzinfo=pytz.UTC)

                fit_lthr = None
                fit_ltpower = None
                try:
                    for msg in fitfile.get_messages("unknown_79"):
                        by_def = {getattr(f, "def_num", None): getattr(f, "value", None) for f in msg.fields}

                        if fit_age_years is None and by_def.get(1) is not None:
                            fit_age_years = int(float(by_def[1]))
                        if fit_lthr is None and by_def.get(11) is not None:
                            fit_lthr = int(float(by_def[11]))
                        if fit_ltpower is None and by_def.get(12) is not None:
                            fit_ltpower = int(float(by_def[12]))

                        if fit_age_years is not None and fit_lthr is not None and fit_ltpower is not None:
                            break

                    logging.info(f"FIT unknown_79 derived: age={fit_age_years}, lthr={fit_lthr}, ltpower={fit_ltpower}")
                except Exception:
                    logging.exception("FIT unknown_79 extraction failed")

                try:
                    ctx.derive_and_write_activity_metrics_v1(
                        activity_id=int(activityID),
                        activity_type=str(activity_type),
                        activity_start_time=activity_start_time,
                        all_records_list=all_records_list,
                    )
                except Exception:
                    logging.exception(f"DerivedActivity computation failed for Activity ID {activityID}")

                # FIT user_profile extraction (gender + birth year + birthdate)
                all_user_list: list[dict[str, object]] = []
                for msg in fitfile.get_messages("user_profile"):
                    d: dict[str, object] = {}
                    for f in msg.fields:
                        d[f.name] = f.value
                    all_user_list.append(d)

                fit_gender = "unknown"
                fit_birth_year: int | None = None
                fit_birthdate: object | None = None

                if all_user_list:
                    for up in all_user_list:
                        g = up.get("gender") or up.get("sex")
                        if g:
                            fit_gender = ctx.norm_gender(g)

                        yob = (
                            up.get("year_of_birth")
                            or up.get("birth_year")
                            or up.get("yearofbirth")
                            or up.get("birthyear")
                            or up.get("yob")
                        )

                        bd = (
                            up.get("birthdate")
                            or up.get("birth_date")
                            or up.get("date_of_birth")
                            or up.get("dob")
                        )

                        if yob is None:
                            for k, v in up.items():
                                ks = str(k).lower()
                                if "birth" in ks and "year" in ks:
                                    yob = v
                                    break

                        if bd is None:
                            for k, v in up.items():
                                ks = str(k).lower()
                                if "birth" in ks and ("date" in ks or "dob" in ks):
                                    bd = v
                                    break

                        if fit_birth_year is None:
                            fit_birth_year = ctx.birth_year_from_any(yob)

                        if fit_birth_year is None and fit_birthdate is None:
                            fit_birthdate = bd

                        if fit_gender in {"male", "female"} and fit_birth_year is not None:
                            break

                if fit_birth_year is None:
                    fit_birth_year = ctx.birth_year_from_any(fit_birthdate)

                try:
                    activity_day = activity_start_time.strftime("%Y-%m-%d")

                    if fit_birth_year is None and fit_age_years is not None:
                        try:
                            y = activity_start_time.year
                            a = int(fit_age_years)
                            if fit_birthdate:
                                yy, mm, dd = [int(x) for x in str(fit_birthdate).split("-")[:3]]
                                had_bday = (activity_start_time.month, activity_start_time.day) >= (mm, dd)
                                fit_birth_year = y - a if had_bday else (y - a - 1)
                            else:
                                fit_birth_year = y - a - 1
                        except Exception:
                            pass

                    if fit_gender in {"male", "female"}:
                        ctx.fit_gender_cache[activity_day] = fit_gender

                        try:
                            by = fit_birth_year or ctx.stored_birth_year_v1() or ctx.get_birth_year_from_garmin_profile()
                            ctx.write_points_to_influxdb(
                                ctx.write_user_profile_point(activity_day, gender=fit_gender, birth_year=by)
                            )
                            logging.info(f"UserProfile updated from FIT for {activity_day}: gender={fit_gender}, birth_year={by}")
                        except Exception:
                            logging.exception("Failed writing UserProfile from FIT gender")

                    ctx.maybe_update_userprofile_master(
                        birth_year=fit_birth_year,
                        age_years=fit_age_years,
                        gender=fit_gender if fit_gender in {"male", "female"} else None,
                        lthr=fit_lthr,
                        source="fit_unknown_79",
                        activity_id=int(activityID),
                    )
                except Exception:
                    logging.exception("UserProfileMaster update from FIT user_profile failed")

                for parsed_record in all_records_list:
                    ts = parsed_record.get("timestamp")
                    if not ts:
                        continue

                    hr = parsed_record.get("heart_rate", None)
                    u140 = parsed_record.get("unknown_140")
                    gas = (u140 / 1000.0) if u140 else None

                    points_list.append(
                        {
                            "measurement": "ActivityGPS",
                            "time": ts.replace(tzinfo=pytz.UTC).isoformat(),
                            "tags": {
                                "Device": ctx.garmin_devicename,
                                "Database_Name": ctx.influxdb_database,
                                "ActivityID": activityID,
                                "ActivitySelector": activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type,
                                "sport_tag": ctx.norm_tag_value(activity_type),
                            },
                            "fields": {
                                "ActivityName": activity_type,
                                "Activity_ID": activityID,
                                "Latitude": int(parsed_record["position_lat"]) * (180 / 2**31) if parsed_record.get("position_lat") else None,
                                "Longitude": int(parsed_record["position_long"]) * (180 / 2**31) if parsed_record.get("position_long") else None,
                                "Altitude": parsed_record.get("enhanced_altitude", None) or parsed_record.get("altitude", None),
                                "Distance": parsed_record.get("distance", None),
                                "DurationSeconds": (ts.replace(tzinfo=pytz.UTC) - activity_start_time).total_seconds(),
                                "HeartRate": float(hr) if hr is not None else None,
                                "Speed": parsed_record.get("enhanced_speed", None) or parsed_record.get("speed", None),
                                "GradeAdjustedSpeed": gas,
                                "RunningEfficiency": (gas / hr) if (gas is not None and hr) else None,
                                "Cadence": parsed_record.get("cadence", None),
                                "Fractional_Cadence": parsed_record.get("fractional_cadence", None),
                                "Temperature": parsed_record.get("temperature", None),
                                "Accumulated_Power": parsed_record.get("accumulated_power", None),
                                "Power": parsed_record.get("power", None),
                                "Vertical_Oscillation": parsed_record.get("vertical_oscillation", None),
                                "Stance_Time": parsed_record.get("stance_time", None),
                                "Vertical_Ratio": parsed_record.get("vertical_ratio", None),
                                "Step_Length": parsed_record.get("step_length", None),
                            },
                        }
                    )

                def _iso_utc(dt: object) -> str | None:
                    if not dt:
                        return None
                    try:
                        return dt.replace(tzinfo=pytz.UTC).isoformat()
                    except Exception:
                        return None

                for session_record in all_sessions_list:
                    t_iso = _iso_utc(session_record.get("start_time") or session_record.get("timestamp"))
                    if not t_iso:
                        continue
                    points_list.append(
                        {
                            "measurement": "ActivitySession",
                            "time": t_iso,
                            "tags": {
                                "Device": ctx.garmin_devicename,
                                "Database_Name": ctx.influxdb_database,
                                "ActivityID": activityID,
                                "ActivitySelector": activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type,
                                "sport_tag": ctx.norm_tag_value(session_record.get("sport")),
                                "sub_sport_tag": ctx.norm_tag_value(session_record.get("sub_sport")),
                            },
                            "fields": {
                                "Index": int(session_record.get("message_index", -1)) + 1,
                                "ActivityName": activity_type,
                                "Activity_ID": activityID,
                                "Sport": str(session_record.get("sport", None)),
                                "Sub_Sport": session_record.get("sub_sport", None),
                                "Pool_Length": session_record.get("pool_length", None),
                                "Pool_Length_Unit": session_record.get("pool_length_unit", None),
                                "Lengths": session_record.get("num_laps", None),
                                "Laps": session_record.get("num_lengths", None),
                                "Aerobic_Training": session_record.get("total_training_effect", None),
                                "Anaerobic_Training": session_record.get("total_anaerobic_training_effect", None),
                                "Primary_Benefit": session_record.get("primary_benefit", None),
                                "Recovery_Time": session_record.get("recovery_time", None),
                            },
                        }
                    )

                for length_record in all_lengths_list:
                    t_iso = _iso_utc(length_record.get("start_time") or length_record.get("timestamp"))
                    if not t_iso:
                        continue
                    points_list.append(
                        {
                            "measurement": "ActivityLength",
                            "time": t_iso,
                            "tags": {
                                "Device": ctx.garmin_devicename,
                                "Database_Name": ctx.influxdb_database,
                                "ActivityID": activityID,
                                "ActivitySelector": activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type,
                            },
                            "fields": {
                                "Index": int(length_record.get("message_index", -1)) + 1,
                                "ActivityName": activity_type,
                                "Activity_ID": activityID,
                                "Elapsed_Time": length_record.get("total_elapsed_time", None),
                                "Strokes": length_record.get("total_strokes", None),
                                "Swim_Stroke": length_record.get("swim_stroke", None),
                                "Avg_Speed": length_record.get("avg_speed", None),
                                "Calories": length_record.get("total_calories", None),
                                "Avg_Cadence": length_record.get("avg_swimming_cadence", None),
                            },
                        }
                    )

                for lap_record in all_laps_list:
                    t_iso = _iso_utc(lap_record.get("start_time") or lap_record.get("timestamp"))
                    if not t_iso:
                        continue
                    points_list.append(
                        {
                            "measurement": "ActivityLap",
                            "time": t_iso,
                            "tags": {
                                "Device": ctx.garmin_devicename,
                                "Database_Name": ctx.influxdb_database,
                                "ActivityID": activityID,
                                "ActivitySelector": activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type,
                                "sport_tag": ctx.norm_tag_value(lap_record.get("sport")),
                            },
                            "fields": {
                                "Index": int(lap_record.get("message_index", -1)) + 1,
                                "ActivityName": activity_type,
                                "Activity_ID": activityID,
                                "Elapsed_Time": lap_record.get("total_elapsed_time", None),
                                "Sport": lap_record.get("sport", None),
                                "Lengths": lap_record.get("num_lengths", None),
                                "Length_Index": lap_record.get("first_length_index", None),
                                "Distance": lap_record.get("total_distance", None),
                                "Cycles": lap_record.get("total_cycles", None),
                                "Avg_Stroke_Distance": lap_record.get("avg_stroke_distance", None),
                                "Moving_Duration": lap_record.get("total_moving_time", None),
                                "Standing_Duration": lap_record.get("time_standing", None),
                                "Avg_Speed": lap_record.get("enhanced_avg_speed", None),
                                "Max_Speed": lap_record.get("enhanced_max_speed", None),
                                "Calories": lap_record.get("total_calories", None),
                                "Avg_Power": lap_record.get("avg_power", None),
                                "Avg_HR": lap_record.get("avg_heart_rate", None),
                                "Max_HR": lap_record.get("max_heart_rate", None),
                                "Avg_Cadence": lap_record.get("avg_cadence", None),
                                "Avg_Temperature": lap_record.get("avg_temperature", None),
                                "Avg_Vertical_Oscillation": lap_record.get("avg_vertical_oscillation", None),
                                "Avg_Stance_Time": lap_record.get("avg_stance_time", None),
                                "Avg_Vertical_Ratio": lap_record.get("avg_vertical_ratio", None),
                                "Avg_Step_Length": lap_record.get("avg_step_length", None),
                            },
                        }
                    )

                if ctx.keep_fit_files:
                    os.makedirs(ctx.fit_file_storage_location, exist_ok=True)
                    fit_path = os.path.join(
                        ctx.fit_file_storage_location,
                        activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type + ".fit",
                    )
                    with open(fit_path, "wb") as f:
                        f.write(fit_data)
                    logging.info(f"Success : Activity ID {activityID} stored in output file {fit_path}")

        except (FileNotFoundError, FitParseError) as err:
            logging.error(err)
            logging.warning(f"Fallback : Failed to use FIT file for activityID {activityID} - Trying TCX file...")

            ns = {
                "tcx": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2",
                "ns3": "http://www.garmin.com/xmlschemas/ActivityExtension/v2",
            }
            try:
                tcx_file_data = ctx.garmin_obj.download_activity(activityID, dl_fmt=ctx.garmin_obj.ActivityDownloadFormat.TCX).decode("UTF-8")
                root = ET.fromstring(tcx_file_data)

                if ctx.keep_fit_files:
                    os.makedirs(ctx.fit_file_storage_location, exist_ok=True)
                    act0 = root.findall("tcx:Activities/tcx:Activity", ns)
                    if act0:
                        activity_start_time = datetime.fromisoformat(act0[0].find("tcx:Id", ns).text.strip("Z"))
                        tcx_path = os.path.join(
                            ctx.fit_file_storage_location,
                            activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type + ".tcx",
                        )
                        with open(tcx_path, "w") as f:
                            f.write(tcx_file_data)
                        logging.info(f"Success : Activity ID {activityID} stored in output file {tcx_path}")

            except requests.exceptions.Timeout:
                logging.warning(f"Request timeout for fetching large activity record {activityID} - skipping record")
                return []
            except Exception:
                logging.exception(f"Unable to fetch TCX for activity record {activityID} : skipping record")
                return []

            for activity in root.findall("tcx:Activities/tcx:Activity", ns):
                activity_start_time = datetime.fromisoformat(activity.find("tcx:Id", ns).text.strip("Z"))
                lap_index = 1
                for lap in activity.findall("tcx:Lap", ns):
                    for tp in lap.findall(".//tcx:Trackpoint", ns):
                        t_txt = tp.findtext("tcx:Time", default=None, namespaces=ns)
                        if not t_txt:
                            continue
                        time_obj = datetime.fromisoformat(t_txt.strip("Z"))

                        lat = tp.findtext("tcx:Position/tcx:LatitudeDegrees", default=None, namespaces=ns)
                        lon = tp.findtext("tcx:Position/tcx:LongitudeDegrees", default=None, namespaces=ns)
                        alt = tp.findtext("tcx:AltitudeMeters", default=None, namespaces=ns)
                        dist = tp.findtext("tcx:DistanceMeters", default=None, namespaces=ns)
                        hr = tp.findtext("tcx:HeartRateBpm/tcx:Value", default=None, namespaces=ns)
                        speed = tp.findtext("tcx:Extensions/ns3:TPX/ns3:Speed", default=None, namespaces=ns)

                        def _to_f(x: object) -> float | None:
                            try:
                                return float(x)  # type: ignore[arg-type]
                            except Exception:
                                return None

                        points_list.append(
                            {
                                "measurement": "ActivityGPS",
                                "time": time_obj.isoformat(),
                                "tags": {
                                    "Device": ctx.garmin_devicename,
                                    "Database_Name": ctx.influxdb_database,
                                    "ActivityID": activityID,
                                    "ActivitySelector": activity_start_time.strftime("%Y%m%dT%H%M%SUTC-") + activity_type,
                                },
                                "fields": {
                                    "ActivityName": activity_type,
                                    "Activity_ID": activityID,
                                    "Latitude": _to_f(lat),
                                    "Longitude": _to_f(lon),
                                    "Altitude": _to_f(alt),
                                    "Distance": _to_f(dist),
                                    "DurationSeconds": (time_obj - activity_start_time).total_seconds(),
                                    "HeartRate": _to_f(hr),
                                    "Speed": _to_f(speed),
                                    "lap": lap_index,
                                },
                            }
                        )
                    lap_index += 1

        logging.info(f"Success : Fetching detailed activity for Activity ID {activityID}")
        ctx.parsed_activity_id_list.append(activityID)

    return points_list

