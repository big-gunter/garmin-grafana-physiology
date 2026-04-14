from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

import pytz
import requests
from garth.exc import GarthHTTPError
from garminconnect import (
    GarminConnectAuthenticationError,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError,
)


@dataclass(frozen=True, slots=True)
class OrchestratorContext:
    # shared services
    garmin_obj: Any
    write_points_to_influxdb: Callable[[list[dict] | None], None]

    # wrappers into garmin_fetch module functions (keeps behavior identical)
    get_last_sync: Callable[[], list[dict]]
    daily_fetch_write: Callable[..., None]
    iter_days: Callable[[str, str], Any]
    should_rollups_after_ingest: Callable[[str, str, timedelta], bool]
    run_rollups_for_range: Callable[[str, str], None]

    # config
    rate_limit_calls_seconds: int
    fetch_failed_wait_seconds: int
    max_consecutive_500_errors: int
    ignore_errors: bool

    # login refresh
    reauth: Callable[[], Any]


@dataclass(frozen=True, slots=True)
class DailyFetchWriteContext:
    # garmin
    garmin_obj: Any

    # config
    request_intraday_data_refresh: bool
    ignore_intraday_data_refresh_days: int
    fetch_selection: list[str]
    userprofile_write_once_per_day: bool

    # user profile helpers
    userprofile_exists_for_day_v1: Callable[[str], bool]
    get_user_gender_from_garmin: Callable[[], str]
    stored_birth_year_v1: Callable[[], int | None]
    get_birth_year_from_garmin_profile: Callable[[], int | None]
    write_user_profile_point: Callable[..., list[dict]]
    fit_gender_cache: dict[str, str]

    # writing
    write_points_to_influxdb: Callable[[list[dict] | None], None]

    # fetchers (existing wrappers)
    get_daily_stats: Callable[[str], list[dict]]
    get_sleep_data: Callable[[str], list[dict]]
    get_intraday_steps: Callable[[str], list[dict]]
    get_intraday_hr: Callable[[str], list[dict]]
    get_intraday_stress: Callable[[str], list[dict]]
    get_intraday_br: Callable[[str], list[dict]]
    get_intraday_hrv: Callable[[str], list[dict]]
    get_fitness_age: Callable[[str], list[dict]]
    get_vo2_max: Callable[[str], list[dict]]
    get_race_predictions: Callable[[str], list[dict]]
    get_body_composition: Callable[[str], list[dict]]
    get_lactate_threshold: Callable[[str], list[dict]]
    get_training_status: Callable[[str], list[dict]]
    get_training_readiness: Callable[[str], list[dict]]
    get_hillscore: Callable[[str], list[dict]]
    get_endurance_score: Callable[[str], list[dict]]
    get_blood_pressure: Callable[[str], list[dict]]
    get_hydration: Callable[[str], list[dict]]
    get_activity_summary: Callable[[str], tuple[list[dict], dict]]
    fetch_activity_GPS: Callable[[dict], list[dict]]
    get_solar_intensity: Callable[[str], list[dict]]
    get_lifestyle_data: Callable[[str], list[dict]]

    # rollups
    compute_and_write_physiology: Callable[[str], None]
    compute_and_write_training_load: Callable[[str], None]
    compute_and_write_performance_daily: Callable[[str], None]

    # sleep/rate limit
    rate_limit_calls_seconds: int


def run_rollups_for_range(
    *,
    start_date: str,
    end_date: str,
    influxdb_version: str,
    compute_and_write_physiology: Callable[[str], None],
    compute_and_write_training_load: Callable[[str], None],
    compute_and_write_performance_daily: Callable[[str], None],
) -> None:
    if influxdb_version != "1":
        logging.warning("Rollups currently implemented for InfluxDB v1 only.")
        return

    logging.info(f"Rollups: running physiology + training load for {start_date} -> {end_date} (chronological)")
    d0 = datetime.strptime(start_date, "%Y-%m-%d")
    d1 = datetime.strptime(end_date, "%Y-%m-%d")
    cur = d0
    while cur <= d1:
        ds = cur.strftime("%Y-%m-%d")
        try:
            compute_and_write_physiology(ds)
        except Exception:
            logging.exception(f"PhysiologyDaily computation failed for {ds}")
        try:
            compute_and_write_training_load(ds)
        except Exception:
            logging.exception(f"TrainingLoadDaily computation failed for {ds}")
        try:
            compute_and_write_performance_daily(ds)
        except Exception:
            logging.exception(f"PerformanceDaily computation failed for {ds}")
        cur += timedelta(days=1)


def daily_fetch_write(date_str: str, *, run_rollups_inline: bool = True, ctx: DailyFetchWriteContext) -> None:
    if ctx.request_intraday_data_refresh and (
        datetime.strptime(date_str, "%Y-%m-%d")
        <= (datetime.today() - timedelta(days=ctx.ignore_intraday_data_refresh_days))
    ):
        data_refresh_response = (
            ctx.garmin_obj.connectapi(f"wellness-service/wellness/epoch/request/{date_str}", method="POST") or {}
        ).get("status", "Unknown")
        logging.info(f"Intraday data refresh request status: {data_refresh_response}")
        if data_refresh_response == "SUBMITTED":
            logging.info("Waiting 10 seconds for refresh request to process...")
            time.sleep(10)
        elif data_refresh_response == "COMPLETE":
            logging.info(f"Data for date {date_str} is already available")
        elif data_refresh_response == "NO_FILES_FOUND":
            logging.info(f"No Data is available for date {date_str} to refresh")
            return
        elif data_refresh_response == "DENIED":
            logging.info(
                "Daily refresh limit reached. Pausing script for 24 hours to ensure Intraday data fetching. Disable REQUEST_INTRADAY_DATA_REFRESH to avoid this!"
            )
            time.sleep(86500)
            data_refresh_response = (
                ctx.garmin_obj.connectapi(f"wellness-service/wellness/epoch/request/{date_str}", method="POST") or {}
            ).get("status", "Unknown")
            logging.info(f"Intraday data refresh request status: {data_refresh_response}")
            logging.info("Waiting 10 seconds...")
            time.sleep(10)
        else:
            logging.info("Refresh response is unknown!")
            time.sleep(5)

    # --- user profile metadata (gender etc) ---
    try:
        if ctx.userprofile_write_once_per_day and ctx.userprofile_exists_for_day_v1(date_str):
            logging.info(f"UserProfile already exists for {date_str}; skipping daily write")
        else:
            g = ctx.get_user_gender_from_garmin()
            if g == "unknown":
                g = ctx.fit_gender_cache.get(date_str, "unknown")

            by = ctx.stored_birth_year_v1() or ctx.get_birth_year_from_garmin_profile()

            if g != "unknown" or by is not None:
                ctx.write_points_to_influxdb(
                    ctx.write_user_profile_point(
                        date_str,
                        gender=None if g == "unknown" else g,
                        birth_year=by,
                    )
                )
            else:
                logging.info(
                    f"{date_str} - UserProfile: not available yet (will be updated from FIT if an activity is processed)"
                )
    except Exception:
        logging.exception(f"UserProfile write failed for {date_str}")

    sel = set(ctx.fetch_selection)
    if "daily_avg" in sel:
        ctx.write_points_to_influxdb(ctx.get_daily_stats(date_str))
    if "sleep" in sel:
        ctx.write_points_to_influxdb(ctx.get_sleep_data(date_str))
    if "steps" in sel:
        ctx.write_points_to_influxdb(ctx.get_intraday_steps(date_str))
    if "heartrate" in sel:
        ctx.write_points_to_influxdb(ctx.get_intraday_hr(date_str))
    if "stress" in sel:
        ctx.write_points_to_influxdb(ctx.get_intraday_stress(date_str))
    if "breathing" in sel:
        ctx.write_points_to_influxdb(ctx.get_intraday_br(date_str))
    if "hrv" in sel:
        ctx.write_points_to_influxdb(ctx.get_intraday_hrv(date_str))
    if "fitness_age" in sel:
        ctx.write_points_to_influxdb(ctx.get_fitness_age(date_str))
    if "vo2" in sel:
        ctx.write_points_to_influxdb(ctx.get_vo2_max(date_str))
    if "race_prediction" in sel:
        ctx.write_points_to_influxdb(ctx.get_race_predictions(date_str))
    if "body_composition" in sel:
        ctx.write_points_to_influxdb(ctx.get_body_composition(date_str))
    if "lactate_threshold" in sel:
        ctx.write_points_to_influxdb(ctx.get_lactate_threshold(date_str))
    if "training_status" in sel:
        ctx.write_points_to_influxdb(ctx.get_training_status(date_str))
    if "training_readiness" in sel:
        ctx.write_points_to_influxdb(ctx.get_training_readiness(date_str))
    if "hill_score" in sel:
        ctx.write_points_to_influxdb(ctx.get_hillscore(date_str))
    if "endurance_score" in sel:
        ctx.write_points_to_influxdb(ctx.get_endurance_score(date_str))
    if "blood_pressure" in sel:
        ctx.write_points_to_influxdb(ctx.get_blood_pressure(date_str))
    if "hydration" in sel:
        ctx.write_points_to_influxdb(ctx.get_hydration(date_str))
    if "activity" in sel:
        activity_summary_points_list, activity_with_gps_id_dict = ctx.get_activity_summary(date_str)
        ctx.write_points_to_influxdb(activity_summary_points_list)
        ctx.write_points_to_influxdb(ctx.fetch_activity_GPS(activity_with_gps_id_dict))
    if "solar_intensity" in sel:
        ctx.write_points_to_influxdb(ctx.get_solar_intensity(date_str))
    if "lifestyle" in sel:
        ctx.write_points_to_influxdb(ctx.get_lifestyle_data(date_str))

    if run_rollups_inline:
        try:
            ctx.compute_and_write_physiology(date_str)
        except Exception:
            logging.exception(f"PhysiologyDaily computation failed for {date_str}")
        try:
            ctx.compute_and_write_training_load(date_str)
        except Exception:
            logging.exception(f"TrainingLoadDaily computation failed for {date_str}")
        try:
            ctx.compute_and_write_performance_daily(date_str)
        except Exception:
            logging.exception(f"PerformanceDaily computation failed for {date_str}")

def compute_rollups_range(
    *,
    start_date: str,
    end_date: str,
    compute_and_write_physiology: Callable[[str], None],
    compute_and_write_training_load: Callable[[str], None],
    compute_and_write_performance_daily: Callable[[str], None],
) -> None:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    while current <= end:
        d = current.strftime("%Y-%m-%d")
        try:
            compute_and_write_physiology(d)
        except Exception:
            logging.exception(f"PhysiologyDaily computation failed for {d}")

        try:
            compute_and_write_training_load(d)
        except Exception:
            logging.exception(f"TrainingLoadDaily computation failed for {d}")

        try:
            compute_and_write_performance_daily(d)
        except Exception:
            logging.exception(f"PerformanceDaily computation failed for {d}")

        current += timedelta(days=1)


def fetch_write_bulk(
    *,
    start_date_str: str,
    end_date_str: str,
    local_timediff: timedelta,
    ctx: OrchestratorContext,
) -> None:
    consecutive_500_errors = 0
    logging.info("Fetching data for the given period in reverse chronological order")
    time.sleep(3)

    ctx.write_points_to_influxdb(ctx.get_last_sync())

    rollups_after = ctx.should_rollups_after_ingest(start_date_str, end_date_str, local_timediff)
    if rollups_after:
        logging.info("Mode: backfill (rollups after ingest)")
    else:
        logging.info("Mode: live (rollups inline)")

    try:
        for current_date in ctx.iter_days(start_date_str, end_date_str):
            repeat_loop = True
            while repeat_loop:
                try:
                    ctx.daily_fetch_write(current_date, run_rollups_inline=(not rollups_after))
                    if consecutive_500_errors > 0:
                        logging.info(
                            f"Successfully fetched data after {consecutive_500_errors} consecutive 500 errors - resetting error counter"
                        )
                        consecutive_500_errors = 0

                    logging.info(
                        f"Success : Fetched all available health metrics for date {current_date} (skipped any if unavailable)"
                    )
                    if ctx.rate_limit_calls_seconds > 0:
                        logging.info(f"Waiting : for {ctx.rate_limit_calls_seconds} seconds")
                        time.sleep(ctx.rate_limit_calls_seconds)
                    repeat_loop = False

                except GarminConnectTooManyRequestsError as err:
                    logging.error(err)
                    logging.info(
                        f"Too many requests (429) : Failed to fetch one or more metrics - will retry for date {current_date}"
                    )
                    logging.info(f"Waiting : for {ctx.fetch_failed_wait_seconds} seconds")
                    time.sleep(ctx.fetch_failed_wait_seconds)
                    repeat_loop = True

                except (requests.exceptions.HTTPError, GarthHTTPError) as err:
                    is_500_error = False
                    if isinstance(err, requests.exceptions.HTTPError):
                        if hasattr(err, "response") and err.response is not None and err.response.status_code == 500:
                            is_500_error = True
                    elif isinstance(err, GarthHTTPError):
                        if getattr(err, "status_code", None) == 500:
                            is_500_error = True
                        elif hasattr(err, "response") and err.response is not None and err.response.status_code == 500:
                            is_500_error = True

                    if is_500_error:
                        consecutive_500_errors += 1
                        logging.error(
                            f"HTTP 500 error ({consecutive_500_errors}/{ctx.max_consecutive_500_errors}) for date {current_date}: {err}"
                        )
                        if consecutive_500_errors >= ctx.max_consecutive_500_errors:
                            logging.warning(
                                f"Received {consecutive_500_errors} consecutive HTTP 500 errors. Logging error and continuing backward in time to fetch remaining data."
                            )
                            logging.warning(f"Skipping date {current_date} due to persistent 500 errors from Garmin API")
                            logging.info(f"Waiting : for {ctx.rate_limit_calls_seconds} seconds before continuing")
                            time.sleep(ctx.rate_limit_calls_seconds)
                            repeat_loop = False
                        else:
                            logging.info(
                                f"HTTP 500 error encountered - will retry for date {current_date} (attempt {consecutive_500_errors}/{ctx.max_consecutive_500_errors})"
                            )
                            logging.info(f"Waiting : for {ctx.rate_limit_calls_seconds} seconds before retry")
                            time.sleep(ctx.rate_limit_calls_seconds)
                            repeat_loop = True
                    else:
                        logging.error(err)
                        logging.info(f"HTTP Error (non-500) : Failed to fetch one or more metrics - skipping date {current_date}")
                        logging.info(f"Waiting : for {ctx.rate_limit_calls_seconds} seconds")
                        time.sleep(ctx.rate_limit_calls_seconds)
                        repeat_loop = False

                except (GarminConnectConnectionError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as err:
                    logging.error(err)
                    logging.info(f"Connection Error : Failed to fetch one or more metrics - skipping date {current_date}")
                    logging.info(f"Waiting : for {ctx.rate_limit_calls_seconds} seconds")
                    time.sleep(ctx.rate_limit_calls_seconds)
                    repeat_loop = False

                except GarminConnectAuthenticationError as err:
                    logging.error(err)
                    logging.info(
                        "Authentication Failed : Retrying login with given credentials (won't work automatically for MFA/2FA enabled accounts)"
                    )
                    ctx.reauth()
                    time.sleep(5)
                    repeat_loop = True

                except Exception as err:
                    if ctx.ignore_errors:
                        logging.warning("IGNORE_ERRORS Enabled >> Failed to process %s:", current_date)
                        logging.exception(err)
                        repeat_loop = False
                    else:
                        raise err
    finally:
        if rollups_after:
            ctx.run_rollups_for_range(start_date_str, end_date_str)

