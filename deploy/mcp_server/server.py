import os
import json
import asyncio
from datetime import datetime, timedelta
from typing import Optional
from influxdb import InfluxDBClient
from mcp.server.fastmcp import FastMCP

# --- Config from environment ---
INFLUX_HOST     = os.environ.get("INFLUX_HOST", "influxdb")
INFLUX_PORT     = int(os.environ.get("INFLUX_PORT", "8086"))
INFLUX_USER     = os.environ.get("INFLUX_USER", "admin")
INFLUX_PASSWORD = os.environ.get("INFLUX_PASSWORD", "")
INFLUX_DATABASE = os.environ.get("INFLUX_DATABASE", "garmin")
DEVICE_NAME     = os.environ.get("GARMIN_DEVICENAME", "")

from mcp.server.transport_security import TransportSecuritySettings

from mcp.server.transport_security import TransportSecuritySettings

mcp = FastMCP(
    "Garmin Physiology MCP",
    streamable_http_path="/",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=["mcp.big-gunter.com", "mcp.big-gunter.com:443"],
        allowed_origins=["https://mcp.big-gunter.com"],
    )
)

def get_client():
    return InfluxDBClient(
        host=INFLUX_HOST,
        port=INFLUX_PORT,
        username=INFLUX_USER,
        password=INFLUX_PASSWORD,
        database=INFLUX_DATABASE
    )

def query(q: str) -> list[dict]:
    client = get_client()
    result = client.query(q)
    return list(result.get_points())

def date_range(days_back: int) -> tuple[str, str]:
    end = datetime.utcnow()
    start = end - timedelta(days=days_back)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ")

def device_filter() -> str:
    if DEVICE_NAME:
        return f" AND \"Device\"='{DEVICE_NAME}'"
    return ""

# --- DAILY HEALTH TOOLS ---

@mcp.tool()
def get_daily_stats(days_back: int = 7) -> str:
    """Get daily health stats including steps, calories, stress, HRV, SpO2, body battery, floors and active minutes for the last N days"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT "restingHeartRate","totalSteps","activeKilocalories","stressDuration",'
        f'"bodyBatteryHighestValue","bodyBatteryLowestValue","averageSpo2",'
        f'"moderateIntensityMinutes","vigorousIntensityMinutes","totalDistanceMeters",'
        f'"floorsAscendedInMeters","highlyActiveSeconds","sedentarySeconds","sleepingSeconds" '
        f'FROM "DailyStats" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} ORDER BY time DESC"
    )
    if not rows:
        return "No daily stats found for the requested period."
    return json.dumps(rows, indent=2, default=str)

@mcp.tool()
def get_sleep_summary(days_back: int = 7) -> str:
    """Get sleep summary data including sleep score, deep/light/REM/awake seconds, HRV, SpO2, respiration and skin temperature for the last N days"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT "sleepScore","sleepTimeSeconds","deepSleepSeconds","lightSleepSeconds",'
        f'"remSleepSeconds","awakeSleepSeconds","avgOvernightHrv","averageSpO2Value",'
        f'"averageRespirationValue","restingHeartRate","bodyBatteryChange",'
        f'"avgSleepStress","restlessMomentsCount","awakeCount",'
        f'"avgSkinTempDeviationC" '
        f'FROM "SleepSummary" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} ORDER BY time DESC"
    )
    if not rows:
        return "No sleep data found for the requested period."
    return json.dumps(rows, indent=2, default=str)

@mcp.tool()
def get_body_battery(days_back: int = 7) -> str:
    """Get body battery intraday levels for the last N days"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT mean("BodyBatteryLevel") AS "avg_body_battery" '
        f'FROM "BodyBatteryIntraday" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} GROUP BY time(1h) fill(none)"
    )
    if not rows:
        return "No body battery data found."
    return json.dumps(rows, indent=2, default=str)

@mcp.tool()
def get_hrv_intraday(days_back: int = 1) -> str:
    """Get intraday HRV values for the last N days"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT mean("hrvValue") AS "hrv" '
        f'FROM "HRV_Intraday" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} GROUP BY time(30m) fill(none)"
    )
    if not rows:
        return "No HRV intraday data found."
    return json.dumps(rows, indent=2, default=str)

@mcp.tool()
def get_stress_intraday(days_back: int = 1) -> str:
    """Get intraday stress levels for the last N days"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT mean("stressLevel") AS "stress" '
        f'FROM "StressIntraday" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} GROUP BY time(30m) fill(none)"
    )
    if not rows:
        return "No stress data found."
    return json.dumps(rows, indent=2, default=str)

# --- ACTIVITY TOOLS ---

@mcp.tool()
def get_recent_activities(days_back: int = 14) -> str:
    """Get recent activity summaries including type, distance, duration, average HR, max HR, calories and HR time in zones"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT "activityName","activityType","distance","elapsedDuration",'
        f'"averageHR","maxHR","calories","averageSpeed","maxSpeed",'
        f'"hrTimeInZone_1","hrTimeInZone_2","hrTimeInZone_3","hrTimeInZone_4","hrTimeInZone_5",'
        f'"lapCount","locationName","movingDuration" '
        f'FROM "ActivitySummary" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} ORDER BY time DESC"
    )
    if not rows:
        return "No activities found for the requested period."
    return json.dumps(rows, indent=2, default=str)

@mcp.tool()
def get_activity_laps(activity_name: str, days_back: int = 30) -> str:
    """Get lap breakdown for activities matching a name within the last N days"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT "ActivityName","Index","Distance","Elapsed_Time","Avg_HR","Max_HR",'
        f'"Avg_Speed","Max_Speed","Avg_Power","Avg_Cadence","Calories",'
        f'"Avg_Stance_Time","Avg_Step_Length","Sport" '
        f'FROM "ActivityLap" '
        f"WHERE time >= '{start}' AND time < '{end}' "
        f"AND \"ActivityName\"='{activity_name}'"
        f"{device_filter()} ORDER BY time ASC"
    )
    if not rows:
        return f"No laps found for activity '{activity_name}' in the last {days_back} days."
    return json.dumps(rows, indent=2, default=str)

@mcp.tool()
def get_derived_activity_metrics(days_back: int = 30) -> str:
    """Get derived performance metrics for activities including critical speed, critical power, TRIMP, TSS, VO2max estimates and LTHR estimates"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT "Activity_ID","cs_mps","cs_pace_s_per_km","cp_watts","wprime_j",'
        f'"vo2max_est","lthr_bpm_est","TRIMP_Banister_ts","TRIMP_Edwards_ts",'
        f'"hrTSS_ts","rTSS_ts","bikeTSS_ts","gap_distance_km",'
        f'"best20m_vam_m_per_h","best30m_vam_m_per_h" '
        f'FROM "DerivedActivity" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} ORDER BY time DESC"
    )
    if not rows:
        return "No derived activity metrics found."
    return json.dumps(rows, indent=2, default=str)

# --- FITNESS & PERFORMANCE TOOLS ---

@mcp.tool()
def get_vo2max(days_back: int = 90) -> str:
    """Get VO2max estimates (running and cycling) over the last N days"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT "VO2_max_value","VO2_max_value_cycling" '
        f'FROM "VO2_Max" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} ORDER BY time DESC"
    )
    if not rows:
        return "No VO2max data found."
    return json.dumps(rows, indent=2, default=str)

@mcp.tool()
def get_race_predictions(days_back: int = 90) -> str:
    """Get Garmin race time predictions for 5K, 10K, half marathon and marathon"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT "time5K","time10K","timeHalfMarathon","timeMarathon" '
        f'FROM "RacePredictions" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} ORDER BY time DESC LIMIT 10"
    )
    if not rows:
        return "No race predictions found."
    # convert seconds to readable format
    def fmt(secs):
        if secs is None:
            return None
        s = int(secs)
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"

    for r in rows:
        for k in ("time5K","time10K","timeHalfMarathon","timeMarathon"):
            if k in r:
                r[k + "_formatted"] = fmt(r[k])
    return json.dumps(rows, indent=2, default=str)

@mcp.tool()
def get_fitness_age(days_back: int = 90) -> str:
    """Get fitness age, chronological age and achievable fitness age estimates"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT "fitnessAge","chronologicalAge","achievableFitnessAge" '
        f'FROM "FitnessAge" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} ORDER BY time DESC LIMIT 10"
    )
    if not rows:
        return "No fitness age data found."
    return json.dumps(rows, indent=2, default=str)

@mcp.tool()
def get_body_composition(days_back: int = 90) -> str:
    """Get body composition data including weight, BMI, body fat, body water, muscle mass and bone mass"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT "weight","bmi","bodyFat","bodyWater","muscleMass","boneMass" '
        f'FROM "BodyComposition" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} ORDER BY time DESC"
    )
    if not rows:
        return "No body composition data found."
    return json.dumps(rows, indent=2, default=str)

# --- TRAINING LOAD TOOLS ---

@mcp.tool()
def get_training_load(days_back: int = 42) -> str:
    """Get daily training load metrics including ATL, CTL, TSB (form), acute and chronic load for the last N days"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT * FROM "TrainingLoad" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} ORDER BY time DESC"
    )
    if not rows:
        return "No training load data found."
    return json.dumps(rows, indent=2, default=str)

@mcp.tool()
def get_physiology_daily(days_back: int = 30) -> str:
    """Get daily physiology rollups including HRmax, resting HR, HR zones and LTHR"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT * FROM "PhysiologyDaily" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} ORDER BY time DESC"
    )
    if not rows:
        return "No physiology daily data found."
    return json.dumps(rows, indent=2, default=str)

# --- SUMMARY / INSIGHT TOOLS ---

@mcp.tool()
def get_weekly_summary(weeks_back: int = 4) -> str:
    """Get weekly aggregated summary of steps, active calories, activity minutes, distance and average resting HR"""
    days = weeks_back * 7
    start, end = date_range(days)
    rows = query(
        f'SELECT sum("totalSteps") AS "total_steps",'
        f'sum("activeKilocalories") AS "total_active_kcal",'
        f'sum("moderateIntensityMinutes") AS "mod_intensity_mins",'
        f'sum("vigorousIntensityMinutes") AS "vig_intensity_mins",'
        f'sum("totalDistanceMeters") AS "total_distance_m",'
        f'mean("restingHeartRate") AS "avg_rhr" '
        f'FROM "DailyStats" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} GROUP BY time(7d)"
    )
    if not rows:
        return "No weekly summary data found."
    return json.dumps(rows, indent=2, default=str)

@mcp.tool()
def get_heart_rate_trends(days_back: int = 30) -> str:
    """Get daily resting HR, max HR and min HR trends over the last N days"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT "restingHeartRate","maxHeartRate","minHeartRate",'
        f'"maxAvgHeartRate","minAvgHeartRate" '
        f'FROM "DailyStats" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} ORDER BY time DESC"
    )
    if not rows:
        return "No heart rate trend data found."
    return json.dumps(rows, indent=2, default=str)

@mcp.tool()
def get_breathing_rate(days_back: int = 14) -> str:
    """Get intraday breathing rate data averaged by hour for the last N days"""
    start, end = date_range(days_back)
    rows = query(
        f'SELECT mean("BreathingRate") AS "avg_breathing_rate" '
        f'FROM "BreathingRateIntraday" '
        f"WHERE time >= '{start}' AND time < '{end}'"
        f"{device_filter()} GROUP BY time(1h) fill(none)"
    )
    if not rows:
        return "No breathing rate data found."
    return json.dumps(rows, indent=2, default=str)

