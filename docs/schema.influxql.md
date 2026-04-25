## InfluxDB schema export (auto-generated)

- Generated: `2026-04-25T12:51:47.663126Z`
- Source: InfluxDB v1 introspection (`SHOW MEASUREMENTS/FIELD KEYS/TAG KEYS`)

### `ActivityGPS`

**Tag keys**
- `ActivityID`
- `ActivitySelector`
- `Database_Name`
- `Device`
- `User_ID`
- `sport_tag`

**Field keys**
- `Accumulated_Power`: `integer`
- `ActivityName`: `string`
- `Activity_ID`: `integer`
- `Altitude`: `float`
- `Cadence`: `integer`
- `Distance`: `float`
- `DurationSeconds`: `float`
- `Fractional_Cadence`: `float`
- `GradeAdjustedSpeed`: `float`
- `HeartRate`: `float`
- `Latitude`: `float`
- `Longitude`: `float`
- `Power`: `integer`
- `RunningEfficiency`: `float`
- `Speed`: `float`
- `Stance_Time`: `float`
- `Step_Length`: `float`
- `Temperature`: `integer`
- `Vertical_Oscillation`: `float`
- `Vertical_Ratio`: `float`

### `ActivityLap`

**Tag keys**
- `ActivityID`
- `ActivitySelector`
- `Database_Name`
- `Device`
- `User_ID`
- `sport_tag`

**Field keys**
- `ActivityName`: `string`
- `Activity_ID`: `integer`
- `Avg_Cadence`: `integer`
- `Avg_HR`: `integer`
- `Avg_Power`: `integer`
- `Avg_Speed`: `float`
- `Avg_Stance_Time`: `float`
- `Avg_Step_Length`: `float`
- `Avg_Temperature`: `integer`
- `Avg_Vertical_Oscillation`: `float`
- `Avg_Vertical_Ratio`: `float`
- `Calories`: `integer`
- `Cycles`: `integer`
- `Distance`: `float`
- `Elapsed_Time`: `float`
- `Index`: `integer`
- `Max_HR`: `integer`
- `Max_Speed`: `float`
- `Sport`: `string`

### `ActivitySession`

**Tag keys**
- `ActivityID`
- `ActivitySelector`
- `Database_Name`
- `Device`
- `User_ID`
- `sport_tag`
- `sub_sport_tag`

**Field keys**
- `ActivityName`: `string`
- `Activity_ID`: `integer`
- `Aerobic_Training`: `float`
- `Anaerobic_Training`: `float`
- `Index`: `integer`
- `Lengths`: `integer`
- `Sport`: `string`
- `Sub_Sport`: `string`

### `ActivitySummary`

**Tag keys**
- `ActivityID`
- `ActivitySelector`
- `Database_Name`
- `Device`
- `User_ID`
- `activity_type_tag`

**Field keys**
- `Activity_ID`: `integer`
- `Device_ID`: `integer`
- `activityName`: `string`
- `activityType`: `string`
- `averageHR`: `float`
- `averageSpeed`: `float`
- `bmrCalories`: `float`
- `calories`: `float`
- `distance`: `float`
- `elapsedDuration`: `float`
- `hrTimeInZone_1`: `integer`
- `hrTimeInZone_2`: `integer`
- `hrTimeInZone_3`: `integer`
- `hrTimeInZone_4`: `integer`
- `hrTimeInZone_5`: `integer`
- `lapCount`: `integer`
- `locationName`: `string`
- `maxHR`: `float`
- `maxSpeed`: `float`
- `movingDuration`: `float`

### `BodyBatteryIntraday`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `BodyBatteryLevel`: `integer`

### `BodyComposition`

**Tag keys**
- `Database_Name`
- `Device`
- `Frequency`
- `SourceType`
- `User_ID`

**Field keys**
- `bmi`: `float`
- `bodyFat`: `float`
- `bodyWater`: `float`
- `boneMass`: `integer`
- `muscleMass`: `integer`
- `weight`: `float`

### `BreathingRateIntraday`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `BreathingRate`: `float`

### `DailyStats`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `activeKilocalories`: `float`
- `activeSeconds`: `integer`
- `activityStressDuration`: `integer`
- `activityStressPercentage`: `float`
- `averageSpo2`: `float`
- `bmrKilocalories`: `float`
- `bodyBatteryAtWakeTime`: `integer`
- `bodyBatteryChargedValue`: `integer`
- `bodyBatteryDrainedValue`: `integer`
- `bodyBatteryDuringSleep`: `integer`
- `bodyBatteryHighestValue`: `integer`
- `bodyBatteryLowestValue`: `integer`
- `floorsAscended`: `float`
- `floorsAscendedInMeters`: `float`
- `floorsDescended`: `float`
- `floorsDescendedInMeters`: `float`
- `highStressDuration`: `integer`
- `highStressPercentage`: `float`
- `highlyActiveSeconds`: `integer`
- `lowStressDuration`: `integer`
- `lowStressPercentage`: `float`
- `lowestSpo2`: `integer`
- `maxAvgHeartRate`: `integer`
- `maxHeartRate`: `integer`
- `mediumStressDuration`: `integer`
- `mediumStressPercentage`: `float`
- `minAvgHeartRate`: `integer`
- `minHeartRate`: `integer`
- `moderateIntensityMinutes`: `integer`
- `restStressDuration`: `integer`
- `restStressPercentage`: `float`
- `restingHeartRate`: `integer`
- `sedentarySeconds`: `integer`
- `sleepingSeconds`: `integer`
- `stressDuration`: `integer`
- `stressPercentage`: `float`
- `totalDistanceMeters`: `integer`
- `totalSteps`: `integer`
- `totalStressDuration`: `integer`
- `uncategorizedStressDuration`: `integer`
- `uncategorizedStressPercentage`: `float`
- `vigorousIntensityMinutes`: `integer`

### `DemoPoint`

**Tag keys**
- `DemoTag`

**Field keys**
- `DemoField`: `integer`

### `DerivedActivity`

**Tag keys**
- `ActivityID`
- `ActivitySelector`
- `Database_Name`
- `Device`
- `User_ID`
- `sport_tag`

**Field keys**
- `Activity_ID`: `integer`
- `best20m_distance_m`: `float`
- `best20m_hr_median`: `float`
- `best20m_vam_m_per_h`: `float`
- `best20m_vert_m_per_min`: `float`
- `best30m_vam_m_per_h`: `float`
- `best30m_vert_m_per_min`: `float`
- `best5m_grade_median`: `float`
- `best5m_speed_mps_raw`: `float`
- `best5m_vo2_all`: `float`
- `best5m_vo2_masked`: `float`
- `cs_mps`: `float`
- `cs_pace_s_per_km`: `float`
- `dprime_m`: `float`
- `gap_distance_km`: `float`
- `gap_distance_m`: `float`
- `lt_pace_s_per_km`: `float`
- `lthr_band`: `float`
- `lthr_min_contig_s`: `integer`
- `lthr_target_mps`: `float`
- `raw_distance_m_from_fit`: `float`
- `raw_distance_m_from_speed`: `float`
- `vam_hr_floor`: `float`
- `vam_speed_floor_mps`: `float`
- `vam_warmup_exclude_s`: `integer`
- `vo2_mask_grade_ceil`: `float`
- `vo2_mask_grade_floor`: `float`
- `vo2_mask_speed_floor_mps`: `float`
- `vo2max_est`: `float`

### `DeviceSync`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `Device_Name`: `string`
- `imageUrl`: `string`

### `FitnessAge`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `achievableFitnessAge`: `float`
- `chronologicalAge`: `float`
- `fitnessAge`: `float`

### `HRV_Intraday`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `hrvValue`: `integer`

### `HeartRateIntraday`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `HeartRate`: `integer`

### `PerformanceDaily`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `CS_mps`: `float`
- `CS_pace_s_per_km`: `float`
- `Dprime_m`: `float`
- `FitnessAge_model`: `float`
- `FitnessAge_model_source`: `string`
- `MountainFitness_source`: `string`
- `VAM20_best_42d_m_per_h`: `float`
- `VAM30_best_42d_m_per_h`: `float`
- `VO2max_est_run`: `float`
- `best5m_vo2_last`: `float`
- `gap_distance_km_last`: `float`
- `raw_distance_m_from_fit_last`: `float`
- `raw_distance_m_from_speed_last`: `float`

### `PhysiologyDaily`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `HRR`: `float`
- `HRmax_est`: `float`
- `HRmax_est_source`: `string`
- `RHR_7d_median`: `float`
- `Z1_High`: `float`
- `Z1_Low`: `float`
- `Z2_High`: `float`
- `Z2_Low`: `float`
- `Z3_High`: `float`
- `Z3_Low`: `float`
- `Z4_High`: `float`
- `Z4_Low`: `float`
- `Z5_High`: `float`
- `Z5_Low`: `float`

### `RacePredictions`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `time10K`: `integer`
- `time5K`: `integer`
- `timeHalfMarathon`: `integer`
- `timeMarathon`: `integer`

### `SleepIntraday`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `SleepMovementActivityLevel`: `float`
- `SleepMovementActivitySeconds`: `integer`
- `SleepStageLevel`: `float`
- `SleepStageSeconds`: `integer`
- `bodyBattery`: `integer`
- `heartRate`: `integer`
- `hrvData`: `float`
- `respirationValue`: `float`
- `sleepRestlessValue`: `integer`
- `spo2Reading`: `integer`
- `stressValue`: `integer`

### `SleepSummary`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `averageRespirationValue`: `float`
- `averageSpO2Value`: `float`
- `avgOvernightHrv`: `float`
- `avgSkinTempDeviationC`: `float`
- `avgSkinTempDeviationF`: `float`
- `avgSleepStress`: `float`
- `awakeCount`: `integer`
- `awakeSleepSeconds`: `integer`
- `bodyBatteryChange`: `integer`
- `deepSleepSeconds`: `integer`
- `highestRespirationValue`: `float`
- `highestSpO2Value`: `integer`
- `lightSleepSeconds`: `integer`
- `lowestRespirationValue`: `float`
- `lowestSpO2Value`: `integer`
- `remSleepSeconds`: `integer`
- `restingHeartRate`: `integer`
- `restlessMomentsCount`: `integer`
- `sleepScore`: `integer`
- `sleepTimeSeconds`: `integer`

### `StepsIntraday`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `StepsCount`: `integer`

### `StressIntraday`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `stressLevel`: `integer`

### `TrainingLoadDaily`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `ATL_7_TRIMP`: `float`
- `ATL_7_TSS`: `float`
- `CTL_42_TRIMP`: `float`
- `CTL_42_TSS`: `float`
- `HRmax_used`: `float`
- `RHR_used`: `float`
- `TRIMP`: `float`
- `TSB_TRIMP`: `float`
- `TSB_TSS`: `float`
- `TSS`: `float`
- `activities_used`: `integer`
- `gender_code_used`: `integer`
- `gender_is_known_used`: `integer`
- `lthr_used`: `float`

### `UserProfile`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `birth_year`: `integer`
- `gender`: `string`
- `gender_code`: `integer`
- `gender_is_known`: `integer`

### `UserProfileMaster`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `activity_id`: `integer`
- `age_years`: `integer`
- `birth_year`: `integer`
- `gender`: `string`
- `gender_code`: `integer`
- `gender_is_known`: `integer`
- `lthr_bpm`: `integer`
- `source`: `string`

### `VO2_Max`

**Tag keys**
- `Database_Name`
- `Device`
- `User_ID`

**Field keys**
- `VO2_max_value`: `float`
- `VO2_max_value_cycling`: `float`
