## Sports-science domain spec (runtime reference)

> **Integration note (Claude / MCP):** This fork removed the in-container AI service. Use this file as **authoritative domain context** when you configure Claude (e.g. attach in Claude Desktop project knowledge, or mount `docs/` into an MCP server). Operational setup: **`docs/claude-mcp-integration.md`**.

This document defines the **sports-science reasoning model** the agent should operate within. It is intentionally explicit so the agent can recompute metrics from raw DB signals reliably.

This is a **runtime reference**: when the LLM writes insights, it should be grounded in these definitions and prefer **agent-calculated** values (computed from raw streams) over vendor/device estimates.

---

### Disciplines in scope
- Running (road, trail, treadmill)
- Cycling (road, gravel, indoor)
- Other activities may exist in Garmin data, but the agent's physiology methods are currently focused on run + ride

---

### Primary signals (inputs from DB)
- **Recovery**: HRV (RMSSD), resting HR, sleep score/duration, stress (if present)
- **Load**: TRIMP (HR-based), TSS-style load (power-based where possible), duration, intensity distribution
- **Performance proxies**: VO₂ demand/proxy, CS/CP/FTP proxies, LTHR proxy, pace/power distributions

---

### Readiness philosophy (MVP)
- Produce a **0..100 readiness** score from recovery vs load balance.
- Prefer **agent-derived** calculations from raw streams and/or agent-derived measurements.
- If Garmin provides `TrainingReadiness`, treat it as a **separate comparator** (never the sole source-of-truth).

---

### Agent-calculated metrics (implemented)

#### VO₂ / VO₂max proxies
- **Running VO₂ demand**: ACSM running metabolic equation using speed + grade.
  - Prefer Grade-Adjusted Pace/Speed (GAP) when available.
  - Clamp grades to reduce GPS noise; avoid downhill/coasting bias when possible via effort filters.
- **Cycling VO₂ proxy**: power→VO₂ conversion using gross efficiency and weight where available.

#### Threshold proxies
- **Running threshold**: Critical Speed (CS) proxy from best rolling mean speeds and/or simple CS models.
- **Cycling threshold**: FTP proxy from best 20-minute power (FTP ≈ 0.95 × P₂₀min) and/or CP-style models.
- **LTHR**: proxy via best steady ~30-minute efforts (e.g., best 30-min mean HR), with steadiness checks where possible.

#### Training load
- **TRIMP**:
  - Banister TRIMP: uses HR reserve fraction and a gender-dependent exponential weighting.
  - Edwards TRIMP: time-in-zone weighted sum (requires zones or HRmax-derived fallback).
- **TSS**:
  - Cycling: NP/IF-style power TSS when power streams exist and FTP/CP can be estimated.
  - HR-based analogue may be used only as an explicit fallback when only HR exists (label as HR-TSS / hybrid).

#### Percentiles / distributions
- **HR percentiles (e.g., p95)**: computed directly from raw `ActivityGPS.HeartRate` samples over a window, with downsampling safeguards for memory.

---

### On-demand vs pre-derived philosophy (required behaviour)
- The agent should answer metric questions **without requiring pre-derivation**.
- If the question implies a metric that is not already in the snapshot, the agent should compute it **on-demand from raw streams** and include:
  - the computed value(s)
  - the method used (high level)
  - at least one caveat about assumptions and data quality

---

### Explanation and anomaly triggers (required behaviour)
- Always include a short "why this number could differ" note when:
  - windowed VO₂ differs materially from last-activity VO₂
  - terrain/grade distribution is skewed (lots of descents/ascents)
  - sample size is low (few activities, short durations)
  - coasting / power dropouts are likely (cycling power with many zeros)
- When a difference is substantive (≥5 ml/kg/min VO₂), explicitly flag it and suggest likely drivers (terrain adjustment, effort filtering, sport mix, or data gaps).

---

### Load normalisation across disciplines (planned)
- Strength: use sRPE × duration and treat as a separate fatigue channel.

---

### Periodisation preferences (planned)
- Recommend a plan shape based on time-to-event and athlete history (polarised/pyramidal/block).
- Always include progression, recovery weeks, specificity, and taper.

---

### Athlete profile fields (planned)
- HRmax, RHR
- Cycling FTP/CP
- Running CS / threshold pace
- Injury constraints, available days, preferred long-day

---

### Training zones

Zone definitions should be derived from `UserProfileMaster.lthr_bpm` where available. If LTHR is absent, fall back to HRmax-percentage estimates and flag the assumption explicitly.

#### Heart rate zones (5-zone model based on % LTHR)

| Zone | Name               | % LTHR  | % HRmax (fallback) | Physiological purpose                           |
|------|--------------------|---------|--------------------|--------------------------------------------------|
| Z1   | Recovery           | <85%    | <68%               | Active recovery, capillary development           |
| Z2   | Aerobic base       | 85–89%  | 69–83%             | Fat oxidation, aerobic efficiency, volume base   |
| Z3   | Tempo              | 90–94%  | 84–88%             | Lactate clearance, threshold conditioning        |
| Z4   | Threshold          | 95–99%  | 89–93%             | Raise lactate threshold, sustained hard effort   |
| Z5   | VO₂max / Anaerobic | ≥100%   | ≥94%               | VO₂max stimulus, neuromuscular power             |

> Note: Z2 is the primary aerobic development zone and should constitute the majority of volume in polarised and pyramidal training models. Z3 is often overused by amateur athletes (the "grey zone") — flag if the athlete is spending disproportionate time there relative to Z2 and Z4/Z5.

#### Power zones — cycling (Coggan 7-zone model, % of FTP)

| Zone | Name                | % FTP    | Typical duration    |
|------|---------------------|----------|---------------------|
| Z1   | Active recovery     | <55%     | Unlimited           |
| Z2   | Endurance           | 56–75%   | Hours               |
| Z3   | Tempo               | 76–90%   | 20 min – 2 hr       |
| Z4   | Lactate threshold   | 91–105%  | 10–60 min           |
| Z5   | VO₂max              | 106–120% | 3–8 min             |
| Z6   | Anaerobic capacity  | 121–150% | 30 sec – 3 min      |
| Z7   | Neuromuscular power | >150%    | <30 sec             |

> Note: If FTP is agent-derived (20-min proxy), label it as estimated and note that actual FTP may differ by ±5%. Use NP (Normalised Power) for variable-effort rides when computing TSS.

#### Running pace zones (% of threshold pace / Critical Speed)

| Zone | Name              | % CS / threshold | Feel                            |
|------|-------------------|------------------|---------------------------------|
| Z1   | Easy / recovery   | <79%             | Conversational, very easy       |
| Z2   | Aerobic base      | 79–89%           | Comfortable, holdable for hours |
| Z3   | Tempo             | 90–95%           | Comfortably hard, focused       |
| Z4   | Threshold         | 96–100%          | Hard, sustainable ~30–60 min    |
| Z5   | VO₂max pace       | 101–110%         | Very hard, 3–8 min efforts      |
| Z6+  | Speed / anaerobic | >110%            | Near-maximal, short intervals   |

---

### Physiological norms (age and gender adjusted)

Use these to contextualise the athlete's metrics relative to population benchmarks. Always note that individual variation is high and trends within an individual are more meaningful than cross-sectional comparisons.

#### VO₂ max norms (ml/kg/min) — running

| Age   | Poor (M) | Fair (M) | Good (M) | Excellent (M) | Superior (M) | Poor (F) | Fair (F) | Good (F) | Excellent (F) | Superior (F) |
|-------|----------|----------|----------|---------------|--------------|----------|----------|----------|---------------|--------------|
| 20–29 | <38      | 38–43    | 44–50    | 51–56         | >56          | <28      | 28–35    | 36–41    | 42–46         | >46          |
| 30–39 | <36      | 36–41    | 42–47    | 48–53         | >53          | <26      | 26–33    | 34–38    | 39–43         | >43          |
| 40–49 | <33      | 33–38    | 39–44    | 45–50         | >50          | <24      | 24–30    | 31–35    | 36–41         | >41          |
| 50–59 | <30      | 30–35    | 36–41    | 42–46         | >46          | <21      | 21–27    | 28–32    | 33–37         | >37          |
| 60–69 | <26      | 26–31    | 32–36    | 37–41         | >41          | <18      | 18–23    | 24–28    | 29–33         | >33          |

> Note: Trained athletes should expect to sit in Excellent–Superior ranges. Use `UserProfileMaster.age_years` and `gender` to select the correct row. If VO₂ max appears consistently in the Poor–Fair range despite regular training history, consider flagging it and asking whether medications or health factors may be relevant.

#### Resting heart rate norms (bpm)

| Category  | Athletes | Excellent | Good  | Above avg | Average | Below avg | Poor |
|-----------|----------|-----------|-------|-----------|---------|-----------|------|
| RHR (bpm) | <50      | 50–55     | 56–61 | 62–65     | 66–69   | 70–73     | >74  |

> Note: RHR is most meaningful as a personal trend. An increase of ≥5 bpm above personal baseline on a given morning is a meaningful recovery flag. Beta blockers will artificially lower RHR and make these norms unreliable — see Medications section.

#### HRV norms (RMSSD, ms) — approximate population reference

| Age   | Low (fatigue / illness risk) | Typical range | High (well recovered / fit) |
|-------|------------------------------|---------------|-----------------------------|
| 20–29 | <40                          | 40–80         | >80                         |
| 30–39 | <35                          | 35–70         | >70                         |
| 40–49 | <30                          | 30–60         | >60                         |
| 50–59 | <25                          | 25–50         | >50                         |
| 60–69 | <20                          | 20–40         | >40                         |

> Note: HRV has extremely high inter-individual variability. Personal baseline (rolling 60-day mean) is far more meaningful than population norms. Beta blockers and some other medications directly alter HRV — see Medications section.

#### Sleep benchmarks for athletes

| Metric             | Minimum | Recommended | Notes                                              |
|--------------------|---------|-------------|-----------------------------------------------------|
| Total sleep        | 7 hr    | 8–9 hr      | Athletes typically need more than general population |
| Deep sleep (N3)    | 15%     | 20–25%      | Critical for physical recovery and GH release       |
| REM sleep          | 15%     | 20–25%      | Critical for cognitive recovery and memory          |
| Sleep efficiency   | >85%    | >90%        | Time asleep / time in bed                           |
| Overnight HRV      | Personal baseline −20% | At or above baseline | Key readiness signal        |

---

### Overtraining and recovery science

#### ATL / CTL / TSB model (Performance Management Chart)

Compute from daily TSS (or TRIMP if power unavailable).

- **ATL (Acute Training Load)** — short-term fatigue. Exponentially weighted moving average of daily TSS, time constant ~7 days. Represents current fatigue level.
- **CTL (Chronic Training Load)** — long-term fitness. Exponentially weighted moving average of daily TSS, time constant ~42 days. Represents fitness base.
- **TSB (Training Stress Balance)** = CTL − ATL. Represents "form" or readiness.

| TSB range   | Interpretation                                              |
|-------------|-------------------------------------------------------------|
| > +25       | Very fresh — possibly detrained or over-tapered             |
| +5 to +25   | Fresh and ready — ideal for race or key workout             |
| -10 to +5   | Neutral — normal training state                             |
| -10 to -30  | Accumulated fatigue — normal during a build phase           |
| < -30       | High fatigue risk — recovery required, injury risk elevated |

> Note: Always present TSB alongside CTL for context. An athlete with CTL 80 at TSB −20 is in a very different state to one with CTL 30 at TSB −20.

#### HRV interpretation guidelines

- Use a **7-day rolling mean** as the personal baseline. Do not react to single-day readings.
- Garmin overnight HRV (`avgOvernightHrv` in SleepSummary) is a reliable proxy for morning HRV protocol.
- **Meaningful drop**: >20% below personal 7-day rolling mean warrants reduced intensity or a rest day.
- **Sustained suppression**: HRV baseline trending downward over 2–4 weeks indicates accumulated fatigue or inadequate recovery. Recommend a load reduction week.
- **Sudden spike after sustained suppression**: May indicate the body transitioning into deeper overtraining, not just recovery — flag if this pattern appears.

#### Overreaching and overtraining indicators

If 3 or more of the following are present simultaneously, recommend a recovery week and suggest the athlete consider consulting a coach or sports medicine professional.

- Resting HR ≥5 bpm above personal 7-day baseline on 3+ consecutive days
- HRV >20% below personal 7-day baseline on 3+ consecutive days
- Sleep score consistently <60 for 5+ days despite adequate time in bed
- Body battery at wake time <30 on 3+ consecutive days
- High stress % (StressIntraday) consistently >40% of waking hours
- VO₂ proxy declining across comparable efforts over a 2–3 week window
- Pace or power declining at equivalent HR over a 2–3 week window

#### Recovery week guidelines

- Recommended every 3–4 weeks of progressive loading, or sooner if overreaching signals are present.
- Volume reduction: 40–60% of peak week volume.
- Intensity: maintain 1–2 short Z4/Z5 sessions to preserve neuromuscular sharpness.
- HRV and RHR should begin recovering within 3–5 days of a proper recovery week.

---

### Medications and their effects on training metrics

#### Behavioural rule for the agent — medication handling

- **Do NOT assume** medication is affecting results unless the user has explicitly confirmed a specific medication.
- **Do NOT introduce** medication topics unprompted in routine readiness summaries.
- **DO proactively ask** whether medications might be a factor when metrics appear consistently below age-adjusted norms AND no clear training, sleep, illness, or lifestyle explanation is apparent.

Suggested prompt when anomaly is detected and no other cause is apparent:
> "Your [metric] appears consistently below typical ranges for your age group. This can have several causes including training history, fatigue accumulation, or in some cases medication effects. Is medication worth factoring into this analysis?"

- Once the user confirms a medication, apply the relevant adjustments in all future analyses without repeatedly asking.
- Never speculate about what medications a person may be taking. Only work with what is confirmed.

---

#### Beta blockers (e.g., metoprolol, atenolol, bisoprolol, carvedilol, propranolol)

**Mechanism**: Block β-adrenergic receptors, blunting the sympathetic nervous system response to exercise and stress.

**Effects on metrics:**
- **Heart rate**: Significantly suppresses RHR (may read 40–55 bpm regardless of fitness level) and blunts maximal HR. HRmax may be reduced by 20–30 bpm or more.
- **HRV**: Directly alters autonomic modulation. HRV values are pharmacologically modified and do not reflect the same recovery state as in an unmedicated athlete. Personal trends are still meaningful; absolute values compared to population norms are not.
- **TRIMP / HR-based TSS**: Severely unreliable. HR-based load metrics underestimate true training stress because the HR response is blunted. Power-based TSS is strongly preferred for cyclists on beta blockers.
- **VO₂ max (Garmin device estimate)**: Likely artificially inflated. Garmin's algorithm sees fast pace at low HR and infers high fitness. Agent-calculated VO₂ from the ACSM equation using pace/power is more reliable.
- **Race predictions**: Garmin race predictions will be inaccurate. Flag explicitly.
- **Training zones**: HR-based zones are not usable for effort guidance. Use pace zones (running) or power zones (cycling) instead.
- **Body battery / readiness scores**: Garmin's algorithms are partly HR-derived and may behave unpredictably. Cross-reference with sleep and subjective signals.

**Agent behaviour when beta blockers are confirmed:**
- Prefer power-based (cycling) or pace-based (running) metrics over HR-based metrics in all calculations.
- Label all HR-derived metrics (TRIMP, HR zones, HR-based readiness) as unreliable due to pharmacological HR suppression.
- Explicitly note that Garmin VO₂ max estimates are likely inflated; prefer agent-calculated values.
- Explicitly flag that Garmin race predictions are not reliable.

---

#### Stimulants — ADHD medications (e.g., Vyvanse/lisdexamfetamine, Adderall/mixed amphetamine salts, Ritalin/methylphenidate, Concerta)

**Mechanism**: Increase dopamine and norepinephrine availability; sympathomimetic effects at therapeutic doses.

**Effects on metrics:**
- **Heart rate**: May elevate RHR by 5–15 bpm and increase HR at submaximal efforts. Exercise HR may trend higher than expected for the effort level — this does not indicate higher fitness.
- **Perceived exertion**: May reduce RPE at a given intensity, masking fatigue. Athletes may push harder than intended, increasing overtraining and injury risk.
- **Appetite**: Significant appetite suppression is common, particularly earlier in the day. Can affect fueling adequacy, glycogen availability, and recovery nutrition — may manifest as body weight loss or slower body composition changes than training load would predict.
- **Sleep**: Can disrupt sleep onset and reduce total sleep time if dose is active in the evening. Watch for declining sleep duration and score trends. REM sleep may be affected.
- **HRV**: May be mildly suppressed due to sympathetic activation, particularly if the dose is active during the overnight HRV measurement window.
- **Body battery**: May trend lower than expected if stimulant-driven sleep disruption is present.
- **Body composition**: Appetite suppression can lead to unintended weight or muscle mass loss over time — visible in BodyComposition trends alongside high training load.

**Agent behaviour when Vyvanse/stimulants are confirmed:**
- Note that HR may be mildly elevated above expected ranges; do not interpret this as a fitness change.
- Flag patterns of poor sleep onset or reduced sleep duration as potentially dose-timing related.
- Flag declining body weight or muscle mass alongside high training load as a potential fueling concern.
- Do not over-interpret mildly elevated RHR as a recovery failure without considering stimulant baseline effect.

---

#### SSRIs / SNRIs (e.g., sertraline/Zoloft, escitalopram/Lexapro, fluoxetine/Prozac, venlafaxine/Effexor, duloxetine/Cymbalta)

**Mechanism**: Modulate serotonin (and norepinephrine for SNRIs) reuptake. Generally low direct cardiac effects at therapeutic doses.

**Effects on metrics:**
- **Heart rate**: Minimal effect in most cases. Some SSRIs (particularly fluoxetine) may cause mild bradycardia or minor HR variability changes.
- **HRV**: Limited direct effect in most people, though some individuals show mild suppression. Less impactful than beta blockers or stimulants.
- **Sleep architecture**: SSRIs commonly suppress REM sleep, particularly in early weeks of use or after dose changes. This can cause lower sleep scores and reduced overnight HRV despite adequate total sleep time. Deep sleep is generally preserved.
- **Body weight**: Some SSRIs associated with weight gain over time (paroxetine most notably); others are weight-neutral. Monitor BodyComposition trends.
- **Exercise performance**: No direct impairment in most cases. Some individuals report reduced motivation or blunted affect that may reduce training intensity or consistency.
- **Fatigue**: Some individuals experience fatigue, particularly in early weeks of treatment or after dose changes.

**Agent behaviour when SSRIs/SNRIs are confirmed:**
- Note that reduced REM% in SleepSummary may be medication-related rather than a training load or recovery issue.
- Do not assume sleep quality issues are training-driven without considering SSRI effects on sleep architecture.
- Monitor BodyComposition weight trends in context of medication history.

---

#### Corticosteroids (e.g., prednisolone, prednisone, dexamethasone — oral or high-dose inhaled)

**Mechanism**: Suppress inflammation and immune response; significant systemic metabolic effects at therapeutic doses.

**Effects on metrics:**
- **Heart rate**: May elevate resting and exercise HR. Can cause palpitations and sleep disturbance.
- **Sleep**: Commonly disrupts sleep — insomnia, fragmented sleep, reduced sleep scores. Most pronounced with evening dosing.
- **Body composition**: Fluid retention causes weight increase not reflecting true mass gain. Potential muscle catabolism with prolonged use. BodyComposition data unreliable during and shortly after an active course.
- **Performance**: Significant reduction in endurance performance with prolonged use due to muscle catabolism and metabolic effects. Short courses (e.g., 5 days) have less impact.
- **Stress markers**: Elevate cortisol baseline; stress scores may trend high irrespective of actual psychological stress or training load.
- **Bone density**: Long-term use increases fracture risk; not captured in Garmin data but relevant to training advice.

**Agent behaviour when corticosteroids are confirmed:**
- Flag BodyComposition data as unreliable during and shortly after a course.
- Note that stress scores and sleep disruption may be medication-driven.
- Performance declines during a course are expected and should not be interpreted as fitness loss.

---

#### NSAIDs / anti-inflammatories (e.g., ibuprofen, naproxen, diclofenac)

**Mechanism**: COX inhibition reducing prostaglandin synthesis; analgesic and anti-inflammatory effects.

**Effects on metrics:**
- **Heart rate / HRV**: Minimal direct effect at standard doses.
- **Performance perception**: Can mask pain and discomfort signals, potentially allowing the athlete to train through early injury warning signs.
- **Recovery adaptation**: Evidence suggests NSAIDs blunt the inflammatory signalling required for training adaptation (muscle protein synthesis, mitochondrial biogenesis). Chronic use around training sessions is not recommended.
- **Kidney function**: Relevant during heavy training in heat; not captured in Garmin data but a safety consideration.

**Agent behaviour when NSAID use is mentioned:**
- Flag chronic use around training sessions as potentially blunting adaptation signals.
- Do not interpret pain-free training during NSAID use as confirmation of injury resolution.

---

#### General medication anomaly detection rule

If any of the following patterns are observed and no clear training, sleep, or lifestyle explanation is apparent, the agent should ask whether medications might be a contributing factor — using the suggested prompt in the behavioural rule above:

- VO₂ proxy or pace/power at threshold consistently in Poor–Fair range for age group despite regular training history
- RHR persistently outside expected range (very low or elevated) without a fitness explanation
- HRV baseline suppressed below age-group norms despite adequate sleep and manageable training load
- Sleep REM% consistently <15% without high training stress as the cause
- Body weight trending unexpectedly (up or down) without obvious nutrition or training cause
- Body battery at wake time consistently <35 despite adequate sleep duration and low training load

---

### Database schema (GarminStats — InfluxDB v1)

Always query the database before answering. Do not assume or fabricate data values.
If a query returns no results, say so and suggest why (e.g. date range, data not yet synced).

**DATABASE:** `GarminStats`

---

#### ACTIVITY DATA

**ActivityGPS** — per-GPS-point data recorded during an activity
```
Latitude            float     GPS latitude
Longitude           float     GPS longitude
Altitude            float     metres above sea level
Distance            float     cumulative distance (metres)
Speed               float     instantaneous speed (m/s)
GradeAdjustedSpeed  float     grade-adjusted speed — prefer this for VO₂ running calcs
HeartRate           float     bpm — use for HR percentiles and TRIMP
Cadence             integer   steps/min (running) or rpm (cycling)
Fractional_Cadence  float     sub-integer cadence component
Power               integer   watts — use for cycling TSS/NP calcs
Accumulated_Power   integer   cumulative power (watts)
RunningEfficiency   float     Garmin running efficiency metric
Stance_Time         float     ground contact time (ms)
Step_Length         float     step length (cm)
Vertical_Oscillation float    vertical oscillation (cm)
Vertical_Ratio      float     vertical oscillation ratio (%)
Temperature         integer   ambient temperature (°C)
DurationSeconds     float     elapsed time at this point (s)
ActivityName        string    activity label
Activity_ID         integer   join key — links to ActivityLap, ActivitySession, ActivitySummary
```

**ActivityLap** — per-lap split summary within an activity
```
Avg_HR              integer   average heart rate (bpm)
Max_HR              integer   max heart rate (bpm)
Avg_Speed           float     average speed (m/s)
Max_Speed           float     max speed (m/s)
Avg_Cadence         integer   average cadence
Avg_Power           integer   average power (watts)
Avg_Stance_Time     float     average ground contact time (ms)
Avg_Step_Length     float     average step length (cm)
Avg_Vertical_Oscillation float average vertical oscillation (cm)
Avg_Vertical_Ratio  float     average vertical oscillation ratio (%)
Distance            float     lap distance (metres)
Elapsed_Time        float     lap elapsed time (s)
Calories            integer   calories in lap
Cycles              integer   total cycles (steps or pedal strokes)
Avg_Temperature     integer   average temperature (°C)
Index               integer   lap number
Sport               string    sport type
ActivityName        string    activity label
Activity_ID         integer   join key
```

**ActivitySession** — training effect and sport classification per activity
```
Aerobic_Training    float     Garmin Aerobic Training Effect (0–5 scale)
Anaerobic_Training  float     Garmin Anaerobic Training Effect (0–5 scale)
Sport               string    primary sport
Sub_Sport           string    sub-sport (e.g. road, trail, indoor)
Lengths             integer   pool lengths (swim only)
Index               integer   session index
ActivityName        string    activity label
Activity_ID         integer   join key
```
> Note: Use Aerobic_Training and Anaerobic_Training to assess training stimulus type. Scores of 3–4 are productive; 5 = overreaching.

**ActivitySummary** — high-level per-activity summary
```
activityType        string    activity type
activityName        string    activity label
averageHR           float     average heart rate (bpm)
maxHR               float     max heart rate (bpm)
averageSpeed        float     average speed (m/s)
maxSpeed            float     max speed (m/s)
distance            float     total distance (metres)
calories            float     total calories
bmrCalories         float     BMR calories
elapsedDuration     float     total elapsed time (s)
movingDuration      float     moving time (s)
hrTimeInZone_1      integer   seconds in HR zone 1
hrTimeInZone_2      integer   seconds in HR zone 2
hrTimeInZone_3      integer   seconds in HR zone 3
hrTimeInZone_4      integer   seconds in HR zone 4
hrTimeInZone_5      integer   seconds in HR zone 5
lapCount            integer   number of laps
locationName        string    location label
Activity_ID         integer   join key
Device_ID           integer   recording device
```
> Note: hrTimeInZone fields are in seconds. Use these to assess training polarisation and zone distribution across a block.

---

#### DAILY WELLNESS

**DailyStats** — daily aggregated wellness and activity metrics
```
totalSteps                    integer   total steps
totalDistanceMeters           integer   total distance (metres)
activeKilocalories            float     active calories burned
bmrKilocalories               float     BMR calories
activeSeconds                 integer   active time (s)
highlyActiveSeconds           integer   highly active time (s)
sedentarySeconds              integer   sedentary time (s)
sleepingSeconds               integer   time asleep (s)
moderateIntensityMinutes      integer   WHO moderate intensity minutes
vigorousIntensityMinutes      integer   WHO vigorous intensity minutes
restingHeartRate              integer   resting HR (bpm) — key recovery indicator
minHeartRate                  integer   daily minimum HR
maxHeartRate                  integer   daily maximum HR
minAvgHeartRate               integer   minimum average HR
maxAvgHeartRate               integer   maximum average HR
averageSpo2                   float     average blood oxygen (%)
lowestSpo2                    integer   lowest SpO₂ reading (%)
bodyBatteryAtWakeTime         integer   body battery on waking (0–100) — key readiness signal
bodyBatteryHighestValue       integer   peak body battery
bodyBatteryLowestValue        integer   lowest body battery
bodyBatteryChargedValue       integer   body battery charged during day
bodyBatteryDrainedValue       integer   body battery drained during day
bodyBatteryDuringSleep        integer   body battery change during sleep
stressDuration                integer   total stress duration (s)
restStressDuration            integer   rest-state stress duration (s)
lowStressDuration             integer   low stress duration (s)
mediumStressDuration          integer   medium stress duration (s)
highStressDuration            integer   high stress duration (s)
activityStressDuration        integer   activity-related stress duration (s)
uncategorizedStressDuration   integer   uncategorized stress duration (s)
stressPercentage              float     overall stress %
restStressPercentage          float     rest stress %
lowStressPercentage           float     low stress %
mediumStressPercentage        float     medium stress %
highStressPercentage          float     high stress %
activityStressPercentage      float     activity stress %
uncategorizedStressPercentage float     uncategorized stress %
floorsAscended                float     floors ascended (count)
floorsDescended               float     floors descended (count)
floorsAscendedInMeters        float     ascent in metres
floorsDescendedInMeters       float     descent in metres
```
> Note: Key readiness indicators are `restingHeartRate`, `bodyBatteryAtWakeTime`, and stress distribution percentages. Use together rather than in isolation.

**BodyBatteryIntraday** — Garmin Body Battery throughout the day
```
BodyBatteryLevel    integer   0–100 energy reserve estimate
```
> Note: Morning values below 40 suggest insufficient overnight recovery. Trend across days is more meaningful than single readings.

**BreathingRateIntraday** — breathing rate throughout the day
```
BreathingRate       float     breaths per minute
```

**HeartRateIntraday** — heart rate throughout the day
```
HeartRate           integer   bpm
```

**StepsIntraday** — step count throughout the day
```
StepsCount          integer   steps in interval
```

---

#### HRV & STRESS

**HRV_Intraday** — heart rate variability measurements
```
hrvValue            integer   HRV in milliseconds (RMSSD proxy)
```
> Note: Higher HRV indicates better autonomic recovery. Use a 7-day rolling average rather than single readings. A drop >20% from personal baseline warrants reduced load. Beta blockers alter HRV values — see Medications section.

**StressIntraday** — stress level throughout the day
```
stressLevel         integer   0–100 stress score; -1 = activity/no data
```
> Note: 0–25 = rest, 26–50 = low, 51–75 = medium, 76–100 = high. Sustained high stress alongside low HRV is a strong recovery flag.

---

#### SLEEP

**SleepSummary** — nightly sleep summary
```
sleepScore                  integer   Garmin sleep score (0–100)
sleepTimeSeconds            integer   total sleep time (s)
deepSleepSeconds            integer   deep (N3) sleep (s)
lightSleepSeconds           integer   light (N1/N2) sleep (s)
remSleepSeconds             integer   REM sleep (s)
awakeSleepSeconds           integer   time awake during sleep (s)
awakeCount                  integer   number of wake events
restlessMomentsCount        integer   restless movement count
avgOvernightHrv             float     average overnight HRV — best single recovery marker
restingHeartRate            integer   resting HR during sleep (bpm)
avgSleepStress              float     average stress during sleep
averageSpO2Value            float     average blood oxygen during sleep (%)
highestSpO2Value            integer   highest SpO₂ during sleep (%)
lowestSpO2Value             integer   lowest SpO₂ during sleep (%)
averageRespirationValue     float     average respiration rate (breaths/min)
highestRespirationValue     float     highest respiration rate
lowestRespirationValue      float     lowest respiration rate
avgSkinTempDeviationC       float     skin temperature deviation from baseline (°C)
avgSkinTempDeviationF       float     skin temperature deviation from baseline (°F)
bodyBatteryChange           integer   body battery delta overnight
```
> Note: Quality athlete sleep typically includes >20% deep sleep and >20% REM. `avgOvernightHrv` is the single best recovery marker in this dataset. Low SpO₂ (<90%) warrants attention. Reduced REM% may be medication-related — see Medications section.

**SleepIntraday** — per-epoch granular sleep data
```
SleepStageLevel              float     sleep stage numeric level
SleepStageSeconds            integer   seconds in this stage
SleepMovementActivityLevel   float     movement level
SleepMovementActivitySeconds integer   seconds of movement
heartRate                    integer   HR at epoch (bpm)
hrvData                      float     HRV at epoch (ms)
respirationValue             float     respiration rate at epoch
stressValue                  integer   stress level at epoch
bodyBattery                  integer   body battery at epoch
spo2Reading                  integer   SpO₂ at epoch (%)
sleepRestlessValue           integer   restlessness value
```

---

#### BODY COMPOSITION

**BodyComposition** — body composition measurements
```
weight              float     body weight (kg)
bmi                 float     body mass index
bodyFat             float     body fat percentage (%)
bodyWater           float     body water percentage (%)
muscleMass          integer   muscle mass (kg)
boneMass            integer   bone mass (kg)
```

---

#### FITNESS METRICS

**FitnessAge** — Garmin fitness age estimates
```
fitnessAge              float   estimated physiological age
chronologicalAge        float   actual age
achievableFitnessAge    float   best achievable fitness age given current data
```

**RacePredictions** — Garmin predicted race times
```
time5K              integer   predicted 5K time (seconds)
time10K             integer   predicted 10K time (seconds)
timeHalfMarathon    integer   predicted half marathon time (seconds)
timeMarathon        integer   predicted marathon time (seconds)
```
> Note: Always convert seconds to HH:MM:SS when presenting. If beta blockers are confirmed, Garmin predictions are unreliable — flag this explicitly.

**VO2_Max** — VO₂ max estimates
```
VO2_max_value           float   running VO₂ max estimate (ml/kg/min)
VO2_max_value_cycling   float   cycling VO₂ max estimate (ml/kg/min)
```
> Note: Prefer agent-calculated VO₂ proxies from raw streams. A difference ≥5 ml/kg/min between device and agent estimates should be flagged. If beta blockers are confirmed, Garmin's HR-based VO₂ estimate will likely be inflated — explicitly prefer agent-calculated values.

---

#### USER DATA

**UserProfile** — basic user profile
```
birth_year      integer   year of birth
gender          string    gender label
gender_code     integer   gender numeric code
gender_is_known integer   whether gender is confirmed
```

**UserProfileMaster** — extended profile with fitness benchmarks
```
birth_year      integer   year of birth
age_years       integer   current age
gender          string    gender label
gender_code     integer   gender numeric code
gender_is_known integer   whether gender is confirmed
lthr_bpm        integer   Lactate Threshold Heart Rate (bpm) — critical for zone setting
activity_id     integer   reference activity
source          string    data source
```
> Note: `lthr_bpm` is the most important field here. Use it to derive all HR training zones. If absent, fall back to best 30-min mean HR proxy and flag the assumption. If beta blockers are confirmed, LTHR derived from HR data is unreliable — use pace/power zones instead.

---

### Query guidelines

- Always apply a time filter to avoid scanning full history unnecessarily.
  - Example: `WHERE time >= now() - 30d`
- Join on `Activity_ID` across `ActivityGPS`, `ActivityLap`, `ActivitySession`, and `ActivitySummary` to build complete activity profiles.
- Use `UserProfileMaster.lthr_bpm` to contextualise all heart rate zone analysis.
- When assessing recovery, use `avgOvernightHrv`, `restingHeartRate`, `bodyBatteryAtWakeTime`, and `sleepScore` together — no single metric is sufficient.
- For intraday measurements (BodyBatteryIntraday, HeartRateIntraday, HRV_Intraday, StressIntraday), aggregate with `MEAN()` or `PERCENTILE()` as appropriate for the question.
- If a query returns empty results, report it clearly and suggest likely causes (date range, activity type filter, data not synced).
