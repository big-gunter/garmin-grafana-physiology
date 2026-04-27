## Sports-science domain spec (runtime reference)

This document defines the **sports-science reasoning model** the agent should operate within. It’s intentionally explicit so the agent can recompute metrics from raw DB signals reliably.

This is a **runtime reference**: when the LLM writes insights, it should be grounded in these definitions and prefer **agent-calculated** values (computed from raw streams) over vendor/device estimates.

### Disciplines in scope
- Running (road, trail, treadmill)
- Cycling (road, gravel, indoor)
- Other activities may exist in Garmin data, but the agent’s physiology methods are currently focused on run + ride

### Primary signals (inputs from DB)
- **Recovery**: HRV (RMSSD), resting HR, sleep score/duration, stress (if present)
- **Load**: TRIMP (HR-based), TSS-style load (power-based where possible), duration, intensity distribution
- **Performance proxies**: VO₂ demand/proxy, CS/CP/FTP proxies, LTHR proxy, pace/power distributions

### Readiness philosophy (MVP)
- Produce a **0..100 readiness** score from recovery vs load balance.
- Prefer **agent-derived** calculations from raw streams and/or agent-derived measurements.
- If Garmin provides `TrainingReadiness`, treat it as a **separate comparator** (never the sole source-of-truth).

### Agent-calculated metrics (implemented)

#### VO₂ / VO₂max proxies
- **Running VO₂ demand**: ACSM running metabolic equation using speed + grade.
  - Prefer Grade-Adjusted Pace/Speed (GAP) when available.
  - Clamp grades to reduce GPS noise; avoid downhill/coasting bias when possible via effort filters.
- **Cycling VO₂ proxy**: power→VO₂ conversion using gross efficiency and weight where available.

#### Threshold proxies
- **Running threshold**: Critical Speed (CS) proxy from best rolling mean speeds and/or simple CS models.
- **Cycling threshold**: FTP proxy from best 20-minute power (\(FTP \approx 0.95 \times P_{20min}\)) and/or CP-style models.
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

### On-demand vs pre-derived philosophy (required behavior)
- The agent should answer metric questions **without requiring pre-derivation**.
- If the question implies a metric that is not already in the snapshot, the agent should compute it **on-demand from raw streams** and include:
  - the computed value(s)
  - the method used (high level)
  - at least one caveat about assumptions and data quality

### Explanation and anomaly triggers (required behavior)
- Always include a short “why this number could differ” note when:
  - windowed VO₂ differs materially from last-activity VO₂
  - terrain/grade distribution is skewed (lots of descents/ascents)
  - sample size is low (few activities, short durations)
  - coasting / power dropouts are likely (cycling power with many zeros)
- When a difference is substantive (rule of thumb: \(\ge 5\) ml/kg/min VO₂), explicitly flag it and suggest likely drivers (terrain adjustment, effort filtering, sport mix, or data gaps).

### Load normalisation across disciplines (planned)
- Strength: use sRPE × duration and treat as a separate fatigue channel.

### Periodisation preferences (planned)
- Recommend a plan shape based on time-to-event and athlete history (polarised/pyramidal/block).
- Always include progression, recovery weeks, specificity, and taper.

### Athlete profile fields (planned)
- HRmax, RHR
- Cycling FTP/CP
- Running CS / threshold pace
- Injury constraints, available days, preferred long-day
