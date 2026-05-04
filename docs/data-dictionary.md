## Data dictionary (starter)

This file is a human-readable guide to the most important measurements and fields in InfluxDB.

It is intentionally incomplete at first. Workflow:

1. Run **`garmin-export-schema`** (with `INFLUXDB_*` env or `.env`) to emit `docs/schema.influxql.md` and see what exists.
2. Fill in meanings, units, and capture timing here.

### Conventions

- Times are stored in UTC in InfluxDB.
- Field units should be stated explicitly (bpm, ms, seconds, watts, m/s, etc.).

### Key measurements to document first

- `PhysiologyDaily`: resting HR, HRV, HR zones, etc.
- `TrainingLoadDaily`: CTL/ATL/TSB (or your chosen load model)
- `SleepSummary`: sleep score/duration/stages
- `DerivedActivity`: per-activity derived thresholds (CS/CP/LTHR), TRIMP/TSS-like fields
- `TrainingReadiness`: Garmin-provided readiness score (if available)
