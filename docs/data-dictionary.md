## Data dictionary (starter)

This file is a human-readable guide to the most important measurements/fields in InfluxDB.

It is intentionally incomplete at first; the workflow is:\n+- run `ai-agent export-schema` to see what exists\n+- fill in meanings, units, and capture timing\n+
### Conventions
- Times are stored in UTC in InfluxDB.\n+- Field units should be stated explicitly (bpm, ms, seconds, watts, m/s, etc.).\n+\n+### Key measurements to document first\n+- `PhysiologyDaily`: resting HR, HRV, HR zones, etc.\n+- `TrainingLoadDaily`: CTL/ATL/TSB (or your chosen load model)\n+- `SleepSummary`: sleep score/duration/stages\n+- `DerivedActivity`: per-activity derived thresholds (CS/CP/LTHR), TRIMP/TSS-like fields\n+- `TrainingReadiness`: Garmin-provided readiness score (if available)\n+\n*** End Patch"} 
