## Sports-science domain spec (seed)

This document defines the model the agent should reason within. It is intentionally explicit so the agent can recompute metrics from the DB reliably.

### Disciplines in scope
- Running (road)
- Trail running
- Cycling (road)
- Gravel cycling
- Strength training (gym)

### Primary signals (inputs from DB)
- **Recovery**: HRV (RMSSD), resting HR, sleep score/duration, subjective stress (if present)
- **Load**: TRIMP, TSS-like metrics, duration, intensity distribution
- **Performance proxies**: CP/CS, LTHR estimate, VO2 demand/est, pace/power zones

### Readiness philosophy (MVP)
- Produce a **0..100 readiness** score from recovery vs load balance.\n+- Prefer **agent-derived** readiness from DB; if Garmin provides TrainingReadiness, include it as a separate comparator input.\n+
### Load normalisation across disciplines (planned)
- Running/Cycling: use time-in-zone and/or TSS/TRIMP analogues\n+- Strength: use sRPE * duration and treat as separate fatigue channel\n+
### Periodisation preferences (planned)
- Default: recommend a plan shape based on time-to-event and athlete history (polarised/pyramidal/block)\n+- Always include: progression, recovery weeks, specificity phase, taper\n+
### Athlete profile fields (planned)
- HRmax, RHR\n+- Cycling FTP or CP\n+- Running threshold pace / CS\n+- Injury constraints, available days, preferred long-day\n+
