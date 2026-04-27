## Agent tool surface (HTTP API + UI)

This project exposes the agent as a FastAPI app (`ai-agent`) running inside Docker, so it can reach InfluxDB and Grafana over the internal Compose network.

This document is the **runtime reference** for what the agent can do and how it should behave.

### Core read-only endpoints

#### `POST /query` (InfluxQL read-only)
- **Input**: `{ "influxql": "SELECT ... " }`
- **Output**: `{ rows: [...], row_count: N }`
- **Side effects**: none
- **Guardrails**: rejects obvious write-like keywords (`DROP`, `DELETE`, `INTO`, `CREATE`, `ALTER`)

#### `POST /snapshot`
- **Input**: `{ "window_days": 42 }`
- **Output**: physiology + training snapshot derived from raw DB measurements
- **Side effects**: none

#### `POST /readiness`
- **Input**: `{ "window_days": 42 }`
- **Output**: `{ snapshot, readiness }` where readiness is 0..100 plus reasons/inputs
- **Side effects**: none

#### `POST /insights`
- **Input**: `{ "window_days": 42, "prompt": "..." }`
- **Output**: `{ snapshot, readiness, insights }` where `insights` is **markdown text**
- **Side effects**: none
- **Important behavior**: the agent performs **on-demand metric computation** based on the prompt and injects results into `snapshot.metrics.on_demand_metrics` before generating the narrative. This removes the need to “pre-derive” metrics for many questions.

### Metric computation endpoints (agent-calculated, from raw streams)

#### `GET /metrics/catalog`
- **Output**: list of supported metric names + descriptions

#### `POST /metrics/compute`
- **Input**: `{ "metrics": ["vo2_window_all", ...], "window_days": 42, "activity_limit": 20 }`
- **Output**: `{ computed, insights }` where `insights` is markdown summarising results

#### `POST /metrics/query`
- **Input**: `{ "window_days": 42, "query": "p95 hr and trimp last 30 days" }`
- **Output**: `{ picked, computed, insights }`
- **Notes**: keyword-based selection is used when no LLM is configured; otherwise an LLM can help select metrics from the catalog.

### Write / side-effect endpoints (explicitly gated)

#### `POST /activities/derive_all`
- **Purpose**: writes `AgentDerivedActivity` to InfluxDB (derived from raw `ActivityGPS` streams)
- **Input**: `{ "window_days": 42, "limit": 500 }`
- **Side effects**: **writes to InfluxDB**
- **Gate**: requires `AI_ALLOW_DB_WRITE=true` (otherwise returns HTTP 400 with a helpful message)

#### `POST /insights/store`
- **Purpose**: writes an `AgentInsights` measurement to InfluxDB
- **Side effects**: **writes to InfluxDB**
- **Gate**: requires `AI_ALLOW_DB_WRITE=true`

### Grafana helpers

#### `POST /grafana/write_dashboard_file`
- **Purpose**: validates the presence of `Grafana_Dashboard/AI-Coach.json` inside the container mount
- **Side effects**: none (file-provisioning happens via Grafana watching the mounted folder)

#### `POST /grafana/push_dashboard_api`
- **Purpose**: pushes `AI-Coach.json` to Grafana via HTTP API
- **Side effects**: updates Grafana state
- **Gate**: requires `GRAFANA_API_TOKEN`

### Garmin auth maintenance (MFA-aware)

Garmin Connect sessions are cached in `TOKEN_DIR` (default `~/.garminconnect`). In Compose, this is persisted via `./garminconnect-tokens:/home/appuser/.garminconnect`.

#### `POST /garmin/auth_status`
- **Purpose**: inspect token cache presence + freshness (does not validate tokens with Garmin)
- **Output**: token dir, file list, most recent mtime (UTC), and whether non-interactive credentials are configured

#### `POST /garmin/force_relogin`
- **Purpose**: delete cached token files so `garmin-fetch-data` is forced to re-auth on its next run
- **MFA note**: if MFA is required and non-interactive credentials are not configured, you may need to perform one interactive login to regenerate tokens.

