## Agent tool surface (MVP-first)

This project’s agent runs **inside Docker** so it can reach InfluxDB and Grafana on the internal network.

### `query_database` (read-only)
- **Input**: `influxql` string (InfluxQL)
- **Output**: JSON rows
- **Side effects**: none
- **Notes**: write-like keywords are rejected.

### `get_physiological_snapshot`
- **Input**: `window_days` (default 42)
- **Output**: JSON snapshot derived from DB measurements (not from in-process computed values)
- **Side effects**: none

### `assess_training_readiness`
- **Input**: `window_days` (default 42)
- **Output**: readiness score (0..100) + reasons + inputs used
- **Side effects**: none

### `update_grafana_dashboard_files` (planned)
- **Input**: dashboard spec or patch instructions
- **Output**: updated JSON under `Grafana_Dashboard/`
- **Side effects**: modifies repo files (reviewable)

### `push_grafana_dashboard_api` (planned)
- **Input**: dashboard JSON
- **Output**: Grafana API response
- **Side effects**: updates Grafana state via HTTP API

### `parse_gpx` (planned)
- **Input**: file path or raw GPX text
- **Output**: distance, elevation gain/loss, grade distribution, climb segments
- **Side effects**: none

### `generate_training_plan` (planned)
- **Input**: event target (date/distance/terrain), athlete profile, constraints, preferences
- **Output**: periodised multi-discipline plan + rationale + sessions
- **Side effects**: none (until stored)

### `store_plan` (planned)
- **Input**: plan JSON
- **Output**: persisted plan reference (file + optional DB write)
- **Side effects**: writes to filesystem and/or InfluxDB measurement(s)

