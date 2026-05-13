# Garmin Grafana Physiology

Self-host a **Garmin Connect → InfluxDB → Grafana** pipeline on your own machine, with optional **WHOOP** integration and **Model Context Protocol (MCP)** servers so tools like Claude Desktop can query your metrics read-only, keep a small **SQLite "agent memory"** for prior conclusions, and optionally drive Grafana's HTTP API. All data stays local unless you choose to expose an HTTPS MCP gateway.

> **Trademarks:** Garmin and Grafana are trademarks of their respective owners. This project is independent and not affiliated with Garmin Ltd., Grafana Labs, or WHOOP Inc.

## Contents

- [What you get](#what-you-get)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Local deployment](#local-deployment)
- [Cloud deployment](#cloud-deployment)
- [WHOOP integration](#whoop-integration)
- [MCP servers](#mcp-servers)
- [Grafana dashboards](#grafana-dashboards)
- [Fetcher data coverage](#fetcher-data-coverage)
- [Configuration reference](#configuration-reference)
- [Bulk historical fetch](#bulk-historical-fetch)
- [Manual file import](#manual-file-import)
- [InfluxDB backup and restore](#influxdb-backup-and-restore)
- [Day-to-day operations](#day-to-day-operations)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Acknowledgments and limitations](#acknowledgments-and-limitations)

---

## What you get

- **Continuous Garmin sync** into **InfluxDB 1.x** (`GarminStats` database). The fetcher wakes every `UPDATE_INTERVAL_SECONDS`, compares the last InfluxDB timestamp against recent Garmin uploads, and backfills any gap.
- **WHOOP sync** (optional) — recovery, sleep, strain, and workouts written to the same database using distinct measurement names (`WhoopRecovery`, `WhoopSleep`, `WhoopStrain`, `WhoopWorkout`). Fetches in 30-day chunks to stay well inside API rate limits.
- **Grafana** dashboards pre-provisioned from `Grafana_Dashboard/`: activity, sleep, stress, HRV, steps heatmaps, workout GPS tracks, training load, and physiological estimates.
- **MCP servers** (Python 3.13 via `uv`):
  - **`garmin-mcp`** — read-only InfluxQL + SQLite agent memory + `docs://agent-domain` resource (sports-science context).
  - **`garmin-mcp-influx`** — InfluxQL + domain doc only (no memory DB).
  - **`garmin-mcp-grafana`** — Grafana HTTP API helpers (list/search dashboards, render panels).
  - **`garmin-mcp-http`** — same tools as `garmin-mcp` over **streamable HTTP** (for remote clients behind TLS + `MCP_AUTH_TOKEN`).
  - **`garmin-export-schema`** — exports live InfluxDB schema hints to `docs/data-dictionary.md`.
- **MCP gateway** (`deploy/mcp_server/`) — a separate OAuth-protected HTTP gateway for the cloud stack (GitHub OAuth, bearer token).
- **Documentation** under `docs/`: `agent-domain.md`, `claude-mcp-integration.md`, `manual-import-instructions.md`, `data-dictionary.md`, `schema.influxql.md`.

There is no in-stack AI container; you attach your own client (e.g. Claude Desktop) via MCP.

---

## Architecture

### Local stack (`compose.yml`)

```
Garmin Connect API
      │
      ▼
garmin-fetch-data  ──writes──▶  influxdb (GarminStats, :8086)
                                      │
                               grafana (:3000)  ◀── browser
                                      │
                          [optional] mcp-gateway (:8000)  ◀── Claude Desktop / remote
```

| Service | Role |
|---|---|
| **`garmin-fetch-data`** | Pulls from Garmin APIs, computes derived metrics, writes to Influx. Session tokens live in `garminconnect-tokens/` (bind mount). |
| **`influxdb`** | InfluxDB **1.11** (`GarminStats`). Bound to `127.0.0.1:8086` so host MCP/CLI can connect; comment out `ports` if you want DB-only on the Docker network. |
| **`grafana`** | UI on `http://localhost:3000`. Dashboards provisioned from `Grafana_Dashboard/`. |
| **`mcp-gateway`** *(profile `mcp-public`)* | Runs `garmin-mcp-http` on the internal network; front with HTTPS on the host. |

All services share bridge network **`garmin-grafana-internal`** and address each other by service name.

### Cloud stack (`deploy/docker-compose.yml`)

| Service | Role |
|---|---|
| **`cloudflared`** | Cloudflare tunnel — no open inbound ports required. |
| **`influxdb`** | Same 1.11 image; data in `/opt/physiology/influxdb`. |
| **`grafana`** | Provisioned from `deploy/grafana/datasource.yaml`. |
| **`garmin-fetch-data`** | Same fetcher image built from repo root `Dockerfile`. |
| **`mcp-server`** | `deploy/mcp_server/` — GitHub OAuth + bearer token gateway. |
| **`whoop-fetch-data`** *(profile `whoop`)* | WHOOP sync; shares the same `GarminStats` database. |

---

## Requirements

- **Docker** and **Docker Compose** (v2)
- **Git**
- **[uv](https://docs.astral.sh/uv/)** — only needed if you run MCP servers on the host (not required for Docker-only use)
- A **Garmin Connect** account
- *(Optional)* A **WHOOP** account and a registered developer app

---

## Local deployment

Runs the full stack on your machine. All data stays local; Grafana is at `http://localhost:3000`.

### 1. Clone and create directories

```bash
git clone https://github.com/big-gunter/garmin-grafana-physiology.git
cd garmin-grafana-physiology
mkdir -p garminconnect-tokens
sudo chown -R 1000:1000 garminconnect-tokens
```

### 2. Create your `.env`

```bash
cp .env.example .env
# Edit .env — keep INFLUXDB_* values aligned with compose.yml
```

Never commit `.env`.

### 3. Build and start

```bash
docker compose build
docker compose up -d
```

### 4. First Garmin login

If you have not set `GARMINCONNECT_EMAIL` and `GARMINCONNECT_BASE64_PASSWORD` in `compose.yml`, authenticate interactively once:

```bash
docker compose run --rm garmin-fetch-data
```

Complete email, password, and 2FA; tokens persist in `garminconnect-tokens/`.

### 5. Verify

Open **`http://localhost:3000`** (default `admin` / `admin` — change it). Follow logs:

```bash
docker compose logs -f garmin-fetch-data
```

The first sync backfills roughly the last week. For older data see [Bulk historical fetch](#bulk-historical-fetch).

---

## Cloud deployment

Runs on a remote server with a Cloudflare tunnel (no open ports), Cloudflare Access protecting Grafana, GitHub OAuth protecting the MCP server, and automated backups. Tested on Hetzner CAX21 ARM (Ubuntu 22.04).

The cloud stack lives entirely under `deploy/`. Run all commands from that directory unless noted.

### Setup scripts

| Script / file | What it does |
|---|---|
| `deploy/setup/01_server_setup.sh` | Install Docker, UFW firewall, fail2ban, move SSH to port 22444 |
| `deploy/setup/02_folders.sh` | Create `/opt/physiology` data dirs, patch dashboard JSON, copy `.env.example` |
| `deploy/setup/03_influxdb_users.sh` | Create `garmin_writer` and `mcp_reader` InfluxDB users with correct grants |
| `deploy/setup/04_backup.sh` | Example nightly cron: InfluxDB portable backup + Grafana volume backup |
| `deploy/setup/05_grafana_token.md` | Instructions for creating a Grafana service account token for MCP |
| `deploy/setup/06_whoop_auth.md` | Full WHOOP OAuth setup guide |
| `deploy/setup/07_whoop_influxdb_reader.sh` | Verify `mcp_reader` grants include WHOOP measurements |

### Step-by-step

```bash
# 1. Clone to server
git clone https://github.com/big-gunter/garmin-grafana-physiology.git /opt/physiology-repo
cd /opt/physiology-repo

# 2. Harden server (SSH moves to port 22444 — reconnect on that port afterwards)
bash deploy/setup/01_server_setup.sh

# 3. Create data directories and .env template
bash deploy/setup/02_folders.sh

# 4. Fill in credentials
nano deploy/.env  # see Configuration reference below

# 5. Build the Garmin fetcher image
cd deploy && docker compose build garmin-fetch-data

# 6. Start InfluxDB, then create users
docker compose up -d influxdb
# wait ~10 seconds for healthy, then:
bash /opt/physiology-repo/deploy/setup/03_influxdb_users.sh

# 7. Authenticate with Garmin (interactive, one-time)
docker compose run --rm garmin-fetch-data

# 8. Start everything
docker compose up -d
```

For Cloudflare tunnel setup, GitHub OAuth app creation, and connecting Claude.ai, see **`deploy/README.md`**.

---

## WHOOP integration

WHOOP data is written to the same `GarminStats` InfluxDB database. The `whoop-fetch-data` service is gated behind the Docker Compose **`whoop`** profile so the rest of the stack runs unchanged without it.

### InfluxDB measurements

| Measurement | Key fields |
|---|---|
| `WhoopRecovery` | `recovery_score`, `hrv_rmssd_milli`, `resting_heart_rate`, `spo2_percentage`, `skin_temp_celsius`, `sleep_need_baseline_milli` |
| `WhoopSleep` | `score_total`, `total_light_sleep_milli`, `total_slow_wave_sleep_milli`, `total_rem_sleep_milli`, `total_awake_milli`, `time_in_bed_millis`, `disturbance_count`, `respiratory_rate`, `sleep_efficiency_percentage` |
| `WhoopStrain` | `day_strain`, `kilojoule`, `average_heart_rate`, `max_heart_rate` |
| `WhoopWorkout` | `score_strain`, `average_heart_rate`, `max_heart_rate`, `kilojoule`, `distance_meter`, `altitude_gain_meter`, `zone_zero_milli` … `zone_five_milli` |

### Step 1 — Register a WHOOP developer app

1. Go to [developer-dashboard.whoop.com](https://developer-dashboard.whoop.com) and sign in.
2. Create a new application — **Redirect URI:** `http://localhost:8080/callback`.
3. Enable all of these scopes: `read:recovery`, `read:cycles`, `read:sleep`, `read:profile`, `read:workout`, `read:body_measurement`, `offline`.
4. Copy **Client ID** and **Client Secret** into `deploy/.env`:

   ```
   WHOOP_CLIENT_ID=your_client_id
   WHOOP_CLIENT_SECRET=your_client_secret
   ```

> **Important:** `read:cycles` is required for strain/cycle data. Tokens issued without it will get 401 on the `/v2/cycle` endpoint even if all other endpoints work.

### Step 2 — Create the token directory on the server

```bash
mkdir -p /opt/physiology/whoop-tokens
chown -R 1000:1000 /opt/physiology/whoop-tokens
```

### Step 3 — Run the initial auth flow locally

The browser redirect must happen on your **local machine**, not the server. Run this from your laptop with the repo checked out:

```bash
cd garmin-grafana-physiology
WHOOP_CLIENT_ID=your_client_id \
WHOOP_CLIENT_SECRET=your_client_secret \
WHOOP_TOKEN_DIR=./whoop-tokens-local \
python -m whoop.whoop_auth
```

A browser tab opens. After you approve permissions, the script prints "Authorization successful — tokens saved." Tokens are written to `./whoop-tokens-local/whoop_tokens.json`.

Alternatively, via Docker:

```bash
docker compose --profile whoop run --rm -p 8080:8080 whoop-fetch-data \
  python -m whoop.whoop_auth
```

### Step 4 — Copy tokens to the server

```bash
scp ./whoop-tokens-local/whoop_tokens.json \
    root@your-server-ip:/opt/physiology/whoop-tokens/whoop_tokens.json
```

### Step 5 — Build and start the WHOOP service

```bash
cd /opt/physiology-repo/deploy
docker compose --profile whoop build whoop-fetch-data
docker compose --profile whoop up -d whoop-fetch-data
docker compose logs -f whoop-fetch-data
```

The service starts from the last synced date (or 90 days ago on first run) and stays up to date every `UPDATE_INTERVAL_SECONDS`.

### WHOOP backfill

```bash
cd /opt/physiology-repo/deploy
docker compose --profile whoop run --rm \
  -e MANUAL_START_DATE=2023-01-01 \
  -e MANUAL_END_DATE=2026-01-01 \
  whoop-fetch-data
```

Data is fetched in 30-day chunks (4 API calls per chunk — one per measurement type). `SKIP_EXISTING_DAILY=True` is the default; re-running the backfill is safe and will not create duplicate data.

### Token refresh notes

Access tokens expire after 1 hour and are refreshed automatically. On startup the fetcher logs the token's expiry time and whether `refresh_token` is present. If you see a warning that `refresh_token` is absent, re-run the auth flow (Step 3–4) to generate a new token file — a refresh cannot add missing OAuth scopes.

---

## MCP servers

The MCP servers expose your Influx data to AI clients (Claude Desktop, etc.) via the [Model Context Protocol](https://modelcontextprotocol.io). They run on the **host** (not in Docker) using `uv`, or as a remote HTTP gateway behind TLS.

### Install

```bash
uv sync   # from repo root — creates .venv and installs all console scripts
```

### Available servers

| Command | Transport | What it provides |
|---|---|---|
| `garmin-mcp` | stdio (Claude Desktop) | InfluxQL queries + SQLite agent memory + `docs://agent-domain` resource |
| `garmin-mcp-influx` | stdio | InfluxQL queries + domain doc (no memory DB) |
| `garmin-mcp-grafana` | stdio | Grafana API: list dashboards, search panels, get datasources |
| `garmin-mcp-http` | Streamable HTTP | Same tools as `garmin-mcp` over HTTP for remote clients |
| `garmin-export-schema` | CLI (one-shot) | Exports live InfluxDB schema to `docs/data-dictionary.md` |

### Claude Desktop config (`garmin-mcp`)

Add to your Claude Desktop `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "garmin": {
      "command": "uv",
      "args": ["--directory", "/path/to/garmin-grafana-physiology", "run", "garmin-mcp"],
      "env": {
        "INFLUXDB_HOST": "127.0.0.1",
        "INFLUXDB_PORT": "8086",
        "INFLUXDB_USERNAME": "mcp_reader",
        "INFLUXDB_PASSWORD": "your_reader_password",
        "INFLUXDB_DATABASE": "GarminStats",
        "GARMIN_AGENT_MEMORY_DIR": "/path/to/garmin-grafana-physiology/data/agent-memory",
        "GARMIN_DOCS_DIR": "/path/to/garmin-grafana-physiology/docs"
      }
    }
  }
}
```

### Adding the Grafana MCP server

```json
{
  "mcpServers": {
    "garmin-grafana": {
      "command": "uv",
      "args": ["--directory", "/path/to/garmin-grafana-physiology", "run", "garmin-mcp-grafana"],
      "env": {
        "GRAFANA_URL": "http://localhost:3000",
        "GRAFANA_API_TOKEN": "your_service_account_token"
      }
    }
  }
}
```

Create the Grafana service account token via **Administration → Service accounts** (see `deploy/setup/05_grafana_token.md`).

### HTTP gateway (local)

Start the HTTP MCP server on port 8000:

```bash
MCP_AUTH_TOKEN=$(openssl rand -hex 32) garmin-mcp-http
```

Or with Docker Compose's `mcp-public` profile (local stack):

```bash
docker compose --profile mcp-public up -d mcp-gateway
```

Configure a remote MCP client with `http://your-host:8000/mcp` and `Authorization: Bearer <token>`.

### Cloud MCP server (`deploy/mcp_server/`)

The cloud stack includes a separate GitHub OAuth gateway in `deploy/mcp_server/` (`main.py`, `server.py`, `auth.py`). It exposes the same InfluxQL tools behind GitHub OAuth + a `TOKEN_SECRET` bearer token. Set `GITHUB_CLIENT_ID`, `GITHUB_CLIENT_SECRET`, `GITHUB_ALLOWED_USER`, and `TOKEN_SECRET` in `deploy/.env`.

### Export schema

```bash
uv run garmin-export-schema
# writes docs/data-dictionary.md with live measurement/field/tag listing
```

---

## Grafana dashboards

JSON in `Grafana_Dashboard/`:

| File | Contents |
|---|---|
| `Garmin-Grafana-Dashboard.json` | Main overview: steps, sleep, stress, HRV, SpO₂, body battery, calories |
| `Garmin-Training-Metrics.json` | Training load (ATL/CTL/TSB), activity summaries, HR zones |
| `AI-Coach.json` | AI coach panel layout (requires MCP + LLM integration) |

Dashboards are provisioned automatically from `Grafana_Dashboard/` via `compose.yml`. Heatmap panels require the **`marcusolsson-hourly-heatmap-panel`** plugin, which is pre-installed via the `GF_INSTALL_PLUGINS` env in `compose.yml`.

To manually import a dashboard: Grafana → **Dashboards → Import → Upload JSON**.

---

## Fetcher data coverage

Control which data the Garmin fetcher pulls with the `FETCH_SELECTION` environment variable in `compose.yml` or your `.env`. Leave it blank to fetch everything.

| `FETCH_SELECTION` value | Data pulled |
|---|---|
| `daily` | Daily stats: steps, calories, sleep summary, stress, respiration, SpO₂, HRV status, body battery |
| `activity` | Activity summaries and GPS tracks (`.fit` files) |
| `sleep` | Detailed sleep stages |
| `health` | Body composition, hydration, menstrual data (where available) |
| `intraday` | Heart rate, stress, respiration, SpO₂ at 1–15 min intervals |
| `training` | Training readiness, training status, VO₂ max estimate |

Comma-separate multiple values, e.g. `FETCH_SELECTION=daily,activity,intraday`.

> **Intraday archiving:** Garmin archives intraday data older than approximately 6 months until it is refreshed in the Garmin Connect mobile app. Daily aggregate data remains accessible. See the [upstream discussion](https://github.com/arpanghosh8453/garmin-grafana/issues/77).

### Derived and computed metrics

Beyond raw API fields the fetcher computes and writes additional metrics:

- **Physiology:** LTHR estimate (from activity HR data), HRmax (activity-based or age-formula fallback), VO₂ max fitness age, HR reserve zones.
- **Training load:** Bannister TRIMP, HR-TSS, ATL (acute), CTL (chronic), TSB (form).
- **Activity metrics:** grade-adjusted pace (GAP), critical power / critical swim pace estimates (from best-effort windows), power zone time distributions, vertical speed.
- **Rolling stats:** 7-day median RHR, 42-day percentile HR, smooth HR series for GPS tracks.

These are written to measurements such as `DerivedActivityMetrics`, `TrainingLoad`, `Physiology`, and `UserProfile`.

---

## Configuration reference

### Local stack (`.env` / `compose.yml`)

| Variable | Default | Description |
|---|---|---|
| `GARMINCONNECT_EMAIL` | *(interactive)* | Garmin Connect login email |
| `GARMINCONNECT_BASE64_PASSWORD` | *(interactive)* | Base64-encoded password (skip 2FA path only) |
| `INFLUXDB_HOST` | `influxdb` | InfluxDB hostname (Docker service name in-stack; `127.0.0.1` for host tools) |
| `INFLUXDB_PORT` | `8086` | InfluxDB port |
| `INFLUXDB_USERNAME` | — | InfluxDB write user |
| `INFLUXDB_PASSWORD` | — | InfluxDB write password |
| `INFLUXDB_DATABASE` | `GarminStats` | Database name |
| `UPDATE_INTERVAL_SECONDS` | `3600` | Seconds between sync cycles |
| `FETCH_SELECTION` | *(all)* | Comma-separated data categories to fetch (see above) |
| `MANUAL_START_DATE` | — | Override: backfill from this date (`YYYY-MM-DD`) |
| `MANUAL_END_DATE` | — | Override: backfill to this date (defaults to today) |
| `USER_TIMEZONE` | `UTC` | Local timezone for day-boundary calculations |
| `GARMIN_DEVICENAME` | *(all)* | Filter by device name (substring match) |
| `KEEP_FIT_FILES` | `False` | Persist downloaded `.fit` files to disk |
| `SKIP_EXISTING_DAILY` | `True` | Skip days that already have data in InfluxDB |
| `TAG_MEASUREMENTS_WITH_USER_EMAIL` | `False` | Add `User_Email` tag to all points |
| `LOG_LEVEL` | `INFO` | Python logging level |

### MCP host tools

| Variable | Description |
|---|---|
| `INFLUXDB_HOST` | `127.0.0.1` when running on host against the local stack |
| `INFLUXDB_PORT` | `8086` |
| `INFLUXDB_USERNAME` | Use the read-only `mcp_reader` user |
| `INFLUXDB_PASSWORD` | `mcp_reader` password |
| `INFLUXDB_DATABASE` | `GarminStats` |
| `GARMIN_AGENT_MEMORY_DIR` | Directory for SQLite findings DB (e.g. `./data/agent-memory`) |
| `GARMIN_DOCS_DIR` | Path to repo `docs/` folder |
| `GRAFANA_URL` | e.g. `http://localhost:3000` |
| `GRAFANA_API_TOKEN` | Grafana service account token |
| `MCP_AUTH_TOKEN` | Bearer token for `garmin-mcp-http` |

### Cloud stack (`deploy/.env`)

| Variable | Description |
|---|---|
| `CLOUDFLARE_TUNNEL_TOKEN` | Cloudflare tunnel token |
| `INFLUX_PASSWORD` | InfluxDB admin password |
| `INFLUX_DATABASE` | `GarminStats` |
| `INFLUX_MCP_PASSWORD` | Password for `mcp_reader` user |
| `INFLUX_WRITER_PASSWORD` | Password for `garmin_writer` user |
| `GRAFANA_ADMIN_PASSWORD` | Grafana admin password |
| `GRAFANA_TOKEN` | Service account token for MCP Grafana tools |
| `GARMIN_DEVICENAME` | Filter by device name (leave blank for all) |
| `GITHUB_CLIENT_ID` | GitHub OAuth app client ID |
| `GITHUB_CLIENT_SECRET` | GitHub OAuth app client secret |
| `GITHUB_ALLOWED_USER` | GitHub username allowed to authenticate |
| `TOKEN_SECRET` | Bearer token for MCP gateway (`openssl rand -hex 32`) |
| `WHOOP_CLIENT_ID` | WHOOP developer app client ID |
| `WHOOP_CLIENT_SECRET` | WHOOP developer app client secret |

---

## Bulk historical fetch

### Garmin

After tokens exist, fill a date range (fetched newest-first):

```bash
docker compose run --rm \
  -e MANUAL_START_DATE=2024-01-01 \
  -e MANUAL_END_DATE=2024-12-31 \
  garmin-fetch-data
```

Limit to specific data categories to speed things up:

```bash
docker compose run --rm \
  -e MANUAL_START_DATE=2024-01-01 \
  -e MANUAL_END_DATE=2024-12-31 \
  -e FETCH_SELECTION=activity,sleep \
  garmin-fetch-data
```

### WHOOP

```bash
cd deploy
docker compose --profile whoop run --rm \
  -e MANUAL_START_DATE=2023-01-01 \
  -e MANUAL_END_DATE=2026-01-01 \
  whoop-fetch-data
```

WHOOP fetches in 30-day chunks (approximately 4 API calls per chunk across recovery, sleep, strain, and workouts). A 3-year backfill makes roughly 150 API calls total with a 2-second pause between chunks.

---

## Manual file import

FIT file and Garmin export workflows are documented in **`docs/manual-import-instructions.md`**. Manual import does not replicate intraday API data (heart rate by minute, etc.) — it supplements activity summaries and GPS tracks only.

---

## InfluxDB backup and restore

### Ad-hoc backup

```bash
TIMESTAMP=$(date +%F_%H-%M)
BACKUP_DIR="./influxdb_backups/$TIMESTAMP"
mkdir -p "$BACKUP_DIR"
docker exec influxdb influxd backup -portable -db GarminStats /tmp/influxdb_backup
docker cp influxdb:/tmp/influxdb_backup "$BACKUP_DIR"
docker exec influxdb rm -rf /tmp/influxdb_backup
```

### Automated nightly backup (cloud)

See `deploy/setup/04_backup.sh` for a cron-based example that backs up both InfluxDB and Grafana volumes.

### Restore

```bash
# Stop the fetcher first
docker compose stop garmin-fetch-data
# Restore into a running InfluxDB container
docker cp ./influxdb_backups/2024-01-01_00-00 influxdb:/tmp/restore
docker exec influxdb influxd restore -portable -db GarminStats /tmp/restore
docker compose start garmin-fetch-data
```

Full documentation: [InfluxDB 1.x backup and restore](https://docs.influxdata.com/influxdb/v1/administration/backup_and_restore/).

---

## Day-to-day operations

### Local stack

```bash
docker compose up -d               # start
docker compose stop                # stop (preserves volumes)
docker compose down                # stop and remove containers (preserves volumes)
docker compose logs -f garmin-fetch-data
docker compose build && docker compose up -d   # rebuild after dep changes
```

> Do **not** use `docker compose down -v` unless you intend to delete InfluxDB and Grafana volumes.

### Cloud stack (run from `deploy/`)

```bash
docker compose up -d
docker compose stop
git pull && docker compose down && docker compose up -d --build   # update

# Re-authenticate Garmin
docker compose stop garmin-fetch-data
docker compose run --rm garmin-fetch-data
docker compose up -d garmin-fetch-data

# Re-authenticate WHOOP (after re-running local auth flow and SCP-ing tokens)
docker compose --profile whoop restart whoop-fetch-data
```

### Export live InfluxDB schema

```bash
uv run garmin-export-schema
# Writes docs/data-dictionary.md
```

---

## Development

See **`DEVELOP.md`** for the full developer workflow. Quick start:

```bash
uv sync          # install all deps into .venv
uv run garmin-mcp            # run any console script
uv run garmin-mcp-http       # HTTP variant
```

**Package layout:**

| Path | Contents |
|---|---|
| `src/garmin_grafana/` | Fetcher entrypoint (`garmin_fetch.py`), orchestrator, InfluxDB writer, rollups, physiological computations, FIT importer, activity GPS |
| `src/garmin_integration/` | CLI helpers, Grafana API client, schema exporter, read-only Influx client |
| `src/garmin_mcp/` | All MCP server variants — tools, memory store, HTTP server |
| `src/whoop/` | WHOOP OAuth (`whoop_auth.py`), data fetcher (`whoop_fetch.py`), config |
| `deploy/mcp_server/` | Cloud MCP gateway with GitHub OAuth |
| `Grafana_Dashboard/` | Dashboard JSON files |
| `Grafana_Datasource/` | Grafana datasource provisioning YAML |
| `docs/` | Agent domain context, MCP integration guide, data dictionary, schema reference |
| `k8s/` | Helm chart for Kubernetes deployment |

**Docker build** copies `README.md` into the image so `hatchling` can populate `pyproject.toml`'s `readme` field.

---

## Troubleshooting

### Garmin

| Symptom | Fix |
|---|---|
| **Token expired** (roughly annually) | Run `docker compose run --rm garmin-fetch-data` to reauthenticate, or set `GARMINCONNECT_BASE64_PASSWORD` if not using 2FA |
| **429 on login** | Garmin rate-limits login attempts; wait ~15 min or try from a different network |
| **401 / VPN** | Wrong credentials, base64 encoding mistake in env, or VPN blocking Garmin SSO |
| **Permission errors on `garminconnect-tokens/`** | `sudo chown -R 1000:1000 garminconnect-tokens`, or use the `user: root` / `/root/.garminconnect` path documented in `compose.yml` |
| **Activity dropdown empty in Grafana** | Check dashboard variable datasource and query (`ActivityGPS` / `ActivitySelector`) |
| **Intraday data missing for old dates** | Garmin archives intraday data ~6 months back; open the Garmin Connect mobile app to trigger an unarchive, then re-fetch |

### WHOOP

| Symptom | Fix |
|---|---|
| **401 on `/v2/cycle`** | Token was issued without `read:cycles` scope — re-run auth flow (Step 3–4) and replace token file |
| **401 on all endpoints** | Token is expired and refresh failed — check logs for `Token refresh failed: HTTP 4xx` and re-run auth flow |
| **`refresh_token` absent** in startup logs | Token file is incomplete — re-run auth flow with `offline` scope enabled |
| **429 rate limit errors** | Fetcher retries once after `Retry-After` seconds (+ 5s buffer) or 60s fallback; if persistent, reduce `FETCH_CHUNK_DAYS` in `whoop_fetch.py` |
| **No data after backfill** | Check that `SKIP_EXISTING_DAILY=False` if you want to force a re-fetch, or verify `start_date` is earlier than the first WHOOP sync date |

### MCP

| Symptom | Fix |
|---|---|
| **InfluxDB unreachable from host MCP** | Ensure `compose.yml` publishes `127.0.0.1:8086:8086` and `INFLUXDB_HOST=127.0.0.1` in the MCP env |
| **`garmin-mcp` not found** | Run `uv sync` from repo root; ensure `.venv/bin` is on PATH or use `uv run garmin-mcp` |
| **Empty query results** | Run `uv run garmin-export-schema` to verify measurements exist; check `INFLUXDB_DATABASE` matches |

---

## Acknowledgments and limitations

- **Upstream lineage:** This repo builds on the community Garmin → InfluxDB → Grafana pattern. Fetch logic depends on **[python-garminconnect](https://github.com/cyberjunky/python-garminconnect)** and **[garth](https://github.com/matin/garth)**. Earlier work by [arpanghosh8453/garmin-grafana](https://github.com/arpanghosh8453/garmin-grafana) was influential.
- **Data path:** Sync is Garmin cloud → your stack, not a direct USB/BLE watch bridge. Cloud outages and Garmin API behaviour apply.
- **WHOOP API:** The WHOOP v2 API is used per the [WHOOP Developer documentation](https://developer.whoop.com/api/). Access requires a registered developer app; the API may change without notice.
- **No warranty:** Use at your own risk. Health data is sensitive — keep backups and lock down network exposure.

Contributing: see `.github/CONTRIBUTING.md`.
