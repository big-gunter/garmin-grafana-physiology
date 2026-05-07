# Garmin Grafana Physiology

Self-host a **Garmin Connect → InfluxDB → Grafana** pipeline on your own machine, with **Model Context Protocol (MCP)** servers so tools like **Claude Desktop** can query your metrics read-only, keep a small **SQLite “agent memory”** for prior conclusions, and optionally drive **Grafana’s HTTP API**. Data stays local unless you choose to expose an HTTPS MCP gateway.

> **Trademarks:** Garmin and Grafana are trademarks of their respective owners. This project is independent and not affiliated with Garmin Ltd. or Grafana Labs.

## Contents

- [What you get](#what-you-get)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Deploy the stack](#deploy-the-stack)
- [Claude and MCP](#claude-and-mcp)
- [Credentials](#credentials)
- [Grafana dashboards](#grafana-dashboards)
- [Day-to-day operations](#day-to-day-operations)
- [Configuration notes](#configuration-notes)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Acknowledgments and limitations](#acknowledgments-and-limitations)

## What you get

- **Continuous sync** from Garmin Connect into **InfluxDB** (time-series), with periodic runs inside Docker.
- **Grafana** dashboards for long-horizon views: activity, sleep, stress, HRV, steps heatmaps, workout GPS tracks, and related fields (exact coverage depends on device and `FETCH_SELECTION` in `compose.yml`).
- **MCP servers** (Python 3.13, `uv`):
  - **`garmin-mcp`** — read-only InfluxQL + SQLite memory + resource `docs://agent-domain` (sports-science context).
  - **`garmin-mcp-influx`** — Influx + domain doc only (no memory DB).
  - **`garmin-mcp-grafana`** — Grafana API helpers (dashboards).
  - **`garmin-mcp-http`** — same tools as `garmin-mcp` over **streamable HTTP** (for remote clients behind TLS + `MCP_AUTH_TOKEN`).
  - **`garmin-export-schema`** — export schema hints for `docs/data-dictionary.md`.
- **Documentation** under `docs/`, including **`docs/agent-domain.md`**, **`docs/claude-mcp-integration.md`**, and **`docs/manual-import-instructions.md`**.

There is **no in-stack AI container**; you attach your own client (e.g. Claude Desktop) via MCP.

## Architecture

| Piece | Role |
|--------|------|
| **`garmin-fetch-data`** | Pulls from Garmin APIs, writes to Influx. Session tokens live in **`garminconnect-tokens/`** (bind mount). |
| **`influxdb`** | InfluxDB **1.x** by default (`GarminStats`). Published on **`127.0.0.1:8086`** in `compose.yml` so **host** MCP/CLI can connect; comment out `ports` if you want the DB only on the Docker network. |
| **`grafana`** | UI on **`http://localhost:3000`**, provisions dashboards from `Grafana_Dashboard/`. |
| **`mcp-gateway`** (optional) | Compose **profile `mcp-public`**: runs **`garmin-mcp-http`** on the internal network; front with HTTPS on the host. |

Network: bridge **`garmin-grafana-internal`**; services talk as **`influxdb`**, **`grafana`**, etc.

## Requirements

- **Docker** and **Docker Compose**
- **Git**
- For MCP on the host: **[uv](https://docs.astral.sh/uv/)** and **`uv sync`** from this repo

## Deploy the stack

1. **Clone** and create the token directory (owned by UID **1000**, the fetch container user):

   ```bash
   git clone <repository-url> garmin-grafana-physiology
   cd garmin-grafana-physiology
   mkdir -p garminconnect-tokens
   sudo chown -R 1000:1000 garminconnect-tokens
   ```

2. **Secrets template** (host tools + optional Compose substitution):

   ```bash
   cp .env.example .env
   ```

   Keep **`INFLUXDB_*`** aligned with **`compose.yml`**. Do not commit **`.env`**.

3. **Build and start:**

   ```bash
   docker compose build
   docker compose up -d
   ```

4. **First Garmin login** (if you did not set `GARMINCONNECT_EMAIL` / `GARMINCONNECT_BASE64_PASSWORD` in `compose.yml`):

   ```bash
   docker compose run --rm garmin-fetch-data
   ```

   Complete email, password, and 2FA once; tokens persist under **`garminconnect-tokens/`**.

5. **Verify:** open **Grafana** at **`http://localhost:3000`** (default **`admin` / `admin`** — change it). Follow logs with `docker compose logs -f garmin-fetch-data`.

The first regular sync typically backfills on the order of **the last week**; use [bulk fetch](#bulk-historical-fetch) for older ranges.

## Claude and MCP

Full checklist, Claude Desktop JSON, HTTPS gateway, and security notes: **`docs/claude-mcp-integration.md`**.

Minimal path:

1. **`uv sync`** in the repo root.
2. Ensure Influx is reachable from the host (**`127.0.0.1:8086`** per `compose.yml`, or adjust).
3. Register **`garmin-mcp`** in Claude Desktop (example `env`: `INFLUXDB_HOST=127.0.0.1`, matching user/password, `GARMIN_AGENT_MEMORY_DIR`, **`GARMIN_DOCS_DIR`** pointing at this repo’s **`docs/`** folder).
4. Restart Claude Desktop.

Optional second server **`garmin-mcp-grafana`** needs **`GRAFANA_URL`** and **`GRAFANA_API_TOKEN`**.

## Credentials

| What | Where it lives |
|------|----------------|
| **Garmin session** | **`garminconnect-tokens/`** after successful login; optional **`GARMINCONNECT_*`** vars in `compose.yml` (password base64 if using env). |
| **Influx (fetch + Grafana datasource)** | **`compose.yml`**: `INFLUXDB_USERNAME` / `INFLUXDB_PASSWORD` on **`garmin-fetch-data`**; **`INFLUXDB_USER`** / **`INFLUXDB_USER_PASSWORD`** on **`influxdb`**. Use the same values for host tools. Prefer a **read-only** Influx user for MCP only. |
| **Grafana API** | Create a **service account** (or API key) in Grafana → **`.env`**: **`GRAFANA_API_TOKEN`**, **`GRAFANA_URL`**. |
| **Remote MCP** | Generate **`MCP_AUTH_TOKEN`** (e.g. `openssl rand -hex 32`); optional **`MCP_PUBLIC_HOSTS`**. |
| **Anthropic** | Not required for **local** Claude Desktop MCP; only if you call Anthropic’s HTTP API from your own code. |

Never expose raw Influx or Grafana admin ports to the public internet. Remote MCP should be **HTTPS + bearer token** only.

## Grafana dashboards

JSON in **`Grafana_Dashboard/`** (e.g. `Garmin-Grafana-Dashboard.json`, `Garmin-Training-Metrics.json`, optional `AI-Coach.json`). Heatmap panels expect the **`marcusolsson-hourly-heatmap-panel`** plugin (preinstall is set in `compose.yml` for Grafana).

## Day-to-day operations

**Lifecycle**

- Start: `docker compose up -d`
- Stop: `docker compose stop` or `docker compose down`  
- **Do not** use `docker compose down -v` unless you intend to **delete** named volumes (Influx/Grafana data).

**Updates**

- Rebuild after changing Python deps: `docker compose build && docker compose up -d`

### Bulk historical fetch

After tokens exist, fill a date range (newest days first inside the job):

```bash
docker compose run --rm \
  -e MANUAL_START_DATE=2024-01-01 \
  -e MANUAL_END_DATE=2024-06-01 \
  garmin-fetch-data
```

Optional: `-e FETCH_SELECTION=activity,sleep` to limit measurements (see comments in `compose.yml`). Garmin may **archive intraday** data older than ~**six months** until refreshed in the mobile app; daily aggregates can still be available. See upstream [discussion on cold storage](https://github.com/arpanghosh8453/garmin-grafana/issues/77).

### Manual file import

FIT / export workflows: **`docs/manual-import-instructions.md`** (does not replace full intraday API backfill).

### Influx backup (1.x)

```bash
TIMESTAMP=$(date +%F_%H-%M)
BACKUP_DIR="./influxdb_backups/$TIMESTAMP"
mkdir -p "$BACKUP_DIR"
docker exec influxdb influxd backup -portable -db GarminStats /tmp/influxdb_backup
docker cp influxdb:/tmp/influxdb_backup "$BACKUP_DIR"
docker exec influxdb rm -rf /tmp/influxdb_backup
```

Restore and portability: [InfluxDB 1.x backup/restore](https://docs.influxdata.com/influxdb/v1/administration/backup_and_restore/).

## Configuration notes

- **Fetch tuning:** `UPDATE_INTERVAL_SECONDS`, `FETCH_SELECTION`, `USER_TIMEZONE`, `KEEP_FIT_FILES`, etc. — see **`compose.yml`** comments. Extra measurement sets: [Discussion #119](https://github.com/arpanghosh8453/garmin-grafana/discussions/119).
- **Permissions:** Fetch runs as **`appuser` (UID 1000)**. If bind mounts fail, fix ownership or follow the `user: root` + `/root/.garminconnect` path documented in `compose.yml`.
- **Influx versions:** Default image is **InfluxDB 1.11** with **InfluxQL**. This ecosystem targets 1.x for long-range Grafana use; **InfluxDB 3** OSS has query-window limits that work poorly for multi-year dashboards—see `compose.yml` if you experiment with v3.

## Development

- **`DEVELOP.md`** — `uv lock`, `uv sync`, console scripts.
- **Layout:** `src/garmin_grafana` (fetch), `src/garmin_integration`, `src/garmin_mcp`.
- **Docker build** copies **`README.md`** into the image so `hatchling` can read `readme` from `pyproject.toml`.

## Troubleshooting

- **Token expired (~year):** run interactive login again or use base64 password env vars if you do not use 2FA.
- **429 on login:** Garmin rate-limits; wait or try another network/public IP.
- **401 / VPN:** wrong credentials, base64 mistake in env, or VPN blocking Garmin SSO.
- **Permission errors:** `chown -R 1000:1000 garminconnect-tokens` or the root-user compose path above.
- **Activity dropdown empty in Grafana:** check dashboard variable datasource and query (`ActivityGPS` / `ActivitySelector`) as in upstream [issues](https://github.com/arpanghosh8453/garmin-grafana/issues).

For broader Garmin-Grafana how-tos, the [upstream project](https://github.com/arpanghosh8453/garmin-grafana) and its issues/discussions remain useful.

## Acknowledgments and limitations

- **Upstream lineage:** This repo builds on the community **Garmin → Influx → Grafana** pattern popularized by projects such as [garmin-grafana](https://github.com/arpanghosh8453/garmin-grafana). Fetch logic depends on **[python-garminconnect](https://github.com/cyberjunky/python-garminconnect)** and **[garth](https://github.com/matin/garth)**.
- **Data path:** Sync is **Garmin cloud → your stack**, not a direct USB/BLE watch bridge. Cloud outages and Garmin API behavior apply.
- **No warranty:** Use at your own risk; health data is sensitive—keep backups and lock down network exposure.

Contributing: see **`.github/CONTRIBUTING.md`** if present.
