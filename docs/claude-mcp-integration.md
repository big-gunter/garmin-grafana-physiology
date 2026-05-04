# Claude + MCP integration (Garmin / Influx / Grafana)

This branch removes the in-stack **AI agent container** in favour of connecting **Claude** (typically **Claude Desktop** with MCP) to your own tooling. Keep **`docs/agent-domain.md`** as the sports-science reference you attach or expose to the model.

---

## Realistic expectations: laptop, phone, and “where the data lives”

| Scenario | What works |
|----------|------------|
| **Claude Desktop on the same machine as Docker** | Run MCP **on the host** (stdio). Use `INFLUXDB_HOST=127.0.0.1` and **publish** Influx to the host (see `compose.yml` commented `ports`) or use a dev container network alias; Grafana is already on `127.0.0.1:3000`. |
| **Claude mobile app (`claude.ai` / phone)** | Does **not** attach to a private MCP server on your laptop. There is **no path** from the phone app alone to `localhost` or your Docker bridge network. |
| **Phone + laptop both online** | To query DB from “outside”, you must expose a **controlled endpoint** to your phone/VPN: e.g. **Tailscale**, **WireGuard**, or an **HTTPS reverse proxy** + auth to an MCP or API gateway running on the laptop or a small home server. |

**Recommended mental model:** data stays on the laptop (or NAS); **remote access = VPN or tunnel**, never raw public Influx ports.

---

## API keys and credentials (what you need, where to get them)

### 1. Grafana HTTP API token (dashboard create/update)

- **Where:** Grafana UI → **Administration → Service accounts** (or **Configuration → API keys** on older layouts) → create token with **Editor** (or **Admin** only if you must; prefer least privilege).
- **Scopes:** Dashboard permissions sufficient to **create/update dashboards** and optionally **list folders**.
- **Used for:** MCP tools that push JSON dashboards (`POST /api/dashboards/db`), search dashboards, etc.
- **Store in:** `.env` as `GRAFANA_API_TOKEN` (never commit).

### 2. InfluxDB credentials (read)

- **Where:** You already define them for Compose (`INFLUXDB_USER` / `INFLUXDB_USER_PASSWORD` for the container). For **least privilege**, create a **dedicated InfluxDB user** with **READ** on database `GarminStats` only (InfluxDB 1.x: GRANT READ ON `<db>` TO `<user>`).
- **Used for:** InfluxQL `SELECT` / schema introspection only.
- **Never:** Grant the MCP user **WRITE** unless you explicitly want the model to mutate data.

### 3. Anthropic / Claude

- **Claude Desktop + MCP:** Usually **no separate “MCP API key”** from Anthropic—MCP is a **local protocol** between Claude Desktop and your server.
- **Optional:** If you build a **hosted** bridge that calls the **Anthropic Messages API**, you need an **`ANTHROPIC_API_KEY`** from Anthropic Console (that’s a different architecture than pure local MCP).

### 4. Optional: MCP server HTTP auth

If you expose MCP over HTTP/SSE (for VPN-only access), use a **random bearer token** (`MCP_AUTH_TOKEN`) generated locally (`openssl rand -hex 32`), not a vendor “API key”.

---

## Two MCP processes (recommended isolation)

To minimise blast radius, split responsibilities:

| MCP server | Credentials | Capabilities |
|------------|-------------|--------------|
| **`garmin-mcp-influx`** (package) | Read-only Influx user | `influxql_query`, `list_measurements`, `describe_measurement`, resource `docs://agent-domain` |
| **`garmin-mcp-grafana`** (package) | Grafana API token | Search/get dashboard, push JSON file, list `Grafana_Dashboard/*.json` |

**Host vs container:** For **Claude Desktop on your laptop**, run the MCP commands on the host and point Influx at **`127.0.0.1:8086`** (uncomment the `ports` block in `compose.yml` for Influx). For an **all-container** design, you would use different transports (e.g. streamable HTTP) and different auth (out of scope for the default stdio flow).

Claude Desktop can register **multiple MCP servers** in its config file.

---

## Docker and network practices (this repo)

- **Named bridge network** `garmin-grafana-internal`: all stack services attach explicitly for stable DNS (`influxdb`, `grafana`).
- **InfluxDB** is **`expose` only** (not `ports`)—reachable from other containers on the network; **not published to the host** by default (reduces accidental exposure). Publish **only** if you need host-side tools without entering the network.
- **Grafana** remains on **`3000:3000`** for local UI; tighten with auth and firewall when exposing beyond localhost.
- **Volumes:** `influxdb_data` and `grafana_data` are **Docker named volumes**—persistent until you run `docker compose down **-v**`. **Never** use `-v` for routine restarts.

---

## Domain reference for Claude

Attach or mount into the MCP container:

- **`docs/agent-domain.md`** — definitions, methodology, constraints for physiology/training narrative.
- **`docs/data-dictionary.md`** — fill over time; regenerate schema hints with `garmin-export-schema` (see README).

---

## Claude Desktop configuration (stdio)

The repo ships two console scripts: **`garmin-mcp-influx`** and **`garmin-mcp-grafana`** (after `uv sync` in the project).

Example fragment for **macOS** Claude Desktop (`~/Library/Application Support/Claude/claude_desktop_config.json`) — adjust absolute paths and secrets:

```json
{
  "mcpServers": {
    "garmin-influx": {
      "command": "uv",
      "args": ["run", "--directory", "/ABSOLUTE/PATH/TO/garmin-grafana-physiology", "garmin-mcp-influx"],
      "env": {
        "INFLUXDB_HOST": "127.0.0.1",
        "INFLUXDB_PORT": "8086",
        "INFLUXDB_USERNAME": "influxdb_user",
        "INFLUXDB_PASSWORD": "YOUR_PASSWORD",
        "INFLUXDB_DATABASE": "GarminStats",
        "INFLUXDB_VERSION": "1"
      }
    },
    "garmin-grafana": {
      "command": "uv",
      "args": ["run", "--directory", "/ABSOLUTE/PATH/TO/garmin-grafana-physiology", "garmin-mcp-grafana"],
      "env": {
        "GRAFANA_URL": "http://127.0.0.1:3000",
        "GRAFANA_API_TOKEN": "YOUR_GRAFANA_TOKEN"
      }
    }
  }
}
```

Restart Claude Desktop after editing. Use a **read-only** Influx user for the first server if you create one in Influx.

### Optional: `GARMIN_DOCS_DIR`

If `docs/agent-domain.md` is not found, set `GARMIN_DOCS_DIR` to the folder containing that file (e.g. your repo `docs/` path).

---

## Summary

- **Keys you generate:** Grafana API token; dedicated read-only Influx user/password; optional MCP bearer token for HTTP transport.
- **Phone alone cannot drive local MCP**; use **VPN/tunnel + Claude Desktop on laptop**, or a deliberate **remote gateway** with strong auth.
