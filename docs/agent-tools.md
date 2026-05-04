## Agent tool surface (MCP replacement)

The legacy **FastAPI `ai-agent` container** has been removed from this branch. Tooling should be implemented as **MCP servers** (or small CLIs) that talk to **InfluxDB** and **Grafana** over the Docker internal network.

See **`docs/claude-mcp-integration.md`** for credentials, network layout, and the recommended split (**read-only Influx MCP** vs **Grafana dashboard MCP**).

### Intended MCP capabilities (to implement)

- **Influx (read):** schema introspection (`SHOW MEASUREMENTS`, field/tag keys), guarded `SELECT` with row limits, rejection of write-like InfluxQL.
- **Grafana:** search/get/create dashboards via HTTP API; optional folder targeting.

### CLI retained in this repo

- **`garmin-export-schema`** — exports InfluxDB v1 schema to markdown (uses `INFLUXDB_*` environment variables).

Historical HTTP routes described in older revisions of this file are **not** available unless you restore the removed service from git history.
