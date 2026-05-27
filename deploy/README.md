# Physiology Stack — Cloud Deployment Runbook

Full deployment guide for the Garmin Grafana Physiology stack on a cloud server.
Tested on Hetzner CAX21 ARM (Ubuntu 22.04) with Cloudflare tunnel.

---

## Architecture

```
Internet
    │
    ▼
Cloudflare (DNS + Tunnel + Access)
    │
    │  Encrypted tunnel (zero open ports on server)
    ▼
Server (e.g. Hetzner CAX21, Ubuntu 22.04)
    ├── cloudflared       → Cloudflare tunnel daemon
    ├── influxdb          → Time-series database (internal only)
    ├── grafana           → grafana.your-domain.com (Cloudflare Access protected)
    ├── garmin-fetch-data → Garmin Connect ingest, writes to InfluxDB
    └── mcp-server        → mcp.your-domain.com (GitHub OAuth protected)
```

All services share a single internal Docker network (`physiology-net`).
No ports are exposed to the public internet — Cloudflare tunnel handles all external traffic.

---

## Prerequisites

- Ubuntu 22.04 VM (Hetzner CAX21 ARM recommended — ~€5.49/mo)
- Domain on Cloudflare (nameservers pointed at Cloudflare)
- GitHub account (for MCP OAuth)
- Claude Pro subscription (for MCP connector)

---

## Phase 1 — Cloudflare Setup (manual, do first)

### 1.1 Create Tunnel

1. **Zero Trust → Networks → Tunnels → Create a tunnel**
2. Name: `physiology-stack`
3. Connector type: Docker
4. Copy the tunnel token → goes in `deploy/.env` as `CLOUDFLARE_TUNNEL_TOKEN`

### 1.2 Add Public Hostnames

In your tunnel config:

| Subdomain | Domain | Service |
|---|---|---|
| `grafana` | your-domain.com | `http://grafana:3000` |
| `mcp` | your-domain.com | `http://mcp-server:8000` |

### 1.3 Add DNS Records

**your-domain.com → DNS → Records:**

| Type | Name | Target | Proxy |
|---|---|---|---|
| CNAME | `grafana` | `<tunnel-id>.cfargotunnel.com` | Proxied |
| CNAME | `mcp` | `<tunnel-id>.cfargotunnel.com` | Proxied |

### 1.4 Cloudflare Access (protects Grafana)

**Zero Trust → Access → Applications → Add application**
- Type: Connect a private web application
- Hostname: `grafana.your-domain.com`
- Policy: Allow → Email → your email address

Do NOT add Access protection to `mcp.your-domain.com` — Claude.ai needs direct access for OAuth.

### 1.5 Security Settings

- **Security → Bots → Bot Fight Mode → On**
- **Security → Settings → Security Level → High**

### 1.6 GitHub OAuth App

**github.com → Settings → Developer Settings → OAuth Apps → New OAuth App**

| Field | Value |
|---|---|
| Application name | `Garmin Physiology MCP` |
| Homepage URL | `https://mcp.your-domain.com` |
| Authorization callback URL | `https://mcp.your-domain.com/oauth/callback` |

Copy Client ID and Client Secret → go in `deploy/.env`.

---

## Phase 2 — Server Setup

SSH into a fresh Ubuntu 22.04 server as root, then clone the repo to `/opt/physiology-repo` and run the server hardening script:

```bash
git clone https://github.com/big-gunter/garmin-grafana-physiology.git /opt/physiology-repo
cd /opt/physiology-repo
bash deploy/setup/01_server_setup.sh
```

> **Warning:** This moves SSH to port **22444**. Before closing your current session, open a second terminal and verify:
> ```bash
> ssh -p 22444 root@<server-ip>
> ```

---

## Phase 3 — Create Data Directories

```bash
cd /opt/physiology-repo
bash deploy/setup/02_folders.sh
```

This creates `/opt/physiology/data/{influxdb,grafana,mcp-server,mcp-server-gpt}`, `/opt/physiology/garminconnect-tokens/`, and `/opt/physiology/backups/` with correct ownership, patches the Grafana dashboard JSON, and creates `deploy/.env` from the template.

> **MCP token persistence** — `mcp-server` and `mcp-server-gpt` are mounted as host volumes so OAuth state (registered clients, issued tokens) survives container restarts. The script creates these directories with `chmod 700` so only the container's `appuser` (uid 1000) can read them.

---

## Phase 4 — Configure Environment

```bash
nano /opt/physiology-repo/deploy/.env
```

Fill in every value:

| Variable | Description |
|---|---|
| `CLOUDFLARE_TUNNEL_TOKEN` | From Phase 1.1 |
| `INFLUX_PASSWORD` | Strong password for InfluxDB admin — set once, do not change |
| `INFLUX_MCP_PASSWORD` | Password for read-only `mcp_reader` user (Grafana datasource + MCP server) |
| `INFLUX_WRITER_PASSWORD` | Password for `garmin_writer` user (Garmin ingest container) |
| `INFLUX_DATABASE` | Leave as `GarminStats` unless you have a reason to change it |
| `GRAFANA_ADMIN_PASSWORD` | Grafana admin login |
| `GARMIN_DEVICENAME` | Your Garmin device name — leave blank to match all devices |
| `GITHUB_CLIENT_ID` | From Phase 1.6 |
| `GITHUB_CLIENT_SECRET` | From Phase 1.6 |
| `GITHUB_ALLOWED_USER` | Your GitHub username — only this account can connect to MCP |
| `TOKEN_SECRET` | Run `openssl rand -hex 32` to generate |

---

## Phase 5 — Build the Image

```bash
cd /opt/physiology-repo/deploy
docker compose build garmin-fetch-data
```

---

## Phase 6 — Start InfluxDB and Create Users

Start InfluxDB on its own first, then create the application users:

```bash
cd /opt/physiology-repo/deploy
docker compose up -d influxdb
docker compose ps   # wait until influxdb shows (healthy)
```

Once healthy:

```bash
bash /opt/physiology-repo/deploy/setup/03_influxdb_users.sh
```

This creates `mcp_reader` (read-only, used by Grafana and the MCP server) and `garmin_writer` (write access, used by the ingest container).

---

## Phase 7 — Authenticate with Garmin Connect

Run the ingest container interactively once to complete the Garmin OAuth/MFA flow:

```bash
cd /opt/physiology-repo/deploy
docker compose run --rm garmin-fetch-data
```

You will be prompted for your Garmin Connect email, password, and 2FA code. Tokens are saved to `/opt/physiology/garminconnect-tokens/` and persist across container restarts. This step only needs to be repeated if tokens expire (~1 year).

---

## Phase 8 — Start the Full Stack

```bash
cd /opt/physiology-repo/deploy
docker compose up -d
docker compose ps
```

All five services should reach `Up` or `Up (healthy)`:

| Service | Expected state |
|---|---|
| cloudflared | Up |
| influxdb | Up (healthy) |
| grafana | Up (healthy) |
| garmin-fetch-data | Up |
| mcp-server | Up (healthy) |

Follow the ingest logs to confirm Garmin data is flowing:

```bash
docker compose logs -f garmin-fetch-data
```

---

## Phase 9 — Connect Claude.ai

1. **claude.ai → Settings → Connectors → Add custom connector**
2. URL: `https://mcp.your-domain.com`
3. Authenticate via GitHub when prompted
4. Enable in a conversation via the **+** button → Connectors

---

## Phase 10 — Automated Backups

```bash
crontab -e
# Add:
0 2 * * * bash /opt/physiology-repo/deploy/setup/04_backup.sh >> /var/log/physiology-backup.log 2>&1
```

Backs up InfluxDB (portable format) and Grafana data daily at 02:00, retaining 7 days.

---

## Updating

```bash
cd /opt/physiology-repo
git pull
cd deploy
docker compose down
docker compose up -d --build
```

---

## Troubleshooting

**Garmin tokens expired:**
```bash
cd /opt/physiology-repo/deploy
docker compose stop garmin-fetch-data
docker compose run --rm garmin-fetch-data   # re-authenticate interactively
docker compose up -d garmin-fetch-data
```

**InfluxDB permission denied on startup:**
```bash
chown -R 1500:1500 /opt/physiology/data/influxdb
docker compose restart influxdb
```

**Grafana permission denied on startup:**
```bash
chown -R 472:472 /opt/physiology/data/grafana
docker compose restart grafana
```

**MCP connector fails in Claude.ai:**
```bash
docker compose logs mcp-server --tail=50
```

**Tunnel not connecting:**
```bash
docker compose logs cloudflared
# Check CLOUDFLARE_TUNNEL_TOKEN in deploy/.env
```

**Garmin ingest not writing data:**
```bash
docker compose logs garmin-fetch-data --tail=50
# Common causes: wrong INFLUX_WRITER_PASSWORD, influxdb not healthy yet,
# or garmin_writer user not created (run 03_influxdb_users.sh)
```

---

## Security Checklist

- [ ] SSH on port 22444, key authentication only
- [ ] UFW blocking all ports except 22444
- [ ] Fail2ban protecting SSH (3600s ban, 5 attempts)
- [ ] Kernel network hardening applied (sysctl)
- [ ] Cloudflare Bot Fight Mode enabled
- [ ] Cloudflare Access protecting Grafana
- [ ] MCP server using read-only InfluxDB user (`mcp_reader`)
- [ ] Grafana datasource using read-only InfluxDB user (`mcp_reader`)
- [ ] GitHub OAuth restricting access to single GitHub account
- [ ] `deploy/.env` permissions 600, owned by root
- [ ] Automatic security updates enabled
- [ ] Automated daily backups configured
