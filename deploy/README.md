# Physiology Stack — Deployment Runbook

Full deployment guide for the Garmin Grafana Physiology stack.
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
Hetzner CAX21 (ARM, 3 vCPU, 8GB RAM, ~€5.49/mo)
    ├── cloudflared     → Cloudflare tunnel daemon
    ├── InfluxDB 1.11   → Time series database (internal only)
    ├── Grafana         → grafana.your-domain.com (Cloudflare Access protected)
    └── MCP Server      → mcp.your-domain.com (GitHub OAuth protected)
```

---

## Prerequisites

- Ubuntu 22.04 VM (Hetzner CAX21 recommended)
- Domain on Cloudflare (nameservers pointed at Cloudflare)
- GitHub account (for MCP OAuth)
- Claude Pro subscription (for MCP connector)

---

## Phase 1 — Cloudflare Setup (manual, do first)

### 1.1 Create Tunnel
1. **Zero Trust → Networks → Tunnels → Create a tunnel**
2. Name: `physiology-stack`
3. Connector type: Docker
4. Copy the tunnel token → goes in `.env` as `CLOUDFLARE_TUNNEL_TOKEN`

### 1.2 Add Public Hostnames in tunnel config
| Subdomain | Domain | Service |
|---|---|---|
| `grafana` | your-domain.com | `http://grafana:3000` |
| `mcp` | your-domain.com | `http://mcp-server:8000` |
| `influxdb` | your-domain.com | `http://influxdb:8086` |

### 1.3 Add DNS Records
Go to **your-domain.com → DNS → Records**, add:

| Type | Name | Target | Proxy |
|---|---|---|---|
| CNAME | `grafana` | `<tunnel-id>.cfargotunnel.com` | ✅ Proxied |
| CNAME | `mcp` | `<tunnel-id>.cfargotunnel.com` | ✅ Proxied |
| CNAME | `influxdb` | `<tunnel-id>.cfargotunnel.com` | ✅ Proxied |

The tunnel ID is the `t` field in your tunnel token (base64 decode to find it),
or visible in the tunnel URL on the Cloudflare dashboard.

### 1.4 Cloudflare Access (protects Grafana + InfluxDB)
**Zero Trust → Access → Applications → Add application**
- Type: **Connect a private web application**
- For Grafana: hostname `grafana`, port `3000`
- For InfluxDB: hostname `influxdb`, port `8086`
- Policy: Allow → Email → your email address

⚠️ Do NOT add Access protection to `mcp.your-domain.com` —
Claude.ai needs direct access for OAuth.

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

Copy Client ID and Client Secret → goes in `.env`

---

## Phase 2 — Server Setup

SSH into fresh Ubuntu 22.04 as root:

```bash
git clone https://github.com/big-gunter/garmin-grafana-physiology.git
cd garmin-grafana-physiology
bash deploy/setup/01_server_setup.sh
```

⚠️ This moves SSH to port 22444. Open a second terminal and verify:
```bash
ssh -p 22444 -i your-key root@<IP>
```
before closing the original session.

---

## Phase 3 — Folder Structure

```bash
bash deploy/setup/02_folders.sh
```

This creates `/opt/physiology/` with correct permissions and copies deployment files.

---

## Phase 4 — Configure Environment

```bash
nano /opt/physiology/.env
```

Fill in every value. Key things:
- `CLOUDFLARE_TUNNEL_TOKEN` — from Phase 1.1
- `INFLUX_PASSWORD` — strong password, set once on first init
- `INFLUX_MCP_PASSWORD` — separate password for read-only MCP user
- `GRAFANA_ADMIN_PASSWORD` — Grafana admin login
- `GITHUB_CLIENT_ID` / `GITHUB_CLIENT_SECRET` — from Phase 1.6
- `GITHUB_ALLOWED_USER` — your GitHub username (only this account can connect)
- `TOKEN_SECRET` — run `openssl rand -hex 32` to generate

---

## Phase 5 — Start Stack

```bash
cd /opt/physiology
docker compose up -d
docker compose ps   # wait for all to be Up
```

InfluxDB takes ~30 seconds to initialise on first boot.

---

## Phase 6 — Create Read-Only InfluxDB User

Wait until InfluxDB shows healthy, then:

```bash
bash deploy/setup/03_influxdb_users.sh
```

---

## Phase 7 — Connect Claude.ai

1. **claude.ai → Settings → Connectors → Add custom connector**
2. URL: `https://mcp.your-domain.com`
3. Authenticate via GitHub when prompted
4. Enable in a conversation via the **+** button → Connectors

---

## Phase 8 — Automated Backups

```bash
crontab -e
# Add:
0 2 * * * /opt/physiology/deploy/setup/04_backup.sh >> /var/log/physiology-backup.log 2>&1
```

---

## Updating

```bash
cd /opt/physiology
git pull
docker compose down
docker compose up -d --build
```

---

## Troubleshooting

**MCP connector fails in Claude.ai:**
```bash
docker compose logs mcp-server --tail=50
# Look for OAuth flow completing then 500 errors
```

**InfluxDB permission denied on startup:**
```bash
# InfluxDB runs as uid 1500
chown -R 1500:1500 /opt/physiology/data/influxdb
docker compose restart influxdb
```

**Grafana permission denied on startup:**
```bash
# Grafana runs as uid 472
chown -R 472:472 /opt/physiology/data/grafana
docker compose restart grafana
```

**Tunnel not connecting:**
```bash
docker compose logs cloudflared
# Check CLOUDFLARE_TUNNEL_TOKEN in .env
```

---

## Security Checklist

- [ ] SSH on port 22444, key authentication only
- [ ] UFW blocking all except port 22444
- [ ] Fail2ban protecting SSH (3600s ban, 5 attempts)
- [ ] Kernel network hardening applied (sysctl)
- [ ] Cloudflare Bot Fight Mode enabled
- [ ] Cloudflare Access protecting Grafana and InfluxDB
- [ ] MCP server using read-only InfluxDB user (mcp_reader)
- [ ] GitHub OAuth restricting access to single GitHub account
- [ ] PKCE enforced on OAuth flow
- [ ] `.env` permissions 600, owned by root
- [ ] Automatic security updates enabled
- [ ] Automated daily backups configured
