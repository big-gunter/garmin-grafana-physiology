# WHOOP OAuth 2.0 Setup

The WHOOP service authenticates via OAuth 2.0. Tokens are obtained once
interactively and then refreshed automatically (access tokens expire after
1 hour; the refresh token is long-lived).

---

## Step 1 — Register a WHOOP developer app

1. Go to [developer-dashboard.whoop.com](https://developer-dashboard.whoop.com)
   and sign in with your WHOOP account.

2. Create a new application:
   - **Name:** anything (e.g. `physiology-ingest`)
   - **Redirect URI:** `http://localhost:8080/callback`
   - **Scopes:** select all of:
     - `read:recovery`
     - `read:sleep`
     - `read:profile`
     - `read:workout`
     - `read:body_measurement`
     - `offline` (required for refresh tokens)

3. Copy the **Client ID** and **Client Secret** into `deploy/.env`:
   ```
   WHOOP_CLIENT_ID=your_client_id
   WHOOP_CLIENT_SECRET=your_client_secret
   ```

---

## Step 2 — Create the token directory on the server

```bash
mkdir -p /opt/physiology/whoop-tokens
chown -R 1000:1000 /opt/physiology/whoop-tokens
```

---

## Step 3 — Run the initial auth flow

The initial flow requires a browser on your **local machine** (not the server).
Run this from your laptop or desktop with the repo checked out and
`WHOOP_CLIENT_ID` / `WHOOP_CLIENT_SECRET` set in your environment or a local
`.env`:

```bash
cd /path/to/garmin-grafana-physiology
WHOOP_CLIENT_ID=your_client_id \
WHOOP_CLIENT_SECRET=your_client_secret \
WHOOP_TOKEN_DIR=./whoop-tokens-local \
python -m whoop.whoop_auth
```

This opens a browser tab. After you approve the WHOOP permissions, the page
redirects to `http://localhost:8080/callback` and the script prints
"Authorization successful — tokens saved."

Tokens are written to `./whoop-tokens-local/whoop_tokens.json`.

---

## Step 4 — Copy tokens to the server

```bash
scp ./whoop-tokens-local/whoop_tokens.json \
    root@your-hetzner-ip:/opt/physiology/whoop-tokens/whoop_tokens.json
```

---

## Step 5 — Start the WHOOP service

```bash
cd /opt/physiology-repo/deploy
docker compose --profile whoop build whoop-fetch-data
docker compose --profile whoop up -d whoop-fetch-data
docker compose logs -f whoop-fetch-data
```

The service will start from the last synced date (or 90 days ago on first
run) and keep the database up to date every `UPDATE_INTERVAL_SECONDS`.

---

## Backfill command

To ingest historical data (e.g. since account creation):

```bash
cd /opt/physiology-repo/deploy
docker compose --profile whoop run --rm \
  -e MANUAL_START_DATE=2020-01-01 \
  -e MANUAL_END_DATE=2026-05-13 \
  whoop-fetch-data
```

`SKIP_EXISTING_DAILY=True` (the default) means already-ingested days are
skipped, so it is safe to re-run the backfill command without duplicating data.

---

## Notes

- The rest of the stack (`garmin-fetch-data`, `grafana`, `influxdb`,
  `cloudflared`, `mcp-server`) runs **unchanged** without `--profile whoop`.
- WHOOP data is written to the same `GarminStats` InfluxDB database using
  distinct measurement names (`WhoopRecovery`, `WhoopSleep`, `WhoopStrain`,
  `WhoopWorkout`). The existing `mcp_reader` user already has READ access.
- Tokens are auto-refreshed every time they are about to expire (within 60
  seconds of the 1-hour TTL). No manual renewal is needed.
