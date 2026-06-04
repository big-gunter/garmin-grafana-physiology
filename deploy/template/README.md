# Deploy Template System

Each user stack is a Docker Compose project generated from a single template.
The generated `docker-compose.<username>.yml` files are the deployed artifacts —
nothing about the production workflow changes, they are just now generated rather
than hand-edited.

## Directory layout

```
deploy/
  template/
    docker-compose.template.yml   ← single source of truth
    .env.template                 ← copy → deploy/.env per user
    .wireguard.env.template       ← copy → deploy/.wireguard.<username>.env
    generate-wg0.sh.template      ← installed to /opt/<username>/wireguard/ by stack.sh generate
    README.md                     ← this file
  users/
    physiology.conf               ← per-user feature flags
    connor.conf
  stack.sh                        ← generate / up / down entrypoint
  docker-compose.physiology.yml   ← generated artifact (gitignored or committed)
  docker-compose.connor.yml       ← generated artifact
  .env                            ← secrets, never committed
  .wireguard.<username>.env       ← wireguard secrets, never committed
```

## Onboarding a new user

1. **Add a user conf** — copy an existing conf and edit:
   ```sh
   cp deploy/users/physiology.conf deploy/users/alice.conf
   # edit USERNAME, ENABLE_* flags
   ```

2. **Create the env file** — copy the template, fill in secrets:
   ```sh
   cp deploy/template/.env.template deploy/.env          # if first user on this server
   chmod 600 deploy/.env
   ```

3. **Generate the compose file**:
   ```sh
   ./deploy/stack.sh alice generate
   ```
   This writes `deploy/docker-compose.alice.yml`.

4. **Start the stack**:
   ```sh
   ./deploy/stack.sh alice up
   ```

5. **If WireGuard is needed**, set `ENABLE_WIREGUARD=true` in the conf, then:
   ```sh
   cp deploy/template/.wireguard.env.template deploy/.wireguard.alice.env
   chmod 600 deploy/.wireguard.alice.env
   # fill in WG_* values
   ./deploy/stack.sh alice generate   # installs generate-wg0.sh
   bash /opt/alice/wireguard/generate-wg0.sh
   ./deploy/stack.sh alice up
   ```

## Day-to-day usage

```sh
# Bring up stack with profiles from conf
./deploy/stack.sh <username> up

# Temporarily add a profile without changing the conf
./deploy/stack.sh <username> up --with wireguard

# Bring up everything (all optional profiles)
./deploy/stack.sh <username> up --all

# Stop entire stack
./deploy/stack.sh <username> down

# Stop a single service
./deploy/stack.sh <username> down wireguard

# Re-generate after editing the template or conf
./deploy/stack.sh <username> generate
```

## Template substitutions

| Placeholder | Replaced with |
|---|---|
| `{{USERNAME}}` | user's username, e.g. `connor` |
| `BEGIN_GARMIN_NETWORK` … `END_GARMIN_NETWORK` | `networks:` block or `network_mode:` — see wireguard section |

## WireGuard — temporary token grab, not persistent routing

WireGuard is used for one thing only: grabbing the initial Garmin OAuth tokens when the
server's IP is blocked. Once tokens are written to the volume, WireGuard is no longer
needed and can be stopped without disrupting anything else in the stack.

`garmin-fetch-data` **always** uses the standard `{{USERNAME}}-network` — it is never
configured with `network_mode`. The wireguard container sits on the same network as a
peer, not as a gateway that other containers depend on.

The token grab works by running a temporary, ephemeral container (`garmin-token-grab`)
that borrows wireguard's network namespace for the auth flow only:

```
wireguard (running) ──network_mode──► garmin-token-grab (ephemeral, exits after auth)
                                            │
                                      writes tokens to shared volume
                                            │
                                      garmin-fetch-data picks them up (direct connection)
```

**Full workflow:**

```sh
# 1. Set up wireguard config (first time only)
cp deploy/template/.wireguard.env.template deploy/.wireguard.<user>.env
chmod 600 deploy/.wireguard.<user>.env
# fill in WG_* values

./deploy/stack.sh <user> generate          # installs generate-wg0.sh if ENABLE_WIREGUARD=true
bash /opt/<user>/wireguard/generate-wg0.sh # writes wg0.conf from env vars

# 2. Start the wireguard tunnel
./deploy/stack.sh <user> up --with wireguard

# 3. Run the token grab through the tunnel
./deploy/stack.sh <user> token-grab
# → ephemeral container authenticates through wireguard, writes tokens, exits

# 4. Tear down wireguard — garmin-fetch-data is completely unaffected
./deploy/stack.sh <user> down wireguard
```

Note: `ENABLE_WIREGUARD` in the user conf controls whether wireguard starts automatically
with `./stack.sh up`. Set it to `false` (the default) so wireguard only runs when
explicitly requested with `--with wireguard`.

## Differences from the existing hand-edited files

The generated files differ from the existing `docker-compose.yml` (physiology) and
`docker-compose.connor.yml` (connor) in the following ways — **review before switching
production stacks to the generated versions**.

### physiology

| Setting | Existing (`docker-compose.yml`) | Generated (`docker-compose.physiology.yml`) |
|---|---|---|
| Compose project name | `physiology` | `physiology` ✓ |
| Network name | `physiology-net` | `physiology-network` ⚠ |
| Grafana URL | `https://grafana.big-gunter.com` | `https://physiology-grafana.big-gunter.com` ⚠ |
| MCP URL | `https://mcp.big-gunter.com` | `https://physiology-mcp.big-gunter.com` ⚠ |
| MCP-GPT URL | `https://mcp-gpt.big-gunter.com` | `https://physiology-mcp-gpt.big-gunter.com` ⚠ |
| mcp-server-gpt | always starts | profile `gpt-mcp` only ⚠ |
| whoop-fetch-data | always starts | profile `whoop` only ⚠ |
| garmin image tag | `garmin-fetch-data:physiology` | `garmin-fetch-data:physiology` ✓ |

**Action required before switching physiology to the generated file:**
- Update Cloudflare tunnel routing and DNS for the new subdomain URLs, or configure
  `physiology.conf` with `PHYSIOLOGY_GRAFANA_URL` overrides (future enhancement), or
  manually patch the generated file after generation.
- The network name change (`physiology-net` → `physiology-network`) will orphan existing
  containers on the old network. Run `docker compose down` first when migrating.

### connor

| Setting | Existing (`docker-compose.connor.yml`) | Generated |
|---|---|---|
| Network name | `connor-network` | `connor-network` ✓ |
| Grafana URL | `https://connor-grafana.big-gunter.com` | `https://connor-grafana.big-gunter.com` ✓ |
| MCP URL | `https://connor-mcp.big-gunter.com` | `https://connor-mcp.big-gunter.com` ✓ |
| MCP-GPT URL | `https://connor-mcp-gpt.big-gunter.com` | `https://connor-mcp-gpt.big-gunter.com` ✓ |
| mcp-server-gpt | always starts | profile `gpt-mcp` only (ENABLE_GPT_MCP=false → not started) ✓ |
| whoop-fetch-data | always starts | profile `whoop` only (ENABLE_WHOOP=false → not started) ✓ |
| garmin image tag | `garmin-fetch-data:physiology` | `garmin-fetch-data:connor` ⚠ (was wrong in original) |

The connor-generated file is a safe drop-in replacement once the image tag difference
is accounted for (rebuild the image: `docker compose build garmin-fetch-data`).
