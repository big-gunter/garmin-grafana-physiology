# Creating a Grafana Service Account Token for MCP

The MCP server uses a Grafana service account token to list dashboards,
create/update dashboards, and query datasources. Follow these steps to
generate one and add it to your deployment.

## Steps

1. Log into [grafana.big-gunter.com](https://grafana.big-gunter.com) as admin.

2. Go to **Administration → Service accounts → Add service account**

3. Fill in:
   - **Display name:** `mcp-coach`
   - **Role:** `Editor` (needs Editor to create/update dashboards)

4. Click **Create**.

5. On the service account page, click **Add service account token**.

6. Give the token a name (e.g. `mcp-server`) and set an expiry if desired.

7. Click **Generate token** and **copy the token immediately** — it is only shown once.

8. Add it to `/opt/physiology-repo/deploy/.env`:
   ```
   GRAFANA_TOKEN=glsa_xxxxxxxxxxxxxxxxxxxx
   ```

9. Restart the MCP server to pick up the new token:
   ```bash
   cd /opt/physiology-repo/deploy
   docker compose restart mcp-server
   ```

## Permissions

The `Editor` role allows the MCP server to:
- List all dashboards (`get_grafana_dashboards`)
- Create and update dashboards (`create_grafana_dashboard`)
- List datasources (`get_grafana_datasources`)

If you only want read access, use the `Viewer` role — but `create_grafana_dashboard` will then return a 403.
