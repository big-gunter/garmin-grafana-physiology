from __future__ import annotations

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .config import load_config
from .influx_ro import create_influx_ro
from .readiness import build_snapshot, readiness_score
from .anthropic_client import LLMConfig, create_client, summarize_readiness


cfg = load_config()
app = FastAPI(title="Garmin Grafana AI Agent", version="0.1.0")

_ro = create_influx_ro(
    version=cfg.influx_version,
    host=cfg.influx_host,
    port=cfg.influx_port,
    username=cfg.influx_username,
    password=cfg.influx_password,
    database=cfg.influx_database,
    endpoint_is_http=cfg.influx_endpoint_is_http,
)


def _require_auth(authorization: str | None) -> None:
    if not cfg.auth_token:
        return
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    # Expect: Authorization: Bearer <token>
    parts = authorization.split(" ", 1)
    token = parts[1].strip() if len(parts) == 2 and parts[0].lower() == "bearer" else authorization.strip()
    if token != cfg.auth_token:
        raise HTTPException(status_code=403, detail="Invalid token")


class QueryRequest(BaseModel):
    influxql: str = Field(..., min_length=1, description="InfluxQL query (read-only)")


class SnapshotRequest(BaseModel):
    window_days: int = Field(42, ge=1, le=365)

class InsightsRequest(BaseModel):
    window_days: int = Field(42, ge=1, le=365)
    prompt: str | None = Field(default=None, description="Optional user context/question")


@app.get("/health")
def health():
    return {"ok": True}

@app.get("/", response_class=HTMLResponse)
def web_ui():
    # Minimal browser UI so the agent can be operated without CLI.
    return """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>AI Training Agent</title>
    <style>
      :root{
        --bg0:#05070c;
        --bg1:#0a0f18;
        --panel:#0b1220cc;
        --panel2:#0d1628e6;
        --border:#1f2937;
        --muted:#94a3b8;
        --text:#e6edf3;
        --accent:#f59e0b;
        --accent2:#60a5fa;
        --good:#22c55e;
      }
      *{ box-sizing:border-box; }
      html,body{ height:100%; }
      body{
        margin:0;
        color:var(--text);
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
        line-height:1.35;
        background-image:
          /* Dark marble veining (procedural SVG) */
          url("data:image/svg+xml,%3Csvg%20xmlns='http://www.w3.org/2000/svg'%20viewBox='0%200%201000%201000'%3E%3Cdefs%3E%3Cfilter%20id='m'%20x='-20%25'%20y='-20%25'%20width='140%25'%20height='140%25'%3E%3CfeTurbulence%20type='fractalNoise'%20baseFrequency='0.012'%20numOctaves='4'%20seed='11'%20stitchTiles='stitch'%20result='noise'/%3E%3CfeColorMatrix%20in='noise'%20type='matrix'%20values='4%200%200%200%20-1.6%200%204%200%200%20-1.6%200%200%204%200%20-1.6%200%200%200%201%200'%20result='hi'/%3E%3CfeComponentTransfer%20in='hi'%20result='thr'%3E%3CfeFuncR%20type='gamma'%20amplitude='1'%20exponent='2.4'%20offset='-0.28'/%3E%3CfeFuncG%20type='gamma'%20amplitude='1'%20exponent='2.4'%20offset='-0.28'/%3E%3CfeFuncB%20type='gamma'%20amplitude='1'%20exponent='2.4'%20offset='-0.28'/%3E%3C/feComponentTransfer%3E%3CfeGaussianBlur%20in='thr'%20stdDeviation='0.8'%20result='soft'%3E%3C/feGaussianBlur%3E%3CfeComponentTransfer%20in='soft'%3E%3CfeFuncA%20type='table'%20tableValues='0%200%200.02%200.06%200.12%200.20%200.32%200.48%200.68%200.90%201'/%3E%3C/feComponentTransfer%3E%3C/filter%3E%3ClinearGradient%20id='g'%20x1='0'%20y1='0'%20x2='0'%20y2='1'%3E%3Cstop%20offset='0%25'%20stop-color='%2303060b'/%3E%3Cstop%20offset='55%25'%20stop-color='%23060a12'/%3E%3Cstop%20offset='100%25'%20stop-color='%23010307'/%3E%3C/linearGradient%3E%3C/defs%3E%3Crect%20width='1000'%20height='1000'%20fill='url(%23g)'/%3E%3Crect%20width='1000'%20height='1000'%20filter='url(%23m)'%20opacity='0.88'%20fill='%23e5e7eb'/%3E%3C/svg%3E"),
          /* subtle vignette only (no color) */
          radial-gradient(1200px 900px at 50% 15%, rgba(255,255,255,0.03), rgba(0,0,0,0.0) 55%),
          linear-gradient(180deg, var(--bg0), var(--bg1));
        /* Prevent tiling; render the marble as a single scaled surface */
        background-repeat: no-repeat, no-repeat, no-repeat;
        background-size: cover, cover, cover;
        background-position: center, center, center;
        background-attachment: fixed, fixed, fixed;
        background-blend-mode: normal, overlay, multiply;
      }
      .container{
        max-width: 1060px;
        margin: 0 auto;
        padding: 28px 20px 48px;
      }
      header{
        display:flex;
        align-items:center;
        justify-content:space-between;
        gap:16px;
        margin-bottom: 14px;
      }
      .titleRow{ display:flex; align-items:baseline; gap:10px; flex-wrap:wrap; }
      h1{ font-size: 20px; margin:0; letter-spacing:0.2px; }
      .badge{
        font-size: 12px;
        padding: 2px 8px;
        border-radius: 999px;
        border:1px solid rgba(245,158,11,0.35);
        background: rgba(245,158,11,0.10);
        color: rgba(245,158,11,0.95);
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }
      .subtitle{ margin:6px 0 0; color: var(--muted); font-size: 13px; }
      .status{
        display:flex; align-items:center; gap:8px;
        color: var(--muted);
        font-size: 13px;
      }
      .dot{
        width:10px;height:10px;border-radius:50%;
        background: var(--good);
        box-shadow: 0 0 0 3px rgba(34,197,94,0.15);
      }
      .banner{
        display:flex;
        gap:10px;
        align-items:flex-start;
        padding:12px 14px;
        border-radius: 12px;
        background: rgba(245,158,11,0.08);
        border: 1px solid rgba(245,158,11,0.22);
        margin: 14px 0 18px;
        color: rgba(245,158,11,0.95);
      }
      .banner strong{ color: rgba(245,158,11,1); }
      .grid{
        display:grid;
        grid-template-columns: 1fr;
        gap:14px;
      }
      @media(min-width: 980px){
        .grid{ grid-template-columns: 420px 1fr; align-items:start; }
      }
      .panel{
        background: var(--panel);
        border: 1px solid rgba(148,163,184,0.18);
        border-radius: 16px;
        padding: 14px;
        backdrop-filter: blur(10px);
      }
      .panel h2{
        font-size: 12px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: rgba(148,163,184,0.9);
        margin: 0 0 12px;
      }
      .row{ display:flex; gap: 12px; flex-wrap:wrap; align-items:center; }
      .field{
        display:flex;
        flex-direction:column;
        gap:6px;
        min-width: 160px;
        flex: 1 1 160px;
      }
      label{ font-size: 12px; color: rgba(148,163,184,0.95); }
      input, textarea{
        width: 100%;
        padding: 10px 12px;
        border-radius: 12px;
        border: 1px solid rgba(148,163,184,0.18);
        background: rgba(2,6,23,0.55);
        color: var(--text);
        outline: none;
      }
      input:focus, textarea:focus{
        border-color: rgba(96,165,250,0.55);
        box-shadow: 0 0 0 4px rgba(96,165,250,0.12);
      }
      textarea{ min-height: 120px; resize: vertical; }
      .actions{
        display:flex;
        flex-wrap:wrap;
        gap:10px;
      }
      button{
        padding: 10px 12px;
        border-radius: 12px;
        border: 1px solid rgba(148,163,184,0.22);
        background: rgba(15,23,42,0.65);
        color: var(--text);
        cursor: pointer;
        transition: transform .05s ease, border-color .15s ease, background .15s ease;
      }
      button:hover{ border-color: rgba(96,165,250,0.35); }
      button:active{ transform: translateY(1px); }
      button.primary{
        background: linear-gradient(180deg, rgba(245,158,11,0.95), rgba(245,158,11,0.75));
        border-color: rgba(245,158,11,0.55);
        color: rgba(15,23,42,0.95);
        font-weight: 700;
      }
      button.ghost{
        background: rgba(2,6,23,0.35);
      }
      .tabs{
        display:flex;
        gap:10px;
        align-items:center;
        flex-wrap:wrap;
        margin-bottom: 10px;
      }
      .tab{
        padding: 8px 10px;
        border-radius: 999px;
        border: 1px solid rgba(148,163,184,0.18);
        background: rgba(2,6,23,0.35);
        color: rgba(226,232,240,0.9);
        font-size: 12px;
        cursor:pointer;
      }
      .tab.active{
        background: rgba(96,165,250,0.12);
        border-color: rgba(96,165,250,0.30);
      }
      pre{
        margin:0;
        background: rgba(2,6,23,0.55);
        border: 1px solid rgba(148,163,184,0.16);
        color: #e6edf3;
        padding: 12px;
        overflow: auto;
        border-radius: 14px;
        min-height: 260px;
      }
    </style>
  </head>
  <body>
    <div class="container">
      <header>
        <div>
          <div class="titleRow">
            <h1>AI Training Agent</h1>
            <span class="badge">beta</span>
          </div>
          <div class="subtitle">Browser UI calling the agent API. Metrics computed from raw Garmin-imported data in InfluxDB.</div>
        </div>
        <div class="status"><span class="dot"></span> Ready</div>
      </header>

      <div class="banner">
        <div>⚠️</div>
        <div><strong>Beta</strong>: tool calls and schema are subject to change. Output may be incomplete or inconsistent across runs.</div>
      </div>

      <div class="grid">
        <div class="panel">
          <h2>Configuration</h2>
          <div class="row">
            <div class="field">
              <label>Window (days)</label>
              <input id="windowDays" type="number" min="1" max="365" value="42"/>
            </div>
            <div class="field">
              <label>Auth token (optional)</label>
              <input id="authToken" type="password" placeholder="Bearer token"/>
            </div>
          </div>

          <div style="height: 12px;"></div>
          <h2>Actions</h2>
          <div class="actions">
            <button class="primary" id="btnInsights">Insights (Anthropic)</button>
            <button id="btnSnapshot">Snapshot</button>
            <button id="btnReadiness">Readiness</button>
            <button class="ghost" id="btnStoreInsights">Store insights to DB</button>
            <button class="ghost" id="btnGrafana">Grafana: write dashboard file</button>
            <button class="ghost" id="btnGrafanaPush">Grafana: push via API</button>
          </div>

          <div style="height: 12px;"></div>
          <h2>Prompt / context</h2>
          <textarea id="prompt" placeholder="e.g., I feel a bit flat today; race in 8 weeks; what should I do?"></textarea>
        </div>

        <div class="panel">
          <div class="tabs">
            <div class="tab active" id="tabOutput">Output</div>
            <div class="tab" id="tabActivity">Activity log</div>
            <div class="tab" id="tabMetrics">Metrics</div>
          </div>
          <pre id="out">{}</pre>
        </div>
      </div>
    </div>

    <script>
      function headers() {
        const t = document.getElementById("authToken").value.trim();
        const h = { "Content-Type": "application/json" };
        if (t) h["Authorization"] = "Bearer " + t;
        return h;
      }
      async function call(path, body) {
        const res = await fetch(path, { method: "POST", headers: headers(), body: JSON.stringify(body) });
        const txt = await res.text();
        let json;
        try { json = JSON.parse(txt); } catch { json = { raw: txt }; }
        if (!res.ok) throw json;
        return json;
      }
      function setOut(x) { document.getElementById("out").textContent = JSON.stringify(x, null, 2); }
      function getWindowDays() { return parseInt(document.getElementById("windowDays").value || "42", 10); }

      document.getElementById("btnSnapshot").onclick = async () => {
        try { setOut(await call("/snapshot", { window_days: getWindowDays() })); }
        catch(e) { setOut(e); }
      };
      document.getElementById("btnReadiness").onclick = async () => {
        try { setOut(await call("/readiness", { window_days: getWindowDays() })); }
        catch(e) { setOut(e); }
      };
      document.getElementById("btnInsights").onclick = async () => {
        const prompt = document.getElementById("prompt").value || null;
        try { setOut(await call("/insights", { window_days: getWindowDays(), prompt })); }
        catch(e) { setOut(e); }
      };
      document.getElementById("btnStoreInsights").onclick = async () => {
        const prompt = document.getElementById("prompt").value || null;
        try { setOut(await call("/insights/store", { window_days: getWindowDays(), prompt })); }
        catch(e) { setOut(e); }
      };
      document.getElementById("btnGrafana").onclick = async () => {
        try { setOut(await call("/grafana/write_dashboard_file", {})); }
        catch(e) { setOut(e); }
      };
      document.getElementById("btnGrafanaPush").onclick = async () => {
        try { setOut(await call("/grafana/push_dashboard_api", {})); }
        catch(e) { setOut(e); }
      };

      // simple local tabs (all show same output for now; reserved for future enhancements)
      function setActive(tabId){
        for (const id of ["tabOutput","tabActivity","tabMetrics"]) {
          const el = document.getElementById(id);
          if (!el) continue;
          el.classList.toggle("active", id === tabId);
        }
      }
      document.getElementById("tabOutput").onclick = () => setActive("tabOutput");
      document.getElementById("tabActivity").onclick = () => setActive("tabActivity");
      document.getElementById("tabMetrics").onclick = () => setActive("tabMetrics");
    </script>
  </body>
</html>
    """.strip()


@app.post("/grafana/write_dashboard_file")
def grafana_write_dashboard_file(authorization: str | None = Header(default=None)):
    _require_auth(authorization)
    # File provisioning: Grafana watches Grafana_Dashboard/ in the repo via bind mount.
    # We keep this minimal: it just confirms the file exists and returns its path.
    import os
    from pathlib import Path

    repo_path = Path("/app/Grafana_Dashboard/AI-Coach.json")
    if not repo_path.exists():
        raise HTTPException(status_code=404, detail="AI-Coach.json not found in mounted Grafana_Dashboard/")
    return {"ok": True, "dashboard_file": str(repo_path), "note": "Grafana file provisioning will auto-reload within ~10s."}


@app.post("/grafana/push_dashboard_api")
def grafana_push_dashboard_api(authorization: str | None = Header(default=None)):
    _require_auth(authorization)
    if not cfg.grafana_api_token:
        raise HTTPException(status_code=400, detail="GRAFANA_API_TOKEN not configured")

    from .grafana_api import push_dashboard_json

    dashboard_path = "/app/Grafana_Dashboard/AI-Coach.json"
    try:
        result = push_dashboard_json(
            grafana_url=cfg.grafana_url,
            api_token=cfg.grafana_api_token,
            dashboard_path=dashboard_path,
            folder_id=0,
            overwrite=True,
        )
        return {"ok": True, "pushed_from": dashboard_path, "grafana_result": result}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="AI-Coach.json not found in mounted Grafana_Dashboard/")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/query")
def query_db(req: QueryRequest, authorization: str | None = Header(default=None)):
    _require_auth(authorization)
    from .influx_ro import query_influxql_df

    q = req.influxql.strip()
    # crude guardrails: prevent obvious writes
    lower = q.lower()
    if any(k in lower for k in ["drop ", "delete ", "into ", "create ", "alter "]):
        raise HTTPException(status_code=400, detail="Query rejected (write-like keyword detected)")
    df = query_influxql_df(_ro, q)
    return {"rows": df.to_dict(orient="records"), "row_count": int(len(df))}


@app.post("/snapshot")
def snapshot(req: SnapshotRequest, authorization: str | None = Header(default=None)):
    _require_auth(authorization)
    snap = build_snapshot(_ro, window_days=req.window_days)
    return snap.to_dict()


@app.post("/readiness")
def readiness(req: SnapshotRequest, authorization: str | None = Header(default=None)):
    _require_auth(authorization)
    snap = build_snapshot(_ro, window_days=req.window_days)
    score = readiness_score(snap)
    return {"snapshot": snap.to_dict(), "readiness": score}


@app.post("/insights")
def insights(req: InsightsRequest, authorization: str | None = Header(default=None)):
    _require_auth(authorization)
    if not cfg.anthropic_api_key:
        raise HTTPException(status_code=400, detail="ANTHROPIC_API_KEY not configured")
    snap = build_snapshot(_ro, window_days=req.window_days)
    score = readiness_score(snap)
    client = create_client(
        LLMConfig(
            api_key=cfg.anthropic_api_key,
            analysis_model=cfg.analysis_model,
            planning_model=cfg.planning_model,
        )
    )
    text = summarize_readiness(
        client=client,
        model=cfg.analysis_model,
        snapshot=snap.to_dict(),
        readiness=score,
        user_prompt=req.prompt,
    )
    return {"snapshot": snap.to_dict(), "readiness": score, "insights": text}


@app.post("/insights/store")
def insights_store(req: InsightsRequest, authorization: str | None = Header(default=None)):
    _require_auth(authorization)
    if not cfg.allow_db_write:
        raise HTTPException(status_code=400, detail="AI_ALLOW_DB_WRITE is false; refusing to write to DB")
    if cfg.influx_version != "1":
        raise HTTPException(status_code=400, detail="Insights storage currently supports InfluxDB v1 only")
    if not cfg.anthropic_api_key:
        raise HTTPException(status_code=400, detail="ANTHROPIC_API_KEY not configured")

    snap = build_snapshot(_ro, window_days=req.window_days)
    readiness = readiness_score(snap)

    client = create_client(
        LLMConfig(
            api_key=cfg.anthropic_api_key,
            analysis_model=cfg.analysis_model,
            planning_model=cfg.planning_model,
        )
    )
    text = summarize_readiness(
        client=client,
        model=cfg.analysis_model,
        snapshot=snap.to_dict(),
        readiness=readiness,
        user_prompt=req.prompt,
    )

    from .influx_write import create_influx_v1_writer, write_agent_insights

    w = create_influx_v1_writer(
        host=cfg.influx_host,
        port=cfg.influx_port,
        username=cfg.influx_username,
        password=cfg.influx_password,
        database=cfg.influx_database,
    )
    write_agent_insights(
        client=w,
        readiness_score=float(readiness.get("score", 0.0)),
        window_days=req.window_days,
        insights_text=text,
        prompt=req.prompt,
    )

    return {"ok": True, "stored_measurement": "AgentInsights", "snapshot": snap.to_dict(), "readiness": readiness, "insights": text}


def main() -> None:
    import uvicorn

    uvicorn.run(app, host=cfg.host, port=cfg.port, log_level="info")


if __name__ == "__main__":
    main()

