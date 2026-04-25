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


class DeriveActivitiesRequest(BaseModel):
    window_days: int = Field(42, ge=1, le=365)
    limit: int = Field(250, ge=1, le=2000, description="Max activities to process")


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
            <button id="btnDeriveAll">Derive metrics from activities</button>
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
      function escapeHtml(s){
        return String(s ?? "").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll("\"","&quot;").replaceAll("'","&#39;");
      }
      function parseSections(md){
        const lines = String(md ?? "").split("\\n");
        const sections = [];
        let cur = { title: "Insights", body: [] };
        for (const ln of lines){
          const m = ln.match(/^#{2,3}\\s+(.*)$/);
          if (m){
            if (cur.body.length || cur.title) sections.push(cur);
            cur = { title: m[1].trim(), body: [] };
          } else {
            cur.body.push(ln);
          }
        }
        sections.push(cur);
        return sections.filter(s => (s.title || "").trim() || s.body.join("").trim());
      }

      function mdToHtml(md){
        // minimal markdown: bullets, bold, code, paragraphs (no tables).
        const esc = escapeHtml;
        const lines = String(md ?? "").split("\\n");
        const out = [];
        let inList = false;
        const flushList = () => { if(inList){ out.push("</ul>"); inList=false; } };
        for (let ln of lines){
          const li = ln.match(/^\\s*[-*]\\s+(.*)$/);
          if (li){
            if(!inList){ out.push("<ul style=\\"margin:10px 0 0 18px;color:rgba(226,232,240,0.92)\\">"); inList=true; }
            let t = esc(li[1]);
            t = t.replaceAll(/\\*\\*(.+?)\\*\\*/g, "<strong>$1</strong>");
            t = t.replaceAll(/`([^`]+)`/g, "<code style=\\"background:rgba(148,163,184,0.10);padding:1px 6px;border-radius:8px;border:1px solid rgba(148,163,184,0.12)\\">$1</code>");
            out.push(`<li>${t}</li>`);
            continue;
          }
          flushList();
          if (!ln.trim()) { out.push("<div style=\\"height:8px\\"></div>"); continue; }
          let t = esc(ln);
          t = t.replaceAll(/\\*\\*(.+?)\\*\\*/g, "<strong>$1</strong>");
          t = t.replaceAll(/`([^`]+)`/g, "<code style=\\"background:rgba(148,163,184,0.10);padding:1px 6px;border-radius:8px;border:1px solid rgba(148,163,184,0.12)\\">$1</code>");
          out.push(`<div style=\\"color:rgba(226,232,240,0.92)\\">${t}</div>`);
        }
        flushList();
        return out.join("");
      }

      function renderInsights(md){
        if (!md || typeof md !== "string") return null;
        const sections = parseSections(md);
        const cards = sections.map((s, idx) => {
          const title = escapeHtml(s.title || (idx === 0 ? "Insights" : ""));
          const body = mdToHtml(s.body.join("\\n"));
          return `<div style="border:1px solid rgba(148,163,184,0.16);border-radius:14px;background:rgba(2,6,23,0.35);padding:12px;margin-bottom:12px">
            <div style="font-size:14px;font-weight:800;margin-bottom:8px">${title}</div>
            ${body}
          </div>`;
        }).join("");
        return cards;
      }

      function setOut(x) {
        const pre = document.getElementById("out");
        const md = (typeof x?.insights === "string") ? x.insights : null;
        const cards = renderInsights(md);
        if (cards) {
          pre.innerHTML = cards + `<details style="margin-top:10px"><summary style="cursor:pointer;color:rgba(148,163,184,0.95)">Raw JSON</summary><pre style="min-height:0;margin-top:10px">${escapeHtml(JSON.stringify(x, null, 2))}</pre></details>`;
        } else {
          pre.textContent = JSON.stringify(x, null, 2);
        }
      }
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
      document.getElementById("btnDeriveAll").onclick = async () => {
        try { setOut(await call("/activities/derive_all", { window_days: getWindowDays(), limit: 500 })); }
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
    insights_obj = summarize_readiness(
        client=client,
        model=cfg.analysis_model,
        snapshot=snap.to_dict(),
        readiness=score,
        user_prompt=req.prompt,
    )
    return {"snapshot": snap.to_dict(), "readiness": score, "insights": insights_obj}


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
    insights_obj = summarize_readiness(
        client=client,
        model=cfg.analysis_model,
        snapshot=snap.to_dict(),
        readiness=readiness,
        user_prompt=req.prompt,
    )
    # Pretty string for storage (Grafana table expects a single field); keep it compact.
    import json as _json
    insights_text = _json.dumps(insights_obj, ensure_ascii=False)

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
        insights_text=insights_text,
        prompt=req.prompt,
    )

    return {"ok": True, "stored_measurement": "AgentInsights", "snapshot": snap.to_dict(), "readiness": readiness, "insights": insights_obj}


@app.post("/activities/derive_all")
def derive_all_activities(req: DeriveActivitiesRequest, authorization: str | None = Header(default=None)):
    _require_auth(authorization)
    if not cfg.allow_db_write:
        raise HTTPException(status_code=400, detail="AI_ALLOW_DB_WRITE is false; refusing to write to DB")
    if cfg.influx_version != "1":
        raise HTTPException(status_code=400, detail="Activity derivations currently support InfluxDB v1 only")

    from datetime import datetime, timedelta, timezone
    import pytz
    import pandas as pd

    from .influx_ro import query_influxql_df
    from .activity_derivations import derive_activity_metrics_from_streams
    from .influx_write import create_influx_v1_writer, write_agent_derived_activity

    since = datetime.now(timezone.utc) - timedelta(days=int(req.window_days))
    since_iso = since.isoformat().replace("+00:00", "Z")

    # Activity list from raw Garmin import
    df_act = query_influxql_df(_ro, f'SELECT * FROM "ActivitySummary" WHERE time >= \'{since_iso}\' ORDER BY time DESC')
    if df_act is None or df_act.empty:
        return {"ok": True, "processed": 0, "note": "No ActivitySummary rows in window"}

    # Try to determine HRmax/RHR/gender from raw Garmin imports
    df_daily = query_influxql_df(_ro, f'SELECT * FROM "DailyStats" WHERE time >= \'{since_iso}\' ORDER BY time ASC')
    hrmax = None
    rhr = None
    if df_daily is not None and not df_daily.empty:
        if "maxHeartRate" in df_daily.columns:
            try:
                hrmax = float(pd.to_numeric(df_daily["maxHeartRate"], errors="coerce").dropna().max())
            except Exception:
                hrmax = None
        if "restingHeartRate" in df_daily.columns:
            try:
                rhr = float(pd.to_numeric(df_daily["restingHeartRate"], errors="coerce").dropna().iloc[-1])
            except Exception:
                rhr = None

    df_profile = query_influxql_df(_ro, f'SELECT * FROM "UserProfileMaster" WHERE time >= \'{since_iso}\' ORDER BY time DESC LIMIT 1')
    gender = None
    if df_profile is not None and not df_profile.empty and "gender" in df_profile.columns:
        try:
            gender = str(df_profile["gender"].iloc[0]).strip().lower()
        except Exception:
            gender = None

    # Weight (kg) from raw Garmin import
    df_bc = query_influxql_df(_ro, f'SELECT * FROM "BodyComposition" WHERE time >= \'{since_iso}\' ORDER BY time DESC LIMIT 1')
    weight_kg = None
    if df_bc is not None and not df_bc.empty and "weight" in df_bc.columns:
        try:
            wv = pd.to_numeric(df_bc["weight"], errors="coerce").dropna()
            if not wv.empty:
                w_raw = float(wv.iloc[0])
                # Garmin exports are sometimes in grams or scaled units; normalize to kg.
                if w_raw > 500:  # implausible kg; assume grams
                    w_raw = w_raw / 1000.0
                weight_kg = w_raw if 20.0 <= w_raw <= 250.0 else None
        except Exception:
            weight_kg = None

    # Writer
    w = create_influx_v1_writer(
        host=cfg.influx_host,
        port=cfg.influx_port,
        username=cfg.influx_username,
        password=cfg.influx_password,
        database=cfg.influx_database,
    )

    processed = 0
    errors: list[dict] = []

    # Iterate activities (most recent first)
    for _, row in df_act.head(int(req.limit)).iterrows():
        act_id = row.get("Activity_ID") or row.get("ActivityId") or row.get("activityId") or row.get("ActivityID")
        if act_id is None:
            continue
        act_id_s = str(int(act_id)) if str(act_id).isdigit() else str(act_id)
        sport_tag = row.get("activity_type_tag") or row.get("activityType") or row.get("activityTypeName")
        start_time = row.get("time")
        if start_time is None:
            continue
        try:
            start_dt = pd.to_datetime(start_time, utc=True)
        except Exception:
            continue
        start_iso = start_dt.to_pydatetime().astimezone(pytz.UTC).isoformat(timespec="seconds")

        # Pull raw stream
        try:
            df_stream = query_influxql_df(
                _ro,
                f'SELECT "Speed","HeartRate","Altitude","Distance","Power","GradeAdjustedSpeed" FROM "ActivityGPS" WHERE "ActivityID" = \'{act_id_s}\' AND time >= \'{since_iso}\' ORDER BY time ASC',
            )
            if df_stream is None or df_stream.empty:
                # no stream: skip
                continue
            # HRmax fallback from activity if present
            hrmax_i = hrmax
            if hrmax_i is None and "maxHR" in row and row.get("maxHR") is not None:
                try:
                    hrmax_i = float(row.get("maxHR"))
                except Exception:
                    pass
            rhr_i = rhr

            derived = derive_activity_metrics_from_streams(
                activity_id=act_id_s,
                sport_tag=str(sport_tag) if sport_tag is not None else None,
                activity_type=str(row.get("activityType")) if row.get("activityType") is not None else None,
                start_time_utc=start_iso,
                df_stream=df_stream,
                hrmax_bpm=hrmax_i,
                rhr_bpm=rhr_i,
                gender=gender,
                weight_kg=weight_kg,
                cycling_gross_eff=cfg.cycling_gross_efficiency,
            )

            write_agent_derived_activity(
                client=w,
                activity_id=act_id_s,
                time_iso=start_iso,
                tags={"sport_tag": str(derived.sport_tag or ""), "activity_type": str(derived.activity_type or "")},
                fields=derived.fields,
            )
            processed += 1
        except Exception as e:
            errors.append({"activity_id": act_id_s, "error": str(e)})
            continue

    return {"ok": True, "processed": processed, "errors": errors[:20], "note": "Wrote AgentDerivedActivity from raw ActivityGPS streams"}


def main() -> None:
    import uvicorn

    uvicorn.run(app, host=cfg.host, port=cfg.port, log_level="info")


if __name__ == "__main__":
    main()

