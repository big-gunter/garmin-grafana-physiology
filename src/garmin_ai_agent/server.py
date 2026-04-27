from __future__ import annotations

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .config import load_config
from .influx_ro import create_influx_ro
from .readiness import build_snapshot, readiness_score
from .anthropic_client import LLMConfig, create_client, summarize_readiness
from . import metrics as metrics_engine


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

class MetricsCatalogResponse(BaseModel):
    metrics: list[dict]


class MetricsComputeRequest(BaseModel):
    metrics: list[str] = Field(..., min_length=1, description="Metric names (see /metrics/catalog)")
    window_days: int = Field(30, ge=1, le=365)
    activity_limit: int = Field(20, ge=1, le=200, description="Max activities to scan for window metrics")


class MetricsQueryRequest(BaseModel):
    query: str = Field(..., min_length=1, description="Natural language metric request")
    window_days: int = Field(30, ge=1, le=365)
    activity_limit: int = Field(20, ge=1, le=200, description="Max activities to scan for window metrics")


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
        /* neutral highlight (replace blue) */
        --accent2:#cbd5e1;
        --good:#22c55e;
      }
      *{ box-sizing:border-box; }
      html,body{ height:100%; }
      body{
        margin:0;
        color:var(--text);
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
        line-height:1.35;
        overflow-x: hidden;
        background-image:
          /* Dark marble veining (procedural SVG) */
          url("data:image/svg+xml,%3Csvg%20xmlns='http://www.w3.org/2000/svg'%20viewBox='0%200%201000%201000'%3E%3Cdefs%3E%3Cfilter%20id='m'%20x='-20%25'%20y='-20%25'%20width='140%25'%20height='140%25'%3E%3CfeTurbulence%20type='fractalNoise'%20baseFrequency='0.012'%20numOctaves='5'%20seed='11'%20stitchTiles='stitch'%20result='noise'/%3E%3CfeColorMatrix%20in='noise'%20type='matrix'%20values='0.333%200.333%200.333%200%200%200.333%200.333%200.333%200%200%200.333%200.333%200.333%200%200%200%200%200%201%200'%20result='mono'/%3E%3CfeComponentTransfer%20in='mono'%20result='thr'%3E%3CfeFuncR%20type='gamma'%20amplitude='1'%20exponent='2.35'%20offset='-0.18'/%3E%3CfeFuncG%20type='gamma'%20amplitude='1'%20exponent='2.35'%20offset='-0.18'/%3E%3CfeFuncB%20type='gamma'%20amplitude='1'%20exponent='2.35'%20offset='-0.18'/%3E%3C/feComponentTransfer%3E%3CfeGaussianBlur%20in='thr'%20stdDeviation='0.7'%20result='soft'/%3E%3CfeComponentTransfer%20in='soft'%3E%3CfeFuncA%20type='table'%20tableValues='0%200%200.01%200.03%200.06%200.10%200.16%200.26%200.40%200.60%201'/%3E%3C/feComponentTransfer%3E%3C/filter%3E%3ClinearGradient%20id='g'%20x1='0'%20y1='0'%20x2='0'%20y2='1'%3E%3Cstop%20offset='0%25'%20stop-color='%23010102'/%3E%3Cstop%20offset='55%25'%20stop-color='%23050506'/%3E%3Cstop%20offset='100%25'%20stop-color='%23000000'/%3E%3C/linearGradient%3E%3C/defs%3E%3Crect%20width='1000'%20height='1000'%20fill='url(%23g)'/%3E%3Crect%20width='1000'%20height='1000'%20filter='url(%23m)'%20opacity='0.92'%20fill='%23ffffff'/%3E%3C/svg%3E"),
          /* subtle vignette only (no color) */
          radial-gradient(1200px 900px at 50% 15%, rgba(255,255,255,0.03), rgba(0,0,0,0.0) 55%),
          linear-gradient(180deg, var(--bg0), var(--bg1));
        /* Prevent tiling; render the marble as a single scaled surface */
        background-repeat: no-repeat, no-repeat, no-repeat;
        background-size: cover, cover, cover;
        background-position: center, center, center;
        background-attachment: fixed, fixed, fixed;
        background-blend-mode: normal, normal, normal;
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
      .dot.busy{
        background: var(--accent2);
        box-shadow: 0 0 0 3px rgba(203,213,225,0.14);
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
        .grid{ grid-template-columns: 420px minmax(0, 1fr); align-items:start; }
      }
      .panel{
        background: var(--panel);
        border: 1px solid rgba(148,163,184,0.18);
        border-radius: 16px;
        padding: 14px;
        backdrop-filter: blur(10px);
        min-width: 0; /* allows children to wrap instead of forcing horizontal overflow */
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
        /* neutral dark grey (avoid blue tint) */
        background: rgba(18,18,18,0.72);
        color: var(--text);
        outline: none;
      }
      input:focus, textarea:focus{
        border-color: rgba(203,213,225,0.45);
        box-shadow: 0 0 0 4px rgba(203,213,225,0.10);
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
      button:hover{ border-color: rgba(203,213,225,0.30); }
      button:active{ transform: translateY(1px); }
      button.primary{
        background: linear-gradient(180deg, rgba(245,158,11,0.95), rgba(245,158,11,0.75));
        border-color: rgba(245,158,11,0.55);
        color: rgba(15,23,42,0.95);
        font-weight: 700;
      }
      button:disabled{
        opacity: 0.55;
        cursor: not-allowed;
        transform: none !important;
      }
      button.ghost{
        background: rgba(18,18,18,0.35);
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
        background: rgba(18,18,18,0.35);
        color: rgba(226,232,240,0.9);
        font-size: 12px;
        cursor:pointer;
      }
      .tab.active{
        background: rgba(203,213,225,0.08);
        border-color: rgba(203,213,225,0.22);
      }
      pre{
        margin:0;
        /* neutral dark grey output surface */
        background: rgba(18,18,18,0.62);
        border: 1px solid rgba(148,163,184,0.16);
        color: #e6edf3;
        padding: 12px;
        overflow-y: auto;
        overflow-x: hidden;
        border-radius: 14px;
        min-height: 260px;
        max-width: 100%;
        white-space: pre-wrap;
        overflow-wrap: anywhere;
        word-break: break-word;
      }
      pre *{
        max-width: 100%;
        overflow-wrap: anywhere;
        word-break: break-word;
      }
      .hint{
        margin-top:8px;
        font-size:12px;
        color: rgba(148,163,184,0.85);
      }
      .chat{
        display:flex;
        flex-direction:column;
        gap:10px;
        height: calc(100vh - 240px);
        min-height: 360px;
        max-height: 70vh;
        overflow-y:auto;
        overflow-x:hidden;
        padding: 2px;
      }
      .msg{
        border:1px solid rgba(148,163,184,0.16);
        border-radius: 14px;
        padding: 12px;
        background: rgba(18,18,18,0.50);
      }
      .msg .meta{
        font-size:12px;
        letter-spacing:0.12em;
        text-transform:uppercase;
        color: rgba(148,163,184,0.9);
        margin-bottom: 8px;
      }
      .msg.user{
        background: rgba(18,18,18,0.38);
      }
      .msg.assistant{
        background: rgba(18,18,18,0.52);
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
        <div class="status"><span class="dot" id="statusDot"></span><span id="statusText">Ready</span></div>
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
            <button id="btnMetrics">Compute metrics</button>
            <button class="ghost" id="btnStoreInsights">Store insights to DB</button>
            <button class="ghost" id="btnGrafana">Grafana: write dashboard file</button>
            <button class="ghost" id="btnGrafanaPush">Grafana: push via API</button>
          </div>

          <div style="height: 12px;"></div>
          <h2>Prompt / context</h2>
          <form id="promptForm">
            <textarea id="prompt" placeholder="e.g., I feel a bit flat today; race in 8 weeks; what should I do?"></textarea>
            <div style="height: 10px;"></div>
            <div class="actions">
              <button class="primary" id="btnSubmitPrompt" type="submit">Send prompt (Insights)</button>
              <button class="ghost" id="btnClear" type="button">Clear</button>
            </div>
            <div class="hint">Tip: press <strong>Enter</strong> to send, <strong>Shift+Enter</strong> for a newline.</div>
          </form>
        </div>

        <div class="panel">
          <div class="tabs">
            <div class="tab active" id="tabOutput">Output</div>
            <div class="tab" id="tabActivity">Activity log</div>
            <div class="tab" id="tabMetrics">Metrics</div>
          </div>
          <div id="chat" class="chat">
            <div class="msg assistant">
              <div class="meta">Assistant</div>
              <div style="color:rgba(226,232,240,0.92)">Ask a question or run an action to start.</div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <script>
      function byId(id){ return document.getElementById(id); }

      function chatEl(){ return byId("chat"); }

      function scrollChatToBottom(){
        const c = chatEl();
        if (!c) return;
        c.scrollTop = c.scrollHeight;
      }

      function appendMsg(role, html){
        const c = chatEl();
        if (!c) return null;
        const el = document.createElement("div");
        el.className = "msg " + role;
        const who = role === "user" ? "You" : "Assistant";
        el.innerHTML = `<div class="meta">${who}</div>` + html;
        c.appendChild(el);
        scrollChatToBottom();
        return el;
      }

      function safeSetOut(msg){
        try { setOut(msg); }
        catch (e) {
          appendMsg("assistant", `<div style="color:rgba(226,232,240,0.92)">${escapeHtml(String(msg))}</div>`);
        }
      }

      // Surface JS errors in the Output pane (helps debug "nothing happens")
      window.addEventListener("error", function(ev){
        const m = ev && ev.message ? ev.message : "Unknown JS error";
        safeSetOut({ error: "UI script error", detail: m });
      });
      window.addEventListener("unhandledrejection", function(ev){
        const r = ev && ev.reason ? ev.reason : "Unhandled promise rejection";
        safeSetOut({ error: "UI promise rejection", detail: String(r && (r.detail || r.message) ? (r.detail || r.message) : r) });
      });

      function setBusy(busy, msg){
        const dot = byId("statusDot");
        const text = byId("statusText");
        if (dot) dot.classList.toggle("busy", !!busy);
        if (text) text.textContent = busy ? (msg || "Working…") : "Ready";
        for (const id of ["btnInsights","btnSnapshot","btnReadiness","btnDeriveAll","btnMetrics","btnStoreInsights","btnGrafana","btnGrafanaPush","btnSubmitPrompt","btnClear"]) {
          const el = byId(id);
          if (el) el.disabled = !!busy;
        }
      }

      function headers() {
        const t = document.getElementById("authToken").value.trim();
        const h = { "Content-Type": "application/json" };
        if (t) h["Authorization"] = "Bearer " + t;
        return h;
      }
      async function call(path, body) {
        // Use an absolute URL so this works behind reverse proxies/base paths.
        const url = new URL(path, window.location.href).toString();
        const res = await fetch(url, { method: "POST", headers: headers(), body: JSON.stringify(body) });
        const txt = await res.text();
        let json;
        try { json = JSON.parse(txt); } catch { json = { raw: txt }; }
        if (!res.ok) throw json;
        return json;
      }
      function escapeHtml(s){
        const x = (s === null || s === undefined) ? "" : String(s);
        return x
          .replace(/&/g,"&amp;")
          .replace(/</g,"&lt;")
          .replace(/>/g,"&gt;")
          .replace(/"/g,"&quot;")
          .replace(/'/g,"&#39;");
      }
      function mdToHtml(md){
        // minimal markdown: bullets, bold, code, paragraphs (no tables).
        const esc = escapeHtml;
        const lines = String((md === null || md === undefined) ? "" : md).split("\\n");
        const out = [];
        let inList = false;
        const flushList = () => { if(inList){ out.push("</ul>"); inList=false; } };
        for (let ln of lines){
          const li = ln.match(/^\\s*[-*]\\s+(.*)$/);
          if (li){
            if(!inList){ out.push("<ul style=\\"margin:10px 0 0 18px;color:rgba(226,232,240,0.92)\\">"); inList=true; }
            let t = esc(li[1]);
            t = t.replace(/\\*\\*(.+?)\\*\\*/g, "<strong>$1</strong>");
            t = t.replace(/`([^`]+)`/g, "<code style=\\"background:rgba(148,163,184,0.10);padding:1px 6px;border-radius:8px;border:1px solid rgba(148,163,184,0.12)\\">$1</code>");
            out.push(`<li>${t}</li>`);
            continue;
          }
          flushList();
          if (!ln.trim()) { out.push("<div style=\\"height:8px\\"></div>"); continue; }
          let t = esc(ln);
          t = t.replace(/\\*\\*(.+?)\\*\\*/g, "<strong>$1</strong>");
          t = t.replace(/`([^`]+)`/g, "<code style=\\"background:rgba(148,163,184,0.10);padding:1px 6px;border-radius:8px;border:1px solid rgba(148,163,184,0.12)\\">$1</code>");
          out.push(`<div style=\\"color:rgba(226,232,240,0.92)\\">${t}</div>`);
        }
        flushList();
        return out.join("");
      }

      function renderInsights(md){
        if (!md || typeof md !== "string") return null;
        const body = mdToHtml(md);
        return `<div>${body}</div>`;
      }

      function renderReadiness(x){
        const r = (x && x.readiness) ? x.readiness : null;
        const snap = (x && x.snapshot) ? x.snapshot : null;
        if (!r || typeof r !== "object") return null;
        const score = (r.score !== undefined && r.score !== null) ? r.score
          : ((r.readiness_score !== undefined && r.readiness_score !== null) ? r.readiness_score : r.value);
        const reasons = Array.isArray(r.reasons) ? r.reasons : [];
        const inputs = (r.inputs && typeof r.inputs === "object") ? r.inputs : null;
        const scoreLine = (score === undefined || score === null) ? "" : `<div style="font-size:34px;font-weight:900;letter-spacing:-0.02em">${escapeHtml(score)}</div>`;
        const reasonsHtml = reasons.length ? `<div style="margin-top:8px">${mdToHtml(reasons.map(v => `- ${v}`).join("\\n"))}</div>` : "";
        const inputsHtml = inputs ? `<details style="margin-top:10px"><summary style="cursor:pointer;color:rgba(148,163,184,0.95)">Inputs used</summary><pre style="min-height:0;margin-top:10px">${escapeHtml(JSON.stringify(inputs, null, 2))}</pre></details>` : "";
        const snapHtml = snap ? `<details style="margin-top:10px"><summary style="cursor:pointer;color:rgba(148,163,184,0.95)">Snapshot (raw)</summary><pre style="min-height:0;margin-top:10px">${escapeHtml(JSON.stringify(snap, null, 2))}</pre></details>` : "";
        return `<div style="border:1px solid rgba(148,163,184,0.16);border-radius:14px;background:rgba(18,18,18,0.50);padding:12px;margin-bottom:12px">
          <div style="font-size:14px;font-weight:800;margin-bottom:8px">Readiness</div>
          ${scoreLine}
          ${reasonsHtml}
          ${inputsHtml}
          ${snapHtml}
        </div>`;
      }

      function renderSnapshot(x){
        const snap = (x && typeof x === "object" && !Array.isArray(x)) ? x : null;
        if (!snap) return null;
        // Heuristic: snapshot responses tend to have a "debug" object plus metric groups.
        const debug = snap.debug && typeof snap.debug === "object" ? snap.debug : null;
        const available = debug && debug.available_signals ? debug.available_signals : null;
        const availHtml = Array.isArray(available) && available.length
          ? `<div style="margin-top:8px">${mdToHtml(["**Available signals:**", ...available.map(s => "- " + s)].join("\\n"))}</div>`
          : "";
        return `<div style="border:1px solid rgba(148,163,184,0.16);border-radius:14px;background:rgba(18,18,18,0.50);padding:12px;margin-bottom:12px">
          <div style="font-size:14px;font-weight:800;margin-bottom:8px">Snapshot</div>
          ${availHtml}
          <details style="margin-top:10px"><summary style="cursor:pointer;color:rgba(148,163,184,0.95)">Raw snapshot</summary><pre style="min-height:0;margin-top:10px">${escapeHtml(JSON.stringify(snap, null, 2))}</pre></details>
        </div>`;
      }

      function setOut(x) {
        // Prefer formatted rendering over raw JSON.
        const insightsMd = (x && typeof x.insights === "string") ? x.insights : null;
        const insightsCards = renderInsights(insightsMd);
        const readinessCard = renderReadiness(x);
        const snapshotCard = (!insightsCards && !readinessCard) ? renderSnapshot(x) : null;
        const errDetail = (x && typeof x === "object") ? (x.detail || x.error || x.message) : null;

        if (insightsCards) {
          appendMsg("assistant", insightsCards + `<details style="margin-top:10px"><summary style="cursor:pointer;color:rgba(148,163,184,0.95)">Raw response</summary><pre style="min-height:0;margin-top:10px;white-space:pre-wrap;overflow-wrap:anywhere;word-break:break-word">${escapeHtml(JSON.stringify(x, null, 2))}</pre></details>`);
          return;
        }
        if (readinessCard) {
          appendMsg("assistant", readinessCard + `<details style="margin-top:10px"><summary style="cursor:pointer;color:rgba(148,163,184,0.95)">Raw response</summary><pre style="min-height:0;margin-top:10px;white-space:pre-wrap;overflow-wrap:anywhere;word-break:break-word">${escapeHtml(JSON.stringify(x, null, 2))}</pre></details>`);
          return;
        }
        if (snapshotCard) {
          appendMsg("assistant", snapshotCard);
          return;
        }
        if (errDetail) {
          appendMsg("assistant", `<div style="border:1px solid rgba(245,158,11,0.22);border-radius:14px;background:rgba(18,18,18,0.55);padding:12px">
            <div style="font-size:14px;font-weight:800;margin-bottom:8px">Error</div>
            ${mdToHtml(`- **Detail**: ${String(errDetail)}`)}
            <details style="margin-top:10px"><summary style="cursor:pointer;color:rgba(148,163,184,0.95)">Raw error</summary><pre style="min-height:0;margin-top:10px">${escapeHtml(JSON.stringify(x, null, 2))}</pre></details>
          </div>`);
          return;
        }
        appendMsg("assistant", `<div style="color:rgba(226,232,240,0.92)">${escapeHtml((typeof x === "string") ? x : JSON.stringify(x, null, 2))}</div>`);
      }
      function getWindowDays() { return parseInt(document.getElementById("windowDays").value || "42", 10); }

      async function runAction(label, fn){
        setBusy(true, label);
        const pending = appendMsg("assistant", `<div style="color:rgba(148,163,184,0.95)">${escapeHtml(label)}</div>`);
        try {
          const res = await fn();
          if (pending) pending.remove();
          setOut(res);
        }
        catch(e) { setOut(e); }
        finally { setBusy(false); }
      }

      function promptValue(){
        const el = byId("prompt");
        const v = el && typeof el.value === "string" ? el.value : "";
        const t = v.replace(/^\\s+|\\s+$/g, "");
        return t ? t : null;
      }

      function submitInsights(){
        const pv = promptValue();
        if (pv) appendMsg("user", `<div style="color:rgba(226,232,240,0.92)">${escapeHtml(pv)}</div>`);
        return runAction("Insights…", () => call("/insights", { window_days: getWindowDays(), prompt: promptValue() }));
      }

      byId("btnSnapshot").onclick = () => runAction("Snapshot…", () => call("/snapshot", { window_days: getWindowDays() }));
      byId("btnReadiness").onclick = () => runAction("Readiness…", () => call("/readiness", { window_days: getWindowDays() }));
      byId("btnInsights").onclick = () => submitInsights();
      byId("btnStoreInsights").onclick = () => runAction("Storing…", () => call("/insights/store", { window_days: getWindowDays(), prompt: promptValue() }));
      byId("btnGrafana").onclick = () => runAction("Writing dashboard…", () => call("/grafana/write_dashboard_file", {}));
      byId("btnGrafanaPush").onclick = () => runAction("Pushing dashboard…", () => call("/grafana/push_dashboard_api", {}));
      byId("btnDeriveAll").onclick = () => runAction("Deriving metrics…", async () => {
        const res = await call("/activities/derive_all", { window_days: getWindowDays(), limit: 500 });
        return res;
      });
      byId("btnMetrics").onclick = () => {
        const q = byId("prompt").value || "";
        if (q && q.trim()) appendMsg("user", `<div style="color:rgba(226,232,240,0.92)">${escapeHtml(q)}</div>`);
        return runAction("Computing metrics…", () => call("/metrics/query", { window_days: getWindowDays(), query: q }));
      };

      byId("btnClear").onclick = () => { byId("prompt").value = ""; byId("prompt").focus(); };

      // Prompt submit (button + Enter-to-send)
      byId("promptForm").addEventListener("submit", (ev) => {
        ev.preventDefault();
        submitInsights();
      });
      byId("prompt").addEventListener("keydown", (ev) => {
        if (ev.key === "Enter" && !ev.shiftKey) {
          ev.preventDefault();
          submitInsights();
        }
      });

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
    snap = build_snapshot(_ro, window_days=req.window_days)
    score = readiness_score(snap)
    if cfg.anthropic_api_key:
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
    else:
        # Deterministic fallback: still provides useful, formatted output based on agent-calculated metrics.
        m = snap.metrics
        agent_vo2 = (m.get("agent_vo2") or {}) if isinstance(m, dict) else {}

        def _fmt_vo2_block(label: str, block: dict) -> list[str]:
            if not isinstance(block, dict) or not block or not block.get("row_count"):
                return [f"- **{label}**: no agent-derived activities in window (run **Derive metrics from activities** first)"]
            lines = [f"- **{label}**:"]
            if block.get("last_activity_time_utc"):
                lines.append(f"  - **last activity (UTC)**: `{block.get('last_activity_time_utc')}` (id `{block.get('last_activity_id','')}`)")
            if block.get("vo2_demand_best5m_last") is not None:
                lines.append(f"  - **VO₂ demand (best 5-min) last**: {block['vo2_demand_best5m_last']:.1f} ml/kg/min")
            if block.get("vo2max_est_last") is not None:
                lines.append(f"  - **VO₂max estimate last**: {block['vo2max_est_last']:.1f} ml/kg/min")
            if block.get("vo2max_est_mean") is not None:
                lines.append(f"  - **VO₂max estimate mean (window)**: {block['vo2max_est_mean']:.1f} ml/kg/min")
            if block.get("vo2max_est_best") is not None:
                lines.append(f"  - **VO₂max estimate best (window)**: {block['vo2max_est_best']:.1f} ml/kg/min")
            return lines

        run_block = agent_vo2.get("running") if isinstance(agent_vo2, dict) else {}
        cyc_block = agent_vo2.get("cycling") if isinstance(agent_vo2, dict) else {}
        methods = agent_vo2.get("methods") if isinstance(agent_vo2, dict) else {}

        lines: list[str] = []
        lines.append(f"## Readiness Score: {score.get('score', 'n/a')}/100")
        if req.prompt:
            lines.append("")
            lines.append("### Your question/context")
            lines.append(f"- {req.prompt.strip()}")
        lines.append("")
        lines.append("### Summary")
        for r in (score.get("reasons") or [])[:6]:
            lines.append(f"- {r}")
        if not (score.get("reasons") or []):
            lines.append("- No strong drivers detected from available signals.")
        lines.append("")
        lines.append("### Agent-calculated VO₂ / VO₂max (from raw activity streams)")
        lines.extend(_fmt_vo2_block("Running", run_block or {}))
        lines.extend(_fmt_vo2_block("Cycling", cyc_block or {}))
        if isinstance(methods, dict) and methods:
            lines.append("")
            lines.append("### Methods used (high level)")
            for k in ["running_vo2_demand", "running_vo2max_est", "cycling_vo2_demand", "cycling_vo2max_est"]:
                if methods.get(k):
                    lines.append(f"- **{k}**: {methods[k]}")
        lines.append("")
        lines.append("### Caveats")
        lines.append("- VO₂max estimates depend on having a reasonable HRmax and good-quality stream data (speed/altitude or power).")
        lines.append("- This is **agent-calculated** from raw streams; it does **not** use Garmin’s VO₂max device estimate.")

        insights_obj = "\n".join(lines).strip()
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
        raise HTTPException(
            status_code=400,
            detail="AI_ALLOW_DB_WRITE is false; refusing to write to DB. Set AI_ALLOW_DB_WRITE=true in .env and restart the stack to enable Derive metrics.",
        )
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


@app.get("/metrics/catalog")
def metrics_catalog(authorization: str | None = Header(default=None)):
    _require_auth(authorization)
    return {"metrics": metrics_engine.catalog()}


@app.post("/metrics/compute")
def metrics_compute(req: MetricsComputeRequest, authorization: str | None = Header(default=None)):
    _require_auth(authorization)
    computed = metrics_engine.compute_metrics(
        _ro,
        metrics=req.metrics,
        window_days=req.window_days,
        activity_limit=req.activity_limit,
        cycling_gross_eff=cfg.cycling_gross_efficiency,
    )
    md = metrics_engine.render_markdown(computed)
    return {"ok": True, "computed": computed, "insights": md}


@app.post("/metrics/query")
def metrics_query(req: MetricsQueryRequest, authorization: str | None = Header(default=None)):
    """
    Natural language convenience endpoint.
    - If Anthropic is configured: use it to select metric names from the catalog.
    - Otherwise: simple keyword matching fallback.
    """
    _require_auth(authorization)
    q = (req.query or "").strip().lower()
    cat = metrics_engine.catalog()
    names = [m["name"] for m in cat]

    picked: list[str] = []
    forced = False
    # Keyword fallback (works offline)
    if ("vo2" in q or "v02" in q) and ("cycle" in q or "bike" in q or "ride" in q or "cycling" in q):
        picked.append("vo2_window_ride" if ("30" in q or "days" in q or "window" in q or "weeks" in q) else "vo2_last_ride")
    if ("vo2" in q or "v02" in q) and ("run" in q or "running" in q or "pace" in q):
        picked.append("vo2_window_run" if ("30" in q or "days" in q or "window" in q or "weeks" in q) else "vo2_last_run")
    if "power curve" in q or ("power" in q and "curve" in q):
        picked.append("power_curve_window_ride" if ("30" in q or "days" in q or "window" in q or "weeks" in q) else "power_curve_last_ride")
    if "ftp" in q or "threshold power" in q or "cp" in q or "critical power" in q:
        picked.append("ftp_est_window_ride" if ("30" in q or "days" in q or "window" in q or "weeks" in q) else "ftp_est_last_ride")
    if "lthr" in q or "threshold hr" in q or "lactate threshold" in q:
        if "cycle" in q or "bike" in q or "ride" in q or "cycling" in q:
            picked.append("lthr_window_ride" if ("30" in q or "days" in q or "window" in q or "weeks" in q) else "lthr_last_ride")
        else:
            picked.append("lthr_window_run" if ("30" in q or "days" in q or "window" in q or "weeks" in q) else "lthr_last_run")
    if "threshold pace" in q or "critical speed" in q or ("threshold" in q and ("pace" in q or "run" in q or "running" in q)):
        picked.append("threshold_window_run")
    if "trimp" in q:
        if "all" in q or "combined" in q:
            picked.append("trimp_window_all")
        elif "cycle" in q or "bike" in q or "ride" in q or "cycling" in q:
            picked.append("trimp_last_ride")
        else:
            picked.append("trimp_last_run")
    if "tss" in q:
        picked.append("tss_window_ride" if ("30" in q or "days" in q or "window" in q or "weeks" in q) else "tss_last_ride")
    if ("p95" in q or "95th" in q or "percentile" in q) and ("hr" in q or "heart rate" in q):
        picked.append("hr_p95_window_all")
        forced = True
    if ("all" in q or "combined" in q) and ("vo2" in q or "v02" in q):
        picked = ["vo2_window_all" if ("30" in q or "days" in q or "window" in q or "weeks" in q) else "vo2_window_all"]
    if ("all" in q or "combined" in q) and ("lthr" in q or "threshold hr" in q or "lactate threshold" in q):
        picked = ["lthr_window_all" if ("30" in q or "days" in q or "window" in q or "weeks" in q) else "lthr_window_all"]
    if ("all" in q or "combined" in q) and ("threshold" in q or "ftp" in q or "critical speed" in q or "cp" in q):
        picked = ["threshold_window_all"]

    # If Anthropic exists, let it choose from the catalog (but keep it safe: only allow names we know)
    # Do not override "forced" keyword picks (e.g. HR percentiles).
    if cfg.anthropic_api_key and not forced:
        try:
            client = create_client(
                LLMConfig(
                    api_key=cfg.anthropic_api_key,
                    analysis_model=cfg.analysis_model,
                    planning_model=cfg.planning_model,
                )
            )
            system = (
                "You map a user's metric request to a small set of metric function names from a provided catalog. "
                "Return ONLY a comma-separated list of metric names. No extra text."
            )
            prompt = (
                "Catalog metric names:\n"
                + "\n".join(f"- {n}" for n in names)
                + "\n\nUser request:\n"
                + req.query
                + "\n\nReturn metric names (comma-separated)."
            )
            msg = client.messages.create(
                model=cfg.analysis_model,
                max_tokens=80,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )
            parts: list[str] = []
            for c in msg.content:
                if getattr(c, "type", None) == "text":
                    parts.append(c.text)
            raw = "\n".join(parts).strip()
            llm_picked = [x.strip() for x in raw.split(",") if x.strip()]
            llm_picked = [x for x in llm_picked if x in names]
            if llm_picked:
                picked = llm_picked
        except Exception:
            # fall back to keyword picks
            pass

    if not picked:
        return {
            "ok": False,
            "detail": "Could not map query to known metrics. Try /metrics/catalog or include keywords like VO2, FTP, power curve, LTHR.",
            "known_metrics": names,
        }

    # Pass activity_limit via ctx by encoding it into window_days-only compute and letting engine read defaults
    computed = metrics_engine.compute_metrics(
        _ro,
        metrics=picked,
        window_days=req.window_days,
        activity_limit=req.activity_limit,
        cycling_gross_eff=cfg.cycling_gross_efficiency,
    )
    md = metrics_engine.render_markdown(computed)
    return {"ok": True, "picked": picked, "computed": computed, "insights": md}


def main() -> None:
    import uvicorn

    uvicorn.run(app, host=cfg.host, port=cfg.port, log_level="info")


if __name__ == "__main__":
    main()

