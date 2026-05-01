from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .config import load_config
from .conversation_store import (
    ConversationState,
    delete_conversation,
    load_conversation,
    sanitize_session_id,
    save_conversation,
)
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
    session_id: str | None = Field(
        default=None,
        description="Client-generated id for multi-turn chat; conversations persist under AGENT_MEMORY_DIR when set",
    )
    reset_conversation: bool = Field(
        default=False,
        description="If true, discard stored turns for this session_id before processing",
    )


def _conversation_path_for_session(session_id: str) -> Path:
    return Path(cfg.memory_dir or "") / f"{session_id}.json"


class DeriveActivitiesRequest(BaseModel):
    window_days: int = Field(42, ge=1, le=365)
    limit: int = Field(250, ge=1, le=2000, description="Max activities to process")


class ForceReloginResponse(BaseModel):
    ok: bool
    token_dir: str
    removed_paths: int
    note: str


class GarminAuthStatusResponse(BaseModel):
    ok: bool
    token_dir: str
    token_files: list[dict]
    file_count: int
    most_recent_mtime_utc: str | None
    noninteractive_credentials_configured: bool
    note: str
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
<html lang="en">
  <head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>AI Training Agent</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Outfit:wght@500;600;700&display=swap" rel="stylesheet">
    <style>
      :root {
        --bg-color: #02040a;
        --panel-bg: rgba(13, 17, 28, 0.65);
        --panel-border: rgba(255, 255, 255, 0.08);
        --text-primary: #f8fafc;
        --text-secondary: #94a3b8;
        --accent: #6366f1;
        --accent-hover: #4f46e5;
        --accent-transparent: rgba(99, 102, 241, 0.15);
        --success: #10b981;
        --warning: #f59e0b;
        --danger: #ef4444;
        --user-bubble: rgba(99, 102, 241, 0.25);
        --assistant-bubble: rgba(30, 41, 59, 0.7);
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        height: 100vh;
        color: var(--text-primary);
        font-family: 'Inter', system-ui, sans-serif;
        background-color: var(--bg-color);
        background-image: 
          url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 1000 1000'%3E%3Cdefs%3E%3Cfilter id='m' x='-20%25' y='-20%25' width='140%25' height='140%25'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.012' numOctaves='5' seed='11' stitchTiles='stitch' result='noise'/%3E%3CfeColorMatrix in='noise' type='matrix' values='0.333 0.333 0.333 0 0 0.333 0.333 0.333 0 0 0.333 0.333 0.333 0 0 0 0 0 1 0' result='mono'/%3E%3CfeComponentTransfer in='mono' result='thr'%3E%3CfeFuncR type='gamma' amplitude='1' exponent='2.35' offset='-0.18'/%3E%3CfeFuncG type='gamma' amplitude='1' exponent='2.35' offset='-0.18'/%3E%3CfeFuncB type='gamma' amplitude='1' exponent='2.35' offset='-0.18'/%3E%3C/feComponentTransfer%3E%3CfeGaussianBlur in='thr' stdDeviation='0.7' result='soft'/%3E%3CfeComponentTransfer in='soft'%3E%3CfeFuncA type='table' tableValues='0 0 0.01 0.03 0.06 0.10 0.16 0.26 0.40 0.60 1'/%3E%3C/feComponentTransfer%3E%3C/filter%3E%3ClinearGradient id='g' x1='0' y1='0' x2='0' y2='1'%3E%3Cstop offset='0%25' stop-color='%23050714'/%3E%3Cstop offset='55%25' stop-color='%2302040a'/%3E%3Cstop offset='100%25' stop-color='%23000000'/%3E%3C/linearGradient%3E%3C/defs%3E%3Crect width='1000' height='1000' fill='url(%23g)'/%3E%3Crect width='1000' height='1000' filter='url(%23m)' opacity='0.55' fill='%236366f1'/%3E%3C/svg%3E"),
          radial-gradient(1200px 900px at 50% 15%, rgba(99,102,241,0.06), rgba(0,0,0,0) 55%);
        background-size: cover;
        background-attachment: fixed;
        display: flex;
        overflow: hidden;
      }
      
      /* Layout structure */
      .app-container {
        display: flex;
        width: 100%;
        height: 100%;
      }
      
      .sidebar {
        width: 360px;
        flex-shrink: 0;
        background: var(--panel-bg);
        border-right: 1px solid var(--panel-border);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        display: flex;
        flex-direction: column;
        height: 100%;
        z-index: 10;
        box-shadow: 4px 0 24px rgba(0,0,0,0.2);
      }
      
      .main-content {
        flex-grow: 1;
        display: flex;
        flex-direction: column;
        height: 100%;
        position: relative;
        min-width: 0;
      }
      
      /* Sidebar styling */
      .sidebar-header {
        padding: 24px 20px;
        border-bottom: 1px solid var(--panel-border);
      }
      
      h1 {
        font-family: 'Outfit', sans-serif;
        font-size: 22px;
        margin: 0;
        font-weight: 700;
        color: var(--text-primary);
        display: flex;
        align-items: center;
        gap: 8px;
        letter-spacing: 0.3px;
      }
      
      .badge {
        font-family: 'Inter', sans-serif;
        font-size: 10px;
        padding: 3px 8px;
        border-radius: 12px;
        border: 1px solid rgba(99, 102, 241, 0.4);
        background: rgba(99, 102, 241, 0.1);
        color: #818cf8;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        font-weight: 600;
      }
      
      .status-indicator {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 13px;
        color: var(--text-secondary);
        margin-top: 12px;
        font-weight: 500;
      }
      
      .dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: var(--success);
        box-shadow: 0 0 10px rgba(16,185,129,0.4);
        transition: all 0.3s ease;
      }
      .dot.busy {
        background: var(--warning);
        box-shadow: 0 0 10px rgba(245,158,11,0.4);
        animation: pulse 1.5s infinite;
      }
      @keyframes pulse {
        0% { opacity: 0.5; transform: scale(0.9); }
        50% { opacity: 1; transform: scale(1.2); }
        100% { opacity: 0.5; transform: scale(0.9); }
      }
      
      .sidebar-scroll {
        flex-grow: 1;
        overflow-y: auto;
        padding: 20px;
        display: flex;
        flex-direction: column;
        gap: 24px;
      }
      
      .sidebar-scroll::-webkit-scrollbar { width: 6px; }
      .sidebar-scroll::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.1); border-radius: 3px; }
      
      .section-title {
        font-family: 'Outfit', sans-serif;
        font-size: 11px;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        color: var(--text-secondary);
        margin: 0 0 12px;
        font-weight: 600;
      }
      
      /* Form controls */
      .form-group {
        display: flex;
        flex-direction: column;
        gap: 8px;
        margin-bottom: 16px;
      }
      
      label {
        font-size: 12px;
        color: var(--text-secondary);
        font-weight: 500;
      }
      
      input, textarea {
        width: 100%;
        padding: 10px 14px;
        border-radius: 10px;
        border: 1px solid var(--panel-border);
        background: rgba(0, 0, 0, 0.3);
        color: var(--text-primary);
        font-family: 'Inter', sans-serif;
        font-size: 14px;
        outline: none;
        transition: all 0.2s ease;
      }
      
      input:focus, textarea:focus {
        border-color: rgba(99, 102, 241, 0.6);
        box-shadow: 0 0 0 3px var(--accent-transparent);
        background: rgba(0, 0, 0, 0.5);
      }
      
      /* Buttons */
      .action-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 10px;
      }
      
      .action-grid.single {
        grid-template-columns: 1fr;
      }
      
      button {
        padding: 10px 14px;
        border-radius: 10px;
        border: 1px solid rgba(255,255,255,0.1);
        background: rgba(255, 255, 255, 0.04);
        color: var(--text-primary);
        font-family: 'Inter', sans-serif;
        font-size: 13px;
        font-weight: 500;
        cursor: pointer;
        transition: all 0.2s ease;
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 8px;
        white-space: nowrap;
      }
      
      button:hover:not(:disabled) {
        background: rgba(255, 255, 255, 0.08);
        border-color: rgba(255, 255, 255, 0.2);
        transform: translateY(-1px);
      }
      
      button:active:not(:disabled) {
        transform: translateY(1px);
      }
      
      button.primary {
        background: linear-gradient(180deg, var(--accent), var(--accent-hover));
        border-color: var(--accent);
        color: white;
        font-weight: 600;
        box-shadow: 0 4px 12px rgba(99, 102, 241, 0.25);
      }
      
      button.primary:hover:not(:disabled) {
        background: linear-gradient(180deg, #7174f3, var(--accent));
        box-shadow: 0 6px 16px rgba(99, 102, 241, 0.4);
      }
      
      button:disabled {
        opacity: 0.5;
        cursor: not-allowed;
        transform: none !important;
      }
      
      /* Main Content Area */
      .chat-header {
        padding: 14px 24px;
        border-bottom: 1px solid var(--panel-border);
        background: rgba(13, 17, 28, 0.4);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        z-index: 5;
        display: flex;
        justify-content: space-between;
        align-items: center;
      }
      
      .tabs {
        display: flex;
        gap: 6px;
      }
      
      .tab {
        padding: 6px 16px;
        border-radius: 20px;
        font-size: 13px;
        font-weight: 500;
        color: var(--text-secondary);
        cursor: pointer;
        transition: all 0.2s;
      }
      
      .tab:hover {
        color: var(--text-primary);
        background: rgba(255, 255, 255, 0.05);
      }
      
      .tab.active {
        color: var(--text-primary);
        background: rgba(255, 255, 255, 0.1);
      }
      
      .chat-container {
        flex-grow: 1;
        overflow-y: auto;
        padding: 32px 24px;
        display: flex;
        flex-direction: column;
        gap: 20px;
        scroll-behavior: smooth;
      }
      
      .chat-container::-webkit-scrollbar { width: 8px; }
      .chat-container::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.1); border-radius: 4px; }
      
      /* Message bubbles */
      .msg-wrapper {
        display: flex;
        width: 100%;
        margin-bottom: 8px;
      }
      
      .msg-wrapper.user { justify-content: flex-end; }
      .msg-wrapper.assistant { justify-content: flex-start; }
      
      .msg {
        max-width: 85%;
        padding: 16px 20px;
        border-radius: 18px;
        line-height: 1.6;
        font-size: 14.5px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.15);
        position: relative;
        animation: slideIn 0.3s ease-out forwards;
        overflow-wrap: anywhere;
      }
      
      @keyframes slideIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
      }
      
      .msg.user {
        background: var(--user-bubble);
        border: 1px solid rgba(99, 102, 241, 0.3);
        border-bottom-right-radius: 4px;
        color: #fff;
      }
      
      .msg.assistant {
        background: var(--assistant-bubble);
        border: 1px solid var(--panel-border);
        border-bottom-left-radius: 4px;
        backdrop-filter: blur(8px);
      }
      
      .msg-author {
        font-family: 'Outfit', sans-serif;
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-bottom: 10px;
        opacity: 0.75;
        display: flex;
        align-items: center;
        gap: 6px;
      }
      
      /* Markdown formatting */
      .msg p { margin: 0 0 12px 0; }
      .msg p:last-child { margin-bottom: 0; }
      .msg ul { margin: 0 0 12px 0; padding-left: 20px; }
      .msg li { margin-bottom: 6px; }
      .msg strong { font-weight: 600; color: #fff; }
      
      .msg code {
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
        font-size: 0.85em;
        background: rgba(0, 0, 0, 0.4);
        padding: 3px 6px;
        border-radius: 6px;
        border: 1px solid rgba(255,255,255,0.08);
      }
      
      .msg pre {
        background: rgba(0, 0, 0, 0.5);
        padding: 16px;
        border-radius: 12px;
        overflow-x: auto;
        border: 1px solid rgba(255,255,255,0.08);
        margin: 12px 0;
      }
      
      .msg pre code {
        background: transparent;
        padding: 0;
        border: none;
        font-size: 13px;
      }
      
      /* Custom Output Cards */
      .output-card {
        background: rgba(15, 23, 42, 0.6);
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 14px;
        padding: 20px;
        margin-top: 12px;
        box-shadow: 0 8px 30px rgba(0,0,0,0.25);
      }
      
      .output-card-title {
        font-family: 'Outfit', sans-serif;
        font-size: 14px;
        font-weight: 600;
        color: var(--accent);
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 16px;
        border-bottom: 1px solid rgba(255,255,255,0.06);
        padding-bottom: 12px;
      }
      
      .score-display {
        font-family: 'Outfit', sans-serif;
        font-size: 48px;
        font-weight: 700;
        line-height: 1;
        margin: 16px 0;
        background: linear-gradient(135deg, #34d399, #10b981);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        letter-spacing: -1px;
      }
      
      details {
        margin-top: 12px;
        background: rgba(0,0,0,0.2);
        border-radius: 8px;
        padding: 8px 12px;
        border: 1px solid rgba(255,255,255,0.05);
      }
      
      details summary {
        cursor: pointer;
        color: var(--text-secondary);
        font-size: 13px;
        font-weight: 500;
        user-select: none;
        outline: none;
        display: flex;
        align-items: center;
      }
      
      details summary:hover { color: var(--text-primary); }
      
      .banner {
        display: flex;
        gap: 12px;
        padding: 12px 16px;
        background: rgba(245, 158, 11, 0.08);
        border: 1px solid rgba(245, 158, 11, 0.2);
        border-radius: 12px;
        margin-bottom: 24px;
        font-size: 13px;
        color: rgba(253, 230, 138, 0.95);
        line-height: 1.5;
      }
      
      /* Input Area Fixed to Bottom */
      .input-area {
        padding: 20px 24px;
        background: rgba(13, 17, 28, 0.85);
        border-top: 1px solid var(--panel-border);
        backdrop-filter: blur(20px);
        -webkit-backdrop-filter: blur(20px);
        z-index: 10;
      }
      
      .input-wrapper {
        position: relative;
        max-width: 900px;
        margin: 0 auto;
      }
      
      .input-wrapper textarea {
        padding: 16px 60px 16px 20px;
        min-height: 56px;
        height: 56px;
        border-radius: 28px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.25);
        background: rgba(15, 23, 42, 0.9);
        resize: none;
        overflow: hidden;
      }
      
      .input-wrapper textarea:focus {
        height: 100px;
        border-radius: 20px;
      }
      
      .send-btn {
        position: absolute;
        right: 8px;
        bottom: 8px;
        width: 40px;
        height: 40px;
        border-radius: 50%;
        padding: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        background: var(--accent);
        border: none;
        color: white;
        transition: all 0.2s;
      }
      
      .send-btn:hover:not(:disabled) {
        background: var(--accent-hover);
        transform: scale(1.05);
        box-shadow: 0 0 12px rgba(99,102,241,0.5);
      }
      
      .send-icon { width: 18px; height: 18px; fill: currentColor; margin-left: 2px; }
      
      .hint {
        text-align: center;
        margin-top: 10px;
        font-size: 11px;
        color: var(--text-secondary);
      }
      
      /* Responsive */
      @media (max-width: 768px) {
        .app-container { flex-direction: column; }
        .sidebar { width: 100%; height: auto; max-height: 50vh; border-right: none; border-bottom: 1px solid var(--panel-border); }
        .msg { max-width: 95%; }
        .chat-container { padding: 20px 16px; }
        .input-area { padding: 16px; }
      }
    </style>
  </head>
  <body>
    <div class="app-container">
      <!-- Sidebar -->
      <div class="sidebar">
        <div class="sidebar-header">
          <h1>
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="color:var(--accent)"><path d="M12 2v20M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>
            AI Training Agent
            <span class="badge">Beta</span>
          </h1>
          <div class="status-indicator">
            <span class="dot" id="statusDot"></span>
            <span id="statusText">Ready</span>
          </div>
        </div>
        
        <div class="sidebar-scroll">
          <div class="banner">
            <div>⚠️</div>
            <div><strong>Beta:</strong> tool calls and schema are subject to change. Output may be incomplete.</div>
          </div>
          
          <div>
            <h2 class="section-title">Configuration</h2>
            <div class="form-group">
              <label>Window (days)</label>
              <input id="windowDays" type="number" min="1" max="365" value="42"/>
            </div>
            <div class="form-group">
              <label>Auth token (optional)</label>
              <input id="authToken" type="password" placeholder="Bearer token"/>
            </div>
          </div>
          
          <div>
            <h2 class="section-title">Core Actions</h2>
            <div class="action-grid single">
              <button class="primary" id="btnInsights">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path></svg>
                Generate Insights
              </button>
            </div>
            <div class="action-grid" style="margin-top:10px;">
              <button id="btnReadiness">Readiness</button>
              <button id="btnSnapshot">Snapshot</button>
            </div>
          </div>
          
          <div>
            <h2 class="section-title">Data Management</h2>
            <div class="action-grid">
              <button id="btnDeriveAll">Derive Metrics</button>
              <button id="btnMetrics">Compute Query</button>
              <button id="btnForceRelogin">Force Re-login</button>
              <button id="btnGarminAuthStatus">Auth Status</button>
            </div>
          </div>
          
          <div>
            <h2 class="section-title">Grafana Integration</h2>
            <div class="action-grid">
              <button id="btnGrafana">Write Dashboard</button>
              <button id="btnGrafanaPush">Push API</button>
            </div>
            <div class="action-grid single" style="margin-top:10px;">
              <button id="btnStoreInsights">Store Insights to DB</button>
            </div>
          </div>
        </div>
      </div>
      
      <!-- Main Content -->
      <div class="main-content">
        <div class="chat-header">
          <div class="tabs">
            <div class="tab active" id="tabOutput">Chat & Output</div>
            <div class="tab" id="tabActivity">Activity Log</div>
            <div class="tab" id="tabMetrics">Metrics Data</div>
          </div>
          <button id="btnClear" style="padding: 6px 12px; font-size: 12px; border-radius: 8px;">Clear Chat</button>
        </div>
        
        <div class="chat-container" id="chat">
          <div class="msg-wrapper assistant">
            <div class="msg assistant">
              <div class="msg-author">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2a10 10 0 1 0 10 10H12V2z"/><path d="M12 12 2.1 7.1"/><path d="M12 12l9.9 4.9"/></svg>
                Assistant
              </div>
              <div>Hello! I'm your AI Training Agent. Ask me a question about your fitness data or use the actions in the sidebar to get started.</div>
            </div>
          </div>
        </div>
        
        <div class="input-area">
          <form id="promptForm" class="input-wrapper">
            <textarea id="prompt" placeholder="Message the AI Training Agent (e.g., I feel a bit flat today; race in 8 weeks...)"></textarea>
            <button class="send-btn" id="btnSubmitPrompt" type="submit" title="Send message">
              <svg class="send-icon" viewBox="0 0 24 24"><path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/></svg>
            </button>
          </form>
          <div class="hint">Tip: press <strong>Enter</strong> to send, <strong>Shift+Enter</strong> for a newline.</div>
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
        
        const wrapper = document.createElement("div");
        wrapper.className = "msg-wrapper " + role;
        
        const msgEl = document.createElement("div");
        msgEl.className = "msg " + role;
        
        const who = role === "user" ? "You" : "Assistant";
        const icon = role === "user" 
            ? '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>'
            : '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2a10 10 0 1 0 10 10H12V2z"/><path d="M12 12 2.1 7.1"/><path d="M12 12l9.9 4.9"/></svg>';
            
        msgEl.innerHTML = `<div class="msg-author">${icon} ${who}</div><div>${html}</div>`;
        
        wrapper.appendChild(msgEl);
        c.appendChild(wrapper);
        scrollChatToBottom();
        return wrapper;
      }

      function safeSetOut(msg){
        try { setOut(msg); }
        catch (e) {
          appendMsg("assistant", `<div>${escapeHtml(String(msg))}</div>`);
        }
      }

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
        for (const id of ["btnInsights","btnSnapshot","btnReadiness","btnDeriveAll","btnForceRelogin","btnGarminAuthStatus","btnMetrics","btnStoreInsights","btnGrafana","btnGrafanaPush","btnSubmitPrompt","btnClear"]) {
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
      function agentSessionId(){
        try {
          let id = localStorage.getItem("garmin_agent_session_id");
          if (!id) {
            id = (crypto.randomUUID && crypto.randomUUID()) || ("" + Date.now() + "-" + Math.random());
            localStorage.setItem("garmin_agent_session_id", id);
          }
          return id;
        } catch (e) {
          return null;
        }
      }
      function resetAgentSession(){
        try { localStorage.removeItem("garmin_agent_session_id"); } catch (e) {}
        return agentSessionId();
      }
      async function call(path, body) {
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
        const esc = escapeHtml;
        const lines = String((md === null || md === undefined) ? "" : md).split("\\n");
        const out = [];
        let inList = false;
        const flushList = () => { if(inList){ out.push("</ul>"); inList=false; } };
        for (let ln of lines){
          const li = ln.match(/^\\s*[-*]\\s+(.*)$/);
          if (li){
            if(!inList){ out.push("<ul>"); inList=true; }
            let t = esc(li[1]);
            t = t.replace(/\\*\\*(.+?)\\*\\*/g, "<strong>$1</strong>");
            t = t.replace(/`([^`]+)`/g, "<code>$1</code>");
            out.push(`<li>${t}</li>`);
            continue;
          }
          flushList();
          if (!ln.trim()) { out.push("<div style=\\"height:8px\\"></div>"); continue; }
          let t = esc(ln);
          t = t.replace(/\\*\\*(.+?)\\*\\*/g, "<strong>$1</strong>");
          t = t.replace(/`([^`]+)`/g, "<code>$1</code>");
          out.push(`<p>${t}</p>`);
        }
        flushList();
        return out.join("");
      }

      function renderInsights(md){
        if (!md || typeof md !== "string") return null;
        return `<div>${mdToHtml(md)}</div>`;
      }

      function renderReadiness(x){
        const r = (x && x.readiness) ? x.readiness : null;
        const snap = (x && x.snapshot) ? x.snapshot : null;
        if (!r || typeof r !== "object") return null;
        const score = (r.score !== undefined && r.score !== null) ? r.score
          : ((r.readiness_score !== undefined && r.readiness_score !== null) ? r.readiness_score : r.value);
        const reasons = Array.isArray(r.reasons) ? r.reasons : [];
        const inputs = (r.inputs && typeof r.inputs === "object") ? r.inputs : null;
        const scoreLine = (score === undefined || score === null) ? "" : `<div class="score-display">${escapeHtml(score)}</div>`;
        const reasonsHtml = reasons.length ? `<div>${mdToHtml(reasons.map(v => `- ${v}`).join("\\n"))}</div>` : "";
        const inputsHtml = inputs ? `<details><summary><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="margin-right:6px"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg> Inputs used</summary><pre><code>${escapeHtml(JSON.stringify(inputs, null, 2))}</code></pre></details>` : "";
        const snapHtml = snap ? `<details><summary><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="margin-right:6px"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg> Snapshot (raw)</summary><pre><code>${escapeHtml(JSON.stringify(snap, null, 2))}</code></pre></details>` : "";
        return `<div class="output-card">
          <div class="output-card-title">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>
            Readiness Assessment
          </div>
          ${scoreLine}
          ${reasonsHtml}
          ${inputsHtml}
          ${snapHtml}
        </div>`;
      }

      function renderSnapshot(x){
        const snap = (x && typeof x === "object" && !Array.isArray(x)) ? x : null;
        if (!snap) return null;
        const debug = snap.debug && typeof snap.debug === "object" ? snap.debug : null;
        const available = debug && debug.available_signals ? debug.available_signals : null;
        const availHtml = Array.isArray(available) && available.length
          ? `<div style="margin-top:12px">${mdToHtml(["**Available signals:**", ...available.map(s => "- " + s)].join("\\n"))}</div>`
          : "";
        return `<div class="output-card">
          <div class="output-card-title">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"/><circle cx="12" cy="13" r="4"/></svg>
            Data Snapshot
          </div>
          ${availHtml}
          <details><summary><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="margin-right:6px"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg> Raw snapshot details</summary><pre><code>${escapeHtml(JSON.stringify(snap, null, 2))}</code></pre></details>
        </div>`;
      }

      function setOut(x) {
        const insightsMd = (x && typeof x.insights === "string") ? x.insights : null;
        const insightsCards = renderInsights(insightsMd);
        const readinessCard = renderReadiness(x);
        const snapshotCard = (!insightsCards && !readinessCard) ? renderSnapshot(x) : null;
        const errDetail = (x && typeof x === "object") ? (x.detail || x.error || x.message) : null;

        if (insightsCards) {
          appendMsg("assistant", insightsCards + `<details><summary><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="margin-right:6px"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg> Raw response</summary><pre><code>${escapeHtml(JSON.stringify(x, null, 2))}</code></pre></details>`);
          return;
        }
        if (readinessCard) {
          appendMsg("assistant", readinessCard + `<details><summary><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="margin-right:6px"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg> Raw response</summary><pre><code>${escapeHtml(JSON.stringify(x, null, 2))}</code></pre></details>`);
          return;
        }
        if (snapshotCard) {
          appendMsg("assistant", snapshotCard);
          return;
        }
        if (errDetail) {
          appendMsg("assistant", `<div class="output-card" style="border-color: rgba(239, 68, 68, 0.4); background: rgba(239, 68, 68, 0.1);">
            <div class="output-card-title" style="color: var(--danger); border-color: rgba(239, 68, 68, 0.2);">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
              Error Encountered
            </div>
            ${mdToHtml(`- **Detail**: ${String(errDetail)}`)}
            <details><summary><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="margin-right:6px"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg> Raw error trace</summary><pre><code>${escapeHtml(JSON.stringify(x, null, 2))}</code></pre></details>
          </div>`);
          return;
        }
        appendMsg("assistant", `<pre><code>${escapeHtml((typeof x === "string") ? x : JSON.stringify(x, null, 2))}</code></pre>`);
      }
      
      function getWindowDays() { return parseInt(document.getElementById("windowDays").value || "42", 10); }

      async function runAction(label, fn){
        setBusy(true, label);
        const pending = appendMsg("assistant", `<div style="color:var(--text-secondary); display:flex; align-items:center; gap:8px;"><span class="dot busy" style="position:static"></span> ${escapeHtml(label)}</div>`);
        try {
          const res = await fn();
          if (pending) pending.remove();
          setOut(res);
        }
        catch(e) { 
          if (pending) pending.remove();
          setOut(e); 
        }
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
        if (pv) {
            appendMsg("user", `<p>${escapeHtml(pv)}</p>`);
            byId("prompt").value = "";
            byId("prompt").style.height = "56px";
        }
        const sid = agentSessionId();
        return runAction("Analyzing data and generating insights...", () => call("/insights", { window_days: getWindowDays(), prompt: pv, session_id: sid }));
      }

      byId("btnSnapshot").onclick = () => runAction("Generating snapshot...", () => call("/snapshot", { window_days: getWindowDays() }));
      byId("btnReadiness").onclick = () => runAction("Calculating readiness...", () => call("/readiness", { window_days: getWindowDays() }));
      byId("btnInsights").onclick = () => submitInsights();
      byId("btnStoreInsights").onclick = () => runAction("Storing insights...", () => call("/insights/store", { window_days: getWindowDays(), prompt: promptValue(), session_id: agentSessionId() }));
      byId("btnGrafana").onclick = () => runAction("Writing dashboard...", () => call("/grafana/write_dashboard_file", {}));
      byId("btnGrafanaPush").onclick = () => runAction("Pushing dashboard...", () => call("/grafana/push_dashboard_api", {}));
      byId("btnDeriveAll").onclick = () => runAction("Deriving metrics...", async () => {
        const res = await call("/activities/derive_all", { window_days: getWindowDays(), limit: 500 });
        return res;
      });
      byId("btnForceRelogin").onclick = () => runAction("Forcing Garmin re-login...", async () => {
        const res = await call("/garmin/force_relogin", {});
        return res;
      });
      byId("btnGarminAuthStatus").onclick = () => runAction("Checking Garmin auth...", async () => {
        const res = await call("/garmin/auth_status", {});
        return res;
      });
      byId("btnMetrics").onclick = () => {
        const q = byId("prompt").value || "";
        if (q && q.trim()) {
            appendMsg("user", `<p>${escapeHtml(q)}</p>`);
            byId("prompt").value = "";
            byId("prompt").style.height = "56px";
        }
        return runAction("Computing metrics...", () => call("/metrics/query", { window_days: getWindowDays(), query: q }));
      };

      byId("btnClear").onclick = () => { 
        resetAgentSession();
        byId("chat").innerHTML = `
          <div class="msg-wrapper assistant">
            <div class="msg assistant">
              <div class="msg-author">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2a10 10 0 1 0 10 10H12V2z"/><path d="M12 12 2.1 7.1"/><path d="M12 12l9.9 4.9"/></svg>
                Assistant
              </div>
              <div>Chat cleared. Ask me a question about your fitness data or use the actions in the sidebar.</div>
            </div>
          </div>
        `;
      };

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
      
      byId("prompt").addEventListener("input", function() {
        this.style.height = '56px';
        this.style.height = (this.scrollHeight) + 'px';
        if (this.scrollHeight > 150) {
            this.style.overflowY = 'auto';
        } else {
            this.style.overflowY = 'hidden';
        }
      });

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
    snap_dict = snap.to_dict()

    sid = sanitize_session_id(req.session_id)
    conv_path: Path | None = None
    conv_state: ConversationState | None = None
    if sid and cfg.memory_dir:
        conv_path = _conversation_path_for_session(sid)
        if req.reset_conversation:
            delete_conversation(conv_path)
        loaded = load_conversation(conv_path)
        if loaded and loaded.window_days == req.window_days:
            conv_state = loaded
        else:
            conv_state = ConversationState(window_days=req.window_days, turns=[])

    api_prior: list[dict[str, str]] = []
    if conv_state and conv_state.turns:
        for t in conv_state.turns:
            u = (t.get("user") or "").strip()
            a = (t.get("assistant") or "").strip()
            if not u:
                continue
            api_prior.append({"role": "user", "content": u[:8000]})
            if a:
                api_prior.append({"role": "assistant", "content": a[:12000]})

    # On-demand computations from raw streams for specific questions (no pre-derived tables required).
    prompt_l = (req.prompt or "").strip().lower()

    def _inject_on_demand(metric_names: list[str]) -> None:
        if not metric_names:
            return
        try:
            computed = metrics_engine.compute_metrics(
                _ro,
                metrics=metric_names,
                window_days=req.window_days,
                activity_limit=20,
                cycling_gross_eff=cfg.cycling_gross_efficiency,
            )
            if isinstance(snap_dict.get("metrics"), dict):
                snap_dict["metrics"].setdefault("on_demand_metrics", {})
                for r in computed.get("results", []):
                    if isinstance(r, dict) and r.get("name") and isinstance(r.get("data"), dict):
                        snap_dict["metrics"]["on_demand_metrics"][str(r["name"])] = r["data"]
            if isinstance(snap_dict.get("debug"), dict):
                snap_dict["debug"].setdefault("on_demand", {})
                snap_dict["debug"]["on_demand"]["computed_metrics"] = computed
        except Exception as e:
            if isinstance(snap_dict.get("debug"), dict):
                snap_dict["debug"]["on_demand_error"] = str(e)

    # Decide what to compute based on the question
    want_p95_hr = ("p95" in prompt_l or "95th" in prompt_l or "percentile" in prompt_l) and ("hr" in prompt_l or "heart rate" in prompt_l)
    want_vo2 = ("vo2" in prompt_l or "v02" in prompt_l)
    want_trimp = ("trimp" in prompt_l)
    want_tss = ("tss" in prompt_l)
    want_lthr = ("lthr" in prompt_l or "lactate threshold" in prompt_l or "threshold hr" in prompt_l)
    want_threshold = ("threshold" in prompt_l or "ftp" in prompt_l or "critical speed" in prompt_l or "critical power" in prompt_l)

    on_demand: list[str] = []
    if want_p95_hr:
        on_demand.append("hr_p95_window_all")
    if want_vo2:
        # Prefer combined window output so it can compare run vs ride automatically.
        on_demand.append("vo2_window_all")
    if want_trimp:
        on_demand.append("trimp_window_all")
    if want_tss:
        on_demand.append("tss_window_ride")
    if want_lthr:
        on_demand.append("lthr_window_all")
    if want_threshold:
        on_demand.append("threshold_window_all")

    # Follow-up: a bare HR number often answers "what is your LTHR?" from the prior turn.
    if (
        api_prior
        and prompt_l
        and re.fullmatch(r"[0-9]{2,3}(?:\.[0-9]+)?", prompt_l.strip())
        and "lthr_window_all" not in on_demand
    ):
        on_demand.append("lthr_window_all")

    if on_demand:
        _inject_on_demand(on_demand)
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
            snapshot=snap_dict,
            readiness=score,
            user_prompt=req.prompt,
            conversation_messages=api_prior or None,
        )
    else:
        # Deterministic fallback: still provides useful, formatted output based on agent-calculated metrics.
        m = snap_dict.get("metrics") if isinstance(snap_dict, dict) else snap.metrics
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
        score_v = score.get("score", "n/a")
        reasons = (score.get("reasons") or [])[:3]
        reasons_txt = "; ".join([str(r) for r in reasons if r]) if reasons else "no single strong driver detected"

        # Conversational, low-markdown fallback. Keep it compact to avoid truncation.
        lines.append(
            f"Readiness is {score_v}/100 today — {reasons_txt}. "
            "This is a quick read based on what’s available in your database window."
        )
        if req.prompt and req.prompt.strip():
            lines.append("")
            lines.append(f"You asked: {req.prompt.strip()}")

        # Mention agent-derived VO2 only if present and non-empty, but avoid long lists.
        def _one_line_vo2(label: str, block: dict) -> str | None:
            if not isinstance(block, dict) or not block or not block.get("row_count"):
                return None
            parts: list[str] = []
            if block.get("vo2max_est_last") is not None:
                parts.append(f"VO₂max proxy last ~{block['vo2max_est_last']:.1f} ml/kg/min")
            if block.get("vo2max_est_mean") is not None:
                parts.append(f"mean ~{block['vo2max_est_mean']:.1f}")
            if not parts:
                return None
            return f"{label}: " + ", ".join(parts) + "."

        vo2_run = _one_line_vo2("Running", run_block or {})
        vo2_cyc = _one_line_vo2("Cycling", cyc_block or {})
        if vo2_run or vo2_cyc:
            lines.append("")
            lines.append("From your activity streams (agent-derived estimates):")
            if vo2_run:
                lines.append(vo2_run)
            if vo2_cyc:
                lines.append(vo2_cyc)

        lines.append("")
        lines.append(
            "If you want a detailed breakdown (full metric list + methods), ask for “debug” or use the JSON output."
        )

        # Surface on-demand metrics in the deterministic fallback too
        if isinstance(m, dict) and isinstance(m.get("on_demand_metrics"), dict) and m["on_demand_metrics"]:
            odm = m["on_demand_metrics"]
            if want_p95_hr and isinstance(odm.get("hr_p95_window_all"), dict):
                hrp = odm["hr_p95_window_all"]
                try:
                    all_p = (hrp.get("all") or {}).get("hr_percentile_bpm")
                    run_p = (hrp.get("running") or {}).get("hr_percentile_bpm")
                    cyc_p = (hrp.get("cycling") or {}).get("hr_percentile_bpm")
                    lines.append("")
                    pieces: list[str] = []
                    if all_p is not None:
                        pieces.append(f"p95 HR all sports ~{float(all_p):.0f} bpm")
                    if run_p is not None:
                        pieces.append(f"running ~{float(run_p):.0f}")
                    if cyc_p is not None:
                        pieces.append(f"cycling ~{float(cyc_p):.0f}")
                    if pieces:
                        lines.append("On-demand HR percentile from raw streams: " + ", ".join(pieces) + ".")
                except Exception:
                    pass

        insights_obj = "\n".join(lines).strip()

    if conv_path is not None and conv_state is not None:
        user_save = (req.prompt or "").strip() or "(insight request)"
        assistant_save = insights_obj.strip() if isinstance(insights_obj, str) else str(insights_obj)
        if len(assistant_save) > 16000:
            assistant_save = assistant_save[:16000] + "\n…(truncated)"
        conv_state.window_days = req.window_days
        conv_state.turns.append({"user": user_save, "assistant": assistant_save})
        save_conversation(conv_path, conv_state, max_pairs=cfg.memory_max_turn_pairs)

    out: dict = {"snapshot": snap_dict, "readiness": score, "insights": insights_obj}
    if sid:
        out["session_id"] = sid
        out["conversation_turn_pairs"] = len(conv_state.turns) if conv_state is not None else 0
    return out


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


@app.post("/garmin/force_relogin", response_model=ForceReloginResponse)
def force_garmin_relogin(authorization: str | None = Header(default=None)):
    _require_auth(authorization)

    token_dir = os.path.expanduser(os.getenv("TOKEN_DIR", "/home/appuser/.garminconnect"))
    p = Path(token_dir).resolve()

    # Safety: only allow deleting within a ".garminconnect" directory
    if p.name != ".garminconnect":
        raise HTTPException(status_code=400, detail=f"Refusing to delete non-standard TOKEN_DIR: {p}")

    removed = 0
    if p.exists() and p.is_dir():
        for child in p.iterdir():
            try:
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink(missing_ok=True)
                removed += 1
            except Exception:
                continue

    return ForceReloginResponse(
        ok=True,
        token_dir=str(p),
        removed_paths=int(removed),
        note=(
            "Cleared cached Garmin session tokens. The `garmin-fetch-data` service should re-authenticate on its next run. "
            "If you use MFA and do not have a non-interactive login configured, you may need to run an interactive login once."
        ),
    )


@app.post("/garmin/auth_status", response_model=GarminAuthStatusResponse)
def garmin_auth_status(authorization: str | None = Header(default=None)):
    _require_auth(authorization)

    token_dir = os.path.expanduser(os.getenv("TOKEN_DIR", "/home/appuser/.garminconnect"))
    p = Path(token_dir).resolve()

    files: list[dict] = []
    newest: float | None = None
    if p.exists() and p.is_dir():
        for child in sorted(p.iterdir(), key=lambda x: x.name):
            try:
                st = child.stat()
                m = float(st.st_mtime)
                if newest is None or m > newest:
                    newest = m
                files.append(
                    {
                        "name": child.name,
                        "is_dir": bool(child.is_dir()),
                        "size_bytes": int(st.st_size),
                        "mtime_epoch": float(m),
                    }
                )
            except Exception:
                continue

    # Non-interactive login requires email + password envs inside garmin-fetch-data
    # (password is usually GARMINCONNECT_BASE64_PASSWORD).
    noninteractive = bool(os.getenv("GARMINCONNECT_EMAIL")) and bool(os.getenv("GARMINCONNECT_BASE64_PASSWORD"))

    most_recent_iso = None
    if newest is not None:
        # ISO UTC; keep dependencies minimal
        import datetime as _dt

        most_recent_iso = _dt.datetime.fromtimestamp(newest, tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")

    note = (
        "Token cache status only. For MFA accounts, a fresh login may require an interactive session to generate new tokens. "
        "If tokens are missing/empty and you see auth failures, run an interactive login in the `garmin-fetch-data` container to regenerate them."
    )

    return GarminAuthStatusResponse(
        ok=True,
        token_dir=str(p),
        token_files=files,
        file_count=int(len(files)),
        most_recent_mtime_utc=most_recent_iso,
        noninteractive_credentials_configured=bool(noninteractive),
        note=note,
    )


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

