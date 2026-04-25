from __future__ import annotations

from fastapi import FastAPI, Header, HTTPException
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


def main() -> None:
    import uvicorn

    uvicorn.run(app, host=cfg.host, port=cfg.port, log_level="info")


if __name__ == "__main__":
    main()

