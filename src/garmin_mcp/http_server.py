"""Streamable HTTP MCP gateway for remote clients (e.g. Claude mobile) behind HTTPS.

Run:
  uv run garmin-mcp-http

Env:
  INFLUXDB_* — same as garmin-fetch / garmin-mcp
  GARMIN_AGENT_MEMORY_* — optional SQLite paths

  MCP_HTTP_HOST — bind address (default 127.0.0.1)
  MCP_HTTP_PORT — listen port (default 8765)
  MCP_STREAMABLE_HTTP_PATH — MCP route (default /mcp)
  MCP_PUBLIC_HOSTS — comma-separated Host values for DNS rebinding protection
    (e.g. mcp.example.com). Set when terminating TLS on a public hostname.
  MCP_AUTH_TOKEN — if set, require ``Authorization: Bearer <token>``
"""

from __future__ import annotations

import logging
import os

import uvicorn
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse

from .stack_build import create_garmin_mcp

logger = logging.getLogger(__name__)


def _transport_security_from_env(bind_host: str) -> TransportSecuritySettings | None:
    raw = os.environ.get("MCP_PUBLIC_HOSTS", "").strip()
    if not raw:
        return None
    hosts = [h.strip() for h in raw.split(",") if h.strip()]
    allowed_hosts: list[str] = []
    for h in hosts:
        allowed_hosts.append(h if ":" in h else f"{h}:*")
    allowed_origins: list[str] = []
    for h in hosts:
        base = h.split(":")[0]
        allowed_origins.extend([f"https://{base}:*", f"http://{base}:*"])
    # Local tooling often uses Host: 127.0.0.1 while developing with public names in TLS.
    if bind_host in ("127.0.0.1", "localhost", "::1"):
        allowed_hosts.extend(
            [
                "127.0.0.1:*",
                "localhost:*",
                "[::1]:*",
            ]
        )
        allowed_origins.extend(
            [
                "http://127.0.0.1:*",
                "http://localhost:*",
                "http://[::1]:*",
            ]
        )
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=allowed_hosts,
        allowed_origins=allowed_origins,
    )


class _BearerGuard:
    """ASGI wrapper: require Authorization Bearer when MCP_AUTH_TOKEN is set."""

    def __init__(self, app: object, token: str | None) -> None:
        self.app = app
        self.token = token

    async def __call__(self, scope: dict, receive: object, send: object) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        if self.token:
            path = scope.get("path") or ""
            if scope.get("method") == b"GET" and path.rstrip("/") == "/health":
                await self.app(scope, receive, send)
                return
            want = f"Bearer {self.token}"
            auth_val: str | None = None
            for key, val in scope.get("headers") or []:
                if key.lower() == b"authorization":
                    auth_val = val.decode("latin-1")
                    break
            if auth_val != want:
                resp = JSONResponse({"detail": "Unauthorized"}, status_code=401)
                await resp(scope, receive, send)
                return
        await self.app(scope, receive, send)


def _parse_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return int(raw.strip())


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

    host = os.environ.get("MCP_HTTP_HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = _parse_int("MCP_HTTP_PORT", 8765)
    path = os.environ.get("MCP_STREAMABLE_HTTP_PATH", "/mcp").strip() or "/mcp"
    token = os.environ.get("MCP_AUTH_TOKEN", "").strip() or None

    if host in ("0.0.0.0", "::") and not token:
        logger.warning(
            "Listening on all interfaces without MCP_AUTH_TOKEN. "
            "Set MCP_AUTH_TOKEN for remote access, or bind to 127.0.0.1 and reverse-proxy with auth."
        )

    ts = _transport_security_from_env(host)

    mcp = create_garmin_mcp(
        host=host,
        port=port,
        streamable_http_path=path,
        transport_security=ts,
    )

    @mcp.custom_route("/health", methods=["GET"])
    async def health_check(_request: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    inner = mcp.streamable_http_app()
    app: object = _BearerGuard(inner, token) if token else inner

    logger.info(
        "Garmin MCP streamable HTTP — bind %s:%s — MCP route %s",
        host,
        port,
        path,
    )
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level=os.environ.get("UVICORN_LOG_LEVEL", "info").lower(),
    )


if __name__ == "__main__":
    main()
