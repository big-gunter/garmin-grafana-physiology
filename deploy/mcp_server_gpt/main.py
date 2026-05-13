"""
Main entry point.
Combines OAuth 2.1 endpoints with MCP streamable-http transport.
All MCP requests require a valid Bearer token.
"""
import os
import time
import uuid
import logging
from urllib.parse import urlencode

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, HTMLResponse
from starlette.routing import Route, Mount
from starlette.middleware.base import BaseHTTPMiddleware

from mcp.server.transport_security import TransportSecuritySettings
import auth
from server import mcp

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

MCP_BASE_URL = os.environ["MCP_BASE_URL"].rstrip("/")
HOSTNAME = MCP_BASE_URL.replace("https://", "").replace("http://", "")


# ---------------------------------------------------------------------------
# Auth middleware — protects /mcp endpoint
# ---------------------------------------------------------------------------

class BearerAuthMiddleware(BaseHTTPMiddleware):
    UNPROTECTED = {
        "/",
        "/.well-known/oauth-authorization-server",
        "/.well-known/oauth-authorization-server/mcp",
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource/mcp",
        "/.well-known/openid-configuration",
        "/oauth/register",
        "/oauth/authorize",
        "/oauth/callback",
        "/oauth/token",
        "/health",
        "/debug/oauth-metadata",
        "/debug/protected-resource",
        "/debug/routes",
    }

    async def dispatch(self, request: Request, call_next):
        req_id = uuid.uuid4().hex[:8]
        started = time.time()
        path = request.url.path
        query = str(request.url.query)
        client_host = request.client.host if request.client else "unknown"

        log.info(
            "[%s] → %s %s%s | accept=%s ct=%s origin=%s auth=%s | cf-ray=%s ip=%s",
            req_id,
            request.method,
            path,
            f"?{query}" if query else "",
            request.headers.get("accept", "")[:60],
            request.headers.get("content-type", ""),
            request.headers.get("origin", ""),
            request.headers.get("authorization", "").startswith("Bearer "),
            request.headers.get("cf-ray", ""),
            client_host,
        )

        response_body_hint = None
        try:
            if path in self.UNPROTECTED:
                response = await call_next(request)
            else:
                auth_header = request.headers.get("Authorization", "")
                if not auth_header.startswith("Bearer "):
                    response_body_hint = "no-token"
                    response = JSONResponse(
                        {"error": "unauthorized", "error_description": "Bearer token required"},
                        status_code=401,
                        headers={
                            "WWW-Authenticate": (
                                f'Bearer resource_metadata="{MCP_BASE_URL}/.well-known/oauth-protected-resource"'
                            )
                        },
                    )
                else:
                    token = auth_header[7:]
                    username = auth.verify_access_token(token)
                    if not username:
                        response_body_hint = "invalid-token"
                        response = JSONResponse(
                            {"error": "invalid_token", "error_description": "Token is invalid or expired"},
                            status_code=401,
                            headers={
                                "WWW-Authenticate": (
                                    f'Bearer error="invalid_token", '
                                    f'resource_metadata="{MCP_BASE_URL}/.well-known/oauth-protected-resource"'
                                )
                            },
                        )
                    else:
                        log.info("[%s] authenticated user=%s", req_id, username)
                        response = await call_next(request)

            status = response.status_code
            response.headers["X-Request-Id"] = req_id
            duration_ms = int((time.time() - started) * 1000)

            if status >= 400:
                log.warning(
                    "[%s] ← %d %s %s | %dms | reason=%s",
                    req_id, status, request.method, path, duration_ms,
                    response_body_hint or "upstream",
                )
            else:
                log.info(
                    "[%s] ← %d %s %s | %dms",
                    req_id, status, request.method, path, duration_ms,
                )

            return response

        except Exception:
            log.exception("[%s] unhandled exception %s %s", req_id, request.method, path)
            raise


# ---------------------------------------------------------------------------
# OAuth endpoints
# ---------------------------------------------------------------------------

async def oauth_metadata(request: Request):
    return JSONResponse(auth.authorization_server_metadata())

async def protected_resource_metadata(request: Request):
    return JSONResponse(auth.protected_resource_metadata())

async def openid_config(request: Request):
    return JSONResponse(auth.openid_configuration())

async def oauth_register(request: Request):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid_request"}, status_code=400)
    log.info(
        "DCR request: client_name=%s redirect_uris=%s grant_types=%s auth_method=%s",
        data.get("client_name"), data.get("redirect_uris"),
        data.get("grant_types"), data.get("token_endpoint_auth_method"),
    )
    client = auth.register_client(data)
    log.info("DCR issued: client_id=%s auth_method=%s", client["client_id"], client.get("token_endpoint_auth_method"))
    return JSONResponse(client, status_code=201)

async def oauth_authorize(request: Request):
    params = dict(request.query_params)
    client_id      = params.get("client_id", "")
    redirect_uri   = params.get("redirect_uri", "")
    state          = params.get("state", "")
    code_challenge = params.get("code_challenge", "")
    code_challenge_method = params.get("code_challenge_method", "S256")
    resource       = params.get("resource", "")  # RFC 8707

    log.info(
        "authorize: client_id=%s redirect_uri=%s state_len=%d pkce=%s resource=%s",
        client_id, redirect_uri, len(state), bool(code_challenge), resource,
    )

    if not all([client_id, redirect_uri, state]):
        log.warning("authorize: missing required params client_id=%s redirect_uri=%s state=%s", client_id, redirect_uri, bool(state))
        return JSONResponse({"error": "invalid_request"}, status_code=400)

    if code_challenge_method != "S256":
        log.warning("authorize: unsupported pkce method=%s", code_challenge_method)
        return JSONResponse({"error": "invalid_request",
                             "error_description": "Only S256 PKCE is supported"},
                            status_code=400)

    client = auth.get_client(client_id)
    if not client:
        log.warning("authorize: unknown client_id=%s", client_id)
        return JSONResponse({"error": "invalid_client"}, status_code=400)

    github_url = auth.build_github_auth_url(state, client_id, redirect_uri, code_challenge, resource)
    log.info("authorize: stored state, redirecting to GitHub for client=%s", client_id)
    return RedirectResponse(github_url, status_code=302)

async def oauth_callback(request: Request):
    params   = dict(request.query_params)
    code     = params.get("code", "")
    state    = params.get("state", "")
    error    = params.get("error", "")

    log.info("callback: received code=%s state_len=%d error=%s", bool(code), len(state), error or None)

    if error:
        log.warning("callback: GitHub returned error=%s", error)
        return HTMLResponse(f"<h2>Authentication failed: {error}</h2>", status_code=400)

    if not code or not state:
        log.warning("callback: missing code or state")
        return HTMLResponse("<h2>Missing code or state</h2>", status_code=400)

    state_data = auth.validate_state(state)
    if not state_data:
        log.warning("callback: invalid or expired state (len=%d)", len(state))
        return HTMLResponse("<h2>Invalid or expired state</h2>", status_code=400)

    log.info("callback: valid state, exchanging GitHub code for client=%s", state_data.get("client_id"))
    auth_code = await auth.exchange_github_code(code, state)
    if not auth_code:
        log.warning("callback: GitHub exchange failed — user not authorised or token exchange error")
        return HTMLResponse(
            "<h2>Authentication failed — your GitHub account is not authorised.</h2>",
            status_code=403
        )

    redirect_uri = state_data["redirect_uri"]
    qs = urlencode({"code": auth_code, "state": state})
    log.info("callback: auth_code issued, redirecting to redirect_uri=%s", redirect_uri)
    return RedirectResponse(f"{redirect_uri}?{qs}", status_code=302)

async def oauth_token(request: Request):
    try:
        form = await request.form()
        data = dict(form)
    except Exception:
        try:
            data = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid_request"}, status_code=400)

    grant_type = data.get("grant_type", "")
    log.info("token: grant_type=%s client_id=%s resource=%s has_verifier=%s",
             grant_type, data.get("client_id"), data.get("resource"), bool(data.get("code_verifier")))

    if grant_type == "authorization_code":
        auth_code      = data.get("code", "")
        code_verifier  = data.get("code_verifier", "")
        client_id      = data.get("client_id", "")
        resource       = data.get("resource", "")

        if not all([auth_code, client_id]):
            log.warning("token: missing auth_code or client_id")
            return JSONResponse({"error": "invalid_request"}, status_code=400)

        tokens = auth.issue_tokens(auth_code, code_verifier, client_id, resource)
        if not tokens:
            log.warning("token: issue_tokens failed for client=%s (bad code/PKCE/expiry)", client_id)
            return JSONResponse({"error": "invalid_grant"}, status_code=400)

        log.info("token: issued access+refresh for client=%s resource=%s", client_id, resource)
        return JSONResponse(tokens)

    elif grant_type == "refresh_token":
        refresh_token = data.get("refresh_token", "")
        client_id     = data.get("client_id", "")

        tokens = auth.refresh_access_token(refresh_token, client_id)
        if not tokens:
            log.warning("token: refresh failed for client=%s (expired or unknown)", client_id)
            return JSONResponse({"error": "invalid_grant"}, status_code=400)

        log.info("token: refreshed for client=%s", client_id)
        return JSONResponse(tokens)

    else:
        log.warning("token: unsupported grant_type=%s", grant_type)
        return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)


async def health(request: Request):
    return JSONResponse({"status": "ok"})

async def root(request: Request):
    return JSONResponse({
        "status": "ok",
        "mcp_endpoint": f"{MCP_BASE_URL}/mcp",
        "authorization_endpoint": f"{MCP_BASE_URL}/oauth/authorize",
        "token_endpoint": f"{MCP_BASE_URL}/oauth/token",
    })


# ---------------------------------------------------------------------------
# Debug endpoints (safe — no Influx/Grafana data, no secrets)
# ---------------------------------------------------------------------------

async def debug_oauth_metadata(request: Request):
    return JSONResponse(auth.authorization_server_metadata())

async def debug_protected_resource(request: Request):
    return JSONResponse(auth.protected_resource_metadata())

async def debug_routes(request: Request):
    return JSONResponse({
        "routes": [
            "GET /",
            "GET /.well-known/oauth-authorization-server",
            "GET /.well-known/oauth-authorization-server/mcp",
            "GET /.well-known/oauth-protected-resource",
            "GET /.well-known/oauth-protected-resource/mcp",
            "GET /.well-known/openid-configuration",
            "POST /oauth/register",
            "GET /oauth/authorize",
            "GET /oauth/callback",
            "POST /oauth/token",
            "GET /health",
            "GET /debug/oauth-metadata",
            "GET /debug/protected-resource",
            "GET /debug/routes",
            "* /mcp  [FastMCP streamable-http, Bearer required]",
        ]
    })


# ---------------------------------------------------------------------------
# Build the MCP streamable-http app
# ---------------------------------------------------------------------------

mcp_app = mcp.streamable_http_app()


# ---------------------------------------------------------------------------
# Assemble the full Starlette app
# ---------------------------------------------------------------------------

routes = [
    Route("/",                                            root,                        methods=["GET"]),
    Route("/.well-known/oauth-authorization-server",      oauth_metadata),
    Route("/.well-known/oauth-authorization-server/mcp",  oauth_metadata),
    Route("/.well-known/oauth-protected-resource",        protected_resource_metadata),
    Route("/.well-known/oauth-protected-resource/mcp",    protected_resource_metadata),
    Route("/.well-known/openid-configuration",            openid_config),
    Route("/oauth/register",       oauth_register,        methods=["POST"]),
    Route("/oauth/authorize",      oauth_authorize,       methods=["GET"]),
    Route("/oauth/callback",       oauth_callback,        methods=["GET"]),
    Route("/oauth/token",          oauth_token,           methods=["POST"]),
    Route("/health",               health,                methods=["GET"]),
    Route("/debug/oauth-metadata", debug_oauth_metadata,  methods=["GET"]),
    Route("/debug/protected-resource", debug_protected_resource, methods=["GET"]),
    Route("/debug/routes",         debug_routes,          methods=["GET"]),
    Mount("/",                     app=mcp_app),
]

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    async with mcp_app.router.lifespan_context(app):
        yield

app = Starlette(routes=routes, lifespan=lifespan)
app.add_middleware(BearerAuthMiddleware)

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        forwarded_allow_ips="*",
        proxy_headers=True,
    )
