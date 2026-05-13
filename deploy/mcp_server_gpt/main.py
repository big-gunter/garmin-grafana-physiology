"""
Main entry point.
Combines OAuth 2.1 endpoints with MCP streamable-http transport.
All MCP requests require a valid Bearer token.
"""
import os
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
        "/.well-known/oauth-authorization-server",
        "/.well-known/oauth-protected-resource",
        "/.well-known/openid-configuration",
        "/oauth/register",
        "/oauth/authorize",
        "/oauth/callback",
        "/oauth/token",
        "/health",
    }

    async def dispatch(self, request: Request, call_next):
        if request.url.path in self.UNPROTECTED:
            return await call_next(request)

        # All other paths (including /mcp) require Bearer token
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return JSONResponse(
                {"error": "unauthorized", "error_description": "Bearer token required"},
                status_code=401,
                headers={"WWW-Authenticate": f'Bearer resource_metadata="{MCP_BASE_URL}/.well-known/oauth-protected-resource"'},
            )

        token = auth_header[7:]
        username = auth.verify_access_token(token)
        if not username:
            log.warning("Invalid or expired token from %s", request.client.host)
            return JSONResponse(
                {"error": "invalid_token", "error_description": "Token is invalid or expired"},
                status_code=401,
                headers={"WWW-Authenticate": f'Bearer error="invalid_token", resource_metadata="{MCP_BASE_URL}/.well-known/oauth-protected-resource"'},
            )

        log.info("Authenticated request from %s to %s", username, request.url.path)
        return await call_next(request)


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
    client = auth.register_client(data)
    log.info("Registered new client: %s (%s)", client["client_id"], client.get("client_name"))
    return JSONResponse(client, status_code=201)

async def oauth_authorize(request: Request):
    params = dict(request.query_params)
    client_id      = params.get("client_id", "")
    redirect_uri   = params.get("redirect_uri", "")
    state          = params.get("state", "")
    code_challenge = params.get("code_challenge", "")
    code_challenge_method = params.get("code_challenge_method", "S256")

    if not all([client_id, redirect_uri, state]):
        return JSONResponse({"error": "invalid_request"}, status_code=400)

    if code_challenge_method != "S256":
        return JSONResponse({"error": "invalid_request",
                             "error_description": "Only S256 PKCE is supported"},
                            status_code=400)

    # Validate client exists
    client = auth.get_client(client_id)
    if not client:
        return JSONResponse({"error": "invalid_client"}, status_code=400)

    # Redirect to GitHub
    github_url = auth.build_github_auth_url(state, client_id, redirect_uri, code_challenge)
    log.info("Redirecting to GitHub OAuth for client %s", client_id)
    return RedirectResponse(github_url, status_code=302)

async def oauth_callback(request: Request):
    params   = dict(request.query_params)
    code     = params.get("code", "")
    state    = params.get("state", "")
    error    = params.get("error", "")

    if error:
        return HTMLResponse(f"<h2>Authentication failed: {error}</h2>", status_code=400)

    if not code or not state:
        return HTMLResponse("<h2>Missing code or state</h2>", status_code=400)

    # Validate state and get original request data
    state_data = auth.validate_state(state)
    if not state_data:
        return HTMLResponse("<h2>Invalid or expired state</h2>", status_code=400)

    # Exchange GitHub code for our auth code
    auth_code = await auth.exchange_github_code(code, state)
    if not auth_code:
        return HTMLResponse(
            "<h2>Authentication failed — your GitHub account is not authorised.</h2>",
            status_code=403
        )

    # Redirect back to Open.AI with the auth code
    redirect_uri = state_data["redirect_uri"]
    qs = qs = urlencode({"code": auth_code, "state": state_data["client_state"]}) 
    #urlencode({"code": auth_code, "state": state})
    log.info("Auth successful, redirecting to %s", redirect_uri)
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

    if grant_type == "authorization_code":
        auth_code      = data.get("code", "")
        code_verifier  = data.get("code_verifier", "")
        client_id      = data.get("client_id", "")

        if not all([auth_code, client_id]):
            return JSONResponse({"error": "invalid_request"}, status_code=400)

        tokens = auth.issue_tokens(auth_code, code_verifier, client_id)
        if not tokens:
            log.warning("Token exchange failed for client %s", client_id)
            return JSONResponse({"error": "invalid_grant"}, status_code=400)

        log.info("Issued tokens for client %s", client_id)
        return JSONResponse(tokens)

    elif grant_type == "refresh_token":
        refresh_token = data.get("refresh_token", "")
        client_id     = data.get("client_id", "")

        tokens = auth.refresh_access_token(refresh_token, client_id)
        if not tokens:
            return JSONResponse({"error": "invalid_grant"}, status_code=400)

        log.info("Refreshed tokens for client %s", client_id)
        return JSONResponse(tokens)

    else:
        return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)


async def health(request: Request):
    return JSONResponse({"status": "ok"})


# ---------------------------------------------------------------------------
# Build the MCP streamable-http app
# ---------------------------------------------------------------------------

mcp_app = mcp.streamable_http_app()


# ---------------------------------------------------------------------------
# Assemble the full Starlette app
# ---------------------------------------------------------------------------

routes = [
    Route("/.well-known/oauth-authorization-server", oauth_metadata),
    Route("/.well-known/oauth-protected-resource",   protected_resource_metadata),
    Route("/.well-known/openid-configuration",       openid_config),
    Route("/oauth/register",  oauth_register,  methods=["POST"]),
    Route("/oauth/authorize", oauth_authorize, methods=["GET"]),
    Route("/oauth/callback",  oauth_callback,  methods=["GET"]),
    Route("/oauth/token",     oauth_token,     methods=["POST"]),
    Route("/health",          health,          methods=["GET"]),
    Mount("/mcp",             app=mcp_app),
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
