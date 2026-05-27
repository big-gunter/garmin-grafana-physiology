"""
OAuth 2.1 implementation with GitHub as identity provider.
Supports Dynamic Client Registration and PKCE as required by Claude.ai.
"""
import os
import json
import time
import secrets
import hashlib
import base64
from typing import Optional
from pathlib import Path

import logging
import httpx
import jwt

log = logging.getLogger(__name__)

# --- Config ---
GITHUB_CLIENT_ID     = os.environ["GITHUB_CLIENT_ID"]
GITHUB_CLIENT_SECRET = os.environ["GITHUB_CLIENT_SECRET"]
GITHUB_ALLOWED_USER  = os.environ["GITHUB_ALLOWED_USER"]
MCP_BASE_URL         = os.environ["MCP_BASE_URL"].rstrip("/")
TOKEN_SECRET         = os.environ["TOKEN_SECRET"]

TOKEN_EXPIRY_SECONDS         = 3600            # 1 hour access tokens
REFRESH_TOKEN_EXPIRY_SECONDS = 30 * 24 * 3600  # 30 days; rolling (extended on each use)

# Persistent storage for registered clients and issued tokens
DATA_DIR = Path("/data")
DATA_DIR.mkdir(exist_ok=True)
CLIENTS_FILE       = DATA_DIR / "clients.json"
AUTH_CODES_FILE    = DATA_DIR / "auth_codes.json"
REFRESH_TOKENS_FILE = DATA_DIR / "refresh_tokens.json"


# --- Storage helpers ---

def _load(path: Path) -> dict:
    try:
        return json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        return {}

def _save(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2))


# --- OAuth metadata ---

def authorization_server_metadata() -> dict:
    return {
        "issuer": MCP_BASE_URL,
        "authorization_endpoint": f"{MCP_BASE_URL}/oauth/authorize",
        "token_endpoint": f"{MCP_BASE_URL}/oauth/token",
        "registration_endpoint": f"{MCP_BASE_URL}/oauth/register",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none", "client_secret_post"],
    }

def protected_resource_metadata() -> dict:
    return {
        "resource": MCP_BASE_URL,
        "authorization_servers": [MCP_BASE_URL],
        "bearer_methods_supported": ["header"],
        "resource_documentation": f"{MCP_BASE_URL}/mcp",
        "mcp_endpoint": f"{MCP_BASE_URL}/mcp",
    }

def openid_configuration() -> dict:
    """OIDC discovery document — superset of OAuth AS metadata.
    ChatGPT probes this endpoint during discovery; returning 401 here
    breaks its OAuth flow even when oauth-authorization-server is valid.
    """
    return {
        **authorization_server_metadata(),
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["HS256"],
    }


# --- Dynamic Client Registration ---

def register_client(request_data: dict) -> dict:
    """Register a new OAuth client (Dynamic Client Registration - RFC 7591).
    When token_endpoint_auth_method=none (public client), no client_secret
    is issued — required for ChatGPT which registers as a public client.
    """
    client_id = f"client_{secrets.token_hex(16)}"
    now = int(time.time())
    auth_method = request_data.get("token_endpoint_auth_method", "client_secret_basic")

    # Public clients (auth_method=none) receive no secret
    client_secret = None if auth_method == "none" else secrets.token_hex(32)

    client = {
        "client_id": client_id,
        "client_secret": client_secret,
        "client_name": request_data.get("client_name", "Unknown"),
        "redirect_uris": request_data.get("redirect_uris", []),
        "grant_types": request_data.get("grant_types", ["authorization_code"]),
        "response_types": request_data.get("response_types", ["code"]),
        "token_endpoint_auth_method": auth_method,
        "created_at": now,
    }

    clients = _load(CLIENTS_FILE)
    clients[client_id] = client
    _save(CLIENTS_FILE, clients)

    response = {
        "client_id": client_id,
        "client_id_issued_at": now,
        "redirect_uris": client["redirect_uris"],
        "grant_types": client["grant_types"],
        "response_types": client["response_types"],
        "token_endpoint_auth_method": auth_method,
    }
    if client_secret is not None:
        response["client_secret"] = client_secret
    return response

def get_client(client_id: str) -> Optional[dict]:
    clients = _load(CLIENTS_FILE)
    return clients.get(client_id)


# --- Authorization flow ---

def build_github_auth_url(state: str, client_id: str, redirect_uri: str,
                           code_challenge: str, resource: str = "") -> str:
    """Build the GitHub OAuth authorization URL."""
    # Store state -> request mapping for validation on callback
    codes = _load(AUTH_CODES_FILE)
    codes[f"state_{state}"] = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "resource": resource,
        "created_at": int(time.time()),
    }
    _save(AUTH_CODES_FILE, codes)

    params = (
        f"client_id={GITHUB_CLIENT_ID}"
        f"&redirect_uri={MCP_BASE_URL}/oauth/callback"
        f"&scope=read:user"
        f"&state={state}"
    )
    return f"https://github.com/login/oauth/authorize?{params}"

def validate_state(state: str) -> Optional[dict]:
    codes = _load(AUTH_CODES_FILE)
    return codes.get(f"state_{state}")


async def exchange_github_code(github_code: str, state: str) -> Optional[str]:
    """
    Exchange GitHub code for a token, verify the user, then issue our own
    auth code to hand back to Claude.ai.
    """
    # Exchange with GitHub
    log.info("github_exchange: sending code to GitHub token endpoint")
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://github.com/login/oauth/access_token",
            data={
                "client_id": GITHUB_CLIENT_ID,
                "client_secret": GITHUB_CLIENT_SECRET,
                "code": github_code,
                "redirect_uri": f"{MCP_BASE_URL}/oauth/callback",
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )
        token_data = resp.json()

    github_token = token_data.get("access_token")
    if not github_token:
        log.warning(
            "github_exchange: no access_token in response — error=%s description=%s",
            token_data.get("error"),
            token_data.get("error_description"),
        )
        return None

    # Verify it's the allowed GitHub user
    log.info("github_exchange: token received, fetching GitHub user")
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://api.github.com/user",
            headers={
                "Authorization": f"Bearer {github_token}",
                "Accept": "application/json",
            },
            timeout=10,
        )
        user_data = resp.json()

    github_username = user_data.get("login", "")
    log.info("github_exchange: GitHub user=%s allowed=%s", github_username, GITHUB_ALLOWED_USER)
    if github_username.lower() != GITHUB_ALLOWED_USER.lower():
        log.warning("github_exchange: user %s not in allowlist", github_username)
        return None

    # Issue our own auth code
    auth_code = secrets.token_urlsafe(32)
    codes = _load(AUTH_CODES_FILE)
    codes[auth_code] = {
        "github_username": github_username,
        "state": state,
        "created_at": int(time.time()),
        "used": False,
    }
    _save(AUTH_CODES_FILE, codes)
    log.info("github_exchange: issued app auth_code for user=%s", github_username)

    return auth_code


# --- Token issuance ---

def _verify_pkce(code_verifier: str, code_challenge: str) -> bool:
    """Verify PKCE S256 challenge."""
    digest = hashlib.sha256(code_verifier.encode()).digest()
    computed = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return computed == code_challenge


def issue_tokens(auth_code: str, code_verifier: str,
                  client_id: str, resource: str = "") -> Optional[dict]:
    """Exchange auth code for access + refresh tokens."""
    codes = _load(AUTH_CODES_FILE)
    code_data = codes.get(auth_code)

    if not code_data:
        return None
    if code_data.get("used"):
        return None
    if int(time.time()) - code_data["created_at"] > 300:  # 5 min expiry
        return None

    # Verify PKCE
    state = code_data["state"]
    state_data = codes.get(f"state_{state}", {})
    code_challenge = state_data.get("code_challenge", "")

    if code_challenge and not _verify_pkce(code_verifier, code_challenge):
        return None

    # Mark code as used
    codes[auth_code]["used"] = True
    _save(AUTH_CODES_FILE, codes)

    username = code_data["github_username"]

    # Resolve resource: prefer explicitly passed value, fall back to what was
    # stored during the authorize request (RFC 8707)
    effective_resource = resource or state_data.get("resource", "")

    # Issue access token (JWT)
    now = int(time.time())
    payload: dict = {
        "sub": username,
        "iss": MCP_BASE_URL,
        "iat": now,
        "exp": now + TOKEN_EXPIRY_SECONDS,
        "client_id": client_id,
    }
    if effective_resource:
        payload["aud"] = effective_resource

    access_token = jwt.encode(payload, TOKEN_SECRET, algorithm="HS256")

    # Issue refresh token
    refresh_token = secrets.token_urlsafe(48)
    refresh_tokens = _load(REFRESH_TOKENS_FILE)
    refresh_tokens[refresh_token] = {
        "username": username,
        "client_id": client_id,
        "resource": effective_resource,
        "created_at": now,
        "expires_at": now + REFRESH_TOKEN_EXPIRY_SECONDS,
    }
    _save(REFRESH_TOKENS_FILE, refresh_tokens)

    return {
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": TOKEN_EXPIRY_SECONDS,
        "refresh_token": refresh_token,
    }


def refresh_access_token(refresh_token: str, client_id: str) -> Optional[dict]:
    """Issue a new access token from a valid refresh token.

    Also extends the refresh token's expiry (rolling window) so that the session
    stays alive as long as it is actively used — callers only need to fully
    re-authenticate if the token is unused for REFRESH_TOKEN_EXPIRY_SECONDS.
    """
    refresh_tokens = _load(REFRESH_TOKENS_FILE)
    rt_data = refresh_tokens.get(refresh_token)

    if not rt_data:
        return None
    if int(time.time()) > rt_data["expires_at"]:
        return None

    username = rt_data["username"]
    now = int(time.time())
    resource = rt_data.get("resource", "")

    payload: dict = {
        "sub": username,
        "iss": MCP_BASE_URL,
        "iat": now,
        "exp": now + TOKEN_EXPIRY_SECONDS,
        "client_id": client_id,
    }
    if resource:
        payload["aud"] = resource

    access_token = jwt.encode(payload, TOKEN_SECRET, algorithm="HS256")

    # Roll the refresh token expiry forward from now, so the session survives
    # as long as there is at least one use within each 30-day window.
    refresh_tokens[refresh_token]["expires_at"] = now + REFRESH_TOKEN_EXPIRY_SECONDS
    _save(REFRESH_TOKENS_FILE, refresh_tokens)

    return {
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": TOKEN_EXPIRY_SECONDS,
        "refresh_token": refresh_token,
    }


def verify_access_token(token: str) -> Optional[str]:
    """Verify a Bearer token and return the username, or None if invalid."""
    try:
        payload = jwt.decode(
            token, TOKEN_SECRET, algorithms=["HS256"],
            options={"verify_aud": False},
        )
        iss = payload.get("iss")
        if iss != MCP_BASE_URL:
            log.warning("verify_token: iss mismatch got=%s expected=%s", iss, MCP_BASE_URL)
            return None
        aud = payload.get("aud")
        if aud is not None:
            targets = aud if isinstance(aud, list) else [aud]
            if MCP_BASE_URL not in targets:
                log.warning("verify_token: aud mismatch got=%s expected=%s", targets, MCP_BASE_URL)
                return None
        username = payload.get("sub", "")
        if username.lower() != GITHUB_ALLOWED_USER.lower():
            log.warning("verify_token: sub mismatch got=%s", username)
            return None
        return username
    except jwt.ExpiredSignatureError:
        log.warning("verify_token: token expired")
        return None
    except jwt.InvalidTokenError as e:
        log.warning("verify_token: invalid token — %s", e)
        return None
