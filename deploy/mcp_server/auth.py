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

import httpx
import jwt

# --- Config ---
GITHUB_CLIENT_ID     = os.environ["GITHUB_CLIENT_ID"]
GITHUB_CLIENT_SECRET = os.environ["GITHUB_CLIENT_SECRET"]
GITHUB_ALLOWED_USER  = os.environ["GITHUB_ALLOWED_USER"]
MCP_BASE_URL         = os.environ["MCP_BASE_URL"].rstrip("/")
TOKEN_SECRET         = os.environ["TOKEN_SECRET"]

TOKEN_EXPIRY_SECONDS         = 3600       # 1 hour access tokens
REFRESH_TOKEN_EXPIRY_SECONDS = 86400      # 24 hour refresh tokens

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
    Many OAuth clients (including ChatGPT) probe this endpoint; returning
    401 here breaks or confuses their discovery even when
    /.well-known/oauth-authorization-server is valid.
    """
    return {
        **authorization_server_metadata(),
        # Required OIDC fields that OAuth-only AS metadata omits
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["HS256"],
    }


# --- Dynamic Client Registration ---

def register_client(request_data: dict) -> dict:
    """Register a new OAuth client (Dynamic Client Registration - RFC 7591)."""
    client_id = f"client_{secrets.token_hex(16)}"
    client_secret = secrets.token_hex(32)

    client = {
        "client_id": client_id,
        "client_secret": client_secret,
        "client_name": request_data.get("client_name", "Unknown"),
        "redirect_uris": request_data.get("redirect_uris", []),
        "grant_types": request_data.get("grant_types", ["authorization_code"]),
        "response_types": request_data.get("response_types", ["code"]),
        "created_at": int(time.time()),
    }

    clients = _load(CLIENTS_FILE)
    clients[client_id] = client
    _save(CLIENTS_FILE, clients)

    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "client_name": client["client_name"],
        "redirect_uris": client["redirect_uris"],
        "grant_types": client["grant_types"],
        "response_types": client["response_types"],
    }

def get_client(client_id: str) -> Optional[dict]:
    clients = _load(CLIENTS_FILE)
    return clients.get(client_id)


# --- Authorization flow ---

def build_github_auth_url(state: str, client_id: str, redirect_uri: str,
                           code_challenge: str) -> str:
    """Build the GitHub OAuth authorization URL."""
    # Store state -> request mapping for validation on callback
    codes = _load(AUTH_CODES_FILE)
    codes[f"state_{state}"] = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
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
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://github.com/login/oauth/access_token",
            json={
                "client_id": GITHUB_CLIENT_ID,
                "client_secret": GITHUB_CLIENT_SECRET,
                "code": github_code,
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )
        token_data = resp.json()

    github_token = token_data.get("access_token")
    if not github_token:
        return None

    # Verify it's the allowed GitHub user
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
    if github_username.lower() != GITHUB_ALLOWED_USER.lower():
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

    return auth_code


# --- Token issuance ---

def _verify_pkce(code_verifier: str, code_challenge: str) -> bool:
    """Verify PKCE S256 challenge."""
    digest = hashlib.sha256(code_verifier.encode()).digest()
    computed = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return computed == code_challenge


def issue_tokens(auth_code: str, code_verifier: str,
                  client_id: str) -> Optional[dict]:
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

    # Issue access token (JWT)
    now = int(time.time())
    access_token = jwt.encode(
        {
            "sub": username,
            "iss": MCP_BASE_URL,
            "iat": now,
            "exp": now + TOKEN_EXPIRY_SECONDS,
            "client_id": client_id,
        },
        TOKEN_SECRET,
        algorithm="HS256",
    )

    # Issue refresh token
    refresh_token = secrets.token_urlsafe(48)
    refresh_tokens = _load(REFRESH_TOKENS_FILE)
    refresh_tokens[refresh_token] = {
        "username": username,
        "client_id": client_id,
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
    """Issue a new access token from a valid refresh token."""
    refresh_tokens = _load(REFRESH_TOKENS_FILE)
    rt_data = refresh_tokens.get(refresh_token)

    if not rt_data:
        return None
    if int(time.time()) > rt_data["expires_at"]:
        return None

    username = rt_data["username"]
    now = int(time.time())

    access_token = jwt.encode(
        {
            "sub": username,
            "iss": MCP_BASE_URL,
            "iat": now,
            "exp": now + TOKEN_EXPIRY_SECONDS,
            "client_id": client_id,
        },
        TOKEN_SECRET,
        algorithm="HS256",
    )

    return {
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": TOKEN_EXPIRY_SECONDS,
        "refresh_token": refresh_token,
    }


def verify_access_token(token: str) -> Optional[str]:
    """Verify a Bearer token and return the username, or None if invalid."""
    try:
        payload = jwt.decode(token, TOKEN_SECRET, algorithms=["HS256"])
        username = payload.get("sub", "")
        if username.lower() != GITHUB_ALLOWED_USER.lower():
            return None
        return username
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None
