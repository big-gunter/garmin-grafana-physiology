from __future__ import annotations

import json
import logging
import os
import secrets
import threading
import time
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

import requests

AUTH_BASE = "https://api.prod.whoop.com/oauth/oauth2"
REDIRECT_URI = "http://localhost:8080/callback"
SCOPES = "read:recovery read:sleep read:profile read:workout read:body_measurement offline"
TOKEN_FILE = "whoop_tokens.json"


class WhoopAuth:
    def __init__(self, client_id: str, client_secret: str, token_dir: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_dir = token_dir
        self._token_file = os.path.join(token_dir, TOKEN_FILE)
        self._tokens: dict = {}

    def load_tokens(self) -> bool:
        if not os.path.exists(self._token_file):
            return False
        try:
            with open(self._token_file) as f:
                self._tokens = json.load(f)
            has_access = bool(self._tokens.get("access_token"))
            has_refresh = bool(self._tokens.get("refresh_token"))
            expires_at = self._tokens.get("expires_at")
            expires_str = (
                datetime.fromtimestamp(expires_at).isoformat()
                if expires_at is not None
                else "MISSING"
            )
            logging.info(
                "Loaded tokens from %s — access_token: %s, refresh_token: %s, expires_at: %s",
                self._token_file,
                "present" if has_access else "MISSING",
                "present" if has_refresh else "MISSING",
                expires_str,
            )
            if not has_refresh:
                logging.warning(
                    "refresh_token is absent from %s — re-run the auth flow to generate new tokens",
                    self._token_file,
                )
            return has_access
        except Exception:
            logging.exception("Failed to load WHOOP tokens from %s", self._token_file)
            return False

    def save_tokens(self) -> None:
        os.makedirs(self.token_dir, exist_ok=True)
        with open(self._token_file, "w") as f:
            json.dump(self._tokens, f, indent=2)
        logging.info("WHOOP tokens saved to %s", self._token_file)

    def _exchange_code(self, code: str) -> dict:
        resp = requests.post(
            f"{AUTH_BASE}/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": REDIRECT_URI,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        data["expires_at"] = time.time() + data.get("expires_in", 3600) - 60
        return data

    def refresh(self) -> None:
        refresh_token = self._tokens.get("refresh_token")
        if not refresh_token:
            raise RuntimeError("No refresh token — run initial auth flow")
        logging.debug("Refreshing token (refresh_token prefix: %s...)", refresh_token[:8])
        resp = requests.post(
            f"{AUTH_BASE}/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        if not resp.ok:
            logging.error(
                "Token refresh failed: HTTP %d — %s",
                resp.status_code,
                resp.text[:500],
            )
            resp.raise_for_status()
        data = resp.json()
        data["expires_at"] = time.time() + data.get("expires_in", 3600) - 60
        self._tokens.update(data)
        self.save_tokens()
        logging.info(
            "WHOOP access token refreshed; new expiry: %s",
            datetime.fromtimestamp(data["expires_at"]).isoformat(),
        )

    def get_access_token(self) -> str:
        if not self._tokens:
            if not self.load_tokens():
                raise RuntimeError(
                    "No WHOOP tokens found. Run initial auth:\n"
                    "  docker compose --profile whoop run --rm -p 8080:8080 whoop-fetch-data "
                    "python -m whoop.whoop_auth"
                )
        now = time.time()
        expires_at = self._tokens.get("expires_at")
        if expires_at is None:
            logging.warning(
                "Token file has no expires_at field — forcing refresh to be safe"
            )
        else:
            logging.debug(
                "Token expiry: %s (in %.0fs)",
                datetime.fromtimestamp(expires_at).isoformat(),
                expires_at - now,
            )
        if expires_at is None or now >= expires_at:
            logging.info("Access token expired or expiry unknown; refreshing...")
            self.refresh()
        return self._tokens["access_token"]

    def run_initial_auth_flow(self) -> None:
        """Start a local HTTP server on :8080, open browser, capture OAuth callback."""
        code_holder: list[str] = []
        done = threading.Event()
        state = secrets.token_urlsafe(16)  # 22-char URL-safe string — above WHOOP's 8-char minimum

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                parsed = urlparse(self.path)
                if parsed.path == "/callback":
                    params = parse_qs(parsed.query)
                    returned_state = (params.get("state") or [None])[0]
                    if returned_state != state:
                        self.send_response(400)
                        self.end_headers()
                        self.wfile.write(b"State mismatch - possible CSRF attempt.")
                        return
                    code = (params.get("code") or [None])[0]
                    if code:
                        code_holder.append(code)
                        self.send_response(200)
                        self.end_headers()
                        self.wfile.write(
                            b"<h1>WHOOP authorization complete</h1>"
                            b"<p>You can close this tab.</p>"
                        )
                        done.set()
                    else:
                        self.send_response(400)
                        self.end_headers()
                        self.wfile.write(b"Missing code - please try again.")

            def log_message(self, *args):
                pass

        server = HTTPServer(("0.0.0.0", 8080), _Handler)
        t = threading.Thread(target=server.handle_request, daemon=True)
        t.start()

        params = urlencode({
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": REDIRECT_URI,
            "scope": SCOPES,
            "state": state,
        })
        auth_url = f"{AUTH_BASE}/auth?{params}"

        print("\n" + "=" * 60)
        print("WHOOP Authorization")
        print("=" * 60)
        print(f"\nOpen this URL in your browser:\n\n  {auth_url}\n")
        print("After approving, you will be redirected to localhost:8080/callback.")
        print("Waiting up to 120 seconds...\n")

        try:
            webbrowser.open(auth_url)
        except Exception:
            pass

        if not done.wait(timeout=120):
            server.server_close()
            raise RuntimeError("Auth timed out — no callback received within 120 seconds")

        server.server_close()

        if not code_holder:
            raise RuntimeError("No authorization code received")

        tokens = self._exchange_code(code_holder[0])
        self._tokens = tokens
        self.save_tokens()
        print("\nAuthorization successful — tokens saved.\n")


if __name__ == "__main__":
    import sys
    from whoop import config as cfg

    if not cfg.WHOOP_CLIENT_ID or not cfg.WHOOP_CLIENT_SECRET:
        print("ERROR: WHOOP_CLIENT_ID and WHOOP_CLIENT_SECRET must be set")
        sys.exit(1)

    auth = WhoopAuth(
        client_id=cfg.WHOOP_CLIENT_ID,
        client_secret=cfg.WHOOP_CLIENT_SECRET,
        token_dir=cfg.WHOOP_TOKEN_DIR,
    )
    auth.run_initial_auth_flow()
    print(f"Tokens written to {auth._token_file}")
