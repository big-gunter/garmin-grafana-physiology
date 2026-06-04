#!/usr/bin/env python3
"""
Garmin OAuth token extraction via real browser login.

For managed/child Garmin accounts where Garmin's SSO blocks programmatic
authentication. Opens a real Chromium browser for manual login, captures
the resulting SSO ticket, and exchanges it for OAuth tokens that
garmin-fetch-data accepts directly.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OPTION A — Run locally (works on Mac/Windows/Linux)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  pip install playwright requests requests-oauthlib
  playwright install chromium
  python deploy/setup/08_garmin_browser_auth.py --output ./tokens
  scp ./tokens/oauth*.json root@<server-ip>:/opt/<username>/garminconnect-tokens/

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OPTION B — Run on server, display forwarded via SSH X11 (Linux/Mac only)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  On your local machine:
    ssh -XC -p 22444 root@<server-ip>

  Then on the server:
    pip install playwright requests requests-oauthlib
    playwright install chromium
    python /opt/<username>/deploy/setup/08_garmin_browser_auth.py \
        --output /opt/<username>/garminconnect-tokens

  Tokens are written directly to the correct path — no SCP needed.
  Mac users may need XQuartz installed: https://www.xquartz.org
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, parse_qs

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sys.exit(
        "playwright not installed.\n"
        "Run: pip install playwright && playwright install chromium"
    )

try:
    import requests
    from requests_oauthlib import OAuth1
except ImportError:
    sys.exit(
        "requests or requests-oauthlib not installed.\n"
        "Run: pip install requests requests-oauthlib"
    )


# ---------------------------------------------------------------------------
# Garmin OAuth constants
# Consumer credentials are fetched from garth's public S3 bucket — the same
# values used by garth/garminconnect. They are the Garmin Android app's
# public OAuth consumer credentials.
# ---------------------------------------------------------------------------
OAUTH_CONSUMER_URL = "https://thegarth.s3.amazonaws.com/oauth_consumer.json"
OAUTH1_ENDPOINT    = "https://connectapi.garmin.com/oauth-service/oauth/preauthorized"
OAUTH2_ENDPOINT    = "https://connectapi.garmin.com/oauth-service/oauth/exchange/user/2.0"
MOBILE_USER_AGENT  = "com.garmin.android.apps.connectmobile"

SSO_URL = "https://sso.garmin.com/sso/signin"
SSO_PARAMS = {
    "id":                             "gauth-widget",
    "embedWidget":                    "true",
    "gauthHost":                      "https://sso.garmin.com/sso",
    "service":                        "https://sso.garmin.com/sso/embed",
    "source":                         "https://sso.garmin.com/sso/embed",
    "redirectAfterAccountLoginUrl":   "https://sso.garmin.com/sso/embed",
    "redirectAfterAccountCreationUrl":"https://sso.garmin.com/sso/embed",
    "clientId":                       "GarminConnect",
    "locale":                         "en_US",
    "consumeServiceTicket":           "false",
}

TICKET_RE = re.compile(r"ticket=(ST-[A-Za-z0-9\-]+)")


def fetch_consumer_credentials():
    print("==> Fetching OAuth consumer credentials...")
    try:
        r = requests.get(OAUTH_CONSUMER_URL, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data["consumer_key"], data["consumer_secret"]
    except Exception as e:
        sys.exit(f"ERROR: Could not fetch OAuth consumer credentials: {e}")


def capture_sso_ticket(timeout_seconds: int = 180) -> str:
    """Open a headed browser, wait for manual login, return the SSO ticket."""
    login_url = f"{SSO_URL}?{urlencode(SSO_PARAMS)}"
    ticket = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        def check_for_ticket(url: str):
            nonlocal ticket
            if not ticket:
                m = TICKET_RE.search(url)
                if m:
                    ticket = m.group(1)

        page.on("response",       lambda r: check_for_ticket(r.url))
        page.on("framenavigated", lambda f: check_for_ticket(f.url))

        print(f"\n==> Browser opening — log in with the child/managed account.")
        print(f"    Waiting up to {timeout_seconds}s for successful login...\n")

        page.goto(login_url)

        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if ticket:
                break
            # Also scan page content for ticket in case event was missed
            try:
                m = TICKET_RE.search(page.content())
                if m:
                    ticket = m.group(1)
            except Exception:
                pass
            time.sleep(0.5)

        browser.close()

    return ticket


def exchange_oauth1(ticket: str, consumer_key: str, consumer_secret: str) -> dict:
    """Exchange an SSO ticket for an OAuth1 token."""
    resp = requests.get(
        OAUTH1_ENDPOINT,
        params={
            "ticket":            ticket,
            "login-url":         "https://sso.garmin.com/sso/embed",
            "accepts-mfa-tokens":"true",
        },
        headers={"User-Agent": MOBILE_USER_AGENT},
        auth=OAuth1(consumer_key, consumer_secret),
        timeout=30,
    )
    resp.raise_for_status()

    parsed = parse_qs(resp.text)
    return {
        "oauth_token":        parsed["oauth_token"][0],
        "oauth_token_secret": parsed["oauth_token_secret"][0],
        "domain":             "garmin.com",
    }


def exchange_oauth2(oauth1: dict, consumer_key: str, consumer_secret: str) -> dict:
    """Exchange an OAuth1 token for an OAuth2 token."""
    resp = requests.post(
        OAUTH2_ENDPOINT,
        headers={
            "User-Agent":   MOBILE_USER_AGENT,
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"audience": "GARMIN_CONNECT_MOBILE_ANDROID_DI"},
        auth=OAuth1(
            consumer_key, consumer_secret,
            oauth1["oauth_token"], oauth1["oauth_token_secret"],
        ),
        timeout=30,
    )
    resp.raise_for_status()

    data = resp.json()
    now = datetime.now(timezone.utc).timestamp()
    data.setdefault("expires_at", now + data.get("expires_in", 3600))
    if "refresh_token_expires_in" in data:
        data.setdefault(
            "refresh_token_expires_at",
            now + data["refresh_token_expires_in"]
        )
    return data


def save_tokens(output_dir: str, oauth1: dict, oauth2: dict):
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    for filename, data in [("oauth1_token.json", oauth1), ("oauth2_token.json", oauth2)]:
        path = out / filename
        path.write_text(json.dumps(data, indent=2))
        path.chmod(0o600)
        print(f"    {path}")


def main():
    parser = argparse.ArgumentParser(
        description="Garmin browser auth — for managed/child accounts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--output", "-o",
        default="./garminconnect-tokens",
        help="Directory to write oauth1_token.json and oauth2_token.json "
             "(default: ./garminconnect-tokens)",
    )
    parser.add_argument(
        "--timeout", "-t",
        type=int, default=180,
        help="Seconds to wait for browser login (default: 180)",
    )
    args = parser.parse_args()

    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  Garmin Browser Authentication")
    print("  For managed/child accounts with SSO restrictions")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

    consumer_key, consumer_secret = fetch_consumer_credentials()
    print("    OK")

    ticket = capture_sso_ticket(timeout_seconds=args.timeout)
    if not ticket:
        sys.exit(
            "\nERROR: No SSO ticket captured.\n"
            "       Login timed out or was cancelled."
        )
    print(f"\n==> SSO ticket captured.")

    print("==> Exchanging for OAuth tokens...")
    try:
        oauth1 = exchange_oauth1(ticket, consumer_key, consumer_secret)
        print("    OAuth1: OK")
    except Exception as e:
        sys.exit(f"ERROR: OAuth1 exchange failed: {e}")

    try:
        oauth2 = exchange_oauth2(oauth1, consumer_key, consumer_secret)
        print("    OAuth2: OK")
    except Exception as e:
        sys.exit(f"ERROR: OAuth2 exchange failed: {e}")

    print(f"\n==> Tokens saved to {args.output}/")
    save_tokens(args.output, oauth1, oauth2)

    abs_output = str(Path(args.output).resolve())
    print("\n==> Done.")
    print("    If you ran this locally, copy tokens to the server:")
    print(f"    scp {abs_output}/oauth*.json root@<server>:/opt/<username>/garminconnect-tokens/")
    print()
    print("    Fix ownership (garmin-fetch-data runs as uid 1000, not root):")
    print("    chown 1000:1000 /opt/<username>/garminconnect-tokens/oauth*.json")
    print()
    print("    Then restart garmin-fetch-data:")
    print("    ./deploy/stack.sh <username> down garmin-fetch-data")
    print("    ./deploy/stack.sh <username> up")


if __name__ == "__main__":
    main()
