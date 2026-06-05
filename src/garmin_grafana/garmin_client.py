from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any

import requests
from garth.exc import GarthHTTPError
from garminconnect import Garmin, GarminConnectAuthenticationError


def _sanitize_token_timestamps(token_dir: str) -> None:
    """Cast float timestamps to int — pydantic OAuth2Token requires strict int."""
    token_file = os.path.join(token_dir, "oauth2_token.json")
    if not os.path.exists(token_file):
        return
    with open(token_file) as f:
        data = json.load(f)
    changed = False
    for key in ("expires_at", "refresh_token_expires_at"):
        if isinstance(data.get(key), float):
            data[key] = int(data[key])
            changed = True
    if changed:
        with open(token_file, "w") as f:
            json.dump(data, f, indent=2)


def garmin_login(
    *,
    token_dir: str,
    garminconnect_email: str | None,
    garminconnect_password: str | None,
    garminconnect_is_cn: bool,
) -> Any:
    """
    Login helper that preserves the original script behavior:
    - Prefer stored tokens in token_dir
    - Fall back to interactive credential prompt (and MFA) if needed
    - Dump tokens and exit(0) after successful interactive login
    """
    try:
        logging.info(f"Trying to login to Garmin Connect using token data from directory '{token_dir}'...")
        _sanitize_token_timestamps(token_dir)
        garmin = Garmin()
        garmin.login(token_dir)
        logging.info("login to Garmin Connect successful using stored session tokens.")
    except (FileNotFoundError, GarthHTTPError, GarminConnectAuthenticationError):
        logging.warning(
            "Session is expired or login information not present/incorrect. You'll need to log in again...login with your Garmin Connect credentials to generate them."
        )
        try:
            user_email = garminconnect_email or input("Enter Garminconnect Login e-mail: ")
            user_password = garminconnect_password or input("Enter Garminconnect password (characters will be visible): ")
            garmin = Garmin(email=user_email, password=user_password, is_cn=garminconnect_is_cn, return_on_mfa=True)
            result1, result2 = garmin.login()
            if result1 == "needs_mfa":
                mfa_code = input("MFA one-time code (via email or SMS): ")
                garmin.resume_login(result2, mfa_code)

            garmin.garth.dump(token_dir)
            logging.info(f"Oauth tokens stored in '{token_dir}' directory for future use")

            garmin.login(token_dir)
            logging.info(
                "login to Garmin Connect successful using stored session tokens. Please restart the script. Saved logins will be used automatically"
            )
            sys.exit(0)

        except (FileNotFoundError, GarthHTTPError, GarminConnectAuthenticationError, requests.exceptions.HTTPError) as err:
            logging.error(str(err))
            raise Exception("Session is expired : please login again and restart the script")

    return garmin

