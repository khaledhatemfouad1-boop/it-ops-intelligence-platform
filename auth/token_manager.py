"""
ManageEngine ServiceDesk Plus On-Demand – OAuth2 Token Manager
Region: Global (.com)  →  accounts.zoho.com
SDP URL:  https://sdpondemand.manageengine.com

FIRST-TIME SETUP:
1. Go to: https://api-console.zoho.com/
2. Create a Self Client → note Client ID + Client Secret
3. Generate code with scope:  SDPOnDemand.requests.READ
4. Run:  python auth/token_manager.py --init
   Paste your code when prompted → refresh token saved to .env

After that, all other scripts call TokenManager().get_access_token() automatically.
"""

import os
import json
import time
import argparse
import requests
from pathlib import Path
from dotenv import load_dotenv, set_key

load_dotenv()

TOKEN_URL   = "https://accounts.zoho.com/oauth/v2/token"
ENV_FILE    = Path(__file__).resolve().parent.parent / ".env"

# Token cache (in-memory between calls in the same process)
_cache: dict = {"access_token": None, "expires_at": 0}


class TokenManager:
    def __init__(self):
        self.client_id     = os.getenv("SDP_CLIENT_ID")
        self.client_secret = os.getenv("SDP_CLIENT_SECRET")
        self.refresh_token = os.getenv("SDP_REFRESH_TOKEN")

        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise EnvironmentError(
                "Missing SDP_CLIENT_ID / SDP_CLIENT_SECRET / SDP_REFRESH_TOKEN in .env\n"
                "Run:  python auth/token_manager.py --init"
            )

    def get_access_token(self) -> str:
        """Return a valid access token, refreshing if within 60 s of expiry."""
        if _cache["access_token"] and time.time() < _cache["expires_at"] - 60:
            return _cache["access_token"]

        resp = requests.post(TOKEN_URL, data={
            "grant_type":    "refresh_token",
            "client_id":     self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            raise ValueError(f"Token refresh failed: {data}")

        _cache["access_token"] = data["access_token"]
        _cache["expires_at"]   = time.time() + data.get("expires_in", 3600)
        return _cache["access_token"]


# ──────────────────────────────────────────────────────────────────────────────
# First-time setup helper
# ──────────────────────────────────────────────────────────────────────────────
def _init_tokens():
    print("\n=== ManageEngine SDP – First-Time OAuth Setup ===\n")
    client_id     = input("Paste your Client ID:      ").strip()
    client_secret = input("Paste your Client Secret:  ").strip()
    code          = input("Paste your OAuth code (you have 3 min!): ").strip()

    resp = requests.post(TOKEN_URL, data={
        "grant_type":    "authorization_code",
        "client_id":     client_id,
        "client_secret": client_secret,
        "code":          code,
        "redirect_uri":  "https://www.zoho.com",   # must match what you set in console
    }, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        print(f"\n❌  Error: {data}")
        return

    refresh_token = data.get("refresh_token")
    if not refresh_token:
        print(f"\n❌  No refresh_token returned. Full response:\n{json.dumps(data, indent=2)}")
        return

    # Save to .env
    ENV_FILE.touch()
    set_key(str(ENV_FILE), "SDP_CLIENT_ID",     client_id)
    set_key(str(ENV_FILE), "SDP_CLIENT_SECRET", client_secret)
    set_key(str(ENV_FILE), "SDP_REFRESH_TOKEN", refresh_token)

    print(f"\n✅  Refresh token saved to {ENV_FILE}")
    print("    You can now run the producer: python producers/sdp_producer.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true", help="First-time token setup")
    args = parser.parse_args()

    if args.init:
        _init_tokens()
    else:
        tm = TokenManager()
        token = tm.get_access_token()
        print(f"✅  Valid access token: {token[:20]}…")
