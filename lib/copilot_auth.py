"""GitHub Copilot API authentication for AI Secretary.

Uses the GitHub Device Flow to obtain an OAuth token, then exchanges it
for a Copilot API token usable with the OpenAI-compatible chat/completions
endpoint at api.individual.githubcopilot.com.

Based on OpenClaw (https://github.com/openclaw/openclaw).
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import time
import webbrowser
from pathlib import Path
from typing import Any

import httpx

log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────

COPILOT_CLIENT_ID = "Iv1.b507a08c87ecfe98"

DEVICE_CODE_URL = "https://github.com/login/device/code"
ACCESS_TOKEN_URL = "https://github.com/login/oauth/access_token"
COPILOT_TOKEN_URL = "https://api.github.com/copilot_internal/v2/token"
DEFAULT_COPILOT_API_BASE_URL = "https://api.individual.githubcopilot.com"

COPILOT_HEADERS = {
    "Editor-Version": "vscode/1.96.2",
    "Editor-Plugin-Version": "copilot-chat/0.26.7",
    "User-Agent": "GitHubCopilotChat/0.26.7",
    "Copilot-Integration-Id": "vscode-chat",
}

# Cache in user_state/ directory (consistent with AI Secretary layout)
_DATA_DIR = Path(__file__).resolve().parent.parent / "user_state"
_GITHUB_TOKEN_CACHE = _DATA_DIR / "copilot-github-token.json"
_COPILOT_TOKEN_CACHE = _DATA_DIR / "copilot-token-cache.json"


# ── Token cache ────────────────────────────────────────────

def _load_cached_copilot_token() -> dict[str, Any] | None:
    try:
        if _COPILOT_TOKEN_CACHE.exists():
            data = json.loads(_COPILOT_TOKEN_CACHE.read_text())
            if data.get("expires_at_ms", 0) - time.time() * 1000 > 5 * 60 * 1000:
                return data
    except Exception:
        pass
    return None


def _save_cached_copilot_token(data: dict[str, Any]) -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    _COPILOT_TOKEN_CACHE.write_text(json.dumps(data, indent=2))


def _load_cached_github_token() -> str | None:
    try:
        if _GITHUB_TOKEN_CACHE.exists():
            data = json.loads(_GITHUB_TOKEN_CACHE.read_text())
            max_age_ms = 30 * 24 * 60 * 60 * 1000
            if data.get("token") and (time.time() * 1000 - data.get("created_at", 0)) < max_age_ms:
                return data["token"]
    except Exception:
        pass
    return None


def _save_cached_github_token(token: str) -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    _GITHUB_TOKEN_CACHE.write_text(json.dumps({
        "token": token,
        "created_at": int(time.time() * 1000),
    }, indent=2))


# ── URL derivation ─────────────────────────────────────────

def derive_base_url_from_token(token: str) -> str:
    match = re.search(r"(?:^|;)\s*proxy-ep=([^;\s]+)", token, re.IGNORECASE)
    if not match:
        return DEFAULT_COPILOT_API_BASE_URL
    host = re.sub(r"^https?://", "", match.group(1).strip())
    host = re.sub(r"^proxy\.", "api.", host, flags=re.IGNORECASE)
    return f"https://{host}"


# ── GitHub token acquisition ──────────────────────────────

def _get_github_token_from_env() -> str | None:
    import os
    for var in ("COPILOT_GITHUB_TOKEN", "GITHUB_TOKEN"):
        val = os.environ.get(var)
        if val:
            return val
    try:
        result = subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return None


# ── Device Flow login ──────────────────────────────────────

def login_device_flow() -> str:
    with httpx.Client(timeout=15) as http:
        resp = http.post(
            DEVICE_CODE_URL,
            data={"client_id": COPILOT_CLIENT_ID, "scope": "read:user copilot"},
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        device = resp.json()

        user_code = device["user_code"]
        verification_uri = device["verification_uri"]
        device_code = device["device_code"]
        interval = max(device.get("interval", 5), 1)
        expires_in = device.get("expires_in", 900)

        print("\n" + "=" * 55)
        print(f"  Open:  {verification_uri}")
        print(f"  Code:  {user_code}")
        print("=" * 55 + "\n")

        try:
            webbrowser.open(verification_uri)
            print("(Browser opened automatically)")
        except Exception:
            print("Please open the link above manually.")

        print("\nWaiting for authorisation...")

        deadline = time.time() + expires_in
        while time.time() < deadline:
            time.sleep(interval)
            resp = http.post(
                ACCESS_TOKEN_URL,
                data={
                    "client_id": COPILOT_CLIENT_ID,
                    "device_code": device_code,
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                },
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()

            if "access_token" in data:
                token = data["access_token"]
                _save_cached_github_token(token)
                print("\nGitHub login successful! Token saved.\n")
                return token

            error = data.get("error", "unknown")
            if error == "authorization_pending":
                continue
            if error == "slow_down":
                interval += 2
                continue
            if error in ("expired_token", "access_denied"):
                raise RuntimeError(f"Device flow failed: {error}")
            raise RuntimeError(f"Device flow unexpected error: {error}")

        raise RuntimeError("Device code expired - please try again.")


# ── Copilot token exchange ─────────────────────────────────

def _exchange_copilot_token(github_token: str) -> dict[str, Any]:
    with httpx.Client(timeout=15) as http:
        resp = http.get(
            COPILOT_TOKEN_URL,
            headers={
                "Accept": "application/json",
                "Authorization": f"token {github_token}",
                **COPILOT_HEADERS,
            },
        )
        resp.raise_for_status()
        data = resp.json()

    if "token" not in data:
        raise RuntimeError("Copilot token response missing 'token' field")

    expires_at = data.get("expires_at", 0)
    expires_at_ms = expires_at * 1000 if expires_at < 10_000_000_000 else expires_at

    base_url = derive_base_url_from_token(data["token"])

    cached = {
        "token": data["token"],
        "expires_at_ms": expires_at_ms,
        "updated_at_ms": int(time.time() * 1000),
        "base_url": base_url,
    }
    _save_cached_copilot_token(cached)
    return cached


# ── Public API ─────────────────────────────────────────────

def get_copilot_credentials(interactive: bool = False) -> dict[str, str] | None:
    """Get Copilot API credentials: {"token": ..., "base_url": ...}."""
    # 1. Cached Copilot token
    cached = _load_cached_copilot_token()
    if cached:
        return {"token": cached["token"], "base_url": cached["base_url"]}

    # 2. Get a GitHub token
    github_token = _load_cached_github_token() or _get_github_token_from_env()

    if not github_token and interactive:
        github_token = login_device_flow()

    if not github_token:
        return None

    # 3. Exchange for Copilot token
    try:
        exchanged = _exchange_copilot_token(github_token)
        return {"token": exchanged["token"], "base_url": exchanged["base_url"]}
    except Exception as exc:
        log.error("Copilot token exchange failed: %s", exc)
        return None
