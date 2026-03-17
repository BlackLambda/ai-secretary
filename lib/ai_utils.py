from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, Iterable

from azure.identity import AzureCliCredential, get_bearer_token_provider
from openai import AzureOpenAI


def supports_json_response_format(model: str | None = None) -> bool:
    """Return True if the model supports response_format={'type':'json_object'}.

    Claude models (used via Copilot backend) do not support this parameter.
    """
    name = (model or "").lower() or DEPLOYMENT_NAME.lower()
    return "claude" not in name


def _log_ai_error(message: str) -> None:
    """Append an AI error entry to user_state/ai_errors.jsonl (best-effort)."""
    import json as _json
    from pathlib import Path as _Path

    print(f"[AI_ERROR] {message}", flush=True)
    try:
        log_path = _Path(__file__).resolve().parent.parent / "user_state" / "ai_errors.jsonl"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        entry = _json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message": message,
        })
        with log_path.open("a", encoding="utf-8") as f:
            f.write(entry + "\n")
    except Exception:
        pass


def ai_chat_json(
    client,
    messages: list,
    *,
    temperature: float = 0.1,
    timeout: float | None = None,
) -> dict:
    """Centralized JSON chat completion — single place for all AI JSON calls.

    - Automatically omits ``response_format`` for Claude models (which don't support it).
    - Strips ``\\`\\`\\`json`` code-fence wrappers before parsing.
    - On failure, writes to ``user_state/ai_errors.jsonl`` (visible in the UI) and raises.
    """
    import json as _json
    import re as _re

    model = DEPLOYMENT_NAME
    kwargs: dict = dict(
        model=model,
        messages=messages,
        temperature=temperature,
        timeout=timeout or AZURE_OPENAI_TIMEOUT_SECONDS,
    )
    if supports_json_response_format(model):
        kwargs["response_format"] = {"type": "json_object"}

    try:
        resp = client.chat.completions.create(**kwargs)
        content = (resp.choices[0].message.content or "").strip()
    except Exception as exc:
        _log_ai_error(f"API call failed (model={model}): {exc}")
        raise

    # Strip code fences if present (some models wrap JSON in ```json ... ```)
    text = _re.sub(r"^```(?:json)?\s*\n?", "", content)
    text = _re.sub(r"\n?```\s*$", "", text).strip()

    if not text:
        _log_ai_error(f"Empty response from model={model}")
        raise ValueError("AI returned an empty response")

    try:
        return _json.loads(text)
    except Exception as exc:
        _log_ai_error(
            f"JSON parse failed (model={model}): {exc}. "
            f"Response snippet: {content[:200]!r}"
        )
        raise ValueError(f"AI response was not valid JSON: {exc}") from exc


@dataclass(frozen=True)
class DeadlineDrop:
    item: dict
    deadline_raw: Any
    deadline_dt: datetime | None


def _best_item_text(item: dict) -> str:
    for k in ("task", "description", "title", "name"):
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def summarize_deadline_drop(drop: DeadlineDrop, *, max_text_len: int = 140) -> str:
    """Return a short, stable log string describing a dropped item."""
    text = _best_item_text(drop.item)
    if text and len(text) > max_text_len:
        text = text[: max_text_len - 1] + "…"
    raw = drop.deadline_raw
    raw_s = raw.strip() if isinstance(raw, str) else repr(raw)
    if len(raw_s) > 80:
        raw_s = raw_s[:79] + "…"
    dt_s = drop.deadline_dt.isoformat() if isinstance(drop.deadline_dt, datetime) else ""
    if text:
        return f"{text} (deadline={raw_s}, parsed={dt_s})"
    return f"(deadline={raw_s}, parsed={dt_s})"


def _local_tzinfo():
    return datetime.now().astimezone().tzinfo


def _parse_deadline_str(s: str, *, now: datetime) -> datetime | None:
    if not s:
        return None

    # Date-only: YYYY-MM-DD (treat as end-of-day local time)
    try:
        d = date.fromisoformat(s)
        return datetime.combine(d, time(23, 59, 59))
    except Exception:
        pass

    # ISO-8601 (best effort)
    try:
        iso = s
        if iso.endswith("Z"):
            iso = iso[:-1] + "+00:00"
        dt = datetime.fromisoformat(iso)
        return dt
    except Exception:
        pass

    # Common UI formats.
    fmts = [
        "%b %d, %Y, %I:%M %p",
        "%b %d, %Y %I:%M %p",
        "%b %d, %I:%M %p",
        "%b %d %I:%M %p",
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y",
    ]

    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            if "%Y" not in fmt:
                dt = dt.replace(year=now.astimezone(_local_tzinfo()).year)
            if "%H" not in fmt and "%I" not in fmt:
                dt = dt.replace(hour=23, minute=59, second=59)
            return dt
        except Exception:
            continue

    return None


def parse_deadline(deadline: Any, *, now: datetime | None = None) -> datetime | None:
    """Best-effort parse for a task deadline.

    Supported inputs:
    - ISO-8601 strings (with or without timezone; supports trailing 'Z')
    - Common UI strings like 'Jan 21, 08:00 AM' (year inferred from `now`)
    - Date-only strings like '2026-01-21' (treated as end-of-day local time)
    - dicts like {'dateTime': '...', 'timeZone': 'UTC'}

    Returns a timezone-aware datetime when possible; otherwise None.
    """
    if deadline is None:
        return None

    now = now or datetime.now(timezone.utc)

    if isinstance(deadline, datetime):
        dt = deadline
    elif isinstance(deadline, date) and not isinstance(deadline, datetime):
        dt = datetime.combine(deadline, time(23, 59, 59))
    elif isinstance(deadline, (int, float)):
        # Assume epoch seconds.
        try:
            dt = datetime.fromtimestamp(float(deadline), tz=timezone.utc)
        except Exception:
            return None
    elif isinstance(deadline, dict):
        # Microsoft Graph-style
        raw = deadline.get("dateTime") or deadline.get("datetime")
        if not isinstance(raw, str) or not raw.strip():
            return None
        dt = _parse_deadline_str(raw.strip(), now=now)
    elif isinstance(deadline, str):
        dt = _parse_deadline_str(deadline.strip(), now=now)
    else:
        return None

    if dt is None:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_local_tzinfo())

    return dt


def drop_items_with_past_deadlines(
    items: Iterable[Any] | None,
    *,
    now: datetime | None = None,
) -> tuple[list[Any], list[DeadlineDrop]]:
    """Drop todo-like dict items whose deadline is strictly before `now`.

    - Items without a parsable deadline are kept.
    - Uses the 'deadline' key when present.
    """
    if items is None:
        return [], []

    now = now or datetime.now(timezone.utc)
    kept: list[Any] = []
    dropped: list[DeadlineDrop] = []

    for it in list(items):
        if not isinstance(it, dict):
            kept.append(it)
            continue

        raw = it.get("deadline")
        dt = parse_deadline(raw, now=now)
        if dt is None:
            kept.append(it)
            continue

        now_cmp = now
        if now_cmp.tzinfo is None:
            now_cmp = now_cmp.replace(tzinfo=timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_local_tzinfo())

        if dt.astimezone(timezone.utc) < now_cmp.astimezone(timezone.utc):
            dropped.append(DeadlineDrop(item=it, deadline_raw=raw, deadline_dt=dt))
        else:
            kept.append(it)

    return kept, dropped


# Configuration (defaults; can be overridden via environment variables)
_DEFAULT_AZURE_ENDPOINT = "https://your-azure-openai-resource.cognitiveservices.azure.com/"
_DEFAULT_API_VERSION = "2024-12-01-preview"
_DEFAULT_DEPLOYMENT_NAME = "gpt-5.1"
_DEFAULT_AZURE_OPENAI_TIMEOUT_SECONDS = 20.0
_DEFAULT_AZURE_OPENAI_MAX_RETRIES = 0

# Copilot backend defaults
_DEFAULT_COPILOT_MODEL = "gpt-4o"

# AI backend selection: "azure" (default) or "copilot"
AI_BACKEND = (os.environ.get("AI_BACKEND") or "").strip().lower() or None  # resolved lazily


AZURE_ENDPOINT = (os.environ.get("AZURE_OPENAI_ENDPOINT") or "").strip() or _DEFAULT_AZURE_ENDPOINT
API_VERSION = (os.environ.get("AZURE_OPENAI_API_VERSION") or "").strip() or _DEFAULT_API_VERSION
DEPLOYMENT_NAME = (os.environ.get("AZURE_OPENAI_DEPLOYMENT") or "").strip() or _DEFAULT_DEPLOYMENT_NAME


def _read_timeout_seconds() -> float:
    raw = (
        (os.environ.get("AZURE_OPENAI_TIMEOUT_SECONDS") or "").strip()
        or (os.environ.get("AZURE_OPENAI_TIMEOUT") or "").strip()
    )
    if not raw:
        return _DEFAULT_AZURE_OPENAI_TIMEOUT_SECONDS
    try:
        v = float(raw)
    except Exception:
        return _DEFAULT_AZURE_OPENAI_TIMEOUT_SECONDS
    # Hard bounds to avoid accidental infinite/negative timeouts.
    if v < 1:
        return 1.0
    if v > 600:
        return 600.0
    return v


AZURE_OPENAI_TIMEOUT_SECONDS = _read_timeout_seconds()


def _read_max_retries() -> int:
    raw = (os.environ.get("AZURE_OPENAI_MAX_RETRIES") or "").strip()
    if not raw:
        return _DEFAULT_AZURE_OPENAI_MAX_RETRIES
    try:
        v = int(raw)
    except Exception:
        return _DEFAULT_AZURE_OPENAI_MAX_RETRIES
    if v < 0:
        return 0
    if v > 10:
        return 10
    return v


AZURE_OPENAI_MAX_RETRIES = _read_max_retries()


def _ensure_azure_cli_available() -> None:
    if shutil.which("az"):
        return

    # Historically this project prepended the default CLI install path on Windows.
    # We avoid mutating PATH now; instead provide an actionable error.
    hint = ""
    if os.name == "nt":
        hint = (
            "\n[HINT] On Windows, ensure Azure CLI is installed and available on PATH. "
            "Common install path: C:\\Program Files\\Microsoft SDKs\\Azure\\CLI2\\wbin"
        )
    raise RuntimeError("Azure CLI ('az') not found on PATH." + hint)

def get_azure_openai_client():
    """
    Initialize and return an OpenAI-compatible client.

    Checks ``AI_BACKEND`` env var and ``pipeline_config.json`` to decide:
      - ``"azure"`` (default): AzureOpenAI via ``az login`` credential.
      - ``"copilot"``: OpenAI client backed by GitHub Copilot API token.

    All callers use the standard ``client.chat.completions.create(model=DEPLOYMENT_NAME, ...)``
    interface regardless of which backend is active.
    """
    backend = _resolve_ai_backend()
    if backend == "copilot":
        return _get_copilot_openai_client()
    return _get_azure_openai_client()


def _resolve_ai_backend() -> str:
    """Return the active AI backend: 'azure' or 'copilot'."""
    global AI_BACKEND, DEPLOYMENT_NAME

    # 1. Env var takes priority
    if AI_BACKEND:
        return AI_BACKEND

    # 2. Check pipeline config
    try:
        import json as _json
        from pathlib import Path as _Path
        cfg_path = _Path(__file__).resolve().parent.parent / "config" / "pipeline_config.json"
        if cfg_path.exists():
            with cfg_path.open("r", encoding="utf-8") as f:
                cfg = _json.load(f) or {}
            val = (str(cfg.get("ai_backend", "")) or "").strip().lower()
            if val in ("copilot", "azure"):
                # Pick up model override from config
                if val == "copilot":
                    model_override = (str(cfg.get("copilot_model", "")) or "").strip()
                    if model_override:
                        DEPLOYMENT_NAME = model_override
                    elif DEPLOYMENT_NAME == _DEFAULT_DEPLOYMENT_NAME:
                        DEPLOYMENT_NAME = _DEFAULT_COPILOT_MODEL
                else:  # azure
                    model_override = (str(cfg.get("azure_model", "")) or "").strip()
                    if model_override:
                        DEPLOYMENT_NAME = model_override
                return val
    except Exception:
        pass

    return "azure"


def _get_copilot_openai_client():
    """Return an ``openai.OpenAI`` client configured for the GitHub Copilot API."""
    try:
        from .copilot_auth import get_copilot_credentials, COPILOT_HEADERS
        from openai import OpenAI

        creds = get_copilot_credentials(interactive=False)
        if creds is None:
            raise RuntimeError(
                "GitHub Copilot credentials not available. "
                "Run the Copilot login from the dashboard onboarding wizard, "
                "or set COPILOT_GITHUB_TOKEN / GITHUB_TOKEN env var."
            )

        # Build httpx client with Copilot-required headers baked in
        import httpx
        timeout_val = float(AZURE_OPENAI_TIMEOUT_SECONDS)
        timeout_opt = httpx.Timeout(
            timeout=timeout_val,
            connect=min(10.0, timeout_val),
            read=timeout_val,
            write=min(10.0, timeout_val),
            pool=min(10.0, timeout_val),
        )
        http_client = httpx.Client(
            timeout=timeout_opt,
            headers=COPILOT_HEADERS,
        )

        client = OpenAI(
            api_key=creds["token"],
            base_url=creds["base_url"] + "/chat/completions/..",  # openai lib appends /chat/completions
            http_client=http_client,
            max_retries=AZURE_OPENAI_MAX_RETRIES,
        )

        # The openai SDK needs base_url to point at the root so it can append /chat/completions.
        # Copilot base_url is already the root (e.g. https://api.individual.githubcopilot.com).
        client.base_url = creds["base_url"]

        print(
            f"[AI] Copilot OpenAI client ready (model={DEPLOYMENT_NAME}, "
            f"timeout={AZURE_OPENAI_TIMEOUT_SECONDS}s)"
        )
        return client
    except Exception as e:
        print(f"[ERROR] Failed to initialize Copilot OpenAI client: {e}")
        raise


def _get_azure_openai_client():
    """
    Initialize and return an Azure OpenAI client using Azure CLI authentication.
    Requires Azure CLI authentication (run `az login`).

    Environment overrides:
      - AZURE_OPENAI_ENDPOINT
      - AZURE_OPENAI_API_VERSION
      - AZURE_OPENAI_DEPLOYMENT
    """
    try:
        _ensure_azure_cli_available()
        credential = AzureCliCredential()
        token_provider = get_bearer_token_provider(
            credential,
            "https://cognitiveservices.azure.com/.default"
        )

        # Enforce timeouts at the transport layer.
        http_client = None
        timeout_opt = AZURE_OPENAI_TIMEOUT_SECONDS
        try:
            import httpx  # openai>=1 uses httpx

            connect_timeout = min(10.0, float(AZURE_OPENAI_TIMEOUT_SECONDS))
            write_timeout = min(10.0, float(AZURE_OPENAI_TIMEOUT_SECONDS))
            pool_timeout = min(10.0, float(AZURE_OPENAI_TIMEOUT_SECONDS))
            read_timeout = float(AZURE_OPENAI_TIMEOUT_SECONDS)

            timeout_opt = httpx.Timeout(
                timeout=float(AZURE_OPENAI_TIMEOUT_SECONDS),
                connect=connect_timeout,
                read=read_timeout,
                write=write_timeout,
                pool=pool_timeout,
            )
            http_client = httpx.Client(timeout=timeout_opt)
        except Exception:
            # Fall back to the SDK default transport.
            http_client = None
            timeout_opt = AZURE_OPENAI_TIMEOUT_SECONDS

        client = AzureOpenAI(
            api_version=API_VERSION,
            azure_endpoint=AZURE_ENDPOINT,
            azure_ad_token_provider=token_provider,
            timeout=timeout_opt,
            max_retries=AZURE_OPENAI_MAX_RETRIES,
            http_client=http_client,
        )
        print(
            f"[AI] AzureOpenAI client ready (timeout={AZURE_OPENAI_TIMEOUT_SECONDS}s, max_retries={AZURE_OPENAI_MAX_RETRIES})"
        )
        return client
    except Exception as e:
        print(f"[ERROR] Failed to initialize Azure OpenAI client: {e}")
        print("Please ensure you are logged in with 'az login'")
        raise e
