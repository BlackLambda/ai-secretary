"""Analyze recent focus (projects/topics) from user activity.

This script leverages the existing `SubstrateDataExtraction/` tooling to fetch:
- Teams messages (then filters to messages sent by the user)
- Sent emails
- Calendar events (proxy for meetings joined)

Then it uses Azure OpenAI (via `ai_utils.get_azure_openai_client`) to infer the
most focused projects/topics over the chosen time window.

Usage examples:
  python analyze_recent_focus.py --days 7
  python analyze_recent_focus.py --days 14 --max-teams 400 --max-emails 300 --max-meetings 200
  python analyze_recent_focus.py --days 7 --no-fetch   # analyze existing Substrate output only

Outputs:
  - JSON report (default: incremental_data/output/recent_focus.json)
  - Optional markdown summary via --md
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import subprocess
import sys
import importlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

ai_utils = None  # lazy import; populated in main after env is configured

# Progress tracking: write current stage to a sidecar file so the UI can poll.
_PROGRESS_PATH: Optional[str] = None  # set in main() based on --output

def _write_progress(step: int, total: int, label: str) -> None:
    """Best-effort write current progress to a sidecar JSON file."""
    if not _PROGRESS_PATH:
        return
    try:
        _write_json(_PROGRESS_PATH, {
            "step": step,
            "total": total,
            "label": label,
            "percent": round(step / total * 100) if total else 0,
            "ts": _utcnow().isoformat(),
        })
    except Exception:
        pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso_z(dt: datetime, end_of_day: bool = False) -> str:
    # Match SubstrateDataExtraction `main.py calendar` examples.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
    else:
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    # Milliseconds precision + trailing Z.
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parse_dt(value: str) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    v = value.strip()
    if not v:
        return None

    # Common Microsoft formats:
    # - 2026-01-21T06:36:34Z
    # - 2025-01-01T00:00:00.000Z
    # - 2026-01-21T06:36:34+00:00
    try:
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        return datetime.fromisoformat(v)
    except Exception:
        return None


def _read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _write_text(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def _run_py(cwd: str, args: List[str]) -> None:
    """Run a python command in a given working directory."""
    cmd = [sys.executable, *args]
    print(f"[RUN] (cwd={cwd}) {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd, check=True)


@dataclass
class ActivityItem:
    kind: str  # teams|email|meeting
    when: str  # ISO string
    title: str
    snippet: str
    participants: List[str]
    source_id: str


def _norm_email(addr: str) -> str:
    return (addr or "").strip().lower()


def _email_from_obj(message: Dict[str, Any], field: str) -> str:
    obj = message.get(field) or {}
    email = obj.get("EmailAddress") or obj.get("emailAddress") or {}
    return (email.get("Address") or email.get("address") or "").strip()


def _recipients(message: Dict[str, Any], field: str) -> List[str]:
    out: List[str] = []
    for r in (message.get(field) or message.get(field.lower()) or [])[:50]:
        email = r.get("EmailAddress") or r.get("emailAddress") or {}
        addr = (email.get("Address") or email.get("address") or "").strip()
        name = (email.get("Name") or email.get("name") or "").strip()
        if addr and name:
            out.append(f"{name} <{addr}>")
        elif addr:
            out.append(addr)
        elif name:
            out.append(name)
    return out


def _clip(text: str, limit: int) -> str:
    if not text:
        return ""
    t = " ".join(text.split())
    return t if len(t) <= limit else (t[: max(0, limit - 1)] + "…")


def _load_my_upn(substrate_output_dir: str) -> str:
    profile_path = os.path.join(substrate_output_dir, "user_profile.json")
    if not os.path.exists(profile_path):
        raise FileNotFoundError(
            f"Missing {profile_path}. Run SubstrateDataExtraction/get_user_profile.py first."
        )
    profile = _read_json(profile_path)
    upn = (profile.get("upn") or "").strip()
    if not upn:
        raise RuntimeError(f"Could not read 'upn' from {profile_path}")
    return upn


def _load_my_display_name(substrate_output_dir: str) -> str:
    profile_path = os.path.join(substrate_output_dir, "user_profile.json")
    if not os.path.exists(profile_path):
        return ""
    try:
        profile = _read_json(profile_path)
    except Exception:
        return ""
    formatted = profile.get("formatted_profile") or {}
    name = (formatted.get("Name") or formatted.get("name") or "").strip()
    return name


def _iter_email_identities(obj: Any) -> List[Tuple[str, str]]:
    """Yield (name, address) pairs found in an arbitrary M365 payload."""
    out: List[Tuple[str, str]] = []
    if isinstance(obj, dict):
        if "EmailAddress" in obj and isinstance(obj["EmailAddress"], dict):
            name = (obj["EmailAddress"].get("Name") or "").strip()
            addr = (obj["EmailAddress"].get("Address") or "").strip()
            if name or addr:
                out.append((name, addr))
        if "emailAddress" in obj and isinstance(obj["emailAddress"], dict):
            name = (obj["emailAddress"].get("name") or "").strip()
            addr = (obj["emailAddress"].get("address") or "").strip()
            if name or addr:
                out.append((name, addr))

        for v in obj.values():
            out.extend(_iter_email_identities(v))
    elif isinstance(obj, list):
        for it in obj:
            out.extend(_iter_email_identities(it))
    return out


def _discover_my_addresses(
    substrate_output_dir: str,
    teams_path: Optional[str] = None,
    emails_path: Optional[str] = None,
    calendar_path: Optional[str] = None,
) -> Tuple[str, List[str]]:
    """Return (display_name, addresses[]) for the current user.

    Substrate outputs sometimes use different primary addresses for the same user
    (e.g. alias@ vs First.Last@). We infer plausible variants by scanning payloads
    for an identity matching the user's display name.
    """
    upn = _load_my_upn(substrate_output_dir)
    display_name = _load_my_display_name(substrate_output_dir)

    addresses = {_norm_email(upn)}

    # Best-effort heuristic: scan artifacts for entries whose Name matches.
    scan_paths = [p for p in [teams_path, emails_path, calendar_path] if p and os.path.exists(p)]
    for p in scan_paths:
        try:
            payload = _read_json(p)
        except Exception:
            continue
        for name, addr in _iter_email_identities(payload):
            if not addr:
                continue
            if display_name and name and name.strip().lower() == display_name.strip().lower():
                addresses.add(_norm_email(addr))

    # Also add a common pattern if we can derive it.
    if display_name and " " in display_name and upn.endswith("@microsoft.com"):
        parts = [p for p in display_name.split(" ") if p]
        if len(parts) >= 2:
            first = parts[0]
            last = parts[-1]
            addresses.add(_norm_email(f"{first}.{last}@microsoft.com"))

    return display_name, sorted(a for a in addresses if a)


def _collect_teams_sent(
    teams_path: str,
    my_addresses: List[str],
    my_display_name: str,
    window_start: datetime,
    window_end: datetime,
    max_items: int,
    min_my_msgs_per_thread: int,
) -> List[ActivityItem]:
    data = _read_json(teams_path)
    items: List[ActivityItem] = []

    # First pass: count my sent messages per thread in-window.
    my_counts: Dict[str, int] = {}
    for m in data.get("messages", []):
        sent_dt = _parse_dt(m.get("SentDateTime") or m.get("sentDateTime") or m.get("CreatedDateTime"))
        if not sent_dt:
            continue
        if sent_dt.tzinfo is None:
            sent_dt = sent_dt.replace(tzinfo=timezone.utc)
        if sent_dt < window_start or sent_dt > window_end:
            continue

        sender_addr = _email_from_obj(m, "From") or _email_from_obj(m, "Sender")
        sender_name = (
            (m.get("From") or {}).get("EmailAddress", {}).get("Name")
            or (m.get("from") or {}).get("emailAddress", {}).get("name")
            or ""
        ).strip()

        sender_ok = _norm_email(sender_addr) in {_norm_email(a) for a in my_addresses}
        if not sender_ok and my_display_name and sender_name:
            sender_ok = sender_name.lower() == my_display_name.lower()
        if not sender_ok:
            continue

        thread = (m.get("ClientThreadId") or m.get("ClientConversationId") or "").strip() or (m.get("ConversationId") or "")
        if not thread:
            continue
        my_counts[thread] = my_counts.get(thread, 0) + 1

    active_threads = {t for t, c in my_counts.items() if c >= max(1, min_my_msgs_per_thread)}

    # Second pass: emit evidence items (only my messages in active threads).
    for m in data.get("messages", [])[: max(0, max_items) * 10]:
        sent_dt = _parse_dt(m.get("SentDateTime") or m.get("sentDateTime") or m.get("CreatedDateTime"))
        if not sent_dt:
            continue
        if sent_dt.tzinfo is None:
            sent_dt = sent_dt.replace(tzinfo=timezone.utc)
        if sent_dt < window_start or sent_dt > window_end:
            continue

        thread = (m.get("ClientThreadId") or m.get("ClientConversationId") or "").strip() or (m.get("ConversationId") or "")
        if thread not in active_threads:
            continue

        sender_addr = _email_from_obj(m, "From") or _email_from_obj(m, "Sender")
        sender_name = (
            (m.get("From") or {}).get("EmailAddress", {}).get("Name")
            or (m.get("from") or {}).get("emailAddress", {}).get("name")
            or ""
        ).strip()

        sender_ok = _norm_email(sender_addr) in {_norm_email(a) for a in my_addresses}
        if not sender_ok and my_display_name and sender_name:
            sender_ok = sender_name.lower() == my_display_name.lower()
        if not sender_ok:
            continue

        preview = m.get("BodyPreview") or m.get("bodyPreview") or ""
        to_list = _recipients(m, "ToRecipients")
        topic = (m.get("ChatTopic") or "").strip()

        title = topic or (m.get("Subject") or "Teams message")
        items.append(
            ActivityItem(
                kind="teams",
                when=sent_dt.astimezone(timezone.utc).isoformat(),
                title=_clip(title, 120),
                snippet=_clip(preview, 300),
                participants=to_list,
                source_id=thread or (m.get("Id") or ""),
            )
        )
        if len(items) >= max_items:
            break

    return items


def _collect_emails_sent(
    emails_path: str,
    my_addresses: List[str],
    my_display_name: str,
    window_start: datetime,
    window_end: datetime,
    max_items: int,
) -> List[ActivityItem]:
    data = _read_json(emails_path)
    items: List[ActivityItem] = []

    for e in data.get("emails", [])[: max(0, max_items) * 5]:
        sent_dt = _parse_dt(e.get("SentDateTime") or e.get("sentDateTime") or e.get("CreatedDateTime"))
        if not sent_dt:
            continue
        if sent_dt.tzinfo is None:
            sent_dt = sent_dt.replace(tzinfo=timezone.utc)

        if sent_dt < window_start or sent_dt > window_end:
            continue

        sender = _email_from_obj(e, "From") or _email_from_obj(e, "Sender")
        sender_name = (
            (e.get("From") or {}).get("EmailAddress", {}).get("Name")
            or (e.get("from") or {}).get("emailAddress", {}).get("name")
            or ""
        ).strip()
        if sender:
            if _norm_email(sender) not in {_norm_email(a) for a in my_addresses}:
                # If pulling from sent items, this is usually redundant, but keep it safe.
                # Still allow name match when address is absent/variant.
                if not (my_display_name and sender_name and sender_name.lower() == my_display_name.lower()):
                    continue

        subject = (e.get("Subject") or e.get("subject") or "(no subject)").strip()
        preview = e.get("BodyPreview") or e.get("bodyPreview") or ""
        recipients = _recipients(e, "ToRecipients") + _recipients(e, "CcRecipients")

        items.append(
            ActivityItem(
                kind="email",
                when=sent_dt.astimezone(timezone.utc).isoformat(),
                title=_clip(subject, 140),
                snippet=_clip(preview, 320),
                participants=recipients[:30],
                source_id=(e.get("Id") or e.get("id") or ""),
            )
        )
        if len(items) >= max_items:
            break

    return items


def _event_subject(ev: Dict[str, Any]) -> str:
    return (ev.get("Subject") or ev.get("subject") or "(no subject)").strip()


def _event_is_meeting(ev: Dict[str, Any]) -> bool:
    # Filter out obvious non-meetings (e.g. birthdays) by requiring attendees or online meeting hint.
    attendees = ev.get("Attendees") or ev.get("attendees") or []
    is_all_day = bool(ev.get("IsAllDay") or ev.get("isAllDay"))
    if is_all_day:
        return False

    # Some payloads include OnlineMeetingUrl / IsOnlineMeeting.
    if ev.get("IsOnlineMeeting") or ev.get("isOnlineMeeting"):
        return True
    if ev.get("OnlineMeetingUrl") or ev.get("onlineMeetingUrl"):
        return True

    return len(attendees) > 0


def _event_my_response(ev: Dict[str, Any]) -> str:
    rs = ev.get("ResponseStatus") or ev.get("responseStatus") or {}
    resp = (rs.get("Response") or rs.get("response") or "").strip()
    return resp


def _event_show_as(ev: Dict[str, Any]) -> str:
    return (ev.get("ShowAs") or ev.get("showAs") or "").strip()


def _event_location_name(ev: Dict[str, Any]) -> str:
    loc = ev.get("Location") or ev.get("location") or {}
    if isinstance(loc, dict):
        name = (loc.get("DisplayName") or loc.get("displayName") or "").strip()
        if name:
            return name
    locs = ev.get("Locations") or ev.get("locations") or []
    if isinstance(locs, list) and locs:
        first = locs[0] if isinstance(locs[0], dict) else {}
        name = (first.get("DisplayName") or first.get("displayName") or "").strip()
        if name:
            return name
    return ""


def _event_organizer_display(ev: Dict[str, Any]) -> str:
    organizer = ev.get("Organizer") or ev.get("organizer") or {}
    if not isinstance(organizer, dict):
        return ""
    org_email = organizer.get("EmailAddress") or organizer.get("emailAddress") or {}
    if not isinstance(org_email, dict):
        return ""
    org_addr = (org_email.get("Address") or org_email.get("address") or "").strip()
    org_name = (org_email.get("Name") or org_email.get("name") or "").strip()
    if org_name and org_addr:
        return f"{org_name} <{org_addr}>"
    return org_addr or org_name


def _event_join_url(ev: Dict[str, Any]) -> str:
    # Common shapes in Outlook/Graph exports.
    online = ev.get("OnlineMeeting") or ev.get("onlineMeeting") or {}
    if isinstance(online, dict):
        url = (online.get("JoinUrl") or online.get("joinUrl") or "").strip()
        if url:
            return url
    url = (ev.get("OnlineMeetingUrl") or ev.get("onlineMeetingUrl") or "").strip()
    if url:
        return url
    return ""


def _shorten_url(url: str) -> str:
    if not url:
        return ""
    u = url.strip()
    m = re.match(r"^(https?://[^/]+)(/.*)?$", u, flags=re.IGNORECASE)
    if not m:
        return _clip(u, 80)
    host = m.group(1)
    return host + "/…"


def _looks_like_invite_header(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return True
    if t.startswith("____"):
        return True
    # Many recurring invites have an email header block.
    head = t[:200]
    if "\nfrom:" in head or head.startswith("from:"):
        return True
    if "\nsent:" in head or "\nto:" in head:
        return True
    return False


def _html_to_text(value: str) -> str:
    if not value:
        return ""
    # Fast/robust-enough tag stripping for evidence snippets.
    s = re.sub(r"<script[\s\S]*?</script>", " ", value, flags=re.IGNORECASE)
    s = re.sub(r"<style[\s\S]*?</style>", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    s = " ".join(s.split())
    return s


def _event_content_snippet(ev: Dict[str, Any]) -> str:
    preview = (ev.get("BodyPreview") or ev.get("bodyPreview") or "").strip()
    if preview and not _looks_like_invite_header(preview):
        return preview

    # Fall back to HTML body and try to find any non-boilerplate description.
    body = ev.get("Body") or ev.get("body") or {}
    if isinstance(body, dict):
        content = (body.get("Content") or body.get("content") or "").strip()
        if content:
            text = _html_to_text(content)
            if text and not _looks_like_invite_header(text):
                return text

    return preview


def _event_is_teams_or_online_meeting(ev: Dict[str, Any]) -> bool:
    if ev.get("IsOnlineMeeting") or ev.get("isOnlineMeeting"):
        return True
    provider = (ev.get("OnlineMeetingProvider") or ev.get("onlineMeetingProvider") or "").strip().lower()
    if "teams" in provider:
        return True
    join_url = _event_join_url(ev).lower()
    if "teams.microsoft.com" in join_url or "aka.ms/jointeamsmeeting" in join_url:
        return True
    loc = _event_location_name(ev).lower()
    if "teams" in loc:
        return True
    preview = (ev.get("BodyPreview") or ev.get("bodyPreview") or "").lower()
    if "teams.microsoft.com" in preview or "microsoft teams" in preview:
        return True
    return False


def _event_evidence_snippet(ev: Dict[str, Any]) -> str:
    subj = _event_subject(ev)
    resp = _event_my_response(ev) or "(unknown response)"
    show_as = _event_show_as(ev) or "(unknown showAs)"
    organizer = _event_organizer_display(ev)
    loc = _event_location_name(ev)
    join_url = _event_join_url(ev)
    attendees = ev.get("Attendees") or ev.get("attendees") or []
    attendee_count = len(attendees) if isinstance(attendees, list) else 0

    meta_parts: List[str] = []
    if organizer:
        meta_parts.append(f"Organizer: {organizer}")
    if loc:
        meta_parts.append(f"Location: {loc}")
    if join_url:
        meta_parts.append(f"Join: {_shorten_url(join_url)}")
    meta_parts.append(f"ShowAs: {show_as}")
    meta_parts.append(f"Response: {resp}")
    if attendee_count:
        meta_parts.append(f"Attendees: {attendee_count}")

    content = _event_content_snippet(ev)
    content = _clip(content, 220)
    meta = "; ".join(meta_parts)
    if content and not _looks_like_invite_header(content):
        return _clip(f"{meta}. {content}", 320)
    # Even if content is empty/boilerplate, still return meta so evidence is useful.
    return _clip(f"{meta}. {subj}", 320)


def _event_likely_joined(ev: Dict[str, Any]) -> bool:
    # We can't perfectly know attendance without telemetry; use conservative signals.
    if ev.get("IsCancelled") or ev.get("isCancelled"):
        return False
    if ev.get("IsOrganizer") or ev.get("isOrganizer"):
        return True

    resp = _event_my_response(ev)
    if resp.lower() == "declined":
        return False

    show_as = _event_show_as(ev).lower()
    if show_as == "free":
        return False

    # Exclude explicit holds.
    subj = _event_subject(ev).lower()
    body_preview = (ev.get("BodyPreview") or ev.get("bodyPreview") or "").lower()
    if "no need to join" in body_preview or "no need to join" in subj:
        return False

    # Best signal: user explicitly accepted.
    if resp in {"Accepted", "TentativelyAccepted"}:
        return True

    # If user did not respond, many orgs still "attend" without RSVP.
    # Treat it as likely attended only when it looks like a real meeting invite.
    return False


def _event_likely_attended_relaxed(ev: Dict[str, Any], now_utc: datetime) -> bool:
    """Relaxed attendance heuristic.

    Includes meetings that are in the past, not declined/cancelled, not free,
    and appear meeting-like (Teams/attendees).
    """
    if ev.get("IsCancelled") or ev.get("isCancelled"):
        return False
    if ev.get("IsOrganizer") or ev.get("isOrganizer"):
        return True

    resp = _event_my_response(ev)
    if resp.lower() == "declined":
        return False

    show_as = _event_show_as(ev).lower()
    if show_as == "free":
        return False

    subj = _event_subject(ev).lower()
    body_preview = (ev.get("BodyPreview") or ev.get("bodyPreview") or "").lower()
    if "no need to join" in body_preview or "no need to join" in subj:
        return False

    # Must be in the past (ended before now) to count as attended.
    end_obj = ev.get("End") or ev.get("end") or {}
    end_str = end_obj.get("DateTime") or end_obj.get("dateTime") or ""
    end_dt = _parse_dt(end_str)
    if end_dt:
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        if end_dt > now_utc:
            return False

    # If explicitly accepted/tentative, include.
    if resp in {"Accepted", "TentativelyAccepted"}:
        return True

    # Otherwise, require meeting-like signals.
    if not _event_is_meeting(ev):
        return False

    # In relaxed mode, we only count events that look like real online (Teams) meetings.
    # This avoids pulling in webinars/trainings or FYI invites with no join signal.
    if not _event_is_teams_or_online_meeting(ev):
        return False

    # Busy/tentative is stronger than unknown.
    if show_as not in {"busy", "tentative", "oof", "workingelsewhere"}:
        return False

    return True


def _collect_meetings(
    calendar_path: str,
    window_start: datetime,
    window_end: datetime,
    max_items: int,
    attendance_mode: str,
) -> List[ActivityItem]:
    data = _read_json(calendar_path)
    events = data.get("events", [])
    items: List[ActivityItem] = []

    now_utc = _utcnow()
    for ev in events[: max(0, max_items) * 5]:
        if attendance_mode == "strict":
            if not _event_likely_joined(ev):
                continue
            if not _event_is_meeting(ev):
                continue
        else:
            if not _event_likely_attended_relaxed(ev, now_utc):
                continue

        start_obj = ev.get("Start") or ev.get("start") or {}
        start_str = start_obj.get("DateTime") or start_obj.get("dateTime") or ""
        start_dt = _parse_dt(start_str)
        if not start_dt:
            continue
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)

        if start_dt < window_start or start_dt > window_end:
            continue

        attendees = ev.get("Attendees") or ev.get("attendees") or []
        participants: List[str] = []
        for a in attendees[:30]:
            email = a.get("EmailAddress") or a.get("emailAddress") or {}
            addr = (email.get("Address") or email.get("address") or "").strip()
            name = (email.get("Name") or email.get("name") or "").strip()
            if addr and name:
                participants.append(f"{name} <{addr}>")
            elif addr:
                participants.append(addr)
            elif name:
                participants.append(name)

        organizer = ev.get("Organizer") or ev.get("organizer") or {}
        org_email = organizer.get("EmailAddress") or organizer.get("emailAddress") or {}
        org_addr = (org_email.get("Address") or org_email.get("address") or "").strip()
        org_name = (org_email.get("Name") or org_email.get("name") or "").strip()
        if org_addr or org_name:
            participants = ([f"{org_name} <{org_addr}>".strip()] if (org_name and org_addr) else [org_addr or org_name]) + participants

        snippet = _event_evidence_snippet(ev)

        items.append(
            ActivityItem(
                kind="meeting",
                when=start_dt.astimezone(timezone.utc).isoformat(),
                title=_clip(_event_subject(ev), 140),
                snippet=snippet,
                participants=participants,
                source_id=(ev.get("Id") or ev.get("id") or ""),
            )
        )
        if len(items) >= max_items:
            break

    return items





def _build_prompt_payload(
    my_upn: str,
    window_start: datetime,
    window_end: datetime,
    teams: List[ActivityItem],
    emails: List[ActivityItem],
    meetings: List[ActivityItem],
) -> Dict[str, Any]:
    def to_dict(x: ActivityItem) -> Dict[str, Any]:
        return {
            "kind": x.kind,
            "when": x.when,
            "title": x.title,
            "snippet": x.snippet,
            "participants": x.participants[:20],
            "source_id": x.source_id,
        }

    return {
        "user": {"upn": my_upn},
        "window": {
            "start": window_start.astimezone(timezone.utc).isoformat(),
            "end": window_end.astimezone(timezone.utc).isoformat(),
        },
        "activity": {
            "teams_messages_sent": [to_dict(x) for x in teams],
            "emails_sent": [to_dict(x) for x in emails],
            "meetings_joined": [to_dict(x) for x in meetings],
        },
    }


# ---------------------------------------------------------------------------
# Post-process: merge topics whose names are near-duplicates
# ---------------------------------------------------------------------------
def _dedup_similar_topics(topics: List[Dict[str, Any]], threshold: float = 0.55) -> List[Dict[str, Any]]:
    """Merge topics whose names are highly similar (SequenceMatcher ratio >= threshold).

    When two topics are merged the one with the higher score is kept as the
    primary; the other's evidence_items and keywords are appended, and
    the score is bumped to max(a, b).
    """
    from difflib import SequenceMatcher

    if not topics:
        return topics

    def _norm(s: str) -> str:
        """Lower-case, strip parenthetical qualifiers & common noise."""
        s = re.sub(r"\s*\([^)]*\)", "", s)          # remove (…) qualifiers
        s = re.sub(r"\s*[\u2013\u2014–—-]+\s*", " ", s)  # dashes → space
        return s.strip().casefold()

    def _is_similar(a: str, b: str) -> bool:
        """Check if two normalised names are near-duplicates."""
        if not a or not b:
            return False
        # Exact substring containment (one is a shortened form of the other)
        if a in b or b in a:
            return True
        # Long common prefix (≥50% of the shorter string and ≥20 chars)
        min_len = min(len(a), len(b))
        common = 0
        for ca, cb in zip(a, b):
            if ca != cb:
                break
            common += 1
        if common >= min_len * 0.5 and common >= 20:
            return True
        # SequenceMatcher ratio
        return SequenceMatcher(None, a, b).ratio() >= threshold

    merged: List[Dict[str, Any]] = []
    used = [False] * len(topics)

    for i, ti in enumerate(topics):
        if used[i]:
            continue
        primary = dict(ti)  # shallow copy
        ni = _norm(primary.get("name") or "")
        for j in range(i + 1, len(topics)):
            if used[j]:
                continue
            nj = _norm(topics[j].get("name") or "")
            if _is_similar(ni, nj):
                # Merge j into primary
                used[j] = True
                donor = topics[j]
                # Keep higher score
                if (donor.get("score") or 0) > (primary.get("score") or 0):
                    primary["score"] = donor["score"]
                    primary["confidence"] = donor.get("confidence", primary.get("confidence"))
                # Merge evidence
                pe = primary.get("evidence_items") or []
                de = donor.get("evidence_items") or []
                seen_ids = {(e.get("kind"), e.get("source_id")) for e in pe if isinstance(e, dict)}
                for ev in de:
                    if isinstance(ev, dict):
                        key = (ev.get("kind"), ev.get("source_id"))
                        if key not in seen_ids:
                            pe.append(ev)
                            seen_ids.add(key)
                primary["evidence_items"] = pe
                # Merge keywords
                pk = set(primary.get("keywords") or [])
                pk.update(donor.get("keywords") or [])
                primary["keywords"] = sorted(pk)
        merged.append(primary)
    return merged


SYSTEM = (
    "You analyze a user's recent work focus based ONLY on the provided activity. "
    "Infer the most focused projects/topics for the time window. "
    "IMPORTANT: Consolidate closely related activities into a SINGLE topic. "
    "If the same project/product appears in multiple contexts (emails, meetings, chats), "
    "merge them into one topic with all evidence combined. "
    "Aim for 5-10 distinct topics maximum; fewer is better than duplicates. "
    "Be conservative: if evidence is weak, say so. "
    "Return JSON only."
)


def _render_md(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# Recent focus")
    window = report.get("window") or {}
    lines.append(f"- Window: {window.get('start')} → {window.get('end')}")
    counts = report.get("counts") or {}
    lines.append(
        f"- Items: teams={counts.get('teams_messages_sent', 0)}, emails={counts.get('emails_sent', 0)}, meetings={counts.get('meetings_joined', 0)}"
    )
    lines.append("")

    focus = report.get("focus") or {}
    topics = focus.get("topics") or []
    if not topics:
        lines.append("No topics returned.")
        return "\n".join(lines) + "\n"

    lines.append("## Top topics")
    for i, t in enumerate(topics, start=1):
        name = t.get("name") or t.get("topic") or f"Topic {i}"
        score = t.get("score")
        conf = t.get("confidence")
        rationale = t.get("rationale") or ""
        lines.append(f"### {i}. {name}")
        if score is not None or conf is not None:
            lines.append(f"- score: {score}  confidence: {conf}")
        if rationale:
            lines.append(f"- {rationale}")

        evidence = t.get("evidence_items") or t.get("evidence") or []
        if evidence:
            lines.append("- evidence:")
            for ev in evidence[:5]:
                when = ev.get("when") or ""
                kind = ev.get("kind") or ""
                title = ev.get("title") or ""
                snippet = ev.get("snippet") or ""
                lines.append(f"  - [{kind}] {when} — {title} — {snippet}")
        lines.append("")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze recent focus from Substrate data + Azure OpenAI")

    parser.add_argument("--days", type=int, default=7, help="Lookback window in days (default: 7)")
    parser.add_argument(
        "--substrate-dir",
        type=str,
        default="SubstrateDataExtraction",
        help="Path to SubstrateDataExtraction folder (default: SubstrateDataExtraction)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=os.path.join("incremental_data", "output", "recent_focus.json"),
        help="Output JSON path (default: incremental_data/output/recent_focus.json)",
    )
    parser.add_argument("--md", type=str, default="", help="Optional markdown output path")
    parser.add_argument("--no-fetch", action="store_true", help="Do not run Substrate extractors; read existing outputs")

    parser.add_argument("--max-teams", type=int, default=300, help="Max Teams messages (sent) to analyze")
    parser.add_argument("--max-emails", type=int, default=200, help="Max sent emails to analyze")
    parser.add_argument("--max-meetings", type=int, default=150, help="Max meetings to analyze")

    parser.add_argument(
        "--min-teams-sent-per-thread",
        type=int,
        default=2,
        help="Only include Teams threads where you sent at least this many messages (default: 2)",
    )

    parser.add_argument(
        "--meeting-attendance",
        choices=["strict", "relaxed"],
        default="relaxed",
        help="How to filter meetings you likely attended (default: relaxed)",
    )

    parser.add_argument(
        "--openai-timeout",
        type=float,
        default=120.0,
        help="Azure OpenAI request timeout in seconds (default: 120)",
    )
    parser.add_argument(
        "--openai-retries",
        type=int,
        default=1,
        help="Azure OpenAI max retries (default: 1)",
    )

    args = parser.parse_args()

    # Configure Azure OpenAI behavior before importing ai_utils.
    # ai_utils reads these at import time.
    os.environ["AZURE_OPENAI_TIMEOUT_SECONDS"] = str(max(1.0, float(args.openai_timeout)))
    os.environ["AZURE_OPENAI_MAX_RETRIES"] = str(max(0, int(args.openai_retries)))

    global ai_utils
    if ai_utils is None:
        import lib.ai_utils as _ai_utils
        ai_utils = _ai_utils
    else:
        ai_utils = importlib.reload(ai_utils)

    substrate_dir = os.path.abspath(args.substrate_dir)
    substrate_output = os.path.join(substrate_dir, "output")

    # Set up progress tracking sidecar (output path with .progress.json suffix)
    global _PROGRESS_PATH
    _PROGRESS_PATH = args.output + ".progress.json"
    TOTAL_STEPS = 10
    _write_progress(0, TOTAL_STEPS, "Initializing…")

    now = _utcnow()
    window_end = now
    window_start = now - timedelta(days=max(0, args.days))

    if not os.path.isdir(substrate_dir):
        raise FileNotFoundError(f"Substrate dir not found: {substrate_dir}")

    profile_path = os.path.join(substrate_output, "user_profile.json")
    _write_progress(1, TOTAL_STEPS, "Fetching user profile")
    if not os.path.exists(profile_path) and not args.no_fetch:
        print("[INFO] user_profile.json not found; fetching user profile via Substrate…")
        _run_py(cwd=substrate_dir, args=["get_user_profile.py"])

    # Paths (used for identity inference and later reads)
    teams_out_name = "teams_messages_recent.json"
    emails_out_name = "emails_sent_recent.json"

    teams_path = os.path.join(substrate_output, teams_out_name)
    emails_path = os.path.join(substrate_output, emails_out_name)
    calendar_path = os.path.join(substrate_output, "calendar_events.json")

    if args.no_fetch:
        # Be forgiving in no-fetch mode: use whatever existing artifacts are present.
        if not os.path.exists(teams_path):
            fallback = os.path.join(substrate_output, "teams_messages.json")
            if os.path.exists(fallback):
                print(f"[WARN] {teams_out_name} not found; using teams_messages.json")
                teams_path = fallback
        if not os.path.exists(emails_path):
            fallback = os.path.join(substrate_output, "emails.json")
            if os.path.exists(fallback):
                print(f"[WARN] {emails_out_name} not found; using emails.json")
                emails_path = fallback

    my_display_name, my_addresses = _discover_my_addresses(
        substrate_output,
        teams_path=teams_path,
        emails_path=emails_path,
        calendar_path=calendar_path,
    )
    print(f"[INFO] Using user identities: name='{my_display_name}', addresses={my_addresses}")

    # (paths already set above)

    if not args.no_fetch:
        print(f"[INFO] Fetching Substrate data for last {args.days} day(s)…")
        _write_progress(2, TOTAL_STEPS, "Fetching Teams messages")
        _run_py(
            cwd=substrate_dir,
            args=[
                "fetch_all_teams_messages.py",
                "--days",
                str(args.days),
                "--max",
                str(args.max_teams * 2),
                "--output",
                teams_out_name,
            ],
        )
        _write_progress(3, TOTAL_STEPS, "Fetching sent emails")
        _run_py(
            cwd=substrate_dir,
            args=[
                "fetch_all_emails.py",
                "--days",
                str(args.days),
                "--sent",
                "--max",
                str(args.max_emails * 2),
                "--output",
                emails_out_name,
            ],
        )
        _write_progress(4, TOTAL_STEPS, "Fetching calendar events")
        _run_py(
            cwd=substrate_dir,
            args=[
                "main.py",
                "calendar",
                "--start",
                _iso_z(window_start, end_of_day=False),
                "--end",
                _iso_z(window_end, end_of_day=True),
                "--top",
                str(args.max_meetings * 2),
            ],
        )

    # Validate expected artifacts exist.
    missing_required: List[str] = []
    for p in [teams_path, emails_path]:
        if not os.path.exists(p):
            missing_required.append(p)
    if missing_required:
        msg = "\n".join(["Missing expected Substrate outputs:", *missing_required])
        msg += "\n\nTip: run without --no-fetch, or run the Substrate extractors manually in SubstrateDataExtraction/."
        raise FileNotFoundError(msg)

    if not os.path.exists(calendar_path):
        print(f"[WARN] calendar_events.json not found; meetings will be omitted")

    _write_progress(5, TOTAL_STEPS, "Filtering Teams messages")
    teams_items = _collect_teams_sent(
        teams_path,
        my_addresses,
        my_display_name,
        window_start,
        window_end,
        args.max_teams,
        min_my_msgs_per_thread=args.min_teams_sent_per_thread,
    )
    _write_progress(6, TOTAL_STEPS, "Filtering sent emails")
    emails_items = _collect_emails_sent(
        emails_path,
        my_addresses,
        my_display_name,
        window_start,
        window_end,
        args.max_emails,
    )
    _write_progress(7, TOTAL_STEPS, "Filtering meetings")
    meeting_items = (
        _collect_meetings(
            calendar_path,
            window_start,
            window_end,
            args.max_meetings,
            attendance_mode=args.meeting_attendance,
        )
        if os.path.exists(calendar_path)
        else []
    )

    primary_upn = my_addresses[0] if my_addresses else ""
    prompt_payload = _build_prompt_payload(primary_upn, window_start, window_end, teams_items, emails_items, meeting_items)

    user_msg = (
        "Analyze this activity and return the user's most focused projects/topics. "
        "Rank by focus/effort, not just frequency. "
        "CRITICAL: Merge related sub-topics into ONE topic. For example, if the same project appears "
        "in coordination emails, development chats, and review meetings, that is ONE topic — not three. "
        "Use a concise, recognizable project name (not a long description). "
        "For each topic, include: name, score (0-100), confidence (0-1), rationale, keywords, and evidence items "
        "(each evidence item must be directly taken from activity: kind/when/title/snippet/source_id). "
        "Return JSON with keys: summary (string), topics (array), and gaps (array of strings).\n\n"
        + json.dumps(prompt_payload, ensure_ascii=False)
    )

    _write_progress(8, TOTAL_STEPS, "Analyzing with AI")
    client = ai_utils.get_azure_openai_client()
    focus = ai_utils.ai_chat_json(
        client,
        messages=[
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": user_msg},
        ],
        temperature=0.2,
        timeout=float(args.openai_timeout),
    )

    _write_progress(9, TOTAL_STEPS, "Processing results")
    # Post-process model output to ensure evidence is truly sourced from activity,
    # and to avoid useless meeting evidence (e.g. empty snippet or snippet==title).
    activity_lookup: Dict[Tuple[str, str], ActivityItem] = {}
    for it in teams_items + emails_items + meeting_items:
        if it.kind and it.source_id:
            activity_lookup[(it.kind, it.source_id)] = it

    topics = focus.get("topics") if isinstance(focus, dict) else None
    if isinstance(topics, list):
        for topic in topics:
            if not isinstance(topic, dict):
                continue
            ev_list = topic.get("evidence_items")
            if ev_list is None:
                ev_list = topic.get("evidence")
                if ev_list is not None and "evidence_items" not in topic:
                    topic["evidence_items"] = ev_list

            if not isinstance(ev_list, list):
                continue

            for ev in ev_list:
                if not isinstance(ev, dict):
                    continue
                kind = (ev.get("kind") or "").strip()
                sid = (ev.get("source_id") or "").strip()
                if not kind or not sid:
                    continue
                src = activity_lookup.get((kind, sid))
                if not src:
                    continue

                title = (ev.get("title") or "").strip()
                snippet = (ev.get("snippet") or "").strip()
                if not title:
                    ev["title"] = src.title
                if not snippet or (title and snippet == title):
                    ev["snippet"] = src.snippet
                if not (ev.get("when") or "").strip():
                    ev["when"] = src.when

    # --- Post-process: deduplicate topics with very similar names --------
    if isinstance(focus, dict) and isinstance(focus.get("topics"), list):
        focus["topics"] = _dedup_similar_topics(focus["topics"])

    report = {
        "generated_at": _utcnow().isoformat(),
        "days": args.days,
        "window": {
            "start": window_start.isoformat(),
            "end": window_end.isoformat(),
        },
        "counts": {
            "teams_messages_sent": len(teams_items),
            "emails_sent": len(emails_items),
            "meetings_joined": len(meeting_items),
        },
        "focus": focus,
    }

    _write_json(args.output, report)
    _write_progress(10, TOTAL_STEPS, "Done")
    print(f"[OK] Wrote report: {args.output}")

    if args.md:
        _write_text(args.md, _render_md(report))
        print(f"[OK] Wrote markdown: {args.md}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
