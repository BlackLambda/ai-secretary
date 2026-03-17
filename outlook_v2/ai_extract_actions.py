import json
import os
import argparse
from pathlib import Path
from datetime import datetime, timezone
import re
import html as _html

YELLOW = "\033[93m"
RESET = "\033[0m"

from lib.ai_utils import get_azure_openai_client, DEPLOYMENT_NAME, AZURE_OPENAI_TIMEOUT_SECONDS, ai_chat_json
from lib.ai_utils import drop_items_with_past_deadlines, summarize_deadline_drop
from lib.pipeline_config_manager import ensure_effective_config
from ai_secretary_core.paths import RepoPaths
from ai_secretary_core.recent_focus import default_recent_focus_path, resolve_effective_active_projects


def _norm_match_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return " ".join(text.replace("\r", "\n").split()).strip().lower()


def _strip_html(html_text: str) -> str:
    if not isinstance(html_text, str) or not html_text:
        return ""
    # Remove tags and unescape entities for best-effort matching.
    txt = re.sub(r"<[^>]+>", " ", html_text)
    txt = _html.unescape(txt)
    return txt


def _get_message_timestamp(msg: dict) -> str | None:
    for k in ("ReceivedDateTime", "SentDateTime", "CreatedDateTime", "LastModifiedDateTime"):
        v = msg.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _get_latest_thread_received_timestamp(thread_ids, thread_map) -> str | None:
    """Best-effort fallback timestamp when quote->message matching fails.

    Returns the latest available message timestamp across the related threads.
    `_get_message_timestamp` prefers `ReceivedDateTime`, so this is effectively
    "latest email received time" when available.
    """
    latest_ms: int | None = None
    latest_raw: str | None = None

    for tid in thread_ids or []:
        thread = thread_map.get(tid)
        if not isinstance(thread, dict):
            continue
        for msg in thread.get("messages", []) or []:
            if not isinstance(msg, dict):
                continue
            raw = _get_message_timestamp(msg)
            if not raw:
                continue
            try:
                ms = int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp() * 1000)
            except Exception:
                continue
            if latest_ms is None or ms > latest_ms:
                latest_ms = ms
                latest_raw = raw

    return latest_raw


def _find_quote_timestamp_in_threads(thread_ids, thread_map, quote: str) -> str | None:
    q = _norm_match_text(quote)
    if len(q) < 5:
        return None

    candidates = [q]
    if len(q) > 120:
        candidates.append(q[:120])
    if len(q) > 80:
        candidates.append(q[:80])

    for tid in thread_ids or []:
        thread = thread_map.get(tid)
        if not isinstance(thread, dict):
            continue
        for msg in thread.get("messages", []) or []:
            if not isinstance(msg, dict):
                continue
            body_preview = msg.get("BodyPreview") or ""
            body_html = (msg.get("Body") or {}).get("Content") if isinstance(msg.get("Body"), dict) else ""
            body_text = _strip_html(body_html)

            haystack = _norm_match_text(body_preview) + " " + _norm_match_text(body_text)
            for cand in candidates:
                if cand and cand in haystack:
                    return _get_message_timestamp(msg)
    return None

def load_json(filepath):
    if not os.path.exists(filepath):
        print(f"Error: File '{filepath}' not found.")
        return None
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_text(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

def load_scoring_system(scoring_path: str):
    """Load user-customizable scoring rubric (optional)."""
    if not os.path.exists(scoring_path):
        return None
    try:
        with open(scoring_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] Failed to load scoring_system.json: {e}")
        return None

def get_thread_content(thread_ids, thread_map):
    """
    Aggregates content from all related threads.
    """
    content = []
    for tid in thread_ids:
        thread = thread_map.get(tid)
        if not thread:
            continue
        
        content.append(f"--- Thread Subject: {thread.get('subject')} ---")
        for msg in thread.get("messages", []):
            sender = msg.get("From", {}).get("EmailAddress", {}).get("Name", "Unknown")
            body = msg.get("BodyPreview", "") or msg.get("Body", {}).get("Content", "")
            # Truncate body if too long to save tokens, keeping the most relevant parts usually at start
            if len(body) > 1000:
                body = body[:1000] + "...[truncated]"
            
            content.append(f"From: {sender}")
            content.append(f"To: {json.dumps(msg.get('ToRecipients', []), default=str)}")
            content.append(f"CC: {json.dumps(msg.get('CcRecipients', []), default=str)}")
            content.append(f"Body: {body}")
            content.append("-" * 20)
            
    return "\n".join(content)


def _extract_recipient_emails(recipients) -> set:
    emails = set()
    if not isinstance(recipients, list):
        return emails
    for r in recipients:
        if not isinstance(r, dict):
            continue
        addr = r.get("EmailAddress") if isinstance(r.get("EmailAddress"), dict) else {}
        email = addr.get("Address")
        if isinstance(email, str) and email.strip():
            emails.add(email.strip().lower())
    return emails


def _target_user_in_to_recipients(thread_ids, thread_map, target_user_email: str) -> bool:
    if not isinstance(target_user_email, str) or not target_user_email.strip():
        return False
    target = target_user_email.strip().lower()
    for tid in thread_ids or []:
        thread = thread_map.get(tid)
        if not isinstance(thread, dict):
            continue
        for msg in thread.get("messages", []) or []:
            to_emails = _extract_recipient_emails(msg.get("ToRecipients", []))
            if target in to_emails:
                return True
    return False


def _clamp_direct_email_scores(event_obj: dict, allow_direct_email: bool) -> None:
    """Ensure `direct_email` only scores when the user is actually in To recipients.

    This is a deterministic safety net in case the model assigns `direct_email` incorrectly.
    It adjusts each item's `priority_score` to stay consistent with the breakdown.
    """
    if not isinstance(event_obj, dict):
        return

    def clamp_item(item: dict) -> None:
        if not isinstance(item, dict):
            return
        breakdown = item.get("scoring_breakdown")
        if not isinstance(breakdown, dict):
            return
        val = breakdown.get("direct_email")
        if not isinstance(val, (int, float)):
            return
        if allow_direct_email:
            return

        if val != 0:
            breakdown["direct_email"] = 0
            evidence = item.get("scoring_evidence")
            if isinstance(evidence, dict):
                # Clear evidence if we removed the score.
                evidence["direct_email"] = ""
            # Recompute score from breakdown (numeric values only)
            total = 0
            for v in breakdown.values():
                if isinstance(v, (int, float)):
                    total += v
            item["priority_score"] = int(total)

    for todo in event_obj.get("todos", []) or []:
        clamp_item(todo)
    for rec in event_obj.get("recommendations", []) or []:
        clamp_item(rec)

def main():
    parser = argparse.ArgumentParser(description="Extract actions from events.")
    parser.add_argument("--input", required=True, help="Path to input events JSON file")
    parser.add_argument("--threads", required=True, help="Path to threads JSON file")
    parser.add_argument("--user-profile", required=True, help="Path to user profile JSON file")
    parser.add_argument("--recent-focus", help="Optional: recent_focus.json path to derive active projects")
    parser.add_argument("--guide", required=True, help="Path to action extraction guide")
    args = parser.parse_args()

    EVENTS_FILE = args.input
    THREADS_FILE = args.threads
    USER_PROFILE_FILE = args.user_profile
    RECENT_FOCUS_FILE = args.recent_focus
    GUIDE_FILE = args.guide

    print("Loading data...")
    print(f"Reading events from: {EVENTS_FILE}")
    
    events_data = load_json(EVENTS_FILE)
    threads_data = load_json(THREADS_FILE)
    user_profile = load_json(USER_PROFILE_FILE)
    guide_template = load_text(GUIDE_FILE)

    # Optional: read scoring rubric path from pipeline_config.json
    # Defaults to scoring_system.json at repo root.
    base_dir = Path(__file__).resolve().parents[1]
    scoring_path = str(RepoPaths(base_dir).outlook_scoring_system_default())
    try:
        cfg = ensure_effective_config(Path(base_dir))
        cfg_path = cfg.get("scoring_system_outlook_path") if isinstance(cfg, dict) else None
        if isinstance(cfg_path, str) and cfg_path.strip():
            scoring_path = str((Path(base_dir) / cfg_path).resolve())
    except Exception as e:
        print(f"[WARN] Failed to read scoring_system_outlook_path from pipeline_config.json: {e}")

    scoring_system = load_scoring_system(scoring_path)

    if not events_data or not threads_data or not user_profile:
        print("Failed to load necessary files.")
        return

    # Create a map for quick thread lookup
    thread_map = {t['id']: t for t in threads_data}

    print("Initializing AI client...")
    client = get_azure_openai_client()

    events = events_data.get("events", [])
    print(f"Processing {len(events)} events...")

    processed_count = 0
    skipped_count = 0

    for i, event in enumerate(events):
        event_name = event.get("event_name")
        
        # Check if this event is related to any of the input threads
        # If the event has no overlap with the provided threads file, we skip it 
        # (assuming it's an old event that hasn't changed)
        related_ids = event.get("related_thread_ids", [])
        has_overlap = any(tid in thread_map for tid in related_ids)
        
        if not has_overlap:
            skipped_count += 1
            continue

        print(f"[{i+1}/{len(events)}] Analyzing: {event_name}")
        processed_count += 1
        
        # Get related thread content
        thread_content = get_thread_content(related_ids, thread_map)
        
        if not thread_content:
            print("  No thread content found (threads might be missing from input), skipping.")
            continue

        # Prepare the prompt
        # Use recent focus derived topics as the only project context.
        project_root = Path(__file__).resolve().parents[1]
        recent_focus_path = Path(RECENT_FOCUS_FILE).resolve() if isinstance(RECENT_FOCUS_FILE, str) and RECENT_FOCUS_FILE.strip() else default_recent_focus_path(project_root)
        active_projects = resolve_effective_active_projects(recent_focus_path=recent_focus_path, user_profile=user_profile)

        user_context = f"""
        TARGET USER PROFILE:
        Name: {user_profile.get('USER_NAME', [''])[0]}
        Alias: {user_profile.get('USER_ALIAS', [''])[0]}
        Email: {user_profile.get('USER_EMAIL', [''])[0]}
        Manager: {user_profile.get('MANAGER_INFO', [''])[0]}
        Team: {user_profile.get('USER_TEAM', [])}

        Active Projects: {active_projects}
        Following: {user_profile.get('following', [])}
        """

        # Hard rule: do not produce already-expired tasks.
        user_context += "\nIMPORTANT: Do not include todos/recommendations whose deadline is already in the past.\n"

        # Deterministic evidence for `direct_email` factor.
        # Only allow `direct_email` points if the target user's email appears in ToRecipients.
        allow_direct_email = _target_user_in_to_recipients(
            related_ids, thread_map, user_profile.get('USER_EMAIL', [''])[0]
        )

        scoring_context = ""
        if isinstance(scoring_system, dict):
            scoring_context = (
                "\n\nUSER-CUSTOMIZABLE SCORING RUBRIC (authoritative):\n"
                + json.dumps(scoring_system, indent=2)
                + "\n\nUse this rubric to produce priority_score and scoring_breakdown.\n"
            )
        
        messages = [
            {"role": "system", "content": guide_template + "\n\n" + user_context + scoring_context},
            {"role": "user", "content": f"""
            EVENT DETAILS:
            {json.dumps(event, indent=2)}

            THREAD CONTENT:
            {thread_content}
            """}
        ]

        try:
            result = ai_chat_json(client, messages)
            
            # Update the event object directly with action data
            event["priority_score"] = result.get("priority_score", 0)
            event["priority_level"] = result.get("priority_level", "Low")
            event["scoring_breakdown"] = result.get("scoring_breakdown", {})

            # Short verb-led summary for sidebar/title usage.
            action_summary = result.get("action_summary")
            if isinstance(action_summary, str) and action_summary.strip():
                event["action_summary"] = action_summary.strip()
            
            current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            
            todos = result.get("todos", [])
            default_thread_ts = _get_latest_thread_received_timestamp(related_ids, thread_map)
            for todo in todos:
                todo["last_updated"] = current_time
                if not todo.get("original_quote_timestamp"):
                    ts = None
                    if todo.get("original_quote"):
                        ts = _find_quote_timestamp_in_threads(related_ids, thread_map, todo.get("original_quote"))
                    todo["original_quote_timestamp"] = ts or default_thread_ts
            event["todos"] = todos

            recommendations = result.get("recommendations", [])
            for rec in recommendations:
                rec["last_updated"] = current_time
                if not rec.get("original_quote_timestamp"):
                    ts = None
                    if rec.get("original_quote"):
                        ts = _find_quote_timestamp_in_threads(related_ids, thread_map, rec.get("original_quote"))
                    rec["original_quote_timestamp"] = ts or default_thread_ts
            # Do not distinguish recommendations from todos: treat all as todos.
            # Keep the recommendations field for backward compatibility, but leave it empty.
            if recommendations:
                event["todos"].extend(recommendations)
            event["recommendations"] = []

            # Hard rule: drop expired deadlines even if the model outputs them.
            kept, dropped = drop_items_with_past_deadlines(event.get("todos", []), now=datetime.now(timezone.utc))
            if dropped:
                print(f"  {YELLOW}[FILTER] Dropped {len(dropped)} past-due todos by deadline{RESET}")
                for d in dropped[:10]:
                    print(f"    {YELLOW}- {summarize_deadline_drop(d)}{RESET}")
            event["todos"] = kept

            # Enforce `direct_email` scoring consistency with actual recipients.
            _clamp_direct_email_scores(event, allow_direct_email)

            event["last_updated"] = current_time
            
            # Log findings
            if event["todos"] or event["priority_level"] == "High":
                print(
                    f"  {YELLOW}-> Found {len(event['todos'])} todos. "
                    f"Priority: {event['priority_level']}{RESET}"
                )
                
                # Log detailed todo information
                for idx, todo in enumerate(event["todos"], 1):
                    task = todo.get("task", "N/A")
                    rationale = todo.get("rationale", "N/A")
                    assignment_reason = todo.get("assignment_reason", "N/A")
                    user_role = todo.get("user_role", "N/A")
                    deadline = todo.get("deadline", "N/A")
                    print(f"     {YELLOW}Todo #{idx}: {task}{RESET}")
                    print(f"       {YELLOW}Rationale: {rationale}{RESET}")
                    print(f"       {YELLOW}Assignment: {assignment_reason}{RESET}")
                    print(f"       {YELLOW}User Role: {user_role}{RESET}")
                    print(f"       {YELLOW}Deadline: {deadline}{RESET}")
                
            else:
                print("  -> No significant actions found.")

        except Exception as e:
            print(f"  Error processing event: {e}")

    # Save updated events back to the same file
    print(f"\nAnalysis complete. Processed {processed_count}, Skipped {skipped_count}.")
    print(f"Updating events file...")
    with open(EVENTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(events_data, f, indent=2)

    print(f"Updated events saved to: {EVENTS_FILE}")

if __name__ == "__main__":
    main()
