import json
import os
import argparse
from datetime import datetime, timezone

from pathlib import Path

YELLOW = "\033[93m"
RESET = "\033[0m"


def _item_text(item) -> str:
    if not isinstance(item, dict):
        return ""
    return (item.get("task") or item.get("description") or "").strip()


def _log_removed_items(prefix: str, removed_items) -> set:
    """Log removed items in yellow; returns set of item texts logged."""
    logged = set()
    if not isinstance(removed_items, list):
        return logged

    for r in removed_items:
        if not isinstance(r, dict):
            continue
        text = _item_text(r)
        if not text:
            continue
        reason = (r.get("removal_reason") or r.get("reason") or "").strip()
        evidence = (r.get("evidence") or r.get("supporting_quote") or "").strip()
        if reason and evidence:
            print(f"    {YELLOW}[REMOVED] {prefix}: {text} — {reason} (evidence: {evidence}){RESET}")
        elif reason:
            print(f"    {YELLOW}[REMOVED] {prefix}: {text} — {reason}{RESET}")
        else:
            print(f"    {YELLOW}[REMOVED] {prefix}: {text}{RESET}")
        logged.add(text)

    return logged

from lib.ai_utils import get_azure_openai_client, DEPLOYMENT_NAME, AZURE_OPENAI_TIMEOUT_SECONDS, ai_chat_json
from lib.ai_utils import drop_items_with_past_deadlines, summarize_deadline_drop
from ai_secretary_core.recent_focus import default_recent_focus_path, resolve_effective_active_projects


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


def _allow_direct_email_from_threads(event_obj: dict, thread_map: dict, target_user_email: str) -> bool:
    if not isinstance(event_obj, dict) or not isinstance(thread_map, dict):
        return False
    if not isinstance(target_user_email, str) or not target_user_email.strip():
        return False
    target = target_user_email.strip().lower()
    for tid in event_obj.get("related_thread_ids", []) or []:
        thread = thread_map.get(tid)
        if not isinstance(thread, dict):
            continue
        for msg in thread.get("messages", []) or []:
            to_emails = _extract_recipient_emails(msg.get("ToRecipients", []))
            if target in to_emails:
                return True
    return False


def _clamp_direct_email_in_actions(event_obj: dict, allow_direct_email: bool) -> None:
    if not isinstance(event_obj, dict):
        return

    def clamp_item(item: dict) -> None:
        if not isinstance(item, dict):
            return
        breakdown = item.get("scoring_breakdown")
        if not isinstance(breakdown, dict):
            return
        v = breakdown.get("direct_email")
        if not isinstance(v, (int, float)):
            return
        if allow_direct_email:
            return
        if v != 0:
            breakdown["direct_email"] = 0
            evidence = item.get("scoring_evidence")
            if isinstance(evidence, dict):
                evidence["direct_email"] = ""
            total = 0
            for bv in breakdown.values():
                if isinstance(bv, (int, float)):
                    total += bv
            item["priority_score"] = int(total)

    for t in event_obj.get("todos", []) or []:
        clamp_item(t)
    for r in event_obj.get("recommendations", []) or []:
        clamp_item(r)


def load_scoring_system(scoring_path: str):
    """Load user-customizable scoring rubric (optional)."""
    if not scoring_path or not os.path.exists(scoring_path):
        return None
    try:
        with open(scoring_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] Failed to load scoring system from {scoring_path}: {e}")
        return None


def resolve_outlook_scoring_path() -> str:
    """Resolve scoring rubric path using pipeline_config.json (best-effort)."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_path = os.path.join(base_dir, "scoring_system.json")
    scoring_path = default_path
    try:
        pipeline_config_path = os.path.join(base_dir, "pipeline_config.json")
        if os.path.exists(pipeline_config_path):
            with open(pipeline_config_path, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
            cfg_path = cfg.get("scoring_system_outlook_path")
            if isinstance(cfg_path, str) and cfg_path.strip():
                scoring_path = os.path.join(base_dir, cfg_path)
    except Exception as e:
        print(f"[WARN] Failed to read scoring_system_outlook_path from pipeline_config.json: {e}")
    return scoring_path

def load_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_text(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()


def _event_has_thread_overlap(event_obj: dict, thread_map: dict) -> bool:
    """Return True if the event is associated with any thread in `thread_map`.

    This is used to keep validation focused on newly detected / updated events.
    The extractor step already uses the same overlap idea.
    """
    if not isinstance(event_obj, dict) or not isinstance(thread_map, dict) or not thread_map:
        return False
    for tid in event_obj.get("related_thread_ids", []) or []:
        if tid in thread_map:
            return True
    return False

def main():
    parser = argparse.ArgumentParser(description="Validate and clean up actions in events.")
    parser.add_argument("--input", required=True, help="Path to input events JSON file")
    parser.add_argument("--output", required=True, help="Path to output events JSON file")
    parser.add_argument("--guide", required=True, help="Path to validation guide")
    parser.add_argument("--threads", help="Optional: threads JSON file to validate recipient-based scoring (direct_email)")
    parser.add_argument("--user-profile", help="Optional: user profile JSON file for recipient-based scoring (direct_email)")
    parser.add_argument("--recent-focus", help="Optional: recent_focus.json path to derive active projects")
    args = parser.parse_args()

    INPUT_FILE = args.input
    OUTPUT_FILE = args.output
    GUIDE_FILE = args.guide
    THREADS_FILE = args.threads
    USER_PROFILE_FILE = args.user_profile
    RECENT_FOCUS_FILE = args.recent_focus

    print("Loading data for action validation...")
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file '{INPUT_FILE}' not found.")
        return

    events_data = load_json(INPUT_FILE)
    
    if not os.path.exists(GUIDE_FILE):
        print(f"Error: Guide file '{GUIDE_FILE}' not found.")
        return
        
    validation_guide = load_text(GUIDE_FILE)

    thread_map = {}
    target_user_email = ""
    user_profile = {}
    if THREADS_FILE and os.path.exists(THREADS_FILE):
        try:
            threads_data = load_json(THREADS_FILE)
            thread_map = {t.get('id'): t for t in (threads_data or []) if isinstance(t, dict) and t.get('id')}
        except Exception as e:
            print(f"[WARN] Failed to load threads for recipient validation: {e}")
            thread_map = {}

    if USER_PROFILE_FILE and os.path.exists(USER_PROFILE_FILE):
        try:
            up = load_json(USER_PROFILE_FILE)
            if isinstance(up, dict):
                user_profile = up
                target_user_email = (user_profile.get('USER_EMAIL', ['']) or [''])[0]
        except Exception as e:
            print(f"[WARN] Failed to load user profile for recipient validation: {e}")
            target_user_email = ""

    scoring_path = resolve_outlook_scoring_path()
    scoring_system = load_scoring_system(scoring_path)
    scoring_context = ""
    if isinstance(scoring_system, dict):
        scoring_context = (
            "\n\nUSER-CUSTOMIZABLE SCORING RUBRIC (authoritative):\n"
            + json.dumps(scoring_system, indent=2)
            + "\n\nValidate and, if needed, correct priority_score and scoring_breakdown to conform to this rubric.\n"
        )

    # Use recent focus derived topics as the only project context.
    project_root = Path(__file__).resolve().parents[1]
    recent_focus_path = Path(RECENT_FOCUS_FILE).resolve() if isinstance(RECENT_FOCUS_FILE, str) and RECENT_FOCUS_FILE.strip() else default_recent_focus_path(project_root)
    active_projects = resolve_effective_active_projects(recent_focus_path=recent_focus_path, user_profile=user_profile)

    user_context = (
        "\n\nTARGET USER PROFILE (additional context):\n"
        f"Name: {(user_profile.get('USER_NAME', ['']) or [''])[0]}\n"
        f"Alias: {(user_profile.get('USER_ALIAS', ['']) or [''])[0]}\n"
        f"Email: {(user_profile.get('USER_EMAIL', ['']) or [''])[0]}\n"
        f"Manager: {(user_profile.get('MANAGER_INFO', ['']) or [''])[0]}\n"
        f"Team: {user_profile.get('USER_TEAM', [])}\n"
        f"Active Projects: {active_projects}\n"
        f"Following: {user_profile.get('following', [])}\n"
    )

    user_context += "\nIMPORTANT: Do not include todos/recommendations whose deadline is already in the past.\n"
    
    print("Initializing AI client...")
    client = get_azure_openai_client()

    # Filter events that have actions
    all_events = events_data.get("events", [])
    actionable_events = [e for e in all_events if e.get("todos") or e.get("recommendations")]

    # If we were given the *new/updated* threads set for this run, focus validation only on
    # events that overlap those threads. This avoids re-validating every historical event.
    if thread_map:
        before = len(actionable_events)
        actionable_events = [e for e in actionable_events if _event_has_thread_overlap(e, thread_map)]
        skipped = before - len(actionable_events)
        print(f"Found {before} events with actions; validating {len(actionable_events)} (skipping {skipped} unchanged).")
    else:
        print(f"Found {len(actionable_events)} events with actions to validate.")
    
    if not actionable_events:
        print("No actions to validate. Copying input to output.")
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(events_data, f, indent=2)
        return

    # Process one by one as requested
    BATCH_SIZE = 1
    validated_count = 0
    
    for i in range(0, len(actionable_events), BATCH_SIZE):
        batch = actionable_events[i:i+BATCH_SIZE]
        # print(f"Validating batch {i//BATCH_SIZE + 1} ({len(batch)} events)...")
        
        # Prepare batch for AI
        # We only need relevant fields to save tokens
        batch_for_ai = []
        for e in batch:
            batch_for_ai.append({
                "event_id": e["event_id"],
                "event_name": e["event_name"],
                    "event_type": e.get("event_type", ""),
                "description": e.get("description", ""),
                "action_summary": e.get("action_summary", ""),
                "todos": e.get("todos", []),
                "recommendations": e.get("recommendations", []),
                "key_participants": e.get("key_participants", []),
                "key_outcomes": e.get("key_outcomes", []),
                "timeline": e.get("timeline", [])
            })
            
        events_str = json.dumps({"events": batch_for_ai}, indent=2)
        
        messages = [
            {"role": "system", "content": validation_guide + user_context + scoring_context},
            {"role": "user", "content": f"Here is the list of events with actions to validate:\n\n{events_str}"}
        ]
        
        try:
            result = ai_chat_json(client, messages)
                
            validated_events = result.get("validated_events", [])
            
            # Update original events with validated actions
            event_map = {e["event_id"]: e for e in all_events}
            
            for val_event in validated_events:
                eid = val_event.get("event_id")
                if eid in event_map:
                    original = event_map[eid]
                    print(f"Validating Event: {original['event_name']} ({eid})")
                    
                    # Update todos
                    if "todos" in val_event:
                        old_todos = original.get("todos", [])
                        new_todos = val_event["todos"]

                        logged_removed = set()
                        if "removed_todos" in val_event:
                            logged_removed |= _log_removed_items("TODO", val_event.get("removed_todos"))
                        
                        if old_todos != new_todos:
                            print(f"  {YELLOW}Todos: {len(old_todos)} -> {len(new_todos)}{RESET}")
                            old_desc = {_item_text(t) for t in old_todos}
                            new_desc = {_item_text(t) for t in new_todos}
                            
                            for d in old_desc - new_desc:
                                if d and d not in logged_removed:
                                    print(f"    {YELLOW}- Removed: {d}{RESET}")
                            for d in new_desc - old_desc:
                                if d: print(f"    {YELLOW}+ Added/Modified: {d}{RESET}")
                                
                            original["todos"] = new_todos
                    
                    # Update recommendations
                    if "recommendations" in val_event:
                        old_recs = original.get("recommendations", [])
                        new_recs = val_event["recommendations"]

                        logged_removed = set()
                        if "removed_recommendations" in val_event:
                            logged_removed |= _log_removed_items("REC", val_event.get("removed_recommendations"))
                        
                        if old_recs != new_recs:
                            print(f"  {YELLOW}Recs: {len(old_recs)} -> {len(new_recs)}{RESET}")
                            old_desc = {_item_text(t) for t in old_recs}
                            new_desc = {_item_text(t) for t in new_recs}
                            
                            for d in old_desc - new_desc:
                                if d and d not in logged_removed:
                                    print(f"    {YELLOW}- Removed: {d}{RESET}")
                            for d in new_desc - old_desc:
                                if d: print(f"    {YELLOW}+ Added/Modified: {d}{RESET}")

                            original["recommendations"] = new_recs

                    # Do not distinguish recommendations: merge them into todos and clear recs.
                    # This keeps the validator prompt stable while enforcing a single todo bucket.
                    recs = original.get("recommendations", [])
                    if isinstance(recs, list) and recs:
                        original.setdefault("todos", [])
                        if isinstance(original["todos"], list):
                            original["todos"].extend(recs)
                        original["recommendations"] = []

                    # Hard rule: drop expired deadlines even if they survived validation.
                    kept, dropped = drop_items_with_past_deadlines(original.get("todos", []), now=datetime.now(timezone.utc))
                    if dropped:
                        print(f"  {YELLOW}[FILTER] Dropped {len(dropped)} past-due todos by deadline{RESET}")
                        for d in dropped[:10]:
                            print(f"    {YELLOW}- {summarize_deadline_drop(d)}{RESET}")
                    original["todos"] = kept
                        
                    # Update participants
                    if "key_participants" in val_event:
                        old_parts = original.get("key_participants", [])
                        new_parts = val_event["key_participants"]
                        if old_parts != new_parts:
                            print(f"  {YELLOW}Participants: {len(old_parts)} -> {len(new_parts)}{RESET}")
                            original["key_participants"] = new_parts

                    # Update outcomes
                    if "key_outcomes" in val_event:
                        old_outcomes = original.get("key_outcomes", [])
                        new_outcomes = val_event["key_outcomes"]
                        if old_outcomes != new_outcomes:
                            print(f"  {YELLOW}Outcomes: {len(old_outcomes)} -> {len(new_outcomes)}{RESET}")
                            original["key_outcomes"] = new_outcomes

                    # Update timeline
                    if "timeline" in val_event:
                        old_timeline = original.get("timeline", [])
                        new_timeline = val_event["timeline"]
                        if old_timeline != new_timeline:
                            print(f"  {YELLOW}Timeline: {len(old_timeline)} -> {len(new_timeline)}{RESET}")
                            original["timeline"] = new_timeline

                    # Update event_type (label normalization)
                    if "event_type" in val_event:
                        new_type = val_event.get("event_type")
                        if isinstance(new_type, str) and new_type.strip() and new_type != original.get("event_type"):
                            print(f"  {YELLOW}Event Type: {original.get('event_type', '')} -> {new_type}{RESET}")
                            original["event_type"] = new_type

                    # Update action summary (sidebar/title)
                    if "action_summary" in val_event:
                        action_summary = val_event.get("action_summary")
                        if isinstance(action_summary, str) and action_summary.strip():
                            original["action_summary"] = action_summary.strip()

                    original["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                    # Deterministic enforcement for `direct_email` scoring.
                    # If we have threads + user profile, allow direct_email only if user is in ToRecipients.
                    if thread_map and target_user_email:
                        allow_direct_email = _allow_direct_email_from_threads(original, thread_map, target_user_email)
                        _clamp_direct_email_in_actions(original, allow_direct_email)
                    validated_count += 1
                    
        except Exception as e:
            print(f"Error validating batch: {e}")

    print(f"Validation complete. Updated {validated_count} events.")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    print(f"Saving validated events to: {OUTPUT_FILE}")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(events_data, f, indent=2)

if __name__ == "__main__":
    main()
