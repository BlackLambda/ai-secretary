import json
import os
import re
import html
import argparse
import uuid
from datetime import datetime, timezone
from pathlib import Path

from lib.ai_utils import get_azure_openai_client, DEPLOYMENT_NAME, AZURE_OPENAI_TIMEOUT_SECONDS, ai_chat_json
from ai_secretary_core.recent_focus import default_recent_focus_path, resolve_effective_active_projects

def load_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_text(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

def clean_html(raw_html):
    if not raw_html:
        return ""
    # Remove style and script tags
    cleanr = re.compile('<style.*?>.*?</style>', re.DOTALL)
    cleantext = re.sub(cleanr, '', raw_html)
    cleanr = re.compile('<script.*?>.*?</script>', re.DOTALL)
    cleantext = re.sub(cleanr, '', cleantext)
    
    # Remove HTML tags
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, ' ', cleantext)
    
    # Unescape and collapse whitespace
    text = html.unescape(cleantext)
    return re.sub(r'\s+', ' ', text).strip()

def simplify_threads(threads):
    """
    Simplify thread data to reduce token usage.
    Keeps only essential information for event extraction.
    """
    simplified = []
    for thread in threads:
        # Get the first message (usually the most relevant for subject/time)
        # or iterate to find meeting details if available
        
        # Basic info from the thread object itself if available, 
        # otherwise derive from the first message
        
        messages_summary = []
        for msg in thread.get("messages", []):
            body_content = msg.get("Body", {}).get("Content", "")
            preview = msg.get("BodyPreview", "")
            
            # Prefer cleaned body content if available and significantly longer/different than preview
            # otherwise fall back to preview
            text_content = clean_html(body_content)
            if len(text_content) < 50: # If cleaning failed or empty
                text_content = preview
            
            # Truncate to avoid hitting token limits, but keep enough for agenda
            # 2000 chars should cover most agendas
            text_content = text_content[:2000]

            messages_summary.append({
                "subject": msg.get("Subject"),
                "sender": msg.get("From", {}).get("EmailAddress", {}).get("Name"),
                "received": msg.get("ReceivedDateTime"),
                "content_snippet": text_content,
                # Check for meeting specific properties
                "meeting_type": msg.get("MeetingMessageType"),
                "is_meeting": msg.get("MeetingMessageType") is not None
            })

        simplified.append({
            "id": thread.get("id"),
            "subject": thread.get("subject"),
            "latest_received": thread.get("latest_received"),
            "message_count": thread.get("count"),
            "messages": messages_summary
        })
    return simplified

def main():
    parser = argparse.ArgumentParser(description="Extract events from threads.")
    parser.add_argument("--input", required=True, help="Path to input threads JSON file")
    parser.add_argument("--output", required=True, help="Path to output events JSON file")
    parser.add_argument("--user-profile", help="Path to user profile JSON file")
    parser.add_argument("--recent-focus", help="Optional: recent_focus.json path to derive active projects")
    parser.add_argument("--guide", required=True, help="Path to event extraction guide")
    parser.add_argument("--relationship-guide", help="Path to relationship guide")
    parser.add_argument("--link-events", action="store_true", help="Enable event relationship analysis")
    parser.add_argument("--existing-events", help="Path to existing events JSON file to merge with")
    parser.add_argument("--index", type=int, default=1, help="Pipeline run index for ID generation")
    args = parser.parse_args()

    INPUT_FILE = args.input
    OUTPUT_FILE = args.output
    USER_PROFILE_FILE = args.user_profile
    RECENT_FOCUS_FILE = args.recent_focus
    GUIDE_FILE = args.guide
    RELATIONSHIP_GUIDE_FILE = args.relationship_guide
    EXISTING_EVENTS_FILE = args.existing_events
    RUN_INDEX = args.index

    print("Loading data...")
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file '{INPUT_FILE}' not found.")
        return

    print(f"Output will be saved to: {OUTPUT_FILE}")

    threads = load_json(INPUT_FILE)
    guide = load_text(GUIDE_FILE)

    # Optional: inject richer target-user context into the system guide.
    user_profile = {}
    if USER_PROFILE_FILE and os.path.exists(USER_PROFILE_FILE):
        try:
            up = load_json(USER_PROFILE_FILE)
            if isinstance(up, dict):
                user_profile = up
        except Exception:
            user_profile = {}

    # Use recent focus derived topics as the only project context.
    project_root = Path(__file__).resolve().parents[1]
    recent_focus_path = Path(RECENT_FOCUS_FILE).resolve() if isinstance(RECENT_FOCUS_FILE, str) and RECENT_FOCUS_FILE.strip() else default_recent_focus_path(project_root)
    active_projects = resolve_effective_active_projects(recent_focus_path=recent_focus_path, user_profile=user_profile)

    if user_profile or active_projects:
        guide += (
            "\n\nTARGET USER PROFILE (additional context):\n"
            f"Name: {(user_profile.get('USER_NAME', ['']) or [''])[0]}\n"
            f"Alias: {(user_profile.get('USER_ALIAS', ['']) or [''])[0]}\n"
            f"Email: {(user_profile.get('USER_EMAIL', ['']) or [''])[0]}\n"
            f"Manager: {(user_profile.get('MANAGER_INFO', ['']) or [''])[0]}\n"
            f"Team: {user_profile.get('USER_TEAM', [])}\n"
            f"Active Projects: {active_projects}\n"
            f"Following: {user_profile.get('following', [])}\n"
        )

    print(f"Loaded {len(threads)} threads.")

    # Simplify data to fit in context
    simplified_threads = simplify_threads(threads)
    
    print("Initializing AI client...")
    client = get_azure_openai_client()

    # Load existing events if provided to populate used IDs
    existing_events = []
    used_ids = set()
    if EXISTING_EVENTS_FILE and os.path.exists(EXISTING_EVENTS_FILE):
        print(f"Loading existing events from {EXISTING_EVENTS_FILE}...")
        try:
            existing_data = load_json(EXISTING_EVENTS_FILE)
            existing_events = existing_data.get("events", [])
            print(f"Loaded {len(existing_events)} existing events.")
            
            for event in existing_events:
                if "event_id" in event:
                    used_ids.add(event["event_id"])
            
        except Exception as e:
            print(f"Error loading existing events: {e}")

    # Batch Processing
    BATCH_SIZE = 10
    new_events = []
    
    # Initialize counter for T{RUN_INDEX}_ IDs based on existing used_ids
    current_seq = 0
    for uid in used_ids:
        match = re.match(r"^T(\d+)_(\d+)$", uid)
        if match:
            idx = int(match.group(1))
            seq = int(match.group(2))
            if idx == RUN_INDEX:
                if seq > current_seq:
                    current_seq = seq

    print(f"Processing {len(simplified_threads)} threads in batches of {BATCH_SIZE}...")
    failed_batches: list[int] = []
    
    for i in range(0, len(simplified_threads), BATCH_SIZE):
        batch_threads = simplified_threads[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"Processing Batch {batch_num} ({len(batch_threads)} threads)...")
        
        threads_str = json.dumps(batch_threads, indent=2)
        
        messages = [
            {"role": "system", "content": guide},
            {"role": "user", "content": f"Here is the list of {len(batch_threads)} email threads to process. \n\nIMPORTANT: You must ensure that ALL {len(batch_threads)} threads are assigned to an event. Do not leave any thread unassigned.\n\n{threads_str}"}
        ]

        try:
            batch_result = ai_chat_json(client, messages)
            
            # Process events from this batch
            for event in batch_result.get("events", []):
                # Generate ID: T{RUN_INDEX}_{sequence}
                current_seq += 1
                candidate_id = f"T{RUN_INDEX}_{current_seq}"
                
                # Ensure uniqueness (in case of gaps or non-sequential existing IDs)
                while candidate_id in used_ids:
                    current_seq += 1
                    candidate_id = f"T{RUN_INDEX}_{current_seq}"
                
                event["event_id"] = candidate_id
                used_ids.add(candidate_id)
                
                # Add last_updated timestamp
                event["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                
                # Remove action_items if present, as they are handled separately
                if "action_items" in event:
                    del event["action_items"]
                    
                new_events.append(event)
                
        except Exception as e:
            failed_batches.append(batch_num)
            print(f"Error processing batch {batch_num}: {e}")
            from lib.ai_utils import _log_ai_error
            _log_ai_error(f"Batch {batch_num} failed during Outlook event extraction: {e}")

    # Combine events
    all_events = existing_events + new_events
    events_data = {"events": all_events}
    
    # --- Post-processing to ensure 100% coverage ---
    # Only check coverage for the NEW threads we processed
    input_thread_ids = {t['id'] for t in simplified_threads}
    thread_map = {t['id']: t for t in simplified_threads}
    
    output_thread_ids = set()
    # Check all events (new and existing) to see if they cover the new threads
    for event in events_data.get("events", []):
        output_thread_ids.update(event.get("related_thread_ids", []))
        
        # Calculate stats for this event (using available thread info)
        # Note: thread_map only has NEW threads. 
        # If an event has old threads not in thread_map, we can't count them accurately here unless we had master threads.
        # But we can at least count the ones we know about.
        # Actually, we should probably preserve existing counts if we don't have the thread data?
        # For now, let's just count what we have in the input.
        
        current_threads = [thread_map.get(tid) for tid in event.get("related_thread_ids", []) if thread_map.get(tid)]
        if current_threads:
             # It's an event touching new threads, update counts based on what we see?
             # Or just add to existing count?
             # This is tricky without master data.
             # Let's just recalculate based on what we have, but this might undercount if we miss old threads.
             # Better approach: Don't reset counts for existing events unless we are sure.
             pass
        
    missing_ids = input_thread_ids - output_thread_ids
    
    if missing_ids:
        print(f"Warning: AI missed {len(missing_ids)} threads. Creating fallback events for them.")
        
        for missing_id in missing_ids:
            thread = thread_map.get(missing_id)
            if not thread: continue
            
            # Create a generic event for this thread
            new_event = {
                "event_id": f"AUTO_{missing_id[:8]}", # Simple ID generation
                "event_name": f"{thread.get('subject')}",
                "event_type": "Email Discussion",
                "start_time": thread.get("latest_received"), # Best guess
                "end_time": thread.get("latest_received"),
                "description": f"Auto-generated event for thread: {thread.get('subject')}",
                "related_thread_ids": [missing_id],
                "key_participants": [], # Could extract from messages if needed
                "key_outcomes": [],
                "timeline": [],
                "thread_count": 1,
                "email_count": thread.get("message_count", 0),
                "is_fallback": True,
                "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            events_data["events"].append(new_event)
    
    # Save intermediate result as requested
    print("Saving initial events list to events.json (before relationship analysis)...")
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(events_data, f, indent=2)
        
    # --- Step 2: Relationship Analysis ---
    if args.link_events:
        print("Analyzing event relationships...")
        if RELATIONSHIP_GUIDE_FILE and os.path.exists(RELATIONSHIP_GUIDE_FILE):
            relationship_guide = load_text(RELATIONSHIP_GUIDE_FILE)
            
            # Prepare events for analysis (remove heavy fields if needed, but keep enough context)
            # We send the events_data we just built/augmented
            events_str = json.dumps(events_data, indent=2)
            
            messages_rel = [
                {"role": "system", "content": relationship_guide},
                {"role": "user", "content": f"Here is the list of events to analyze for relationships:\\n\\n{events_str}"}
            ]
            
            try:
                final_events_data = ai_chat_json(client, messages_rel)
                
                # Merge back any fields we might have lost if the LLM didn't return everything?
                # The guide asks to return the full structure, but let's be safe.
                # Actually, let's trust the LLM to return the structure as requested, 
                # but we can also just merge the 'related_event_ids' back into our local object 
                # to avoid data loss if the LLM hallucinates or omits fields.
                
                final_map = {e['event_id']: e for e in final_events_data.get("events", [])}
                
                relationship_count = 0
                for event in events_data["events"]:
                    related_ids = final_map.get(event['event_id'], {}).get('related_event_ids', [])
                    event['related_event_ids'] = related_ids
                    if related_ids:
                        relationship_count += 1
                        event["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        print(f"  - Linked '{event['event_name']}' to {related_ids}")

                print(f"Relationship analysis complete. Found relationships for {relationship_count} events.")

            except Exception as e:
                print(f"Error during relationship analysis: {e}")
                # If this fails, we still save the events without relationships
                for event in events_data["events"]:
                    event['related_event_ids'] = []
        else:
            print(f"Warning: Relationship guide '{RELATIONSHIP_GUIDE_FILE}' not found. Skipping.")
    else:
        print("Skipping event relationship analysis (use --link-events to enable).")

    # -------------------------------------

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    print("Updating events.json with relationship data...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(events_data, f, indent=2)

    if failed_batches:
        from lib.ai_utils import _log_ai_error
        total = (len(simplified_threads) + BATCH_SIZE - 1) // BATCH_SIZE
        msg = f"{len(failed_batches)}/{total} batches failed during Outlook event extraction (partial data saved). Failed batches: {failed_batches}"
        print(f"[WARN] {msg}")
        _log_ai_error(msg)
        import sys as _sys
        _sys.exit(1)

    print(f"Successfully generated and updated events list.")
    print(f"Final output saved to: {OUTPUT_FILE}")
    
    # Print a summary
    events = events_data.get("events", [])
    
    # Calculate output threads count
    output_thread_ids = set()
    for event in events:
        output_thread_ids.update(event.get("related_thread_ids", []))

    print(f"Input threads processed: {len(threads)}")
    print(f"Output threads assigned to events: {len(output_thread_ids)}")
    print(f"Found {len(events)} events.")
    for event in events:
        fallback_mark = "[Fallback]" if event.get("is_fallback") else ""
        related_count = len(event.get("related_event_ids", []))
        print(f"- {event.get('event_name')} {fallback_mark} - Threads: {event.get('thread_count')}, Emails: {event.get('email_count')}, Related: {related_count}")

if __name__ == "__main__":
    main()
