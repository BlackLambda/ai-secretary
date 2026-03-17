import json
import os
import argparse
from datetime import datetime, timezone

YELLOW = "\033[93m"
RESET = "\033[0m"

from lib.ai_utils import get_azure_openai_client, DEPLOYMENT_NAME, AZURE_OPENAI_TIMEOUT_SECONDS, ai_chat_json

LABEL_NORMALIZATION_PROMPT = """
You are standardizing event labels.

Input:
- A JSON object with:
  - \"candidate_labels\": a list of label strings collected from events

Task:
- Produce a canonical set of at most 20 labels.
- Each label must be:
  - lowercase
  - 1-3 words
  - concise and reusable across events
- Merge synonyms/variants (e.g., \"pr review\", \"pr-review\", \"pull request review\" -> \"pr-review\").

Output:
Return a JSON object:
{
  \"canonical_labels\": [\"label1\", \"label2\", ...]
}
"""


def normalize_label(label: str) -> str:
    return (label or "").strip().lower()


def normalize_event_labels_via_ai(client, events):
    """Normalize all event labels to a canonical set (<= 20) and rewrite events in-place."""
    raw_labels = []
    for e in events:
        for l in (e.get("labels") or []):
            nl = normalize_label(l)
            if nl:
                raw_labels.append(nl)

    unique_labels = sorted(set(raw_labels))
    if not unique_labels:
        return

    messages = [
        {"role": "system", "content": LABEL_NORMALIZATION_PROMPT},
        {"role": "user", "content": json.dumps({"candidate_labels": unique_labels}, indent=2)},
    ]

    try:
        result = ai_chat_json(client, messages)
        canonical = [normalize_label(x) for x in result.get("canonical_labels", [])]
        canonical = [x for x in canonical if x][:20]
        if not canonical:
            return
    except Exception as e:
        print(f"Warning: label normalization failed: {e}")
        return

    def map_label(lab: str) -> str:
        if lab in canonical:
            return lab
        for c in canonical:
            if lab.replace(" ", "-") == c or lab.replace("-", " ") == c:
                return c
        for c in canonical:
            if lab in c or c in lab:
                return c
        return lab

    for e in events:
        labels = [normalize_label(x) for x in (e.get("labels") or [])]
        labels = [x for x in labels if x]
        mapped = [map_label(x) for x in labels]
        seen = set()
        out = []
        for x in mapped:
            if x not in seen:
                out.append(x)
                seen.add(x)
        e["labels"] = out[:3]

def load_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_text(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

def main():
    parser = argparse.ArgumentParser(description="Deduplicate events.")
    parser.add_argument("--input", required=True, help="Path to input events JSON file")
    parser.add_argument("--output", required=True, help="Path to output events JSON file")
    parser.add_argument("--guide", required=True, help="Path to deduplication guide")
    parser.add_argument("--max-dedup-run", type=int, default=3, help="Maximum number of deduplication iterations (default: 3)")
    args = parser.parse_args()

    INPUT_FILE = args.input
    OUTPUT_FILE = args.output
    GUIDE_FILE = args.guide

    print("Loading data for deduplication...")
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file '{INPUT_FILE}' not found.")
        return

    events_data = load_json(INPUT_FILE)
    
    if not os.path.exists(GUIDE_FILE):
        print(f"Error: Guide file '{GUIDE_FILE}' not found.")
        return
        
    dedup_guide = load_text(GUIDE_FILE)
    
    print("Initializing AI client...")
    client = get_azure_openai_client()

    # Normalize labels across all events before deduplication to improve merge quality
    try:
        normalize_event_labels_via_ai(client, events_data.get("events", []))
    except Exception as e:
        print(f"Warning: skipping label normalization due to error: {e}")

    print(f"Deduplicating events (max {args.max_dedup_run} iterations)...")
    
    total_deduped_events = []
    
    for iteration in range(1, args.max_dedup_run + 1):
        print(f"\n[Iteration {iteration}/{args.max_dedup_run}]")
        
        # Filter only events with actions
        all_events = events_data.get("events", [])
        actionable_events = [e for e in all_events if e.get("todos") or e.get("recommendations")]
        print(f"Selected {len(actionable_events)} actionable events out of {len(all_events)} total for deduplication.")

        merges = []
        if len(actionable_events) >= 2:
            events_subset = {"events": actionable_events}
            events_str = json.dumps(events_subset, indent=2)
            
            messages_dedup = [
                {"role": "system", "content": dedup_guide},
                {"role": "user", "content": f"Here is the list of events to deduplicate:\n\n{events_str}"}
            ]
            
            try:
                result_dedup = ai_chat_json(client, messages_dedup)
                merges = result_dedup.get("merges", [])
            except Exception as e:
                print(f"Error during AI deduplication: {e}")

        try:
            if merges:
                print(f"Found {len(merges)} merge groups.")
                
                # Create a map for easy access
                event_map = {e["event_id"]: e for e in events_data["events"]}
                events_to_remove = set()
                deduped_events = []
                
                for merge in merges:
                    primary_id = merge.get("primary_event_id")
                    secondary_ids = merge.get("secondary_event_ids", [])
                    merge_reason = (merge.get("reason") or "").strip() or "Duplicate event merged during deduplication"
                    
                    if primary_id not in event_map:
                        print(f"Warning: Primary ID {primary_id} not found.")
                        continue
                        
                    primary_event = event_map[primary_id]
                    print(f"Merging into '{primary_event['event_name']}' ({primary_id}): {secondary_ids}")

                    # Record merge info on the primary event so frontend can display the AI's reason
                    primary_event.setdefault("dedup_merge_info", [])
                    
                    for sec_id in secondary_ids:
                        if sec_id not in event_map or sec_id == primary_id:
                            continue
                            
                        sec_event = event_map[sec_id]
                        events_to_remove.add(sec_id)
                        
                        # Add to deduped list with merge info
                        deduped_events.append({
                            "event_id": sec_id,
                            "event_name": sec_event.get("event_name"),
                            "merged_into": primary_id,
                            "merged_into_name": primary_event.get("event_name"),
                            "reason": merge_reason,
                        })

                        primary_event["dedup_merge_info"].append({
                            "event_id": sec_id,
                            "event_name": sec_event.get("event_name"),
                            "reason": merge_reason,
                        })
                        
                        # Merge Logic
                        # 1. Threads
                        existing_threads = set(primary_event.get("related_thread_ids", []))
                        new_threads = sec_event.get("related_thread_ids", [])
                        for tid in new_threads:
                            if tid not in existing_threads:
                                primary_event["related_thread_ids"].append(tid)

                        # 1b. Labels
                        existing_labels = {normalize_label(l) for l in primary_event.get("labels", [])}
                        for l in (sec_event.get("labels", []) or []):
                            ln = normalize_label(l)
                            if ln and ln not in existing_labels:
                                primary_event.setdefault("labels", []).append(ln)
                                existing_labels.add(ln)
                        if primary_event.get("labels"):
                            primary_event["labels"] = primary_event["labels"][:3]
                        
                        # 2. Participants
                        existing_parts = {p.strip().lower() for p in primary_event.get("key_participants", [])}
                        for p in sec_event.get("key_participants", []):
                            p_norm = p.strip().lower()
                            if p_norm and p_norm not in existing_parts:
                                primary_event["key_participants"].append(p)
                                existing_parts.add(p_norm)
                                
                        # 3. Action Items
                        # Deduplicate based on task description (normalized)
                        existing_todos = {
                            (t.get("task") or t.get("description") or "").strip().lower() 
                            for t in primary_event.get("todos", [])
                        }
                        for todo in sec_event.get("todos", []):
                            desc = (todo.get("task") or todo.get("description") or "").strip()
                            desc_norm = desc.lower()
                            if desc and desc_norm not in existing_todos:
                                primary_event.setdefault("todos", []).append(todo)
                                existing_todos.add(desc_norm)

                        existing_recs = {
                            (r.get("task") or r.get("description") or "").strip().lower() 
                            for r in primary_event.get("recommendations", [])
                        }
                        for rec in sec_event.get("recommendations", []):
                            desc = (rec.get("task") or rec.get("description") or "").strip()
                            desc_norm = desc.lower()
                            if desc and desc_norm not in existing_recs:
                                primary_event.setdefault("recommendations", []).append(rec)
                                existing_recs.add(desc_norm)
                                
                        # 4. Outcomes
                        existing_outcomes = {o.strip().lower() for o in primary_event.get("key_outcomes", [])}
                        for o in sec_event.get("key_outcomes", []):
                            o_norm = o.strip().lower()
                            if o_norm and o_norm not in existing_outcomes:
                                primary_event["key_outcomes"].append(o)
                                existing_outcomes.add(o_norm)
                                
                        # 5. Timeline
                        # Deduplicate based on date + description
                        existing_timeline = set()
                        for item in primary_event.get("timeline", []):
                            if isinstance(item, dict):
                                key = (item.get("date"), (item.get("description") or "").strip().lower())
                            else:
                                key = str(item).strip().lower()
                            existing_timeline.add(key)
                        
                        for item in sec_event.get("timeline", []):
                            if isinstance(item, dict):
                                key = (item.get("date"), (item.get("description") or "").strip().lower())
                            else:
                                key = str(item).strip().lower()
                            
                            if key not in existing_timeline:
                                primary_event.setdefault("timeline", []).append(item)
                                existing_timeline.add(key)
                        
                        # 6. Update counts
                        primary_event["thread_count"] = primary_event.get("thread_count", 0) + sec_event.get("thread_count", 0)
                        primary_event["email_count"] = primary_event.get("email_count", 0) + sec_event.get("email_count", 0)
                        primary_event["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        
                        # 7. Merge Description/Summary?
                        # Keep primary as source of truth.
                
                # Filter out removed events
                events_data["events"] = [e for e in events_data["events"] if e["event_id"] not in events_to_remove]
                total_deduped_events.extend(deduped_events)
                print(f"{YELLOW}Iteration {iteration}: Removed {len(events_to_remove)} events.{RESET}")
                
                # Continue to next iteration if duplicates were found
                
            else:
                print(f"{YELLOW}Iteration {iteration}: No duplicates found. Stopping.{RESET}")
                break
                
        except Exception as e:
            print(f"Error during deduplication iteration {iteration}: {e}")
            break
    
    # Set final deduped_events list
    events_data["deduped_events"] = total_deduped_events
    print(f"\n{YELLOW}Deduplication complete. Total removed across all iterations: {len(total_deduped_events)} events.{RESET}")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    print(f"Saving deduplicated events to: {OUTPUT_FILE}")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(events_data, f, indent=2)

if __name__ == "__main__":
    main()
