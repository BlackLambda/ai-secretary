import json
import os
import argparse
import glob
from pathlib import Path

YELLOW = "\033[93m"
RESET = "\033[0m"

def load_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def save_json(data, file_path):
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Saved filtered data to {file_path}")
    except Exception as e:
        print(f"Error saving {file_path}: {e}")

def get_config(base_dir):
    """Return merged config (pipeline_config preferred, legacy supported)."""
    merged = {}

    # Preferred: repo-root pipeline_config.json
    try:
        repo_root = Path(base_dir)
        p = repo_root / 'pipeline_config.json'
        if p.exists():
            cfg = load_json(str(p))
            if isinstance(cfg, dict):
                merged.update(cfg)
    except Exception:
        pass

    # Back-compat: outlook_v2/config.json (and create if missing, as before)
    config_path = os.path.join(base_dir, 'outlook_v2', 'config.json')
    if not os.path.exists(config_path):
        try:
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump({}, f, indent=2)
            print(f"Created empty legacy config at {config_path}")
        except Exception as e:
            print(f"Error creating legacy config: {e}")
            return merged

    legacy = load_json(config_path)
    if isinstance(legacy, dict):
        merged.update(legacy)

    return merged

def should_skip_email(email, skip_senders, skip_keywords):
    # Check sender
    sender_email = None
    sender_info = email.get("Sender") or email.get("From")
    if sender_info and "EmailAddress" in sender_info:
        sender_email = sender_info["EmailAddress"].get("Address")
    
    if sender_email and sender_email.lower() in skip_senders:
        return True

    # Check subject keyword
    subject = email.get("Subject", "") or ""
    for keyword in skip_keywords:
        if keyword in subject.lower():
            return True
            
    return False

def filter_threads(threads_file, skip_senders, skip_keywords):
    print(f"Filtering threads in {threads_file}...")
    threads = load_json(threads_file)
    if not threads:
        return set()

    filtered_threads = []
    removed_thread_ids = set()

    for thread in threads:
        # Check if any email in the thread triggers the filter? 
        # Or just the latest? Or all?
        # Usually if the thread contains skipped content, we might want to skip the whole thread 
        # or just specific messages. 
        # Let's assume if the thread's subject matches or the sender of ANY message matches, we might want to be careful.
        # But typically filters apply to incoming items.
        # Let's check the messages in the thread.
        
        # If we filter out specific messages, we might empty the thread.
        
        new_messages = []
        for msg in thread.get('messages', []):
            if not should_skip_email(msg, skip_senders, skip_keywords):
                new_messages.append(msg)
        
        if not new_messages:
            removed_thread_ids.add(thread['id'])
            subject = thread.get('subject', "No Subject")
            senders = set()
            for msg in thread.get('messages', []):
                sender_info = msg.get("Sender") or msg.get("From")
                if sender_info and "EmailAddress" in sender_info:
                    senders.add(sender_info["EmailAddress"].get("Address"))
            print(f"{YELLOW}[REMOVED] Thread '{subject}' (Senders: {', '.join(senders)}) - All messages filtered.{RESET}")
            continue
            
        # If messages remain, update the thread object
        thread['messages'] = new_messages
        thread['count'] = len(new_messages)
        # Re-evaluate subject/latest if needed, but usually they are fine or just slightly stale.
        # Let's keep it simple.
        
        # Also check the thread subject itself (which is usually the subject of the first/latest email)
        subject = thread.get('subject') or ""
        skip_subject = False
        if subject:
            for keyword in skip_keywords:
                if keyword in subject.lower():
                    skip_subject = True
                    break
        
        if skip_subject:
            removed_thread_ids.add(thread['id'])
            print(f"{YELLOW}[REMOVED] Thread '{subject}' - Subject matched keyword.{RESET}")
            continue

        filtered_threads.append(thread)

    print(f"{YELLOW}Removed {len(threads) - len(filtered_threads)} threads.{RESET}")
    save_json(filtered_threads, threads_file)
    return removed_thread_ids

def filter_events(events_file, removed_thread_ids, skip_keywords):
    print(f"Filtering events in {events_file}...")
    data = load_json(events_file)
    if not data:
        return

    events = data.get('events', [])
    filtered_events = []
    
    for event in events:
        # 1. Remove if linked to a removed thread
        # Check related_thread_ids (list) or source_thread_id (string)
        should_remove = False
        
        source_thread_id = event.get('source_thread_id')
        if source_thread_id and source_thread_id in removed_thread_ids:
            should_remove = True
            
        if not should_remove:
            related_threads = event.get('related_thread_ids', [])
            # If ALL related threads are removed, remove the event? 
            # Or if ANY? Usually an event is derived from threads. If the source is gone, event should go.
            # If it has multiple threads, and some are removed, maybe we should keep it but update the list?
            # For simplicity, if it's linked to a removed thread, let's remove it or check if it has other valid threads.
            
            # Let's check if it has ANY valid thread left.
            if related_threads:
                valid_threads = [tid for tid in related_threads if tid not in removed_thread_ids]
                if not valid_threads:
                    should_remove = True
                else:
                    # Update the event to only include valid threads
                    event['related_thread_ids'] = valid_threads
            elif source_thread_id in removed_thread_ids:
                 # Fallback if related_thread_ids is empty but source_thread_id was set
                 should_remove = True

        if should_remove:
            print(f"{YELLOW}[REMOVED] Event '{event.get('title')}' - Linked to removed thread.{RESET}")
            continue
            
        # 2. Remove if event title matches keywords (Subject only)
        title = event.get('title', "") or ""
        
        skip = False
        for keyword in skip_keywords:
            if keyword in title.lower():
                skip = True
                break
        
        if skip:
            continue
            
        filtered_events.append(event)

    print(f"{YELLOW}Removed {len(events) - len(filtered_events)} events.{RESET}")
    data['events'] = filtered_events
    save_json(data, events_file)

def main():
    base_dir = str(Path(__file__).resolve().parent.parent)
    config = get_config(base_dir)
    
    # New preferred keys live in pipeline_config.json; legacy keys still work.
    skip_senders = set(s.lower() for s in config.get("outlook_skip_sender_emails", []) if str(s).strip())
    if not skip_senders:
        skip_senders = set(s.lower() for s in config.get("skip_sender_emails", []) if str(s).strip())
        skip_senders.update(s.lower() for s in config.get("skip_email_sender", []) if str(s).strip())

    skip_keywords = [k.lower() for k in config.get("outlook_skip_subject_terms", []) if str(k).strip()]
    if not skip_keywords:
        skip_keywords = [k.lower() for k in config.get("skip_email_subject_keyword", []) if str(k).strip()]
    
    print(f"Configuration loaded.")
    print(f"Skip Senders: {skip_senders}")
    print(f"Skip Keywords: {skip_keywords}")
    
    incremental_dir = os.path.join(base_dir, 'incremental_data')
    
    # 1. Filter Master Threads
    master_threads_file = os.path.join(incremental_dir, 'outlook', 'master_threads.json')
    removed_thread_ids = set()
    if os.path.exists(master_threads_file):
        removed_thread_ids = filter_threads(master_threads_file, skip_senders, skip_keywords)
    
    # 2. Filter Master Events
    # Find all master event files (usually master_outlook_events_ALIAS.json)
    event_files = glob.glob(os.path.join(incremental_dir, 'outlook', 'master_outlook_events_*.json'))
    for ev_file in event_files:
        filter_events(ev_file, removed_thread_ids, skip_keywords)

if __name__ == "__main__":
    main()
