import json
import os
import argparse
from pathlib import Path


def _load_pipeline_config() -> dict:
    """Best-effort load of repo-root pipeline_config.json."""
    try:
        repo_root = Path(__file__).resolve().parent.parent
        cfg_path = repo_root / 'pipeline_config.json'
        if not cfg_path.exists():
            return {}
        with cfg_path.open('r', encoding='utf-8') as f:
            cfg = json.load(f)
        return cfg if isinstance(cfg, dict) else {}
    except Exception:
        return {}


def _as_str_list(value) -> list[str]:
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, str):
        s = value.strip()
        return [s] if s else []
    return []

def load_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Process email threads.")
    parser.add_argument("--input", required=True, help="Path to input emails JSON file")
    parser.add_argument("--output", required=True, help="Path to output threads JSON file")
    parser.add_argument("--config", help="Path to config JSON file")
    parser.add_argument("--max-threads", type=int, help="Maximum number of threads to process")
    args = parser.parse_args()

    emails_path = args.input
    output_path = args.output
    
    # Legacy config locations (still supported as fallback)
    if args.config:
        config_path = args.config
    else:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(BASE_DIR, '..', 'outlook', 'config.json')
        if not os.path.exists(config_path):
            config_path = os.path.join(BASE_DIR, 'config.json')
    
    # Load emails
    data = load_json(emails_path)
    if not data:
        print("No email data found.")
        return
        
    emails = []
    if isinstance(data, dict):
        # Support multiple formats: "emails", "items", "value"
        emails = data.get("emails") or data.get("items") or data.get("value", [])
    elif isinstance(data, list):
        emails = data
        
    # Load skip rules.
    # Preferred: pipeline_config.json keys.
    # Back-compat: legacy outlook config keys if pipeline settings are empty.
    skip_senders: set[str] = set()
    skip_keywords: set[str] = set()

    pipeline_cfg = _load_pipeline_config()
    if pipeline_cfg:
        skip_senders = set(s.lower() for s in _as_str_list(pipeline_cfg.get('outlook_skip_sender_emails')))
        skip_keywords = set(s.lower() for s in _as_str_list(pipeline_cfg.get('outlook_skip_subject_terms')))

    legacy = load_json(config_path)
    if legacy:
        # Support both skip_email_sender and skip_sender_emails for backward compatibility
        if not skip_senders:
            skip_senders = set(s.lower() for s in _as_str_list(legacy.get('skip_email_sender', legacy.get('skip_sender_emails', []))))
        if not skip_keywords:
            skip_keywords = set(k.lower() for k in _as_str_list(legacy.get('skip_email_subject_keyword', [])))
        
    print(f"Loaded {len(emails)} emails.")
    print(f"Skip senders: {skip_senders}")
    print(f"Skip subject terms: {skip_keywords}")
    
    # Group by ConversationId
    threads = {}
    seen_ids = set()
    
    for email in emails:
        # Deduplication
        # Prefer InternetMessageId for deduplication as it stays constant across folders/copies
        unique_id = email.get("InternetMessageId") or email.get("Id")
        
        if unique_id:
            if unique_id in seen_ids:
                continue
            seen_ids.add(unique_id)

        # Get sender email
        sender_email = None
        sender_info = email.get("Sender") or email.get("From")
        if sender_info and "EmailAddress" in sender_info:
            sender_email = sender_info["EmailAddress"].get("Address")
            
        if sender_email and sender_email.lower() in skip_senders:
            continue
        
        # Check subject keywords
        subject = email.get("Subject") or ""
        if subject and any(keyword in subject.lower() for keyword in skip_keywords):
            continue
            
        conv_id = email.get("ConversationId")
        if not conv_id:
            continue
            
        if conv_id not in threads:
            threads[conv_id] = []
        threads[conv_id].append(email)
        
    # Filter threads with at least one email
    valid_threads = {cid: msgs for cid, msgs in threads.items() if len(msgs) > 0}
    
    print(f"Found {len(valid_threads)} threads after filtering.")
    
    total_emails_count = sum(len(msgs) for msgs in valid_threads.values())
    print(f"Total emails after deduplication: {total_emails_count}")
    
    # Output threads
    # Sort threads by the most recent email in the thread
    sorted_threads = []
    for cid, msgs in valid_threads.items():
        # Sort messages in thread by time
        msgs.sort(key=lambda x: x.get("ReceivedDateTime", ""), reverse=True)
        latest_msg = msgs[0]
        subject = latest_msg.get("Subject", "No Subject")
        sorted_threads.append({
            "id": cid,
            "subject": subject,
            "count": len(msgs),
            "latest_received": latest_msg.get("ReceivedDateTime"),
            "messages": msgs
        })
        
    sorted_threads.sort(key=lambda x: x["latest_received"] or "", reverse=True)
    
    # Apply max_threads limit if specified
    if args.max_threads and args.max_threads > 0:
        original_count = len(sorted_threads)
        sorted_threads = sorted_threads[:args.max_threads]
        print(f"Limited threads from {original_count} to {len(sorted_threads)} (max_threads={args.max_threads})")
    
    # Save to JSON
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(sorted_threads, f, indent=2, ensure_ascii=False)
        
    print(f"Threads saved to {output_path}")
    
    for thread in sorted_threads:
        print(f"Thread: {thread['subject']} ({thread['count']} emails)")
        print(f"  ID: {thread['id']}")
        print(f"  Latest: {thread['latest_received']}")
        print("-" * 40)

if __name__ == "__main__":
    main()
