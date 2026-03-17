import os
import sys
import json
import subprocess
import shutil
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.pipeline_config_manager import ensure_effective_config
from ai_secretary_core import json_io

REPO_NAME = "SubstrateDataExtraction"
INCREMENTAL_DIR = "incremental_data"  # legacy default; overridden by pipeline_config active_data_folder_path


def resolve_active_data_folder(base_dir: Path) -> Path:
    """Resolve the active dataset folder (equivalent to incremental_data/).

    Mirrors server_react.py selection logic: active_data_folder_path -> data_folder_paths[0] -> data_folder_path -> default.
    """
    try:
        cfg = ensure_effective_config(base_dir)
    except Exception:
        cfg = {}

    chosen = None
    if isinstance(cfg, dict):
        active = cfg.get('active_data_folder_path')
        if isinstance(active, str) and active.strip():
            chosen = active.strip()

        if chosen is None:
            paths = cfg.get('data_folder_paths')
            if isinstance(paths, list):
                for x in paths:
                    s = str(x or '').strip()
                    if s:
                        chosen = s
                        break

        if chosen is None:
            legacy = cfg.get('data_folder_path')
            if isinstance(legacy, str) and legacy.strip():
                chosen = legacy.strip()

    if not chosen:
        chosen = INCREMENTAL_DIR

    p = Path(chosen)
    if not p.is_absolute():
        p = (base_dir / p).resolve()
    return p

def load_log(log_path):
    if log_path.exists():
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return []

def save_log(log_path, entry):
    logs = load_log(log_path)
    logs.append(entry)
    with open(log_path, 'w', encoding='utf-8') as f:
        json.dump(logs, f, indent=2)

def get_last_run_info(log_path):
    logs = load_log(log_path)
    if logs:
        last = logs[-1]
        return last.get('index', 0), last.get('timestamp')
    return 0, None

def get_state_from_logs(outlook_log, teams_log):
    """
    Determines the next index and last run time by inspecting log files.
    Returns: (next_index, last_run_iso_string)
    """
    idx_outlook, time_outlook = get_last_run_info(outlook_log)
    idx_teams, time_teams = get_last_run_info(teams_log)
    
    # Use the max index to keep them in sync or increment
    max_index = max(idx_outlook, idx_teams)
    
    # Use the latest timestamp found, or None
    last_run = time_outlook if time_outlook else time_teams
    if time_outlook and time_teams:
        # If both exist, pick the later one? Or the earlier one to be safe?
        # Usually we want to fetch from the last successful run.
        # If they are synced, they should be similar.
        # Let's pick the max to avoid re-fetching too much if one failed?
        # No, if one failed, we might want to retry.
        # But here we assume success.
        last_run = max(time_outlook, time_teams)
        
    return max_index + 1, last_run

def initialize_from_existing_data(base_dir, input_dir, incremental_path, outlook_path, teams_path, outlook_log, teams_log):
    """
    Initializes the incremental pipeline by loading existing data from input directory.
    This creates index 0 as the baseline with all existing emails and teams messages.
    Also copies user profile if it exists.
    Returns: True if initialization was successful, False otherwise
    """
    input_path = Path(input_dir)
    if not input_path.exists():
        print(f"[ERROR] Input directory not found: {input_path}")
        return False
    
    print(f"\n[INIT] Initializing from existing data in {input_path}")
    
    # Copy user profile if it exists (dataset-scoped)
    user_profile_src = input_path / "user_profile.json"
    if user_profile_src.exists():
        user_profile_dst = Path(incremental_path) / "user_profile.json"
        shutil.copy2(str(user_profile_src), str(user_profile_dst))
        print(f"[INFO] Copied user profile from {user_profile_src.name}")
    
    current_run_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    initialization_success = True
    
    # Process Emails
    emails_src = input_path / "all_emails.json"
    if emails_src.exists():
        try:
            with open(emails_src, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Handle both list and dict wrapper formats
                if isinstance(data, list):
                    emails = data
                elif isinstance(data, dict):
                    emails = data.get('emails') or data.get('items') or data.get('value') or []
                else:
                    emails = []
            
            # Save as index 0 (initial data)
            delta_filename = "emails_0.json"
            delta_path = outlook_path / delta_filename
            
            # Save in standard format
            with open(delta_path, 'w', encoding='utf-8') as f:
                json.dump({"emails": emails}, f, indent=2)
            
            print(f"[INFO] Saved {len(emails)} existing emails as {delta_filename}")
            
            # Copy to all_emails.json
            all_emails_path = outlook_path / "all_emails.json"
            shutil.copy2(str(delta_path), str(all_emails_path))
            
            # Create log entry for index 0
            log_entry = {
                "index": 0,
                "timestamp": current_run_time,
                "filter_since": "initialization",
                "file": delta_filename,
                "fetched_count": len(emails),
                "status": "initialized"
            }
            save_log(outlook_log, log_entry)
            print(f"[INFO] Created Outlook log entry for index 0")
            
        except Exception as e:
            print(f"[ERROR] Failed to process emails: {e}")
            initialization_success = False
    else:
        print(f"[WARN] No all_emails.json found in {input_path}")
    
    # Process Teams Messages
    teams_src = input_path / "all_teams_messages.json"
    if teams_src.exists():
        try:
            with open(teams_src, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Handle both list and dict wrapper formats
                if isinstance(data, list):
                    messages = data
                elif isinstance(data, dict):
                    messages = data.get('messages') or data.get('items') or data.get('value') or []
                else:
                    messages = []
            
            # Save as index 0 (initial data)
            delta_filename = "teams_0.json"
            delta_path = teams_path / delta_filename
            
            # Save in standard format
            with open(delta_path, 'w', encoding='utf-8') as f:
                json.dump({"messages": messages}, f, indent=2)
            
            print(f"[INFO] Saved {len(messages)} existing teams messages as {delta_filename}")
            
            # Copy to all_teams_messages.json
            all_teams_path = teams_path / "all_teams_messages.json"
            shutil.copy2(str(delta_path), str(all_teams_path))
            
            # Create log entry for index 0
            log_entry = {
                "index": 0,
                "timestamp": current_run_time,
                "filter_since": "initialization",
                "file": delta_filename,
                "fetched_count": len(messages),
                "status": "initialized"
            }
            save_log(teams_log, log_entry)
            print(f"[INFO] Created Teams log entry for index 0")
            
        except Exception as e:
            print(f"[ERROR] Failed to process teams messages: {e}")
            initialization_success = False
    else:
        print(f"[WARN] No all_teams_messages.json found in {input_path}")
    
    if initialization_success:
        print(f"[SUCCESS] Initialized pipeline with existing data as index 0")
        print(f"[INFO] Next run will be index 1 and fetch new data since {current_run_time}")
    
    return initialization_success

def run_command(command, cwd=None, description=None):
    """Runs a command and prints status."""
    if description:
        print(f"\n[STEP] {description}")
    
    cmd_str = ' '.join(command) if isinstance(command, list) else command
    print(f"Running: {cmd_str}")
    
    try:
        subprocess.check_call(command, cwd=cwd)
        print("[OK] Success")
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Command failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        return False

def merge_data(new_items, all_data_file, item_key='Id'):
    """Merges new items into the all_data file, avoiding duplicates."""
    all_items = []
    if os.path.exists(all_data_file):
        try:
            with open(all_data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Handle both list and dict wrapper formats
                if isinstance(data, list):
                    all_items = data
                elif isinstance(data, dict):
                    # Try to find the list in common keys
                    for key in ['emails', 'messages', 'value', 'items']:
                        if key in data and isinstance(data[key], list):
                            all_items = data[key]
                            break
        except Exception as e:
            print(f"[WARN] Failed to load existing all data from {all_data_file}: {e}")

    # Create a map of existing items for fast lookup
    existing_ids = {item.get(item_key) for item in all_items if item.get(item_key)}
    
    added_count = 0
    for item in new_items:
        item_id = item.get(item_key)
        if item_id and item_id not in existing_ids:
            all_items.append(item)
            existing_ids.add(item_id)
            added_count += 1
            
    # Save back
    with open(all_data_file, 'w', encoding='utf-8') as f:
        # Save as a wrapper to be consistent
        json.dump({"count": len(all_items), "items": all_items}, f, indent=2)
        
    return added_count, len(all_items)

def main():
    base_dir = Path(__file__).resolve().parent.parent
    substrate_dir = base_dir / REPO_NAME
    incremental_path = resolve_active_data_folder(base_dir)
    
    # Create subdirectories
    outlook_path = incremental_path / "outlook"
    teams_path = incremental_path / "teams"
    
    incremental_path.mkdir(exist_ok=True)
    outlook_path.mkdir(exist_ok=True)
    teams_path.mkdir(exist_ok=True)
    
    outlook_log = outlook_path / "fetch_log.json"
    teams_log = teams_path / "fetch_log.json"
    
    if not substrate_dir.exists():
        print(f"[ERROR] SubstrateDataExtraction directory not found at {substrate_dir}")
        print("Please run setup_v2.py first to clone the repository.")
        sys.exit(1)

    # Determine state from logs
    current_index, last_run = get_state_from_logs(outlook_log, teams_log)

    # Always load config so initial_fetch_days can override or clamp last_run.
    try:
        ensure_effective_config(base_dir)
    except Exception:
        pass
    # Config lives in base_dir/config/pipeline_config.json (written by pipeline_config_manager).
    config_path = base_dir / "config" / "pipeline_config.json"
    config = {}
    if config_path.exists():
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f) or {}
        except Exception as e:
            print(f"[WARN] Failed to load config: {e}")

    initial_data_dir = config.get("initial_data_dir")
    default_days = float(config.get("initial_fetch_days", 1))

    if last_run:
        print(f"[INFO] Found last run timestamp from logs: {last_run}")
    else:
        # No previous run — initialize from existing data or use lookback window.
        if initial_data_dir:
            initial_data_path = base_dir / initial_data_dir
            if initial_data_path.exists():
                if initialize_from_existing_data(base_dir, initial_data_path, incremental_path,
                                                outlook_path, teams_path, outlook_log, teams_log):
                    print("\n[INFO] Initialization complete. Pipeline is ready for incremental updates.")
                    return
                else:
                    print("[WARN] Initialization failed. Falling back to default fetch behavior.")
            else:
                print(f"[WARN] Configured initial_data_dir not found: {initial_data_path}")

        last_run = (datetime.now(timezone.utc) - timedelta(days=default_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"[INFO] No previous run found. Using initial_fetch_days={default_days} day(s) ago: {last_run}")

    print(f"[INFO] Current Fetch Index: {current_index}")

    # Construct filter for delta fetch
    filter_query = f"ReceivedDateTime ge {last_run}"
    current_run_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    any_data_found = False

    # --- 1. Fetch Emails ---
    email_script = substrate_dir / "fetch_all_emails.py"
    if email_script.exists():
        cmd = [
            sys.executable, 
            str(email_script), 
            "--filter", filter_query,
            "--output", "delta_emails.json"
        ]
        if run_command(cmd, cwd=substrate_dir, description="Fetching Delta Emails"):
            src = substrate_dir / "output" / "delta_emails.json"
            
            if src.exists():
                # Load fetched data with error handling
                emails = []
                delta_filename = f"emails_{current_index}.json"
                processing_success = False
                
                try:
                    data = json_io.load_json_best_effort(str(src), {})
                    if not data:
                        print(f"[WARN] Failed to parse {src}, trying to read as plain JSON...")
                        with open(src, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                    emails = data.get('emails', [] if isinstance(data, dict) else [])
                    processing_success = True
                except Exception as e:
                    print(f"[ERROR] Failed to load {src}: {e}")
                    print(f"[ERROR] File may be corrupted. Skipping email processing this cycle.")
                    # Remove corrupted file to allow clean fetch next time
                    try:
                        src.unlink()
                        print(f"[INFO] Removed corrupted file: {src}")
                    except Exception:
                        pass
                
                if processing_success and emails:
                    # 1. Save Delta File (No timestamp in filename)
                    delta_path = outlook_path / delta_filename
                    shutil.copy2(str(src), str(delta_path))
                    print(f"[INFO] Saved delta emails to {delta_path}")
                    
                    # Log Entry
                    log_entry = {
                        "index": current_index,
                        "timestamp": current_run_time,
                        "filter_since": last_run,
                        "file": delta_filename,
                        "fetched_count": len(emails),
                        "status": "success"
                    }
                    save_log(outlook_log, log_entry)

                    any_data_found = True
                    # 2. Update All Data File
                    all_emails_path = outlook_path / "all_emails.json"
                    added, total = merge_data(emails, all_emails_path, item_key='Id')
                    print(f"[INFO] Merged {added} new emails into all_emails.json (Total: {total})")

                    # 3. Copy to Pipeline Input (Delta) - Always copy to ensure pipeline sees current state
                    dest_dir = base_dir / "outlook_v2" / "input"
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    dest = dest_dir / "all_emails.json"
                    shutil.copy2(str(src), str(dest))
                    print(f"[INFO] Copied delta emails to pipeline input: {dest}")

                    # Print Summary
                    print(f"\n[DELTA EMAILS] Found {len(emails)} new emails:")
                    for email in emails:
                        subject = email.get('Subject', 'No Subject')
                        sender = email.get('From', {}).get('EmailAddress', {}).get('Name', 'Unknown')
                        received = email.get('ReceivedDateTime', '')
                        print(f" - [{received}] {sender}: {subject}")
                elif processing_success:
                    print("[INFO] No new emails found.")
                    # Still log the successful fetch
                    log_entry = {
                        "index": current_index,
                        "timestamp": current_run_time,
                        "filter_since": last_run,
                        "file": delta_filename,
                        "fetched_count": 0,
                        "status": "success"
                    }
                    save_log(outlook_log, log_entry)
            else:
                print("[WARN] Delta emails file not found after execution.")
    else:
        print(f"[ERROR] Script not found: {email_script}")

    # --- 2. Fetch Teams Messages ---
    teams_script = substrate_dir / "fetch_all_teams_messages.py"
    if teams_script.exists():
        cmd = [
            sys.executable, 
            str(teams_script), 
            "--filter", filter_query,
            "--output", "delta_teams_messages.json"
        ]
        if run_command(cmd, cwd=substrate_dir, description="Fetching Delta Teams Messages"):
            src = substrate_dir / "output" / "delta_teams_messages.json"
            
            if src.exists():
                # Load fetched data
                with open(src, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    messages = data.get('messages', [])

                # 1. Save Delta File (No timestamp in filename)
                delta_filename = f"teams_{current_index}.json"
                delta_path = teams_path / delta_filename
                shutil.copy2(str(src), str(delta_path))
                print(f"[INFO] Saved delta teams messages to {delta_path}")
                
                # Log Entry
                log_entry = {
                    "index": current_index,
                    "timestamp": current_run_time,
                    "filter_since": last_run,
                    "file": delta_filename,
                    "fetched_count": len(messages),
                    "status": "success"
                }
                save_log(teams_log, log_entry)

                if len(messages) > 0:
                    any_data_found = True
                    # 2. Update All Data File
                    all_teams_path = teams_path / "all_teams_messages.json"
                    added, total = merge_data(messages, all_teams_path, item_key='Id')
                    print(f"[INFO] Merged {added} new messages into all_teams_messages.json (Total: {total})")
                else:
                    print("[INFO] No new teams messages found.")

                # 3. Copy to Pipeline Input (Delta) - Always copy
                dest_dir = base_dir / "teams" / "input"
                dest_dir.mkdir(parents=True, exist_ok=True)
                dest = dest_dir / "all_teams_messages.json"
                shutil.copy2(str(src), str(dest))
                print(f"[INFO] Copied delta teams messages to pipeline input: {dest}")

                # Print Summary
                print(f"\n[DELTA TEAMS] Found {len(messages)} new messages:")
                for msg in messages:
                    content = msg.get('Body', {}).get('Content', '').strip().replace('\n', ' ')[:100]
                    sender = msg.get('From', {}).get('User', {}).get('DisplayName', 'Unknown')
                    created = msg.get('CreatedDateTime', '')
                    print(f" - [{created}] {sender}: {content}...")
            else:
                print("[WARN] Delta teams messages file not found after execution.")
    else:
        print(f"[ERROR] Script not found: {teams_script}")

    if any_data_found:
        print(f"[INFO] Fetch complete. Data saved with index {current_index}.")
    else:
        print(f"[INFO] Fetch complete. No new data found, but empty files saved with index {current_index}.")

if __name__ == "__main__":
    main()
