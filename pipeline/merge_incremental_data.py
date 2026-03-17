import json
import os
import argparse
from pathlib import Path

def load_json(file_path):
    if not os.path.exists(file_path):
        return {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] Error reading {file_path}: {e}")
        return {}

def save_json(data, file_path):
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"[INFO] Saved to {file_path}")
    except Exception as e:
        print(f"[ERROR] Failed to save {file_path}: {e}")

def merge_threads(new_threads_file, master_threads_file, output_file):
    """
    Merges new threads into the master threads list.
    - If a thread ID exists, update it (add new messages, update latest_received).
    - If it's new, add it.
    """
    print(f"\n[MERGE] Merging Email Threads...")
    
    # Load new threads (list of dicts)
    new_threads_list = load_json(new_threads_file)
    if not isinstance(new_threads_list, list):
        new_threads_list = []
    
    # Load master threads (list of dicts)
    master_threads_list = load_json(master_threads_file)
    if not isinstance(master_threads_list, list):
        master_threads_list = []

    # Convert master to dict for easy lookup by ID
    master_map = {t['id']: t for t in master_threads_list}
    
    updated_count = 0
    new_count = 0
    
    updated_threads_list = [] # List of threads that were touched (new or updated)

    for new_thread in new_threads_list:
        tid = new_thread['id']
        
        if tid in master_map:
            # Update existing thread
            existing = master_map[tid]
            
            # Merge messages (deduplicate by InternetMessageId or Id)
            existing_msgs = existing.get('messages', [])
            new_msgs = new_thread.get('messages', [])
            
            # Create a set of existing message IDs
            existing_msg_ids = set()
            for m in existing_msgs:
                mid = m.get('InternetMessageId') or m.get('Id')
                if mid:
                    existing_msg_ids.add(mid)
            
            # Add only new messages
            added_msgs = []
            for m in new_msgs:
                mid = m.get('InternetMessageId') or m.get('Id')
                if mid and mid not in existing_msg_ids:
                    existing_msgs.append(m)
                    added_msgs.append(m)
                    existing_msg_ids.add(mid)
            
            if added_msgs:
                # Re-sort messages by time
                existing_msgs.sort(key=lambda x: x.get("ReceivedDateTime", ""), reverse=True)
                
                # Update metadata
                existing['count'] = len(existing_msgs)
                existing['latest_received'] = existing_msgs[0].get("ReceivedDateTime")
                # Subject might change? Let's keep original or update? Usually keep original unless empty.
                if not existing.get('subject') and new_thread.get('subject'):
                    existing['subject'] = new_thread['subject']
                
                updated_count += 1
                updated_threads_list.append(existing)
        else:
            # New thread
            master_map[tid] = new_thread
            new_count += 1
            updated_threads_list.append(new_thread)

    # Convert back to list and sort by latest_received
    final_list = list(master_map.values())
    final_list.sort(key=lambda x: x.get("latest_received", "") or "", reverse=True)
    
    # Save Master
    save_json(final_list, master_threads_file)
    
    # Save Delta (Updated/New threads only)
    save_json(updated_threads_list, output_file)
    
    print(f"[MERGE] Threads: {new_count} new, {updated_count} updated. Total: {len(final_list)}")

def merge_teams_conversations(new_conv_dir, master_conv_dir, output_delta_dir):
    """
    Merges new Teams conversations into the master repository.
    - Teams conversations are stored as individual JSON files.
    - We match by 'conversation_id'.
    """
    print(f"\n[MERGE] Merging Teams Conversations...")
    
    new_dir = Path(new_conv_dir)
    master_dir = Path(master_conv_dir)
    delta_dir = Path(output_delta_dir)
    
    master_dir.mkdir(parents=True, exist_ok=True)
    delta_dir.mkdir(parents=True, exist_ok=True)
    
    if not new_dir.exists():
        print("[WARN] New conversations directory not found.")
        return

    updated_count = 0
    new_count = 0

    # Iterate over new conversation files
    for new_file in new_dir.glob("conversation_*.json"):
        new_data = load_json(new_file)
        conv_id = new_data.get('conversation_id')
        
        if not conv_id:
            continue
            
        # Find if this conversation exists in master
        # Since filenames might differ slightly (due to safe naming), we might need to search or rely on consistent naming.
        # process_teams_messages uses: f"conversation_{idx:03d}_{safe_conv_id[:50]}.json"
        # The idx might change. The safe_conv_id should be stable.
        # Let's try to find a file in master that contains the safe_conv_id or open them to check ID.
        # Opening all is slow. Let's rely on the fact that we can construct the safe ID.
        
        safe_conv_id = conv_id.replace(':', '_').replace('@', '_at_').replace('.', '_')
        # We look for any file containing this safe_conv_id in master
        # This is a bit heuristic.
        
        existing_file = None
        for f in master_dir.glob(f"*_{safe_conv_id[:50]}.json"):
            existing_file = f
            break
            
        if existing_file:
            # Update existing
            master_data = load_json(existing_file)
            
            # Merge messages
            existing_msgs = master_data.get('messages', [])
            new_msgs = new_data.get('messages', [])
            
            existing_ids = {m.get('message_id') for m in existing_msgs if m.get('message_id')}
            
            added = False
            for m in new_msgs:
                mid = m.get('message_id')
                if mid and mid not in existing_ids:
                    existing_msgs.append(m)
                    existing_ids.add(mid)
                    added = True
            
            if added:
                # Re-sort
                existing_msgs.sort(key=lambda x: x.get('timestamp', ''))
                
                # Update metadata
                master_data['messages'] = existing_msgs
                master_data['message_count'] = len(existing_msgs)
                master_data['last_message_time'] = existing_msgs[-1].get('timestamp')
                # Update participants if needed
                # ... (simplified for now)
                
                save_json(master_data, existing_file)
                save_json(master_data, delta_dir / existing_file.name) # Save to delta
                updated_count += 1
        else:
            # New conversation
            # Copy to master
            # We need a unique filename. The new_file name has an index that might conflict.
            # Let's generate a name without the index or use a new index?
            # Simple approach: Use the new filename but ensure no collision? 
            # Or just copy it.
            target_name = new_file.name
            # If target exists (collision of index but different ID?), rename.
            # Actually, process_teams_messages generates unique names per run.
            # Let's just copy it to master.
            
            shutil.copy2(new_file, master_dir / target_name)
            shutil.copy2(new_file, delta_dir / target_name)
            new_count += 1

    print(f"[MERGE] Teams: {new_count} new, {updated_count} updated conversations.")

def main():
    parser = argparse.ArgumentParser(description="Merge incremental data into master datasets.")
    parser.add_argument("--new-threads", help="Path to new threads JSON")
    parser.add_argument("--master-threads", help="Path to master threads JSON")
    parser.add_argument("--output-threads", help="Path to save updated/new threads (delta)")
    
    parser.add_argument("--new-teams-dir", help="Directory containing new teams conversation JSONs")
    parser.add_argument("--master-teams-dir", help="Directory containing master teams conversation JSONs")
    parser.add_argument("--output-teams-dir", help="Directory to save updated/new teams conversations (delta)")
    
    args = parser.parse_args()

    if args.new_threads and args.master_threads and args.output_threads:
        merge_threads(args.new_threads, args.master_threads, args.output_threads)
        
    if args.new_teams_dir and args.master_teams_dir and args.output_teams_dir:
        merge_teams_conversations(args.new_teams_dir, args.master_teams_dir, args.output_teams_dir)

if __name__ == "__main__":
    import shutil # Ensure imported
    main()
