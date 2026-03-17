import json
import argparse
import os
from datetime import datetime


def _get_item_text(item):
    if not isinstance(item, dict):
        return ""
    return (item.get("task") or item.get("description") or "").strip()


def _teams_action_stable_id(action: dict) -> str | None:
    if not isinstance(action, dict):
        return None
    for k in ("task_id", "action_id", "id", "item_id", "message_id"):
        v = action.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


def _teams_action_dedup_key(action: dict) -> str | None:
    if not isinstance(action, dict):
        return None
    stable = _teams_action_stable_id(action)
    if stable:
        return f"id:{stable}"
    desc = str(action.get("description") or action.get("task") or "").strip()
    if desc:
        return f"desc:{desc}"
    return None


def compute_action_summary(items):
    """Fallback action summary for sidebar/title.

    Prefers the first action text; if multiple, appends a compact count.
    """
    texts = [t for t in (_get_item_text(i) for i in (items or [])) if t]
    if not texts:
        return None
    if len(texts) == 1:
        return texts[0]
    return f"{texts[0]} (+{len(texts) - 1})"

def load_json(filepath):
    if not os.path.exists(filepath):
        print(f"Warning: File not found: {filepath}")
        return {}
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_history(fetch_log_path, user_id="user"):
    history_map = {}
    if not os.path.exists(fetch_log_path):
        return history_map
    
    try:
        print(f"Loading history from {fetch_log_path}...")
        with open(fetch_log_path, 'r', encoding='utf-8') as f:
            logs = json.load(f)
            
        base_dir = os.path.dirname(fetch_log_path)
        
        for entry in logs:
            idx = entry.get('index')
            ts = entry.get('timestamp')
            # Construct event file path
            event_filename = f"outlook_events_{user_id}_{idx}.json"
            event_path = os.path.join(base_dir, event_filename)
            
            if os.path.exists(event_path):
                with open(event_path, 'r', encoding='utf-8') as ef:
                    event_data = json.load(ef)
                    for event in event_data.get('events', []):
                        eid = event.get('event_id')
                        if eid not in history_map:
                            history_map[eid] = []
                        
                        # Extract relevant info
                        history_map[eid].append({
                            'timestamp': ts,
                            'summary': event.get('summary'),
                            'todos': event.get('todos', []),
                            'recommendations': event.get('recommendations', [])
                        })
    except Exception as e:
        print(f"Error loading history: {e}")
        
    return history_map

def load_teams_history(fetch_log_path, user_id):
    history_map = {}
    if not os.path.exists(fetch_log_path):
        return history_map
    
    try:
        print(f"Loading Teams history from {fetch_log_path}...")
        with open(fetch_log_path, 'r', encoding='utf-8') as f:
            logs = json.load(f)
            
        base_dir = os.path.dirname(fetch_log_path)
        
        for entry in logs:
            idx = entry.get('index')
            ts = entry.get('timestamp')
            
            # Construct analysis file path
            # Folder: teams_analysis_{idx}
            # File: teams_analysis_summary_{user_id}.json
            analysis_dir = os.path.join(base_dir, f"teams_analysis_{idx}")
            analysis_file = os.path.join(analysis_dir, f"teams_analysis_summary_{user_id}.json")
            
            if os.path.exists(analysis_file):
                with open(analysis_file, 'r', encoding='utf-8') as af:
                    analysis_data = json.load(af)
                    
                    # Handle list or dict wrapper
                    conversations = []
                    if isinstance(analysis_data, list):
                        conversations = analysis_data
                    elif isinstance(analysis_data, dict):
                        conversations = analysis_data.get('results', [])
                        
                    for conv in conversations:
                        cid = conv.get('conversation_id')
                        if cid not in history_map:
                            history_map[cid] = []
                        
                        # Extract relevant info
                        history_map[cid].append({
                            'timestamp': ts,
                            'summary': conv.get('summary', {}),
                            # Unified actions: treat tasks + recommended_actions as todos.
                            'todos': conv.get('todos') or ((conv.get('tasks') or []) + (conv.get('recommended_actions') or []))
                        })
    except Exception as e:
        print(f"Error loading Teams history: {e}")
        
    return history_map

def get_top_participants(messages, limit=3):
    counts = {}
    for msg in messages:
        sender = msg.get('From', {}).get('EmailAddress', {}).get('Name', 'Unknown')
        counts[sender] = counts.get(sender, 0) + 1
    
    sorted_participants = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    return [p[0] for p in sorted_participants[:limit]]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--teams-linked", required=False)
    parser.add_argument("--teams-analysis", required=True, help="Path to teams analysis summary JSON")
    parser.add_argument("--outlook-events", required=True)
    parser.add_argument("--teams-raw", required=True)
    parser.add_argument("--outlook-threads", required=False, help="Path to raw outlook threads JSON")
    parser.add_argument("--fetch-log", required=False, help="Path to fetch_log.json for history")
    parser.add_argument("--teams-fetch-log", required=False, help="Path to teams fetch_log.json for history")
    parser.add_argument("--user-id", required=False, default="user", help="User ID for file paths")
    parser.add_argument("--output-json", required=True, help="Path to output data JSON")
    args = parser.parse_args()

    # Load Teams skip sender names for filtering.
    # Preferred: pipeline_config.json (repo root).
    # Back-compat: teams/config.json legacy key.
    skip_sender_names = []
    try:
        from pathlib import Path
        repo_root = Path(__file__).resolve().parent.parent
        pipeline_path = repo_root / 'pipeline_config.json'
        if pipeline_path.exists():
            with pipeline_path.open('r', encoding='utf-8') as f:
                cfg = json.load(f) or {}
            if isinstance(cfg, dict):
                skip_sender_names = cfg.get('teams_skip_sender_names', []) or []
    except Exception as e:
        print(f"Warning: Could not load pipeline_config.json: {e}")

    if not skip_sender_names:
        teams_config_path = os.path.join('teams', 'config.json')
        if os.path.exists(teams_config_path):
            try:
                with open(teams_config_path, 'r', encoding='utf-8') as f:
                    teams_config = json.load(f)
                if isinstance(teams_config, dict):
                    skip_sender_names = teams_config.get('skip_sender_names', []) or []
            except Exception as e:
                print(f"Warning: Could not load legacy teams config: {e}")

    # Normalize
    if isinstance(skip_sender_names, list):
        skip_sender_names = [str(s).strip().lower() for s in skip_sender_names if str(s).strip()]
    else:
        skip_sender_names = []

    print("Loading data...")
    teams_linked = load_json(args.teams_linked) if args.teams_linked else {}
    teams_analysis = load_json(args.teams_analysis)
    
    # Use linked data if available (has relationships), otherwise use raw analysis
    teams_data = teams_linked if teams_linked else teams_analysis

    outlook_events = load_json(args.outlook_events)
    teams_raw = load_json(args.teams_raw)
    
    # Extract deduped events list if present
    deduped_cards = []
    if 'deduped_events' in outlook_events and outlook_events['deduped_events']:
        for deduped in outlook_events['deduped_events']:
            deduped_cards.append({
                "type": "Outlook",
                "event_id": deduped.get("event_id"),
                "event_name": deduped.get("event_name"),
                "merged_into": deduped.get("merged_into"),
                "merged_into_name": deduped.get("merged_into_name"),
                "reason": deduped.get("reason")
            })
        print(f"Found {len(deduped_cards)} deduplicated events")
    
    outlook_threads_lookup = {}
    if args.outlook_threads:
        print(f"Loading Outlook threads from {args.outlook_threads}")
        threads_data = load_json(args.outlook_threads)
        
        if isinstance(threads_data, list):
            # It's the processed threads.json
            for t in threads_data:
                outlook_threads_lookup[t['id']] = t
        elif isinstance(threads_data, dict) and ('emails' in threads_data or 'items' in threads_data):
            # It's the raw all_emails.json
            print("Detected raw emails format. Grouping by ConversationId...")
            email_list = threads_data.get('emails', []) or threads_data.get('items', [])
            for email in email_list:
                cid = email.get('ConversationId')
                if not cid:
                    continue
                
                if cid not in outlook_threads_lookup:
                    outlook_threads_lookup[cid] = {'id': cid, 'messages': []}
                
                outlook_threads_lookup[cid]['messages'].append(email)
        else:
            print("Warning: Unknown format for outlook threads file.")

    # Group Teams messages by Conversation ID
    chat_lookup = {}
    raw_messages = teams_raw.get('messages', []) or teams_raw.get('items', [])
    for msg in raw_messages:
        cid = msg.get('ClientConversationId')
        if not cid:
            continue
        if cid not in chat_lookup:
            chat_lookup[cid] = {'id': cid, 'messages': []}
        
        chat_lookup[cid]['messages'].append(msg)

    # Sort messages in each chat by time
    for cid in chat_lookup:
        chat_lookup[cid]['messages'].sort(key=lambda x: x.get('CreatedDateTime', ''))

    # Load History
    history_map = {}
    if args.fetch_log:
        history_map = load_history(args.fetch_log, args.user_id)

    teams_history_map = {}
    if args.teams_fetch_log:
        teams_history_map = load_teams_history(args.teams_fetch_log, args.user_id)

    # Build Series Map
    print("Building Event map...")
    series_map = {}
    
    for e in outlook_events.get('events', []):
        eid = e['event_id']
        event_name = e.get('event_name', 'Unnamed Event')
        
        series_map[eid] = e
        series_map[eid]['linked_teams_items'] = []
        
        # Assign UI IDs to Outlook items
        for i, t in enumerate(e.get('todos', [])):
            t['_ui_id'] = f"action-outlook-todo-{eid}-{i}"
        for i, r in enumerate(e.get('recommendations', [])):
            r['_ui_id'] = f"action-outlook-rec-{eid}-{i}"

    # Process Teams Links
    print("Processing Teams links...")
    unlinked_conversations = []
    
    teams_results = teams_data.get('results', []) if isinstance(teams_data, dict) else teams_data
    
    for conv in teams_results:
        cid = conv.get('conversation_id')
        safe_cid = cid.replace(':', '_').replace('@', '_').replace('.', '_')
        chat_name = conv.get('chat_name')
        
        linked_count = 0
        unlinked_items = []
        linked_items = []
        
        # Unified Teams todos (tasks + recommended_actions)
        todos = conv.get('todos')
        if not isinstance(todos, list):
            todos = (conv.get('tasks') or []) + (conv.get('recommended_actions') or [])

        seen_keys = set()
        for i, t in enumerate(todos):
            if not isinstance(t, dict):
                continue
            dedup_key = _teams_action_dedup_key(t)
            if dedup_key and dedup_key in seen_keys:
                continue
            if dedup_key:
                seen_keys.add(dedup_key)

            link = (t or {}).get('related_outlook_event') if isinstance(t, dict) else None
            stable = _teams_action_stable_id(t)
            suffix = stable if stable else str(i)
            ui_id = f"action-teams-todo-{safe_cid}-{suffix}"
            item = {
                # Frontend uses this to pick a stable suffix; treating everything as a task.
                'type': 'Task',
                'description': (t or {}).get('description') if isinstance(t, dict) else None,
                'chat_name': chat_name,
                'chat_id': safe_cid,
                'link': link,
                'original_data': t,
                '_ui_id': ui_id
            }
            if link and link in series_map:
                series_map[link]['linked_teams_items'].append(item)
                linked_items.append(item)
                linked_count += 1
            else:
                unlinked_items.append(item)
        
        # Store for unlinked section
        # Strip raw task lists from embedded conversation object to avoid duplicates in briefing_data.
        # Canonical items are provided as linked_items/unlinked_items.
        if isinstance(conv, dict):
            conv_clean = {k: v for k, v in conv.items() if k not in ('tasks', 'recommended_actions', 'todos')}
        else:
            conv_clean = conv
        unlinked_conversations.append({
            'conversation': conv_clean,
            'unlinked_items': unlinked_items,
            'linked_items': linked_items,
            'linked_count': linked_count
        })

    # Calculate priority for each conversation and sort
    priority_map = {'High': 0, 'Medium': 1, 'Low': 2}
    
    for item in unlinked_conversations:
        all_items = item['unlinked_items'] + item['linked_items']
        highest_priority = 'Low'
        current_min_val = 2
        
        for i in all_items:
            p = i.get('original_data', {}).get('priority', 'Medium')
            val = priority_map.get(p, 1) # Default to Medium (1) if unknown
            if val < current_min_val:
                current_min_val = val
                highest_priority = p
        
        item['calculated_priority'] = highest_priority

    # Sort unlinked_conversations
    unlinked_conversations.sort(key=lambda x: priority_map.get(x['calculated_priority'], 2))

    # Merge and Sort
    all_cards = []
    priority_order = {'High': 0, 'Medium': 1, 'Low': 2}

    # Add Outlook Series
    for s in series_map.values():
        # Reclassify recommendations with user_role as assignee or collaborator to todos
        recommendations = s.get('recommendations', [])
        todos = s.get('todos', [])
        
        new_recommendations = []
        for rec in recommendations:
            orig = rec.get('original_data', rec)
            user_role = orig.get('user_role', 'observer')
            
            if user_role in ['assignee', 'collaborator']:
                # Move to todos
                todos.append(rec)
            else:
                new_recommendations.append(rec)
        
        s['todos'] = todos
        s['recommendations'] = new_recommendations

        # Ensure we have a concise action summary for the UI sidebar.
        if not (isinstance(s.get('action_summary'), str) and s.get('action_summary', '').strip()):
            summary = compute_action_summary((s.get('todos') or []) + (s.get('recommendations') or []))
            if summary:
                s['action_summary'] = summary
        
        p = s.get('priority_level', 'Medium')
        all_cards.append({
            'type': 'Outlook',
            'priority': p,
            'sort_val': priority_order.get(p, 3),
            'data': s
        })

    # Add Teams Conversations
    filtered_teams_count = 0
    for item in unlinked_conversations:
        # No longer reclassify by type; everything is treated as a todo.
        
        p = item['calculated_priority']
        
        # Calculate top participants here
        cid = item['conversation'].get('conversation_id')
        raw_chat = chat_lookup.get(cid, {})
        messages = raw_chat.get('messages', [])
        top_participants = get_top_participants(messages)
        item['top_participants'] = top_participants

        # Filter out conversations with only 2 participants where one is in skip_sender_names
        if skip_sender_names:
            has_skipped_sender = any(sender in skip_sender_names for sender in top_participants)
            if has_skipped_sender:
                filtered_teams_count += 1
                chat_name = item['conversation'].get('chat_name', 'Unknown')
                print(f"\033[93m[FILTERED] Teams conversation '{chat_name}' - 2 participants with one in skip list: {top_participants}\033[0m")
                continue

        # Ensure we have a concise action summary for the UI sidebar.
        conv_summary = (item.get('conversation') or {}).get('summary')
        if not isinstance(conv_summary, dict):
            item.setdefault('conversation', {}).setdefault('summary', {})
            conv_summary = item['conversation']['summary']

        if not (isinstance(conv_summary.get('action_summary'), str) and conv_summary.get('action_summary', '').strip()):
            # Prefer raw extracted todo descriptions if available.
            raw_actions = []
            conv_obj = item.get('conversation', {})
            todos = conv_obj.get('todos')
            if not isinstance(todos, list):
                todos = (conv_obj.get('tasks') or []) + (conv_obj.get('recommended_actions') or [])
            for t in todos:
                if isinstance(t, dict):
                    raw_actions.append({'description': t.get('description')})

            summary = compute_action_summary(raw_actions)
            if not summary:
                # Fallback to the items we constructed for the UI.
                summary = compute_action_summary((item.get('unlinked_items') or []) + (item.get('linked_items') or []))
            if summary:
                conv_summary['action_summary'] = summary

        all_cards.append({
            'type': 'Teams',
            'priority': p,
            'sort_val': priority_order.get(p, 3),
            'data': item
        })
    
    if filtered_teams_count > 0:
        print(f"\n\033[93m[SUMMARY] Filtered out {filtered_teams_count} Teams conversations with 2 participants where one is in skip list.\033[0m")

    # Sort by priority
    all_cards.sort(key=lambda x: x['sort_val'])

    # Construct final data object
    data = {
        "cards": all_cards,
        "deduped_cards": deduped_cards,
        "history_map": history_map,
        "teams_history_map": teams_history_map,
        "chat_lookup": chat_lookup,
        "outlook_threads_lookup": outlook_threads_lookup
    }

    print(f"Saving processed data to {args.output_json}...")
    with open(args.output_json, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    print("Done.")

if __name__ == "__main__":
    main()
