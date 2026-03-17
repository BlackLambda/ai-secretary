"""
Process Teams messages from all_teams_messages.json and generate conversation history for each chat group.
Groups messages by ClientConversationId to create organized conversation threads.
"""

import json
import sys
import shutil
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import html


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


def extract_text_from_body(body_content):
    """Extract plain text from HTML body content."""
    if not body_content:
        return ""
    
    # Simple HTML tag removal
    text = body_content.replace('<br>', '\n').replace('</p>', '\n').replace('<p>', '')
    # Remove all other HTML tags
    import re
    text = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities
    text = html.unescape(text)
    # Clean up whitespace
    text = '\n'.join(line.strip() for line in text.split('\n') if line.strip())
    return text


def get_participants(messages):
    """Extract all unique participants from messages."""
    participants_set = set()
    
    for msg in messages:
        # Add sender
        sender_info = msg.get('Sender', {}).get('EmailAddress', {})
        sender_email = sender_info.get('Address', '')
        sender_name = sender_info.get('Name', '')
        
        if sender_email:
            participants_set.add(sender_email)
        elif sender_name:
            # Fallback to name if email is missing (e.g. bots)
            participants_set.add(sender_name)
        
        # Add recipients
        for recipient in msg.get('ToRecipients', []):
            email = recipient.get('EmailAddress', {}).get('Address', '')
            if email:
                participants_set.add(email)
    
    return sorted(list(participants_set))


def get_chat_name(messages, participants, topic=None):
    """Generate a descriptive name for the chat group."""
    # Use provided topic if available
    if topic:
        return topic

    # Get participant names (not emails)
    names = set()
    for msg in messages:
        sender = msg.get('Sender', {}).get('EmailAddress', {}).get('Name', '')
        if sender:
            names.add(sender)
        
        for recipient in msg.get('ToRecipients', []):
            name = recipient.get('EmailAddress', {}).get('Name', '')
            if name:
                names.add(name)
    
    # Check if it's a meeting chat (contains "meeting_" in ClientConversationId)
    client_conv_id = messages[0].get('ClientConversationId', '')
    if 'meeting_' in client_conv_id:
        return f"Meeting Chat ({len(names)} participants)"
    
    # For small groups, list names
    if len(names) <= 3:
        return " & ".join(sorted(names))
    
    # For larger groups, use participant count
    return f"Group Chat ({len(names)} participants)"


def format_message(msg):
    """Format a single message as a dictionary."""
    sender_name = msg.get('Sender', {}).get('EmailAddress', {}).get('Name', 'Unknown')
    sender_email = msg.get('Sender', {}).get('EmailAddress', {}).get('Address', '')
    sent_time = msg.get('SentDateTime', '')
    subject = msg.get('Subject', '').strip()
    
    # Get message body
    body = msg.get('Body', {})
    content = body.get('Content', '')
    text_content = extract_text_from_body(content)
    
    # Check for attachments
    has_attachments = msg.get('HasAttachments', False)
    
    # Format message as dictionary
    formatted = {
        'timestamp': sent_time,
        'sender_name': sender_name,
        'sender_email': sender_email,
        'subject': subject if subject else None,
        'content': text_content if text_content else None,
        'has_attachments': has_attachments,
        'message_id': msg.get('Id', '')
    }
    
    return formatted


def process_teams_messages(input_file, output_dir, config_file=None, max_conversations=None):
    """Process Teams messages and generate conversation history files."""
    
    print(f"[PROCESSING] Reading Teams messages from: {input_file}")
    
    # Load configuration for skipping.
    # Preferred source: repo-root pipeline_config.json.
    # Back-compat: also accept legacy teams/config.json keys when provided.
    skip_chat_names: list[str] = []
    skip_ids: list[str] = []
    skip_sender_names: list[str] = []
    skip_sender_without_email = False

    pipeline_cfg = _load_pipeline_config()
    if pipeline_cfg:
        skip_chat_names = [s.lower() for s in _as_str_list(pipeline_cfg.get('teams_skip_chat_name_terms'))]
        skip_sender_names = [s.lower() for s in _as_str_list(pipeline_cfg.get('teams_skip_sender_names'))]
        try:
            skip_sender_without_email = bool(pipeline_cfg.get('teams_skip_sender_without_email', False))
        except Exception:
            skip_sender_without_email = False

    if config_file and Path(config_file).exists():
        print(f"[CONFIG] Loading legacy Teams config from: {config_file}")
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                legacy = json.load(f)

            if isinstance(legacy, list):
                print(f"[WARNING] Legacy config file contains a list, expected a dictionary. Content: {legacy[:2]}...")
                legacy = {}

            # Only fall back to legacy keys when pipeline_config didn't specify them.
            if not skip_chat_names:
                skip_chat_names = [name.lower() for name in _as_str_list(legacy.get('skip_chat_names'))]
            skip_ids = _as_str_list(legacy.get('skip_conversation_ids'))
            if not skip_sender_names:
                skip_sender_names = [name.lower() for name in _as_str_list(legacy.get('skip_sender_names'))]
            if not skip_sender_without_email:
                skip_sender_without_email = bool(legacy.get('skip_sender_without_email', False))

        except Exception as e:
            print(f"[WARNING] Failed to load legacy config file: {e}")

    if skip_chat_names or skip_ids or skip_sender_names or skip_sender_without_email:
        print(
            f"[CONFIG] Loaded skip rules: {len(skip_chat_names)} chat name terms, {len(skip_ids)} IDs, "
            f"{len(skip_sender_names)} sender names, skip_no_email={skip_sender_without_email}"
        )
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Clean output directory content
    print(f"[INFO] Cleaning output directory: {output_path}")
    for item in output_path.iterdir():
        # Safety check: preserve teams_analysis folder if it exists here
        if item.name == 'teams_analysis':
            continue

        try:
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        except Exception as e:
            print(f"[WARNING] Failed to delete {item.name}: {e}")

    # Load messages
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to read input file: {e}")
        return False
    
    # Support multiple formats: "messages", "items", "value"
    messages = data.get('messages') or data.get('items') or data.get('value', [])
    total_count = data.get('total_count') or data.get('count') or len(messages)
    
    print(f"[INFO] Total messages: {total_count}")
    
    # Group messages by ClientConversationId
    conversations = defaultdict(list)
    for msg in messages:
        conv_id = msg.get('ClientConversationId', 'unknown')
        conversations[conv_id].append(msg)
    
    print(f"[INFO] Found {len(conversations)} conversation groups")
    
    # Sort conversations by most recent message and apply limit
    sorted_conversations = sorted(
        conversations.items(),
        key=lambda x: max((msg.get('CreatedDateTime', '') for msg in x[1]), default=''),
        reverse=True
    )
    
    # Apply max_conversations limit if specified
    if max_conversations and max_conversations > 0:
        original_count = len(sorted_conversations)
        sorted_conversations = sorted_conversations[:max_conversations]
        print(f"[INFO] Limited conversations from {original_count} to {len(sorted_conversations)} (max_conversations={max_conversations})")
    
    # Process each conversation
    conv_summaries = []
    conversations_json = []
    
    for idx, (conv_id, msgs) in enumerate(sorted_conversations, 1):
        # Filter messages by sender name and email
        if skip_sender_names or skip_sender_without_email:
            filtered_msgs = []
            for msg in msgs:
                sender_name = msg.get('Sender', {}).get('EmailAddress', {}).get('Name', '')
                sender_email = msg.get('Sender', {}).get('EmailAddress', {}).get('Address', '')
                
                # Check sender name
                if sender_name and sender_name.lower() in skip_sender_names:
                    continue
                
                # Check sender email
                if skip_sender_without_email and not sender_email:
                    continue
                
                # Check for empty content (no text and no attachments)
                # We need to format it first or check raw body to know if it's empty
                # Let's check raw body here to be efficient
                body = msg.get('Body', {})
                content = body.get('Content', '')
                text_content = extract_text_from_body(content)
                has_attachments = msg.get('HasAttachments', False)
                
                if not text_content and not has_attachments:
                    continue

                filtered_msgs.append(msg)
            msgs = filtered_msgs
        
        if not msgs:
            continue

        # Sort messages by sent time
        msgs_sorted = sorted(msgs, key=lambda m: m.get('SentDateTime', ''))
        
        # Get participants
        participants = get_participants(msgs_sorted)
        
        # Check if conversation should be dropped
        # If after filtering, only 1 participant remains (likely the user) or 0, drop it.
        if len(participants) <= 1:
             print(f"[SKIP] Dropping conversation {conv_id} - {len(participants)} participant(s) remaining.")
             continue

        # Get topic from the first message that has one
        topic = None
        for msg in msgs_sorted:
            if msg.get('ChatTopic'):
                topic = msg.get('ChatTopic')
                break

        # Generate chat name
        chat_name = get_chat_name(msgs_sorted, participants, topic)
        
        # Check if chat should be skipped
        should_skip = False
        
        # Check ID
        if conv_id in skip_ids:
            should_skip = True
            print(f"[SKIP] Skipping chat by ID: {conv_id}")
        
        # Check Name
        if not should_skip and skip_chat_names:
            chat_name_lower = chat_name.lower()
            for skip_term in skip_chat_names:
                if skip_term in chat_name_lower:
                    should_skip = True
                    print(f"[SKIP] Skipping chat by Name: {chat_name} (matched '{skip_term}')")
                    break
            
        if should_skip:
            continue
        
        # Create safe filename
        safe_conv_id = conv_id.replace(':', '_').replace('@', '_at_').replace('.', '_')
        filename = f"conversation_{idx:03d}_{safe_conv_id[:50]}.json"
        output_file = output_path / filename
        
        # Get date range
        first_msg_time = msgs_sorted[0].get('SentDateTime', '')
        last_msg_time = msgs_sorted[-1].get('SentDateTime', '')
        
        try:
            first_dt = datetime.fromisoformat(first_msg_time.replace('Z', '+00:00'))
            last_dt = datetime.fromisoformat(last_msg_time.replace('Z', '+00:00'))
            date_range = f"{first_dt.strftime('%Y-%m-%d')} to {last_dt.strftime('%Y-%m-%d')}"
        except:
            date_range = f"{first_msg_time} to {last_msg_time}"
        
        # Format messages
        formatted_messages = [format_message(msg) for msg in msgs_sorted]
        
        # Create conversation JSON object
        conversation_data = {
            'conversation_id': conv_id,
            'chat_name': chat_name,
            'chat_topic': topic,
            'participants': participants,
            'participantsCount': len(participants),
            'message_count': len(msgs_sorted),
            'date_range': date_range,
            'first_message_time': first_msg_time,
            'last_message_time': last_msg_time,
            'messages': formatted_messages
        }
        
        # Write conversation JSON file
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(conversation_data, f, indent=2, ensure_ascii=False)
            
            conv_summaries.append({
                'file': filename,
                'chat_name': chat_name,
                'message_count': len(msgs_sorted),
                'date_range': date_range,
                'conv_id': conv_id,
                'participants': participants,
                'participantsCount': len(participants)
            })
            
            conversations_json.append(conversation_data)
            
            print(f"[OK] Generated: {filename} ({len(msgs_sorted)} messages, {len(participants)} participants)")
            
        except Exception as e:
            print(f"[ERROR] Failed to write conversation {idx}: {e}")
    
    # Generate complete JSON with all conversations
    all_conversations_file = output_path / "all_conversations.json"
    try:
        with open(all_conversations_file, 'w', encoding='utf-8') as f:
            json.dump({
                'generated_at': datetime.now().isoformat(),
                'total_conversations': len(conversations_json),
                'total_messages': total_count,
                'conversations': conversations_json
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n[OK] All conversations JSON generated: {all_conversations_file}")
        
    except Exception as e:
        print(f"[ERROR] Failed to write all conversations JSON: {e}")
    
    # Generate JSON summary (without individual messages)
    json_summary_file = output_path / "conversations_summary.json"
    try:
        # Sort by message count (most active first)
        conv_summaries.sort(key=lambda x: x['message_count'], reverse=True)
        
        with open(json_summary_file, 'w', encoding='utf-8') as f:
            json.dump({
                'generated_at': datetime.now().isoformat(),
                'total_conversations': len(conv_summaries),
                'total_messages': total_count,
                'conversations': conv_summaries
            }, f, indent=2, ensure_ascii=False)
        
        print(f"[OK] Conversations summary generated: {json_summary_file}")
        
    except Exception as e:
        print(f"[ERROR] Failed to write JSON summary: {e}")
    
    return True


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Process Teams messages and generate conversation history files.")
    parser.add_argument("input_json", help="Path to input Teams messages JSON file")
    parser.add_argument("output_dir", nargs='?', default=None, help="Output directory for conversation files")
    parser.add_argument("--config", help="Path to config JSON file")
    parser.add_argument("--max-conversations", type=int, help="Maximum number of conversations to process")
    args = parser.parse_args()
    
    input_file = Path(args.input_json)
    
    # Determine output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = Path("output") / "teams_conversations"
    
    # Check for config file
    config_file = args.config
    
    # Validate input file
    if not input_file.exists():
        print(f"[ERROR] Input file not found: {input_file}")
        sys.exit(1)
    
    print("=" * 80)
    print("TEAMS MESSAGES CONVERSATION PROCESSOR")
    print("=" * 80)
    
    success = process_teams_messages(input_file, output_dir, config_file, args.max_conversations)
    
    if success:
        print("\n[SUCCESS] All conversations processed successfully!")
        print(f"[INFO] Output directory: {output_dir.absolute()}")
    else:
        print("\n[FAILED] Processing encountered errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
