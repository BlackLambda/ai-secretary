"""
Delete Teams messages from output files based on message content.

This tool helps remove sensitive messages from the exported Teams data.
It uses smart matching to handle emojis, HTML, and whitespace variations.

Usage:
    python delete_teams_messages.py "message content to delete"
"""

import sys
import os
import json
import re
from typing import Dict, List, Tuple
from html.parser import HTMLParser

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class HTMLStripper(HTMLParser):
    """Strip HTML tags from text."""
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = []

    def handle_data(self, d):
        self.text.append(d)

    def get_data(self):
        return ''.join(self.text)


def strip_html(html_text: str) -> str:
    """Remove HTML tags and get plain text."""
    if not html_text:
        return ""

    stripper = HTMLStripper()
    try:
        stripper.feed(html_text)
        return stripper.get_data()
    except Exception:
        # If HTML parsing fails, try simple regex
        return re.sub(r'<[^>]+>', '', html_text)


def normalize_text(text: str) -> str:
    """
    Normalize text for fuzzy matching:
    - Convert to lowercase
    - Normalize whitespace (including nbsp, tabs, etc.)
    - Remove HTML entities
    """
    if not text:
        return ""

    # Strip HTML first
    text = strip_html(text)

    # Replace common HTML entities
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&quot;', '"')
    text = text.replace('&#39;', "'")
    text = text.replace('\r\n', '\n')
    text = text.replace('\r', '\n')

    # Normalize whitespace
    text = ' '.join(text.split())

    # Convert to lowercase for case-insensitive matching
    text = text.lower()

    return text.strip()


def extract_message_text(msg: Dict) -> Tuple[str, str, str]:
    """
    Extract message text in different formats for matching.

    Returns:
        (body_html, body_text, body_preview)
    """
    # Get body content (HTML version)
    body = msg.get('Body') or msg.get('body', {})
    if isinstance(body, dict):
        body_html = body.get('Content', body.get('content', ''))
    else:
        body_html = str(body) if body else ''

    # Get plain text version
    body_text = strip_html(body_html)

    # Get preview
    body_preview = msg.get('BodyPreview', msg.get('bodyPreview', ''))

    return body_html, body_text, body_preview


def matches_search(msg: Dict, search_term: str) -> Tuple[bool, str]:
    """
    Check if message matches search term using multiple strategies.

    Returns:
        (matched, match_type) where match_type is one of:
        - "exact_html": Exact match in HTML content
        - "exact_text": Exact match in plain text
        - "exact_preview": Exact match in preview
        - "fuzzy_html": Fuzzy match in HTML content
        - "fuzzy_text": Fuzzy match in plain text
        - "fuzzy_preview": Fuzzy match in preview
        - "": No match
    """
    body_html, body_text, body_preview = extract_message_text(msg)

    # Strategy 1: Exact match in HTML
    if search_term in body_html:
        return True, "exact_html"

    # Strategy 2: Exact match in plain text
    if search_term in body_text:
        return True, "exact_text"

    # Strategy 3: Exact match in preview
    if search_term in body_preview:
        return True, "exact_preview"

    # Strategy 4: Fuzzy match (normalized)
    normalized_search = normalize_text(search_term)

    if normalized_search in normalize_text(body_html):
        return True, "fuzzy_html"

    if normalized_search in normalize_text(body_text):
        return True, "fuzzy_text"

    if normalized_search in normalize_text(body_preview):
        return True, "fuzzy_preview"

    return False, ""


def find_matching_messages(data: Dict, search_term: str) -> List[Dict]:
    """
    Find all messages matching the search term.

    Returns:
        List of dicts with structure:
        {
            "collaborator": "name",
            "thread_type": "1on1_threads" or "group_threads",
            "thread_index": 0,
            "message_index": 1,
            "match_type": "exact_html",
            "message_preview": "...",
            "sender": "...",
            "date": "..."
        }
    """
    matches = []

    for collaborator, collab_data in data.items():
        # Skip summary section
        if collaborator == "summary" or not isinstance(collab_data, dict):
            continue

        # Check both 1on1 and group threads
        for thread_type in ["1on1_threads", "group_threads"]:
            threads = collab_data.get(thread_type, [])

            for thread_idx, thread in enumerate(threads):
                messages = thread.get('messages', [])

                for msg_idx, msg in enumerate(messages):
                    matched, match_type = matches_search(msg, search_term)

                    if matched:
                        # Extract message info for display
                        from_field = msg.get('From') or msg.get('from', {})
                        sender = ""
                        if from_field and 'EmailAddress' in from_field:
                            sender = from_field['EmailAddress'].get('Address', '')
                        elif from_field and 'emailAddress' in from_field:
                            sender = from_field['emailAddress'].get('address', '')

                        _, body_text, body_preview = extract_message_text(msg)
                        preview = body_preview or body_text[:150]

                        matches.append({
                            "collaborator": collaborator,
                            "thread_type": thread_type,
                            "thread_index": thread_idx,
                            "message_index": msg_idx,
                            "match_type": match_type,
                            "message_preview": preview[:150],
                            "sender": sender,
                            "date": msg.get('ReceivedDateTime', msg.get('receivedDateTime', ''))
                        })

    return matches


def delete_messages(data: Dict, matches: List[Dict]) -> Tuple[Dict, int]:
    """
    Delete messages from data based on match list.

    Returns:
        (modified_data, deleted_count)
    """
    # Group matches by collaborator > thread_type > thread_index > message_index
    # We need to delete in reverse order to avoid index shifting
    from collections import defaultdict

    deletion_map = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for match in matches:
        collaborator = match['collaborator']
        thread_type = match['thread_type']
        thread_idx = match['thread_index']
        msg_idx = match['message_index']

        deletion_map[collaborator][thread_type][thread_idx].append(msg_idx)

    deleted_count = 0

    # Process deletions
    for collaborator, thread_types in deletion_map.items():
        for thread_type, threads_dict in thread_types.items():
            for thread_idx, msg_indices in threads_dict.items():
                # Sort in reverse order to delete from end to start
                msg_indices.sort(reverse=True)

                thread = data[collaborator][thread_type][thread_idx]
                messages = thread['messages']

                for msg_idx in msg_indices:
                    if 0 <= msg_idx < len(messages):
                        del messages[msg_idx]
                        deleted_count += 1

                # Update thread message count
                thread['message_count'] = len(messages)

    # Recalculate totals for each collaborator
    for collaborator, collab_data in data.items():
        if collaborator == "summary" or not isinstance(collab_data, dict):
            continue

        total_threads = 0
        total_messages = 0

        for thread_type in ["1on1_threads", "group_threads"]:
            threads = collab_data.get(thread_type, [])
            for thread in threads:
                total_threads += 1
                total_messages += thread.get('message_count', 0)

        collab_data['thread_count'] = total_threads
        collab_data['message_count'] = total_messages

    # Recalculate summary
    if "summary" in data:
        total_threads = sum(
            d.get('thread_count', 0)
            for name, d in data.items()
            if name != "summary" and isinstance(d, dict)
        )
        total_messages = sum(
            d.get('message_count', 0)
            for name, d in data.items()
            if name != "summary" and isinstance(d, dict)
        )

        data["summary"]["total_threads"] = total_threads
        data["summary"]["total_messages"] = total_messages

    return data, deleted_count


def main():
    """Main entry point."""
    # Set UTF-8 encoding for Windows console
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

    # Check command line arguments
    if len(sys.argv) != 2:
        print("Usage: python delete_teams_messages.py \"message content to delete\"")
        sys.exit(1)

    search_term = sys.argv[1]

    # Load data from default file
    file_path = os.path.join(os.path.dirname(__file__), 'output', 'collaborator_teams_threads.json')

    if not os.path.exists(file_path):
        print(f"[ERROR] File not found: {file_path}")
        print("[ERROR] Please run get_collaborator_teams_threads.py first")
        sys.exit(1)

    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Find matches
    matches = find_matching_messages(data, search_term)

    if not matches:
        print(f"No messages found containing: '{search_term}'")
        return

    # Display matches for confirmation
    print(f"\nFound {len(matches)} message(s) containing: '{search_term}'\n")

    for i, match in enumerate(matches, 1):
        print(f"[{i}] Date: {match['date'][:10]}  |  From: {match['sender']}")
        print(f"    Preview: {match['message_preview'][:80]}...")
        print()

    # Confirm deletion
    response = input(f"Delete these {len(matches)} message(s)? [y/N]: ").strip().lower()

    if response != 'y':
        print("Cancelled. No messages were deleted.")
        return

    # Perform deletion
    modified_data, deleted_count = delete_messages(data, matches)

    # Save result
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(modified_data, f, indent=4, ensure_ascii=False)

    print(f"\nDeleted {deleted_count} message(s) from {file_path}")


if __name__ == "__main__":
    main()
