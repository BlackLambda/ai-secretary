#!/usr/bin/env python3
"""
Microsoft 365 Data Extractor - Master Script
==============================================

Complete user journey for M365 data extraction:
1. Setup: First-time setup with key collaborators review
2. Backfill: Historical data extraction (configurable days)
3. Daily: Automated daily extraction at 8AM Beijing time

Modes:
    --setup     : First-time setup (get collaborators + review)
    --backfill  : Extract historical data (today + N previous days)
    --daily     : Extract past 24 hours
    (no args)   : Auto-detect mode based on existing files

Usage:
    python m365_extractor.py --setup                    # Initial setup
    python m365_extractor.py --backfill --days 2        # Today + 2 previous days (3 days total)
    python m365_extractor.py --daily                    # Daily extraction
    python m365_extractor.py                            # Auto mode
"""

import sys
import os
import json
import logging
import argparse
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
import traceback

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.services.emails import EmailService
from src.services.teams import TeamsService
from src.services.calendars import CalendarService
from src.services.collaborators import CollaboratorsService
from src.client.substrate_client import SubstrateClient
from src.utils.json_writer import save_json


# ============================================================================
# CONFIGURATION
# ============================================================================

# Beijing Time Zone (UTC+8)
BEIJING_TZ_OFFSET = 8

# Output directory
OUTPUT_DIR = "output/daily"

# Log directory
LOG_DIR = "logs"

# Log file (date-based, set in main())
LOG_FILE = None


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def setup_logging(date_str: str = None):
    """Setup logging with date-based file handler."""
    global LOG_FILE

    # Create logs directory if needed
    os.makedirs(LOG_DIR, exist_ok=True)

    # Set log file path
    if date_str:
        LOG_FILE = os.path.join(LOG_DIR, f"{date_str}.log")
    else:
        today = datetime.now().strftime("%Y-%m-%d")
        LOG_FILE = os.path.join(LOG_DIR, f"{today}.log")

    # Clear previous handlers
    logging.root.handlers = []

    # Setup logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    logging.info("=" * 80)
    logging.info("Microsoft 365 Data Extractor Started")
    logging.info("=" * 80)
    logging.info(f"Log file: {LOG_FILE}")


def get_beijing_time_now():
    """Get current time in Beijing (UTC+8)."""
    utc_now = datetime.now(timezone.utc)
    beijing_tz = timezone(timedelta(hours=BEIJING_TZ_OFFSET))
    return utc_now.astimezone(beijing_tz)


def calculate_extraction_window():
    """
    Calculate the 24-hour window ending at 8:00 AM Beijing time today.

    Returns:
        tuple: (start_datetime, end_datetime, date_str)
    """
    beijing_now = get_beijing_time_now()

    # Set target time to 8:00 AM Beijing time today
    target_time = beijing_now.replace(hour=8, minute=0, second=0, microsecond=0)

    # If it's before 8:00 AM, use yesterday's 8:00 AM
    if beijing_now < target_time:
        target_time = target_time - timedelta(days=1)

    # Calculate 24-hour window ending at target_time
    end_time = target_time
    start_time = target_time - timedelta(hours=24)

    # Format date string for filenames - use start date (YYYY-MM-DD)
    # e.g., extraction from Nov 09 08:00 to Nov 10 08:00 Beijing time = 2025-11-09
    date_str = start_time.strftime("%Y-%m-%d")

    # Convert Beijing time to UTC for API filtering
    start_time_utc = start_time.astimezone(timezone.utc)
    end_time_utc = end_time.astimezone(timezone.utc)

    # Format for API calls (ISO 8601) - use UTC times
    start_iso = start_time_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_iso = end_time_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    logging.info(f"Extraction window: {start_iso} to {end_iso} ({date_str})")
    logging.info(f"Beijing time: {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')} (UTC+8)")
    logging.info(f"UTC time:      {start_time_utc.strftime('%Y-%m-%d %H:%M')} to {end_time_utc.strftime('%Y-%m-%d %H:%M')} (UTC)")

    return start_iso, end_iso, date_str


def ensure_output_directory():
    """Create output directory if needed."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logging.info(f"Output directory: {OUTPUT_DIR}")


# ============================================================================
# EXTRACTION FUNCTIONS (from daily_extract.py)
# ============================================================================

def filter_emails(emails: List[Dict], my_email: str, collaborator_email: str = None) -> tuple[List[Dict], List[Dict]]:
    """Filter emails based on sophisticated rules."""
    filtered = []
    filtered_out = []

    for email in emails:
        # Rule 1: Remove emails without subjects
        subject = email.get('Subject') or email.get('subject', '')
        if not subject or subject.strip() == '':
            filtered_out.append({
                "email_id": email.get('Id', email.get('id', 'unknown')),
                "reason": "No subject",
                "date": email.get('ReceivedDateTime', email.get('receivedDateTime', '')),
                "sender": _get_sender_email(email),
                "subject": "(empty)",
                "to_count": len(email.get('ToRecipients') or email.get('toRecipients', []))
            })
            continue

        # Get sender email
        sender_email = _get_sender_email(email)

        # If filtering for specific collaborator
        if collaborator_email:
            # Rule 2: Keep all if sender is me or collaborator
            if sender_email.lower() == my_email.lower() or sender_email.lower() == collaborator_email.lower():
                filtered.append(email)
                continue

            # Rule 3: For other senders, check TO recipient count
            to_recipients = email.get('ToRecipients') or email.get('toRecipients', [])
            recipient_count = len(to_recipients)

            if recipient_count <= 30:
                filtered.append(email)
            else:
                filtered_out.append({
                    "email_id": email.get('Id', email.get('id', 'unknown')),
                    "reason": f"Too many recipients: {recipient_count} > 30",
                    "date": email.get('ReceivedDateTime', email.get('receivedDateTime', '')),
                    "sender": sender_email,
                    "subject": subject,
                    "to_count": recipient_count
                })
        else:
            # For general filtering (no specific collaborator), just remove empty subjects
            filtered.append(email)

    return filtered, filtered_out


def _get_sender_email(email: Dict) -> str:
    """Extract sender email from email object."""
    from_field = email.get('From') or email.get('from', {})
    if from_field and 'EmailAddress' in from_field:
        return from_field['EmailAddress'].get('Address', '')
    elif from_field and 'emailAddress' in from_field:
        return from_field['emailAddress'].get('address', '')
    return ''


def get_top_collaborators(top_n: int = 20) -> List[Dict]:
    """
    Extract top N collaborators using the CollaboratorsService.

    Args:
        top_n: Number of top collaborators to extract

    Returns:
        List of collaborator dictionaries
    """
    logging.info(f"Extracting top {top_n} collaborators...")

    try:
        collab_service = CollaboratorsService()
        result = collab_service.get_top_collaborators(top_n=top_n)

        if not result or 'collaborators' not in result:
            logging.warning("No collaborators found in response")
            return []

        collaborators = result.get('collaborators', [])

        logging.info(f"Successfully extracted {len(collaborators)} collaborators")

        # Save collaborators to file
        save_json({'count': len(collaborators), 'collaborators': collaborators}, "collaborators.json", OUTPUT_DIR)

        return collaborators

    except Exception as e:
        logging.error(f"Failed to extract collaborators: {str(e)}")
        logging.error(traceback.format_exc())
        return []


def extract_email_exchanges(collaborators: List[Dict], start_date: str, end_date: str, date_str: str):
    """Extract email exchanges with top collaborators."""
    logging.info("Extracting email exchanges...")

    try:
        # Get my email from the client
        client = SubstrateClient()
        my_email = client.upn

        logging.info(f"My email: {my_email}")

        # Get all emails in the date range
        email_service = EmailService()
        all_emails_result = email_service.get_emails(
            top=1000,
            filter_query=f"ReceivedDateTime ge {start_date} and ReceivedDateTime le {end_date}",
            orderby="ReceivedDateTime desc"
        )

        all_emails = all_emails_result.get('emails', [])
        logging.info(f"Retrieved {len(all_emails)} total emails")

        # Apply sophisticated email filtering (Rule 1: remove emails without subjects)
        all_emails_filtered, filtered_out_subjects = filter_emails(all_emails, my_email)
        logging.info(f"Filtered out {len(filtered_out_subjects)} emails without subjects")

        # Filter for exchanges with collaborators
        collaborator_emails = []
        collaborator_set = {c['email'] for c in collaborators}

        for email in all_emails_filtered:
            # Check if email involves any collaborator
            sender_email = None
            if 'From' in email and 'EmailAddress' in email['From']:
                sender_email = email['From']['EmailAddress'].get('Address', '').lower()

            recipient_emails = []
            if 'ToRecipients' in email:
                for rec in email['ToRecipients']:
                    if 'EmailAddress' in rec:
                        recipient_emails.append(rec['EmailAddress'].get('Address', '').lower())

            # Check if sender or any recipient is a collaborator
            if sender_email in collaborator_set or any(r in collaborator_set for r in recipient_emails):
                collaborator_emails.append(email)

        logging.info(f"Found {len(collaborator_emails)} emails with collaborators")

        # Categorize emails
        direct_emails = []  # Between me and collaborator only
        group_emails = []   # Involving me, collaborator, and others

        for email in collaborator_emails:
            sender_email = None
            if 'From' in email and 'EmailAddress' in email['From']:
                sender_email = email['From']['EmailAddress'].get('Address', '').lower()

            recipient_emails = []
            if 'ToRecipients' in email:
                for rec in email['ToRecipients']:
                    if 'EmailAddress' in rec:
                        recipient_emails.append(rec['EmailAddress'].get('Address', '').lower())

            # Identify which top collaborators are involved
            involved_collaborators = []
            if sender_email and sender_email in collaborator_set:
                involved_collaborators.append(sender_email)
            for r in recipient_emails:
                if r in collaborator_set and r not in involved_collaborators:
                    involved_collaborators.append(r)

            # Count participants (sender + recipients, excluding me)
            participants = set()
            if sender_email:
                participants.add(sender_email)
            for r in recipient_emails:
                if r != my_email.lower():
                    participants.add(r)

            # Check if it's direct (only me + 1 collaborator) or group
            if len(participants) == 1:
                # Add collaborator info to email
                email_copy = email.copy()
                email_copy['with_collaborators'] = involved_collaborators
                direct_emails.append(email_copy)
            else:
                # For group emails, apply Rule 2 & 3: Check sender and TO recipient count
                # Rule 2: Keep all if sender is me or any collaborator (always preserve direct comm)
                is_from_me_or_collaborator = (sender_email == my_email.lower() or
                                               any(sender_email == collab.lower() for collab in involved_collaborators))

                if is_from_me_or_collaborator:
                    # Rule 2: Keep all direct communications
                    email_copy = email.copy()
                    email_copy['with_collaborators'] = involved_collaborators
                    group_emails.append(email_copy)
                else:
                    # Rule 3: For other senders, check TO recipient count
                    to_recipients = email.get('ToRecipients') or email.get('toRecipients', [])
                    recipient_count = len(to_recipients)

                    if recipient_count <= 30:
                        # Keep group emails with ≤30 recipients
                        email_copy = email.copy()
                        email_copy['with_collaborators'] = involved_collaborators
                        group_emails.append(email_copy)
                    else:
                        # Reject group emails with >30 recipients (mass distribution)
                        logging.info(f"Filtered out group email with {recipient_count} TO recipients: {email.get('Subject', '')[:50]}")
                        continue

        result = {
            'date': date_str,
            'extraction_window': {'start': start_date, 'end': end_date},
            'total_emails': len(all_emails),
            'collaborator_emails': len(collaborator_emails),
            'direct_emails': len(direct_emails),
            'group_emails': len(group_emails),
            'direct_emails_list': direct_emails,
            'group_emails_list': group_emails,
            'collaborators_count': len(collaborator_set)
        }

        # Save to file
        filename = f"{date_str}_email_exchanges.json"
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        logging.info(f"Saved {len(direct_emails)} direct and {len(group_emails)} group emails to {filename}")

        return result

    except Exception as e:
        logging.error(f"Failed to extract email exchanges: {str(e)}")
        logging.error(traceback.format_exc())
        return None


def get_teams_threads_metadata(client: SubstrateClient, max_threads: int = 1000) -> Dict[str, Dict]:
    """Fetch Teams threads metadata including group names with persistent mapping."""
    # Check for existing mapping file
    mapping_file = os.path.join(OUTPUT_DIR, "teams_thread_mapping.json")
    existing_mapping = {}

    if os.path.exists(mapping_file):
        logging.info(f"Loading existing Teams thread mapping from {mapping_file}")
        try:
            with open(mapping_file, 'r', encoding='utf-8') as f:
                existing_mapping = json.load(f)
            logging.info(f"Loaded {len(existing_mapping)} thread mappings from cache")
        except Exception as e:
            logging.warning(f"Failed to load existing mapping: {e}, starting fresh")
            existing_mapping = {}

    # Fetch new metadata from API
    logging.info("Fetching fresh Teams threads metadata from API...")
    url = "https://substrate.office.com/entityserve/api/search"
    headers = {
        "content-type": "application/json",
        "X-AnchorMailbox": f"UPN:{client.upn}",
        "Accept": "application/json",
        "X-ScenarioTag": "ES_Explorer"
    }
    payload = {
        "Query": {
            "QueryType": "None",
            "MaxResults": 0
        },
        "EntityRequests": [
            {
            "EntityType": "Teams",
            "ModifiedQuery": {
                "QueryType": "None",
                "MaxResults": max_threads
            },
            "Grammar": {},
            "GraphSelect": [
                {
                "GraphName": "*",
                "PredicateName": "*",
                "MaxEntities": 1
                }
            ],
            "RankerConfigSettings": {},
            "AllowQueryCompletions": False,
            "ConfigurationOptions": { "SlowInfixMatchType": "Infix" }
            }
        ],
        "MailboxInformation": {
            "MailboxType": "Unknown",
            "UserType": "Unknown"
        }
    }
    response = client.post(url, json=payload, extra_headers=headers)

    # Merge new metadata with existing mapping
    new_mapping = {}
    for entity in response["EntityResults"][0]["Entities"]:
        thread_id = entity["ThreadId"]
        metadata = {
            "name": entity.get("Name"),
            "type": entity.get("Type"),
            "thread_type": entity.get("ThreadType"),
            "last_message_time": entity.get("LastMessageTime"),
            "my_last_message_time": entity.get("MyLastMessageTime"),
        }
        new_mapping[thread_id] = metadata

    # Update the mapping (new values override old ones)
    updated_mapping = {**existing_mapping, **new_mapping}

    # Save updated mapping
    try:
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(updated_mapping, f, indent=2, ensure_ascii=False)
        logging.info(f"Saved {len(updated_mapping)} thread mappings to {mapping_file}")
        logging.info(f"  - From cache: {len(existing_mapping)}")
        logging.info(f"  - New from API: {len(new_mapping)}")
        logging.info(f"  - Total in file: {len(updated_mapping)}")
    except Exception as e:
        logging.error(f"Failed to save mapping file: {e}")

    return updated_mapping


def extract_teams_messages(collaborators: List[Dict], start_date: str, end_date: str, date_str: str):
    """Extract Teams messages with top collaborators."""
    logging.info("Extracting Teams messages...")

    try:
        # Get my email from the client
        client = SubstrateClient()
        my_email = client.upn

        threads_metadata = get_teams_threads_metadata(client, max_threads=1000)

        # Get Teams messages in the date range
        teams_service = TeamsService()
        all_messages_result = teams_service.get_teams_messages(
            top=1000,
            filter_query=f"ReceivedDateTime ge {start_date} and ReceivedDateTime le {end_date}",
            orderby="ReceivedDateTime desc"
        )

        all_messages = all_messages_result.get('messages', [])
        logging.info(f"Retrieved {len(all_messages)} total Teams messages")

        # Filter for messages with collaborators
        collaborator_messages = []
        collaborator_set = {c['email'] for c in collaborators}

        for message in all_messages:
            # Check if message involves any collaborator
            sender_email = None
            if 'From' in message and 'EmailAddress' in message['From']:
                sender_email = message['From']['EmailAddress'].get('Address', '').lower()

            recipient_emails = []
            if 'ToRecipients' in message:
                for rec in message['ToRecipients']:
                    if 'EmailAddress' in rec:
                        recipient_emails.append(rec['EmailAddress'].get('Address', '').lower())

            # Check if sender or any recipient is a collaborator
            if sender_email in collaborator_set or any(r in collaborator_set for r in recipient_emails):
                collaborator_messages.append(message)

        logging.info(f"Found {len(collaborator_messages)} Teams messages with collaborators")

        # Categorize messages by thread
        threads = {}
        for message in collaborator_messages:
            thread_id = message.get('ClientThreadId', '')
            if thread_id not in threads:
                threads[thread_id] = []
            threads[thread_id].append(message)

        logging.info(f"Found {len(threads)} unique threads")

        # Analyze each thread
        direct_threads = []   # 1 external participant
        group_threads = []    # 2+ external participants

        for thread_id, messages in threads.items():
            # Count external participants (not me)
            external_participants = set()

            for msg in messages:
                # Get sender
                sender_email = None
                if 'From' in msg and 'EmailAddress' in msg['From']:
                    sender_email = msg['From']['EmailAddress'].get('Address', '').lower()

                if sender_email and sender_email != my_email.lower():
                    external_participants.add(sender_email)

                # Get recipients
                if 'ToRecipients' in msg:
                    for rec in msg['ToRecipients']:
                        if 'EmailAddress' in rec:
                            rec_email = rec['EmailAddress'].get('Address', '').lower()
                            if rec_email != my_email.lower():
                                external_participants.add(rec_email)

            # Identify which top collaborators are involved
            involved_collaborators = []
            for p in external_participants:
                if p in collaborator_set and p not in involved_collaborators:
                    involved_collaborators.append(p)

            # Log thread details for groups
            if len(external_participants) > 1:
                logging.info(f"Thread {thread_id[:30]}: {len(external_participants)} external participants")
                logging.info(f"  Participants: {', '.join(list(external_participants)[:3])}... (+{len(external_participants)-3} more)")
                # Log which top collaborators are involved
                if involved_collaborators:
                    logging.info(f"  Top collaborators: {', '.join(involved_collaborators)}")

            # Check if it's direct (only me + 1 collaborator) or group
            # IMPORTANT: For Teams, a "direct" thread should have exactly 1 external participant
            thread_metadata = threads_metadata.get(thread_id, {})
            group_name = thread_metadata.get('name', '')

            if len(external_participants) == 1:
                # Only 1 external participant = direct conversation
                direct_threads.append({
                    'thread_id': thread_id,
                    'group_name': group_name,
                    'message_count': len(messages),
                    'external_participant': list(external_participants)[0],
                    'with_collaborators': involved_collaborators,  # List of which top collaborators
                    'messages': messages
                })
            else:
                # Multiple external participants = group conversation
                group_threads.append({
                    'thread_id': thread_id,
                    'group_name': group_name,
                    'message_count': len(messages),
                    'external_participants': list(external_participants),
                    'participants_count': len(external_participants),
                    'with_collaborators': involved_collaborators,  # List of which top collaborators
                    'messages': messages
                })

        result = {
            'date': date_str,
            'extraction_window': {'start': start_date, 'end': end_date},
            'total_messages': len(all_messages),
            'collaborator_messages': len(collaborator_messages),
            'direct_threads': len(direct_threads),
            'group_threads': len(group_threads),
            'direct_threads_list': direct_threads,
            'group_threads_list': group_threads,
            'collaborators_count': len(collaborator_set)
        }

        # Save to file
        filename = f"{date_str}_teams_messages.json"
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        logging.info(f"Saved {len(direct_threads)} direct and {len(group_threads)} group threads to {filename}")

        return result

    except Exception as e:
        logging.error(f"Failed to extract Teams messages: {str(e)}")
        logging.error(traceback.format_exc())
        return None


def extract_calendar_events(start_date: str, end_date: str, date_str: str):
    """Extract calendar events."""
    logging.info("Extracting calendar events...")

    try:
        calendar_service = CalendarService()
        events_result = calendar_service.get_events(
            start_date=start_date,
            end_date=end_date,
            top=500
        )

        events = events_result.get('events', [])
        if not events:
            logging.info("No calendar events found")
            result = {
                'date': date_str,
                'extraction_window': {'start': start_date, 'end': end_date},
                'total_events': 0,
                'events_list': []
            }
        else:
            logging.info(f"Retrieved {len(events)} calendar events")
            result = {
                'date': date_str,
                'extraction_window': {'start': start_date, 'end': end_date},
                'total_events': len(events),
                'events_list': events
            }

        # Save to file
        filename = f"{date_str}_calendar_events.json"
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        logging.info(f"Saved {len(events) if events else 0} events to {filename}")

        return result

    except Exception as e:
        logging.error(f"Failed to extract calendar events: {str(e)}")
        logging.error(traceback.format_exc())
        return None


def extract_meeting_transcripts(start_date: str, end_date: str, date_str: str):
    """Extract meeting transcripts (text content from meeting recordings)."""
    logging.info("Extracting meeting transcripts...")

    try:
        calendar_service = CalendarService()
        transcripts = calendar_service.get_meeting_most_recent_transcripts(
            start_datetime=start_date,
            end_datetime=end_date,
            top=500
        )

        if not transcripts:
            logging.info("No meeting transcripts found")
            result = {
                'date': date_str,
                'extraction_window': {'start': start_date, 'end': end_date},
                'total_transcripts': 0,
                'transcripts_list': []
            }
        else:
            logging.info(f"Retrieved {len(transcripts)} meeting transcripts")

            # Process transcripts to extract key info
            processed_transcripts = []
            filtered_out_empty = 0
            for transcript in transcripts:
                processed = {
                    'file_name': transcript.get('FileName', ''),
                    'file_extension': transcript.get('FileExtension', ''),
                    'created_time': transcript.get('ItemProperties', {}).get('Default', {}).get('Created', ''),
                    'teams_thread_id': transcript.get('ItemProperties', {}).get('Default', {}).get('MeetingRecording', {}).get('MeetingRecordingTeamsThreadId', ''),
                    'call_id': transcript.get('ItemProperties', {}).get('Default', {}).get('MeetingRecording', {}).get('MeetingRecordingCallId', ''),
                    'sharepoint_item': transcript.get('SharePointItem', {}),
                    'text_content': transcript.get('FileContent', {}).get('Text', '')  # This is the actual transcript text!
                }
                # Filter out transcripts with empty text_content
                if processed['text_content'] and processed['text_content'].strip():
                    processed_transcripts.append(processed)
                else:
                    filtered_out_empty += 1

            if filtered_out_empty > 0:
                logging.info(f"Filtered out {filtered_out_empty} transcripts with empty text_content")

            result = {
                'date': date_str,
                'extraction_window': {'start': start_date, 'end': end_date},
                'total_transcripts': len(transcripts),
                'transcripts_with_content': len(processed_transcripts),
                'filtered_out_empty': filtered_out_empty,
                'transcripts_list': processed_transcripts
            }

        # Save to file
        filename = f"{date_str}_meeting_transcripts.json"
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        logging.info(f"Saved {result['transcripts_with_content'] if transcripts else 0} transcripts with content to {filename}")

        return result

    except Exception as e:
        logging.error(f"Failed to extract meeting transcripts: {str(e)}")
        logging.error(traceback.format_exc())
        return None


# ============================================================================
# MODE FUNCTIONS
# ============================================================================

def run_setup_mode():
    """Setup mode: First-time setup with collaborators review."""
    logging.info("=" * 80)
    logging.info("SETUP MODE - First-time configuration")
    logging.info("=" * 80)

    # Step 1: Extract top collaborators
    logging.info("\nStep 1: Extracting top 20 collaborators...")
    collaborators = get_top_collaborators(top_n=20)

    if not collaborators:
        logging.error("Failed to extract collaborators. Setup cannot continue.")
        return False

    # Step 2: Save collaborators
    collab_file = os.path.join(OUTPUT_DIR, "collaborators.json")
    logging.info(f"\nStep 2: Saving collaborators to {collab_file}")

    # Step 3: Ask user to review
    print("\n" + "=" * 80)
    print("SETUP - MANUAL REVIEW REQUIRED")
    print("=" * 80)
    print(f"\nCollaborators file: {collab_file}")
    print(f"\nPlease review and edit this file:")
    print(f"  - You can remove people you don't want to track")
    print(f"  - You can add additional collaborators (with 'alias' and 'email' fields)")
    print(f"  - Keep the file as valid JSON format")
    print(f"\nThe file has been opened for your review (if possible) or you can open it manually.")
    print("\nPress ENTER after you have finished reviewing and editing the file...")

    try:
        input()
    except KeyboardInterrupt:
        print("\n\nSetup cancelled by user.")
        return False

    # Verify file still exists and is valid
    if not os.path.exists(collab_file):
        logging.error(f"Collaborators file not found: {collab_file}")
        return False

    try:
        with open(collab_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            verified_collaborators = data.get('collaborators', [])
        logging.info(f"Verified: File contains {len(verified_collaborators)} collaborators")
    except Exception as e:
        logging.error(f"Failed to verify collaborators file: {e}")
        return False

    # Step 4: Success
    logging.info("\n" + "=" * 80)
    logging.info("SETUP COMPLETE")
    logging.info("=" * 80)
    logging.info(f"Collaborators verified: {len(verified_collaborators)}")
    logging.info(f"You can now run: python {os.path.basename(__file__)} --backfill")
    logging.info(f"Or schedule daily runs: python {os.path.basename(__file__)} --daily")
    logging.info("=" * 80)

    return True


def run_backfill_mode(days: int = 14, max_retries: int = 3, download_videos: bool = False):
    """
    Backfill mode: Extract historical data for last N days + today.
    
    Args:
        days: Number of previous days (today + days = total days extracted)
              e.g., days=2 extracts 3 days total (today + 2 previous)
    """
    logging.info("=" * 80)
    logging.info(f"BACKFILL MODE - Extracting {days + 1} days of data (today + {days} previous)")
    logging.info("=" * 80)

    # Check if collaborators file exists
    collab_file = os.path.join(OUTPUT_DIR, "collaborators.json")
    if not os.path.exists(collab_file):
        logging.error(f"Collaborators file not found: {collab_file}")
        logging.error("Please run setup mode first: python m365_extractor.py --setup")
        return False

    # Load collaborators
    with open(collab_file, 'r', encoding='utf-8') as f:
        collab_data = json.load(f)
        collaborators = collab_data.get('collaborators', [])

    if not collaborators:
        logging.error("No collaborators found in file. Please run setup mode again.")
        return False

    logging.info(f"Loaded {len(collaborators)} collaborators from cache")

    # Calculate date range - start from 00:00 AM Beijing time
    beijing_now = get_beijing_time_now()
    today_start = beijing_now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Start from N days ago at 00:00 AM
    current_date = today_start - timedelta(days=days)

    successful_days = 0
    failed_days = 0
    total_days = days + 1  # today + N previous days

    for day_offset in range(total_days):
        # Calculate extraction window for this day (00:00 to 23:59:59 or current time for today)
        start_time = current_date
        
        # For today, end at current time; for past days, end at 23:59:59
        if day_offset == total_days - 1:  # Last iteration = today
            end_time = beijing_now
        else:
            end_time = current_date.replace(hour=23, minute=59, second=59, microsecond=999999)

        # Format date string
        date_str = current_date.strftime("%Y-%m-%d")

        # Convert to UTC for API
        start_iso = start_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_iso = end_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

        logging.info("\n" + "=" * 80)
        logging.info(f"Day {day_offset + 1}/{total_days}: {date_str}")
        logging.info(f"Beijing: {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"UTC:     {start_iso} to {end_iso}")
        logging.info("=" * 80)

        # Retry logic
        success = False
        for retry in range(max_retries):
            try:
                # Extract all data types
                email_result = extract_email_exchanges(collaborators, start_iso, end_iso, date_str)
                teams_result = extract_teams_messages(collaborators, start_iso, end_iso, date_str)
                calendar_result = extract_calendar_events(start_iso, end_iso, date_str)
                transcript_result = extract_meeting_transcripts(start_iso, end_iso, date_str)

                if email_result and teams_result and calendar_result and transcript_result:
                    logging.info(f"✓ Successfully extracted data for {date_str}")
                    successful_days += 1
                    success = True
                    break
                else:
                    logging.warning(f"⚠ Partial success for {date_str}, retrying...")
            except Exception as e:
                logging.error(f"✗ Failed attempt {retry + 1}/{max_retries} for {date_str}: {str(e)}")

        if not success:
            logging.error(f"✗ All retries failed for {date_str}")
            failed_days += 1

        # Move to next day
        current_date += timedelta(days=1)

    # Summary
    logging.info("\n" + "=" * 80)
    logging.info("BACKFILL COMPLETE")
    logging.info("=" * 80)
    logging.info(f"Total days: {total_days}")
    logging.info(f"Successful: {successful_days}")
    logging.info(f"Failed: {failed_days}")
    logging.info("=" * 80)

    return failed_days == 0


def run_daily_mode():
    """Daily mode: Extract past 24 hours of data."""
    logging.info("=" * 80)
    logging.info("DAILY MODE - Extracting past 24 hours")
    logging.info("=" * 80)

    # Check if collaborators file exists
    collab_file = os.path.join(OUTPUT_DIR, "collaborators.json")
    if not os.path.exists(collab_file):
        logging.error(f"Collaborators file not found: {collab_file}")
        logging.error("Please run setup mode first: python m365_extractor.py --setup")
        return False

    # Load collaborators
    with open(collab_file, 'r', encoding='utf-8') as f:
        collab_data = json.load(f)
        collaborators = collab_data.get('collaborators', [])

    if not collaborators:
        logging.error("No collaborators found in file. Please run setup mode again.")
        return False

    logging.info(f"Loaded {len(collaborators)} collaborators from cache")

    # Calculate extraction window
    start_date, end_date, date_str = calculate_extraction_window()

    # Ensure output directory exists
    ensure_output_directory()

    # Check if file already exists
    expected_file = f"{date_str}_email_exchanges.json"
    expected_path = os.path.join(OUTPUT_DIR, expected_file)
    if os.path.exists(expected_path):
        logging.info(f"Data for {date_str} already exists, skipping...")
        logging.info("To re-run, delete the existing files first.")
        return True

    # Extract all data types
    logging.info("\n" + "=" * 80)
    logging.info("Starting data extraction...")
    logging.info("=" * 80)

    email_result = extract_email_exchanges(collaborators, start_date, end_date, date_str)
    teams_result = extract_teams_messages(collaborators, start_date, end_date, date_str)
    calendar_result = extract_calendar_events(start_date, end_date, date_str)
    transcript_result = extract_meeting_transcripts(start_date, end_date, date_str)

    # Summary
    logging.info("\n" + "=" * 80)
    logging.info("DAILY EXTRACTION COMPLETE")
    logging.info("=" * 80)
    logging.info(f"Date: {date_str}")
    logging.info(f"Email exchanges: {email_result['collaborator_emails'] if email_result else 0}")
    logging.info(f"Teams messages: {teams_result['collaborator_messages'] if teams_result else 0}")
    logging.info(f"Calendar events: {calendar_result['total_events'] if calendar_result else 0}")
    logging.info(f"Meeting transcripts: {transcript_result.get('transcripts_with_content', 0) if transcript_result else 0}")
    logging.info(f"Output directory: {OUTPUT_DIR}")
    logging.info("=" * 80)

    return True


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Microsoft 365 Data Extractor - Complete user journey",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modes:
  --setup     First-time setup (get collaborators + manual review)
  --backfill  Extract historical data (today + N previous days)
  --daily     Extract past 24 hours of data
  (no args)   Auto-detect mode based on existing files

Examples:
  python m365_extractor.py --setup                    # Initial setup
  python m365_extractor.py --backfill --days 2        # Today + 2 previous days (3 days total)
  python m365_extractor.py --backfill --days 14       # Today + 14 previous days (15 days total)
  python m365_extractor.py --daily                    # Daily extraction
  python m365_extractor.py --daily --downloadVideos   # Daily extraction with video download
  python m365_extractor.py                            # Auto mode
        """
    )

    parser.add_argument('--setup', action='store_true',
                        help='Setup mode: First-time configuration')
    parser.add_argument('--backfill', action='store_true',
                        help='Backfill mode: Extract historical data')
    parser.add_argument('--daily', action='store_true',
                        help='Daily mode: Extract past 24 hours')
    parser.add_argument('--days', type=int, default=2,
                        help='Number of previous days for backfill (default: 2, total days = days + today)')
    parser.add_argument('--retries', type=int, default=3,
                        help='Max retries for backfill (default: 3)')
    parser.add_argument('--downloadVideos', action='store_true', default=False,
                        help='Enable video downloading from meeting transcripts (default: False)')

    args = parser.parse_args()

    # Determine mode
    if args.setup:
        mode = 'setup'
    elif args.backfill:
        mode = 'backfill'
    elif args.daily:
        mode = 'daily'
    else:
        # Auto-detect mode
        collab_file = os.path.join(OUTPUT_DIR, "collaborators.json")
        if not os.path.exists(collab_file):
            mode = 'setup'
        else:
            mode = 'daily'

    # Setup logging with today's date
    today_str = datetime.now().strftime("%Y-%m-%d")
    setup_logging(today_str)

    logging.info(f"Mode: {mode.upper()}")

    # Run the appropriate mode
    try:
        if mode == 'setup':
            success = run_setup_mode()
        elif mode == 'backfill':
            success = run_backfill_mode(days=args.days, max_retries=args.retries)
        elif mode == 'daily':
            success = run_daily_mode()
        else:
            logging.error(f"Unknown mode: {mode}")
            success = False

        # Exit with appropriate code
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logging.info("\nOperation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
