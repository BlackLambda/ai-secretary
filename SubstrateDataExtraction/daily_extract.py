#!/usr/bin/env python3
"""
Daily M365 Data Extractor
==========================

Automatically extracts email exchanges, Teams messages, calendar events,
and meeting transcripts daily at 8:00 AM Beijing time for the past 24 hours.

Data Types Extracted:
- Email exchanges with top collaborators
- Teams messages with top collaborators
- Calendar events and meetings
- Meeting transcripts (text content from meeting recordings)

Output:
- Separate JSON files in output/daily/ directory
- Date-prefixed filenames

Usage:
    python daily_extract.py

Automation:
    Windows Task Scheduler (see setup instructions at end of file)
"""

import sys
import os
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
import traceback
from pathlib import Path

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

# Get username dynamically
USERNAME = os.environ.get('USERNAME', os.environ.get('USER', 'zhuoyingwang'))

# Output directory - OneDrive path
ONEDRIVE_OUTPUT_DIR = f"C:\\Users\\{USERNAME}\\OneDrive - Microsoft\\Documents\\OutputSubstrateDataExtraction\\daily"

# Local backup directory
LOCAL_OUTPUT_DIR = "output/daily"

# Use OneDrive as primary output
OUTPUT_DIR = ONEDRIVE_OUTPUT_DIR

# Log file
LOG_FILE = "daily_extract.log"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

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
    # Beijing (UTC+8) to UTC means subtract 8 hours
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
    """Ensure the output directory exists."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logging.info(f"Created output directory: {OUTPUT_DIR}")
    
    # Also ensure local backup directory exists
    if not os.path.exists(LOCAL_OUTPUT_DIR):
        os.makedirs(LOCAL_OUTPUT_DIR)
        logging.info(f"Created local backup directory: {LOCAL_OUTPUT_DIR}")


def sync_to_onedrive():
    """
    Sync local output to OneDrive using robocopy.
    This ensures all data is backed up to OneDrive after extraction.
    """
    try:
        logging.info("Syncing output to OneDrive...")
        
        # Get source and destination paths
        source_dir = os.path.abspath("output")
        dest_dir = f"C:\\Users\\{USERNAME}\\OneDrive - Microsoft\\Documents\\OutputSubstrateDataExtraction"
        
        # Build robocopy command
        # /E = copy subdirectories including empty ones
        # /XO = exclude older files (only copy newer files)
        # /NFL = no file list (less verbose)
        # /NDL = no directory list (less verbose)
        # /NP = no progress indicator (for cleaner logs)
        cmd = [
            'robocopy',
            source_dir,
            dest_dir,
            '/E',      # Copy all subdirectories
            '/XO',     # Exclude older files
            '/NFL',    # No file list
            '/NDL',    # No directory list  
            '/NP'      # No progress
        ]
        
        # Run robocopy
        import subprocess
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Robocopy exit codes:
        # 0 = No files copied
        # 1 = Files copied successfully
        # 2+ = Some errors occurred
        if result.returncode <= 1:
            logging.info("Successfully synced to OneDrive")
        else:
            logging.warning(f"Robocopy finished with code {result.returncode}")
            if result.stdout:
                logging.info(f"Output: {result.stdout}")
        
    except Exception as e:
        logging.error(f"Failed to sync to OneDrive: {str(e)}")
        # Don't fail the entire extraction if sync fails


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )


# ============================================================================
# DATA EXTRACTION FUNCTIONS
# ============================================================================

def extract_top_collaborators(top_n=20):
    """
    Extract top collaborators (run once or periodically).

    Args:
        top_n: Number of top collaborators to extract

    Returns:
        List of collaborator dictionaries
    """
    logging.info(f"Extracting top {top_n} collaborators...")

    try:
        service = CollaboratorsService()
        result = service.get_top_collaborators(top_n=top_n)
        collaborators = result.get('collaborators', [])

        logging.info(f"Successfully extracted {len(collaborators)} collaborators")

        # Save collaborators to file
        save_json({'count': len(collaborators), 'collaborators': collaborators}, "collaborators.json", OUTPUT_DIR)

        return collaborators

    except Exception as e:
        logging.error(f"Failed to extract collaborators: {str(e)}")
        logging.error(traceback.format_exc())
        return []


def filter_emails(emails: List[Dict], my_email: str, collaborator_email: str = None) -> tuple[List[Dict], List[Dict]]:
    """
    Filter emails based on sophisticated rules.

    Rules:
    1. Remove emails without subjects
    2. Keep all if sender is me or collaborator
    3. For other senders: keep if ≤30 TO recipients, reject if >30

    Args:
        emails: List of email dictionaries
        my_email: Current user's email
        collaborator_email: Optional - filter for specific collaborator

    Returns:
        tuple: (filtered_emails, filtered_out_info)
    """
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


def extract_email_exchanges(collaborators: List[Dict], start_date: str, end_date: str, date_str: str):
    """
    Extract email exchanges with top collaborators.

    Args:
        collaborators: List of top collaborators
        start_date: Start datetime in ISO format
        end_date: End datetime in ISO format
        date_str: Date string for filename

    Returns:
        Dict containing extracted emails
    """
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
    """
    Fetch Teams threads metadata including group names.
    Maintains a persistent mapping file of thread_id to group_name.

    Args:
        client: SubstrateClient instance
        max_threads: Maximum number of threads to fetch metadata for

    Returns:
        Dict mapping thread_id to metadata (including group name)
    """
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
    """
    Extract Teams messages with top collaborators.

    Args:
        collaborators: List of top collaborators
        start_date: Start datetime in ISO format
        end_date: End datetime in ISO format
        date_str: Date string for filename

    Returns:
        Dict containing extracted Teams messages
    """
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

        # Categorize threads
        direct_threads = []  # Between me and collaborator only
        group_threads = []   # Involving me, collaborator, and others

        for thread_id, messages in threads.items():
            # Analyze participants in the thread (sender + ALL recipients)
            participants = set()
            for msg in messages:
                # Add sender
                sender_email = None
                if 'From' in msg and 'EmailAddress' in msg['From']:
                    sender_email = msg['From']['EmailAddress'].get('Address', '').lower()
                if sender_email:
                    participants.add(sender_email)

                # Add ToRecipients
                if 'ToRecipients' in msg:
                    for rec in msg['ToRecipients']:
                        if 'EmailAddress' in rec:
                            participants.add(rec['EmailAddress'].get('Address', '').lower())

                # Add CcRecipients
                if 'CcRecipients' in msg:
                    for rec in msg['CcRecipients']:
                        if 'EmailAddress' in rec:
                            participants.add(rec['EmailAddress'].get('Address', '').lower())

                # Add BccRecipients
                if 'BccRecipients' in msg:
                    for rec in msg['BccRecipients']:
                        if 'EmailAddress' in rec:
                            participants.add(rec['EmailAddress'].get('Address', '').lower())

            # Count participants (excluding me)
            external_participants = {p for p in participants if p != my_email.lower()}

            # Identify which TOP collaborators are involved
            involved_collaborators = [p for p in external_participants if p in collaborator_set]

            # Log categorization for threads with collaborators
            if len(external_participants) > 0:
                thread_preview = thread_id[:20] + "..." if len(thread_id) > 20 else thread_id
                logging.info(f"Thread {thread_preview}: {len(external_participants)} external participants")
                if len(external_participants) <= 3:
                    logging.info(f"  Participants: {', '.join(list(external_participants)[:5])}")
                else:
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
    """
    Extract calendar events for the date range.

    Args:
        start_date: Start datetime in ISO format
        end_date: End datetime in ISO format
        date_str: Date string for filename

    Returns:
        Dict containing extracted calendar events
    """
    logging.info("Extracting calendar events...")

    try:
        # Convert dates for calendar API (remove milliseconds)
        start_date_cal = start_date.split('.')[0] + 'Z'
        end_date_cal = end_date.split('.')[0] + 'Z'

        calendar_service = CalendarService()
        events_result = calendar_service.get_events(
            start_date=start_date_cal,
            end_date=end_date_cal,
            top=500
        )

        events = events_result.get('events', [])
        logging.info(f"Retrieved {len(events)} calendar events")

        # Filter for events with attendees
        events_with_attendees = []
        for event in events:
            if 'Attendees' in event and event['Attendees']:
                events_with_attendees.append(event)

        logging.info(f"Found {len(events_with_attendees)} events with attendees")

        result = {
            'date': date_str,
            'extraction_window': {'start': start_date_cal, 'end': end_date_cal},
            'total_events': len(events),
            'events_with_attendees': len(events_with_attendees),
            'events_list': events
        }

        # Save to file
        filename = f"{date_str}_calendar_events.json"
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        logging.info(f"Saved {len(events)} events to {filename}")

        return result

    except Exception as e:
        logging.error(f"Failed to extract calendar events: {str(e)}")
        logging.error(traceback.format_exc())
        return None


# ============================================================================
# TRANSCRIPT TEXT FILE HELPERS
# ============================================================================

def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe file system usage."""
    # Remove .mp4 extension
    if filename.lower().endswith('.mp4'):
        filename = filename[:-4]
    
    # Replace spaces with underscores
    filename = filename.replace(' ', '_')
    
    # Remove or replace problematic characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Remove multiple consecutive underscores
    filename = re.sub(r'_+', '_', filename)
    
    # Remove leading/trailing underscores
    filename = filename.strip('_')
    
    return filename


def save_transcript_text_file(transcript: Dict, date_str: str, output_dir: str = "output/meetings"):
    """
    Save transcript as individual text file.
    
    Args:
        transcript: Transcript data from daily extraction
        date_str: Date string for filename
        output_dir: Directory to save text files
        
    Returns:
        Filename if successful, None if failed
    """
    try:
        # Ensure output directory exists
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Get file name and sanitize
        file_name = transcript.get('file_name', 'unknown_meeting')
        sanitized_name = sanitize_filename(file_name)
        
        # Generate filename: YYYY-MM-DD_meeting_name.txt
        filename = f"{date_str}_{sanitized_name}.txt"
        filepath = os.path.join(output_dir, filename)
        
        # Format content
        created_time = transcript.get('created_time', 'Unknown')
        text_content = transcript.get('text_content', '')
        
        content = f"""Meeting: {file_name.replace('.mp4', '')}
Date: {created_time}

========================================
TRANSCRIPT:
========================================

{text_content}
"""
        
        # Write to file
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return filename
        
    except Exception as e:
        logging.error(f"Failed to save transcript text file: {e}")
        return None


def extract_meeting_transcripts(start_date: str, end_date: str, date_str: str):
    """
    Extract meeting transcripts (text content from meeting recordings).


    Args:
        start_date: Start datetime in ISO format
        end_date: End datetime in ISO format
        date_str: Date string for filename

    Returns:
        Dict containing extracted transcripts
    """
    logging.info("Extracting meeting transcripts...")

    try:
        calendar_service = CalendarService()
        transcripts = calendar_service.get_meeting_most_recent_transcripts(
            start_datetime=start_date,
            end_datetime=end_date,
            top=100
        )

        if not transcripts:
            logging.info("No meeting transcripts found")
            result = {
                'date': date_str,
                'extraction_window': {'start': start_date, 'end': end_date},
                'total_transcripts': 0,
                'transcripts_with_content': 0,
                'filtered_out_empty': 0,
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

        # Also save individual transcript text files
        if processed_transcripts:
            saved_files = 0
            for transcript in processed_transcripts:
                saved_filename = save_transcript_text_file(transcript, date_str)
                if saved_filename:
                    saved_files += 1
            
            logging.info(f"Saved {saved_files} transcript text files to output/meetings/")
            result['transcript_text_files_saved'] = saved_files
        else:
            result['transcript_text_files_saved'] = 0

        return result

    except Exception as e:
        logging.error(f"Failed to extract meeting transcripts: {str(e)}")
        logging.error(traceback.format_exc())
        return None


# ============================================================================
# MAIN EXTRACTION FUNCTION
# ============================================================================

def run_daily_extraction():
    """Run the complete daily extraction process."""
    logging.info("=" * 80)
    logging.info("Starting daily M365 data extraction")
    logging.info("=" * 80)

    try:
        # Calculate extraction window
        start_date, end_date, date_str = calculate_extraction_window()

        # Ensure output directory exists
        ensure_output_directory()

        # Step 1: Extract collaborators (if needed)
        # Check if collaborators file already exists
        collab_file = os.path.join(OUTPUT_DIR, "collaborators.json")

        if os.path.exists(collab_file):
            logging.info(f"Using existing collaborators from {collab_file}")
            with open(collab_file, 'r', encoding='utf-8') as f:
                collab_data = json.load(f)
                collaborators = collab_data.get('collaborators', [])
            logging.info(f"Loaded {len(collaborators)} collaborators from cache")
        else:
            logging.info("No existing collaborators found, extracting fresh collaborators...")
            collaborators = extract_top_collaborators(top_n=20)

        if not collaborators:
            logging.warning("No collaborators found, skipping email/Teams extraction")
            return

        # Step 2: Extract email exchanges
        email_result = extract_email_exchanges(collaborators, start_date, end_date, date_str)

        # Step 3: Extract Teams messages
        teams_result = extract_teams_messages(collaborators, start_date, end_date, date_str)

        # Step 4: Extract calendar events
        calendar_result = extract_calendar_events(start_date, end_date, date_str)

        # Step 5: Extract meeting transcripts
        transcript_result = extract_meeting_transcripts(start_date, end_date, date_str)

        # Summary
        logging.info("=" * 80)
        logging.info("Daily extraction completed")
        logging.info(f"Date: {date_str}")
        logging.info(f"Email exchanges: {email_result['collaborator_emails'] if email_result else 0}")
        logging.info(f"Teams messages: {teams_result['collaborator_messages'] if teams_result else 0}")
        logging.info(f"Calendar events: {calendar_result['total_events'] if calendar_result else 0}")
        logging.info(f"Meeting transcripts: {transcript_result['total_transcripts'] if transcript_result else 0}")
        logging.info(f"Output directory: {OUTPUT_DIR}")
        logging.info("=" * 80)

        # Sync to OneDrive after successful extraction
        sync_to_onedrive()

        return True

    except Exception as e:
        logging.error(f"Daily extraction failed: {str(e)}")
        logging.error(traceback.format_exc())
        return False


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    # Setup logging
    setup_logging()

    # Run extraction
    success = run_daily_extraction()

    # Exit with appropriate code
    sys.exit(0 if success else 1)


# ============================================================================
# WINDOWS TASK SCHEDULER SETUP INSTRUCTIONS
# ============================================================================

r"""
SETUP FOR WINDOWS TASK SCHEDULER
=================================

To run this script automatically at 8:00 AM Beijing time every day:

1. Open Task Scheduler (taskschd.msc)

2. Click "Create Basic Task..."

3. Name: "Daily M365 Data Extraction"

4. Trigger: Daily, 8:00 AM
   - Note: Task Scheduler uses local time, so set to 8:00 AM

5. Action: Start a program
   - Program: python
   - Arguments: full_path_to_script\\daily_extract.py
   - Start in: full_path_to_script_directory

6. In the "Edit Action" dialog:
   - Program/script: C:\\Python314\\python.exe
   - Add arguments: C:\\Users\\zhuoyingwang\\Documents\\SubstrateDataExtract\\daily_extract.py
   - Start in: C:\\Users\\zhuoyingwang\\Documents\\SubstrateDataExtract

7. Click "Finish"

8. To test: Right-click the task → "Run"

Alternatively, use PowerShell to create the task:

powershell -Command "$action = New-ScheduledTaskAction -Execute 'python' -Argument 'C:\\Users\\zhuoyingwang\\Documents\\SubstrateDataExtract\\daily_extract.py' -WorkingDirectory 'C:\\Users\\zhuoyingwang\\Documents\\SubstrateDataExtract'; $trigger = New-ScheduledTaskTrigger -Daily -At 8:00AM; Register-ScheduledTask -TaskName 'Daily M365 Data Extraction' -Action $action -Trigger $trigger"

"""
