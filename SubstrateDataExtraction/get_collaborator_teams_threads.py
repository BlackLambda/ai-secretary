"""
Get Teams message threads between me and key collaborators.

Strategy:
1. Fetch all Teams messages from last N days
2. Group messages by ClientThreadId (thread)
3. Categorize threads by participant count
4. Only filter out threads where BOTH you AND collaborator have 0% engagement

Categories:
- 1on1_with_collaborator: 2 participants, one is top collaborator
- group_threads: 3+ participants

Filtering:
- Only excludes threads where you AND the collaborator BOTH have 0% engagement

Prerequisites:
- Run `python get_top_collaborators.py` first to generate top_collaborators.json
"""

import sys
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Set
from collections import defaultdict

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.client.substrate_client import SubstrateClient
from src.utils.json_writer import save_json


class CollaboratorTeamsThreadsService:
    """Service for fetching Teams threads with key collaborators."""

    def __init__(self):
        """Initialize the service with a Substrate client."""
        self.client = SubstrateClient()
        self.my_email = self.client.upn  # Get my email from token
        print(f"[INFO] My email: {self.my_email}")

    def fetch_all_teams_messages(self, days: int = 30) -> List[Dict]:
        """
        Fetch all Teams messages from the last N days with pagination.

        Args:
            days: Number of days to look back (default: 30)

        Returns:
            List of all Teams messages
        """
        # Calculate date filter
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        date_filter = f"ReceivedDateTime ge {start_date.strftime('%Y-%m-%dT00:00:00Z')}"

        print(f"\n{'='*60}")
        print(f"Fetching Teams Messages from last {days} days")
        print(f"Date filter: {date_filter}")
        print(f"{'='*60}")

        url = "https://substrate.office.com/api/beta/me/mailfolders('TeamsMessagesData')/messages"

        all_messages = []
        page = 1
        next_url = url

        params = {
            "$filter": date_filter,
            "$top": 1000,
            "$orderby": "ReceivedDateTime desc"
        }

        while next_url:
            print(f"[Page {page}] Fetching up to 1000 messages...")

            # For first request, use params; for subsequent, nextUrl already has params
            if page == 1:
                response = self.client.get(next_url, params=params)
            else:
                response = self.client.get(next_url)

            messages = response.get('value', [])
            all_messages.extend(messages)
            print(f"[Page {page}] Retrieved {len(messages)} messages (Total: {len(all_messages)})")

            # Check for next page
            next_url = response.get('@odata.nextLink')
            page += 1

        print(f"[DONE] Total messages fetched: {len(all_messages)}")
        return all_messages

    def group_messages_by_thread(self, messages: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Group messages by ClientThreadId.

        Args:
            messages: List of Teams messages

        Returns:
            Dict mapping thread_id -> list of messages
        """
        threads = defaultdict(list)

        for msg in messages:
            thread_id = msg.get('ClientThreadId', '')
            if thread_id:
                threads[thread_id].append(msg)

        print(f"\n[INFO] Grouped {len(messages)} messages into {len(threads)} threads")
        return dict(threads)

    def analyze_thread(self, thread_id: str, messages: List[Dict]) -> Dict:
        """
        Analyze a thread to extract engagement metrics.

        Returns:
            {
                "thread_id": "...",
                "message_count": N,
                "participants": {"email": message_count, ...},
                "participant_count": N,
                "my_message_count": N,
                "my_engagement_rate": 0.XX,
                "messages": [...],
                "first_message_date": "...",
                "last_message_date": "..."
            }
        """
        # Count messages per participant
        participant_messages = defaultdict(int)

        for msg in messages:
            sender_email = ''
            from_field = msg.get('From') or msg.get('from', {})
            if from_field and 'EmailAddress' in from_field:
                sender_email = from_field['EmailAddress'].get('Address', '').lower()
            elif from_field and 'emailAddress' in from_field:
                sender_email = from_field['emailAddress'].get('address', '').lower()

            if sender_email:
                participant_messages[sender_email] += 1

        # Calculate metrics
        total_messages = len(messages)
        my_message_count = participant_messages.get(self.my_email.lower(), 0)
        my_engagement_rate = my_message_count / total_messages if total_messages > 0 else 0

        # Get date range
        dates = [msg.get('ReceivedDateTime', '') for msg in messages if msg.get('ReceivedDateTime')]
        dates.sort()

        return {
            "thread_id": thread_id,
            "message_count": total_messages,
            "participants": dict(participant_messages),
            "participant_count": len(participant_messages),
            "my_message_count": my_message_count,
            "my_engagement_rate": round(my_engagement_rate, 3),
            "messages": sorted(messages, key=lambda x: x.get('ReceivedDateTime', '')),
            "first_message_date": dates[0] if dates else "",
            "last_message_date": dates[-1] if dates else ""
        }

    def _create_thread_debug_info(
        self,
        thread_id: str,
        analysis: Dict,
        category: str,
        collaborator_email: str,
        messages: List[Dict]
    ) -> Dict:
        """Create debug info for a filtered thread."""
        # Get sample messages (first 3)
        sample_messages = []
        for msg in sorted(messages, key=lambda x: x.get('ReceivedDateTime', ''))[:3]:
            sender = ''
            from_field = msg.get('From') or msg.get('from', {})
            if from_field and 'EmailAddress' in from_field:
                sender = from_field['EmailAddress'].get('Address', '')
            elif from_field and 'emailAddress' in from_field:
                sender = from_field['emailAddress'].get('address', '')

            body_preview = msg.get('BodyPreview', msg.get('bodyPreview', ''))
            if not body_preview:
                body = msg.get('Body') or msg.get('body', {})
                if isinstance(body, dict):
                    body_content = body.get('Content', body.get('content', ''))
                    body_preview = body_content[:150] if body_content else ''

            sample_messages.append({
                "date": msg.get('ReceivedDateTime', msg.get('receivedDateTime', '')),
                "sender": sender,
                "preview": body_preview[:150] if body_preview else ''
            })

        # Build reason string
        participants = analysis['participants']
        my_engagement = analysis['my_engagement_rate']
        collab_message_count = participants.get(collaborator_email.lower(), 0)
        collab_engagement = collab_message_count / analysis['message_count'] if analysis['message_count'] > 0 else 0

        if category == "noise":
            reason = f"Low engagement - Participants: {analysis['participant_count']}, Your engagement: {my_engagement*100:.1f}%, Collaborator engagement: {collab_engagement*100:.1f}%"
        elif category == "your_high_engagement":
            reason = f"Your high engagement but categorized separately - Your engagement: {my_engagement*100:.1f}%, Collaborator engagement: {collab_engagement*100:.1f}%"
        else:
            reason = f"Category: {category}"

        return {
            "thread_id": thread_id,
            "reason": reason,
            "participant_count": analysis['participant_count'],
            "message_count": analysis['message_count'],
            "my_engagement_rate": analysis['my_engagement_rate'],
            "collaborator_engagement_rate": round(collab_engagement, 3),
            "date_range": f"{analysis['first_message_date']} to {analysis['last_message_date']}",
            "all_participants": list(participants.keys()),
            "sample_messages": sample_messages
        }

    def categorize_thread(
        self,
        thread_analysis: Dict,
        collaborator_email: str = None
    ) -> str:
        """
        Categorize thread based on participant count.

        Categories:
        - "1on1_with_collaborator": 2 participants, one is collaborator
        - "group_thread": 3+ participants
        - "noise": Both you AND collaborator have 0% engagement (excluded)

        Args:
            thread_analysis: Result from analyze_thread()
            collaborator_email: Email of collaborator to check (optional)

        Returns:
            Category name
        """
        participant_count = thread_analysis['participant_count']
        my_engagement = thread_analysis['my_engagement_rate']
        participants = thread_analysis['participants']

        # Get collaborator engagement if provided
        collab_engagement = 0
        if collaborator_email:
            collab_message_count = participants.get(collaborator_email.lower(), 0)
            collab_engagement = collab_message_count / thread_analysis['message_count'] if thread_analysis['message_count'] > 0 else 0

        # ONLY FILTER: Both you AND collaborator have 0% engagement
        if collaborator_email and my_engagement == 0 and collab_engagement == 0:
            return "noise"

        # Check if I'm in the thread
        my_in_thread = self.my_email.lower() in participants

        # 1on1 with collaborator - exactly 2 people: me and the collaborator
        if participant_count == 2 and collaborator_email and collab_engagement > 0 and my_in_thread:
            return "1on1_with_collaborator"

        # All other threads are group threads
        return "group_thread"

    def extract_threads_with_collaborator(
        self,
        collaborator_name: str,
        collaborator_email: str,
        all_threads: Dict[str, List[Dict]]
    ) -> Dict:
        """
        Extract and categorize threads involving a specific collaborator.

        Args:
            collaborator_name: Name of collaborator
            collaborator_email: Email of collaborator
            all_threads: All threads grouped by thread_id

        Returns:
            {
                "1on1_threads": [...],
                "group_threads": [...],
                "thread_count": N,
                "message_count": M,
                "debug_filtered": [...]
            }
        """
        print(f"\n{'='*60}")
        print(f"Processing threads with: {collaborator_name} ({collaborator_email})")
        print(f"{'='*60}")

        categorized_threads = {
            "1on1_threads": [],
            "group_threads": []
        }

        filtered_out = []
        total_messages = 0

        for thread_id, messages in all_threads.items():
            # Check if collaborator is in this thread
            thread_participants = set()
            for msg in messages:
                from_field = msg.get('From') or msg.get('from', {})
                if from_field and 'EmailAddress' in from_field:
                    sender_email = from_field['EmailAddress'].get('Address', '').lower()
                    thread_participants.add(sender_email)
                elif from_field and 'emailAddress' in from_field:
                    sender_email = from_field['emailAddress'].get('address', '').lower()
                    thread_participants.add(sender_email)

            # Skip if collaborator not in thread
            if collaborator_email.lower() not in thread_participants:
                continue

            # Analyze thread
            analysis = self.analyze_thread(thread_id, messages)

            # Categorize thread
            category = self.categorize_thread(analysis, collaborator_email)

            # Add to appropriate category
            if category == "1on1_with_collaborator":
                categorized_threads["1on1_threads"].append(analysis)
                total_messages += analysis['message_count']
            elif category == "group_thread":
                categorized_threads["group_threads"].append(analysis)
                total_messages += analysis['message_count']
            else:
                # "noise" - both you and collaborator have 0% engagement
                # Log them for debugging
                filtered_out.append(self._create_thread_debug_info(
                    thread_id, analysis, category, collaborator_email, messages
                ))

        print(f"[Results] 1-on-1: {len(categorized_threads['1on1_threads'])} threads")
        print(f"[Results] Group: {len(categorized_threads['group_threads'])} threads")
        print(f"[Results] Total messages: {total_messages}")
        print(f"[Results] Filtered out (0% engagement): {len(filtered_out)} threads")

        if filtered_out:
            print(f"\n[DEBUG] Filtered threads details:")
            for item in filtered_out:
                print(f"  Thread ({item['participant_count']} participants, {item['message_count']} messages)")
                print(f"    Date range: {item['date_range']}")
                print(f"    Reason: {item['reason']}")
                print(f"    Participants: {', '.join(item['all_participants'][:8])}")
                if len(item['all_participants']) > 8:
                    print(f"      ... and {len(item['all_participants']) - 8} more")
                if item.get('sample_messages'):
                    print(f"    Sample messages:")
                    for msg in item['sample_messages'][:2]:
                        print(f"      [{msg['date'][:10]}] {msg['sender']}: {msg['preview'][:80]}...")
                print()

        return {
            **categorized_threads,
            "thread_count": len(categorized_threads['1on1_threads']) +
                          len(categorized_threads['group_threads']),
            "message_count": total_messages,
            "debug_filtered": filtered_out
        }

    def extract_your_high_engagement_threads(
        self,
        all_threads: Dict[str, List[Dict]],
        collaborator_emails: Set[str]
    ) -> Dict:
        """
        Extract threads where you have high engagement (>20%)
        regardless of collaborator presence.

        Args:
            all_threads: All threads grouped by thread_id
            collaborator_emails: Set of collaborator emails to track separately

        Returns:
            {
                "high_engagement_threads": [...],
                "thread_count": N,
                "message_count": M
            }
        """
        print(f"\n{'='*60}")
        print(f"Processing your high-engagement threads")
        print(f"{'='*60}")

        high_engagement_threads = []
        total_messages = 0

        for thread_id, messages in all_threads.items():
            # Analyze thread
            analysis = self.analyze_thread(thread_id, messages)

            # Check if high engagement
            if analysis['my_engagement_rate'] > 0.20:
                # Categorize without specific collaborator
                category = self.categorize_thread(analysis, collaborator_email=None)

                if category == "your_high_engagement":
                    high_engagement_threads.append(analysis)
                    total_messages += analysis['message_count']

        print(f"[Results] High engagement threads: {len(high_engagement_threads)}")
        print(f"[Results] Total messages: {total_messages}")

        return {
            "high_engagement_threads": high_engagement_threads,
            "thread_count": len(high_engagement_threads),
            "message_count": total_messages
        }

    def load_collaborators_from_file(self) -> List[Dict]:
        """
        Load collaborators from top_collaborators.json.

        Returns:
            List of collaborator dicts with 'alias', 'email', 'upn'

        Raises:
            FileNotFoundError: If top_collaborators.json doesn't exist
            ValueError: If file format is invalid
        """
        output_dir = os.path.join(os.path.dirname(__file__), 'output')
        file_path = os.path.join(output_dir, 'top_collaborators.json')

        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"Could not find {file_path}\n"
                f"Please run: python get_top_collaborators.py --top N"
            )

        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if 'collaborators' not in data:
            raise ValueError(f"Invalid format in {file_path}: missing 'collaborators' key")

        collaborators = data['collaborators']

        # Remove myself from the list
        collaborators = [c for c in collaborators if c['email'].lower() != self.my_email.lower()]

        return collaborators

    def get_all_collaborator_threads(
        self,
        days: int = 30
    ) -> Dict:
        """
        Get Teams threads with collaborators from top_collaborators.json.

        Args:
            days: Number of days to look back (default: 30)

        Returns:
            {
                "collaborator_name": {
                    "1on1_threads": [...],
                    "group_threads": [...],
                    "thread_count": N,
                    "message_count": M
                },
                "summary": {
                    "total_threads": N,
                    "total_messages": M,
                    "date_range": "...",
                    "days": 30
                }
            }
        """
        # Step 1: Load collaborators from file
        print(f"\n{'='*60}")
        print(f"Loading Collaborators from top_collaborators.json")
        print(f"{'='*60}")

        try:
            collaborators = self.load_collaborators_from_file()
        except FileNotFoundError as e:
            print(f"[ERROR] {e}")
            sys.exit(1)
        except ValueError as e:
            print(f"[ERROR] {e}")
            sys.exit(1)

        collaborator_emails = {c['email'].lower() for c in collaborators}

        print(f"[INFO] Processing {len(collaborators)} collaborators (excluding self)")

        # Step 2: Fetch all Teams messages
        all_messages = self.fetch_all_teams_messages(days=days)

        # Step 3: Group by thread
        all_threads = self.group_messages_by_thread(all_messages)

        # Step 4: Extract threads for each collaborator
        results = {}

        for collab in collaborators:
            name = collab.get('alias', collab.get('email'))
            email = collab['email']

            try:
                threads = self.extract_threads_with_collaborator(name, email, all_threads)
                results[name] = threads
            except Exception as e:
                print(f"[ERROR] Failed to process {name}: {e}")
                results[name] = {
                    "1on1_threads": [],
                    "group_threads": [],
                    "thread_count": 0,
                    "message_count": 0,
                    "error": str(e)
                }

        # Step 5: Calculate summary (no separate high-engagement threads needed)
        total_threads = sum(r.get('thread_count', 0) for r in results.values() if isinstance(r, dict))
        total_messages = sum(r.get('message_count', 0) for r in results.values() if isinstance(r, dict))

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        results["summary"] = {
            "total_threads": total_threads,
            "total_messages": total_messages,
            "date_range": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
            "days": days
        }

        return results
    
    def get_teams_threads_metadata(self, max_threads: int = 1000) -> Dict[str, str]:
        url = "https://substrate.office.com/entityserve/api/search"
        headers = {
            "content-type": "application/json",
            "X-AnchorMailbox": f"UPN:{self.client.upn}",
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
        response = self.client.post(url, json=payload, extra_headers=headers)
        result = {}
        for entity in response["EntityResults"][0]["Entities"]:
            result[entity["ThreadId"]] = {
                "name": entity.get("Name"),
                "type": entity.get("Type"),
                "thread_type": entity.get("ThreadType"),
                "last_message_time": entity.get("LastMessageTime"),
                "my_last_message_time": entity.get("MyLastMessageTime"),
            }
        return result


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Get Teams message threads with key collaborators from top_collaborators.json"
    )
    parser.add_argument(
        '--days',
        type=int,
        default=30,
        help='Number of days to look back (default: 30)'
    )

    args = parser.parse_args()

    # Initialize service
    service = CollaboratorTeamsThreadsService()

    # Get Teams threads map info
    threads_metadata = service.get_teams_threads_metadata()
    save_json(threads_metadata, "collaborator_teams_threads_metadata.json")

    # Get all threads
    results = service.get_all_collaborator_threads(
        days=args.days
    )

    # Extract debug info for separate file
    debug_info = {}
    clean_results = {}
    for name, data in results.items():
        if isinstance(data, dict) and 'debug_filtered' in data:
            debug_info[name] = data['debug_filtered']
            for t in [*data.get('1on1_threads', []), *data.get('group_threads', [])]:
                thread_id = t.get("thread_id")
                t["thread_metadata"] = threads_metadata.get(thread_id)
            clean_results[name] = {
                "1on1_threads": data.get('1on1_threads', []),
                "group_threads": data.get('group_threads', []),
                "thread_count": data.get('thread_count', 0),
                "message_count": data.get('message_count', 0)
            }
        else:
            clean_results[name] = data

    # Save main results (without debug info)
    save_json(clean_results, "collaborator_teams_threads.json")

    # Save debug info to separate file
    if debug_info:
        save_json(debug_info, "filtered_threads_debug.json")
        total_filtered = sum(len(items) for items in debug_info.values() if isinstance(items, list))
        print(f"\n[DEBUG] Saved {total_filtered} filtered threads to filtered_threads_debug.json")

    # Print summary
    print(f"\n{'='*60}")
    print(f"Summary")
    print(f"{'='*60}")

    for name, data in clean_results.items():
        if name == "summary":
            continue
        if isinstance(data, dict):
            print(f"{name}:")
            print(f"  1-on-1: {len(data.get('1on1_threads', []))} threads")
            print(f"  Group: {len(data.get('group_threads', []))} threads")
            print(f"  Total: {data.get('thread_count', 0)} threads, {data.get('message_count', 0)} messages")

    print(f"\n{clean_results.get('summary', {})}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
