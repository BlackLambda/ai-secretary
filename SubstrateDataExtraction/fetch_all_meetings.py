"""
Get email exchanges between me and key collaborators.

Extracts three types of emails:
1. Direct: Collaborator → Me
2. Direct: Me → Collaborator (from Sent Items)
3. Group: Both included in TO field

Then filters based on:
- Remove emails without subjects
- Keep all direct communications (sender is me or collaborator)
- For group emails: reject if >30 TO recipients, keep if ≤30

Prerequisites:
- Run `python get_top_collaborators.py` first to generate top_collaborators.json
"""

import sys
import os
import json
from datetime import datetime
from typing import Dict, List

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.client.substrate_client import SubstrateClient
from src.utils.json_writer import save_json


class CollaboratorExchangeService:
    """Service for fetching email exchanges with key collaborators."""

    def __init__(self):
        """Initialize the service with a Substrate client."""
        self.client = SubstrateClient()
        self.my_email = self.client.upn  # Get my email from token
        print(f"[INFO] My email: {self.my_email}")

    def get_sent_folder_id(self) -> str:
        """Get the Sent Items folder ID - just use 'sentitems' directly."""
        # Based on example_cpde.py, we can use 'sentitems' directly in the path
        return "sentitems"

    def search_emails_type1(self, collaborator_email: str, date_filter: str = None) -> List[Dict]:
        """
        Type 1: Collaborator → Me
        Search: from:{collaborator} AND to:{me}
        Note: $search and $filter cannot be used together in Graph API
        Uses pagination via @odata.nextLink to get all emails (max 1000 per page)
        """
        url = "https://substrate.office.com/api/v2.0/me/messages"

        # Build search query
        search_query = f"from:{collaborator_email} AND to:{self.my_email}"

        all_emails = []
        page = 1
        next_url = url

        params = {
            "$search": f'"{search_query}"',
            "$top": 1000
        }

        print(f"[Type 1] Searching: {search_query}")

        while next_url:
            print(f"[Type 1] Page {page}: Fetching up to 1000 emails...")

            # For first request, use params; for subsequent, nextUrl already has params
            if page == 1:
                response = self.client.get(next_url, params=params)
            else:
                response = self.client.get(next_url)

            emails = response.get('value', [])
            all_emails.extend(emails)
            print(f"[Type 1] Page {page}: Retrieved {len(emails)} emails")

            # Check for next page
            next_url = response.get('@odata.nextLink')
            page += 1

        print(f"[Type 1] Total found: {len(all_emails)} emails across {page-1} page(s)")
        return all_emails

    def search_emails_type2(self, collaborator_email: str, sent_folder_id: str, date_filter: str = None) -> List[Dict]:
        """
        Type 2: Me → Collaborator
        Search in Sent Items: to:{collaborator}
        Note: $search and $filter cannot be used together in Graph API
        Uses pagination via @odata.nextLink to get all emails (max 1000 per page)
        """
        url = f"https://substrate.office.com/api/v2.0/me/mailFolders/{sent_folder_id}/messages"

        # Build search query
        search_query = f"to:{collaborator_email}"

        all_emails = []
        page = 1
        next_url = url

        params = {
            "$search": f'"{search_query}"',
            "$top": 1000
        }

        print(f"[Type 2] Searching: {search_query}")

        while next_url:
            print(f"[Type 2] Page {page}: Fetching up to 1000 emails...")

            # For first request, use params; for subsequent, nextUrl already has params
            if page == 1:
                response = self.client.get(next_url, params=params)
            else:
                response = self.client.get(next_url)

            emails = response.get('value', [])
            all_emails.extend(emails)
            print(f"[Type 2] Page {page}: Retrieved {len(emails)} emails")

            # Check for next page
            next_url = response.get('@odata.nextLink')
            page += 1

        print(f"[Type 2] Total found: {len(all_emails)} emails across {page-1} page(s)")
        return all_emails

    def search_emails_type3(self, collaborator_email: str, date_filter: str = None) -> List[Dict]:
        """
        Type 3: Group emails where both are in TO field
        Search: to:{me} AND to:{collaborator}
        Note: $search and $filter cannot be used together in Graph API
        Uses pagination via @odata.nextLink to get all emails (max 1000 per page)
        """
        url = "https://substrate.office.com/api/v2.0/me/messages"

        # Build search query
        search_query = f"to:{self.my_email} AND to:{collaborator_email}"

        all_emails = []
        page = 1
        next_url = url

        params = {
            "$search": f'"{search_query}"',
            "$top": 1000
        }

        print(f"[Type 3] Searching: {search_query}")

        while next_url:
            print(f"[Type 3] Page {page}: Fetching up to 1000 emails...")

            # For first request, use params; for subsequent, nextUrl already has params
            if page == 1:
                response = self.client.get(next_url, params=params)
            else:
                response = self.client.get(next_url)

            emails = response.get('value', [])
            all_emails.extend(emails)
            print(f"[Type 3] Page {page}: Retrieved {len(emails)} emails")

            # Check for next page
            next_url = response.get('@odata.nextLink')
            page += 1

        print(f"[Type 3] Total found: {len(all_emails)} emails across {page-1} page(s)")
        return all_emails

    def filter_emails(self, emails: List[Dict], collaborator_email: str) -> tuple[List[Dict], List[Dict]]:
        """
        Phase 2: Filter emails based on rules:
        1. Remove emails without subjects
        2. Keep all if sender is me or collaborator
        3. For other senders: keep if ≤30 TO recipients, reject if >30

        Returns:
            (filtered_emails, debug_info_for_filtered)
        """
        filtered = []
        filtered_out = []

        for email in emails:
            # Rule 1: Remove emails without subjects
            # Try both lowercase and uppercase keys
            subject = email.get('Subject') or email.get('subject', '')
            if not subject or subject.strip() == '':
                filtered_out.append({
                    "email_id": email.get('Id', email.get('id', 'unknown')),
                    "reason": "No subject",
                    "date": email.get('ReceivedDateTime', email.get('receivedDateTime', '')),
                    "sender": self._get_sender_email(email),
                    "subject": "(empty)",
                    "to_count": len(email.get('ToRecipients') or email.get('toRecipients', []))
                })
                continue

            # Get sender email (try both capitalized and lowercase field names)
            sender_email = self._get_sender_email(email)

            # Rule 2: Keep all if sender is me or collaborator
            if sender_email.lower() == self.my_email.lower() or sender_email.lower() == collaborator_email.lower():
                filtered.append(email)
                continue

            # Rule 3: For other senders, check TO recipient count
            to_recipients = email.get('ToRecipients') or email.get('toRecipients', [])
            recipient_count = len(to_recipients)

            if recipient_count <= 30:
                filtered.append(email)
            else:
                # Get preview of body
                body = email.get('Body') or email.get('body', {})
                body_preview = email.get('BodyPreview', email.get('bodyPreview', ''))
                if not body_preview and isinstance(body, dict):
                    body_content = body.get('Content', body.get('content', ''))
                    body_preview = body_content[:200] if body_content else ''

                # Get sample recipients
                sample_recipients = [
                    r.get('EmailAddress', r.get('emailAddress', {})).get('Address', r.get('EmailAddress', r.get('emailAddress', {})).get('address', ''))
                    for r in to_recipients[:10]
                ]

                filtered_out.append({
                    "email_id": email.get('Id', email.get('id', 'unknown')),
                    "reason": f"Too many recipients: {recipient_count} > 30",
                    "date": email.get('ReceivedDateTime', email.get('receivedDateTime', '')),
                    "sender": sender_email,
                    "subject": subject,
                    "body_preview": body_preview[:200] if body_preview else '',
                    "to_count": recipient_count,
                    "sample_recipients": sample_recipients
                })

        return filtered, filtered_out

    def _get_sender_email(self, email: Dict) -> str:
        """Extract sender email from email object."""
        from_field = email.get('From') or email.get('from', {})
        if from_field and 'EmailAddress' in from_field:
            return from_field['EmailAddress'].get('Address', '')
        elif from_field and 'emailAddress' in from_field:
            return from_field['emailAddress'].get('address', '')
        return ''

    def get_exchanges_with_collaborator(
        self,
        collaborator_name: str,
        collaborator_email: str,
        date_filter: str = None
    ) -> Dict:
        """
        Get all email exchanges with a specific collaborator.

        Returns:
            {
                "count": N,
                "data": [...],
                "debug_filtered": [...]
            }
        """
        print(f"\n{'='*60}")
        print(f"Fetching emails with: {collaborator_name} ({collaborator_email})")
        print(f"{'='*60}")

        # Get Sent Items folder ID
        sent_folder_id = self.get_sent_folder_id()

        # Phase 1: Extract three types of emails
        all_emails = []

        # Type 1: Collaborator → Me
        emails_type1 = self.search_emails_type1(collaborator_email, date_filter)
        all_emails.extend(emails_type1)

        # Type 2: Me → Collaborator
        emails_type2 = self.search_emails_type2(collaborator_email, sent_folder_id, date_filter)
        all_emails.extend(emails_type2)

        # Type 3: Group emails (both in TO)
        emails_type3 = self.search_emails_type3(collaborator_email, date_filter)
        all_emails.extend(emails_type3)

        print(f"[Phase 1] Total emails before filtering: {len(all_emails)}")

        # Phase 2: Filter emails
        filtered_emails, filtered_out = self.filter_emails(all_emails, collaborator_email)

        print(f"[Phase 2] Total emails after filtering: {len(filtered_emails)}")
        print(f"[Phase 2] Filtered out: {len(filtered_out)} emails")
        if filtered_out:
            # Group by reason
            reason_counts = {}
            for item in filtered_out:
                reason = item['reason']
                if reason not in reason_counts:
                    reason_counts[reason] = 0
                reason_counts[reason] += 1
            for reason, count in reason_counts.items():
                print(f"  - {reason}: {count} emails")

            # Show details of filtered emails
            print(f"\n[DEBUG] Filtered emails details:")
            for item in filtered_out:
                print(f"  [{item['date'][:10]}] From: {item['sender']}")
                print(f"    Subject: {item['subject'][:80]}")
                print(f"    Reason: {item['reason']}")
                if item.get('sample_recipients'):
                    print(f"    Recipients sample: {', '.join(item['sample_recipients'][:5])}")
                if item.get('body_preview'):
                    print(f"    Preview: {item['body_preview'][:100]}...")
                print()
        print(f"{'='*60}\n")

        return {
            "count": len(filtered_emails),
            "data": filtered_emails,
            "debug_filtered": filtered_out
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

    def get_all_collaborator_exchanges(
        self,
        date_filter: str = None
    ) -> Dict:
        """
        Get email exchanges with collaborators from top_collaborators.json.

        Args:
            date_filter: OData date filter (e.g., "ReceivedDateTime ge 2025-10-11T00:00:00Z")

        Returns:
            {
                "collaborator_name": {
                    "count": N,
                    "data": [...]
                },
                ...
            }
        """
        print(f"\n{'='*60}")
        print(f"Loading Collaborators from top_collaborators.json")
        print(f"{'='*60}")

        # Load collaborators from file
        try:
            collaborators = self.load_collaborators_from_file()
        except FileNotFoundError as e:
            print(f"[ERROR] {e}")
            sys.exit(1)
        except ValueError as e:
            print(f"[ERROR] {e}")
            sys.exit(1)

        print(f"[INFO] Processing {len(collaborators)} collaborators (excluding self)")

        # Get exchanges with each collaborator
        results = {}

        for collab in collaborators:
            name = collab.get('alias', collab.get('email'))
            email = collab['email']

            try:
                exchanges = self.get_exchanges_with_collaborator(name, email, date_filter)
                results[name] = exchanges
            except Exception as e:
                print(f"[ERROR] Failed to get exchanges with {name}: {e}")
                results[name] = {
                    "count": 0,
                    "data": [],
                    "error": str(e)
                }

        return results


def main():
    """Main entry point for testing."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Get email exchanges with key collaborators from top_collaborators.json"
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Date filter (YYYY-MM-DD format). Example: 2025-10-11'
    )
    parser.add_argument(
        '--test-type',
        type=int,
        choices=[1, 2, 3],
        help='Test only one query type (1, 2, or 3) - requires top_collaborators.json'
    )

    args = parser.parse_args()

    # Build date filter if provided
    date_filter = None
    if args.date:
        try:
            # Parse date and create filter
            date_obj = datetime.strptime(args.date, '%Y-%m-%d')
            date_str = date_obj.strftime('%Y-%m-%dT00:00:00Z')
            date_filter = f"ReceivedDateTime ge {date_str}"
            print(f"[INFO] Using date filter: {date_filter}")
        except ValueError:
            print(f"[ERROR] Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)

    # Initialize service
    service = CollaboratorExchangeService()

    # For initial testing, allow testing individual query types
    if args.test_type:
        print(f"\n[TEST MODE] Testing Type {args.test_type} query only")

        # Load first collaborator from file for testing
        try:
            collaborators = service.load_collaborators_from_file()
        except FileNotFoundError as e:
            print(f"[ERROR] {e}")
            sys.exit(1)

        if not collaborators:
            print("[ERROR] No collaborators found in top_collaborators.json")
            sys.exit(1)

        collab = collaborators[0]
        collab_email = collab['email']

        print(f"[TEST] Using collaborator: {collab_email}")

        # Test specific query type
        if args.test_type == 1:
            emails = service.search_emails_type1(collab_email, date_filter)
        elif args.test_type == 2:
            sent_folder_id = service.get_sent_folder_id()
            emails = service.search_emails_type2(collab_email, sent_folder_id, date_filter)
        elif args.test_type == 3:
            emails = service.search_emails_type3(collab_email, date_filter)

        # Save test results
        test_result = {
            "test_type": args.test_type,
            "collaborator": collab_email,
            "count": len(emails),
            "emails": emails
        }
        save_json(test_result, f"test_type{args.test_type}_emails.json")
        print(f"\n[SUCCESS] Test completed. Found {len(emails)} emails")
    else:
        # Normal mode: get all exchanges
        results = service.get_all_collaborator_exchanges(
            date_filter=date_filter
        )

        # Extract debug info for separate file
        debug_info = {}
        clean_results = {}
        for name, data in results.items():
            if isinstance(data, dict) and 'debug_filtered' in data:
                debug_info[name] = data['debug_filtered']
                clean_results[name] = {
                    "count": data['count'],
                    "data": data['data']
                }
            else:
                clean_results[name] = data

        # Save main results (without debug info)
        save_json(clean_results, "collaborator_exchanges.json")

        # Save debug info to separate file
        if debug_info:
            save_json(debug_info, "filtered_emails_debug.json")
            total_filtered = sum(len(items) for items in debug_info.values())
            print(f"\n[DEBUG] Saved {total_filtered} filtered emails to filtered_emails_debug.json")

        # Print summary
        print(f"\n{'='*60}")
        print(f"Summary")
        print(f"{'='*60}")
        total_emails = 0
        for name, data in clean_results.items():
            count = data['count']
            total_emails += count
            print(f"{name}: {count} emails")
        print(f"{'='*60}")
        print(f"Total: {total_emails} emails across {len(clean_results)} collaborators")
        print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
