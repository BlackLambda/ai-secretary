"""
Fetch ALL email messages with automatic pagination.

This script ensures complete data extraction by:
1. Fetching emails in batches
2. Automatically paginating until all emails are retrieved
3. Combining all results into a single output file
4. Providing progress tracking and statistics

Usage:
    python fetch_all_emails.py --days 30
    python fetch_all_emails.py --filter "IsRead eq false"
    python fetch_all_emails.py --start-date 2025-09-10
    python fetch_all_emails.py --inbox  # Inbox only
    python fetch_all_emails.py --sent   # Sent items only
"""

import argparse
import sys
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.services.emails import EmailService
from src.utils.json_writer import save_json


class EmailPaginationFetcher:
    """Fetches all email messages with automatic pagination."""

    # Batch size: 100 is a good balance between:
    # - Fewer requests (reduces API calls)
    # - Manageable response size (reduces memory/network issues)
    # - API limits (typically allow up to 999-1000)
    BATCH_SIZE = 100

    # Retry configuration for robustness
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # seconds

    def __init__(self):
        """Initialize the fetcher with an Email service."""
        self.service = EmailService()
        self.total_fetched = 0
        self.batch_count = 0

    def _fetch_batch_with_retry(
        self,
        top: int,
        skip: int,
        filter_query: Optional[str],
        orderby: Optional[str],
        folder_type: Optional[str] = None
    ) -> Dict:
        """
        Fetch a single batch with retry logic.

        Args:
            top: Number of emails to fetch
            skip: Number of emails to skip
            filter_query: OData filter
            orderby: OData orderby
            folder_type: 'inbox', 'sent', or None for all

        Returns:
            dict: Batch result

        Raises:
            Exception: If all retries failed
        """
        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                if folder_type == 'inbox':
                    result = self.service.get_inbox_emails(
                        top=top,
                        skip=skip,
                        filter_query=filter_query,
                        orderby=orderby
                    )
                elif folder_type == 'sent':
                    result = self.service.get_sent_emails(
                        top=top,
                        skip=skip,
                        filter_query=filter_query,
                        orderby=orderby
                    )
                else:
                    result = self.service.get_emails(
                        top=top,
                        skip=skip,
                        filter_query=filter_query,
                        orderby=orderby
                    )
                return result

            except Exception as e:
                last_error = e
                if attempt < self.MAX_RETRIES:
                    print(f"[WARN] Attempt {attempt} failed: {e}")
                    print(f"[RETRY] Waiting {self.RETRY_DELAY}s before retry {attempt + 1}/{self.MAX_RETRIES}...")
                    time.sleep(self.RETRY_DELAY)
                else:
                    print(f"[ERROR] All {self.MAX_RETRIES} attempts failed for batch at skip={skip}")

        # All retries failed
        raise last_error

    def fetch_all(
        self,
        filter_query: Optional[str] = None,
        orderby: Optional[str] = "ReceivedDateTime desc",
        max_emails: Optional[int] = None,
        folder_type: Optional[str] = None
    ) -> Dict:
        """
        Fetch all email messages with automatic pagination.

        Args:
            filter_query: OData filter expression
            orderby: OData orderby expression (default: newest first)
            max_emails: Maximum emails to fetch (None = fetch all)
            folder_type: 'inbox', 'sent', or None for all emails

        Returns:
            dict: {
                "total_count": N,
                "batches": M,
                "filter": "...",
                "emails": [...]
            }
        """
        all_emails = []
        skip = 0
        has_more = True

        folder_desc = f" from {folder_type.upper()}" if folder_type else ""
        print("\n" + "="*70)
        print(f"FETCHING ALL EMAIL MESSAGES{folder_desc} WITH PAGINATION")
        print("="*70)
        if filter_query:
            print(f"Filter: {filter_query}")
        if orderby:
            print(f"Order: {orderby}")
        print(f"Batch size: {self.BATCH_SIZE}")
        if max_emails:
            print(f"Max emails: {max_emails}")
        print("="*70 + "\n")

        while has_more:
            # Calculate how many to fetch in this batch
            batch_size = self.BATCH_SIZE
            if max_emails:
                remaining = max_emails - len(all_emails)
                if remaining <= 0:
                    break
                batch_size = min(batch_size, remaining)

            # Fetch batch
            print(f"[Batch {self.batch_count + 1}] Fetching emails {skip} to {skip + batch_size}...")

            try:
                result = self._fetch_batch_with_retry(
                    top=batch_size,
                    skip=skip,
                    filter_query=filter_query,
                    orderby=orderby,
                    folder_type=folder_type
                )

                emails = result.get('emails', [])
                count = len(emails)

                if count > 0:
                    all_emails.extend(emails)
                    self.batch_count += 1
                    self.total_fetched += count

                    print(f"[Batch {self.batch_count}] Retrieved {count} emails (Total: {self.total_fetched})")

                    # Check if we got fewer emails than requested
                    # This means we've reached the end
                    if count < batch_size:
                        print(f"[Info] Got {count} emails (less than batch size {batch_size}), reached the end")
                        has_more = False
                    else:
                        # Move to next batch
                        skip += batch_size
                else:
                    # No more emails
                    print(f"[Info] No more emails found")
                    has_more = False

            except Exception as e:
                print(f"\n[ERROR] Failed to fetch batch at skip={skip} after {self.MAX_RETRIES} retries: {e}")
                print(f"[Info] Successfully fetched {self.total_fetched} emails before error")

                # Save partial data
                print("\nPartial data has been collected. Saving what we have...")
                break

        # Prepare final result
        result = {
            "total_count": len(all_emails),
            "batches_fetched": self.batch_count,
            "batch_size_used": self.BATCH_SIZE,
            "fetched_at": datetime.now().isoformat(),
            "emails": all_emails
        }

        if folder_type:
            result["folder"] = folder_type

        if filter_query:
            result["filter"] = filter_query

        if orderby:
            result["orderby"] = orderby

        return result


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Fetch ALL email messages with automatic pagination",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch all emails from last 30 days
  python fetch_all_emails.py --days 30

  # Fetch all emails from last 7 days
  python fetch_all_emails.py --days 7

  # Fetch all emails from specific date
  python fetch_all_emails.py --start-date 2025-09-10

  # Fetch all unread emails
  python fetch_all_emails.py --filter "IsRead eq false"

  # Fetch inbox emails only
  python fetch_all_emails.py --inbox --days 30

  # Fetch sent emails only
  python fetch_all_emails.py --sent --days 30

  # Fetch emails from specific sender
  python fetch_all_emails.py --filter "From/EmailAddress/Address eq 'someone@example.com'"

  # Fetch with custom filter and limit
  python fetch_all_emails.py --filter "ReceivedDateTime ge 2025-09-01T00:00:00Z" --max 500
        """
    )

    parser.add_argument(
        '--days',
        type=int,
        help='Fetch emails from last N days (e.g., --days 30)'
    )

    parser.add_argument(
        '--start-date',
        type=str,
        help='Fetch emails from this date onwards (format: YYYY-MM-DD)'
    )

    parser.add_argument(
        '--filter',
        type=str,
        help='Custom OData filter query'
    )

    parser.add_argument(
        '--orderby',
        type=str,
        default='ReceivedDateTime desc',
        help='OData orderby expression (default: "ReceivedDateTime desc")'
    )

    parser.add_argument(
        '--max',
        type=int,
        help='Maximum number of emails to fetch (default: fetch all)'
    )

    parser.add_argument(
        '--inbox',
        action='store_true',
        help='Fetch from Inbox folder only'
    )

    parser.add_argument(
        '--sent',
        action='store_true',
        help='Fetch from Sent Items folder only'
    )

    parser.add_argument(
        '--output',
        type=str,
        default='all_emails.json',
        help='Output filename (default: all_emails.json)'
    )

    args = parser.parse_args()

    # Determine folder type
    folder_type = None
    if args.inbox:
        folder_type = 'inbox'
    elif args.sent:
        folder_type = 'sent'

    # Build filter query
    filter_query = None

    if args.filter:
        # User provided custom filter
        filter_query = args.filter
    elif args.days:
        # Calculate date from N days ago
        start_date = datetime.now() - timedelta(days=args.days)
        start_date_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
        filter_query = f"ReceivedDateTime ge {start_date_str}"
        print(f"[Info] Fetching emails from last {args.days} days (since {start_date_str})")
    elif args.start_date:
        # Use provided start date
        try:
            # Parse and validate date
            parsed_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            start_date_str = parsed_date.strftime("%Y-%m-%dT00:00:00Z")
            filter_query = f"ReceivedDateTime ge {start_date_str}"
            print(f"[Info] Fetching emails from {start_date_str}")
        except ValueError:
            print(f"[ERROR] Invalid date format: {args.start_date}")
            print("Expected format: YYYY-MM-DD (e.g., 2025-09-10)")
            sys.exit(1)

    # Create fetcher and fetch all emails
    try:
        fetcher = EmailPaginationFetcher()
        result = fetcher.fetch_all(
            filter_query=filter_query,
            orderby=args.orderby,
            max_emails=args.max,
            folder_type=folder_type
        )

        # Save to file
        save_json(result, args.output)

        # Print summary
        print("\n" + "="*70)
        print("FETCH COMPLETE")
        print("="*70)
        print(f"Total emails fetched: {result['total_count']}")
        print(f"Number of batches: {result['batches_fetched']}")
        print(f"Batch size used: {result['batch_size_used']}")
        if folder_type:
            print(f"Folder: {folder_type}")
        if filter_query:
            print(f"Filter applied: {filter_query}")
        print(f"Saved to: output/{args.output}")
        print("="*70 + "\n")

    except KeyboardInterrupt:
        print("\n\n[INFO] Fetch interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] Fetch failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
