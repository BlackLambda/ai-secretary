"""
Fetch ALL Teams messages with automatic pagination.

This script ensures complete data extraction by:
1. Fetching messages in batches
2. Automatically paginating until all messages are retrieved
3. Combining all results into a single output file
4. Providing progress tracking and statistics

Usage:
    python fetch_all_teams_messages.py --days 30
    python fetch_all_teams_messages.py --filter "IsRead eq false"
    python fetch_all_teams_messages.py --start-date 2025-09-10
"""

import argparse
import sys
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.services.teams import TeamsService
from src.utils.json_writer import save_json


class TeamsPaginationFetcher:
    """Fetches all Teams messages with automatic pagination."""

    # Batch size: 100 is a good balance between:
    # - Fewer requests (reduces API calls)
    # - Manageable response size (reduces memory/network issues)
    # - API limits (typically allow up to 999-1000)
    BATCH_SIZE = 100

    # Retry configuration for robustness
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # seconds

    def __init__(self):
        """Initialize the fetcher with a Teams service."""
        self.service = TeamsService()
        self.total_fetched = 0
        self.batch_count = 0

    def _fetch_batch_with_retry(
        self,
        top: int,
        skip: int,
        filter_query: Optional[str],
        orderby: Optional[str]
    ) -> Dict:
        """
        Fetch a single batch with retry logic.

        Args:
            top: Number of messages to fetch
            skip: Number of messages to skip
            filter_query: OData filter
            orderby: OData orderby

        Returns:
            dict: Batch result

        Raises:
            Exception: If all retries failed
        """
        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                result = self.service.get_teams_messages(
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
        max_messages: Optional[int] = None
    ) -> Dict:
        """
        Fetch all Teams messages with automatic pagination.

        Args:
            filter_query: OData filter expression
            orderby: OData orderby expression (default: newest first)
            max_messages: Maximum messages to fetch (None = fetch all)

        Returns:
            dict: {
                "total_count": N,
                "batches": M,
                "filter": "...",
                "messages": [...]
            }
        """
        all_messages = []
        skip = 0
        has_more = True

        print("\n" + "="*70)
        print("FETCHING ALL TEAMS MESSAGES WITH PAGINATION")
        print("="*70)
        if filter_query:
            print(f"Filter: {filter_query}")
        if orderby:
            print(f"Order: {orderby}")
        print(f"Batch size: {self.BATCH_SIZE}")
        if max_messages:
            print(f"Max messages: {max_messages}")
        print("="*70 + "\n")

        while has_more:
            # Calculate how many to fetch in this batch
            batch_size = self.BATCH_SIZE
            if max_messages:
                remaining = max_messages - len(all_messages)
                if remaining <= 0:
                    break
                batch_size = min(batch_size, remaining)

            # Fetch batch with retry
            print(f"[Batch {self.batch_count + 1}] Fetching messages {skip} to {skip + batch_size}...")

            try:
                result = self._fetch_batch_with_retry(
                    top=batch_size,
                    skip=skip,
                    filter_query=filter_query,
                    orderby=orderby
                )

                messages = result.get('messages', [])
                count = len(messages)

                if count > 0:
                    all_messages.extend(messages)
                    self.batch_count += 1
                    self.total_fetched += count

                    print(f"[Batch {self.batch_count}] Retrieved {count} messages (Total: {self.total_fetched})")

                    # Check if we got fewer messages than requested
                    # This means we've reached the end
                    if count < batch_size:
                        print(f"[Info] Got {count} messages (less than batch size {batch_size}), reached the end")
                        has_more = False
                    else:
                        # Move to next batch
                        skip += batch_size
                else:
                    # No more messages
                    print(f"[Info] No more messages found")
                    has_more = False

            except Exception as e:
                print(f"\n[ERROR] Failed to fetch batch at skip={skip} after {self.MAX_RETRIES} retries: {e}")
                print(f"[Info] Successfully fetched {self.total_fetched} messages before error")

                # Save partial data
                print("\nPartial data has been collected. Saving what we have...")
                break

        # Fetch thread metadata and enrich messages
        if all_messages:
            thread_metadata = self.get_teams_threads_metadata()
            print("Enriching messages with chat topics...")
            enriched_count = 0
            for msg in all_messages:
                thread_id = msg.get('ClientConversationId') or msg.get('ConversationId')
                if thread_id and thread_id in thread_metadata:
                    metadata = thread_metadata[thread_id]
                    if metadata.get('name'):
                        msg['ChatTopic'] = metadata['name']
                        enriched_count += 1
            print(f"[Info] Enriched {enriched_count} messages with topics")

        # Prepare final result
        result = {
            "total_count": len(all_messages),
            "batches_fetched": self.batch_count,
            "batch_size_used": self.BATCH_SIZE,
            "fetched_at": datetime.now().isoformat(),
            "messages": all_messages
        }

        if filter_query:
            result["filter"] = filter_query

        if orderby:
            result["orderby"] = orderby

        return result

    def get_teams_threads_metadata(self, max_threads: int = 1000) -> Dict[str, Dict]:
        """Fetch metadata (including topic) for Teams threads."""
        print(f"\nFetching metadata for up to {max_threads} threads...")
        url = "https://substrate.office.com/entityserve/api/search"
        headers = {
            "content-type": "application/json",
            "X-AnchorMailbox": f"UPN:{self.service.client.upn}",
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
        
        try:
            response = self.service.client.post(url, json=payload, extra_headers=headers)
            result = {}
            if "EntityResults" in response and len(response["EntityResults"]) > 0:
                for entity in response["EntityResults"][0].get("Entities", []):
                    result[entity["ThreadId"]] = {
                        "name": entity.get("Name"),
                        "type": entity.get("Type"),
                        "thread_type": entity.get("ThreadType")
                    }
            print(f"[Info] Retrieved metadata for {len(result)} threads")
            return result
        except Exception as e:
            print(f"[WARN] Failed to fetch thread metadata: {e}")
            return {}


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Fetch ALL Teams messages with automatic pagination",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch all messages from last 30 days
  python fetch_all_teams_messages.py --days 30

  # Fetch all messages from last 7 days
  python fetch_all_teams_messages.py --days 7

  # Fetch all messages from specific date
  python fetch_all_teams_messages.py --start-date 2025-09-10

  # Fetch all unread messages
  python fetch_all_teams_messages.py --filter "IsRead eq false"

  # Fetch all messages from specific conversation
  python fetch_all_teams_messages.py --filter "ClientConversationId eq '19:xxx@thread.v2'"

  # Fetch with custom filter and limit
  python fetch_all_teams_messages.py --filter "ReceivedDateTime ge 2025-09-01T00:00:00Z" --max 500
        """
    )

    parser.add_argument(
        '--days',
        type=int,
        help='Fetch messages from last N days (e.g., --days 30)'
    )

    parser.add_argument(
        '--start-date',
        type=str,
        help='Fetch messages from this date onwards (format: YYYY-MM-DD)'
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
        help='Maximum number of messages to fetch (default: fetch all)'
    )

    parser.add_argument(
        '--output',
        type=str,
        default='all_teams_messages.json',
        help='Output filename (default: all_teams_messages.json)'
    )

    args = parser.parse_args()

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
        print(f"[Info] Fetching messages from last {args.days} days (since {start_date_str})")
    elif args.start_date:
        # Use provided start date
        try:
            # Parse and validate date
            parsed_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            start_date_str = parsed_date.strftime("%Y-%m-%dT00:00:00Z")
            filter_query = f"ReceivedDateTime ge {start_date_str}"
            print(f"[Info] Fetching messages from {start_date_str}")
        except ValueError:
            print(f"[ERROR] Invalid date format: {args.start_date}")
            print("Expected format: YYYY-MM-DD (e.g., 2025-09-10)")
            sys.exit(1)

    # Create fetcher and fetch all messages
    try:
        fetcher = TeamsPaginationFetcher()
        result = fetcher.fetch_all(
            filter_query=filter_query,
            orderby=args.orderby,
            max_messages=args.max
        )

        # Save to file
        save_json(result, args.output)

        # Print summary
        print("\n" + "="*70)
        print("FETCH COMPLETE")
        print("="*70)
        print(f"Total messages fetched: {result['total_count']}")
        print(f"Number of batches: {result['batches_fetched']}")
        print(f"Batch size used: {result['batch_size_used']}")
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
