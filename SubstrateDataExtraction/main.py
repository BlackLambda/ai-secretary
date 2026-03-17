"""
Main CLI entry point for SubstrateDataExtract.

Usage:
    python main.py collaborators --top 10
    python main.py calendar --start 2025-01-01 --end 2025-01-31
    python main.py teams --team-id <id>
    python main.py files --top 50
"""

import argparse
import sys
import os
from datetime import datetime, timedelta

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.services.collaborators import CollaboratorsService
from src.services.calendars import CalendarService
from src.services.teams import TeamsService
from src.services.files import FilesService
from src.services.emails import EmailService


def cmd_collaborators(args):
    """Fetch top collaborators."""
    service = CollaboratorsService()
    result = service.get_top_collaborators(top_n=args.top)
    print(f"\n[SUCCESS] Fetched {result['count']} collaborators")


def cmd_calendar(args):
    """Fetch calendar events."""
    service = CalendarService()

    # Parse dates or use defaults
    start_date = args.start if args.start else None
    end_date = args.end if args.end else None

    # If no dates provided, use last 7 days
    if not start_date or not end_date:
        end = datetime.now()
        start = end - timedelta(days=7)
        start_date = start.strftime("%Y-%m-%dT00:00:00.000Z")
        end_date = end.strftime("%Y-%m-%dT23:59:59.999Z")
        print(f"[INFO] No dates provided, using last 7 days")

    result = service.get_events(
        start_date=start_date,
        end_date=end_date,
        top=args.top
    )
    print(f"\n[SUCCESS] Fetched {result['count']} calendar events")


def cmd_teams(args):
    """Fetch Teams messages."""
    service = TeamsService()

    if args.folders:
        # List mail folders
        result = service.get_mail_folders()
        print(f"\n[SUCCESS] Fetched {result['count']} mail folders")
    else:
        # Fetch Teams messages
        result = service.get_teams_messages(
            top=args.top,
            skip=args.skip,
            filter_query=args.filter,
            orderby=args.orderby
        )
        print(f"\n[SUCCESS] Fetched {result['count']} Teams messages")


def cmd_files(args):
    """Fetch files."""
    service = FilesService()

    if args.search:
        # Search files
        result = service.search_files(query=args.search, top=args.top)
        print(f"\n[SUCCESS] Found {result['count']} files matching '{args.search}'")
    elif args.shared:
        # Get shared files
        result = service.get_shared_files(top=args.top)
        print(f"\n[SUCCESS] Fetched {result['count']} shared files")
    else:
        # Get recent files (default)
        result = service.get_recent_files(top=args.top)
        print(f"\n[SUCCESS] Fetched {result['count']} recent files")


def cmd_email(args):
    """Fetch email messages."""
    service = EmailService()

    if args.folders:
        # List mail folders
        result = service.get_mail_folders()
        print(f"\n[SUCCESS] Fetched {result['count']} mail folders")
    elif args.inbox:
        # Fetch inbox emails
        result = service.get_inbox_emails(
            top=args.top,
            skip=args.skip,
            filter_query=args.filter,
            orderby=args.orderby
        )
        print(f"\n[SUCCESS] Fetched {result['count']} inbox emails")
    elif args.sent:
        # Fetch sent emails
        result = service.get_sent_emails(
            top=args.top,
            skip=args.skip,
            filter_query=args.filter,
            orderby=args.orderby
        )
        print(f"\n[SUCCESS] Fetched {result['count']} sent emails")
    else:
        # Fetch all emails
        result = service.get_emails(
            top=args.top,
            skip=args.skip,
            filter_query=args.filter,
            orderby=args.orderby
        )
        print(f"\n[SUCCESS] Fetched {result['count']} emails")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="SubstrateDataExtract - Fetch data from Substrate APIs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py collaborators --top 10
  python main.py calendar --start 2025-01-01T00:00:00.000Z --end 2025-01-31T23:59:59.999Z
  python main.py teams --top 20
  python main.py teams --top 10 --skip 20
  python main.py teams --filter "IsRead eq false"
  python main.py teams --filter "ReceivedDateTime ge 2025-10-01T00:00:00Z" --orderby "ReceivedDateTime desc"
  python main.py teams --folders
  python main.py email --top 50
  python main.py email --inbox --top 100
  python main.py email --sent --top 50
  python main.py email --filter "IsRead eq false"
  python main.py email --filter "ReceivedDateTime ge 2025-10-01T00:00:00Z" --orderby "ReceivedDateTime desc"
  python main.py email --folders
  python main.py files --top 50
  python main.py files --search "report"
  python main.py files --shared
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Collaborators command
    parser_collab = subparsers.add_parser('collaborators', help='Fetch top collaborators')
    parser_collab.add_argument('--top', type=int, default=10, help='Number of collaborators (default: 10)')
    parser_collab.set_defaults(func=cmd_collaborators)

    # Calendar command
    parser_calendar = subparsers.add_parser('calendar', help='Fetch calendar events')
    parser_calendar.add_argument('--start', type=str, help='Start date (ISO format: 2025-01-01T00:00:00.000Z)')
    parser_calendar.add_argument('--end', type=str, help='End date (ISO format: 2025-01-31T23:59:59.999Z)')
    parser_calendar.add_argument('--top', type=int, default=100, help='Max events (default: 100)')
    parser_calendar.set_defaults(func=cmd_calendar)

    # Teams command
    parser_teams = subparsers.add_parser('teams', help='Fetch Teams messages')
    parser_teams.add_argument('--top', type=int, default=50, help='Max messages (default: 50)')
    parser_teams.add_argument('--skip', type=int, default=0, help='Skip N messages for pagination (default: 0)')
    parser_teams.add_argument('--filter', type=str, help='OData filter query (e.g., "IsRead eq false")')
    parser_teams.add_argument('--orderby', type=str, help='OData orderby (e.g., "ReceivedDateTime desc")')
    parser_teams.add_argument('--folders', action='store_true', help='List mail folders instead')
    parser_teams.set_defaults(func=cmd_teams)

    # Email command
    parser_email = subparsers.add_parser('email', help='Fetch email messages')
    parser_email.add_argument('--top', type=int, default=50, help='Max emails (default: 50)')
    parser_email.add_argument('--skip', type=int, default=0, help='Skip N emails for pagination (default: 0)')
    parser_email.add_argument('--filter', type=str, help='OData filter query (e.g., "IsRead eq false")')
    parser_email.add_argument('--orderby', type=str, help='OData orderby (e.g., "ReceivedDateTime desc")')
    parser_email.add_argument('--inbox', action='store_true', help='Fetch from Inbox only')
    parser_email.add_argument('--sent', action='store_true', help='Fetch from Sent Items only')
    parser_email.add_argument('--folders', action='store_true', help='List mail folders instead')
    parser_email.set_defaults(func=cmd_email)

    # Files command
    parser_files = subparsers.add_parser('files', help='Fetch files')
    parser_files.add_argument('--top', type=int, default=100, help='Max files (default: 100)')
    parser_files.add_argument('--search', type=str, help='Search query')
    parser_files.add_argument('--shared', action='store_true', help='Get shared files')
    parser_files.set_defaults(func=cmd_files)

    # Parse arguments
    args = parser.parse_args()

    # Show help if no command provided
    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Execute command
    try:
        args.func(args)
    except Exception as e:
        print(f"\n[ERROR] Command failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
