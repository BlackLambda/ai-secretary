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
from src.services.calendar import CalendarService
from src.services.teams import TeamsService
from src.services.files import FilesService
from src.services.email import EmailService


if __name__ == "__main__":
    
    test_client = CalendarService()

    test_client.get_events("2025-10-17", "2025-10-23")

    test_client.get_meetings_recap_by_timerage("2025-10-10", "2025-10-17")

    test_client.get_large_meetings_recap_by_timerage("2025-10-01", "2025-10-21", 100)

    test_client.get_meetings_recap_by_calluid("040000008200E00074C5B7101A82E00807E90A11A3C559083930DC010000000000000000100000003CAA668A0EB86D4D9E855DC8DC887F9C")

    test_client.get_meeting_most_recent_transcripts(500)