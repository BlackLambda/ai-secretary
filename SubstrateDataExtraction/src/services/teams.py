"""
Teams Service - Fetch Teams messages using Substrate API.
"""

from typing import Dict, Optional

from src.client.substrate_client import SubstrateClient
from src.utils.json_writer import save_json


class TeamsService:
    """Service for fetching Microsoft Teams data via Substrate API."""

    def __init__(self):
        """Initialize the service with a Substrate client."""
        self.client = SubstrateClient()

    def export_data_in_date_range(self, start_date: str, end_date: str):

        recieved_teams_messages = self.get_teams_messages_in_time_window(start_date, end_date)

        return {"recieved_teams_messages": recieved_teams_messages}
    

    def get_teams_messages_in_time_window(
        self,
        start_date : str,
        end_date: str,
        top: int = 50
    ) -> dict:
        # Substrate API endpoint for Teams messages
        url = f"https://substrate.office.com/api/beta/me/mailfolders('TeamsMessagesData')/messages?$filter=ReceivedDateTime ge {start_date}T00:00:00Z and ReceivedDateTime lt {end_date}T23:59:00Z&$orderby=ReceivedDateTime desc&$top={top}"

        params = {
            "Accept": "application/json",
            "Prefer": 'exchange.behavior="ApplicationData,SubstrateFiles"'
        }

        # Make request
        response_data = self.client.get(url, params=params)

        # Extract messages
        messages = response_data.get('value', [])

        return messages

    def get_teams_messages(
        self,
        top: int = 50,
        skip: int = 0,
        filter_query: Optional[str] = None,
        orderby: Optional[str] = None
    ) -> dict:
        """
        Fetch Teams messages from the TeamsMessagesData folder.

        This uses the Substrate API endpoint that accesses Teams messages
        stored in the special 'TeamsMessagesData' mail folder.

        Args:
            top: Maximum number of messages to fetch (default: 50)
            skip: Number of messages to skip (for pagination, default: 0)
            filter_query: OData filter expression (e.g., "ReceivedDateTime ge 2025-01-01T00:00:00Z")
            orderby: OData orderby expression (e.g., "ReceivedDateTime desc")

        Returns:
            dict: {
                "count": N,
                "filter": "..." (if applied),
                "orderby": "..." (if applied),
                "messages": [...]
            }

        Saves to: output/teams_messages.json

        Examples:
            # Filter by date
            service.get_teams_messages(filter_query="ReceivedDateTime ge 2025-01-01T00:00:00Z")

            # Filter by sender
            service.get_teams_messages(filter_query="From/EmailAddress/Address eq 'user@microsoft.com'")

            # Filter unread messages
            service.get_teams_messages(filter_query="IsRead eq false")

            # Order by date (newest first)
            service.get_teams_messages(orderby="ReceivedDateTime desc")
        """
        filter_info = f", filter={filter_query}" if filter_query else ""
        orderby_info = f", orderby={orderby}" if orderby else ""

        print(f"\n{'='*60}")
        print(f"Fetching Teams Messages (top={top}, skip={skip}{filter_info}{orderby_info})")
        print(f"{'='*60}")

        # Substrate API endpoint for Teams messages
        url = f"https://substrate.office.com/api/beta/me/mailfolders('TeamsMessagesData')/messages"

        params = {
            "$top": top,
            "$skip": skip
        }

        # Add optional filters
        if filter_query:
            params["$filter"] = filter_query

        if orderby:
            params["$orderby"] = orderby

        # Make request
        response_data = self.client.get(url, params=params)

        # Extract messages
        messages = response_data.get('value', [])

        # Prepare output
        result = {
            "count": len(messages),
            "skip": skip,
            "messages": messages
        }

        if filter_query:
            result["filter"] = filter_query

        if orderby:
            result["orderby"] = orderby

        # Save to file
        save_json(result, "teams_messages.json")

        print(f"{'='*60}\n")
        return result

    def get_mail_folders(self) -> dict:
        """
        Fetch available mail folders to explore Teams data locations.

        Returns:
            dict: {
                "count": N,
                "folders": [...]
            }

        Saves to: output/mail_folders.json
        """
        print(f"\n{'='*60}")
        print(f"Fetching Mail Folders")
        print(f"{'='*60}")

        url = "https://substrate.office.com/api/beta/me/mailfolders"

        # Make request
        response_data = self.client.get(url)

        # Extract folders
        folders = response_data.get('value', [])

        # Prepare output
        result = {
            "count": len(folders),
            "folders": folders
        }

        # Save to file
        save_json(result, "mail_folders.json")

        print(f"{'='*60}\n")
        return result
