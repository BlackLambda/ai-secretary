"""
Email Service - Fetch email messages using Substrate API.
"""

from typing import Dict, Optional

from src.client.substrate_client import SubstrateClient
from src.utils.json_writer import save_json


class EmailService:
    """Service for fetching Microsoft Outlook email data via Substrate API."""

    def __init__(self):
        """Initialize the service with a Substrate client."""
        self.client = SubstrateClient()

    def export_data_in_date_range(self, start_date: str, end_date: str):

        recieved_emails = self.get_emails_within_time_window(start_date, end_date)

        return {"recieved_emails": recieved_emails}

    def get_emails_within_time_window(
        self,
        start_date : str,
        end_date: str,
        top: int = 50
    ) -> dict:
        # Substrate API endpoint for emails (v2.0)
        url = f"https://substrate.office.com/api/v2.0/me/messages?$filter=ReceivedDateTime ge {start_date}T00:00:00Z and ReceivedDateTime lt {end_date}T23:59:00Z&$orderby=ReceivedDateTime desc&$top={top}"

        params = {
            "Prefer": 'exchange.behavior="ApplicationData,SubstrateFiles"',
            "Accept": "application/json"
        }

        # Make request
        response_data = self.client.get(url, params = params)

        # Extract emails
        emails = response_data.get('value', [])

        return emails


    def get_emails(
        self,
        top: int = 50,
        skip: int = 0,
        filter_query: Optional[str] = None,
        orderby: Optional[str] = None,
        select: Optional[str] = None
    ) -> dict:
        """
        Fetch email messages from Outlook.

        This uses the Substrate API v2.0 endpoint that accesses Outlook emails.

        Args:
            top: Maximum number of emails to fetch (default: 50)
            skip: Number of emails to skip (for pagination, default: 0)
            filter_query: OData filter expression (e.g., "ReceivedDateTime ge 2025-01-01T00:00:00Z")
            orderby: OData orderby expression (e.g., "ReceivedDateTime desc")
            select: OData select expression to limit fields returned

        Returns:
            dict: {
                "count": N,
                "filter": "..." (if applied),
                "orderby": "..." (if applied),
                "emails": [...]
            }

        Saves to: output/emails.json

        Examples:
            # Get recent emails
            service.get_emails(top=100)

            # Filter by date
            service.get_emails(filter_query="ReceivedDateTime ge 2025-09-01T00:00:00Z")

            # Filter by sender
            service.get_emails(filter_query="From/EmailAddress/Address eq 'sender@example.com'")

            # Filter unread emails
            service.get_emails(filter_query="IsRead eq false")

            # Filter by subject (contains)
            service.get_emails(filter_query="contains(Subject, 'important')")

            # Order by date (newest first)
            service.get_emails(orderby="ReceivedDateTime desc")

            # Select specific fields only
            service.get_emails(select="Subject,From,ReceivedDateTime,IsRead")

            # Filter by folder (Inbox, SentItems, etc.)
            service.get_emails(filter_query="ParentFolderId eq 'inbox'")
        """
        filter_info = f", filter={filter_query}" if filter_query else ""
        orderby_info = f", orderby={orderby}" if orderby else ""
        select_info = f", select={select}" if select else ""

        print(f"\n{'='*60}")
        print(f"Fetching Emails (top={top}, skip={skip}{filter_info}{orderby_info}{select_info})")
        print(f"{'='*60}")

        # Substrate API endpoint for emails (v2.0)
        url = "https://substrate.office.com/api/v2.0/me/messages"

        params = {
            "$top": top,
            "$skip": skip
        }

        # Add optional filters
        if filter_query:
            params["$filter"] = filter_query

        if orderby:
            params["$orderby"] = orderby

        if select:
            params["$select"] = select

        # Make request
        response_data = self.client.get(url, params=params)

        # Extract emails
        emails = response_data.get('value', [])

        # Prepare output
        result = {
            "count": len(emails),
            "skip": skip,
            "emails": emails
        }

        if filter_query:
            result["filter"] = filter_query

        if orderby:
            result["orderby"] = orderby

        if select:
            result["select"] = select

        # Save to file
        save_json(result, "emails.json")

        print(f"{'='*60}\n")
        return result

    def get_email_by_id(self, email_id: str) -> dict:
        """
        Fetch a specific email by its ID.

        Args:
            email_id: The email message ID

        Returns:
            dict: Email message data

        Saves to: output/email_details.json
        """
        print(f"\n{'='*60}")
        print(f"Fetching Email by ID: {email_id[:50]}...")
        print(f"{'='*60}")

        url = f"https://substrate.office.com/api/v2.0/me/messages/{email_id}"

        # Make request
        email_data = self.client.get(url)

        # Save to file
        save_json(email_data, "email_details.json")

        print(f"{'='*60}\n")
        return email_data

    def get_inbox_emails(
        self,
        top: int = 50,
        skip: int = 0,
        filter_query: Optional[str] = None,
        orderby: Optional[str] = None
    ) -> dict:
        """
        Fetch emails from Inbox folder specifically.

        Args:
            top: Maximum number of emails to fetch (default: 50)
            skip: Number of emails to skip (default: 0)
            filter_query: OData filter expression
            orderby: OData orderby expression (default: "ReceivedDateTime desc")

        Returns:
            dict: Email data

        Saves to: output/inbox_emails.json
        """
        print(f"\n{'='*60}")
        print(f"Fetching Inbox Emails")
        print(f"{'='*60}")

        url = "https://substrate.office.com/api/v2.0/me/mailFolders/inbox/messages"

        params = {
            "$top": top,
            "$skip": skip
        }

        if filter_query:
            params["$filter"] = filter_query

        if orderby:
            params["$orderby"] = orderby

        # Make request
        response_data = self.client.get(url, params=params)

        # Extract emails
        emails = response_data.get('value', [])

        # Prepare output
        result = {
            "count": len(emails),
            "folder": "inbox",
            "skip": skip,
            "emails": emails
        }

        if filter_query:
            result["filter"] = filter_query

        if orderby:
            result["orderby"] = orderby

        # Save to file
        save_json(result, "inbox_emails.json")

        print(f"{'='*60}\n")
        return result

    def get_sent_emails(
        self,
        top: int = 50,
        skip: int = 0,
        filter_query: Optional[str] = None,
        orderby: Optional[str] = None
    ) -> dict:
        """
        Fetch emails from SentItems folder.

        Args:
            top: Maximum number of emails to fetch (default: 50)
            skip: Number of emails to skip (default: 0)
            filter_query: OData filter expression
            orderby: OData orderby expression

        Returns:
            dict: Email data

        Saves to: output/sent_emails.json
        """
        print(f"\n{'='*60}")
        print(f"Fetching Sent Emails")
        print(f"{'='*60}")

        url = "https://substrate.office.com/api/v2.0/me/mailFolders/sentitems/messages"

        params = {
            "$top": top,
            "$skip": skip
        }

        if filter_query:
            params["$filter"] = filter_query

        if orderby:
            params["$orderby"] = orderby

        # Make request
        response_data = self.client.get(url, params=params)

        # Extract emails
        emails = response_data.get('value', [])

        # Prepare output
        result = {
            "count": len(emails),
            "folder": "sentitems",
            "skip": skip,
            "emails": emails
        }

        if filter_query:
            result["filter"] = filter_query

        if orderby:
            result["orderby"] = orderby

        # Save to file
        save_json(result, "sent_emails.json")

        print(f"{'='*60}\n")
        return result

    def get_mail_folders(self) -> dict:
        """
        Fetch available mail folders.

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

        url = "https://substrate.office.com/api/v2.0/me/mailFolders"

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
