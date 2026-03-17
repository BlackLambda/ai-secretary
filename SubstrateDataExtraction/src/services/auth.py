"""
Teams Service - Fetch Teams messages using Substrate API.
"""

from typing import Dict, Optional

from src.client.substrate_client import SubstrateClient
from src.utils.json_writer import save_json


class AuthService:
    """Service for fetching Microsoft Teams data via Substrate API."""

    def __init__(self):
        """Initialize the service with a Substrate client."""
        self.client = SubstrateClient()

    def refresh_token(self):
        self.client._refresh_token(True)

        return self.client._token