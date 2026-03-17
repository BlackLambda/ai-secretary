"""
Email Service - Fetch email messages using Substrate API.
"""

from typing import Dict, Optional

from src.client.substrate_client import SubstrateClient
from src.utils.json_writer import save_json


class ProfileService:
    """Service for fetching Microsoft Outlook email data via Substrate API."""

    def __init__(self):
        """Initialize the service with a Substrate client."""
        self.client = SubstrateClient()

    def get_profile(
        self,
    ):
        
        url = "https://substrate.office.com/contextb2/api/v1.0/me/contextprompt"

        # Make request
        profile = self.client.get(url)

        # Save to file
        #save_json([profile], "profile.json")

        print(f"{'='*60}\n")
        return {"user_profile": profile}

    def get_extended_profile(self, manager_name: Optional[str] = None) -> Dict:
        """
        Fetch extended profile information by searching for the user in the People API.
        """
        print(f"Fetching extended profile from People API...")
        
        # Search for the current user by their alias or name
        # We use the alias from the UPN
        upn = self.client.upn
        alias = upn.split('@')[0]
        
        # Try searching for the alias first
        url = f"https://substrate.office.com/api/beta/me/people/?$search={alias}"
        
        try:
            response_data = self.client.get(url)
            people = response_data.get('value', [])
            
            # Find the person entry that matches our UPN
            for person in people:
                if person.get('UserPrincipalName', '').lower() == upn.lower():
                    print(f"Found self in People API: {person.get('DisplayName')}")
                    return {
                        "department": person.get('Department'),
                        "officeLocation": person.get('OfficeLocation'),
                        "jobTitle": person.get('Title'),
                        "companyName": person.get('CompanyName'),
                        "phones": person.get('Phones', [])
                    }
            
            print("Could not find self in People API search results.")
            
        except Exception as e:
            print(f"Error fetching extended profile from People API: {e}")
            
        return {}
