"""
Collaborators Service - Fetch top collaborators from EntityServe.
"""

from typing import Dict, List

from src.client.substrate_client import SubstrateClient
from src.utils.json_writer import save_json


class CollaboratorsService:
    """Service for fetching collaborator data."""

    def __init__(self):
        """Initialize the service with a Substrate client."""
        self.client = SubstrateClient()

    def get_top_collaborators(self, top_n: int = 10) -> dict:
        """
        Fetch top N collaborators from EntityServe API.

        Args:
            top_n: Number of top collaborators to fetch (default: 10)

        Returns:
            dict: {
                "count": int,
                "collaborators": [
                    {"alias": "...", "email": "...", "upn": "..."},
                    ...
                ]
            }

        Note: Caller is responsible for saving to desired location.
        """
        print(f"\n{'='*60}")
        print(f"Fetching Top {top_n} Collaborators")
        print(f"{'='*60}")

        url = "https://substrate.office.com/entityserve/api/search?"

        # Extra headers required for EntityServe
        extra_headers = {
            "X-AnchorMailbox": f"UPN:{self.client.upn}",
            "X-DebugMode": "True",
            "X-Flights": "",
            "x-partnername": "esexplorer",
            "X-ScenarioTag": "ES_Explorer",
            "Request-Context": "appId=cid-v1:dd0afc98-a611-46a1-b9f6-64621c18164e",
        }

        # Request body for EntityServe People query
        body = {
            "Query": {
                "QueryType": "None",
                "MaxResults": 0
            },
            "EntityRequests": [
                {
                    "EntityType": "People",
                    "ModifiedQuery": {
                        "QueryType": "None",
                        "MaxResults": top_n
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
                    "ConfigurationOptions": {
                        "SlowInfixMatchType": "Infix"
                    }
                }
            ],
            "MailboxInformation": {
                "MailboxType": "Unknown",
                "UserType": "Unknown"
            },
            "RequestMetadata": {
                "CorrelationId": "bb7c1b2c-f8b6-4602-b7de-52827aab4b52",
                "ClientRequestId": "edc23542-c92e-47b1-a70c-4cc1db3dbac8",
                "CorrelationVector": "mock_cV",
                "ScenarioTag": "ES_Explorer",
                "SharePointContext": {},
                "GuardlistMatches": {
                    "LowestMatchedTermType": "Unknown"
                }
            }
        }

        # Make request
        response_data = self.client.post(url, json=body, extra_headers=extra_headers)

        # Parse response
        collaborators = self._parse_collaborators(response_data)

        # Prepare output
        result = {
            "count": len(collaborators),
            "collaborators": collaborators
        }

        # Note: Saving is handled by the caller to allow flexibility in file location

        print(f"{'='*60}\n")
        return result

    def _parse_collaborators(self, response_data: dict) -> List[Dict]:
        """
        Parse EntityServe response to extract collaborator information.

        Args:
            response_data: Raw API response

        Returns:
            List of collaborator dicts
        """
        collaborators = []

        try:
            entities = response_data['EntityResults'][0]['Entities']
            print(f"[Parse] Found {len(entities)} collaborators")

            for entity in entities:
                alias = entity.get('Alias', '')
                email_addresses = entity.get('EmailAddresses', [])
                email = email_addresses[0] if email_addresses else ''
                upn = entity.get('UPN', '')

                collaborators.append({
                    "alias": alias,
                    "email": email,
                    "upn": upn
                })

                print(f"  - {alias}: {email}")

        except (KeyError, IndexError) as e:
            print(f"[ERROR] Failed to parse response: {e}")
            raise

        return collaborators
