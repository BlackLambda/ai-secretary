"""
Files Service - Fetch file and document information using Substrate API.
FIXED VERSION - Uses Substrate API instead of Microsoft Graph API
"""

from typing import Dict, Optional

from src.client.substrate_client import SubstrateClient
from src.utils.json_writer import save_json


class FilesService:
    """Service for fetching file and document data via Substrate API."""

    def __init__(self):
        """Initialize the service with a Substrate client."""
        self.client = SubstrateClient()

    def __len__(self):
        return 1
    
    def export_data_in_date_range(self, start_date: str, end_date: str, top: int = 500):

        recent_files = self.get_recent_wset_files(top)

        # Combine and save all data
        combined_data = {
            "recent_files": recent_files
        }

        return combined_data
    
    def get_recent_wset_files(self, top: int = 100) -> dict:
        """
        Fetch recently used files.

        Args:
            top: Maximum number of files to fetch (default: 100)

        Returns:
            dict: {
                "count": N,
                "files": [...]
            }

        Saves to: output/recent_files.json
        """
        print(f"\n{'='*60}")
        print(f"Fetching Recent Files (top {top})")
        print(f"{'='*60}")

        url = f"https://substrate.office.com/api/beta/me/WorkingSetFiles?$select=FileName,FileContent,SharePointItem,Visualization,ItemProperties/AccessedByMailboxOwner,ItemProperties/RecentActivities&$filter=IsEmptyCopy eq false&$orderby=ItemProperties/AccessedByMailboxOwner/LastAccessDateTime desc&top={top}"
        params = {"Accept": "application/json", "Prefer": 'exchange.behavior="SubstrateFiles"'}

        # Make request
        response_data = self.client.get(url, params=params)

        # Extract files
        files = response_data.get('value', [])

        # Prepare output
        result = {
            "count": len(files),
            "files": files
        }

        # Save to file
        # save_json(result, "recent_files.json")

        print(f"{'='*60}\n")
        return result


    def get_recent_files(self, top: int = 100) -> dict:
        """
        Fetch recently used files using Substrate API.

        Args:
            top: Maximum number of files to fetch (default: 100)

        Returns:
            dict: {
                "count": N,
                "files": [...]
            }

        Saves to: output/recent_files.json
        """
        print(f"\n{'='*60}")
        print(f"Fetching Recent Files (top {top})")
        print(f"{'='*60}")

        # FIXED: Use Substrate API instead of Graph API
        url = f"https://substrate.office.com/api/beta/me/WorkingSetFiles"
        params = {
            "$select": "FileName,FileContent,SharePointItem,Visualization,ItemProperties/AccessedByMailboxOwner,ItemProperties/RecentActivities",
            "$filter": "IsEmptyCopy eq false",
            "$orderby": "ItemProperties/AccessedByMailboxOwner/LastAccessDateTime desc",
            "$top": str(top)
        }

        # Make request
        response_data = self.client.get(url, params=params)

        # Extract files
        files = response_data.get('value', [])

        # Prepare output
        result = {
            "count": len(files),
            "files": files
        }

        # Save to file
        # save_json(result, "recent_files.json")

        print(f"{'='*60}\n")
        return result

    def get_shared_files(self, top: int = 100) -> dict:
        """
        Fetch files that may be shared (external content) using Substrate API.

        Note: Substrate API doesn't have a direct "shared with me" filter,
              so we filter for files that are external (IsExternalContent = true).

        Args:
            top: Maximum number of files to fetch (default: 100)

        Returns:
            dict: {
                "count": N,
                "files": [...]
            }

        Saves to: output/shared_files.json
        """
        print(f"\n{'='*60}")
        print(f"Fetching External/Shared Files (top {top})")
        print(f"{'='*60}")

        # Use Substrate API - filter for external content (shared)
        url = f"https://substrate.office.com/api/beta/me/WorkingSetFiles"
        params = {
            "$select": "FileName,FileContent,SharePointItem,Visualization,ItemProperties/AccessedByMailboxOwner,ItemProperties/RecentActivities",
            "$filter": "IsEmptyCopy eq false and SharePointItem/IsExternalContent eq true",
            "$orderby": "ItemProperties/AccessedByMailboxOwner/LastAccessDateTime desc",
            "$top": str(top)
        }

        # Make request
        response_data = self.client.get(url, params=params)

        # Extract files
        files = response_data.get('value', [])

        # Prepare output
        result = {
            "count": len(files),
            "files": files
        }

        # Save to file
        save_json(result, "shared_files.json")

        print(f"{'='*60}\n")
        return result

    def search_files(self, query: str, top: int = 50) -> dict:
        """
        Search for files by query using Substrate API.

        Args:
            query: Search query string
            top: Maximum number of results (default: 50)

        Returns:
            dict: {
                "count": N,
                "query": "...",
                "files": [...]
            }

        Saves to: output/search_files.json
        """
        print(f"\n{'='*60}")
        print(f"Searching Files: '{query}'")
        print(f"{'='*60}")

        # Use Substrate API search
        url = f"https://substrate.office.com/api/beta/me/WorkingSetFiles"
        params = {
            "$select": "FileName,FileContent,SharePointItem,Visualization,ItemProperties/AccessedByMailboxOwner,ItemProperties/RecentActivities",
            "$filter": f"IsEmptyCopy eq false and contains(FileName,'{query}')",
            "$orderby": "ItemProperties/AccessedByMailboxOwner/LastAccessDateTime desc",
            "$top": str(top)
        }

        # Make request
        response_data = self.client.get(url, params=params)

        # Extract files
        files = response_data.get('value', [])

        # Prepare output
        result = {
            "count": len(files),
            "query": query,
            "files": files
        }

        # Save to file
        save_json(result, "search_files.json")

        print(f"{'='*60}\n")
        return result
