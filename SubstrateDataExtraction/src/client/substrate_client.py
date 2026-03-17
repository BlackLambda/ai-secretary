"""
Substrate Client - HTTP client that injects authentication tokens.
"""

import requests
from typing import Dict, Any, Optional

from src.auth.token_manager import TokenManager


class SubstrateClient:
    """
    HTTP client for Substrate APIs with automatic token injection.

    Features:
    - Automatic token management
    - Simple GET/POST methods
    - Returns JSON data as dict
    """

    def __init__(self, use_cache: bool = True):
        """
        Initialize Substrate client.

        Args:
            use_cache: Enable token caching (default: True)
        """
        self.token_manager = TokenManager(use_cache=use_cache)
        self._token = None
        self._upn = None

        # Get token once during initialization
        self._refresh_token()

    def _refresh_token(self, force: bool = False):
        """
        Get fresh token from token manager.

        Args:
            force: Force token refresh even if not expired
        """
        self._token, self._upn = self.token_manager.get_token(force_refresh=force)

    def _get_headers(self, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """
        Build request headers with authentication token.

        Args:
            extra_headers: Additional headers to include

        Returns:
            Complete headers dictionary
        """
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
            "Prefer": 'outlook.data-source="Substrate",exchange.behavior="ApplicationData,SubstrateFiles,EhamJitProvisioning,OpenComplexTypeExtensions"',
        }

        if extra_headers:
            headers.update(extra_headers)

        return headers
    
    def _get_headers_v2(self, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """
        Build request headers with authentication token.

        Args:
            extra_headers: Additional headers to include

        Returns:
            Complete headers dictionary
        """
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
            "Prefer": 'exchange.behavior="SignalAccessV2,OpenComplexTypeExtensions"',
        }

        if extra_headers:
            headers.update(extra_headers)

        return headers

    def get(self, url: str, version: Optional[str] = "v1", params: Optional[Dict[str, Any]] = None) -> dict:
        """
        Make a GET request to Substrate API with automatic token refresh on 401.

        Args:
            url: Full API URL
            params: Optional URL parameters

        Returns:
            Response JSON as dict

        Raises:
            requests.exceptions.RequestException: If request fails
        """
        print(f"[GET] {url}")

        # First attempt with current token
        try:
            if version == "v1":
                response = requests.get(
                    url,
                    headers=self._get_headers(),
                    params=params
                )
            else:
                response = requests.get(
                    url,
                    headers=self._get_headers_v2(),
                    params=params
                )
            response.raise_for_status()

            data = response.json()

            # Log summary
            if 'value' in data:
                print(f"[OK] Retrieved {len(data['value'])} items")
            else:
                print(f"[OK] Request successful")

            return data

        except requests.exceptions.HTTPError as e:
            # If 401 Unauthorized, refresh token and retry once
            if e.response.status_code == 401:
                print(f"[WARN] 401 Unauthorized - refreshing token and retrying...")
                self._refresh_token(force=True)

                # Retry with new token
                try:
                    response = requests.get(
                        url,
                        headers=self._get_headers(),
                        params=params
                    )
                    response.raise_for_status()

                    data = response.json()

                    # Log summary
                    if 'value' in data:
                        print(f"[OK] Retrieved {len(data['value'])} items (after token refresh)")
                    else:
                        print(f"[OK] Request successful (after token refresh)")

                    return data

                except requests.exceptions.RequestException as retry_error:
                    print(f"[ERROR] GET request failed after token refresh: {retry_error}")
                    raise
            else:
                print(f"[ERROR] GET request failed: {e}")
                raise
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] GET request failed: {e}")
            raise

    def post(self, url: str, json: Dict[str, Any], extra_headers: Optional[Dict[str, str]] = None) -> dict:
        """
        Make a POST request to Substrate API with automatic token refresh on 401.

        Args:
            url: Full API URL
            json: Request body (will be JSON-encoded)
            extra_headers: Additional headers (e.g., X-AnchorMailbox)

        Returns:
            Response JSON as dict

        Raises:
            requests.exceptions.RequestException: If request fails
        """
        print(f"[POST] {url}")

        # First attempt with current token
        try:
            response = requests.post(
                url,
                headers=self._get_headers(extra_headers),
                json=json
            )
            response.raise_for_status()

            data = response.json()
            print(f"[OK] Request successful")

            return data

        except requests.exceptions.HTTPError as e:
            # If 401 Unauthorized, refresh token and retry once
            if e.response.status_code == 401:
                print(f"[WARN] 401 Unauthorized - refreshing token and retrying...")
                #self._refresh_token(force=True)

                # Retry with new token
                try:
                    response = requests.post(
                        url,
                        headers=self._get_headers(extra_headers),
                        json=json
                    )
                    response.raise_for_status()

                    data = response.json()
                    print(f"[OK] Request successful (after token refresh)")

                    return data

                except requests.exceptions.RequestException as retry_error:
                    print(f"[ERROR] POST request failed after token refresh: {retry_error}")
                    raise
            else:
                print(f"[ERROR] POST request failed: {e}")
                raise
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] POST request failed: {e}")
            raise

    @property
    def upn(self) -> str:
        """Get current user principal name."""
        return self._upn
