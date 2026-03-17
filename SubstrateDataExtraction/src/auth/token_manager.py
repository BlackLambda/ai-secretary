"""
Token Manager - Simple token generation with optional caching using MSAL.
"""

import json
import os
import jwt
import msal
from datetime import datetime, timezone
from typing import Tuple, Optional
from pathlib import Path


class TokenManager:
    """
    Simple token generation with optional file-based caching using MSAL interactive authentication.

    Features:
    - MSAL PublicClientApplication with broker support
    - Optional file-based cache
    - Manual cache clearing
    - Interactive authentication when needed
    """

    _cache_dir = "token_cache"
    _cache_file = "token.json"

    # MSAL Configuration
    _config = {
        "client_id": "d3590ed6-52b3-4102-aeff-aad2292ab01c",
        "authority": "https://login.microsoftonline.com/common",
        "scope": ["https://outlook.office365.com/.default"],
    }

    def __init__(self, use_cache: bool = True):
        """
        Initialize token manager.

        Args:
            use_cache: If True, cache token to disk (default: True)
        """
        self.use_cache = use_cache
        self._token = None
        self._upn = None
        
        # Initialize MSAL app
        self._msal_app = msal.PublicClientApplication(
            self._config["client_id"],
            authority=self._config["authority"],
            enable_broker_on_windows=True,
        )

    def get_token(self, force_refresh: bool = False) -> Tuple[str, str]:
        """
        Get authentication token with automatic expiration checking.

        Args:
            force_refresh: Force token refresh even if not expired (default: False)

        Returns:
            Tuple[str, str]: (token, upn)
        """
        # Check if we need to refresh
        if force_refresh:
            print("[Token] Force refresh requested")
            self._token = None
            self._upn = None
        elif self._token and not self._is_token_expired(self._token):
            # In-memory token is still valid
            print(f"[Cache] Using in-memory token for {self._upn}")
            return self._token, self._upn
        elif self.use_cache:
            # Try to load from cache
            cached = self._load_from_cache()
            if cached:
                cached_token, cached_upn = cached
                if not self._is_token_expired(cached_token):
                    self._token, self._upn = cached_token, cached_upn
                    print(f"[Cache] Using cached token for {self._upn}")
                    return self._token, self._upn
                else:
                    print("[Cache] Cached token has expired")

        # Generate new token using MSAL
        print("[Token] Generating new token...")
        result = self._get_access_token()
        
        if "access_token" in result:
            self._token = result['access_token']
            # Extract UPN from token claims or use account info
            accounts = self._msal_app.get_accounts()
            self._upn = accounts[0]['username'] if accounts else "unknown"
            print(f"[Token] Token generated for {self._upn}")
            
            # Save to cache if enabled
            if self.use_cache:
                self._save_to_cache(self._token, self._upn)
            
            return self._token, self._upn
        else:
            error_msg = result.get('error', 'Unknown error')
            error_desc = result.get('error_description', 'No description')
            raise Exception(f"Authentication failed: {error_msg} - {error_desc}")

    def _get_access_token(self):
        """
        Acquire access token using interactive authentication.
        Checks cache first, then prompts for login if needed.
        """
        result = None
        
        # Check if account exists in cache
        accounts = self._msal_app.get_accounts(username=self._config.get("username"))
        if accounts:
            print("Account(s) already signed in:")
            for a in accounts:
                print(f"  - {a['username']}")
            chosen = accounts[0]
            print(f"Proceeding with account: {chosen['username']}")
            result = self._msal_app.acquire_token_silent(self._config["scope"], account=chosen)
        
        if not result:
            print("No suitable token exists in cache. Getting a new one from AAD.")
            print("A local browser window will open for you to sign in. CTRL+C to cancel.")
            result = self._msal_app.acquire_token_interactive(
                self._config["scope"],
                login_hint=self._config.get("username"),
                parent_window_handle=self._msal_app.CONSOLE_WINDOW_HANDLE
            )
        
        return result

    def clear_cache(self):
        """Clear cached token (forces fresh token on next call)."""
        self._token = None
        self._upn = None

        cache_path = self._get_cache_path()
        if os.path.exists(cache_path):
            os.remove(cache_path)
            print("[Cache] Token cache cleared")

    def _get_cache_path(self) -> str:
        """Get full path to cache file."""
        base_dir = Path(__file__).parent.parent.parent
        cache_dir = base_dir / self._cache_dir
        return str(cache_dir / self._cache_file)

    def _load_from_cache(self) -> Tuple[str, str] or None:
        """
        Load token from cache file.

        Returns:
            Tuple[str, str] or None: (token, upn) if cache exists, None otherwise
        """
        cache_path = self._get_cache_path()

        if not os.path.exists(cache_path):
            return None

        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            token = data.get('token')
            upn = data.get('upn')

            if token and upn:
                return token, upn

            return None

        except Exception as e:
            print(f"[WARN] Failed to load cache: {e}")
            return None

    def _save_to_cache(self, token: str, upn: str):
        """
        Save token to cache file.

        Args:
            token: Authentication token
            upn: User principal name
        """
        cache_path = self._get_cache_path()

        try:
            # Ensure cache directory exists
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)

            data = {
                "token": token,
                "upn": upn,
                "cached_at": datetime.now().isoformat()
            }

            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)

            print(f"[Cache] Token cached to {cache_path}")

        except Exception as e:
            print(f"[WARN] Failed to save cache: {e}")

    def _is_token_expired(self, token: str, buffer_seconds: int = 300) -> bool:
        """
        Check if a JWT token is expired or will expire soon.

        Args:
            token: JWT token string
            buffer_seconds: Consider token expired if it expires within this many seconds (default: 300 = 5 minutes)

        Returns:
            bool: True if token is expired or expiring soon, False otherwise
        """
        try:
            # Decode JWT without verification (we just need to read the expiration)
            decoded = jwt.decode(token, options={"verify_signature": False})

            # Get expiration timestamp
            exp = decoded.get('exp')
            if not exp:
                print("[WARN] Token does not contain expiration claim, treating as expired")
                return True

            # Convert to datetime
            exp_time = datetime.fromtimestamp(exp, tz=timezone.utc)
            current_time = datetime.now(timezone.utc)

            # Calculate time until expiration
            time_until_expiry = (exp_time - current_time).total_seconds()

            if time_until_expiry <= 0:
                print(f"[Token] Token expired {-time_until_expiry:.0f} seconds ago")
                return True
            elif time_until_expiry <= buffer_seconds:
                print(f"[Token] Token expiring in {time_until_expiry:.0f} seconds (within buffer of {buffer_seconds}s)")
                return True
            else:
                print(f"[Token] Token valid for {time_until_expiry:.0f} seconds")
                return False

        except jwt.DecodeError as e:
            print(f"[WARN] Failed to decode JWT: {e}, treating as expired")
            return True
        except Exception as e:
            print(f"[WARN] Error checking token expiration: {e}, treating as expired")
            return True
