#encoding=utf8
import requests
from typing import Iterable, Optional
from src.client.substrate_client import SubstrateClient


def get_signals(
    token: str,
    signal_types: Iterable[str],
    since_utc_iso: Optional[str] = None,   # e.g., "2025-10-01T00:00:00Z",
    end_utc_iso: Optional[str] = None,   # e.g., "2025-10-01T00:00:00Z",
    top: int = 200,
    fields: Optional[Iterable[str]] = None,
    host: str = "https://substrate.office.com",  # or "https://substrate-sdf.office.com"
    api_version: str = "v2.0",                  # try "beta" first; switch to "v2.0" if available
    timeout_sec: int = 30
):
    """
    Fetch SIGS from Substrate with proper headers and paging.
    Adds: exchange.behavior = SignalAccessV2,OpenComplexTypeExtensions
    """

    base = f"{host}/api/{api_version}/me/signals"

    end_utc_iso = "%sT23:59:59.999Z" % end_utc_iso
    since_utc_iso = "%sT00:00:00.000Z" % since_utc_iso

    # Build the $filter
    types_filter = " or ".join([f"SignalType eq '{t}'" for t in signal_types])
    filt = f"({types_filter})"
    if since_utc_iso:
        # StartTime comparison against UTC ISO 'Z'
        filt += f" and StartTime ge {since_utc_iso} and EndTime le {end_utc_iso}"

    params = {
        "$filter": filt,
        "$orderby": "StartTime desc",
        "$top": str(top)
    }

    if fields:
        params["$select"] = ",".join(fields)
    else:
        params["$select"] = "SignalType,StartTime,EndTime,Item,Actor,CustomProperties"

    # Required headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        # 👇 Added per your request
        "Prefer": 'exchange.behavior="SignalAccessV2,OpenComplexTypeExtensions"'
    }

    items = []
    url = base
    while True:
        r = requests.get(url, headers=headers, params=params, timeout=timeout_sec)
        # For debugging: show helpful server response if not 2xx
        try:
            r.raise_for_status()
        except requests.HTTPError:
            print("== Request failed ==")
            print("URL:", r.url)
            print("Status:", r.status_code)
            print("Body:", r.text[:1000])
            raise

        data = r.json()
        batch = data.get("value", [])
        items.extend(batch)

        # Follow @odata.nextLink if present (paging)
        next_link = data.get("@odata.nextLink")
        if not next_link:
            break
        url, params = next_link, None  # nextLink already includes paging token

    return items

class SignalServices:

    def __init__(self, token = ""):
        """Initialize the service with a Substrate client."""
        self.client = SubstrateClient()
        self.client._token = token

        self.signal_types = ["FileAccessed", "FileModified", "CommentCreated", "AddedToSharedWithMe", "JoinCall", "LeaveCall"]

    def __len__(self):
        return 1
    
    def export_data_in_date_range(self, start_date: str, end_date: str, top: int = 500):

        activities = self.get_signal(signal_types = self.signal_types, since_utc_iso=start_date, end_utc_iso=end_date)

        # Combine and save all data
        combined_data = {
            "activities": activities
        }

        return combined_data

    def get_signal(self,
                    signal_types: Iterable[str],
                    since_utc_iso: str,   # e.g., "2025-10-01",
                    end_utc_iso: str,   # e.g., "2025-10-01",
                    top: int = 200,
                    fields: Optional[Iterable[str]] = None,
                    host: str = "https://substrate.office.com",  # or "https://substrate-sdf.office.com"
                    api_version: str = "v2.0",                  # try "beta" first; switch to "v2.0" if available
                    timeout_sec: int = 30):
        
        return get_signals(self.client._token, signal_types, since_utc_iso, end_utc_iso, top, fields, host, api_version, timeout_sec)

# ---------- Example usages ----------

# 1) JoinCall signals since Oct 1, 2025 on PROD, beta
# NOTE: Keep 'Z' (UTC) in the since_utc_iso timestamp.
if __name__ == "__main__":
    token = ""
    signals = get_signals(
        token=token,
        signal_types=["JoinCall"],
        since_utc_iso="2025-10-01T00:00:00Z",
        end_utc_iso="2025-10-20T00:00:00Z",
        top=200,
        host="https://substrate.office.com",
        api_version="v2.0"    # if you get a segment error, try beta first
    )
    print("Fetched:", len(signals))
    if signals:
        print("Sample:", {k: signals[0].get(k) for k in ["SignalType", "StartTime", "EndTime"]})
