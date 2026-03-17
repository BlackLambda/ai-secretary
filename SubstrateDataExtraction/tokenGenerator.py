# credit to Long Qiu who is the author of this script

import base64
import binascii
import json
import os
import pathlib
import subprocess
import time
from logging import Logger
from typing import Any, Dict, List, Optional, Tuple

from msal import ConfidentialClientApplication, SerializableTokenCache  # type: ignore

MSIT_TENANT = "72f988bf-86f1-41af-91ab-2d7cd011db47"
AUTHORITY_BASE = "https://login.microsoftonline.com/"
MSIT_AUTHORITY = AUTHORITY_BASE + MSIT_TENANT
RESOURCE_URL = "https://outlook.office365.com/"


SCOPE = ["https://substrate.office.com/search/"]

CACHE_DIR = "token_cache"


def create_token_generator_with_retries(
    max_retries: int, delay_between_retries: int, logger: Logger
):
    for attempt in range(1, max_retries + 1):
        try:
            tokenGenerator = TokenGenerator()
            return tokenGenerator  # Success
        except Exception as e:
            logger.info(f"Attempt {attempt} failed with error: {e}")
            if attempt == max_retries:
                logger.info("Max retries reached. Unable to create TokenGenerator.")
                raise  # Re-raise the last exception
            else:
                logger.info(f"Retrying in {delay_between_retries} seconds...")
                time.sleep(delay_between_retries)


class TokenGenerator:
    _instance = None

    aad_clientid: str
    aad_authority: str
    aad_cert: Dict[str, Any]

    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._instance, cls):
            cls._instance = super(TokenGenerator, cls).__new__(cls, *args, **kwargs)
        return cls._instance


    def get_token(self, force: bool = False) -> str:
        # Get path to store token cache
        auth_path = pathlib.Path(__file__).parent.resolve().as_posix()
        type_hash = str(binascii.crc32(":".join("LLM_DEV").encode("utf-8")))[-5:]
        cache_path = os.path.join(auth_path, CACHE_DIR, f".{type_hash}.bin")

        # if force is true, delete the cache file
        if force and os.path.exists(cache_path):
            os.remove(cache_path)

        # Initialize MSAL SerializableTokenCache
        cache = SerializableTokenCache()

        # check if cache file exists and load it
        if os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8") as file:
                cache.deserialize(file.read())

        token = self.get_app_token(
            scopes=SCOPE,
            client_id=self.aad_clientid,
            authority=self.aad_authority,
            credential=self.aad_cert,
            cache=cache,
        )

        # Write the cache to disk
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        open(cache_path, "w", encoding="utf-8").write(cache.serialize())
        return token

    def get_ews_token(self) -> str:
        ps_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "get_token.ps1"
        )
        command = [
            "powershell",
            "-Command",
            '& {. "%s"; %s -ServiceUri:"%s"}'
            % (ps_script_path, "Get-UserToken-Text", RESOURCE_URL),
        ]
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        token_bytes, errors_bytes = process.communicate()

        token_result = None
        if token_bytes:
            token_str = token_bytes.decode("utf-8")
            try:
                token_result = json.loads(token_str)
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON: {e}")
                print(f"Raw output: {token_str}")

        errors = None
        if errors_bytes:
            errors = errors_bytes.decode("utf-8")
            print(errors)

        if (
            token_result is None
            or "token" not in token_result
            or "expires_on" not in token_result
            or "upn" not in token_result
        ):
            raise Exception(errors)

        return token_result["token"], token_result["upn"]

    def get_app_token(
        self,
        scopes: List[str],
        client_id: str,
        authority: str,
        credential: Any,
        cache: Any = None,
    ) -> str:
        app = ConfidentialClientApplication(
            client_id=client_id,
            authority=authority,
            client_credential=credential,
            token_cache=cache,
        )

        accounts = app.get_accounts()
        result = None

        if accounts:
            result = app.acquire_token_silent(scopes, account=accounts[0])

        if not result:
            result = app.acquire_token_for_client(scopes=scopes)

        return result["access_token"]



if __name__ == "__main__":
    #get ews token
    tokenGenerator = TokenGenerator()
    token, email_address = tokenGenerator.get_ews_token()
    print(f"Token: {token}, Email Address: {email_address}")
