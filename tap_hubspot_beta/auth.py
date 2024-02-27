"""hubspot Authentication."""

import json
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.streams import Stream as RESTStreamBase
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
import backoff


class OAuth2Authenticator(APIAuthenticatorBase):
    """API Authenticator for OAuth 2.0 flows."""

    def __init__(
        self,
        stream: RESTStreamBase,
        config_file: Optional[str] = None,
        auth_endpoint: Optional[str] = None,
    ) -> None:
        super().__init__(stream=stream)
        self._auth_endpoint = auth_endpoint
        self._config_file = config_file
        self._tap = stream._tap

    @property
    def auth_headers(self) -> dict:
        """Return a dictionary of auth headers to be applied.

        These will be merged with any `http_headers` specified in the stream.

        Returns:
            HTTP headers for authentication.
        """
        if not self.is_token_valid():
            self.update_access_token()
        result = super().auth_headers
        result["Authorization"] = f"Bearer {self._tap._config.get('access_token')}"
        return result

    @property
    def auth_endpoint(self) -> str:
        """Get the authorization endpoint.

        Returns:
            The API authorization endpoint if it is set.

        Raises:
            ValueError: If the endpoint is not set.
        """
        if not self._auth_endpoint:
            raise ValueError("Authorization endpoint not set.")
        return self._auth_endpoint

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the hubspot API."""
        return {
            "client_id": self._tap._config["client_id"],
            "client_secret": self._tap._config["client_secret"],
            "redirect_uri": self._tap._config["redirect_uri"],
            "refresh_token": self._tap._config["refresh_token"],
            "grant_type": "refresh_token",
        }

    def is_token_valid(self) -> bool:
        access_token = self._tap._config.get("access_token")
        now = round(datetime.utcnow().timestamp())
        expires_in = self._tap._config.get("expires_in")

        return not bool(
            (not access_token) or (not expires_in) or ((expires_in - now) < 60)
        )

    @property
    def oauth_request_payload(self) -> dict:
        """Get request body.

        Returns:
            A plain (OAuth) or encrypted (JWT) request body.
        """
        return self.oauth_request_body

    @backoff.on_exception(backoff.expo, RetriableAPIError, max_tries=2)
    def request_token(self, endpoint, data):
        token_response = requests.post(endpoint, data)
        if 500 <= token_response.status_code <= 600:
            raise RetriableAPIError(f"Auth error: {token_response.text}")
        elif 400 <= token_response.status_code < 500:
            raise FatalAPIError(f"Auth error: {token_response.text}")
        return token_response

    # Authentication and refresh
    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.

        Raises:
            RuntimeError: When OAuth login fails.
        """
        request_time = round(datetime.utcnow().timestamp())
        auth_request_payload = self.oauth_request_payload
        token_response = self.request_token(self.auth_endpoint, data=auth_request_payload)
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        expires_in = request_time + token_json["expires_in"]

        self._tap._config["access_token"] = token_json["access_token"]
        self._tap._config["expires_in"] = expires_in
        with open(self._tap.config_file, "w") as outfile:
            json.dump(self._tap._config, outfile, indent=4)
