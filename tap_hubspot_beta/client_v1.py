"""REST client handling, including hubspotStream base class."""

from typing import Any, Dict, Optional, Iterable

import requests
from hotglue_singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hubspot_beta.client_base import hubspotStream
from tap_hubspot_beta.utils import merge_responses
import copy
import urllib


class hubspotV1Stream(hubspotStream):
    """hubspot stream class."""

    page_size = 100
    properties_param = "property"

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        response_json = response.json()
        if isinstance(response_json, list):
            return None
        if "has-more" not in response_json and "hasMore" not in response_json:
            items = len(
                list(extract_jsonpath(self.records_jsonpath, input=response.json()))
            )
            if items == self.page_size:
                previous_token = (
                    0 if not previous_token else previous_token.get("offset")
                )
                offset = self.page_size + previous_token
                return dict(offset=offset)
        if response_json.get("has-more") or response_json.get("hasMore"):
            offset = response_json.get("offset")
            vid_offset = response_json.get("vid-offset")
            if offset:
                return dict(offset=offset)
            elif vid_offset:
                return dict(vidOffset=vid_offset)
        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["count"] = self.page_size
        if next_page_token:
            params.update(next_page_token)
        params.update(self.additional_params)
        params[self.properties_param] = self.selected_properties
        return params


    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.

        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            # only use fullsync_deals in the first sync
            if self.name == "fullsync_deals" and not self.is_first_sync():
                finished = True
                yield from []
                break
            resp = decorated_request(prepared_request, context)
            # NOOP

            parsed_response = list(self.parse_response(resp))
            # fetch associations for all records in the response
            parsed_response = self.get_associations_data(parsed_response)

            yield from parsed_response
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token


class hubspotV1SplitUrlStream(hubspotV1Stream):

    def get_params_from_url(self, url):
        parsed_url = urllib.parse.urlparse(url)
        return urllib.parse.parse_qs(parsed_url.query)

    def split_request_generator(self, prepared_request: requests.PreparedRequest, context: Optional[dict]):
        MAX_LEN_URL = 9500 - max(len(prop) for prop in self.selected_properties)
        url = self.get_url(context)
        fixed_params = self.get_params_from_url(prepared_request.url)
        params = copy.deepcopy(fixed_params)

        params_properties = params.pop(self.properties_param, None)
        params[self.properties_param] = []
        for prop in params_properties:
            params[self.properties_param].append(prop)
            prepared_request.prepare_url(url, params)
            if len(prepared_request.url) >= MAX_LEN_URL:
                yield prepared_request
                params = copy.deepcopy(fixed_params)
                params[self.properties_param] = []

        # yield last split request, or the normal request if it's less than the MAX_LEN_URL
        yield prepared_request


    def _request(
        self, prepared_request: requests.PreparedRequest, context: Optional[dict]
    ) -> requests.Response:

        decorated_request = self.request_decorator(super()._request)

        authenticator = self.authenticator
        if authenticator:
            prepared_request.headers.update(authenticator.auth_headers or {})

        MAX_LEN_URL = 3000
        if len(prepared_request.url) > MAX_LEN_URL:
            responses = []
            for req in self.split_request_generator(prepared_request, context):
                responses.append(decorated_request(req, context))
            return merge_responses(responses, self.merge_pk, self.records_jsonpath)
        return decorated_request(prepared_request, context)