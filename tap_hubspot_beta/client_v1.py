"""REST client handling, including hubspotStream base class."""

import logging
import pendulum
from datetime import datetime
from typing import Any, Dict, Optional, Iterable

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hubspot_beta.client_base import hubspotStream
from tap_hubspot_beta.utils import merge_responses
import copy
import urllib
import backoff
from singer_sdk.exceptions import RetriableAPIError
import copy


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

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        if self.properties_url:
            if row.get("properties"):
                for name, value in row.get("properties", {}).items():
                    row[name] = value.get("value")
                del row["properties"]
        for field in self.datetime_fields:
            if row.get(field) is not None:
                if row.get(field) in [0, ""]:
                    row[field] = None
                else:
                    try:
                        row[field] = pendulum.parse(row[field])
                    except Exception:
                        dt_field = datetime.fromtimestamp(int(row[field]) / 1000)
                        row[field] = dt_field.replace(tzinfo=None)
        row = self.process_row_types(row)
        return row

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
            yield from self.parse_response(resp)
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

    # the response validation happens in _handle_request, having backoff in _request as well hides errors
    def backoff_max_tries(self) -> int:
        return 1

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
        

    @backoff.on_exception(backoff.expo, RetriableAPIError, max_tries=7, max_value=320, base=2, factor=10)
    def _handle_request(self, prepared_request: requests.PreparedRequest, context: Optional[dict]) -> requests.Response:
        response = self.requests_session.send(prepared_request, timeout=self.timeout)
        if self._LOG_REQUEST_METRICS:
            extra_tags = {}
            if self._LOG_REQUEST_METRIC_URLS:
                extra_tags["url"] = prepared_request.path_url
            self._write_request_duration_log(
                endpoint=self.path,
                response=response,
                context=context,
                extra_tags=extra_tags,
            )
        self.validate_response(response)
        self.logger.debug("Response received successfully.")
        return response

    def _request(
        self, prepared_request: requests.PreparedRequest, context: Optional[dict]
    ) -> requests.Response:

        authenticator = self.authenticator
        if authenticator:
            prepared_request.headers.update(authenticator.auth_headers or {})

        MAX_LEN_URL = 3000
        if len(prepared_request.url) > MAX_LEN_URL:
            responses = []
            for req in self.split_request_generator(prepared_request, context):
                responses.append(self._handle_request(req, context))
            return merge_responses(responses, self.merge_pk, self.records_jsonpath)
        return self._handle_request(prepared_request, context)
