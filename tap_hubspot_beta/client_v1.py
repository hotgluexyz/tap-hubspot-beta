"""REST client handling, including hubspotStream base class."""

import logging
import pendulum
from datetime import datetime
from typing import Any, Dict, Optional
import copy
import urllib
import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_hubspot_beta.utils import merge_responses
from tap_hubspot_beta.client_base import hubspotStream
import backoff
from singer_sdk.exceptions import RetriableAPIError

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
        params.update(self.additional_prarams)
        params[self.properties_param] = self.selected_properties
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        super().post_process(row, context)
        if self.properties_url:
            if row.get("properties"):
                for name, value in row.get("properties", {}).items():
                    row[name] = self.parse_value(name, value.get("value"))
                del row["properties"]
        for field in self.datetime_fields:
            if row.get(field) is not None:
                if row.get(field) in [0, ""]:
                    row[field] = None
                else:
                    try:
                        dt_field = pendulum.parse(row[field])
                        row[field] = dt_field.isoformat()
                    except Exception:
                        dt_field = datetime.fromtimestamp(int(row[field]) / 1000)
                        dt_field = dt_field.replace(tzinfo=None)
                        row[field] = dt_field.isoformat()
        return row



class hubspotV1SplitUrlStream(hubspotV1Stream):
    properties_param = "properties"
    # the response validation happens in _handle_request, having backoff in _request as well hides errors
    def backoff_max_tries(self) -> int:
        return 1
    
    @property
    def merge_pk(self):
        raise NotImplementedError(f"Merge PK not specified for SplitURLStream: {self.name}")

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
