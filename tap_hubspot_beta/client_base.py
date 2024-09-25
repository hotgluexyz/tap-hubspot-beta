"""REST client handling, including hubspotStream base class."""
import copy
import logging

import requests
import backoff
from copy import deepcopy
from typing import Any, Dict, Optional, cast, List
from backports.cached_property import cached_property
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.streams import RESTStream
from urllib3.exceptions import ProtocolError
from singer_sdk.mapper import  SameRecordTransform, StreamMap
from singer_sdk.helpers._flattening import get_flattening_options
import curlify

from pendulum import parse

from tap_hubspot_beta.auth import OAuth2Authenticator

logging.getLogger("backoff").setLevel(logging.CRITICAL)


class hubspotStream(RESTStream):
    """hubspot stream class."""

    url_base = "https://api.hubapi.com/"
    base_properties = []
    additional_prarams = {}
    properties_url = None
    page_size = 100

    stream_metadata = {}
    fields_metadata = {}
    object_type = None
    fields_metadata = {}

    def load_fields_metadata(self):
        if not self.properties_url:
            self.logger.info(f"Skipping fields_meta for {self.name} stream, because there is no properties_url set")
            return

        req = requests.get(
            f"{self.url_base}{self.properties_url}",
            headers = self.authenticator.auth_headers or {},
        )

        if req.status_code != 200:
            self.logger.info(f"Skipping fields_meta for {self.name} stream")
            return

        self.fields_metadata = {v["name"]: v for v in req.json()}

    def _request(
        self, prepared_request: requests.PreparedRequest, context: Optional[dict]
    ) -> requests.Response:

        authenticator = self.authenticator
        if authenticator:
            prepared_request.headers.update(authenticator.auth_headers or {})

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
        logging.debug("Response received successfully.")
        return response

    @cached_property
    def last_job(self):
        if self.tap_state.get("bookmarks"):
            last_job = self.tap_state["bookmarks"].get("last_job")
            if last_job:
                return parse(last_job.get("value"))
        return

    def request_records(self, context):
        """Request records from REST endpoint(s), returning response records."""
        next_page_token = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            logging.getLogger("backoff").setLevel(logging.CRITICAL)
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            resp = decorated_request(prepared_request, context)
            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            finished = not next_page_token

    @property
    def authenticator(self) -> OAuth2Authenticator:
        """Return a new authenticator object."""
        return OAuth2Authenticator(
            self, self._tap.config_file, "https://api.hubapi.com/oauth/v1/token"
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Content-Type"] = "application/json"
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    @cached_property
    def datetime_fields(self):
        datetime_fields = []
        for key, value in self.schema["properties"].items():
            if value.get("format") == "date-time":
                datetime_fields.append(key)
        return datetime_fields

    @cached_property
    def selected_properties(self):
        selected_properties = []
        for key, value in self.metadata.items():
            if isinstance(key, tuple) and len(key) == 2 and value.selected:
                selected_properties.append(key[-1])
        return selected_properties

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if 500 <= response.status_code < 600 or response.status_code in [429, 401, 104]:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
            )
            curl_command = curlify.to_curl(response.request)
            logging.error(f"Response code: {response.status_code}, info: {response.text}")
            logging.error(f"CURL command for failed request: {curl_command}")
            raise RetriableAPIError(f"Msg {msg}, response {response.text}")

        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
            )
            curl_command = curlify.to_curl(response.request)
            logging.error(f"Response code: {response.status_code}, info: {response.text}")
            logging.error(f"CURL command for failed request: {curl_command}")
            raise FatalAPIError(f"Msg {msg}, response {response.text}")

    @staticmethod
    def extract_type(field, type_booleancheckbox_as_boolean=False):
        field_type = field.get("type")
        if field_type == "bool":
            return th.BooleanType
        if field.get("fieldType") == "booleancheckbox" and type_booleancheckbox_as_boolean:
            return th.BooleanType
        if field_type in ["string", "enumeration", "phone_number", "date", "json", "object_coordinates"]:
            return th.StringType
        if field_type == "number":
            return th.StringType
        if field_type == "datetime":
            return th.DateTimeType

        # TODO: Changed default because tap errors if type is None
        return th.StringType

    def request_schema(self, url, headers):
        response = requests.get(url, headers=headers)
        self.validate_response(response)
        return response

    @cached_property
    def schema(self):
        properties = deepcopy(self.base_properties)
        headers = self.http_headers
        headers.update(self.authenticator.auth_headers or {})
        url = self.url_base + self.properties_url
        response = self.request_decorator(self.request_schema)(url, headers=headers)

        fields = response.json()
        for field in fields:
            if not field.get("deleted"):
                property = th.Property(field.get("name"), self.extract_type(field, self.config.get("type_booleancheckbox_as_boolean")))
                properties.append(property)
        return th.PropertiesList(*properties).to_dict()

    def finalize_state_progress_markers(self, state: Optional[dict] = None) -> None:

        def finalize_state_progress_markers(stream_or_partition_state: dict) -> Optional[dict]:
            """Promote or wipe progress markers once sync is complete."""
            signpost_value = stream_or_partition_state.pop("replication_key_signpost", None)
            stream_or_partition_state.pop("starting_replication_value", None)
            if "progress_markers" in stream_or_partition_state:
                if "replication_key" in stream_or_partition_state["progress_markers"]:
                    # Replication keys valid (only) after sync is complete
                    progress_markers = stream_or_partition_state["progress_markers"]
                    stream_or_partition_state["replication_key"] = progress_markers.pop(
                        "replication_key"
                    )
                    new_rk_value = progress_markers.pop("replication_key_value")
                    if signpost_value and new_rk_value > signpost_value:
                        new_rk_value = signpost_value
                    stream_or_partition_state["replication_key_value"] = new_rk_value

            # Wipe and return any markers that have not been promoted
            progress_markers = stream_or_partition_state.pop("progress_markers", {})
            # Remove auto-generated human-readable note:
            progress_markers.pop("Note", None)
            # Return remaining 'progress_markers' if any:
            return progress_markers or None

        if state is None or state == {}:
            for child_stream in self.child_streams or []:
                child_stream.finalize_state_progress_markers()

            if self.tap_state is None:
                raise ValueError("Cannot write state to missing state dictionary.")

            if "bookmarks" not in self.tap_state:
                self.tap_state["bookmarks"] = {}
            if self.name not in self.tap_state["bookmarks"]:
                self.tap_state["bookmarks"][self.name] = {}
            stream_state = cast(dict, self.tap_state["bookmarks"][self.name])
            if "partitions" not in stream_state:
                stream_state["partitions"] = []
            stream_state_partitions: List[dict] = stream_state["partitions"]

            context: Optional[dict]
            for context in self.partitions or [{}]:
                context = context or None

                state_partition_context = self._get_state_partition_context(context)

                if state_partition_context:
                    index, found = next(((i, partition_state) for i, partition_state in enumerate(stream_state_partitions) if partition_state["context"] == state_partition_context), (None, None))
                    if found:
                        state = found
                        del stream_state_partitions[index]
                    else:
                        state = stream_state_partitions.append({"context": state_partition_context})
                else:
                    state = self.stream_state
                finalize_state_progress_markers(state)
            return
        finalize_state_progress_markers(state)

    def request_decorator(self, func):
        """Instantiate a decorator for handling request failures."""
        decorator = backoff.on_exception(
            self.backoff_wait_generator,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                ProtocolError
            ),
            max_tries=self.backoff_max_tries,
            on_backoff=self.backoff_handler,
        )(func)
        return decorator
    
    @property
    def stream_maps(self) -> List[StreamMap]:
        """Get stream transformation maps.

        The 0th item is the primary stream map. List should not be empty.

        Returns:
            A list of one or more map transformations for this stream.
        """
        if self._stream_maps:
            return self._stream_maps

        if self._tap.mapper:
            #Append deals association stream if it is not in the catalog. 
            if self.name == "deals_association_parent" and self.name not in self._tap.mapper.stream_maps:
                self._tap.mapper.stream_maps.update({"deals_association_parent":self._tap.mapper.stream_maps["deals"]})
                self._tap.mapper.stream_maps["deals_association_parent"][0].stream_alias = "deals_association_parent"
            self._stream_maps = self._tap.mapper.stream_maps[self.name]
            self.logger.info(
                f"Tap has custom mapper. Using {len(self.stream_maps)} provided map(s)."
            )
        else:
            self.logger.info(
                f"No custom mapper provided for '{self.name}'. "
                "Using SameRecordTransform."
            )
            self._stream_maps = [
                SameRecordTransform(
                    stream_alias=self.name,
                    raw_schema=self.schema,
                    key_properties=self.primary_keys,
                    flattening_options=get_flattening_options(self.config),
                )
            ]
        return self._stream_maps


class hubspotStreamSchema(hubspotStream):

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        response_json = response.json()
        if response_json.get("has-more"):
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
        return params

    def backoff_wait_generator(self):
        """The wait generator used by the backoff decorator on request failure. """
        return backoff.expo(factor=3)

    def backoff_max_tries(self) -> int:
        """The number of attempts before giving up when retrying requests."""
        return 8