"""REST client handling, including hubspotStream base class."""
import copy
import logging
import urllib3

import requests
import backoff
from copy import deepcopy
from typing import Any, Dict, Optional, cast, List, Callable, Generator

from backports.cached_property import cached_property
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.streams import RESTStream
from singer_sdk.mapper import  SameRecordTransform, StreamMap
from singer_sdk.helpers._flattening import get_flattening_options
from singer_sdk import Stream
import time

from pendulum import parse

from tap_hubspot_beta.auth import OAuth2Authenticator
import singer
from singer import StateMessage
from datetime import datetime
import pytz
import requests
logging.getLogger("backoff").setLevel(logging.CRITICAL)


class hubspotStream(RESTStream):
    """hubspot stream class."""

    url_base = "https://api.hubapi.com/"
    base_properties = []
    additional_params = {}
    properties_url = None
    page_size = 100

    stream_metadata = {}
    fields_metadata = {}
    object_type = None
    fields_metadata = {}
    bulk_child_size = 1000
    is_first_sync = False
    visible_in_catalog = True

    def get_associations(self, from_current_object: str, to_current_object: str) -> list:
        # request associations for the from_current_object and to_current_object
        associations = requests.get(
            f"{self.url_base}crm/v3/associations/{from_current_object}/{to_current_object}/types",
            headers = self.authenticator.auth_headers or {},
        )
        return associations.json().get("results", [])
    
    def get_crm_associations_metadata(self) -> dict:
        # request all associations for the stream
        associations_objects = self.config.get("add_associations_to_schema", [])
        from_current_object = next((obj for obj in associations_objects if obj.lower() == self.name.lower()), None)

        associations_metadata = {}
        # get assoaciations for all permutations of from_current_object and to_current_object
        for object in associations_objects:
            if object.lower() == self.name.lower():
                continue
            # get associations for the object
            associations = self.get_associations(from_current_object, object)
            for association in associations:
                associations_metadata[association["name"]] = {
                    "toObjectTypeId": object,
                    "associationTypeId": association.get("id")
                }
        self._tap.associations_metadata[self.name] = associations_metadata
        return associations_metadata

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
            
            # only use companies stream for incremental syncs
            if self.name == "companies":
                fullsync_companies_state = self.tap_state.get("bookmarks", {}).get("fullsync_companies", {})
                fullsync_on = False
                try:
                    # Check if the fullsync stream is selected or not
                    fullsync_on = [s for s in self._tap.streams.items() if str(s[0]) == "fullsync_companies"][0][1].selected
                except:
                    pass
                if fullsync_on and not fullsync_companies_state.get("replication_key") and self.is_first_sync():
                    finished = True
                    yield from []
                    break
                elif fullsync_companies_state.get("replication_key") and self.is_first_sync():
                    self.stream_state.update(fullsync_companies_state)
                    self.stream_state["starting_replication_value"] = self.stream_state["replication_key_value"]
            
            # only use deals stream for incremental syncs
            if self.name == "deals":
                fullsync_deals_state = self.tap_state.get("bookmarks", {}).get("fullsync_deals", {})
                fullsync_on = False
                try:
                    # Check if the fullsync stream is selected or not
                    fullsync_on = [s for s in self._tap.streams.items() if str(s[0]) == "fullsync_deals"][0][1].selected
                except:
                    pass
                if fullsync_on and not fullsync_deals_state.get("replication_key") and self.is_first_sync():
                    finished = True
                    yield from []
                    break
                elif fullsync_deals_state.get("replication_key") and self.is_first_sync():
                    self.stream_state.update(fullsync_deals_state)
                    self.stream_state["starting_replication_value"] = self.stream_state["replication_key_value"]
            
            # only use contacts stream for incremental syncs
            contacts_v3_name = self._tap.legacy_streams_mapping.get("contacts_v3", "contacts_v3")
            
            if self.name == contacts_v3_name:
                fullsync_contacts_v3_state = self.tap_state.get("bookmarks", {}).get("fullsync_contacts_v3", {})                  
                if not self.stream_state.get("replication_key_value") and self._tap.streams["fullsync_contacts_v3"].is_first_sync():
                    finished = True
                    yield from []
                    break
                if not self.stream_state.get("replication_key_value") and fullsync_contacts_v3_state.get("replication_key"):
                    self.stream_state.update(fullsync_contacts_v3_state)
                    self.stream_state["starting_replication_value"] = self.stream_state["replication_key_value"]  

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
    
    def curlify_request(self, request):
        command = "curl -X {method} -H {headers} -d '{data}' '{uri}'"
        method = request.method
        uri = request.url
        data = request.body

        headers = []
        for k, v in request.headers.items():
            # Mask the Authorization header
            if k.lower() == "authorization":
                v = "__MASKED__"
            headers.append('"{0}: {1}"'.format(k, v))

        headers = " -H ".join(headers)
        return command.format(method=method, headers=headers, data=data, uri=uri)

    def validate_response(self, response: requests.Response) -> None:
        #Rate limit logic
        #@TODO enable this if 429 handling fails. 
        # not using because if it is daily limit it could cause job to be stuck for a day
        # headers = response.headers
        # #Prevent rate limit from being triggered
        # if (
        #     "X-HubSpot-RateLimit-Remaining" in headers
        #     and int(headers["X-HubSpot-RateLimit-Remaining"]) <= 10
        # ):
        #     #Default sleep time
        #     sleep_time = 10
        #     if "X-HubSpot-RateLimit-Interval-Milliseconds" in headers:
        #         # Sleep based on milliseconds limit of the API
        #         sleep_time = int(headers["X-HubSpot-RateLimit-Interval-Milliseconds"]) / 1000
        #         if sleep_time < 0:
        #             sleep_time = 10
        #     self.logger.warn(f"Rate limit reached. Sleeping for {sleep_time} seconds.")        
        #     time.sleep(sleep_time)
        
        # if 429 is triggered log the response code and retry    
        if response.status_code == 429:
            self.logger.warn(f"Rate limit reached. Response code: {response.status_code}, info: {response.text}, headers: {response.headers}")
            time.sleep(30)
            raise RetriableAPIError(f"Response code: {response.status_code}, info: {response.text}")    
            
        """Validate HTTP response."""
        try:
            json_response = response.json()
        except ValueError:
            json_response = {}
            
        def _log_and_raise(exception_class, message):
            curl_command = self.curlify_request(response.request)
            logging.info(f"Response code: {response.status_code}, info: {response.text}")
            logging.info(f"CURL command for failed request: {curl_command}")
            raise exception_class(f"Msg {message}, response {response.text}")

        if 500 <= response.status_code < 600 or response.status_code in [429, 401, 104]:
            msg = f"{response.status_code} Server Error: {response.reason} for path: {self.path}"
            _log_and_raise(RetriableAPIError, msg)
        
        elif self.name == "list_membership_v3" and response.status_code == 403 and "You do not have permissions to view object" in response.text:
            curl_command = self.curlify_request(response.request)
            logging.info(f"Response code: {response.status_code}, info: {response.text}")
            logging.info(f"CURL command for failed request: {curl_command}")
            # Skip list memberships for this list and continue the sync
            return
            

        elif response.status_code == 400 and "Invalid JSON input" in json_response.get('message'):
            msg = f"{response.status_code} Client Error:  {response.reason} for path: {self.path}"
            _log_and_raise(RetriableAPIError, msg)

        elif 400 <= response.status_code < 500:
            msg = f"{response.status_code} Client Error: {response.reason} for path: {self.path}"
            if "FORM_TYPE_NOT_ALLOWED" in response.text:
                #Skip this form and continue the sync
                return
            if "invalid json input" in response.text.lower() or "problem with the request" in response.text.lower():
                raise RetriableAPIError(msg)
            _log_and_raise(FatalAPIError, msg)


    @staticmethod
    def extract_type(field):
        field_type = field.get("type")
        if field_type == "bool" or field.get("fieldType") == "booleancheckbox":
            return th.BooleanType
        if field_type in ["string", "enumeration", "phone_number", "json", "object_coordinates"]:
            return th.StringType
        if field_type == "number":
            return th.StringType
        if field_type in ["datetime", "date"]:
            return th.DateTimeType

        # TODO: Changed default because tap errors if type is None
        return th.StringType

    def request_schema(self, url, headers):
        response = requests.get(url, headers=headers)
        try:
            self.validate_response(response)
        except Exception as e:
            if "You do not have permissions" in str(e):
                self.logger.error(f"Insufficient permissions for {self.name}: {e}")
                return None
            raise e
        return response

    @cached_property
    def schema(self):
        properties = deepcopy(self.base_properties)
        headers = self.http_headers
        headers.update(self.authenticator.auth_headers or {})
        url = self.url_base + self.properties_url
        response = self.request_decorator(self.request_schema)(url, headers=headers)

        deduplicate_columns = self.config.get("deduplicate_columns", True)
        base_properties = []
        if isinstance(self.base_properties, list):
            base_properties = [property.name.lower() for property in self.base_properties]

        if response:
            schema_res = response.json()
            fields = schema_res.get("results",[]) if isinstance(schema_res, dict) and schema_res.get("results") else schema_res
            for field in fields:
                field_name = field.get("name")
                # filter duplicated columns (case insensitive)
                if deduplicate_columns:
                    if field_name.lower() in base_properties:
                        self.logger.info(f"Not including field {field_name} in catalog as it's a duplicate(case insensitive) of a base property for stream {self.name}")
                        continue

                if not field.get("deleted"):
                    property = th.Property(field_name, self.extract_type(field))
                    properties.append(property)
        
        # get crm objects associations metadata
        if self.config.get("add_associations_to_schema") and self.name.lower() in [col.lower() for col in self.config.get("add_associations_to_schema")]:
            associations = self.get_crm_associations_metadata()
            for association in associations:
                properties.append(th.Property(association, th.StringType))

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
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.RequestException,
                urllib3.exceptions.HTTPError
            ),
            max_tries=8,
            factor=3,
            on_backoff=self.backoff_handler,
        )(func)
        return decorator

    def backoff_wait_generator(self) -> Callable[..., Generator[int, Any, None]]:
        """
        Example:
            - 1st retry: 10 seconds
            - 2nd retry: 20 seconds
            - 3rd retry: 40 seconds
            - 4th retry: 80 seconds
            - 5th retry: 160 seconds
            - 6th retry: 320 seconds (capped at 5 minutes)
        """
        return backoff.expo(base=2, factor=10, max_value=320)

    def backoff_max_tries(self) -> int:
        return 7

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

    def process_row_types(self,row) -> Dict[str, Any]:
        schema = self.schema['properties']
        # If the row is null we ignore
        if row is None:
            return row

        for field, value in row.items():
            if field not in schema:
                # Skip fields not found in the schema
                continue

            field_info = schema[field]
            field_type = field_info.get("type", ["null"])[0]

            if field_type == "boolean":
                if isinstance(value, str):
                    if value.lower() == "true":
                        row[field] = True
                    elif value.lower() == "false":
                        row[field] = False

        return row

    def is_first_sync(self):
        if self.stream_state.get("replication_key"):
            return False
        return True
    
    def _write_state_message(self) -> None:
        """Write out a STATE message with the latest state."""
        tap_state = self.tap_state

        if tap_state and tap_state.get("bookmarks"):
            for stream_name in tap_state.get("bookmarks").keys():
                if tap_state["bookmarks"][stream_name].get("partitions"):
                    tap_state["bookmarks"][stream_name]["partitions"] = []

        singer.write_message(StateMessage(value=tap_state))

    def backoff_handler(self, details):
        """Log backoff retry details."""
        self.logger.warning(
            f"Backing off {details['wait']} seconds after {details['tries']} tries "
            f"calling function {details['target']} with args {details['args']} "
            f"and kwargs {details['kwargs']}"
        )
    
    def parse_properties(self, row, skip_id=None):
        if self.properties_url:
            for name, value in row.get("properties", {}).items():
                if skip_id and name == "id":
                    continue
                if isinstance(value, dict) and "value" in value:
                    row[name] = value["value"]
                else:
                    row[name] = value
            del row["properties"]
        return row
    
    def parse_datetimes(self, row):
        for field in self.datetime_fields:
            if row.get(field) is not None:
                if row.get(field) in [0, ""]:
                    row[field] = None
                else:
                    try:
                        row[field] = parse(row[field])
                    except Exception:
                        dt_field = datetime.fromtimestamp(int(row[field]) / 1000, tz=pytz.UTC)
                        row[field] = dt_field.replace(tzinfo=None)
        return row

    def post_process(self, row: dict, context: Optional[dict], skip_id=None) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        row = self.parse_properties(row, skip_id=skip_id)
        row = self.parse_datetimes(row)
        row = self.process_row_types(row)
        return row

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
