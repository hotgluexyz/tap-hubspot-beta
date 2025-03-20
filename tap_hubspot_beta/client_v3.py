"""REST client handling, including hubspotStream base class."""

from typing import Any, Dict, Optional, List
import copy

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hubspot_beta.client_base import hubspotStream
from pendulum import parse
from singer_sdk import typing as th
import singer


from singer_sdk.exceptions import InvalidStreamSortException
from singer_sdk.helpers._state import (
    finalize_state_progress_markers,
    log_sort_error
)


class hubspotV3SearchStream(hubspotStream):
    """hubspot stream class."""

    rest_method = "POST"

    records_jsonpath = "$.results[*]"
    next_page_token_jsonpath = "$.paging.next.after"
    filter = None
    starting_time = None
    page_size = 100
    special_replication = False

    def get_starting_time(self, context):
        start_date = self.get_starting_timestamp(context)
        if start_date:
            return int(start_date.timestamp() * 1000)

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        all_matches = extract_jsonpath(self.next_page_token_jsonpath, response.json())

        if not previous_token:
            self.logger.info(f"Total records to fetch for stream = {self.name}: {response.json().get('total')}")

        next_page_token = next(iter(all_matches), None)
        if next_page_token == "10000":
            start_date = self.stream_state.get("progress_markers", {}).get(
                "replication_key_value"
            )
            if self.name in ["deals_association_parent"]:
                data = response.json()
                # extract maximum modified date to overcome 10000 pagination limit
                hs_lastmodifieddates = [
                    entry["properties"]["hs_lastmodifieddate"]
                    for entry in data["results"]
                    if "properties" in entry
                    and "hs_lastmodifieddate" in entry["properties"]
                ]
                hs_lastmodifieddates = [
                    date for date in hs_lastmodifieddates if date is not None
                ]
                max_date = max(hs_lastmodifieddates) if hs_lastmodifieddates else None
                if max_date:
                    start_date = max_date
                    self.special_replication = True
                else:
                    return None

            if start_date:
                start_date = parse(start_date)
                self.starting_time = int(start_date.timestamp() * 1000)
            next_page_token = "0"
        return next_page_token

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        payload = {}
        payload["limit"] = self.page_size
        payload["filters"] = []
        starting_time = self.starting_time or self.get_starting_time(context)
        if self.filter:
            payload["filters"].append(self.filter)
        if next_page_token and next_page_token!="0":
            payload["after"] = next_page_token
        if self.replication_key and starting_time or self.special_replication:
            payload["filters"].append(
                {
                    "propertyName": self.replication_key_filter,
                    "operator": "GT",
                    "value": starting_time,
                }
            )
            payload["sorts"] = [{
                "propertyName": self.replication_key_filter,
                "direction": "ASCENDING"
            }]
            if self.properties_url:
                if self.name =="deals_association_parent":
                    payload["properties"] = ["id"]
                else:
                    payload["properties"] = self.selected_properties
            else:
                payload["properties"] = []
        return payload

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        if self.properties_url:
            for name, value in row["properties"].items():
                row[name] = value
            del row["properties"]
        # store archived value in _hg_archived
        row["_hg_archived"] = False
        row = self.process_row_types(row)
        return row

    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        """Sync records, emitting RECORD and STATE messages. """

        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            child_context_bulk = {"ids": []}
            for record_result in self.get_records(current_context):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result
                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                # Sync children, except when primary mapper filters out the record
                if self.stream_maps[0].get_filter_result(record):
                    child_context_bulk["ids"].append(child_context)
                if len(child_context_bulk["ids"])>=self.bulk_child_size:
                    self._sync_children(child_context_bulk)
                    child_context_bulk = {"ids": []}
                self._check_max_record_limit(record_count)
                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                        self._write_state_message()
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex

                record_count += 1
                partition_record_count += 1
            if len(child_context_bulk):
                self._sync_children(child_context_bulk)
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)
        # Reset interim bookmarks before emitting final STATE message:
        self._write_state_message()

    
    def _sync_children(self, child_context: dict) -> None:
        for child_stream in self.child_streams:
            if child_stream.selected or child_stream.has_selected_descendents:
                if not child_stream.bulk_child:
                    ids = child_context.get("ids") or []
                    for id in ids:
                        child_stream.sync(context=id)
                else:
                    child_stream.sync(context=child_context)

class hubspotV3Stream(hubspotStream):
    """hubspot stream class."""

    records_jsonpath = "$.results[*]"
    next_page_token_jsonpath = "$.paging.next.after"

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        all_matches = extract_jsonpath(self.next_page_token_jsonpath, response.json())
        return next(iter(all_matches), None)

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["limit"] = self.page_size
        params.update(self.additional_prarams)
        if self.properties_url:
            # requesting either properties or properties with history
            # if we send both it returns an error saying the url is too long
            if params.get("propertiesWithHistory"):
                params["propertiesWithHistory"] = ",".join(self.selected_properties)
            else:
                params["properties"] = ",".join(self.selected_properties)
        if next_page_token:
            params["after"] = next_page_token
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        if self.properties_url:
            for name, value in row["properties"].items():
                row[name] = value
            del row["properties"]
        row = self.process_row_types(row)
        return row


class hubspotV3SingleSearchStream(hubspotStream):
    """hubspot stream class."""

    rest_method = "POST"

    records_jsonpath = "$.results[*]"
    next_page_token_jsonpath = "$.paging.next.after"
    filter = None
    starting_time = None
    page_size = 100

    def get_starting_time(self, context):
        start_date = self.get_starting_timestamp(context)
        if start_date:
            return int(start_date.timestamp() * 1000)

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        all_matches = extract_jsonpath(self.next_page_token_jsonpath, response.json())
        next_page_token = next(iter(all_matches), None)
        if next_page_token == "10000":

            start_date = self.stream_state.get("progress_markers", {}).get("replication_key_value")

            if start_date:
                start_date = parse(start_date)
                self.starting_time = int(start_date.timestamp() * 1000)

            next_page_token = "0"
        return next_page_token

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        payload = {}
        payload["limit"] = self.page_size
        payload["filters"] = []
        if self.filter:
            payload["filters"].append(self.filter)
        if next_page_token and next_page_token!="0":
            payload["after"] = next_page_token
        return payload

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        if self.properties_url:
            for name, value in row["properties"].items():
                row[name] = value
            del row["properties"]
        row = self.process_row_types(row)
        return row
    
class hubspotHistoryV3Stream(hubspotV3Stream):
    rest_method = "POST"
    bulk_child = True
    schema_written = False

    def post_process(self, row: dict, context) -> dict:
        row = super().post_process(row, context)
        props = row.get("propertiesWithHistory") or dict()
        row["propertiesWithHistory"] = {k:v for (k,v) in props.items() if v}
        row = {k:v for k,v in row.items() if k in ["id", "propertiesWithHistory", "createdAt", "updatedAt", "archived", "archivedAt"]}
        return row
    
    def _write_schema_message(self) -> None:
        """Write out a SCHEMA message with the stream schema."""
        if not self.schema_written:
            for schema_message in self._generate_schema_messages():
                schema_message.schema = th.PropertiesList(*self.base_properties).to_dict()
                singer.write_message(schema_message)
                self.schema_written = True
    
    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {}

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        payload = {}
        payload["propertiesWithHistory"] = self.selected_properties
        payload["inputs"] = context["ids"]
        return payload
