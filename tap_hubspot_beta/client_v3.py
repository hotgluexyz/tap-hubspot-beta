"""REST client handling, including hubspotStream base class."""

from typing import Any, Dict, Optional, List
import copy

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hubspot_beta.client_base import hubspotStream
from pendulum import parse
from datetime import datetime
from singer_sdk import typing as th
import singer
from tap_hubspot_beta.utils import merge_responses
from singer_sdk.exceptions import RetriableAPIError


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
    previous_starting_time = None
    max_dates = []
    starting_times = []
    bulk_child = True
    query_end_time = None
    max_bucket_size = 10000
    buckets = None
    current_bucket = None

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

        if self.current_bucket["filter_by_id"]:
            data = response.json()

            if len(data["results"]) > 0:
                new_last_record_id = data["results"][-1]["properties"]["hs_object_id"]

                if self.current_bucket["last_record_id"] == new_last_record_id:
                    self.logger.warn(f"Bucket pagination loop detected. Skipping to next bucket")
                    next_page_token = None
                else:
                    self.current_bucket["last_record_id"] = new_last_record_id
                    next_page_token = "-1" if previous_token == "0" else "0"

        # we've reached the end of this time bucket
        if not next_page_token:
            if len(self.buckets) > 0:
                self.current_bucket = self.buckets.pop(0)
                next_page_token = "-1" if previous_token == "0" else "0"
            else:
                self.logger.warn(f"No more buckets to process")

        return next_page_token
    
    def get_end_time(self):
        # Maintain consistent end_time within stream syncs
        if self.query_end_time:
            return self.query_end_time
        end_date = self._tap.config.get("end_date")
        if end_date:
            return int(parse(end_date).timestamp() * 1000)
        end_date = datetime.now()
        self.query_end_time = int(end_date.timestamp() * 1000)
        return int(end_date.timestamp() * 1000)

    def prepare_bucket_request_payload(self, starting_time: int, end_time: int):
        payload = {}
        payload["limit"] = 1
        payload["filters"] = []
        payload["properties"] = []
        if self.filter:
            payload["filters"].append(self.filter)
        if self.replication_key and starting_time or self.special_replication:
            payload["filters"].extend([
                {
                    "propertyName": self.replication_key_filter,
                    "operator": "GT",
                    "value": starting_time,
                },
                {
                    "propertyName": self.replication_key_filter,
                    "operator": "LTE",
                    "value": end_time
                }
            ])
            payload["sorts"] = [{
                "propertyName": self.replication_key_filter,
                "direction": "ASCENDING"
            }]
            
        return payload

    def prepare_bucket_request(
        self, context: dict | None, request_data: dict | None
    ) -> requests.PreparedRequest:
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token=None)
        headers = self.http_headers

        request = requests.Request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data
        )

        return self.requests_session.prepare_request(request)

    def get_time_bucket_size(self, context: dict, starting_time: int, end_time: int):
        decorated_request = self.request_decorator(self._request)
        request_data = self.prepare_bucket_request_payload(starting_time, end_time)
        prepared_request = self.prepare_bucket_request(context, request_data)
        response = decorated_request(prepared_request, context).json()

        return response.get('total')

    def split_time_bucket(self, context: dict, starting_time: int, end_time: int):
        buckets = []
        time_diff = end_time - starting_time

        if time_diff == 1:
            bucket_size = self.get_time_bucket_size(context, starting_time, end_time)
            self.logger.info(f"Cannot split bucket anymore. Bucket size is {bucket_size}")
            return [{"starting_time": starting_time, "end_time": end_time, "bucket_size": bucket_size}]

        mid_time = end_time - int(time_diff/2)
        bucket_1_size = self.get_time_bucket_size(context, starting_time, mid_time)
        bucket_2_size = self.get_time_bucket_size(context, mid_time, end_time)

        if bucket_1_size > self.max_bucket_size:
            self.logger.info(f"Bucket size is {bucket_1_size}, splitting into multiple buckets")
            buckets += self.split_time_bucket(context, starting_time, mid_time)
        elif bucket_1_size > 0:
            buckets += [{"starting_time": starting_time, "end_time": mid_time, "bucket_size": bucket_1_size}]
        
        if bucket_2_size > self.max_bucket_size:
            self.logger.info(f"Bucket size is {bucket_2_size}, splitting into multiple buckets")
            buckets += self.split_time_bucket(context, mid_time, end_time)
        elif bucket_2_size > 0:
            buckets += [{"starting_time": mid_time, "end_time": end_time, "bucket_size": bucket_2_size}]

        return buckets

    def merge_small_adjacent_buckets(self, buckets: List[dict]):
        bucket_index = 0
        while bucket_index < (len(buckets)-1):
            bucket_1 = buckets[bucket_index]
            bucket_2 = buckets[bucket_index+1]
            if (bucket_1["bucket_size"] + bucket_2["bucket_size"]) <= self.max_bucket_size:
                bucket_1["end_time"] = bucket_2["end_time"]
                bucket_1["bucket_size"] += bucket_2["bucket_size"]
                buckets.pop(bucket_index+1)
            else:
                bucket_index += 1

    def create_time_buckets(self, context: Optional[dict]):
        self.logger.info(f"Creating time buckets for stream = {self.name}")
        starting_time = self.starting_time or self.get_starting_time(context)
        end_time = self.get_end_time()
        buckets = []
        bucket_size = self.get_time_bucket_size(context, starting_time, end_time)

        self.logger.info(f"Total records to fetch for stream = {self.name}: {bucket_size}")

        if bucket_size > self.max_bucket_size:
            self.logger.info(f"Bucket size is greater than {self.max_bucket_size}, splitting into multiple buckets")
            buckets += self.split_time_bucket(context,starting_time, end_time)
        else:
            self.logger.info(f"Bucket size is less than {self.max_bucket_size}, creating single bucket")
            buckets = [{"starting_time": starting_time, "end_time": end_time, "bucket_size": bucket_size}]

        self.logger.info(f"Merging small adjacent buckets")
        self.merge_small_adjacent_buckets(buckets)

        for bucket in buckets:
            filter_by_id = False
            last_record_id = None
            if bucket["bucket_size"] > self.max_bucket_size:
                filter_by_id = True
                last_record_id = "0"
            bucket["filter_by_id"] = filter_by_id
            bucket["last_record_id"] = last_record_id

        self.logger.info(f"Total buckets created: {len(buckets)} with total records: {sum([b['bucket_size'] for b in buckets])}")
        self.logger.info(buckets)

        return buckets


    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        if self.buckets is None:
            # in the first request, create the buckets with all time ranges
            self.buckets = self.create_time_buckets(context)
            self.current_bucket = self.buckets.pop(0)
        
        payload = {}
        payload["limit"] = self.page_size
        payload["filters"] = []
        starting_time = self.starting_time or self.get_starting_time(context)
        if self.filter:
            payload["filters"].append(self.filter)
        if next_page_token and next_page_token not in ["0", "-1"]:
            payload["after"] = next_page_token
        if self.replication_key and starting_time or self.special_replication:
            payload["filters"].extend([
                {
                    "propertyName": self.replication_key_filter,
                    "operator": "GT",
                    "value": self.current_bucket["starting_time"],
                },
                {
                    "propertyName": self.replication_key_filter,
                    "operator": "LTE",
                    "value": self.current_bucket["end_time"]
                }
            ])
            if self.current_bucket["filter_by_id"]:
                payload["filters"].append({
                    "propertyName": "hs_object_id",
                    "operator": "GT",
                    "value": self.current_bucket["last_record_id"]
                })
            if self.current_bucket["last_record_id"]:
                payload["sorts"] = [{
                    "propertyName": "hs_object_id",
                    "direction": "ASCENDING"
                }]
            else:
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
        row = super().post_process(row, context, skip_id=True)
        # store archived value in _hg_archived
        row["_hg_archived"] = False
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

            record_id_set = set()
            for record_result in self.get_records(current_context):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result

                # Skip duplicate records
                if record["id"] in record_id_set:
                    continue
                record_id_set.add(record["id"])

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
        params.update(self.additional_params)
        if self.properties_url:
            # requesting either properties or properties with history
            # if we send both it returns an error saying the url is too long
            if params.get("propertiesWithHistory"):
                params["propertiesWithHistory"] = ",".join(self.selected_properties)
            else:
                params["properties"] = ",".join(self.selected_properties)
        if next_page_token:
            params["after"] = next_page_token
        #TODO: from 6444 -° should we keep?
        if self.name == "forms":
            params["formTypes"] = "all"  
        return params



class AssociationsV3ParentStream(hubspotV3Stream):
    name = "associations_v3_parent"
    primary_keys = ["id"]

    def get_child_context(self, record: dict, context) -> dict:
        return {"id": record["id"]}

    def _sync_records(
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
        response_json = response.json()
        if response_json.get("hasMore"):
            offset = response_json.get("offset")
            if offset:
                return offset

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
            payload["offset"] = next_page_token
        return payload


    
class hubspotHistoryV3Stream(hubspotV3Stream):
    rest_method = "POST"
    bulk_child = True
    schema_written = False

    properties_param = "propertiesWithHistory"
    merge_pk = "id"

    # the response validation happens in _handle_request, having backoff in _request as well hides errors
    def backoff_max_tries(self) -> int:
        return 1

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

