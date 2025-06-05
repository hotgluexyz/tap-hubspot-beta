"""REST client handling, including hubspotStream base class."""

import copy
from typing import Any, List, Optional

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.exceptions import InvalidStreamSortException
from singer_sdk.helpers._state import (
    finalize_state_progress_markers,
    log_sort_error
)

from tap_hubspot_beta.client_base import hubspotStream


class hubspotV4Stream(hubspotStream):
    """hubspot stream class."""

    rest_method = "POST"
    records_jsonpath = "$.results[*]"
    bulk_child = True

    def get_url(self, context: Optional[dict]) -> str:
        """Get stream entity URL. """
        return "".join([self.url_base, self.path or ""])

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        payload = {"inputs": context["ids"]}
        return payload

    def parse_response(self, response: requests.Response):
        """Parse the response and return an iterator of result rows."""
        for row in extract_jsonpath(self.records_jsonpath, input=response.json()):
            output = {}
            output["from_id"] = row["from"]["id"]
            for to in row["to"]:
                output["to_id"] = str(to["toObjectId"])
                if "associationTypes" in to:
                    output['associationTypes'] = to['associationTypes']
                    if len(to['associationTypes'])>0:
                        output['category'] = to['associationTypes'][0]['category']
                        output['typeId'] = to['associationTypes'][0]['typeId']
                        output['label'] = to['associationTypes'][0]['label']

                yield output

    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        """Sync records, emitting RECORD and STATE messages."""
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
                    self._sync_children(child_context)
                self._check_max_record_limit(record_count)
                if selected:
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
            if self.tap_state.get("bookmarks"):
                self.tap_state["bookmarks"][self.name] = {}
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)