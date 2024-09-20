"""REST client handling, including hubspotStream base class."""

import logging
from typing import Any, Dict, Optional
import copy

from tap_hubspot_beta.client_base import hubspotStream


class hubspotV2Stream(hubspotStream):
    """hubspot stream class."""

    records_jsonpath = "$.results[*]"
    
    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params.update(self.additional_params)
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        if self.properties_url:
            for name, value in row["properties"].items():
                row[name] = value
            del row["properties"]
        return row
    
    def populate_d1_breakdowns(context):
        raise NotImplementedError("Streams that uses d1 and/or d2 breakdowns, needs to implement populate_d1_breakdowns")

    def request_records(self, context):
        """Request records from REST endpoint(s), returning response records."""
        next_page_token = None
        finished = False
        decorated_request = self.request_decorator(self._request)
        if getattr(self, "is_d1_stream", False) or getattr(self, "is_d2_stream", False):
            self.populate_d1_breakdowns(context)

        while not finished:
            self.logger.setLevel(logging.CRITICAL)
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
