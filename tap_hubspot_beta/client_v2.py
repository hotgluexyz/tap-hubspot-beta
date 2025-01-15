
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Iterable

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hubspot_beta.client_base import hubspotStreamSchema
import copy


class hubspotV2Stream(hubspotStreamSchema):
    """hubspot stream class."""

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["limit"] = self.page_size
        params.update(self.additional_prarams)
        if self.properties_url:
            params["properties"] = self.selected_properties
        if next_page_token:
            params["offset"] = next_page_token["offset"]
        return params
    
    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        if self.properties_url:
            for name, value in row.get("properties").items():
                row[name] = self.parse_value(name, value.get("value"))
            row["id"] = str(row["companyId"])
            del row["properties"]
        for field in self.datetime_fields:
            if row.get(field) is not None:
                if row.get(field) in [0, ""]:
                    row[field] = None
                else:
                    # format datetime as hubspot standard ex. 2024-04-24T20:20:53.386Z
                    dt_field = datetime.fromtimestamp(int(row[field]) / 1000)
                    row[field] = dt_field.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        row["updatedAt"] = row["hs_lastmodifieddate"]
        row["createdAt"] = row["createdate"]
        row["archived"] = False
        return row

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            # only use fullsync_companies in the first sync
            if self.name == "fullsync_companies" and not self.is_first_sync():
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