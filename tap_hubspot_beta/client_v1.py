"""REST client handling, including hubspotStream base class."""

import logging
import pendulum
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hubspot_beta.client_base import hubspotStream


class hubspotV1Stream(hubspotStream):
    """hubspot stream class."""

    page_size = 100

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
        params["property"] = self.selected_properties
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
                        dt_field = pendulum.parse(row[field])
                        dt_field = datetime(
                            dt_field.year,
                            dt_field.month,
                            dt_field.day,
                            dt_field.hour,
                            dt_field.minute,
                            dt_field.second,
                            dt_field.microsecond,
                        )
                        row[field] = dt_field.isoformat()
                    except Exception:
                        dt_field = datetime.fromtimestamp(int(row[field]) / 1000)
                        dt_field = dt_field.replace(tzinfo=None)
                        row[field] = dt_field.isoformat()
        return row
