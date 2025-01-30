import json
import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
import re


def deep_merge(dict1, dict2):
    for key, value in dict2.items():
        if isinstance(value, dict) and key in dict1:
            dict1[key] = deep_merge(dict1[key], value)
        else:
            dict1[key] = value
    return dict1

def merge_responses(responses, pk, jsonpath):
    merged_records = {}

    # Extract the top-level field name from the JSONPath (e.g., "$.contacts[*]" → "contacts")
    field_match = re.match(r"\$\.(\w+)\[\*\]", jsonpath)
    if not field_match:
        raise ValueError(f"Invalid JSONPath format: {jsonpath}")
    field_name = field_match.group(1)  # Extracts "contacts"

    for response in responses:
        data = response.json()
        records = extract_jsonpath(jsonpath, input=data)  # Extract the list of records

        for record in records:
            item_id = record[pk]
            if item_id not in merged_records:
                merged_records[item_id] = record  # First time seeing this record
            else:
                merged_records[item_id] = deep_merge(merged_records[item_id], record)  # Merge duplicate records

    # Use the first response as the base to get additional fields used for pagination like offset or has_more
    base_response = responses[0].json()

    # Replace the jsonpath field data
    base_response[field_name] = list(merged_records.values())  # Convert merged dict back to list

    # Build the merged response
    merged_response = requests.Response()
    merged_response._content = json.dumps(base_response).encode("utf-8")
    merged_response.status_code = responses[0].status_code
    merged_response.headers = responses[0].headers

    return merged_response