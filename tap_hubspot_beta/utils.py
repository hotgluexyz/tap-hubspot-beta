import os
import json
import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
import re
import psutil

def deep_merge(dict1, dict2):
    for key, value in dict2.items():
        if isinstance(value, dict) and dict1.get(key):
            dict1[key] = deep_merge(dict1[key], value)
        else:
            dict1[key] = value
    return dict1

def merge_responses(responses, pk, jsonpath=None):
    merged_records = {}

    if jsonpath:
        # Extract the top-level field name from JSONPath (e.g., "$.contacts[*]" → "contacts")
        if jsonpath:
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
                    merged_records[item_id] = record
                else:
                    merged_records[item_id] = deep_merge(merged_records[item_id], record)

        # Use the first response as the base and replace the jsonpath field
        base_response = responses[0].json()
        base_response[field_name] = list(merged_records.values())
        merged_data = base_response
    
    else:
        merged_data = {}
        for response in responses:
            data = response.json()  # Parse JSON response
            merged_data = deep_merge(merged_data, data)  # Merge dictionaries

    # Build the merged response
    merged_response = requests.Response()
    merged_response._content = json.dumps(merged_data).encode("utf-8")
    merged_response.status_code = responses[0].status_code
    merged_response.headers = responses[0].headers

    return merged_response


def deep_merge_dicts(a, b):
    """
    Deep merge two dictionaries:
    - If both values are dicts, merge recursively.
    - If both values are strings and differ, make a list with both.
    - If one is list and the other a string, append (keeping unique).
    - Otherwise, b overwrites a.
    """
    merged = dict(a)

    for key, b_val in b.items():
        if key in merged:
            a_val = merged[key]

            # Case 1: both dicts → recursive merge
            if isinstance(a_val, dict) and isinstance(b_val, dict):
                merged[key] = deep_merge_dicts(a_val, b_val)

            # Case 2: both strings
            elif isinstance(a_val, str) and isinstance(b_val, str):
                if a_val == b_val:
                    merged[key] = a_val
                else:
                    merged[key] = [a_val, b_val]

            # Case 3: string + list
            elif isinstance(a_val, str) and isinstance(b_val, list):
                merged[key] = list(set([a_val] + b_val))
            elif isinstance(a_val, list) and isinstance(b_val, str):
                merged[key] = list(set(a_val + [b_val]))

            # Case 4: both lists
            elif isinstance(a_val, list) and isinstance(b_val, list):
                merged[key] = list(set(a_val + b_val))

            # Case 5: fallback — overwrite
            else:
                merged[key] = b_val
        else:
            merged[key] = b_val

    return merged

def get_memory_usage():
    used_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2
    return f"Memory {round(used_memory)}MB"