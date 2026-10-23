import json
import logging
import requests
from hotglue_singer_sdk.helpers.jsonpath import extract_jsonpath
import re
from typing import Any, Optional

HUBSPOT_ACCESS_TOKEN_METADATA_URL = (
    "https://api.hubapi.com/oauth/v1/access-tokens/{token}"
)

SCOPE_OBJECT_TYPE_ALIASES = {
    "lineitems": "line_items",
    "contacts_v3": "contacts",
    "archived_contacts": "contacts",
    "archived_contacts_v3": "contacts",
    "archived_deals": "deals",
    "archived_companies": "companies",
}


def get_property_data_sensitivity(field: dict) -> Optional[str]:
    """Return sensitivity level for a HubSpot property definition, if any."""
    sensitivity = field.get("dataSensitivity")
    if sensitivity in ("sensitive", "highly_sensitive"):
        return sensitivity
    categories = field.get("sensitiveDataCategories") or []
    if categories:
        return "sensitive"
    return None


def sensitive_read_scope_name(object_type: str, level: str) -> str:
    """OAuth scope required to read property values at the given sensitivity level."""
    normalized = SCOPE_OBJECT_TYPE_ALIASES.get(object_type, object_type)
    if normalized == "custom":
        return f"crm.objects.custom.{level}.read"
    if normalized == "tickets":
        if level == "highly_sensitive":
            return "tickets.highly_sensitive"
        return "tickets.sensitive"
    return f"crm.objects.{normalized}.{level}.read"


def has_sensitive_read_scope(
    scopes: frozenset,
    object_type: str,
    level: str,
) -> bool:
    return sensitive_read_scope_name(object_type, level) in scopes


def can_read_property_values(
    field: dict,
    scopes: frozenset,
    object_type: str,
) -> bool:
    """Whether the current token may read values for this property."""
    level = get_property_data_sensitivity(field)
    if not level:
        return True
    return has_sensitive_read_scope(scopes, object_type, level)


def resolve_oauth_scopes(
    config: dict,
    logger: logging.Logger,
    auth_headers: Optional[dict] = None,
) -> frozenset:
    """Resolve granted OAuth scopes from config or HubSpot token metadata."""
    scope = config.get("scope")
    if scope:
        if isinstance(scope, str):
            return frozenset(scope.split())
        return frozenset(str(item) for item in scope)

    access_token = config.get("access_token")
    if not access_token:
        return frozenset()

    headers = dict(auth_headers or {})
    try:
        response = requests.get(
            HUBSPOT_ACCESS_TOKEN_METADATA_URL.format(token=access_token),
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        return frozenset(response.json().get("scopes") or [])
    except Exception as exc:
        logger.warning(
            "Could not resolve HubSpot OAuth scopes; sensitive properties will be "
            "excluded from sync: %s",
            exc,
        )
        return frozenset()


def parse_selected_id_label(value: Any) -> Optional[str]:
    """Extract an id from a bare id or a ``{name} ({id})`` filter label."""
    if value is None:
        return None
    text = str(value).strip()
    _prefix, sep, tail = text.rpartition(" (")
    if sep and tail.endswith(")"):
        return tail[:-1]
    return text


def ids_from_filter_clause(clause: dict) -> frozenset:
    """Parse EQ/IN clause values into a frozenset of id strings."""
    operator = clause.get("operator")
    if operator == "EQ":
        values = [clause.get("value")]
    else:  # operator == "IN"
        values = clause.get("value", [])
    return frozenset(
        parsed
        for value in values
        if (parsed := parse_selected_id_label(value)) is not None
    )


def ids_from_config(config_value: Any) -> Optional[frozenset]:
    """Normalize a config list-id value to frozenset, or None when unset."""
    if not config_value:
        return None
    return frozenset(str(value) for value in config_value)


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


def _association_id_parts(value):
    """Expand an association field value into ordered ID parts."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(part) for part in value]
    return [str(value)]


def _join_association_ids(ids):
    """Join association IDs with ';', preserving first-seen order and dropping duplicates."""
    seen = set()
    ordered = []
    for item in ids:
        item_str = str(item)
        if item_str in seen:
            continue
        seen.add(item_str)
        ordered.append(item_str)
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    return ";".join(ordered)


def merge_association_values(left, right):
    """Merge two association values into a single string (or None)."""
    return _join_association_ids(_association_id_parts(left) + _association_id_parts(right))


def deep_merge_dicts(a, b):
    """
    Deep merge two dictionaries:
    - If both values are dicts, merge recursively.
    - If both values are strings and differ, join with ';'.
    - If one is list and the other a string, join preserving order.
    - Otherwise, b overwrites a.
    """
    merged = dict(a)

    for key, b_val in b.items():
        if key in merged:
            a_val = merged[key]

            # Case 1: both dicts → recursive merge
            if isinstance(a_val, dict) and isinstance(b_val, dict):
                merged[key] = deep_merge_dicts(a_val, b_val)

            # Case 2: both strings → join with ';' when different
            # Case 3: string + list → join preserving order
            # Case 4: both lists → join preserving order
            elif isinstance(a_val, (str, list)) and isinstance(b_val, (str, list)):
                merged[key] = merge_association_values(a_val, b_val)

            # Case 5: fallback — overwrite
            else:
                merged[key] = b_val
        else:
            merged[key] = b_val

    return merged
