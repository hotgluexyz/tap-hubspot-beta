import json
import logging
import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
import re
from typing import Any, Optional

HUBSPOT_ACCESS_TOKEN_METADATA_URL = (
    "https://api.hubapi.com/oauth/2026-03/token/introspect"
)

# Match stream name with hubspot object name.
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
    # standard and high are for v1 properties, sensitive and highly_sensitive are for the other properties endpoints.
    if sensitivity in ("sensitive", "highly_sensitive", "standard", "high"):
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
    scope = sensitive_read_scope_name(object_type, level)
    return scope in scopes or f"{scope}.v2" in scopes


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
) -> frozenset:
    """Resolve granted OAuth scopes from config or HubSpot token introspection."""
    access_token = config.get("access_token")
    client_id = config.get("client_id")
    client_secret = config.get("client_secret")
    if not access_token or not client_id or not client_secret:
        return frozenset()

    try:
        response = requests.post(
            HUBSPOT_ACCESS_TOKEN_METADATA_URL,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "token": access_token,
                "token_type_hint": "access_token",
            },
            timeout=30,
        )
        response.raise_for_status()
        return frozenset(response.json().get("scopes") or [])
    except Exception:
        # Empty scopes => can_read_property_values treats sensitive fields as unreadable.
        logger.warning(
            "Could not resolve HubSpot OAuth scopes; sensitive properties will be "
            "excluded from sync."
        )
        return frozenset()


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