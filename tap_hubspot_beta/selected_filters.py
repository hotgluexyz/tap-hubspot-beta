"""Helpers for parsing Hotglue selected filters for HubSpot streams."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

_CLAUSE_KEY_RE = re.compile(r"^clause_(\d+)$")
_SUPPORTED_FIELDS = {"eventType"}
_SUPPORTED_OPERATORS = {"EQ", "IN"}


def _ordered_clauses(stream_filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    clauses: List[Tuple[int, Dict[str, Any]]] = []
    for key, val in stream_filters.items():
        m = _CLAUSE_KEY_RE.match(key)
        if not m:
            continue
        if not isinstance(val, dict):
            raise ValueError(f"Invalid {key}: expected an object, got {type(val).__name__}")
        clauses.append((int(m.group(1)), val))
    clauses.sort(key=lambda x: x[0])
    return [c[1] for c in clauses]


def _normalize_clause_values(clause: Dict[str, Any]) -> List[str]:
    try:
        field = clause["field"]
        operator = str(clause["operator"]).strip().upper()
        value = clause["value"]
    except KeyError as exc:
        raise ValueError(f"Clause missing required key: {exc}") from exc

    if not isinstance(field, str) or field.strip() not in _SUPPORTED_FIELDS:
        supported_fields = ", ".join(sorted(_SUPPORTED_FIELDS))
        raise ValueError(
            f"Unsupported filter field {field!r}; supported: {supported_fields}."
        )

    if operator not in _SUPPORTED_OPERATORS:
        supported_ops = ", ".join(sorted(_SUPPORTED_OPERATORS))
        raise ValueError(
            f"Unsupported clause operator {operator!r}; supported: {supported_ops}."
        )

    if operator == "EQ":
        if isinstance(value, list):
            raise ValueError("EQ operator expects a single value, got a list.")
        normalized = str(value).strip()
        return [normalized] if normalized else []

    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]

    normalized = str(value).strip()
    return [normalized] if normalized else []


def parse_contact_events_types_filters(stream_filters: Dict[str, Any]) -> List[str]:
    """Return normalized selected event types from one stream filter object."""
    clauses = _ordered_clauses(stream_filters)
    event_types: List[str] = []
    for clause in clauses:
        if clause.get("field") != "eventType":
            continue
        event_types.extend(_normalize_clause_values(clause))

    # Preserve order while removing duplicates.
    return list(dict.fromkeys(event_types))