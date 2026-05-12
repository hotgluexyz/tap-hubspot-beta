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


def parse_contact_events_types_filters(stream_filters: Dict[str, Any]) -> List[str]:
    """Return normalized selected event types from one stream filter object."""
    clauses = _ordered_clauses(stream_filters)
    event_types: List[str] = []
    for clause in clauses:
        if clause.get("field") != "eventType":
            continue
        
        if clause["operator"] == "EQ":
            event_types.append(str(clause["value"]).strip())
        else:  # IN
            event_types.extend(str(item).strip() for item in clause["value"])

    # Preserve order while removing duplicates.
    return list(dict.fromkeys(event_types))
