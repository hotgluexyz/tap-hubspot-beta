"""Tests for tap_hubspot_beta.utils helpers."""

import pytest

from tap_hubspot_beta.client_base import hubspotStream
from tap_hubspot_beta.utils import (
    HUBSPOT_LIST_ID_CONFIG_KEYS,
    HUBSPOT_OBJECT_STREAM_LIST_ID_CONFIG_KEYS,
    InvalidHubSpotListIdError,
    coerce_hubspot_list_id,
    deep_merge_dicts,
    ids_from_config,
    ids_from_filter_clause,
    merge_association_values,
    validate_hubspot_list_id_config,
)


@pytest.mark.parametrize(
    "left,right,expected",
    [
        ("1001", "1002", "1001;1002"),
        ("1001", ["1002"], "1001;1002"),
        (["1001", "1002", "1001"], "1003", "1001;1002;1003"),
        ("1001", "1001", "1001"),
    ],
)
def test_merge_association_values(left, right, expected):
    assert merge_association_values(left, right) == expected


def test_deep_merge_dicts_joins_duplicate_association_fields():
    left = {"record-1": {"Accounts": "1001"}}
    right = {"record-1": {"Accounts": "1002"}}

    merged = deep_merge_dicts(left, right)

    assert merged["record-1"]["Accounts"] == "1001;1002"


def test_deep_merge_dicts_merges_string_with_list():
    left = {"record-1": {"companies_to_companies": "1001"}}
    right = {"record-1": {"companies_to_companies": ["1002"]}}

    merged = deep_merge_dicts(left, right)

    assert merged["record-1"]["companies_to_companies"] == "1001;1002"


@pytest.mark.parametrize(
    "value",
    ["none", "", "12a", True, -1],
)
def test_coerce_hubspot_list_id_rejects_invalid(value):
    with pytest.raises(InvalidHubSpotListIdError):
        coerce_hubspot_list_id(value, field="list_ids")


def test_ids_from_config_accepts_string_and_int_ids():
    assert ids_from_config([9, "10"]) == frozenset({"9", "10"})


def test_validate_hubspot_list_id_config_rejects_none_sentinel():
    with pytest.raises(InvalidHubSpotListIdError, match="list_ids"):
        validate_hubspot_list_id_config({"list_ids": ["none"]})


def test_validate_hubspot_list_id_config_checks_all_keys():
    with pytest.raises(InvalidHubSpotListIdError, match="contacts_list_ids"):
        validate_hubspot_list_id_config({"contacts_list_ids": ["bad-id"]})


def test_hubspot_list_id_config_keys_cover_object_stream_mapping():
    object_keys = set(HUBSPOT_OBJECT_STREAM_LIST_ID_CONFIG_KEYS.values())
    assert object_keys.issubset(set(HUBSPOT_LIST_ID_CONFIG_KEYS))
    assert hubspotStream._list_id_config_mapping == HUBSPOT_OBJECT_STREAM_LIST_ID_CONFIG_KEYS


def test_ids_from_filter_clause_parses_label():
    assert ids_from_filter_clause(
        {"operator": "IN", "value": ["Special (11)"]},
    ) == frozenset({"11"})
