"""Tests for tap_hubspot_beta.utils association merge helpers."""

import pytest

from tap_hubspot_beta.utils import (
    can_read_property_values,
    deep_merge_dicts,
    get_property_data_sensitivity,
    has_sensitive_read_scope,
    merge_association_values,
    sensitive_read_scope_name,
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


@pytest.mark.parametrize(
    "field,expected",
    [
        ({"dataSensitivity": "sensitive"}, "sensitive"),
        ({"dataSensitivity": "highly_sensitive"}, "highly_sensitive"),
        ({"sensitiveDataCategories": ["HIPAA"]}, "sensitive"),
        ({"dataSensitivity": "non_sensitive"}, None),
        ({}, None),
    ],
)
def test_get_property_data_sensitivity(field, expected):
    assert get_property_data_sensitivity(field) == expected


def test_sensitive_read_scope_name_for_deals_and_tickets():
    assert sensitive_read_scope_name("deals", "sensitive") == (
        "crm.objects.deals.sensitive.read"
    )
    assert sensitive_read_scope_name("tickets", "highly_sensitive") == (
        "tickets.highly_sensitive"
    )


def test_can_read_property_values_respects_granted_scopes():
    field = {"dataSensitivity": "sensitive"}
    scopes = frozenset({"crm.objects.deals.sensitive.read"})
    assert can_read_property_values(field, scopes, "deals")
    assert not can_read_property_values(field, frozenset(), "deals")
    assert has_sensitive_read_scope(scopes, "deals", "sensitive")


def test_has_sensitive_read_scope_accepts_v2_scope_names():
    assert has_sensitive_read_scope(
        frozenset({"crm.objects.deals.sensitive.read.v2"}),
        "deals",
        "sensitive",
    )
    assert has_sensitive_read_scope(
        frozenset({"crm.objects.custom.highly_sensitive.read.v2"}),
        "custom",
        "highly_sensitive",
    )
    assert has_sensitive_read_scope(
        frozenset({"tickets.sensitive.v2"}),
        "tickets",
        "sensitive",
    )


def test_deep_merge_dicts_merges_string_with_list():
    left = {"record-1": {"companies_to_companies": "1001"}}
    right = {"record-1": {"companies_to_companies": ["1002"]}}

    merged = deep_merge_dicts(left, right)

    assert merged["record-1"]["companies_to_companies"] == "1001;1002"
