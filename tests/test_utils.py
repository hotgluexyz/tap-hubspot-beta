"""Tests for tap_hubspot_beta.utils association merge helpers."""

import pytest

from tap_hubspot_beta.utils import deep_merge_dicts, merge_association_values


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
