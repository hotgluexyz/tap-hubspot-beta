"""Tests for HubSpot search-stream estimated record counts."""

from unittest.mock import Mock

from tap_hubspot_beta.client_v3 import hubspotV3SearchStream


def test_estimated_record_count_uses_the_sync_time_window():
    stream = Mock()
    stream.starting_time = None
    stream.get_starting_time.return_value = 100
    stream.get_end_time.return_value = 200
    stream.get_time_bucket_size.return_value = 12

    assert hubspotV3SearchStream.get_estimated_record_count(stream) == 12
    stream.get_time_bucket_size.assert_called_once_with(
        context=None,
        starting_time=100,
        end_time=200,
    )


def test_estimated_record_count_returns_none_when_the_search_fails():
    stream = Mock()
    stream.starting_time = 100
    stream.get_end_time.return_value = 200
    stream.get_time_bucket_size.side_effect = RuntimeError("unavailable")
    stream.name = "contacts"

    assert hubspotV3SearchStream.get_estimated_record_count(stream) is None
    stream.logger.warning.assert_called_once()
