"""Tests for INCLUDE_STREAMS / IGNORE_STREAMS handling of parent-child streams."""

import tap_hubspot_beta.tap as tap
from tap_hubspot_beta.streams import (
    CallsStream,
    CampaignAssetsStream,
    CampaignsStream,
    ContactsStream,
    MessagesStream,
    ThreadsStream,
    TranscriptsStream,
)

STREAMS = [
    ContactsStream,
    CallsStream,
    CampaignsStream,
    CampaignAssetsStream,
    ThreadsStream,
    TranscriptsStream,
    MessagesStream,
]


def select(monkeypatch, include=(), ignore=()):
    monkeypatch.setattr(tap, "include_streams", list(include))
    monkeypatch.setattr(tap, "ignore_streams", list(ignore))
    return tap.add_streams(STREAMS)


def test_no_selection_keeps_all_streams(monkeypatch):
    assert select(monkeypatch) == STREAMS


def test_include_child_keeps_its_parent(monkeypatch):
    assert select(monkeypatch, include=["MessagesStream"]) == [ThreadsStream, MessagesStream]
    assert select(monkeypatch, include=["TranscriptsStream"]) == [CallsStream, TranscriptsStream]


def test_include_parent_does_not_add_its_children(monkeypatch):
    assert select(monkeypatch, include=["ThreadsStream"]) == [ThreadsStream]


def test_ignore_parent_drops_its_children(monkeypatch):
    selected = select(monkeypatch, ignore=["ThreadsStream", "CampaignsStream"])
    assert selected == [ContactsStream, CallsStream, TranscriptsStream]


def test_ignore_child_keeps_its_parent(monkeypatch):
    selected = select(monkeypatch, ignore=["MessagesStream"])
    assert ThreadsStream in selected
    assert MessagesStream not in selected
