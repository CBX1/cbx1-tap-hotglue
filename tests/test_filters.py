"""Tests for CBX1Stream.prepare_request_payload filter assembly.

These tests exercise prepare_request_payload directly via the unbound method
to avoid constructing the full Singer SDK stream (which triggers auth and
schema fetches over the network).
"""

import os
from types import SimpleNamespace


HOTGLUE_UUID = "d6435b86-31f9-470a-97e5-33ed6a5024d5"


def _stream(*, replication_key=None, start_date=None):
    """Build a minimal stream-like object suitable for prepare_request_payload."""
    os.environ.setdefault("BASE_URL", "http://example.invalid/")
    from tap_cbx1.client import CBX1Stream

    obj = SimpleNamespace()
    obj.page_size = 10
    obj.replication_key_field = replication_key
    obj.get_starting_time = lambda ctx: start_date
    # Bind prepare_request_payload as an unbound function call against `obj`.
    obj.prepare_request_payload = lambda ctx, tok: CBX1Stream.prepare_request_payload(
        obj, ctx, tok
    )
    return obj


def _filters(stream):
    return stream.prepare_request_payload(None, 0)["filters"]


def test_filter_omitted_when_env_var_unset(monkeypatch):
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    assert "updatedBy" not in _filters(_stream())


def test_filter_omitted_when_env_var_empty(monkeypatch):
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", "")
    assert "updatedBy" not in _filters(_stream())


def test_filter_added_when_env_var_set(monkeypatch):
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    assert _filters(_stream())["updatedBy"] == {"type": "NOT_EQUALS", "value": HOTGLUE_UUID}


def test_test_metadata_filter_always_present(monkeypatch):
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    assert _filters(_stream())["testMetadata"] == {"type": "EQUALS", "value": None}


def test_filter_coexists_with_replication_key_between(monkeypatch):
    from pendulum import parse

    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    stream = _stream(replication_key="updatedAt", start_date=parse("2026-01-01T00:00:00Z"))
    filters = _filters(stream)
    assert filters["testMetadata"] == {"type": "EQUALS", "value": None}
    assert filters["updatedBy"] == {"type": "NOT_EQUALS", "value": HOTGLUE_UUID}
    assert filters["updatedAt"]["type"] == "BETWEEN"
