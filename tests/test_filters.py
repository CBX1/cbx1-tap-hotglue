"""Tests for tap-side filtering of HotGlue-authored records.

These tests stub the prepared HTTP request and the decorated request
function so we exercise `request_records` end-to-end without booting
Singer SDK auth or schema fetches.
"""

import os
from types import SimpleNamespace
from unittest.mock import MagicMock


HOTGLUE_UUID = "d6435b86-31f9-470a-97e5-33ed6a5024d5"


def _stream(*, replication_key=None, start_date=None):
    """Minimal stream-like object for prepare_request_payload tests."""
    os.environ.setdefault("BASE_URL", "http://example.invalid/")
    from tap_cbx1.client import CBX1Stream

    obj = SimpleNamespace()
    obj.page_size = 10
    obj.replication_key_field = replication_key
    obj.get_starting_time = lambda ctx: start_date
    obj.prepare_request_payload = lambda ctx, tok: CBX1Stream.prepare_request_payload(
        obj, ctx, tok
    )
    return obj


def _filters(stream):
    return stream.prepare_request_payload(None, 0)["filters"]


# ---- prepare_request_payload: server-side filter must be absent ----

def test_payload_does_not_push_updatedby_filter_server_side(monkeypatch):
    """Even with the env var set, the request payload must NOT carry an
    updatedBy filter — pushing $ne updatedBy to Mongo regresses tenants
    where HotGlue is the dominant writer (verified via prod explain)."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    assert "updatedBy" not in _filters(_stream())


def test_test_metadata_filter_always_present(monkeypatch):
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    assert _filters(_stream())["testMetadata"] == {"type": "EQUALS", "value": None}


def test_payload_includes_replication_key_when_set(monkeypatch):
    from pendulum import parse

    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    stream = _stream(replication_key="updatedAt", start_date=parse("2026-01-01T00:00:00Z"))
    filters = _filters(stream)
    assert filters["updatedAt"]["type"] == "BETWEEN"
    assert "updatedBy" not in filters


# ---- request_records: tap-side filter ----

def _mock_response(records, page_number, total_pages):
    resp = MagicMock()
    resp.json.return_value = {
        "data": {
            "content": records,
            "number": page_number,
            "totalPages": total_pages,
        }
    }
    return resp


def _stream_with_pages(pages):
    """Build a stream-like object whose `request_records` walks the given
    list of (records, page_number, total_pages) tuples.

    `get_next_page_token` issues a trailing fetch beyond the last real page
    (it terminates only when the response signals number == totalPages), so
    we auto-emit an empty page once the supplied list is exhausted.
    """
    os.environ.setdefault("BASE_URL", "http://example.invalid/")
    from tap_cbx1.client import CBX1Stream

    obj = SimpleNamespace()
    obj.logger = MagicMock()
    obj.prepare_request = lambda context, next_page_token: f"req-{next_page_token}"
    obj.request_decorator = lambda fn: fn

    iterator = iter(pages)
    last_total_pages = pages[-1][2] if pages else 1

    def _next_response(*_args, **_kwargs):
        try:
            return _mock_response(*next(iterator))
        except StopIteration:
            # Trailing empty page — number == totalPages terminates pagination.
            return _mock_response([], last_total_pages, last_total_pages)

    obj._request = _next_response
    obj.get_next_page_token = lambda resp, prev: (
        CBX1Stream.get_next_page_token(obj, resp, prev)
    )
    obj.request_records = lambda context: CBX1Stream.request_records(obj, context)
    return obj


def _rec(updated_by, _id="id"):
    return {"id": _id, "updatedBy": updated_by, "updatedAt": "2026-01-01T00:00:00.000Z"}


def test_request_records_skips_hotglue_authored(monkeypatch):
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        ([_rec(HOTGLUE_UUID, "a"), _rec("other", "b"), _rec(HOTGLUE_UUID, "c")], 0, 1),
    ]
    out = list(_stream_with_pages(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["b"]


def test_request_records_passthrough_when_env_unset(monkeypatch):
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    pages = [
        ([_rec(HOTGLUE_UUID, "a"), _rec("other", "b")], 0, 1),
    ]
    out = list(_stream_with_pages(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["a", "b"]


def test_pagination_continues_across_all_filtered_page(monkeypatch):
    """Critical: a page where every record is HotGlue must NOT stop pagination.
    The next page is fetched regardless of how many records survived the filter."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        # page 0: ALL records are HotGlue — yields nothing
        ([_rec(HOTGLUE_UUID, "a"), _rec(HOTGLUE_UUID, "b"), _rec(HOTGLUE_UUID, "c")], 0, 3),
        # page 1: also all HotGlue
        ([_rec(HOTGLUE_UUID, "d"), _rec(HOTGLUE_UUID, "e")], 1, 3),
        # page 2: finally a non-HotGlue record
        ([_rec(HOTGLUE_UUID, "f"), _rec("other", "survivor")], 2, 3),
    ]
    out = list(_stream_with_pages(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["survivor"]


def test_pagination_terminates_when_all_pages_filtered(monkeypatch):
    """Worst case: every page is all-HotGlue. Must yield nothing and terminate
    cleanly (no infinite loop, no exception)."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        ([_rec(HOTGLUE_UUID, "a")], 0, 2),
        ([_rec(HOTGLUE_UUID, "b")], 1, 2),
    ]
    out = list(_stream_with_pages(pages).request_records(context=None))
    assert out == []


def test_pagination_handles_empty_response_page(monkeypatch):
    """An empty content array must terminate the loop cleanly (last-page case)."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        ([_rec("other", "a")], 0, 1),
    ]
    out = list(_stream_with_pages(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["a"]


def test_request_records_handles_missing_updatedby_field(monkeypatch):
    """Records with no updatedBy stamp (e.g., legacy data) must pass through —
    `None != hotglue_uuid` is the correct semantic."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        ([_rec(None, "legacy"), _rec(HOTGLUE_UUID, "skip"), _rec("other", "keep")], 0, 1),
    ]
    out = list(_stream_with_pages(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["legacy", "keep"]


# ---- bookmark signpost ----

def test_replication_key_signpost_returns_now_not_none():
    """When 0 records are yielded (e.g., all-HotGlue tenant), state must still
    advance. Singer SDK caps state at the signpost; returning a fresh "now"
    moves the bookmark forward even when nothing yielded."""
    from datetime import datetime, timezone

    from tap_cbx1.client import CBX1Stream

    obj = SimpleNamespace()
    signpost = CBX1Stream.get_replication_key_signpost(obj, context=None)
    assert signpost is not None
    # pendulum DateTime is a subclass of datetime; should be tz-aware and "now-ish".
    delta = abs((datetime.now(timezone.utc) - signpost).total_seconds())
    assert delta < 5
