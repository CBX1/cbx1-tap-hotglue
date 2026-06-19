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

PAGE_SIZE = 10


def _mock_response(records, page_number, total_pages, *, last=None, slice_shape=False):
    """Build a mocked response in the backend's Page/Slice DTO shape.

    `last` defaults to a realistic value derived from the page index
    (`page_number >= total_pages - 1`) when `total_pages` is known. Pass an
    explicit `last` to override, or `slice_shape=True` to emulate the
    count-free backend that returns null `totalElements`/`totalPages`.
    """
    if last is None and total_pages is not None:
        last = page_number >= total_pages - 1

    total_elements = None if slice_shape else (
        total_pages * PAGE_SIZE if total_pages is not None else None
    )
    out_total_pages = None if slice_shape else total_pages

    resp = MagicMock()
    resp.json.return_value = {
        "data": {
            "content": records,
            "number": page_number,
            "size": PAGE_SIZE,
            "first": page_number == 0,
            "last": last,
            "numberOfElements": len(records),
            "empty": len(records) == 0,
            "totalElements": total_elements,
            "totalPages": out_total_pages,
        }
    }
    return resp


def _stream_with_pages(pages, *, slice_shape=False):
    """Build a stream-like object whose `request_records` walks the given
    list of (records, page_number, total_pages) tuples.

    Each emitted page carries a realistic `last` flag, so pagination
    terminates on the final page itself — no trailing fetch is required.
    """
    os.environ.setdefault("BASE_URL", "http://example.invalid/")
    from tap_cbx1.client import CBX1Stream

    obj = SimpleNamespace()
    obj.logger = MagicMock()
    obj.page_size = PAGE_SIZE
    obj.prepare_request = lambda context, next_page_token: f"req-{next_page_token}"
    obj.request_decorator = lambda fn: fn

    iterator = iter(pages)
    last_total_pages = pages[-1][2] if pages else 1

    def _next_response(*_args, **_kwargs):
        try:
            return _mock_response(*next(iterator), slice_shape=slice_shape)
        except StopIteration:
            # Safety net: should not be reached now that the final page sets
            # `last=True`. Emit a terminal empty page if the loop overruns.
            return _mock_response([], last_total_pages, last_total_pages,
                                  last=True, slice_shape=slice_shape)

    obj._request = _next_response
    obj.get_next_page_token = lambda resp, prev: (
        CBX1Stream.get_next_page_token(obj, resp, prev)
    )
    obj.request_records = lambda context: CBX1Stream.request_records(obj, context)
    return obj


def _rec(updated_by, _id="id"):
    return {"id": _id, "updatedBy": updated_by, "updatedAt": "2026-01-01T00:00:00.000Z"}


def _full_page(updated_by, prefix):
    """A full page (len == PAGE_SIZE) of identically-authored records.

    Non-final pages must be full; under the count-free contract a short page
    is itself a terminal signal, so intermediate pages carry PAGE_SIZE records.
    """
    return [_rec(updated_by, f"{prefix}{i}") for i in range(PAGE_SIZE)]


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
        # page 0: ALL records are HotGlue — yields nothing (full page, not last)
        (_full_page(HOTGLUE_UUID, "a"), 0, 3),
        # page 1: also all HotGlue (full page, not last)
        (_full_page(HOTGLUE_UUID, "d"), 1, 3),
        # page 2: finally a non-HotGlue record (short/last page)
        ([_rec(HOTGLUE_UUID, "f"), _rec("other", "survivor")], 2, 3),
    ]
    out = list(_stream_with_pages(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["survivor"]


def test_pagination_terminates_when_all_pages_filtered(monkeypatch):
    """Worst case: every page is all-HotGlue. Must yield nothing and terminate
    cleanly (no infinite loop, no exception)."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        (_full_page(HOTGLUE_UUID, "a"), 0, 2),
        ([_rec(HOTGLUE_UUID, "b")], 1, 2),  # short final page
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


# ---- get_next_page_token: termination contract ----

def _token_for(page_data):
    """Call the real CBX1Stream.get_next_page_token against a raw `data` dict."""
    from tap_cbx1.client import CBX1Stream

    obj = SimpleNamespace()
    obj.page_size = PAGE_SIZE
    resp = MagicMock()
    resp.json.return_value = {"data": page_data}
    return CBX1Stream.get_next_page_token(obj, resp, previous_token=page_data.get("number"))


def test_next_page_token_terminates_on_last_true_without_totalpages():
    """Slice scenario: `last: True` with totalPages ABSENT must terminate and
    must NOT raise TypeError (no arithmetic on a null totalPages)."""
    # A FULL page so the short-page defensive check does not mask the `last` path.
    page = {
        "content": [_rec(HOTGLUE_UUID, f"x{i}") for i in range(PAGE_SIZE)],
        "number": 5,
        "size": PAGE_SIZE,
        "last": True,
        # totalElements / totalPages intentionally absent (count-free backend)
    }
    assert _token_for(page) is None


def test_next_page_token_no_typeerror_when_totalpages_null_and_last_false():
    """Count-free backend mid-stream: last=False, totalPages=None, full page →
    must advance (not crash) and must not compare against None."""
    page = {
        "content": [_rec(HOTGLUE_UUID, f"x{i}") for i in range(PAGE_SIZE)],
        "number": 2,
        "size": PAGE_SIZE,
        "last": False,
        "totalElements": None,
        "totalPages": None,
    }
    assert _token_for(page) == 3  # previous_token(2) + 1


def test_next_page_token_terminates_on_short_page_when_last_absent():
    """Backend omits `last` entirely: a short page (len < page_size) is the
    terminal signal."""
    page = {
        "content": [_rec("other", f"x{i}") for i in range(3)],  # 3 < PAGE_SIZE
        "number": 0,
        "size": PAGE_SIZE,
        # no `last`, no totals
    }
    assert _token_for(page) is None


def test_next_page_token_terminates_on_empty_page_when_last_absent():
    page = {"content": [], "number": 0, "size": PAGE_SIZE}
    assert _token_for(page) is None


def test_next_page_token_legacy_totalpages_fallback_when_last_absent():
    """Legacy Page DTO without `last`: stop when number >= totalPages - 1, but
    only when both are non-null. A full final page must still terminate."""
    final = {
        "content": [_rec("other", f"x{i}") for i in range(PAGE_SIZE)],
        "number": 2,
        "totalPages": 3,  # number(2) >= 3 - 1 → last page
    }
    assert _token_for(final) is None

    mid = {
        "content": [_rec("other", f"x{i}") for i in range(PAGE_SIZE)],
        "number": 1,
        "totalPages": 3,  # number(1) < 3 - 1 → more pages
    }
    assert _token_for(mid) == 2


def test_next_page_token_advances_on_full_page_current_backend():
    """Current backend: full page, last=False, totalPages present → advance."""
    page = {
        "content": [_rec("other", f"x{i}") for i in range(PAGE_SIZE)],
        "number": 0,
        "last": False,
        "totalElements": 30,
        "totalPages": 3,
    }
    assert _token_for(page) == 1


# ---- CROSS-REPO CONTRACT / E2E: tap <-> backend handshake, both versions ----

def _page_json(records, number, total_pages, *, slice_shape):
    """Emit one page in the EXACT backend JSON shape.

    slice_shape=False → legacy Page DTO (totalElements/totalPages populated).
    slice_shape=True  → count-free Slice DTO (totalElements/totalPages null).
    Both shapes always carry `last`.
    """
    last = number >= total_pages - 1
    return {
        "data": {
            "content": records,
            "number": number,
            "size": PAGE_SIZE,
            "first": number == 0,
            "last": last,
            "numberOfElements": len(records),
            "empty": len(records) == 0,
            "totalElements": None if slice_shape else total_pages * PAGE_SIZE,
            "totalPages": None if slice_shape else total_pages,
        }
    }


def _e2e_stream(page_jsons, *, max_iterations=50):
    """Drive the REAL request_records over a mocked transport that returns the
    given sequence of raw page JSON dicts. The transport itself emits the JSON;
    we do NOT bypass get_next_page_token. Iteration is capped to fail loudly on
    a runaway loop instead of hanging.
    """
    os.environ.setdefault("BASE_URL", "http://example.invalid/")
    from tap_cbx1.client import CBX1Stream

    obj = SimpleNamespace()
    obj.logger = MagicMock()
    obj.page_size = PAGE_SIZE
    obj.prepare_request = lambda context, next_page_token: f"req-{next_page_token}"
    obj.request_decorator = lambda fn: fn

    state = {"calls": 0}

    def _transport(*_args, **_kwargs):
        idx = state["calls"]
        state["calls"] += 1
        if state["calls"] > max_iterations:
            raise AssertionError("pagination did not terminate (runaway loop)")
        # Real transport returns a response object exposing .json()
        payload = page_jsons[idx] if idx < len(page_jsons) else page_jsons[-1]
        resp = MagicMock()
        resp.json.return_value = payload
        return resp

    obj._request = _transport
    obj.get_next_page_token = lambda resp, prev: CBX1Stream.get_next_page_token(
        obj, resp, prev
    )
    obj.request_records = lambda context: CBX1Stream.request_records(obj, context)
    return obj, state


def _three_page_sequence(slice_shape):
    """3 pages, 10 + 10 + 4 = 24 records; final page short. Both shapes set
    `last` on page 2."""
    total_pages = 3
    p0 = _page_json([_rec("other", f"p0-{i}") for i in range(PAGE_SIZE)], 0, total_pages, slice_shape=slice_shape)
    p1 = _page_json([_rec("other", f"p1-{i}") for i in range(PAGE_SIZE)], 1, total_pages, slice_shape=slice_shape)
    p2 = _page_json([_rec("other", f"p2-{i}") for i in range(4)], 2, total_pages, slice_shape=slice_shape)
    return [p0, p1, p2]


def test_e2e_contract_legacy_page_shape(monkeypatch):
    """(a) Legacy Page shape: totals populated. Loop yields ALL 24 records,
    terminates, no exception."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    pages = _three_page_sequence(slice_shape=False)
    # Sanity: this fixture really is the legacy shape.
    assert pages[0]["data"]["totalPages"] == 3
    assert pages[0]["data"]["totalElements"] == 30

    stream, state = _e2e_stream(pages)
    out = list(stream.request_records(context=None))
    assert [r["id"] for r in out] == (
        [f"p0-{i}" for i in range(PAGE_SIZE)]
        + [f"p1-{i}" for i in range(PAGE_SIZE)]
        + [f"p2-{i}" for i in range(4)]
    )
    assert len(out) == 24
    assert state["calls"] == 3  # exactly 3 fetches, no trailing probe


def test_e2e_contract_count_free_slice_shape(monkeypatch):
    """(b) New count-free Slice shape: totalElements/totalPages NULL, `last`
    drives termination. Same 24 records, terminates, no exception/TypeError."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    pages = _three_page_sequence(slice_shape=True)
    # Sanity: this fixture really is the count-free shape.
    assert pages[0]["data"]["totalPages"] is None
    assert pages[0]["data"]["totalElements"] is None
    assert pages[2]["data"]["last"] is True

    stream, state = _e2e_stream(pages)
    out = list(stream.request_records(context=None))
    assert len(out) == 24
    assert [r["id"] for r in out][0] == "p0-0"
    assert [r["id"] for r in out][-1] == "p2-3"
    assert state["calls"] == 3


def test_e2e_contract_slice_full_final_page_with_last_true(monkeypatch):
    """Count-free edge: the final page is FULL (len == page_size) but carries
    `last: True`. Termination must come from `last`, not the short-page check."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    total_pages = 2
    p0 = _page_json([_rec("other", f"p0-{i}") for i in range(PAGE_SIZE)], 0, total_pages, slice_shape=True)
    p1 = _page_json([_rec("other", f"p1-{i}") for i in range(PAGE_SIZE)], 1, total_pages, slice_shape=True)
    assert p1["data"]["last"] is True and len(p1["data"]["content"]) == PAGE_SIZE
    assert p1["data"]["totalPages"] is None  # would TypeError under old logic

    stream, state = _e2e_stream([p0, p1])
    out = list(stream.request_records(context=None))
    assert len(out) == 2 * PAGE_SIZE
    assert state["calls"] == 2  # full final page still stops via `last`


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
