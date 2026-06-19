"""Tests for tap-side filtering and keyset (cursor) pagination.

These tests stub the prepared HTTP request and the decorated request
function so we exercise `request_records` end-to-end without booting
Singer SDK auth or schema fetches.

Pagination uses keyset/cursor paging: the request body carries a `cursor`
(empty string on the first page) instead of a `pageNumber`, and the backend
returns the next `data.cursor`. We never compute a `skip = pageNumber *
pageSize` offset, which on large orgs forces Mongo to walk every skipped
index entry per page (CM100 timeout / CPU spike).
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


def _payload(stream, token=""):
    return stream.prepare_request_payload(None, token)


def _filters(stream):
    return _payload(stream)["filters"]


# ---- prepare_request_payload: keyset cursor, no pageNumber ----

def test_payload_uses_cursor_not_pagenumber(monkeypatch):
    """Keyset contract: the body carries a `cursor`, never a `pageNumber`.
    Pushing a deep skip offset to Mongo (skip = pageNumber * pageSize) walks
    every skipped index entry per page → CM100 timeout on large orgs."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    payload = _payload(_stream(), token="")
    assert "pageNumber" not in payload
    assert "cursor" in payload


def test_payload_cursor_empty_string_on_first_page(monkeypatch):
    """First page: a None token (Singer SDK's initial value) maps to the empty
    string, which signals keyset mode to the backend."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    assert _payload(_stream(), token=None)["cursor"] == ""
    # The tap also seeds the first page with the literal empty string.
    assert _payload(_stream(), token="")["cursor"] == ""


def test_payload_echoes_backend_cursor_token(monkeypatch):
    """Subsequent pages: the opaque backend token is sent back verbatim."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    token = "eyJ1cGRhdGVkQXQiOiIyMDI2LTAxLTAxIiwiaWQiOiJhYmMifQ=="
    assert _payload(_stream(), token=token)["cursor"] == token


def test_payload_keeps_sort_and_page_size(monkeypatch):
    """sortBy/sortDirection/pageSize are unchanged by the cursor migration."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    payload = _payload(_stream(replication_key="updatedAt"), token="")
    assert payload["pageSize"] == 10
    assert payload["sortBy"] == "updatedAt"
    assert payload["sortDirection"] == "DESC"


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


def _mock_response(records, *, cursor=None, last=None):
    """Build a mocked response in the backend's keyset DTO shape.

    `cursor` is the opaque next-page token (None/"" ⇒ no further page).
    `last` is the backend's terminal boolean; defaults to None (absent).
    """
    resp = MagicMock()
    resp.json.return_value = {
        "data": {
            "content": records,
            "size": PAGE_SIZE,
            "last": last,
            "cursor": cursor,
            "numberOfElements": len(records),
            "empty": len(records) == 0,
        }
    }
    return resp


def _stream_with_pages(pages):
    """Build a stream-like object whose `request_records` walks the given list
    of page dicts. Each entry is ``{"records": [...], "cursor": <token-or-None>,
    "last": <bool-or-None>}``.

    The transport returns each page in order; pagination terminates via the
    real `get_next_page_token` (last / missing-cursor / short-page).
    """
    os.environ.setdefault("BASE_URL", "http://example.invalid/")
    from tap_cbx1.client import CBX1Stream

    obj = SimpleNamespace()
    obj.logger = MagicMock()
    obj.page_size = PAGE_SIZE
    obj.prepare_request = lambda context, next_page_token: f"req-{next_page_token}"
    obj.request_decorator = lambda fn: fn

    iterator = iter(pages)

    def _next_response(*_args, **_kwargs):
        try:
            page = next(iterator)
        except StopIteration:
            # Safety net: should not be reached. Emit a terminal empty page.
            return _mock_response([], cursor=None, last=True)
        return _mock_response(
            page["records"], cursor=page.get("cursor"), last=page.get("last")
        )

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

    Non-final pages must be full; a short page is itself a terminal signal,
    so intermediate pages carry PAGE_SIZE records.
    """
    return [_rec(updated_by, f"{prefix}{i}") for i in range(PAGE_SIZE)]


def test_request_records_skips_hotglue_authored(monkeypatch):
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        {"records": [_rec(HOTGLUE_UUID, "a"), _rec("other", "b"), _rec(HOTGLUE_UUID, "c")],
         "cursor": None, "last": True},
    ]
    out = list(_stream_with_pages(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["b"]


def test_request_records_passthrough_when_env_unset(monkeypatch):
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    pages = [
        {"records": [_rec(HOTGLUE_UUID, "a"), _rec("other", "b")], "cursor": None, "last": True},
    ]
    out = list(_stream_with_pages(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["a", "b"]


def test_pagination_continues_across_all_filtered_page(monkeypatch):
    """Critical: a page where every record is HotGlue must NOT stop pagination.
    The next page is fetched (via cursor) regardless of how many records
    survived the tap-side filter."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        # page 0: ALL records are HotGlue — yields nothing (full page, has cursor)
        {"records": _full_page(HOTGLUE_UUID, "a"), "cursor": "c1", "last": False},
        # page 1: also all HotGlue (full page, has cursor)
        {"records": _full_page(HOTGLUE_UUID, "d"), "cursor": "c2", "last": False},
        # page 2: finally a non-HotGlue record (short/last page)
        {"records": [_rec(HOTGLUE_UUID, "f"), _rec("other", "survivor")], "cursor": None, "last": True},
    ]
    out = list(_stream_with_pages(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["survivor"]


def test_pagination_terminates_when_all_pages_filtered(monkeypatch):
    """Worst case: every page is all-HotGlue. Must yield nothing and terminate
    cleanly (no infinite loop, no exception)."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        {"records": _full_page(HOTGLUE_UUID, "a"), "cursor": "c1", "last": False},
        {"records": [_rec(HOTGLUE_UUID, "b")], "cursor": None, "last": True},  # short final page
    ]
    out = list(_stream_with_pages(pages).request_records(context=None))
    assert out == []


def test_pagination_handles_empty_response_page(monkeypatch):
    """A single short page must terminate the loop cleanly (last-page case)."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        {"records": [_rec("other", "a")], "cursor": None, "last": True},
    ]
    out = list(_stream_with_pages(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["a"]


def test_request_records_handles_missing_updatedby_field(monkeypatch):
    """Records with no updatedBy stamp (e.g., legacy data) must pass through —
    `None != hotglue_uuid` is the correct semantic."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        {"records": [_rec(None, "legacy"), _rec(HOTGLUE_UUID, "skip"), _rec("other", "keep")],
         "cursor": None, "last": True},
    ]
    out = list(_stream_with_pages(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["legacy", "keep"]


# ---- get_next_page_token: keyset termination contract ----

def _token_for(page_data):
    """Call the real CBX1Stream.get_next_page_token against a raw `data` dict."""
    from tap_cbx1.client import CBX1Stream

    obj = SimpleNamespace()
    obj.page_size = PAGE_SIZE
    resp = MagicMock()
    resp.json.return_value = {"data": page_data}
    return CBX1Stream.get_next_page_token(obj, resp, previous_token=None)


def test_next_page_token_returns_cursor_on_full_non_last_page():
    """Happy path: full page, last=False, cursor present → return the cursor."""
    page = {
        "content": [_rec("other", f"x{i}") for i in range(PAGE_SIZE)],
        "last": False,
        "cursor": "next-token-abc",
    }
    assert _token_for(page) == "next-token-abc"


def test_next_page_token_terminates_on_last_true():
    """`last: True` terminates even when a cursor is still present and the page
    is full (termination comes from `last`, not the short-page check)."""
    page = {
        "content": [_rec("other", f"x{i}") for i in range(PAGE_SIZE)],
        "last": True,
        "cursor": "would-be-next",  # ignored: `last` wins
    }
    assert _token_for(page) is None


def test_next_page_token_terminates_when_cursor_missing():
    """No `cursor` key (or a non-keyset / older backend) terminates."""
    page = {
        "content": [_rec("other", f"x{i}") for i in range(PAGE_SIZE)],
        "last": False,
        # cursor intentionally absent
    }
    assert _token_for(page) is None


def test_next_page_token_terminates_when_cursor_empty():
    """An empty-string cursor means the backend has no more pages."""
    page = {
        "content": [_rec("other", f"x{i}") for i in range(PAGE_SIZE)],
        "last": False,
        "cursor": "",
    }
    assert _token_for(page) is None


def test_next_page_token_terminates_on_short_page():
    """A short page (len < page_size) is a terminal signal even if a cursor is
    present."""
    page = {
        "content": [_rec("other", f"x{i}") for i in range(3)],  # 3 < PAGE_SIZE
        "last": False,
        "cursor": "still-here",
    }
    assert _token_for(page) is None


def test_next_page_token_terminates_on_empty_page():
    page = {"content": [], "last": False, "cursor": "x"}
    assert _token_for(page) is None


# ---- CROSS-REPO CONTRACT / E2E: tap <-> backend handshake ----

def _page_json(records, *, cursor, last):
    """Emit one page in the EXACT backend keyset JSON shape.

    `cursor` is the opaque next-page token the backend returns; the tap echoes
    it back as the body `cursor` on the following request. `last` is the
    backend's terminal boolean.
    """
    return {
        "data": {
            "content": records,
            "size": PAGE_SIZE,
            "last": last,
            "cursor": cursor,
            "numberOfElements": len(records),
            "empty": len(records) == 0,
        }
    }


def _e2e_stream(page_jsons, *, max_iterations=50):
    """Drive the REAL request_records over a mocked transport that returns the
    given sequence of raw page JSON dicts AND asserts the tap echoes each
    backend cursor back on the next request body. We do NOT bypass
    get_next_page_token or prepare_request_payload. Iteration is capped to fail
    loudly on a runaway loop instead of hanging.
    """
    os.environ.setdefault("BASE_URL", "http://example.invalid/")
    from tap_cbx1.client import CBX1Stream

    obj = SimpleNamespace()
    obj.logger = MagicMock()
    obj.page_size = PAGE_SIZE
    obj.replication_key_field = "updatedAt"
    obj.get_starting_time = lambda ctx: None

    state = {"calls": 0, "sent_cursors": []}

    # Build the real request body each call and record the cursor the tap sent,
    # so the E2E test asserts the full handshake (backend cursor → next body).
    def _prepare_request(context, next_page_token):
        body = CBX1Stream.prepare_request_payload(obj, context, next_page_token)
        state["sent_cursors"].append(body["cursor"])
        assert "pageNumber" not in body
        return f"req-{next_page_token}"

    obj.prepare_request = _prepare_request
    obj.request_decorator = lambda fn: fn

    def _transport(*_args, **_kwargs):
        idx = state["calls"]
        state["calls"] += 1
        if state["calls"] > max_iterations:
            raise AssertionError("pagination did not terminate (runaway loop)")
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


def _three_page_sequence():
    """3 pages, 10 + 10 + 4 = 24 records; final page short with `last: True`.

    Each non-final page hands the tap an opaque cursor; the final page returns
    no cursor.
    """
    p0 = _page_json([_rec("other", f"p0-{i}") for i in range(PAGE_SIZE)], cursor="cur-1", last=False)
    p1 = _page_json([_rec("other", f"p1-{i}") for i in range(PAGE_SIZE)], cursor="cur-2", last=False)
    p2 = _page_json([_rec("other", f"p2-{i}") for i in range(4)], cursor=None, last=True)
    return [p0, p1, p2]


def test_e2e_contract_keyset_handshake(monkeypatch):
    """Full handshake: the tap seeds an empty cursor, echoes each backend
    cursor back on the next request, yields ALL 24 records, and terminates."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    pages = _three_page_sequence()
    # Sanity: this fixture is the keyset shape (cursor present, no totals).
    assert pages[0]["data"]["cursor"] == "cur-1"
    assert "totalPages" not in pages[0]["data"]

    stream, state = _e2e_stream(pages)
    out = list(stream.request_records(context=None))
    assert [r["id"] for r in out] == (
        [f"p0-{i}" for i in range(PAGE_SIZE)]
        + [f"p1-{i}" for i in range(PAGE_SIZE)]
        + [f"p2-{i}" for i in range(4)]
    )
    assert len(out) == 24
    assert state["calls"] == 3  # exactly 3 fetches, no trailing probe
    # The handshake: first request seeds "", then each backend cursor is echoed.
    assert state["sent_cursors"] == ["", "cur-1", "cur-2"]


def test_e2e_contract_missing_cursor_terminates(monkeypatch):
    """A non-keyset / older backend that returns no `cursor` (and no `last`)
    must terminate after the first page rather than loop forever."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    # Full page, last absent, cursor absent → terminate (no next token).
    p0 = {
        "data": {
            "content": [_rec("other", f"p0-{i}") for i in range(PAGE_SIZE)],
            "size": PAGE_SIZE,
            # no `last`, no `cursor`
        }
    }
    stream, state = _e2e_stream([p0])
    out = list(stream.request_records(context=None))
    assert len(out) == PAGE_SIZE
    assert state["calls"] == 1  # terminates immediately, no runaway
    assert state["sent_cursors"] == [""]


def test_e2e_contract_full_final_page_with_last_true(monkeypatch):
    """Edge: the final page is FULL (len == page_size) but carries `last: True`.
    Termination must come from `last`, not the short-page check."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    p0 = _page_json([_rec("other", f"p0-{i}") for i in range(PAGE_SIZE)], cursor="cur-1", last=False)
    p1 = _page_json([_rec("other", f"p1-{i}") for i in range(PAGE_SIZE)], cursor="cur-2", last=True)
    assert p1["data"]["last"] is True and len(p1["data"]["content"]) == PAGE_SIZE

    stream, state = _e2e_stream([p0, p1])
    out = list(stream.request_records(context=None))
    assert len(out) == 2 * PAGE_SIZE
    assert state["calls"] == 2  # full final page still stops via `last`
    assert state["sent_cursors"] == ["", "cur-1"]


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
