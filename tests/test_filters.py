"""Tests for tap-side filtering and durable keyset (cursor) pagination.

These tests stub the prepared HTTP request and the decorated request function so we
exercise ``request_records`` end-to-end without booting Singer SDK auth or schema
fetches.

Pagination uses keyset/cursor paging: the request body carries a ``cursor`` (empty
string on the first page) instead of a ``pageNumber``, and the backend returns the
next ``data.cursor``. We never compute ``skip = pageNumber * pageSize``, which on
large orgs forces Mongo to walk every skipped index entry per page (CM100 timeout /
CPU spike).

Forward progress is durable: after each page's records are emitted, the next-page
cursor and the PINNED window upper bound are persisted to Singer state, so a run that
ends partway resumes from exactly the unread remainder. The run-to-run ``updatedAt``
bookmark advances only on confirmed window completion.
"""

import copy
import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pendulum import parse


HOTGLUE_UUID = "d6435b86-31f9-470a-97e5-33ed6a5024d5"
PAGE_SIZE = 10


def _cbx1():
    os.environ.setdefault("BASE_URL", "http://example.invalid/")
    from tap_cbx1.client import CBX1Stream

    return CBX1Stream


def _rec(updated_by, _id="id", updated_at="2026-01-01T00:00:00.000Z"):
    return {"id": _id, "updatedBy": updated_by, "updatedAt": updated_at}


def _full_page(updated_by, prefix):
    """A full page (len == PAGE_SIZE) of identically-authored records.

    Non-final pages must be full; a short page is itself a terminal signal.
    """
    return [_rec(updated_by, f"{prefix}{i}") for i in range(PAGE_SIZE)]


# =====================================================================================
# prepare_request_payload: keyset cursor, pinned window, no pageNumber
# =====================================================================================

def _payload_stream(*, replication_key=None, start_date=None, stream_state=None, page_size=PAGE_SIZE):
    """Minimal stream-like object for prepare_request_payload tests."""
    CBX1Stream = _cbx1()
    obj = SimpleNamespace()
    obj.page_size = page_size
    obj.replication_key_field = replication_key
    obj.stream_state = {} if stream_state is None else stream_state
    obj.get_starting_time = lambda ctx: start_date
    obj._resume_state = lambda: CBX1Stream._resume_state(obj)
    obj.prepare_request_payload = lambda ctx, tok: CBX1Stream.prepare_request_payload(obj, ctx, tok)
    return obj


def _payload(stream, token=""):
    return stream.prepare_request_payload(None, token)


def _filters(stream):
    return _payload(stream)["filters"]


def test_payload_uses_cursor_not_pagenumber(monkeypatch):
    """Keyset contract: the body carries a `cursor`, never a `pageNumber`."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    payload = _payload(_payload_stream(), token="")
    assert "pageNumber" not in payload
    assert "cursor" in payload


def test_payload_cursor_empty_string_on_first_page(monkeypatch):
    """First page: a None token (the seed) maps to the empty string, which signals
    keyset mode to the backend."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    assert _payload(_payload_stream(), token=None)["cursor"] == ""
    assert _payload(_payload_stream(), token="")["cursor"] == ""


def test_payload_echoes_backend_cursor_token(monkeypatch):
    """Subsequent pages: the opaque backend token is sent back verbatim."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    token = "eyJ1cGRhdGVkQXQiOiIyMDI2LTAxLTAxIiwiaWQiOiJhYmMifQ=="
    assert _payload(_payload_stream(), token=token)["cursor"] == token


def test_payload_keeps_sort_and_page_size(monkeypatch):
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    payload = _payload(_payload_stream(replication_key="updatedAt"), token="")
    assert payload["pageSize"] == PAGE_SIZE
    assert payload["sortBy"] == "updatedAt"
    assert payload["sortDirection"] == "DESC"


def test_payload_does_not_push_updatedby_filter_server_side(monkeypatch):
    """Pushing $ne updatedBy to Mongo regresses tenants where HotGlue dominates."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    assert "updatedBy" not in _filters(_payload_stream())


def test_test_metadata_filter_always_present(monkeypatch):
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    assert _filters(_payload_stream())["testMetadata"] == {"type": "EQUALS", "value": None}


def test_payload_includes_replication_key_when_set(monkeypatch):
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    stream = _payload_stream(replication_key="updatedAt", start_date=parse("2026-01-01T00:00:00Z"))
    filters = _filters(stream)
    assert filters["updatedAt"]["type"] == "BETWEEN"
    assert "updatedBy" not in filters


def test_payload_pins_window_end_from_saved_state(monkeypatch):
    """On a resume run the BETWEEN endValue is the PINNED window_end from state, not
    a fresh now() — otherwise a DESC scan would skip records that arrived between
    runs."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    saved_window_end = "2026-06-10T12:00:00.000000Z"
    stream = _payload_stream(
        replication_key="updatedAt",
        start_date=parse("2026-06-01T00:00:00Z"),
        stream_state={"cursor": "c2", "window_end": saved_window_end},
    )
    assert _filters(stream)["updatedAt"]["endValue"] == saved_window_end


# =====================================================================================
# page_size config knob
# =====================================================================================

def test_page_size_default_override_and_invalid():
    CBX1Stream = _cbx1()
    assert CBX1Stream.page_size.fget(SimpleNamespace(config={})) == 100
    assert CBX1Stream.page_size.fget(SimpleNamespace(config={"page_size": 250})) == 250
    # An invalid value falls back to the default rather than crashing the run.
    assert CBX1Stream.page_size.fget(SimpleNamespace(config={"page_size": "nope"})) == 100


# =====================================================================================
# request_records driver harness
# =====================================================================================

def _run_stream(pages=None, *, transport=None, stream_state=None, replication_key="updatedAt",
                start_date=None, page_size=PAGE_SIZE, max_iterations=50):
    """Drive the REAL request_records over a mocked transport.

    Provides a live ``stream_state`` dict, captures every STATE emission as a deep
    snapshot in ``obj.states``, records each sent body cursor in ``obj.sent_cursors``,
    and keeps the last request body in ``obj.last_body``. Pages are dicts:
    ``{"records": [...], "cursor": <token-or-None>, "last": <bool-or-None>}``.
    """
    CBX1Stream = _cbx1()
    obj = SimpleNamespace()
    obj.logger = MagicMock()
    obj.page_size = page_size
    obj.replication_key = replication_key
    obj.replication_key_field = "updatedAt"
    obj.stream_state = {} if stream_state is None else stream_state
    obj.get_starting_time = lambda ctx: start_date
    obj.states = []
    obj.sent_cursors = []
    obj.last_body = None

    def _prepare(context, next_page_token):
        body = CBX1Stream.prepare_request_payload(obj, context, next_page_token)
        obj.sent_cursors.append(body["cursor"])
        obj.last_body = body
        return f"req-{next_page_token}"

    obj.prepare_request = _prepare
    obj.request_decorator = lambda fn: fn
    obj._write_state_message = lambda: obj.states.append(copy.deepcopy(obj.stream_state))

    if transport is not None:
        obj._request = transport
    else:
        calls = {"n": 0}

        def _t(*_a, **_k):
            i = calls["n"]
            calls["n"] += 1
            if calls["n"] > max_iterations:
                raise AssertionError("pagination did not terminate (runaway loop)")
            page = pages[i] if i < len(pages) else pages[-1]
            resp = MagicMock()
            resp.json.return_value = {
                "data": {
                    "content": page["records"],
                    "cursor": page.get("cursor"),
                    "last": page.get("last"),
                    "size": page_size,
                }
            }
            return resp

        obj._request = _t

    obj.get_next_page_token = lambda resp, prev: CBX1Stream.get_next_page_token(obj, resp, prev)
    obj._resume_state = lambda: CBX1Stream._resume_state(obj)
    obj.request_records = lambda context: CBX1Stream.request_records(obj, context)
    return obj


# ---- tap-side HotGlue filter ----

def test_request_records_skips_hotglue_authored(monkeypatch):
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        {"records": [_rec(HOTGLUE_UUID, "a"), _rec("other", "b"), _rec(HOTGLUE_UUID, "c")],
         "cursor": None, "last": True},
    ]
    out = list(_run_stream(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["b"]


def test_request_records_passthrough_when_env_unset(monkeypatch):
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    pages = [
        {"records": [_rec(HOTGLUE_UUID, "a"), _rec("other", "b")], "cursor": None, "last": True},
    ]
    out = list(_run_stream(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["a", "b"]


def test_pagination_continues_across_all_filtered_page(monkeypatch):
    """A page where every record is HotGlue must NOT stop pagination — the cursor
    drives the next fetch regardless of how many records survived the filter."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        {"records": _full_page(HOTGLUE_UUID, "a"), "cursor": "c1", "last": False},
        {"records": _full_page(HOTGLUE_UUID, "d"), "cursor": "c2", "last": False},
        {"records": [_rec(HOTGLUE_UUID, "f"), _rec("other", "survivor")], "cursor": None, "last": True},
    ]
    out = list(_run_stream(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["survivor"]


def test_pagination_handles_empty_response_page(monkeypatch):
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [{"records": [_rec("other", "a")], "cursor": None, "last": True}]
    out = list(_run_stream(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["a"]


def test_request_records_handles_missing_updatedby_field(monkeypatch):
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    pages = [
        {"records": [_rec(None, "legacy"), _rec(HOTGLUE_UUID, "skip"), _rec("other", "keep")],
         "cursor": None, "last": True},
    ]
    out = list(_run_stream(pages).request_records(context=None))
    assert [r["id"] for r in out] == ["legacy", "keep"]


def test_request_records_handles_missing_data_key(monkeypatch):
    """Defensive parse: a response with no `data` key must not raise (Mycroft #3)."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)

    def transport(*_a, **_k):
        resp = MagicMock()
        resp.json.return_value = {}  # no 'data'
        return resp

    out = list(_run_stream(transport=transport).request_records(context=None))
    assert out == []


# =====================================================================================
# durable forward progress: mid-run failure, resume, completion
# =====================================================================================

def test_mid_run_failure_persists_resume_cursor(monkeypatch):
    """Acceptance: transport raises while fetching page k -> pages < k are emitted, a
    STATE carrying the page-k cursor + pinned window is persisted, and the run-to-run
    watermark is NOT advanced (no false progress)."""
    from singer_sdk.exceptions import RetriableAPIError

    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    calls = {"n": 0}
    p0 = {"records": [_rec("other", f"p0-{i}") for i in range(PAGE_SIZE)], "cursor": "c1", "last": False}
    p1 = {"records": [_rec("other", f"p1-{i}") for i in range(PAGE_SIZE)], "cursor": "c2", "last": False}

    def transport(*_a, **_k):
        i = calls["n"]
        calls["n"] += 1
        if i == 0:
            page = p0
        elif i == 1:
            page = p1
        else:
            raise RetriableAPIError("transport boom fetching page 2")
        resp = MagicMock()
        resp.json.return_value = {"data": {"content": page["records"], "cursor": page["cursor"],
                                           "last": page["last"], "size": PAGE_SIZE}}
        return resp

    obj = _run_stream(transport=transport, stream_state={}, start_date=parse("2026-01-01T00:00:00Z"))
    out = []
    with pytest.raises(RetriableAPIError):
        for r in obj.request_records(context=None):
            out.append(r)

    assert [r["id"] for r in out] == (
        [f"p0-{i}" for i in range(PAGE_SIZE)] + [f"p1-{i}" for i in range(PAGE_SIZE)]
    )
    # Resume point persisted: cursor for the unread page 2 + a pinned window_end.
    assert obj.stream_state["cursor"] == "c2"
    assert "window_end" in obj.stream_state
    # The run-to-run bookmark must NOT have advanced (partial run).
    assert "replication_key_value" not in obj.stream_state


def test_resume_reads_only_remainder_against_pinned_window(monkeypatch):
    """Acceptance: a resume run seeds the saved cursor, reuses the PINNED window
    (not a fresh now()), emits only the remainder, and on completion advances the
    watermark to the pinned window and clears the cursor."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    saved_window_end = "2026-06-10T12:00:00.000000Z"
    state = {
        "cursor": "c2",
        "window_end": saved_window_end,
        "replication_key": "updatedAt",
        "replication_key_value": "2026-06-01T00:00:00.000000Z",
    }
    pages = [{"records": [_rec("other", f"p2-{i}") for i in range(3)], "cursor": None, "last": True}]
    obj = _run_stream(pages, stream_state=state, start_date=parse("2026-06-01T00:00:00Z"))

    out = list(obj.request_records(context=None))
    assert [r["id"] for r in out] == [f"p2-{i}" for i in range(3)]
    # Resumed from the saved cursor, not "".
    assert obj.sent_cursors[0] == "c2"
    # Used the pinned window_end as the upper bound (no skip of between-run arrivals).
    assert obj.last_body["filters"]["updatedAt"]["endValue"] == saved_window_end
    # Completion advanced the watermark to the pinned window_end and cleared resume state.
    assert obj.stream_state["replication_key_value"] == saved_window_end
    assert obj.stream_state["replication_key"] == "updatedAt"
    assert "cursor" not in obj.stream_state
    assert "window_end" not in obj.stream_state


def test_completion_advances_watermark_to_pinned_window(monkeypatch):
    """Acceptance: on full window completion the watermark advances to the pinned
    window upper bound (== the endValue actually sent) and the cursor is cleared."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    pages = [
        {"records": [_rec("other", f"p0-{i}") for i in range(PAGE_SIZE)], "cursor": "c1", "last": False},
        {"records": [_rec("other", "last")], "cursor": None, "last": True},
    ]
    obj = _run_stream(pages, stream_state={}, start_date=parse("2026-01-01T00:00:00Z"))

    out = list(obj.request_records(context=None))
    assert len(out) == PAGE_SIZE + 1
    assert obj.stream_state.get("cursor") is None
    assert obj.stream_state.get("window_end") is None
    assert obj.stream_state["replication_key"] == "updatedAt"
    # The committed watermark equals the pinned upper bound used for the BETWEEN filter.
    assert obj.stream_state["replication_key_value"] == obj.last_body["filters"]["updatedAt"]["endValue"]


def test_zero_record_window_still_advances_watermark(monkeypatch):
    """A window that yields no records (empty first page, last=true) still advances
    the bookmark — completion, not record count, drives advancement."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    pages = [{"records": [], "cursor": None, "last": True}]
    obj = _run_stream(pages, stream_state={}, start_date=parse("2026-01-01T00:00:00Z"))

    out = list(obj.request_records(context=None))
    assert out == []
    assert obj.stream_state["replication_key_value"] == obj.last_body["filters"]["updatedAt"]["endValue"]


# =====================================================================================
# get_next_page_token: completion vs loud-failure contract (Mycroft #1)
# =====================================================================================

def _token_for(page_data):
    CBX1Stream = _cbx1()
    obj = SimpleNamespace()
    obj.page_size = PAGE_SIZE
    resp = MagicMock()
    resp.json.return_value = {"data": page_data}
    return CBX1Stream.get_next_page_token(obj, resp, previous_token=None)


def test_next_page_token_returns_cursor_on_full_non_last_page():
    page = {"content": [_rec("other", f"x{i}") for i in range(PAGE_SIZE)], "last": False, "cursor": "next-abc"}
    assert _token_for(page) == "next-abc"


def test_next_page_token_terminates_on_last_true():
    page = {"content": [_rec("other", f"x{i}") for i in range(PAGE_SIZE)], "last": True, "cursor": "ignored"}
    assert _token_for(page) is None


def test_next_page_token_terminates_on_short_page_without_cursor():
    page = {"content": [_rec("other", "a")], "last": False}  # short, no cursor -> clean end
    assert _token_for(page) is None


def test_next_page_token_terminates_on_short_page_with_cursor():
    page = {"content": [_rec("other", "a")], "last": False, "cursor": "still-here"}
    assert _token_for(page) is None


def test_next_page_token_terminates_on_empty_page():
    assert _token_for({"content": [], "last": False}) is None


def test_next_page_token_raises_on_full_page_without_cursor():
    """The data-loss guard (Mycroft #1): a FULL page that is not flagged `last` and
    carries no cursor cannot be advanced safely. Rather than terminate silently
    (which would let the bookmark jump to ~now and drop the unread tail — the
    signature of an older/non-keyset backend), fail loudly."""
    page = {"content": [_rec("other", f"x{i}") for i in range(PAGE_SIZE)], "last": False}  # no cursor
    with pytest.raises(RuntimeError):
        _token_for(page)


def test_request_records_raises_on_incompatible_backend_without_advancing(monkeypatch):
    """End-to-end of the guard: a full page + no cursor + not last makes the run fail
    loudly, emits NO records for that page, and does NOT advance the watermark."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    pages = [{"records": _full_page("other", "p0"), "cursor": None, "last": None}]
    obj = _run_stream(pages, stream_state={}, start_date=parse("2026-01-01T00:00:00Z"))

    out = []
    with pytest.raises(RuntimeError):
        for r in obj.request_records(context=None):
            out.append(r)
    assert out == []  # anomaly detected before emitting
    assert "replication_key_value" not in obj.stream_state


# =====================================================================================
# signpost: now manual (no SDK signpost)
# =====================================================================================

def test_replication_key_signpost_is_none():
    """Watermark advancement is manual now (see _increment_stream_state). The SDK
    signpost is therefore intentionally None."""
    CBX1Stream = _cbx1()
    assert CBX1Stream.get_replication_key_signpost(SimpleNamespace(), context=None) is None


def test_increment_stream_state_is_noop():
    """The SDK's ascending high-watermark is disabled; advancement is manual."""
    CBX1Stream = _cbx1()
    obj = SimpleNamespace()
    # Must not raise and must not touch any state.
    assert CBX1Stream._increment_stream_state(obj, {"updatedAt": "2026-01-01T00:00:00Z"}) is None


# =====================================================================================
# CROSS-REPO CONTRACT / E2E handshake
# =====================================================================================

def test_e2e_contract_keyset_handshake(monkeypatch):
    """Full handshake: seed an empty cursor, echo each backend cursor back on the
    next request, yield ALL 24 records, terminate in exactly 3 fetches, and persist
    a cursor at each page boundary."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    pages = [
        {"records": [_rec("other", f"p0-{i}") for i in range(PAGE_SIZE)], "cursor": "cur-1", "last": False},
        {"records": [_rec("other", f"p1-{i}") for i in range(PAGE_SIZE)], "cursor": "cur-2", "last": False},
        {"records": [_rec("other", f"p2-{i}") for i in range(4)], "cursor": None, "last": True},
    ]
    obj = _run_stream(pages, stream_state={}, start_date=parse("2026-01-01T00:00:00Z"))
    out = list(obj.request_records(context=None))

    assert [r["id"] for r in out] == (
        [f"p0-{i}" for i in range(PAGE_SIZE)]
        + [f"p1-{i}" for i in range(PAGE_SIZE)]
        + [f"p2-{i}" for i in range(4)]
    )
    assert len(out) == 24
    assert obj.sent_cursors == ["", "cur-1", "cur-2"]
    # Intermediate STATE snapshots carried the resume cursor; final cleared it.
    assert obj.states[0]["cursor"] == "cur-1"
    assert "cursor" not in obj.states[-1]


def test_e2e_contract_full_final_page_with_last_true(monkeypatch):
    """Edge: a FULL final page (len == page_size) terminates via `last`, not the
    short-page check."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    pages = [
        {"records": [_rec("other", f"p0-{i}") for i in range(PAGE_SIZE)], "cursor": "cur-1", "last": False},
        {"records": [_rec("other", f"p1-{i}") for i in range(PAGE_SIZE)], "cursor": "cur-2", "last": True},
    ]
    obj = _run_stream(pages, stream_state={}, start_date=parse("2026-01-01T00:00:00Z"))
    out = list(obj.request_records(context=None))
    assert len(out) == 2 * PAGE_SIZE
    assert obj.sent_cursors == ["", "cur-1"]


# =====================================================================================
# AccountStream: narrow the exception catch (Mycroft #2)
# =====================================================================================

def _account_instance(state=None):
    """A real AccountStream instance whose stream_state is backed by tap_state.

    (stream_state is a read-only property, so it cannot be set as an attribute.)
    """
    from tap_cbx1.streams import AccountStream

    inst = object.__new__(AccountStream)
    inst.name = "accounts"
    inst._tap_state = {"bookmarks": {"accounts": dict(state or {})}}
    return inst


def _account_super_raises(monkeypatch, exc):
    """Patch the parent CBX1Stream.request_records to raise `exc` immediately (before
    any progress), so AccountStream's `yield from super()...` triggers its handler."""
    import tap_cbx1.client as client_mod

    def raising(self, context):
        raise exc
        yield  # pragma: no cover - makes this a generator

    monkeypatch.setattr(client_mod.CBX1Stream, "request_records", raising)


def test_account_stream_swallows_fatal_on_fresh_first_page(monkeypatch):
    """A missing ACCOUNT mapping fails the FIRST fetch with a 4xx (FatalAPIError)
    before any progress -> swallowed (yield nothing) rather than failing the run."""
    from singer_sdk.exceptions import FatalAPIError
    from tap_cbx1.streams import AccountStream

    _account_super_raises(monkeypatch, FatalAPIError("400 Client Error: mapping not found"))
    out = list(AccountStream.request_records(_account_instance({}), context=None))
    assert out == []


def test_account_stream_propagates_retriable_error(monkeypatch):
    """A transient failure (5xx/timeout -> RetriableAPIError) must NOT be swallowed."""
    from singer_sdk.exceptions import RetriableAPIError
    from tap_cbx1.streams import AccountStream

    _account_super_raises(monkeypatch, RetriableAPIError("503 Service Unavailable"))
    with pytest.raises(RetriableAPIError):
        list(AccountStream.request_records(_account_instance({}), context=None))


def test_account_stream_propagates_keyset_anomaly(monkeypatch):
    """The incompatible-backend RuntimeError guard must also propagate (no swallow)."""
    from tap_cbx1.streams import AccountStream

    _account_super_raises(monkeypatch, RuntimeError("full page, no cursor, not last"))
    with pytest.raises(RuntimeError):
        list(AccountStream.request_records(_account_instance({}), context=None))


def _account_run_instance(transport, *, state=None, start_date=None):
    """A real AccountStream instance wired to drive the REAL parent request_records
    over `transport`, so cursor persistence actually happens. stream_state is backed
    by tap_state and page_size by config (both are read-only properties)."""
    from tap_cbx1.streams import AccountStream

    inst = object.__new__(AccountStream)
    inst.name = "accounts"
    inst._tap_state = {"bookmarks": {"accounts": dict(state or {})}}
    inst._config = {"page_size": PAGE_SIZE}
    inst.get_starting_time = lambda ctx: start_date
    inst.prepare_request = lambda context, next_page_token: f"req-{next_page_token}"
    inst.request_decorator = lambda fn: fn
    inst._request = transport
    inst._write_state_message = lambda: None
    return inst


def test_account_stream_propagates_fatal_after_progress_no_wedge(monkeypatch):
    """Regression (the persisted-cursor wedge): page 1 succeeds (cursor persisted),
    then page 2 raises FatalAPIError. The error MUST propagate, not be swallowed —
    otherwise the persisted cursor would make every later run resume to the same
    failing page, swallow again, and never advance or clear (permanent wedge)."""
    from singer_sdk.exceptions import FatalAPIError
    from tap_cbx1.streams import AccountStream

    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    calls = {"n": 0}
    p0 = [_rec("other", f"p0-{i}") for i in range(PAGE_SIZE)]

    def transport(*_a, **_k):
        i = calls["n"]
        calls["n"] += 1
        if i == 0:
            resp = MagicMock()
            resp.json.return_value = {"data": {"content": p0, "cursor": "c1", "last": False, "size": PAGE_SIZE}}
            return resp
        raise FatalAPIError("413 Client Error mid-window")

    inst = _account_run_instance(transport, state={})
    out = []
    with pytest.raises(FatalAPIError):
        for r in AccountStream.request_records(inst, context=None):
            out.append(r)

    assert [r["id"] for r in out] == [f"p0-{i}" for i in range(PAGE_SIZE)]
    # Progress was persisted and the mid-window 4xx propagated (loud), not swallowed.
    assert inst.stream_state["cursor"] == "c1"


def test_account_stream_swallows_fatal_only_before_progress_with_all_filtered_page(monkeypatch):
    """Edge of the guard: page 1 is a FULL page of all-HotGlue records (yields
    nothing) so a cursor IS persisted, then page 2 raises FatalAPIError. Even though
    nothing was yielded, the persisted cursor means progress was made -> propagate."""
    from singer_sdk.exceptions import FatalAPIError
    from tap_cbx1.streams import AccountStream

    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    calls = {"n": 0}
    all_hotglue = _full_page(HOTGLUE_UUID, "p0")

    def transport(*_a, **_k):
        i = calls["n"]
        calls["n"] += 1
        if i == 0:
            resp = MagicMock()
            resp.json.return_value = {"data": {"content": all_hotglue, "cursor": "c1", "last": False, "size": PAGE_SIZE}}
            return resp
        raise FatalAPIError("400 Client Error mid-window")

    inst = _account_run_instance(transport, state={})
    with pytest.raises(FatalAPIError):
        list(AccountStream.request_records(inst, context=None))
    assert inst.stream_state["cursor"] == "c1"
