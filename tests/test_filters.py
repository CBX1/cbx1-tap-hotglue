"""Tests for the CBX1 tap: keyset pagination + tap-side filtering.

These stub the prepared HTTP request and the decorated request function so we
exercise `request_records` / `get_next_page_token` / `prepare_request_payload`
end-to-end without booting Singer SDK auth or schema fetches.

Pagination is KEYSET (not offset): every request is pageNumber=0 and the
`updatedAt` BETWEEN upper bound moves down to the previous page's minimum
`updatedAt`. This keeps each request on the backend's ~7ms index seek instead of
`skip(pageNumber*pageSize)`, which scanned the whole tenant partition and blew
the 1s maxTimeMS on large orgs (HotGlue SYNC_FAILED / 500 on /CONTACT).
"""

import os
from types import SimpleNamespace
from unittest.mock import MagicMock


HOTGLUE_UUID = "d6435b86-31f9-470a-97e5-33ed6a5024d5"
PAGE_SIZE = 10


# --------------------------------------------------------------------------- #
# prepare_request_payload
# --------------------------------------------------------------------------- #

def _stream(*, replication_key="updatedAt", start_date=None):
    os.environ.setdefault("BASE_URL", "http://example.invalid/")
    from tap_cbx1.client import CBX1Stream

    obj = SimpleNamespace()
    obj.page_size = PAGE_SIZE
    obj.replication_key_field = replication_key
    obj.get_starting_time = lambda ctx: start_date
    obj.prepare_request_payload = lambda ctx, tok: CBX1Stream.prepare_request_payload(obj, ctx, tok)
    return obj


def _payload(stream, token=None):
    return stream.prepare_request_payload(None, token)


def test_payload_is_always_page_zero_desc():
    """Keyset: pageNumber is pinned to 0 and sort is updatedAt DESC."""
    p = _payload(_stream())
    assert p["pageNumber"] == 0
    assert p["sortBy"] == "updatedAt"
    assert p["sortDirection"] == "DESC"
    assert p["pageSize"] == PAGE_SIZE


def test_payload_does_not_push_updatedby_filter_server_side(monkeypatch):
    """Even with the env var set, the payload must NOT carry an updatedBy filter —
    pushing $ne updatedBy to Mongo regresses HotGlue-dominant tenants."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    assert "updatedBy" not in _payload(_stream())["filters"]


def test_test_metadata_filter_always_present(monkeypatch):
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    assert _payload(_stream())["filters"]["testMetadata"] == {"type": "EQUALS", "value": None}


def test_first_page_window_has_lower_and_upper_bound():
    """First page (token None): updatedAt BETWEEN [lower, now]."""
    from pendulum import parse

    f = _payload(_stream(start_date=parse("2026-01-01T00:00:00Z")), token=None)["filters"]
    assert f["updatedAt"]["type"] == "BETWEEN"
    # lower = start_date + 1ms
    assert f["updatedAt"]["value"] == "2026-01-01T00:00:00.001000Z"
    # upper ~ now (just assert it's a populated, later timestamp)
    assert f["updatedAt"]["endValue"] > f["updatedAt"]["value"]


def test_cursor_token_becomes_the_upper_bound():
    """A keyset cursor token sets the BETWEEN endValue (the moving upper bound)."""
    from pendulum import parse

    cursor = "2026-03-15T08:30:00.123000Z"
    f = _payload(_stream(start_date=parse("2026-01-01T00:00:00Z")), token=cursor)["filters"]
    assert f["updatedAt"]["endValue"] == "2026-03-15T08:30:00.123000Z"
    assert f["updatedAt"]["value"] == "2026-01-01T00:00:00.001000Z"


def test_first_full_sync_defaults_lower_bound_to_epoch():
    """No bookmark/start_date -> still a bounded BETWEEN (epoch..now) for keyset."""
    f = _payload(_stream(start_date=None), token=None)["filters"]
    assert f["updatedAt"]["value"] == "1970-01-01T00:00:00.000000Z"
    assert f["updatedAt"]["endValue"] > f["updatedAt"]["value"]


# --------------------------------------------------------------------------- #
# request_records / get_next_page_token harness
# --------------------------------------------------------------------------- #

def _rec(_id, updated_at, updated_by="other"):
    return {"id": _id, "updatedBy": updated_by, "updatedAt": updated_at}


def _resp(records, *, last=None, number=0, total_pages=None, slice_shape=False):
    if last is None and total_pages is not None:
        last = number >= total_pages - 1
    total_elements = None if slice_shape else (total_pages * PAGE_SIZE if total_pages is not None else None)
    out = MagicMock()
    out.json.return_value = {
        "data": {
            "content": records,
            "number": number,
            "size": PAGE_SIZE,
            "first": number == 0,
            "last": last,
            "numberOfElements": len(records),
            "empty": len(records) == 0,
            "totalElements": total_elements,
            "totalPages": None if slice_shape else total_pages,
        }
    }
    return out


def _stream_with_responses(responses, *, max_iterations=50):
    """Drive the REAL request_records / get_next_page_token over a transport that
    returns the queued responses in order. The transport ignores the cursor (we
    assert the yield/dedup/termination behaviour, not server-side filtering)."""
    os.environ.setdefault("BASE_URL", "http://example.invalid/")
    from tap_cbx1.client import CBX1Stream

    obj = SimpleNamespace()
    obj.logger = MagicMock()
    obj.page_size = PAGE_SIZE
    obj.replication_key_field = "updatedAt"
    obj.prepare_request = lambda context, next_page_token: f"req::{next_page_token}"
    obj.request_decorator = lambda fn: fn

    state = {"calls": 0}

    def _transport(*_a, **_k):
        idx = state["calls"]
        state["calls"] += 1
        if state["calls"] > max_iterations:
            raise AssertionError("pagination did not terminate (runaway loop)")
        return responses[idx] if idx < len(responses) else responses[-1]

    obj._request = _transport
    obj.get_next_page_token = lambda resp, prev: CBX1Stream.get_next_page_token(obj, resp, prev)
    obj.request_records = lambda context: CBX1Stream.request_records(obj, context)
    return obj, state


def _full(prefix, ts):
    """A full page (len == PAGE_SIZE) of records all at timestamp `ts`."""
    return [_rec(f"{prefix}{i}", ts) for i in range(PAGE_SIZE)]


# ---- happy path: walk the cursor across pages -----------------------------

def test_keyset_walks_all_pages_distinct_timestamps(monkeypatch):
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    # 3 full pages, strictly-descending timestamps (no boundary ties), final short page.
    responses = [
        _resp([_rec(f"p0-{i}", f"2026-03-03T00:00:{59-i:02d}.000Z") for i in range(PAGE_SIZE)], total_pages=3, number=0),
        _resp([_rec(f"p1-{i}", f"2026-03-02T00:00:{59-i:02d}.000Z") for i in range(PAGE_SIZE)], total_pages=3, number=0),
        _resp([_rec(f"p2-{i}", f"2026-03-01T00:00:{59-i:02d}.000Z") for i in range(4)], total_pages=3, number=0),
    ]
    stream, state = _stream_with_responses(responses)
    out = list(stream.request_records(context=None))
    assert len(out) == 24
    assert out[0]["id"] == "p0-0" and out[-1]["id"] == "p2-3"
    assert state["calls"] == 3


def test_keyset_terminates_on_last_true_full_page(monkeypatch):
    """Count-free Slice: a FULL final page with last=True and null totalPages must
    terminate via `last` (no TypeError, no extra fetch)."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    responses = [
        _resp(_full("a", "2026-03-03T00:00:01.000Z"), last=False, slice_shape=True),
        _resp(_full("b", "2026-03-02T00:00:01.000Z"), last=True, slice_shape=True),
    ]
    stream, state = _stream_with_responses(responses)
    out = list(stream.request_records(context=None))
    assert len(out) == 2 * PAGE_SIZE
    assert state["calls"] == 2


# ---- boundary de-dup (inclusive cursor) -----------------------------------

def test_keyset_dedups_inclusive_boundary_record(monkeypatch):
    """The inclusive upper bound re-returns the boundary record on the next page;
    it must be emitted exactly once."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    boundary = _rec("BORDER", "2026-03-02T00:00:00.000Z")
    page0 = [_rec(f"p0-{i}", f"2026-03-03T00:00:{59-i:02d}.000Z") for i in range(PAGE_SIZE - 1)] + [boundary]
    # page1 cursor == boundary.updatedAt; backend re-includes BORDER, then older rows.
    page1 = [dict(boundary)] + [_rec(f"p1-{i}", f"2026-03-01T00:00:{59-i:02d}.000Z") for i in range(3)]
    stream, _ = _stream_with_responses([
        _resp(page0, total_pages=2, number=0),
        _resp(page1, total_pages=2, number=0),
    ])
    out = [r["id"] for r in stream.request_records(context=None)]
    assert out.count("BORDER") == 1                      # emitted once, not twice
    assert out[:PAGE_SIZE][-1] == "BORDER"
    assert "p1-0" in out


def test_keyset_keeps_unseen_tie_member_at_boundary(monkeypatch):
    """A second record sharing the boundary updatedAt that we have NOT seen yet
    must still be emitted (no data loss from the dedup)."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    ts = "2026-03-02T00:00:00.000Z"
    seen = _rec("SEEN", ts)
    page0 = [_rec(f"p0-{i}", f"2026-03-03T00:00:{59-i:02d}.000Z") for i in range(PAGE_SIZE - 1)] + [seen]
    # next page re-includes SEEN (dedup) AND a tie partner UNSEEN at the same ts.
    page1 = [dict(seen), _rec("UNSEEN", ts)] + [_rec(f"p1-{i}", f"2026-03-01T00:00:{59-i:02d}.000Z") for i in range(2)]
    stream, _ = _stream_with_responses([
        _resp(page0, total_pages=2, number=0),
        _resp(page1, total_pages=2, number=0),
    ])
    out = [r["id"] for r in stream.request_records(context=None)]
    assert out.count("SEEN") == 1
    assert "UNSEEN" in out


# ---- no-progress guard -----------------------------------------------------

def test_keyset_no_progress_guard_stops(monkeypatch):
    """Pathological: two full pages all sharing one updatedAt cannot advance an
    updatedAt-only cursor. Must stop (no infinite loop) and warn."""
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    same = "2026-03-02T00:00:00.000Z"
    stream, state = _stream_with_responses([
        _resp(_full("a", same), last=False, slice_shape=True),
        _resp(_full("b", same), last=False, slice_shape=True),
    ])
    out = list(stream.request_records(context=None))
    assert state["calls"] == 2          # bailed, did not loop forever
    assert len(out) == 2 * PAGE_SIZE     # both fetched pages were emitted
    assert stream.logger.warning.called


# ---- tap-side HotGlue-author filtering (preserved behaviour) ---------------

def test_request_records_skips_hotglue_authored(monkeypatch):
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    page = [_rec("a", "2026-03-03T00:00:03.000Z", HOTGLUE_UUID),
            _rec("b", "2026-03-03T00:00:02.000Z", "other"),
            _rec("c", "2026-03-03T00:00:01.000Z", HOTGLUE_UUID)]
    stream, _ = _stream_with_responses([_resp(page, last=True)])
    out = [r["id"] for r in stream.request_records(context=None)]
    assert out == ["b"]


def test_pagination_continues_across_all_filtered_page(monkeypatch):
    """A page where every record is HotGlue-authored must NOT stop pagination."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    responses = [
        _resp([_rec(f"a{i}", f"2026-03-03T00:00:{59-i:02d}.000Z", HOTGLUE_UUID) for i in range(PAGE_SIZE)], total_pages=3, number=0),
        _resp([_rec(f"d{i}", f"2026-03-02T00:00:{59-i:02d}.000Z", HOTGLUE_UUID) for i in range(PAGE_SIZE)], total_pages=3, number=0),
        _resp([_rec("f", "2026-03-01T00:00:02.000Z", HOTGLUE_UUID), _rec("survivor", "2026-03-01T00:00:01.000Z", "other")], total_pages=3, number=0),
    ]
    stream, _ = _stream_with_responses(responses)
    out = [r["id"] for r in stream.request_records(context=None)]
    assert out == ["survivor"]


def test_request_records_handles_missing_updatedby_field(monkeypatch):
    """Records with no updatedBy stamp must pass through (None != hotglue uuid)."""
    monkeypatch.setenv("HOTGLUE_PRINCIPAL_ID", HOTGLUE_UUID)
    page = [_rec("legacy", "2026-03-03T00:00:03.000Z", None),
            _rec("skip", "2026-03-03T00:00:02.000Z", HOTGLUE_UUID),
            _rec("keep", "2026-03-03T00:00:01.000Z", "other")]
    stream, _ = _stream_with_responses([_resp(page, last=True)])
    out = [r["id"] for r in stream.request_records(context=None)]
    assert out == ["legacy", "keep"]


def test_request_records_handles_empty_page(monkeypatch):
    monkeypatch.delenv("HOTGLUE_PRINCIPAL_ID", raising=False)
    stream, state = _stream_with_responses([_resp([], last=True)])
    assert list(stream.request_records(context=None)) == []
    assert state["calls"] == 1


# --------------------------------------------------------------------------- #
# get_next_page_token: cursor + termination contract
# --------------------------------------------------------------------------- #

def _token_for(page_data):
    from tap_cbx1.client import CBX1Stream

    obj = SimpleNamespace()
    obj.page_size = PAGE_SIZE
    obj.replication_key_field = "updatedAt"
    resp = MagicMock()
    resp.json.return_value = {"data": page_data}
    return CBX1Stream.get_next_page_token(obj, resp, previous_token=None)


def test_token_is_min_updatedat_on_full_page():
    """A full page returns the last (minimum, DESC) record's updatedAt as cursor."""
    recs = [_rec(f"x{i}", f"2026-03-03T00:00:{59-i:02d}.000Z") for i in range(PAGE_SIZE)]
    page = {"content": recs, "number": 0, "last": False, "totalPages": 5}
    assert _token_for(page) == recs[-1]["updatedAt"]


def test_token_none_on_last_true_full_page():
    recs = _full("x", "2026-03-03T00:00:01.000Z")
    page = {"content": recs, "number": 5, "last": True}  # totalPages absent (slice)
    assert _token_for(page) is None


def test_token_none_on_short_page():
    page = {"content": [_rec(f"x{i}", "2026-03-03T00:00:01.000Z") for i in range(3)], "number": 0}
    assert _token_for(page) is None


def test_token_none_on_empty_page():
    assert _token_for({"content": [], "number": 0}) is None


def test_token_legacy_totalpages_fallback():
    recs = _full("x", "2026-03-03T00:00:01.000Z")
    assert _token_for({"content": recs, "number": 2, "totalPages": 3}) is None        # 2 >= 3-1
    assert _token_for({"content": recs, "number": 0, "totalPages": 3}) == recs[-1]["updatedAt"]


# --------------------------------------------------------------------------- #
# bookmark signpost
# --------------------------------------------------------------------------- #

def test_replication_key_signpost_returns_now_not_none():
    from datetime import datetime, timezone
    from tap_cbx1.client import CBX1Stream

    signpost = CBX1Stream.get_replication_key_signpost(SimpleNamespace(), context=None)
    assert signpost is not None
    assert abs((datetime.now(timezone.utc) - signpost).total_seconds()) < 5
