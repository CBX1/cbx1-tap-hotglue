"""REST client handling, including CBX1Stream base class."""

from typing import Any, Iterable, Optional, TypeVar
from functools import cached_property
import requests
from pendulum import parse
from singer_sdk.streams import RESTStream
import singer
import os
from singer import StateMessage
from tap_cbx1.auth import TapCBX1Auth
from tap_cbx1.schema_utils import fetch_schema_from_api
from datetime import timedelta
from tap_cbx1.constants import (
    CRM_KEY,
    HOTGLUE_PRINCIPAL_ID_ENV,
    PAGE_SIZE_KEY,
    DEFAULT_PAGE_SIZE,
)

_TToken = TypeVar("_TToken")

# Extra Singer-state bookmark fields (alongside the SDK's replication_key_value).
# Persisting these makes intra-run keyset progress durable: a run that ends partway
# leaves a resume point so the next run reads strictly the unread remainder.
_CURSOR_STATE_KEY = "cursor"
_WINDOW_END_STATE_KEY = "window_end"
_ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class CBX1Stream(RESTStream):
    """CBX1 stream class."""

    # Target name for schema discovery (e.g., "accounts", "contacts")
    target_name = None

    @property
    def url_base(self):
        return os.getenv("BASE_URL") + "api/t/v1/targets/integrations"

    rest_method = "POST"
    replication_key_field = "updatedAt"

    @property
    def page_size(self) -> int:
        """Records per page, configurable via the ``page_size`` setting.

        Keyset pagination has no per-page ``skip`` cost, so a larger page is not
        more expensive on the backend — it simply cuts the number of round-trips
        (200k records: ~400 requests at 500 vs ~20k at 10). Falls back to
        ``DEFAULT_PAGE_SIZE`` on a missing/invalid value.
        """
        try:
            return int(self.config.get(PAGE_SIZE_KEY, DEFAULT_PAGE_SIZE))
        except (TypeError, ValueError):
            return DEFAULT_PAGE_SIZE

    @property
    def authenticator(self) -> TapCBX1Auth:
        """Return a new authenticator object."""
        return TapCBX1Auth.create_for_stream(self)

    def _resume_state(self):
        """Resolve ``(initial_token, pinned_window_end)`` for THIS run, memoized once.

        Resumes from a persisted ``(cursor, window_end)`` pair when BOTH are present
        (a prior run ended partway); otherwise starts a fresh window whose upper
        bound is pinned at ``now()``.

        The window upper bound is PINNED and reused on resume on purpose: the sort
        is DESC (newest -> oldest), so recomputing ``now()`` on a resume run would
        let records that arrived between runs slip in at the top of the window and
        be skipped. Any half-written resume state (a cursor without a window_end) is
        discarded and treated as a fresh window.
        """
        if getattr(self, "_resume_cache", None) is None:
            state = self.stream_state
            saved_cursor = state.get(_CURSOR_STATE_KEY)
            saved_window_end = state.get(_WINDOW_END_STATE_KEY)
            if saved_cursor and saved_window_end:
                self._resume_cache = (saved_cursor, parse(saved_window_end))
            else:
                state.pop(_CURSOR_STATE_KEY, None)
                state.pop(_WINDOW_END_STATE_KEY, None)
                self._resume_cache = ("", parse("now"))
        return self._resume_cache

    def get_next_page_token(
            self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return the next keyset cursor, ``None`` on clean completion, or raise.

        Uses keyset (cursor) pagination, NOT page-number/skip pagination.

        WHY keyset: with skip-based paging the backend computes ``skip =
        pageNumber * pageSize`` and Mongo walks every skipped index entry on every
        page, so deep pages cost progressively more and pin the prod primary (a
        ``CM100`` query timeout / CPU spike). The cursor is the backend's opaque
        ``(updatedAt, id)`` keyset token: each page seeks directly to its starting
        key, so every page costs the same regardless of depth. The compound ``id``
        tiebreaker is required because Mongo stores ``updatedAt`` at millisecond
        precision and a bulk write shares one millisecond across many documents.

        Returns:
            - ``None`` for a clean end of the window: the backend flags ``last``,
              or returns a short page (``len(content) < page_size``).
            - the next ``cursor`` token when a full page reports more data.

        Raises:
            RuntimeError: a full page reports it is NOT the last page yet carries no
                cursor. Terminating here would let the bookmark advance to ~now()
                and silently drop the unread older tail — the signature of an
                older/non-keyset backend (rolling deploy / rollback). Fail loudly so
                the run does not record false progress; deploy the keyset backend
                first.
        """
        page_data = response.json().get('data') or {}
        content = page_data.get('content') or []

        # Primary signal: the backend says this is the final page.
        if page_data.get('last') is True:
            return None

        cursor = page_data.get('cursor')
        if cursor:
            # A short page alongside a cursor still means nothing follows it.
            if len(content) < self.page_size:
                return None
            return cursor

        # No cursor and not flagged last: a short/empty page means we are done
        # (e.g. a legacy short final page); a FULL page means the backend cannot
        # hand us a resume point even though more data exists -> fail loudly.
        if len(content) < self.page_size:
            return None

        raise RuntimeError(
            f"Egestion list returned a full page (size {len(content)}) with no "
            "keyset `cursor` and `last` != true. Refusing to terminate silently — "
            "that would drop the unread remainder of the window. Is the backend on "
            "the keyset-cursor build?"
        )

    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        if start_date:
            start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def get_url(self, context: dict | None) -> str:
        crm = self.config.get(CRM_KEY)
        url = "".join([self.url_base, self.path or "", f"/{crm}/list"])
        return url

    def get_url_params(
            self,
            context: dict | None,
            next_page_token: Any | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"deanonymizePIIData": "true"}
        return params

    def prepare_request_payload(
            self,
            context: dict | None,
            next_page_token: _TToken | None,
    ) -> dict | None:
        start_date = self.get_starting_time(context)
        _, window_end = self._resume_state()

        # Keyset pagination: send the opaque cursor, never a pageNumber/skip.
        # An empty-string cursor on the first page signals keyset mode to the
        # backend; subsequent pages echo back the token the backend returned.
        payload = {
            "cursor": next_page_token if next_page_token is not None else "",
            "pageSize": self.page_size,
            "sortBy": self.replication_key_field,
            "sortDirection": "DESC",
        }

        # Always filter out test records (testMetadata: null means not a test record)
        filters = {
            "testMetadata": {
                "type": "EQUALS",
                "value": None
            }
        }

        if self.replication_key_field and start_date:
            # Increment start date by 1 millisecond
            start_date = start_date + timedelta(milliseconds=1)
            iso_start_date = start_date.strftime(_ISO_FORMAT)
            # PINNED window upper bound (not a fresh now()): resume runs reuse the
            # same endValue so a DESC scan cannot skip records that arrived between
            # runs. See _resume_state.
            iso_window_end = window_end.strftime(_ISO_FORMAT)
            filters[self.replication_key_field] = {
                "type": "BETWEEN",
                "value": iso_start_date,
                "endValue": iso_window_end
            }

        payload["filters"] = filters
        return payload

    @property
    def http_headers(self) -> dict:
        result = self._http_headers
        return result

    def _increment_stream_state(self, latest_record, *, context=None) -> None:
        """Disabled: the run-to-run watermark is advanced manually (request_records).

        The SDK's record-driven high-watermark assumes ASCENDING replication-key
        order and finalizes on ANY clean generator return. Our egestion read is
        DESC (newest first), so letting it run would commit the NEWEST updatedAt
        (seen on page 1) as the bookmark on a partial run and then silently skip the
        unread older tail. We instead advance ``replication_key_value`` only on
        confirmed window completion.
        """
        return None

    def get_replication_key_signpost(self, context: Optional[dict]) -> Optional[Any]:
        """No signpost — watermark advancement is manual (see _increment_stream_state).

        With auto-increment disabled the signpost would be inert anyway; returning
        None keeps the state clean. Window completion advances the bookmark to the
        pinned window upper bound even when zero records were yielded.
        """
        return None

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """Yield records for the pinned window, persisting a durable keyset cursor.

        Forward progress is durable across runs: after every page's records have
        been emitted, the next-page cursor and the pinned window upper bound are
        written to Singer state. If the run ends partway (transport error, OOM, wall
        clock), the next run resumes from that cursor against the same window and
        reads only the unread remainder — no record skipped or duplicated. On clean
        completion the run-to-run ``updatedAt`` bookmark is advanced to the pinned
        upper bound and the cursor is cleared, so the next run starts a fresh window.
        """
        token, window_end = self._resume_state()
        window_end_iso = window_end.strftime(_ISO_FORMAT)
        replication_key = self.replication_key or self.replication_key_field
        state = self.stream_state

        decorated_request = self.request_decorator(self._request)
        finished = False
        # Tap-side filter: drop records HotGlue itself last-modified to avoid re-ingesting our own
        # writes. Pushed-down `$ne updatedBy` regresses Mongo on tenants where HotGlue dominates.
        hotglue_principal_id = os.getenv(HOTGLUE_PRINCIPAL_ID_ENV)
        skipped = 0

        while not finished:
            prepared_request = self.prepare_request(context, next_page_token=token)
            resp = decorated_request(prepared_request, context)
            content = (resp.json().get('data') or {}).get('content') or []

            # Classify the response BEFORE emitting: an incompatible/unsafe page
            # (full page, not last, no cursor) raises here so its records are never
            # emitted and the run fails without recording false progress.
            next_token = self.get_next_page_token(resp, token)

            for record in content:
                if hotglue_principal_id and record.get("updatedBy") == hotglue_principal_id:
                    skipped += 1
                    continue
                yield record

            # Every record for this page has now been emitted by the SDK, so it is
            # safe to advance the persisted resume point past this page.
            if next_token is None:
                # Window complete: advance the run-to-run watermark to the pinned
                # upper bound and clear the intra-run cursor.
                state["replication_key"] = replication_key
                state["replication_key_value"] = window_end_iso
                state.pop(_CURSOR_STATE_KEY, None)
                state.pop(_WINDOW_END_STATE_KEY, None)
                self._write_state_message()
                finished = True
            else:
                # Persist the resume point (cursor + pinned window) WITHOUT advancing
                # the run-to-run watermark.
                state[_CURSOR_STATE_KEY] = next_token
                state[_WINDOW_END_STATE_KEY] = window_end_iso
                self._write_state_message()
                token = next_token

        if hotglue_principal_id and skipped:
            self.logger.info("Skipped %d records last-modified by HotGlue principal", skipped)

    def _write_state_message(self) -> None:
        """Write out a STATE message with the latest state."""
        tap_state = self.tap_state

        if tap_state and tap_state.get("bookmarks"):
            for stream_name in tap_state.get("bookmarks").keys():
                if tap_state["bookmarks"][stream_name].get("partitions"):
                    tap_state["bookmarks"][stream_name] = {"partitions": []}

        singer.write_message(StateMessage(value=tap_state))

    def get_schema(self) -> dict:
        """Get schema dynamically from CBX1 API."""
        if not self.target_name:
            raise ValueError(f"target_name must be set for {self.__class__.__name__}")

        headers = {}
        if self.authenticator:
            headers.update(self.authenticator.auth_headers or {})

        schema = fetch_schema_from_api(self.url_base, self.target_name,self.config.get(CRM_KEY), headers)

        if schema is None:
            raise RuntimeError(f"Failed to fetch schema for target {self.target_name}")

        return schema

    @cached_property
    def schema(self) -> dict:
        """Cached schema property."""
        return self.get_schema()
