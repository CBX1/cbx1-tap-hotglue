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
from tap_cbx1.constants import CRM_KEY, HOTGLUE_PRINCIPAL_ID_ENV

_TToken = TypeVar("_TToken")

# ISO-8601 millisecond format the backend BETWEEN filter expects.
_ISO_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"
# Lower bound for a first/full sync (no replication bookmark yet) so the request
# still has a bounded BETWEEN window the keyset cursor can walk down.
_EPOCH = parse("1970-01-01T00:00:00Z")


class CBX1Stream(RESTStream):
    """CBX1 stream class."""

    # Target name for schema discovery (e.g., "accounts", "contacts")
    target_name = None

    @property
    def url_base(self):
        return os.getenv("BASE_URL") + "api/t/v1/targets/integrations"

    # Keyset pagination keeps every request on page 0 (a ~7ms index seek
    # regardless of org size), so a larger page is purely fewer round-trips with
    # no skip cost.
    page_size = 100
    rest_method = "POST"
    replication_key_field = "updatedAt"

    @property
    def authenticator(self) -> TapCBX1Auth:
        """Return a new authenticator object."""
        return TapCBX1Auth.create_for_stream(self)

    def get_next_page_token(
            self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return the keyset cursor for the next page, or None when done.

        KEYSET (not offset): records come back sorted by ``updatedAt`` DESC, so
        the last record on a full page carries the page's minimum ``updatedAt``.
        We return that timestamp as the cursor; the next request asks for the
        window up to (and including) it — an index range seek, ~7ms regardless
        of how far the sync has progressed. The old offset scheme
        (``previous_token + 1`` -> ``skip(pageNumber*pageSize)``) made deep pages
        scan the whole tenant partition and blow the backend's 1s ``maxTimeMS``
        on large orgs (HotGlue ``SYNC_FAILED`` / 500 on ``/CONTACT``).

        Termination still uses ``data.last`` (the backend already returns it),
        falling back to a short page; we never do arithmetic on a possibly-null
        ``totalPages``.
        """
        page_data = response.json().get('data') or {}
        content = page_data.get('content') or []

        # Primary signal: the backend says this is the final page.
        # Works for both the current Page DTO and the count-free Slice.
        if page_data.get('last') is True:
            return None

        # Defensive: a short or empty page means there is nothing after it.
        if len(content) < self.page_size:
            return None

        # Legacy fallback ONLY when `last` is absent. With keyset every request is
        # pageNumber=0, so `number` is always 0 and this only fires when the whole
        # remaining window fits in one page (totalPages<=1) — i.e. the last page.
        if page_data.get('last') is None:
            number = page_data.get('number')
            total_pages = page_data.get('totalPages')
            if number is not None and total_pages is not None and number >= total_pages - 1:
                return None

        # Cursor = minimum updatedAt on this (full, DESC-sorted) page = last record.
        # Inclusive upper bound; request_records de-dups the boundary record(s) by id.
        last_rec = content[-1] if content else None
        return last_rec.get(self.replication_key_field) if last_rec else None

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
        rep = self.replication_key_field

        # Keyset: always page 0. The cursor moves (the BETWEEN upper bound below),
        # never the offset — so the backend never does skip(pageNumber*pageSize).
        payload = {
            "pageNumber": 0,
            "pageSize": self.page_size,
            "sortBy": rep,
            "sortDirection": "DESC",
        }

        # Always filter out test records (testMetadata: null means not a test record)
        filters = {
            "testMetadata": {
                "type": "EQUALS",
                "value": None
            }
        }

        if rep:
            # Lower bound: the incremental floor (bookmark / start_date). Default to
            # epoch on a first/full sync so the window is still bounded for keyset.
            if start_date:
                lower = (start_date + timedelta(milliseconds=1)).strftime(_ISO_FMT)
            else:
                lower = _EPOCH.strftime(_ISO_FMT)
            # Upper bound: the keyset cursor (previous page's min updatedAt), or
            # "now" for the first page. Inclusive — request_records de-dups the
            # boundary record(s) by id so the inclusive bound never re-emits them.
            upper = (parse(next_page_token) if next_page_token else parse("now")).strftime(_ISO_FMT)
            filters[rep] = {
                "type": "BETWEEN",
                "value": lower,
                "endValue": upper,
            }

        payload["filters"] = filters
        return payload

    @property
    def http_headers(self) -> dict:
        result = self._http_headers
        return result

    def request_records(self, context: dict | None) -> Iterable[dict]:
        decorated_request = self.request_decorator(self._request)
        # Tap-side filter: drop records HotGlue itself last-modified to avoid re-ingesting our own
        # writes. Pushed-down `$ne updatedBy` regresses Mongo on tenants where HotGlue dominates.
        hotglue_principal_id = os.getenv(HOTGLUE_PRINCIPAL_ID_ENV)
        rep = self.replication_key_field
        skipped = 0

        next_page_token = None        # keyset cursor; None => first page (upper = now)
        seen_at_cursor: set = set()   # ids already emitted at the current cursor boundary

        while True:
            prepared_request = self.prepare_request(
                context,
                next_page_token=next_page_token
            )
            resp = decorated_request(prepared_request, context)
            response_content = (resp.json().get('data') or {}).get('content') or []

            for content in response_content:
                # De-dup the inclusive keyset boundary: a record at exactly the
                # cursor timestamp that we already emitted on the previous page.
                if next_page_token is not None and rep \
                        and content.get(rep) == next_page_token \
                        and content.get("id") in seen_at_cursor:
                    continue
                if hotglue_principal_id and content.get("updatedBy") == hotglue_principal_id:
                    skipped += 1
                    continue
                yield content

            new_token = self.get_next_page_token(resp, next_page_token)
            if new_token is None:
                break
            # No-progress guard: a full page whose records all share one updatedAt
            # cannot advance an updatedAt-only cursor. Real data tops out at a
            # couple per instant; bail loudly rather than loop forever.
            if new_token == next_page_token:
                self.logger.warning(
                    "Keyset cursor stalled at %s (a full page shares one updatedAt); "
                    "stopping to avoid an infinite loop. Paging this tenant would need "
                    "a compound (updatedAt,_id) cursor plus a backend _id index.",
                    new_token,
                )
                break
            # Remember the ids at the new boundary so the next page skips them.
            seen_at_cursor = {
                r.get("id") for r in response_content if rep and r.get(rep) == new_token
            }
            next_page_token = new_token

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

    def get_replication_key_signpost(self, context: Optional[dict]) -> Optional[Any]:
        # Cap state advancement at "now" so a sync that yields zero records
        # (e.g., every record in the window was HotGlue-authored and filtered
        # out tap-side) still moves the bookmark forward instead of replaying
        # the same window on the next run.
        return parse("now")

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
