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


class CBX1Stream(RESTStream):
    """CBX1 stream class."""

    # Target name for schema discovery (e.g., "accounts", "contacts")
    target_name = None

    @property
    def url_base(self):
        return os.getenv("BASE_URL") + "api/t/v1/targets/integrations"

    page_size = 10
    rest_method = "POST"
    replication_key_field = "updatedAt"

    @property
    def authenticator(self) -> TapCBX1Auth:
        """Return a new authenticator object."""
        return TapCBX1Auth.create_for_stream(self)

    def get_next_page_token(
            self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages.

        Terminates on ``data.last`` (a boolean the backend already returns)
        rather than ``data.totalPages``. Computing ``totalPages`` forces the
        backend to run a per-page full-partition Mongo ``count()`` which drives
        the prod primary to ~94% CPU; the count-free backend therefore returns
        ``null`` for ``totalElements``/``totalPages`` while still sending
        ``last``. We never do arithmetic on a possibly-null ``totalPages``.
        """
        previous_token = previous_token or 0
        page_data = response.json().get('data') or {}
        content = page_data.get('content') or []

        # Primary signal: the backend says this is the final page.
        # Works for both the current Page DTO and the future count-free Slice.
        if page_data.get('last') is True:
            return None

        # Defensive: a short or empty page means there is nothing after it.
        # Handles a backend that omits `last` entirely.
        if len(content) < self.page_size:
            return None

        # Legacy fallback ONLY when `last` is absent: stop on the last page by
        # index. Never compare when totalPages is None (would TypeError).
        if page_data.get('last') is None:
            number = page_data.get('number')
            total_pages = page_data.get('totalPages')
            if number is not None and total_pages is not None and number >= total_pages - 1:
                return None

        return previous_token + 1

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

        payload = {
            "pageNumber": next_page_token,
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
            iso_start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            iso_now = parse("now").strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            filters[self.replication_key_field] = {
                "type": "BETWEEN",
                "value": iso_start_date,
                "endValue": iso_now
            }

        payload["filters"] = filters
        return payload

    @property
    def http_headers(self) -> dict:
        result = self._http_headers
        return result

    def request_records(self, context: dict | None) -> Iterable[dict]:
        next_page_token = 0
        decorated_request = self.request_decorator(self._request)
        finished = False
        # Tap-side filter: drop records HotGlue itself last-modified to avoid re-ingesting our own
        # writes. Pushed-down `$ne updatedBy` regresses Mongo on tenants where HotGlue dominates.
        hotglue_principal_id = os.getenv(HOTGLUE_PRINCIPAL_ID_ENV)
        skipped = 0

        while not finished:
            prepared_request = self.prepare_request(
                context,
                next_page_token=next_page_token
            )
            resp = decorated_request(prepared_request, context)
            response_content = resp.json().get('data').get('content')
            for content in response_content:
                if hotglue_principal_id and content.get("updatedBy") == hotglue_principal_id:
                    skipped += 1
                    continue
                yield content

            next_page_token = self.get_next_page_token(resp, next_page_token)
            finished = next_page_token is None

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
