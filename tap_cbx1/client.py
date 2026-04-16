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
from tap_cbx1.constants import CRM_KEY

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
        """Return a token for identifying next page or None if no more pages."""
        previous_token = previous_token or 0
        page_data = response.json().get('data')
        if page_data.get('number') < page_data.get('totalPages'):
            next_page_token = previous_token + 1
            return next_page_token
        return None

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

        while not finished:
            prepared_request = self.prepare_request(
                context,
                next_page_token=next_page_token
            )
            resp = decorated_request(prepared_request, context)
            response_content = resp.json().get('data').get('content')
            for content in response_content:
                yield content

            next_page_token = self.get_next_page_token(resp, next_page_token)
            finished = next_page_token is None

    def _write_state_message(self) -> None:
        """Write out a STATE message with the latest state."""
        tap_state = self.tap_state

        if tap_state and tap_state.get("bookmarks"):
            for stream_name in tap_state.get("bookmarks").keys():
                if tap_state["bookmarks"][stream_name].get("partitions"):
                    tap_state["bookmarks"][stream_name] = {"partitions": []}

        singer.write_message(StateMessage(value=tap_state))

    def get_replication_key_signpost(self, context: Optional[dict]) -> Optional[Any]:
        return None

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


class CBX1EventStream(CBX1Stream):
    """Base class for event streams using keyset (cursor-based) pagination."""

    page_size = 1000
    rest_method = "POST"
    replication_key_field = "occurredAt"

    def get_url(self, context: dict | None) -> str:
        crm = self.config.get(CRM_KEY)
        url = "".join([self.url_base, self.path or "", f"/{crm}/events"])
        return url

    @cached_property
    def schema(self) -> dict:
        """Schema matching the enrichment-mapped output from the backend.

        The backend applies TenantEgestionMapping before returning records,
        so fields here are the MAPPED names (not raw Redshift column names).
        Using additionalProperties: true so any extra mapped fields pass through.
        """
        return {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "inputs": {
                    "type": ["array", "null"],
                    "items": {
                        "type": "object",
                        "properties": {
                            "uuid": {"type": ["string", "null"]},
                            "occurredAt": {"type": ["string", "null"], "format": "date-time"},
                            "eventName": {"type": ["string", "null"]},
                            "email": {"type": ["string", "null"]},
                            "objectId": {"type": ["string", "null"]},
                            "properties": {"type": ["object", "null"]},
                        },
                    },
                },
                "uuid": {"type": ["string", "null"]},
                "occurredAt": {"type": ["string", "null"], "format": "date-time"},
            },
        }

    def prepare_request_payload(
            self,
            context: dict | None,
            next_page_token: Any | None,
    ) -> dict | None:
        """Build keyset pagination request payload."""
        payload = {
            "pageSize": self.page_size,
            "botFilterEnabled": self.config.get("bot_filter", True),
        }

        # next_page_token is a dict {lastCreatedAt, lastEventId} from previous response
        if next_page_token and isinstance(next_page_token, dict):
            payload["lastCreatedAt"] = next_page_token.get("lastCreatedAt")
            payload["lastEventId"] = next_page_token.get("lastEventId")
        else:
            # First page - use start_date from config if available
            start_date = self.config.get("start_date")
            if start_date:
                payload["lastCreatedAt"] = start_date

        # Add event actions filter if configured
        event_actions = getattr(self, 'event_actions', None)
        if event_actions:
            payload["eventActions"] = event_actions

        return payload

    def get_next_page_token(
            self, response: requests.Response, previous_token: Any | None
    ) -> Any | None:
        """Return compound cursor {lastCreatedAt, lastEventId} if hasMore, else None."""
        data = response.json().get('data', {})
        if data.get('hasMore', False):
            return {
                "lastCreatedAt": data.get('nextCursor'),
                "lastEventId": data.get('nextEventId'),
            }
        return None

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """Yield one record per page — each record is a batch of events.

        Each yielded record contains an 'inputs' array with all events from
        that page, matching HubSpot's bulk custom events API format.
        The 'occurredAt' is taken from the last event for replication key tracking.
        """
        next_page_token = None
        decorated_request = self.request_decorator(self._request)
        finished = False

        while not finished:
            prepared_request = self.prepare_request(
                context,
                next_page_token=next_page_token
            )
            resp = decorated_request(prepared_request, context)
            data = resp.json().get('data', {})
            records = data.get('records', [])

            if records:
                # Yield the whole page as a single batch record
                last_record = records[-1]
                yield {
                    "inputs": records,
                    "uuid": last_record.get("uuid", ""),
                    "occurredAt": last_record.get("occurredAt", ""),
                }

            next_page_token = self.get_next_page_token(resp, next_page_token)
            finished = next_page_token is None
