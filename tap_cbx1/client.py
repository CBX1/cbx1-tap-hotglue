"""REST client handling, including LightspeedStream base class."""

from typing import Any, Iterable, Optional, TypeVar
import requests
from pendulum import parse
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.streams import RESTStream
import singer
from singer import StateMessage
from tap_cbx1.auth import TapCBX1Auth

_TToken = TypeVar("_TToken")

class CBX1Stream(RESTStream):
    """CBX1 stream class."""

    @property
    def url_base(self):
        return 'https://qa-api.cbx1.app/api/t/v1'

    
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
    

    @property
    def http_headers(self) -> dict:
        result = self._http_headers
        result["x-organisation-id"] = self.config.get("organization_id")
        result["x-user-id"] = self.config.get("user_id")
        return result
    
    def get_url(self, context: dict | None) -> str:
        url = "".join([self.url_base, self.path or "", "/list"])
        return url

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
        if self.replication_key_field and start_date:
            iso_start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            iso_now = parse("now").strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            payload["filters"] = {
                self.replication_key_field: {
                    "type": "BETWEEN",
                    "value": iso_start_date,
                    "endValue": iso_now
                }
            }
        return payload
    
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
    
