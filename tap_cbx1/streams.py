import logging
from typing import Iterable

from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError

from tap_cbx1.client import CBX1Stream, CURSOR_STATE_KEY

logger = logging.getLogger(__name__)

# Minimal Singer schema used when no ACCOUNT egestion mapping is configured for a tenant.
# Provides the primary key and replication key so discovery succeeds; sync yields nothing.
_ACCOUNT_FALLBACK_SCHEMA = th.PropertiesList(
    th.Property("id", th.StringType),
    th.Property("updatedAt", th.DateTimeType),
).to_dict()


class ContactStream(CBX1Stream):
    """Contact stream with dynamic schema discovery."""
    name = "contacts"
    path = "/CONTACT"
    target_name = "CONTACT"
    primary_keys = ["id"]
    replication_key = "updatedAt"


class AccountStream(CBX1Stream):
    """Account stream with dynamic schema discovery.

    Gracefully skips tenants that have no TenantEgestionMapping for ACCOUNT→CRM:
    - Discovery: falls back to a minimal schema instead of raising RuntimeError
    - Sync: yields no records if the list endpoint returns a non-2xx response
    """
    name = "accounts"
    path = "/ACCOUNT"
    target_name = "ACCOUNT"
    primary_keys = ["id"]
    replication_key = "updatedAt"

    def get_schema(self) -> dict:
        try:
            return super().get_schema()
        except RuntimeError:
            logger.warning(
                "No ACCOUNT egestion mapping configured for this tenant "
                "(CRM=%s); using fallback schema — stream will produce no records.",
                self.config.get("CRMSystem"),
            )
            return _ACCOUNT_FALLBACK_SCHEMA

    def request_records(self, context) -> Iterable[dict]:
        yielded_any = False
        try:
            for record in super().request_records(context):
                yielded_any = True
                yield record
        except FatalAPIError as e:
            # Treat a 4xx as "no ACCOUNT egestion mapping for this tenant" and yield
            # nothing ONLY when it happens before any progress: the very first fetch
            # of a fresh window, with no records yielded AND no resume cursor
            # persisted. A 4xx AFTER progress is a real error — swallowing it would
            # leave the persisted (cursor, window_end) in state, so every later run
            # would resume to the same failing page, swallow again, and never advance
            # or clear: a permanent silent wedge. Fail loudly in that case. Transient
            # errors (5xx/timeouts -> RetriableAPIError) and the keyset-anomaly
            # RuntimeError are not caught here and always propagate.
            if yielded_any or self.stream_state.get(CURSOR_STATE_KEY):
                raise
            logger.warning(
                "ACCOUNT egestion list returned a client error on the first page "
                "for this tenant (likely no ACCOUNT egestion mapping configured); "
                "yielding no records: %s",
                e,
            )

