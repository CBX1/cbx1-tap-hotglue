import logging
from typing import Iterable

from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError

from tap_cbx1.client import CBX1Stream

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
        try:
            yield from super().request_records(context)
        except FatalAPIError as e:
            # A missing ACCOUNT egestion mapping makes the list endpoint return a
            # 4xx, which the SDK surfaces as FatalAPIError. Treat that as "no records
            # for this tenant" and yield nothing. We deliberately catch ONLY
            # FatalAPIError (4xx): transient failures (5xx/timeouts -> RetriableAPIError,
            # connection errors) and the keyset-anomaly RuntimeError are NOT caught and
            # propagate, so the run fails loudly instead of silently dropping records
            # and recording false progress.
            logger.warning(
                "ACCOUNT egestion list returned a client error for this tenant "
                "(likely no ACCOUNT egestion mapping configured); yielding no "
                "records: %s",
                e,
            )

