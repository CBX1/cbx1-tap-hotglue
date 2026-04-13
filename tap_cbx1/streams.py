from tap_cbx1.client import CBX1Stream, CBX1EventStream




class ContactStream(CBX1Stream):
    """Contact stream with dynamic schema discovery."""
    name = "contacts"
    path = "/CONTACT"
    target_name = "CONTACT"
    primary_keys = ["id"]
    replication_key = "updatedAt"


class EmailEventStream(CBX1EventStream):
    """Email event stream (opens, clicks)."""
    name = "email_events"
    path = "/EMAIL_EVENT"
    target_name = "EMAIL_EVENT"
    primary_keys = ["id"]
    replication_key = "event_timestamp"
    event_actions = ["open", "click"]


class FormEventStream(CBX1EventStream):
    """Form event stream (form submissions)."""
    name = "form_events"
    path = "/FORM_EVENT"
    target_name = "FORM_EVENT"
    primary_keys = ["id"]
    replication_key = "event_timestamp"


class PageVisitStream(CBX1EventStream):
    """Page visit stream."""
    name = "page_visits"
    path = "/PAGE_VISIT"
    target_name = "PAGE_VISIT"
    primary_keys = ["id"]
    replication_key = "event_timestamp"

