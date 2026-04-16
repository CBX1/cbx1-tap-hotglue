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
    primary_keys = ["uuid"]
    replication_key = "occurredAt"


class FormEventStream(CBX1EventStream):
    """Form event stream (form submissions)."""
    name = "form_events"
    path = "/FORM_EVENT"
    target_name = "FORM_EVENT"
    primary_keys = ["uuid"]
    replication_key = "occurredAt"


class PageEventStream(CBX1EventStream):
    """Page event stream."""
    name = "page_events"
    path = "/PAGE_EVENT"
    target_name = "PAGE_EVENT"
    primary_keys = ["uuid"]
    replication_key = "occurredAt"
