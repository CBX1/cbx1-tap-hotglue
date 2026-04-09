from tap_cbx1.client import CBX1Stream




class ContactStream(CBX1Stream):
    """Contact stream with dynamic schema discovery."""
    name = "contacts"
    path = "/CONTACT"
    target_name = "CONTACT"
    primary_keys = ["id"]
    replication_key = "updatedAt"


class EmailEventStream(CBX1Stream):
    """Email event stream (opens, clicks)."""
    name = "email_events"
    path = "/EMAIL_EVENT"
    target_name = "EMAIL_EVENT"
    primary_keys = ["id"]
    replication_key = "updatedAt"


class FormEventStream(CBX1Stream):
    """Form event stream (form submissions)."""
    name = "form_events"
    path = "/FORM_EVENT"
    target_name = "FORM_EVENT"
    primary_keys = ["id"]
    replication_key = "updatedAt"


class PageVisitStream(CBX1Stream):
    """Page visit stream."""
    name = "page_visits"
    path = "/PAGE_VISIT"
    target_name = "PAGE_VISIT"
    primary_keys = ["id"]
    replication_key = "updatedAt"

