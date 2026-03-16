from tap_cbx1.client import CBX1Stream




class ContactStream(CBX1Stream):
    """Contact stream with dynamic schema discovery."""
    name = "contacts"
    path = "/CONTACT"
    target_name = "CONTACT"
    primary_keys = ["id"]
    replication_key = "updatedAt"

