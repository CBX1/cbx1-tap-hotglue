from tap_cbx1.client import CBX1Stream

class AccountStream(CBX1Stream):
    """Account stream with dynamic schema discovery."""
    name = "accounts"
    path = "/targets/accounts"
    target_name = "accounts"
    primary_keys = ["id"]
    replication_key = "updatedAt"



class ContactStream(CBX1Stream):
    """Contact stream with dynamic schema discovery."""
    name = "contacts"
    path = "/targets/contacts"
    target_name = "contacts"
    primary_keys = ["id"]
    replication_key = "updatedAt"

