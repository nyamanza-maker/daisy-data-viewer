# db_utils.py

from typing import Optional, Dict, Any
from firebase_admin import firestore
from config import (
    CUSTOMERS_COLL,
    BOOKINGS_COLL,
    NOTES_COLL,
    MIGRATIONS_COLL,
    TENANTS_COLL,
)

db = firestore.client()

# ---------- Tenant / Franchise ----------

def get_franchise_for_user(uid: str, email: str) -> Optional[Dict[str, Any]]:
    """
    Resolve which franchise this user belongs to.
    tenants/{tenant_id}:
      {
        "business_id": "97774",
        "allowed_uids": [...],
        "allowed_emails": [...]
      }
    """
    tenants_ref = db.collection(TENANTS_COLL)
    # Simple approach: query by allowed_uids or allowed_emails
    by_uid = tenants_ref.where("allowed_uids", "array_contains", uid).stream()
    tenants = list(by_uid)
    if tenants:
        doc = tenants[0]
        return {"tenant_id": doc.id, **doc.to_dict()}

    by_email = tenants_ref.where("allowed_emails", "array_contains", email).stream()
    tenants = list(by_email)
    if tenants:
        doc = tenants[0]
        return {"tenant_id": doc.id, **doc.to_dict()}

    return None


# ---------- CRUD Helpers ----------

def customer_doc(tenant_id: str, customer_id: str):
    return db.collection(TENANTS_COLL).document(tenant_id).collection(CUSTOMERS_COLL).document(str(customer_id))


def booking_doc(tenant_id: str, booking_id: str):
    return db.collection(TENANTS_COLL).document(tenant_id).collection(BOOKINGS_COLL).document(str(booking_id))


def note_doc(tenant_id: str, note_id: str):
    return db.collection(TENANTS_COLL).document(tenant_id).collection(NOTES_COLL).document(str(note_id))


# ---------- Migration Flags ----------

def _mig_doc(uid: str, coll: str, doc_id: str):
    return db.collection(MIGRATIONS_COLL).document(uid).collection(coll).document(str(doc_id))


def set_migrated(uid: str, coll: str, doc_id: str, value: bool):
    doc_ref = _mig_doc(uid, coll, doc_id)
    doc_ref.set({"migrated": bool(value)}, merge=True)


def get_migrated(uid: str, coll: str, doc_id: str) -> bool:
    doc_ref = _mig_doc(uid, coll, doc_id)
    doc = doc_ref.get()
    if not doc.exists:
        return False
    data = doc.to_dict() or {}
    return bool(data.get("migrated", False))
