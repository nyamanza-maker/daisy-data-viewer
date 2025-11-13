import io
import pandas as pd
import streamlit as st
from firebase_setup import init_pyrebase, init_firebase_admin

# Initialize SDKs
_pb = init_pyrebase()
_auth = _pb.auth()
_storage = _pb.storage()
_db, _admin_bucket = init_firebase_admin()


# -----------------------------
# AUTH
# -----------------------------
def login_email_password(email: str, password: str):
    user = _auth.sign_in_with_email_and_password(email, password)
    info = _auth.get_account_info(user['idToken'])
    uid = info["users"][0]["localId"]
    return {"uid": uid, "idToken": user["idToken"]}


def logout():
    st.session_state.pop("auth", None)


# -----------------------------
# STORAGE HELPERS
# -----------------------------
def storage_path_for(uid: str, filename: str) -> str:
    # e.g. franchises/<uid>/Customers.csv
    return f"franchises/{uid}/{filename}"


def upload_bytes(uid: str, filename: str, content: bytes, id_token: str):
    """
    Upload bytes to Firebase Storage.
    """
    path = storage_path_for(uid, filename)
    _storage.child(path).put(io.BytesIO(content), id_token)


def file_exists(uid: str, filename: str, id_token: str) -> bool:
    """
    Check if a file exists in Firebase Storage by using metadata.
    This is reliable and does NOT falsely return True.
    """
    try:
        path = storage_path_for(uid, filename)
        _storage.child(path).get_metadata(id_token)
        return True
    except Exception:
        return False


def download_csv_as_df(uid: str, filename: str, id_token: str, **read_csv_kwargs) -> pd.DataFrame:
    """
    Download a CSV file from Firebase Storage using raw bytes.
    This avoids broken get_url() download tokens and HTTP errors.
    """
    path = storage_path_for(uid, filename)

    try:
        # Download raw file bytes
        file_bytes = _storage.child(path).get(id_token)
    except Exception as e:
        raise RuntimeError(f"Failed to download {filename}: {e}")

    # Wrap in BytesIO and let pandas parse
    return pd.read_csv(io.BytesIO(file_bytes), **read_csv_kwargs)


# -----------------------------
# FIRESTORE MIGRATION FLAGS
# -----------------------------
def _mig_doc(uid: str, coll: str, doc_id: str):
    # /migrations/{uid}/{coll}/{doc_id}
    return _db.collection("migrations").document(uid).collection(coll).document(str(doc_id))


def set_migrated(uid: str, coll: str, doc_id: str, value: bool):
    _mig_doc(uid, coll, doc_id).set({"migrated": bool(value)}, merge=True)


def get_migrated(uid: str, coll: str, doc_id: str) -> bool:
    doc = _mig_doc(uid, coll, doc_id).get()
    if doc.exists:
        data = doc.to_dict()
        return bool(data.get("migrated", False))
    return False


def any_migrated_for(uid: str, coll: str, doc_ids: list[str]) -> bool:
    for d in doc_ids:
        if get_migrated(uid, coll, d):
            return True
    return False
