import io
import json
from datetime import datetime, timedelta
from time import time

import pandas as pd
import streamlit as st

import pyrebase
import firebase_admin
from firebase_admin import credentials, firestore

import requests

# DEBUG: Check what secrets exist
st.sidebar.write("Available secrets:")
st.sidebar.write(list(st.secrets.keys()))
# DEBUG: Check FIREBASE section contents
st.sidebar.write("FIREBASE section keys:")
if "FIREBASE" in st.secrets:
    st.sidebar.write(list(st.secrets["FIREBASE"].keys()))
# ----------------------------------
# Page config
# ----------------------------------
st.set_page_config(page_title="Daisy Data Viewer", layout="wide")

# ----------------------------------
# Firebase client (Pyrebase)
# ----------------------------------
firebase_section = st.secrets.get("FIREBASE", None)
if firebase_section is None:
    st.error("FIREBASE config not found in Streamlit secrets.")
    st.stop()

firebase_config = {
    "apiKey": firebase_section["api_key"],
    "authDomain": firebase_section["auth_domain"],
    "projectId": firebase_section["project_id"],
    "storageBucket": firebase_section["storage_bucket"],
    "messagingSenderId": firebase_section["messaging_sender_id"],
    "appId": firebase_section["app_id"],
    "databaseURL": firebase_section.get("database_url", "https://dummy.firebaseio.com"),
}

firebase = pyrebase.initialize_app(firebase_config)
auth = firebase.auth()
storage = firebase.storage()


# ----------------------------------
# Friendly Firebase login error parser
# ----------------------------------
def parse_firebase_login_error(e: Exception) -> str:
    raw = str(e)

    if "INVALID_EMAIL" in raw:
        return "The email address format is invalid."

    if "EMAIL_NOT_FOUND" in raw:
        return "No account exists with this email."

    if "INVALID_PASSWORD" in raw or "INVALID_LOGIN_CREDENTIALS" in raw or "wrong password" in raw.lower():
        return "Incorrect password. Please try again."

    if "USER_DISABLED" in raw:
        return "This account has been disabled. Please contact support."

    if "TOO_MANY_ATTEMPTS_TRY_LATER" in raw:
        return "Too many login attempts. Please try again later."

    return "Authentication failed. Please check your email and password."



# ----------------------------------
# Firebase Admin (Firestore)
# ----------------------------------
# Initialize db as None at module level
db = None

pythondef init_admin_db():
    """
    Initialise Firebase Admin SDK using FIREBASE admin_json secret.
    If not present, we simply don't persist migration flags.
    """
    global db
    
    # Check if admin_json exists within FIREBASE section
    if "FIREBASE" not in st.secrets or "admin_json" not in st.secrets["FIREBASE"]:
        st.sidebar.warning("⚠️ FIREBASE.admin_json not found in secrets - migration flags won't persist!")
        return None

    try:
        admin_data = st.secrets["FIREBASE"]["admin_json"]
        
        # Convert AttrDict or dict to regular dict, or parse JSON string
        if isinstance(admin_data, str):
            cred_info = json.loads(admin_data)
        else:
            # Handle dict, AttrDict, or any dict-like object
            cred_info = dict(admin_data)

        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_info)
            firebase_admin.initialize_app(cred)
        
        db = firestore.client()
        st.sidebar.success("✓ Firestore connected")
        return db
    except Exception as e:
        st.sidebar.error(f"❌ Firestore initialization failed: {type(e).__name__}: {e}")
        import traceback
        st.sidebar.code(traceback.format_exc())
        return None


def _mig_doc(uid: str, coll: str, doc_id: str):
    """
    Firestore path:
      /migrations/{uid}/{coll}/{doc_id}
    """
    global db
    if db is None:
        return None
    return db.collection("migrations").document(uid).collection(coll).document(str(doc_id))


def set_migrated(uid: str, coll: str, doc_id: str, value: bool):
    global db
    if db is None:
        return
    doc_ref = _mig_doc(uid, coll, doc_id)
    if doc_ref is not None:
        doc_ref.set({"migrated": bool(value)}, merge=True)


def get_migrated(uid: str, coll: str, doc_id: str) -> bool:
    global db
    if db is None:
        return False
    doc_ref = _mig_doc(uid, coll, doc_id)
    if doc_ref is None:
        return False
    doc = doc_ref.get()
    if doc.exists:
        data = doc.to_dict()
        return bool(data.get("migrated", False))
    return False


# Initialize the database
db = init_admin_db()

def _mig_doc(uid: str, coll: str, doc_id: str):
    if db is None:
        return None
    return db.collection("migrations").document(uid).collection(coll).document(str(doc_id))


def set_migrated(uid: str, coll: str, doc_id: str, value: bool):
    if db is None:
        return
    doc_ref = _mig_doc(uid, coll, doc_id)
    if doc_ref is not None:
        doc_ref.set({"migrated": bool(value)}, merge=True)


def get_migrated(uid: str, coll: str, doc_id: str) -> bool:
    if db is None:
        return False
    doc_ref = _mig_doc(uid, coll, doc_id)
    if doc_ref is None:
        return False
    doc = doc_ref.get()
    if doc.exists:
        data = doc.to_dict()
        return bool(data.get("migrated", False))
    return False


# ----------------------------------
# Storage helpers (Firebase Storage)
# ----------------------------------
def storage_path_for(uid: str, filename: str) -> str:
    return f"franchises/{uid}/{filename}"


def upload_bytes(uid: str, filename: str, content: bytes, id_token: str):
    path = storage_path_for(uid, filename)
    storage.child(path).put(io.BytesIO(content), id_token)


def file_exists(uid: str, filename: str, id_token: str) -> bool:
    path = f"franchises/{uid}/{filename}"
    url = (
        f"https://firebasestorage.googleapis.com/v0/b/"
        f"{firebase_config['storageBucket']}/o/{path.replace('/', '%2F')}"
    )
    headers = {"Authorization": f"Bearer {id_token}"}

    try:
        r = requests.get(url, headers=headers)
        return r.status_code == 200
    except Exception:
        return False


def download_csv_as_df(uid: str, filename: str, id_token: str, **read_csv_kwargs):
    path = f"franchises/{uid}/{filename}"
    url = (
        f"https://firebasestorage.googleapis.com/v0/b/"
        f"{firebase_config['storageBucket']}/o/{path.replace('/', '%2F')}?alt=media"
    )
    headers = {"Authorization": f"Bearer {id_token}"}

    r = requests.get(url, headers=headers)

    if r.status_code != 200:
        raise RuntimeError(f"Failed to download {filename}: {r.text}")

    return pd.read_csv(io.BytesIO(r.content), **read_csv_kwargs)


# ----------------------------------
# Misc helpers
# ----------------------------------
def to_bool(v):
    if pd.isna(v):
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v == 1
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes", "y", "t")
    return False


def add_migration_flags(customers: pd.DataFrame,
                        notes: pd.DataFrame,
                        bookings: pd.DataFrame,
                        uid: str):
    if customers is None:
        customers = pd.DataFrame()
    if notes is None:
        notes = pd.DataFrame()
    if bookings is None:
        bookings = pd.DataFrame()

    if "CustomerId" in customers.columns:
        customers["Migrated"] = customers["CustomerId"].apply(
            lambda cid: get_migrated(uid, "customers", cid)
        )
    else:
        customers["Migrated"] = False

    if "CustomerId" in notes.columns:
        notes["Migrated"] = notes["CustomerId"].apply(
            lambda cid: get_migrated(uid, "notes", cid)
        )
    else:
        notes["Migrated"] = False

    if "BookingId" in bookings.columns:
        bookings["migrated"] = bookings["BookingId"].apply(
            lambda bid: get_migrated(uid, "bookings", bid)
        )
    else:
        bookings["migrated"] = False

    return customers, notes, bookings


# ----------------------------------
# Search history management
# ----------------------------------
def add_to_search_history(query: str):
    """Add query to search history, keep last 10."""
    if "search_history" not in st.session_state:
        st.session_state["search_history"] = []
    
    query = query.strip()
    if query and query not in st.session_state["search_history"]:
        st.session_state["search_history"].insert(0, query)
        st.session_state["search_history"] = st.session_state["search_history"][:10]


# ----------------------------------
# Auth state + rate limiting state
# ----------------------------------
if "auth" not in st.session_state:
    st.session_state["auth"] = None

if "login_attempts" not in st.session_state:
    st.session_state["login_attempts"] = 0

if "lockout_until" not in st.session_state:
    st.session_state["lockout_until"] = 0

if "search_history" not in st.session_state:
    st.session_state["search_history"] = []


# ----------------------------------
# Authentication UI (Sidebar)
# ----------------------------------
with st.sidebar:
    st.header("Sign in")

    now = time()
    MAX_ATTEMPTS = 5
    LOCKOUT_DURATION = 60 * 5

    if st.session_state["auth"] is None:
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")

        if st.session_state["lockout_until"] > now:
            remaining = int(st.session_state["lockout_until"] - now)
            st.error(f"Too many failed attempts. Try again in {remaining} seconds.")
        else:
            if st.button("Login"):
                if st.session_state["login_attempts"] >= MAX_ATTEMPTS:
                    st.session_state["lockout_until"] = now + LOCKOUT_DURATION
                    st.session_state["login_attempts"] = 0
                    st.error("Too many failed attempts. Locked out for 5 minutes.")
                else:
                    try:
                        user = auth.sign_in_with_email_and_password(email, password)
                        account = auth.get_account_info(user["idToken"])["users"][0]

                        if not account.get("emailVerified", False):
                            st.session_state["login_attempts"] += 1
                            st.error("Please verify your email address before logging in.")
                        else:
                            st.session_state["login_attempts"] = 0
                            st.session_state["auth"] = {
                                "email": email,
                                "uid": account["localId"],
                                "idToken": user["idToken"],
                            }
                            st.success("Signed in.")
                            st.rerun()

                    except Exception as e:
                        st.session_state["login_attempts"] += 1
                        msg = parse_firebase_login_error(e)
                        st.error(msg)

        if email and password and st.session_state["lockout_until"] <= now:
            if st.button("Resend verification email"):
                try:
                    temp_user = auth.sign_in_with_email_and_password(email, password)
                    auth.send_email_verification(temp_user["idToken"])
                    st.success("Verification email sent.")
                except Exception:
                    st.error("Unable to send verification email. Check your email and password.")
    else:
        st.write(f"Signed in as: **{st.session_state['auth']['email']}**")
        if st.button("Logout"):
            st.session_state["auth"] = None
            st.rerun()

if st.session_state["auth"] is None:
    st.title("Daisy Data Viewer")
    st.info("Please sign in to begin.")
    st.stop()

uid = st.session_state["auth"]["uid"]
id_token = st.session_state["auth"]["idToken"]


# ----------------------------------
# CSV Upload section (AFTER auth)
# ----------------------------------
with st.sidebar:
    # Check if files exist
    has_customers = file_exists(uid, "Customers.csv", id_token)
    has_notes = file_exists(uid, "Notes.csv", id_token)
    has_bookings = file_exists(uid, "Bookings.csv", id_token)
    
    all_uploaded = has_customers and has_notes and has_bookings
    
    # Collapsible upload section if all files uploaded
    if all_uploaded:
        with st.expander("CSV Uploads (All files uploaded ✓)", expanded=False):
            st.caption("Upload any of your three CSV files. They will be stored securely in Firebase Storage.")
            cust_file = st.file_uploader("Customers.csv", type=["csv"], key="cust_up")
            notes_file = st.file_uploader("Notes.csv", type=["csv"], key="notes_up")
            book_file = st.file_uploader("Bookings.csv", type=["csv"], key="book_up")

            if st.button("Upload Now", key="upload_now_btn"):
                try:
                    if cust_file is not None:
                        upload_bytes(uid, "Customers.csv", cust_file.getvalue(), id_token)
                    if notes_file is not None:
                        upload_bytes(uid, "Notes.csv", notes_file.getvalue(), id_token)
                    if book_file is not None:
                        upload_bytes(uid, "Bookings.csv", book_file.getvalue(), id_token)

                    st.success("Upload complete. Reloading…")
                    st.rerun()
                except Exception as e:
                    st.error(f"Upload failed: {e}")
    else:
        st.header("CSV Uploads")
        st.caption("Upload any of your three CSV files. They will be stored securely in Firebase Storage.")

        cust_file = st.file_uploader("Customers.csv", type=["csv"], key="cust_up")
        notes_file = st.file_uploader("Notes.csv", type=["csv"], key="notes_up")
        book_file = st.file_uploader("Bookings.csv", type=["csv"], key="book_up")

        if st.button("Upload Now", key="upload_now_btn"):
            try:
                if cust_file is not None:
                    upload_bytes(uid, "Customers.csv", cust_file.getvalue(), id_token)
                if notes_file is not None:
                    upload_bytes(uid, "Notes.csv", notes_file.getvalue(), id_token)
                if book_file is not None:
                    upload_bytes(uid, "Bookings.csv", book_file.getvalue(), id_token)

                st.success("Upload complete. Reloading…")
                st.rerun()
            except Exception as e:
                st.error(f"Upload failed: {e}")

        st.markdown("**Storage status:**")
        st.write("Customers.csv:", "✓" if has_customers else "✗")
        st.write("Notes.csv:", "✓" if has_notes else "✗")
        st.write("Bookings.csv:", "✓" if has_bookings else "✗")


# ----------------------------------
# Load Data from Firebase Storage
# ----------------------------------
@st.cache_data(ttl=300)
def load_data_for_user(uid: str, id_token: str):
    has_customers = file_exists(uid, "Customers.csv", id_token)
    has_notes = file_exists(uid, "Notes.csv", id_token)
    has_bookings = file_exists(uid, "Bookings.csv", id_token)

    customers = None
    notes = None
    bookings = None

    missing = []
    if not has_customers:
        missing.append("Customers.csv")
    if not has_notes:
        missing.append("Notes.csv")
    if not has_bookings:
        missing.append("Bookings.csv")

    if has_customers:
        customers = download_csv_as_df(uid, "Customers.csv", id_token, low_memory=False)
        cust_cols = [
            c for c in customers.columns if c in
            ["CustomerId", "FirstName", "LastName", "CompanyName",
             "Telephone", "SMS", "PhysicalAddress", "Gender", "CustomerName"]
        ]
        customers = customers[cust_cols].copy() if cust_cols else customers.copy()

    if has_notes:
        notes = download_csv_as_df(uid, "Notes.csv", id_token, low_memory=False)
        note_cols = [c for c in notes.columns if c in ["CustomerId", "CustomerName", "NoteText"]]
        notes = notes[note_cols].copy() if note_cols else notes.copy()

    if has_bookings:
        bookings = download_csv_as_df(uid, "Bookings.csv", id_token, low_memory=False)
        book_cols = [
            c for c in bookings.columns if c in
            ["BookingId", "CustomerId", "CustomerName", "Staff", "Service",
             "StartDateTime", "EndDateTime", "Notes",
             "RecurringAppointment", "Price"]
        ]
        bookings = bookings[book_cols].copy() if book_cols else bookings.copy()

    return customers, notes, bookings, missing


customers, notes, bookings, missing_files = load_data_for_user(uid, id_token)

# ----------------------------------
# Require at least Customers.csv
# ----------------------------------
if customers is None:
    st.title("Daisy Data Viewer")
    st.warning(
        "You need to upload at least **Customers.csv** to begin.\n\n"
        "Use the *CSV Uploads* section in the sidebar."
    )
    st.stop()

optional_missing = [f for f in missing_files if f != "Customers.csv"]
if optional_missing:
    st.info(
        "The following optional files are not uploaded yet:\n\n"
        + "\n".join(f"- {f}" for f in optional_missing)
        + "\n\nCustomers will still display, but notes/bookings may be empty."
    )

if notes is None:
    notes = pd.DataFrame(columns=["CustomerId", "CustomerName", "NoteText"])
if bookings is None:
    bookings = pd.DataFrame(columns=[
        "BookingId", "CustomerId", "CustomerName", "Staff", "Service",
        "StartDateTime", "EndDateTime", "Notes",
        "RecurringAppointment", "Price"
    ])

# Add Firestore-backed migration flags
customers, notes, bookings = add_migration_flags(customers, notes, bookings, uid)


# ----------------------------------
# Sidebar Search with History
# ----------------------------------
st.sidebar.header("🔍 Search Customer")

# Initialize search state
if "current_search" not in st.session_state:
    st.session_state["current_search"] = ""

# Search input with on_change callback
def update_search():
    new_query = st.session_state.get("search_input_widget", "").strip()
    if new_query != st.session_state["current_search"]:
        st.session_state["current_search"] = new_query
        if new_query:
            add_to_search_history(new_query)

search_text = st.sidebar.text_input(
    "Enter name or company:",
    value=st.session_state["current_search"],
    key="search_input_widget",
    on_change=update_search
)

# Show search history
if st.session_state["search_history"]:
    with st.sidebar.expander("Recent Searches", expanded=False):
        for hist_query in st.session_state["search_history"]:
            if st.button(hist_query, key=f"hist_{hist_query}"):
                st.session_state["current_search"] = hist_query
                st.session_state["search_input_widget"] = hist_query
                add_to_search_history(hist_query)
                st.rerun()

search_name = st.session_state["current_search"].lower()

max_results = st.sidebar.number_input(
    "Max results to show", min_value=25, max_value=5000, value=200, step=25
)

# DisplayName logic
if "FirstName" in customers.columns and "LastName" in customers.columns:
    customers["DisplayName"] = (
        customers["FirstName"].fillna("") + " " + customers["LastName"].fillna("")
    ).str.strip()
elif "CustomerName" in customers.columns:
    customers["DisplayName"] = customers["CustomerName"]
else:
    customers["DisplayName"] = customers.get(
        "CompanyName", pd.Series(["Unknown"] * len(customers))
    ).fillna("Unknown")

if search_name:
    mask = customers.apply(
        lambda x: x.astype(str).str.lower().str.contains(search_name, na=False)
    )
    matched_customers = customers[mask.any(axis=1)].sort_values("DisplayName")
else:
    matched_customers = customers.sort_values("DisplayName")

# Filter: only future appointments
only_future = st.sidebar.checkbox("Only customers with future appointments", value=False)
if only_future and not bookings.empty and "CustomerId" in bookings.columns and "StartDateTime" in bookings.columns:
    start_dt_all = pd.to_datetime(bookings["StartDateTime"], errors="coerce")
    future_ids = bookings.loc[start_dt_all >= datetime.now(), "CustomerId"].unique().tolist()
    matched_customers = matched_customers[matched_customers["CustomerId"].isin(future_ids)]
elif only_future:
    st.sidebar.warning("StartDateTime column missing in bookings; future filter ignored.")

# Filter: exclude migrated
exclude_migrated = st.sidebar.checkbox("Exclude migrated customers", value=True)
if exclude_migrated and "Migrated" in matched_customers.columns:
    matched_customers = matched_customers[~matched_customers["Migrated"].map(to_bool)]

if not st.sidebar.checkbox("Show all matches", value=False):
    matched_customers = matched_customers.head(max_results)

st.sidebar.write(f"🧾 {len(matched_customers)} shown ({customers.shape[0]} total)")

options = matched_customers["DisplayName"].tolist()
selected_customer = st.sidebar.radio("Matches", options) if options else None


# ----------------------------------
# Main Display
# ----------------------------------
if selected_customer:
    selected_row = matched_customers[matched_customers["DisplayName"] == selected_customer].iloc[0]
    customer_id = selected_row["CustomerId"]

    st.title(f"👤 {selected_customer}")

    # Customer details
    st.subheader("Customer Details")
    for col, val in selected_row.items():
        if col not in ["CustomerId", "Migrated", "DisplayName"]:
            col1, col2 = st.columns([0.3, 0.7])
            col1.markdown(f"**{col}**")
            col2.code(str(val))

    # Customer migrated toggle
    if to_bool(selected_row.get("Migrated", False)):
        st.success("✓ Customer migrated")
    else:
        if st.button("✅ Mark this customer as migrated", key="migrate_customer"):
            set_migrated(uid, "customers", customer_id, True)
            st.cache_data.clear()
            st.rerun()

    # Notes
    st.subheader("📝 Notes")
    if not notes.empty and "CustomerId" in notes.columns:
        cust_notes = notes[notes["CustomerId"] == customer_id]
    else:
        cust_notes = pd.DataFrame()

    if cust_notes.empty:
        st.info("No notes for this customer (or Notes.csv not uploaded).")
    else:
        for _, n in cust_notes.iterrows():
            st.code(n.get("NoteText", ""))

        if "Migrated" in cust_notes.columns and cust_notes["Migrated"].map(to_bool).any():
            st.success("✓ Notes migrated")
        else:
            if st.button("✅ Mark notes as migrated", key="migrate_notes"):
                set_migrated(uid, "notes", customer_id, True)
                st.cache_data.clear()
                st.rerun()

    # Bookings
    st.subheader("📅 Bookings")

    if bookings.empty or "CustomerId" not in bookings.columns:
        st.info("No bookings for this customer (or Bookings.csv not uploaded).")
    else:
        cust_bookings = bookings[bookings["CustomerId"] == customer_id].copy()

        if cust_bookings.empty:
            st.info("No bookings for this customer.")
        else:
            if "StartDateTime" not in cust_bookings.columns or "EndDateTime" not in cust_bookings.columns:
                st.error("Cannot find StartDateTime/EndDateTime columns in bookings.")
            else:
                # Parse dates
                cust_bookings["StartDateTimeParsed"] = pd.to_datetime(
                    cust_bookings["StartDateTime"], errors="coerce"
                )
                cust_bookings["EndDateTimeParsed"] = pd.to_datetime(
                    cust_bookings["EndDateTime"], errors="coerce"
                )

                cust_bookings["StartDate"] = cust_bookings["StartDateTimeParsed"].dt.strftime("%d/%m/%Y")
                cust_bookings["StartTime"] = cust_bookings["StartDateTimeParsed"].dt.strftime("%H:%M")
                cust_bookings["EndDate"] = cust_bookings["EndDateTimeParsed"].dt.strftime("%d/%m/%Y")
                cust_bookings["EndTime"] = cust_bookings["EndDateTimeParsed"].dt.strftime("%H:%M")

                filter_option = st.radio(
                    "Show bookings:",
                    ["All", "Past", "Next 3 Months", "Next 6 Months", "Next 12 Months"],
                    horizontal=True
                )

                now_dt = datetime.now()
                future_ranges = {
                    "Next 3 Months": now_dt + timedelta(days=90),
                    "Next 6 Months": now_dt + timedelta(days=180),
                    "Next 12 Months": now_dt + timedelta(days=365),
                }

                if filter_option == "Past":
                    cust_bookings = cust_bookings[cust_bookings["StartDateTimeParsed"] < now_dt]
                elif filter_option in future_ranges:
                    end_date = future_ranges[filter_option]
                    cust_bookings = cust_bookings[
                        (cust_bookings["StartDateTimeParsed"] >= now_dt) &
                        (cust_bookings["StartDateTimeParsed"] <= end_date)
                    ]

                for idx, b in cust_bookings.iterrows():
                    is_migrated = to_bool(b.get("migrated", False))

                    color = "#3cb371" if b["StartDateTimeParsed"] >= now_dt else "#888888"
                    strike = "text-decoration: line-through;" if is_migrated else ""

                    st.markdown(
                        f"<div style='background-color:{color}25;padding:8px;"
                        f"border-radius:6px;margin-bottom:6px;{strike}'>"
                        f"<b>{b.get('Staff', '')}</b> | {b.get('Service', '')} | "
                        f"{b['StartDate']} {b['StartTime']} → "
                        f"{b['EndDate']} {b['EndTime']}"
                        f"</div>",
                        unsafe_allow_html=True
                    )

                    fields = {
                        "Service": b.get("Service", ""),
                        "Staff": b.get("Staff", ""),
                        "Price": b.get("Price", ""),
                        "Recurring": "Yes" if to_bool(b.get("RecurringAppointment")) else "No",
                        "Start Date": b["StartDate"],
                        "Start Time": b["StartTime"],
                        "End Date": b["EndDate"],
                        "End Time": b["EndTime"],
                    }

                    # Reduce spacing between booking fields
                    st.markdown("""
                        <style>
                        div[data-testid="stVerticalBlock"] > div:has(div.stCodeBlock) {
                            margin-bottom: -1rem;
                        }
                        </style>
                    """, unsafe_allow_html=True)

                    for label, val in fields.items():
                        col1, col2 = st.columns([0.3, 0.7])
                        col1.markdown(f"**{label}**")
                        if is_migrated:
                            col2.markdown(
                                f"<code style='text-decoration: line-through;'>{val}</code>",
                                unsafe_allow_html=True
                            )
                        else:
                            col2.code(str(val), language=None)

                    notes_txt = b.get("Notes", "")
                    if notes_txt and str(notes_txt) != "nan":
                        st.markdown("**Notes**")
                        
                        # Escape backticks and quotes for JavaScript
                        escaped_notes = str(notes_txt).replace('\\', '\\\\').replace('`', '\\`').replace("'", "\\'")
                        
                        strike = "text-decoration: line-through;" if is_migrated else ""
                        
                        st.markdown(
                            f"<style>"
                            f".notes-container-{idx} .copy-btn {{ opacity: 0; transition: opacity 0.2s; }}"
                            f".notes-container-{idx}:hover .copy-btn {{ opacity: 1; }}"
                            f"</style>"
                            f"<div class='notes-container-{idx}' style='position: relative;'>"
                            f"<button class='copy-btn' onclick=\"navigator.clipboard.writeText(`{escaped_notes}`)\" "
                            f"style='position: absolute; top: 8px; right: 8px; z-index: 10; background: white; border: 1px solid #ccc; "
                            f"border-radius: 3px; padding: 4px 8px; cursor: pointer; font-size: 12px;'>📋</button>"
                            f"<pre style='max-height: 150px; overflow-y: auto; background-color: #f0f0f0; "
                            f"padding: 0.5rem; border-radius: 0.25rem; border: 1px solid rgba(49, 51, 63, 0.2); "
                            f"{strike} white-space: pre-wrap; font-family: \"Source Code Pro\", monospace; font-size: 14px; margin: 0;'>{notes_txt}</pre>"
                            f"</div>",
                            unsafe_allow_html=True
                        )

                    # Per-booking migration toggle (uses BookingId)
                    booking_id = b.get("BookingId", None)
                    if booking_id is not None and booking_id != "":
                        if is_migrated:
                            st.success("✓ This booking is marked as migrated.")
                        else:
                            if st.button(
                                "✅ Mark this booking as migrated",
                                key=f"migrate-booking-{booking_id}-{idx}"
                            ):
                                set_migrated(uid, "bookings", booking_id, True)
                                st.cache_data.clear()
                                st.rerun()
                    else:
                        st.warning("⚠️ This booking row has no BookingId – cannot persist migrated state.")

                    st.markdown("---")