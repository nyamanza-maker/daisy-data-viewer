"""
Daisy Data Viewer - Advanced Data Cleansing & Migration Tool
Beautiful UI with AI-powered data enrichment and validation
"""

import io
import json
import re
from datetime import datetime, timedelta
from time import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import requests
import html

import pyrebase
import firebase_admin
from firebase_admin import credentials, firestore

from cleansing import (
    AddressCacheManager,
    CachedGeocoder,
    CustomerProcessor,
    BookingProcessor
)

# ----------------------------------
# Page Configuration
# ----------------------------------
st.set_page_config(
    page_title="Daisy Data Viewer",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🚗"
)

# Custom CSS for beautiful UI
st.markdown("""
    <style>
    /* Main theme colors */
    :root {
        --daisy-pink: #E91E63;
        --daisy-dark: #1a1a1a;
        --daisy-gray: #f5f5f5;
        --daisy-orange: #FF9800;
        --daisy-green: #4CAF50;
    }
    
    /* Header styling */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 2rem;
    }
    
    /* Section cards */
    .section-card {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        margin-bottom: 1.5rem;
        border-left: 4px solid #667eea;
    }
    
    /* Data view toggle */
    .view-toggle {
        background: #f8f9fa;
        padding: 0.5rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }
    
    /* Address warning */
    .address-warning {
        color: #FF9800;
        font-weight: 500;
    }
    
    /* Migrated styling */
    .migrated-item {
        text-decoration: line-through;
        opacity: 0.6;
    }
    
    /* Stats cards */
    .stat-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 8px;
        color: white;
        text-align: center;
    }
    
    /* Clean/Original badge */
    .data-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-left: 0.5rem;
    }
    
    .badge-clean {
        background: #4CAF50;
        color: white;
    }
    
    .badge-original {
        background: #607D8B;
        color: white;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    </style>
""", unsafe_allow_html=True)

# ----------------------------------
# Firebase Configuration
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
# Firebase Admin (Firestore)
# ----------------------------------
db = None

def init_admin_db():
    global db
    if "FIREBASE" not in st.secrets or "admin_json" not in st.secrets["FIREBASE"]:
        return None
    try:
        admin_data = st.secrets["FIREBASE"]["admin_json"]
        if isinstance(admin_data, str):
            cred_info = json.loads(admin_data)
        else:
            cred_info = dict(admin_data)
        
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_info)
            firebase_admin.initialize_app(cred)
        
        db = firestore.client()
        return db
    except Exception as e:
        st.sidebar.error(f"Firestore error: {e}")
        return None

db = init_admin_db()

# ----------------------------------
# Geocoding Components
# ----------------------------------
def get_geocoding_components():
    """Initialize geocoding components"""
    if db is None:
        return None, None, None, None
    
    if "GOOGLE" not in st.secrets or "geocoding_api_key" not in st.secrets["GOOGLE"]:
        return None, None, None, None
    
    api_key = st.secrets["GOOGLE"]["geocoding_api_key"]
    cache_mgr = AddressCacheManager(db)
    geocoder = CachedGeocoder(api_key, cache_mgr, rate_limit=50)
    customer_proc = CustomerProcessor(geocoder, cache_mgr)
    booking_proc = BookingProcessor(geocoder, cache_mgr)
    
    return cache_mgr, geocoder, customer_proc, booking_proc

# ----------------------------------
# Helper Functions
# ----------------------------------
def parse_firebase_login_error(e: Exception) -> str:
    raw = str(e)
    if "INVALID_EMAIL" in raw:
        return "Invalid email format."
    if "EMAIL_NOT_FOUND" in raw:
        return "No account exists with this email."
    if "INVALID_PASSWORD" in raw or "INVALID_LOGIN_CREDENTIALS" in raw:
        return "Incorrect password."
    if "USER_DISABLED" in raw:
        return "Account disabled."
    if "TOO_MANY_ATTEMPTS_TRY_LATER" in raw:
        return "Too many attempts. Try again later."
    return "Authentication failed."

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

# ----------------------------------
# Firestore Migration Functions
# ----------------------------------
def _mig_doc(uid: str, coll: str, doc_id: str):
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
        return bool(doc.to_dict().get("migrated", False))
    return False

def add_migration_flags_batch(customers, notes, bookings, uid: str):
    """Batch load migration flags from Firestore"""
    if db is None:
        if customers is not None:
            customers["Migrated"] = False
        if notes is not None:
            notes["Migrated"] = False
        if bookings is not None:
            bookings["migrated"] = False
        return customers, notes, bookings
    
    try:
        migrations_ref = db.collection("migrations").document(uid)
        
        # Batch load customer migrations
        customer_migrations = {}
        if customers is not None and "CustomerId" in customers.columns:
            for doc in migrations_ref.collection("customers").stream():
                customer_migrations[str(doc.id)] = doc.to_dict().get("migrated", False)
            customers["Migrated"] = customers["CustomerId"].apply(
                lambda cid: customer_migrations.get(str(cid), False)
            )
        
        # Batch load note migrations
        note_migrations = {}
        if notes is not None and "CustomerId" in notes.columns:
            for doc in migrations_ref.collection("notes").stream():
                note_migrations[str(doc.id)] = doc.to_dict().get("migrated", False)
            notes["Migrated"] = notes["CustomerId"].apply(
                lambda cid: note_migrations.get(str(cid), False)
            )
        
        # Batch load booking migrations
        booking_migrations = {}
        if bookings is not None and "BookingId" in bookings.columns:
            for doc in migrations_ref.collection("bookings").stream():
                booking_migrations[str(doc.id)] = doc.to_dict().get("migrated", False)
            bookings["migrated"] = bookings["BookingId"].apply(
                lambda bid: booking_migrations.get(str(bid), False)
            )
    except Exception as e:
        st.warning(f"Could not load migration flags: {e}")
    
    return customers, notes, bookings

# ----------------------------------
# Storage Functions
# ----------------------------------
def storage_path_for(uid: str, filename: str) -> str:
    return f"franchises/{uid}/{filename}"

def upload_bytes(uid: str, filename: str, content: bytes, id_token: str):
    path = storage_path_for(uid, filename)
    storage.child(path).put(io.BytesIO(content), id_token)

def file_exists(uid: str, filename: str, id_token: str) -> bool:
    path = f"franchises/{uid}/{filename}"
    url = f"https://firebasestorage.googleapis.com/v0/b/{firebase_config['storageBucket']}/o/{path.replace('/', '%2F')}"
    headers = {"Authorization": f"Bearer {id_token}"}
    try:
        r = requests.get(url, headers=headers)
        return r.status_code == 200
    except:
        return False

def download_csv_as_df(uid: str, filename: str, id_token: str, **kwargs):
    path = f"franchises/{uid}/{filename}"
    url = f"https://firebasestorage.googleapis.com/v0/b/{firebase_config['storageBucket']}/o/{path.replace('/', '%2F')}?alt=media"
    headers = {"Authorization": f"Bearer {id_token}"}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to download {filename}")
    return pd.read_csv(io.BytesIO(r.content), **kwargs)

# ----------------------------------
# Data Cleansing Functions
# ----------------------------------
def clean_customer_name(name: str) -> Tuple[str, str]:
    """Clean and split customer name into first and last"""
    if pd.isna(name):
        return "", ""
    
    # Remove common suffixes and noise
    noise_patterns = [
        r'\s*-\s*ACC\s*$', r'\s*ACC\s*$', r'\s*Albany\s*$',
        r'\s*TM\s*$', r'\(.*?\)', r'\s*-\s*CMA.*$'
    ]
    
    cleaned = str(name).strip()
    for pattern in noise_patterns:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    cleaned = cleaned.strip()
    
    # Split into first and last
    parts = cleaned.split()
    if len(parts) == 0:
        return "", ""
    elif len(parts) == 1:
        return parts[0], ""
    else:
        return parts[0], " ".join(parts[1:])

def extract_booking_addresses(notes_text: str) -> Dict[str, str]:
    """Extract FROM, TO, and remaining notes from booking notes"""
    if pd.isna(notes_text) or not notes_text:
        return {"from": "", "to": "", "notes": ""}
    
    text = str(notes_text)
    
    # Pattern matching for FROM and TO
    from_match = re.search(r'FROM[:\s]+([^G]*?)(?=GOING TO|TO:|$)', text, re.IGNORECASE)
    to_match = re.search(r'(?:GOING TO|TO)[:\s]+([^*]*?)(?=\*\*|$)', text, re.IGNORECASE)
    
    from_addr = from_match.group(1).strip() if from_match else ""
    to_addr = to_match.group(1).strip() if to_match else ""
    
    # Extract remaining notes (everything else)
    remaining = text
    if from_match:
        remaining = remaining.replace(from_match.group(0), '')
    if to_match:
        remaining = remaining.replace(to_match.group(0), '')
    
    # Clean up remaining notes
    remaining = re.sub(r'\*+', '', remaining).strip()
    
    return {
        "from": from_addr,
        "to": to_addr,
        "notes": remaining
    }

def clean_note_text(note_text: str) -> str:
    """Lightweight cleansing for free‑text notes.
    - Normalize whitespace and newlines
    - Remove BOM/non‑breaking spaces
    - Collapse decorative asterisks
    """
    if pd.isna(note_text):
        return ""
    t = str(note_text)
    t = t.replace("\ufeff", "").replace("\u00a0", " ")
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    t = "\n".join(line.strip() for line in t.split("\n"))
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\*{2,}", "*", t)
    return t.strip()

# ----------------------------------
# Session State Initialization
# ----------------------------------
if "auth" not in st.session_state:
    st.session_state["auth"] = None
if "login_attempts" not in st.session_state:
    st.session_state["login_attempts"] = 0
if "lockout_until" not in st.session_state:
    st.session_state["lockout_until"] = 0
if "search_history" not in st.session_state:
    st.session_state["search_history"] = []
if "view_mode" not in st.session_state:
    st.session_state["view_mode"] = "cleansed"

# ----------------------------------
# Authentication UI
# ----------------------------------
with st.sidebar:
    st.markdown("### 🚗 Daisy Data Viewer")
    st.markdown("---")
    
    now = time()
    MAX_ATTEMPTS = 5
    LOCKOUT_DURATION = 300
    
    if st.session_state["auth"] is None:
        st.markdown("#### Sign In")
        email = st.text_input("Email", key="login_email")
        password = st.text_input("Password", type="password", key="login_password")
        
        if st.session_state["lockout_until"] > now:
            remaining = int(st.session_state["lockout_until"] - now)
            st.error(f"🔒 Locked out for {remaining}s")
        else:
            if st.button("Login", type="primary", use_container_width=True):
                if st.session_state["login_attempts"] >= MAX_ATTEMPTS:
                    st.session_state["lockout_until"] = now + LOCKOUT_DURATION
                    st.session_state["login_attempts"] = 0
                    st.error("Too many attempts. Locked out for 5 minutes.")
                else:
                    try:
                        user = auth.sign_in_with_email_and_password(email, password)
                        account = auth.get_account_info(user["idToken"])["users"][0]
                        
                        if not account.get("emailVerified", False):
                            st.session_state["login_attempts"] += 1
                            st.error("Please verify your email first.")
                        else:
                            st.session_state["login_attempts"] = 0
                            st.session_state["auth"] = {
                                "email": email,
                                "uid": account["localId"],
                                "idToken": user["idToken"],
                            }
                            st.success("✓ Signed in")
                            st.rerun()
                    except Exception as e:
                        st.session_state["login_attempts"] += 1
                        st.error(parse_firebase_login_error(e))
    else:
        st.success(f"✓ {st.session_state['auth']['email']}")
        if st.button("Logout", use_container_width=True):
            st.session_state["auth"] = None
            st.rerun()

if st.session_state["auth"] is None:
    st.markdown("<div class='main-header'><h1>🚗 Daisy Data Viewer</h1><p>Please sign in to continue</p></div>", unsafe_allow_html=True)
    st.stop()

uid = st.session_state["auth"]["uid"]
id_token = st.session_state["auth"]["idToken"]

# ----------------------------------
# File Upload Section
# ----------------------------------
with st.sidebar:
    st.markdown("---")
    st.markdown("#### 📁 Data Files")
    
    has_customers = file_exists(uid, "Customers.csv", id_token)
    has_notes = file_exists(uid, "Notes.csv", id_token)
    has_bookings = file_exists(uid, "Bookings.csv", id_token)
    
    all_uploaded = has_customers and has_notes and has_bookings
    
    if all_uploaded:
        with st.expander("✓ All files uploaded", expanded=False):
            cust_file = st.file_uploader("Customers", type=["csv"], key="cust")
            notes_file = st.file_uploader("Notes", type=["csv"], key="notes")
            book_file = st.file_uploader("Bookings", type=["csv"], key="book")
            
            if st.button("Upload", use_container_width=True):
                try:
                    if cust_file:
                        upload_bytes(uid, "Customers.csv", cust_file.getvalue(), id_token)
                    if notes_file:
                        upload_bytes(uid, "Notes.csv", notes_file.getvalue(), id_token)
                    if book_file:
                        upload_bytes(uid, "Bookings.csv", book_file.getvalue(), id_token)
                    st.success("✓ Uploaded")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Upload failed: {e}")
    else:
        cust_file = st.file_uploader("Customers", type=["csv"], key="cust")
        notes_file = st.file_uploader("Notes", type=["csv"], key="notes")
        book_file = st.file_uploader("Bookings", type=["csv"], key="book")
        
        if st.button("Upload Files", type="primary", use_container_width=True):
            try:
                if cust_file:
                    upload_bytes(uid, "Customers.csv", cust_file.getvalue(), id_token)
                if notes_file:
                    upload_bytes(uid, "Notes.csv", notes_file.getvalue(), id_token)
                if book_file:
                    upload_bytes(uid, "Bookings.csv", book_file.getvalue(), id_token)
                st.success("✓ Uploaded")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Upload failed: {e}")
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Customers", "✓" if has_customers else "✗")
        col2.metric("Notes", "✓" if has_notes else "✗")
        col3.metric("Bookings", "✓" if has_bookings else "✗")

# ----------------------------------
# Address Validation Section (NEW)
# ----------------------------------
with st.sidebar:
    st.markdown("---")
    st.markdown("#### 🌍 Address Validation")
    
    cache_mgr, geocoder, customer_proc, booking_proc = get_geocoding_components()
    
    if cache_mgr:
        # Show cache stats
        stats = cache_mgr.get_cache_stats()
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Cached", stats.get("total_addresses", 0))
        col2.metric("Valid", stats.get("valid", 0))
        col3.metric("Invalid", stats.get("invalid", 0))
        
        if stats.get("deduplication_rate", 0) > 0:
            st.caption(f"💾 {stats.get('deduplication_rate', 0)}% cache savings")
        
        # Process customers button
        if st.button("🚀 Validate Customer Addresses", use_container_width=True, key="validate_customers"):
            progress_placeholder = st.empty()
            status_placeholder = st.empty()
            
            def progress_callback(msg):
                progress_placeholder.info(msg)
            
            with st.spinner("Processing customer addresses..."):
                try:
                    # Reload customers fresh
                    if file_exists(uid, "Customers.csv", id_token):
                        customers_to_process = download_csv_as_df(uid, "Customers.csv", id_token, low_memory=False)
                        
                        processed_customers = customer_proc.process_customers(
                            customers_to_process,
                            uid,
                            progress_callback
                        )
                        
                        final_stats = geocoder.get_stats()
                        status_placeholder.success(
                            f"✅ Processed {len(processed_customers):,} customers | "
                            f"API calls: {final_stats['api_requests']} | "
                            f"Cache hits: {final_stats['cache_hits']:,} | "
                            f"Cost: ${final_stats['estimated_cost']}"
                        )
                        
                        st.info("💡 Geocoding complete! Addresses cached in Firestore.")
                        
                except Exception as e:
                    status_placeholder.error(f"Error: {e}")
                    import traceback
                    st.code(traceback.format_exc())
    else:
        st.warning("⚠️ Geocoding not configured")
        st.caption("Add Google API key to secrets to enable address validation")

# ----------------------------------
# Load Data
# ----------------------------------
@st.cache_data(ttl=300)
def load_data(uid: str, id_token: str):
    customers = notes = bookings = None
    
    if file_exists(uid, "Customers.csv", id_token):
        customers = download_csv_as_df(uid, "Customers.csv", id_token, low_memory=False)
        
        # Add cleansed fields
        if "CustomerName" in customers.columns:
            customers[["CleanFirstName", "CleanLastName"]] = customers["CustomerName"].apply(
                lambda x: pd.Series(clean_customer_name(x))
            )
    
    if file_exists(uid, "Notes.csv", id_token):
        notes = download_csv_as_df(uid, "Notes.csv", id_token, low_memory=False)
    
    if file_exists(uid, "Bookings.csv", id_token):
        bookings = download_csv_as_df(uid, "Bookings.csv", id_token, low_memory=False)
        
        # Extract booking addresses
        if "Notes" in bookings.columns:
            extracted = bookings["Notes"].apply(extract_booking_addresses)
            bookings["CleanFrom"] = extracted.apply(lambda x: x["from"])
            bookings["CleanTo"] = extracted.apply(lambda x: x["to"])
            bookings["CleanNotes"] = extracted.apply(lambda x: x["notes"])
    
    return customers, notes, bookings

customers, notes, bookings = load_data(uid, id_token)

if customers is None:
    st.warning("⚠️ Please upload Customers.csv to begin")
    st.stop()

# Fill in missing dataframes
if notes is None:
    notes = pd.DataFrame(columns=["CustomerId", "NoteText"])
if bookings is None:
    bookings = pd.DataFrame(columns=["BookingId", "CustomerId", "Notes"])

# Add migration flags
customers, notes, bookings = add_migration_flags_batch(customers, notes, bookings, uid)

# ----------------------------------
# Main Header
# ----------------------------------
st.markdown("""
    <div class='main-header'>
        <h1>🚗 Daisy Data Viewer</h1>
        <p>Advanced Data Cleansing & Migration Tool</p>
    </div>
""", unsafe_allow_html=True)

# Stats Row
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(f"""
        <div class='stat-card'>
            <h3>{len(customers):,}</h3>
            <p>Customers</p>
        </div>
    """, unsafe_allow_html=True)
with col2:
    st.markdown(f"""
        <div class='stat-card'>
            <h3>{len(notes):,}</h3>
            <p>Notes</p>
        </div>
    """, unsafe_allow_html=True)
with col3:
    st.markdown(f"""
        <div class='stat-card'>
            <h3>{len(bookings):,}</h3>
            <p>Bookings</p>
        </div>
    """, unsafe_allow_html=True)
with col4:
    migrated_count = customers["Migrated"].sum() if "Migrated" in customers.columns else 0
    st.markdown(f"""
        <div class='stat-card'>
            <h3>{int(migrated_count):,}</h3>
            <p>Migrated</p>
        </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# ----------------------------------
# Search & Filters
# ----------------------------------
if "current_search" not in st.session_state:
    st.session_state["current_search"] = ""

search_query = st.sidebar.text_input(
    "Search customers by name, company, or phone",
    value=st.session_state["current_search"],
    key="sidebar_search_input",
    placeholder="Type to search..."
)

if search_query != st.session_state["current_search"]:
    st.session_state["current_search"] = search_query

# Filters (Sidebar)
exclude_migrated = st.sidebar.checkbox("Hide migrated customers", value=True)
future_only = st.sidebar.checkbox("Only with future bookings", value=False)
max_results = st.sidebar.number_input("Max results", 25, 5000, 200, 25)

# Cleanup legacy widget state if present
if "search_input" in st.session_state:
    try:
        del st.session_state["search_input"]
    except Exception:
        pass

# Apply filters
if "CustomerName" in customers.columns:
    customers["DisplayName"] = customers["CustomerName"]
elif "CompanyName" in customers.columns:
    customers["DisplayName"] = customers["CompanyName"]
else:
    customers["DisplayName"] = "Unknown"

# Search filter
search_lower = st.session_state["current_search"].lower()
if search_lower:
    mask = customers.apply(
        lambda x: x.astype(str).str.lower().str.contains(search_lower, na=False)
    ).any(axis=1)
    filtered_customers = customers[mask]
else:
    filtered_customers = customers.copy()

# Migration filter
if exclude_migrated and "Migrated" in filtered_customers.columns:
    filtered_customers = filtered_customers[~filtered_customers["Migrated"].map(to_bool)]

# Future bookings filter
if future_only and not bookings.empty and "StartDateTime" in bookings.columns:
    bookings["StartDT"] = pd.to_datetime(bookings["StartDateTime"], errors="coerce")
    future_cids = bookings[bookings["StartDT"] >= datetime.now()]["CustomerId"].unique()
    filtered_customers = filtered_customers[filtered_customers["CustomerId"].isin(future_cids)]

# Limit results
filtered_customers = filtered_customers.head(max_results)

st.markdown(f"**Showing {len(filtered_customers):,} customers**")

# ----------------------------------
# Customer Selection
# ----------------------------------
if len(filtered_customers) == 0:
    st.info("No customers found. Try adjusting your search or filters.")
    st.stop()

selected_customer_name = st.selectbox(
    "Select a customer",
    filtered_customers["DisplayName"].tolist(),
    key="customer_selector"
)

selected_customer = filtered_customers[
    filtered_customers["DisplayName"] == selected_customer_name
].iloc[0]

customer_id = selected_customer["CustomerId"]

# ----------------------------------
# Customer Details Section
# ----------------------------------
st.markdown("---")
st.markdown(f"## 👤 {selected_customer_name}")

is_customer_migrated = to_bool(selected_customer.get("Migrated", False))

if is_customer_migrated:
    st.success("✓ Customer marked as migrated")
else:
    if st.button("✅ Mark customer as migrated", key="migrate_customer"):
        set_migrated(uid, "customers", customer_id, True)
        st.cache_data.clear()
        st.rerun()

st.markdown("<div class='section-card'>", unsafe_allow_html=True)

# Customer section view toggle (right-aligned)
cust_head_left, cust_head_right = st.columns([0.8, 0.2])
with cust_head_right:
    cust_cleansed = st.checkbox(
        "Cleansed view",
        value=st.session_state.get("view_mode_customer", True),
        key="view_toggle_customer",
    )
st.session_state["view_mode_customer"] = bool(cust_cleansed)
cust_view_is_cleansed = bool(cust_cleansed)
badge_class = "badge-clean" if cust_view_is_cleansed else "badge-original"
badge_text = "CLEANSED" if cust_view_is_cleansed else "ORIGINAL"

st.markdown(f"### Customer Information <span class='data-badge {badge_class}'>{badge_text}</span>", unsafe_allow_html=True)

# Display customer fields
if cust_view_is_cleansed:
    fields = {
        "First Name": selected_customer.get("CleanFirstName", ""),
        "Last Name": selected_customer.get("CleanLastName", ""),
        "Phone": selected_customer.get("Telephone", ""),
        "Mobile": selected_customer.get("SMS", ""),
        "Email": selected_customer.get("Email", ""),
        "Address": selected_customer.get("PhysicalAddress", ""),
        "Gender": selected_customer.get("Gender", ""),
        "DOB": selected_customer.get("DateOfBirth", ""),
    }
else:
    fields = {
        "Name": selected_customer.get("CustomerName", ""),
        "Company": selected_customer.get("CompanyName", ""),
        "Phone": selected_customer.get("Telephone", ""),
        "Mobile": selected_customer.get("SMS", ""),
        "Email": selected_customer.get("Email", ""),
        "Physical Address": selected_customer.get("PhysicalAddress", ""),
        "Postal Address": selected_customer.get("PostalAddress", ""),
        "Gender": selected_customer.get("Gender", ""),
        "DOB": selected_customer.get("DateOfBirth", ""),
    }

for label, value in fields.items():
    if pd.notna(value) and str(value).strip():
        col1, col2 = st.columns([0.3, 0.7])
        col1.markdown(f"**{label}**")
        strike = "migrated-item" if is_customer_migrated else ""
        col2.markdown(f"<span class='{strike}'>{value}</span>", unsafe_allow_html=True)

st.markdown("</div>", unsafe_allow_html=True)

# ----------------------------------
# Validated Address Display (NEW)
# ----------------------------------
if "PhysicalAddress" in selected_customer and pd.notna(selected_customer["PhysicalAddress"]):
    address = str(selected_customer["PhysicalAddress"]).strip()
    
    # Check if we have geocoding data from cache
    cache_mgr, geocoder, _, _ = get_geocoding_components()
    
    if cache_mgr and address:
        cached_result = cache_mgr.get_cached_geocoding(address)
        
        if cached_result:
            st.markdown("---")
            st.markdown("### 📍 Validated Address")
            
            col1, col2 = st.columns([0.75, 0.25])
            
            with col1:
                is_valid = cached_result.get("valid", False)
                is_partial = cached_result.get("partial_match", False)
                
                if is_valid and not is_partial:
                    st.success(f"✓ {cached_result.get('formatted_address', '')}")
                elif is_valid and is_partial:
                    st.warning(f"⚠️ Partial match: {cached_result.get('formatted_address', '')}")
                else:
                    st.error(f"❌ Could not validate: {address}")
                
                # Show address components
                if is_valid:
                    components = []
                    if cached_result.get("suburb"):
                        components.append(cached_result["suburb"])
                    if cached_result.get("state"):
                        components.append(cached_result["state"])
                    if cached_result.get("postcode"):
                        components.append(cached_result["postcode"])
                    
                    if components:
                        st.caption(f"📮 {', '.join(str(c) for c in components)}")
                    
                    # Show coordinates
                    if cached_result.get("lat") and cached_result.get("lng"):
                        st.caption(f"🗺️ {cached_result['lat']}, {cached_result['lng']}")
            
            with col2:
                if st.button("🔄 Recheck", key=f"recheck_{customer_id}"):
                    if geocoder:
                        with st.spinner("Rechecking address..."):
                            result = geocoder.geocode(address, uid, force_recheck=True)
                            
                            if result:
                                st.success("✓ Address rechecked!")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error("Failed to recheck address")
                    else:
                        st.error("Geocoding not available")
        else:
            # Address not yet geocoded
            st.info("💡 Address not yet validated. Click 'Validate Customer Addresses' in the sidebar to geocode all addresses.")

# ----------------------------------
# Notes Section
# ----------------------------------
st.markdown("### 📝 Customer Notes")

# Notes section view toggle (right-aligned, UI only)
notes_head_left, notes_head_right = st.columns([0.8, 0.2])
with notes_head_right:
    notes_cleansed = st.checkbox(
        "Cleansed view",
        value=st.session_state.get("view_mode_notes", True),
        key="view_toggle_notes",
    )
st.session_state["view_mode_notes"] = bool(notes_cleansed)

customer_notes = notes[notes["CustomerId"] == customer_id] if "CustomerId" in notes.columns else pd.DataFrame()

if customer_notes.empty:
    st.info("No notes for this customer")
else:
    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    # Notes view badge for cleansed/original
    notes_view_is_cleansed = bool(st.session_state.get("view_mode_notes", True))
    # Badge is implicit via the right-aligned toggle; remove extra box
    
    for idx, note in customer_notes.iterrows():
        is_note_migrated = to_bool(note.get("Migrated", False))
        note_text = note.get("NoteText", "")
        note_date = note.get("NoteDate", "")
        
        col1, col2 = st.columns([0.8, 0.2])
        with col1:
            if note_date:
                st.markdown(f"**{note_date}**")
            strike = "text-decoration: line-through;" if is_note_migrated else ""
            # Choose cleansed or original text
            if notes_view_is_cleansed:
                display_text = note.get("CleanNoteText") or clean_note_text(note_text)
            else:
                display_text = str(note_text or "")
            st.markdown(
                f"<div style='{strike}'><pre style='white-space: pre-wrap; margin: 0;'>" +
                f"{html.escape(display_text)}</pre></div>",
                unsafe_allow_html=True,
            )
        
        with col2:
            if is_note_migrated:
                st.success("✓ Migrated")
            else:
                if st.button("Mark migrated", key=f"note_{idx}"):
                    set_migrated(uid, "notes", customer_id, True)
                    st.cache_data.clear()
                    st.rerun()
        
        st.markdown("---")
    
    st.markdown("</div>", unsafe_allow_html=True)

# ----------------------------------
# Bookings Section
# ----------------------------------
st.markdown("### 📅 Customer Bookings")

customer_bookings = bookings[bookings["CustomerId"] == customer_id].copy() if "CustomerId" in bookings.columns else pd.DataFrame()

if customer_bookings.empty:
    st.info("No bookings for this customer")
else:
    # Booking filters + view toggle
    col1, col2, col3 = st.columns([0.6, 0.2, 0.2])
    
    with col1:
        booking_filter = st.radio(
            "Show bookings:",
            ["All", "Past", "Next 3 Months", "Next 6 Months", "Next 12 Months"],
            horizontal=True,
            key="booking_filter"
        )
    
    with col2:
        hide_migrated_bookings = st.checkbox("Hide migrated", value=False, key="hide_migrated")
    with col3:
        book_cleansed = st.checkbox(
            "Cleansed view",
            value=st.session_state.get("view_mode_bookings", True),
            key="view_toggle_bookings",
        )
        st.session_state["view_mode_bookings"] = bool(book_cleansed)
    
    # Parse dates
    if "StartDateTime" in customer_bookings.columns:
        customer_bookings["StartDT"] = pd.to_datetime(
            customer_bookings["StartDateTime"], errors="coerce"
        )
        customer_bookings["EndDT"] = pd.to_datetime(
            customer_bookings["EndDateTime"], errors="coerce"
        )
        
        customer_bookings["StartDate"] = customer_bookings["StartDT"].dt.strftime("%d/%m/%Y")
        customer_bookings["StartTime"] = customer_bookings["StartDT"].dt.strftime("%H:%M")
        customer_bookings["EndDate"] = customer_bookings["EndDT"].dt.strftime("%d/%m/%Y")
        customer_bookings["EndTime"] = customer_bookings["EndDT"].dt.strftime("%H:%M")
        
        # Apply date filter
        now = datetime.now()
        if booking_filter == "Past":
            customer_bookings = customer_bookings[customer_bookings["StartDT"] < now]
        elif booking_filter == "Next 3 Months":
            end_date = now + timedelta(days=90)
            customer_bookings = customer_bookings[
                (customer_bookings["StartDT"] >= now) & 
                (customer_bookings["StartDT"] <= end_date)
            ]
        elif booking_filter == "Next 6 Months":
            end_date = now + timedelta(days=180)
            customer_bookings = customer_bookings[
                (customer_bookings["StartDT"] >= now) & 
                (customer_bookings["StartDT"] <= end_date)
            ]
        elif booking_filter == "Next 12 Months":
            end_date = now + timedelta(days=365)
            customer_bookings = customer_bookings[
                (customer_bookings["StartDT"] >= now) & 
                (customer_bookings["StartDT"] <= end_date)
            ]
        
        # Apply migrated filter
        if hide_migrated_bookings and "migrated" in customer_bookings.columns:
            customer_bookings = customer_bookings[~customer_bookings["migrated"].map(to_bool)]
    
    st.markdown(f"**{len(customer_bookings)} bookings shown**")
    
    # Display bookings
    for idx, booking in customer_bookings.iterrows():
        is_migrated = to_bool(booking.get("migrated", False))
        booking_id = booking.get("BookingId", "")
        
        # Determine card color
        is_future = False
        if "StartDT" in booking and pd.notna(booking["StartDT"]):
            is_future = booking["StartDT"] >= datetime.now()
        
        card_color = "#e8f5e9" if is_future else "#f5f5f5"
        border_color = "#4CAF50" if is_future else "#9E9E9E"
        
        if is_migrated:
            card_color = "#fafafa"
            border_color = "#BDBDBD"
        
        st.markdown(f"""
            <div style='background-color:{card_color}; 
                        border-left: 4px solid {border_color}; 
                        padding: 1rem; 
                        border-radius: 8px; 
                        margin-bottom: 1rem;'>
        """, unsafe_allow_html=True)
        
        # Booking header
        staff = booking.get("Staff", "Unassigned")
        service = booking.get("Service", "Unknown Service")
        start_date = booking.get("StartDate", "")
        start_time = booking.get("StartTime", "")
        end_date = booking.get("EndDate", "")
        end_time = booking.get("EndTime", "")
        
        strike = "text-decoration: line-through;" if is_migrated else ""
        
        st.markdown(f"""
            <div style='{strike}'>
                <strong>👨‍💼 {staff}</strong> | 
                🚗 {service} | 
                🕒 {start_date} {start_time} → {end_date} {end_time}
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown("</div>", unsafe_allow_html=True)

        # Per-booking view toggle (right-aligned)
        header_spacer, header_toggle = st.columns([0.8, 0.2])
        with header_toggle:
            _default = bool(st.session_state.get("view_mode_bookings", True))
            _toggle_key = f"view_mode_booking_{booking_id or idx}"
            _val = st.checkbox(
                "Cleansed view",
                value=bool(st.session_state.get(_toggle_key, _default)),
                key=f"toggle_{booking_id or idx}",
            )
            st.session_state[_toggle_key] = bool(_val)
        
        # Booking details in expandable section
        with st.expander("View booking details", expanded=False):
            st.markdown("<div class='section-card'>", unsafe_allow_html=True)
            
            # Show cleansed or original based on per-booking toggle
            default_book_view = bool(st.session_state.get("view_mode_bookings", True))
            booking_key = f"view_mode_booking_{booking_id or idx}"
            booking_view_is_cleansed = bool(st.session_state.get(booking_key, default_book_view))
            if booking_view_is_cleansed:
                fields = {
                    "Service": booking.get("Service", ""),
                    "Staff": booking.get("Staff", ""),
                    "Price": booking.get("Price", ""),
                    "Recurring": "Yes" if to_bool(booking.get("RecurringAppointment")) else "No",
                    "Start Date": start_date,
                    "Start Time": start_time,
                    "End Date": end_date,
                    "End Time": end_time,
                }
                
                # Display fields
                for label, value in fields.items():
                    col1, col2 = st.columns([0.3, 0.7])
                    col1.markdown(f"**{label}**")
                    if is_migrated:
                        col2.markdown(f"<code style='text-decoration: line-through;'>{value}</code>", unsafe_allow_html=True)
                    else:
                        col2.code(str(value), language=None)
                
                # Booking FROM
                if "CleanFrom" in booking and booking["CleanFrom"]:
                    st.markdown("**FROM Address**")
                    from_text = booking["CleanFrom"]
                    if is_migrated:
                        st.markdown(f"<code style='text-decoration: line-through;'>{from_text}</code>", unsafe_allow_html=True)
                    else:
                        st.code(from_text, language=None)
                
                # Booking TO
                if "CleanTo" in booking and booking["CleanTo"]:
                    st.markdown("**TO Address**")
                    to_text = booking["CleanTo"]
                    if is_migrated:
                        st.markdown(f"<code style='text-decoration: line-through;'>{to_text}</code>", unsafe_allow_html=True)
                    else:
                        st.code(to_text, language=None)
                
                # Cleansed notes
                if "CleanNotes" in booking and booking["CleanNotes"]:
                    st.markdown("**Booking Notes**")
                    notes_text = booking["CleanNotes"]
                    escaped_notes = str(notes_text).replace('\\', '\\\\').replace('`', '\\`').replace("'", "\\'")
                    
                    strike_style = "text-decoration: line-through;" if is_migrated else ""
                    
                    st.markdown(f"""
                        <style>
                        .notes-container-{idx} .copy-btn {{ opacity: 0; transition: opacity 0.2s; }}
                        .notes-container-{idx}:hover .copy-btn {{ opacity: 1; }}
                        </style>
                        <div class='notes-container-{idx}' style='position: relative;'>
                            <button class='copy-btn' onclick="navigator.clipboard.writeText(`{escaped_notes}`)" 
                                style='position: absolute; top: 8px; right: 8px; z-index: 10; background: white; 
                                border: 1px solid #ccc; border-radius: 3px; padding: 4px 8px; cursor: pointer; 
                                font-size: 12px;'>📋</button>
                            <pre style='max-height: 150px; overflow-y: auto; background-color: #f0f0f0; 
                                padding: 0.5rem; border-radius: 0.25rem; border: 1px solid rgba(49, 51, 63, 0.2); 
                                {strike_style} white-space: pre-wrap; font-family: "Source Code Pro", monospace; 
                                font-size: 14px; margin: 0;'>{notes_text}</pre>
                        </div>
                    """, unsafe_allow_html=True)
            
            else:
                # Original view - show raw booking notes
                fields = {
                    "Service": booking.get("Service", ""),
                    "Staff": booking.get("Staff", ""),
                    "Price": booking.get("Price", ""),
                    "Recurring": "Yes" if to_bool(booking.get("RecurringAppointment")) else "No",
                    "Start Date": start_date,
                    "Start Time": start_time,
                    "End Date": end_date,
                    "End Time": end_time,
                }
                
                for label, value in fields.items():
                    col1, col2 = st.columns([0.3, 0.7])
                    col1.markdown(f"**{label}**")
                    if is_migrated:
                        col2.markdown(f"<code style='text-decoration: line-through;'>{value}</code>", unsafe_allow_html=True)
                    else:
                        col2.code(str(value), language=None)
                
                # Original notes
                if "Notes" in booking and pd.notna(booking["Notes"]) and str(booking["Notes"]) != "nan":
                    st.markdown("**Original Booking Notes**")
                    notes_text = booking["Notes"]
                    escaped_notes = str(notes_text).replace('\\', '\\\\').replace('`', '\\`').replace("'", "\\'")
                    
                    strike_style = "text-decoration: line-through;" if is_migrated else ""
                    
                    st.markdown(f"""
                        <style>
                        .notes-container-orig-{idx} .copy-btn {{ opacity: 0; transition: opacity 0.2s; }}
                        .notes-container-orig-{idx}:hover .copy-btn {{ opacity: 1; }}
                        </style>
                        <div class='notes-container-orig-{idx}' style='position: relative;'>
                            <button class='copy-btn' onclick="navigator.clipboard.writeText(`{escaped_notes}`)" 
                                style='position: absolute; top: 8px; right: 8px; z-index: 10; background: white; 
                                border: 1px solid #ccc; border-radius: 3px; padding: 4px 8px; cursor: pointer; 
                                font-size: 12px;'>📋</button>
                            <pre style='max-height: 150px; overflow-y: auto; background-color: #f0f0f0; 
                                padding: 0.5rem; border-radius: 0.25rem; border: 1px solid rgba(49, 51, 63, 0.2); 
                                {strike_style} white-space: pre-wrap; font-family: "Source Code Pro", monospace; 
                                font-size: 14px; margin: 0;'>{notes_text}</pre>
                        </div>
                    """, unsafe_allow_html=True)
            
            # Migration button
            st.markdown("---")
            if is_migrated:
                st.success("✓ This booking is marked as migrated")
            else:
                if booking_id and st.button(
                    "✅ Mark this booking as migrated",
                    key=f"migrate_booking_{booking_id}_{idx}"
                ):
                    set_migrated(uid, "bookings", booking_id, True)
                    st.cache_data.clear()
                    st.rerun()
            
            st.markdown("</div>", unsafe_allow_html=True)

# ----------------------------------
# Footer
# ----------------------------------
st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #666; padding: 2rem;'>
        <p>🚗 Daisy Data Viewer | Advanced Data Cleansing & Migration</p>
        <p style='font-size: 0.9rem;'>Secure • Private • AI-Powered</p>
    </div>
""", unsafe_allow_html=True)
