"""
Daisy Data Viewer - Stable Fast Version
"""

import io
import json
import re
import time
from datetime import datetime, timedelta
from typing import Dict, Tuple

import pandas as pd
import streamlit as st
import requests
import pyrebase
import firebase_admin
from firebase_admin import credentials, firestore

# -----------------------------------------------------------------------------
# 1. PAGE CONFIG (MUST BE FIRST)
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Daisy Data Viewer",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🚗"
)

# -----------------------------------------------------------------------------
# 2. CUSTOM CSS & JS
# -----------------------------------------------------------------------------
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    
    /* Data Cards */
    .data-field-card {
        background-color: #f8f9fa; border: 1px solid #e9ecef; border-radius: 8px;
        padding: 10px 14px; margin-bottom: 8px; cursor: pointer; transition: all 0.2s ease;
    }
    .stDark .data-field-card { background-color: #262730; border: 1px solid #414248; }
    .data-field-card:hover { border-color: #FF4B4B; transform: translateY(-1px); }
    
    .data-field-label { font-size: 0.7rem; text-transform: uppercase; color: #888; font-weight: 600; display: flex; justify-content: space-between; }
    .data-field-value { font-size: 0.95rem; font-weight: 500; color: #111; word-break: break-word; margin-top: 4px;}
    .stDark .data-field-value { color: #fff; }
    
    /* Hide Standard Streamlit Elements */
    #MainMenu {visibility: hidden;} footer {visibility: hidden;}
    
    /* Toast */
    #toast-container { position: fixed; bottom: 20px; right: 20px; z-index: 9999; }
    .toast { background: #333; color: white; padding: 10px 20px; border-radius: 4px; margin-top: 10px; animation: fadeIn 0.3s, fadeOut 0.3s 2.5s forwards; }
    @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
    @keyframes fadeOut { from { opacity: 1; } to { opacity: 0; } }
    </style>
    
    <div id="toast-container"></div>
    <script>
    function copyToClipboard(text) {
        navigator.clipboard.writeText(text).then(function() {
            const container = document.getElementById('toast-container');
            const toast = document.createElement('div');
            toast.className = 'toast';
            toast.innerHTML = '📋 Copied to clipboard';
            container.appendChild(toast);
            setTimeout(() => { toast.remove(); }, 3000);
        }, function(err) { console.error('Could not copy text: ', err); });
    }
    </script>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 3. LOGIC: CLEANSING & GEOCODING
# -----------------------------------------------------------------------------
def clean_customer_name(name: str) -> Tuple[str, str]:
    if pd.isna(name): return "", ""
    noise = [r'\s*-\s*ACC\s*$', r'\s*ACC\s*$', r'\s*Albany\s*$', r'\s*TM\s*$', r'\(.*?\)', r'\s*-\s*CMA.*$']
    cleaned = str(name).strip()
    for pattern in noise:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    parts = cleaned.strip().split()
    if not parts: return "", ""
    if len(parts) == 1: return parts[0], ""
    return parts[0], " ".join(parts[1:])

def extract_booking_notes(notes_text: str) -> Dict[str, str]:
    if pd.isna(notes_text) or not notes_text: return {"from": "", "to": "", "notes": ""}
    text = str(notes_text)
    from_match = re.search(r'FROM[:\s]+([^G]*?)(?=GOING TO|TO:|$)', text, re.IGNORECASE)
    to_match = re.search(r'(?:GOING TO|TO)[:\s]+([^*]*?)(?=\*\*|$)', text, re.IGNORECASE)
    
    remaining = text
    if from_match: remaining = remaining.replace(from_match.group(0), '')
    if to_match: remaining = remaining.replace(to_match.group(0), '')
    remaining = re.sub(r'\*+', '', remaining).strip()
    
    return {
        "from": from_match.group(1).strip() if from_match else "",
        "to": to_match.group(1).strip() if to_match else "",
        "notes": remaining
    }

class AddressCacheManager:
    def __init__(self, db):
        self.db = db
        self.collection = "address_cache"

    def get_cached_geocoding(self, raw_address):
        if not self.db or not raw_address: return None
        doc_id = re.sub(r'[^a-zA-Z0-9]', '', str(raw_address).lower())
        if not doc_id: return None
        doc = self.db.collection(self.collection).document(doc_id).get()
        return doc.to_dict() if doc.exists else None

    def cache_result(self, raw_address, result):
        if not self.db or not raw_address: return
        doc_id = re.sub(r'[^a-zA-Z0-9]', '', str(raw_address).lower())
        if not doc_id: return
        self.db.collection(self.collection).document(doc_id).set(result)

class CachedGeocoder:
    def __init__(self, api_key, cache_mgr):
        self.api_key = api_key
        self.cache = cache_mgr
    
    def geocode(self, address):
        cached = self.cache.get_cached_geocoding(address)
        if cached: return cached
        
        base = "https://maps.googleapis.com/maps/api/geocode/json"
        try:
            r = requests.get(base, params={"address": address, "key": self.api_key})
            data = r.json()
            if data['status'] == 'OK':
                res = data['results'][0]
                result = {
                    "raw": address, "valid": True,
                    "formatted_address": res['formatted_address'],
                    "lat": res['geometry']['location']['lat'],
                    "lng": res['geometry']['location']['lng']
                }
                self.cache.cache_result(address, result)
                return result
            else:
                fail = {"raw": address, "valid": False, "error": data['status']}
                self.cache.cache_result(address, fail)
                return fail
        except Exception as e:
            return {"valid": False, "error": str(e)}

# -----------------------------------------------------------------------------
# 4. FIREBASE INITIALIZATION (Lazy Loading to prevent WSOD)
# -----------------------------------------------------------------------------
def get_firebase_app():
    # Check secrets
    if "FIREBASE" not in st.secrets:
        st.error("❌ Secrets missing: [FIREBASE] section not found.")
        st.stop()
        
    fb_sec = st.secrets["FIREBASE"]
    
    # 1. Initialize Pyrebase (Auth/Storage)
    # We forcefully inject databaseURL to prevent KeyError, even if not used.
    config = {
        "apiKey": fb_sec.get("api_key"),
        "authDomain": fb_sec.get("auth_domain"),
        "projectId": fb_sec.get("project_id"),
        "storageBucket": fb_sec.get("storage_bucket"),
        "messagingSenderId": fb_sec.get("messaging_sender_id"),
        "appId": fb_sec.get("app_id"),
        "databaseURL": fb_sec.get("database_url", "https://dummy-placeholder.firebaseio.com")
    }
    
    try:
        app = pyrebase.initialize_app(config)
    except Exception as e:
        st.error(f"❌ Pyrebase Init Error: {e}")
        st.stop()

    # 2. Initialize Firestore (Admin)
    db = None
    if "admin_json" in fb_sec:
        try:
            if not firebase_admin._apps:
                admin_data = fb_sec["admin_json"]
                # Handle string vs dict
                if isinstance(admin_data, str):
                    cred_info = json.loads(admin_data)
                else:
                    cred_info = dict(admin_data)
                cred = credentials.Certificate(cred_info)
                firebase_admin.initialize_app(cred)
            db = firestore.client()
        except Exception as e:
            st.warning(f"⚠️ Firestore Warning: {e}")
            
    return app, db

# -----------------------------------------------------------------------------
# 5. DATA LOADING
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False, ttl=3600)
def load_data(uid, _auth_instance, _storage_instance):
    # We pass instances to avoid pickling issues, but use token inside
    # Re-auth if needed or just use token
    user = st.session_state["auth"]
    token = user["token"]
    bucket_name = st.secrets['FIREBASE']['storage_bucket']
    
    def download_csv(filename):
        path = f"franchises/{uid}/{filename}"
        # Direct download link generation to avoid pyrebase storage complexity if possible
        # But we use pyrebase logic here:
        url = f"https://firebasestorage.googleapis.com/v0/b/{bucket_name}/o/{path.replace('/', '%2F')}?alt=media"
        try:
            r = requests.get(url, headers={"Authorization": f"Bearer {token}"})
            if r.status_code == 200:
                return pd.read_csv(io.BytesIO(r.content), low_memory=False)
        except Exception:
            pass
        return None

    cust = download_csv("Customers.csv")
    notes = download_csv("Notes.csv")
    bookings = download_csv("Bookings.csv")

    # Defaults
    if cust is None: cust = pd.DataFrame(columns=["CustomerId", "CustomerName"])
    if bookings is None: bookings = pd.DataFrame(columns=["BookingId", "CustomerId", "StartDateTime"])
    if notes is None: notes = pd.DataFrame(columns=["CustomerId", "NoteText"])

    # Type conversion for IDs
    for df in [cust, notes, bookings]:
        if not df.empty and "CustomerId" in df.columns:
            df["CustomerId"] = df["CustomerId"].astype(str).str.split('.').str[0]

    # Cleansing
    if "CustomerName" in cust.columns:
        cust["DisplayName"] = cust["CustomerName"].fillna(cust.get("CompanyName", "Unknown"))
        names = cust["DisplayName"].apply(clean_customer_name)
        cust["CleanFirstName"] = names.apply(lambda x: x[0])
        cust["CleanLastName"] = names.apply(lambda x: x[1])

    if not bookings.empty:
        bookings["StartDT"] = pd.to_datetime(bookings["StartDateTime"], errors='coerce')
        if "Notes" in bookings.columns:
            extracted = bookings["Notes"].apply(extract_booking_notes)
            bookings["CleanFrom"] = extracted.apply(lambda x: x["from"])
            bookings["CleanTo"] = extracted.apply(lambda x: x["to"])
            bookings["CleanNotes"] = extracted.apply(lambda x: x["notes"])

    return cust, notes, bookings

def get_migrations(uid, db):
    if not db: return {}
    try:
        docs = db.collection("migrations").document(uid).collection("customers").stream()
        return {d.id: True for d in docs if d.to_dict().get("migrated")}
    except:
        return {}

# -----------------------------------------------------------------------------
# 6. UI RENDERERS
# -----------------------------------------------------------------------------
def render_field(label, value):
    if pd.isna(value) or str(value).strip() == "": return
    clean_val = str(value).strip()
    safe_val = clean_val.replace("'", "\\'").replace('"', '&quot;')
    st.markdown(f"""
    <div class="data-field-card" onclick="copyToClipboard('{safe_val}')">
        <div class="data-field-label">{label} <span>📋</span></div>
        <div class="data-field-value">{clean_val}</div>
    </div>
    """, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 7. MAIN APP
# -----------------------------------------------------------------------------
def main():
    # Initialize Firebase SAFELY inside main
    firebase_app, db = get_firebase_app()
    auth = firebase_app.auth()
    
    # Session State
    if "auth" not in st.session_state: st.session_state["auth"] = None
    if "migrations" not in st.session_state: st.session_state["migrations"] = None

    # --- LOGIN SCREEN ---
    if not st.session_state["auth"]:
        c1, c2, c3 = st.columns([1,2,1])
        with c2:
            st.title("Daisy Data Viewer 🚗")
            with st.form("login_form"):
                email = st.text_input("Email")
                password = st.text_input("Password", type="password")
                submitted = st.form_submit_button("Login", use_container_width=True)
                
                if submitted:
                    try:
                        user = auth.sign_in_with_email_and_password(email, password)
                        # Get User Info
                        acc = auth.get_account_info(user["idToken"])["users"][0]
                        st.session_state["auth"] = {
                            "uid": acc["localId"], 
                            "token": user["idToken"], 
                            "email": email
                        }
                        st.rerun()
                    except Exception as e:
                        st.error(f"Login failed: {e}")
        return

    # --- APP LOAD ---
    uid = st.session_state["auth"]["uid"]
    
    with st.spinner("Loading franchise data..."):
        cust_df, notes_df, bookings_df = load_data(uid, auth, firebase_app.storage())
        
        if st.session_state["migrations"] is None:
            st.session_state["migrations"] = get_migrations(uid, db)

    # Apply Migrations
    cust_df["Migrated"] = cust_df["CustomerId"].map(st.session_state["migrations"]).fillna(False)

    # --- SIDEBAR & FILTERS ---
    with st.sidebar:
        st.header("🔍 Filters")
        search = st.text_input("Search", placeholder="Name, Phone, ID...")
        
        st.subheader("Bookings")
        time_filter = st.radio("Timeframe", ["All Future", "Next 3 Months", "Next 6 Months", "Next 12 Months", "Any Time"])
        
        hide_migrated = st.checkbox("Hide Migrated Customers", value=True)
        
        st.divider()
        if st.button("Refresh Data"):
            st.cache_data.clear()
            st.rerun()
            
    # --- FILTERING LOGIC ---
    filtered = cust_df.copy()
    
    # 1. Migrated
    if hide_migrated:
        filtered = filtered[~filtered["Migrated"]]
        
    # 2. Search
    if search:
        s = search.lower()
        # Fast concat search
        corpus = (
            filtered["DisplayName"].fillna("") + " " + 
            filtered["Telephone"].fillna("") + " " + 
            filtered["PhysicalAddress"].fillna("")
        ).str.lower()
        filtered = filtered[corpus.str.contains(s)]
        
    # 3. Timeframe (The complicated one)
    if not bookings_df.empty:
        now = datetime.now()
        # Get list of customers with valid bookings based on timeframe
        valid_bookings = bookings_df.copy()
        
        if time_filter != "Any Time":
            valid_bookings = valid_bookings[valid_bookings["StartDT"] >= now]
            
            if time_filter == "Next 3 Months":
                valid_bookings = valid_bookings[valid_bookings["StartDT"] <= now + timedelta(days=90)]
            elif time_filter == "Next 6 Months":
                valid_bookings = valid_bookings[valid_bookings["StartDT"] <= now + timedelta(days=180)]
            elif time_filter == "Next 12 Months":
                valid_bookings = valid_bookings[valid_bookings["StartDT"] <= now + timedelta(days=365)]
        
        # If filtering by time, restrict customers
        if time_filter != "Any Time":
            valid_cids = valid_bookings["CustomerId"].unique()
            filtered = filtered[filtered["CustomerId"].isin(valid_cids)]

    # --- MAIN UI ---
    c_list, c_detail = st.columns([4, 6], gap="medium")
    
    # LEFT: List
    with c_list:
        st.markdown(f"### Customers ({len(filtered)})")
        
        # Prepare display table
        display_tbl = filtered[["Migrated", "DisplayName", "Telephone", "CustomerId"]].copy()
        display_tbl["Status"] = display_tbl["Migrated"].apply(lambda x: "✅" if x else "⚡")
        
        selection = st.dataframe(
            display_tbl[["Status", "DisplayName", "Telephone"]],
            use_container_width=True,
            hide_index=True,
            height=700,
            on_select="rerun",
            selection_mode="single-row",
            column_config={
                "Status": st.column_config.TextColumn("", width="small"),
                "DisplayName": st.column_config.TextColumn("Name", width="large")
            }
        )
        
        selected_id = None
        if selection.selection.rows:
            idx = selection.selection.rows[0]
            selected_id = filtered.iloc[idx]["CustomerId"]
            
    # RIGHT: Details
    with c_detail:
        if selected_id:
            cust = cust_df[cust_df["CustomerId"] == selected_id].iloc[0]
            c_bookings = bookings_df[bookings_df["CustomerId"] == selected_id]
            c_notes = notes_df[notes_df["CustomerId"] == selected_id]
            
            # Header
            h1, h2 = st.columns([3, 1])
            with h1:
                st.markdown(f"## {cust['DisplayName']}")
                st.caption(f"ID: {selected_id}")
            with h2:
                view_mode = st.radio("View", ["Clean", "Raw"], horizontal=True, label_visibility="collapsed")
                is_clean = (view_mode == "Clean")
            
            # Migration Control
            if cust["Migrated"]:
                if st.button("↩ Undo Migration", use_container_width=True):
                    st.session_state["migrations"][selected_id] = False
                    if db: db.collection("migrations").document(uid).collection("customers").document(selected_id).set({"migrated": False}, merge=True)
                    st.rerun()
            else:
                if st.button("✅ Mark Done", type="primary", use_container_width=True):
                    st.session_state["migrations"][selected_id] = True
                    if db: db.collection("migrations").document(uid).collection("customers").document(selected_id).set({"migrated": True}, merge=True)
                    st.rerun()
            
            st.markdown("---")
            
            # Fields
            f1, f2 = st.columns(2)
            with f1:
                render_field("First Name", cust.get("CleanFirstName") if is_clean else "")
                render_field("Last Name", cust.get("CleanLastName") if is_clean else "")
                render_field("Phone", cust.get("Telephone"))
                render_field("Email", cust.get("Email"))
            with f2:
                render_field("Mobile", cust.get("SMS"))
                addr = cust.get("PhysicalAddress")
                render_field("Address", addr)
                
                # Geocoding
                if addr and "GOOGLE" in st.secrets:
                    mgr = AddressCacheManager(db)
                    cached = mgr.get_cached_geocoding(addr)
                    if cached:
                        if cached.get("valid"):
                            st.success(f"📍 {cached.get('formatted_address')}")
                        else:
                            st.error("Address not found")
                    else:
                        if st.button("Validate Address"):
                            geo = CachedGeocoder(st.secrets["GOOGLE"]["geocoding_api_key"], mgr)
                            geo.geocode(addr)
                            st.rerun()

            # History Tabs
            t1, t2 = st.tabs([f"Bookings ({len(c_bookings)})", f"Notes ({len(c_notes)})"])
            
            with t1:
                if not c_bookings.empty:
                    c_bookings = c_bookings.sort_values("StartDT", ascending=False)
                    for _, b in c_bookings.iterrows():
                        dstr = b["StartDT"].strftime("%d %b %Y %H:%M") if pd.notna(b["StartDT"]) else "No Date"
                        
                        if is_clean and "CleanFrom" in b:
                            with st.container(border=True):
                                st.markdown(f"**{dstr}**")
                                cA, cB = st.columns(2)
                                cA.markdown(f"**From:** {b['CleanFrom']}")
                                cB.markdown(f"**To:** {b['CleanTo']}")
                                if b['CleanNotes']: st.caption(b['CleanNotes'])
                        else:
                            st.text(f"{dstr}: {b.get('Notes','')}")
                else:
                    st.info("No bookings")
            
            with t2:
                for _, n in c_notes.iterrows():
                    st.info(n["NoteText"])
                    
        else:
            st.markdown("<div style='text-align:center; color:#888; margin-top:50px;'>Select a customer</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()