"""
Daisy Data Viewer - Enterprise Edition v2
High-performance Master-Detail UI with Geocoding, Advanced Filters, and Smart Caching
"""

import io
import json
import re
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import requests
import pyrebase
import firebase_admin
from firebase_admin import credentials, firestore

# -----------------------------------------------------------------------------
# 1. CONFIGURATION & STYLING
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Daisy Data Viewer",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🚗"
)

# INJECT CUSTOM CSS & JS FOR CLIPBOARD & LAYOUT
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* Custom Card for Data Fields */
    .data-field-card {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 8px;
        padding: 10px 14px;
        margin-bottom: 8px;
        cursor: pointer;
        transition: all 0.2s ease;
        position: relative;
        display: flex;
        flex-direction: column;
    }
    
    .stDark .data-field-card {
        background-color: #262730;
        border: 1px solid #414248;
    }

    .data-field-card:hover {
        border-color: #FF4B4B;
        background-color: #fff;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
    }
    
    .stDark .data-field-card:hover {
        background-color: #31333F;
    }

    .card-header-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 4px;
    }

    .data-field-label {
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #888;
        font-weight: 600;
    }
    
    .copy-icon {
        font-size: 0.8rem;
        opacity: 0.3;
    }
    
    .data-field-card:hover .copy-icon {
        opacity: 1;
        color: #FF4B4B;
    }
    
    .data-field-value {
        font-size: 0.95rem;
        font-weight: 500;
        color: #111;
        word-break: break-word;
    }
    
    .stDark .data-field-value {
        color: #fff;
    }

    /* Toast Notification */
    #toast-container {
        position: fixed;
        bottom: 20px;
        right: 20px;
        z-index: 9999;
    }
    
    .toast {
        background: #333;
        color: white;
        padding: 10px 20px;
        border-radius: 4px;
        margin-top: 10px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        animation: fadeIn 0.3s, fadeOut 0.3s 2.5s forwards;
        font-size: 0.9rem;
    }

    @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
    @keyframes fadeOut { from { opacity: 1; } to { opacity: 0; } }

    /* Layout Utilities */
    .master-header {
        border-bottom: 1px solid #eee;
        padding-bottom: 1rem;
        margin-bottom: 1rem;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Badge Styles */
    .badge-clean { background: #e6fffa; color: #047857; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; border: 1px solid #a7f3d0; }
    .badge-raw { background: #f3f4f6; color: #4b5563; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; border: 1px solid #e5e7eb; }
    </style>

    <div id="toast-container"></div>
    <script>
    function copyToClipboard(text) {
        navigator.clipboard.writeText(text).then(function() {
            const container = document.getElementById('toast-container');
            const toast = document.createElement('div');
            toast.className = 'toast';
            toast.innerHTML = '📋 Copied: ' + text.substring(0, 20) + (text.length > 20 ? '...' : '');
            container.appendChild(toast);
            setTimeout(() => { toast.remove(); }, 3000);
        }, function(err) {
            console.error('Could not copy text: ', err);
        });
    }
    </script>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 2. UTILS & CLEANSING LOGIC
# -----------------------------------------------------------------------------
def clean_customer_name(name: str) -> Tuple[str, str]:
    """Split name into First/Last and remove noise."""
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
    """Extract TO/FROM from booking notes."""
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

# -----------------------------------------------------------------------------
# 3. GEOCODING CLASSES (Restored)
# -----------------------------------------------------------------------------
class AddressCacheManager:
    def __init__(self, db):
        self.db = db
        self.collection = "address_cache"

    def get_cached_geocoding(self, raw_address):
        if not self.db or not raw_address: return None
        doc_id = re.sub(r'[^a-zA-Z0-9]', '', str(raw_address).lower())
        doc = self.db.collection(self.collection).document(doc_id).get()
        return doc.to_dict() if doc.exists else None

    def cache_result(self, raw_address, result):
        if not self.db or not raw_address: return
        doc_id = re.sub(r'[^a-zA-Z0-9]', '', str(raw_address).lower())
        self.db.collection(self.collection).document(doc_id).set(result)

class CachedGeocoder:
    def __init__(self, api_key, cache_mgr):
        self.api_key = api_key
        self.cache = cache_mgr
    
    def geocode(self, address):
        # 1. Check Cache
        cached = self.cache.get_cached_geocoding(address)
        if cached: return cached
        
        # 2. Call API
        base = "https://maps.googleapis.com/maps/api/geocode/json"
        try:
            r = requests.get(base, params={"address": address, "key": self.api_key})
            data = r.json()
            
            if data['status'] == 'OK':
                res = data['results'][0]
                formatted = res['formatted_address']
                loc = res['geometry']['location']
                
                # Extract components
                comps = res['address_components']
                suburb = next((c['long_name'] for c in comps if 'locality' in c['types']), "")
                state = next((c['short_name'] for c in comps if 'administrative_area_level_1' in c['types']), "")
                postal = next((c['long_name'] for c in comps if 'postal_code' in c['types']), "")
                
                result = {
                    "raw": address,
                    "valid": True,
                    "formatted_address": formatted,
                    "suburb": suburb, "state": state, "postcode": postal,
                    "lat": loc['lat'], "lng": loc['lng'],
                    "timestamp": firestore.SERVER_TIMESTAMP
                }
                self.cache.cache_result(address, result)
                return result
            else:
                fail_res = {"raw": address, "valid": False, "error": data['status']}
                self.cache.cache_result(address, fail_res)
                return fail_res
        except Exception as e:
            return {"valid": False, "error": str(e)}

# -----------------------------------------------------------------------------
# 4. FIREBASE & SETUP
# -----------------------------------------------------------------------------
def init_firebase():
    fb_sec = st.secrets.get("FIREBASE")
    if not fb_sec: st.error("Missing FIREBASE secrets"); st.stop()
    
    # Client SDK
    config = {k:v for k,v in fb_sec.items() if k in ['apiKey','authDomain','projectId','storageBucket','messagingSenderId','appId','databaseURL']}
    firebase_app = pyrebase.initialize_app(config)
    
    # Admin SDK
    db = None
    if "admin_json" in fb_sec:
        try:
            admin_data = fb_sec["admin_json"]
            cred_info = json.loads(admin_data) if isinstance(admin_data, str) else dict(admin_data)
            if not firebase_admin._apps:
                cred = credentials.Certificate(cred_info)
                firebase_admin.initialize_app(cred)
            db = firestore.client()
        except Exception as e:
            st.warning(f"Firestore init failed: {e}")

    return firebase_app, db

firebase, db = init_firebase()
auth = firebase.auth()
storage = firebase.storage()

# -----------------------------------------------------------------------------
# 5. DATA ENGINE (Cached & Cleansed)
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False, ttl=3600)
def load_and_clean_data(uid, id_token):
    start_time = time.time()
    
    def get_df(fname):
        path = f"franchises/{uid}/{fname}"
        url = f"https://firebasestorage.googleapis.com/v0/b/{st.secrets['FIREBASE']['storage_bucket']}/o/{path.replace('/', '%2F')}?alt=media"
        try:
            r = requests.get(url, headers={"Authorization": f"Bearer {id_token}"})
            if r.status_code == 200:
                return pd.read_csv(io.BytesIO(r.content), low_memory=False)
        except: pass
        return None

    cust = get_df("Customers.csv")
    notes = get_df("Notes.csv")
    bookings = get_df("Bookings.csv")
    
    # Initialize defaults
    if cust is None: cust = pd.DataFrame(columns=["CustomerId", "CustomerName"])
    if bookings is None: bookings = pd.DataFrame(columns=["BookingId", "CustomerId", "StartDateTime"])

    # --- PROCESSING ---
    
    # 1. Ensure IDs are Strings for merging
    cust["CustomerId"] = cust["CustomerId"].astype(str).str.split('.').str[0]
    if not bookings.empty and "CustomerId" in bookings.columns:
        bookings["CustomerId"] = bookings["CustomerId"].astype(str).str.split('.').str[0]
    if notes is not None and not notes.empty:
         notes["CustomerId"] = notes["CustomerId"].astype(str).str.split('.').str[0]

    # 2. Clean Customers
    if "CustomerName" in cust.columns:
        cust["DisplayName"] = cust["CustomerName"].fillna(cust.get("CompanyName", "Unknown"))
        cleaned_names = cust["DisplayName"].apply(clean_customer_name)
        cust["CleanFirstName"] = cleaned_names.apply(lambda x: x[0])
        cust["CleanLastName"] = cleaned_names.apply(lambda x: x[1])
    
    # 3. Process Bookings (Dates & Notes)
    if not bookings.empty:
        bookings["StartDT"] = pd.to_datetime(bookings["StartDateTime"], errors='coerce')
        if "Notes" in bookings.columns:
            extracted = bookings["Notes"].apply(extract_booking_notes)
            bookings["CleanFrom"] = extracted.apply(lambda x: x["from"])
            bookings["CleanTo"] = extracted.apply(lambda x: x["to"])

    load_time = time.time() - start_time
    return cust, notes, bookings, load_time

def get_migration_status(uid):
    """Fetches migration map from Firestore."""
    if not db: return {}
    try:
        # Fetch all at once for performance
        docs = db.collection("migrations").document(uid).collection("customers").stream()
        return {d.id: True for d in docs if d.to_dict().get("migrated")}
    except:
        return {}

# -----------------------------------------------------------------------------
# 6. SESSION STATE
# -----------------------------------------------------------------------------
if "auth" not in st.session_state: st.session_state["auth"] = None
if "local_migrations" not in st.session_state: st.session_state["local_migrations"] = None
if "view_mode" not in st.session_state: st.session_state["view_mode"] = "Cleansed"

# -----------------------------------------------------------------------------
# 7. UI COMPONENTS
# -----------------------------------------------------------------------------
def render_copy_card(label, value):
    """Renders a clickable card that copies value to clipboard."""
    if pd.isna(value) or str(value).strip() == "": return
    
    clean_val = str(value).strip()
    escaped_val = clean_val.replace("'", "\\'").replace('"', '&quot;')
    
    html = f"""
    <div class="data-field-card" onclick="copyToClipboard('{escaped_val}')">
        <div class="card-header-row">
            <span class="data-field-label">{label}</span>
            <span class="copy-icon">📋</span>
        </div>
        <div class="data-field-value">{clean_val}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 8. MAIN APPLICATION
# -----------------------------------------------------------------------------
def main():
    # LOGIN
    if not st.session_state["auth"]:
        c1, c2, c3 = st.columns([1,2,1])
        with c2:
            st.title("🚗 Daisy Data")
            with st.form("login"):
                email = st.text_input("Email")
                password = st.text_input("Password", type="password")
                if st.form_submit_button("Sign In", use_container_width=True):
                    try:
                        user = auth.sign_in_with_email_and_password(email, password)
                        acc = auth.get_account_info(user["idToken"])["users"][0]
                        st.session_state["auth"] = {"uid": acc["localId"], "token": user["idToken"], "email": email}
                        st.rerun()
                    except Exception as e:
                        st.error(f"Login failed: {e}")
        return

    # APP INIT
    uid = st.session_state["auth"]["uid"]
    token = st.session_state["auth"]["token"]
    
    # Load Data
    cust_df, notes_df, bookings_df, load_time = load_and_clean_data(uid, token)
    
    # Load Migrations (Once)
    if st.session_state["local_migrations"] is None:
        st.session_state["local_migrations"] = get_migration_status(uid)

    # Merge Migration Status
    cust_df["Migrated"] = cust_df["CustomerId"].map(st.session_state["local_migrations"]).fillna(False)

    # --- SIDEBAR ---
    with st.sidebar:
        st.header("🔍 Filters")
        search_term = st.text_input("Search", placeholder="Name, Phone, ID...", label_visibility="collapsed")
        
        st.subheader("Booking Filters")
        filter_timeframe = st.radio("Show customers with bookings in:", 
                                  ["Any Time", "Next 3 Months", "Next 6 Months", "Next 12 Months", "Future (All)"])
        
        hide_migrated = st.checkbox("Hide Migrated", value=True)
        
        st.divider()
        
        # Debug / System Health
        with st.expander("⚙️ System Health"):
            st.metric("Load Time", f"{load_time:.2f}s")
            st.text(f"Customers: {len(cust_df)}")
            st.text(f"Bookings: {len(bookings_df)}")
            st.text(f"Notes: {len(notes_df)}")
            if st.button("🗑 Clear Cache"):
                st.cache_data.clear()
                st.rerun()

    # --- FILTERING LOGIC ---
    filtered_df = cust_df.copy()
    
    # 1. Migration Filter
    if hide_migrated:
        filtered_df = filtered_df[~filtered_df["Migrated"]]

    # 2. Search Filter
    if search_term:
        s = search_term.lower()
        search_series = (
            filtered_df["DisplayName"].fillna("") + " " + 
            filtered_df["Telephone"].fillna("") + " " + 
            filtered_df["PhysicalAddress"].fillna("") + " " +
            filtered_df["CustomerId"]
        ).str.lower()
        filtered_df = filtered_df[search_series.str.contains(s)]

    # 3. Booking Timeframe Filter
    if filter_timeframe != "Any Time" and not bookings_df.empty:
        now = datetime.now()
        future_bookings = bookings_df[bookings_df["StartDT"] >= now]
        
        if filter_timeframe == "Next 3 Months":
            future_bookings = future_bookings[future_bookings["StartDT"] <= now + timedelta(days=90)]
        elif filter_timeframe == "Next 6 Months":
            future_bookings = future_bookings[future_bookings["StartDT"] <= now + timedelta(days=180)]
        elif filter_timeframe == "Next 12 Months":
            future_bookings = future_bookings[future_bookings["StartDT"] <= now + timedelta(days=365)]
            
        valid_cids = future_bookings["CustomerId"].unique()
        filtered_df = filtered_df[filtered_df["CustomerId"].isin(valid_cids)]

    # --- MAIN LAYOUT ---
    col_list, col_detail = st.columns([4, 6], gap="large")

    # --- LEFT: CUSTOMER LIST ---
    with col_list:
        st.markdown(f'<div class="master-header"><h3>👥 Customers ({len(filtered_df)})</h3></div>', unsafe_allow_html=True)
        
        # Simplified table for speed
        table_df = filtered_df[["Migrated", "DisplayName", "Telephone", "CustomerId"]].copy()
        table_df["Status"] = table_df["Migrated"].apply(lambda x: "✅" if x else "⚡")
        
        event = st.dataframe(
            table_df[["Status", "DisplayName", "Telephone"]],
            use_container_width=True,
            hide_index=True,
            height=700,
            on_select="rerun",
            selection_mode="single-row",
            column_config={
                "Status": st.column_config.TextColumn("", width="small"),
                "DisplayName": st.column_config.TextColumn("Customer", width="large"),
            }
        )

        selected_cid = None
        if event.selection.rows:
            idx = event.selection.rows[0]
            selected_cid = filtered_df.iloc[idx]["CustomerId"]

    # --- RIGHT: DETAIL WORKSPACE ---
    with col_detail:
        if selected_cid:
            # Get Data
            cust = cust_df[cust_df["CustomerId"] == selected_cid].iloc[0]
            c_notes = notes_df[notes_df["CustomerId"] == selected_cid] if notes_df is not None else pd.DataFrame()
            c_bookings = bookings_df[bookings_df["CustomerId"] == selected_cid] if bookings_df is not None else pd.DataFrame()

            # Header
            c1, c2 = st.columns([0.7, 0.3])
            with c1:
                st.title(cust["DisplayName"])
                st.caption(f"ID: {selected_cid}")
            with c2:
                # View Toggle
                mode = st.radio("View Mode", ["Cleansed", "Original"], horizontal=True, label_visibility="collapsed")
                is_clean = (mode == "Cleansed")
                
                # Migration Button
                if cust["Migrated"]:
                    if st.button("↩ Undo Migration", use_container_width=True):
                        st.session_state["local_migrations"][selected_cid] = False
                        if db: db.collection("migrations").document(uid).collection("customers").document(selected_cid).set({"migrated": False}, merge=True)
                        st.rerun()
                else:
                    if st.button("✅ Mark Migrated", type="primary", use_container_width=True):
                        st.session_state["local_migrations"][selected_cid] = True
                        if db: db.collection("migrations").document(uid).collection("customers").document(selected_cid).set({"migrated": True}, merge=True)
                        st.rerun()

            st.divider()

            # Data Fields
            cols = st.columns(2)
            with cols[0]:
                render_copy_card("First Name", cust.get("CleanFirstName") if is_clean else "")
                render_copy_card("Last Name", cust.get("CleanLastName") if is_clean else "")
                render_copy_card("Full Name", cust.get("DisplayName") if is_clean else cust.get("CustomerName"))
                render_copy_card("Email", cust.get("Email"))
            with cols[1]:
                render_copy_card("Mobile", cust.get("SMS"))
                render_copy_card("Telephone", cust.get("Telephone"))
                addr_val = cust.get("PhysicalAddress")
                render_copy_card("Address", addr_val)
                
                # Address Validation Logic
                if addr_val and "GOOGLE" in st.secrets:
                    cache_mgr = AddressCacheManager(db)
                    geocoder = CachedGeocoder(st.secrets["GOOGLE"]["geocoding_api_key"], cache_mgr)
                    
                    cached_geo = cache_mgr.get_cached_geocoding(addr_val)
                    
                    if cached_geo:
                        if cached_geo.get('valid'):
                            st.success(f"📍 {cached_geo.get('formatted_address')}")
                        else:
                            st.error("Address Invalid")
                    else:
                        if st.button("Verify Address"):
                            with st.spinner("Checking Google Maps..."):
                                res = geocoder.geocode(addr_val)
                                st.rerun()

            # Tabs for Notes & Bookings
            st.markdown("### History")
            tab1, tab2 = st.tabs([f"Bookings ({len(c_bookings)})", f"Notes ({len(c_notes)})"])
            
            with tab1:
                if not c_bookings.empty:
                    # Sort by date desc
                    c_bookings = c_bookings.sort_values("StartDT", ascending=False)
                    
                    for _, row in c_bookings.iterrows():
                        dt_str = row["StartDT"].strftime("%d %b %Y %H:%M") if pd.notna(row["StartDT"]) else "No Date"
                        
                        if is_clean and "CleanFrom" in row:
                            # Clean Card View
                            with st.container(border=True):
                                st.markdown(f"**{dt_str}**")
                                cA, cB = st.columns(2)
                                cA.markdown(f"**From:** {row['CleanFrom']}")
                                cB.markdown(f"**To:** {row['CleanTo']}")
                        else:
                            # Raw View
                            st.text(f"{dt_str} - {row.get('Notes', '')}")
                else:
                    st.info("No bookings found for this customer.")

            with tab2:
                if not c_notes.empty:
                    for _, row in c_notes.iterrows():
                        st.info(row["NoteText"])
                else:
                    st.caption("No notes.")
        else:
            st.markdown("""
                <div style='text-align: center; margin-top: 100px; color: #aaa;'>
                    <h3>👈 Select a customer</h3>
                </div>
            """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()