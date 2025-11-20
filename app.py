"""
Daisy Data Viewer - Enterprise Edition
High-performance Master-Detail UI with Optimistic State Management
"""

import io
import json
import re
import time
import uuid
from datetime import datetime
from typing import Dict, Optional, Tuple

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
    initial_sidebar_state="collapsed", # Collapsed because we use a custom split layout
    page_icon="⚡"
)

# INJECT CUSTOM CSS & JS FOR CLIPBOARD & LAYOUT
st.markdown("""
    <style>
    /* Global Reset & Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* Layout Tweaks */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        padding-left: 3rem;
        padding-right: 3rem;
        max-width: 100%;
    }

    /* Custom Card for Data Fields */
    .data-field-card {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 10px;
        cursor: pointer;
        transition: all 0.2s ease;
        position: relative;
        overflow: hidden;
    }
    
    .stDark .data-field-card {
        background-color: #262730;
        border: 1px solid #414248;
    }

    .data-field-card:hover {
        border-color: #FF4B4B;
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
    }

    .data-field-label {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #888;
        margin-bottom: 4px;
        font-weight: 600;
    }
    
    .data-field-value {
        font-size: 1rem;
        font-weight: 500;
        color: #111;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    
    .stDark .data-field-value {
        color: #fff;
    }

    /* Copy Feedback Toast */
    #toast-container {
        position: fixed;
        bottom: 20px;
        right: 20px;
        z-index: 9999;
    }
    
    .toast {
        background: #333;
        color: white;
        padding: 12px 24px;
        border-radius: 50px;
        margin-top: 10px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        animation: fadeIn 0.3s, fadeOut 0.3s 2.5s forwards;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    @keyframes fadeIn { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }
    @keyframes fadeOut { from { opacity: 1; } to { opacity: 0; } }

    /* Status Badges */
    .badge {
        padding: 4px 12px;
        border-radius: 12px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    .badge-migrated { background: #dcfce7; color: #166534; }
    .badge-pending { background: #fef9c3; color: #854d0e; }
    
    /* Sidebar/Master List Styling */
    .master-header {
        border-bottom: 2px solid #f0f2f6;
        padding-bottom: 1rem;
        margin-bottom: 1rem;
    }
    
    /* Hide default Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    </style>

    <div id="toast-container"></div>
    <script>
    function copyToClipboard(text) {
        navigator.clipboard.writeText(text).then(function() {
            const container = document.getElementById('toast-container');
            const toast = document.createElement('div');
            toast.className = 'toast';
            toast.innerHTML = '<span>📋</span> Copied to clipboard';
            container.appendChild(toast);
            setTimeout(() => { toast.remove(); }, 3000);
        }, function(err) {
            console.error('Async: Could not copy text: ', err);
        });
    }
    </script>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 2. FIREBASE SETUP (Robust)
# -----------------------------------------------------------------------------
def init_firebase():
    # 1. Client SDK (Auth/Storage)
    fb_sec = st.secrets.get("FIREBASE")
    if not fb_sec: st.error("Missing FIREBASE secrets"); st.stop()
    
    config = {
        "apiKey": fb_sec["api_key"],
        "authDomain": fb_sec["auth_domain"],
        "projectId": fb_sec["project_id"],
        "storageBucket": fb_sec["storage_bucket"],
        "messagingSenderId": fb_sec["messaging_sender_id"],
        "appId": fb_sec["app_id"],
        "databaseURL": fb_sec.get("database_url", ""),
    }
    firebase_app = pyrebase.initialize_app(config)
    
    # 2. Admin SDK (Firestore)
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
# 3. SESSION STATE & HELPERS
# -----------------------------------------------------------------------------
if "auth" not in st.session_state: st.session_state["auth"] = None
if "data_hash" not in st.session_state: st.session_state["data_hash"] = None
if "selected_customer_id" not in st.session_state: st.session_state["selected_customer_id"] = None
if "local_migrations" not in st.session_state: st.session_state["local_migrations"] = {}

def to_bool(v):
    if pd.isna(v): return False
    return str(v).lower() in ("true", "1", "yes", "y", "t")

# -----------------------------------------------------------------------------
# 4. DATA ENGINE (Optimized)
# -----------------------------------------------------------------------------

@st.cache_data(show_spinner=False, ttl=3600) # Cache for 1 hour
def download_raw_data(uid, id_token):
    """Downloads CSVs once. Does NOT handle migration status."""
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
    
    # Initialize empty if missing
    if cust is None: cust = pd.DataFrame(columns=["CustomerId", "CustomerName", "Telephone"])
    if notes is None: notes = pd.DataFrame(columns=["CustomerId", "NoteText"])
    if bookings is None: bookings = pd.DataFrame(columns=["BookingId", "CustomerId", "StartDateTime"])

    # Basic Cleanup (Done once)
    if "CustomerName" in cust.columns:
        cust["DisplayName"] = cust["CustomerName"].fillna(cust.get("CompanyName", "Unknown"))
    
    # Ensure ID is string for merging
    cust["CustomerId"] = cust["CustomerId"].astype(str)
    
    return cust, notes, bookings

def get_migration_status(uid, customer_ids):
    """
    Fetches migration status from Firestore. 
    This is separate so we don't re-download CSVs when refreshing status.
    """
    if not db: return {}
    
    # Performance: If list is huge, we might fetch all collection docs instead of querying
    # For now, fetch all migration docs for this franchise
    ref = db.collection("migrations").document(uid).collection("customers")
    docs = ref.stream()
    
    status_map = {}
    for d in docs:
        if d.to_dict().get("migrated", False):
            status_map[d.id] = True
    return status_map

def update_migration(uid, customer_id, status):
    """Updates Firestore and Local State immediately"""
    # 1. Update Local State (Instant UI feedback)
    st.session_state["local_migrations"][str(customer_id)] = status
    
    # 2. Update Firestore (Background-ish)
    if db:
        doc_ref = db.collection("migrations").document(uid).collection("customers").document(str(customer_id))
        doc_ref.set({"migrated": status}, merge=True)

# -----------------------------------------------------------------------------
# 5. UI COMPONENTS
# -----------------------------------------------------------------------------

def render_copy_field(label, value, width=1):
    """Renders a beautiful clickable card that invokes the JS copy function"""
    if pd.isna(value) or str(value).strip() == "":
        return # Don't render empty fields to keep UI clean
    
    clean_val = str(value).strip()
    escaped_val = clean_val.replace("'", "\\'") # Escape for JS
    
    html = f"""
    <div class="data-field-card" onclick="copyToClipboard('{escaped_val}')">
        <div class="data-field-label">{label}</div>
        <div class="data-field-value" title="{clean_val}">{clean_val}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 6. MAIN APP FLOW
# -----------------------------------------------------------------------------

def main():
    # --- LOGIN SCREEN ---
    if not st.session_state["auth"]:
        c1, c2, c3 = st.columns([1,2,1])
        with c2:
            st.markdown("## ⚡ Daisy Data Viewer")
            with st.form("login"):
                email = st.text_input("Email")
                password = st.text_input("Password", type="password")
                if st.form_submit_button("Sign In", use_container_width=True):
                    try:
                        user = auth.sign_in_with_email_and_password(email, password)
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
    token = st.session_state["auth"]["token"]
    
    with st.spinner("Loading Data Engine..."):
        cust_df_raw, notes_df, bookings_df = download_raw_data(uid, token)
        
        # Load remote migrations only once per session or on explicit refresh
        if not st.session_state["local_migrations"]:
            st.session_state["local_migrations"] = get_migration_status(uid, cust_df_raw["CustomerId"].unique())

    # Merge migration status efficiently
    # We create a working copy so we don't mutate the cached dataframe
    df_display = cust_df_raw.copy()
    
    # Map local state onto the dataframe
    # This is fast because it uses a dictionary lookup
    df_display["Migrated"] = df_display["CustomerId"].map(st.session_state["local_migrations"]).fillna(False)

    # --- LAYOUT: SPLIT SCREEN ---
    col_master, col_detail = st.columns([4, 6], gap="large")

    # --- LEFT COLUMN: MASTER LIST & SEARCH ---
    with col_master:
        st.markdown('<div class="master-header"><h3>👥 Customers</h3></div>', unsafe_allow_html=True)
        
        # 1. Search & Filter
        col_search, col_filter = st.columns([3, 1])
        search_term = col_search.text_input("Search", placeholder="Name, Phone, Address...", label_visibility="collapsed")
        show_migrated = col_filter.checkbox("Show Done", value=False, help="Show migrated customers")

        # 2. Filtering Logic
        mask = pd.Series(True, index=df_display.index)
        
        if not show_migrated:
            mask &= ~df_display["Migrated"]
            
        if search_term:
            s = search_term.lower()
            # Create a searchable string for speed
            search_series = (
                df_display["DisplayName"].fillna("") + " " + 
                df_display["Telephone"].fillna("") + " " + 
                df_display["PhysicalAddress"].fillna("")
            ).str.lower()
            mask &= search_series.str.contains(s)

        filtered_df = df_display[mask].copy()
        
        # 3. Display List (Using st.dataframe selection for speed)
        st.caption(f"Showing {len(filtered_df)} customers")
        
        # Prepare table for display (hide internal IDs)
        table_df = filtered_df[["DisplayName", "Telephone", "Migrated"]]
        table_df.insert(0, "Status", table_df["Migrated"].apply(lambda x: "✅" if x else "⚡"))
        
        selection = st.dataframe(
            table_df[["Status", "DisplayName", "Telephone"]],
            use_container_width=True,
            hide_index=True,
            height=600,
            on_select="rerun",
            selection_mode="single-row",
            column_config={
                "Status": st.column_config.TextColumn("St", width="small"),
                "DisplayName": st.column_config.TextColumn("Name", width="large"),
                "Telephone": st.column_config.TextColumn("Phone", width="medium"),
            }
        )

        # Handle Selection
        if selection.selection.rows:
            selected_index = selection.selection.rows[0]
            # Map back to original dataframe using the index
            selected_row = filtered_df.iloc[selected_index]
            st.session_state["selected_customer_id"] = selected_row["CustomerId"]

    # --- RIGHT COLUMN: DETAIL WORKSPACE ---
    with col_detail:
        if st.session_state["selected_customer_id"]:
            # Get full customer object
            cust = df_display[df_display["CustomerId"] == st.session_state["selected_customer_id"]].iloc[0]
            cid = cust["CustomerId"]
            is_migrated = cust["Migrated"]

            # HEADER
            h1, h2 = st.columns([3, 1])
            with h1:
                st.markdown(f"## {cust['DisplayName']}")
                st.caption(f"ID: {cid}")
            with h2:
                if is_migrated:
                    if st.button("↩ Undo", use_container_width=True):
                        update_migration(uid, cid, False)
                        st.rerun()
                else:
                    if st.button("✅ Complete", type="primary", use_container_width=True):
                        update_migration(uid, cid, True)
                        st.rerun()

            st.divider()

            # DATA CARDS GRID
            st.markdown("#### 📋 Client Details")
            
            c1, c2 = st.columns(2)
            with c1:
                render_copy_field("Full Name", cust.get("CustomerName"))
                render_copy_field("Company", cust.get("CompanyName"))
                render_copy_field("Email", cust.get("Email"))
                render_copy_field("DOB", cust.get("DateOfBirth"))
            with c2:
                render_copy_field("Mobile", cust.get("SMS"))
                render_copy_field("Telephone", cust.get("Telephone"))
                render_copy_field("Address", cust.get("PhysicalAddress"))
                render_copy_field("Gender", cust.get("Gender"))

            # NOTES TAB
            st.markdown("#### 📝 Notes & History")
            
            cust_notes = notes_df[notes_df["CustomerId"] == cid]
            cust_bookings = bookings_df[bookings_df["CustomerId"] == cid]
            
            tab_notes, tab_bookings = st.tabs([f"Notes ({len(cust_notes)})", f"Bookings ({len(cust_bookings)})"])
            
            with tab_notes:
                if not cust_notes.empty:
                    for _, note in cust_notes.iterrows():
                        st.info(note["NoteText"])
                else:
                    st.caption("No notes found.")

            with tab_bookings:
                if not cust_bookings.empty:
                    st.dataframe(
                        cust_bookings, 
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.caption("No bookings found.")

        else:
            # Empty State
            st.markdown("""
                <div style='text-align: center; padding: 5rem; color: #888;'>
                    <h3>👈 Select a customer</h3>
                    <p>Choose a customer from the list to view details, copy data, and manage migration status.</p>
                </div>
            """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()