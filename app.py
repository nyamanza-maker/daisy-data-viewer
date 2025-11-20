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
    if bookings is None: bookings = pd.