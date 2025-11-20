"""
Daisy Data Viewer - Enterprise Edition v2.1
High-performance Master-Detail UI with Geocoding, Advanced Filters, and Smart Caching
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
# 3. GEOCODING CLASSES
# -----------------------------------------------------------------------------
class AddressCacheManager:
    def __init__(self, db):
        self.db = db
        self.collection = "address_cache"

    def get_cached_geocoding(self, raw_address):
        if not self.db or not raw_address: return None
        # Create a safe document ID from the address
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
    
    # 1. Construct Client Config (Pyrebase)
    # CRITICAL FIX: We assume secrets use snake_case, but Pyrebase needs camelCase