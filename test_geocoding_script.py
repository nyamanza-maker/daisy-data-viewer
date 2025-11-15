"""
Test script for geocoding functionality
Run this to verify your Google API key and geocoding setup
"""

import streamlit as st
import pandas as pd
from cleansing import AddressCacheManager, CachedGeocoder

st.set_page_config(page_title="Geocoding Test", layout="wide")

st.title("🧪 Geocoding Test Suite")

# Check secrets
st.header("1️⃣ Configuration Check")

if "GOOGLE" in st.secrets and "geocoding_api_key" in st.secrets["GOOGLE"]:
    st.success("✅ Google API key found in secrets")
    api_key = st.secrets["GOOGLE"]["geocoding_api_key"]
    st.code(f"API Key: {api_key[:10]}...{api_key[-4:]}")
else:
    st.error("❌ Google API key not found in secrets")
    st.stop()

# Initialize Firebase
from firebase_admin import credentials, firestore
import firebase_admin
import json

try:
    if "FIREBASE" in st.secrets and "admin_json" in st.secrets["FIREBASE"]:
        admin_data = st.secrets["FIREBASE"]["admin_json"]
        
        if isinstance(admin_data, str):
            cred_info = json.loads(admin_data)
        else:
            cred_info = dict(admin_data)
        
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_info)
            firebase_admin.initialize_app(cred)
        
        db = firestore.client()
        st.success("✅ Firestore connected")
    else:
        st.error("❌ Firebase admin credentials not found")
        st.stop()
except Exception as e:
    st.error(f"❌ Firestore initialization failed: {e}")
    st.stop()

# Initialize geocoding components
st.header("2️⃣ Geocoding Components")

try:
    cache_mgr = AddressCacheManager(db)
    geocoder = CachedGeocoder(api_key, cache_mgr, rate_limit=10)
    st.success("✅ Geocoding components initialized")
except Exception as e:
    st.error(f"❌ Failed to initialize: {e}")
    st.stop()

# Test single address
st.header("3️⃣ Single Address Test")

test_addresses = [
    "113 The Avenue, Albany, Auckland, New Zealand",
    "Flow Academy of Motion 4/59 Corinthian Drive, Albany",
    "1600 Amphitheatre Parkway, Mountain View, CA",
    "Invalid Address That Doesn't Exist 99999"
]

selected_address = st.selectbox("Select test address:", test_addresses)

if st.button("🧪 Test Geocoding"):
    with st.spinner("Geocoding..."):
        result = geocoder.geocode(selected_address, "test_user", force_recheck=False)
        
        if result:
            st.success("✅ Geocoding successful!")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Result:**")
                st.json(result)
            
            with col2:
                st.markdown("**Map:**")
                if result.get("lat") and result.get("lng"):
                    map_df = pd.DataFrame({
                        'lat': [result["lat"]],
                        'lon': [result["lng"]]
                    })
                    st.map(map_df, zoom=15)
                else:
                    st.warning("No coordinates available")
            
            # Show if cached
            stats = geocoder.get_stats()
            if stats['cache_hits'] > 0:
                st.info(f"ℹ️ This result was served from cache (no API call made)")
            else:
                st.info(f"ℹ️ API call made. Cost: $0.005")
        else:
            st.error("❌ Geocoding failed")

# Test batch processing
st.header("4️⃣ Batch Test")

st.markdown("Test multiple addresses at once:")

batch_addresses = st.text_area(
    "Enter addresses (one per line):",
    value="113 The Avenue, Albany\n4/59 Corinthian Drive, Albany\n1600 Amphitheatre Parkway",
    height=100
)

if st.button("🧪 Batch Test"):
    addresses = [a.strip() for a in batch_addresses.split('\n') if a.strip()]
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    results = []
    
    for i, address in enumerate(addresses):
        status_text.text(f"Processing {i+1}/{len(addresses)}: {address}")
        
        result = geocoder.geocode(address, "test_user", force_recheck=False)
        
        if result:
            results.append({
                "Address": address,
                "Valid": "✅" if result.get("valid") else "❌",
                "Formatted": result.get("formatted_address", ""),
                "Suburb": result.get("suburb", ""),
                "State": result.get("state", ""),
                "Lat": result.get("lat"),
                "Lng": result.get("lng")
            })
        else:
            results.append({
                "Address": address,
                "Valid": "❌",
                "Formatted": "Failed",
                "Suburb": "",
                "State": "",
                "Lat": None,
                "Lng": None
            })
        
        progress_bar.progress((i + 1) / len(addresses))
    
    status_text.empty()
    progress_bar.empty()
    
    # Show results
    results_df = pd.DataFrame(results)
    st.dataframe(results_df, use_container_width=True)
    
    # Show stats
    stats = geocoder.get_stats()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("API Calls", stats['api_requests'])
    col2.metric("Cache Hits", stats['cache_hits'])
    col3.metric("Cache Rate", f"{stats['cache_hit_rate']}%")
    col4.metric("Est. Cost", f"${stats['estimated_cost']}")

# Show cache stats
st.header("5️⃣ Cache Statistics")

cache_stats = cache_mgr.get_cache_stats()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Cached", cache_stats.get("total_addresses", 0))
col2.metric("Valid", cache_stats.get("valid", 0))
col3.metric("Invalid", cache_stats.get("invalid", 0))
col4.metric("Dedup Rate", f"{cache_stats.get('deduplication_rate', 0)}%")

if st.button("🗑️ Clear Test Cache"):
    if st.checkbox("Are you sure? This will delete all cached addresses."):
        count = cache_mgr.clear_cache()
        st.success(f"✅ Cleared {count} cached addresses")
        st.rerun()

# Show invalid addresses
st.header("6️⃣ Invalid Addresses Report")

invalid_addresses = cache_mgr.get_invalid_addresses(limit=20)
if invalid_addresses:
    st.warning(f"⚠️ Found {len(invalid_addresses)} invalid addresses in cache")
    invalid_df = pd.DataFrame(invalid_addresses)
    st.dataframe(invalid_df, use_container_width=True)
else:
    st.success("✅ No invalid addresses in cache")

# Footer
st.markdown("---")
st.caption("🧪 Geocoding Test Suite | Verify your setup before processing production data")
