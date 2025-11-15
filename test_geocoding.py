import requests
import streamlit as st
print("request made...")
# Load API key
api_key = st.secrets["GOOGLE"]["geocoding_api_key"]
print("request made...")
# Test address
address = "113 The Avenue, Albany, Auckland, New Zealand"
print("making request...")
# Make request
url = "https://maps.googleapis.com/maps/api/geocode/json"
params = {
    "address": address,
    "key": api_key
}
print("request made...")
response = requests.get(url, params=params)
data = response.json()

if data["status"] == "OK":
    result = data["results"][0]
    print("✅ API is working!")
    print(f"Formatted Address: {result['formatted_address']}")
    print(f"Lat/Lng: {result['geometry']['location']}")
    
    # Parse components
    for component in result["address_components"]:
        print(f"{component['types'][0]}: {component['long_name']}")
else:
    print(f"❌ Error: {data['status']}")
    if "error_message" in data:
        print(f"Message: {data['error_message']}")