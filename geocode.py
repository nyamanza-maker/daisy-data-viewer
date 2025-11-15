# geocode.py

import requests
from typing import Dict, Any
from config import GOOGLE_MAPS_API_KEY

def geocode_address(address: str) -> Dict[str, Any]:
    """
    Geocode a single address string using Google Maps.
    Returns a dict with address components and flags.
    """
    if not GOOGLE_MAPS_API_KEY:
        return {
            "valid": False,
            "reason": "NO_API_KEY",
            "formatted_address": address,
        }

    params = {
        "address": address,
        "key": GOOGLE_MAPS_API_KEY,
    }
    resp = requests.get("https://maps.googleapis.com/maps/api/geocode/json", params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "OK" or not data.get("results"):
        return {
            "valid": False,
            "reason": data.get("status", "UNKNOWN"),
            "formatted_address": address,
        }

    result = data["results"][0]
    formatted = result.get("formatted_address", address)
    partial = result.get("partial_match", False)
    loc = result.get("geometry", {}).get("location", {}) or {}

    # Components
    comps = {c["types"][0]: c["long_name"] for c in result.get("address_components", []) if c.get("types")}

    def comp(t: str, default: str = ""):
        return comps.get(t, default)

    return {
        "valid": not partial,
        "reason": "OK" if not partial else "PARTIAL",
        "formatted_address": formatted,
        "street_number": comp("street_number"),
        "street_name": comp("route"),
        "suburb": comp("locality") or comp("sublocality"),
        "state": comp("administrative_area_level_1"),
        "postcode": comp("postal_code"),
        "country": comp("country"),
        "lat": loc.get("lat"),
        "lng": loc.get("lng"),
    }
