"""
Google Maps Geocoding API wrapper with caching and rate limiting
"""

import requests
import time
from typing import Dict, Optional
from .address_cache import AddressCacheManager


class CachedGeocoder:
    def __init__(self, api_key: str, cache_manager: AddressCacheManager, rate_limit: int = 50):
        """
        Initialize geocoder with Firestore caching and rate limiting
        
        Args:
            api_key: Google Maps API key
            cache_manager: AddressCacheManager instance
            rate_limit: Max requests per second (default 50, Google allows 50/sec)
        """
        self.api_key = api_key
        self.cache_manager = cache_manager
        self.rate_limit = rate_limit
        self.last_request_time = 0
        self.requests_made = 0
        self.cache_hits = 0
    
    def _rate_limit(self):
        """Enforce rate limiting to avoid hitting Google API limits"""
        if self.rate_limit > 0:
            min_interval = 1.0 / self.rate_limit
            elapsed = time.time() - self.last_request_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        self.last_request_time = time.time()
    
    def _call_google_api(self, address: str) -> Optional[Dict]:
        """
        Call Google Geocoding API directly
        
        Args:
            address: Address to geocode
            
        Returns:
            Structured geocoding result or None on error
        """
        self._rate_limit()
        self.requests_made += 1
        
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "address": address,
            "key": self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data["status"] == "OK":
                result = data["results"][0]
                
                # Parse address components
                components = {
                    "street_number": "",
                    "street_name": "",
                    "suburb": "",
                    "state": "",
                    "postcode": "",
                    "country": ""
                }
                
                for comp in result["address_components"]:
                    types = comp["types"]
                    long_name = comp.get("long_name", "")
                    
                    if "street_number" in types:
                        components["street_number"] = long_name
                    elif "route" in types:
                        components["street_name"] = long_name
                    elif "locality" in types:
                        components["suburb"] = long_name
                    elif "sublocality" in types and not components["suburb"]:
                        components["suburb"] = long_name
                    elif "administrative_area_level_1" in types:
                        components["state"] = long_name
                    elif "postal_code" in types:
                        components["postcode"] = long_name
                    elif "country" in types:
                        components["country"] = long_name
                
                # Build structured result
                geocoded = {
                    "formatted_address": result.get("formatted_address", ""),
                    "street_number": components["street_number"],
                    "street_name": components["street_name"],
                    "suburb": components["suburb"],
                    "state": components["state"],
                    "postcode": components["postcode"],
                    "country": components["country"],
                    "lat": result["geometry"]["location"]["lat"],
                    "lng": result["geometry"]["location"]["lng"],
                    "valid": True,
                    "partial_match": result.get("partial_match", False),
                    "place_id": result.get("place_id", ""),
                    "location_type": result["geometry"].get("location_type", "")
                }
                
                return geocoded
            
            elif data["status"] == "ZERO_RESULTS":
                # Address not found by Google
                return {
                    "formatted_address": address,
                    "street_number": "",
                    "street_name": "",
                    "suburb": "",
                    "state": "",
                    "postcode": "",
                    "country": "",
                    "lat": None,
                    "lng": None,
                    "valid": False,
                    "partial_match": False,
                    "error": "Address not found"
                }
            
            elif data["status"] == "OVER_QUERY_LIMIT":
                print(f"⚠️ Google API quota exceeded!")
                return None
            
            elif data["status"] == "REQUEST_DENIED":
                print(f"⚠️ Google API request denied: {data.get('error_message', 'No error message')}")
                return None
            
            else:
                print(f"⚠️ Geocoding error for '{address}': {data['status']}")
                if "error_message" in data:
                    print(f"   Error: {data['error_message']}")
                return None
        
        except requests.exceptions.Timeout:
            print(f"⚠️ Timeout geocoding '{address}'")
            return None
        
        except Exception as e:
            print(f"⚠️ Exception geocoding '{address}': {e}")
            return None
    
    def geocode(self, address: str, uid: str, force_recheck: bool = False) -> Optional[Dict]:
        """
        Geocode an address with Firestore caching
        
        Args:
            address: Address to geocode
            uid: User ID (for tracking who geocoded)
            force_recheck: If True, bypass cache and re-geocode
        
        Returns:
            Geocoding result dict or None
        """
        if not address or not address.strip():
            return None
        
        # Check cache first (unless force_recheck)
        if not force_recheck:
            cached = self.cache_manager.get_cached_geocoding(address)
            if cached:
                self.cache_hits += 1
                return cached
        
        # Not in cache or force recheck - call Google API
        result = self._call_google_api(address)
        
        if result:
            # Save to cache
            if force_recheck:
                self.cache_manager.manual_recheck(address, result, uid)
            else:
                self.cache_manager.save_geocoding_result(address, result, uid)
        
        return result
    
    def geocode_batch(self, addresses: list, uid: str, progress_callback=None) -> Dict[str, Dict]:
        """
        Geocode multiple addresses efficiently
        
        Args:
            addresses: List of address strings
            uid: User ID
            progress_callback: Optional callback function(current, total, address)
        
        Returns:
            Dict mapping address -> geocoding result
        """
        results = {}
        total = len(addresses)
        
        for i, address in enumerate(addresses):
            if address and address.strip():
                result = self.geocode(address, uid, force_recheck=False)
                results[address] = result
                
                if progress_callback and (i % 10 == 0 or i == total - 1):
                    progress_callback(i + 1, total, address)
        
        return results
    
    def get_requests_count(self) -> int:
        """Get number of API requests made this session"""
        return self.requests_made
    
    def get_cache_hits(self) -> int:
        """Get number of cache hits this session"""
        return self.cache_hits
    
    def get_stats(self) -> Dict:
        """Get geocoding statistics for this session"""
        total = self.requests_made + self.cache_hits
        cache_rate = (self.cache_hits / total * 100) if total > 0 else 0
        
        return {
            "api_requests": self.requests_made,
            "cache_hits": self.cache_hits,
            "total_lookups": total,
            "cache_hit_rate": round(cache_rate, 1),
            "estimated_cost": round(self.requests_made * 0.005, 2)  # $5 per 1000 requests
        }
