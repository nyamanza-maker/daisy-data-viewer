"""
Address Cache Manager - Stores geocoding results in Firestore
Ensures we only geocode each unique address once
"""

import hashlib
import re
from datetime import datetime
from typing import Dict, Optional, List
from firebase_admin import firestore


class AddressCacheManager:
    def __init__(self, db):
        """
        Manage address geocoding cache in Firestore
        
        Args:
            db: Firestore client
        """
        self.db = db
        self.cache_collection = db.collection("address_cache")
    
    def normalize_address(self, address: str) -> str:
        """
        Normalize address for consistent matching
        - Lowercase
        - Remove extra whitespace
        - Remove punctuation
        - Standardize common abbreviations
        
        Args:
            address: Raw address string
            
        Returns:
            Normalized address string
        """
        if not address or not isinstance(address, str):
            return ""
        
        normalized = address.lower().strip()
        
        # Remove common punctuation
        normalized = re.sub(r'[,\.#]', ' ', normalized)
        
        # Standardize abbreviations
        replacements = {
            r'\bst\b': 'street',
            r'\brd\b': 'road',
            r'\bave\b': 'avenue',
            r'\bdr\b': 'drive',
            r'\bpl\b': 'place',
            r'\bapt\b': 'apartment',
            r'\bunit\b': 'unit',
        }
        
        for pattern, replacement in replacements.items():
            normalized = re.sub(pattern, replacement, normalized)
        
        # Collapse multiple spaces
        normalized = re.sub(r'\s+', ' ', normalized)
        
        return normalized.strip()
    
    def get_address_hash(self, address: str) -> str:
        """
        Generate unique hash for address (used as Firestore doc ID)
        
        Args:
            address: Address string
            
        Returns:
            16-character hash
        """
        normalized = self.normalize_address(address)
        if not normalized:
            return ""
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]
    
    def get_cached_geocoding(self, address: str) -> Optional[Dict]:
        """
        Get cached geocoding result if it exists
        
        Args:
            address: Address to lookup
            
        Returns:
            Geocoding result dict or None if not cached
        """
        if not address or not address.strip():
            return None
        
        address_hash = self.get_address_hash(address)
        if not address_hash:
            return None
        
        try:
            doc = self.cache_collection.document(address_hash).get()
            
            if doc.exists:
                data = doc.to_dict()
                
                # Update usage stats
                self.cache_collection.document(address_hash).update({
                    "usage_count": firestore.Increment(1),
                    "last_used": datetime.now()
                })
                
                return data.get("google_result")
            
            return None
        
        except Exception as e:
            print(f"Error getting cached address '{address}': {e}")
            return None
    
    def save_geocoding_result(self, address: str, geocoding_result: Dict, uid: str) -> str:
        """
        Save geocoding result to cache
        
        Args:
            address: Original address string
            geocoding_result: Result from Google Geocoding API
            uid: User ID who triggered the geocoding
            
        Returns:
            address_hash (the cache ID)
        """
        if not address or not address.strip():
            return ""
        
        address_hash = self.get_address_hash(address)
        if not address_hash:
            return ""
        
        normalized = self.normalize_address(address)
        
        cache_data = {
            "original_address": address.strip(),
            "normalized_address": normalized,
            "geocoded_at": datetime.now(),
            "geocoded_by_uid": uid,
            "google_result": geocoding_result,
            "usage_count": 1,
            "last_used": datetime.now(),
            "manual_override": False
        }
        
        try:
            self.cache_collection.document(address_hash).set(cache_data, merge=True)
            return address_hash
        
        except Exception as e:
            print(f"Error saving geocoding result for '{address}': {e}")
            return ""
    
    def manual_recheck(self, address: str, geocoding_result: Dict, uid: str) -> str:
        """
        Manually recheck and update an address
        User explicitly requested re-geocoding
        
        Args:
            address: Address to update
            geocoding_result: New geocoding result
            uid: User ID who triggered recheck
            
        Returns:
            address_hash
        """
        address_hash = self.get_address_hash(address)
        if not address_hash:
            return ""
        
        update_data = {
            "google_result": geocoding_result,
            "manual_override": True,
            "override_at": datetime.now(),
            "override_by_uid": uid
        }
        
        try:
            self.cache_collection.document(address_hash).update(update_data)
            return address_hash
        
        except Exception as e:
            print(f"Error updating address '{address}': {e}")
            return ""
    
    def get_cache_stats(self) -> Dict:
        """
        Get statistics about the address cache
        
        Returns:
            Dict with cache statistics
        """
        try:
            docs = self.cache_collection.stream()
            
            total = 0
            valid = 0
            invalid = 0
            manual_overrides = 0
            partial_matches = 0
            total_usage = 0
            
            for doc in docs:
                total += 1
                data = doc.to_dict()
                
                google_result = data.get("google_result", {})
                
                if google_result.get("valid"):
                    valid += 1
                else:
                    invalid += 1
                
                if google_result.get("partial_match"):
                    partial_matches += 1
                
                if data.get("manual_override"):
                    manual_overrides += 1
                
                total_usage += data.get("usage_count", 0)
            
            return {
                "total_addresses": total,
                "valid": valid,
                "invalid": invalid,
                "partial_matches": partial_matches,
                "manual_overrides": manual_overrides,
                "total_usage": total_usage,
                "deduplication_rate": round((total_usage - total) / total_usage * 100, 1) if total_usage > 0 else 0
            }
        
        except Exception as e:
            print(f"Error getting cache stats: {e}")
            return {
                "total_addresses": 0,
                "valid": 0,
                "invalid": 0,
                "partial_matches": 0,
                "manual_overrides": 0,
                "total_usage": 0,
                "deduplication_rate": 0
            }
    
    def get_invalid_addresses(self, limit: int = 100) -> List[Dict]:
        """
        Get list of addresses that failed validation
        
        Args:
            limit: Maximum number of results
            
        Returns:
            List of invalid address records
        """
        try:
            docs = (
                self.cache_collection
                .where("google_result.valid", "==", False)
                .limit(limit)
                .stream()
            )
            
            invalid_addresses = []
            for doc in docs:
                data = doc.to_dict()
                invalid_addresses.append({
                    "address": data.get("original_address", ""),
                    "geocoded_at": data.get("geocoded_at"),
                    "usage_count": data.get("usage_count", 0)
                })
            
            return invalid_addresses
        
        except Exception as e:
            print(f"Error getting invalid addresses: {e}")
            return []
    
    def clear_cache(self, older_than_days: Optional[int] = None):
        """
        Clear address cache (use with caution!)
        
        Args:
            older_than_days: Only clear entries older than this many days
                           If None, clears everything
        """
        try:
            query = self.cache_collection
            
            if older_than_days:
                cutoff_date = datetime.now() - timedelta(days=older_than_days)
                query = query.where("last_used", "<", cutoff_date)
            
            docs = query.stream()
            batch = self.db.batch()
            count = 0
            
            for doc in docs:
                batch.delete(doc.reference)
                count += 1
                
                # Firestore batch limit is 500
                if count % 500 == 0:
                    batch.commit()
                    batch = self.db.batch()
            
            if count % 500 != 0:
                batch.commit()
            
            print(f"Cleared {count} cached addresses")
            return count
        
        except Exception as e:
            print(f"Error clearing cache: {e}")
            return 0
