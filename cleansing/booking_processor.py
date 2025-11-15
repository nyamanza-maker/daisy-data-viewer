"""
Booking data processor - extracts FROM/TO addresses and geocodes them
"""

import pandas as pd
import re
from typing import Dict, Callable, Optional
from .geocoder import CachedGeocoder
from .address_cache import AddressCacheManager


class BookingProcessor:
    def __init__(self, geocoder: CachedGeocoder, cache_manager: AddressCacheManager):
        """
        Initialize booking processor
        
        Args:
            geocoder: CachedGeocoder instance
            cache_manager: AddressCacheManager instance
        """
        self.geocoder = geocoder
        self.cache_manager = cache_manager
    
    def extract_booking_addresses(self, notes_text: str) -> Dict[str, str]:
        """
        Extract FROM, TO, and remaining notes from booking notes
        
        Args:
            notes_text: Raw booking notes text
            
        Returns:
            Dict with keys: from, to, notes
        """
        if pd.isna(notes_text) or not notes_text:
            return {"from": "", "to": "", "notes": ""}
        
        text = str(notes_text)
        
        # Pattern matching for FROM
        from_patterns = [
            r'FROM[:\s]+([^G]*?)(?=GOING TO|TO:|$)',
            r'PICK\s*UP[:\s]+([^G]*?)(?=GOING TO|TO:|$)',
            r'PICKUP[:\s]+([^G]*?)(?=GOING TO|TO:|$)',
        ]
        
        from_addr = ""
        from_match = None
        for pattern in from_patterns:
            from_match = re.search(pattern, text, re.IGNORECASE)
            if from_match:
                from_addr = from_match.group(1).strip()
                break
        
        # Pattern matching for TO
        to_patterns = [
            r'(?:GOING TO|TO)[:\s]+([^*]*?)(?=\*\*|$)',
            r'DROP\s*OFF[:\s]+([^*]*?)(?=\*\*|$)',
            r'DROPOFF[:\s]+([^*]*?)(?=\*\*|$)',
        ]
        
        to_addr = ""
        to_match = None
        for pattern in to_patterns:
            to_match = re.search(pattern, text, re.IGNORECASE)
            if to_match:
                to_addr = to_match.group(1).strip()
                break
        
        # Extract remaining notes (everything else)
        remaining = text
        if from_match:
            remaining = remaining.replace(from_match.group(0), '', 1)
        if to_match:
            remaining = remaining.replace(to_match.group(0), '', 1)
        
        # Clean up remaining notes
        remaining = re.sub(r'\*+', ' ', remaining).strip()
        remaining = re.sub(r'\s+', ' ', remaining).strip()
        
        return {
            "from": from_addr,
            "to": to_addr,
            "notes": remaining
        }
    
    def process_single_booking(self, row: pd.Series, uid: str) -> pd.Series:
        """
        Process a single booking record
        
        Args:
            row: Booking data row
            uid: User ID
            
        Returns:
            Updated row with cleansed fields
        """
        # Extract addresses from notes
        if "Notes" in row and pd.notna(row["Notes"]):
            extracted = self.extract_booking_addresses(row["Notes"])
            
            row["CleanFrom"] = extracted["from"]
            row["CleanTo"] = extracted["to"]
            row["CleanNotes"] = extracted["notes"]
            
            # Geocode FROM address
            if extracted["from"]:
                from_result = self.geocoder.geocode(extracted["from"], uid, force_recheck=False)
                if from_result:
                    row["GoogleFromAddress"] = from_result.get("formatted_address", "")
                    row["GoogleFromSuburb"] = from_result.get("suburb", "")
                    row["GoogleFromState"] = from_result.get("state", "")
                    row["GoogleFromPostcode"] = from_result.get("postcode", "")
                    row["GoogleFromLat"] = from_result.get("lat")
                    row["GoogleFromLng"] = from_result.get("lng")
                    row["FromAddressValid"] = from_result.get("valid", False)
                    row["FromAddressPartialMatch"] = from_result.get("partial_match", False)
                    row["FromAddressHash"] = self.cache_manager.get_address_hash(extracted["from"])
            
            # Geocode TO address
            if extracted["to"]:
                to_result = self.geocoder.geocode(extracted["to"], uid, force_recheck=False)
                if to_result:
                    row["GoogleToAddress"] = to_result.get("formatted_address", "")
                    row["GoogleToSuburb"] = to_result.get("suburb", "")
                    row["GoogleToState"] = to_result.get("state", "")
                    row["GoogleToPostcode"] = to_result.get("postcode", "")
                    row["GoogleToLat"] = to_result.get("lat")
                    row["GoogleToLng"] = to_result.get("lng")
                    row["ToAddressValid"] = to_result.get("valid", False)
                    row["ToAddressPartialMatch"] = to_result.get("partial_match", False)
                    row["ToAddressHash"] = self.cache_manager.get_address_hash(extracted["to"])
        
        return row
    
    def process_bookings(
        self,
        bookings_df: pd.DataFrame,
        uid: str,
        progress_callback: Optional[Callable] = None,
        batch_size: int = 1000
    ) -> pd.DataFrame:
        """
        Process all bookings - extract and geocode FROM/TO addresses
        
        Args:
            bookings_df: DataFrame with booking data
            uid: User ID
            progress_callback: Optional function(message) to report progress
            batch_size: Process in batches for large datasets
        
        Returns:
            DataFrame with added cleansed fields
        """
        if bookings_df.empty:
            return bookings_df
        
        total = len(bookings_df)
        processed = 0
        
        # Get unique addresses for progress estimation
        if "Notes" in bookings_df.columns:
            sample_extract = bookings_df["Notes"].head(100).apply(self.extract_booking_addresses)
            from_addrs = sample_extract.apply(lambda x: x["from"]).unique()
            to_addrs = sample_extract.apply(lambda x: x["to"]).unique()
            
            if progress_callback:
                progress_callback(
                    f"🔍 Estimated ~{len(from_addrs) * 10 + len(to_addrs) * 10} unique addresses "
                    f"in {total:,} bookings"
                )
        
        # Initialize new columns
        bookings_df["CleanFrom"] = ""
        bookings_df["CleanTo"] = ""
        bookings_df["CleanNotes"] = ""
        bookings_df["GoogleFromAddress"] = ""
        bookings_df["GoogleFromSuburb"] = ""
        bookings_df["GoogleFromState"] = ""
        bookings_df["GoogleFromPostcode"] = ""
        bookings_df["GoogleFromLat"] = None
        bookings_df["GoogleFromLng"] = None
        bookings_df["FromAddressValid"] = False
        bookings_df["FromAddressPartialMatch"] = False
        bookings_df["FromAddressHash"] = ""
        bookings_df["GoogleToAddress"] = ""
        bookings_df["GoogleToSuburb"] = ""
        bookings_df["GoogleToState"] = ""
        bookings_df["GoogleToPostcode"] = ""
        bookings_df["GoogleToLat"] = None
        bookings_df["GoogleToLng"] = None
        bookings_df["ToAddressValid"] = False
        bookings_df["ToAddressPartialMatch"] = False
        bookings_df["ToAddressHash"] = ""
        
        # Process in batches
        for start_idx in range(0, total, batch_size):
            end_idx = min(start_idx + batch_size, total)
            batch = bookings_df.iloc[start_idx:end_idx]
            
            for idx, row in batch.iterrows():
                bookings_df.loc[idx] = self.process_single_booking(row, uid)
                
                processed += 1
                
                if progress_callback and (processed % 500 == 0 or processed == total):
                    api_stats = self.geocoder.get_stats()
                    progress_callback(
                        f"⏳ Processed {processed:,}/{total:,} bookings | "
                        f"API calls: {api_stats['api_requests']} | "
                        f"Cache hits: {api_stats['cache_hits']:,} | "
                        f"Est. cost: ${api_stats['estimated_cost']}"
                    )
        
        if progress_callback:
            final_stats = self.geocoder.get_stats()
            progress_callback(
                f"✅ Complete! Processed {total:,} bookings | "
                f"API calls: {final_stats['api_requests']} | "
                f"Cache hit rate: {final_stats['cache_hit_rate']}% | "
                f"Estimated cost: ${final_stats['estimated_cost']}"
            )
        
        return bookings_df
    
    def get_invalid_addresses_report(self, bookings_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get report of bookings with invalid addresses
        
        Args:
            bookings_df: Processed booking DataFrame
            
        Returns:
            DataFrame with invalid address records
        """
        if "FromAddressValid" not in bookings_df.columns:
            return pd.DataFrame()
        
        invalid_from = bookings_df[bookings_df["FromAddressValid"] == False]
        invalid_to = bookings_df[bookings_df["ToAddressValid"] == False]
        
        invalid = pd.concat([invalid_from, invalid_to]).drop_duplicates()
        
        if not invalid.empty:
            report_cols = [
                "BookingId", "CustomerName", "StartDateTime",
                "CleanFrom", "CleanTo", "GoogleFromAddress", "GoogleToAddress"
            ]
            report_cols = [c for c in report_cols if c in invalid.columns]
            return invalid[report_cols]
        
        return pd.DataFrame()
