"""
Customer data processor - handles name cleaning and address geocoding
"""

import pandas as pd
import re
from typing import Tuple, Callable, Optional
from .geocoder import CachedGeocoder
from .address_cache import AddressCacheManager


class CustomerProcessor:
    def __init__(self, geocoder: CachedGeocoder, cache_manager: AddressCacheManager):
        """
        Initialize customer processor
        
        Args:
            geocoder: CachedGeocoder instance
            cache_manager: AddressCacheManager instance
        """
        self.geocoder = geocoder
        self.cache_manager = cache_manager
    
    def clean_customer_name(self, name: str) -> Tuple[str, str, str]:
        """
        Clean and split customer name
        
        Args:
            name: Raw customer name
            
        Returns:
            Tuple of (first_name, last_name, full_cleaned_name)
        """
        if pd.isna(name) or not name:
            return "", "", ""
        
        # Remove common suffixes and noise
        noise_patterns = [
            r'\s*-\s*ACC\s*$',
            r'\s*ACC\s*$',
            r'\s*Albany\s*$',
            r'\s*TM\s*$',
            r'\s*CMA\s*$',
            r'\s*INVOICE\s*.*$',
            r'\(.*?\)',  # Remove parentheses content
            r'\s*-\s*CMA.*$'
        ]
        
        cleaned = str(name).strip()
        for pattern in noise_patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        
        cleaned = cleaned.strip()
        
        # Convert to title case
        cleaned = cleaned.title()
        
        # Split into first and last name
        parts = cleaned.split()
        if len(parts) == 0:
            return "", "", ""
        elif len(parts) == 1:
            return parts[0], "", cleaned
        else:
            first_name = parts[0]
            last_name = " ".join(parts[1:])
            return first_name, last_name, cleaned
    
    def process_single_customer(self, row: pd.Series, uid: str) -> pd.Series:
        """
        Process a single customer record
        
        Args:
            row: Customer data row
            uid: User ID
            
        Returns:
            Updated row with cleansed fields
        """
        # Clean name
        if "CustomerName" in row:
            first, last, full = self.clean_customer_name(row["CustomerName"])
            row["CleanFirstName"] = first
            row["CleanLastName"] = last
            row["CleanFullName"] = full
        
        # Geocode physical address
        if "PhysicalAddress" in row and pd.notna(row["PhysicalAddress"]):
            address = str(row["PhysicalAddress"]).strip()
            if address:
                result = self.geocoder.geocode(address, uid, force_recheck=False)
                
                if result:
                    row["GoogleAddress"] = result.get("formatted_address", "")
                    row["GoogleStreetNumber"] = result.get("street_number", "")
                    row["GoogleStreetName"] = result.get("street_name", "")
                    row["GoogleSuburb"] = result.get("suburb", "")
                    row["GoogleState"] = result.get("state", "")
                    row["GooglePostcode"] = result.get("postcode", "")
                    row["GoogleCountry"] = result.get("country", "")
                    row["GoogleLat"] = result.get("lat")
                    row["GoogleLng"] = result.get("lng")
                    row["AddressValid"] = result.get("valid", False)
                    row["AddressPartialMatch"] = result.get("partial_match", False)
                    row["AddressHash"] = self.cache_manager.get_address_hash(address)
        
        return row
    
    def process_customers(
        self, 
        customers_df: pd.DataFrame, 
        uid: str,
        progress_callback: Optional[Callable] = None
    ) -> pd.DataFrame:
        """
        Process all customers - clean names and geocode addresses
        
        Args:
            customers_df: DataFrame with customer data
            uid: User ID
            progress_callback: Optional function(message) to report progress
        
        Returns:
            DataFrame with added cleansed fields
        """
        if customers_df.empty:
            return customers_df
        
        total = len(customers_df)
        processed = 0
        
        # Get unique addresses for progress estimation
        if "PhysicalAddress" in customers_df.columns:
            unique_addresses = customers_df["PhysicalAddress"].dropna().unique()
            if progress_callback:
                progress_callback(f"🔍 Found {len(unique_addresses)} unique addresses to geocode")
        
        # Initialize new columns
        customers_df["CleanFirstName"] = ""
        customers_df["CleanLastName"] = ""
        customers_df["CleanFullName"] = ""
        customers_df["GoogleAddress"] = ""
        customers_df["GoogleStreetNumber"] = ""
        customers_df["GoogleStreetName"] = ""
        customers_df["GoogleSuburb"] = ""
        customers_df["GoogleState"] = ""
        customers_df["GooglePostcode"] = ""
        customers_df["GoogleCountry"] = ""
        customers_df["GoogleLat"] = None
        customers_df["GoogleLng"] = None
        customers_df["AddressValid"] = False
        customers_df["AddressPartialMatch"] = False
        customers_df["AddressHash"] = ""
        
        # Process each customer
        for idx, row in customers_df.iterrows():
            customers_df.loc[idx] = self.process_single_customer(row, uid)
            
            processed += 1
            
            if progress_callback and (processed % 50 == 0 or processed == total):
                api_stats = self.geocoder.get_stats()
                progress_callback(
                    f"⏳ Processed {processed}/{total} customers | "
                    f"API calls: {api_stats['api_requests']} | "
                    f"Cache hits: {api_stats['cache_hits']} | "
                    f"Est. cost: ${api_stats['estimated_cost']}"
                )
        
        if progress_callback:
            final_stats = self.geocoder.get_stats()
            progress_callback(
                f"✅ Complete! Processed {total} customers | "
                f"API calls: {final_stats['api_requests']} | "
                f"Cache hit rate: {final_stats['cache_hit_rate']}% | "
                f"Estimated cost: ${final_stats['estimated_cost']}"
            )
        
        return customers_df
    
    def get_invalid_addresses_report(self, customers_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get report of customers with invalid addresses
        
        Args:
            customers_df: Processed customer DataFrame
            
        Returns:
            DataFrame with invalid address records
        """
        if "AddressValid" not in customers_df.columns:
            return pd.DataFrame()
        
        invalid = customers_df[customers_df["AddressValid"] == False].copy()
        
        if not invalid.empty:
            report_cols = ["CustomerId", "CustomerName", "PhysicalAddress", "GoogleAddress"]
            report_cols = [c for c in report_cols if c in invalid.columns]
            return invalid[report_cols]
        
        return pd.DataFrame()
    
    def get_partial_match_report(self, customers_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get report of customers with partial address matches (may need review)
        
        Args:
            customers_df: Processed customer DataFrame
            
        Returns:
            DataFrame with partial match records
        """
        if "AddressPartialMatch" not in customers_df.columns:
            return pd.DataFrame()
        
        partial = customers_df[customers_df["AddressPartialMatch"] == True].copy()
        
        if not partial.empty:
            report_cols = [
                "CustomerId", "CustomerName", "PhysicalAddress", 
                "GoogleAddress", "GoogleSuburb", "GoogleState"
            ]
            report_cols = [c for c in report_cols if c in partial.columns]
            return partial[report_cols]
        
        return pd.DataFrame()
