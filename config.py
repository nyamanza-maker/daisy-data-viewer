# config.py

import os

# Firestore collection names
CUSTOMERS_COLL = "customers"
BOOKINGS_COLL = "bookings"
NOTES_COLL = "notes"
MIGRATIONS_COLL = "migrations"
TENANTS_COLL = "tenants"

# Ollama / local LLM
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "phi3.5")

# Google Maps
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")

# Booking note parsing patterns (can be made into a JSON config later)
FROM_PATTERNS = ["FROM:", "FROM "]
TO_PATTERNS = ["GOING TO:", "TO:", "TO "]

# Name cleaning tokens to strip
NAME_STRIP_TOKENS = [
    "ACC", "ALBANY", "TM", "CHILDREN", "CLIENT", "NB", "ALB", "NORTH BAYS"
]
