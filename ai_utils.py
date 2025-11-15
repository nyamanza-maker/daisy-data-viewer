# ai_utils.py

import requests
from typing import Dict
from config import OLLAMA_BASE_URL, OLLAMA_MODEL, FROM_PATTERNS, TO_PATTERNS, NAME_STRIP_TOKENS

def ollama_chat(prompt: str) -> str:
    """Simple Ollama chat wrapper."""
    url = f"{OLLAMA_BASE_URL}/api/generate"
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
    }
    resp = requests.post(url, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("response", "").strip()


def extract_booking_from_to_and_notes(raw_text: str) -> Dict[str, str]:
    """
    Use LLM to split booking notes into from, to, notes.
    Returns {"from": ..., "to": ..., "notes": ...}
    """
    prompt = f"""
You are a data extraction assistant.

From the following booking note, extract:
1) The pickup address (FROM)
2) The dropoff address (TO)
3) The remaining notes (NOTES)

If something is unknown, leave it empty. 
Output JSON only, like:
{{"from": "...", "to": "...", "notes": "..."}}

Booking note:
\"\"\"{raw_text}\"\"\"
"""
    resp = ollama_chat(prompt)
    # Very simple safety parse – in a real system you'd use json5 or robust parsing
    import json
    try:
        data = json.loads(resp)
    except Exception:
        data = {"from": "", "to": "", "notes": raw_text}
    return {
        "from": data.get("from", "").strip(),
        "to": data.get("to", "").strip(),
        "notes": data.get("notes", "").strip(),
    }


def clean_customer_name(raw_name: str) -> Dict[str, str]:
    """
    Remove tokens like ACC, Albany etc. Then attempt to split into first and last.
    """
    if not raw_name:
        return {"full": "", "first": "", "second": ""}

    name = raw_name.strip()

    # Strip common tokens
    for token in NAME_STRIP_TOKENS:
        name = name.replace(token, " ")

    # Remove multiple spaces and punctuation-style separators
    import re
    name = re.sub(r"[-–,*/]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()

    # Title case
    name_tc = name.title()

    parts = name_tc.split()
    if len(parts) == 1:
        first = parts[0]
        second = ""
    else:
        first = parts[0]
        second = " ".join(parts[1:])

    return {
        "full": name_tc,
        "first": first,
        "second": second,
    }
