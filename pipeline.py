# pipeline.py

from typing import Optional
import pandas as pd
from datetime import datetime

from db_utils import customer_doc, booking_doc, note_doc
from ai_utils import clean_customer_name, extract_booking_from_to_and_notes
from geocode import geocode_address


def parse_datetime(dt_str: str) -> Optional[datetime]:
    if not dt_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    return None


def process_customers_df(tenant_id: str, customers_df: pd.DataFrame):
    for _, row in customers_df.iterrows():
        cid = row.get("CustomerId")
        if not cid:
            continue

        raw_name = row.get("CustomerName") or row.get("CompanyName") or ""
        name_clean = clean_customer_name(raw_name)

        physical = str(row.get("PhysicalAddress") or "").strip()
        google_addr = geocode_address(physical) if physical else {"valid": False}

        doc_ref = customer_doc(tenant_id, cid)
        doc_ref.set(
            {
                "CustomerId": cid,
                "franchise_id": tenant_id,
                "raw": {
                    "CustomerName": raw_name,
                    "Telephone": row.get("Telephone") or row.get("SmsNumber") or "",
                    "Email": row.get("Email") or "",
                    "PhysicalAddress": physical,
                    "Gender": row.get("Gender") or row.get("GenderDescription") or "",
                    "DateOfBirth": row.get("DateOfBirth") or "",
                },
                "clean": {
                    "full_name": name_clean["full"],
                    "first_name": name_clean["first"],
                    "second_name": name_clean["second"],
                    "primary_phone": row.get("Telephone") or row.get("SmsNumber") or "",
                    "email": row.get("Email") or "",
                    "primary_address_text": google_addr.get("formatted_address", physical) if google_addr.get("valid") else physical,
                    "gender": row.get("Gender") or row.get("GenderDescription") or "",
                    "dob": row.get("DateOfBirth") or "",
                },
                "google_address": google_addr,
                "flags": {
                    "address_valid": google_addr.get("valid", False),
                    "address_ai_guessed": False,  # reserved if we add AI guessing
                },
                "meta": {
                    "ingested_at": datetime.utcnow().isoformat(),
                },
            },
            merge=True,
        )


def process_notes_df(tenant_id: str, notes_df: pd.DataFrame):
    for _, row in notes_df.iterrows():
        nid = row.get("NoteId")
        if not nid:
            continue

        note_date = parse_datetime(str(row.get("NoteDate") or ""))
        note_text = str(row.get("NoteText") or "")

        doc_ref = note_doc(tenant_id, nid)
        doc_ref.set(
            {
                "NoteId": nid,
                "CustomerId": row.get("CustomerId"),
                "franchise_id": tenant_id,
                "raw": {
                    "note_text": note_text,
                },
                "clean": {
                    "note_text": note_text,  # we can add formatting cleanup later
                },
                "note_date": note_date.isoformat() if note_date else None,
                "meta": {
                    "ingested_at": datetime.utcnow().isoformat(),
                },
            },
            merge=True,
        )


def process_bookings_df(tenant_id: str, bookings_df: pd.DataFrame):
    for _, row in bookings_df.iterrows():
        bid = row.get("BookingId")
        if not bid:
            continue

        raw_notes = str(row.get("Notes") or "")

        parsed = extract_booking_from_to_and_notes(raw_notes)
        from_text = parsed["from"]
        to_text = parsed["to"]
        extra_notes = parsed["notes"]

        google_from = geocode_address(from_text) if from_text else {"valid": False}
        google_to = geocode_address(to_text) if to_text else {"valid": False}

        start_dt = parse_datetime(str(row.get("StartDateTime") or ""))
        end_dt = parse_datetime(str(row.get("EndDateTime") or ""))

        doc_ref = booking_doc(tenant_id, bid)
        doc_ref.set(
            {
                "BookingId": bid,
                "CustomerId": row.get("CustomerId"),
                "franchise_id": tenant_id,
                "BusinessId": row.get("BusinessId"),
                "Staff": row.get("Staff"),
                "ServiceId": row.get("ServiceId"),
                "Service": row.get("Service"),
                "RecurringAppointment": bool(row.get("RecurringAppointment") in [True, "TRUE", "True", "true", 1, "1"]),
                "Price": row.get("Price"),
                "Status": row.get("Status"),
                "StartDateTime": start_dt.isoformat() if start_dt else None,
                "EndDateTime": end_dt.isoformat() if end_dt else None,
                "raw": {
                    "notes": raw_notes,
                },
                "clean": {
                    "from_text": from_text,
                    "to_text": to_text,
                    "extra_notes": extra_notes,
                },
                "from_address_google": google_from,
                "to_address_google": google_to,
                "flags": {
                    "from_valid": google_from.get("valid", False),
                    "to_valid": google_to.get("valid", False),
                    "from_ai_guessed": not google_from.get("valid", False) and bool(from_text),
                    "to_ai_guessed": not google_to.get("valid", False) and bool(to_text),
                },
                "meta": {
                    "ingested_at": datetime.utcnow().isoformat(),
                },
            },
            merge=True,
        )
