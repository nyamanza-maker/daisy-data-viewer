import streamlit as st
import pandas as pd
import os
import pyrebase
from datetime import datetime, timedelta
# Optional: pyrebase for Firebase Auth
try:
    import pyrebase as pyrebase
except ImportError:
    pyrebase = None

st.set_page_config(page_title="Daisy Data Viewer", layout="wide")

# ---------- Firebase Authentication Setup ----------


# Load Firebase section safely (this is the FIX)
firebase_section = st.secrets.get("FIREBASE", None)
FIREBASE_ENABLED = pyrebase is not None and firebase_section is not None
#st.write("Firebase Enabled:", FIREBASE_ENABLED)
#st.write("Secrets Loaded:", "FIREBASE" in st.secrets)
#st.write("Loaded FIREBASE secrets:", firebase_section)
#st.write("Firebase keys:", list(firebase_section.keys()))
if FIREBASE_ENABLED:
    fb_conf = firebase_section

    firebase_config = {
        "apiKey": fb_conf["api_key"],
        "authDomain": fb_conf["auth_domain"],
        "projectId": fb_conf["project_id"],
        "storageBucket": fb_conf["storage_bucket"],
        "messagingSenderId": fb_conf["messaging_sender_id"],
        "appId": fb_conf["app_id"],
        "databaseURL": "https://daisy-data-viewer.firebaseio.com",
    }

    firebase = pyrebase.initialize_app(firebase_config)
    auth = firebase.auth()

    def login():
        st.title("Daisy Data Viewer – Login")

        email = st.text_input("Email")
        password = st.text_input("Password", type="password")

        if st.button("Sign in"):
            try:
                user = auth.sign_in_with_email_and_password(email, password)
                st.session_state["user"] = user
                st.session_state["email"] = email
                st.session_state["franchise_id"] = (
                    email.split("@")[0].replace(".", "_").lower()
                )
                st.success("Signed in successfully.")
                st.rerun()
            except Exception as e:
                st.error(f"Login failed: {e}")

        st.stop()

    if "user" not in st.session_state:
        login()

else:
    # Dev mode / local fallback
    st.warning("Firebase not configured; running without authentication.")
    st.session_state.setdefault("franchise_id", "local")


# ---------- Helper functions ----------

def get_migration_file(original_file):
    base, ext = os.path.splitext(original_file)
    return f"{base}_migrated{ext}"


def load_or_create_migration_df(original_df, original_file):
    migrated_file = get_migration_file(original_file)
    if os.path.exists(migrated_file):
        migrated_df = pd.read_csv(migrated_file)
    else:
        id_col = "CustomerId" if "CustomerId" in original_df.columns else original_df.columns[0]
        migrated_df = original_df[[id_col]].drop_duplicates().copy()
        migrated_df["Migrated"] = False
        migrated_df.to_csv(migrated_file, index=False)
    return migrated_df, migrated_file


def update_migration_status(migrated_file, ids_to_update):
    migrated_df = pd.read_csv(migrated_file)
    if "CustomerId" not in migrated_df.columns:
        return
    migrated_df.loc[migrated_df["CustomerId"].isin(ids_to_update), "Migrated"] = True
    migrated_df.to_csv(migrated_file, index=False)


def to_bool(v):
    if pd.isna(v):
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v == 1
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes", "y", "t")
    return False


# ---------- Load Data ----------

def load_data():
    customers = pd.read_csv("CustomersSAM.csv", low_memory=False)
    notes = pd.read_csv("NotesSAM.csv", low_memory=False)
    bookings = pd.read_csv("BookingsSAM.csv", low_memory=False)

    cust_cols = [c for c in customers.columns if c in [
        "CustomerId", "FirstName", "LastName", "CompanyName",
        "Telephone", "SMS", "PhysicalAddress", "Gender"
    ]]
    note_cols = [c for c in notes.columns if c in ["CustomerId", "CustomerName", "NoteText"]]
    book_cols = [c for c in bookings.columns if c in [
        "CustomerId", "CustomerName", "Staff", "Service",
        "StartDateTime", "EndDateTime", "Notes", "RecurringAppointment", "Price"
    ]]

    customers = customers[cust_cols].copy()
    notes = notes[note_cols].copy()
    bookings = bookings[book_cols].copy()

    cust_mig, cust_mig_file = load_or_create_migration_df(customers, "CustomersSAM.csv")
    notes_mig, notes_mig_file = load_or_create_migration_df(notes, "NotesSAM.csv")
    book_mig, book_mig_file = load_or_create_migration_df(bookings, "BookingsSAM.csv")

    customers = customers.merge(cust_mig, on="CustomerId", how="left")
    notes = notes.merge(notes_mig, on="CustomerId", how="left")
    bookings = bookings.merge(book_mig, on="CustomerId", how="left")

    return customers, notes, bookings, cust_mig_file, notes_mig_file, book_mig_file


customers, notes, bookings, cust_mig_file, notes_mig_file, book_mig_file = load_data()


# ---------- Sidebar Search ----------

st.sidebar.header("🔍 Search Customer")

def _commit_search():
    st.session_state["search_query"] = st.session_state.get("search_input", "")

if "search_query" not in st.session_state:
    st.session_state["search_query"] = ""
if "search_input" not in st.session_state:
    st.session_state["search_input"] = ""

st.sidebar.text_input("Enter name or company:", key="search_input", on_change=_commit_search)
search_name = st.session_state["search_query"].strip().lower()

max_results = st.sidebar.number_input(
    "Max results to show", min_value=25, max_value=5000, value=200, step=25
)

if "FirstName" in customers.columns and "LastName" in customers.columns:
    customers["DisplayName"] = (
        customers["FirstName"].fillna("") + " " + customers["LastName"].fillna("")
    ).str.strip()
elif "CustomerName" in customers.columns:
    customers["DisplayName"] = customers["CustomerName"]
else:
    customers["DisplayName"] = customers["CompanyName"].fillna("Unknown")

if search_name:
    mask = customers.apply(
        lambda x: x.astype(str).str.lower().str.contains(search_name, na=False)
    )
    matched_customers = customers[mask.any(axis=1)].sort_values("DisplayName")
else:
    matched_customers = customers.sort_values("DisplayName")


# Filter: only future appointments
only_future = st.sidebar.checkbox("Only customers with future appointments", value=False)
if only_future:
    start_col = next((c for c in bookings.columns if "startdatetime" in c.lower()), None)
    if start_col:
        start_dt = pd.to_datetime(bookings[start_col], errors="coerce", infer_datetime_format=True)
        future_ids = bookings.loc[start_dt >= datetime.now(), "CustomerId"].unique().tolist()
        matched_customers = matched_customers[matched_customers["CustomerId"].isin(future_ids)]
    else:
        st.sidebar.warning("StartDateTime column missing.")


# Filter: exclude migrated
exclude_migrated = st.sidebar.checkbox("Exclude migrated customers", value=True)
if exclude_migrated and "Migrated" in matched_customers.columns:
    matched_customers = matched_customers[~matched_customers["Migrated"].map(to_bool)]


if not st.sidebar.checkbox("Show all matches", value=False):
    matched_customers = matched_customers.head(max_results)

st.sidebar.write(f"🧾 {len(matched_customers)} shown ({customers.shape[0]} total)")

options = matched_customers["DisplayName"].tolist()

selected_customer = st.sidebar.radio("Matches", options) if options else None


# ---------- Display ----------

if selected_customer:
    selected_row = matched_customers[matched_customers["DisplayName"] == selected_customer].iloc[0]
    customer_id = selected_row["CustomerId"]

    st.title(f"👤 {selected_customer}")

    # ---- Customer Details ----
    st.subheader("Customer Details")

    for col, val in selected_row.items():
        if col not in ["CustomerId", "Migrated"]:
            col1, col2 = st.columns([0.3, 0.7])
            col1.markdown(f"**{col}**")
            col2.code(str(val))

    # --- Customer migrated toggle ---
    if to_bool(selected_row.get("Migrated", False)):
        st.success("Customer migrated")
    else:
        if st.button("Mark this customer as migrated"):
            update_migration_status(cust_mig_file, [customer_id])
            st.cache_data.clear()
            st.rerun()

    # ---- Notes ----
    st.subheader("📝 Notes")
    cust_notes = notes[notes["CustomerId"] == customer_id]

    if cust_notes.empty:
        st.info("No notes for this customer.")
    else:
        for _, n in cust_notes.iterrows():
            st.code(n["NoteText"])

        if "Migrated" in cust_notes.columns and cust_notes["Migrated"].map(to_bool).any():
            st.success("Notes migrated")
        else:
            if st.button("Mark notes as migrated"):
                ids = cust_notes["CustomerId"].unique().tolist()
                update_migration_status(notes_mig_file, ids)
                st.cache_data.clear()
                st.rerun()

    # ---- Bookings ----
    st.subheader("📅 Bookings")

    cust_bookings = bookings[bookings["CustomerId"] == customer_id].copy()

    if cust_bookings.empty:
        st.info("No bookings for this customer.")
    else:

        # Normalize columns
        cust_bookings.columns = (
            cust_bookings.columns
            .str.strip()
            .str.replace("\ufeff", "", regex=False)
            .str.lower()
        )

        start_field = next((c for c in cust_bookings.columns if "startdatetime" in c), None)
        end_field = next((c for c in cust_bookings.columns if "enddatetime" in c), None)

        if not start_field or not end_field:
            st.error("Cannot find StartDateTime/EndDateTime columns.")
        else:
            cust_bookings[start_field] = pd.to_datetime(
                cust_bookings[start_field], errors="coerce", infer_datetime_format=True
            )
            cust_bookings[end_field] = pd.to_datetime(
                cust_bookings[end_field], errors="coerce", infer_datetime_format=True
            )

            cust_bookings["start_date"] = cust_bookings[start_field].dt.strftime("%d/%m/%Y")
            cust_bookings["start_time"] = cust_bookings[start_field].dt.strftime("%H:%M")
            cust_bookings["end_date"] = cust_bookings[end_field].dt.strftime("%d/%m/%Y")
            cust_bookings["end_time"] = cust_bookings[end_field].dt.strftime("%H:%M")

            filter_option = st.radio(
                "Show bookings:",
                ["All", "Past", "Next 3 Months", "Next 6 Months", "Next 12 Months"],
                horizontal=True
            )

            now = datetime.now()
            future_ranges = {
                "Next 3 Months": now + timedelta(days=90),
                "Next 6 Months": now + timedelta(days=180),
                "Next 12 Months": now + timedelta(days=365),
            }

            if filter_option == "Past":
                cust_bookings = cust_bookings[cust_bookings[start_field] < now]
            elif filter_option in future_ranges:
                end_date = future_ranges[filter_option]
                cust_bookings = cust_bookings[
                    (cust_bookings[start_field] >= now) &
                    (cust_bookings[start_field] <= end_date)
                ]

            for idx, b in cust_bookings.iterrows():
                is_migrated = to_bool(b.get("migrated"))
                color = "#3cb371" if b[start_field] >= now else "#888888"
                strike = "text-decoration: line-through;" if is_migrated else ""

                st.markdown(
                    f"<div style='background-color:{color}25;padding:8px;border-radius:6px;margin-bottom:6px;{strike}'>"
                    f"<b>{b['staff']}</b> | {b['service']} | "
                    f"{b['start_date']} {b['start_time']} → {b['end_date']} {b['end_time']}"
                    f"</div>",
                    unsafe_allow_html=True
                )

                fields = {
                    "Service": b["service"],
                    "Staff": b["staff"],
                    "Price": b["price"],
                    "Recurring": "Yes" if to_bool(b.get("recurringappointment")) else "No",
                    "Start Date": b["start_date"],
                    "Start Time": b["start_time"],
                    "End Date": b["end_date"],
                    "End Time": b["end_time"],
                }

                for label, val in fields.items():
                    col1, col2 = st.columns([0.2, 0.8])
                    col1.markdown(f"**{label}:**")
                    if is_migrated:
                        col2.markdown(f"<span style='text-decoration: line-through;'>{val}</span>",
                                      unsafe_allow_html=True)
                    else:
                        col2.text(str(val))

                # Notes
                notes_txt = b.get("notes", "")
                if notes_txt:
                    st.markdown("**Notes:**")
                    if is_migrated:
                        st.markdown(
                            f"<div style='background-color:#f0f0f0;border-radius:4px;"
                            f"padding:8px;text-decoration:line-through;'>{notes_txt}</div>",
                            unsafe_allow_html=True
                        )
                    else:
                        st.text_area(
                            "Booking Notes",
                            value=str(notes_txt),
                            key=f"notes-{idx}",
                            height=100,
                            label_visibility="collapsed",
                        )

                if is_migrated:
                    st.success("Migrated")
                else:
                    if st.button("Mark as migrated", key=f"migrate-{idx}"):
                        update_migration_status(book_mig_file, [b["customerid"]])
                        st.cache_data.clear()
                        st.rerun()

                st.markdown("---")
