# pages/1_Booking_Calendar.py

import io
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
import pyrebase

# -----------------------------
# Shared Firebase config
# (same pattern as app.py)
# -----------------------------
firebase_section = st.secrets.get("FIREBASE", None)
if firebase_section is None:
    st.error("FIREBASE config not found in Streamlit secrets.")
    st.stop()

firebase_config = {
    "apiKey": firebase_section["api_key"],
    "authDomain": firebase_section["auth_domain"],
    "projectId": firebase_section["project_id"],
    "storageBucket": firebase_section["storage_bucket"],
    "messagingSenderId": firebase_section["messaging_sender_id"],
    "appId": firebase_section["app_id"],
    "databaseURL": firebase_section.get("database_url", "https://dummy.firebaseio.com"),
}

firebase = pyrebase.initialize_app(firebase_config)
storage = firebase.storage()


# -----------------------------
# Helpers (trimmed from app.py)
# -----------------------------
def file_exists(uid: str, filename: str, id_token: str) -> bool:
    path = f"franchises/{uid}/{filename}"
    url = f"https://firebasestorage.googleapis.com/v0/b/{firebase_config['storageBucket']}/o/{path.replace('/', '%2F')}"
    headers = {"Authorization": f"Bearer {id_token}"}
    try:
        r = requests.get(url, headers=headers)
        return r.status_code == 200
    except Exception:
        return False


def download_csv_as_df(uid: str, filename: str, id_token: str, **kwargs):
    path = f"franchises/{uid}/{filename}"
    url = f"https://firebasestorage.googleapis.com/v0/b/{firebase_config['storageBucket']}/o/{path.replace('/', '%2F')}?alt=media"
    headers = {"Authorization": f"Bearer {id_token}"}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to download {filename}")
    return pd.read_csv(io.BytesIO(r.content), **kwargs)


@st.cache_data(ttl=300)
def load_data(uid: str, id_token: str):
    customers = notes = bookings = None

    if file_exists(uid, "Customers.csv", id_token):
        customers = download_csv_as_df(uid, "Customers.csv", id_token, low_memory=False)

        # same display name logic as app.py
        if "CustomerName" in customers.columns:
            customers["DisplayName"] = customers["CustomerName"]
        elif "CompanyName" in customers.columns:
            customers["DisplayName"] = customers["CompanyName"]
        else:
            customers["DisplayName"] = "Unknown"

    if file_exists(uid, "Notes.csv", id_token):
        notes = download_csv_as_df(uid, "Notes.csv", id_token, low_memory=False)

    if file_exists(uid, "Bookings.csv", id_token):
        bookings = download_csv_as_df(uid, "Bookings.csv", id_token, low_memory=False)

    return customers, notes, bookings


# -----------------------------
# Auth check (reuse session)
# -----------------------------
st.set_page_config(
    page_title="Daisy Data Viewer - Booking Calendar",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="📅",
)

if "auth" not in st.session_state or st.session_state["auth"] is None:
    st.markdown("### 📅 Booking Calendar")
    st.warning("Please sign in via the main Daisy Data Viewer page first.")
    st.stop()

uid = st.session_state["auth"]["uid"]
id_token = st.session_state["auth"]["idToken"]

# Reuse cleansed/original mode from main app
view_mode = st.session_state.get("view_mode", "cleansed").lower()
view_is_cleansed = view_mode == "cleansed"

# -----------------------------
# Load data
# -----------------------------
customers, notes, bookings = load_data(uid, id_token)

if bookings is None or bookings.empty:
    st.markdown("### 📅 Booking Calendar")
    st.info("No bookings found. Please upload Bookings.csv first.")
    st.stop()

# Map CustomerId -> DisplayName
customer_name_map = {}
if customers is not None and "CustomerId" in customers.columns:
    customer_name_map = (
        customers.set_index("CustomerId")["DisplayName"].to_dict()
    )

# Parse dates
if "StartDateTime" not in bookings.columns or "EndDateTime" not in bookings.columns:
    st.error("Bookings.csv must contain StartDateTime and EndDateTime columns.")
    st.stop()

bookings["StartDT"] = pd.to_datetime(bookings["StartDateTime"], errors="coerce")
bookings["EndDT"] = pd.to_datetime(bookings["EndDateTime"], errors="coerce")

bookings = bookings.dropna(subset=["StartDT", "EndDT"])
if bookings.empty:
    st.info("All bookings have invalid or missing dates.")
    st.stop()

# Enrich with customer name and text label
bookings["CustomerName"] = bookings["CustomerId"].map(customer_name_map)
bookings["CustomerName"] = bookings["CustomerName"].fillna("(no customer)")

bookings["Staff"] = bookings["Staff"].fillna("Unassigned")
bookings["Service"] = bookings["Service"].fillna("Service")

# Choose notes source based on view mode
if view_is_cleansed and "CleanNotes" in bookings.columns:
    bookings["NotesForView"] = bookings["CleanNotes"].fillna("")
elif "Notes" in bookings.columns:
    bookings["NotesForView"] = bookings["Notes"].fillna("")
else:
    bookings["NotesForView"] = ""

# Compact label shown inside the bar
bookings["Label"] = (
    bookings["CustomerName"].astype(str).str.slice(0, 20)
    + " - "
    + bookings["Service"].astype(str).str.slice(0, 20)
)

# -----------------------------
# UI Controls
# -----------------------------
st.markdown("""
<div class='main-header'>
    <h1>📅 Booking Calendar</h1>
    <p>Week and day schedule view with 15 minute resolution</p>
</div>
""", unsafe_allow_html=True)

col_top1, col_top2, col_top3 = st.columns([1.5, 1, 1])

with col_top1:
    # Pick a reference date; we snap to the Monday of that week
    selected_date = st.date_input("Select date", datetime.today())

with col_top2:
    view_range = st.radio(
        "View",
        ["Week", "Day"],
        horizontal=True,
        key="calendar_view_range",
    )

with col_top3:
    # Staff filter
    all_staff = sorted(bookings["Staff"].dropna().unique().tolist())
    selected_staff = st.multiselect(
        "Staff filter",
        options=all_staff,
        default=all_staff,
    )

# Time range for the calendar (15 min blocks inside)
col_time1, col_time2 = st.columns(2)
with col_time1:
    day_start_str = st.text_input("Day start (HH:MM)", "07:00")
with col_time2:
    day_end_str = st.text_input("Day end (HH:MM)", "19:00")

# -----------------------------
# Filter by date range and staff
# -----------------------------
selected_dt = datetime.combine(selected_date, datetime.min.time())

if view_range == "Week":
    week_start = selected_dt - timedelta(days=selected_dt.weekday())  # Monday
    week_end = week_start + timedelta(days=7)
    mask = (bookings["StartDT"] >= week_start) & (bookings["StartDT"] < week_end)
else:
    day_start = selected_dt
    day_end = day_start + timedelta(days=1)
    mask = (bookings["StartDT"] >= day_start) & (bookings["StartDT"] < day_end)

bookings_view = bookings[mask].copy()

if selected_staff:
    bookings_view = bookings_view[bookings_view["Staff"].isin(selected_staff)]

if bookings_view.empty:
    st.info("No bookings in this range with the selected filters.")
    st.stop()

# -----------------------------
# Build Plotly timeline
# -----------------------------
try:
    day_start_time = datetime.strptime(day_start_str, "%H:%M").time()
    day_end_time = datetime.strptime(day_end_str, "%H:%M").time()
except ValueError:
    st.error("Time values must be in HH:MM format.")
    st.stop()

# Constrain x-range for display (keeps focus on the working day)
if view_range == "Week":
    x_start = datetime.combine(week_start.date(), day_start_time)
    x_end = datetime.combine((week_start + timedelta(days=6)).date(), day_end_time)
else:
    x_start = datetime.combine(selected_dt.date(), day_start_time)
    x_end = datetime.combine(selected_dt.date(), day_end_time)

# Plotly timeline
fig = px.timeline(
    bookings_view,
    x_start="StartDT",
    x_end="EndDT",
    y="Staff",
    color="Service",
    text="Label",
    hover_data={
        "CustomerName": True,
        "Service": True,
        "StartDT": True,
        "EndDT": True,
        "NotesForView": True,
    },
)

# Staff rows in natural top-to-bottom order
fig.update_yaxes(autorange="reversed")

# Text inside bars
fig.update_traces(
    textposition="inside",
    insidetextanchor="start",
    hovertemplate="<b>%{customdata[0]}</b><br>"
                  "Service: %{customdata[1]}<br>"
                  "Start: %{customdata[2]}<br>"
                  "End: %{customdata[3]}<br>"
                  "<br>Notes:<br>%{customdata[4]}<extra></extra>"
)

# 15 minute ticks on x-axis
fig.update_layout(
    margin=dict(l=10, r=10, t=40, b=10),
    xaxis=dict(
        range=[x_start, x_end],
        tickformat="%a %H:%M",
        dtick=15 * 60 * 1000,  # 15 minutes in ms
    ),
    height=700,
)

st.plotly_chart(fig, use_container_width=True)

# -----------------------------
# Detail table of bookings
# -----------------------------
st.markdown("### 📋 Bookings in this view")

# Nice compact table
show_cols = [
    "StartDT",
    "EndDT",
    "Staff",
    "CustomerName",
    "Service",
    "NotesForView",
]

df_display = bookings_view[show_cols].sort_values("StartDT").copy()
df_display["StartDT"] = df_display["StartDT"].dt.strftime("%a %d %b %Y %H:%M")
df_display["EndDT"] = df_display["EndDT"].dt.strftime("%a %d %b %Y %H:%M")
df_display.rename(
    columns={
        "StartDT": "Start",
        "EndDT": "End",
        "NotesForView": "Notes",
    },
    inplace=True,
)

st.dataframe(df_display, use_container_width=True, hide_index=True)

st.markdown(
    "<p style='color:#666;font-size:0.9rem;'>Tip: hover bookings in the calendar to see full notes.</p>",
    unsafe_allow_html=True,
)
