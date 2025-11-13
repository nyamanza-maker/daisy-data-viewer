# firebase_setup.py
import json
import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore, storage as admin_storage
import pyrebase

def get_pyrebase_config():
    return {
        "apiKey": st.secrets["FIREBASE_API_KEY"],
        "authDomain": st.secrets["FIREBASE_AUTH_DOMAIN"],
        "projectId": st.secrets["FIREBASE_PROJECT_ID"],
        "storageBucket": st.secrets["FIREBASE_STORAGE_BUCKET"],
        "messagingSenderId": st.secrets["FIREBASE_MESSAGING_SENDER_ID"],
        "appId": st.secrets["FIREBASE_APP_ID"],
        "databaseURL": ""  # not using Realtime DB
    }

@st.cache_resource
def init_pyrebase():
    return pyrebase.initialize_app(get_pyrebase_config())

@st.cache_resource
def init_firebase_admin():
    # Use service account for Firestore (server-side writes)
    admin_json = st.secrets["FIREBASE_ADMIN_JSON"]
    cred = credentials.Certificate(json.loads(admin_json))
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred, {
            "storageBucket": st.secrets["FIREBASE_STORAGE_BUCKET"]
        })
    db = firestore.client()
    bucket = admin_storage.bucket()
    return db, bucket
