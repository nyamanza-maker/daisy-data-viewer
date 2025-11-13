from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd

app = Flask(__name__)
CORS(app)

# Load data once at startup
customers = pd.read_csv("CustomersSAM.csv")
bookings = pd.read_csv("BookingsSAM.csv")
notes = pd.read_csv("NotesSAM.csv")

# Merge for convenience
merged = (
    customers
    .merge(bookings, on="CustomerId", how="left")
    .merge(notes, on="CustomerId", how="left", suffixes=("_booking", "_note"))
)

@app.route("/clients", methods=["GET"])
def get_clients():
    query = request.args.get("q", "").lower()
    filtered = customers[
        customers["FirstName"].str.lower().str.contains(query, na=False)
        | customers["LastName"].str.lower().str.contains(query, na=False)
        | customers["Email"].str.lower().str.contains(query, na=False)
    ] if query else customers

    return jsonify(filtered.to_dict(orient="records"))

@app.route("/clients/<int:client_id>", methods=["GET"])
def get_client(client_id):
    client = merged[merged["CustomerId"] == client_id]
    if client.empty:
        return jsonify({"error": "Client not found"}), 404

    data = {
        "client": client.iloc[0].to_dict(),
        "notes": client[["NoteDate", "NoteText"]].dropna().to_dict(orient="records"),
        "bookings": client[["BookingId", "StartDateTime", "EndDateTime", "Status"]].dropna().to_dict(orient="records"),
    }
    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)
