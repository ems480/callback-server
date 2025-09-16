# server_debug.py
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, g
import os
import logging
import sqlite3
import json
import requests
import uuid
from datetime import datetime

API_MODE = os.getenv("API_MODE", "sandbox")
SANDBOX_API_TOKEN = os.getenv("SANDBOX_API_TOKEN")
LIVE_API_TOKEN = os.getenv("LIVE_API_TOKEN")
API_TOKEN = LIVE_API_TOKEN if API_MODE == "live" else SANDBOX_API_TOKEN
PAWAPAY_URL = "https://api.sandbox.pawapay.io/deposits" if API_MODE == "sandbox" \
    else "https://api.pawapay.io/deposits"

# DB
DATABASE = os.path.join(os.path.dirname(__file__), "transactions.db")

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Simple table for debugging raw callbacks
def init_db():
    db = sqlite3.connect(DATABASE)
    cur = db.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        depositId TEXT UNIQUE,
        status TEXT,
        amount REAL,
        currency TEXT,
        phoneNumber TEXT,
        provider TEXT,
        providerTransactionId TEXT,
        failureCode TEXT,
        failureMessage TEXT,
        metadata TEXT,
        received_at TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS raw_callbacks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        depositId TEXT,
        raw TEXT,
        headers TEXT,
        received_at TEXT
    )
    """)
    db.commit()
    db.close()

with app.app_context():
    init_db()

def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()

@app.route("/")
def home():
    logger.info(f"Using API_MODE={API_MODE}, API_TOKEN startswith={str(API_TOKEN)[:8]}, URL={PAWAPAY_URL}")
    return "PawaPay Callback Receiver (debug) is running ✅"

# Initiate payment (same idea — keep logging the third-party response)
@app.route('/initiate-payment', methods=['POST'])
def initiate_payment():
    try:
        data = request.json or {}
        phone = data.get("phone")
        amount = data.get("amount")
        correspondent = data.get("correspondent", "MTN_MOMO_ZMB")
        currency = data.get("currency", "ZMW")

        if not phone or not amount:
            return jsonify({"error": "Missing phone or amount"}), 400

        # Generate depositId locally (OK if pawaPay supports a provided depositId)
        deposit_id = str(uuid.uuid4())
        customer_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = {
            "depositId": deposit_id,
            "amount": str(amount),
            "currency": currency,
            "correspondent": correspondent,
            "payer": {"type": "MSISDN", "address": {"value": phone}},
            "customerTimestamp": customer_timestamp,
            "statementDescription": "StudyCraftPay",
            "metadata": [
                {"fieldName": "orderId", "fieldValue": "ORD-" + deposit_id},
                {"fieldName": "customerId", "fieldValue": phone, "isPII": True}
            ]
        }

        headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
        resp = requests.post(PAWAPAY_URL, json=payload, headers=headers, timeout=15)
        try:
            result = resp.json()
        except Exception:
            result = {"raw_text": resp.text}

        logger.info("Initiate payment → pawaPay response: %s", json.dumps(result)[:1000])

        # Store initial record (use status from pawaPay response if provided)
        status = result.get("status", "PENDING") if isinstance(result, dict) else "PENDING"
        db = get_db()
        cur = db.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO transactions
            (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId, failureCode, failureMessage, metadata, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            deposit_id, status, float(amount), currency, phone, correspondent, None, None, None,
            json.dumps(payload.get("metadata")), datetime.utcnow().isoformat()
        ))
        db.commit()

        # Return depositId and whatever pawaPay returned, plus is_final hint
        is_final = status in ("COMPLETED", "FAILED", "REJECTED")
        return jsonify({"depositId": deposit_id, "status": status, "is_final": is_final, "raw_pawapay": result}), 200

    except Exception:
        logger.exception("Error initiating payment")
        return jsonify({"error": "Internal server error"}), 500

# Debug: store and log raw callback body + headers
@app.route('/callback/deposit', methods=['POST'])
def deposit_callback():
    try:
        raw_body = request.get_data(as_text=True)
        headers = dict(request.headers)
        logger.info("Received callback headers: %s", json.dumps({k: headers[k] for k in headers if k.lower().startswith('x-') or k.lower().startswith('signature')})[:1000])
        logger.info("Received callback body: %s", raw_body[:2000])

        # attempt to parse JSON
        try:
            data = request.get_json(force=True)
        except Exception:
            data = None

        # Try to extract depositId from common places
        deposit_id = None
        if data:
            # top-level keys
            deposit_id = data.get("depositId") or data.get("data", {}).get("deposit", {}).get("depositId") \
                         or data.get("data", {}).get("depositId")
        # fall back to parsing raw text for a UUID-like string (optional)
        if not deposit_id and raw_body:
            # naive attempt (not perfect) to find uuid-like substring
            import re
            m = re.search(r"[0-9a-fA-F-]{36}", raw_body)
            if m:
                deposit_id = m.group(0)

        # record raw callback for debugging
        db = get_db()
        cur = db.cursor()
        cur.execute("""
            INSERT INTO raw_callbacks (depositId, raw, headers, received_at) VALUES (?, ?, ?, ?)
        """, (deposit_id, raw_body, json.dumps(headers), datetime.utcnow().isoformat()))
        db.commit()

        # If we couldn't parse JSON, return 400 to help debugging (but still record)
        if not data:
            logger.warning("Callback had no JSON body")
            return jsonify({"error": "No JSON received"}), 400

        # Extract useful fields defensively
        status = data.get("status") or data.get("data", {}).get("status")
        amount = data.get("amount") or data.get("data", {}).get("amount")
        currency = data.get("currency") or data.get("data", {}).get("currency")
        payer = data.get("payer") or data.get("data", {}).get("payer") or {}
        account_details = payer.get("accountDetails") if isinstance(payer, dict) else {}
        phone_number = account_details.get("phoneNumber") or payer.get("address", {}).get("value") or None
        provider = account_details.get("provider") or None
        provider_txn = data.get("providerTransactionId") or data.get("data", {}).get("providerTransactionId")
        failure_reason = data.get("failureReason") or data.get("data", {}).get("failureReason", {})

        # Persist callback fields to transactions table (replace/insert)
        cur.execute("""
            INSERT OR REPLACE INTO transactions 
            (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId, failureCode, failureMessage, metadata, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            deposit_id,
            status,
            float(amount) if amount else None,
            currency,
            phone_number,
            provider,
            provider_txn,
            failure_reason.get("failureCode") if isinstance(failure_reason, dict) else None,
            failure_reason.get("failureMessage") if isinstance(failure_reason, dict) else None,
            json.dumps(data.get("metadata")) if data.get("metadata") else json.dumps(data.get("data", {}).get("metadata")) if data.get("data") else None,
            datetime.utcnow().isoformat()
        ))
        db.commit()

        # respond quickly 200 — pawaPay expects a fast 200
        return jsonify({"received": True}), 200

    except Exception:
        logger.exception("Error handling deposit callback")
        return jsonify({"error": "Internal server error"}), 500

# GET deposit status (includes is_final)
@app.route('/transactions/<deposit_id>', methods=['GET'])
def get_transaction(deposit_id):
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM transactions WHERE depositId = ?", (deposit_id,))
    row = cur.fetchone()
    if not row:
        return jsonify({"error": "not found"}), 404
    result = {k: row[k] for k in row.keys()}
    if result.get("metadata"):
        try:
            result["metadata"] = json.loads(result["metadata"])
        except Exception:
            pass
    status = result.get("status")
    is_final = status in ("COMPLETED", "FAILED", "REJECTED")
    result["is_final"] = bool(is_final)
    return jsonify(result), 200

# Extra debug endpoints
@app.route('/debug/callbacks', methods=['GET'])
def debug_callbacks():
    # list last 20 raw callbacks
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT id, depositId, raw, headers, received_at FROM raw_callbacks ORDER BY id DESC LIMIT 20")
    rows = cur.fetchall()
    items = []
    for r in rows:
        items.append({"id": r["id"], "depositId": r["depositId"], "raw": r["raw"][:2000], "headers": json.loads(r["headers"]) if r["headers"] else None, "received_at": r["received_at"]})
    return jsonify(items), 200

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)


# from dotenv import load_dotenv
# load_dotenv()  # Load .env file locally

# from flask import Flask, request, jsonify, g
# import os
# import logging
# import sqlite3
# import json
# import requests
# import uuid
# from datetime import datetime


# # -------------------------
# # API CONFIGURATION
# # -------------------------
# API_MODE = os.getenv("API_MODE", "sandbox")  # default sandbox
# SANDBOX_API_TOKEN = os.getenv("SANDBOX_API_TOKEN")
# LIVE_API_TOKEN = os.getenv("LIVE_API_TOKEN")

# # Decide which one to use
# if API_MODE == "live":
#     API_TOKEN = LIVE_API_TOKEN
# else:
#     API_TOKEN = SANDBOX_API_TOKEN

# PAWAPAY_URL = "https://api.sandbox.pawapay.io/deposits" if API_MODE == "sandbox" \
#     else "https://api.pawapay.io/deposits"


# # -------------------------
# # DATABASE CONFIGURATION
# # -------------------------
# DATABASE = os.path.join(os.path.dirname(__file__), "transactions.db")

# app = Flask(__name__)
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Ensure DB initialized at startup (for Render)
# def init_db():
#     """Create transactions table if it does not exist."""
#     db = sqlite3.connect(DATABASE)
#     cur = db.cursor()
#     cur.execute("""
#     CREATE TABLE IF NOT EXISTS transactions (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         depositId TEXT UNIQUE,
#         status TEXT,
#         amount REAL,
#         currency TEXT,
#         phoneNumber TEXT,
#         provider TEXT,
#         providerTransactionId TEXT,
#         failureCode TEXT,
#         failureMessage TEXT,
#         metadata TEXT,
#         received_at TEXT
#     )
#     """)
#     db.commit()
#     db.close()

# with app.app_context():
#     init_db()  # 🔥 always run at startup


# def get_db():
#     db = getattr(g, "_database", None)
#     if db is None:
#         db = g._database = sqlite3.connect(DATABASE)
#         db.row_factory = sqlite3.Row
#     return db


# @app.teardown_appcontext
# def close_connection(exception):
#     db = getattr(g, "_database", None)
#     if db is not None:
#         db.close()


# # -------------------------
# # HEALTHCHECK
# # -------------------------
# @app.route('/')
# def home():
#     logger.info(f"Using API_MODE={API_MODE}, API_TOKEN startswith={str(API_TOKEN)[:10]}, URL={PAWAPAY_URL}")
#     return "PawaPay Callback Receiver is running ✅"


# # -------------------------
# # INITIATE PAYMENT (App → Server → PawaPay)
# # -------------------------
# @app.route('/initiate-payment', methods=['POST'])
# def initiate_payment():
#     try:
#         data = request.json
#         phone = data.get("phone")
#         amount = data.get("amount")
#         correspondent = data.get("correspondent", "MTN_MOMO_ZMB")  # default MTN
#         currency = data.get("currency", "ZMW")  # default ZMW

#         if not phone or not amount:
#             return jsonify({"error": "Missing phone or amount"}), 400

#         deposit_id = str(uuid.uuid4())
#         customer_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

#         payload = {
#             "depositId": deposit_id,
#             "amount": str(amount),
#             "currency": currency,
#             "correspondent": correspondent,
#             "payer": {"type": "MSISDN", "address": {"value": phone}},
#             "customerTimestamp": customer_timestamp,
#             "statementDescription": "StudyCraftPay",
#             "metadata": [
#                 {"fieldName": "orderId", "fieldValue": "ORD-" + deposit_id},
#                 {"fieldName": "customerId", "fieldValue": phone, "isPII": True}
#             ]
#         }

#         headers = {
#             "Authorization": f"Bearer {API_TOKEN}",
#             "Content-Type": "application/json"
#         }

#         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)
#         result = resp.json()

#         # Save to DB
#         db = get_db()
#         cur = db.cursor()
#         cur.execute("""
#             INSERT OR REPLACE INTO transactions 
#             (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId, failureCode, failureMessage, metadata, received_at)
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#         """, (
#             deposit_id,
#             result.get("status", "PENDING"),
#             float(amount),
#             currency,
#             phone,
#             correspondent,
#             None,
#             None,
#             None,
#             json.dumps(payload.get("metadata")),
#             datetime.utcnow().isoformat()
#         ))
#         db.commit()

#         return jsonify({"depositId": deposit_id, **result}), 200

#     except Exception as e:
#         logger.exception("Error initiating payment")
#         return jsonify({"error": "Internal server error"}), 500



# # -------------------------
# # DEPOSIT CALLBACK RECEIVER
# # -------------------------
# @app.route('/callback/deposit', methods=['POST'])
# def deposit_callback():
#     try:
#         data = request.get_json()
#         if not data:
#             return jsonify({"error": "No JSON data"}), 400

#         deposit_id = data.get("depositId")
#         status = data.get("status")
#         amount = data.get("amount")
#         currency = data.get("currency")
#         payer = data.get("payer", {})
#         account_details = payer.get("accountDetails", {})
#         phone_number = account_details.get("phoneNumber")
#         provider = account_details.get("provider")
#         provider_txn = data.get("providerTransactionId")
#         failure_reason = data.get("failureReason", {})
#         failure_code = failure_reason.get("failureCode")
#         failure_message = failure_reason.get("failureMessage")
#         metadata = data.get("metadata")

#         # Persist callback to DB
#         db = get_db()
#         cur = db.cursor()
#         cur.execute("""
#             INSERT OR REPLACE INTO transactions 
#             (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId, failureCode, failureMessage, metadata, received_at)
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#         """, (
#             deposit_id,
#             status,
#             float(amount) if amount else None,
#             currency,
#             phone_number,
#             provider,
#             provider_txn,
#             failure_code,
#             failure_message,
#             json.dumps(metadata) if metadata else None,
#             datetime.utcnow().isoformat()
#         ))
#         db.commit()
#         return jsonify({"received": True}), 200

#     except Exception:
#         logger.exception("Error handling deposit callback")
#         return jsonify({"error": "Internal server error"}), 500


# # -------------------------
# # POLL / GET STATUS BY DEPOSIT ID
# # -------------------------
# @app.route('/deposit_status/<deposit_id>', methods=['GET'])
# def get_deposit_status(deposit_id):
#     db = get_db()
#     cur = db.cursor()
#     cur.execute("SELECT * FROM transactions WHERE depositId = ?", (deposit_id,))
#     row = cur.fetchone()
#     if not row:
#         return jsonify({"status": None, "message": "Deposit not found"}), 404

#     result = {k: row[k] for k in row.keys()}
#     if result.get("metadata"):
#         try:
#             result["metadata"] = json.loads(result["metadata"])
#         except Exception:
#             pass

#     return jsonify(result), 200


# @app.route('/transactions/<deposit_id>', methods=['GET'])
# def get_transaction(deposit_id):
#     db = get_db()
#     cur = db.cursor()
#     cur.execute("SELECT * FROM transactions WHERE depositId = ?", (deposit_id,))
#     row = cur.fetchone()
#     if not row:
#         return jsonify({"error": "not found"}), 404

#     result = {k: row[k] for k in row.keys()}
#     if result.get("metadata"):
#         try:
#             result["metadata"] = json.loads(result["metadata"])
#         except Exception:
#             pass

#     return jsonify(result), 200


# # -------------------------
# # RUN SERVER LOCALLY
# # -------------------------
# if __name__ == '__main__':
#     init_db()
#     port = int(os.environ.get("PORT", 5000))
#     app.run(host="0.0.0.0", port=port)


