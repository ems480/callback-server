from flask import Flask, request, jsonify, g
import os
import logging
import sqlite3
import json
from datetime import datetime

# -------------------------
# DATABASE CONFIGURATION
# -------------------------
DATABASE = os.path.join(os.path.dirname(__file__), "transactions.db")

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db

def init_db():
    """Create transactions table if it does not exist."""
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
    db.commit()
    db.close()

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()

# -------------------------
# HEALTHCHECK
# -------------------------
@app.route('/')
def home():
    return "PawaPay Callback Receiver is running ✅"

# -------------------------
# DEPOSIT CALLBACK RECEIVER
# -------------------------
@app.route('/callback/deposit', methods=['POST'])
def deposit_callback():
    try:
        data = request.get_json()
        if not data:
            logger.warning("Deposit callback received with no JSON body")
            return jsonify({"error": "No JSON data"}), 400

        # Extract fields safely
        deposit_id = data.get("depositId")
        status = data.get("status")
        amount = data.get("amount") or None
        currency = data.get("currency")
        payer = data.get("payer", {})
        account_details = payer.get("accountDetails", {})
        phone_number = account_details.get("phoneNumber")
        provider = account_details.get("provider")
        provider_txn = data.get("providerTransactionId")
        failure_reason = data.get("failureReason", {})
        failure_code = failure_reason.get("failureCode")
        failure_message = failure_reason.get("failureMessage")
        metadata = data.get("metadata")

        logger.info(
            f"Deposit Callback → depositId={deposit_id}, status={status}, amount={amount} {currency}, "
            f"phone={phone_number}, provider={provider}, providerTxnId={provider_txn}, failureReason={failure_reason}"
        )

        # Persist to DB
        db = get_db()
        cur = db.cursor()
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
            failure_code,
            failure_message,
            json.dumps(metadata) if metadata else None,
            datetime.utcnow().isoformat()
        ))
        db.commit()
        return jsonify({"received": True}), 200

    except Exception as e:
        logger.exception("Error handling deposit callback")
        return jsonify({"error": "Internal server error"}), 500

# -------------------------
# POLL / GET STATUS BY DEPOSIT ID
# -------------------------
@app.route('/deposit_status/<deposit_id>', methods=['GET'])
def get_deposit_status(deposit_id):
    """Return latest status of a deposit, handles minimal or full callback."""
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM transactions WHERE depositId = ?", (deposit_id,))
    row = cur.fetchone()
    if not row:
        return jsonify({"status": None, "message": "Deposit not found"}), 404

    result = {k: row[k] for k in row.keys()}
    if result.get("metadata"):
        try:
            result["metadata"] = json.loads(result["metadata"])
        except Exception:
            pass

    return jsonify(result), 200

# -------------------------
# SIMPLE TRANSACTION LOOKUP
# -------------------------
@app.route('/transactions/<deposit_id>', methods=['GET'])
def get_transaction(deposit_id):
    """Inspect stored transaction by depositId."""
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

    return jsonify(result), 200

# -------------------------
# RUN SERVER
# -------------------------
if __name__ == '__main__':
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

# from flask import Flask, request, jsonify, g
# import os
# import logging
# import sqlite3
# import json
# from datetime import datetime

# # -------------------------
# # DATABASE CONFIGURATION
# # -------------------------
# DATABASE = os.path.join(os.path.dirname(__file__), "transactions.db")

# app = Flask(__name__)
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# def get_db():
#     db = getattr(g, "_database", None)
#     if db is None:
#         db = g._database = sqlite3.connect(DATABASE)
#         db.row_factory = sqlite3.Row
#     return db

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
#     return "PawaPay Callback Receiver is running ✅"

# # -------------------------
# # DEPOSIT CALLBACK RECEIVER
# # -------------------------
# @app.route('/callback/deposit', methods=['POST'])
# def deposit_callback():
#     try:
#         data = request.get_json()
#         if not data:
#             logger.warning("Deposit callback received with no JSON body")
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

#         logger.info(
#             f"Deposit Callback → depositId={deposit_id}, status={status}, amount={amount} {currency}, "
#             f"phone={phone_number}, provider={provider}, providerTxnId={provider_txn}, failureReason={failure_reason}"
#         )

#         # Persist to DB
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

#     except Exception as e:
#         logger.exception("Error handling deposit callback")
#         return jsonify({"error": "Internal server error"}), 500

# # -------------------------
# # POLL / GET STATUS BY DEPOSIT ID
# # -------------------------
# @app.route('/deposit_status/<deposit_id>', methods=['GET'])
# def get_deposit_status(deposit_id):
#     """Return latest status of a deposit."""
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

# # -------------------------
# # SIMPLE TRANSACTION LOOKUP
# # -------------------------
# @app.route('/transactions/<deposit_id>', methods=['GET'])
# def get_transaction(deposit_id):
#     """Inspect stored transaction by depositId."""
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
# # RUN SERVER
# # -------------------------
# if __name__ == '__main__':
#     init_db()
#     port = int(os.environ.get("PORT", 5000))
#     app.run(host="0.0.0.0", port=port)










