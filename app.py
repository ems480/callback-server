# from dotenv import load_dotenv
# load_dotenv()

# from flask import Flask, request, jsonify, g
# import os
# import logging
# import sqlite3
# import json
# import requests
# import uuid
# from datetime import datetime

# # -------------------------
# # CONFIG
# # -------------------------
# API_MODE = os.getenv("API_MODE", "sandbox")  # sandbox | live
# SANDBOX_API_TOKEN = os.getenv("SANDBOX_API_TOKEN")
# LIVE_API_TOKEN = os.getenv("LIVE_API_TOKEN")

# if API_MODE == "live":
#     API_TOKEN = LIVE_API_TOKEN
#     PAWAPAY_DEPOSITS_URL = "https://api.pawapay.io/deposits"
#     PAWAPAY_PAYOUTS_URL = "https://api.pawapay.io/v2/payouts"
# else:
#     API_TOKEN = SANDBOX_API_TOKEN
#     PAWAPAY_DEPOSITS_URL = "https://api.sandbox.pawapay.io/deposits"
#     PAWAPAY_PAYOUTS_URL = "https://api.sandbox.pawapay.io/v2/payouts"

# DATABASE = os.path.join(os.path.dirname(__file__), "transactions.db")

# app = Flask(__name__)
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# # -------------------------
# # DATABASE
# # -------------------------
# def init_db():
#     db = sqlite3.connect(DATABASE)
#     cur = db.cursor()

#     # deposits table
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

#     # payouts table
#     cur.execute("""
#     CREATE TABLE IF NOT EXISTS payouts (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         payoutId TEXT UNIQUE,
#         status TEXT,
#         amount REAL,
#         currency TEXT,
#         phoneNumber TEXT,
#         provider TEXT,
#         providerTransactionId TEXT,
#         failureCode TEXT,
#         failureMessage TEXT,
#         metadata TEXT,
#         sent_at TEXT
#     )
#     """)

#     db.commit()
#     db.close()


# with app.app_context():
#     init_db()


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
# # ROUTES
# # -------------------------
# @app.route("/")
# def home():
#     return "PawaPay Deposit + Payout Server ✅"


# # -------------------------
# # DEPOSIT (Investors send money in)
# # -------------------------
# @app.route("/initiate-payment", methods=["POST"])
# def initiate_payment():
#     try:
#         data = request.json
#         phone = data.get("phone")
#         amount = data.get("amount")
#         correspondent = data.get("correspondent", "MTN_MOMO_ZMB")
#         currency = data.get("currency", "ZMW")

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

#         resp = requests.post(PAWAPAY_DEPOSITS_URL, json=payload, headers=headers)
#         result = resp.json()

#         # Save to DB
#         db = get_db()
#         cur = db.cursor()
#         cur.execute("""
#             INSERT OR REPLACE INTO transactions
#             (depositId, status, amount, currency, phoneNumber, provider,
#              providerTransactionId, failureCode, failureMessage, metadata, received_at)
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


# @app.route('/transactions/<deposit_id>', methods=['GET'])
# def get_transaction(deposit_id):
#     db = get_db()
#     cur = db.cursor()
#     cur.execute("SELECT * FROM transactions WHERE depositId = ?", (deposit_id,))
#     row = cur.fetchone()

#     if not row:
#         return jsonify({"error": "Transaction not found"}), 404

#     result = {k: row[k] for k in row.keys()}
#     if result.get("metadata"):
#         try:
#             result["metadata"] = json.loads(result["metadata"])
#         except Exception:
#             pass

#     return jsonify(result), 200


# # -------------------------
# # PAYOUT (Loans → Borrowers)
# # -------------------------
# @app.route("/initiate-payout", methods=["POST"])
# def initiate_payout():
#     try:
#         data = request.json
#         phone = data.get("phone")
#         amount = data.get("amount")
#         provider = data.get("provider", "MTN_MOMO_ZMB")
#         currency = data.get("currency", "ZMW")
#         customer_message = data.get("customerMessage", "Loan disbursement")

#         if not phone or not amount:
#             return jsonify({"error": "Missing phone or amount"}), 400

#         payout_id = str(uuid.uuid4())

#         payload = {
#             "payoutId": payout_id,
#             "recipient": {
#                 "type": "MMO",
#                 "accountDetails": {
#                     "phoneNumber": phone,
#                     "provider": provider
#                 }
#             },
#             "customerMessage": customer_message,
#             "amount": str(amount),
#             "currency": currency,
#             "metadata": [
#                 {"orderId": "ORD-" + payout_id},
#                 {"customerPhone": phone, "isPII": True}
#             ]
#         }

#         headers = {
#             "Authorization": f"Bearer {API_TOKEN}",
#             "Content-Type": "application/json"
#         }

#         resp = requests.post(PAWAPAY_PAYOUTS_URL, json=payload, headers=headers)
#         result = resp.json()

#         # Save payout to DB
#         db = get_db()
#         cur = db.cursor()
#         cur.execute("""
#             INSERT OR REPLACE INTO payouts
#             (payoutId, status, amount, currency, phoneNumber, provider,
#              providerTransactionId, failureCode, failureMessage, metadata, sent_at)
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#         """, (
#             payout_id,
#             result.get("status", "PENDING"),
#             float(amount),
#             currency,
#             phone,
#             provider,
#             None,
#             None,
#             None,
#             json.dumps(payload.get("metadata")),
#             datetime.utcnow().isoformat()
#         ))
#         db.commit()

#         return jsonify({"payoutId": payout_id, **result}), 200

#     except Exception:
#         logger.exception("Error initiating payout")
#         return jsonify({"error": "Internal server error"}), 500


# @app.route('/callback/payout', methods=['POST'])
# def payout_callback():
#     try:
#         data = request.get_json()
#         if not data:
#             return jsonify({"error": "No JSON data"}), 400

#         payout_id = data.get("payoutId")
#         status = data.get("status")
#         amount = data.get("amount")
#         currency = data.get("currency")
#         recipient = data.get("recipient", {})
#         account_details = recipient.get("accountDetails", {})
#         phone_number = account_details.get("phoneNumber")
#         provider = account_details.get("provider")
#         provider_txn = data.get("providerTransactionId")
#         failure_reason = data.get("failureReason", {})
#         failure_code = failure_reason.get("failureCode")
#         failure_message = failure_reason.get("failureMessage")
#         metadata = data.get("metadata")

#         db = get_db()
#         cur = db.cursor()
#         cur.execute("""
#             INSERT OR REPLACE INTO payouts
#             (payoutId, status, amount, currency, phoneNumber, provider, providerTransactionId, failureCode, failureMessage, metadata, sent_at)
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#         """, (
#             payout_id,
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
#         logger.exception("Error handling payout callback")
#         return jsonify({"error": "Internal server error"}), 500

# @app.route("/api/loans", methods=["GET"])
# def get_all_loans():
#     """
#     Admin endpoint: Get all loans for disbursement dashboard.
#     """
#     try:
#         db = get_db()
#         cur = db.cursor()
#         cur.execute("SELECT * FROM loans")
#         rows = cur.fetchall()

#         loans = []
#         for row in rows:
#             loans.append({
#                 "loan_id": row["loan_id"],
#                 "user_id": row["user_id"],
#                 "amount": row["amount"],
#                 "status": row["status"],
#                 "requested_at": row["requested_at"]
#             })

#         return jsonify({"loans": loans}), 200
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

# @app.route('/payouts/<payout_id>', methods=['GET'])
# def get_payout(payout_id):
#     db = get_db()
#     cur = db.cursor()
#     cur.execute("SELECT * FROM payouts WHERE payoutId = ?", (payout_id,))
#     row = cur.fetchone()

#     if not row:
#         return jsonify({"error": "Payout not found"}), 404

#     result = {k: row[k] for k in row.keys()}
#     if result.get("metadata"):
#         try:
#             result["metadata"] = json.loads(result["metadata"])
#         except Exception:
#             pass

#     return jsonify(result), 200


# # -------------------------
# # MAIN
# # -------------------------
# if __name__ == "__main__":
#     init_db()
#     port = int(os.environ.get("PORT", 5000))
#     app.run(host="0.0.0.0", port=port)


from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, g
import os, logging, sqlite3, json, requests, uuid
from datetime import datetime, timedelta

# -------------------------
# CONFIG
# -------------------------
API_MODE = os.getenv("API_MODE", "sandbox")  # sandbox | live
SANDBOX_API_TOKEN = os.getenv("SANDBOX_API_TOKEN")
LIVE_API_TOKEN = os.getenv("LIVE_API_TOKEN")

if API_MODE == "live":
    API_TOKEN = LIVE_API_TOKEN
    PAWAPAY_DEPOSITS_URL = "https://api.pawapay.io/deposits"
    PAWAPAY_PAYOUTS_URL = "https://api.pawapay.io/v2/payouts"
else:
    API_TOKEN = SANDBOX_API_TOKEN
    PAWAPAY_DEPOSITS_URL = "https://api.sandbox.pawapay.io/deposits"
    PAWAPAY_PAYOUTS_URL = "https://api.sandbox.pawapay.io/v2/payouts"

DATABASE = os.path.join(os.path.dirname(__file__), "transactions.db")

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------
# DATABASE
# -------------------------
def init_db():
    db = sqlite3.connect(DATABASE)
    cur = db.cursor()

    # deposits / investments
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        depositId TEXT UNIQUE,
        user_id TEXT,
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

    # payouts (loan disbursements)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS payouts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payoutId TEXT UNIQUE,
        loan_id TEXT,
        status TEXT,
        amount REAL,
        currency TEXT,
        phoneNumber TEXT,
        provider TEXT,
        providerTransactionId TEXT,
        failureCode TEXT,
        failureMessage TEXT,
        metadata TEXT,
        sent_at TEXT
    )
    """)

    # loans
    cur.execute("""
    CREATE TABLE IF NOT EXISTS loans (
        loan_id TEXT PRIMARY KEY,
        user_id TEXT,
        amount REAL,
        balance REAL,
        status TEXT,
        requested_at TEXT,
        approved_at TEXT,
        return_date TEXT
    )
    """)

    db.commit()
    db.close()

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

# -------------------------
# INVESTMENTS
# -------------------------
@app.route("/api/investments/initiate", methods=["POST"])
def initiate_investment():
    try:
        data = request.json
        user_id = data.get("user_id")
        phone = data.get("phone")
        amount = data.get("amount")

        if not user_id or not phone or not amount:
            return jsonify({"error": "Missing user_id, phone, or amount"}), 400

        deposit_id = str(uuid.uuid4())

        # Store pending investment
        db = get_db()
        cur = db.cursor()
        cur.execute("""
            INSERT INTO transactions
            (depositId, user_id, status, amount, currency, phoneNumber, provider, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (deposit_id, user_id, "PENDING", float(amount), "ZMW", phone, "MTN_MOMO_ZMB", datetime.utcnow().isoformat()))
        db.commit()

        return jsonify({"deposit_id": deposit_id, "status": "PENDING"}), 200
    except Exception as e:
        logger.exception("Error initiating investment")
        return jsonify({"error": str(e)}), 500

@app.route("/api/investments/<user_id>", methods=["GET"])
def get_investments(user_id):
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT * FROM transactions WHERE user_id=?", (user_id,))
        rows = cur.fetchall()

        investments = []
        for row in rows:
            investments.append({
                "deposit_id": row["depositId"],
                "amount": row["amount"],
                "status": row["status"],
                "balance": row["amount"],  # simple model for now
                "return_date": (datetime.utcnow() + timedelta(days=30)).isoformat()
            })

        return jsonify({"investments": investments}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -------------------------
# LOANS
# -------------------------
@app.route("/api/loans/request", methods=["POST"])
def request_loan():
    try:
        data = request.json
        user_id = data.get("user_id")
        amount = data.get("amount")
        if not user_id or not amount:
            return jsonify({"error": "Missing user_id or amount"}), 400

        loan_id = str(uuid.uuid4())
        db = get_db()
        cur = db.cursor()
        cur.execute("""
            INSERT INTO loans (loan_id, user_id, amount, balance, status, requested_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (loan_id, user_id, float(amount), float(amount), "PENDING", datetime.utcnow().isoformat()))
        db.commit()

        return jsonify({"loan_id": loan_id, "status": "PENDING"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/loans/<user_id>", methods=["GET"])
def get_user_loans(user_id):
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT * FROM loans WHERE user_id=?", (user_id,))
        rows = cur.fetchall()

        loans = []
        for row in rows:
            loans.append({
                "loan_id": row["loan_id"],
                "user_id": row["user_id"],
                "amount": row["amount"],
                "balance": row["balance"],
                "status": row["status"],
                "requested_at": row["requested_at"],
                "return_date": row["return_date"]
            })

        return jsonify({"loans": loans}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/loans", methods=["GET"])
def get_all_loans():
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT * FROM loans")
        rows = cur.fetchall()
        loans = [{k: row[k] for k in row.keys()} for row in rows]
        return jsonify({"loans": loans}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Approve loan → disburse payout
@app.route("/api/loans/<loan_id>/approve", methods=["POST"])
def approve_loan(loan_id):
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT * FROM loans WHERE loan_id=?", (loan_id,))
        loan = cur.fetchone()
        if not loan:
            return jsonify({"error": "Loan not found"}), 404

        if loan["status"] != "PENDING":
            return jsonify({"error": "Loan not pending"}), 400

        # mark approved
        return_date = (datetime.utcnow() + timedelta(days=30)).isoformat()
        cur.execute("UPDATE loans SET status=?, approved_at=?, return_date=? WHERE loan_id=?",
                    ("APPROVED", datetime.utcnow().isoformat(), return_date, loan_id))
        db.commit()

        return jsonify({"loan_id": loan_id, "status": "APPROVED"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Repayment
@app.route("/api/loans/<loan_id>/repay", methods=["POST"])
def repay_loan(loan_id):
    try:
        data = request.json
        amount = float(data.get("amount", 0))

        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT * FROM loans WHERE loan_id=?", (loan_id,))
        loan = cur.fetchone()
        if not loan:
            return jsonify({"error": "Loan not found"}), 404

        new_balance = max(0, loan["balance"] - amount)
        status = "REPAID" if new_balance == 0 else "PARTIAL"

        cur.execute("UPDATE loans SET balance=?, status=? WHERE loan_id=?",
                    (new_balance, status, loan_id))
        db.commit()

        return jsonify({"loan_id": loan_id, "status": status, "balance": new_balance}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)




