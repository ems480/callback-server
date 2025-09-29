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


# app.py
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

    # transactions (deposits / investments). status will be PENDING / COMPLETED / FAILED etc.
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
        phoneNumber TEXT,
        amount REAL,
        balance REAL,
        status TEXT,
        requested_at TEXT,
        approved_at TEXT,
        disbursed_at TEXT,
        return_date TEXT
    )
    """)

    # simple jobs table (optional for future worker queue)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT UNIQUE,
        job_type TEXT,
        payload TEXT,
        status TEXT,
        attempts INTEGER DEFAULT 0,
        max_attempts INTEGER DEFAULT 5,
        run_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )
    """)

    db.commit()
    db.close()


def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE, timeout=30)
        db.row_factory = sqlite3.Row
        # Better concurrency for sqlite usage in production-ish scenarios
        db.execute("PRAGMA journal_mode=WAL;")
        db.execute("PRAGMA synchronous=NORMAL;")
        db.execute("PRAGMA busy_timeout=5000;")
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


# -------------------------
# Helpers
# -------------------------
def now_iso():
    return datetime.utcnow().isoformat()


def calculate_available_funds():
    """
    Simple availability calculation:
      total_completed_deposits = sum(amount) of transactions with COMPLETED-like statuses
      reserved_or_disbursed = sum(amount) of loans with status APPROVED or DISBURSED
      available = completed - reserved/disbursed
    This is a simple model. In future replace with allocations table per-loan.
    """
    db = get_db()
    cur = db.cursor()
    # sum deposits that are completed
    cur.execute("""
        SELECT IFNULL(SUM(amount), 0) as total
        FROM transactions
        WHERE status IN ('COMPLETED', 'SUCCESS', 'PAYMENT_COMPLETED')
    """)
    total_completed = cur.fetchone()["total"] or 0.0

    cur.execute("""
        SELECT IFNULL(SUM(amount), 0) as reserved
        FROM loans
        WHERE status IN ('APPROVED', 'DISBURSED')
    """)
    reserved = cur.fetchone()["reserved"] or 0.0

    available = float(total_completed) - float(reserved)
    return max(0.0, available)


# -------------------------
# ROUTES - HEALTH
# -------------------------
@app.route("/")
def home():
    return jsonify({"ok": True, "api_mode": API_MODE}), 200


# -------------------------
# DEPOSIT (investor -> start deposit flow)
# - This mirrors your earlier deposit flow (we store PENDING and rely on callback to mark COMPLETE)
# -------------------------
@app.route("/initiate-payment", methods=["POST"])
def initiate_payment():
    try:
        data = request.json or {}
        phone = data.get("phone")
        amount = data.get("amount")
        correspondent = data.get("correspondent", "MTN_MOMO_ZMB")
        currency = data.get("currency", "ZMW")
        user_id = data.get("user_id")  # optional caller supplied user id to link deposit to investor

        if not phone or not amount:
            return jsonify({"error": "Missing phone or amount"}), 400

        deposit_id = str(uuid.uuid4())
        customer_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        payload = {
            "depositId": deposit_id,
            "amount": str(amount),
            "currency": currency,
            "correspondent": correspondent,
            "payer": {"type": "MSISDN", "address": {"value": phone}},
            "customerTimestamp": customer_timestamp,
            "statementDescription": "VillageBank Investment",
            "metadata": [
                {"fieldName": "userId", "fieldValue": user_id or ""},
                {"fieldName": "purpose", "fieldValue": "investment"}
            ]
        }

        headers = {"Authorization": f"Bearer {API_TOKEN}" if API_TOKEN else "", "Content-Type": "application/json"}

        try:
            resp = requests.post(PAWAPAY_DEPOSITS_URL, json=payload, headers=headers, timeout=15)
            pay_result = resp.json() if resp.content else {"status": "PENDING"}
        except Exception as e:
            logger.warning("PawaPay request failed: %s", e)
            pay_result = {"status": "PENDING", "error": str(e)}

        # Save to DB
        db = get_db()
        cur = db.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO transactions
            (depositId, user_id, status, amount, currency, phoneNumber, provider, metadata, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            deposit_id,
            user_id,
            pay_result.get("status", "PENDING"),
            float(amount),
            currency,
            phone,
            correspondent,
            json.dumps(payload.get("metadata")),
            now_iso()
        ))
        db.commit()

        return jsonify({"depositId": deposit_id, "status": pay_result.get("status", "PENDING"), "payment_response": pay_result}), 200

    except Exception as e:
        logger.exception("Error initiating payment")
        return jsonify({"error": "Internal server error", "detail": str(e)}), 500


@app.route('/callback/deposit', methods=['POST'])
def deposit_callback():
    """
    PawaPay will call this. We store/update transaction status using depositId.
    Expectation: payload includes depositId, status, amount, payer.accountDetails etc.
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data"}), 400

        deposit_id = data.get("depositId")
        status = data.get("status")
        amount = data.get("amount")
        currency = data.get("currency")
        payer = data.get("payer", {})
        account_details = payer.get("accountDetails", {}) if payer else {}
        phone_number = account_details.get("phoneNumber")
        provider = account_details.get("provider")
        provider_txn = data.get("providerTransactionId")
        failure_reason = data.get("failureReason", {})
        failure_code = failure_reason.get("failureCode")
        failure_message = failure_reason.get("failureMessage")
        metadata = data.get("metadata")

        logger.info("=== CALLBACK RECEIVED ===")
        logger.info(f"DepositId: {deposit_id}, Status: {status}, Amount: {amount} {currency}, Phone: {phone_number}")
        logger.info(f"Metadata: {metadata}")

        db = get_db()
        cur = db.cursor()
        # Upsert: if depositId exists update, else insert
        cur.execute("""
            INSERT INTO transactions
            (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId, failureCode, failureMessage, metadata, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(depositId) DO UPDATE SET
                status=excluded.status,
                amount=excluded.amount,
                providerTransactionId=excluded.providerTransactionId,
                failureCode=excluded.failureCode,
                failureMessage=excluded.failureMessage,
                metadata=excluded.metadata,
                received_at=excluded.received_at
        """, (
            deposit_id,
            status,
            float(amount) if amount is not None else None,
            currency,
            phone_number,
            provider,
            provider_txn,
            failure_code,
            failure_message,
            json.dumps(metadata) if metadata else None,
            now_iso()
        ))
        db.commit()

        return jsonify({"received": True}), 200

    except Exception:
        logger.exception("Error handling deposit callback")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/transactions/<deposit_id>', methods=['GET'])
def get_transaction(deposit_id):
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM transactions WHERE depositId = ?", (deposit_id,))
    row = cur.fetchone()

    if not row:
        return jsonify({"error": "Transaction not found"}), 404

    result = {k: row[k] for k in row.keys()}
    if result.get("metadata"):
        try:
            result["metadata"] = json.loads(result["metadata"])
        except Exception:
            pass

    return jsonify(result), 200


# -------------------------
# INVESTMENTS API (app UI)
# -------------------------
@app.route("/api/investments/initiate", methods=["POST"])
def api_initiate_investment():
    """
    Endpoint used by the Kivy app to start an investment (wrapper over initiate_payment)
    """
    return initiate_payment()


@app.route("/api/investments/<user_id>", methods=["GET"])
def api_get_investments(user_id):
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM transactions WHERE user_id = ? ORDER BY received_at DESC", (user_id,))
    rows = cur.fetchall()
    investments = []
    for row in rows:
        investments.append({
            "depositId": row["depositId"],
            "amount": row["amount"],
            "status": row["status"],
            "balance": row["amount"],  # for now, same as amount until allocation/repay
            "return_date": (datetime.utcnow() + timedelta(days=30)).isoformat()
        })
    return jsonify({"investments": investments}), 200


# -------------------------
# LOANS API
# -------------------------
@app.route("/api/loans/request", methods=["POST"])
def api_request_loan():
    try:
        data = request.json or {}
        user_id = data.get("user_id")
        amount = data.get("amount")
        phone = data.get("phone")  # phone to disburse to later
        if not user_id or not amount or not phone:
            return jsonify({"error": "Missing user_id, amount or phone"}), 400

        # Basic check: this is just request stage - server accepts requests even if currently unavailable.
        loan_id = str(uuid.uuid4())
        db = get_db()
        cur = db.cursor()
        cur.execute("""
            INSERT INTO loans (loan_id, user_id, phoneNumber, amount, balance, status, requested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (loan_id, user_id, phone, float(amount), float(amount), "PENDING", now_iso()))
        db.commit()
        return jsonify({"loan_id": loan_id, "status": "PENDING"}), 200
    except Exception as e:
        logger.exception("Error requesting loan")
        return jsonify({"error": "Internal server error", "detail": str(e)}), 500


@app.route("/api/loans/<user_id>", methods=["GET"])
def api_get_user_loans(user_id):
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM loans WHERE user_id = ? ORDER BY requested_at DESC", (user_id,))
    rows = cur.fetchall()
    loans = []
    for row in rows:
        loans.append({k: row[k] for k in row.keys()})
    return jsonify({"loans": loans}), 200


@app.route("/api/loans", methods=["GET"])
def api_get_all_loans():
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM loans ORDER BY requested_at DESC")
    rows = cur.fetchall()
    loans = [{k: row[k] for k in row.keys()} for row in rows]
    return jsonify({"loans": loans}), 200


@app.route("/api/loans/<loan_id>/approve", methods=["POST"])
def api_approve_loan(loan_id):
    """
    Approve loan (admin). This marks loan APPROVED only if enough available funds.
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT * FROM loans WHERE loan_id = ?", (loan_id,))
        loan = cur.fetchone()
        if not loan:
            return jsonify({"error": "Loan not found"}), 404
        if loan["status"] != "PENDING":
            return jsonify({"error": f"Loan not in PENDING state (current: {loan['status']})"}), 400

        # Check available funds
        available = calculate_available_funds()
        if float(loan["amount"]) > available:
            return jsonify({"error": "Insufficient available funds to approve loan", "available": available}), 400

        # Approve
        approved_at = now_iso()
        return_date = (datetime.utcnow() + timedelta(days=30)).isoformat()
        cur.execute("UPDATE loans SET status = ?, approved_at = ?, return_date = ? WHERE loan_id = ?",
                    ("APPROVED", approved_at, return_date, loan_id))
        db.commit()
        return jsonify({"loan_id": loan_id, "status": "APPROVED", "available_after": available - float(loan["amount"])}), 200
    except Exception as e:
        logger.exception("Error approving loan")
        return jsonify({"error": "Internal server error", "detail": str(e)}), 500


@app.route("/api/loans/<loan_id>/reject", methods=["POST"])
def api_reject_loan(loan_id):
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT * FROM loans WHERE loan_id = ?", (loan_id,))
        loan = cur.fetchone()
        if not loan:
            return jsonify({"error": "Loan not found"}), 404
        if loan["status"] not in ("PENDING", "APPROVED"):
            return jsonify({"error": "Loan cannot be rejected in current state"}), 400
        cur.execute("UPDATE loans SET status = ? WHERE loan_id = ?", ("REJECTED", loan_id))
        db.commit()
        return jsonify({"loan_id": loan_id, "status": "REJECTED"}), 200
    except Exception as e:
        logger.exception("Error rejecting loan")
        return jsonify({"error": "Internal server error", "detail": str(e)}), 500


@app.route("/api/loans/<loan_id>/disburse", methods=["POST"])
def api_disburse_loan(loan_id):
    """
    Admin triggers disbursement (payout). This:
      - verifies loan is APPROVED
      - checks available funds again
      - calls PawaPay Payout API to send money to loan.phoneNumber
      - records payout and updates loan status to DISBURSED
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT * FROM loans WHERE loan_id = ?", (loan_id,))
        loan = cur.fetchone()
        if not loan:
            return jsonify({"error": "Loan not found"}), 404

        if loan["status"] != "APPROVED":
            return jsonify({"error": f"Loan must be APPROVED before disbursement (current: {loan['status']})"}), 400

        available = calculate_available_funds()
        if float(loan["amount"]) > available:
            return jsonify({"error": "Insufficient available funds to disburse", "available": available}), 400

        # Build payout payload
        payout_id = str(uuid.uuid4())
        payload = {
            "payoutId": payout_id,
            "recipient": {
                "type": "MMO",
                "accountDetails": {
                    "phoneNumber": loan["phoneNumber"],
                    "provider": "MTN_MOMO_ZMB"
                }
            },
            "customerMessage": "Loan disbursement",
            "amount": str(loan["amount"]),
            "currency": "ZMW",
            "metadata": [
                {"fieldName": "loanId", "fieldValue": loan_id},
                {"fieldName": "userId", "fieldValue": loan["user_id"]}
            ]
        }

        headers = {"Authorization": f"Bearer {API_TOKEN}" if API_TOKEN else "", "Content-Type": "application/json"}

        try:
            resp = requests.post(PAWAPAY_PAYOUTS_URL, json=payload, headers=headers, timeout=20)
            pay_result = resp.json() if resp.content else {"status": "PENDING"}
        except Exception as e:
            logger.exception("Payout request exception")
            pay_result = {"status": "PENDING", "error": str(e)}

        # Save payout
        cur.execute("""
            INSERT OR REPLACE INTO payouts
            (payoutId, loan_id, status, amount, currency, phoneNumber, provider, metadata, sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            payout_id,
            loan_id,
            pay_result.get("status", "PENDING"),
            float(loan["amount"]),
            "ZMW",
            loan["phoneNumber"],
            "MTN_MOMO_ZMB",
            json.dumps(payload.get("metadata")),
            now_iso()
        ))

        # mark loan as DISBURSED (we assume payout attempt is underway; callback will confirm)
        cur.execute("UPDATE loans SET status = ?, disbursed_at = ? WHERE loan_id = ?", ("DISBURSED", now_iso(), loan_id))
        db.commit()

        return jsonify({"loan_id": loan_id, "status": "DISBURSED", "payout_response": pay_result, "payout_id": payout_id}), 200

    except Exception as e:
        logger.exception("Error disbursing loan")
        return jsonify({"error": "Internal server error", "detail": str(e)}), 500


@app.route("/api/loans/<loan_id>/repay", methods=["POST"])
def api_repay_loan(loan_id):
    """
    Borrower repays loan (this endpoint is for manual/manual-testing repayments).
    In production repayments should come via deposit callback matched to loanId via metadata.
    """
    try:
        data = request.json or {}
        amount = float(data.get("amount", 0))

        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT * FROM loans WHERE loan_id = ?", (loan_id,))
        loan = cur.fetchone()
        if not loan:
            return jsonify({"error": "Loan not found"}), 404

        new_balance = max(0.0, float(loan["balance"]) - float(amount))
        status = "REPAID" if new_balance == 0 else "PARTIAL"

        cur.execute("UPDATE loans SET balance = ?, status = ? WHERE loan_id = ?", (new_balance, status, loan_id))
        db.commit()

        # TODO: allocate repayment to investments (update transactions/investor balances) and create notify jobs.
        return jsonify({"loan_id": loan_id, "status": status, "balance": new_balance}), 200

    except Exception as e:
        logger.exception("Error repaying loan")
        return jsonify({"error": "Internal server error", "detail": str(e)}), 500


# -------------------------
# PAYOUT CALLBACK (PawaPay will call when payout completes/updates)
# -------------------------
@app.route('/callback/payout', methods=['POST'])
def payout_callback():
    """
    PawaPay will post payout status updates here.
    We upsert into payouts table and can adjust loan status if needed.
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON"}), 400

        payout_id = data.get("payoutId")
        status = data.get("status")
        amount = data.get("amount")
        recipient = data.get("recipient", {})
        account_details = recipient.get("accountDetails", {}) if recipient else {}
        phone_number = account_details.get("phoneNumber")
        provider = account_details.get("provider")
        provider_txn = data.get("providerTransactionId")
        failure_reason = data.get("failureReason", {})
        failure_code = failure_reason.get("failureCode")
        failure_message = failure_reason.get("failureMessage")
        metadata = data.get("metadata")

        db = get_db()
        cur = db.cursor()
        cur.execute("""
            INSERT INTO payouts (payoutId, status, amount, currency, phoneNumber, provider, providerTransactionId, failureCode, failureMessage, metadata, sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(payoutId) DO UPDATE SET
                status=excluded.status,
                providerTransactionId=excluded.providerTransactionId,
                failureCode=excluded.failureCode,
                failureMessage=excluded.failureMessage,
                metadata=excluded.metadata,
                sent_at=excluded.sent_at
        """, (
            payout_id,
            status,
            float(amount) if amount is not None else None,
            data.get("currency"),
            phone_number,
            provider,
            provider_txn,
            failure_code,
            failure_message,
            json.dumps(metadata) if metadata else None,
            now_iso()
        ))
        db.commit()

        # If payout completed, we might want to mark loan as PAID_OUT (already DISBURSED)
        # We can also react to failures (e.g., mark loan back to APPROVED so admin can retry)
        # Attempt to map payout -> loan via metadata if present:
        loan_id = None
        if metadata and isinstance(metadata, list):
            for m in metadata:
                if m.get("fieldName") == "loanId":
                    loan_id = m.get("fieldValue")
                    break

        if loan_id:
            if status and status.upper() in ("COMPLETED", "SUCCESS"):
                cur.execute("UPDATE loans SET status = ? WHERE loan_id = ?", ("DISBURSED", loan_id))
            elif status and status.upper() in ("REJECTED","FAILED"):
                # revert loan status so admin can retry
                cur.execute("UPDATE loans SET status = ? WHERE loan_id = ?", ("APPROVED", loan_id))
            db.commit()

        return jsonify({"ok": True}), 200

    except Exception:
        logger.exception("Error in payout callback")
        return jsonify({"error": "Internal server error"}), 500


# -------------------------
# ADMIN: get payout by id
# -------------------------
@app.route('/payouts/<payout_id>', methods=['GET'])
def get_payout(payout_id):
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM payouts WHERE payoutId = ?", (payout_id,))
    row = cur.fetchone()
    if not row:
        return jsonify({"error": "Payout not found"}), 404
    result = {k: row[k] for k in row.keys()}
    if result.get("metadata"):
        try:
            result["metadata"] = json.loads(result["metadata"])
        except Exception:
            pass
    return jsonify(result), 200


# -------------------------
# START
# -------------------------
if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)






