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
#     PAWAPAY_URL = "https://api.pawapay.io/deposits"
# else:
#     API_TOKEN = SANDBOX_API_TOKEN
#     PAWAPAY_URL = "https://api.sandbox.pawapay.io/deposits"

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
#     return "PawaPay Callback Receiver ✅"


# @app.route("/initiate-payment", methods=["POST"])
# def initiate_payment():
#     """App → Server → PawaPay"""
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

#         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)
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

#         # 🔥 LOG FULL CALLBACK
#         logger.info("=== CALLBACK RECEIVED ===")
#         logger.info(f"DepositId: {deposit_id}")
#         logger.info(f"Status: {status}")
#         logger.info(f"Amount: {amount} {currency}")
#         logger.info(f"Phone: {phone_number}, Provider: {provider}")
#         logger.info(f"ProviderTxn: {provider_txn}")
#         logger.info(f"Failure: {failure_code} - {failure_message}")
#         logger.info(f"Metadata: {metadata}")
#         logger.info("=========================")

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



# @app.route('/transactions/<deposit_id>', methods=['GET'])
# def get_transaction(deposit_id):
#     """
#     Returns the latest transaction status for a given deposit_id.
#     This now explicitly handles PENDING, ACCEPTED, RECONCILIATION, COMPLETED, FAILED.
#     """
#     db = get_db()
#     cur = db.cursor()
#     cur.execute("SELECT * FROM transactions WHERE depositId = ?", (deposit_id,))
#     row = cur.fetchone()

#     if not row:
#         return jsonify({"error": "Transaction not found"}), 404

#     # Construct response with all fields
#     result = {k: row[k] for k in row.keys()}

#     # Ensure metadata is returned as JSON
#     if result.get("metadata"):
#         try:
#             result["metadata"] = json.loads(result["metadata"])
#         except Exception:
#             pass

#     # Make sure status is exactly what PawaPay sent, including RECONCILIATION
#     status = result.get("status", "PENDING")
#     result["status"] = status  # This ensures the Kivy app sees the current state

#     # Optional: log for debugging
#     logger.info(f"Transaction queried: DepositId={deposit_id}, Status={status}")

#     return jsonify(result), 200


# # -------------------------
# # MAIN
# # -------------------------
# if __name__ == "__main__":
#     init_db()
#     port = int(os.environ.get("PORT", 5000))
#     app.run(host="0.0.0.0", port=port)


# # from dotenv import load_dotenv
# # load_dotenv()  # Load .env file locally

# # from flask import Flask, request, jsonify, g
# # import os
# # import logging
# # import sqlite3
# # import json
# # import requests
# # import uuid
# # from datetime import datetime


# # # -------------------------
# # # API CONFIGURATION
# # # -------------------------
# # API_MODE = os.getenv("API_MODE", "sandbox")  # default sandbox
# # SANDBOX_API_TOKEN = os.getenv("SANDBOX_API_TOKEN")
# # LIVE_API_TOKEN = os.getenv("LIVE_API_TOKEN")

# # # Decide which one to use
# # if API_MODE == "live":
# #     API_TOKEN = LIVE_API_TOKEN
# # else:
# #     API_TOKEN = SANDBOX_API_TOKEN

# # PAWAPAY_URL = "https://api.sandbox.pawapay.io/deposits" if API_MODE == "sandbox" \
# #     else "https://api.pawapay.io/deposits"


# # # -------------------------
# # # DATABASE CONFIGURATION
# # # -------------------------
# # DATABASE = os.path.join(os.path.dirname(__file__), "transactions.db")

# # app = Flask(__name__)
# # logging.basicConfig(level=logging.INFO)
# # logger = logging.getLogger(__name__)

# # # Ensure DB initialized at startup (for Render)
# # def init_db():
# #     """Create transactions table if it does not exist."""
# #     db = sqlite3.connect(DATABASE)
# #     cur = db.cursor()
# #     cur.execute("""
# #     CREATE TABLE IF NOT EXISTS transactions (
# #         id INTEGER PRIMARY KEY AUTOINCREMENT,
# #         depositId TEXT UNIQUE,
# #         status TEXT,
# #         amount REAL,
# #         currency TEXT,
# #         phoneNumber TEXT,
# #         provider TEXT,
# #         providerTransactionId TEXT,
# #         failureCode TEXT,
# #         failureMessage TEXT,
# #         metadata TEXT,
# #         received_at TEXT
# #     )
# #     """)
# #     db.commit()
# #     db.close()

# # with app.app_context():
# #     init_db()  # 🔥 always run at startup


# # def get_db():
# #     db = getattr(g, "_database", None)
# #     if db is None:
# #         db = g._database = sqlite3.connect(DATABASE)
# #         db.row_factory = sqlite3.Row
# #     return db


# # @app.teardown_appcontext
# # def close_connection(exception):
# #     db = getattr(g, "_database", None)
# #     if db is not None:
# #         db.close()


# # # -------------------------
# # # HEALTHCHECK
# # # -------------------------
# # @app.route('/')
# # def home():
# #     logger.info(f"Using API_MODE={API_MODE}, API_TOKEN startswith={str(API_TOKEN)[:10]}, URL={PAWAPAY_URL}")
# #     return "PawaPay Callback Receiver is running ✅"


# # # -------------------------
# # # INITIATE PAYMENT (App → Server → PawaPay)
# # # -------------------------
# # @app.route('/initiate-payment', methods=['POST'])
# # def initiate_payment():
# #     try:
# #         data = request.json
# #         phone = data.get("phone")
# #         amount = data.get("amount")
# #         correspondent = data.get("correspondent", "MTN_MOMO_ZMB")  # default MTN
# #         currency = data.get("currency", "ZMW")  # default ZMW

# #         if not phone or not amount:
# #             return jsonify({"error": "Missing phone or amount"}), 400

# #         deposit_id = str(uuid.uuid4())
# #         customer_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# #         payload = {
# #             "depositId": deposit_id,
# #             "amount": str(amount),
# #             "currency": currency,
# #             "correspondent": correspondent,
# #             "payer": {"type": "MSISDN", "address": {"value": phone}},
# #             "customerTimestamp": customer_timestamp,
# #             "statementDescription": "StudyCraftPay",
# #             "metadata": [
# #                 {"fieldName": "orderId", "fieldValue": "ORD-" + deposit_id},
# #                 {"fieldName": "customerId", "fieldValue": phone, "isPII": True}
# #             ]
# #         }

# #         headers = {
# #             "Authorization": f"Bearer {API_TOKEN}",
# #             "Content-Type": "application/json"
# #         }

# #         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)
# #         result = resp.json()

# #         # Save to DB
# #         db = get_db()
# #         cur = db.cursor()
# #         cur.execute("""
# #             INSERT OR REPLACE INTO transactions 
# #             (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId, failureCode, failureMessage, metadata, received_at)
# #             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
# #         """, (
# #             deposit_id,
# #             result.get("status", "PENDING"),
# #             float(amount),
# #             currency,
# #             phone,
# #             correspondent,
# #             None,
# #             None,
# #             None,
# #             json.dumps(payload.get("metadata")),
# #             datetime.utcnow().isoformat()
# #         ))
# #         db.commit()

# #         return jsonify({"depositId": deposit_id, **result}), 200

# #     except Exception as e:
# #         logger.exception("Error initiating payment")
# #         return jsonify({"error": "Internal server error"}), 500



# # # -------------------------
# # # DEPOSIT CALLBACK RECEIVER
# # # -------------------------
# # @app.route('/callback/deposit', methods=['POST'])
# # def deposit_callback():
# #     try:
# #         data = request.get_json()
# #         if not data:
# #             return jsonify({"error": "No JSON data"}), 400

# #         deposit_id = data.get("depositId")
# #         status = data.get("status")
# #         amount = data.get("amount")
# #         currency = data.get("currency")
# #         payer = data.get("payer", {})
# #         account_details = payer.get("accountDetails", {})
# #         phone_number = account_details.get("phoneNumber")
# #         provider = account_details.get("provider")
# #         provider_txn = data.get("providerTransactionId")
# #         failure_reason = data.get("failureReason", {})
# #         failure_code = failure_reason.get("failureCode")
# #         failure_message = failure_reason.get("failureMessage")
# #         metadata = data.get("metadata")

# #         # Persist callback to DB
# #         db = get_db()
# #         cur = db.cursor()
# #         cur.execute("""
# #             INSERT OR REPLACE INTO transactions 
# #             (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId, failureCode, failureMessage, metadata, received_at)
# #             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
# #         """, (
# #             deposit_id,
# #             status,
# #             float(amount) if amount else None,
# #             currency,
# #             phone_number,
# #             provider,
# #             provider_txn,
# #             failure_code,
# #             failure_message,
# #             json.dumps(metadata) if metadata else None,
# #             datetime.utcnow().isoformat()
# #         ))
# #         db.commit()
# #         return jsonify({"received": True}), 200

# #     except Exception:
# #         logger.exception("Error handling deposit callback")
# #         return jsonify({"error": "Internal server error"}), 500


# # # -------------------------
# # # POLL / GET STATUS BY DEPOSIT ID
# # # -------------------------
# # @app.route('/deposit_status/<deposit_id>', methods=['GET'])
# # def get_deposit_status(deposit_id):
# #     db = get_db()
# #     cur = db.cursor()
# #     cur.execute("SELECT * FROM transactions WHERE depositId = ?", (deposit_id,))
# #     row = cur.fetchone()
# #     if not row:
# #         return jsonify({"status": None, "message": "Deposit not found"}), 404

# #     result = {k: row[k] for k in row.keys()}
# #     if result.get("metadata"):
# #         try:
# #             result["metadata"] = json.loads(result["metadata"])
# #         except Exception:
# #             pass

# #     return jsonify(result), 200


# # @app.route('/transactions/<deposit_id>', methods=['GET'])
# # def get_transaction(deposit_id):
# #     db = get_db()
# #     cur = db.cursor()
# #     cur.execute("SELECT * FROM transactions WHERE depositId = ?", (deposit_id,))
# #     row = cur.fetchone()
# #     if not row:
# #         return jsonify({"error": "not found"}), 404

# #     result = {k: row[k] for k in row.keys()}
# #     if result.get("metadata"):
# #         try:
# #             result["metadata"] = json.loads(result["metadata"])
# #         except Exception:
# #             pass

# #     return jsonify(result), 200


# # # -------------------------
# # # RUN SERVER LOCALLY
# # # -------------------------
# # if __name__ == '__main__':
# #     init_db()
# #     port = int(os.environ.get("PORT", 5000))
# #     app.run(host="0.0.0.0", port=port)


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

    # deposits table
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

    # payouts table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS payouts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payoutId TEXT UNIQUE,
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


# -------------------------
# ROUTES
# -------------------------
@app.route("/")
def home():
    return "PawaPay Deposit + Payout Server ✅"


# -------------------------
# DEPOSIT (Investors send money in)
# -------------------------
@app.route("/initiate-payment", methods=["POST"])
def initiate_payment():
    try:
        data = request.json
        phone = data.get("phone")
        amount = data.get("amount")
        correspondent = data.get("correspondent", "MTN_MOMO_ZMB")
        currency = data.get("currency", "ZMW")

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
            "statementDescription": "StudyCraftPay",
            "metadata": [
                {"fieldName": "orderId", "fieldValue": "ORD-" + deposit_id},
                {"fieldName": "customerId", "fieldValue": phone, "isPII": True}
            ]
        }

        headers = {
            "Authorization": f"Bearer {API_TOKEN}",
            "Content-Type": "application/json"
        }

        resp = requests.post(PAWAPAY_DEPOSITS_URL, json=payload, headers=headers)
        result = resp.json()

        # Save to DB
        db = get_db()
        cur = db.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO transactions
            (depositId, status, amount, currency, phoneNumber, provider,
             providerTransactionId, failureCode, failureMessage, metadata, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            deposit_id,
            result.get("status", "PENDING"),
            float(amount),
            currency,
            phone,
            correspondent,
            None,
            None,
            None,
            json.dumps(payload.get("metadata")),
            datetime.utcnow().isoformat()
        ))
        db.commit()

        return jsonify({"depositId": deposit_id, **result}), 200

    except Exception as e:
        logger.exception("Error initiating payment")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/callback/deposit', methods=['POST'])
def deposit_callback():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data"}), 400

        deposit_id = data.get("depositId")
        status = data.get("status")
        amount = data.get("amount")
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
# PAYOUT (Loans → Borrowers)
# -------------------------
@app.route("/initiate-payout", methods=["POST"])
def initiate_payout():
    try:
        data = request.json
        phone = data.get("phone")
        amount = data.get("amount")
        provider = data.get("provider", "MTN_MOMO_ZMB")
        currency = data.get("currency", "ZMW")
        customer_message = data.get("customerMessage", "Loan disbursement")

        if not phone or not amount:
            return jsonify({"error": "Missing phone or amount"}), 400

        payout_id = str(uuid.uuid4())

        payload = {
            "payoutId": payout_id,
            "recipient": {
                "type": "MMO",
                "accountDetails": {
                    "phoneNumber": phone,
                    "provider": provider
                }
            },
            "customerMessage": customer_message,
            "amount": str(amount),
            "currency": currency,
            "metadata": [
                {"orderId": "ORD-" + payout_id},
                {"customerPhone": phone, "isPII": True}
            ]
        }

        headers = {
            "Authorization": f"Bearer {API_TOKEN}",
            "Content-Type": "application/json"
        }

        resp = requests.post(PAWAPAY_PAYOUTS_URL, json=payload, headers=headers)
        result = resp.json()

        # Save payout to DB
        db = get_db()
        cur = db.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO payouts
            (payoutId, status, amount, currency, phoneNumber, provider,
             providerTransactionId, failureCode, failureMessage, metadata, sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            payout_id,
            result.get("status", "PENDING"),
            float(amount),
            currency,
            phone,
            provider,
            None,
            None,
            None,
            json.dumps(payload.get("metadata")),
            datetime.utcnow().isoformat()
        ))
        db.commit()

        return jsonify({"payoutId": payout_id, **result}), 200

    except Exception:
        logger.exception("Error initiating payout")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/callback/payout', methods=['POST'])
def payout_callback():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data"}), 400

        payout_id = data.get("payoutId")
        status = data.get("status")
        amount = data.get("amount")
        currency = data.get("currency")
        recipient = data.get("recipient", {})
        account_details = recipient.get("accountDetails", {})
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
            INSERT OR REPLACE INTO payouts
            (payoutId, status, amount, currency, phoneNumber, provider, providerTransactionId, failureCode, failureMessage, metadata, sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            payout_id,
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

    except Exception:
        logger.exception("Error handling payout callback")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/api/loans", methods=["GET"])
def get_all_loans():
    """
    Admin endpoint: Get all loans for disbursement dashboard.
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT * FROM loans")
        rows = cur.fetchall()

        loans = []
        for row in rows:
            loans.append({
                "loan_id": row["loan_id"],
                "user_id": row["user_id"],
                "amount": row["amount"],
                "status": row["status"],
                "requested_at": row["requested_at"]
            })

        return jsonify({"loans": loans}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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
# MAIN
# -------------------------
if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)



