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
    """Return latest status of a deposit."""
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










#SERVER 6 WORKED WELL
# app.py
# from flask import Flask, request, jsonify, g
# import os
# import logging
# import sqlite3
# import json
# from datetime import datetime

# # Path to sqlite DB file (created next to this script)
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

# @app.route('/')
# def home():
#     return "PawaPay Callback Receiver is running ✅"

# @app.route('/callback/deposit', methods=['POST'])
# def deposit_callback():
#     """
#     Receive deposit callbacks from PawaPay.
#     Expected JSON fields (common): depositId, status, amount, currency, payer -> accountDetails -> phoneNumber/provider,
#     providerTransactionId, failureReason (object), metadata (optional)
#     """
#     try:
#         data = request.get_json()
#         if not data:
#             logger.warning("Deposit callback received with no JSON body")
#             return jsonify({"error": "No JSON data"}), 400

#         # Extract PawaPay fields (use PawaPay terminology)
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

#         # Persist to sqlite DB
#         db = get_db()
#         cur = db.cursor()
#         cur.execute("""
#             INSERT OR REPLACE INTO transactions 
#             (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId, failureCode, failureMessage, metadata, received_at)
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#         """, (
#             deposit_id,
#             status,
#             float(amount) if amount is not None else None,
#             currency,
#             phone_number,
#             provider,
#             provider_txn,
#             failure_code,
#             failure_message,
#             json.dumps(metadata) if metadata is not None else None,
#             datetime.utcnow().isoformat()
#         ))
#         db.commit()

#         # Return 200 OK so PawaPay knows callback delivered
#         return jsonify({"received": True}), 200

#     except Exception as e:
#         logger.exception("Error handling deposit callback")
#         return jsonify({"error": "Internal server error"}), 500

# @app.route('/transactions/<deposit_id>', methods=['GET'])
# def get_transaction(deposit_id):
#     """Simple lookup to inspect stored transaction by depositId."""
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

# if __name__ == '__main__':
#     init_db()
#     port = int(os.environ.get("PORT", 5000))
#     app.run(host="0.0.0.0", port=port)

#SERVER 5
# from flask import Flask, request, jsonify
# import os
# import logging

# app = Flask(__name__)
# logging.basicConfig(level=logging.INFO)

# @app.route('/')
# def home():
#     return "PawaPay Callback Receiver is running ✅"

# @app.route('/callback/deposit', methods=['POST'])
# def deposit_callback():
#     """
#     Endpoint to receive deposit callbacks from PawaPay
#     with transaction status (COMPLETED, FAILED, etc).
#     """
#     try:
#         data = request.get_json()
#         if not data:
#             app.logger.warning("Deposit callback received with no JSON body")
#             return jsonify({"error": "No JSON data"}), 400

#         deposit_id = data.get("depositId")
#         status = data.get("status")
#         amount = data.get("amount")
#         currency = data.get("currency")
#         payer = data.get("payer", {})
#         phone_number = payer.get("accountDetails", {}).get("phoneNumber")
#         provider = payer.get("accountDetails", {}).get("provider")
#         failure_reason = data.get("failureReason", {})

#         app.logger.info(
#             f"Deposit Callback → depositId={deposit_id}, status={status}, "
#             f"amount={amount} {currency}, phone={phone_number}, provider={provider}, "
#             f"failureReason={failure_reason}"
#         )

#         # TODO: Save this data in a database if needed

#         return jsonify({"received": True}), 200

#     except Exception as e:
#         app.logger.error(f"Error handling callback: {e}", exc_info=True)
#         return jsonify({"error": "Internal server error"}), 500


# if __name__ == '__main__':
#     port = int(os.environ.get("PORT", 5000))
#     app.run(host="0.0.0.0", port=port)

# # SERVER 4
# # app.py
# from flask import Flask, request, jsonify
# import os
# from datetime import datetime

# app = Flask(__name__)

# # ===========================
# # Root route (browser test)
# # ===========================
# @app.route('/')
# def home():
#     return "✅ Callback server is running!"

# # ===========================
# # Callback route (for PawaPay)
# # ===========================
# @app.route('/callback', methods=['POST'])
# def callback():
#     data = request.json

#     if not data:
#         # No JSON received
#         return jsonify({"status": "error", "message": "No JSON data received"}), 400

#     # Optional: validate essential fields from PawaPay
#     required_fields = ["transaction_id", "status", "amount", "currency"]
#     missing_fields = [field for field in required_fields if field not in data]

#     if missing_fields:
#         return jsonify({
#             "status": "error",
#             "message": f"Missing fields: {', '.join(missing_fields)}"
#         }), 400

#     # Log received callback in Render dashboard
#     print("Callback received:", data)

#     # Optional: save callback to file
#     log_file = "callback_log.txt"
#     with open(log_file, "a") as f:
#         f.write(f"{datetime.utcnow()} - {data}\n")

#     # Respond to PawaPay to acknowledge callback
#     return jsonify({"status": "success", "message": "Callback received"}), 200

# # ===========================
# # Run server
# # ===========================
# if __name__ == '__main__':
#     # Use Render-assigned port
#     port = int(os.environ.get("PORT", 5000))
#     app.run(host='0.0.0.0', port=port)


#SERVER 3
# from flask import Flask, request, jsonify

# app = Flask(__name__)

# @app.route('/callback', methods=['POST'])
# def callback():
#     try:
#         data = request.get_json()
#         if not data:
#             return jsonify({"error": "Invalid JSON"}), 400

#         transaction_id = data.get("transactionId")
#         status = data.get("status")
#         failure = data.get("failureReason", {})
#         failure_code = failure.get("failureCode")
#         failure_msg = failure.get("failureMessage")

#         print(f"Transaction ID: {transaction_id}, Status: {status}")

#         if status == "SUCCESSFUL":
#             print("✅ Payment successful. Unlock the service for the user.")
#         elif status == "FAILED":
#             print(f"❌ Payment failed: {failure_code} - {failure_msg}")
#         elif status == "PENDING":
#             print("⏳ Payment is pending. Wait for final update.")
#         else:
#             print(f"⚠️ Unknown status received: {status}")

#         return jsonify({"message": "Callback received", "status": status}), 200

#     except Exception as e:
#         print("Error handling callback:", str(e))
#         return jsonify({"error": "Server error"}), 500

# if __name__ == '__main__':
#     app.run(host="0.0.0.0", port=5000)


#SERVER 2
# from flask import Flask, request, jsonify

# app = Flask(__name__)

# @app.route('/callback', methods=['POST'])
# def callback():
#     try:
#         data = request.get_json()

#         if not data:
#             return jsonify({"error": "Invalid JSON"}), 400

#         # Extract details from the callback
#         transaction_id = data.get("transaction_id")
#         status = data.get("status")

#         # Log transaction details
#         print(f"Transaction ID: {transaction_id}, Status: {status}")

#         # Handle different payment statuses
#         if status == "SUCCESS":
#             print("✅ Payment successful. Unlock the service for the user.")
#         elif status == "FAILED":
#             print("❌ Payment failed. Inform the user or retry.")
#         elif status == "PENDING":
#             print("⏳ Payment is pending. Wait for final update.")
#         else:
#             print(f"⚠️ Unknown status received: {status}")

#         # 🔥 FIXED: return the actual status instead of hardcoding "success"
#         return jsonify({"message": "Callback received", "status": status}), 200

#     except Exception as e:
#         print("Error handling callback:", str(e))
#         return jsonify({"error": "Server error"}), 500

# if __name__ == '__main__':
#     app.run(host="0.0.0.0", port=5000)



# SERVER 1
# from flask import Flask, request, jsonify

# app = Flask(__name__)

# # Example callback route
# @app.route('/callback', methods=['POST'])
# def callback():
#     data = request.json
#     print("Callback received:", data)  # log in Render dashboard
#     return jsonify({"status": "success", "message": "Callback received"}), 200

# @app.route('/')
# def home():
#     return "Callback server is running 🚀"

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=5000)
