from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, g
import os, logging, sqlite3, json, requests, uuid
from datetime import datetime

# -------------------------
# API CONFIGURATION
# -------------------------
API_MODE = os.getenv("API_MODE", "sandbox")
SANDBOX_API_TOKEN = os.getenv("SANDBOX_API_TOKEN")
LIVE_API_TOKEN = os.getenv("LIVE_API_TOKEN")

API_TOKEN = LIVE_API_TOKEN if API_MODE == "live" else SANDBOX_API_TOKEN
PAWAPAY_URL = (
    "https://api.pawapay.io/deposits"
    if API_MODE == "live"
    else "https://api.sandbox.pawapay.io/deposits"
)

PAWAPAY_PAYOUT_URL = (
    "https://api.pawapay.io/v2/payouts"
    if API_MODE == "live"
    else "https://api.sandbox.pawapay.io/v2/payouts"
)

# -------------------------
# DATABASE
# -------------------------
DATABASE = os.path.join(os.path.dirname(__file__), "transactions.db")
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_db():
    """
    Create the transactions table if missing and safely add any missing columns.
    Also run a small backfill to populate 'type' and 'user_id' from metadata where possible.
    """
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    # Create table with the full set of columns we want to support
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
            received_at TEXT,
            updated_at TEXT,
            type TEXT DEFAULT 'payment',
            user_id TEXT
        )
    """)
    conn.commit()

    # Inspect columns that actually exist and add any missing ones.
    cur.execute("PRAGMA table_info(transactions)")
    existing_cols = [r[1] for r in cur.fetchall()]

    # Add missing columns one-by-one in a safe way
    needed = {
        "phoneNumber": "TEXT",
        "metadata": "TEXT",
        "updated_at": "TEXT",
        "type": "TEXT DEFAULT 'payment'",
        "user_id": "TEXT"
    }
    for col, coltype in needed.items():
        if col not in existing_cols:
            try:
                cur.execute(f"ALTER TABLE transactions ADD COLUMN {col} {coltype}")
                logger.info("Added column %s to transactions table", col)
            except sqlite3.OperationalError:
                # race or already present
                logger.warning("Could not add column %s (may already exist)", col)

    conn.commit()

    # Backfill 'type' and 'user_id' from metadata where possible
    try:
        cur.execute("SELECT depositId, metadata, type, user_id FROM transactions")
        rows = cur.fetchall()
        updates = []
        for deposit_id, metadata, cur_type, cur_user in rows:
            new_type = cur_type
            new_user = cur_user
            changed = False
            if metadata:
                try:
                    meta_obj = json.loads(metadata)
                except Exception:
                    meta_obj = None

                if isinstance(meta_obj, list):
                    for entry in meta_obj:
                        if not isinstance(entry, dict):
                            continue
                        fn = str(entry.get("fieldName") or "").lower()
                        fv = entry.get("fieldValue")
                        if fn == "userid" and fv and not new_user:
                            new_user = str(fv)
                            changed = True
                        if fn == "purpose" and isinstance(fv, str) and fv.lower() == "investment":
                            if new_type != "investment":
                                new_type = "investment"
                                changed = True
                elif isinstance(meta_obj, dict):
                    if "userId" in meta_obj and not new_user:
                        new_user = str(meta_obj.get("userId"))
                        changed = True
                    purpose = meta_obj.get("purpose")
                    if isinstance(purpose, str) and purpose.lower() == "investment":
                        if new_type != "investment":
                            new_type = "investment"
                            changed = True

            # default type to 'payment' if None
            if new_type is None:
                new_type = "payment"

            if changed or (cur_user is None and new_user is not None) or (cur_type is None and new_type):
                updates.append((new_user, new_type, deposit_id))

        for u, t, dep in updates:
            cur.execute("UPDATE transactions SET user_id = ?, type = ? WHERE depositId = ?", (u, t, dep))
        if updates:
            conn.commit()
            logger.info("Backfilled %d transactions with user_id/type from metadata.", len(updates))
    except Exception:
        logger.exception("Error during migration/backfill pass")

    conn.close()


with app.app_context():
    init_db()


def get_db():
    """
    Return a DB connection scoped to the Flask request context.
    Row factory is sqlite3.Row for dict-like rows.
    """
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db





# -------------------------
# LOANS TABLE INIT
# -------------------------
def init_loans_table():
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS loans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        loanId TEXT UNIQUE,
        user_id TEXT,
        phone TEXT,                -- 🔹 NEW: borrower's phone number for payouts
        investment_id TEXT,        -- 🔹 links this loan to an investment
        amount REAL,
        interest REAL,
        status TEXT,               -- PENDING, APPROVED, DISAPPROVED, PAID
        expected_return_date TEXT,
        created_at TEXT,
        approved_by TEXT
    )
    """)

    conn.commit()
    conn.close()

# def init_loans_table():
#     conn = sqlite3.connect(DATABASE)
#     cur = conn.cursor()
#     cur.execute("""
#     CREATE TABLE IF NOT EXISTS loans (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         loanId TEXT UNIQUE,
#         user_id TEXT,
#         investment_id TEXT,       -- 🔹 NEW: links this loan to an investment
#         amount REAL,
#         interest REAL,
#         status TEXT,              -- PENDING, APPROVED, DISAPPROVED, PAID
#         expected_return_date TEXT,
#         created_at TEXT,
#         approved_by TEXT
#         )
#     """)

#     conn.commit()
#     conn.close()

with app.app_context():
    init_loans_table()


# -------------------------
# REQUEST A LOAN
# # -------------------------

@app.route("/api/loans/request", methods=["POST"])
def request_loan():
    data = request.json
    user_id = data.get("user_id")
    investment_id = data.get("investment_id")
    amount = data.get("amount")
    interest = data.get("interest", 5)
    expected_return_date = data.get("expected_return_date")
    phone = data.get("phone")  # <- NEW

    if not user_id or not amount or not expected_return_date or not investment_id or not phone:
        return jsonify({"error": "Missing required fields"}), 400

    loanId = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat()

    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO loans (loanId, user_id, investment_id, amount, interest, status, expected_return_date, created_at, phone)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (loanId, user_id, investment_id, amount, interest, "PENDING", expected_return_date, created_at, phone))
    conn.commit()
    conn.close()

    return jsonify({"loanId": loanId, "status": "PENDING"}), 200

# @app.route("/api/loans/request", methods=["POST"])
# def request_loan():
#     data = request.json
#     user_id = data.get("user_id")
#     investment_id = data.get("investment_id")   # 🔹 NEW
#     amount = data.get("amount")
#     interest = data.get("interest", 5)
#     expected_return_date = data.get("expected_return_date")

#     if not user_id or not amount or not expected_return_date or not investment_id:
#         return jsonify({"error": "Missing required fields"}), 400

#     loanId = str(uuid.uuid4())
#     created_at = datetime.utcnow().isoformat()

#     conn = sqlite3.connect(DATABASE)
#     cur = conn.cursor()
#     cur.execute("""
#         INSERT INTO loans (loanId, user_id, investment_id, amount, interest, status, expected_return_date, created_at)
#         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#     """, (loanId, user_id, investment_id, amount, interest, "PENDING", expected_return_date, created_at))
#     conn.commit()
#     conn.close()

#     return jsonify({"loanId": loanId, "status": "PENDING"}), 200


# -------------------------
# LIST PENDING LOANS (ADMIN VIEW)
# -------------------------
@app.route("/api/loans/pending", methods=["GET"])
def pending_loans():
    db = get_db()
    rows = db.execute("SELECT * FROM loans WHERE status='PENDING' ORDER BY created_at DESC").fetchall()
    results = [dict(row) for row in rows]
    return jsonify(results), 200


# -------------------------
# APPROVE LOAN
# -------------------------
@app.route("/api/loans/approve/<loan_id>", methods=["POST"])
def approve_loan(loan_id):
    admin_id = request.json.get("admin_id", "admin_default")
    db = get_db()
    db.execute("UPDATE loans SET status='APPROVED', approved_by=? WHERE loanId=?", (admin_id, loan_id))
    db.commit()

    # update investor transaction status to LOANED_OUT if exists
    loan = db.execute("SELECT * FROM loans WHERE loanId=?", (loan_id,)).fetchone()
    if loan:
        db.execute("""
            UPDATE transactions
            SET status='LOANED_OUT',
                updated_at=?,
                metadata=COALESCE(metadata, ''),
                failureMessage='Loan Approved',
                failureCode='LOAN'
            WHERE user_id=? AND type='investment'
        """, (datetime.utcnow().isoformat(), loan["user_id"]))
        db.commit()

    return jsonify({"message": "Loan approved"}), 200


# -------------------------
# DISAPPROVE LOAN
# -------------------------
@app.route("/api/loans/disapprove/<loan_id>", methods=["POST"])
def disapprove_loan(loan_id):
    db = get_db()
    db.execute("UPDATE loans SET status='DISAPPROVED' WHERE loanId=?", (loan_id,))
    db.commit()
    return jsonify({"message": "Loan disapproved"}), 200


# -------------------------
# INVESTOR LOANS VIEW
# -------------------------
@app.route("/api/loans/user/<user_id>", methods=["GET"])
def user_loans(user_id):
    db = get_db()
    rows = db.execute("SELECT * FROM loans WHERE user_id=? ORDER BY created_at DESC", (user_id,)).fetchall()
    results = [dict(row) for row in rows]
    return jsonify(results), 200







@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


# -------------------------
# HEALTH
# -------------------------
@app.route("/")
def home():
    return f"PawaPay Callback Receiver running ✅ (API_MODE={API_MODE})"


# -------------------------
# ORIGINAL PAYMENT ENDPOINTS
# -------------------------
@app.route("/initiate-payment", methods=["POST"])
def initiate_payment():
    try:
        data = request.json
        phone = data.get("phone")
        amount = data.get("amount")
        if not phone or not amount:
            return jsonify({"error": "Missing phone or amount"}), 400

        deposit_id = str(uuid.uuid4())
        customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        payload = {
            "depositId": deposit_id,
            "amount": str(amount),
            "currency": "ZMW",
            "correspondent": "MTN_MOMO_ZMB",
            "payer": {"type": "MSISDN", "address": {"value": phone}},
            "customerTimestamp": customer_ts,
            "statementDescription": "StudyCraftPay",
            "metadata": [
                {"fieldName": "orderId", "fieldValue": "ORD-" + deposit_id},
                {"fieldName": "customerId", "fieldValue": phone, "isPII": True},
            ],
        }

        headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
        resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)
        result = {}
        try:
            result = resp.json()
        except Exception:
            logger.warning("Non-JSON response from PawaPay for initiate-payment: %s", resp.text)

        db = get_db()
        db.execute("""
            INSERT OR REPLACE INTO transactions
            (depositId,status,amount,currency,phoneNumber,provider,
             providerTransactionId,failureCode,failureMessage,metadata,received_at,type,user_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            deposit_id,
            result.get("status", "PENDING"),
            float(amount),
            "ZMW",
            phone,
            None, None, None, None,
            json.dumps(payload["metadata"]),
            datetime.utcnow().isoformat(),
            "payment",
            None
        ))
        db.commit()
        logger.info("initiate-payment: inserted depositId=%s status=%s", deposit_id, result.get("status", "PENDING"))
        return jsonify({"depositId": deposit_id, **result}), 200

    except Exception:
        logger.exception("Payment initiation error")
        return jsonify({"error": "Internal server error"}), 500


# # TEST 1 callback 1....................
# @app.route("/callback/deposit", methods=["POST"])
# def deposit_callback():
#     try:
#         data = request.get_json(force=True)
#         deposit_id = data.get("depositId")
#         if not deposit_id:
#             return jsonify({"error": "Missing depositId"}), 400

#         status = data.get("status")
#         amount = data.get("amount")
#         currency = data.get("currency")
#         payer_phone = data.get("payer", {}).get("accountDetails", {}).get("phoneNumber")
#         provider = data.get("payer", {}).get("accountDetails", {}).get("provider")
#         provider_txn = data.get("providerTransactionId")
#         failure_code = data.get("failureReason", {}).get("failureCode")
#         failure_message = data.get("failureReason", {}).get("failureMessage")
#         metadata_obj = data.get("metadata")

#         user_id = None
#         loan_id = None
#         if metadata_obj:
#             if isinstance(metadata_obj, dict):
#                 user_id = metadata_obj.get("userId")
#                 loan_id = metadata_obj.get("loanId")
#             elif isinstance(metadata_obj, list):
#                 for entry in metadata_obj:
#                     if isinstance(entry, dict):
#                         if entry.get("fieldName") == "userId":
#                             user_id = entry.get("fieldValue")
#                         if entry.get("fieldName") == "loanId":
#                             loan_id = entry.get("fieldValue")

#         db = get_db()
#         existing = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
#         now_iso = datetime.utcnow().isoformat()
#         metadata_str = json.dumps(metadata_obj) if metadata_obj else None

#         if existing:
#             db.execute("""
#                 UPDATE transactions
#                 SET
#                     status = COALESCE(?, status),
#                     amount = COALESCE(?, amount),
#                     currency = COALESCE(?, currency),
#                     phoneNumber = COALESCE(?, phoneNumber),
#                     provider = COALESCE(?, provider),
#                     providerTransactionId = COALESCE(?, providerTransactionId),
#                     failureCode = COALESCE(?, failureCode),
#                     failureMessage = COALESCE(?, failureMessage),
#                     metadata = COALESCE(?, metadata),
#                     updated_at = ?,
#                     user_id = COALESCE(?, user_id)
#                 WHERE depositId = ?
#             """, (
#                 status,
#                 float(amount) if amount else None,
#                 currency,
#                 payer_phone,
#                 provider,
#                 provider_txn,
#                 failure_code,
#                 failure_message,
#                 metadata_str,
#                 now_iso,
#                 user_id,
#                 deposit_id
#             ))
#         else:
#             # Determine type: deposit or payout
#             txn_type = "payout" if loan_id else "payment"
#             db.execute("""
#                 INSERT INTO transactions
#                 (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId,
#                  failureCode, failureMessage, metadata, received_at, updated_at, type, user_id)
#                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#             """, (
#                 deposit_id,
#                 status,
#                 float(amount) if amount else None,
#                 currency,
#                 payer_phone,
#                 provider,
#                 provider_txn,
#                 failure_code,
#                 failure_message,
#                 metadata_str,
#                 now_iso,
#                 now_iso,
#                 txn_type,
#                 user_id
#             ))

#         # If payout succeeded, update loan status
#         if loan_id and status in ("COMPLETED", "SUCCESS", "PAYMENT_COMPLETED"):
#             db.execute("UPDATE loans SET status=? WHERE loanId=?", (status, loan_id))

#         db.commit()
#         return jsonify({"received": True}), 200

#     except Exception:
#         logger.exception("Callback error")
#         return jsonify({"error": "Internal server error"}), 500

# # -------------------------
# # CALLBACK RECEIVER (upsert-safe for deposits and payouts)
# # -------------------------
# @app.route("/callback/deposit", methods=["POST"])
# def deposit_callback():
#     """
#     Handle both deposit and payout callbacks from PawaPay.
#     Deposits update the transactions table.
#     Payouts update the loans table (status & payoutId).
#     """
#     try:
#         data = request.get_json(force=True)

#         # -------------------------
#         # HANDLE PAYOUT CALLBACK
#         # -------------------------
#         payout_id = data.get("payoutId")
#         if payout_id:
#             status = data.get("status")
#             amount = data.get("amount")
#             currency = data.get("currency")
#             recipient_phone = data.get("recipient", {}).get("accountDetails", {}).get("phoneNumber")
#             provider = data.get("recipient", {}).get("accountDetails", {}).get("provider")
#             metadata_obj = data.get("metadata")

#             # parse user_id and loan_id from metadata if present
#             loan_id = None
#             user_id = None
#             if metadata_obj and isinstance(metadata_obj, list):
#                 for entry in metadata_obj:
#                     if isinstance(entry, dict):
#                         if entry.get("fieldName","").lower() == "loanid":
#                             loan_id = entry.get("fieldValue")
#                         elif entry.get("fieldName","").lower() == "userid":
#                             user_id = entry.get("fieldValue")

#             db = get_db()
#             now_iso = datetime.utcnow().isoformat()
#             metadata_str = json.dumps(metadata_obj) if metadata_obj else None

#             if loan_id:
#                 existing = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
#                 if existing:
#                     # Update existing loan row with new payout status
#                     db.execute("""
#                         UPDATE loans
#                         SET
#                             status = COALESCE(?, status),
#                             payoutId = COALESCE(?, payoutId),
#                             updated_at = ?,
#                             user_id = COALESCE(?, user_id)
#                         WHERE loanId = ?
#                     """, (
#                         status,
#                         payout_id,
#                         now_iso,
#                         user_id,
#                         loan_id
#                     ))
#                     db.commit()
#                     logger.info("Payout callback: updated loanId=%s status=%s user_id=%s", loan_id, status, user_id)
#                 else:
#                     # Insert new loan row if it doesn't exist (rare case)
#                     db.execute("""
#                         INSERT INTO loans
#                         (loanId, status, payoutId, amount, currency, phoneNumber, provider, metadata, received_at, updated_at, user_id)
#                         VALUES (?,?,?,?,?,?,?,?,?,?,?)
#                     """, (
#                         loan_id,
#                         status,
#                         payout_id,
#                         float(amount) if amount else None,
#                         currency,
#                         recipient_phone,
#                         provider,
#                         metadata_str,
#                         now_iso,
#                         now_iso,
#                         user_id
#                     ))
#                     db.commit()
#                     logger.info("Payout callback: inserted loanId=%s status=%s user_id=%s", loan_id, status, user_id)

#             return jsonify({"received": True, "payoutId": payout_id}), 200

#         # -------------------------
#         # ORIGINAL DEPOSIT CALLBACK LOGIC
#         # -------------------------
#         deposit_id = data.get("depositId")
#         if not deposit_id:
#             return jsonify({"error": "Missing depositId"}), 400

#         # extract useful fields
#         status = data.get("status")
#         amount = data.get("amount")
#         currency = data.get("currency")
#         payer_phone = data.get("payer", {}).get("accountDetails", {}).get("phoneNumber")
#         provider = data.get("payer", {}).get("accountDetails", {}).get("provider")
#         provider_txn = data.get("providerTransactionId")
#         failure_code = data.get("failureReason", {}).get("failureCode")
#         failure_message = data.get("failureReason", {}).get("failureMessage")
#         metadata_obj = data.get("metadata")

#         # parse user_id from metadata if present
#         user_id = None
#         if metadata_obj:
#             if isinstance(metadata_obj, dict):
#                 user_id = metadata_obj.get("userId") or metadata_obj.get("userid")
#             elif isinstance(metadata_obj, list):
#                 for entry in metadata_obj:
#                     if isinstance(entry, dict) and str(entry.get("fieldName", "")).lower() == "userid":
#                         user_id = entry.get("fieldValue")
#                         break

#         db = get_db()
#         existing = db.execute("SELECT * FROM transactions WHERE depositId = ?", (deposit_id,)).fetchone()
#         now_iso = datetime.utcnow().isoformat()
#         metadata_str = json.dumps(metadata_obj) if metadata_obj is not None else None

#         if existing:
#             db.execute("""
#                 UPDATE transactions
#                 SET
#                     status = COALESCE(?, status),
#                     amount = COALESCE(?, amount),
#                     currency = COALESCE(?, currency),
#                     phoneNumber = COALESCE(?, phoneNumber),
#                     provider = COALESCE(?, provider),
#                     providerTransactionId = COALESCE(?, providerTransactionId),
#                     failureCode = COALESCE(?, failureCode),
#                     failureMessage = COALESCE(?, failureMessage),
#                     metadata = COALESCE(?, metadata),
#                     updated_at = ?,
#                     user_id = COALESCE(?, user_id)
#                 WHERE depositId = ?
#             """, (
#                 status,
#                 float(amount) if amount is not None else None,
#                 currency,
#                 payer_phone,
#                 provider,
#                 provider_txn,
#                 failure_code,
#                 failure_message,
#                 metadata_str,
#                 now_iso,
#                 user_id,
#                 deposit_id
#             ))
#             db.commit()
#             logger.info("deposit_callback: updated depositId=%s status=%s user_id=%s", deposit_id, status, user_id)
#         else:
#             db.execute("""
#                 INSERT INTO transactions
#                 (depositId,status,amount,currency,phoneNumber,provider,providerTransactionId,
#                  failureCode,failureMessage,metadata,received_at,updated_at,type,user_id)
#                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
#             """, (
#                 deposit_id,
#                 status,
#                 float(amount) if amount is not None else None,
#                 currency,
#                 payer_phone,
#                 provider,
#                 provider_txn,
#                 failure_code,
#                 failure_message,
#                 metadata_str,
#                 now_iso,
#                 now_iso,
#                 "payment",
#                 user_id
#             ))
#             db.commit()
#             logger.info("deposit_callback: inserted depositId=%s status=%s user_id=%s", deposit_id, status, user_id)

#         return jsonify({"received": True}), 200

#     except Exception:
#         logger.exception("Callback error")
#         return jsonify({"error": "Internal server error"}), 500

#Test 2 callback 2
@app.route("/callback/deposit", methods=["POST"])
def deposit_callback():
    try:
        data = request.get_json(force=True)

        # Determine if deposit or payout
        deposit_id = data.get("depositId")
        payout_id = data.get("payoutId")

        if not deposit_id and not payout_id:
            return jsonify({"error": "Missing depositId/payoutId"}), 400

        txn_type = "payment" if deposit_id else "payout"
        txn_id = deposit_id or payout_id

        # Amount, status, currency
        status = data.get("status")
        amount = data.get("amount")
        currency = data.get("currency")

        # Phone & provider
        if txn_type == "payment":
            phone = data.get("payer", {}).get("accountDetails", {}).get("phoneNumber")
            provider = data.get("payer", {}).get("accountDetails", {}).get("provider")
        else:  # payout
            phone = data.get("recipient", {}).get("accountDetails", {}).get("phoneNumber")
            provider = data.get("recipient", {}).get("accountDetails", {}).get("provider")

        provider_txn = data.get("providerTransactionId")
        failure_code = data.get("failureReason", {}).get("failureCode")
        failure_message = data.get("failureReason", {}).get("failureMessage")
        metadata_obj = data.get("metadata")

        user_id = None
        loan_id = None
        if metadata_obj:
            if isinstance(metadata_obj, dict):
                user_id = metadata_obj.get("userId")
                loan_id = metadata_obj.get("loanId")
            elif isinstance(metadata_obj, list):
                for entry in metadata_obj:
                    if isinstance(entry, dict):
                        if entry.get("fieldName") == "userId":
                            user_id = entry.get("fieldValue")
                        if entry.get("fieldName") == "loanId":
                            loan_id = entry.get("fieldValue")

        db = get_db()
        existing = db.execute("SELECT * FROM transactions WHERE depositId=? OR depositId=?",
                              (deposit_id, payout_id)).fetchone()
        now_iso = datetime.utcnow().isoformat()
        metadata_str = json.dumps(metadata_obj) if metadata_obj else None

        if existing:
            db.execute("""
                UPDATE transactions
                SET
                    status = COALESCE(?, status),
                    amount = COALESCE(?, amount),
                    currency = COALESCE(?, currency),
                    phoneNumber = COALESCE(?, phoneNumber),
                    provider = COALESCE(?, provider),
                    providerTransactionId = COALESCE(?, providerTransactionId),
                    failureCode = COALESCE(?, failureCode),
                    failureMessage = COALESCE(?, failureMessage),
                    metadata = COALESCE(?, metadata),
                    updated_at = ?,
                    user_id = COALESCE(?, user_id)
                WHERE depositId = ? OR depositId = ?
            """, (
                status,
                float(amount) if amount else None,
                currency,
                phone,
                provider,
                provider_txn,
                failure_code,
                failure_message,
                metadata_str,
                now_iso,
                user_id,
                deposit_id,
                payout_id
            ))
        else:
            db.execute("""
                INSERT INTO transactions
                (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId,
                 failureCode, failureMessage, metadata, received_at, updated_at, type, user_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                txn_id,
                status,
                float(amount) if amount else None,
                currency,
                phone,
                provider,
                provider_txn,
                failure_code,
                failure_message,
                metadata_str,
                now_iso,
                now_iso,
                txn_type,
                user_id
            ))

        # Update loan if payout succeeded
        if txn_type == "payout" and loan_id and status in ("COMPLETED", "SUCCESS", "PAYMENT_COMPLETED"):
            db.execute("UPDATE loans SET status=? WHERE loanId=?", (status, loan_id))

        db.commit()
        return jsonify({"received": True}), 200

    except Exception:
        logger.exception("Callback error")
        return jsonify({"error": "Internal server error"}), 500



# # -------------------------
# # CALLBACK RECEIVER (upsert-safe)
# # -------------------------
# @app.route("/callback/deposit", methods=["POST"])
# def deposit_callback():
#     """
#     When PawaPay calls back, update existing transaction if present,
#     otherwise insert a new transaction. Preserve existing columns (type, user_id)
#     if they are not provided in callback payload.
#     """
#     try:
#         data = request.get_json(force=True)
#         deposit_id = data.get("depositId")
#         if not deposit_id:
#             return jsonify({"error": "Missing depositId"}), 400

#         # extract useful fields
#         status = data.get("status")
#         amount = data.get("amount")
#         currency = data.get("currency")
#         payer_phone = data.get("payer", {}).get("accountDetails", {}).get("phoneNumber")
#         provider = data.get("payer", {}).get("accountDetails", {}).get("provider")
#         provider_txn = data.get("providerTransactionId")
#         failure_code = data.get("failureReason", {}).get("failureCode")
#         failure_message = data.get("failureReason", {}).get("failureMessage")
#         metadata_obj = data.get("metadata")

#         # parse user_id from metadata if present (handles both dict and list-of-dicts)
#         user_id = None
#         if metadata_obj:
#             if isinstance(metadata_obj, dict):
#                 user_id = metadata_obj.get("userId") or metadata_obj.get("userid")
#             elif isinstance(metadata_obj, list):
#                 for entry in metadata_obj:
#                     if isinstance(entry, dict) and str(entry.get("fieldName", "")).lower() == "userid":
#                         user_id = entry.get("fieldValue")
#                         break

#         db = get_db()

#         # Check if we already have a record for this depositId
#         existing = db.execute("SELECT * FROM transactions WHERE depositId = ?", (deposit_id,)).fetchone()

#         now_iso = datetime.utcnow().isoformat()
#         metadata_str = json.dumps(metadata_obj) if metadata_obj is not None else None

#         if existing:
#             # Update existing record but preserve existing user_id/type/phoneNumber if callback doesn't provide them
#             db.execute("""
#                 UPDATE transactions
#                 SET
#                     status = COALESCE(?, status),
#                     amount = COALESCE(?, amount),
#                     currency = COALESCE(?, currency),
#                     phoneNumber = COALESCE(?, phoneNumber),
#                     provider = COALESCE(?, provider),
#                     providerTransactionId = COALESCE(?, providerTransactionId),
#                     failureCode = COALESCE(?, failureCode),
#                     failureMessage = COALESCE(?, failureMessage),
#                     metadata = COALESCE(?, metadata),
#                     updated_at = ?,
#                     user_id = COALESCE(?, user_id)
#                 WHERE depositId = ?
#             """, (
#                 status,
#                 float(amount) if amount is not None else None,
#                 currency,
#                 payer_phone,
#                 provider,
#                 provider_txn,
#                 failure_code,
#                 failure_message,
#                 metadata_str,
#                 now_iso,
#                 user_id,
#                 deposit_id
#             ))
#             db.commit()
#             logger.info("deposit_callback: updated depositId=%s status=%s user_id=%s", deposit_id, status, user_id)
#         else:
#             # Insert new record (callback arrived before initiate or inserted separately on remote)
#             db.execute("""
#                 INSERT INTO transactions
#                 (depositId,status,amount,currency,phoneNumber,provider,providerTransactionId,
#                  failureCode,failureMessage,metadata,received_at,updated_at,type,user_id)
#                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
#             """, (
#                 deposit_id,
#                 status,
#                 float(amount) if amount is not None else None,
#                 currency,
#                 payer_phone,
#                 provider,
#                 provider_txn,
#                 failure_code,
#                 failure_message,
#                 metadata_str,
#                 now_iso,
#                 now_iso,
#                 "payment",
#                 user_id
#             ))
#             db.commit()
#             logger.info("deposit_callback: inserted depositId=%s status=%s user_id=%s", deposit_id, status, user_id)

#         return jsonify({"received": True}), 200

#     except Exception:
#         logger.exception("Callback error")
#         return jsonify({"error": "Internal server error"}), 500


# -------------------------
# DEPOSIT STATUS / TRANSACTION LOOKUP
# -------------------------
@app.route("/deposit_status/<deposit_id>")
def deposit_status(deposit_id):
    db = get_db()
    row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
    if not row:
        return jsonify({"status": None, "message": "Deposit not found"}), 404
    res = {k: row[k] for k in row.keys()}
    if res.get("metadata"):
        try:
            res["metadata"] = json.loads(res["metadata"])
        except:
            pass
    return jsonify(res), 200


@app.route("/transactions/<deposit_id>")
def get_transaction(deposit_id):
    db = get_db()
    row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
    if not row:
        return jsonify({"error": "not found"}), 404
    res = {k: row[k] for k in row.keys()}
    if res.get("metadata"):
        try:
            res["metadata"] = json.loads(res["metadata"])
        except:
            pass
    return jsonify(res), 200


# -------------------------
# INVESTMENT ENDPOINTS
# -------------------------
@app.route("/api/investments/initiate", methods=["POST"])
def initiate_investment():
    try:
        data = request.json or {}
        # Support both "phone" and "phoneNumber" keys from different clients
        phone = data.get("phone") or data.get("phoneNumber")
        amount = data.get("amount")
        correspondent = data.get("correspondent", "MTN_MOMO_ZMB")
        currency = data.get("currency", "ZMW")
        # prefer explicit user_id, but don't crash if missing
        user_id = data.get("user_id") or data.get("userId") or "unknown"

        if not phone or amount is None:
            return jsonify({"error": "Missing phone or amount"}), 400

        deposit_id = str(uuid.uuid4())
        customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        payload = {
            "depositId": deposit_id,
            "amount": str(amount),
            "currency": currency,
            "correspondent": correspondent,
            "payer": {"type": "MSISDN", "address": {"value": phone}},
            "customerTimestamp": customer_ts,
            "statementDescription": "Investment",
            "metadata": [
                {"fieldName": "purpose", "fieldValue": "investment"},
                {"fieldName": "userId", "fieldValue": str(user_id), "isPII": True},
            ],
        }

        headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
        resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)

        try:
            result = resp.json()
        except Exception:
            logger.error("PawaPay response not JSON: %s", resp.text)
            return jsonify({"error": "Invalid response from PawaPay"}), 502

        status = result.get("status", "PENDING")

        db = get_db()
        # Insert a new investment record (depositId will be unique)
        db.execute("""
            INSERT OR REPLACE INTO transactions
            (depositId,status,amount,currency,phoneNumber,provider,
             providerTransactionId,failureCode,failureMessage,metadata,received_at,type,user_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            deposit_id,
            status,
            float(amount),
            currency,
            phone,
            None, None, None, None,
            json.dumps(payload["metadata"]),
            datetime.utcnow().isoformat(),
            "investment",
            user_id
        ))
        db.commit()
        logger.info("initiate_investment: inserted depositId=%s user_id=%s amount=%s status=%s",
                    deposit_id, user_id, amount, status)

        return jsonify({"depositId": deposit_id, "status": status}), 200

    except Exception as e:
        logger.exception("Investment initiation error")
        return jsonify({"error": str(e)}), 500


@app.route("/api/investments/user/<user_id>", methods=["GET"])
def get_user_investments(user_id):
    """
    Return investments for a user. We select type='investment' and the exact user_id column.
    This returns a list of rows (may be empty).
    """
    db = get_db()
    rows = db.execute(
        "SELECT * FROM transactions WHERE type='investment' AND user_id=? ORDER BY received_at DESC",
        (user_id,)
    ).fetchall()

    results = []
    for row in rows:
        res = {k: row[k] for k in row.keys()}
        if res.get("metadata"):
            try:
                res["metadata"] = json.loads(res["metadata"])
            except:
                pass
        results.append(res)

    return jsonify(results), 200


# -------------------------
# SAMPLE INVESTMENT ROUTE (handy for testing)
# -------------------------
@app.route("/sample-investment", methods=["POST"])
def add_sample():
    """Add a test investment to verify DB works"""
    try:
        db = get_db()
        deposit_id = str(uuid.uuid4())
        payload_metadata = [{"fieldName": "purpose", "fieldValue": "investment"},
                            {"fieldName": "userId", "fieldValue": "user_1"}]
        received_at = datetime.utcnow().isoformat()
        db.execute("""
            INSERT INTO transactions
            (depositId,status,amount,currency,phoneNumber,metadata,received_at,type,user_id)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            deposit_id,
            "SUCCESS",
            1000.0,
            "ZMW",
            "0965123456",
            json.dumps(payload_metadata),
            received_at,
            "investment",
            "user_1"
        ))
        db.commit()
        logger.info("Added sample investment depositId=%s", deposit_id)
        return jsonify({"message":"Sample investment added","depositId":deposit_id}), 200
    except Exception as e:
        logger.exception("Failed to insert sample")
        return jsonify({"error": str(e)}), 500


# -------------------------
# OPTIONAL: debug route to see all transactions (helpful during testing)
# -------------------------
@app.route("/debug/transactions", methods=["GET"])
def debug_transactions():
    db = get_db()
    rows = db.execute("SELECT * FROM transactions ORDER BY received_at DESC").fetchall()
    results = []
    for row in rows:
        res = {k: row[k] for k in row.keys()}
        if res.get("metadata"):
            try:
                res["metadata"] = json.loads(res["metadata"])
            except:
                pass
        results.append(res)
    return jsonify(results), 200



#-----------------------------------
# GET PENDING REQUESTS
#----------------------------------

@app.route("/api/loans/pending", methods=["GET"])
def get_pending_loans():
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute("SELECT loanId, user_id, amount, interest, status, expected_return_date FROM loans WHERE status = ?", ("PENDING",))
    rows = cur.fetchall()
    conn.close()

    loans = []
    for row in rows:
        loans.append({
            "loanId": row[0],
            "user_id": row[1],
            "amount": row[2],
            "interest": row[3],
            "status": row[4],
            "expected_return_date": row[5]
        })

    return jsonify(loans), 200



# -------------------------
# DISBURSE LOAN (ADMIN ACTION)
# -------------------------

#Test ................1
# @app.route("/api/loans/disburse/<loan_id>", methods=["POST"])
# def disburse_loan(loan_id):
#     """
#     Admin approves and disburses a pending loan via PawaPay payout.
#     """
#     data = request.json or {}
#     admin_id = data.get("admin_id", "admin_default")

#     db = get_db()
#     loan = db.execute("SELECT * FROM loans WHERE loanId=?", (loan_id,)).fetchone()
#     if not loan:
#         return jsonify({"error": "Loan not found"}), 404
#     if loan["status"] != "PENDING":
#         return jsonify({"error": f"Loan already {loan['status']}"}), 400

#     # Get borrower phone from last investment transaction
#     t = db.execute(
#         "SELECT phoneNumber FROM transactions WHERE user_id=? AND type='payment' ORDER BY received_at DESC LIMIT 1",
#         (loan["user_id"],)
#     ).fetchone()
#     if not t:
#         return jsonify({"error": "No phone number found for user"}), 400
#     phone = t["phoneNumber"]

#     # Build payout request
#     payout_id = str(uuid.uuid4())
#     payload = {
#         "payoutId": payout_id,
#         "amount": str(loan["amount"]),
#         "currency": "ZMW",
#         "recipient": {
#             "type": "MMO",
#             "accountDetails": {
#                 "phoneNumber": phone,
#                 "provider": "MTN_MOMO_ZMB"
#             }
#         },
#         "customerMessage": f"Loan {loan_id} disbursement",
#         "metadata": [
#             {"fieldName": "loanId", "fieldValue": loan_id},
#             {"fieldName": "userId", "fieldValue": loan["user_id"]}
#         ]
#     }
#     headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

#     try:
#         resp = requests.post(PAWAPAY_PAYOUT_URL, json=payload, headers=headers, timeout=20)
#         payout_response = resp.json()
#         payout_status = payout_response.get("status", "PROCESSING")  # sandbox often returns PROCESSING
#     except Exception as e:
#         return jsonify({"error": f"Payout request failed: {str(e)}"}), 500

#     # Update loan row immediately with payout status
#     db.execute("UPDATE loans SET status=?, approved_by=? WHERE loanId=?", 
#                (payout_status, admin_id, loan_id))
#     db.commit()

#     # Insert a transaction record for payout (so callback updates will work)
#     now_iso = datetime.utcnow().isoformat()
#     db.execute("""
#         INSERT INTO transactions 
#         (depositId, status, amount, currency, phoneNumber, provider, metadata, received_at, updated_at, type, user_id)
#         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#     """, (
#         payout_id,
#         payout_status,
#         loan["amount"],
#         "ZMW",
#         phone,
#         "MTN_MOMO_ZMB",
#         json.dumps(payload.get("metadata")),
#         now_iso,
#         now_iso,
#         "payout",
#         loan["user_id"]
#     ))
#     db.commit()

#     return jsonify({
#         "loanId": loan_id,
#         "payoutId": payout_id,
#         "status": payout_status,
#         "payout_response": payout_response
#     }), 200

# Test 2...................
# @app.route("/api/loans/disburse/<loan_id>", methods=["POST"])
# def disburse_loan(loan_id):
#     data = request.json or {}
#     admin_id = data.get("admin_id", "admin_default")

#     db = get_db()
#     loan = db.execute("SELECT * FROM loans WHERE loanId=?", (loan_id,)).fetchone()
#     if not loan:
#         return jsonify({"error": "Loan not found"}), 404
#     if loan["status"] != "PENDING":
#         return jsonify({"error": f"Loan already {loan['status']}"}), 400

#     phone = loan["phone"] #loan.get("phone")
#     if not phone:
#         return jsonify({"error": f"No phone number found for loan {loan_id}"}), 400

#     # Build payout request
#     payout_id = str(uuid.uuid4())
#     payload = {
#         "payoutId": payout_id,
#         "recipient": {
#             "type": "MMO",
#             "accountDetails": {
#                 "phoneNumber": str(phone),
#                 "provider": "MTN_MOMO_ZMB"
#             }
#         },
#         "customerMessage": f"Loan {loan_id} disbursement",
#         "amount": str(loan["amount"]),
#         "currency": "ZMW",
#         "metadata": [
#             {"loanId": loan_id},
#             {"userId": loan["user_id"]}
#         ]
#     }
#     headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

#     try:
#         resp = requests.post(PAWAPAY_PAYOUT_URL, json=payload, headers=headers, timeout=20)
#         payout_response = resp.json()
#     except Exception as e:
#         return jsonify({"error": f"Payout request failed: {str(e)}"}), 500

#     payout_status = payout_response.get("status", "UNKNOWN")

#     db.execute("UPDATE loans SET status=?, approved_by=? WHERE loanId=?", (payout_status, admin_id, loan_id))
#     db.commit()

#     return jsonify({
#         "loanId": loan_id,
#         "payoutId": payout_id,
#         "status": payout_status,
#         "payout_response": payout_response
#     }), 200


#Test 3............................
# -------------------------
# DISBURSE LOAN (ADMIN ACTION)
# -------------------------
@app.route("/api/loans/disburse/<loan_id>", methods=["POST"])
def disburse_loan(loan_id):
    """
    Admin approves and disburses a pending loan via PawaPay payout.
    """
    data = request.json or {}
    admin_id = data.get("admin_id", "admin_default")

    db = get_db()
    db.row_factory = sqlite3.Row  # ensure rows can be accessed by column names

    loan_row = db.execute("SELECT * FROM loans WHERE loanId=?", (loan_id,)).fetchone()
    if not loan_row:
        return jsonify({"error": "Loan not found"}), 404

    loan = dict(loan_row)  # convert to dict for safe .get() usage

    if loan.get("status") != "PENDING":
        return jsonify({"error": f"Loan already {loan.get('status')}"}), 400

    # Get borrower's phone from loan table if available
    phone = loan.get("phone")
    if not phone:
        # fallback: try fetching from transactions linked to user's investment
        user_id = loan.get("user_id")
        t = db.execute("""
            SELECT phoneNumber FROM transactions 
            WHERE user_id=? AND type='investment'
            ORDER BY received_at DESC LIMIT 1
        """, (user_id,)).fetchone()
        if t:
            phone = t["phoneNumber"]
        else:
            return jsonify({"error": "No phone number found for user"}), 400

    # Build payout request
    payout_id = str(uuid.uuid4())
    payload = {
        "payoutId": payout_id,
        "recipient": {
            "type": "MMO",
            "accountDetails": {
                "phoneNumber": str(phone),
                "provider": "MTN_MOMO_ZMB"   # 🔹 later make this dynamic
            }
        },
        "customerMessage": f"Loan {loan_id} disbursement",
        "amount": str(loan.get("amount")),
        "currency": "ZMW",
        "metadata": [
            {"fieldName": "loanId", "fieldValue": loan_id},
            {"fieldName": "userId", "fieldValue": loan.get("user_id")}
        ]
    }
    headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

    try:
        resp = requests.post(PAWAPAY_PAYOUT_URL, json=payload, headers=headers, timeout=20)
        payout_response = resp.json()
    except Exception as e:
        return jsonify({"error": f"Payout request failed: {str(e)}"}), 500

    payout_status = payout_response.get("status", "UNKNOWN")

    # Update loan row
    db.execute(
        "UPDATE loans SET status=?, approved_by=? WHERE loanId=?",
        (payout_status, admin_id, loan_id)
    )
    db.commit()

    return jsonify({
        "loanId": loan_id,
        "payoutId": payout_id,
        "status": payout_status,
        "payout_response": payout_response
    }), 200



# @app.route("/api/loans/disburse/<loan_id>", methods=["POST"])
# def disburse_loan(loan_id):
#     """
#     Admin approves and disburses a pending loan via Pawapay payout.
#     """
#     data = request.json or {}
#     admin_id = data.get("admin_id", "admin_default")

#     db = get_db()
#     loan = db.execute("SELECT * FROM loans WHERE loanId=?", (loan_id,)).fetchone()
#     if not loan:
#         return jsonify({"error": "Loan not found"}), 404
#     if loan["status"] != "PENDING":
#         return jsonify({"error": f"Loan already {loan['status']}"}), 400

#     # Get borrower phone from transactions table (investment record)
#     user_id = loan["user_id"]
#     t = db.execute("SELECT phoneNumber FROM transactions WHERE user_id=? AND type='investment' ORDER BY received_at DESC LIMIT 1", (user_id,)).fetchone()
#     if not t:
#         return jsonify({"error": "No phone number found for user"}), 400
#     phone = t["phoneNumber"]

#     # Build payout request
#     payout_id = str(uuid.uuid4())
#     payload = {
#         "payoutId": payout_id,
#         "recipient": {
#             "type": "MMO",
#             "accountDetails": {
#                 "phoneNumber": str(phone),
#                 "provider": "MTN_MOMO_ZMB"   # 🔹 later make this dynamic
#             }
#         },
#         "customerMessage": f"Loan {loan_id} disbursement",
#         "amount": str(loan["amount"]),
#         "currency": "ZMW",
#         "metadata": [
#             {"loanId": loan_id},
#             {"userId": user_id}
#         ]
#     }
#     headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

#     try:
#         resp = requests.post(
#             PAWAPAY_PAYOUT_URL,
#             json=payload,
#             headers=headers,
#             timeout=20
#         )

#         payout_response = resp.json()
#     except Exception as e:
#         return jsonify({"error": f"Payout request failed: {str(e)}"}), 500

#     payout_status = payout_response.get("status", "UNKNOWN")

#     # Update loan row
#     db.execute("UPDATE loans SET status=?, approved_by=? WHERE loanId=?", (payout_status, admin_id, loan_id))
#     db.commit()

#     return jsonify({
#         "loanId": loan_id,
#         "payoutId": payout_id,
#         "status": payout_status,
#         "payout_response": payout_response
#     }), 200



# -------------------------
# REJECT LOAN (ADMIN ACTION)
# -------------------------
@app.route("/api/loans/reject/<loan_id>", methods=["POST"])
def reject_loan(loan_id):
    admin_id = request.json.get("admin_id", "admin_default")
    db = get_db()
    loan = db.execute("SELECT * FROM loans WHERE loanId=?", (loan_id,)).fetchone()
    if not loan:
        return jsonify({"error": "Loan not found"}), 404
    if loan["status"] != "PENDING":
        return jsonify({"error": f"Loan already {loan['status']}"}), 400

    db.execute("UPDATE loans SET status='REJECTED', approved_by=? WHERE loanId=?", (admin_id, loan_id))
    db.commit()

    return jsonify({"loanId": loan_id, "status": "REJECTED"}), 200


# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    with app.app_context():
        init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)


# from dotenv import load_dotenv
# load_dotenv()

# from flask import Flask, request, jsonify, g
# import os, logging, sqlite3, json, requests, uuid
# from datetime import datetime

# # -------------------------
# # API CONFIGURATION
# # -------------------------
# API_MODE = os.getenv("API_MODE", "sandbox")
# SANDBOX_API_TOKEN = os.getenv("SANDBOX_API_TOKEN")
# LIVE_API_TOKEN = os.getenv("LIVE_API_TOKEN")

# API_TOKEN = LIVE_API_TOKEN if API_MODE == "live" else SANDBOX_API_TOKEN
# PAWAPAY_URL = (
#     "https://api.pawapay.io/deposits"
#     if API_MODE == "live"
#     else "https://api.sandbox.pawapay.io/deposits"
# )

# # -------------------------
# # DATABASE
# # -------------------------
# DATABASE = os.path.join(os.path.dirname(__file__), "transactions.db")
# app = Flask(__name__)
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# def init_db():
#     """Initialize database and ensure schema consistency."""
#     db = sqlite3.connect(DATABASE)
#     cur = db.cursor()

#     # Create base table if missing
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS transactions (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             depositId TEXT UNIQUE,
#             status TEXT,
#             amount REAL,
#             currency TEXT,
#             provider TEXT,
#             providerTransactionId TEXT,
#             failureCode TEXT,
#             failureMessage TEXT,
#             metadata TEXT,
#             received_at TEXT,
#             type TEXT DEFAULT 'payment'
#         )
#     """)
#     db.commit()

#     # Ensure columns phoneNumber and user_id exist
#     existing_cols = [row[1] for row in cur.execute("PRAGMA table_info(transactions)").fetchall()]
#     if "phoneNumber" not in existing_cols:
#         cur.execute("ALTER TABLE transactions ADD COLUMN phoneNumber TEXT")
#     if "user_id" not in existing_cols:
#         cur.execute("ALTER TABLE transactions ADD COLUMN user_id TEXT")
#     db.commit()

#     # Ensure migration of old metadata-based userId/type
#     try:
#         rows = cur.execute("SELECT depositId, metadata, type, user_id FROM transactions").fetchall()
#         for r in rows:
#             deposit_id, metadata, cur_type, cur_user = r
#             new_type = cur_type or "payment"
#             new_user = cur_user
#             changed = False

#             if metadata:
#                 try:
#                     meta_obj = json.loads(metadata)
#                 except Exception:
#                     meta_obj = None

#                 if isinstance(meta_obj, list):
#                     for entry in meta_obj:
#                         if not isinstance(entry, dict):
#                             continue
#                         fn = str(entry.get("fieldName") or "").lower()
#                         fv = entry.get("fieldValue")
#                         if fn == "userid" and fv and not new_user:
#                             new_user = str(fv)
#                             changed = True
#                         if fn == "purpose" and isinstance(fv, str):
#                             if fv.lower() == "investment" and new_type != "investment":
#                                 new_type = "investment"
#                                 changed = True
#                 elif isinstance(meta_obj, dict):
#                     if "userId" in meta_obj and not new_user:
#                         new_user = str(meta_obj.get("userId"))
#                         changed = True
#                     purpose = meta_obj.get("purpose")
#                     if purpose and isinstance(purpose, str) and purpose.lower() == "investment" and new_type != "investment":
#                         new_type = "investment"
#                         changed = True

#             if changed or (cur_user is None and new_user) or (cur_type is None and new_type):
#                 cur.execute("UPDATE transactions SET user_id=?, type=? WHERE depositId=?",
#                             (new_user, new_type, deposit_id))

#         db.commit()
#     except Exception:
#         logger.exception("Error during migration/backfill pass")

#     db.close()


# with app.app_context():
#     init_db()


# def get_db():
#     """Get or create a DB connection bound to Flask app context."""
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
# # HEALTH
# # -------------------------
# @app.route("/")
# def home():
#     return f"PawaPay Callback Receiver running ✅ (API_MODE={API_MODE})"


# # -------------------------
# # ORIGINAL PAYMENT ENDPOINTS
# # -------------------------
# @app.route("/initiate-payment", methods=["POST"])
# def initiate_payment():
#     try:
#         data = request.json
#         phone = data.get("phone")
#         amount = data.get("amount")
#         if not phone or not amount:
#             return jsonify({"error": "Missing phone or amount"}), 400

#         deposit_id = str(uuid.uuid4())
#         customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

#         payload = {
#             "depositId": deposit_id,
#             "amount": str(amount),
#             "currency": "ZMW",
#             "correspondent": "MTN_MOMO_ZMB",
#             "payer": {"type": "MSISDN", "address": {"value": phone}},
#             "customerTimestamp": customer_ts,
#             "statementDescription": "StudyCraftPay",
#             "metadata": [
#                 {"fieldName": "orderId", "fieldValue": "ORD-" + deposit_id},
#                 {"fieldName": "customerId", "fieldValue": phone, "isPII": True},
#             ],
#         }

#         headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
#         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)
#         result = resp.json()

#         db = get_db()
#         db.execute("""
#             INSERT OR REPLACE INTO transactions
#             (depositId,status,amount,currency,phoneNumber,provider,
#              providerTransactionId,failureCode,failureMessage,metadata,received_at,type,user_id)
#             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
#         """, (
#             deposit_id,
#             result.get("status", "PENDING"),
#             float(amount),
#             "ZMW",
#             phone,
#             None, None, None, None,
#             json.dumps(payload["metadata"]),
#             datetime.utcnow().isoformat(),
#             "payment",
#             None
#         ))
#         db.commit()
#         return jsonify({"depositId": deposit_id, **result}), 200

#     except Exception:
#         logger.exception("Payment initiation error")
#         return jsonify({"error": "Internal server error"}), 500


# # @app.route("/callback/deposit", methods=["POST"])
# # def deposit_callback():
# #     try:
# #         data = request.get_json(force=True)
# #         deposit_id = data.get("depositId")
# #         db = get_db()
# #         db.execute("""
# #             INSERT OR REPLACE INTO transactions
# #             (depositId,status,amount,currency,phoneNumber,provider,
# #              providerTransactionId,failureCode,failureMessage,metadata,received_at)
# #             VALUES (?,?,?,?,?,?,?,?,?,?,?)
# #         """, (
# #             deposit_id,
# #             data.get("status"),
# #             float(data.get("amount", 0)) if data.get("amount") else None,
# #             data.get("currency"),
# #             data.get("payer", {}).get("accountDetails", {}).get("phoneNumber"),
# #             data.get("payer", {}).get("accountDetails", {}).get("provider"),
# #             data.get("providerTransactionId"),
# #             data.get("failureReason", {}).get("failureCode"),
# #             data.get("failureReason", {}).get("failureMessage"),
# #             json.dumps(data.get("metadata")) if data.get("metadata") else None,
# #             datetime.utcnow().isoformat()
# #         ))
# #         db.commit()
# #         return jsonify({"received": True}), 200
# #     except Exception:
# #         logger.exception("Callback error")
# #         return jsonify({"error": "Internal server error"}), 500

# @app.route("/callback/deposit", methods=["POST"])
# def deposit_callback():
#     try:
#         data = request.get_json(force=True)
#         deposit_id = data.get("depositId")
#         db = get_db()

#         # ✅ Extract user_id from metadata if present
#         user_id = None
#         if "metadata" in data and data["metadata"]:
#             if isinstance(data["metadata"], dict):
#                 user_id = data["metadata"].get("userId")
#             elif isinstance(data["metadata"], list):
#                 for entry in data["metadata"]:
#                     if entry.get("fieldName", "").lower() == "userid":
#                         user_id = entry.get("fieldValue")

#         db.execute("""
#             INSERT OR REPLACE INTO transactions
#             (depositId,status,amount,currency,phoneNumber,provider,
#              providerTransactionId,failureCode,failureMessage,metadata,received_at,user_id)
#             VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
#         """, (
#             deposit_id,
#             data.get("status"),
#             float(data.get("amount", 0)) if data.get("amount") else None,
#             data.get("currency"),
#             data.get("payer", {}).get("accountDetails", {}).get("phoneNumber"),
#             data.get("payer", {}).get("accountDetails", {}).get("provider"),
#             data.get("providerTransactionId"),
#             data.get("failureReason", {}).get("failureCode"),
#             data.get("failureReason", {}).get("failureMessage"),
#             json.dumps(data.get("metadata")) if data.get("metadata") else None,
#             datetime.utcnow().isoformat(),
#             user_id
#         ))
#         db.commit()
#         return jsonify({"received": True}), 200
#     except Exception:
#         logger.exception("Callback error")
#         return jsonify({"error": "Internal server error"}), 500

# @app.route("/deposit_status/<deposit_id>")
# def deposit_status(deposit_id):
#     db = get_db()
#     row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
#     if not row:
#         return jsonify({"status": None, "message": "Deposit not found"}), 404
#     res = {k: row[k] for k in row.keys()}
#     if res.get("metadata"):
#         try:
#             res["metadata"] = json.loads(res["metadata"])
#         except:
#             pass
#     return jsonify(res), 200


# @app.route("/transactions/<deposit_id>")
# def get_transaction(deposit_id):
#     db = get_db()
#     row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
#     if not row:
#         return jsonify({"error": "not found"}), 404
#     res = {k: row[k] for k in row.keys()}
#     if res.get("metadata"):
#         try:
#             res["metadata"] = json.loads(res["metadata"])
#         except:
#             pass
#     return jsonify(res), 200


# # -------------------------
# # INVESTMENT ENDPOINTS
# # -------------------------
# @app.route("/api/investments/initiate", methods=["POST"])
# def initiate_investment():
#     try:
#         data = request.json
#         phone = data.get("phone")
#         amount = data.get("amount")
#         correspondent = data.get("correspondent", "MTN_MOMO_ZMB")
#         currency = data.get("currency", "ZMW")
#         # user_id = data.get("user_id", "unknown")
#         user_id = data["user_id"]

#         if not phone or not amount:
#             return jsonify({"error": "Missing phone or amount"}), 400

#         deposit_id = str(uuid.uuid4())
#         customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

#         payload = {
#             "depositId": deposit_id,
#             "amount": str(amount),
#             "currency": currency,
#             "correspondent": correspondent,
#             "payer": {"type": "MSISDN", "address": {"value": phone}},
#             "customerTimestamp": customer_ts,
#             "statementDescription": "Investment",
#             "metadata": [
#                 {"fieldName": "purpose", "fieldValue": "investment"},
#                 {"fieldName": "userId", "fieldValue": str(user_id), "isPII": True},
#             ],
#         }

#         headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
#         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)

#         try:
#             result = resp.json()
#         except Exception:
#             logger.error(f"PawaPay response not JSON: {resp.text}")
#             return jsonify({"error": "Invalid response from PawaPay"}), 502

#         status = result.get("status", "PENDING")

#         db = get_db()
#         db.execute("""
#             INSERT OR REPLACE INTO transactions
#             (depositId,status,amount,currency,phoneNumber,provider,
#              providerTransactionId,failureCode,failureMessage,metadata,received_at,type,user_id)
#             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
#         """, (
#             deposit_id,
#             status,
#             float(amount),
#             currency,
#             phone,
#             None, None, None, None,
#             json.dumps(payload["metadata"]),
#             datetime.utcnow().isoformat(),
#             "investment",
#             user_id
#         ))
#         db.commit()
#         return jsonify({"depositId": deposit_id, "status": status}), 200

#     except Exception as e:
#         logger.exception("Investment initiation error")
#         return jsonify({"error": str(e)}), 500


# @app.route("/api/investments/user/<user_id>", methods=["GET"])
# def get_user_investments(user_id):
#     db = get_db()
#     rows = db.execute(
#         "SELECT * FROM transactions WHERE type='investment' AND user_id=? ORDER BY received_at DESC",
#         (user_id,)
#     ).fetchall()

#     results = []
#     for row in rows:
#         res = {k: row[k] for k in row.keys()}
#         if res.get("metadata"):
#             try:
#                 res["metadata"] = json.loads(res["metadata"])
#             except:
#                 pass
#         results.append(res)

#     return jsonify(results), 200

# # -------------------------
# # SAMPLE INVESTMENT ROUTE
# # -------------------------
# @app.route("/sample-investment", methods=["POST"])
# def add_sample():
#     """Add a test investment to verify DB works"""
#     try:
#         db = get_db()
#         deposit_id = str(uuid.uuid4())
#         payload = {
#             "depositId": deposit_id,
#             "amount": 1000.0,
#             "currency": "ZMW",
#             "phoneNumber": "0977123456",
#             "metadata": json.dumps([{"fieldName":"purpose","fieldValue":"investment"},{"fieldName":"userId","fieldValue":"user_1"}]),
#             "received_at": datetime.utcnow().isoformat(),
#             "type": "investment",
#             "user_id": "user_1"
#         }
#         db.execute("""
#             INSERT INTO transactions
#             (depositId,status,amount,currency,phoneNumber,metadata,received_at,type,user_id)
#             VALUES (?,?,?,?,?,?,?,?,?)
#         """, (
#             deposit_id,
#             "SUCCESS",
#             payload["amount"],
#             payload["currency"],
#             payload["phoneNumber"],
#             payload["metadata"],
#             payload["received_at"],
#             payload["type"],
#             payload["user_id"]
#         ))
#         db.commit()
#         return jsonify({"message":"Sample investment added","depositId":deposit_id}), 200
#     except Exception as e:
#         logger.exception("Failed to insert sample")
#         return jsonify({"error": str(e)}), 500

# # -------------------------
# # RUN
# # -------------------------
# if __name__ == "__main__":
#     with app.app_context():
#         init_db()
#     port = int(os.environ.get("PORT", 5000))
#     app.run(host="0.0.0.0", port=port)












