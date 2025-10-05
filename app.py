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

# JUST ADDED 1___________________________________________
def notify_investor(user_id, message):
    """
    Notify investor of investment status change.
    In real systems this could send email, SMS, or push.
    For now, we just log and store in a notifications table.
    """
    try:
        conn = sqlite3.connect(DATABASE)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                message TEXT,
                created_at TEXT
            )
        """)
        conn.commit()

        cur.execute("""
            INSERT INTO notifications (user_id, message, created_at)
            VALUES (?, ?, ?)
        """, (user_id, message, datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()

        logger.info(f"📢 Notification sent to investor {user_id}: {message}")
    except Exception as e:
        logger.error(f"❌ Failed to notify investor {user_id}: {e}")

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
    # needed = {
    #     "phoneNumber": "TEXT",
    #     "metadata": "TEXT",
    #     "updated_at": "TEXT",
    #     "type": "TEXT DEFAULT 'payment'",
    #     "user_id": "TEXT"
    # }
    needed = {
        "phoneNumber": "TEXT",
        "metadata": "TEXT",
        "updated_at": "TEXT",
        "type": "TEXT DEFAULT 'payment'",
        "user_id": "TEXT",
        "investment_id": "TEXT"   # ✅ NEW
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

# -------------------------
# CALLBACK RECEIVER (upsert-safe for deposits and payouts)
# -------------------------

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
        # Update loan if payout succeeded
        if txn_type == "payout" and loan_id and status in ("COMPLETED", "SUCCESS", "PAYMENT_COMPLETED"):
            db.execute("UPDATE loans SET status=? WHERE loanId=?", (status, loan_id))

            # Find user and notify repayment
            loan_row = db.execute("SELECT user_id FROM loans WHERE loanId=?", (loan_id,)).fetchone()
            if loan_row and loan_row["user_id"]:
                notify_investor(
                    loan_row["user_id"],
                    f"Loan {loan_id[:8]} has been successfully repaid."
                )

        # if txn_type == "payout" and loan_id and status in ("COMPLETED", "SUCCESS", "PAYMENT_COMPLETED"):
        #     db.execute("UPDATE loans SET status=? WHERE loanId=?", (status, loan_id))

        db.commit()
        return jsonify({"received": True}), 200

    except Exception:
        logger.exception("Callback error")
        return jsonify({"error": "Internal server error"}), 500

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

#TEST 4
@app.route("/api/loans/disburse/<loan_id>", methods=["POST"])
def disburse_loan(loan_id):
    try:
        db = get_db()  # ✅ Get the database connection
        data = request.get_json() or {}
        logger.info(f"Disbursing loan {loan_id} with data: {data}")

        # ✅ Fetch loan details
        loan = db.execute("SELECT * FROM loans WHERE loan_id = ?", (loan_id,)).fetchone()
        if not loan:
            return jsonify({"error": "Loan not found"}), 404

        # ✅ Ensure loan is approved before disbursement
        if loan["status"] != "approved":
            return jsonify({"error": "Loan is not approved for disbursement"}), 400

        borrower_id = loan["borrower_id"]
        amount = float(loan["amount"])

        # ✅ Fetch borrower wallet
        borrower_wallet = db.execute(
            "SELECT * FROM wallets WHERE user_id = ?", (borrower_id,)
        ).fetchone()
        if not borrower_wallet:
            return jsonify({"error": "Borrower wallet not found"}), 404

        borrower_balance = float(borrower_wallet["balance"])

        # ✅ Credit borrower wallet
        new_balance = borrower_balance + amount
        db.execute(
            "UPDATE wallets SET balance = ?, updated_at = ? WHERE user_id = ?",
            (new_balance, datetime.utcnow().isoformat(), borrower_id)
        )

        # ✅ Mark loan as disbursed
        db.execute(
            "UPDATE loans SET status = 'disbursed', disbursed_at = ? WHERE loan_id = ?",
            (datetime.utcnow().isoformat(), loan_id)
        )

        # ✅ Record the disbursement transaction
        db.execute("""
            INSERT INTO transactions (user_id, amount, type, status, reference, created_at, updated_at)
            VALUES (?, ?, 'loan_disbursement', 'SUCCESS', ?, ?, ?)
        """, (
            borrower_id, amount, loan_id,
            datetime.utcnow().isoformat(),
            datetime.utcnow().isoformat()
        ))

        db.commit()

        # ✅ Link this loan to one available investment
        try:
            investment_row = db.execute("""
                SELECT depositId FROM transactions
                WHERE type = 'investment' AND status = 'ACTIVE'
                ORDER BY received_at ASC LIMIT 1
            """).fetchone()

            if investment_row:
                investment_id = investment_row["depositId"]

                # ✅ Mark that single investment as LOANED_OUT
                db.execute("""
                    UPDATE transactions
                    SET status = 'LOANED_OUT', investment_id = ?, updated_at = ?
                    WHERE depositId = ?
                """, (loan_id, datetime.utcnow().isoformat(), investment_id))
                db.commit()

                # ✅ Notify the investor
                investor_row = db.execute("""
                    SELECT user_id FROM transactions
                    WHERE depositId = ? AND type = 'investment'
                """, (investment_id,)).fetchone()

                if investor_row and investor_row["user_id"]:
                    notify_investor(
                        investor_row["user_id"],
                        f"Your investment {investment_id[:8]} has been loaned out to borrower {loan_id[:8]}."
                    )

                logger.info(f"Investment {investment_id} linked to loan {loan_id}")
            else:
                logger.warning("No available active investment found to link with this loan.")

        except Exception as e:
            logger.error(f"Error linking investment to loan {loan_id}: {e}")

        return jsonify({
            "message": f"Loan {loan_id} successfully disbursed",
            "borrower_id": borrower_id,
            "amount": amount,
            "new_balance": new_balance
        }), 200

    except Exception as e:
        logger.error(f"Error disbursing loan {loan_id}: {e}")
        return jsonify({"error": str(e)}), 500


# @app.route("/api/loans/disburse/<loan_id>", methods=["POST"])
# def disburse_loan(loan_id):
#     """
#     Admin approves and disburses a pending loan via PawaPay payout.
#     - trims customerMessage to <=22 chars (PawaPay requirement)
#     - returns the full payout_response so client can show details
#     """
#     data = request.json or {}
#     admin_id = data.get("admin_id", "admin_default")

#     db = get_db()
#     db.row_factory = sqlite3.Row

#     loan_row = db.execute("SELECT * FROM loans WHERE loanId=?", (loan_id,)).fetchone()
#     if not loan_row:
#         return jsonify({"error": "Loan not found"}), 404

#     # convert sqlite3.Row -> dict for .get() usage
#     loan = dict(loan_row)

#     if loan.get("status") != "PENDING":
#         return jsonify({"error": f"Loan already {loan.get('status')}"}), 400

#     # Prefer phone saved on loan, else fallback to user's latest investment phone
#     phone = loan.get("phone")
#     if not phone:
#         user_id = loan.get("user_id")
#         t = db.execute("""
#             SELECT phoneNumber FROM transactions 
#             WHERE user_id=? AND type='investment'
#             ORDER BY received_at DESC LIMIT 1
#         """, (user_id,)).fetchone()
#         if t:
#             phone = t["phoneNumber"]
#         else:
#             return jsonify({"error": "No phone number found for user"}), 400

#     # Build a short customer message (PawaPay requires <=22 chars)
#     # Use a compact form like "Loan:abcd1234" (8 chars of id). Adjust if you want different format.
#     short_msg = f"Loan:{loan_id[:8]}"
#     if len(short_msg) > 22:
#         short_msg = short_msg[:22]

#     # Build payout payload (metadata as fieldName/fieldValue so callback parsing is consistent)
#     # --- Build safe PawaPay-compliant payout ---
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
#         "amount": str(loan.get("amount")),
#         "currency": "ZMW"
#     }
    
#     headers = {
#         "Authorization": f"Bearer {API_TOKEN}",
#         "Content-Type": "application/json"
#     }
    
#     try:
#         resp = requests.post(PAWAPAY_PAYOUT_URL, json=payload, headers=headers, timeout=20)
#         payout_response = resp.json()
#     except Exception as e:
#         return jsonify({"error": f"Payout request failed: {str(e)}"}), 500
    
#     payout_status = payout_response.get("status", "UNKNOWN")
    
#     # --- Update DB ---
#     db.execute(
#         "UPDATE loans SET status=?, approved_by=? WHERE loanId=?",
#         (payout_status, admin_id, loan_id)
#     )
#     db.commit()
    
#     # ----------------------------------------------------
#     # 🔁 Update investment status to 'LOANED_OUT' if linked + notify investor
#     # ----------------------------------------------------
#     if payout_status in ["SUCCESS", "ACCEPTED", "PENDING"]:
#         try:
#             db.execute("""
#                 UPDATE transactions
#                 SET status = 'LOANED_OUT', updated_at = ?
#                 WHERE type = 'investment' AND user_id = ?
#             """, (datetime.utcnow().isoformat(), loan["user_id"]))
#             db.commit()

#             # Notify investor
#             notify_investor(
#                 loan["user_id"],
#                 f"Your investment linked to loan {loan_id[:8]} has been loaned out."
#             )

#             print(f"✅ Investment for Loan {loan_id} marked as LOANED_OUT & investor notified.")
#         except Exception as e:
#             print("⚠️ Failed to update investment/notify investor:", str(e))
            
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

# OPTIONAL CODE CHECK NOTIFICATION 
@app.route("/api/notifications/<user_id>", methods=["GET"])
def get_notifications(user_id):
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC", (user_id,))
    rows = cur.fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows]), 200

# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    with app.app_context():
        init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)







