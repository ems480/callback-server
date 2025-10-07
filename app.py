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
DATABASE = os.path.join(os.path.dirname(__file__), "estack.db")
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# ✅ DATABASE CONFIG
# =========================
# DATABASE = os.path.join(os.path.dirname(__file__), "estack.db")

def init_db():
    """
    Create the estack_transactions table if missing.
    Stores combined transaction info and status only.
    """
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    # ✅ Create the single table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS estack_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name_of_transaction TEXT NOT NULL,  -- e.g. "K1000 | user_123 | DEP4567"
            status TEXT NOT NULL,               -- e.g. "invested", "loaned_out", "repaid"
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()
    print("✅ estack.db initialized with estack_transactions table.")


# ✅ Initialize database once Flask app starts
with app.app_context():
    init_db()


def get_db():
    """
    Return a DB connection scoped to the Flask request context.
    Row factory is sqlite3.Row for dict-like access.
    """
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db
    
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
# # -------------------------
@app.route("/api/loans/approve/<loan_id>", methods=["POST"])
def approve_loan(loan_id):
    try:
        db = get_db()
        admin_id = request.json.get("admin_id", "admin_default")

        # ✅ Fetch loan by loanId
        loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
        if not loan:
            return jsonify({"error": "Loan not found"}), 404

        # ✅ Prevent double approval
        if loan["status"] and loan["status"].upper() == "APPROVED":
            return jsonify({"message": "Loan already approved"}), 200

        now = datetime.utcnow().isoformat()

        # ✅ Approve loan
        db.execute("""
            UPDATE loans
            SET status = 'APPROVED',
                approved_by = ?,
                approved_at = ?,
                updated_at = ?
            WHERE loanId = ?
        """, (admin_id, now, now, loan_id))

        # ✅ Update investor’s transaction using investment_id, not user_id
        if loan["investment_id"]:
            db.execute("""
                UPDATE transactions
                SET status = 'LOANED_OUT',
                    updated_at = ?,
                    failureMessage = 'Loan Approved',
                    failureCode = 'LOAN'
                WHERE depositId = ?
            """, (now, loan["investment_id"]))
            logger.info(f"✅ Investor transaction {loan['investment_id']} marked as LOANED_OUT.")

            # ✅ Notify investor
            txn = db.execute("SELECT user_id FROM transactions WHERE depositId=?", (loan["investment_id"],)).fetchone()
            if txn and txn["user_id"]:
                notify_investor(txn["user_id"], f"Your investment {loan['investment_id']} has been loaned out.")

        db.commit()
        return jsonify({"message": f"Loan {loan_id} approved and linked investor updated"}), 200

    except Exception as e:
        db.rollback()
        logger.exception("Error approving loan")
        return jsonify({"error": str(e)}), 500


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
import sqlite3
from flask import Flask, request, jsonify

app = Flask(__name__)

import sqlite3
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/callback/deposit", methods=["POST"])
def deposit_callback():
    try:
        data = request.get_json(force=True)
        print("📩 Full callback data:", data)

        deposit_id = data.get("depositId")
        status = data.get("status", "PENDING").strip().upper()
        amount = data.get("depositedAmount", 0)
        metadata = data.get("metadata", {})
        user_id = metadata.get("userId", "unknown")

        if not deposit_id:
            return jsonify({"error": "Missing depositId"}), 400

        name_of_transaction = f"ZMW{amount} | {user_id} | {deposit_id}"

        db = sqlite3.connect("estack.db")
        db.row_factory = sqlite3.Row
        cur = db.cursor()

        # ✅ Use the correct table (same one as in /initiate)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS estack_transactions (
                name_of_transaction TEXT NOT NULL,
                status TEXT NOT NULL
            )
        """)

        # ✅ Check if transaction already exists
        existing = cur.execute(
            "SELECT name_of_transaction FROM estack_transactions WHERE name_of_transaction LIKE ?",
            (f"%{deposit_id}%",)
        ).fetchone()

        if existing:
            cur.execute(
                "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
                (status, f"%{deposit_id}%")
            )
            print(f"🔄 Updated transaction {deposit_id} → {status}")
        else:
            cur.execute(
                "INSERT INTO estack_transactions (name_of_transaction, status) VALUES (?, ?)",
                (name_of_transaction, status)
            )
            print(f"💾 Inserted new transaction {deposit_id} → {status}")

        db.commit()
        db.close()

        return jsonify({"success": True, "deposit_id": deposit_id, "status": status}), 200

    except Exception as e:
        print("❌ Error in /callback/deposit:", e)
        return jsonify({"error": str(e)}), 500

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
# INVESTMENT ENDPOINTS (Using estack.db)
# -------------------------
@app.route("/api/investments/initiate", methods=["POST"])
def initiate_investment():
    try:
        data = request.json or {}
        phone = data.get("phone") or data.get("phoneNumber")
        amount = data.get("amount")
        correspondent = data.get("correspondent", "MTN_MOMO_ZMB")
        currency = data.get("currency", "ZMW")
        user_id = data.get("user_id") or data.get("userId") or "unknown"

        if not phone or amount is None:
            return jsonify({"error": "Missing phone or amount"}), 400

        # Generate a unique deposit ID for this investment
        deposit_id = str(uuid.uuid4())
        customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # Prepare payload for PawaPay (still sending live or test request)
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

        # Try decoding the response
        try:
            result = resp.json()
        except Exception:
            logger.error("PawaPay response not JSON: %s", resp.text)
            return jsonify({"error": "Invalid response from PawaPay"}), 502

        status = result.get("status", "PENDING")

        # Create readable name for transaction
        # e.g. "K500 | user_001 | DEP12345"
        name_of_transaction = f"{currency}{amount} | {user_id} | {deposit_id}"

        # Save to estack.db
        db = get_db()
        db.execute("""
            INSERT INTO estack_transactions (name_of_transaction, status)
            VALUES (?, ?)
        """, (name_of_transaction, status))
        db.commit()

        logger.info("💰 Investment initiated: %s (user_id=%s, status=%s)",
                    name_of_transaction, user_id, status)

        return jsonify({
            "message": "Investment initiated successfully",
            "depositId": deposit_id,
            "user_id": user_id,
            "amount": amount,
            "status": status
        }), 200

    except Exception as e:
        logger.exception("Investment initiation error")
        return jsonify({"error": str(e)}), 500

@app.route("/api/investments/user/<user_id>", methods=["GET"])
def get_user_investments(user_id):
    try:
        db = get_db()
        rows = db.execute("""
            SELECT name_of_transaction, status
            FROM estack_transactions
            WHERE name_of_transaction LIKE ?
            ORDER BY rowid DESC
        """, (f"%{user_id}%",)).fetchall()

        results = [{"name_of_transaction": r["name_of_transaction"], "status": r["status"]} for r in rows]
        return jsonify(results), 200

    except Exception as e:
        logger.exception("Error fetching user investments")
        return jsonify({"error": str(e)}), 500

@app.route("/api/investments/status/<deposit_id>", methods=["GET"])
def get_investment_status(deposit_id):
    try:
        db = sqlite3.connect("estack.db")
        db.row_factory = sqlite3.Row
        cur = db.cursor()

        # ✅ Match the same table name
        cur.execute("SELECT status FROM estack_transactions WHERE name_of_transaction LIKE ?", (f"%{deposit_id}%",))
        row = cur.fetchone()
        db.close()

        if row:
            return jsonify({"status": row["status"]}), 200
        else:
            return jsonify({"error": "Transaction not found"}), 404

    except Exception as e:
        print("Error in get_investment_status:", e)
        return jsonify({"error": str(e)}), 500
# +++++++++++++++++++++++++++++++++++++++
# Rerieving loans requests
# +++++++++++++++++++++++++++++++++++++++
@app.route("/api/loans/user/<user_id>", methods=["GET"])
def get_user_loans(user_id):
    try:
        db = get_db()
        rows = db.execute("""
            SELECT loan_id, amount, status, borrower_id, investor_id
            FROM loans
            WHERE borrower_id = ?
            ORDER BY created_at DESC
        """, (user_id,)).fetchall()

        loans = [dict(row) for row in rows]
        return jsonify(loans), 200

    except Exception as e:
        logger.exception("Error fetching user loans")
        return jsonify({"error": str(e)}), 500

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

        # ✅ Fetch loan details (fixed column name)
        loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
        if not loan:
            return jsonify({"error": "Loan not found"}), 404

        # ✅ Ensure loan is approved before disbursement
        # if loan["status"] != "approved":
        #     return jsonify({"error": "Loan is not approved for disbursement"}), 400

        borrower_id = loan["user_id"]
        amount = float(loan["amount"])

        # ✅ Fetch borrower wallet
        borrower_wallet = db.execute(
            "SELECT * FROM wallets WHERE user_id = ?", (borrower_id,)
        ).fetchone()
        
        if not borrower_wallet:
            db.execute("""
                INSERT INTO wallets (user_id, balance, created_at, updated_at)
                VALUES (?, 0, ?, ?)
            """, (borrower_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat()))
            db.commit()
            logger.info(f"✅ Created new wallet for borrower {borrower_id}")
        
            borrower_wallet = db.execute(
                "SELECT * FROM wallets WHERE user_id = ?", (borrower_id,)
            ).fetchone()

        borrower_balance = float(borrower_wallet["balance"])

        # ✅ Credit borrower wallet
        new_balance = borrower_balance + amount
        db.execute(
            "UPDATE wallets SET balance = ?, updated_at = ? WHERE user_id = ?",
            (new_balance, datetime.utcnow().isoformat(), borrower_id)
        )

        # ✅ Mark loan as disbursed (fixed column name)
        db.execute(
            "UPDATE loans SET status = 'disbursed', disbursed_at = ? WHERE loanId = ?",
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
            # investment_row = db.execute("""
            #     SELECT depositId FROM transactions
            #     WHERE type = 'investment' AND status = 'ACTIVE'
            #     ORDER BY received_at ASC LIMIT 1
            # """).fetchone()
            investment_row = db.execute("""
                SELECT reference FROM transactions
                WHERE type = 'investment' AND status = 'ACTIVE'
                ORDER BY created_at ASC LIMIT 1
            """).fetchone()


            if investment_row:
                # investment_id = investment_row["depositId"]
                investment_id = investment_row["reference"]


                # ✅ Mark that single investment as LOANED_OUT
                db.execute("""
                    UPDATE transactions
                    SET status = 'LOANED_OUT', updated_at = ?
                    WHERE reference = ?

                    # UPDATE transactions
                    # SET status = 'LOANED_OUT', investment_id = ?, updated_at = ?
                    # WHERE depositId = ?
                """, (loan_id, datetime.utcnow().isoformat(), investment_id))
                db.commit()

                # ✅ Notify the investor
                investor_row = db.execute("""
                    SELECT user_id FROM transactions
                    WHERE reference = ? AND type = 'investment'
                    # SELECT user_id FROM transactions
                    # WHERE depositId = ? AND type = 'investment'
                """, (investment_id,)).fetchone()
                # investment_row = db.execute("""
                #     SELECT reference FROM transactions
                #     WHERE type = 'investment' AND status = 'ACTIVE'
                #     ORDER BY created_at ASC LIMIT 1
                # """).fetchone()

                # ✅ Also mark investor's transaction as DISBURSED
                if loan["investment_id"]:
                    db.execute("""
                        UPDATE transactions
                        SET status = 'DISBURSED', updated_at = ?
                        WHERE depositId = ?
                    """, (datetime.utcnow().isoformat(), loan["investment_id"]))
                    logger.info(f"✅ Investor transaction {loan['investment_id']} marked as DISBURSED.")



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
