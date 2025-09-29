from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, g
import os, logging, sqlite3, uuid
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


@app.route("/transactions/<deposit_id>", methods=["GET"])
def get_transaction(deposit_id):
    """Return single investment transaction by depositId (used by Kivy check_status)."""
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Transaction not found"}), 404

        return jsonify({
            "deposit_id": row["depositId"],
            "status": row["status"],
            "amount": row["amount"],
            "currency": row["currency"],
            "phone": row["phoneNumber"],
            "provider": row["provider"]
        }), 200
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

        # create payout entry (queued for later processing)
        payout_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO payouts (payoutId, loan_id, status, amount, currency, phoneNumber, provider, sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (payout_id, loan_id, "PENDING", loan["amount"], "ZMW", "260970000000", "MTN_MOMO_ZMB", datetime.utcnow().isoformat()))
        db.commit()

        return jsonify({"loan_id": loan_id, "status": "APPROVED", "payout_id": payout_id}), 200
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
