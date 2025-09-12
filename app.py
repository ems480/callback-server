# SERVER 4
# app.py
from flask import Flask, request, jsonify
import os
from datetime import datetime

app = Flask(__name__)

# ===========================
# Root route (browser test)
# ===========================
@app.route('/')
def home():
    return "✅ Callback server is running!"

# ===========================
# Callback route (for PawaPay)
# ===========================
@app.route('/callback', methods=['POST'])
def callback():
    data = request.json

    if not data:
        # No JSON received
        return jsonify({"status": "error", "message": "No JSON data received"}), 400

    # Optional: validate essential fields from PawaPay
    required_fields = ["transaction_id", "status", "amount", "currency"]
    missing_fields = [field for field in required_fields if field not in data]

    if missing_fields:
        return jsonify({
            "status": "error",
            "message": f"Missing fields: {', '.join(missing_fields)}"
        }), 400

    # Log received callback in Render dashboard
    print("Callback received:", data)

    # Optional: save callback to file
    log_file = "callback_log.txt"
    with open(log_file, "a") as f:
        f.write(f"{datetime.utcnow()} - {data}\n")

    # Respond to PawaPay to acknowledge callback
    return jsonify({"status": "success", "message": "Callback received"}), 200

# ===========================
# Run server
# ===========================
if __name__ == '__main__':
    # Use Render-assigned port
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)


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
