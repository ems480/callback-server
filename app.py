from flask import Flask, request, jsonify

app = Flask(__name__)

# Example callback route
@app.route('/callback', methods=['POST'])
def callback():
    data = request.json
    print("Callback received:", data)  # log in Render dashboard
    return jsonify({"status": "success", "message": "Callback received"}), 200

@app.route('/')
def home():
    return "Callback server is running 🚀"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
