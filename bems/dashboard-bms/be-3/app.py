from flask import Flask, request, jsonify, session
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from datetime import datetime, timedelta
import hashlib
import jwt

# Flask App Setup
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:xxx@database:xxx/heb_iot_alpha'
app.config['SECRET_KEY'] = 'xxx'  # Secret key for JWT
app.config['JWT_EXPIRATION_MINUTES'] = 60  # JWT valid for 1 hour
app.config['SESSION_TYPE'] = 'filesystem'  # Store session data in filesystem
app.secret_key = 'supersecretkey'  # Secret key for Flask session

db = SQLAlchemy(app)
CORS(app, supports_credentials=True, origins=["http://xxx:xxx"])

# User Model
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(100), nullable=False)
    timestamp = db.Column(db.DateTime, nullable=True)  # Waktu terakhir token dibuat


# Create Tables
with app.app_context():
    db.create_all()

# Hash Password Function
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Login Endpoint
@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    user = User.query.filter_by(username=username).first()

    debug_info = {
        "username_from_client": username,
        "password_from_client": password,
        "password_len": len(password),
        "password_ord": [ord(c) for c in password],
        "hashed_password_from_client": hash_password(password),
        "db_password": user.password if user else None,
        "user_found": user.username if user else None
    }

    if user and user.password == hash_password(password):
        expiration = datetime.utcnow() + timedelta(minutes=app.config['JWT_EXPIRATION_MINUTES'])
        token = jwt.encode({"sub": username, "exp": expiration}, app.config['SECRET_KEY'], algorithm="HS256")

        user.token = token
        user.timestamp = datetime.utcnow()
        db.session.commit()

        session['auth_token'] = token
        session['login_timestamp'] = datetime.utcnow().isoformat()

        return jsonify({
            "message": "Login successful!",
            "token": token,
            "username": username,
            "debug": debug_info
        }), 200
    else:
        return jsonify({
            "message": "Invalid credentials",
            "debug": debug_info
        }), 401


# Verify Token Endpoint
@app.route('/api/verify_token', methods=['POST'])
def verify_token():
    data = request.get_json()
    token = data.get("token")  # Ambil token dari body

    if not token:
        print("Token not provided")
        return jsonify({"message": "Token not provided"}), 401

    try:
        # Dekode token JWT
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])

        # Ambil username dari klaim 'sub'
        username = decoded['sub']
        print(f"Decoded token for user: {username}")

        # Memeriksa masa berlaku token
        expiration_time = decoded.get('exp')  # Waktu kedaluwarsa token
        if expiration_time < datetime.utcnow().timestamp():
            print("Token expired")
            return jsonify({"message": "Token expired"}), 401

        # Jika token masih valid
        return jsonify({"message": "Token is valid"}), 200

    except jwt.ExpiredSignatureError:
        print("Token expired")
        return jsonify({"message": "Token expired"}), 401
    except jwt.InvalidTokenError as e:
        print(f"Invalid token: {e}")
        return jsonify({"message": "Invalid token"}), 401



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10009, debug=True)
