import webbrowser
import requests
import hashlib
import base64
from urllib.parse import urlparse, parse_qs, quote

# --------------------
# CONFIG (FILL THESE)
# --------------------
APP_ID = "03VEQP97U0-100"          # e.g. "XXXXXX-100"
APP_SECRET = "5AB0LWYM1B"  # regenerate this in FYERS portal
REDIRECT_URI = "https://www.google.com/"   # must exactly match FYERS app settings

TOKEN_FILE = "fyers_access_token.txt"

# --------------------
# STEP 1: Open browser to get auth_code
# --------------------
# FYERS v3 endpoint for auth code
base_auth_url = "https://api-t1.fyers.in/api/v3/generate-authcode"

params = (
    f"?client_id={quote(APP_ID)}"
    f"&redirect_uri={quote(REDIRECT_URI, safe='')}"
    f"&response_type=code"
    f"&state=sample_state"
)

auth_url = base_auth_url + params

print("Open this URL in your browser and login:")
print(auth_url)

try:
    webbrowser.open(auth_url)
except Exception:
    pass

# --------------------
# STEP 2: Paste redirected URL and extract auth_code
# --------------------
redirected = input("\nAfter login, paste the FULL redirected URL here:\n> ").strip()

parsed = urlparse(redirected)
qs = parse_qs(parsed.query)

if "auth_code" not in qs:
    print("❌ Could not find auth_code in the URL you pasted.")
    print("URL:", redirected)
    raise SystemExit(1)

auth_code = qs["auth_code"][0]
print("Auth code:", auth_code)

# --------------------
# STEP 3: Exchange auth_code for access token
# --------------------
token_url = "https://api-t1.fyers.in/api/v3/validate-authcode"

# FYERS requires appIdHash = SHA256(APP_ID:APP_SECRET)
app_id_hash = hashlib.sha256(f"{APP_ID}:{APP_SECRET}".encode()).hexdigest()

payload = {
    "grant_type": "authorization_code",
    "appIdHash": app_id_hash,
    "code": auth_code,
}

headers = {
    "Content-Type": "application/json",
}

resp = requests.post(token_url, json=payload, headers=headers, timeout=30)
data = resp.json()

if "access_token" not in data:
    print("❌ Failed to generate access token")
    print("Response:", data)
    raise SystemExit(1)

access_token = data["access_token"]

print("✅ Access Token generated successfully!")q
print("Access Token:", access_token)

# --------------------
# STEP 4: Save token
# --------------------
with open(TOKEN_FILE, "w") as f:
    f.write(access_token)

print(f"💾 Token saved to {TOKEN_FILE}")
print("🎉 Done. Use this token with fyers-apiv3 SDK.")