"""
QuickBooks OAuth2 Authorization Flow.

Starts a local server, opens the QB auth page in your browser,
captures the callback, exchanges the code for tokens, and saves
the refresh token to .env.

Usage:
    python3 scripts/qb_auth.py

Prerequisites:
    1. Go to https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization
    2. In your Intuit app settings, add this redirect URI:
       http://localhost:8080/callback
    3. Make sure QB_CLIENT_ID and QB_CLIENT_SECRET are set in .env
"""

import os
import sys
import json
import base64
import webbrowser
import urllib.request
import urllib.parse
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load .env
env_path = os.path.join(PROJECT_ROOT, ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip("'\""))

CLIENT_ID = os.environ.get("QB_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("QB_CLIENT_SECRET", "")
REALM_ID = os.environ.get("QB_REALM_ID", "")

REDIRECT_URI = "http://localhost:8080/callback"
AUTH_URL = "https://appcenter.intuit.com/connect/oauth2"
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
SCOPES = "com.intuit.quickbooks.accounting"

# Will be set by the callback handler
auth_code = None


class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if "code" in params:
            auth_code = params["code"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"""
                <html><body style="font-family: sans-serif; text-align: center; padding: 50px;">
                <h1>QuickBooks Connected!</h1>
                <p>You can close this window and return to the terminal.</p>
                </body></html>
            """)
            print(f"\n  Authorization code received.")
        elif "error" in params:
            error = params.get("error", ["unknown"])[0]
            self.send_response(400)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(f"<html><body><h1>Error: {error}</h1></body></html>".encode())
            print(f"\n  ERROR: {error}")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress server logs


def exchange_code_for_tokens(code):
    """Exchange authorization code for access + refresh tokens."""
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

    data = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
    }).encode()

    req = urllib.request.Request(TOKEN_URL, data=data, headers={
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    })

    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def update_env_file(refresh_token):
    """Update .env file with the new refresh token."""
    lines = []
    with open(env_path) as f:
        lines = f.readlines()

    with open(env_path, "w") as f:
        for line in lines:
            if line.strip().startswith("QB_REFRESH_TOKEN="):
                f.write(f"QB_REFRESH_TOKEN={refresh_token}\n")
            else:
                f.write(line)

    print(f"  Refresh token saved to .env")


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: QB_CLIENT_ID and QB_CLIENT_SECRET must be set in .env")
        sys.exit(1)

    print("QuickBooks OAuth2 Setup")
    print("=" * 50)
    print()
    print("Make sure this redirect URI is in your Intuit app settings:")
    print(f"  {REDIRECT_URI}")

    # Build auth URL
    params = urllib.parse.urlencode({
        "client_id": CLIENT_ID,
        "response_type": "code",
        "scope": SCOPES,
        "redirect_uri": REDIRECT_URI,
        "state": "mme_dashboard",
    })
    auth_url = f"{AUTH_URL}?{params}"

    print(f"\n  Opening browser for QuickBooks authorization...")
    print(f"  If the browser doesn't open, visit this URL:")
    print(f"  {auth_url}\n")

    # Start local server
    server = HTTPServer(("localhost", 8080), CallbackHandler)

    # Open browser
    webbrowser.open(auth_url)

    print("  Waiting for authorization callback...")

    # Handle one request (the callback)
    while auth_code is None:
        server.handle_request()

    server.server_close()

    # Exchange code for tokens
    print("  Exchanging code for tokens...")
    try:
        tokens = exchange_code_for_tokens(auth_code)
        access_token = tokens.get("access_token")
        refresh_token = tokens.get("refresh_token")
        expires_in = tokens.get("expires_in", 3600)

        print(f"\n  Access token received (expires in {expires_in}s)")
        print(f"  Refresh token received")

        # Save refresh token
        update_env_file(refresh_token)

        # Quick test — get company info
        print(f"\n  Testing connection...")
        realm = REALM_ID.replace(" ", "").replace("'", "")
        test_url = f"https://quickbooks.api.intuit.com/v3/company/{realm}/companyinfo/{realm}"
        req = urllib.request.Request(test_url, headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            company = data.get("CompanyInfo", {})
            print(f"  Connected to: {company.get('CompanyName', 'Unknown')}")
            print(f"  Company ID:   {realm}")

        print(f"\n{'='*50}")
        print("  QuickBooks is connected and ready!")
        print(f"{'='*50}")

    except urllib.error.HTTPError as e:
        print(f"\n  ERROR {e.code}: {e.reason}")
        body = e.read().decode()
        if body:
            print(f"  {body[:500]}")
        sys.exit(1)
    except Exception as e:
        print(f"\n  ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
