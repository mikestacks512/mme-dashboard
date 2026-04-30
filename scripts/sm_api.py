"""
SmartMoving API client. Shared by all report scripts.
"""

import os
import json
import time
import urllib.request
import urllib.error

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load .env
env_path = os.path.join(PROJECT_ROOT, ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())

API_KEY = os.environ.get("SMARTMOVING_API_KEY", "")
CLIENT_ID = os.environ.get("SMARTMOVING_CLIENT_ID", "")
BASE_URL = os.environ.get("SMARTMOVING_BASE_URL", "https://smartmoving-prod-api-management.azure-api.net/v1/api")

HEADERS = {"x-api-key": API_KEY, "Content-Type": "application/json"}
if CLIENT_ID:
    HEADERS["x-client-id"] = CLIENT_ID


def api_get(endpoint, params=None, retries=5):
    url = f"{BASE_URL}{endpoint}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url += f"?{qs}" if "?" not in url else f"&{qs}"
    for attempt in range(retries):
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:
                wait = min(5 * (attempt + 1), 60)  # 5s, 10s, 15s, 20s
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            raise


def paginate(endpoint, page_size=200):
    page = 1
    while True:
        data = api_get(endpoint, {"Page": page, "PageSize": page_size})
        for item in data.get("pageResults", []):
            yield item
        if data.get("lastPage", True):
            break
        page += 1
        time.sleep(0.2)


def get_all(endpoint, page_size=200):
    return list(paginate(endpoint, page_size))


def get_opportunity_detail(opp_id):
    return api_get(f"/opportunities/{opp_id}")


def get_customer_opportunities(customer_id):
    return api_get(f"/customers/{customer_id}/opportunities")
