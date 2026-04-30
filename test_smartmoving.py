import os
import json
import urllib.request
import urllib.error

# Read .env manually (no dependencies needed)
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())

API_KEY = os.environ.get("SMARTMOVING_API_KEY")
CLIENT_ID = os.environ.get("SMARTMOVING_CLIENT_ID")
BASE_URL = os.environ.get("SMARTMOVING_BASE_URL", "https://smartmoving-prod-api-management.azure-api.net/v1/api")

if not API_KEY:
    print("ERROR: SMARTMOVING_API_KEY is empty in .env")
    exit(1)

headers = {
    "x-api-key": API_KEY,
    "Content-Type": "application/json",
}
if CLIENT_ID:
    headers["x-client-id"] = CLIENT_ID

# Test all discovered endpoints
endpoints = [
    "/customers?Page=1&PageSize=2",
    "/leads?Page=1&PageSize=2",
    "/branches?Page=1&PageSize=10",
    "/move-sizes?Page=1&PageSize=5",
    "/referral-sources?Page=1&PageSize=5",
    "/users?Page=1&PageSize=5",
]

for endpoint in endpoints:
    url = f"{BASE_URL}{endpoint}"
    print(f"\n{'='*60}")
    print(f"GET {url}")
    print('='*60)

    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req) as response:
            raw = response.read().decode()
            print(f"STATUS: {response.status}")
            print(f"Content-Type: {response.getheader('Content-Type')}")
            try:
                data = json.loads(raw)
                print(json.dumps(data, indent=2, default=str)[:3000])
            except json.JSONDecodeError:
                print(f"RAW RESPONSE (first 500 chars):\n{raw[:500]}")
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code}: {e.reason}")
        body = e.read().decode()
        if body:
            print(body[:500])
    except Exception as e:
        print(f"ERROR: {e}")

# Test opportunity detail (jobs are nested here)
print(f"\n{'='*60}")
print("Testing opportunity detail (jobs are nested inside opportunities)")
print('='*60)

# First get a customer with opportunities
url = f"{BASE_URL}/customers?Page=1&PageSize=5"
req = urllib.request.Request(url, headers=headers)
try:
    with urllib.request.urlopen(req) as response:
        customers = json.loads(response.read().decode())
        for cust in customers["pageResults"]:
            opp_url = f"{BASE_URL}/customers/{cust['id']}/opportunities"
            opp_req = urllib.request.Request(opp_url, headers=headers)
            with urllib.request.urlopen(opp_req) as opp_resp:
                opps = json.loads(opp_resp.read().decode())
                if opps["totalResults"] > 0:
                    opp_id = opps["pageResults"][0]["id"]
                    detail_url = f"{BASE_URL}/opportunities/{opp_id}"
                    detail_req = urllib.request.Request(detail_url, headers=headers)
                    with urllib.request.urlopen(detail_req) as detail_resp:
                        detail = json.loads(detail_resp.read().decode())
                        print(f"\nOpportunity #{detail.get('quoteNumber')} for {detail['customer']['name']}:")
                        print(f"  Status: {detail['status']}")
                        print(f"  Service Date: {detail['serviceDate']}")
                        print(f"  Estimated Total: {detail['estimatedTotal']}")
                        print(f"  Jobs: {len(detail.get('jobs', []))}")
                        for job in detail.get("jobs", []):
                            print(f"    - Job {job['jobNumber']} on {job['jobDate']} (type={job['type']}, confirmed={job['confirmed']})")
                        print(json.dumps(detail, indent=2, default=str)[:2000])
                    break
except Exception as e:
    print(f"ERROR: {e}")
