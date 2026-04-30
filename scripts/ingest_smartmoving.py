"""
SmartMoving API → DuckDB ingestion script.
Pulls all data from SmartMoving Open API and upserts into DuckDB.

Usage:
    python3 scripts/ingest_smartmoving.py              # full sync
    python3 scripts/ingest_smartmoving.py --table leads # single table
"""

import os
import sys
import json
import time
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timezone

import duckdb

# ── Config ──

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "mme_dashboard.duckdb")

env_path = os.path.join(PROJECT_ROOT, ".env")
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
    print("ERROR: SMARTMOVING_API_KEY not set")
    sys.exit(1)

HEADERS = {"x-api-key": API_KEY, "Content-Type": "application/json"}
if CLIENT_ID:
    HEADERS["x-client-id"] = CLIENT_ID

PAGE_SIZE = 200


# ── API helpers ──

def api_get(endpoint, params=None):
    """GET from SmartMoving API, returns parsed JSON."""
    url = f"{BASE_URL}{endpoint}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url += f"?{qs}" if "?" not in url else f"&{qs}"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def paginate(endpoint, page_size=PAGE_SIZE):
    """Yield all pageResults from a paginated endpoint."""
    page = 1
    while True:
        data = api_get(endpoint, {"Page": page, "PageSize": page_size})
        for item in data.get("pageResults", []):
            yield item
        if data.get("lastPage", True):
            break
        page += 1
        time.sleep(0.2)  # rate limit courtesy


# ── Sync functions ──

def sync_branches(con):
    """Sync branches table."""
    records = list(paginate("/branches"))
    con.execute("DELETE FROM branches")
    for r in records:
        loc = r.get("dispatchLocation") or {}
        con.execute("""
            INSERT INTO branches (id, name, phone_number, address_full, city, state, zip, lat, lng, is_primary, synced_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
        """, [r["id"], r.get("name"), r.get("phoneNumber"),
              loc.get("fullAddress"), loc.get("city"), loc.get("state"), loc.get("zip"),
              loc.get("lat"), loc.get("lng"), r.get("isPrimary")])
    return len(records)


def sync_users(con):
    """Sync users table."""
    records = list(paginate("/users"))
    con.execute("DELETE FROM users")
    for r in records:
        branch = r.get("primaryBranch") or {}
        role = r.get("role") or {}
        con.execute("""
            INSERT INTO users (id, name, title, email, primary_branch_id, primary_branch_name, role_id, role_name, synced_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
        """, [r["id"], r.get("name"), r.get("title"), r.get("email"),
              branch.get("id"), branch.get("name"), role.get("id"), role.get("name")])
    return len(records)


def sync_move_sizes(con):
    """Sync move_sizes table."""
    records = list(paginate("/move-sizes"))
    con.execute("DELETE FROM move_sizes")
    for r in records:
        con.execute("""
            INSERT INTO move_sizes (id, name, description, volume, weight, synced_at)
            VALUES (?, ?, ?, ?, ?, current_timestamp)
        """, [r["id"], r.get("name"), r.get("description"), r.get("volume"), r.get("weight")])
    return len(records)


def sync_referral_sources(con):
    """Sync referral_sources table."""
    records = list(paginate("/referral-sources"))
    con.execute("DELETE FROM referral_sources")
    for r in records:
        con.execute("""
            INSERT INTO referral_sources (id, name, is_lead_provider, is_public, synced_at)
            VALUES (?, ?, ?, ?, current_timestamp)
        """, [r["id"], r.get("name"), r.get("isLeadProvider"), r.get("isPublic")])
    return len(records)


def sync_leads(con):
    """Sync leads table."""
    records = list(paginate("/leads"))
    con.execute("DELETE FROM leads")
    for r in records:
        created = r.get("createdAtUtc")
        con.execute("""
            INSERT INTO leads (id, customer_name, email_address, phone_number, phone_type,
                referral_source, referral_source_name, affiliate_name, service_date,
                sales_person_id, sales_person, type, branch_id, branch_name,
                origin_address_full, destination_address_full, move_size_id, move_size_name,
                status, lost_reason, created_at_utc, synced_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
        """, [r["id"], r.get("customerName"), r.get("emailAddress"), r.get("phoneNumber"),
              r.get("phoneType"), r.get("referralSource"), r.get("referralSourceName"),
              r.get("affiliateName"), r.get("serviceDate"), r.get("salesPersonId"),
              r.get("salesPerson"), r.get("type"), r.get("branchId"), r.get("branchName"),
              r.get("originAddressFull"), r.get("destinationAddressFull"),
              r.get("moveSizeId"), r.get("moveSizeName"), r.get("status"),
              r.get("lostReason"), created])
    return len(records)


def sync_customers(con):
    """Sync customers table. Large table — uses pagination."""
    count = 0
    con.execute("DELETE FROM customers")
    for r in paginate("/customers"):
        con.execute("""
            INSERT INTO customers (id, name, phone_number, phone_type, email_address, address, synced_at)
            VALUES (?, ?, ?, ?, ?, ?, current_timestamp)
        """, [r["id"], r.get("name"), r.get("phoneNumber"), r.get("phoneType"),
              r.get("emailAddress"), r.get("address")])
        count += 1
        if count % 1000 == 0:
            print(f"  customers: {count} synced...")
    return count


def sync_opportunities_and_jobs(con):
    """
    Sync opportunities and jobs. Jobs are nested inside opportunity detail.
    Strategy: iterate customers, fetch their opportunities, then fetch full detail for each.
    """
    con.execute("DELETE FROM opportunities")
    con.execute("DELETE FROM jobs")
    con.execute("DELETE FROM job_crew_members")

    opp_count = 0
    job_count = 0
    cust_count = 0

    # Get all customer IDs
    customer_ids = [row[0] for row in con.execute("SELECT id FROM customers").fetchall()]
    total_customers = len(customer_ids)

    for cust_id in customer_ids:
        cust_count += 1
        if cust_count % 500 == 0:
            print(f"  opportunities: scanned {cust_count}/{total_customers} customers, found {opp_count} opportunities, {job_count} jobs...")

        try:
            opp_list = api_get(f"/customers/{cust_id}/opportunities")
        except Exception:
            continue

        for opp_summary in opp_list.get("pageResults", []):
            opp_id = opp_summary["id"]
            try:
                opp = api_get(f"/opportunities/{opp_id}")
            except Exception:
                continue

            customer = opp.get("customer") or {}
            est = opp.get("estimatedTotal") or {}

            con.execute("""
                INSERT OR REPLACE INTO opportunities (
                    id, quote_number, customer_id, customer_name, customer_email, customer_phone,
                    branch_name, opportunity_type, type, service_date, status, lead_status,
                    move_size, volume, weight, estimated_subtotal, estimated_tax, estimated_total,
                    estimator, sales_assignee, referral_source,
                    custom_field_01, custom_field_02, custom_field_03,
                    created_at_utc, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
            """, [opp["id"], opp.get("quoteNumber"), customer.get("id"), customer.get("name"),
                  customer.get("emailAddress"), customer.get("phoneNumber"),
                  (opp.get("branch") or {}).get("name"),
                  opp.get("opportunityType"), opp.get("type"), opp.get("serviceDate"),
                  opp.get("status"), opp.get("leadStatus"), opp.get("moveSize"),
                  opp.get("volume"), opp.get("weight"),
                  est.get("subtotal"), est.get("tax"), est.get("finalTotal"),
                  opp.get("estimator"), opp.get("salesAssignee"), opp.get("referralSource"),
                  opp.get("customField01"), opp.get("customField02"), opp.get("customField03"),
                  opp.get("createdAtUtc")])
            opp_count += 1

            for job in opp.get("jobs") or []:
                job_est = job.get("estimatedCharges") or {}
                job_act = job.get("actualCharges") or {}
                arrival = job.get("arrivalWindow") or {}

                con.execute("""
                    INSERT OR REPLACE INTO jobs (
                        id, opportunity_id, job_number, job_date, type, confirmed,
                        estimated_subtotal, estimated_tax, estimated_total,
                        actual_subtotal, actual_tax, actual_total,
                        total_tips, arrival_window_description, arrival_window_start, arrival_window_end,
                        synced_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                """, [job["id"], opp["id"], job.get("jobNumber"), job.get("jobDate"),
                      job.get("type"), job.get("confirmed"),
                      job_est.get("subtotal"), job_est.get("tax"), job_est.get("finalTotal"),
                      job_act.get("subtotal"), job_act.get("tax"), job_act.get("finalTotal"),
                      job.get("totalTips"),
                      arrival.get("description"), arrival.get("startTime"), arrival.get("endTime")])
                job_count += 1

                for crew in job.get("crewMembers") or []:
                    crew_name = crew if isinstance(crew, str) else crew.get("name", str(crew))
                    con.execute("""
                        INSERT OR REPLACE INTO job_crew_members (job_id, crew_member_name, synced_at)
                        VALUES (?, ?, current_timestamp)
                    """, [job["id"], crew_name])

            time.sleep(0.1)  # rate limit

    return opp_count, job_count


def log_sync(con, source, table_name, records, started, status, error=None):
    """Write to sync_log."""
    con.execute("""
        INSERT INTO sync_log (source, table_name, records_synced, started_at, completed_at, status, error_message)
        VALUES (?, ?, ?, ?, current_timestamp, ?, ?)
    """, [source, table_name, records, started, status, error])


# ── Main ──

def main():
    parser = argparse.ArgumentParser(description="Ingest SmartMoving data into DuckDB")
    parser.add_argument("--table", help="Sync a single table (branches, users, move_sizes, referral_sources, leads, customers, opportunities)")
    args = parser.parse_args()

    con = duckdb.connect(DB_PATH)
    tables_to_sync = [args.table] if args.table else [
        "branches", "users", "move_sizes", "referral_sources", "leads", "customers", "opportunities"
    ]

    sync_map = {
        "branches": ("branches", sync_branches),
        "users": ("users", sync_users),
        "move_sizes": ("move_sizes", sync_move_sizes),
        "referral_sources": ("referral_sources", sync_referral_sources),
        "leads": ("leads", sync_leads),
        "customers": ("customers", sync_customers),
        "opportunities": ("opportunities + jobs", sync_opportunities_and_jobs),
    }

    for table in tables_to_sync:
        if table not in sync_map:
            print(f"Unknown table: {table}")
            continue

        label, sync_fn = sync_map[table]
        started = datetime.now(timezone.utc)
        print(f"\nSyncing {label}...")

        try:
            result = sync_fn(con)
            if isinstance(result, tuple):
                opp_count, job_count = result
                print(f"  ✓ {opp_count} opportunities, {job_count} jobs")
                log_sync(con, "smartmoving", "opportunities", opp_count, started, "success")
                log_sync(con, "smartmoving", "jobs", job_count, started, "success")
            else:
                print(f"  ✓ {result} records")
                log_sync(con, "smartmoving", table, result, started, "success")
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
            log_sync(con, "smartmoving", table, 0, started, "error", str(e))

    # Print summary
    print(f"\n{'='*40}")
    print("Sync complete. Table counts:")
    for t in ["branches", "users", "move_sizes", "referral_sources", "leads", "customers", "opportunities", "jobs", "job_crew_members"]:
        count = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t}: {count}")

    con.close()


if __name__ == "__main__":
    main()
