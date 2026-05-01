"""
Fast sync — scan all customers for 2026 opportunities only.
No sleep delays. Retries on 429. Stores in DuckDB.
"""

import os
import sys
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

# Force unbuffered output so we can see progress
sys.stdout.reconfigure(line_buffering=True)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))
from sm_api import api_get, get_opportunity_detail, _get_headers, _get_base_url

DB_PATH = os.path.join(PROJECT_ROOT, "db", "mme_dashboard.duckdb")
import duckdb

MIN_DATE = 20250401  # Last 12 months — skip fetching detail for older opps


def fast_get(url, retries=10):
    """Aggressive GET with retry on 429."""
    headers = _get_headers()
    for attempt in range(retries):
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = min(2 * (attempt + 1), 30)
                print(f"    429 — waiting {wait}s (attempt {attempt+1})...")
                time.sleep(wait)
                continue
            raise
    raise Exception(f"Rate limited after {retries} retries")


def sync_customers(con):
    """Reload all customers from API."""
    print("Syncing customers...")
    con.execute("DELETE FROM customers")
    count = 0
    page = 1
    while True:
        data = fast_get(f"{_get_base_url()}/customers?Page={page}&PageSize=200")
        for r in data.get("pageResults", []):
            con.execute("""
                INSERT INTO customers (id, name, phone_number, phone_type, email_address, address, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, current_timestamp)
            """, [r["id"], r.get("name"), r.get("phoneNumber"), r.get("phoneType"),
                  r.get("emailAddress"), r.get("address")])
            count += 1
        if count % 5000 == 0:
            print(f"  {count} customers...")
        if data.get("lastPage", True):
            break
        page += 1
    print(f"  {count} customers loaded.")
    return count


def _log_sync(con, source, table_name, records, started, status, error=None):
    """Write a row to sync_log."""
    try:
        con.execute("""
            INSERT INTO sync_log (source, table_name, records_synced, started_at, completed_at, status, error_message)
            VALUES (?, ?, ?, ?, current_timestamp, ?, ?)
        """, [source, table_name, records, started, status, error])
    except Exception as e:
        print(f"  Warning: could not write sync_log: {e}")


def main():
    con = duckdb.connect(DB_PATH)
    sync_started = datetime.now(timezone.utc)

    # Reload all customers first
    sync_customers(con)

    # Get all customer IDs from DuckDB
    customer_ids = [r[0] for r in con.execute("SELECT id FROM customers").fetchall()]
    total = len(customer_ids)
    print(f"Scanning {total} customers for 2026 opportunities...")
    print(f"Start: {datetime.now().strftime('%H:%M:%S')}")

    # Clear existing opportunity/job data and rebuild
    con.execute("DELETE FROM opportunities")
    con.execute("DELETE FROM jobs")
    con.execute("DELETE FROM job_crew_members")

    opp_count = 0
    job_count = 0
    skipped = 0
    errors = 0

    for i, cust_id in enumerate(customer_ids):
        if (i + 1) % 500 == 0:
            elapsed = time.time() - start
            rate = (i + 1) / elapsed * 60
            eta = (total - i - 1) / rate if rate > 0 else 0
            print(f"  {i+1}/{total} — {opp_count} opps, {job_count} jobs — {rate:.0f} customers/min — ETA {eta:.0f}min")

        if i == 0:
            start = time.time()

        try:
            url = f"{_get_base_url()}/customers/{cust_id}/opportunities"
            opps_data = fast_get(url)
        except Exception as e:
            errors += 1
            continue

        time.sleep(1.2)  # ~50 req/min, stays under rate limit

        for opp_s in opps_data.get("pageResults", []):
            svc_date = opp_s.get("serviceDate", 0)
            if svc_date < MIN_DATE:
                skipped += 1
                continue

            # Fetch full detail
            try:
                opp = fast_get(f"{_get_base_url()}/opportunities/{opp_s['id']}")
                time.sleep(1.2)
            except Exception:
                errors += 1
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
                  opp.get("status"), opp.get("leadStatus"),
                  (opp.get("moveSize") or {}).get("name") if isinstance(opp.get("moveSize"), dict) else opp.get("moveSize"),
                  opp.get("volume"), opp.get("weight"),
                  est.get("subtotal"), est.get("tax"), est.get("finalTotal"),
                  (opp.get("estimator") or {}).get("name") if isinstance(opp.get("estimator"), dict) else opp.get("estimator"),
                  (opp.get("salesAssignee") or {}).get("name") if isinstance(opp.get("salesAssignee"), dict) else opp.get("salesAssignee"),
                  opp.get("referralSource"),
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

    elapsed = time.time() - start
    print(f"\nDone in {elapsed/60:.1f} minutes")
    print(f"  Opportunities: {opp_count}")
    print(f"  Jobs: {job_count}")
    print(f"  Skipped (pre-2026): {skipped}")
    print(f"  Errors: {errors}")

    # Log sync results
    _log_sync(con, "SmartMoving", "customers", len(customer_ids), sync_started, "success")
    _log_sync(con, "SmartMoving", "opportunities", opp_count, sync_started, "success")
    _log_sync(con, "SmartMoving", "jobs", job_count, sync_started, "success")
    print("  Sync logged to sync_log table.")

    con.close()


if __name__ == "__main__":
    main()
