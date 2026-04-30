"""
Daily Summary Report — SmartMoving operational snapshot.

Pulls leads live from API. For opportunity/job data, scans recent
customer opportunities to find today's and this week's activity.

Usage:
    python3 scripts/report_daily.py              # today's summary
    python3 scripts/report_daily.py --days 7     # last 7 days
    python3 scripts/report_daily.py --full-scan   # scan ALL customers (slow, hours)
"""

import sys
import os
import argparse
import time
from datetime import datetime, timedelta
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_api import get_all, api_get, get_opportunity_detail, paginate

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "mme_dashboard.duckdb")


def date_int(dt):
    return int(dt.strftime("%Y%m%d"))


def format_money(amount):
    if amount is None:
        return "$0"
    return f"${amount:,.2f}"


def format_date_int(d):
    if not d:
        return "N/A"
    s = str(d)
    return f"{s[4:6]}/{s[6:8]}/{s[:4]}"


def get_customer_ids_from_db():
    """Get customer IDs from DuckDB if available."""
    try:
        import duckdb
        if not os.path.exists(DB_PATH):
            return None
        con = duckdb.connect(DB_PATH, read_only=True)
        ids = [r[0] for r in con.execute("SELECT id FROM customers").fetchall()]
        con.close()
        return ids if ids else None
    except Exception:
        return None


def scan_opportunities(customer_ids, date_from, date_to, max_customers=None):
    """Scan customer opportunities for ones with service dates in range."""
    opportunities = []
    scanned = 0
    total = len(customer_ids)

    if max_customers:
        customer_ids = customer_ids[:max_customers]
        total = len(customer_ids)

    for cust_id in customer_ids:
        scanned += 1
        if scanned % 500 == 0:
            print(f"  Scanning... {scanned}/{total} customers, {len(opportunities)} opportunities found")

        try:
            opps = api_get(f"/customers/{cust_id}/opportunities")
        except Exception:
            continue

        for opp_summary in opps.get("pageResults", []):
            svc_date = opp_summary.get("serviceDate", 0)
            if date_from <= svc_date <= date_to:
                try:
                    opp_detail = get_opportunity_detail(opp_summary["id"])
                    opportunities.append(opp_detail)
                except Exception:
                    opportunities.append(opp_summary)

        time.sleep(0.15)

    return opportunities, scanned


# ── Opportunity status codes ──
STATUS_MAP = {
    0: "New",
    1: "Estimated",
    2: "Follow Up",
    3: "Booked",
    5: "Confirmed",
    10: "Completed",
    11: "Closed",
    20: "Lost",
    30: "Cancelled",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=1, help="Number of days to look back (default: 1 = today only)")
    parser.add_argument("--full-scan", action="store_true", help="Scan all customers (slow)")
    parser.add_argument("--sample", type=int, default=5000, help="Max customers to scan (default: 5000)")
    args = parser.parse_args()

    now = datetime.now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    date_from = date_int(today - timedelta(days=args.days - 1))
    date_to = date_int(today)

    print(f"{'='*70}")
    print(f"  DAILY SUMMARY — {now.strftime('%B %d, %Y %I:%M %p')}")
    if args.days > 1:
        print(f"  Period: {format_date_int(date_from)} – {format_date_int(date_to)}")
    print(f"{'='*70}")

    # ── Leads (live from API) ──
    print("\n  Pulling leads...")
    leads = get_all("/leads")
    today_leads = [l for l in leads if l.get("serviceDate") and date_from <= l["serviceDate"] <= date_to]

    print(f"\n  LEADS")
    print(f"  {'─'*40}")
    print(f"    Active Leads:        {len(leads)}")
    print(f"    Service Date Today:  {len(today_leads)}")

    source_counts = Counter(l.get("referralSourceName", "?") for l in leads)
    print(f"    By Source:           {', '.join(f'{s}({c})' for s, c in source_counts.most_common(5))}")

    # ── Opportunities scan ──
    customer_ids = get_customer_ids_from_db()
    if customer_ids:
        max_cust = None if args.full_scan else args.sample
        label = "all" if args.full_scan else f"sample of {max_cust}"
        print(f"\n  Scanning opportunities ({label} of {len(customer_ids)} customers)...")

        opps, scanned = scan_opportunities(customer_ids, date_from, date_to, max_cust)

        if opps:
            # Revenue
            total_estimated = sum((o.get("estimatedTotal") or {}).get("finalTotal", 0) for o in opps)

            # Job counts
            total_jobs = sum(len(o.get("jobs") or []) for o in opps)
            confirmed_jobs = sum(
                1 for o in opps for j in (o.get("jobs") or []) if j.get("confirmed")
            )

            # Actual revenue from jobs
            total_actual = 0
            jobs_with_actual = 0
            for o in opps:
                for j in (o.get("jobs") or []):
                    act = (j.get("actualCharges") or {}).get("finalTotal")
                    if act and act > 0:
                        total_actual += act
                        jobs_with_actual += 1

            # Status breakdown
            status_counts = Counter(STATUS_MAP.get(o.get("status"), f"({o.get('status')})") for o in opps)

            # By branch
            branch_rev = defaultdict(float)
            branch_count = defaultdict(int)
            for o in opps:
                branch = (o.get("branch") or {}).get("name") or o.get("branchName") or "Unknown"
                branch_rev[branch] += (o.get("estimatedTotal") or {}).get("finalTotal", 0)
                branch_count[branch] += 1

            # By sales rep
            rep_rev = defaultdict(float)
            rep_count = defaultdict(int)
            for o in opps:
                rep = (o.get("salesAssignee") or {}).get("name") or "Unassigned"
                rep_rev[rep] += (o.get("estimatedTotal") or {}).get("finalTotal", 0)
                rep_count[rep] += 1

            # By referral source
            ref_rev = defaultdict(float)
            ref_count = defaultdict(int)
            for o in opps:
                ref = o.get("referralSource") or "Unknown"
                ref_rev[ref] += (o.get("estimatedTotal") or {}).get("finalTotal", 0)
                ref_count[ref] += 1

            # Average job size
            avg_est = total_estimated / len(opps) if opps else 0

            print(f"\n  REVENUE & JOBS")
            print(f"  {'─'*40}")
            print(f"    Opportunities:       {len(opps)}")
            print(f"    Estimated Revenue:   {format_money(total_estimated)}")
            print(f"    Avg Job Size:        {format_money(avg_est)}")
            print(f"    Total Jobs:          {total_jobs}")
            print(f"    Confirmed Jobs:      {confirmed_jobs}")
            if jobs_with_actual > 0:
                print(f"    Actual Revenue:      {format_money(total_actual)} ({jobs_with_actual} jobs)")

            print(f"\n  STATUS BREAKDOWN")
            print(f"  {'─'*40}")
            for status, count in status_counts.most_common():
                print(f"    {status:<20} {count:>5}")

            if len(branch_rev) > 1:
                print(f"\n  BY BRANCH")
                print(f"  {'─'*40}")
                for branch in sorted(branch_rev, key=branch_rev.get, reverse=True):
                    print(f"    {branch:<35} {branch_count[branch]:>4} opps  {format_money(branch_rev[branch]):>12}")

            print(f"\n  BY SALES REP")
            print(f"  {'─'*40}")
            for rep in sorted(rep_rev, key=rep_rev.get, reverse=True):
                print(f"    {rep:<25} {rep_count[rep]:>4} opps  {format_money(rep_rev[rep]):>12}")

            print(f"\n  BY REFERRAL SOURCE")
            print(f"  {'─'*40}")
            for ref in sorted(ref_rev, key=ref_rev.get, reverse=True)[:10]:
                print(f"    {ref:<25} {ref_count[ref]:>4} opps  {format_money(ref_rev[ref]):>12}")

        else:
            print(f"\n  No opportunities found with service dates in range.")
            print(f"  (Scanned {scanned} customers)")
    else:
        print("\n  ⚠  No customer data in DuckDB. Run the customer sync first:")
        print("     python3 scripts/ingest_smartmoving.py --table customers")

    print(f"\n{'='*70}")


if __name__ == "__main__":
    main()
