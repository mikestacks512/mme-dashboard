"""
Sales Performance Report — per-rep metrics from SmartMoving.

Scans opportunities to build rep-level performance stats.

Usage:
    python3 scripts/report_sales.py                # last 30 days, sample
    python3 scripts/report_sales.py --days 90      # last 90 days
    python3 scripts/report_sales.py --full-scan     # all customers (slow)
"""

import sys
import os
import argparse
import time
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_api import api_get, get_opportunity_detail

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "mme_dashboard.duckdb")

STATUS_MAP = {
    0: "New", 1: "Estimated", 2: "Follow Up", 3: "Booked",
    5: "Confirmed", 10: "Completed", 11: "Closed",
    20: "Lost", 30: "Cancelled",
}

BOOKED_STATUSES = {3, 5, 10, 11}  # Booked, Confirmed, Completed, Closed
ESTIMATED_STATUSES = {1, 2}  # Estimated, Follow Up


def date_int(dt):
    return int(dt.strftime("%Y%m%d"))


def format_money(amount):
    if amount is None or amount == 0:
        return "$0"
    return f"${amount:,.0f}"


def get_customer_ids():
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--full-scan", action="store_true")
    parser.add_argument("--sample", type=int, default=5000)
    args = parser.parse_args()

    now = datetime.now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    date_from = date_int(today - timedelta(days=args.days))
    date_to = date_int(today)

    customer_ids = get_customer_ids()
    if not customer_ids:
        print("No customer data. Run: python3 scripts/ingest_smartmoving.py --table customers")
        return

    max_cust = None if args.full_scan else args.sample
    if max_cust:
        customer_ids = customer_ids[:max_cust]

    print(f"Scanning {len(customer_ids)} customers for last {args.days} days...\n")

    # Collect all opportunities in date range
    reps = defaultdict(lambda: {
        "estimated_count": 0, "booked_count": 0, "lost_count": 0, "cancelled_count": 0,
        "estimated_revenue": 0.0, "booked_revenue": 0.0,
        "actual_revenue": 0.0, "jobs_with_actual": 0,
        "total_jobs": 0,
    })

    opp_count = 0
    for i, cust_id in enumerate(customer_ids):
        if (i + 1) % 500 == 0:
            print(f"  Scanning... {i+1}/{len(customer_ids)}, {opp_count} opps found")
        try:
            opps = api_get(f"/customers/{cust_id}/opportunities")
        except Exception:
            continue

        for opp_summary in opps.get("pageResults", []):
            svc_date = opp_summary.get("serviceDate", 0)
            if not (date_from <= svc_date <= date_to):
                continue

            try:
                opp = get_opportunity_detail(opp_summary["id"])
            except Exception:
                continue

            opp_count += 1
            rep_name = (opp.get("salesAssignee") or {}).get("name") or "Unassigned"
            rep = reps[rep_name]
            est_total = (opp.get("estimatedTotal") or {}).get("finalTotal", 0) or 0
            status = opp.get("status", 0)

            rep["estimated_count"] += 1
            rep["estimated_revenue"] += est_total

            if status in BOOKED_STATUSES:
                rep["booked_count"] += 1
                rep["booked_revenue"] += est_total
            elif status == 20:
                rep["lost_count"] += 1
            elif status == 30:
                rep["cancelled_count"] += 1

            for j in opp.get("jobs") or []:
                rep["total_jobs"] += 1
                act = (j.get("actualCharges") or {}).get("finalTotal")
                if act and act > 0:
                    rep["actual_revenue"] += act
                    rep["jobs_with_actual"] += 1

        time.sleep(0.15)

    # ── Output ──
    print(f"\n{'='*90}")
    print(f"  SALES PERFORMANCE — Last {args.days} Days ({now.strftime('%B %d, %Y')})")
    print(f"{'='*90}")
    print(f"  Total Opportunities: {opp_count} | Customers Scanned: {len(customer_ids)}")

    print(f"\n  {'Rep':<22} {'Opps':>5} {'Booked':>7} {'Book%':>6} {'Lost':>5} {'Est Rev':>12} {'Booked Rev':>12} {'Avg Size':>10}")
    print(f"  {'-'*22} {'-'*5} {'-'*7} {'-'*6} {'-'*5} {'-'*12} {'-'*12} {'-'*10}")

    for rep_name in sorted(reps, key=lambda r: reps[r]["booked_revenue"], reverse=True):
        r = reps[rep_name]
        book_rate = (r["booked_count"] / r["estimated_count"] * 100) if r["estimated_count"] > 0 else 0
        avg_size = r["booked_revenue"] / r["booked_count"] if r["booked_count"] > 0 else 0

        print(f"  {rep_name:<22} {r['estimated_count']:>5} {r['booked_count']:>7} {book_rate:>5.0f}% {r['lost_count']:>5} "
              f"{format_money(r['estimated_revenue']):>12} {format_money(r['booked_revenue']):>12} {format_money(avg_size):>10}")

    # Summary
    totals = {k: sum(r[k] for r in reps.values()) for k in ["estimated_count", "booked_count", "lost_count", "estimated_revenue", "booked_revenue"]}
    overall_rate = (totals["booked_count"] / totals["estimated_count"] * 100) if totals["estimated_count"] > 0 else 0
    overall_avg = totals["booked_revenue"] / totals["booked_count"] if totals["booked_count"] > 0 else 0

    print(f"  {'-'*22} {'-'*5} {'-'*7} {'-'*6} {'-'*5} {'-'*12} {'-'*12} {'-'*10}")
    print(f"  {'TOTAL':<22} {totals['estimated_count']:>5} {totals['booked_count']:>7} {overall_rate:>5.0f}% {totals['lost_count']:>5} "
          f"{format_money(totals['estimated_revenue']):>12} {format_money(totals['booked_revenue']):>12} {format_money(overall_avg):>10}")

    print(f"\n{'='*90}")


if __name__ == "__main__":
    main()
