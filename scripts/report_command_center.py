"""
Page 1: Command Center (The Scoreboard)
Answers: "Are we making money today?"

Data sources:
  - SmartMoving API: leads, opportunities (via customer scan)
  - QuickBooks CSV: current month P&L for margin/profit context

Usage:
    python3 scripts/report_command_center.py              # today
    python3 scripts/report_command_center.py --days 7     # last 7 days
    python3 scripts/report_command_center.py --mtd        # month to date
"""

import sys
import os
import csv
import argparse
import time
from datetime import datetime, timedelta
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_api import get_all, api_get, get_opportunity_detail

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPORT_DIR = os.path.join(PROJECT_ROOT, "exports")
DB_PATH = os.path.join(PROJECT_ROOT, "db", "mme_dashboard.duckdb")

STATUS_MAP = {
    0: "New", 1: "Estimated", 2: "Follow Up", 3: "Booked",
    5: "Confirmed", 10: "Completed", 11: "Closed",
    20: "Lost", 30: "Cancelled",
}
BOOKED = {3, 5, 10, 11}
COMPLETED = {10, 11}


def date_int(dt):
    return int(dt.strftime("%Y%m%d"))


def fmt(amount):
    if amount is None:
        return "$0"
    if amount >= 1000:
        return f"${amount:,.0f}"
    return f"${amount:,.2f}"


def parse_amount(s):
    if not s:
        return 0.0
    return float(s.strip().replace("$", "").replace(",", "").replace("(", "-").replace(")", ""))


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


def get_qb_monthly_context():
    """Get current month financial context from QB P&L."""
    pl_path = next((os.path.join(EXPORT_DIR, f) for f in os.listdir(EXPORT_DIR) if "profit" in f.lower() and "loss" in f.lower() and f.endswith(".csv")), None)
    if not os.path.exists(pl_path):
        return None

    now = datetime.now()
    current_month = now.strftime("%Y-%m")
    last_month = (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

    monthly = defaultdict(lambda: {"revenue": 0, "cogs": 0})

    current_account = None
    with open(pl_path) as f:
        for row in csv.reader(f):
            if not row:
                continue
            if row[0].strip() and (len(row) < 2 or not row[1].strip()):
                name = row[0].strip()
                if not name.startswith("Total for") and not name.startswith("Ordinary"):
                    current_account = name
            elif len(row) > 9 and row[1].strip() and current_account:
                try:
                    dt = datetime.strptime(row[1].strip(), "%m/%d/%Y")
                    mk = dt.strftime("%Y-%m")
                    amt = parse_amount(row[9])
                    if current_account.startswith("4"):
                        monthly[mk]["revenue"] += amt
                    elif current_account.startswith("5"):
                        monthly[mk]["cogs"] += amt
                except (ValueError, IndexError):
                    pass

    return {
        "current": monthly.get(current_month, {"revenue": 0, "cogs": 0}),
        "last": monthly.get(last_month, {"revenue": 0, "cogs": 0}),
        "current_month": current_month,
        "last_month": last_month,
    }


def indicator(actual, target, direction=">="):
    """Return colored status indicator."""
    if direction == ">=" and actual >= target:
        return "OK"
    elif direction == "<=" and actual <= target:
        return "OK"
    elif direction == ">=" and actual >= target * 0.9:
        return "WARN"
    elif direction == "<=" and actual <= target * 1.1:
        return "WARN"
    return "MISS"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--mtd", action="store_true")
    parser.add_argument("--sample", type=int, default=5000)
    parser.add_argument("--full-scan", action="store_true")
    args = parser.parse_args()

    now = datetime.now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if args.mtd:
        date_from = date_int(today.replace(day=1))
        period_label = f"Month to Date ({today.strftime('%B %Y')})"
    else:
        date_from = date_int(today - timedelta(days=args.days - 1))
        period_label = "Today" if args.days == 1 else f"Last {args.days} Days"

    date_to = date_int(today)

    # ── Header ──
    print(f"\n{'='*70}")
    print(f"  COMMAND CENTER — {now.strftime('%B %d, %Y %I:%M %p')}")
    print(f"  Period: {period_label}")
    print(f"{'='*70}")

    # ── 1. Leads (live API) ──
    leads = get_all("/leads")
    leads_today = [l for l in leads if l.get("serviceDate") and date_from <= l["serviceDate"] <= date_to]
    lead_sources = Counter(l.get("referralSourceName", "?") for l in leads)

    # ── 2. Opportunity scan ──
    customer_ids = get_customer_ids()
    opps = []
    scanned = 0

    if customer_ids:
        max_cust = None if args.full_scan else args.sample
        if max_cust:
            customer_ids = customer_ids[:max_cust]

        print(f"\n  Scanning {len(customer_ids)} customers...")
        for i, cust_id in enumerate(customer_ids):
            if (i + 1) % 1000 == 0:
                print(f"    ...{i+1}/{len(customer_ids)}, {len(opps)} opps found")
            try:
                result = api_get(f"/customers/{cust_id}/opportunities")
            except Exception:
                continue
            for opp_s in result.get("pageResults", []):
                if date_from <= opp_s.get("serviceDate", 0) <= date_to:
                    try:
                        opps.append(get_opportunity_detail(opp_s["id"]))
                    except Exception:
                        opps.append(opp_s)
            scanned += 1
            time.sleep(0.15)

    # Calculate metrics from opportunities
    est_revenue_booked = sum(
        (o.get("estimatedTotal") or {}).get("finalTotal", 0)
        for o in opps if o.get("status") in BOOKED
    )
    est_revenue_all = sum(
        (o.get("estimatedTotal") or {}).get("finalTotal", 0)
        for o in opps
    )
    jobs_total = sum(len(o.get("jobs") or []) for o in opps)
    jobs_confirmed = sum(1 for o in opps for j in (o.get("jobs") or []) if j.get("confirmed"))
    opps_booked = sum(1 for o in opps if o.get("status") in BOOKED)
    opps_total = len(opps)
    booking_rate = (opps_booked / opps_total * 100) if opps_total else 0
    avg_job_size = (est_revenue_booked / opps_booked) if opps_booked else 0
    estimates_outstanding = sum(1 for o in opps if o.get("status") in {1, 2})

    # Status breakdown
    status_counts = Counter(STATUS_MAP.get(o.get("status"), "?") for o in opps)

    # ── 3. QuickBooks context ──
    qb = get_qb_monthly_context()

    # ── Output: Scoreboard ──
    print(f"\n  {'METRIC':<40} {'VALUE':>14} {'TARGET':>10} {'STATUS':>8}")
    print(f"  {'─'*40} {'─'*14} {'─'*10} {'─'*8}")

    # Revenue
    print(f"  {'Revenue Booked (period)':<40} {fmt(est_revenue_booked):>14}")
    print(f"  {'Revenue All Opps (period)':<40} {fmt(est_revenue_all):>14}")

    # Jobs
    print(f"  {'Opportunities':<40} {opps_total:>14}")
    print(f"  {'  Booked':<40} {opps_booked:>14}")
    print(f"  {'  Estimates Outstanding':<40} {estimates_outstanding:>14}")
    print(f"  {'Jobs (total / confirmed)':<40} {f'{jobs_total} / {jobs_confirmed}':>14}")
    print(f"  {'Average Job Size':<40} {fmt(avg_job_size):>14}")

    # Booking Rate
    bk_status = indicator(booking_rate, 25, ">=")
    print(f"  {'Booking Rate':<40} {f'{booking_rate:.0f}%':>14} {'≥ 25%':>10} {'['+bk_status+']':>8}")

    # Leads
    print(f"  {'Active Leads':<40} {len(leads):>14}")
    print(f"  {'Leads w/ Service Date in Period':<40} {len(leads_today):>14}")

    # QB financials if available
    if qb and qb["current"]["revenue"] > 0:
        cm = qb["current"]
        rev = cm["revenue"]
        cogs = cm["cogs"]
        gp = rev - cogs
        margin = (gp / rev * 100) if rev else 0
        margin_status = indicator(margin, 45, ">=")

        print(f"\n  {'─'*40} {'─'*14} {'─'*10} {'─'*8}")
        print(f"  {'Revenue MTD (QuickBooks)':<40} {fmt(rev):>14}")
        print(f"  {'COGS MTD':<40} {fmt(cogs):>14}")
        print(f"  {'Gross Profit MTD':<40} {fmt(gp):>14}")
        print(f"  {'Gross Margin MTD':<40} {f'{margin:.1f}%':>14} {'≥ 45%':>10} {'['+margin_status+']':>8}")

        # Last month comparison
        lm = qb["last"]
        if lm["revenue"] > 0:
            lm_rev = lm["revenue"]
            lm_gp = lm_rev - lm["cogs"]
            lm_margin = (lm_gp / lm_rev * 100) if lm_rev else 0
            print(f"\n  {'Last Month ({})'.format(qb['last_month']):<40}")
            print(f"  {'  Revenue':<40} {fmt(lm_rev):>14}")
            print(f"  {'  Gross Profit':<40} {fmt(lm_gp):>14}")
            print(f"  {'  Gross Margin':<40} {f'{lm_margin:.1f}%':>14}")

    # Status breakdown
    print(f"\n  PIPELINE STATUS")
    print(f"  {'─'*40}")
    for status in ["Booked", "Confirmed", "Completed", "Closed", "Estimated", "Follow Up", "New", "Lost", "Cancelled"]:
        if status_counts.get(status, 0) > 0:
            print(f"    {status:<25} {status_counts[status]:>5}")

    # Lead sources
    print(f"\n  LEAD SOURCES")
    print(f"  {'─'*40}")
    for source, count in lead_sources.most_common():
        bar = "█" * count
        print(f"    {source:<20} {count:>3}  {bar}")

    # Urgent leads
    urgent = [l for l in leads
              if l.get("serviceDate") and
              0 <= (datetime.strptime(str(l["serviceDate"]), "%Y%m%d") - today).days <= 3]
    if urgent:
        print(f"\n  URGENT — Service date within 3 days:")
        for l in sorted(urgent, key=lambda x: x.get("serviceDate", 0)):
            days = (datetime.strptime(str(l["serviceDate"]), "%Y%m%d") - today).days
            label = "TODAY" if days == 0 else "TOMORROW" if days == 1 else f"in {days} days"
            print(f"    • {l.get('customerName')} — {label}")

    # Sample note
    if customer_ids and not args.full_scan:
        extrapolation = opps_total * (36208 / len(customer_ids)) if customer_ids else 0
        print(f"\n  Note: Scanned {len(customer_ids)} of 36,208 customers.")
        print(f"  Estimated total opportunities in period: ~{extrapolation:.0f}")
        print(f"  Run with --full-scan for complete data.")

    print(f"\n{'='*70}")


if __name__ == "__main__":
    main()
