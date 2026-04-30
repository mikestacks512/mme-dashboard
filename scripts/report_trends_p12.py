"""
Page 12: Weekly/Monthly Trends
Answers: "Where are we heading?"

Data sources:
  - DuckDB: opportunity and job trends from SmartMoving
  - QuickBooks P&L: monthly revenue and cost trends

Usage:
    python3 scripts/report_trends_p12.py
"""

import os
import csv
from datetime import datetime, timedelta
from collections import defaultdict

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "mme_dashboard.duckdb")
EXPORT_DIR = os.path.join(PROJECT_ROOT, "exports")

import duckdb


def fmt(v):
    if v is None:
        return "$0"
    return f"${v:,.0f}" if abs(v) >= 1000 else f"${v:,.2f}"


def load_qb_monthly():
    pl_path = next((os.path.join(EXPORT_DIR, f) for f in os.listdir(EXPORT_DIR) if "profit" in f.lower() and "loss" in f.lower() and f.endswith(".csv")), None)
    if not os.path.exists(pl_path):
        return {}
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
                    amt = float(row[9].replace("$", "").replace(",", ""))
                    if current_account.startswith("4"):
                        monthly[mk]["revenue"] += amt
                    elif current_account.startswith("5"):
                        monthly[mk]["cogs"] += amt
                    elif current_account.startswith("6"):
                        monthly[mk].setdefault("overhead", 0)
                        monthly[mk]["overhead"] += amt
                except (ValueError, IndexError):
                    pass
    return monthly


def main():
    if not os.path.exists(DB_PATH):
        print("ERROR: DuckDB not found.")
        return

    con = duckdb.connect(DB_PATH, read_only=True)
    qb = load_qb_monthly()

    print(f"\n{'='*85}")
    print(f"  WEEKLY / MONTHLY TRENDS — Page 12")
    print(f"  Generated: {datetime.now().strftime('%B %d, %Y %I:%M %p')}")
    print(f"{'='*85}")

    # Monthly opportunity trends from DuckDB
    monthly_opps = con.execute("""
        SELECT
            SUBSTR(CAST(service_date AS VARCHAR), 1, 6) as month,
            COUNT(*) as opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            SUM(CASE WHEN status = 20 THEN 1 ELSE 0 END) as lost,
            SUM(CASE WHEN status = 30 THEN 1 ELSE 0 END) as cancelled,
            SUM(estimated_total) as est_rev,
            AVG(CASE WHEN estimated_total > 0 THEN estimated_total END) as avg_job
        FROM opportunities
        GROUP BY SUBSTR(CAST(service_date AS VARCHAR), 1, 6)
        ORDER BY month
    """).fetchall()

    if monthly_opps:
        print(f"\n  MONTHLY OPPORTUNITY TRENDS (SmartMoving)")
        print(f"  {'─'*80}")
        print(f"  {'Month':<10} {'Opps':>6} {'Booked':>7} {'Book%':>6} {'Lost':>5} {'Cxl':>5} {'Est Rev':>12} {'Avg Job':>10}")
        print(f"  {'-'*10} {'-'*6} {'-'*7} {'-'*6} {'-'*5} {'-'*5} {'-'*12} {'-'*10}")

        for month, opps, booked, lost, cancelled, rev, avg in monthly_opps:
            m_str = f"{month[:4]}-{month[4:]}"
            rate = (booked / opps * 100) if opps > 0 else 0
            print(f"  {m_str:<10} {opps:>6} {booked:>7} {rate:>5.0f}% {lost:>5} {cancelled:>5} {fmt(rev or 0):>12} {fmt(avg or 0):>10}")

    # QB monthly P&L trend
    if qb:
        print(f"\n  MONTHLY P&L TREND (QuickBooks)")
        print(f"  {'─'*80}")
        print(f"  {'Month':<10} {'Revenue':>12} {'COGS':>12} {'Gross Profit':>12} {'Margin':>8} {'vs 45%':>8} {'Status':>8}")
        print(f"  {'-'*10} {'-'*12} {'-'*12} {'-'*12} {'-'*8} {'-'*8} {'-'*8}")

        prev_rev = None
        for month in sorted(qb.keys()):
            d = qb[month]
            rev = d["revenue"]
            cogs = d["cogs"]
            gp = rev - cogs
            margin = (gp / rev * 100) if rev else 0
            gap = margin - 45
            st = "OK" if margin >= 45 else "WARN" if margin >= 40 else "MISS"

            # MoM change
            if prev_rev and prev_rev > 0:
                change = (rev - prev_rev) / prev_rev * 100
                change_str = f"{change:+.0f}%"
            else:
                change_str = ""
            prev_rev = rev

            print(f"  {month:<10} {fmt(rev):>12} {fmt(cogs):>12} {fmt(gp):>12} {margin:>6.1f}% {gap:>+6.1f}% [{st}]  {change_str}")

    # Weekly trends from DuckDB
    weekly = con.execute("""
        SELECT
            CAST(service_date / 100 AS INTEGER) * 100 +
            CASE WHEN service_date % 100 <= 7 THEN 1
                 WHEN service_date % 100 <= 14 THEN 2
                 WHEN service_date % 100 <= 21 THEN 3
                 ELSE 4 END as week_bucket,
            COUNT(*) as opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            SUM(estimated_total) as rev
        FROM opportunities
        WHERE service_date >= 20260301
        GROUP BY week_bucket
        ORDER BY week_bucket
    """).fetchall()

    if weekly:
        print(f"\n  RECENT WEEKLY TREND (2026)")
        print(f"  {'─'*50}")
        print(f"  {'Week':<15} {'Opps':>6} {'Booked':>7} {'Est Rev':>12}")
        print(f"  {'-'*15} {'-'*6} {'-'*7} {'-'*12}")
        for wb, opps, booked, rev in weekly:
            month = wb // 100
            week = wb % 100
            label = f"{month//100}-{month%100:02d} Wk{week}"
            print(f"  {label:<15} {opps:>6} {booked:>7} {fmt(rev or 0):>12}")

    # Referral source trend
    source_trend = con.execute("""
        SELECT
            SUBSTR(CAST(service_date AS VARCHAR), 1, 6) as month,
            COALESCE(referral_source, 'Unknown') as source,
            COUNT(*) as leads,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked
        FROM opportunities
        GROUP BY month, source
        HAVING leads >= 3
        ORDER BY month, leads DESC
    """).fetchall()

    if source_trend:
        # Show top sources per month
        print(f"\n  TOP LEAD SOURCES BY MONTH")
        print(f"  {'─'*60}")
        current_month = None
        for month, source, leads, booked in source_trend:
            m_str = f"{month[:4]}-{month[4:]}"
            if m_str != current_month:
                current_month = m_str
                print(f"\n  {m_str}:")
            rate = (booked / leads * 100) if leads else 0
            print(f"    {source:<25} {leads:>4} leads, {booked:>3} booked ({rate:.0f}%)")

    con.close()
    print(f"\n{'='*85}")


if __name__ == "__main__":
    main()
