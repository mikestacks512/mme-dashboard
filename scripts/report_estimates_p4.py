"""
Page 4: Estimate Accuracy & Pricing Intelligence
Answers: "Are we booking the right jobs at the right prices?"

Data source: DuckDB (synced from SmartMoving API)

Note: SmartMoving API does not expose job-level estimatedCharges/actualCharges.
This report uses opportunity-level estimatedTotal for pricing analysis.
For actual estimate vs actual variance, use SmartMoving's built-in estimate
accuracy report (export CSV from SmartMoving app).

Usage:
    python3 scripts/report_estimates_p4.py
"""

import os
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "mme_dashboard.duckdb")

import duckdb


def fmt(v):
    if v is None:
        return "$0"
    return f"${v:,.0f}" if abs(v) >= 1000 else f"${v:,.2f}"


def main():
    if not os.path.exists(DB_PATH):
        print("ERROR: DuckDB not found. Run fast_sync.py first.")
        return

    con = duckdb.connect(DB_PATH, read_only=True)

    opp_count = con.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
    if opp_count == 0:
        print("ERROR: No opportunities in DuckDB. Run fast_sync.py first.")
        con.close()
        return

    print(f"\n{'='*85}")
    print(f"  ESTIMATE ACCURACY & PRICING INTELLIGENCE — Page 4")
    print(f"  Generated: {datetime.now().strftime('%B %d, %Y %I:%M %p')}")
    print(f"  Data: {opp_count} opportunities in DuckDB")
    print(f"{'='*85}")

    # Pricing Intelligence by revenue bucket
    buckets = con.execute("""
        SELECT
            CASE
                WHEN estimated_total < 500 THEN '1. Under $500'
                WHEN estimated_total < 1000 THEN '2. $500-$1,000'
                WHEN estimated_total < 1500 THEN '3. $1,000-$1,500'
                WHEN estimated_total < 2500 THEN '4. $1,500-$2,500'
                WHEN estimated_total < 3500 THEN '5. $2,500-$3,500'
                ELSE '6. $3,500+'
            END as bucket,
            COUNT(*) as opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            SUM(CASE WHEN status = 20 THEN 1 ELSE 0 END) as lost,
            SUM(CASE WHEN status = 30 THEN 1 ELSE 0 END) as cancelled,
            SUM(estimated_total) as total_rev,
            AVG(estimated_total) as avg_rev,
            SUM(CASE WHEN status IN (3,5,10,11) THEN estimated_total ELSE 0 END) as booked_rev
        FROM opportunities
        WHERE estimated_total > 0
        GROUP BY bucket
        ORDER BY bucket
    """).fetchall()

    print(f"\n  PRICING BY REVENUE BUCKET")
    print(f"  {'─'*80}")
    print(f"  {'Bucket':<18} {'Opps':>6} {'Booked':>7} {'Book%':>6} {'Lost':>5} {'Cxl':>5} {'Revenue':>12} {'Avg Size':>10}")
    print(f"  {'-'*18} {'-'*6} {'-'*7} {'-'*6} {'-'*5} {'-'*5} {'-'*12} {'-'*10}")

    for bucket, opps, booked, lost, cancelled, total_rev, avg, booked_rev in buckets:
        label = bucket[3:]  # strip sort prefix
        rate = (booked / opps * 100) if opps else 0
        print(f"  {label:<18} {opps:>6} {booked:>7} {rate:>5.0f}% {lost:>5} {cancelled:>5} {fmt(booked_rev or 0):>12} {fmt(avg or 0):>10}")

    # Estimate patterns by rep
    by_rep = con.execute("""
        SELECT
            COALESCE(sales_assignee, 'Unassigned') as rep,
            COUNT(*) as opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            AVG(CASE WHEN estimated_total > 0 THEN estimated_total END) as avg_est,
            MIN(CASE WHEN estimated_total > 0 THEN estimated_total END) as min_est,
            MAX(estimated_total) as max_est,
            SUM(CASE WHEN status IN (3,5,10,11) THEN estimated_total ELSE 0 END) as booked_rev
        FROM opportunities
        WHERE estimated_total > 0
        GROUP BY COALESCE(sales_assignee, 'Unassigned')
        ORDER BY booked_rev DESC NULLS LAST
    """).fetchall()

    print(f"\n  ESTIMATE PATTERNS BY REP")
    print(f"  {'─'*80}")
    print(f"  {'Rep':<22} {'Opps':>5} {'Booked':>7} {'Book%':>6} {'Avg Est':>10} {'Min':>8} {'Max':>10} {'Booked Rev':>12}")
    print(f"  {'-'*22} {'-'*5} {'-'*7} {'-'*6} {'-'*10} {'-'*8} {'-'*10} {'-'*12}")

    for rep, opps, booked, avg, mn, mx, booked_rev in by_rep:
        rate = (booked / opps * 100) if opps else 0
        print(f"  {rep:<22} {opps:>5} {booked:>7} {rate:>5.0f}% {fmt(avg or 0):>10} {fmt(mn or 0):>8} {fmt(mx or 0):>10} {fmt(booked_rev or 0):>12}")

    # By move size
    by_size = con.execute("""
        SELECT
            COALESCE(move_size, 'Unknown') as size,
            COUNT(*) as opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            AVG(CASE WHEN estimated_total > 0 THEN estimated_total END) as avg_est,
            SUM(CASE WHEN status IN (3,5,10,11) THEN estimated_total ELSE 0 END) as booked_rev
        FROM opportunities
        WHERE estimated_total > 0
        GROUP BY COALESCE(move_size, 'Unknown')
        HAVING COUNT(*) >= 3
        ORDER BY avg_est DESC NULLS LAST
    """).fetchall()

    print(f"\n  PRICING BY MOVE SIZE (3+ opps)")
    print(f"  {'─'*75}")
    print(f"  {'Move Size':<30} {'Opps':>6} {'Booked':>7} {'Book%':>6} {'Avg Est':>10} {'Booked Rev':>12}")
    print(f"  {'-'*30} {'-'*6} {'-'*7} {'-'*6} {'-'*10} {'-'*12}")

    for size, opps, booked, avg, booked_rev in by_size:
        rate = (booked / opps * 100) if opps else 0
        print(f"  {size:<30} {opps:>6} {booked:>7} {rate:>5.0f}% {fmt(avg or 0):>10} {fmt(booked_rev or 0):>12}")

    # By branch
    by_branch = con.execute("""
        SELECT
            COALESCE(branch_name, 'Unknown') as branch,
            COUNT(*) as opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            AVG(CASE WHEN estimated_total > 0 THEN estimated_total END) as avg_est,
            SUM(CASE WHEN status IN (3,5,10,11) THEN estimated_total ELSE 0 END) as booked_rev
        FROM opportunities
        WHERE estimated_total > 0
        GROUP BY COALESCE(branch_name, 'Unknown')
        ORDER BY booked_rev DESC NULLS LAST
    """).fetchall()

    print(f"\n  PRICING BY BRANCH")
    print(f"  {'─'*75}")
    print(f"  {'Branch':<40} {'Opps':>6} {'Booked':>7} {'Book%':>6} {'Avg Est':>10}")
    print(f"  {'-'*40} {'-'*6} {'-'*7} {'-'*6} {'-'*10}")

    for branch, opps, booked, avg, booked_rev in by_branch:
        rate = (booked / opps * 100) if opps else 0
        print(f"  {branch:<40} {opps:>6} {booked:>7} {rate:>5.0f}% {fmt(avg or 0):>10}")

    # Note about estimate accuracy limitation
    print(f"\n  {'─'*75}")
    print(f"  NOTE: SmartMoving API does not expose job-level estimated vs actual charges.")
    print(f"  For estimate accuracy (est vs actual variance by rep), export the")
    print(f"  'Estimate Accuracy Summary' report from SmartMoving app directly.")

    con.close()
    print(f"\n{'='*85}")


if __name__ == "__main__":
    main()
