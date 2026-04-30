"""
Page 5: Sales Performance
Answers: "Are reps booking profitable work at accurate estimates?"

Data source: DuckDB (synced from SmartMoving API)

Usage:
    python3 scripts/report_sales_p5.py
"""

import os
import sys
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

    print(f"\n{'='*90}")
    print(f"  SALES PERFORMANCE — Page 5")
    print(f"  Generated: {datetime.now().strftime('%B %d, %Y %I:%M %p')}")
    print(f"  Data: {opp_count} opportunities in DuckDB")
    print(f"{'='*90}")

    # Revenue per rep
    reps = con.execute("""
        SELECT
            COALESCE(sales_assignee, 'Unassigned') as rep,
            COUNT(*) as total_opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            SUM(CASE WHEN status = 20 THEN 1 ELSE 0 END) as lost,
            SUM(CASE WHEN status = 30 THEN 1 ELSE 0 END) as cancelled,
            SUM(CASE WHEN status IN (1,2) THEN 1 ELSE 0 END) as pending,
            SUM(estimated_total) as total_est,
            SUM(CASE WHEN status IN (3,5,10,11) THEN estimated_total ELSE 0 END) as booked_rev,
            AVG(CASE WHEN status IN (3,5,10,11) AND estimated_total > 0 THEN estimated_total END) as avg_job
        FROM opportunities
        GROUP BY COALESCE(sales_assignee, 'Unassigned')
        ORDER BY booked_rev DESC NULLS LAST
    """).fetchall()

    print(f"\n  {'Rep':<22} {'Opps':>5} {'Booked':>7} {'Book%':>6} {'Lost':>5} {'Cxl':>5} {'Booked Rev':>12} {'Avg Job':>10}")
    print(f"  {'-'*22} {'-'*5} {'-'*7} {'-'*6} {'-'*5} {'-'*5} {'-'*12} {'-'*10}")

    totals = [0, 0, 0, 0, 0, 0.0, 0.0]
    for rep, total, booked, lost, cancelled, pending, est, booked_rev, avg_job in reps:
        rate = (booked / total * 100) if total > 0 else 0
        booked_rev = booked_rev or 0
        avg_job = avg_job or 0
        print(f"  {rep:<22} {total:>5} {booked:>7} {rate:>5.0f}% {lost:>5} {cancelled:>5} {fmt(booked_rev):>12} {fmt(avg_job):>10}")
        totals[0] += total
        totals[1] += booked
        totals[2] += lost
        totals[3] += cancelled
        totals[4] += pending
        totals[5] += booked_rev
        totals[6] = totals[5] / totals[1] if totals[1] > 0 else 0

    overall_rate = (totals[1] / totals[0] * 100) if totals[0] > 0 else 0
    print(f"  {'-'*22} {'-'*5} {'-'*7} {'-'*6} {'-'*5} {'-'*5} {'-'*12} {'-'*10}")
    print(f"  {'TOTAL':<22} {totals[0]:>5} {totals[1]:>7} {overall_rate:>5.0f}% {totals[2]:>5} {totals[3]:>5} {fmt(totals[5]):>12} {fmt(totals[6]):>10}")

    # Booking rate target check
    bk_status = "OK" if overall_rate >= 25 else "MISS"
    print(f"\n  Booking Rate: {overall_rate:.0f}% (target ≥ 25%) [{bk_status}]")

    # Cancellation rate by rep
    print(f"\n  CANCELLATION RATE BY REP")
    print(f"  {'─'*50}")
    for rep, total, booked, lost, cancelled, pending, est, booked_rev, avg_job in reps:
        if total > 5:
            cxl_rate = (cancelled / total * 100) if total > 0 else 0
            if cxl_rate > 0:
                print(f"    {rep:<25} {cxl_rate:.0f}% ({cancelled} of {total})")

    # Revenue by referral source
    sources = con.execute("""
        SELECT
            COALESCE(referral_source, 'Unknown') as source,
            COUNT(*) as opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            SUM(CASE WHEN status IN (3,5,10,11) THEN estimated_total ELSE 0 END) as booked_rev
        FROM opportunities
        GROUP BY COALESCE(referral_source, 'Unknown')
        HAVING booked > 0
        ORDER BY booked_rev DESC NULLS LAST
        LIMIT 15
    """).fetchall()

    print(f"\n  REVENUE BY REFERRAL SOURCE (Top 15)")
    print(f"  {'─'*65}")
    print(f"  {'Source':<25} {'Opps':>6} {'Booked':>7} {'Book%':>6} {'Revenue':>12}")
    print(f"  {'-'*25} {'-'*6} {'-'*7} {'-'*6} {'-'*12}")
    for source, opps, booked, rev in sources:
        rate = (booked / opps * 100) if opps > 0 else 0
        print(f"  {source:<25} {opps:>6} {booked:>7} {rate:>5.0f}% {fmt(rev or 0):>12}")

    # Pipeline status
    pipeline = con.execute("""
        SELECT
            CASE status
                WHEN 0 THEN 'New'
                WHEN 1 THEN 'Estimated'
                WHEN 2 THEN 'Follow Up'
                WHEN 3 THEN 'Booked'
                WHEN 5 THEN 'Confirmed'
                WHEN 10 THEN 'Completed'
                WHEN 11 THEN 'Closed'
                WHEN 20 THEN 'Lost'
                WHEN 30 THEN 'Cancelled'
                ELSE 'Other'
            END as status_name,
            COUNT(*) as cnt,
            SUM(estimated_total) as est_rev
        FROM opportunities
        GROUP BY status
        ORDER BY cnt DESC
    """).fetchall()

    print(f"\n  PIPELINE BY STATUS")
    print(f"  {'─'*50}")
    print(f"  {'Status':<20} {'Count':>8} {'Est Revenue':>14}")
    print(f"  {'-'*20} {'-'*8} {'-'*14}")
    for status_name, cnt, rev in pipeline:
        print(f"  {status_name:<20} {cnt:>8} {fmt(rev or 0):>14}")

    con.close()
    print(f"\n{'='*90}")


if __name__ == "__main__":
    main()
