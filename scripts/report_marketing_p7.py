"""
Page 7: Marketing Performance
Answers: "Are marketing channels producing profitable customers?"

Data sources:
  - DuckDB opportunities: lead source attribution, booking rates, revenue
  - QuickBooks P&L: marketing spend ($189K)

Usage:
    python3 scripts/report_marketing_p7.py
"""

import os
import csv
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "mme_dashboard.duckdb")
EXPORT_DIR = os.path.join(PROJECT_ROOT, "exports")

import duckdb


def fmt(v):
    if v is None:
        return "$0"
    return f"${v:,.0f}" if abs(v) >= 1000 else f"${v:,.2f}"


def get_marketing_spend():
    """Get total marketing spend from QB P&L."""
    pl_path = next((os.path.join(EXPORT_DIR, f) for f in os.listdir(EXPORT_DIR) if "profit" in f.lower() and "loss" in f.lower() and f.endswith(".csv")), None)
    if not os.path.exists(pl_path):
        return 0
    with open(pl_path) as f:
        for row in csv.reader(f):
            if row and row[0] == "Total for 6002 Advertising, Marketing, & Promo" and len(row) > 9:
                return float(row[9].replace("$", "").replace(",", ""))
    return 0


def main():
    if not os.path.exists(DB_PATH):
        print("ERROR: DuckDB not found. Run fast_sync.py first.")
        return

    con = duckdb.connect(DB_PATH, read_only=True)
    marketing_spend = get_marketing_spend()

    print(f"\n{'='*85}")
    print(f"  MARKETING PERFORMANCE — Page 7")
    print(f"  Generated: {datetime.now().strftime('%B %d, %Y %I:%M %p')}")
    print(f"  Marketing Spend (QB): {fmt(marketing_spend)}")
    print(f"{'='*85}")

    # Channel performance
    channels = con.execute("""
        SELECT
            COALESCE(referral_source, 'Unknown') as channel,
            COUNT(*) as leads,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            SUM(CASE WHEN status = 20 THEN 1 ELSE 0 END) as lost,
            SUM(CASE WHEN status = 30 THEN 1 ELSE 0 END) as cancelled,
            SUM(CASE WHEN status IN (3,5,10,11) THEN estimated_total ELSE 0 END) as revenue,
            AVG(CASE WHEN status IN (3,5,10,11) AND estimated_total > 0 THEN estimated_total END) as avg_job
        FROM opportunities
        GROUP BY COALESCE(referral_source, 'Unknown')
        ORDER BY revenue DESC NULLS LAST
    """).fetchall()

    total_leads = sum(c[1] for c in channels)
    total_booked = sum(c[2] for c in channels)
    total_rev = sum(c[5] or 0 for c in channels)

    print(f"\n  CHANNEL PERFORMANCE")
    print(f"  {'─'*80}")
    print(f"  {'Channel':<22} {'Leads':>6} {'Booked':>7} {'Book%':>6} {'Revenue':>12} {'Avg Job':>10} {'ROI':>8}")
    print(f"  {'-'*22} {'-'*6} {'-'*7} {'-'*6} {'-'*12} {'-'*10} {'-'*8}")

    for channel, leads, booked, lost, cancelled, rev, avg_job in channels:
        rate = (booked / leads * 100) if leads > 0 else 0
        rev = rev or 0
        avg_job = avg_job or 0
        # ROI only meaningful for paid channels
        roi_str = ""
        print(f"  {channel:<22} {leads:>6} {booked:>7} {rate:>5.0f}% {fmt(rev):>12} {fmt(avg_job):>10} {roi_str:>8}")

    print(f"  {'-'*22} {'-'*6} {'-'*7} {'-'*6} {'-'*12} {'-'*10}")
    overall_rate = (total_booked / total_leads * 100) if total_leads else 0
    print(f"  {'TOTAL':<22} {total_leads:>6} {total_booked:>7} {overall_rate:>5.0f}% {fmt(total_rev):>12}")

    # Repeat vs new customer analysis
    repeat = next((c for c in channels if c[0] == "Repeat"), None)
    referral = next((c for c in channels if "Referral" in (c[0] or "")), None)

    if repeat or referral:
        print(f"\n  FREE REVENUE (No Marketing Cost)")
        print(f"  {'─'*50}")
        if repeat:
            pct = ((repeat[5] or 0) / total_rev * 100) if total_rev else 0
            print(f"    Repeat Customers:  {fmt(repeat[5] or 0)} revenue ({pct:.0f}% of total)")
            print(f"                       {repeat[2]} booked, {repeat[1]} leads, {repeat[2]/repeat[1]*100:.0f}% booking rate")
        if referral:
            pct = ((referral[5] or 0) / total_rev * 100) if total_rev else 0
            print(f"    Referrals:         {fmt(referral[5] or 0)} revenue ({pct:.0f}% of total)")
            print(f"                       {referral[2]} booked, {referral[1]} leads, {referral[2]/referral[1]*100:.0f}% booking rate")

    # Cost metrics
    if marketing_spend > 0 and total_rev > 0:
        print(f"\n  MARKETING EFFICIENCY")
        print(f"  {'─'*50}")
        print(f"    Total Marketing Spend:     {fmt(marketing_spend)}")
        print(f"    Total Revenue:             {fmt(total_rev)}")
        print(f"    Marketing ROI:             {total_rev / marketing_spend:.1f}x")
        print(f"    Cost Per Lead (blended):   {fmt(marketing_spend / total_leads)}")
        print(f"    Cost Per Booked Job:       {fmt(marketing_spend / total_booked)}")
        cac = marketing_spend / total_booked if total_booked else 0
        cac_status = "OK" if cac < 500 else "WARN" if cac < 750 else "HIGH"
        print(f"    CAC:                       {fmt(cac)} [{cac_status}]")

    con.close()
    print(f"\n{'='*85}")


if __name__ == "__main__":
    main()
