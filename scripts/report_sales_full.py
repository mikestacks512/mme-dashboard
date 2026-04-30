"""
Sales Performance Report — from SmartMoving sales-person-performance export.

Usage:
    python3 scripts/report_sales_full.py
"""

import os
import csv
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPORT_DIR = os.path.join(PROJECT_ROOT, "exports")


def parse_amount(s):
    if not s:
        return 0.0
    s = s.strip().replace("$", "").replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def main():
    path = os.path.join(EXPORT_DIR, "sales-person-performance-report.csv")
    if not os.path.exists(path):
        print("ERROR: sales-person-performance-report.csv not found")
        return

    with open(path) as f:
        reader = csv.DictReader(f)
        reps = list(reader)

    # Estimate accuracy
    est_path = os.path.join(EXPORT_DIR, "estimate-accuracy-summary-report.csv")
    est_data = {}
    if os.path.exists(est_path):
        with open(est_path) as f:
            for row in csv.DictReader(f):
                name = row.get("NAME", "")
                if name and name != "NAME":
                    est_data[name] = row

    print(f"\n{'='*90}")
    print(f"  SALES PERFORMANCE REPORT (from SmartMoving)")
    print(f"  Generated: {datetime.now().strftime('%B %d, %Y %I:%M %p')}")
    print(f"{'='*90}")

    # Summary table
    print(f"\n  {'Rep':<22} {'Leads':>6} {'Booked':>7} {'Book%':>6} {'Lost':>5} {'Bad':>5} {'Revenue':>14} {'Avg Book':>10}")
    print(f"  {'-'*22} {'-'*6} {'-'*7} {'-'*6} {'-'*5} {'-'*5} {'-'*14} {'-'*10}")

    total_leads = 0
    total_booked = 0
    total_rev = 0

    for rep in sorted(reps, key=lambda r: parse_amount(r.get("BOOKED TOTAL", "")), reverse=True):
        name = rep.get("NAME", "")[:21]
        leads = rep.get("# LEADS RECEIVED", "--")
        booked = rep.get("# Booked", "--")
        book_pct = rep.get("% Booked", "--")
        lost = rep.get("# Lost", "--")
        bad = rep.get("# Bad", "--")
        revenue = rep.get("BOOKED TOTAL", "$0")
        avg = rep.get("AVERAGE BOOKING", "$0")

        print(f"  {name:<22} {leads:>6} {booked:>7} {book_pct:>6} {lost:>5} {bad:>5} {revenue:>14} {avg:>10}")

        if leads != "--":
            total_leads += int(leads)
        if booked != "--":
            total_booked += int(booked)
        total_rev += parse_amount(revenue)

    overall_rate = (total_booked / total_leads * 100) if total_leads else 0
    overall_avg = (total_rev / total_booked) if total_booked else 0
    print(f"  {'-'*22} {'-'*6} {'-'*7} {'-'*6} {'-'*5} {'-'*5} {'-'*14} {'-'*10}")
    print(f"  {'TOTAL':<22} {total_leads:>6} {total_booked:>7} {overall_rate:>5.0f}% {'':>5} {'':>5} ${total_rev:>13,.2f} ${overall_avg:>9,.2f}")

    # Estimate accuracy
    if est_data:
        print(f"\n  ESTIMATE ACCURACY BY REP")
        print(f"  {'─'*75}")
        print(f"  {'Rep':<22} {'Avg $ Over/Under':>18} {'Avg %':>8} {'Time Over/Under':>18} {'Assessment'}")
        print(f"  {'-'*22} {'-'*18} {'-'*8} {'-'*18} {'-'*15}")
        for name, row in sorted(est_data.items(), key=lambda x: abs(parse_amount(x[1].get("AVERAGE $ OVER/UNDER", ""))), reverse=True):
            dollar = row.get("AVERAGE $ OVER/UNDER", "")
            pct = row.get("AVERAGE $ OVER/UNDER (%)", "")
            time_ou = row.get("AVERAGE TIME OVER/UNDER", "")
            pct_val = int(pct.replace("%", "")) if pct.replace("%", "").replace("-", "").isdigit() else 0
            if abs(pct_val) <= 5:
                assessment = "Accurate"
            elif pct_val > 10:
                assessment = "Over-estimates"
            elif pct_val < -10:
                assessment = "Under-estimates"
            else:
                assessment = "Slightly off"
            print(f"  {name:<22} {dollar:>18} {pct:>8} {time_ou:>18} {assessment}")

    print(f"\n{'='*90}")


if __name__ == "__main__":
    main()
