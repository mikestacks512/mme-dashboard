"""
Lead Pipeline Report — pulls live from SmartMoving API.

Usage:
    python3 scripts/report_leads.py
"""

import sys
import os
from datetime import datetime, timedelta
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_api import get_all

TODAY = datetime.now().strftime("%Y%m%d")
TODAY_INT = int(datetime.now().strftime("%Y%m%d"))


def format_date(d):
    """Convert SmartMoving date int (20260412) to readable string."""
    if not d:
        return "N/A"
    s = str(d)
    return f"{s[4:6]}/{s[6:8]}/{s[:4]}"


def days_until(service_date_int):
    """Days from today until service date."""
    if not service_date_int:
        return None
    sd = datetime.strptime(str(service_date_int), "%Y%m%d")
    return (sd - datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)).days


def main():
    print("Pulling leads from SmartMoving...\n")
    leads = get_all("/leads")

    if not leads:
        print("No active leads found.")
        return

    # Status mapping (from SmartMoving)
    status_map = {0: "New", 1: "Contacted", 2: "Qualified", 3: "Lost", 10: "Converted"}

    # ── Header ──
    print(f"{'='*70}")
    print(f"  LEAD PIPELINE REPORT — {datetime.now().strftime('%B %d, %Y %I:%M %p')}")
    print(f"{'='*70}")
    print(f"  Active Leads: {len(leads)}")
    print()

    # ── Lead Detail ──
    print(f"{'Customer':<25} {'Source':<12} {'Move Size':<18} {'Service Date':<14} {'Days Out'}")
    print(f"{'-'*25} {'-'*12} {'-'*18} {'-'*14} {'-'*8}")

    for lead in sorted(leads, key=lambda l: l.get("serviceDate", 0)):
        name = (lead.get("customerName") or "Unknown")[:24]
        source = (lead.get("referralSourceName") or "?")[:11]
        move_size = (lead.get("moveSizeName") or "?")[:17]
        svc_date = format_date(lead.get("serviceDate"))
        days = days_until(lead.get("serviceDate"))
        days_str = f"{days}d" if days is not None else "?"
        status = status_map.get(lead.get("status"), f"({lead.get('status')})")

        print(f"  {name:<24} {source:<12} {move_size:<18} {svc_date:<14} {days_str}")

    # ── Source Breakdown ──
    print(f"\n{'─'*40}")
    print("  Leads by Source:")
    source_counts = Counter(l.get("referralSourceName", "Unknown") for l in leads)
    for source, count in source_counts.most_common():
        bar = "█" * count
        print(f"    {source:<20} {count:>3}  {bar}")

    # ── Branch Breakdown ──
    branch_counts = Counter(l.get("branchName", "Unknown") for l in leads)
    if len(branch_counts) > 1:
        print(f"\n  Leads by Branch:")
        for branch, count in branch_counts.most_common():
            print(f"    {branch:<35} {count:>3}")

    # ── Move Size Breakdown ──
    print(f"\n  Leads by Move Size:")
    size_counts = Counter(l.get("moveSizeName", "Unknown") for l in leads)
    for size, count in size_counts.most_common():
        print(f"    {size:<25} {count:>3}")

    # ── Urgency (upcoming service dates) ──
    urgent = [l for l in leads if days_until(l.get("serviceDate")) is not None and days_until(l.get("serviceDate")) <= 7]
    if urgent:
        print(f"\n  ⚡ URGENT — Service date within 7 days:")
        for l in sorted(urgent, key=lambda x: x.get("serviceDate", 0)):
            days = days_until(l.get("serviceDate"))
            label = "TODAY" if days == 0 else "TOMORROW" if days == 1 else f"in {days} days"
            print(f"    • {l.get('customerName')} — {format_date(l.get('serviceDate'))} ({label})")

    print(f"\n{'='*70}")


if __name__ == "__main__":
    main()
