"""
Storage Operations Report — from SmartMoving storage accounts export.

Usage:
    python3 scripts/report_storage.py
"""

import os
import csv
from datetime import datetime
from collections import defaultdict

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
    path = os.path.join(EXPORT_DIR, "storage_accounts.csv")
    if not os.path.exists(path):
        print("ERROR: storage_accounts.csv not found in exports/")
        return

    with open(path) as f:
        reader = csv.DictReader(f)
        accounts = list(reader)

    active = [a for a in accounts if a.get("Status", "").strip() == "Active"]
    inactive = [a for a in accounts if a.get("Status", "").strip() != "Active"]

    # Revenue
    total_monthly = sum(parse_amount(a.get("$ Recurring Storage", "")) for a in active)

    # Delinquency
    total_0_30 = sum(parse_amount(a.get("0-30 Days", "")) for a in active)
    total_31_60 = sum(parse_amount(a.get("31-60 Days", "")) for a in active)
    total_61_90 = sum(parse_amount(a.get("61-90 Days", "")) for a in active)
    total_90_plus = sum(parse_amount(a.get("90+ Days", "")) for a in active)
    total_delinquent = total_0_30 + total_31_60 + total_61_90 + total_90_plus

    delinquent_accounts = [a for a in active if (
        parse_amount(a.get("0-30 Days", "")) +
        parse_amount(a.get("31-60 Days", "")) +
        parse_amount(a.get("61-90 Days", "")) +
        parse_amount(a.get("90+ Days", ""))
    ) > 0]

    # Volume
    total_volume = sum(parse_amount(a.get("Volume", "")) for a in active)

    # Auto-collect
    auto_collect_yes = sum(1 for a in active if a.get("Auto-Collect", "").strip() == "Yes")
    card_on_file = sum(1 for a in active if a.get("Card on File", "").strip() == "Yes")

    # Revenue per cu ft
    rev_per_cuft = (total_monthly / total_volume * 12) if total_volume else 0

    print(f"\n{'='*75}")
    print(f"  STORAGE OPERATIONS REPORT")
    print(f"  Generated: {datetime.now().strftime('%B %d, %Y %I:%M %p')}")
    print(f"{'='*75}")

    print(f"\n  OVERVIEW")
    print(f"  {'─'*65}")
    print(f"    Total Active Accounts:       {len(active)}")
    print(f"    Inactive/Closed Accounts:    {len(inactive)}")
    print(f"    Monthly Recurring Revenue:   ${total_monthly:>12,.2f}")
    print(f"    Annualized Revenue:          ${total_monthly * 12:>12,.2f}")
    print(f"    Total Volume (cu ft):        {total_volume:>12,.0f}")
    print(f"    Annual Rev / Cu Ft:          ${rev_per_cuft:>12,.2f}")

    print(f"\n  PAYMENT STATUS")
    print(f"  {'─'*65}")
    print(f"    Card on File:                {card_on_file} of {len(active)} ({card_on_file/len(active)*100:.0f}%)")
    print(f"    Auto-Collect Enabled:        {auto_collect_yes} of {len(active)} ({auto_collect_yes/len(active)*100:.0f}%)")

    print(f"\n  DELINQUENCY AGING")
    print(f"  {'─'*65}")
    print(f"    Current (0-30 days):         ${total_0_30:>12,.2f}")
    print(f"    31-60 days:                  ${total_31_60:>12,.2f}")
    print(f"    61-90 days:                  ${total_61_90:>12,.2f}")
    print(f"    90+ days:                    ${total_90_plus:>12,.2f}")
    print(f"    {'─'*45}")
    print(f"    TOTAL OUTSTANDING:           ${total_delinquent:>12,.2f}")
    if total_monthly > 0:
        print(f"    Delinquency Rate:            {total_delinquent / total_monthly * 100:>11.1f}%")
    print(f"    Accounts with Balance Due:   {len(delinquent_accounts)} of {len(active)}")

    # Top delinquent accounts
    if delinquent_accounts:
        delinquent_accounts.sort(
            key=lambda a: parse_amount(a.get("0-30 Days", "")) +
                          parse_amount(a.get("31-60 Days", "")) +
                          parse_amount(a.get("61-90 Days", "")) +
                          parse_amount(a.get("90+ Days", "")),
            reverse=True
        )

        print(f"\n  TOP DELINQUENT ACCOUNTS")
        print(f"  {'─'*65}")
        print(f"    {'Account':<12} {'Name':<25} {'Monthly':>10} {'Total Due':>12} {'Aging'}")
        print(f"    {'-'*12} {'-'*25} {'-'*10} {'-'*12} {'-'*20}")

        for a in delinquent_accounts[:15]:
            acct = a.get("Account #", "")
            name = (a.get("Name", ""))[:24]
            monthly = parse_amount(a.get("$ Recurring Storage", ""))
            d30 = parse_amount(a.get("0-30 Days", ""))
            d60 = parse_amount(a.get("31-60 Days", ""))
            d90 = parse_amount(a.get("61-90 Days", ""))
            d90p = parse_amount(a.get("90+ Days", ""))
            total_due = d30 + d60 + d90 + d90p

            aging_parts = []
            if d90p > 0:
                aging_parts.append(f"90+: ${d90p:,.0f}")
            elif d90 > 0:
                aging_parts.append(f"61-90: ${d90:,.0f}")
            elif d60 > 0:
                aging_parts.append(f"31-60: ${d60:,.0f}")
            elif d30 > 0:
                aging_parts.append(f"0-30: ${d30:,.0f}")

            print(f"    {acct:<12} {name:<25} ${monthly:>9,.2f} ${total_due:>11,.2f} {', '.join(aging_parts)}")

    # Accounts approaching lien threshold (90+ days)
    lien_candidates = [a for a in active if parse_amount(a.get("90+ Days", "")) > 0]
    if lien_candidates:
        print(f"\n  LIEN PIPELINE (90+ Days Delinquent)")
        print(f"  {'─'*65}")
        for a in lien_candidates:
            acct = a.get("Account #", "")
            name = a.get("Name", "")
            d90p = parse_amount(a.get("90+ Days", ""))
            print(f"    {acct} — {name}: ${d90p:,.2f} past 90 days")

    # Revenue by account size
    print(f"\n  ACCOUNTS BY MONTHLY RATE")
    print(f"  {'─'*65}")
    buckets = {"$0": 0, "$1-$100": 0, "$100-$300": 0, "$300-$500": 0, "$500-$1000": 0, "$1000+": 0}
    bucket_rev = {"$0": 0, "$1-$100": 0, "$100-$300": 0, "$300-$500": 0, "$500-$1000": 0, "$1000+": 0}

    for a in active:
        rate = parse_amount(a.get("$ Recurring Storage", ""))
        if rate == 0:
            b = "$0"
        elif rate <= 100:
            b = "$1-$100"
        elif rate <= 300:
            b = "$100-$300"
        elif rate <= 500:
            b = "$300-$500"
        elif rate <= 1000:
            b = "$500-$1000"
        else:
            b = "$1000+"
        buckets[b] += 1
        bucket_rev[b] += rate

    print(f"    {'Rate Bucket':<15} {'Accounts':>10} {'Monthly Rev':>14} {'% of Rev':>10}")
    print(f"    {'-'*15} {'-'*10} {'-'*14} {'-'*10}")
    for b in ["$0", "$1-$100", "$100-$300", "$300-$500", "$500-$1000", "$1000+"]:
        pct = (bucket_rev[b] / total_monthly * 100) if total_monthly else 0
        print(f"    {b:<15} {buckets[b]:>10} ${bucket_rev[b]:>13,.2f} {pct:>9.1f}%")

    print(f"\n{'='*75}")


if __name__ == "__main__":
    main()
