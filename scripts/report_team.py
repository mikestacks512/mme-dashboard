"""
Team & Branches Report — pulls live from SmartMoving API.

Usage:
    python3 scripts/report_team.py
"""

import sys
import os
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_api import get_all


def main():
    print("Pulling team and branch data...\n")
    branches = get_all("/branches")
    users = get_all("/users")
    referral_sources = get_all("/referral-sources")
    move_sizes = get_all("/move-sizes")

    print(f"{'='*70}")
    print(f"  TEAM & OPERATIONS OVERVIEW — {datetime.now().strftime('%B %d, %Y')}")
    print(f"{'='*70}")

    # ── Branches ──
    print(f"\n  BRANCHES ({len(branches)})")
    print(f"  {'-'*60}")
    for b in branches:
        loc = b.get("dispatchLocation") or {}
        primary = " [PRIMARY]" if b.get("isPrimary") else ""
        city = loc.get("city", "")
        state = loc.get("state", "")
        print(f"    {b['name']}{primary}")
        print(f"      {city}, {state} | {b.get('phoneNumber', 'N/A')}")

    # ── Team ──
    print(f"\n  TEAM MEMBERS ({len(users)})")
    print(f"  {'-'*60}")

    by_role = defaultdict(list)
    for u in users:
        role = (u.get("role") or {}).get("name", "Unknown")
        by_role[role].append(u)

    for role, members in sorted(by_role.items()):
        print(f"\n    {role}:")
        for u in members:
            branch = (u.get("primaryBranch") or {}).get("name", "Unassigned")
            print(f"      • {u['name']:<25} {branch}")

    # ── Referral Sources ──
    print(f"\n  REFERRAL SOURCES ({len(referral_sources)})")
    print(f"  {'-'*60}")
    providers = [r for r in referral_sources if r.get("isLeadProvider")]
    non_providers = [r for r in referral_sources if not r.get("isLeadProvider")]

    if providers:
        print(f"    Lead Providers: {', '.join(r['name'] for r in providers)}")
    print(f"    Other Sources:  {', '.join(r['name'] for r in non_providers)}")

    # ── Move Sizes ──
    print(f"\n  MOVE SIZE CATEGORIES ({len(move_sizes)})")
    print(f"  {'-'*60}")
    sized = [m for m in move_sizes if m.get("volume") and m["volume"] > 0]
    sized.sort(key=lambda m: m.get("volume", 0))
    for m in sized[:15]:
        vol = m.get("volume") or 0
        wt = m.get("weight") or 0
        print(f"    {m['name']:<30} {vol:>5} cu ft  {wt:>7,.0f} lbs")
    if len(sized) > 15:
        print(f"    ... and {len(sized) - 15} more")

    print(f"\n{'='*70}")


if __name__ == "__main__":
    main()
