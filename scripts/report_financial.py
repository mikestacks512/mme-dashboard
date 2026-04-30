"""
Page 3: Financial Control
Answers: "Are we making money? Are costs under control?"

Each metric shows: current %, target, variance, and status indicator.
Data source: QuickBooks P&L Detail CSV

Usage:
    python3 scripts/report_financial.py
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
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return 0.0


def load_pl_totals(filepath):
    totals = {}
    with open(filepath) as f:
        for row in csv.reader(f):
            if row and row[0].startswith("Total for") and len(row) > 9 and row[9]:
                key = row[0].replace("Total for ", "")
                totals[key] = parse_amount(row[9])
    return totals


def load_monthly(filepath):
    monthly = defaultdict(lambda: defaultdict(float))
    current_account = None
    with open(filepath) as f:
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
                    monthly[mk][current_account] += parse_amount(row[9])
                except (ValueError, IndexError):
                    pass
    return monthly


def status(actual, target, direction):
    if direction == "<=" and actual <= target:
        return "OK"
    elif direction == ">=" and actual >= target:
        return "OK"
    elif direction == "<=" and actual <= target * 1.1:
        return "WARN"
    elif direction == ">=" and actual >= target * 0.9:
        return "WARN"
    return "MISS"


def fmt(v):
    return f"${v:,.0f}" if abs(v) >= 1000 else f"${v:,.2f}"


def main():
    # Find P&L file (name may vary)
    pl_path = None
    for name in os.listdir(EXPORT_DIR):
        if "profit" in name.lower() and "loss" in name.lower() and name.endswith(".csv"):
            pl_path = os.path.join(EXPORT_DIR, name)
            break
    if not pl_path:
        print("ERROR: P&L Detail CSV not found in exports/")
        return

    t = load_pl_totals(pl_path)
    monthly = load_monthly(pl_path)

    # ── Pull key figures ──
    revenue = t.get("Income with sub-accounts", 0)
    cogs = t.get("Cost of Goods Sold with sub-accounts", 0)
    overhead = t.get("Expenses with sub-accounts", 0)
    other_income = t.get("Other Income with sub-accounts", 0)
    other_expense = t.get("Other Expense with sub-accounts", 0)

    labor_direct = t.get("5001 Direct Labor with sub-accounts", 0)
    labor_contracted = t.get("5001c Contracted Direct Labor", 0)
    labor_w2 = t.get("5001a Payroll - Direct Labor", 0) + t.get("5001b Payroll - Direct Labor Taxes", 0) + t.get("5001d Workers Compensation (Direct)", 0)
    sales_labor = t.get("5002 Other Direct Labor with sub-accounts", 0)
    marketing = t.get("6002 Advertising, Marketing, & Promo", 0)
    fuel = t.get("5007 Truck Fuel", 0)
    claims = t.get("5009 Valuation/Damages", 0)
    merchant = t.get("5004 Merchant Fees (COGS)", 0)
    materials = t.get("5003 Packing Materials", 0)
    truck_lease = t.get("5008 Truck Lease/Rent", 0)
    insurance = t.get("6003 Insurance", 0)
    admin_payroll = t.get("6001 Payroll Indirect with sub-accounts", 0)

    gross_profit = revenue - cogs
    contribution_profit = gross_profit
    fully_loaded = revenue - cogs - overhead
    net_income = revenue + other_income - cogs - overhead - other_expense

    gm = (gross_profit / revenue * 100) if revenue else 0
    net_margin = (fully_loaded / revenue * 100) if revenue else 0

    # ── Output ──
    print(f"\n{'='*75}")
    print(f"  FINANCIAL CONTROL — Muscleman Elite Moving & Storage")
    print(f"  Period: April 2025 – April 2026 (12 months)")
    print(f"  Generated: {datetime.now().strftime('%B %d, %Y %I:%M %p')}")
    print(f"{'='*75}")

    # ── Profitability ──
    print(f"\n  PROFITABILITY")
    print(f"  {'─'*68}")
    print(f"  {'Metric':<35} {'Actual':>10} {'Target':>10} {'Variance':>10} {'':>8}")
    print(f"  {'-'*35} {'-'*10} {'-'*10} {'-'*10} {'-'*8}")

    rows = [
        ("Total Revenue", fmt(revenue), "", "", ""),
        ("Total COGS", fmt(cogs), "", f"{cogs/revenue*100:.1f}% rev", ""),
        ("Gross Profit", fmt(gross_profit), "", "", ""),
        ("Gross Margin", f"{gm:.1f}%", "≥ 45%", f"{gm-45:+.1f}pp", status(gm, 45, ">=")),
        ("Total Overhead", fmt(overhead), "", f"{overhead/revenue*100:.1f}% rev", ""),
        ("Fully Loaded Profit", fmt(fully_loaded), "", "", ""),
        ("EBITDA (proxy)", f"{net_margin:.1f}%", "≥ 20%", f"{net_margin-20:+.1f}pp", status(net_margin, 20, ">=")),
        ("Net Income (w/ other)", fmt(net_income), "", "", ""),
    ]
    for label, actual, target, variance, st in rows:
        st_str = f"[{st}]" if st else ""
        print(f"  {label:<35} {actual:>10} {target:>10} {variance:>10} {st_str:>8}")

    # ── Cost Control ──
    print(f"\n  COST CONTROL vs TARGETS")
    print(f"  {'─'*68}")
    print(f"  {'Cost Category':<35} {'% of Rev':>10} {'Target':>10} {'Variance':>10} {'':>8}")
    print(f"  {'-'*35} {'-'*10} {'-'*10} {'-'*10} {'-'*8}")

    cost_rows = [
        ("Labor (Direct)", labor_direct/revenue*100, 34, "<="),
        ("  W-2 Labor", labor_w2/revenue*100, None, None),
        ("  Contracted Labor", labor_contracted/revenue*100, None, None),
        ("Sales Payroll", sales_labor/revenue*100, 7, "<="),
        ("Marketing & Advertising", marketing/revenue*100, 7, "<="),
        ("Fuel", fuel/revenue*100, 5, "<="),
        ("Claims / Damages", claims/revenue*100, 1, "<="),
        ("Merchant Fees", merchant/revenue*100, None, None),
        ("Packing Materials", materials/revenue*100, None, None),
        ("Truck Lease/Rent", truck_lease/revenue*100, None, None),
        ("Insurance", insurance/revenue*100, None, None),
        ("Admin Payroll", admin_payroll/revenue*100, None, None),
    ]
    for item in cost_rows:
        label, pct = item[0], item[1]
        target = item[2] if len(item) > 2 else None
        direction = item[3] if len(item) > 3 else None
        if target:
            var = pct - target
            st = status(pct, target, direction)
            print(f"  {label:<35} {pct:>9.1f}% {'≤ '+str(target)+'%':>10} {var:>+9.1f}pp {'['+st+']':>8}")
        else:
            print(f"  {label:<35} {pct:>9.1f}%")

    # ── Contractor vs W-2 ──
    print(f"\n  CONTRACTOR vs W-2 LABOR")
    print(f"  {'─'*68}")
    print(f"    W-2 Direct Labor:       {fmt(labor_w2):>14}  ({labor_w2/revenue*100:.1f}% of revenue)")
    print(f"    Contracted Labor:       {fmt(labor_contracted):>14}  ({labor_contracted/revenue*100:.1f}% of revenue)")
    ratio = labor_contracted / (labor_w2 + labor_contracted) * 100 if (labor_w2 + labor_contracted) > 0 else 0
    print(f"    Dependency Ratio:       {ratio:.0f}% contractor / {100-ratio:.0f}% W-2")

    # ── Contribution vs Fully Loaded ──
    print(f"\n  PROFIT WATERFALL")
    print(f"  {'─'*68}")
    print(f"    Revenue                         {fmt(revenue):>14}")
    print(f"    − Direct Labor                  {fmt(labor_direct):>14}")
    print(f"    − Sales Labor                   {fmt(sales_labor):>14}")
    print(f"    − Materials, Fuel, Fees         {fmt(materials + fuel + merchant + t.get('5006 Travel Expenses for Drivers', 0) + t.get('5005 Parking, Tolls, & Inspection', 0) + t.get('5099 Additional Direct Expenses', 0)):>14}")
    print(f"    − Truck Lease/Rent              {fmt(truck_lease):>14}")
    print(f"    − Claims/Damages                {fmt(claims):>14}")
    print(f"    − Tips offset                   {fmt(t.get('5001z SmartMoving Tips Received', 0)):>14}")
    print(f"    {'─'*42}")
    print(f"    = Contribution Profit           {fmt(contribution_profit):>14}  ({gm:.1f}%)")
    print(f"    − Marketing                     {fmt(marketing):>14}")
    print(f"    − Insurance                     {fmt(insurance):>14}")
    print(f"    − Admin Payroll                 {fmt(admin_payroll):>14}")
    print(f"    − Other Overhead                {fmt(overhead - marketing - insurance - admin_payroll):>14}")
    print(f"    {'─'*42}")
    print(f"    = Fully Loaded Profit           {fmt(fully_loaded):>14}  ({net_margin:.1f}%)")

    # ── Monthly Trend ──
    print(f"\n  MONTHLY TREND")
    print(f"  {'─'*68}")
    print(f"  {'Month':<10} {'Revenue':>12} {'COGS':>12} {'GP':>12} {'Margin':>8} {'vs 45%':>8}")
    print(f"  {'-'*10} {'-'*12} {'-'*12} {'-'*12} {'-'*8} {'-'*8}")

    for month in sorted(monthly.keys()):
        m = monthly[month]
        rev = sum(v for k, v in m.items() if k.startswith("4"))
        cogs_m = sum(v for k, v in m.items() if k.startswith("5"))
        gp = rev - cogs_m
        margin = (gp / rev * 100) if rev else 0
        gap = margin - 45
        st = "OK" if margin >= 45 else "WARN" if margin >= 40 else "MISS"
        print(f"  {month:<10} {fmt(rev):>12} {fmt(cogs_m):>12} {fmt(gp):>12} {margin:>6.1f}% {gap:>+6.1f}% [{st}]")

    print(f"\n{'='*75}")


if __name__ == "__main__":
    main()
