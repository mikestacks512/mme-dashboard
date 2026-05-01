"""
Data-returning versions of each CLI report.
Each function returns a dict suitable for JSON serialization.
"""

import os
import csv
import sys
from datetime import datetime, timedelta
from collections import Counter, defaultdict

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "mme_dashboard.duckdb")
EXPORT_DIR = os.path.join(PROJECT_ROOT, "exports")

# Make sure scripts/ is importable for sm_api
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))
from sm_api import get_all, api_get, get_opportunity_detail


# ── Shared helpers ──

def _get_last_sync(con, table_name=None):
    """Get the last successful sync time from sync_log, or fall back to max synced_at."""
    try:
        if table_name:
            row = con.execute(
                "SELECT completed_at FROM sync_log WHERE status='success' AND table_name=? ORDER BY completed_at DESC LIMIT 1",
                [table_name]).fetchone()
        else:
            row = con.execute(
                "SELECT completed_at FROM sync_log WHERE status='success' ORDER BY completed_at DESC LIMIT 1"
            ).fetchone()
        if row and row[0]:
            return row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0])
    except Exception:
        pass
    # Fallback: max synced_at from the table itself
    if table_name:
        try:
            row = con.execute(f"SELECT MAX(synced_at) FROM {table_name}").fetchone()
            if row and row[0]:
                return row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0])
        except Exception:
            pass
    return None


def _format_yyyymmdd(d):
    """Convert YYYYMMDD integer to MM/DD/YYYY string."""
    s = str(d)
    if len(s) == 8:
        return f"{s[4:6]}/{s[6:8]}/{s[:4]}"
    return s

def _parse_amount(s):
    if not s:
        return 0.0
    s = s.strip().replace("$", "").replace(",", "")
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return 0.0


def _get_duckdb():
    import duckdb
    if not os.path.exists(DB_PATH):
        return None
    try:
        return duckdb.connect(DB_PATH, read_only=True)
    except Exception:
        # If sync is writing, open in read-write mode instead
        try:
            return duckdb.connect(DB_PATH)
        except Exception:
            return None


def _find_pl_csvs():
    """Find all P&L CSV files in the exports directory, sorted by modification time (newest last)."""
    if not os.path.exists(EXPORT_DIR):
        return []
    files = []
    for name in os.listdir(EXPORT_DIR):
        if "profit" in name.lower() and "loss" in name.lower() and name.endswith(".csv"):
            path = os.path.join(EXPORT_DIR, name)
            files.append((os.path.getmtime(path), path))
    # Sort by modification time — oldest first, newest last (newest wins on overlap)
    files.sort()
    return [f[1] for f in files]


def _find_pl_csv():
    """Backward compat: return first P&L CSV found."""
    files = _find_pl_csvs()
    return files[0] if files else None


def _load_pl_totals_single(filepath):
    totals = {}
    with open(filepath) as f:
        for row in csv.reader(f):
            if row and row[0].startswith("Total for") and len(row) > 9 and row[9]:
                key = row[0].replace("Total for ", "")
                totals[key] = _parse_amount(row[9])
    return totals


def _load_qb_monthly_single(filepath):
    monthly = defaultdict(lambda: {"revenue": 0, "cogs": 0, "overhead": 0, "other_income": 0, "other_expense": 0, "accounts": defaultdict(float)})
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
                    amt = _parse_amount(row[9])
                    monthly[mk]["accounts"][current_account] += amt
                    if current_account.startswith("4"):
                        monthly[mk]["revenue"] += amt
                    elif current_account.startswith("5"):
                        monthly[mk]["cogs"] += amt
                    elif current_account.startswith("6"):
                        monthly[mk]["overhead"] += amt
                    elif current_account.startswith("7"):
                        monthly[mk]["other_income"] += amt
                    elif current_account.startswith("8"):
                        monthly[mk]["other_expense"] += amt
                except (ValueError, IndexError):
                    pass
    # Convert defaultdicts to regular dicts for serialization
    for mk in monthly:
        monthly[mk]["accounts"] = dict(monthly[mk]["accounts"])
    return dict(monthly)


def _load_qb_monthly(filepath=None):
    """Load monthly data from all P&L files. Newer files override overlapping months."""
    files = _find_pl_csvs()
    if not files:
        return {}
    merged = {}
    for f in files:
        file_monthly = _load_qb_monthly_single(f)
        # Newer file's months completely replace older file's months
        for month, data in file_monthly.items():
            merged[month] = data
    return merged


def _load_pl_totals(filepath=None):
    """Calculate totals from merged monthly data so multi-file uploads stay consistent.

    Account-level totals are summed from the per-account data tracked in each month.
    This ensures that when a newer P&L upload overrides specific months, the totals
    reflect the merged data rather than one file clobbering another.
    """
    monthly = _load_qb_monthly()
    if not monthly:
        return {}

    # Sum account-level details across all merged months
    account_totals = defaultdict(float)
    for month_data in monthly.values():
        for acct, amt in month_data.get("accounts", {}).items():
            account_totals[acct] += amt

    # Build the totals dict the rest of the code expects
    totals = dict(account_totals)

    # Add high-level rollups
    totals["Income with sub-accounts"] = sum(d.get("revenue", 0) for d in monthly.values())
    totals["Cost of Goods Sold with sub-accounts"] = sum(d.get("cogs", 0) for d in monthly.values())
    totals["Expenses with sub-accounts"] = sum(d.get("overhead", 0) for d in monthly.values())
    totals["Other Income with sub-accounts"] = sum(d.get("other_income", 0) for d in monthly.values())
    totals["Other Expense with sub-accounts"] = sum(d.get("other_expense", 0) for d in monthly.values())

    # Build "with sub-accounts" rollups for parent account groups
    # e.g. "5001a..." + "5001b..." + "5001c..." -> "5001 Direct Labor with sub-accounts"
    # Also read the base P&L file's "Total for" rows to get the correct parent names
    parent_sums = defaultdict(float)
    for acct, amt in account_totals.items():
        parts = acct.split(" ", 1)
        if len(parts) == 2:
            num = parts[0]
            base = num.rstrip("abcdefghijklmnopqrstuvwxyz")
            if base:
                parent_sums[base] += amt

    # Read "Total for" rows from all files to get the parent account names
    for f in _find_pl_csvs():
        file_totals = _load_pl_totals_single(f)
        for key, amt in file_totals.items():
            # Extract the account number from keys like "5001 Direct Labor with sub-accounts"
            if " with sub-accounts" in key:
                base_name = key.replace(" with sub-accounts", "")
                num = base_name.split(" ", 1)[0].rstrip("abcdefghijklmnopqrstuvwxyz")
                if num in parent_sums:
                    totals[key] = parent_sums[num]

    return totals


# ── Report: Leads ──

def get_leads():
    """Live lead pipeline from SmartMoving API."""
    leads = get_all("/leads")
    if not leads:
        return {"leads": [], "by_source": [], "by_branch": [], "by_size": [], "urgent": [], "total": 0}

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    status_map = {0: "New", 1: "Contacted", 2: "Qualified", 3: "Lost", 10: "Converted"}

    def days_until(sd):
        if not sd:
            return None
        return (datetime.strptime(str(sd), "%Y%m%d") - today).days

    lead_list = []
    for lead in sorted(leads, key=lambda l: l.get("serviceDate", 0)):
        sd = lead.get("serviceDate")
        d = days_until(sd)
        lead_list.append({
            "customer": lead.get("customerName") or "Unknown",
            "source": lead.get("referralSourceName") or "?",
            "move_size": lead.get("moveSizeName") or "?",
            "service_date": f"{str(sd)[4:6]}/{str(sd)[6:8]}/{str(sd)[:4]}" if sd else "N/A",
            "days_out": d,
            "branch": lead.get("branchName") or "Unknown",
            "status": status_map.get(lead.get("status"), str(lead.get("status"))),
        })

    source_counts = Counter(l.get("referralSourceName", "Unknown") for l in leads)
    branch_counts = Counter(l.get("branchName", "Unknown") for l in leads)
    size_counts = Counter(l.get("moveSizeName", "Unknown") for l in leads)

    urgent = [l for l in lead_list if l["days_out"] is not None and l["days_out"] <= 7 and l["days_out"] >= 0]

    # Date range from lead service dates
    service_dates = [l.get("serviceDate") for l in leads if l.get("serviceDate")]
    date_range_start = _format_yyyymmdd(min(service_dates)) if service_dates else None
    date_range_end = _format_yyyymmdd(max(service_dates)) if service_dates else None

    return {
        "generated": datetime.now().isoformat(),
        "total": len(leads),
        "leads": lead_list,
        "by_source": [{"name": k, "count": v} for k, v in source_counts.most_common()],
        "by_branch": [{"name": k, "count": v} for k, v in branch_counts.most_common()],
        "by_size": [{"name": k, "count": v} for k, v in size_counts.most_common()],
        "urgent": urgent,
        "date_range_start": date_range_start,
        "date_range_end": date_range_end,
    }


# ── Report: Financial ──

def get_financial():
    """Financial control from QuickBooks P&L CSV(s)."""
    files = _find_pl_csvs()
    if not files:
        return {"error": "P&L Detail CSV not found in exports/"}

    t = _load_pl_totals()
    monthly_raw = _load_qb_monthly()

    revenue = t.get("Income with sub-accounts", 0)
    if revenue == 0:
        return {"error": "No revenue data found in P&L"}

    cogs = t.get("Cost of Goods Sold with sub-accounts", 0)
    overhead = t.get("Expenses with sub-accounts", 0)
    other_income = t.get("Other Income with sub-accounts", 0)
    other_expense = t.get("Other Expense with sub-accounts", 0)

    labor_direct = t.get("5001 Direct Labor with sub-accounts", 0)
    labor_contracted = t.get("5001c Contracted Direct Labor", 0)
    labor_w2 = (t.get("5001a Payroll - Direct Labor", 0) +
                t.get("5001b Payroll - Direct Labor Taxes", 0) +
                t.get("5001d Workers Compensation (Direct)", 0))
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
    fully_loaded = revenue - cogs - overhead
    net_income = revenue + other_income - cogs - overhead - other_expense
    gm = (gross_profit / revenue * 100) if revenue else 0
    net_margin = (fully_loaded / revenue * 100) if revenue else 0
    contractor_ratio = (labor_contracted / (labor_w2 + labor_contracted) * 100
                        if (labor_w2 + labor_contracted) > 0 else 0)

    # Contribution Profit (per the spec): Revenue minus direct job costs only
    direct_costs = labor_direct + materials + fuel + merchant + claims + truck_lease
    contribution_profit = revenue - direct_costs
    contribution_margin = (contribution_profit / revenue * 100) if revenue else 0

    def _status(actual, target, direction):
        if direction == "<=" and actual <= target:
            return "OK"
        elif direction == ">=" and actual >= target:
            return "OK"
        elif direction == "<=" and actual <= target * 1.1:
            return "WARN"
        elif direction == ">=" and actual >= target * 0.9:
            return "WARN"
        return "MISS"

    # Monthly trend
    monthly_trend = []
    for month in sorted(monthly_raw.keys()):
        m = monthly_raw[month]
        rev = m["revenue"]
        c = m["cogs"]
        gp = rev - c
        margin = (gp / rev * 100) if rev else 0
        monthly_trend.append({
            "month": month, "revenue": round(rev, 2), "cogs": round(c, 2),
            "gross_profit": round(gp, 2), "margin": round(margin, 1),
            "status": "OK" if margin >= 45 else "WARN" if margin >= 40 else "MISS",
        })

    # Date range from QB monthly data
    months = sorted(monthly_raw.keys())
    qb_date_start = months[0] if months else None
    qb_date_end = months[-1] if months else None

    return {
        "generated": datetime.now().isoformat(),
        "date_range_start": qb_date_start,
        "date_range_end": qb_date_end,
        "profitability": {
            "revenue": round(revenue, 2),
            "cogs": round(cogs, 2),
            "cogs_pct": round(cogs / revenue * 100, 1),
            "gross_profit": round(gross_profit, 2),
            "gross_margin": round(gm, 1),
            "gross_margin_target": 45,
            "gross_margin_status": _status(gm, 45, ">="),
            "contribution_profit": round(contribution_profit, 2),
            "contribution_margin": round(contribution_margin, 1),
            "contribution_status": _status(contribution_margin, 55, ">="),
            "overhead": round(overhead, 2),
            "overhead_pct": round(overhead / revenue * 100, 1),
            "fully_loaded_profit": round(fully_loaded, 2),
            "ebitda_margin": round(net_margin, 1),
            "ebitda_target": 20,
            "ebitda_status": _status(net_margin, 20, ">="),
            "net_income": round(net_income, 2),
        },
        "cost_control": [
            {"category": "Labor (Direct)", "pct": round(labor_direct / revenue * 100, 1), "target": 34, "status": _status(labor_direct / revenue * 100, 34, "<=")},
            {"category": "  W-2 Labor", "pct": round(labor_w2 / revenue * 100, 1)},
            {"category": "  Contracted Labor", "pct": round(labor_contracted / revenue * 100, 1)},
            {"category": "Sales Payroll", "pct": round(sales_labor / revenue * 100, 1), "target": 7, "status": _status(sales_labor / revenue * 100, 7, "<=")},
            {"category": "Marketing", "pct": round(marketing / revenue * 100, 1), "target": 7, "status": _status(marketing / revenue * 100, 7, "<=")},
            {"category": "Fuel", "pct": round(fuel / revenue * 100, 1), "target": 5, "status": _status(fuel / revenue * 100, 5, "<=")},
            {"category": "Claims/Damages", "pct": round(claims / revenue * 100, 1), "target": 1, "status": _status(claims / revenue * 100, 1, "<=")},
            {"category": "Merchant Fees", "pct": round(merchant / revenue * 100, 1)},
            {"category": "Packing Materials", "pct": round(materials / revenue * 100, 1)},
            {"category": "Truck Lease/Rent", "pct": round(truck_lease / revenue * 100, 1)},
            {"category": "Insurance", "pct": round(insurance / revenue * 100, 1)},
            {"category": "Admin Payroll", "pct": round(admin_payroll / revenue * 100, 1)},
        ],
        "labor_split": {
            "w2": round(labor_w2, 2),
            "contracted": round(labor_contracted, 2),
            "contractor_pct": round(contractor_ratio, 0),
        },
        "waterfall": [
            {"label": "Revenue", "amount": round(revenue, 2)},
            {"label": "Direct Labor", "amount": round(-labor_direct, 2)},
            {"label": "Packing Materials", "amount": round(-materials, 2)},
            {"label": "Fuel", "amount": round(-fuel, 2)},
            {"label": "Merchant Fees", "amount": round(-merchant, 2)},
            {"label": "Truck Lease/Rent", "amount": round(-truck_lease, 2)},
            {"label": "Claims/Damages", "amount": round(-claims, 2)},
            {"label": "= Contribution Profit", "amount": round(contribution_profit, 2), "subtotal": True},
            {"label": "Sales Labor", "amount": round(-sales_labor, 2)},
            {"label": "Marketing", "amount": round(-marketing, 2)},
            {"label": "Insurance", "amount": round(-insurance, 2)},
            {"label": "Admin Payroll", "amount": round(-admin_payroll, 2)},
            {"label": "Other Overhead", "amount": round(-(overhead - marketing - insurance - admin_payroll), 2)},
            {"label": "Other COGS", "amount": round(-(cogs - direct_costs - sales_labor), 2)},
            {"label": "= Fully Loaded Profit", "amount": round(fully_loaded, 2), "subtotal": True},
        ],
        "monthly_trend": monthly_trend,
    }


# ── Report: Estimates (Page 4) ──

def get_estimates():
    """Estimate accuracy & pricing intelligence from DuckDB."""
    con = _get_duckdb()
    if not con:
        return {"error": "DuckDB not found. Run fast_sync.py first."}

    opp_count = con.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
    if opp_count == 0:
        con.close()
        return {"error": "No opportunities in DuckDB."}

    # By revenue bucket
    buckets = con.execute("""
        SELECT
            CASE
                WHEN estimated_total < 500 THEN 'Under $500'
                WHEN estimated_total < 1000 THEN '$500-$1K'
                WHEN estimated_total < 1500 THEN '$1K-$1.5K'
                WHEN estimated_total < 2500 THEN '$1.5K-$2.5K'
                WHEN estimated_total < 3500 THEN '$2.5K-$3.5K'
                ELSE '$3.5K+'
            END as bucket,
            COUNT(*) as opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            SUM(CASE WHEN status = 20 THEN 1 ELSE 0 END) as lost,
            SUM(CASE WHEN status = 30 THEN 1 ELSE 0 END) as cancelled,
            SUM(CASE WHEN status IN (3,5,10,11) THEN estimated_total ELSE 0 END) as booked_rev,
            AVG(estimated_total) as avg_est
        FROM opportunities WHERE estimated_total > 0
        GROUP BY bucket ORDER BY MIN(estimated_total)
    """).fetchall()

    by_rep = con.execute("""
        SELECT
            COALESCE(sales_assignee, 'Unassigned') as rep,
            COUNT(*) as opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            AVG(CASE WHEN estimated_total > 0 THEN estimated_total END) as avg_est,
            MIN(CASE WHEN estimated_total > 0 THEN estimated_total END) as min_est,
            MAX(estimated_total) as max_est,
            SUM(CASE WHEN status IN (3,5,10,11) THEN estimated_total ELSE 0 END) as booked_rev
        FROM opportunities WHERE estimated_total > 0
        GROUP BY COALESCE(sales_assignee, 'Unassigned')
        ORDER BY booked_rev DESC NULLS LAST
    """).fetchall()

    by_size = con.execute("""
        SELECT
            COALESCE(move_size, 'Unknown') as size,
            COUNT(*) as opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            AVG(CASE WHEN estimated_total > 0 THEN estimated_total END) as avg_est,
            SUM(CASE WHEN status IN (3,5,10,11) THEN estimated_total ELSE 0 END) as booked_rev
        FROM opportunities WHERE estimated_total > 0
        GROUP BY COALESCE(move_size, 'Unknown')
        HAVING COUNT(*) >= 3
        ORDER BY avg_est DESC NULLS LAST
    """).fetchall()

    # Date range and last sync
    date_range = con.execute(
        "SELECT MIN(service_date), MAX(service_date) FROM opportunities WHERE service_date > 0"
    ).fetchone()
    last_sync = _get_last_sync(con, "opportunities")

    con.close()

    return {
        "generated": datetime.now().isoformat(),
        "total_opportunities": opp_count,
        "date_range_start": _format_yyyymmdd(date_range[0]) if date_range and date_range[0] else None,
        "date_range_end": _format_yyyymmdd(date_range[1]) if date_range and date_range[1] else None,
        "last_sync": last_sync,
        "by_bucket": [
            {"bucket": b, "opps": o, "booked": bk, "book_pct": round(bk / o * 100) if o else 0,
             "lost": l, "cancelled": c, "booked_rev": round(br or 0, 2), "avg_est": round(a or 0, 2)}
            for b, o, bk, l, c, br, a in buckets
        ],
        "by_rep": [
            {"rep": r, "opps": o, "booked": bk, "book_pct": round(bk / o * 100) if o else 0,
             "avg_est": round(a or 0, 2), "min_est": round(mn or 0, 2),
             "max_est": round(mx or 0, 2), "booked_rev": round(br or 0, 2)}
            for r, o, bk, a, mn, mx, br in by_rep
        ],
        "by_size": [
            {"size": s, "opps": o, "booked": bk, "book_pct": round(bk / o * 100) if o else 0,
             "avg_est": round(a or 0, 2), "booked_rev": round(br or 0, 2)}
            for s, o, bk, a, br in by_size
        ],
    }


# ── Report: Sales (Page 5) ──

def get_sales():
    """Sales performance by rep from DuckDB."""
    con = _get_duckdb()
    if not con:
        return {"error": "DuckDB not found."}

    opp_count = con.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
    if opp_count == 0:
        con.close()
        return {"error": "No opportunities in DuckDB."}

    reps = con.execute("""
        SELECT
            COALESCE(sales_assignee, 'Unassigned') as rep,
            COUNT(*) as total_opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            SUM(CASE WHEN status = 20 THEN 1 ELSE 0 END) as lost,
            SUM(CASE WHEN status = 30 THEN 1 ELSE 0 END) as cancelled,
            SUM(CASE WHEN status IN (1,2) THEN 1 ELSE 0 END) as pending,
            SUM(CASE WHEN status IN (3,5,10,11) THEN estimated_total ELSE 0 END) as booked_rev,
            AVG(CASE WHEN status IN (3,5,10,11) AND estimated_total > 0 THEN estimated_total END) as avg_job
        FROM opportunities
        GROUP BY COALESCE(sales_assignee, 'Unassigned')
        ORDER BY booked_rev DESC NULLS LAST
    """).fetchall()

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

    pipeline = con.execute("""
        SELECT
            CASE status
                WHEN 0 THEN 'New' WHEN 1 THEN 'Estimated' WHEN 2 THEN 'Follow Up'
                WHEN 3 THEN 'Booked' WHEN 5 THEN 'Confirmed' WHEN 10 THEN 'Completed'
                WHEN 11 THEN 'Closed' WHEN 20 THEN 'Lost' WHEN 30 THEN 'Cancelled'
                ELSE 'Other'
            END as status_name,
            COUNT(*) as cnt,
            SUM(estimated_total) as est_rev
        FROM opportunities GROUP BY status ORDER BY cnt DESC
    """).fetchall()

    # Date range and last sync
    date_range = con.execute(
        "SELECT MIN(service_date), MAX(service_date) FROM opportunities WHERE service_date > 0"
    ).fetchone()
    last_sync = _get_last_sync(con, "opportunities")

    con.close()

    total_opps = sum(r[1] for r in reps)
    total_booked = sum(r[2] for r in reps)
    total_rev = sum((r[6] or 0) for r in reps)
    overall_rate = round(total_booked / total_opps * 100) if total_opps else 0

    return {
        "generated": datetime.now().isoformat(),
        "total_opportunities": opp_count,
        "date_range_start": _format_yyyymmdd(date_range[0]) if date_range and date_range[0] else None,
        "date_range_end": _format_yyyymmdd(date_range[1]) if date_range and date_range[1] else None,
        "last_sync": last_sync,
        "overall_booking_rate": overall_rate,
        "booking_rate_status": "OK" if overall_rate >= 25 else "MISS",
        "by_rep": [
            {"rep": r, "opps": t, "booked": b, "book_pct": round(b / t * 100) if t else 0,
             "lost": l, "cancelled": c, "pending": p,
             "booked_rev": round(br or 0, 2), "avg_job": round(aj or 0, 2)}
            for r, t, b, l, c, p, br, aj in reps
        ],
        "totals": {"opps": total_opps, "booked": total_booked, "revenue": round(total_rev, 2)},
        "by_source": [
            {"source": s, "opps": o, "booked": b, "book_pct": round(b / o * 100) if o else 0,
             "revenue": round(r or 0, 2)}
            for s, o, b, r in sources
        ],
        "pipeline": [
            {"status": s, "count": c, "est_revenue": round(r or 0, 2)}
            for s, c, r in pipeline
        ],
    }


# ── Report: Marketing (Page 7) ──

def get_marketing():
    """Marketing channel performance from DuckDB + QB."""
    con = _get_duckdb()
    if not con:
        return {"error": "DuckDB not found."}

    # Marketing spend from QB (merged across all P&L files)
    marketing_spend = 0
    t = _load_pl_totals()
    if t:
        marketing_spend = t.get("6002 Advertising, Marketing, & Promo", 0)

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

    # Date range and last sync
    date_range = con.execute(
        "SELECT MIN(service_date), MAX(service_date) FROM opportunities WHERE service_date > 0"
    ).fetchone()
    last_sync = _get_last_sync(con, "opportunities")

    con.close()

    total_leads = sum(c[1] for c in channels)
    total_booked = sum(c[2] for c in channels)
    total_rev = sum(c[5] or 0 for c in channels)

    channel_list = [
        {"channel": ch, "leads": ld, "booked": bk, "book_pct": round(bk / ld * 100) if ld else 0,
         "lost": lo, "cancelled": cx, "revenue": round(rv or 0, 2), "avg_job": round(aj or 0, 2)}
        for ch, ld, bk, lo, cx, rv, aj in channels
    ]

    efficiency = None
    if marketing_spend > 0 and total_booked > 0:
        cac = marketing_spend / total_booked
        efficiency = {
            "spend": round(marketing_spend, 2),
            "roi": round(total_rev / marketing_spend, 1),
            "cost_per_lead": round(marketing_spend / total_leads, 2) if total_leads else 0,
            "cost_per_booking": round(marketing_spend / total_booked, 2),
            "cac": round(cac, 2),
            "cac_status": "OK" if cac < 500 else "WARN" if cac < 750 else "HIGH",
        }

    return {
        "generated": datetime.now().isoformat(),
        "date_range_start": _format_yyyymmdd(date_range[0]) if date_range and date_range[0] else None,
        "date_range_end": _format_yyyymmdd(date_range[1]) if date_range and date_range[1] else None,
        "last_sync": last_sync,
        "marketing_spend": round(marketing_spend, 2),
        "total_leads": total_leads,
        "total_booked": total_booked,
        "total_revenue": round(total_rev, 2),
        "channels": channel_list,
        "efficiency": efficiency,
    }


# ── Report: Trends (Page 12) ──

def get_trends():
    """Monthly and weekly trends from DuckDB + QB."""
    con = _get_duckdb()
    if not con:
        return {"error": "DuckDB not found."}

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
        GROUP BY week_bucket ORDER BY week_bucket
    """).fetchall()

    # Date range and last sync
    opp_date_range = con.execute(
        "SELECT MIN(service_date), MAX(service_date) FROM opportunities WHERE service_date > 0"
    ).fetchone()
    last_sync = _get_last_sync(con, "opportunities")

    con.close()

    # QB monthly (merged across all P&L files)
    qb_monthly = []
    qb_raw = _load_qb_monthly()
    if qb_raw:
        for month in sorted(qb_raw.keys()):
            d = qb_raw[month]
            rev = d["revenue"]
            c = d["cogs"]
            gp = rev - c
            margin = (gp / rev * 100) if rev else 0
            qb_monthly.append({
                "month": month, "revenue": round(rev, 2), "cogs": round(c, 2),
                "gross_profit": round(gp, 2), "margin": round(margin, 1),
                "status": "OK" if margin >= 45 else "WARN" if margin >= 40 else "MISS",
            })

    opp_trend = []
    for month, opps, booked, lost, cancelled, rev, avg in monthly_opps:
        m_str = f"{month[:4]}-{month[4:]}"
        rate = round(booked / opps * 100) if opps else 0
        opp_trend.append({
            "month": m_str, "opps": opps, "booked": booked, "book_pct": rate,
            "lost": lost, "cancelled": cancelled,
            "est_revenue": round(rev or 0, 2), "avg_job": round(avg or 0, 2),
        })

    weekly_trend = []
    for wb, opps, booked, rev in weekly:
        month_num = wb // 100
        week = wb % 100
        label = f"{month_num // 100}-{month_num % 100:02d} Wk{week}"
        weekly_trend.append({
            "label": label, "opps": opps, "booked": booked, "est_revenue": round(rev or 0, 2),
        })

    # Combine date ranges from both QB and opportunity data
    qb_months = [m["month"] for m in qb_monthly] if qb_monthly else []
    opp_months = [m["month"] for m in opp_trend] if opp_trend else []
    all_months = sorted(set(qb_months + opp_months))

    return {
        "generated": datetime.now().isoformat(),
        "date_range_start": all_months[0] if all_months else None,
        "date_range_end": all_months[-1] if all_months else None,
        "opp_date_range_start": _format_yyyymmdd(opp_date_range[0]) if opp_date_range and opp_date_range[0] else None,
        "opp_date_range_end": _format_yyyymmdd(opp_date_range[1]) if opp_date_range and opp_date_range[1] else None,
        "qb_date_range_start": qb_months[0] if qb_months else None,
        "qb_date_range_end": qb_months[-1] if qb_months else None,
        "last_sync": last_sync,
        "opportunity_trend": opp_trend,
        "qb_monthly": qb_monthly,
        "weekly_trend": weekly_trend,
    }


# ── Report: Virtual CFO ──

def get_cfo():
    """Analyze all data and generate prioritized needle-mover recommendations."""
    recommendations = []

    # ── Financial analysis ──
    fin = get_financial()
    if "profitability" in fin:
        p = fin["profitability"]
        revenue = p["revenue"]

        # Gross margin gap
        gm = p["gross_margin"]
        gm_target = 45
        if gm < gm_target:
            gap_pts = round(gm_target - gm, 1)
            dollar_impact = round(revenue * gap_pts / 100, 2)
            recommendations.append({
                "priority": 1,
                "category": "Profitability",
                "title": f"Close the {gap_pts}-point gross margin gap",
                "impact": dollar_impact,
                "impact_label": f"+{gap_pts} pts margin = +${dollar_impact:,.0f} gross profit",
                "detail": f"Current gross margin is {gm}% vs the 45% target. Each percentage point recovered is worth ${revenue/100:,.0f}. Focus on the largest cost overruns below.",
                "status": "MISS" if gap_pts > 5 else "WARN",
            })

        # Net margin gap
        nm = p["ebitda_margin"]
        nm_target = 20
        if nm < nm_target:
            gap_pts = round(nm_target - nm, 1)
            dollar_impact = round(revenue * gap_pts / 100, 2)
            recommendations.append({
                "priority": 2,
                "category": "Profitability",
                "title": f"Net margin at {nm}% vs 20% target",
                "impact": dollar_impact,
                "impact_label": f"Closing the gap = +${dollar_impact:,.0f} to bottom line",
                "detail": f"After all expenses, only {nm}% of revenue becomes profit. The gap represents ${dollar_impact:,.0f} in potential profit improvement.",
                "status": "MISS" if nm < 15 else "WARN",
            })

        # Cost overruns — find the biggest leaks
        for cc in fin.get("cost_control", []):
            if cc.get("target") and cc.get("status") in ("MISS", "WARN"):
                cat = cc["category"].strip()
                over = round(cc["pct"] - cc["target"], 1)
                dollar_over = round(revenue * over / 100, 2)
                if over > 0:
                    recommendations.append({
                        "priority": 3,
                        "category": "Cost Control",
                        "title": f"{cat} is {over} pts over target",
                        "impact": dollar_over,
                        "impact_label": f"Saving {over} pts = ${dollar_over:,.0f}/year",
                        "detail": f"{cat} is running at {cc['pct']}% of revenue vs the {cc['target']}% target. That's ${dollar_over:,.0f} over budget. This is one of the fastest levers to pull.",
                        "status": cc["status"],
                    })

        # Contractor ratio insight
        ls = fin.get("labor_split")
        if ls and ls["contractor_pct"] > 60:
            recommendations.append({
                "priority": 4,
                "category": "Labor Strategy",
                "title": f"Contractor ratio at {ls['contractor_pct']:.0f}% — review W-2 conversion",
                "impact": 0,
                "impact_label": "Strategic — reduces risk & may lower cost",
                "detail": f"Over {ls['contractor_pct']:.0f}% of direct labor is contractors (${ls['contracted']:,.0f}). High contractor ratios create 1099 compliance risk and often cost more per hour. Evaluate converting top performers to W-2.",
                "status": "WARN",
            })

        # Monthly trend — margin direction
        trend = fin.get("monthly_trend", [])
        if len(trend) >= 3:
            last3 = trend[-3:]
            margins = [m["margin"] for m in last3]
            if margins[-1] < margins[0]:
                drop = round(margins[0] - margins[-1], 1)
                recommendations.append({
                    "priority": 2,
                    "category": "Trend Alert",
                    "title": f"Gross margin trending down {drop} pts over last 3 months",
                    "impact": round(revenue / 12 * 3 * drop / 100, 2),
                    "impact_label": f"If trend continues: -${revenue/12*drop/100:,.0f}/month",
                    "detail": f"Margins went from {margins[0]}% to {margins[-1]}% over the last 3 months ({last3[0]['month']} to {last3[-1]['month']}). Investigate whether this is seasonal or a structural cost creep.",
                    "status": "MISS" if drop > 5 else "WARN",
                })

    # ── Sales analysis ──
    sales = get_sales()
    if "by_rep" in sales:
        reps = sales["by_rep"]
        total_opps = sales.get("totals", {}).get("opps", 0)
        total_rev = sales.get("totals", {}).get("revenue", 0)
        avg_rate = sales.get("overall_booking_rate", 0)

        # Booking rate improvement
        if avg_rate < 25:
            # What would 25% booking rate be worth?
            current_booked = sales.get("totals", {}).get("booked", 0)
            avg_job = total_rev / current_booked if current_booked > 0 else 0
            target_booked = round(total_opps * 0.25)
            additional_jobs = target_booked - current_booked
            additional_rev = round(additional_jobs * avg_job, 2)
            if additional_rev > 0:
                recommendations.append({
                    "priority": 1,
                    "category": "Sales",
                    "title": f"Raise booking rate from {avg_rate}% to 25%",
                    "impact": additional_rev,
                    "impact_label": f"+{additional_jobs} jobs = +${additional_rev:,.0f} revenue",
                    "detail": f"Currently booking {avg_rate}% of {total_opps:,} opportunities. Reaching 25% means {additional_jobs} more jobs at ${avg_job:,.0f} average. Focus coaching on the lowest-performing reps.",
                    "status": "MISS",
                })

        # Underperforming reps with volume
        for rep in reps:
            if rep["opps"] >= 20 and rep["book_pct"] < avg_rate * 0.7:
                avg_job = rep.get("avg_job", 0) or (rep["booked_rev"] / rep["booked"] if rep["booked"] > 0 else 0)
                potential_bookings = round(rep["opps"] * avg_rate / 100) - rep["booked"]
                potential_rev = round(potential_bookings * avg_job, 2) if avg_job > 0 else 0
                if potential_rev > 0:
                    recommendations.append({
                        "priority": 3,
                        "category": "Sales Coaching",
                        "title": f"{rep['rep']}: {rep['book_pct']}% booking rate vs {avg_rate}% average",
                        "impact": potential_rev,
                        "impact_label": f"Bringing to average = +${potential_rev:,.0f}",
                        "detail": f"{rep['rep']} has {rep['opps']} opportunities but only books {rep['book_pct']}%. If they hit the team average of {avg_rate}%, that's {potential_bookings} more jobs worth ~${potential_rev:,.0f}.",
                        "status": "WARN",
                    })

    # ── Marketing analysis ──
    mkt = get_marketing()
    if "channels" in mkt:
        channels = mkt["channels"]

        # High-ROI free channels
        free_rev = 0
        free_channels = []
        for ch in channels:
            if ch["channel"] in ("Repeat", "Referral", "Customer Referral", "Word of Mouth"):
                free_rev += ch.get("revenue", 0)
                free_channels.append(ch["channel"])

        total_mkt_rev = mkt.get("total_revenue", 0)
        if free_rev > 0 and total_mkt_rev > 0:
            free_pct = round(free_rev / total_mkt_rev * 100)
            recommendations.append({
                "priority": 3,
                "category": "Marketing",
                "title": f"Free channels drive {free_pct}% of revenue — invest in growing them",
                "impact": round(free_rev * 0.2, 2),
                "impact_label": f"20% growth in referrals = +${free_rev * 0.2:,.0f}",
                "detail": f"Repeat customers and referrals generated ${free_rev:,.0f} with zero marketing spend. A referral program or post-move follow-up could grow this 20%+ with minimal cost.",
                "status": "OK",
            })

        # Low-converting paid channels
        for ch in channels:
            if ch["channel"] not in ("Repeat", "Referral", "Customer Referral", "Word of Mouth", "Unknown"):
                if ch["leads"] >= 10 and ch["book_pct"] < 15:
                    recommendations.append({
                        "priority": 4,
                        "category": "Marketing",
                        "title": f"{ch['channel']}: {ch['leads']} leads but only {ch['book_pct']}% book",
                        "impact": 0,
                        "impact_label": "Cut waste or fix conversion",
                        "detail": f"{ch['channel']} is generating leads that don't convert. Either the lead quality is poor (wrong audience) or the follow-up process needs work. Consider reducing spend here and reallocating to higher-converting channels.",
                        "status": "WARN",
                    })

    # Sort by impact (highest first), then priority
    recommendations.sort(key=lambda r: (-r.get("impact", 0), r["priority"]))

    # Number them
    for i, rec in enumerate(recommendations):
        rec["rank"] = i + 1

    # Summary stats
    total_opportunity = sum(r["impact"] for r in recommendations if r["impact"] > 0)

    return {
        "generated": datetime.now().isoformat(),
        "total_opportunity": round(total_opportunity, 2),
        "recommendation_count": len(recommendations),
        "recommendations": recommendations,
    }


# ── Report: Cancellations ──

def get_cancellations():
    """Analyse cancelled and lost opportunities."""
    con = _get_duckdb()
    if not con:
        return {"error": "DuckDB not found. Run fast_sync.py first."}

    total_opps = con.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
    if total_opps == 0:
        con.close()
        return {"error": "No opportunities in DuckDB."}

    # Cancelled summary
    cancelled = con.execute("""
        SELECT COUNT(*), COALESCE(SUM(estimated_total), 0)
        FROM opportunities WHERE status = 30
    """).fetchone()

    # Lost summary
    lost = con.execute("""
        SELECT COUNT(*), COALESCE(SUM(estimated_total), 0)
        FROM opportunities WHERE status = 20
    """).fetchone()

    cancel_rate = round(cancelled[0] / total_opps * 100, 1) if total_opps else 0
    lost_rate = round(lost[0] / total_opps * 100, 1) if total_opps else 0

    # By rep
    by_rep = con.execute("""
        SELECT
            COALESCE(sales_assignee, 'Unassigned') as rep,
            COUNT(*) as total,
            SUM(CASE WHEN status = 30 THEN 1 ELSE 0 END) as cancelled,
            SUM(CASE WHEN status = 20 THEN 1 ELSE 0 END) as lost,
            SUM(CASE WHEN status = 30 THEN estimated_total ELSE 0 END) as cancel_rev,
            SUM(CASE WHEN status = 20 THEN estimated_total ELSE 0 END) as lost_rev
        FROM opportunities
        GROUP BY COALESCE(sales_assignee, 'Unassigned')
        HAVING cancelled > 0 OR lost > 0
        ORDER BY cancelled + lost DESC
    """).fetchall()

    # By month (using service_date)
    by_month = con.execute("""
        SELECT
            SUBSTR(CAST(service_date AS VARCHAR), 1, 6) as month,
            SUM(CASE WHEN status = 30 THEN 1 ELSE 0 END) as cancelled,
            SUM(CASE WHEN status = 20 THEN 1 ELSE 0 END) as lost,
            COUNT(*) as total,
            SUM(CASE WHEN status = 30 THEN estimated_total ELSE 0 END) as cancel_rev
        FROM opportunities
        WHERE service_date > 0
        GROUP BY SUBSTR(CAST(service_date AS VARCHAR), 1, 6)
        ORDER BY month
    """).fetchall()

    # By referral source
    by_source = con.execute("""
        SELECT
            COALESCE(referral_source, 'Unknown') as source,
            COUNT(*) as total,
            SUM(CASE WHEN status = 30 THEN 1 ELSE 0 END) as cancelled,
            SUM(CASE WHEN status = 20 THEN 1 ELSE 0 END) as lost
        FROM opportunities
        GROUP BY COALESCE(referral_source, 'Unknown')
        HAVING cancelled > 0 OR lost > 0
        ORDER BY cancelled + lost DESC
    """).fetchall()

    # Top 10 largest cancelled
    top_cancelled = con.execute("""
        SELECT customer_name, estimated_total, service_date, sales_assignee
        FROM opportunities
        WHERE status = 30 AND estimated_total > 0
        ORDER BY estimated_total DESC
        LIMIT 10
    """).fetchall()

    # Date range and last sync
    date_range = con.execute(
        "SELECT MIN(service_date), MAX(service_date) FROM opportunities WHERE service_date > 0"
    ).fetchone()
    last_sync = _get_last_sync(con, "opportunities")

    con.close()

    return {
        "generated": datetime.now().isoformat(),
        "date_range_start": _format_yyyymmdd(date_range[0]) if date_range and date_range[0] else None,
        "date_range_end": _format_yyyymmdd(date_range[1]) if date_range and date_range[1] else None,
        "last_sync": last_sync,
        "total_opportunities": total_opps,
        "cancelled_count": cancelled[0],
        "cancelled_revenue": round(cancelled[1], 2),
        "cancellation_rate": cancel_rate,
        "lost_count": lost[0],
        "lost_revenue": round(lost[1], 2),
        "lost_rate": lost_rate,
        "by_rep": [
            {"rep": r, "total": t, "cancelled": c, "lost": l,
             "cancel_rate": round(c / t * 100, 1) if t else 0,
             "cancel_rev": round(cr or 0, 2), "lost_rev": round(lr or 0, 2)}
            for r, t, c, l, cr, lr in by_rep
        ],
        "by_month": [
            {"month": f"{m[:4]}-{m[4:]}", "cancelled": c, "lost": l, "total": t,
             "cancel_rate": round(c / t * 100, 1) if t else 0,
             "cancel_rev": round(cr or 0, 2)}
            for m, c, l, t, cr in by_month
        ],
        "by_source": [
            {"source": s, "total": t, "cancelled": c, "lost": l,
             "cancel_rate": round(c / t * 100, 1) if t else 0}
            for s, t, c, l in by_source
        ],
        "top_cancelled": [
            {"customer": name or "Unknown", "estimated_total": round(est or 0, 2),
             "service_date": _format_yyyymmdd(sd) if sd else "N/A",
             "rep": rep or "Unassigned"}
            for name, est, sd, rep in top_cancelled
        ],
    }


# ── Report: Pipeline ──

def get_pipeline():
    """Forecast from active pipeline (non-terminal statuses)."""
    con = _get_duckdb()
    if not con:
        return {"error": "DuckDB not found. Run fast_sync.py first."}

    opp_count = con.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
    if opp_count == 0:
        con.close()
        return {"error": "No opportunities in DuckDB."}

    now = datetime.now()
    today_int = int(now.strftime("%Y%m%d"))
    day30 = int((now + timedelta(days=30)).strftime("%Y%m%d"))
    day60 = int((now + timedelta(days=60)).strftime("%Y%m%d"))
    day90 = int((now + timedelta(days=90)).strftime("%Y%m%d"))

    status_labels = {0: "New", 1: "Estimated", 2: "Follow Up", 3: "Booked", 5: "Confirmed"}
    active_statuses = (0, 1, 2, 3, 5)

    # Total pipeline
    pipeline_total = con.execute("""
        SELECT COUNT(*), COALESCE(SUM(estimated_total), 0)
        FROM opportunities WHERE status IN (0,1,2,3,5)
    """).fetchone()

    # By status breakdown
    by_status = con.execute("""
        SELECT status, COUNT(*), COALESCE(SUM(estimated_total), 0)
        FROM opportunities WHERE status IN (0,1,2,3,5)
        GROUP BY status ORDER BY status
    """).fetchall()

    # Next 30/60/90 days
    next30 = con.execute("""
        SELECT COUNT(*), COALESCE(SUM(estimated_total), 0)
        FROM opportunities
        WHERE status IN (0,1,2,3,5) AND service_date >= ? AND service_date <= ?
    """, [today_int, day30]).fetchone()

    next60 = con.execute("""
        SELECT COUNT(*), COALESCE(SUM(estimated_total), 0)
        FROM opportunities
        WHERE status IN (0,1,2,3,5) AND service_date >= ? AND service_date <= ?
    """, [today_int, day60]).fetchone()

    next90 = con.execute("""
        SELECT COUNT(*), COALESCE(SUM(estimated_total), 0)
        FROM opportunities
        WHERE status IN (0,1,2,3,5) AND service_date >= ? AND service_date <= ?
    """, [today_int, day90]).fetchone()

    # By rep
    by_rep = con.execute("""
        SELECT
            COALESCE(sales_assignee, 'Unassigned') as rep,
            COUNT(*) as cnt,
            COALESCE(SUM(estimated_total), 0) as est_rev
        FROM opportunities WHERE status IN (0,1,2,3,5)
        GROUP BY COALESCE(sales_assignee, 'Unassigned')
        ORDER BY est_rev DESC
    """).fetchall()

    # Average days from created to service date
    avg_days = con.execute("""
        SELECT AVG(
            (CAST(SUBSTR(CAST(service_date AS VARCHAR), 1, 4) AS INTEGER) * 365 +
             CAST(SUBSTR(CAST(service_date AS VARCHAR), 5, 2) AS INTEGER) * 30 +
             CAST(SUBSTR(CAST(service_date AS VARCHAR), 7, 2) AS INTEGER))
            -
            (CAST(SUBSTR(CAST(CAST(created_at_utc AS DATE) AS VARCHAR), 1, 4) AS INTEGER) * 365 +
             CAST(SUBSTR(CAST(CAST(created_at_utc AS DATE) AS VARCHAR), 6, 2) AS INTEGER) * 30 +
             CAST(SUBSTR(CAST(CAST(created_at_utc AS DATE) AS VARCHAR), 9, 2) AS INTEGER))
        )
        FROM opportunities
        WHERE status IN (0,1,2,3,5) AND service_date > 0 AND created_at_utc IS NOT NULL
    """).fetchone()

    last_sync = _get_last_sync(con, "opportunities")
    con.close()

    return {
        "generated": datetime.now().isoformat(),
        "last_sync": last_sync,
        "total_pipeline_count": pipeline_total[0],
        "total_pipeline_revenue": round(pipeline_total[1], 2),
        "by_status": [
            {"status": status_labels.get(s, f"Status {s}"), "count": c, "est_revenue": round(r, 2)}
            for s, c, r in by_status
        ],
        "next_30_days": {"count": next30[0], "est_revenue": round(next30[1], 2)},
        "next_60_days": {"count": next60[0], "est_revenue": round(next60[1], 2)},
        "next_90_days": {"count": next90[0], "est_revenue": round(next90[1], 2)},
        "by_rep": [
            {"rep": r, "count": c, "est_revenue": round(rv, 2)}
            for r, c, rv in by_rep
        ],
        "avg_days_created_to_service": round(avg_days[0]) if avg_days and avg_days[0] else None,
    }


# ── Report: Reviews ──

def get_reviews():
    """Read reviews from CSV in exports directory."""
    if not os.path.exists(EXPORT_DIR):
        return {"error": "No reviews data. Upload a CSV via Data Management."}

    # Find review CSV
    review_file = None
    for name in os.listdir(EXPORT_DIR):
        if "review" in name.lower() and name.endswith(".csv"):
            review_file = os.path.join(EXPORT_DIR, name)
            break

    if not review_file:
        return {"error": "No reviews data. Upload a CSV via Data Management."}

    reviews = []
    try:
        with open(review_file) as f:
            reader = csv.reader(f)
            header = next(reader, None)  # skip header
            for row in reader:
                if len(row) < 4:
                    continue
                try:
                    rating = int(float(row[1]))
                except (ValueError, IndexError):
                    rating = 0
                reviews.append({
                    "date": row[0].strip(),
                    "rating": rating,
                    "reviewer": row[2].strip(),
                    "text": row[3].strip(),
                })
    except Exception as e:
        return {"error": f"Error reading reviews CSV: {e}"}

    if not reviews:
        return {"error": "Reviews CSV is empty."}

    total = len(reviews)
    ratings = [r["rating"] for r in reviews if r["rating"] > 0]
    avg_rating = round(sum(ratings) / len(ratings), 2) if ratings else 0

    # Rating distribution
    dist = Counter(ratings)
    rating_distribution = [
        {"stars": s, "count": dist.get(s, 0), "pct": round(dist.get(s, 0) / total * 100, 1) if total else 0}
        for s in range(5, 0, -1)
    ]

    # By month
    by_month = defaultdict(lambda: {"count": 0, "total_rating": 0})
    for r in reviews:
        try:
            dt = datetime.strptime(r["date"], "%m/%d/%Y")
            mk = dt.strftime("%Y-%m")
        except ValueError:
            try:
                dt = datetime.strptime(r["date"], "%Y-%m-%d")
                mk = dt.strftime("%Y-%m")
            except ValueError:
                continue
        by_month[mk]["count"] += 1
        by_month[mk]["total_rating"] += r["rating"]

    monthly = []
    for m in sorted(by_month.keys()):
        d = by_month[m]
        monthly.append({
            "month": m, "count": d["count"],
            "avg_rating": round(d["total_rating"] / d["count"], 2) if d["count"] else 0,
        })

    # Recent 10
    recent = reviews[-10:] if len(reviews) >= 10 else reviews[:]
    recent.reverse()

    return {
        "generated": datetime.now().isoformat(),
        "total_reviews": total,
        "average_rating": avg_rating,
        "rating_distribution": rating_distribution,
        "by_month": monthly,
        "recent_reviews": recent,
    }


# ── Report: Claims ──

def get_claims():
    """Read claims from CSV in exports directory."""
    if not os.path.exists(EXPORT_DIR):
        return {"error": "No claims data. Upload a CSV via Data Management."}

    # Find claims CSV
    claims_file = None
    for name in os.listdir(EXPORT_DIR):
        if "claim" in name.lower() and name.endswith(".csv"):
            claims_file = os.path.join(EXPORT_DIR, name)
            break

    if not claims_file:
        return {"error": "No claims data. Upload a CSV via Data Management."}

    claims = []
    try:
        with open(claims_file) as f:
            reader = csv.reader(f)
            header = next(reader, None)  # skip header
            for row in reader:
                if len(row) < 5:
                    continue
                try:
                    amount = float(row[2].strip().replace("$", "").replace(",", ""))
                except (ValueError, IndexError):
                    amount = 0.0
                claims.append({
                    "date": row[0].strip(),
                    "customer": row[1].strip(),
                    "amount": round(amount, 2),
                    "description": row[3].strip(),
                    "status": row[4].strip(),
                })
    except Exception as e:
        return {"error": f"Error reading claims CSV: {e}"}

    if not claims:
        return {"error": "Claims CSV is empty."}

    total = len(claims)
    total_amount = round(sum(c["amount"] for c in claims), 2)
    open_count = sum(1 for c in claims if c["status"].lower() in ("open", "pending", "new"))
    resolved_count = sum(1 for c in claims if c["status"].lower() in ("resolved", "closed", "paid", "settled"))

    # By month
    by_month = defaultdict(lambda: {"count": 0, "total_amount": 0})
    for c in claims:
        try:
            dt = datetime.strptime(c["date"], "%m/%d/%Y")
            mk = dt.strftime("%Y-%m")
        except ValueError:
            try:
                dt = datetime.strptime(c["date"], "%Y-%m-%d")
                mk = dt.strftime("%Y-%m")
            except ValueError:
                continue
        by_month[mk]["count"] += 1
        by_month[mk]["total_amount"] += c["amount"]

    monthly = []
    for m in sorted(by_month.keys()):
        d = by_month[m]
        monthly.append({
            "month": m, "count": d["count"], "total_amount": round(d["total_amount"], 2),
        })

    # Recent claims (last 10 from the list)
    recent = claims[-10:] if len(claims) >= 10 else claims[:]
    recent.reverse()

    return {
        "generated": datetime.now().isoformat(),
        "total_claims": total,
        "total_amount": total_amount,
        "open_count": open_count,
        "resolved_count": resolved_count,
        "by_month": monthly,
        "recent_claims": recent,
    }


# ── Report: Year-over-Year ──

def get_yoy():
    """Year-over-year comparison of opportunities and financials."""
    con = _get_duckdb()
    if not con:
        return {"error": "DuckDB not found. Run fast_sync.py first."}

    opp_count = con.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
    if opp_count == 0:
        con.close()
        return {"error": "No opportunities in DuckDB."}

    # By year
    by_year = con.execute("""
        SELECT
            SUBSTR(CAST(service_date AS VARCHAR), 1, 4) as year,
            COUNT(*) as total_opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            SUM(CASE WHEN status IN (3,5,10,11) THEN estimated_total ELSE 0 END) as booked_rev,
            SUM(estimated_total) as total_est_rev,
            AVG(CASE WHEN status IN (3,5,10,11) AND estimated_total > 0 THEN estimated_total END) as avg_job
        FROM opportunities
        WHERE service_date > 0
        GROUP BY SUBSTR(CAST(service_date AS VARCHAR), 1, 4)
        ORDER BY year
    """).fetchall()

    # Monthly by year-month for side-by-side
    monthly_raw = con.execute("""
        SELECT
            SUBSTR(CAST(service_date AS VARCHAR), 1, 4) as year,
            SUBSTR(CAST(service_date AS VARCHAR), 5, 2) as month_num,
            COUNT(*) as opps,
            SUM(CASE WHEN status IN (3,5,10,11) THEN 1 ELSE 0 END) as booked,
            SUM(CASE WHEN status IN (3,5,10,11) THEN estimated_total ELSE 0 END) as booked_rev,
            AVG(CASE WHEN estimated_total > 0 THEN estimated_total END) as avg_est
        FROM opportunities
        WHERE service_date > 0
        GROUP BY SUBSTR(CAST(service_date AS VARCHAR), 1, 4),
                 SUBSTR(CAST(service_date AS VARCHAR), 5, 2)
        ORDER BY year, month_num
    """).fetchall()

    last_sync = _get_last_sync(con, "opportunities")
    con.close()

    # Count cancelled/lost per year
    cancel_lost = con.execute("""
        SELECT
            SUBSTR(CAST(service_date AS VARCHAR), 1, 4) as year,
            SUM(CASE WHEN status = 30 THEN 1 ELSE 0 END) as cancelled,
            SUM(CASE WHEN status = 20 THEN 1 ELSE 0 END) as lost
        FROM opportunities WHERE service_date > 0
        GROUP BY SUBSTR(CAST(service_date AS VARCHAR), 1, 4)
    """).fetchall()
    cl_map = {r[0]: (r[1], r[2]) for r in cancel_lost}

    # Build year summaries
    year_summaries = []
    for y, total, booked, booked_rev, total_rev, avg_job in by_year:
        booking_rate = round(booked / total * 100, 1) if total else 0
        cx, lo = cl_map.get(y, (0, 0))
        year_summaries.append({
            "year": y, "total_opps": total, "booked": booked,
            "booking_rate": booking_rate,
            "total_est_revenue": round(total_rev or 0, 2),
            "booked_revenue": round(booked_rev or 0, 2),
            "avg_job": round(avg_job or 0, 2),
            "cancelled": cx, "lost": lo,
        })

    # Build monthly comparison grouped by month number
    year_list = sorted(set(r[0] for r in monthly_raw))
    monthly_data = defaultdict(dict)
    for y, mn, opps, booked, rev, avg in monthly_raw:
        monthly_data[mn][y] = {
            "opps": opps, "booked": booked,
            "booked_rev": round(rev or 0, 2),
            "avg_est": round(avg or 0, 2),
        }

    month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    # Build flat monthly comparison with year1/year2 fields
    monthly_comparison = []
    for mn in sorted(monthly_data.keys()):
        mn_int = int(mn) if mn.isdigit() else 0
        label = month_names[mn_int] if 1 <= mn_int <= 12 else mn
        y1_data = monthly_data[mn].get(year_list[0]) if len(year_list) > 0 else None
        y2_data = monthly_data[mn].get(year_list[-1]) if len(year_list) > 1 else None
        monthly_comparison.append({
            "month_label": label,
            "year1_opps": y1_data["opps"] if y1_data else None,
            "year1_booked": y1_data["booked"] if y1_data else None,
            "year1_rev": round(y1_data["booked_rev"], 2) if y1_data else None,
            "year2_opps": y2_data["opps"] if y2_data else None,
            "year2_booked": y2_data["booked"] if y2_data else None,
            "year2_rev": round(y2_data["booked_rev"], 2) if y2_data else None,
        })

    # QB financial YoY — flat list for easy table rendering
    qb_monthly = _load_qb_monthly()
    qb_year_totals = defaultdict(lambda: {"revenue": 0, "cogs": 0})
    for mk, data in qb_monthly.items():
        year = mk[:4]
        qb_year_totals[year]["revenue"] += data.get("revenue", 0)
        qb_year_totals[year]["cogs"] += data.get("cogs", 0)

    qb_by_year = []
    for year in sorted(qb_year_totals.keys()):
        d = qb_year_totals[year]
        gp = d["revenue"] - d["cogs"]
        margin = round(gp / d["revenue"] * 100, 1) if d["revenue"] else 0
        qb_by_year.append({
            "year": year,
            "revenue": round(d["revenue"], 2),
            "cogs": round(d["cogs"], 2),
            "gross_profit": round(gp, 2),
            "margin": margin,
        })

    return {
        "generated": datetime.now().isoformat(),
        "last_sync": last_sync,
        "by_year": year_summaries,
        "years": year_list,
        "monthly_comparison": monthly_comparison,
        "qb_by_year": qb_by_year,
    }


# ── Report: Monthly Detail ──

def get_monthly_detail(month):
    """Detailed P&L for a specific month (format YYYY-MM)."""
    if not month or len(month) != 7 or month[4] != "-":
        return {"error": "Invalid month format. Use YYYY-MM (e.g. 2025-04)."}

    qb_monthly = _load_qb_monthly()
    if not qb_monthly:
        return {"error": "No QuickBooks data. Upload a P&L CSV via Data Management."}

    if month not in qb_monthly:
        available = sorted(qb_monthly.keys())
        return {"error": f"No data for {month}. Available months: {', '.join(available)}"}

    data = qb_monthly[month]
    revenue = data.get("revenue", 0)
    cogs = data.get("cogs", 0)
    overhead = data.get("overhead", 0)
    other_income = data.get("other_income", 0)
    other_expense = data.get("other_expense", 0)
    gross_profit = revenue - cogs
    margin = round(gross_profit / revenue * 100, 1) if revenue else 0
    net_income = revenue + other_income - cogs - overhead - other_expense

    accounts = data.get("accounts", {})

    # Group accounts by type
    revenue_accounts = {}
    cogs_accounts = {}
    overhead_accounts = {}
    other_accounts = {}
    for acct, amt in sorted(accounts.items()):
        if acct.startswith("4"):
            revenue_accounts[acct] = round(amt, 2)
        elif acct.startswith("5"):
            cogs_accounts[acct] = round(amt, 2)
        elif acct.startswith("6"):
            overhead_accounts[acct] = round(amt, 2)
        else:
            other_accounts[acct] = round(amt, 2)

    return {
        "generated": datetime.now().isoformat(),
        "month": month,
        "summary": {
            "revenue": round(revenue, 2),
            "cogs": round(cogs, 2),
            "gross_profit": round(gross_profit, 2),
            "gross_margin": margin,
            "overhead": round(overhead, 2),
            "other_income": round(other_income, 2),
            "other_expense": round(other_expense, 2),
            "net_income": round(net_income, 2),
        },
        "accounts": {
            "revenue": revenue_accounts,
            "cogs": cogs_accounts,
            "overhead": overhead_accounts,
            "other": other_accounts,
        },
    }