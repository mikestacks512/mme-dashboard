"""
Estimate Accuracy Report — estimated vs actual variance.

Usage:
    python3 scripts/report_estimates.py              # last 30 days
    python3 scripts/report_estimates.py --days 90    # last 90 days
"""

import sys
import os
import argparse
import time
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_api import api_get, get_opportunity_detail

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "mme_dashboard.duckdb")


def date_int(dt):
    return int(dt.strftime("%Y%m%d"))


def format_money(amount):
    if amount is None or amount == 0:
        return "$0"
    return f"${amount:,.0f}"


def get_customer_ids():
    try:
        import duckdb
        if not os.path.exists(DB_PATH):
            return None
        con = duckdb.connect(DB_PATH, read_only=True)
        ids = [r[0] for r in con.execute("SELECT id FROM customers").fetchall()]
        con.close()
        return ids if ids else None
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--full-scan", action="store_true")
    parser.add_argument("--sample", type=int, default=5000)
    args = parser.parse_args()

    now = datetime.now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    date_from = date_int(today - timedelta(days=args.days))
    date_to = date_int(today)

    customer_ids = get_customer_ids()
    if not customer_ids:
        print("No customer data. Run: python3 scripts/ingest_smartmoving.py --table customers")
        return

    max_cust = None if args.full_scan else args.sample
    if max_cust:
        customer_ids = customer_ids[:max_cust]

    print(f"Scanning {len(customer_ids)} customers for estimate accuracy (last {args.days} days)...\n")

    jobs_with_both = []  # jobs that have both estimated and actual charges
    all_opps = []

    for i, cust_id in enumerate(customer_ids):
        if (i + 1) % 500 == 0:
            print(f"  Scanning... {i+1}/{len(customer_ids)}, {len(jobs_with_both)} jobs with est+actual")
        try:
            opps = api_get(f"/customers/{cust_id}/opportunities")
        except Exception:
            continue

        for opp_summary in opps.get("pageResults", []):
            svc_date = opp_summary.get("serviceDate", 0)
            if not (date_from <= svc_date <= date_to):
                continue

            try:
                opp = get_opportunity_detail(opp_summary["id"])
            except Exception:
                continue

            all_opps.append(opp)
            rep_name = (opp.get("salesAssignee") or {}).get("name") or "Unassigned"
            move_size = (opp.get("moveSize") or {}).get("name") if isinstance(opp.get("moveSize"), dict) else opp.get("moveSize")
            ref_source = opp.get("referralSource") or "Unknown"

            for j in opp.get("jobs") or []:
                est = (j.get("estimatedCharges") or {}).get("finalTotal")
                act = (j.get("actualCharges") or {}).get("finalTotal")
                if est and act and est > 0:
                    variance = act - est
                    variance_pct = (variance / est) * 100
                    jobs_with_both.append({
                        "job_number": j.get("jobNumber"),
                        "quote_number": opp.get("quoteNumber"),
                        "customer": (opp.get("customer") or {}).get("name"),
                        "rep": rep_name,
                        "move_size": move_size,
                        "ref_source": ref_source,
                        "estimated": est,
                        "actual": act,
                        "variance": variance,
                        "variance_pct": variance_pct,
                        "service_date": opp.get("serviceDate"),
                    })

        time.sleep(0.15)

    # ── Output ──
    print(f"\n{'='*90}")
    print(f"  ESTIMATE ACCURACY REPORT — Last {args.days} Days ({now.strftime('%B %d, %Y')})")
    print(f"{'='*90}")
    print(f"  Opportunities scanned: {len(all_opps)} | Jobs with est+actual: {len(jobs_with_both)}")

    if not jobs_with_both:
        # Fall back to opportunity-level estimates only
        opps_with_est = [o for o in all_opps if (o.get("estimatedTotal") or {}).get("finalTotal", 0) > 0]
        print(f"\n  No jobs have both estimated AND actual charges populated.")
        print(f"  Opportunities with estimates: {len(opps_with_est)}")
        if opps_with_est:
            total_est = sum((o.get("estimatedTotal") or {}).get("finalTotal", 0) for o in opps_with_est)
            avg_est = total_est / len(opps_with_est)
            print(f"  Total Estimated Revenue: {format_money(total_est)}")
            print(f"  Avg Estimate:            {format_money(avg_est)}")

            # By rep
            rep_estimates = defaultdict(list)
            for o in opps_with_est:
                rep = (o.get("salesAssignee") or {}).get("name") or "Unassigned"
                rep_estimates[rep].append((o.get("estimatedTotal") or {}).get("finalTotal", 0))

            print(f"\n  ESTIMATES BY REP")
            print(f"  {'─'*50}")
            print(f"  {'Rep':<25} {'Count':>6} {'Total':>12} {'Avg':>10}")
            print(f"  {'-'*25} {'-'*6} {'-'*12} {'-'*10}")
            for rep in sorted(rep_estimates, key=lambda r: sum(rep_estimates[r]), reverse=True):
                vals = rep_estimates[rep]
                print(f"  {rep:<25} {len(vals):>6} {format_money(sum(vals)):>12} {format_money(sum(vals)/len(vals)):>10}")

        print(f"\n{'='*90}")
        return

    # Full analysis with both est + actual
    total_est = sum(j["estimated"] for j in jobs_with_both)
    total_act = sum(j["actual"] for j in jobs_with_both)
    total_var = total_act - total_est
    avg_var_pct = (total_var / total_est * 100) if total_est else 0

    over_10 = [j for j in jobs_with_both if j["variance_pct"] > 10]
    under_10 = [j for j in jobs_with_both if j["variance_pct"] < -10]

    print(f"\n  SUMMARY")
    print(f"  {'─'*50}")
    print(f"    Total Estimated:    {format_money(total_est)}")
    print(f"    Total Actual:       {format_money(total_act)}")
    print(f"    Net Variance:       {format_money(total_var)} ({avg_var_pct:+.1f}%)")
    print(f"    Over-Estimated >10%: {len(over_10)} jobs (left money on table)")
    print(f"    Under-Estimated >10%: {len(under_10)} jobs (eating margin)")

    # By rep
    rep_data = defaultdict(list)
    for j in jobs_with_both:
        rep_data[j["rep"]].append(j)

    print(f"\n  VARIANCE BY REP (absolute, not netted)")
    print(f"  {'─'*70}")
    print(f"  {'Rep':<22} {'Jobs':>5} {'Avg Var%':>9} {'Over 10%':>9} {'Under 10%':>10} {'Net Var':>12}")
    print(f"  {'-'*22} {'-'*5} {'-'*9} {'-'*9} {'-'*10} {'-'*12}")

    for rep in sorted(rep_data, key=lambda r: abs(sum(j["variance"] for j in rep_data[r])), reverse=True):
        rj = rep_data[rep]
        avg_v = sum(abs(j["variance_pct"]) for j in rj) / len(rj)  # absolute avg
        over = sum(1 for j in rj if j["variance_pct"] > 10)
        under = sum(1 for j in rj if j["variance_pct"] < -10)
        net = sum(j["variance"] for j in rj)
        print(f"  {rep:<22} {len(rj):>5} {avg_v:>8.1f}% {over:>9} {under:>10} {format_money(net):>12}")

    # Worst over-estimates
    if over_10:
        print(f"\n  TOP OVER-ESTIMATES (left money on table)")
        print(f"  {'─'*70}")
        for j in sorted(over_10, key=lambda x: x["variance"], reverse=True)[:10]:
            print(f"    Job {j['job_number']}: Est {format_money(j['estimated'])} → Actual {format_money(j['actual'])} ({j['variance_pct']:+.0f}%) — {j['rep']}")

    # Worst under-estimates
    if under_10:
        print(f"\n  TOP UNDER-ESTIMATES (eating margin)")
        print(f"  {'─'*70}")
        for j in sorted(under_10, key=lambda x: x["variance"])[:10]:
            print(f"    Job {j['job_number']}: Est {format_money(j['estimated'])} → Actual {format_money(j['actual'])} ({j['variance_pct']:+.0f}%) — {j['rep']}")

    print(f"\n{'='*90}")


if __name__ == "__main__":
    main()
