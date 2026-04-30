#!/usr/bin/env python3
"""
MME Report Runner — operational reports mapped to dashboard spec pages.

Usage:
    python3 report.py command-center         # Page 1: The Scoreboard
    python3 report.py financial              # Page 3: Financial Control
    python3 report.py leads                  # Lead Pipeline (instant)
    python3 report.py all                    # All fast reports
    python3 report.py all --save             # Save to reports/ folder

Page 1 and leads are live from SmartMoving API.
Page 3 is from QuickBooks CSV exports in exports/.
"""

import sys
import os
import subprocess
from datetime import datetime

SCRIPTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts")
REPORTS_OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")

REPORTS = {
    "command-center": ("report_command_center.py", "Page 1: Command Center — Are we making money?"),
    "financial":      ("report_financial.py",      "Page 3: Financial Control — Costs & Profitability"),
    "estimates":      ("report_estimates_p4.py",   "Page 4: Estimate Accuracy & Pricing Intelligence"),
    "sales":          ("report_sales_p5.py",       "Page 5: Sales Performance by Rep"),
    "marketing":      ("report_marketing_p7.py",   "Page 7: Marketing Channel Performance"),
    "trends":         ("report_trends_p12.py",     "Page 12: Weekly/Monthly Trends"),
    "leads":          ("report_leads.py",          "Lead Pipeline — Active leads & urgency"),
}

# Reports that run instantly from local data (DuckDB + QB CSV)
FAST_REPORTS = ["financial", "estimates", "sales", "marketing", "trends", "leads"]


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help", "help"):
        print("MME Operational Reports\n")
        print("Reports:")
        for name, (_, desc) in REPORTS.items():
            speed = "(fast)" if name in FAST_REPORTS else "(scans API)"
            print(f"  {name:<20} {desc} {speed}")
        print(f"\n  {'all':<20} Run all fast reports")
        print(f"\nOptions:")
        print(f"  --save           Save output to reports/ folder")
        print(f"  --days N         Period for command-center (default: today)")
        print(f"  --mtd            Month to date for command-center")
        print(f"  --sample N       Customers to scan (default: 5000)")
        print(f"  --full-scan      Scan all 36K customers (slow)")
        print(f"\nExamples:")
        print(f"  python3 report.py command-center --mtd")
        print(f"  python3 report.py financial --save")
        print(f"  python3 report.py all --save")
        return

    save = "--save" in sys.argv
    extra_args = [a for a in sys.argv[2:] if a != "--save"]
    report_name = sys.argv[1]

    if report_name == "all":
        os.makedirs(REPORTS_OUT, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        for name in FAST_REPORTS:
            script, desc = REPORTS[name]
            print(f"\n{'#'*70}")
            print(f"# {desc}")
            print(f"{'#'*70}")
            if save:
                outfile = os.path.join(REPORTS_OUT, f"{name}_{timestamp}.txt")
                with open(outfile, "w") as f:
                    subprocess.run([sys.executable, os.path.join(SCRIPTS, script)] + extra_args,
                                   stdout=f, stderr=subprocess.STDOUT)
                with open(outfile) as f:
                    print(f.read())
                print(f"  >> Saved to {outfile}")
            else:
                subprocess.run([sys.executable, os.path.join(SCRIPTS, script)] + extra_args)
        if save:
            print(f"\n  All reports saved to: {REPORTS_OUT}/")
        return

    if report_name not in REPORTS:
        print(f"Unknown report: {report_name}")
        print(f"Available: {', '.join(REPORTS.keys())}")
        sys.exit(1)

    script, desc = REPORTS[report_name]
    if save:
        os.makedirs(REPORTS_OUT, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        outfile = os.path.join(REPORTS_OUT, f"{report_name}_{timestamp}.txt")
        with open(outfile, "w") as f:
            subprocess.run([sys.executable, os.path.join(SCRIPTS, script)] + extra_args,
                           stdout=f, stderr=subprocess.STDOUT)
        with open(outfile) as f:
            print(f.read())
        print(f"\n  >> Saved to {outfile}")
    else:
        subprocess.run([sys.executable, os.path.join(SCRIPTS, script)] + extra_args)


if __name__ == "__main__":
    main()
