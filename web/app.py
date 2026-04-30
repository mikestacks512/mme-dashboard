"""
MME Dashboard — FastAPI web application.

Usage:
    python3 web/app.py                  # Start on port 8000
    python3 web/app.py --port 8080      # Custom port
"""

import os
import sys
import argparse
from datetime import datetime

# Ensure project root is on the path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from web.reports import get_leads, get_financial, get_estimates, get_sales, get_marketing, get_trends, get_cfo

app = FastAPI(title="MME Dashboard", version="1.0")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


@app.get("/", response_class=HTMLResponse)
def index():
    index_path = os.path.join(STATIC_DIR, "index.html")
    with open(index_path) as f:
        return f.read()


@app.get("/api/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


@app.get("/api/overview")
def api_overview():
    """Pulls key metrics from financial, sales, marketing, and leads for the big picture."""
    fin = get_financial()
    sales = get_sales()
    mkt = get_marketing()
    return {
        "generated": datetime.now().isoformat(),
        "qb_date_range_start": fin.get("date_range_start"),
        "qb_date_range_end": fin.get("date_range_end"),
        "opp_date_range_start": sales.get("date_range_start"),
        "opp_date_range_end": sales.get("date_range_end"),
        "last_sync": sales.get("last_sync"),
        "financial": fin.get("profitability") if "profitability" in fin else None,
        "financial_trend": fin.get("monthly_trend", [])[-3:] if "monthly_trend" in fin else [],
        "cost_control": fin.get("cost_control", []),
        "labor_split": fin.get("labor_split"),
        "sales": {
            "total_opps": sales.get("totals", {}).get("opps", 0),
            "total_booked": sales.get("totals", {}).get("booked", 0),
            "booking_rate": sales.get("overall_booking_rate", 0),
            "booking_rate_status": sales.get("booking_rate_status", ""),
            "revenue": sales.get("totals", {}).get("revenue", 0),
            "top_reps": sales.get("by_rep", [])[:5],
            "pipeline": sales.get("pipeline", []),
        },
        "marketing": {
            "spend": mkt.get("marketing_spend", 0),
            "total_revenue": mkt.get("total_revenue", 0),
            "efficiency": mkt.get("efficiency"),
            "top_channels": (mkt.get("channels") or [])[:5],
        },
    }


@app.get("/api/leads")
def api_leads():
    return get_leads()


@app.get("/api/financial")
def api_financial():
    return get_financial()


@app.get("/api/estimates")
def api_estimates():
    return get_estimates()


@app.get("/api/sales")
def api_sales():
    return get_sales()


@app.get("/api/marketing")
def api_marketing():
    return get_marketing()


@app.get("/api/trends")
def api_trends():
    return get_trends()


@app.get("/api/cfo")
def api_cfo():
    return get_cfo()


# Static files mounted last so routes take priority
if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", default="0.0.0.0")
    args = parser.parse_args()
    print(f"\n  MME Dashboard starting at http://{args.host}:{args.port}")
    print(f"  Open http://localhost:{args.port} in your browser\n")
    uvicorn.run(app, host=args.host, port=args.port)
