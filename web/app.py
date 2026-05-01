"""
MME Dashboard — FastAPI web application.

Usage:
    python3 web/app.py                  # Start on port 8000
    python3 web/app.py --port 8080      # Custom port
"""

import os
import sys
import argparse
import threading
from datetime import datetime

# Ensure project root is on the path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from db.init_schema import init_schema
from web.reports import get_leads, get_financial, get_estimates, get_sales, get_marketing, get_trends, get_cfo, DB_PATH, EXPORT_DIR

app = FastAPI(title="MME Dashboard", version="1.0")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

# Track sync status
sync_status = {"running": False, "last_result": None}


# ── Auto-init DB on startup ──
@app.on_event("startup")
def startup_init():
    """Create DuckDB and schema, then auto-sync if DB is empty."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    os.makedirs(EXPORT_DIR, exist_ok=True)
    init_schema(DB_PATH)
    print(f"  DB ready: {DB_PATH}")

    # Auto-sync SmartMoving data if DB is empty
    try:
        import duckdb
        con = duckdb.connect(DB_PATH, read_only=True)
        opp_count = con.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
        con.close()
        if opp_count == 0:
            print("  No opportunity data — starting auto-sync...")
            api_sync()
    except Exception as e:
        print(f"  Auto-sync check failed: {e}")


@app.get("/", response_class=HTMLResponse)
def index():
    index_path = os.path.join(STATIC_DIR, "index.html")
    with open(index_path) as f:
        return f.read()


@app.get("/api/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


# ── Data sync trigger ──
@app.post("/api/sync")
def api_sync():
    """Trigger a SmartMoving data sync in the background."""
    if sync_status["running"]:
        return {"status": "already_running", "message": "Sync is already in progress."}

    def run_sync():
        sync_status["running"] = True
        sync_status["last_result"] = None
        try:
            sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))
            from fast_sync import main as sync_main
            sync_main()
            sync_status["last_result"] = {"status": "success", "completed": datetime.now().isoformat()}
        except Exception as e:
            sync_status["last_result"] = {"status": "error", "error": str(e), "completed": datetime.now().isoformat()}
        finally:
            sync_status["running"] = False

    thread = threading.Thread(target=run_sync, daemon=True)
    thread.start()
    return {"status": "started", "message": "Sync started in background. Check /api/sync/status for progress."}


@app.get("/api/sync/status")
def api_sync_status():
    return {
        "running": sync_status["running"],
        "last_result": sync_status["last_result"],
    }


# ── QB CSV upload ──
@app.post("/api/upload-pl")
async def upload_pl(file: UploadFile = File(...)):
    """Upload a QuickBooks Profit & Loss Detail CSV. Saved alongside existing files — newer uploads override overlapping months."""
    if not file.filename.endswith(".csv"):
        return JSONResponse(status_code=400, content={"error": "File must be a CSV"})

    os.makedirs(EXPORT_DIR, exist_ok=True)
    # Save with timestamp so multiple uploads coexist (newest wins for overlapping months)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(EXPORT_DIR, f"profit_and_loss_upload_{ts}.csv")
    content = await file.read()
    with open(dest, "wb") as f:
        f.write(content)

    return {"status": "ok", "message": f"Uploaded {file.filename} ({len(content):,} bytes). Data merged with existing P&L files — updated months will override older data.", "path": dest}


@app.get("/api/data-status")
def api_data_status():
    """Check data freshness — when was QB CSV last uploaded, when was last sync."""
    import duckdb
    result = {"qb_uploaded": None, "last_sync": None, "opp_count": 0}

    # Check QB CSV
    pl_path = os.path.join(EXPORT_DIR, "mme_profit_and_loss_detail.csv")
    if os.path.exists(pl_path):
        mtime = os.path.getmtime(pl_path)
        result["qb_uploaded"] = datetime.fromtimestamp(mtime).isoformat()

    # Check DuckDB
    if os.path.exists(DB_PATH):
        try:
            con = duckdb.connect(DB_PATH, read_only=True)
            row = con.execute("SELECT COUNT(*) FROM opportunities").fetchone()
            result["opp_count"] = row[0] if row else 0
            sync_row = con.execute(
                "SELECT completed_at FROM sync_log WHERE status='success' ORDER BY completed_at DESC LIMIT 1"
            ).fetchone()
            if sync_row and sync_row[0]:
                result["last_sync"] = sync_row[0].isoformat() if hasattr(sync_row[0], 'isoformat') else str(sync_row[0])
            con.close()
        except Exception:
            pass

    return result


@app.get("/api/overview")
def api_overview():
    """Pulls key metrics from financial, sales, marketing, and leads for the big picture."""
    # Each sub-report may return {"error": "..."} — handle gracefully
    try:
        fin = get_financial()
    except Exception:
        fin = {"error": "Financial data unavailable"}
    try:
        sales = get_sales()
    except Exception:
        sales = {"error": "Sales data unavailable"}
    try:
        mkt = get_marketing()
    except Exception:
        mkt = {"error": "Marketing data unavailable"}

    has_fin = "error" not in fin
    has_sales = "error" not in sales
    has_mkt = "error" not in mkt

    return {
        "generated": datetime.now().isoformat(),
        "qb_date_range_start": fin.get("date_range_start") if has_fin else None,
        "qb_date_range_end": fin.get("date_range_end") if has_fin else None,
        "opp_date_range_start": sales.get("date_range_start") if has_sales else None,
        "opp_date_range_end": sales.get("date_range_end") if has_sales else None,
        "last_sync": sales.get("last_sync") if has_sales else None,
        "financial": fin.get("profitability") if has_fin else None,
        "financial_trend": fin.get("monthly_trend", [])[-3:] if has_fin else [],
        "cost_control": fin.get("cost_control", []) if has_fin else [],
        "labor_split": fin.get("labor_split") if has_fin else None,
        "sales": {
            "total_opps": sales.get("totals", {}).get("opps", 0) if has_sales else 0,
            "total_booked": sales.get("totals", {}).get("booked", 0) if has_sales else 0,
            "booking_rate": sales.get("overall_booking_rate", 0) if has_sales else 0,
            "booking_rate_status": sales.get("booking_rate_status", "") if has_sales else "",
            "revenue": sales.get("totals", {}).get("revenue", 0) if has_sales else 0,
            "top_reps": sales.get("by_rep", [])[:5] if has_sales else [],
            "pipeline": sales.get("pipeline", []) if has_sales else [],
        },
        "marketing": {
            "spend": mkt.get("marketing_spend", 0) if has_mkt else 0,
            "total_revenue": mkt.get("total_revenue", 0) if has_mkt else 0,
            "efficiency": mkt.get("efficiency") if has_mkt else None,
            "top_channels": (mkt.get("channels") or [])[:5] if has_mkt else [],
        },
        "data_status": {
            "has_financial": has_fin,
            "has_sales": has_sales,
            "has_marketing": has_mkt,
            "financial_error": fin.get("error") if not has_fin else None,
            "sales_error": sales.get("error") if not has_sales else None,
        },
    }


@app.get("/api/leads")
def api_leads():
    try:
        return get_leads()
    except Exception as e:
        return {"error": f"Leads unavailable: {e}"}


@app.get("/api/financial")
def api_financial():
    try:
        return get_financial()
    except Exception as e:
        return {"error": f"Financial data unavailable: {e}"}


@app.get("/api/estimates")
def api_estimates():
    try:
        return get_estimates()
    except Exception as e:
        return {"error": f"Estimates unavailable: {e}"}


@app.get("/api/sales")
def api_sales():
    try:
        return get_sales()
    except Exception as e:
        return {"error": f"Sales data unavailable: {e}"}


@app.get("/api/marketing")
def api_marketing():
    try:
        return get_marketing()
    except Exception as e:
        return {"error": f"Marketing data unavailable: {e}"}


@app.get("/api/trends")
def api_trends():
    try:
        return get_trends()
    except Exception as e:
        return {"error": f"Trends unavailable: {e}"}


@app.get("/api/cfo")
def api_cfo():
    try:
        return get_cfo()
    except Exception as e:
        return {"error": f"CFO analysis unavailable: {e}"}


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
