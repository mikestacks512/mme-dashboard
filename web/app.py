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
from typing import Optional

# Ensure project root is on the path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from db.init_schema import init_schema
from web.reports import (
    get_leads, get_financial, get_estimates, get_sales, get_marketing,
    get_trends, get_cfo, get_cancellations, get_pipeline, get_reviews,
    get_claims, get_yoy, get_monthly_detail, get_dispatch,
    DB_PATH, EXPORT_DIR,
)
from web import inputs as manual_inputs

app = FastAPI(title="MME Dashboard", version="2.0")

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


# ── File uploads ──
@app.post("/api/upload-pl")
async def upload_pl(file: UploadFile = File(...)):
    """Upload a QuickBooks Profit & Loss Detail CSV."""
    if not file.filename.endswith(".csv"):
        return JSONResponse(status_code=400, content={"error": "File must be a CSV"})
    os.makedirs(EXPORT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(EXPORT_DIR, f"profit_and_loss_upload_{ts}.csv")
    content = await file.read()
    with open(dest, "wb") as f:
        f.write(content)
    return {"status": "ok", "message": f"Uploaded {file.filename} ({len(content):,} bytes). Updated months will override older data."}


@app.post("/api/upload-reviews")
async def upload_reviews(file: UploadFile = File(...)):
    """Upload a Google Reviews CSV. Format: date,rating,reviewer,text"""
    if not file.filename.endswith(".csv"):
        return JSONResponse(status_code=400, content={"error": "File must be a CSV"})
    os.makedirs(EXPORT_DIR, exist_ok=True)
    dest = os.path.join(EXPORT_DIR, "reviews.csv")
    content = await file.read()
    with open(dest, "wb") as f:
        f.write(content)
    return {"status": "ok", "message": f"Uploaded {file.filename} ({len(content):,} bytes)."}


@app.post("/api/upload-claims")
async def upload_claims(file: UploadFile = File(...)):
    """Upload a Claims CSV. Format: date,customer,amount,description,status"""
    if not file.filename.endswith(".csv"):
        return JSONResponse(status_code=400, content={"error": "File must be a CSV"})
    os.makedirs(EXPORT_DIR, exist_ok=True)
    dest = os.path.join(EXPORT_DIR, "claims.csv")
    content = await file.read()
    with open(dest, "wb") as f:
        f.write(content)
    return {"status": "ok", "message": f"Uploaded {file.filename} ({len(content):,} bytes)."}


@app.get("/api/data-status")
def api_data_status():
    """Check data freshness."""
    import duckdb
    result = {"qb_uploaded": None, "last_sync": None, "opp_count": 0,
              "has_reviews": False, "has_claims": False}

    pl_path = os.path.join(EXPORT_DIR, "mme_profit_and_loss_detail.csv")
    if os.path.exists(pl_path):
        result["qb_uploaded"] = datetime.fromtimestamp(os.path.getmtime(pl_path)).isoformat()
    result["has_reviews"] = os.path.exists(os.path.join(EXPORT_DIR, "reviews.csv"))
    result["has_claims"] = os.path.exists(os.path.join(EXPORT_DIR, "claims.csv"))

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


# ── Report endpoints ──

def _safe(fn, label):
    """Wrap a report function with error handling."""
    try:
        return fn()
    except Exception as e:
        return {"error": f"{label}: {e}"}


@app.get("/api/overview")
def api_overview():
    fin = _safe(get_financial, "Financial")
    sales = _safe(get_sales, "Sales")
    mkt = _safe(get_marketing, "Marketing")

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
    return _safe(get_leads, "Leads")

@app.get("/api/financial")
def api_financial():
    return _safe(get_financial, "Financial")

@app.get("/api/estimates")
def api_estimates():
    return _safe(get_estimates, "Estimates")

@app.get("/api/sales")
def api_sales():
    return _safe(get_sales, "Sales")

@app.get("/api/marketing")
def api_marketing():
    return _safe(get_marketing, "Marketing")

@app.get("/api/trends")
def api_trends():
    return _safe(get_trends, "Trends")

@app.get("/api/cfo")
def api_cfo():
    return _safe(get_cfo, "CFO")

@app.get("/api/cancellations")
def api_cancellations():
    return _safe(get_cancellations, "Cancellations")

@app.get("/api/pipeline")
def api_pipeline():
    return _safe(get_pipeline, "Pipeline")

@app.get("/api/reviews")
def api_reviews():
    return _safe(get_reviews, "Reviews")

@app.get("/api/claims")
def api_claims():
    return _safe(get_claims, "Claims")

@app.get("/api/yoy")
def api_yoy():
    return _safe(get_yoy, "Year-over-Year")

@app.get("/api/monthly/{month}")
def api_monthly(month: str):
    try:
        return get_monthly_detail(month)
    except Exception as e:
        return {"error": f"Monthly detail: {e}"}

# ── Manual inputs (trucks, storage, turnaways) ──

def _input_error(e):
    return JSONResponse(status_code=400, content={"error": str(e)})


@app.get("/api/inputs/summary")
def api_inputs_summary():
    try:
        return manual_inputs.inputs_summary()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/inputs/trucks")
def api_inputs_trucks_list():
    try:
        return {"trucks": manual_inputs.list_trucks()}
    except Exception as e:
        return _input_error(e)


@app.post("/api/inputs/trucks")
async def api_inputs_trucks_upsert(request: Request):
    try:
        payload = await request.json()
        return manual_inputs.upsert_truck(payload)
    except ValueError as e:
        return _input_error(e)
    except Exception as e:
        return _input_error(e)


@app.delete("/api/inputs/trucks/{truck_id}")
def api_inputs_trucks_delete(truck_id: str):
    try:
        return manual_inputs.delete_truck(truck_id)
    except Exception as e:
        return _input_error(e)


@app.get("/api/inputs/truck-utilization")
def api_inputs_util_list(start: Optional[str] = None, end: Optional[str] = None):
    try:
        return {"entries": manual_inputs.list_truck_utilization(start, end)}
    except Exception as e:
        return _input_error(e)


@app.post("/api/inputs/truck-utilization")
async def api_inputs_util_upsert(request: Request):
    try:
        payload = await request.json()
        return manual_inputs.upsert_truck_utilization(payload)
    except ValueError as e:
        return _input_error(e)
    except Exception as e:
        return _input_error(e)


@app.delete("/api/inputs/truck-utilization")
def api_inputs_util_delete(entry_date: str, truck_id: str):
    try:
        return manual_inputs.delete_truck_utilization(entry_date, truck_id)
    except Exception as e:
        return _input_error(e)


@app.get("/api/inputs/storage-units")
def api_inputs_units_list():
    try:
        return {"units": manual_inputs.list_storage_units()}
    except Exception as e:
        return _input_error(e)


@app.post("/api/inputs/storage-units")
async def api_inputs_units_upsert(request: Request):
    try:
        payload = await request.json()
        return manual_inputs.upsert_storage_unit(payload)
    except ValueError as e:
        return _input_error(e)
    except Exception as e:
        return _input_error(e)


@app.delete("/api/inputs/storage-units/{unit_id}")
def api_inputs_units_delete(unit_id: str):
    try:
        return manual_inputs.delete_storage_unit(unit_id)
    except Exception as e:
        return _input_error(e)


@app.get("/api/inputs/storage-snapshots")
def api_inputs_snapshots_list(limit: int = 90):
    try:
        return {"snapshots": manual_inputs.list_storage_snapshots(limit)}
    except Exception as e:
        return _input_error(e)


@app.post("/api/inputs/storage-snapshots")
async def api_inputs_snapshots_upsert(request: Request):
    try:
        payload = await request.json()
        return manual_inputs.upsert_storage_snapshot(payload)
    except ValueError as e:
        return _input_error(e)
    except Exception as e:
        return _input_error(e)


@app.delete("/api/inputs/storage-snapshots/{snapshot_date}")
def api_inputs_snapshots_delete(snapshot_date: str):
    try:
        return manual_inputs.delete_storage_snapshot(snapshot_date)
    except Exception as e:
        return _input_error(e)


@app.get("/api/inputs/turnaways")
def api_inputs_turnaways_list(limit: int = 200):
    try:
        return {"entries": manual_inputs.list_turnaways(limit)}
    except Exception as e:
        return _input_error(e)


@app.post("/api/inputs/turnaways")
async def api_inputs_turnaways_upsert(request: Request):
    try:
        payload = await request.json()
        return manual_inputs.upsert_turnaway(payload)
    except ValueError as e:
        return _input_error(e)
    except Exception as e:
        return _input_error(e)


@app.delete("/api/inputs/turnaways")
def api_inputs_turnaways_delete(entry_date: str, id: str):
    try:
        return manual_inputs.delete_turnaway(entry_date, id)
    except Exception as e:
        return _input_error(e)


@app.get("/api/dispatch")
def api_dispatch():
    return _safe(get_dispatch, "Dispatch")


@app.get("/api/storage")
def api_storage():
    try:
        return manual_inputs.get_storage_report()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/fleet")
def api_fleet(days: int = 30):
    try:
        return manual_inputs.get_truck_report(days)
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/callcenter")
def api_callcenter():
    try:
        sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))
        from dialpad_api import get_call_center_report
        return get_call_center_report(days=90)
    except Exception as e:
        return {"error": f"Call center data unavailable: {e}"}


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
