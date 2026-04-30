# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Operations dashboard for Muscleman Elite Moving (MME). Ingests data from SmartMoving CRM API and QuickBooks into a local DuckDB database, then generates CLI-based reports. Runs on WSL2 (Linux on Windows).

## Key Commands

```bash
# Activate virtualenv (required before running anything)
source venv/bin/activate

# Initialize/reset the DuckDB schema (safe to re-run)
python3 db/init_schema.py

# Ingest all SmartMoving data into DuckDB
python3 scripts/ingest_smartmoving.py

# Ingest a single table
python3 scripts/ingest_smartmoving.py --table leads

# Run reports via the report runner
python3 report.py leads          # Lead pipeline (live API, fast)
python3 report.py team           # Team & branches (live API, fast)
python3 report.py daily          # Today's summary (scans customers, slow)
python3 report.py daily --days 7 # Last 7 days
python3 report.py sales          # Sales rep performance (scans customers)
python3 report.py estimates      # Estimate accuracy (scans customers)

# QuickBooks OAuth setup (interactive, opens browser)
python3 scripts/qb_auth.py

# QuickBooks CSV export via Selenium (interactive, opens Chrome)
python3 scripts/qb_export.py --months 6
```

## Architecture

### Data Flow
SmartMoving API → `scripts/ingest_smartmoving.py` → DuckDB (`db/mme_dashboard.duckdb`) → Report scripts

### Key Files
- **`scripts/sm_api.py`** — Shared SmartMoving API client. All report scripts import from here. Handles auth headers, pagination (`pageResults`/`lastPage` pattern), and rate-limit retries (429 backoff).
- **`scripts/ingest_smartmoving.py`** — Full sync: pulls all entities from SmartMoving API and upserts into DuckDB via delete-then-insert. Supports `--table` for single-table sync.
- **`db/init_schema.py`** — DuckDB schema definition. All tables use `IF NOT EXISTS`.
- **`report.py`** — CLI entry point that dispatches to individual report scripts in `scripts/`.
- **`scripts/qb_auth.py`** — QuickBooks OAuth2 flow (local HTTP callback server on port 8080).
- **`scripts/qb_export.py`** — Selenium-based QBO report CSV exporter using Windows Chrome via WSL.

### Database
DuckDB at `db/mme_dashboard.duckdb`. Tables: `branches`, `users`, `move_sizes`, `referral_sources`, `customers`, `leads`, `opportunities`, `jobs`, `job_crew_members`, `daily_snapshots`, `sync_log`.

### SmartMoving API Conventions
- Base URL and credentials loaded from `.env` via manual parser (no `python-dotenv` dependency)
- Auth: `x-api-key` + `x-client-id` headers
- Pagination: `Page`/`PageSize` query params; response has `pageResults` array and `lastPage` boolean
- Dates are integers in `YYYYMMDD` format (e.g., `20260412`)
- Opportunity detail (with nested jobs) requires per-opportunity GET: `/opportunities/{id}`
- Jobs are not a standalone endpoint — they're nested inside opportunity detail

### Report Categories
- **Fast reports** (`leads`, `team`): Hit the API directly, no DuckDB dependency
- **Slow reports** (`daily`, `sales`, `estimates`): Scan customer opportunities via API. Support `--sample` (default 5000) and `--full-scan` flags. Require customers table populated in DuckDB first.

## Dependencies

Python 3.12 with `duckdb` and `selenium` (for QB export only). No web framework. All HTTP calls use stdlib `urllib` — no `requests` library.

## Environment

WSL2 environment. The QB export script uses Windows Chrome (`/mnt/c/Program Files/Google/Chrome/Application/chrome.exe`) and a Windows-side ChromeDriver at `bin/chromedriver.exe`. Path conversion between WSL and Windows uses `wslpath`.
