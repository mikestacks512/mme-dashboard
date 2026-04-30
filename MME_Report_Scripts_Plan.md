# Muscleman Elite — Operational Report Scripts

**Technical Plan for CTO / Technical Advisor**

Moving & Storage Operations | April 2026 | CONFIDENTIAL

---

## Table of Contents

1. [Objective](#1-objective)
2. [Why Scripts First](#2-why-scripts-first)
3. [Architecture](#3-architecture)
4. [Available Data (Confirmed)](#4-available-data-confirmed)
5. [Report Inventory](#5-report-inventory)
6. [Data Sync Strategy](#6-data-sync-strategy)
7. [API Rate Limits & Constraints](#7-api-rate-limits--constraints)
8. [Implementation Phases](#8-implementation-phases)
9. [Migration Path to Dashboard](#9-migration-path-to-dashboard)
10. [Current Status](#10-current-status)

---

## 1. Objective

Replace the 15+ day reporting lag with **on-demand Python report scripts** that pull live data from SmartMoving (and eventually QuickBooks, Dialpad, Google) and output formatted summaries to the terminal, CSV, or HTML.

Same goal as the Grafana dashboard spec — answer these questions instantly:

1. Are we making money today?
2. Are trucks and crews being utilized efficiently?
3. Are we booking the right jobs at the right prices?
4. Is our storage operation healthy?
5. Are marketing channels producing profitable customers?

But delivered in **days instead of months**, with zero infrastructure to maintain.

---

## 2. Why Scripts First

| Factor | Grafana Dashboard | Report Scripts |
|--------|-------------------|----------------|
| **Time to first value** | 2–3 weeks (Phase 1) | 1–2 days |
| **Infrastructure required** | PostgreSQL + Grafana + dbt + ingestion layer | Python + DuckDB (local file) |
| **Monthly cost** | $200–$600 | $0 |
| **Maintenance** | Server uptime, DB backups, Grafana updates | Run a script |
| **Shareability** | Browser link | Terminal output, CSV, email, Slack |
| **Customization** | Grafana panel config | Edit Python |
| **Path to dashboard** | Already built | Scripts become the data layer |

**Key insight:** The report scripts are not a dead end. The same data-pulling logic, calculations, and DuckDB cache become the foundation for a Grafana dashboard later. Nothing is thrown away.

---

## 3. Architecture

```
SmartMoving API ──┐
QuickBooks API  ──┤──> Python Scripts ──> DuckDB (local cache)
Dialpad API     ──┤         │                    │
Google APIs     ──┘         │                    │
                            ▼                    ▼
                     Terminal Output        Report Queries
                     CSV / HTML export     (instant, offline)
                     Email / Slack
```

| Component | Technology | Purpose |
|-----------|-----------|---------|
| API Client | `sm_api.py` (Python, stdlib only) | Shared SmartMoving API client with retry/rate-limit handling |
| Local Cache | DuckDB (`mme_dashboard.duckdb`) | Columnar DB for fast analytics on synced data, zero infrastructure |
| Report Runner | `report.py` | Single entry point: `python3 report.py <report_name>` |
| Report Scripts | `scripts/report_*.py` | One script per report, pulls from API or DuckDB |
| Data Sync | `scripts/ingest_smartmoving.py` | Full or incremental sync of SmartMoving data into DuckDB |

### File Structure

```
dashboard/
├── .env                              # API credentials
├── report.py                         # Main report runner
├── scripts/
│   ├── sm_api.py                     # Shared API client (retry, rate limiting)
│   ├── report_leads.py               # Lead Pipeline (live API)
│   ├── report_team.py                # Team & Branches (live API)
│   ├── report_daily.py               # Daily Summary (API + DuckDB)
│   ├── report_sales.py               # Sales Performance (API + DuckDB)
│   ├── report_estimates.py           # Estimate Accuracy (API + DuckDB)
│   ├── report_weekly.py              # Weekly Trends (DuckDB)
│   ├── report_financial.py           # Financial Control (QuickBooks — Phase 2)
│   ├── report_marketing.py           # Marketing Performance (Google — Phase 3)
│   ├── report_calls.py               # Call Center (Dialpad — Phase 3)
│   ├── report_storage.py             # Storage Operations (Phase 4)
│   ├── report_reviews.py             # Reputation (Google Business — Phase 4)
│   ├── report_claims.py              # Claims & Damage (Phase 4)
│   └── ingest_smartmoving.py         # Full data sync into DuckDB
├── db/
│   ├── init_schema.py                # DuckDB schema setup
│   └── mme_dashboard.duckdb          # Local database file
└── MME_Unified_Dashboard_Spec.md     # Original Grafana dashboard spec
```

---

## 4. Available Data (Confirmed)

### SmartMoving Open API (Connected & Tested)

**Base URL:** `https://smartmoving-prod-api-management.azure-api.net/v1/api`
**Auth:** `x-api-key` + `x-client-id` headers
**Rate Limit:** ~200–400 requests/minute (Azure API Management, exact limit TBD)

| Endpoint | Records | Key Fields |
|----------|---------|------------|
| `GET /customers` | 36,208 | id, name, phone, email, address |
| `GET /customers/{id}/opportunities` | Per customer | id, quoteNumber, serviceDate, status |
| `GET /opportunities/{id}` | Full detail | estimatedTotal, jobs[], payments[], salesAssignee, referralSource, moveSize, branch |
| `GET /leads` | 9 active | customerName, referralSource, serviceDate, moveSize, origin/destination, status |
| `GET /branches` | 6 | name, phone, location, isPrimary |
| `GET /users` | 8 | name, title, email, branch, role |
| `GET /move-sizes` | 54 | name, volume, weight |
| `GET /referral-sources` | 24 | name, isLeadProvider |

**Data Flow:** Lead → Customer → Opportunity → Job(s)

**Opportunity Detail Includes:**
- `estimatedTotal` (subtotal, tax, finalTotal)
- `status` (New=0, Estimated=1, FollowUp=2, Booked=3, Confirmed=5, Completed=10, Closed=11, Lost=20, Cancelled=30)
- `salesAssignee` (name, id)
- `referralSource` (string)
- `moveSize` (name, volume)
- `branch` (name)
- `jobs[]` — each with estimatedCharges, actualCharges, crewMembers, jobTime, arrivalWindow, tips
- `payments[]`
- `createdAtUtc`

**Known Limitations:**
- No date-range filtering on `/customers` endpoint — must paginate all 36K to find specific dates
- `estimatedCharges` and `actualCharges` at job level are null for many records (data depends on SmartMoving usage patterns)
- Customers are returned in alphabetical order, not chronological
- Rate limit ~200–400 req/min means full scans of all customers take significant time

### QuickBooks (Credentials Ready, Not Yet Connected)

- Client ID and secret configured in `.env`
- **Missing:** Refresh token (need OAuth flow)
- Will provide: contractor payouts, W-2 payroll, fuel, materials, merchant fees, overhead

### Not Yet Connected (Future Phases)

- **Dialpad:** Call center metrics (no API key yet)
- **Google Ads / LSA:** Marketing spend and attribution
- **Google Analytics:** Web traffic and conversion
- **Google Business Profile:** Reviews and ratings

---

## 5. Report Inventory

### Tier 1: Instant Reports (Live API, < 5 seconds)

These pull from small/fast API endpoints and return immediately.

#### `report.py leads` — Lead Pipeline

| Metric | Source |
|--------|--------|
| Active lead count | `/leads` |
| Leads by referral source | `/leads` |
| Leads by move size | `/leads` |
| Leads by branch | `/leads` |
| Service date urgency (today, this week) | `/leads` |
| Lead detail table (customer, source, size, date, days out) | `/leads` |

#### `report.py team` — Team & Operations Overview

| Metric | Source |
|--------|--------|
| Branch list with locations | `/branches` |
| Team members by role | `/users` |
| Referral source catalog | `/referral-sources` |
| Move size categories | `/move-sizes` |

---

### Tier 2: Scan Reports (API + DuckDB Cache, minutes)

These scan customer opportunities via the API. Run times depend on sample size — default samples 5,000 customers, `--full-scan` does all 36K.

#### `report.py daily` — Daily Summary (Command Center equivalent)

| Metric | Source | Spec Page |
|--------|--------|-----------|
| Active leads + source breakdown | `/leads` (live) | Page 1 |
| Opportunities by status | Customer scan | Page 1 |
| Estimated revenue (today / period) | Customer scan | Page 1 |
| Actual revenue (where available) | Customer scan | Page 1 |
| Jobs count (total, confirmed) | Customer scan | Page 1 |
| Average job size | Calculated | Page 1 |
| Revenue by branch | Customer scan | Page 1 |
| Revenue by sales rep | Customer scan | Page 1 |
| Revenue by referral source | Customer scan | Page 7 |

**Usage:**
```bash
python3 report.py daily              # today only, sample 5K customers
python3 report.py daily --days 7     # last 7 days
python3 report.py daily --days 30    # last 30 days
python3 report.py daily --full-scan  # all 36K customers (slow)
```

#### `report.py sales` — Sales Performance (Page 5 equivalent)

| Metric | Source | Spec Page |
|--------|--------|-----------|
| Opportunities per rep | Customer scan | Page 5 |
| Booked count per rep | Customer scan | Page 5 |
| Booking rate per rep | Calculated | Page 5 |
| Lost/cancelled per rep | Customer scan | Page 5 |
| Estimated revenue per rep | Customer scan | Page 5 |
| Booked revenue per rep | Customer scan | Page 5 |
| Average job size per rep | Calculated | Page 5 |

**Usage:**
```bash
python3 report.py sales              # last 30 days
python3 report.py sales --days 90    # last quarter
```

#### `report.py estimates` — Estimate Accuracy (Page 4 equivalent)

| Metric | Source | Spec Page |
|--------|--------|-----------|
| Estimated vs actual variance per job | Job detail | Page 4 |
| Over-estimate rate (>10% over) | Calculated | Page 4 |
| Under-estimate rate (>10% under) | Calculated | Page 4 |
| Variance by sales rep (absolute, not netted) | Calculated | Page 4 |
| Top over-estimates (left money on table) | Calculated | Page 4 |
| Top under-estimates (eating margin) | Calculated | Page 4 |
| Estimates by rep (count, total, average) | Customer scan | Page 4 |

**Note:** Estimate accuracy depends on `estimatedCharges` and `actualCharges` being populated at the job level in SmartMoving. If these are sparse, the report falls back to opportunity-level `estimatedTotal` analysis.

---

### Tier 3: DuckDB Reports (Offline, requires full sync)

These run entirely against the local DuckDB database after a full data sync. Sub-second response times.

#### `report.py weekly` — Weekly Trends (Page 12 equivalent)

| Metric | Source | Spec Page |
|--------|--------|-----------|
| Revenue this week vs last week | DuckDB opportunities | Page 12 |
| Jobs completed trend | DuckDB jobs | Page 12 |
| Average job size trend | Calculated | Page 12 |
| Booking lead time | DuckDB opportunities | Page 12 |
| Lead source trends | DuckDB opportunities | Page 12 |

**Requires:** Full opportunity sync into DuckDB (one-time, then incremental).

---

### Tier 4: Future Reports (Pending API Connections)

#### `report.py financial` — Financial Control (Page 3 equivalent)
- **Requires:** QuickBooks OAuth connection
- Labor % of revenue, fuel %, claims %, gross margin, EBITDA
- Contribution profit, fully loaded profit
- Contractor vs W-2 comparison

#### `report.py marketing` — Marketing Performance (Page 7 equivalent)
- **Requires:** Google Ads API + GA4
- Cost per lead by channel, CAC, marketing ROI

#### `report.py calls` — Call Center (Page 8 equivalent)
- **Requires:** Dialpad API
- Inbound calls, answered rate, missed calls, speed to answer

#### `report.py storage` — Storage Operations (Page 9 equivalent)
- **Requires:** Custom storage tracking (manual or separate DB)
- Occupancy rate, revenue per cubic foot, delinquency

#### `report.py reviews` — Reputation (Page 10 equivalent)
- **Requires:** Google Business Profile API
- Review count, average rating, capture rate

#### `report.py claims` — Claims & Damage (Page 11 equivalent)
- **Requires:** QuickBooks + custom tracking
- Claims filed, claims rate, claims by crew

---

## 6. Data Sync Strategy

### The Problem

The SmartMoving API does not support date-range filtering on the `/customers` endpoint. To find opportunities for a specific date range, we must:
1. Paginate through all 36,208 customers
2. For each customer, call `/customers/{id}/opportunities`
3. For matching opportunities, call `/opportunities/{id}` for full detail

This is ~72,000+ API calls for a full scan, which takes significant time due to rate limits.

### The Solution: DuckDB as Local Cache

**One-time full sync** (run overnight or during off-hours):
```bash
python3 scripts/ingest_smartmoving.py
```

This loads all customers, opportunities, and jobs into DuckDB. After that, reports query locally in milliseconds.

**Incremental sync** (daily, takes minutes):
- Small reference tables (branches, users, leads, move-sizes, referral-sources): full refresh each run
- Customers: sync only new customers (future enhancement — track last sync page)
- Opportunities: scan only recently modified customers (future enhancement)

### Sync Schedule Recommendation

| Data | Frequency | Method |
|------|-----------|--------|
| Leads | Every report run | Live API (9 records, instant) |
| Branches, Users, Move Sizes, Referral Sources | Daily | Full refresh (small tables) |
| Customers | Weekly | Full refresh or incremental |
| Opportunities + Jobs | Nightly | Full scan (run as cron job overnight) |

### Cron Setup (Optional)

```bash
# Add to crontab: crontab -e
# Nightly full sync at 2 AM
0 2 * * * cd /home/mike/projects/dashboard && python3 scripts/ingest_smartmoving.py >> logs/sync.log 2>&1
```

---

## 7. API Rate Limits & Constraints

### SmartMoving (Azure API Management)

- **Observed limit:** ~200–400 requests/minute (429 Too Many Requests after that)
- **Retry strategy:** Exponential backoff with 5s base, up to 60s wait, 5 retries
- **Mitigation:** 0.15s sleep between sequential requests in scan reports
- **Full sync time estimate:** 2–4 hours for all 36K customers + opportunities

### Recommendations

1. **Never run multiple scan reports simultaneously** — they compete for rate limit
2. **Use DuckDB cache** for any report that needs to scan >100 customers
3. **Run full syncs during off-hours** (nights/weekends)
4. **Tier 1 reports (leads, team) are always safe** — small, fast endpoints

---

## 8. Implementation Phases

### Phase 1: SmartMoving Reports (Current — 1–2 days)

**Status: In Progress**

- [x] SmartMoving API connection and endpoint discovery
- [x] DuckDB schema and customer sync (36K loaded)
- [x] Lead Pipeline report (`report.py leads`)
- [x] Team & Branches report (`report.py team`)
- [x] Daily Summary report (`report.py daily`)
- [x] Sales Performance report (`report.py sales`)
- [x] Estimate Accuracy report (`report.py estimates`)
- [ ] Run full opportunity sync into DuckDB
- [ ] Weekly Trends report (`report.py weekly`)
- [ ] CSV/HTML export option for all reports

**Cost:** $0 (just development time)

### Phase 2: QuickBooks Integration (1 week)

- [ ] Complete QuickBooks OAuth2 flow (get refresh token)
- [ ] Build QuickBooks API client (`qb_api.py`)
- [ ] Pull expense data: contractor payouts, payroll, fuel, materials, fees
- [ ] Financial Control report (`report.py financial`)
- [ ] Contribution profit calculation
- [ ] Fully loaded profit calculation
- [ ] Update Daily Summary with profit metrics

**Depends on:** QuickBooks refresh token

### Phase 3: Dialpad + Marketing (1–2 weeks)

- [ ] Dialpad API integration
- [ ] Call Center report (`report.py calls`)
- [ ] Google Ads API integration
- [ ] Marketing Performance report (`report.py marketing`)
- [ ] Update Daily Summary with call + marketing metrics

**Depends on:** Dialpad API key, Google Ads API access

### Phase 4: Storage, Reviews, Claims (1–2 weeks)

- [ ] Google Business Profile API integration
- [ ] Reputation report (`report.py reviews`)
- [ ] Storage tracking setup (manual entry or separate DB)
- [ ] Storage Operations report (`report.py storage`)
- [ ] Claims tracking setup
- [ ] Claims & Damage report (`report.py claims`)

### Cost Summary

| | Estimate |
|---|---|
| **Total Build Cost** | $0 – $5,000 (development time only) |
| **Monthly Operating Cost** | $0 |
| **Infrastructure** | None (runs on any machine with Python 3) |

---

## 9. Migration Path to Dashboard

When the report scripts are delivering value and the data model is stable, migrating to a Grafana dashboard is straightforward:

1. **DuckDB → PostgreSQL:** Export DuckDB tables to Postgres (DuckDB has built-in Postgres export)
2. **Ingestion scripts → Scheduled jobs:** Same Python scripts, run on a cron or via Make.com/n8n
3. **Report logic → Grafana SQL queries:** The SQL queries in the reports become Grafana panel queries
4. **DuckDB stays as local dev/backup:** Keep DuckDB for local testing and as a backup data source

The scripts-first approach **de-risks** the dashboard build by validating data availability, API constraints, and business logic before investing in infrastructure.

---

## 10. Current Status (April 12, 2026)

### Working

- SmartMoving API fully connected and authenticated
- 6 endpoints confirmed working (customers, leads, opportunities, branches, users, move-sizes, referral-sources)
- DuckDB schema created (11 tables)
- 36,208 customers loaded into DuckDB
- Lead Pipeline report operational
- Team & Branches report operational
- Daily Summary, Sales, and Estimate Accuracy reports built (pending rate limit cooldown for testing)

### Blocked

- **API rate limiting:** Background customer sync consumed rate limit quota. Reports with customer scanning need cooldown before testing. Retry logic with backoff is implemented.
- **Full opportunity sync:** Not yet run (requires extended API time window, ~2–4 hours)

### Next Steps

1. Wait for API rate limit to reset
2. Test all scan-based reports (daily, sales, estimates)
3. Run full opportunity sync overnight
4. Build Weekly Trends report against DuckDB
5. Add CSV/HTML export to all reports
6. Begin QuickBooks OAuth flow

---

*Companion document to MME_Unified_Dashboard_Spec.md — the dashboard spec remains the north star, this plan is the fast-path to getting value from the same data.*
