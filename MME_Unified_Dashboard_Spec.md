# Muscleman Elite — Unified Operational Intelligence Dashboard

**
Technical Specification for CTO / Technical Advisor**

Moving & Storage Operations | April 2026 | CONFIDENTIAL

---

## Table of Contents

1. [Objective](#1-objective)
2. [Gap Analysis: What Was Missing](#2-gap-analysis-what-was-missing)
3. [System Architecture](#3-system-architecture)
4. [Dashboard Pages](#4-dashboard-pages)
5. [Profitability Model](#5-profitability-model)
6. [Real-Time vs. Report Classification](#6-real-time-vs-report-classification)
7. [Grafana Alerting Rules](#7-grafana-alerting-rules)
8. [Implementation Phases](#8-implementation-phases)
9. [Success Criteria](#9-success-criteria)

---

## 1. Objective

Build a centralized, Grafana-based operational dashboard that integrates operational, financial, marketing, storage, and reputation data into a single automated system for real-time management of profitability and performance.

Current reporting arrives **15+ days after month end**, which prevents operational adjustments. This system will provide daily and near real-time insight into:

- Profitability (contribution and fully loaded)
- Operational efficiency (crews, trucks, dispatch)
- Marketing ROI and lead attribution
- Sales performance and estimate accuracy
- Storage operations and occupancy
- Reputation and review capture
- Claims and damage tracking

**The dashboard must answer these questions at a glance:**

1. Are we making money today?
2. Are trucks and crews being utilized efficiently?
3. Are we booking the right jobs at the right prices?
4. Is our storage operation healthy?
5. Are marketing channels producing profitable customers?

---

## 2. Gap Analysis: What Was Missing

This specification was developed by analyzing two prior dashboard proposals (Conservative and Expensive options) and performing deep industry research. The following critical gaps were identified in **both** prior proposals:

| Gap | Impact |
|-----|--------|
| **Storage Operations (entirely absent)** | Cannot track occupancy, delinquency, revenue per cubic foot, or storage business health |
| **Estimate Accuracy Tracking** | Biggest hidden profit lever; SmartMoving nets variance, hiding over/under-quoting by estimators |
| **Customer Lifecycle / Pipeline** | No visibility into cancellations, no-shows, reschedules, aging estimates, or funnel health |
| **Claims & Damage Tracking** | Both mention claims % target but neither actually tracks claims data |
| **Dispatch / Real-Time Operations** | No live view of crew status, job progress, or capacity utilization |
| **Call Center Lead Response Time** | Speed-to-lead is critical for booking rate; neither spec tracks response time distribution |
| **Capacity Planning** | Cannot answer: Can we take more work, or are we maxed out? |
| **Contractor vs. W-2 Comparison** | Hybrid crew model needs margin comparison between contractor and employee jobs |
| **Fleet Health / DOT Compliance** | Trucks treated as revenue generators only; no maintenance, mileage, or compliance tracking |
| **Automated Alerting** | Neither spec included any proactive alert system |

---

## 3. System Architecture

### 3.1 Architecture Overview

```
Data Sources
  SmartMoving API  ──┐
  QuickBooks API   ──┤
  Dialpad API      ──┼──> Ingestion Layer (Make.com or n8n)
  Google Ads API   ──┤              │
  LSA API          ──┤              ▼
  Google Analytics ──┤     PostgreSQL or BigQuery
  Google Business  ──┘     (staging + snapshots)
                                    │
                                    ▼
                          Transformation Layer
                          (dbt or scheduled SQL)
                                    │
                                    ▼
                               Grafana
                         (dashboards + alerts)
```

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Data Sources | SmartMoving API, QuickBooks API, Dialpad API, Google Ads API, LSA API, GA4, Google Business Profile | Raw operational, financial, marketing, and reputation data |
| Data Ingestion | Make.com or n8n | Scheduled API pulls, webhook listeners, data normalization |
| Data Warehouse | PostgreSQL (or BigQuery) | Centralized storage, joins, historical queries, snapshot tables |
| Transformation | dbt or scheduled SQL | All metric calculations, business logic, data quality checks |
| Visualization | Grafana | Dashboards, scorecards, alerting, TV display mode |
| Alerting | Grafana Alerting | Threshold-based alerts to Slack, email, or SMS |

**Why PostgreSQL over Google Sheets:** Supports joins, indexing, historical queries, and Grafana connects natively. BigQuery is an alternative but PostgreSQL is simpler and cheaper at this scale.

**Why Grafana over Looker Studio:** Real-time alerting, better data source flexibility, kiosk/TV display mode, and full customization. Enterprise-grade without enterprise cost.

### 3.2 Data Sources

#### SmartMoving (Primary Operational Source)
Contains: jobs, revenue, crews, trucks, crew hours, job type, city, sales rep, lead source, estimates, customer data, storage units.

#### QuickBooks (Primary Financial Source)
Contains: contractor payouts (actual, not flat 55%), W-2 payroll allocations, fuel expenses, materials, merchant fees, overhead categories.

#### Dialpad (Call Center)
Contains: inbound calls, answered rate, missed calls, abandoned calls, spam calls, average speed to answer.

#### Marketing Sources
Google Ads, Google Local Services Ads (LSA), Google Analytics (GA4). Metrics: leads, cost per lead, cost per booked job, revenue attribution, CAC.

#### Google Business Profile (Reputation)
Contains: reviews received, average rating, review text, review capture rate.

### 3.3 Refresh Rates

| Data Type | Refresh Interval | Rationale |
|-----------|-----------------|-----------|
| Financial / Revenue | Every 30 minutes | Balance between timeliness and API rate limits |
| Call Center (Dialpad) | Every 15 minutes | Missed calls need fast visibility |
| Marketing Spend | Every 2 hours | Ad platform data has natural lag |
| Reviews | Every 1 hour | Bad reviews need timely response, not second-by-second |
| Storage Occupancy | Every 1 hour | Occupancy changes are infrequent |

### 3.4 Data Safeguards

- **Rule 1 — Single Source of Truth:** Each metric comes from one system only. Never mix SmartMoving revenue with QuickBooks revenue.
- **Rule 2 — Centralized Calculations:** All calculations occur in dbt/SQL transformation layer, never inside Grafana.
- **Rule 3 — Historical Data Locking:** Daily snapshot table freezes once the day closes. Late data entry cannot change historical metrics.
- **Rule 4 — Anomaly Flagging:** Any metric that moves >30% from prior day gets flagged for manual review before being displayed.

**Daily snapshot table fields:** date, revenue, jobs, contribution_profit, fully_loaded_profit, trucks, leads, reviews, storage_occupancy, claims_count.

---

## 4. Dashboard Pages

The system includes **12 dashboard pages** organized into two tiers:

- **Real-Time Monitoring (Pages 1–3):** Displayed on office TV / kept open all day. Refreshed every 15–60 minutes.
- **Analytical Pages (Pages 4–12):** Reviewed daily or weekly. Used for deeper analysis and reporting.

---

### Page 1: Command Center (The Scoreboard)

> Maximum 14 metrics. Large-card scoreboard format. **This is the page that goes on the TV in the office.**

| Metric | Source | Why / Notes |
|--------|--------|-------------|
| Revenue Booked Today | SmartMoving | Know if today is a good day |
| Revenue Completed Today | SmartMoving | Cash that actually came in |
| Jobs In Progress Right Now | SmartMoving | Live operational pulse |
| Trucks Running / Total Available | SmartMoving | Utilization at a glance |
| Revenue Per Truck Today | Calculated | Are trucks earning their keep? |
| Contribution Profit Today | SM + QB | Are we making money? |
| Gross Margin % (vs. 45% target) | Calculated | Red/yellow/green indicator |
| Leads Today | SM + Dialpad | Pipeline health |
| Booking Rate Today | SmartMoving | Are we converting? |
| Missed Calls Today | Dialpad | Money walking out the door |
| Estimates Outstanding | SmartMoving | Pipeline of unconverted quotes |
| Storage Occupancy % | SM / Custom | Storage health at a glance |
| Reviews Today / Capture Rate | Google Business | Reputation pulse |
| Active Claims Count | Custom / QB | Liability exposure |

**Grafana Alerts (Page 1):**
- Gross margin drops below 40% → Slack/email alert
- Missed calls exceed 15% of inbound → alert
- Any crew idle for 2+ hours → alert
- Booking rate drops below 25% → alert

---

### Page 2: Dispatch & Operations

> Real-time crew and truck status. **Operations Manager view.**

| Metric | Source | Why / Notes |
|--------|--------|-------------|
| Crew Status Board | SmartMoving | Idle / en route / on site / loading / complete |
| Jobs Scheduled vs. Completed Today | SmartMoving | Are we on track for the day? |
| On-Time Arrival Rate | SmartMoving | % of jobs where crew arrives in promised window |
| Average Job Duration Today | SmartMoving | Spot delays early |
| Overtime Hours Triggered Today | SM / Payroll | Cost control |
| Available Capacity (crew-days this week) | Calculated | Can we take more work? |
| Booking Lead Time | SmartMoving | How far out are we booked? |
| Jobs Turned Away This Week | Manual / SM | Lost revenue from capacity constraints |
| Reschedules Today | SmartMoving | Operational disruption indicator |
| No-Shows Today | SmartMoving | Customer reliability tracking |

---

### Page 3: Financial Control

> Financial guardrails with targets and variance indicators. **CEO / Owner view.** Each metric shows: current %, target, variance, and a sparkline of the last 30 days.

| Metric | Target | Source |
|--------|--------|--------|
| Labor % of Revenue | ≤ 34% | QuickBooks + SmartMoving |
| Sales Payroll % | ≤ 7% | QuickBooks |
| Marketing % | ≤ 7% | Google Ads + QuickBooks |
| Fuel % | ≤ 5% | QuickBooks |
| Claims % | ≤ 1% | QuickBooks / Custom |
| Gross Margin | ≥ 45% | Calculated |
| EBITDA | ≥ 20% | Calculated |
| Contribution Profit (MTD) | vs. monthly target | Calculated |
| Fully Loaded Profit (MTD) | vs. monthly target | Calculated |
| Contractor Cost % vs. W-2 Cost % | Track trend | QuickBooks |
| Contractor Dependency Ratio | Track trend | SM + QuickBooks |

---

### Page 4: Estimate Accuracy & Pricing Intelligence

> **This is the page that will make you the most money.** Tracks absolute variance between estimated and actual job costs — not netted averages.

#### Estimate Accuracy Metrics

| Metric | Source | Why / Notes |
|--------|--------|-------------|
| Estimate vs. Actual Variance (per job) | SmartMoving | Absolute variance, not netted |
| Over-Estimate Rate (>10% over actual) | Calculated | Left money on the table or overcharged customer |
| Under-Estimate Rate (>10% under actual) | Calculated | Eating margin on these jobs |
| Variance by Sales Rep / Estimator | SmartMoving | Who is accurate, who is not |
| Variance by Job Type | SmartMoving | Local vs long distance vs commercial |
| Variance by Job Size Bucket | SmartMoving | Where does estimating break down? |

#### Pricing Intelligence

Jobs segmented by revenue size with profitability per bucket:

| Revenue Bucket | Metrics Tracked |
|----------------|----------------|
| Under $500 | Job count, revenue, contribution profit, margin % |
| $500 – $1,000 | Job count, revenue, contribution profit, margin % |
| $1,000 – $1,500 | Job count, revenue, contribution profit, margin % |
| $1,500 – $2,500 | Job count, revenue, contribution profit, margin % |
| $2,500 – $3,500 | Job count, revenue, contribution profit, margin % |
| $3,500+ | Job count, revenue, contribution profit, margin % |

Additional breakdowns: Margin by job type (local / long distance / commercial), profit by service type (packing, labor-only, full-service).

**Key visualization:** Scatter plot of estimated vs. actual per job with >10% variance highlighted red, filterable by rep.

---

### Page 5: Sales Performance

> Ensure sales reps book profitable work at accurate estimates.

| Metric | Source | Why / Notes |
|--------|--------|-------------|
| Revenue Booked Per Rep | SmartMoving | Volume indicator |
| Revenue Completed Per Rep | SmartMoving | Actual delivered revenue |
| Average Job Size Per Rep | SmartMoving | Are they booking big or small? |
| Profit Per Rep | Calculated | The metric that matters most |
| Booking Rate Per Rep | SmartMoving | Conversion effectiveness |
| Estimate Accuracy Per Rep | SmartMoving | Absolute variance (not netted) |
| Estimates Given This Week | SmartMoving | Activity level indicator |
| Follow-Up Compliance Rate | SmartMoving | Are they following the process? |
| Lead Response Time Per Rep | Dialpad + SM | Speed-to-lead by individual |
| Cancellation Rate Per Rep | SmartMoving | Are customers bailing after booking? |

---

### Page 6: Crew & Truck Productivity

> Identify operational inefficiencies and compare contractor vs. W-2 performance.

| Metric | Source | Why / Notes |
|--------|--------|-------------|
| Revenue Per Crew | SmartMoving | Crew-level revenue generation |
| Profit Per Crew | Calculated | After direct costs |
| Jobs Per Crew | SmartMoving | Throughput |
| Revenue Per Crew Hour | SmartMoving | Efficiency metric |
| Revenue Per Truck | SmartMoving | Asset utilization |
| Profit Per Truck | Calculated | Asset ROI |
| Jobs Per Truck | SmartMoving | Throughput per asset |
| Truck Utilization % | SmartMoving | Days in service / available days |
| Contractor vs. W-2 Margin Comparison | Calculated | Which model is more profitable? |
| Claims Per Crew | Custom | Identifies problem crews |
| Overtime Hours Per Crew | Payroll | Cost control by crew |

---

### Page 7: Marketing Performance

> Track marketing ROI by channel to identify what actually works.

| Metric | Source | Why / Notes |
|--------|--------|-------------|
| Leads by Channel | SM + Google | Volume by source |
| Cost Per Lead by Channel | Calculated | Efficiency of spend |
| Cost Per Booked Job by Channel | Calculated | True acquisition cost |
| CAC by Channel | Calculated | Marketing Spend ÷ New Customers |
| Revenue Attribution by Channel | SmartMoving | Which channels drive real revenue? |
| Marketing ROI by Channel | Calculated | Revenue returned per dollar spent |
| Repeat Customer Rate | SmartMoving | Free revenue (no marketing cost) |
| Referral Rate | SmartMoving | Word-of-mouth health |

**Channels tracked:** Google Ads, LSA, Organic, Referral, Repeat Customer.

---

### Page 8: Call Center & Lead Response

> Monitor call handling quality and speed-to-lead performance.

| Metric | Source | Why / Notes |
|--------|--------|-------------|
| Inbound Calls | Dialpad | Total volume |
| Answered Rate | Dialpad | % of calls picked up |
| Missed Calls | Dialpad | Money walking out the door |
| Abandoned Calls | Dialpad | Caller hung up waiting |
| Spam Calls (filtered) | Dialpad | Exclude from metrics |
| Average Speed to Answer | Dialpad | Customer experience indicator |
| After-Hours Missed Call Volume | Dialpad | Leads lost overnight/weekends |
| Lead Response Time Distribution | Dialpad + SM | % answered within 5/10/30 min |
| Call-to-Estimate Conversion | Dialpad + SM | Quality of phone interactions |

---

### Page 9: Storage Operations

> **This page is entirely new** — neither prior dashboard spec included any storage metrics. Critical for tracking the portable storage and future warehouse operations.

| Metric | Source | Why / Notes |
|--------|--------|-------------|
| Total Containers / Units | Custom DB | Inventory count |
| Physical Occupancy Rate | Calculated | Units rented / total units |
| Economic Occupancy Rate | Calculated | Actual revenue / potential at street rate |
| Revenue Per Cubic Foot | Calculated | Core pricing metric |
| Average Rate Per Container | Calculated | Pricing health |
| Net Move-In Rate | Custom DB | Move-ins minus move-outs per period |
| Average Length of Stay | Calculated | Customer retention indicator |
| Storage Revenue (MTD) | QB / Custom | Revenue from storage operations |
| Storage Revenue as % of Total | Calculated | Business mix tracking |
| Delinquency Rate | Custom DB | Uncollected income exposure |
| Delinquency Aging (30/60/90 days) | Custom DB | Escalation pipeline |
| Lien Pipeline | Custom DB | Units approaching auction threshold |
| Move-Out Reasons | Manual / Survey | Why are customers leaving? |

**Grafana Alerts (Storage):**
- Occupancy drops below 85% → alert
- Any account hits 60-day delinquent → alert
- Net move-in rate goes negative for 2+ weeks → alert

---

### Page 10: Reputation & Reviews

> Track review generation and customer satisfaction.

| Metric | Source | Why / Notes |
|--------|--------|-------------|
| Reviews Received (daily/weekly/monthly) | Google Business | Volume tracking |
| Review Capture Rate | Calculated | Reviews ÷ completed jobs (target: 40–50%) |
| Average Rating | Google Business | Overall satisfaction score |
| Rating Trend (trailing 30/60/90 days) | Calculated | Are we improving or declining? |
| Bad Review Count (< 5 stars) | Google Business | Requires immediate response |
| Bad Review Rate | Calculated | % of reviews below 5 stars |

---

### Page 11: Claims & Damage

> Track claims exposure and identify problem patterns. Neither prior spec tracked actual claims data despite both referencing a 1% target.

| Metric | Source | Why / Notes |
|--------|--------|-------------|
| Claims Filed This Period | Custom / QB | Volume of new claims |
| Claims Rate | Calculated | Claims ÷ completed jobs |
| Claims Cost as % of Revenue | QuickBooks | vs. 1% target |
| Claims by Crew | Custom | Identify problem crews |
| Average Claim Value | Custom / QB | Severity indicator |
| Open Claims Count | Custom | Current liability exposure |
| Open Claims Aging | Custom | Time to resolution tracking |
| Valuation Coverage Selection Rate | SmartMoving | % buying full-value protection |

---

### Page 12: Weekly/Monthly Trends

> Historical trend analysis for strategic decision-making. Reviewed weekly at minimum.

| Metric | Source | Why / Notes |
|--------|--------|-------------|
| Revenue: This Week vs. Last Week vs. YoY | SmartMoving | Trend direction |
| Jobs Completed Trend | SmartMoving | Volume trajectory |
| Average Job Size Trend | SmartMoving | Pricing trend |
| Profit Trend (Contribution + Fully Loaded) | Calculated | Bottom line trajectory |
| Labor % Trend | QuickBooks | Cost control over time |
| Marketing % Trend | QuickBooks | Spend discipline |
| Storage Occupancy Trend | Custom | Growth trajectory |
| Booking Lead Time Trend | SmartMoving | Demand indicator |
| Employee Turnover (monthly) | HR / Manual | Workforce stability |
| Overtime Hours Trend | Payroll | Workload management |

---

## 5. Profitability Model

Two profit views must be calculated. Both are essential for different audiences.

### 5.1 Contribution Profit (Operational)

Used for daily operations, pricing decisions, and crew/truck performance evaluation.

| Component | Source | Notes |
|-----------|--------|-------|
| Revenue | SmartMoving | Completed job revenue |
| − Contractor Payout | QuickBooks | **ACTUAL from QB, not flat 55%** |
| − Direct W-2 Labor Cost | QuickBooks | Allocated payroll for crew hours |
| − Packing Materials | QuickBooks | Per-job material costs |
| − Merchant Fees | QuickBooks | Payment processing costs |
| − Fuel | QuickBooks | Allocated per job or per truck-day |
| − Direct Job Costs | QuickBooks | Any other job-specific expenses |
| **= Contribution Profit** | **Calculated** | **What the job actually earned** |

### 5.2 Fully Loaded Profit (Executive)

Used for executive financial view, monthly reporting, and strategic decisions.

| Component | Source | Notes |
|-----------|--------|-------|
| Contribution Profit | Calculated | From above |
| − Marketing Allocation | Google Ads + QB | Proportional marketing spend |
| − Sales Payroll Allocation | QuickBooks | Sales team compensation |
| − Overhead Allocation | QuickBooks | Rent, insurance, admin, etc. |
| **= Fully Loaded Profit** | **Calculated** | **True bottom-line profitability** |

**Key principle:** Contractor payouts must come from QuickBooks actuals, not a flat percentage estimate. The typical 55% varies significantly by job and contractor.

---

## 6. Real-Time vs. Report Classification

Not everything needs to be live on a TV. Here is how metrics should be consumed:

| Real-Time (TV / Always Open) | Daily Check (Review) | Weekly/Monthly Report |
|------------------------------|---------------------|----------------------|
| Revenue booked/completed today | Estimate accuracy by rep | Revenue trends YoY |
| Jobs in progress | Job profitability by bucket | Fully loaded profit |
| Trucks running / utilization | Sales rep performance | Employee turnover |
| Missed calls | Marketing ROI by channel | Fleet maintenance schedule |
| Gross margin vs. target | Storage delinquency aging | Seasonal capacity planning |
| Leads & booking rate today | Claims by crew | DOT compliance audit |
| Storage occupancy | Crew productivity rankings | Training completion |
| Active claims count | Cancellation/no-show rates | Contractor vs W-2 analysis |
| Financial control gauges | Call center performance | Booking curve analysis |

---

## 7. Grafana Alerting Rules

Proactive alerts sent via Slack, email, or SMS when critical thresholds are breached:

| Alert Trigger | Threshold | Channel | Priority |
|---------------|-----------|---------|----------|
| Gross margin drops | Below 40% | Slack + Email | **Critical** |
| Missed calls exceed | 15% of inbound | Slack | High |
| Crew idle time | 2+ hours | Slack | Medium |
| Booking rate drops | Below 25% | Slack + Email | High |
| Storage occupancy drops | Below 85% | Email | Medium |
| Account hits 60-day delinquent | Any account | Email | High |
| Net move-in rate negative | 2+ consecutive weeks | Email | Medium |
| Daily metric anomaly | >30% change from prior day | Slack | Review |
| Labor % exceeds target | Above 38% | Email | High |
| Claims % exceeds target | Above 1.5% | Email + Slack | **Critical** |

---

## 8. Implementation Phases

### Phase 1: Infrastructure & Core Scoreboard

- PostgreSQL / BigQuery warehouse setup
- SmartMoving API integration
- QuickBooks API integration
- Base data schema and snapshot tables
- Command Center page (Page 1)
- Basic Grafana alerting

**Timeline:** 2–3 weeks
**Estimated Cost:** $4,000 – $8,000

### Phase 2: Profit Engine & Operations

- Contribution profit calculations
- Fully loaded profit calculations
- Dispatch & Operations page (Page 2)
- Financial Control page (Page 3)
- Estimate Accuracy & Pricing Intelligence page (Page 4)
- Crew & Truck Productivity page (Page 6)

**Timeline:** 3–4 weeks
**Estimated Cost:** $5,000 – $10,000

### Phase 3: Sales, Marketing & Call Center

- Sales Performance page (Page 5)
- Marketing Performance page (Page 7)
- Call Center page (Page 8)
- Dialpad API integration
- Google Ads / LSA / GA4 integrations

**Timeline:** 2–3 weeks
**Estimated Cost:** $3,000 – $7,000

### Phase 4: Storage, Reputation & Claims

- Storage Operations page (Page 9)
- Reputation & Reviews page (Page 10)
- Claims & Damage page (Page 11)
- Weekly/Monthly Trends page (Page 12)
- Google Business Profile integration
- Custom storage tracking database

**Timeline:** 2–3 weeks
**Estimated Cost:** $3,000 – $7,000

### Cost Summary

| | Low Estimate | High Estimate |
|---|---|---|
| **Total Build Cost** | $15,000 | $32,000 |
| **Monthly Operating Cost** | $200 | $600 |
| **Grafana Cloud (if used)** | $0 (self-hosted) | $299/mo (Cloud Pro) |

---

## 9. Success Criteria

The dashboard is functioning correctly when leadership can answer these questions **instantly**:

1. **Are we making money today?** — Revenue, profit, margin visible in real-time
2. **Are trucks and crews producing enough revenue?** — Utilization, revenue per truck/crew
3. **Are we booking profitable jobs?** — Job profitability by bucket, estimate accuracy
4. **Are marketing channels producing profitable customers?** — CAC by channel, ROI
5. **Is our storage operation healthy?** — Occupancy, delinquency, revenue per cubic foot
6. **Are our estimators accurate?** — Absolute variance per rep, not netted averages
7. **Can we take more work?** — Available capacity, booking lead time
8. **Are we managing risk?** — Claims rate, open claims, crew-level damage tracking

If those questions are answered clearly within 30 seconds of opening the dashboard, the system is delivering its intended value.

---

*END OF SPECIFICATION*
