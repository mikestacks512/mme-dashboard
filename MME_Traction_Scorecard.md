# MME Traction Meeting Scorecard

KPI reference for weekly/monthly traction meetings. Each metric includes the formula, data source, and target where applicable.

---

## 1. Revenue & Profitability

| KPI | Formula | Target |
|-----|---------|--------|
| **Gross Revenue** | Sum of all invoiced/actual job charges | -- |
| **Gross Profit** | Revenue - COGS | -- |
| **Gross Margin %** | (Gross Profit / Revenue) x 100 | >= 45% |
| **Net Margin %** | (Revenue - COGS - Overhead) / Revenue x 100 | >= 20% |
| **Net Income** | Revenue + Other Income - COGS - Overhead - Other Expense | -- |

**COGS includes:** Direct labor (W-2 payroll + payroll taxes + workers comp + contracted labor), fuel, packing materials, truck lease/rent, merchant fees, claims/damages.

**Overhead includes:** Admin payroll, sales payroll, marketing, insurance, rent, utilities, all other operating expenses not in COGS.

---

## 2. Cost Control (as % of Revenue)

Each line item is calculated the same way:

> **Formula:** (Line Item Spend / Gross Revenue) x 100

| KPI | What Goes In the Numerator | Target |
|-----|---------------------------|--------|
| **Direct Labor %** | W-2 payroll + payroll taxes + workers comp + contracted labor | <= 34% |
| **Sales Payroll %** | Sales team compensation | <= 7% |
| **Marketing %** | All advertising & marketing spend | <= 7% |
| **Fuel %** | Fuel costs | <= 5% |
| **Claims/Damages %** | Customer claims and damage payouts | <= 1% |
| **Contracted Labor %** | Subcontracted/temp labor only | Monitor |
| **Merchant Fees %** | Credit card processing fees | Monitor |
| **Packing Materials %** | Boxes, tape, wrap, etc. | Monitor |

### Labor Dependency Ratio

> **Formula:** Contracted Labor / (W-2 Labor + Contracted Labor) x 100

Shows reliance on subcontracted vs. in-house crews. Higher = more risk and less margin control.

---

## 3. Sales & Booking

| KPI | Formula | Target |
|-----|---------|--------|
| **Booking Rate** | (Booked Opps / Total Opps) x 100 | >= 25% |
| **Lost Rate** | (Lost Opps / Total Opps) x 100 | Monitor |
| **Cancellation Rate** | (Cancelled Opps / Total Opps) x 100 | Monitor |
| **Average Job Size** | Booked Revenue / Booked Opp Count | Monitor |
| **Pending Estimates** | Count of opps in Estimated or Follow Up status | Monitor |

**SmartMoving status codes:**
- 0 = New, 1 = Estimated, 2 = Follow Up, 3 = Booked, 5 = Confirmed
- 10 = Completed, 11 = Closed, 20 = Lost, 30 = Cancelled

**"Booked" for formula purposes** = status in {3, 5, 10, 11} (Booked + Confirmed + Completed + Closed).

---

## 4. Sales Rep Performance

All metrics are per rep, filtered by the rep assigned to the opportunity.

| KPI | Formula |
|-----|---------|
| **Rep Booking Rate** | (Rep's Booked Opps / Rep's Total Opps) x 100 |
| **Rep Booked Revenue** | Sum of estimated totals for rep's booked opps |
| **Rep Avg Job Size** | Rep's Booked Revenue / Rep's Booked Count |
| **Rep Lost Count** | Count of rep's opps with status = 20 |
| **Rep Cancelled Count** | Count of rep's opps with status = 30 |

### Cost per Sales Rep

> **Formula:** Total Sales Payroll / Number of Active Sales Reps

Tells you the average fully-loaded cost of each rep. Compare against Rep Booked Revenue to gauge ROI per head.

### Revenue per Sales Rep

> **Formula:** Total Booked Revenue / Number of Active Sales Reps

---

## 5. Estimate Accuracy

| KPI | Formula |
|-----|---------|
| **Variance ($)** | Actual Job Total - Estimated Job Total |
| **Variance %** | (Variance / Estimated Total) x 100 |
| **Net Variance** | Sum of all individual variances across jobs |
| **Net Variance %** | (Net Variance / Total Estimated) x 100 |
| **Over-Estimate Count** | Jobs where variance % > +10% |
| **Under-Estimate Count** | Jobs where variance % < -10% |
| **Rep Avg Variance %** | Sum of |variance %| / Rep's Job Count |

A positive variance means the customer paid more than estimated (under-estimated the job). A negative variance means the customer paid less (over-estimated).

---

## 6. Lead Pipeline

| KPI | Formula |
|-----|---------|
| **Total Leads** | Count of all leads in period |
| **Leads by Source** | Count of leads grouped by referral source |
| **Leads by Branch** | Count of leads grouped by branch |
| **Urgent Leads** | Leads where service date is within 7 days |
| **Conversion Rate** | (Booked Leads / Total Leads) x 100 |

---

## 7. Marketing & Customer Acquisition

| KPI | Formula | Target |
|-----|---------|--------|
| **Cost per Lead** | Total Marketing Spend / Total Leads | Monitor |
| **Customer Acquisition Cost (CAC)** | Total Marketing Spend / Booked Customers | < $500 |
| **Cost per Booked Job** | Total Marketing Spend / Total Booked Jobs | Monitor |
| **Marketing ROI** | Total Revenue / Marketing Spend | Monitor |
| **Channel Booking Rate** | (Channel Booked / Channel Total Leads) x 100 | Per channel |
| **Channel Avg Job Size** | Channel Booked Revenue / Channel Booked Count | Per channel |

**CAC thresholds:** OK = under $500, WARN = $500-$750, HIGH = over $750.

---

## 8. Storage

| KPI | Formula |
|-----|---------|
| **Active Accounts** | Count where status = Active |
| **Monthly Recurring Revenue** | Sum of recurring storage charges for active accounts |
| **Annualized Storage Revenue** | Monthly Recurring Revenue x 12 |
| **Revenue per Cu Ft** | (Monthly Revenue / Total Active Volume) x 12 |
| **Total Delinquent** | Sum of all aging bucket balances |
| **Delinquency Rate** | (Total Delinquent / Monthly Recurring Revenue) x 100 |
| **Card on File %** | (Accounts with Card / Active Accounts) x 100 |
| **Auto-Collect %** | (Accounts with Auto-Collect / Active Accounts) x 100 |

### Delinquency Aging Buckets

Break down total delinquent into: **0-30 days**, **31-60 days**, **61-90 days**, **90+ days**.

---

## 9. Trends (Monthly)

Track these month-over-month to spot patterns:

| KPI | Formula |
|-----|---------|
| **MoM Revenue Change %** | (This Month Revenue - Last Month Revenue) / Last Month Revenue x 100 |
| **Monthly Booking Rate** | (Monthly Booked / Monthly Total Opps) x 100 |
| **Monthly Avg Job Size** | Monthly Booked Revenue / Monthly Booked Count |
| **Monthly Gross Margin** | (Monthly Revenue - Monthly COGS) / Monthly Revenue x 100 |

---

## 10. Scorecard Status Indicators

For any metric with a target, use this system:

| Indicator | Meaning | Rule |
|-----------|---------|------|
| **OK** | On track | Meets or exceeds target |
| **WARN** | Watch it | Within 10% of missing target |
| **MISS** | Off track | Below target threshold |

**Example:** Direct Labor target is <= 34%.
- 33% = OK
- 35% = WARN (within 10% over)
- 38% = MISS

---

## Data Sources

| Source | What It Provides |
|--------|-----------------|
| **SmartMoving API** | Leads, opportunities, jobs, customers, branches, users, referral sources, move sizes |
| **QuickBooks P&L** | Revenue, COGS line items, overhead, all financial data |
| **QuickBooks Balance Sheet** | Assets, liabilities, equity |
| **DuckDB** | Cached SmartMoving data for faster reporting |

---

## How to Pull the Numbers

1. **Financial metrics (sections 1-2):** Run QuickBooks P&L Detail report for the period, or use `python3 report.py daily` for a quick summary
2. **Sales & booking (sections 3-4):** `python3 report.py sales` or `python3 report.py daily`
3. **Estimate accuracy (section 5):** `python3 report.py estimates`
4. **Lead pipeline (section 6):** `python3 report.py leads`
5. **Marketing (section 7):** Combine lead report data with marketing spend from QuickBooks
6. **Storage (section 8):** `python3 report.py storage` (requires storage CSV export)
7. **Trends (section 9):** `python3 report.py trends` or review monthly snapshots
