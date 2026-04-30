# QuickBooks Report Exporter — runs in PowerShell on Windows
#
# Usage (from PowerShell):
#   cd \\wsl.localhost\Ubuntu\home\mike\projects\dashboard
#   .\scripts\qb_export.ps1
#
# Or from WSL:
#   powershell.exe -ExecutionPolicy Bypass -File scripts/qb_export.ps1

param(
    [int]$Months = 12,
    [string]$DateFrom = "",
    [string]$DateTo = ""
)

$ExportDir = Join-Path $PSScriptRoot "..\exports"
if (-not (Test-Path $ExportDir)) { New-Item -ItemType Directory -Path $ExportDir | Out-Null }
$ExportDir = (Resolve-Path $ExportDir).Path

# Date range
if ($DateTo -eq "") { $DateTo = (Get-Date).ToString("yyyy-MM-dd") }
if ($DateFrom -eq "") { $DateFrom = (Get-Date).AddDays(-($Months * 30)).ToString("yyyy-MM-dd") }

$Reports = @(
    @{ Key = "profit_and_loss_detail";  Name = "Profit and Loss Detail";     Token = "ProfitAndLossDetail" },
    @{ Key = "expenses_by_vendor";      Name = "Expenses by Vendor Detail";  Token = "ExpensesByVendorDetail" },
    @{ Key = "general_ledger";          Name = "General Ledger";             Token = "GeneralLedger" },
    @{ Key = "balance_sheet";           Name = "Balance Sheet Detail";       Token = "BalanceSheetDetail" }
)

Write-Host "============================================================"
Write-Host "  QuickBooks Report Exporter"
Write-Host "  Date range: $DateFrom to $DateTo"
Write-Host "  Export to:  $ExportDir"
Write-Host "============================================================"
Write-Host ""

$i = 0
foreach ($report in $Reports) {
    $i++
    $url = "https://qbo.intuit.com/app/reportv2?token=$($report.Token)&start_date=$DateFrom&end_date=$DateTo"
    Write-Host ""
    Write-Host "  [$i of $($Reports.Count)] $($report.Name)"
    Write-Host ""

    Read-Host "  Press Enter to open this report in your browser"

    Start-Process $url

    Write-Host ""
    Write-Host "  Report opened. Steps:"
    Write-Host "    1. Log in if prompted"
    Write-Host "    2. Wait for the report to load"
    Write-Host "    3. Click the Export icon (top right)"
    Write-Host "    4. Select 'Export to CSV'"
    Write-Host "    5. Save to: $ExportDir"
    Write-Host ""

    Read-Host "  Press Enter when done exporting this report"
}

Write-Host ""
Write-Host "============================================================"
Write-Host "  Done! Check $ExportDir for your CSV files."
Write-Host "============================================================"

# List exported files
Get-ChildItem $ExportDir -Filter "*.csv" | ForEach-Object {
    Write-Host "  - $($_.Name) ($([math]::Round($_.Length/1KB, 1)) KB)"
}
