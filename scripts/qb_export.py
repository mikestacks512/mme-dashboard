"""
QuickBooks Online CSV Report Exporter via Selenium + Windows Chrome.

Opens Chrome, lets you log in to QBO, then downloads reports as CSV.

Usage:
    python3 scripts/qb_export.py                   # last 12 months
    python3 scripts/qb_export.py --months 6        # last 6 months
    python3 scripts/qb_export.py --from 2025-01-01 --to 2026-04-12
"""

import os
import sys
import glob
import time
import argparse
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPORT_DIR = os.path.join(PROJECT_ROOT, "exports")
os.makedirs(EXPORT_DIR, exist_ok=True)

# Convert WSL path to Windows path for Chrome download dir
EXPORT_DIR_WIN = EXPORT_DIR.replace("/mnt/c/", "C:\\\\").replace("/", "\\\\")
if EXPORT_DIR.startswith("/home/"):
    # WSL home directory — use a Windows-accessible temp path instead
    EXPORT_DIR_WIN = os.path.join(EXPORT_DIR)

# Load .env
env_path = os.path.join(PROJECT_ROOT, ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip("'\""))

REALM_ID = os.environ.get("QB_REALM_ID", "")

# QBO report URLs
# Date params: start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
REPORTS = [
    {
        "key": "profit_and_loss_detail",
        "name": "Profit and Loss Detail",
        "token": "ProfitAndLossDetail",
    },
    {
        "key": "expenses_by_vendor",
        "name": "Expenses by Vendor Detail",
        "token": "ExpensesByVendorDetail",
    },
    {
        "key": "general_ledger",
        "name": "General Ledger",
        "token": "GeneralLedger",
    },
    {
        "key": "balance_sheet",
        "name": "Balance Sheet Detail",
        "token": "BalanceSheetDetail",
    },
]


def get_chrome_driver(download_dir):
    """Set up Chrome via Windows Chrome + Windows ChromeDriver."""
    options = Options()
    options.binary_location = "/mnt/c/Program Files/Google/Chrome/Application/chrome.exe"

    # Convert WSL download path to Windows path
    win_download_dir = wsl_to_win_path(download_dir)

    prefs = {
        "download.default_directory": win_download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--window-size=1400,900")
    options.add_argument("--no-sandbox")

    # Use Windows chromedriver.exe
    chromedriver_path = os.path.join(PROJECT_ROOT, "bin", "chromedriver.exe")
    service = Service(executable_path=chromedriver_path)

    driver = webdriver.Chrome(service=service, options=options)
    return driver


def wsl_to_win_path(wsl_path):
    """Convert a WSL path to a Windows path."""
    import subprocess
    try:
        result = subprocess.run(["wslpath", "-w", wsl_path], capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    # Fallback
    if wsl_path.startswith("/mnt/"):
        drive = wsl_path[5].upper()
        rest = wsl_path[7:].replace("/", "\\")
        return f"{drive}:\\{rest}"
    return wsl_path


def wait_for_download(directory, prefix, timeout=60):
    """Wait for a new file to appear in the download directory."""
    start = time.time()
    existing = set(glob.glob(os.path.join(directory, "*")))
    while time.time() - start < timeout:
        current = set(glob.glob(os.path.join(directory, "*")))
        new_files = current - existing
        # Filter out partial downloads
        complete = [f for f in new_files if not f.endswith(".crdownload") and not f.endswith(".tmp")]
        if complete:
            return list(complete)[0]
        time.sleep(1)
    return None


def export_report(driver, report, date_from, date_to, download_dir):
    """Navigate to a QBO report and export as CSV."""
    name = report["name"]
    token = report["token"]
    print(f"\n  Loading: {name}...")

    url = f"https://qbo.intuit.com/app/reportv2?token={token}&start_date={date_from}&end_date={date_to}"
    driver.get(url)

    # Wait for report to load
    time.sleep(8)

    # Try to find and click export button
    export_selectors = [
        (By.CSS_SELECTOR, 'button[aria-label="Export"]'),
        (By.CSS_SELECTOR, '[data-automation="export-button"]'),
        (By.CSS_SELECTOR, '[data-testid="export-button"]'),
        (By.XPATH, '//button[contains(@aria-label, "xport")]'),
        (By.XPATH, '//button[contains(., "Export")]'),
        (By.CSS_SELECTOR, '.iconExport'),
        (By.CSS_SELECTOR, '#export-button'),
        (By.XPATH, '//ids-button[contains(@text, "Export")]'),
    ]

    export_btn = None
    for by, selector in export_selectors:
        try:
            export_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((by, selector))
            )
            break
        except Exception:
            continue

    if not export_btn:
        # Try finding any button/icon that looks like export
        try:
            buttons = driver.find_elements(By.TAG_NAME, "button")
            for btn in buttons:
                label = btn.get_attribute("aria-label") or btn.text or ""
                if "export" in label.lower() or "download" in label.lower():
                    export_btn = btn
                    break
        except Exception:
            pass

    if not export_btn:
        print(f"  Could not find export button for {name}.")
        print(f"  Page is open — please export manually.")
        return False

    export_btn.click()
    time.sleep(2)

    # Look for CSV option in dropdown
    csv_selectors = [
        (By.XPATH, '//button[contains(., "CSV")]'),
        (By.XPATH, '//a[contains(., "CSV")]'),
        (By.XPATH, '//li[contains(., "CSV")]'),
        (By.XPATH, '//*[contains(text(), "Export to CSV")]'),
        (By.XPATH, '//*[contains(text(), "as CSV")]'),
        (By.CSS_SELECTOR, '[data-automation="csv-export"]'),
    ]

    csv_btn = None
    for by, selector in csv_selectors:
        try:
            csv_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((by, selector))
            )
            break
        except Exception:
            continue

    if not csv_btn:
        # Maybe the export button directly downloads without a dropdown
        print(f"  No CSV dropdown found — export may have started directly.")
    else:
        csv_btn.click()

    # Wait for download
    print(f"  Waiting for download...")
    downloaded = wait_for_download(download_dir, report["key"], timeout=60)
    if downloaded:
        # Rename to our convention
        ext = os.path.splitext(downloaded)[1]
        dest = os.path.join(download_dir, f"{report['key']}_{date_from}_to_{date_to}{ext}")
        if downloaded != dest:
            os.rename(downloaded, dest)
        print(f"  Saved: {dest}")
        return True
    else:
        print(f"  Download not detected. Check your Downloads folder.")
        return False


def main():
    parser = argparse.ArgumentParser(description="Export QuickBooks reports via browser")
    parser.add_argument("--months", type=int, default=12, help="Months to export (default: 12)")
    parser.add_argument("--from", dest="date_from", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="date_to", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.date_to:
        date_to = args.date_to
    else:
        date_to = datetime.now().strftime("%Y-%m-%d")

    if args.date_from:
        date_from = args.date_from
    else:
        dt = datetime.now() - timedelta(days=args.months * 30)
        date_from = dt.strftime("%Y-%m-%d")

    print(f"{'='*60}")
    print(f"  QuickBooks Report Exporter")
    print(f"  Date range: {date_from} to {date_to}")
    print(f"  Reports: {', '.join(r['name'] for r in REPORTS)}")
    print(f"  Export to: {EXPORT_DIR}/")
    print(f"{'='*60}")
    print()
    print("  Chrome will open. Log into QuickBooks when prompted.")
    print("  After login, reports download automatically.")
    print()

    driver = get_chrome_driver(EXPORT_DIR)

    try:
        # Navigate to QBO — will redirect to login
        print("  Opening QuickBooks Online...")
        driver.get("https://qbo.intuit.com/app")

        # Wait for user to log in (detect QBO app loaded)
        print("\n  >>> Log into QuickBooks in the Chrome window <<<")
        print("  Waiting for login (up to 3 minutes)...\n")

        try:
            WebDriverWait(driver, 180).until(
                lambda d: "/app" in d.current_url and "login" not in d.current_url.lower()
            )
            time.sleep(5)
            print("  Logged in!")
        except Exception:
            print("  Login timeout. Make sure you're logged in, then the script will continue.")
            print("  Waiting another 2 minutes...")
            try:
                WebDriverWait(driver, 120).until(
                    lambda d: "/app" in d.current_url and "login" not in d.current_url.lower()
                )
            except Exception:
                print("  Could not detect login. Exiting.")
                driver.quit()
                return

        # Download each report
        success = 0
        for report in REPORTS:
            try:
                if export_report(driver, report, date_from, date_to, EXPORT_DIR):
                    success += 1
            except Exception as e:
                print(f"  Error on {report['name']}: {e}")

        print(f"\n{'='*60}")
        print(f"  Done! {success}/{len(REPORTS)} reports exported.")
        if success < len(REPORTS):
            print(f"  Browser stays open for manual exports.")
            print(f"  Close Chrome when finished.")
            try:
                input("  Press Enter to close...")
            except (EOFError, KeyboardInterrupt):
                time.sleep(300)
        print(f"{'='*60}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
