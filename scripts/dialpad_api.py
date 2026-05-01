"""
Dialpad API client. Pulls call center data for the ops dashboard.

Uses the async stats export endpoint:
  1. POST /stats to initiate export
  2. Poll GET /stats/{request_id} until complete
  3. Download CSV from the returned URL
"""

import os
import csv
import io
import json
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timedelta
from collections import Counter, defaultdict

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load .env (if present)
env_path = os.path.join(PROJECT_ROOT, ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())

BASE_URL = "https://dialpad.com/api/v2"


def _get_api_key():
    return os.environ.get("DIALPAD_API_KEY", "")


def _get_office_id():
    return os.environ.get("DIALPAD_OFFICE_ID", "5518484519911424")


def _get_headers():
    return {
        "Authorization": f"Bearer {_get_api_key()}",
        "Content-Type": "application/json",
    }


def _api_request(method, endpoint, data=None, retries=3):
    url = f"{BASE_URL}{endpoint}"
    headers = _get_headers()
    body = json.dumps(data).encode() if data else None
    for attempt in range(retries):
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
                continue
            raise


def fetch_call_records(days=90):
    """Fetch call records from Dialpad via async stats export.

    Returns a list of dicts parsed from the CSV export.
    """
    api_key = _get_api_key()
    if not api_key:
        return []

    office_id = _get_office_id()

    # Step 1: Initiate export
    result = _api_request("POST", "/stats", {
        "stat_type": "calls",
        "days_ago_start": days,
        "days_ago_end": 0,
        "export_type": "records",
        "target_id": office_id,
        "target_type": "office",
    })

    request_id = result.get("request_id")
    if not request_id:
        return []

    # Step 2: Poll until complete (max 60 seconds)
    for _ in range(12):
        time.sleep(5)
        status = _api_request("GET", f"/stats/{request_id}")
        if status.get("status") == "complete" and status.get("download_url"):
            # Step 3: Download CSV
            req = urllib.request.Request(status["download_url"])
            with urllib.request.urlopen(req, timeout=60) as resp:
                content = resp.read().decode()
            reader = csv.DictReader(io.StringIO(content))
            return list(reader)
        elif status.get("status") == "error":
            return []

    return []


def get_call_center_report(days=90):
    """Build call center report from Dialpad data.

    Returns a dict with call stats, hourly distribution, daily trend, etc.
    """
    records = fetch_call_records(days)
    if not records:
        return {"error": "No Dialpad data available. Check API key."}

    # Parse records by Dialpad category
    inbound_all = [r for r in records if r.get("category") in ("incoming", "missed", "abandoned", "forwarded")]
    cancelled = [r for r in records if r.get("category") == "cancelled"]  # Short-ring, likely spam
    outbound = [r for r in records if r.get("category") == "outgoing"]

    answered = [r for r in records if r.get("category") == "incoming"]
    missed = [r for r in records if r.get("category") == "missed"]
    abandoned = [r for r in records if r.get("category") == "abandoned"]
    forwarded = [r for r in records if r.get("category") == "forwarded"]

    # Real inbound = answered + missed + abandoned (excludes cancelled/spam)
    inbound = answered + missed + abandoned
    total_inbound = len(inbound)
    answered_count = len(answered) + len(forwarded)
    missed_count = len(missed)
    abandoned_count = len(abandoned)
    cancelled_count = len(cancelled)
    answer_rate = round(answered_count / total_inbound * 100, 1) if total_inbound else 0

    # Average talk duration for answered calls
    durations = []
    for r in answered:
        d = r.get("talk_duration", "0")
        try:
            durations.append(float(d))
        except (ValueError, TypeError):
            pass
    avg_duration = round(sum(durations) / len(durations), 0) if durations else 0

    # Daily trend
    daily = defaultdict(lambda: {"inbound": 0, "answered": 0, "missed": 0, "abandoned": 0})
    for r in inbound:
        ds = r.get("date_started", "")[:10]
        if ds:
            cat = r.get("category", "")
            daily[ds]["inbound"] += 1
            if cat == "incoming":
                daily[ds]["answered"] += 1
            elif cat == "abandoned":
                daily[ds]["abandoned"] += 1
            else:
                daily[ds]["missed"] += 1

    daily_trend = []
    for day in sorted(daily.keys()):
        d = daily[day]
        rate = round(d["answered"] / d["inbound"] * 100, 1) if d["inbound"] else 0
        daily_trend.append({
            "date": day, "inbound": d["inbound"],
            "answered": d["answered"], "missed": d["missed"],
            "answer_rate": rate,
        })

    # Hourly distribution
    hourly = defaultdict(lambda: {"inbound": 0, "answered": 0, "missed": 0})
    for r in inbound:
        ds = r.get("date_started", "")
        if len(ds) >= 13:
            hour = ds[11:13]
            cat = r.get("category", "")
            hourly[hour]["inbound"] += 1
            if cat == "incoming":
                hourly[hour]["answered"] += 1
            else:
                hourly[hour]["missed"] += 1

    hourly_dist = []
    for hour in sorted(hourly.keys()):
        h = hourly[hour]
        rate = round(h["answered"] / h["inbound"] * 100, 1) if h["inbound"] else 0
        hourly_dist.append({
            "hour": f"{hour}:00", "inbound": h["inbound"],
            "answered": h["answered"], "missed": h["missed"],
            "answer_rate": rate,
        })

    # By call center / target
    by_target = defaultdict(lambda: {"inbound": 0, "answered": 0, "missed": 0})
    for r in inbound:
        target = r.get("name") or r.get("target_type") or "Unknown"
        cat = r.get("category", "")
        by_target[target]["inbound"] += 1
        if cat == "incoming":
            by_target[target]["answered"] += 1
        else:
            by_target[target]["missed"] += 1

    by_target_list = []
    for name, data in sorted(by_target.items(), key=lambda x: -x[1]["inbound"]):
        rate = round(data["answered"] / data["inbound"] * 100, 1) if data["inbound"] else 0
        by_target_list.append({
            "name": name, "inbound": data["inbound"],
            "answered": data["answered"], "missed": data["missed"],
            "answer_rate": rate,
        })

    # Speed to answer — ringing_duration for answered ("incoming") calls
    def _parse_duration(val):
        """Parse a duration string to float, returning None on failure."""
        if not val or not val.strip():
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    ring_times = []
    for r in answered:
        rt = _parse_duration(r.get("ringing_duration", ""))
        if rt is not None:
            ring_times.append(rt)

    avg_speed_to_answer = round(sum(ring_times) / len(ring_times), 1) if ring_times else 0

    # Speed distribution buckets
    buckets = [
        ("Under 10s", 0, 10),
        ("10-30s", 10, 30),
        ("30-60s", 30, 60),
        ("1-2 min", 60, 120),
        ("Over 2 min", 120, float("inf")),
    ]
    bucket_counts = {label: 0 for label, _, _ in buckets}
    for rt in ring_times:
        for label, lo, hi in buckets:
            if lo <= rt < hi:
                bucket_counts[label] += 1
                break

    total_ring = len(ring_times)
    speed_distribution = []
    for label, _, _ in buckets:
        pct = round(bucket_counts[label] / total_ring * 100, 1) if total_ring else 0
        speed_distribution.append({
            "bucket": label,
            "count": bucket_counts[label],
            "pct": pct,
        })

    # Average talk duration for answered calls
    talk_times = []
    for r in answered:
        tt = _parse_duration(r.get("talk_duration", ""))
        if tt is not None:
            talk_times.append(tt)
    avg_talk_duration = round(sum(talk_times) / len(talk_times), 1) if talk_times else 0

    # Speed to answer by hour of day
    hourly_speed = defaultdict(list)
    for r in answered:
        ds = r.get("date_started", "")
        if len(ds) >= 13:
            hour = ds[11:13]
            rt = _parse_duration(r.get("ringing_duration", ""))
            if rt is not None:
                hourly_speed[hour].append(rt)

    speed_by_hour = []
    for hour in sorted(hourly_speed.keys()):
        vals = hourly_speed[hour]
        speed_by_hour.append({
            "hour": f"{hour}:00",
            "avg_speed_to_answer": round(sum(vals) / len(vals), 1),
            "call_count": len(vals),
        })

    # After-hours calls (before 8am or after 6pm)
    after_hours_missed = 0
    for r in missed:
        ds = r.get("date_started", "")
        if len(ds) >= 13:
            hour = int(ds[11:13])
            if hour < 8 or hour >= 18:
                after_hours_missed += 1

    # Weekly summary (last 8 weeks)
    weekly = defaultdict(lambda: {"inbound": 0, "answered": 0, "missed": 0})
    for r in inbound:
        ds = r.get("date_started", "")[:10]
        if ds:
            try:
                dt = datetime.strptime(ds, "%Y-%m-%d")
                week_start = (dt - timedelta(days=dt.weekday())).strftime("%Y-%m-%d")
                cat = r.get("category", "")
                weekly[week_start]["inbound"] += 1
                if cat == "incoming":
                    weekly[week_start]["answered"] += 1
                else:
                    weekly[week_start]["missed"] += 1
            except ValueError:
                pass

    weekly_trend = []
    for week in sorted(weekly.keys())[-8:]:
        w = weekly[week]
        rate = round(w["answered"] / w["inbound"] * 100, 1) if w["inbound"] else 0
        weekly_trend.append({
            "week_of": week, "inbound": w["inbound"],
            "answered": w["answered"], "missed": w["missed"],
            "answer_rate": rate,
        })

    return {
        "generated": datetime.now().isoformat(),
        "period_days": days,
        "total_calls": len(records),
        "total_inbound": total_inbound,
        "total_outbound": len(outbound),
        "answered": answered_count,
        "missed": missed_count,
        "abandoned": abandoned_count,
        "cancelled_spam": cancelled_count,
        "answer_rate": answer_rate,
        "avg_duration_seconds": avg_duration,
        "avg_speed_to_answer": avg_speed_to_answer,
        "speed_distribution": speed_distribution,
        "avg_talk_duration": avg_talk_duration,
        "speed_by_hour": speed_by_hour,
        "after_hours_missed": after_hours_missed,
        "daily_trend": daily_trend[-30:],
        "weekly_trend": weekly_trend,
        "hourly_distribution": hourly_dist,
        "by_target": by_target_list,
    }


if __name__ == "__main__":
    import sys
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 90
    print(f"Fetching Dialpad call data (last {days} days)...")
    report = get_call_center_report(days)
    if "error" in report:
        print(f"Error: {report['error']}")
    else:
        print(f"Total calls: {report['total_calls']}")
        print(f"Inbound: {report['total_inbound']}")
        print(f"Answered: {report['answered']} ({report['answer_rate']}%)")
        print(f"Missed: {report['missed']}")
        print(f"After-hours missed: {report['after_hours_missed']}")
        print(f"Avg duration: {report['avg_duration_seconds']}s")
