"""
Manual data inputs — fleet, storage, and turnaway tracking.

These are the metrics from MME_Unified_Dashboard_Spec.md that have no API
source: truck roster + daily utilization, storage units + occupancy
snapshots, and jobs-turned-away. Persisted to DuckDB.
"""

import os
import uuid
from datetime import date, datetime

import duckdb

from web.reports import DB_PATH


def _con(read_only=False):
    return duckdb.connect(DB_PATH, read_only=read_only)


def _row_to_dict(cursor, row):
    cols = [d[0] for d in cursor.description]
    return dict(zip(cols, row))


def _rows(cursor):
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, r)) for r in cursor.fetchall()]


# ───────────────────────────── Trucks ─────────────────────────────

def list_trucks():
    con = _con(read_only=True)
    try:
        cur = con.execute(
            "SELECT id, name, truck_type, capacity_cuft, active, notes "
            "FROM trucks ORDER BY active DESC, name"
        )
        return _rows(cur)
    finally:
        con.close()


def upsert_truck(payload):
    tid = (payload.get("id") or "").strip() or str(uuid.uuid4())
    name = (payload.get("name") or "").strip()
    if not name:
        raise ValueError("Truck name is required")
    truck_type = (payload.get("truck_type") or "").strip() or None
    capacity = payload.get("capacity_cuft")
    capacity = int(capacity) if capacity not in (None, "") else None
    active = bool(payload.get("active", True))
    notes = (payload.get("notes") or "").strip() or None

    con = _con()
    try:
        con.execute(
            """
            INSERT INTO trucks (id, name, truck_type, capacity_cuft, active, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                name=excluded.name,
                truck_type=excluded.truck_type,
                capacity_cuft=excluded.capacity_cuft,
                active=excluded.active,
                notes=excluded.notes
            """,
            [tid, name, truck_type, capacity, active, notes],
        )
        return {"id": tid, "name": name, "truck_type": truck_type,
                "capacity_cuft": capacity, "active": active, "notes": notes}
    finally:
        con.close()


def delete_truck(truck_id):
    con = _con()
    try:
        con.execute("DELETE FROM trucks WHERE id = ?", [truck_id])
        con.execute("DELETE FROM truck_utilization_daily WHERE truck_id = ?", [truck_id])
        return {"ok": True, "id": truck_id}
    finally:
        con.close()


# ──────────────────── Daily truck utilization ────────────────────

def list_truck_utilization(start=None, end=None):
    con = _con(read_only=True)
    try:
        sql = (
            "SELECT u.entry_date, u.truck_id, t.name AS truck_name, "
            "u.in_service, u.jobs_count, u.hours_used, u.notes "
            "FROM truck_utilization_daily u "
            "LEFT JOIN trucks t ON t.id = u.truck_id "
        )
        params = []
        clauses = []
        if start:
            clauses.append("u.entry_date >= ?")
            params.append(start)
        if end:
            clauses.append("u.entry_date <= ?")
            params.append(end)
        if clauses:
            sql += "WHERE " + " AND ".join(clauses) + " "
        sql += "ORDER BY u.entry_date DESC, t.name"
        cur = con.execute(sql, params)
        rows = _rows(cur)
        for r in rows:
            if isinstance(r.get("entry_date"), (date, datetime)):
                r["entry_date"] = r["entry_date"].isoformat()
        return rows
    finally:
        con.close()


def upsert_truck_utilization(payload):
    entry_date = payload.get("entry_date")
    truck_id = (payload.get("truck_id") or "").strip()
    if not entry_date or not truck_id:
        raise ValueError("entry_date and truck_id are required")
    in_service = bool(payload.get("in_service", True))
    jobs_count = int(payload.get("jobs_count") or 0)
    hours_used = float(payload.get("hours_used") or 0)
    notes = (payload.get("notes") or "").strip() or None

    con = _con()
    try:
        con.execute(
            """
            INSERT INTO truck_utilization_daily
                (entry_date, truck_id, in_service, jobs_count, hours_used, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (entry_date, truck_id) DO UPDATE SET
                in_service=excluded.in_service,
                jobs_count=excluded.jobs_count,
                hours_used=excluded.hours_used,
                notes=excluded.notes
            """,
            [entry_date, truck_id, in_service, jobs_count, hours_used, notes],
        )
        return {"entry_date": entry_date, "truck_id": truck_id,
                "in_service": in_service, "jobs_count": jobs_count,
                "hours_used": hours_used, "notes": notes}
    finally:
        con.close()


def delete_truck_utilization(entry_date, truck_id):
    con = _con()
    try:
        con.execute(
            "DELETE FROM truck_utilization_daily WHERE entry_date = ? AND truck_id = ?",
            [entry_date, truck_id],
        )
        return {"ok": True}
    finally:
        con.close()


# ────────────────────────── Storage units ─────────────────────────

def list_storage_units():
    con = _con(read_only=True)
    try:
        cur = con.execute(
            "SELECT id, label, size_class, cubic_feet, monthly_rate, active, notes "
            "FROM storage_units ORDER BY active DESC, label"
        )
        return _rows(cur)
    finally:
        con.close()


def upsert_storage_unit(payload):
    uid = (payload.get("id") or "").strip() or str(uuid.uuid4())
    label = (payload.get("label") or "").strip()
    if not label:
        raise ValueError("Storage unit label is required")
    size_class = (payload.get("size_class") or "").strip() or None
    cubic_feet = payload.get("cubic_feet")
    cubic_feet = float(cubic_feet) if cubic_feet not in (None, "") else None
    monthly_rate = payload.get("monthly_rate")
    monthly_rate = float(monthly_rate) if monthly_rate not in (None, "") else None
    active = bool(payload.get("active", True))
    notes = (payload.get("notes") or "").strip() or None

    con = _con()
    try:
        con.execute(
            """
            INSERT INTO storage_units (id, label, size_class, cubic_feet, monthly_rate, active, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                label=excluded.label,
                size_class=excluded.size_class,
                cubic_feet=excluded.cubic_feet,
                monthly_rate=excluded.monthly_rate,
                active=excluded.active,
                notes=excluded.notes
            """,
            [uid, label, size_class, cubic_feet, monthly_rate, active, notes],
        )
        return {"id": uid, "label": label, "size_class": size_class,
                "cubic_feet": cubic_feet, "monthly_rate": monthly_rate,
                "active": active, "notes": notes}
    finally:
        con.close()


def delete_storage_unit(unit_id):
    con = _con()
    try:
        con.execute("DELETE FROM storage_units WHERE id = ?", [unit_id])
        return {"ok": True, "id": unit_id}
    finally:
        con.close()


# ─────────────────────── Storage snapshots ───────────────────────

def list_storage_snapshots(limit=90):
    con = _con(read_only=True)
    try:
        cur = con.execute(
            "SELECT snapshot_date, units_total, units_occupied, mrr, "
            "delinquent_30, delinquent_60, delinquent_90, "
            "move_ins, move_outs, notes "
            "FROM storage_snapshots ORDER BY snapshot_date DESC LIMIT ?",
            [int(limit)],
        )
        rows = _rows(cur)
        for r in rows:
            if isinstance(r.get("snapshot_date"), (date, datetime)):
                r["snapshot_date"] = r["snapshot_date"].isoformat()
            total = r.get("units_total") or 0
            occ = r.get("units_occupied") or 0
            r["occupancy_pct"] = round(occ / total * 100, 1) if total else None
        return rows
    finally:
        con.close()


def upsert_storage_snapshot(payload):
    snap_date = payload.get("snapshot_date")
    if not snap_date:
        raise ValueError("snapshot_date is required")

    def _i(key):
        v = payload.get(key)
        return int(v) if v not in (None, "") else None

    def _f(key):
        v = payload.get(key)
        return float(v) if v not in (None, "") else None

    units_total = _i("units_total")
    units_occupied = _i("units_occupied")
    mrr = _f("mrr")
    delinquent_30 = _i("delinquent_30") or 0
    delinquent_60 = _i("delinquent_60") or 0
    delinquent_90 = _i("delinquent_90") or 0
    move_ins = _i("move_ins") or 0
    move_outs = _i("move_outs") or 0
    notes = (payload.get("notes") or "").strip() or None

    con = _con()
    try:
        con.execute(
            """
            INSERT INTO storage_snapshots
                (snapshot_date, units_total, units_occupied, mrr,
                 delinquent_30, delinquent_60, delinquent_90, move_ins, move_outs, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (snapshot_date) DO UPDATE SET
                units_total=excluded.units_total,
                units_occupied=excluded.units_occupied,
                mrr=excluded.mrr,
                delinquent_30=excluded.delinquent_30,
                delinquent_60=excluded.delinquent_60,
                delinquent_90=excluded.delinquent_90,
                move_ins=excluded.move_ins,
                move_outs=excluded.move_outs,
                notes=excluded.notes
            """,
            [snap_date, units_total, units_occupied, mrr,
             delinquent_30, delinquent_60, delinquent_90, move_ins, move_outs, notes],
        )
        return {"snapshot_date": snap_date, "units_total": units_total,
                "units_occupied": units_occupied, "mrr": mrr,
                "delinquent_30": delinquent_30, "delinquent_60": delinquent_60,
                "delinquent_90": delinquent_90, "move_ins": move_ins,
                "move_outs": move_outs, "notes": notes}
    finally:
        con.close()


def delete_storage_snapshot(snapshot_date):
    con = _con()
    try:
        con.execute("DELETE FROM storage_snapshots WHERE snapshot_date = ?", [snapshot_date])
        return {"ok": True}
    finally:
        con.close()


# ───────────────────────── Jobs turned away ───────────────────────

def list_turnaways(limit=200):
    con = _con(read_only=True)
    try:
        cur = con.execute(
            "SELECT entry_date, id, count, est_revenue_lost, reason, notes "
            "FROM jobs_turned_away ORDER BY entry_date DESC LIMIT ?",
            [int(limit)],
        )
        rows = _rows(cur)
        for r in rows:
            if isinstance(r.get("entry_date"), (date, datetime)):
                r["entry_date"] = r["entry_date"].isoformat()
        return rows
    finally:
        con.close()


def upsert_turnaway(payload):
    entry_date = payload.get("entry_date")
    if not entry_date:
        raise ValueError("entry_date is required")
    tid = (payload.get("id") or "").strip() or str(uuid.uuid4())
    count = int(payload.get("count") or 1)
    est_revenue_lost = float(payload.get("est_revenue_lost") or 0)
    reason = (payload.get("reason") or "").strip() or None
    notes = (payload.get("notes") or "").strip() or None

    con = _con()
    try:
        con.execute(
            """
            INSERT INTO jobs_turned_away (entry_date, id, count, est_revenue_lost, reason, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (entry_date, id) DO UPDATE SET
                count=excluded.count,
                est_revenue_lost=excluded.est_revenue_lost,
                reason=excluded.reason,
                notes=excluded.notes
            """,
            [entry_date, tid, count, est_revenue_lost, reason, notes],
        )
        return {"entry_date": entry_date, "id": tid, "count": count,
                "est_revenue_lost": est_revenue_lost, "reason": reason, "notes": notes}
    finally:
        con.close()


def delete_turnaway(entry_date, tid):
    con = _con()
    try:
        con.execute(
            "DELETE FROM jobs_turned_away WHERE entry_date = ? AND id = ?",
            [entry_date, tid],
        )
        return {"ok": True}
    finally:
        con.close()


# ─────────────────────── Fleet productivity ──────────────────────

def get_truck_report(days=30):
    """Crew & Truck Productivity — Page 6 of MME_Unified_Dashboard_Spec.md.

    Aggregates the truck roster against the daily utilization log over a
    rolling window (default 30 days). Fleet utilization % is computed as
    (days each truck was logged as in-service) / (active-truck × window),
    so a 5-truck fleet with all in service every day = 100%.
    """
    days = max(1, int(days))
    con = _con(read_only=True)
    try:
        active_trucks = con.execute(
            "SELECT COUNT(*) FROM trucks WHERE active"
        ).fetchone()[0] or 0
        total_trucks = con.execute("SELECT COUNT(*) FROM trucks").fetchone()[0] or 0

        # Per-truck stats over the window
        per_truck = con.execute(
            f"""
            SELECT
                t.id, t.name, t.truck_type, t.capacity_cuft, t.active,
                COUNT(u.entry_date) AS days_logged,
                COUNT(u.entry_date) FILTER (WHERE u.in_service) AS days_in_service,
                COALESCE(SUM(u.jobs_count), 0) AS jobs_total,
                COALESCE(SUM(u.hours_used), 0) AS hours_total
            FROM trucks t
            LEFT JOIN truck_utilization_daily u
                ON u.truck_id = t.id
                AND u.entry_date >= CURRENT_DATE - INTERVAL {days} DAY
            GROUP BY t.id, t.name, t.truck_type, t.capacity_cuft, t.active
            ORDER BY t.active DESC, hours_total DESC, t.name
            """
        ).fetchall()

        truck_rows = []
        fleet_jobs = 0
        fleet_hours = 0.0
        fleet_in_service_days = 0
        for tid, name, ttype, cap, active, dl, dis, jobs, hours in per_truck:
            dl = dl or 0
            dis = dis or 0
            jobs = int(jobs or 0)
            hours = float(hours or 0)
            util_pct = round(dis / days * 100, 1) if days else None
            # Avg per logged day (not per window day) — what crews are doing on the days they ran
            avg_jobs_per_day = round(jobs / dis, 2) if dis else None
            avg_hours_per_day = round(hours / dis, 2) if dis else None
            truck_rows.append({
                "id": tid,
                "name": name,
                "truck_type": ttype,
                "capacity_cuft": cap,
                "active": bool(active),
                "days_logged": dl,
                "days_in_service": dis,
                "util_pct": util_pct,
                "jobs_total": jobs,
                "hours_total": round(hours, 2),
                "avg_jobs_per_day": avg_jobs_per_day,
                "avg_hours_per_day": avg_hours_per_day,
            })
            if active:
                fleet_jobs += jobs
                fleet_hours += hours
                fleet_in_service_days += dis

        denominator = active_trucks * days
        fleet_util_pct = (
            round(fleet_in_service_days / denominator * 100, 1) if denominator else None
        )

        # Daily fleet-in-service trend
        trend_rows = con.execute(
            f"""
            SELECT entry_date,
                   COUNT(*) FILTER (WHERE in_service) AS in_service_count,
                   COUNT(*) AS logged_count
            FROM truck_utilization_daily
            WHERE entry_date >= CURRENT_DATE - INTERVAL {days} DAY
            GROUP BY entry_date
            ORDER BY entry_date
            """
        ).fetchall()
        trend = []
        for d, in_svc, logged in trend_rows:
            in_svc = in_svc or 0
            trend.append({
                "entry_date": d.isoformat() if hasattr(d, "isoformat") else str(d),
                "in_service_count": in_svc,
                "logged_count": logged or 0,
                "fleet_pct": (
                    round(in_svc / active_trucks * 100, 1) if active_trucks else None
                ),
            })

        return {
            "window_days": days,
            "fleet": {
                "active_trucks": active_trucks,
                "total_trucks": total_trucks,
                "fleet_util_pct": fleet_util_pct,
                "fleet_jobs": fleet_jobs,
                "fleet_hours": round(fleet_hours, 2),
                "in_service_days_logged": fleet_in_service_days,
                "denominator_truck_days": denominator,
            },
            "trucks": truck_rows,
            "daily_trend": trend,
        }
    finally:
        con.close()


# ─────────────────── Storage operations report ───────────────────

def get_storage_report():
    """Storage Operations — Page 9 of MME_Unified_Dashboard_Spec.md.

    Combines the storage roster (potential capacity & street rate) with
    snapshot history (actual occupancy & MRR) to compute the spec metrics.
    """
    con = _con(read_only=True)
    try:
        # Roster aggregates — potential at street rate
        roster = con.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE active) AS active_units,
                COUNT(*) AS total_units,
                COALESCE(SUM(monthly_rate) FILTER (WHERE active), 0) AS potential_mrr,
                COALESCE(SUM(cubic_feet) FILTER (WHERE active), 0) AS active_cuft
            FROM storage_units
            """
        ).fetchone()
        active_units, total_units, potential_mrr, active_cuft = roster
        active_units = active_units or 0
        total_units = total_units or 0
        potential_mrr = float(potential_mrr or 0)
        active_cuft = float(active_cuft or 0)

        # Latest snapshot
        latest_row = con.execute(
            "SELECT snapshot_date, units_total, units_occupied, mrr, "
            "delinquent_30, delinquent_60, delinquent_90, move_ins, move_outs, notes "
            "FROM storage_snapshots ORDER BY snapshot_date DESC LIMIT 1"
        ).fetchone()

        latest = None
        if latest_row:
            d, ut, uo, mrr, d30, d60, d90, mi, mo, notes = latest_row
            ut = ut or 0
            uo = uo or 0
            mrr = float(mrr or 0)
            occupancy_pct = round(uo / ut * 100, 1) if ut else None
            avg_rate_per_occupied = round(mrr / uo, 2) if uo else None
            economic_occupancy_pct = (
                round(mrr / potential_mrr * 100, 1) if potential_mrr else None
            )
            revenue_per_cuft = round(mrr / active_cuft, 2) if active_cuft else None
            delinquent_total = (d30 or 0) + (d60 or 0) + (d90 or 0)
            delinquent_pct = round(delinquent_total / uo * 100, 1) if uo else None
            net_move_ins = (mi or 0) - (mo or 0)

            # Verdict thresholds from the spec (occupancy alert at 85%)
            occ_status = (
                "OK" if (occupancy_pct or 0) >= 85
                else "WARN" if (occupancy_pct or 0) >= 70
                else "MISS"
            ) if occupancy_pct is not None else ""

            latest = {
                "snapshot_date": d.isoformat() if hasattr(d, "isoformat") else str(d),
                "units_total": ut,
                "units_occupied": uo,
                "mrr": mrr,
                "delinquent_30": d30 or 0,
                "delinquent_60": d60 or 0,
                "delinquent_90": d90 or 0,
                "delinquent_total": delinquent_total,
                "delinquent_pct": delinquent_pct,
                "move_ins": mi or 0,
                "move_outs": mo or 0,
                "net_move_ins": net_move_ins,
                "notes": notes,
                "occupancy_pct": occupancy_pct,
                "occupancy_status": occ_status,
                "avg_rate_per_occupied": avg_rate_per_occupied,
                "economic_occupancy_pct": economic_occupancy_pct,
                "revenue_per_cuft": revenue_per_cuft,
            }

        # Trend (last 90 days)
        trend_rows = con.execute(
            """
            SELECT snapshot_date, units_total, units_occupied, mrr,
                   delinquent_30, delinquent_60, delinquent_90,
                   move_ins, move_outs
            FROM storage_snapshots
            WHERE snapshot_date >= CURRENT_DATE - INTERVAL 90 DAY
            ORDER BY snapshot_date
            """
        ).fetchall()
        trend = []
        for r in trend_rows:
            d, ut, uo, mrr, d30, d60, d90, mi, mo = r
            trend.append({
                "snapshot_date": d.isoformat() if hasattr(d, "isoformat") else str(d),
                "units_total": ut or 0,
                "units_occupied": uo or 0,
                "occupancy_pct": round((uo or 0) / ut * 100, 1) if ut else None,
                "mrr": float(mrr or 0),
                "delinquent_30": d30 or 0,
                "delinquent_60": d60 or 0,
                "delinquent_90": d90 or 0,
                "move_ins": mi or 0,
                "move_outs": mo or 0,
                "net_move_ins": (mi or 0) - (mo or 0),
            })

        # 30-day net move-in rate (sum of move-ins minus move-outs across snapshots in window)
        flow_30 = con.execute(
            """
            SELECT COALESCE(SUM(move_ins), 0), COALESCE(SUM(move_outs), 0)
            FROM storage_snapshots
            WHERE snapshot_date >= CURRENT_DATE - INTERVAL 30 DAY
            """
        ).fetchone()
        flow_move_ins, flow_move_outs = (flow_30[0] or 0), (flow_30[1] or 0)

        # Active unit roster (for breakdown by size class)
        size_breakdown = con.execute(
            """
            SELECT COALESCE(NULLIF(size_class, ''), 'Unspecified') AS size,
                   COUNT(*) AS count,
                   COALESCE(SUM(monthly_rate), 0) AS potential_mrr,
                   COALESCE(SUM(cubic_feet), 0) AS cubic_feet
            FROM storage_units
            WHERE active
            GROUP BY size
            ORDER BY count DESC
            """
        ).fetchall()
        size_classes = [
            {"size": r[0], "count": r[1], "potential_mrr": float(r[2] or 0),
             "cubic_feet": float(r[3] or 0)}
            for r in size_breakdown
        ]

        return {
            "roster": {
                "active_units": active_units,
                "total_units": total_units,
                "potential_mrr": potential_mrr,
                "active_cuft": active_cuft,
            },
            "latest": latest,
            "trend": trend,
            "flow_30d": {
                "move_ins": flow_move_ins,
                "move_outs": flow_move_outs,
                "net": flow_move_ins - flow_move_outs,
            },
            "size_classes": size_classes,
        }
    finally:
        con.close()


# ─────────────────────── Aggregate summary ───────────────────────

def inputs_summary():
    """Lightweight summary used by the Inputs tab header and Overview tile."""
    con = _con(read_only=True)
    try:
        result = {}
        result["truck_count"] = con.execute(
            "SELECT COUNT(*) FROM trucks WHERE active"
        ).fetchone()[0]
        result["storage_unit_count"] = con.execute(
            "SELECT COUNT(*) FROM storage_units WHERE active"
        ).fetchone()[0]

        latest = con.execute(
            "SELECT snapshot_date, units_total, units_occupied, mrr "
            "FROM storage_snapshots ORDER BY snapshot_date DESC LIMIT 1"
        ).fetchone()
        if latest:
            d = latest[0]
            result["latest_snapshot"] = {
                "snapshot_date": d.isoformat() if hasattr(d, "isoformat") else str(d),
                "units_total": latest[1],
                "units_occupied": latest[2],
                "mrr": latest[3],
                "occupancy_pct": (
                    round(latest[2] / latest[1] * 100, 1) if latest[1] else None
                ),
            }
        else:
            result["latest_snapshot"] = None

        # Last 7 days truck utilization
        util = con.execute(
            """
            SELECT COUNT(DISTINCT entry_date) AS days_logged,
                   AVG(CASE WHEN in_service THEN 1.0 ELSE 0.0 END) AS avg_in_service
            FROM truck_utilization_daily
            WHERE entry_date >= CURRENT_DATE - INTERVAL 7 DAY
            """
        ).fetchone()
        result["util_last_7"] = {
            "days_logged": util[0] or 0,
            "avg_in_service_pct": round((util[1] or 0) * 100, 1),
        }

        # Last 30 days turnaways
        ta = con.execute(
            """
            SELECT COALESCE(SUM(count), 0), COALESCE(SUM(est_revenue_lost), 0)
            FROM jobs_turned_away
            WHERE entry_date >= CURRENT_DATE - INTERVAL 30 DAY
            """
        ).fetchone()
        result["turnaways_last_30"] = {
            "count": int(ta[0]),
            "est_revenue_lost": float(ta[1]),
        }
        return result
    finally:
        con.close()
