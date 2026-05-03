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
