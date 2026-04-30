"""
Initialize DuckDB schema for MME Unified Dashboard.
Tables mirror SmartMoving API data model.
Run once to create, safe to re-run (uses IF NOT EXISTS).
"""

import duckdb
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mme_dashboard.duckdb")


def init_schema(db_path=DB_PATH):
    con = duckdb.connect(db_path)

    # ── Staging tables (raw API data, refreshed on each sync) ──

    con.execute("""
        CREATE TABLE IF NOT EXISTS branches (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            phone_number VARCHAR,
            address_full VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip VARCHAR,
            lat DOUBLE,
            lng DOUBLE,
            is_primary BOOLEAN,
            synced_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            title VARCHAR,
            email VARCHAR,
            primary_branch_id VARCHAR,
            primary_branch_name VARCHAR,
            role_id VARCHAR,
            role_name VARCHAR,
            synced_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS move_sizes (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            description VARCHAR,
            volume INTEGER,
            weight DOUBLE,
            synced_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS referral_sources (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            is_lead_provider BOOLEAN,
            is_public BOOLEAN,
            synced_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            phone_number VARCHAR,
            phone_type INTEGER,
            email_address VARCHAR,
            address VARCHAR,
            synced_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id VARCHAR PRIMARY KEY,
            customer_name VARCHAR,
            email_address VARCHAR,
            phone_number VARCHAR,
            phone_type INTEGER,
            referral_source VARCHAR,
            referral_source_name VARCHAR,
            affiliate_name VARCHAR,
            service_date INTEGER,
            sales_person_id VARCHAR,
            sales_person VARCHAR,
            type VARCHAR,
            branch_id VARCHAR,
            branch_name VARCHAR,
            origin_address_full VARCHAR,
            destination_address_full VARCHAR,
            move_size_id VARCHAR,
            move_size_name VARCHAR,
            status INTEGER,
            lost_reason VARCHAR,
            created_at_utc TIMESTAMP,
            synced_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS opportunities (
            id VARCHAR PRIMARY KEY,
            quote_number INTEGER,
            customer_id VARCHAR,
            customer_name VARCHAR,
            customer_email VARCHAR,
            customer_phone VARCHAR,
            branch_name VARCHAR,
            opportunity_type INTEGER,
            type INTEGER,
            service_date INTEGER,
            status INTEGER,
            lead_status VARCHAR,
            move_size VARCHAR,
            volume DOUBLE,
            weight DOUBLE,
            estimated_subtotal DOUBLE,
            estimated_tax DOUBLE,
            estimated_total DOUBLE,
            estimator VARCHAR,
            sales_assignee VARCHAR,
            referral_source VARCHAR,
            custom_field_01 VARCHAR,
            custom_field_02 VARCHAR,
            custom_field_03 VARCHAR,
            created_at_utc TIMESTAMP,
            synced_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id VARCHAR PRIMARY KEY,
            opportunity_id VARCHAR,
            job_number VARCHAR,
            job_date INTEGER,
            type INTEGER,
            confirmed BOOLEAN,
            estimated_subtotal DOUBLE,
            estimated_tax DOUBLE,
            estimated_total DOUBLE,
            actual_subtotal DOUBLE,
            actual_tax DOUBLE,
            actual_total DOUBLE,
            total_tips DOUBLE,
            arrival_window_description VARCHAR,
            arrival_window_start DOUBLE,
            arrival_window_end DOUBLE,
            synced_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS job_crew_members (
            job_id VARCHAR,
            crew_member_name VARCHAR,
            synced_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (job_id, crew_member_name)
        )
    """)

    # ── Snapshot table (frozen daily metrics, append-only) ──

    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_snapshots (
            snapshot_date DATE PRIMARY KEY,
            total_revenue DOUBLE,
            total_jobs INTEGER,
            contribution_profit DOUBLE,
            fully_loaded_profit DOUBLE,
            trucks_used INTEGER,
            total_leads INTEGER,
            total_reviews INTEGER,
            storage_occupancy_pct DOUBLE,
            claims_count INTEGER,
            created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    # ── Sync log (track ingestion runs) ──

    con.execute("CREATE SEQUENCE IF NOT EXISTS sync_log_seq START 1")

    con.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY DEFAULT nextval('sync_log_seq'),
            source VARCHAR,
            table_name VARCHAR,
            records_synced INTEGER,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            status VARCHAR,
            error_message VARCHAR
        )
    """)

    con.close()
    print(f"Schema initialized: {db_path}")


if __name__ == "__main__":
    init_schema()
