"""
Data Warehouse Sync — DuckDB Migration
Reads warehouse parquet tables from DuckDB, writes to PostgreSQL
via psycopg2 COPY (fast bulk load).
"""
import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from io import StringIO

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH

log = logging.getLogger("data-warehouse")

# Warehouse table → DuckDB source table/query mapping
WAREHOUSE_TABLES = {
    "user_detail":                      "user_warehouse_computed",
    "content":                          "content_warehouse_computed",
    "user_enrolments":                  "enrolment_warehouse_computed",
    "content_resource":                 None,  # written by course_report
    "assessment_detail":                None,  # written by course_based_assessment_report
    "bp_enrolments":                    None,  # written by blended_report
    "kcm_dictionary":                   None,  # written by kcm_report
    "kcm_content_mapping":              None,  # written by kcm_report
    "cb_plan":                          None,  # written by acbp_report
    "events":                           "warehouse_events",
    "events_enrolment":                 "warehouse_event_enrolments",
    "course_completion_survey_details": None,  # written by course_completion_survey
    "user_activity":                    None,  # written by user_activity
    "apar_cbp_enrollment":              None,  # written by acbp_report
}


def _write_to_postgres(con, table_name, source, pg_url, pg_user, pg_pass):
    """Export DuckDB source (table name or SQL) → PostgreSQL table via DuckDB postgres_scanner."""
    try:
        con.execute("INSTALL postgres; LOAD postgres;")
    except Exception:
        pass  # Already installed

    # Attach PostgreSQL
    con.execute(f"""
        ATTACH '{pg_url}' AS pg_db (
            TYPE POSTGRES,
            USER '{pg_user}',
            PASSWORD '{pg_pass}'
        )
    """)

    # Drop and recreate from DuckDB source
    con.execute(f"DROP TABLE IF EXISTS pg_db.{table_name}")
    con.execute(f"CREATE TABLE pg_db.{table_name} AS SELECT * FROM {source}")
    con.execute("DETACH pg_db")
    log.info(f"  Synced → PostgreSQL: {table_name}")


def _write_parquet_to_postgres(config, table_name, parquet_dir, pg_url, pg_user, pg_pass):
    """Read warehouse parquet written by Stage 2 jobs → PostgreSQL."""
    parquet_path = f"{config.warehouseReportDir}/{table_name}"
    if not os.path.exists(parquet_path):
        log.warning(f"  Skipping {table_name}: parquet not found at {parquet_path}")
        return

    tmp_con = duckdb.connect()
    try:
        tmp_con.execute("INSTALL postgres; LOAD postgres;")
    except Exception:
        pass

    tmp_con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _wh AS
        SELECT * FROM read_parquet('{parquet_path}/**/*.parquet', union_by_name=true)
    """)

    tmp_con.execute(f"""
        ATTACH '{pg_url}' AS pg_db (
            TYPE POSTGRES,
            USER '{pg_user}',
            PASSWORD '{pg_pass}'
        )
    """)
    tmp_con.execute(f"DROP TABLE IF EXISTS pg_db.{table_name}")
    tmp_con.execute(f"CREATE TABLE pg_db.{table_name} AS SELECT * FROM _wh")
    tmp_con.execute("DETACH pg_db")
    tmp_con.close()
    log.info(f"  Synced → PostgreSQL: {table_name} (from parquet)")


def process_data(config, db_path=None):
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    wh_host = config.warehousePostgresHost
    wh_schema = getattr(config, 'warehousePostgresSchema', 'igot_warehouse')
    wh_user = config.warehousePostgresUsername
    wh_pass = config.warehousePostgresCredential
    pg_url = f"postgresql://{wh_host}/{wh_schema}"

    log.info(f"Syncing warehouse tables to PostgreSQL at {wh_host}...")

    for table_name, duckdb_source in WAREHOUSE_TABLES.items():
        if duckdb_source:
            # Table exists inside the initialized DuckDB
            _write_to_postgres(con, table_name, duckdb_source,
                               pg_url, wh_user, wh_pass)
        else:
            # Table was exported as parquet by a Stage 2 job
            _write_parquet_to_postgres(config, table_name, None,
                                       pg_url, wh_user, wh_pass)

    # Also sync org_hierarchy
    _write_to_postgres(con, "org_hierarchy", "org_hierarchy_select",
                       pg_url, wh_user, wh_pass)

    con.close()
    log.info("DataWarehouse sync — done")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] DataWarehouse at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] DataWarehouse — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
