"""
DuckDB Export Utilities
=======================
CSV per-MDO, single CSV, and warehouse Parquet export functions
that replace the PySpark-based dfexportutil module.
"""

import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb

log = logging.getLogger("duckdb-export")


# ───────────────────────────────────────────────────────────────────
# Duration format helper (SQL expression)
# ───────────────────────────────────────────────────────────────────
def duration_format_expr(col_name: str, alias: str = None) -> str:
    """Return a DuckDB SQL expression that converts *col_name* (seconds)
    to ``HH:MM:SS`` string.  Use inside a SELECT clause."""
    alias = alias or col_name
    return (
        f"CASE WHEN \"{col_name}\" IS NULL OR \"{col_name}\" = 0 THEN '' ELSE "
        f"LPAD(CAST(CAST(\"{col_name}\" / 3600 AS INT) AS VARCHAR), 2, '0') || ':' || "
        f"LPAD(CAST(CAST(\"{col_name}\" % 3600 / 60 AS INT) AS VARCHAR), 2, '0') || ':' || "
        f"LPAD(CAST(CAST(\"{col_name}\" % 60 AS INT) AS VARCHAR), 2, '0') "
        f"END AS \"{alias}\""
    )


# ───────────────────────────────────────────────────────────────────
# Write CSV — single file
# ───────────────────────────────────────────────────────────────────
def write_single_csv(con: duckdb.DuckDBPyConnection, sql: str,
                     output_path: str):
    """Execute *sql* and write the result as a single CSV file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    con.execute(f"""
        COPY ({sql}) TO '{output_path}'
        (FORMAT CSV, HEADER TRUE, DELIMITER ',')
    """)
    log.info(f"Wrote CSV → {output_path}")


# ───────────────────────────────────────────────────────────────────
# Write CSV — per MDO (partitioned by an org-id column)
# ───────────────────────────────────────────────────────────────────
def write_csv_per_mdo(con: duckdb.DuckDBPyConnection, sql: str,
                      output_dir: str, mdo_column: str,
                      csv_filename: str = "report.csv",
                      max_workers: int = 4):
    """
    Partition the result of *sql* by *mdo_column* and write one CSV
    per unique value into ``output_dir/mdoid=<value>/<csv_filename>``.

    Uses a temporary table to avoid re-executing the query for every MDO.
    """
    tmp_table = "_export_tmp"
    con.execute(f"CREATE OR REPLACE TEMP TABLE {tmp_table} AS {sql}")

    mdo_ids = [
        row[0] for row in
        con.execute(
            f'SELECT DISTINCT "{mdo_column}" FROM {tmp_table} '
            f'WHERE "{mdo_column}" IS NOT NULL'
        ).fetchall()
    ]

    log.info(f"Writing CSV for {len(mdo_ids)} MDOs → {output_dir}")

    def _write_one(mdo_id):
        mdo_dir = os.path.join(output_dir, f"mdoid={mdo_id}")
        os.makedirs(mdo_dir, exist_ok=True)
        out_file = os.path.join(mdo_dir, csv_filename)
        # Open a new connection for thread safety
        thread_con = duckdb.connect(con.execute("SELECT current_database()").fetchone()[0], read_only=True)
        try:
            # Re-read from the temp table written to the main connection
            # Since DuckDB temp tables are connection-scoped, we need
            # to use the main connection approach differently
            pass
        finally:
            thread_con.close()

        # Single-threaded safe approach: use the main connection
        escaped = str(mdo_id).replace("'", "''")
        select_cols = ", ".join(
            f'"{c}"' for c in
            [r[0] for r in con.execute(f"DESCRIBE {tmp_table}").fetchall()]
            if c != mdo_column
        )
        con.execute(f"""
            COPY (
                SELECT {select_cols}
                FROM {tmp_table}
                WHERE "{mdo_column}" = '{escaped}'
            ) TO '{out_file}'
            (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        """)

    for mdo_id in mdo_ids:
        _write_one(mdo_id)

    con.execute(f"DROP TABLE IF EXISTS {tmp_table}")
    log.info(f"Done writing {len(mdo_ids)} CSVs")


# ───────────────────────────────────────────────────────────────────
# Write warehouse Parquet
# ───────────────────────────────────────────────────────────────────
def write_warehouse_parquet(con: duckdb.DuckDBPyConnection, sql: str,
                            output_path: str):
    """Execute *sql* and write result as a snappy-compressed Parquet file."""
    os.makedirs(output_path, exist_ok=True)
    out_file = os.path.join(output_path, "part-00000.snappy.parquet")
    con.execute(f"""
        COPY ({sql}) TO '{out_file}'
        (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    log.info(f"Wrote Parquet → {out_file}")


# ───────────────────────────────────────────────────────────────────
# Ministry / Department / Organization hierarchy columns (SQL)
# ───────────────────────────────────────────────────────────────────
MDO_HIERARCHY_COLUMNS = """
    "userOrgName" AS "MDO_Name",
    CASE WHEN "ministry_name" IS NULL THEN "userOrgName"
         ELSE "ministry_name" END AS "Ministry",
    CASE WHEN "ministry_name" IS NOT NULL
              AND "ministry_name" <> "userOrgName"
              AND ("dept_name" IS NULL OR "dept_name" = '')
         THEN "userOrgName"
         ELSE "dept_name"
    END AS "Department",
    CASE WHEN "ministry_name" <> "userOrgName"
              AND "dept_name" <> "userOrgName"
         THEN "userOrgName"
         ELSE ''
    END AS "Organization"
"""

CURRENT_DATETIME_EXPR = "strftime(NOW(), '%Y-%m-%d %H:%M:%S')"
CURRENT_DATE_EXPR = "CAST(CURRENT_DATE AS VARCHAR)"
