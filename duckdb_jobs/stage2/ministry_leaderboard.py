"""
Ministry Leaderboard — DuckDB Migration
Already DuckDB-based in original. Reorganized to use the
initialized DuckDB database + write to Postgres.
"""
import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH

log = logging.getLogger("ministry-leaderboard")


def process_data(config, db_path=None):
    t0 = time.time()
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    cache_path = getattr(config, 'baseCachePath',
                         str(Path(__file__).resolve().parents[2] / "data-res/pq_files/cache_pq"))

    # Previous month date range
    today = datetime.now().date()
    month_end_date = today.replace(day=1) - timedelta(days=1)
    month_start = month_end_date.replace(day=1)
    month_start_str = f"{month_start} 00:00:00"
    month_end_str = f"{month_end_date} 23:59:59"
    month_num = month_start.month
    year_num = month_start.year

    log.info(f"Processing month {month_num}/{year_num}: {month_start_str} → {month_end_str}")

    # ── Create temp DuckDB for write operations ──────────────────
    tmp_dir = str(Path(__file__).resolve().parents[2] / "temp_leaderboard_duckdb")
    os.makedirs(tmp_dir, exist_ok=True)
    wcon = duckdb.connect(f"{tmp_dir}/leaderboard.duckdb")
    wcon.execute(f"SET temp_directory='{tmp_dir}'")
    wcon.execute("SET memory_limit='8GB'")

    # Load data from initialized DB + cache
    user_org_path = f"{cache_path}/../../output"  # adjust to actual user_org_computed path

    # Read from main DuckDB
    user_org_df = con.execute("SELECT * FROM user_org_computed").fetchdf()
    org_h_df = con.execute("SELECT * FROM org_hierarchy_select").fetchdf()

    wcon.execute("CREATE OR REPLACE TABLE user_org AS SELECT * FROM user_org_df")
    wcon.execute("CREATE OR REPLACE TABLE org_hierarchy AS SELECT * FROM org_h_df")

    wcon.execute(f"""
        CREATE OR REPLACE TABLE karma_points AS
        SELECT userid, points, credit_date
        FROM read_parquet('{cache_path}/userKarmaPoints/**/*.parquet', union_by_name=true)
        WHERE credit_date >= '{month_start_str}' AND credit_date <= '{month_end_str}'
    """)

    wcon.execute("""
        CREATE OR REPLACE TABLE karma_aggregated AS
        SELECT userid, SUM(points) AS total_points, MAX(credit_date) AS last_credit_date
        FROM karma_points GROUP BY userid
    """)

    # Distinct MDOs
    wcon.execute("""
        CREATE OR REPLACE TABLE distinct_mdos AS
        SELECT DISTINCT "userOrgID" FROM user_org
    """)
    wcon.execute("""
        CREATE OR REPLACE TABLE joined_orgs AS
        SELECT oh.* FROM org_hierarchy oh
        INNER JOIN distinct_mdos dm ON oh.mdo_id = dm."userOrgID"
    """)

    # L3: MDO level (non-department MDOs)
    wcon.execute("""
        CREATE OR REPLACE TABLE orgs_l3 AS
        SELECT DISTINCT uo."userID", uo."userOrgID" AS userParentID,
            uo."professionalDetails_designation" AS designation,
            uo."userProfileImgUrl", uo."fullName", uo."userOrgName"
        FROM user_org uo
        INNER JOIN (
            SELECT DISTINCT mdo_id AS organisationID FROM joined_orgs
        ) orgs ON uo."userOrgID" = orgs.organisationID
    """)

    # Build leaderboard per org level
    for level_table in ["orgs_l3"]:
        wcon.execute(f"""
            CREATE OR REPLACE TABLE {level_table}_leaderboard AS
            SELECT
                u.userParentID AS org_id,
                u."userID" AS userid,
                COALESCE(ka.total_points, 0) AS total_points,
                ka.last_credit_date,
                u.fullName AS full_name,
                u.designation,
                u.userProfileImgUrl AS profile_image,
                u.userOrgName AS org_name,
                {month_num} AS month,
                {year_num} AS year,
                DENSE_RANK() OVER (PARTITION BY u.userParentID ORDER BY COALESCE(ka.total_points, 0) DESC) AS rank,
                ROW_NUMBER() OVER (PARTITION BY u.userParentID ORDER BY
                    COALESCE(ka.total_points, 0) DESC, ka.last_credit_date DESC NULLS LAST) AS row_num
            FROM {level_table} u
            LEFT JOIN karma_aggregated ka ON u."userID" = ka.userid
        """)

    # ── Write to Postgres ────────────────────────────────────────
    app_pg_url = f"postgresql://{config.appPostgresHost}/{config.appPostgresSchema}"
    try:
        wcon.execute("INSTALL postgres; LOAD postgres;")
    except Exception:
        pass

    wcon.execute(f"""
        ATTACH '{app_pg_url}' AS pg_db (
            TYPE POSTGRES,
            USER '{config.appPostgresUsername}',
            PASSWORD '{config.appPostgresCredential}'
        )
    """)

    wcon.execute("DROP TABLE IF EXISTS pg_db.ministry_leaderboard")
    wcon.execute("""
        CREATE TABLE pg_db.ministry_leaderboard AS
        SELECT org_id, userid, total_points, last_credit_date, full_name,
               designation, profile_image, org_name, month, year, rank, row_num
        FROM orgs_l3_leaderboard
    """)

    wcon.execute("DETACH pg_db")
    wcon.close()
    con.close()

    log.info(f"[SUCCESS] MinistryLeaderboard — completed in {time.time() - t0:.1f}s")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] MinistryLeaderboard at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] MinistryLeaderboard — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
