"""
Ministry Metrics — DuckDB Migration
Per-ministry/department/org rolled-up metrics → Redis.
Active users (via Druid), certificates, enrolments, user counts.
"""
import sys
import logging
from pathlib import Path
from datetime import datetime

import duckdb
import requests

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH
from dfutil.utils.redis import Redis

log = logging.getLogger("ministry-metrics")


def _druid_query(host, sql):
    url = f"http://{host}/druid/v2/sql"
    resp = requests.post(url, json={"query": sql, "resultFormat": "object"}, timeout=120)
    resp.raise_for_status()
    return resp.json()


def _dispatch_dict(redis_key, data_dict, config):
    """Write dict {key: value} as a Redis hash (like dispatchDataFrame)."""
    Redis.dispatch(redis_key, data_dict, replace=True, conf=config)


def process_data(config, db_path=None):
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    # ── Build org-hierarchy lookup ───────────────────────────────
    # ministry_names: mdo_name → mdo_id mapping
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _org_h AS
        SELECT mdo_id, mdo_name AS ministry, department
        FROM org_hierarchy_select
    """)

    # ── Active users & base user set ─────────────────────────────
    # user_computed with userStatus=1
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _active_users AS
        SELECT "userID" AS user_id, "userOrgID" AS user_org_id
        FROM user_computed WHERE "userStatus" = 1
    """)

    # ── 24hr active users from Druid ─────────────────────────────
    try:
        druid_result = _druid_query(config.sparkDruidRouterHost, """
            SELECT DISTINCT uid AS user_id
            FROM "summary-events"
            WHERE dimensions_type = 'app'
            AND __time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR
        """)
        logged_in_ids = [r["user_id"] for r in druid_result] if druid_result else []
    except Exception as e:
        log.warning(f"Druid 24hr active user query failed: {e}")
        logged_in_ids = []

    if logged_in_ids:
        con.execute("CREATE OR REPLACE TEMP TABLE _logged_in (user_id VARCHAR)")
        con.executemany("INSERT INTO _logged_in VALUES (?)", [(uid,) for uid in logged_in_ids])

        # Active user counts at ministry / department / org level
        active_user_counts = con.execute("""
            SELECT oh.mdo_id AS ministry_id,
                   CAST(COUNT(*) AS VARCHAR) AS active_count
            FROM _logged_in li
            INNER JOIN _active_users au ON li.user_id = au.user_id
            LEFT JOIN _org_h oh ON au.user_org_id = oh.mdo_id
            WHERE oh.mdo_id IS NOT NULL
            GROUP BY oh.mdo_id
        """).fetchall()
        _dispatch_dict("dashboard_rolled_up_login_percent_last_24_hrs",
                       {r[0]: r[1] for r in active_user_counts}, config)

    # ── User count per ministry/dept/org ─────────────────────────
    user_counts = con.execute("""
        SELECT oh.mdo_id AS ministry_id,
               CAST(COUNT(*) AS VARCHAR) AS user_count
        FROM _active_users au
        LEFT JOIN _org_h oh ON au.user_org_id = oh.mdo_id
        WHERE oh.mdo_id IS NOT NULL
        GROUP BY oh.mdo_id
    """).fetchall()
    _dispatch_dict("dashboard_rolled_up_user_count",
                   {r[0]: r[1] for r in user_counts}, config)

    # ── Certificate count per ministry/dept/org ──────────────────
    cert_counts = con.execute("""
        SELECT oh.mdo_id AS ministry_id,
               CAST(COUNT(DISTINCT ew."certificateID") AS VARCHAR) AS cert_count
        FROM enrolment_warehouse_computed ew
        INNER JOIN _active_users au ON ew."userID" = au.user_id
        LEFT JOIN _org_h oh ON au.user_org_id = oh.mdo_id
        WHERE oh.mdo_id IS NOT NULL AND ew."certificateID" IS NOT NULL
        GROUP BY oh.mdo_id
    """).fetchall()
    _dispatch_dict("dashboard_rolled_up_certificates_generated_count",
                   {r[0]: r[1] for r in cert_counts}, config)

    # ── Enrolment count per ministry/dept/org ────────────────────
    enrol_counts = con.execute("""
        SELECT oh.mdo_id AS ministry_id,
               CAST(COUNT(*) AS VARCHAR) AS enrol_count
        FROM enrolment_warehouse_computed ew
        INNER JOIN _active_users au ON ew."userID" = au.user_id
        LEFT JOIN _org_h oh ON au.user_org_id = oh.mdo_id
        WHERE oh.mdo_id IS NOT NULL
        GROUP BY oh.mdo_id
    """).fetchall()
    _dispatch_dict("dashboard_rolled_up_enrolment_content_count",
                   {r[0]: r[1] for r in enrol_counts}, config)

    con.close()
    log.info("[SUCCESS] MinistryMetrics updated")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] MinistryMetrics at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] MinistryMetrics — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
