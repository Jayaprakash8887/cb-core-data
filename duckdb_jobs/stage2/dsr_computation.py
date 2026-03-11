"""
DSR Computation — DuckDB Migration
Reads from DuckDB + Druid, writes metrics to Redis.
"""
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta, time, timezone

import duckdb
import requests

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH
from dfutil.utils.redis import Redis

log = logging.getLogger("dsr-computation")


def _druid_query(host, sql, limit=10_000_000):
    """Execute a Druid SQL query and return list of dicts."""
    url = f"http://{host}/druid/v2/sql"
    payload = {"query": sql, "resultFormat": "object", "context": {"sqlQueryId": "dsr-duckdb"}}
    resp = requests.post(url, json=payload, timeout=120)
    resp.raise_for_status()
    return resp.json()


def process_data(config, db_path=None):
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    # ── Active users: status == 1 ──────────────────────────────
    total_active_users = con.execute("""
        SELECT COUNT(*) FROM user_computed WHERE "userStatus" = 1
    """).fetchone()[0]
    Redis.update("mdo_total_registered_officer_count", str(total_active_users), conf=config)

    # ── Users registered yesterday ─────────────────────────────
    users_registered_yday = con.execute("""
        SELECT COUNT(*) FROM user_computed
        WHERE "userStatus" = 1
          AND TRY_CAST("userCreatedDate" AS TIMESTAMP)
              BETWEEN CURRENT_DATE - INTERVAL 1 DAY AND CURRENT_DATE
    """).fetchone()[0]
    Redis.update("dashboard_new_users_registered_yesterday", str(users_registered_yday), conf=config)

    # ── Content enrolments (active users, Live/Retired content) ──
    # Total enrolments = platform + external
    platform_enrolments = con.execute("""
        SELECT COUNT(*) FROM enrolment_select e
        INNER JOIN user_computed u ON e."userID" = u."userID" AND u."userStatus" = 1
        INNER JOIN content_computed c ON e."courseID" = c."courseID"
        WHERE c."category" IN ('Course','Program','Blended Program','CuratedCollections','Curated Program')
          AND c."courseStatus" IN ('Live','Retired')
    """).fetchone()[0]
    external_enrolments = con.execute("""
        SELECT COUNT(*) FROM external_enrolment_computed
    """).fetchone()[0]
    Redis.update("dashboard_enrolment_count", str(platform_enrolments + external_enrolments), conf=config)

    # ── Unique users enrolled in courses ─────────────────────────
    unique_enrolled = con.execute("""
        SELECT COUNT(DISTINCT e."userID") FROM enrolment_select e
        INNER JOIN user_computed u ON e."userID" = u."userID"
        INNER JOIN content_computed c ON e."courseID" = c."courseID"
        WHERE c."category" = 'Course'
          AND c."courseStatus" IN ('Live','Retired')
          AND u."userOrgID" IS NOT NULL
    """).fetchone()[0]
    Redis.update("dashboard_unique_users_enrolled_count", str(unique_enrolled), conf=config)

    # ── Total content completions ────────────────────────────────
    platform_completions = con.execute("""
        SELECT COUNT(*) FROM enrolment_select e
        INNER JOIN user_computed u ON e."userID" = u."userID" AND u."userStatus" = 1
        INNER JOIN content_computed c ON e."courseID" = c."courseID"
        WHERE c."category" IN ('Course','Program','Blended Program','CuratedCollections','Curated Program')
          AND c."courseStatus" IN ('Live','Retired')
          AND e."dbCompletionStatus" = 2
    """).fetchone()[0]
    external_completions = con.execute("""
        SELECT COUNT(*) FROM external_enrolment_computed WHERE status = 2
    """).fetchone()[0]
    Redis.update("dashboard_completed_count", str(platform_completions + external_completions), conf=config)

    # ── Event metrics ────────────────────────────────────────────
    event_enrolments = con.execute("SELECT COUNT(*) FROM warehouse_event_enrolments").fetchone()[0]
    Redis.update("dashboard_events_enrolment_count", str(event_enrolments), conf=config)

    nlw_start = getattr(config, 'nationalLearningWeekStart', '2024-01-01 00:00:00')
    event_completions = con.execute(f"""
        SELECT COUNT(DISTINCT certificate_id) FROM warehouse_event_enrolments
        WHERE certificate_id IS NOT NULL
          AND status = 'completed'
          AND enrolled_on_datetime >= '{nlw_start}'
    """).fetchone()[0]
    Redis.update("dashboard_events_completed_count", str(event_completions), conf=config)

    # ── Certificates generated yesterday ─────────────────────────
    ist_offset = timezone(timedelta(hours=5, minutes=30))
    current_date = datetime.now(ist_offset).date()
    prev_start = datetime.combine(current_date - timedelta(days=1), time.min, tzinfo=ist_offset).strftime("%Y-%m-%d %H:%M:%S")
    prev_end = (datetime.combine(current_date, time.min, tzinfo=ist_offset) - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

    content_certs_yday = con.execute(f"""
        SELECT COUNT(*) FROM enrolment_select e
        INNER JOIN user_computed u ON e."userID" = u."userID" AND u."userStatus" = 1
        INNER JOIN content_computed c ON e."courseID" = c."courseID"
        WHERE c."category" IN ('Course','Program','Blended Program','CuratedCollections','Curated Program')
          AND c."courseStatus" IN ('Live','Retired')
          AND e."dbCompletionStatus" = 2
          AND TRY_CAST(e."firstCompletedOn" AS TIMESTAMP) BETWEEN TIMESTAMP '{prev_start}' AND TIMESTAMP '{prev_end}'
    """).fetchone()[0]
    event_certs_yday = con.execute(f"""
        SELECT COUNT(DISTINCT certificate_id) FROM warehouse_event_enrolments
        WHERE certificate_id IS NOT NULL AND status = 'completed'
          AND enrolled_on_datetime BETWEEN '{prev_start}' AND '{prev_end}'
    """).fetchone()[0]
    Redis.update("lp_completed_yesterday_count", str(content_certs_yday + event_certs_yday), conf=config)

    # ── MAU (last 30 days) via Druid ─────────────────────────────
    try:
        mau_result = _druid_query(config.sparkDruidRouterHost, """
            SELECT COUNT(DISTINCT uid) AS activeCount
            FROM "summary-events"
            WHERE dimensions_type = 'app'
            AND __time >= TIME_FLOOR(CURRENT_TIMESTAMP, 'P1D') - INTERVAL '30' DAY
            AND __time < TIME_FLOOR(CURRENT_TIMESTAMP, 'P1D')
        """)
        mau = mau_result[0]["activeCount"] if mau_result else 0
    except Exception as e:
        log.warning(f"Druid MAU query failed: {e}")
        mau = 0
    Redis.update("lp_monthly_active_users", str(mau), conf=config)

    # ── Users logged in yesterday via Druid ──────────────────────
    try:
        login_result = _druid_query(config.sparkDruidRouterHost, """
            SELECT DISTINCT actor_id AS user_id
            FROM "telemetry-events-syncts"
            WHERE eid = 'IMPRESSION' AND actor_type = 'User'
            AND __time >= TIME_FLOOR(CURRENT_TIMESTAMP + INTERVAL '5:30' HOUR TO MINUTE - INTERVAL '24' HOUR, 'P1D')
            AND __time < TIME_FLOOR(CURRENT_TIMESTAMP + INTERVAL '5:30' HOUR TO MINUTE, 'P1D')
        """)
        logged_in_uids = {r["user_id"] for r in login_result} if login_result else set()

        if logged_in_uids:
            # Cross-reference with active users in DuckDB
            con.execute("CREATE OR REPLACE TEMP TABLE _logged_in (user_id VARCHAR)")
            con.executemany("INSERT INTO _logged_in VALUES (?)", [(uid,) for uid in logged_in_uids])
            logged_in_active = con.execute("""
                SELECT COUNT(DISTINCT l.user_id)
                FROM _logged_in l
                INNER JOIN user_computed u ON l.user_id = u."userID" AND u."userStatus" = 1
            """).fetchone()[0]
        else:
            logged_in_active = 0
    except Exception as e:
        log.warning(f"Druid login query failed: {e}")
        logged_in_active = 0
    Redis.update("dashboard_users_logged_in_yday", str(logged_in_active), conf=config)

    con.close()
    log.info("[SUCCESS] DSRComputationModel metrics updated")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] DSRComputation at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] DSRComputation — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
