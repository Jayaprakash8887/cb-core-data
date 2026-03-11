"""
DSR Computation Updated — DuckDB Migration
Overall/yesterday metrics for content published, enrolments,
completions, and registrations → Redis.
"""
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta, time, timezone

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH
from dfutil.utils.redis import Redis

log = logging.getLogger("dsr-computation-updated")


def process_data(config, db_path=None):
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    ist_offset = timezone(timedelta(hours=5, minutes=30))
    current_date = datetime.now(ist_offset).date()
    prev_start = datetime.combine(current_date - timedelta(days=1), time.min, tzinfo=ist_offset).strftime("%Y-%m-%d %H:%M:%S")
    prev_end = (datetime.combine(current_date, time.min, tzinfo=ist_offset) - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

    log.info(f"Yesterday window: {prev_start} → {prev_end}")

    # Active users (status == 1)
    active_users = con.execute("""
        SELECT COUNT(*) FROM user_computed WHERE "userStatus" = 1
    """).fetchone()[0]

    # ── Live content counts ──────────────────────────────────────
    overall_live = con.execute("""
        SELECT COUNT(*) FROM content_computed WHERE "courseStatus" = 'Live'
    """).fetchone()[0]
    yday_live = con.execute(f"""
        SELECT COUNT(*) FROM content_computed
        WHERE "courseStatus" = 'Live'
          AND TRY_CAST("lastPublishedOn" AS TIMESTAMP) BETWEEN TIMESTAMP '{prev_start}' AND TIMESTAMP '{prev_end}'
    """).fetchone()[0]
    Redis.update("overall_live_course_published", str(overall_live), conf=config)
    Redis.update("yesterday_live_course_published", str(yday_live), conf=config)

    # ── Enrolments (platform + external) ────────────────────────
    content_filter = "('Course','Program','Blended Program','CuratedCollections','Curated Program')"
    platform_enrol = con.execute(f"""
        SELECT COUNT(*) FROM enrolment_select e
        INNER JOIN user_computed u ON e."userID" = u."userID" AND u."userStatus" = 1
        INNER JOIN content_computed c ON e."courseID" = c."courseID"
          AND c."category" IN {content_filter}
          AND c."courseStatus" IN ('Live','Retired')
    """).fetchone()[0]
    external_enrol = con.execute("SELECT COUNT(*) FROM external_enrolment_computed").fetchone()[0]
    Redis.update("overall_course_enrolments", str(platform_enrol + external_enrol), conf=config)

    # Yesterday enrolments
    yday_platform_enrol = con.execute(f"""
        SELECT COUNT(*) FROM enrolment_select e
        INNER JOIN user_computed u ON e."userID" = u."userID" AND u."userStatus" = 1
        INNER JOIN content_computed c ON e."courseID" = c."courseID"
          AND c."category" IN {content_filter} AND c."courseStatus" IN ('Live','Retired')
        WHERE TRY_CAST(e."courseEnrolledTimestamp" AS TIMESTAMP)
            BETWEEN TIMESTAMP '{prev_start}' AND TIMESTAMP '{prev_end}'
    """).fetchone()[0]
    yday_external_enrol = con.execute(f"""
        SELECT COUNT(*) FROM external_enrolment_computed
        WHERE TRY_CAST(enrolled_date AS TIMESTAMP)
            BETWEEN TIMESTAMP '{prev_start}' AND TIMESTAMP '{prev_end}'
    """).fetchone()[0]
    Redis.update("yesterday_course_enrolments", str(yday_platform_enrol + yday_external_enrol), conf=config)

    # ── Completions (platform + external) ────────────────────────
    platform_complete = con.execute(f"""
        SELECT COUNT(*) FROM enrolment_select e
        INNER JOIN user_computed u ON e."userID" = u."userID" AND u."userStatus" = 1
        INNER JOIN content_computed c ON e."courseID" = c."courseID"
          AND c."category" IN {content_filter} AND c."courseStatus" IN ('Live','Retired')
        WHERE e."dbCompletionStatus" = 2
    """).fetchone()[0]
    external_complete = con.execute("SELECT COUNT(*) FROM external_enrolment_computed WHERE status = 2").fetchone()[0]
    Redis.update("overall_course_completion", str(platform_complete + external_complete), conf=config)

    # Yesterday completions
    yday_platform_complete = con.execute(f"""
        SELECT COUNT(*) FROM enrolment_select e
        INNER JOIN user_computed u ON e."userID" = u."userID" AND u."userStatus" = 1
        INNER JOIN content_computed c ON e."courseID" = c."courseID"
          AND c."category" IN {content_filter} AND c."courseStatus" IN ('Live','Retired')
        WHERE e."dbCompletionStatus" = 2
          AND TRY_CAST(e."courseCompletedTimestamp" AS TIMESTAMP)
              BETWEEN TIMESTAMP '{prev_start}' AND TIMESTAMP '{prev_end}'
    """).fetchone()[0]
    yday_external_complete = con.execute(f"""
        SELECT COUNT(*) FROM external_enrolment_computed
        WHERE status = 2
          AND TRY_CAST(completedon AS TIMESTAMP)
              BETWEEN TIMESTAMP '{prev_start}' AND TIMESTAMP '{prev_end}'
    """).fetchone()[0]
    Redis.update("yerterday_course_completion", str(yday_platform_complete + yday_external_complete), conf=config)

    # ── Registered users ─────────────────────────────────────────
    Redis.update("overall_registered_users", str(active_users), conf=config)

    yday_registrations = con.execute(f"""
        SELECT COUNT(*) FROM user_computed
        WHERE "userStatus" = 1
          AND TRY_CAST("userCreatedDate" AS TIMESTAMP)
              BETWEEN TIMESTAMP '{prev_start}' AND CURRENT_DATE
    """).fetchone()[0]
    Redis.update("users_registered_yersterday", str(yday_registrations), conf=config)

    con.close()
    log.info("[SUCCESS] DSRComputationUpdated metrics updated")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] DSRComputationUpdated at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] DSRComputationUpdated — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
