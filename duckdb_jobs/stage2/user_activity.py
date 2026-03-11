"""
User Activity Report — DuckDB Migration
Combines content enrolments and event enrolments into a unified
user_activity warehouse parquet.
"""
import os
import sys
import logging
from pathlib import Path
from datetime import datetime

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH
from duckdb_jobs.core.export_utils import write_warehouse_parquet, CURRENT_DATETIME_EXPR

log = logging.getLogger("user-activity")


def process_data(config, db_path=None):
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    # ── Content enrolments ───────────────────────────────────────
    # Uses enrolment_warehouse_computed + content_computed
    content_sql = """
    SELECT
        ew."userID"            AS user_id,
        ew."batchID"           AS batch_id,
        ew."courseID"           AS content_id,
        cc."category"          AS content_type,
        ew."enrolled_on"       AS enrolled_on,
        ew."completionPercentage" AS progress,
        CASE WHEN ew."user_consumption_status" = 'completed' THEN 'Yes' ELSE 'No' END AS certificate_generated,
        ew."certificateID"     AS certificate_id,
        ew."first_completed_on" AS completed_on,
        COALESCE(TRY_CAST(ew."userRating" AS DOUBLE), 0) AS user_rating,
        ew."resource_count_consumed" AS resource_count_consumed,
        'content'              AS activity_type
    FROM enrolment_warehouse_computed ew
    LEFT JOIN content_computed cc ON ew."courseID" = cc."courseID"
    """

    # ── Event enrolments ─────────────────────────────────────────
    event_sql = """
    SELECT
        wee.user_id            AS user_id,
        'NA'                   AS batch_id,
        wee.event_id           AS content_id,
        'Event'                AS content_type,
        wee.enrolled_on_datetime AS enrolled_on,
        CASE WHEN wee.status = 'completed' THEN 100 ELSE 0 END AS progress,
        CASE WHEN wee.certificate_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS certificate_generated,
        wee.certificate_id     AS certificate_id,
        wee.completed_on_datetime AS completed_on,
        0                      AS user_rating,
        1                      AS resource_count_consumed,
        'event'                AS activity_type
    FROM warehouse_event_enrolments wee
    """

    # ── Union both into warehouse ────────────────────────────────
    combined_sql = f"""
    SELECT *, {CURRENT_DATETIME_EXPR} AS data_last_generated_on
    FROM (
        {content_sql}
        UNION ALL
        {event_sql}
    ) combined
    """

    write_warehouse_parquet(con, combined_sql,
                           f"{config.warehouseReportDir}/{config.dwUserActivityTable}")

    con.close()
    log.info("UserActivityReport — done")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] UserActivityReport at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] UserActivityReport — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
