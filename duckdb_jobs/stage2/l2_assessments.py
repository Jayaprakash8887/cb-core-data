"""
L2 Assessment Report — DuckDB Migration
Combines APAR consumption and CAP assessment data into a unified
L2 assessment warehouse parquet.
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

log = logging.getLogger("l2-assessments")


def process_data(config, db_path=None):
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    # ── Part 1: APAR Consumption ─────────────────────────────────
    # ACBP plans where isapar='true', exploded to courses,
    # joined with enrolment (in-progress/completed), content, user, KCM
    apar_sql = """
    SELECT
        uo."userID"                  AS user_id,
        uo."fullName"                AS full_name,
        uo."email"                   AS email,
        uo."userOrgID"               AS mdo_id,
        uo."userOrgName"             AS mdo_name,
        cc."courseID"                 AS content_id,
        cc."courseName"              AS content_name,
        cc."category"                AS content_type,
        cc."courseOrgName"            AS content_provider,
        ec."dbCompletionStatus"      AS completion_status_code,
        CASE
            WHEN ec."dbCompletionStatus" = 2 THEN 'completed'
            WHEN ec."dbCompletionStatus" = 1 THEN 'in-progress'
            ELSE 'not-started'
        END                          AS consumption_status,
        ec."completionPercentage"    AS completion_percentage,
        ec."courseCompletedTimestamp" AS completed_on,
        a."completionDueDate"        AS mandate_due_date,
        a."acbpOrgID"                AS cbp_org_id,
        'true'                       AS is_apar,
        'APAR'                       AS source
    FROM acbp_select a,
    LATERAL (SELECT UNNEST(CAST(a."acbpCourseIDList" AS VARCHAR[])) AS course_id) courses
    INNER JOIN content_computed cc ON courses.course_id = cc."courseID"
    LEFT  JOIN enrolment_computed ec ON courses.course_id = ec."courseID"
    LEFT  JOIN user_org_computed uo  ON ec."userID" = uo."userID"
    WHERE a."isapar" = 'true'
      AND a."acbpStatus" = 'Live'
      AND a."acbpCourseIDList" IS NOT NULL
      AND ec."dbCompletionStatus" IN (1, 2)
    """

    # ── Part 2: CAP Assessment ───────────────────────────────────
    # Content with sub-type 'Comprehensive Assessment Program', Live status
    # joined with enrolments + assessment details + user
    cap_sql = """
    SELECT
        uo."userID"                  AS user_id,
        uo."fullName"                AS full_name,
        uo."email"                   AS email,
        uo."userOrgID"               AS mdo_id,
        uo."userOrgName"             AS mdo_name,
        cc."courseID"                 AS content_id,
        cc."courseName"              AS content_name,
        cc."category"                AS content_type,
        cc."courseOrgName"            AS content_provider,
        ec."dbCompletionStatus"      AS completion_status_code,
        CASE
            WHEN ec."dbCompletionStatus" = 2 THEN 'completed'
            WHEN ec."dbCompletionStatus" = 1 THEN 'in-progress'
            ELSE 'not-started'
        END                          AS consumption_status,
        ec."completionPercentage"    AS completion_percentage,
        ec."courseCompletedTimestamp" AS completed_on,
        NULL                         AS mandate_due_date,
        NULL                         AS cbp_org_id,
        'false'                      AS is_apar,
        'CAP'                        AS source
    FROM content_computed cc
    INNER JOIN enrolment_computed ec ON cc."courseID" = ec."courseID"
    LEFT  JOIN user_org_computed uo  ON ec."userID" = uo."userID"
    WHERE cc."courseStatus" = 'Live'
      AND cc."additionalTags" LIKE '%Comprehensive Assessment Program%'
      AND ec."dbCompletionStatus" IN (1, 2)
    """

    # ── Union and deduplicate ────────────────────────────────────
    combined_sql = f"""
    SELECT DISTINCT ON (user_id, content_id) *
    FROM (
        {apar_sql}
        UNION ALL
        {cap_sql}
    ) t
    ORDER BY user_id, content_id, completed_on DESC NULLS LAST
    """

    report_path = getattr(config, 'l2AssessmentReportPath',
                          f"{config.warehouseReportDir}/l2_assessment_report")
    write_warehouse_parquet(con, combined_sql, report_path)

    con.close()
    log.info("L2AssessmentReport — done")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] L2AssessmentReport at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] L2AssessmentReport — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
