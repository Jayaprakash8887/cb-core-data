"""
ACBP Report — DuckDB Migration
Reads from initialized DuckDB, produces:
  - CBP Enrollment CSV per MDO (CBPEnrollmentReport.csv)
  - CBP User Summary CSV per MDO (CBPUserSummaryReport.csv)
  - Warehouse parquet (cb_plan, apar_cbp_enrollment)
"""
import os
import sys
import logging
from pathlib import Path
from datetime import datetime

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH
from duckdb_jobs.core.export_utils import (
    write_csv_per_mdo, write_warehouse_parquet,
    duration_format_expr, MDO_HIERARCHY_COLUMNS, CURRENT_DATETIME_EXPR,
)

log = logging.getLogger("acbp-report")


def process_data(config, db_path=None):
    today = datetime.now().strftime("%Y-%m-%d")
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    # ── CB-Plan warehouse ────────────────────────────────────────
    # acbp_select has: acbpOrgID, acbpCourseIDList, acbpCreatedBy,
    #   assignmentTypeInfo, completionDueDate, acbpStatus, acbpCreatedDate,
    #   acbpUpdatedDate, isapar, ...
    cb_plan_sql = f"""
    SELECT
        a."acbpOrgID"            AS organisation_id,
        a."acbpCreatedBy"        AS created_by,
        CASE
            WHEN json_extract_string(at_info.val, '$.assignmentType') = 'rootorgid'
            THEN json_extract_string(at_info.val, '$.assignmentTypeValue')
            ELSE json_extract_string(at_info.val, '$.assignmentType')
        END                      AS allotment_type,
        json_extract_string(at_info.val, '$.assignmentTypeValue') AS allotment_to,
        c.val                    AS content_id,
        a."completionDueDate"    AS completion_due_date,
        a."acbpStatus"           AS status,
        a."acbpCreatedDate"      AS created_on,
        a."acbpUpdatedDate"      AS updated_on,
        a."isapar"               AS is_apar,
        {CURRENT_DATETIME_EXPR}  AS data_last_generated_on
    FROM acbp_select a,
    LATERAL (SELECT UNNEST(CAST(a."acbpCourseIDList" AS VARCHAR[])) AS val) c,
    LATERAL (SELECT UNNEST(CAST(a."assignmentTypeInfo" AS JSON[])) AS val) at_info
    WHERE a."acbpCourseIDList" IS NOT NULL
    """
    write_warehouse_parquet(con, cb_plan_sql,
                           f"{config.warehouseReportDir}/{config.dwCBPlanTable}")

    # ── Enrollment report: join ACBP → content → enrolment → user ─
    # Live ACBP with course IDs exploded, deduplicate on user + course
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _acbp_courses AS
        SELECT DISTINCT
            a."acbpOrgID"        AS acbp_org_id,
            c.val                AS acbp_content_id,
            a."completionDueDate" AS mandate_due_date,
            a."isapar"
        FROM acbp_select a,
        LATERAL (SELECT UNNEST(CAST(a."acbpCourseIDList" AS VARCHAR[])) AS val) c
        WHERE a."acbpStatus" = 'Live'
          AND a."acbpCourseIDList" IS NOT NULL
    """)

    enrolment_report_sql = f"""
    SELECT
        uo."fullName"                AS "Full_Name",
        uo."email"                   AS "Email",
        uo."professionalDetails_designation" AS "Designation",
        {MDO_HIERARCHY_COLUMNS},
        cc."courseName"              AS "CBP_Name",
        cc."courseOrgName"            AS "Content_Provider",
        cc."category"                AS "CBP_Type",
        ec."dbCompletionStatus"      AS "Status_Code",
        CASE
            WHEN ec."dbCompletionStatus" = 2 THEN 'Completed'
            WHEN ec."dbCompletionStatus" = 1 THEN 'In Progress'
            WHEN ec."dbCompletionStatus" = 0 THEN 'Not Started'
            ELSE 'Enrolled'
        END                          AS "Completion_Status",
        ec."completionPercentage"    AS "Completion_Percentage",
        ac.mandate_due_date          AS "Mandate_Due_Date",
        ec."courseEnrolledTimestamp"  AS "Enrolled_On",
        ec."courseCompletedTimestamp" AS "Completed_On",
        CASE WHEN ec."issuedCertificates" IS NOT NULL
             AND ec."issuedCertificates" != '[]' THEN 'Yes' ELSE 'No'
        END                          AS "Certificate_Generated",
        uo."userOrgID"               AS mdoid
    FROM user_org_computed uo
    INNER JOIN _acbp_courses ac
        ON uo."userOrgID" = ac.acbp_org_id
    LEFT JOIN enrolment_computed ec
        ON uo."userID" = ec."userID" AND ac.acbp_content_id = ec."courseID"
    LEFT JOIN content_computed cc
        ON ac.acbp_content_id = cc."courseID"
    LEFT JOIN org_hierarchy_select oh
        ON uo."userOrgID" = oh.mdo_id
    WHERE cc."courseStatus" IN ('Live', 'Retired')
    """

    report_path = f"{config.localReportDir}/{config.acbpReportPath}/{today}"
    os.makedirs(report_path, exist_ok=True)
    write_csv_per_mdo(con, enrolment_report_sql, report_path, "mdoid",
                      config.cbpEnrolmentReport)

    # ── User summary: count allocated, completed, completed-before-due ──
    summary_sql = f"""
    SELECT
        uo."fullName"                         AS "Full_Name",
        uo."email"                            AS "Email",
        uo."professionalDetails_designation"  AS "Designation",
        {MDO_HIERARCHY_COLUMNS},
        COUNT(DISTINCT ac.acbp_content_id)    AS "Allocated_CBPs",
        COUNT(DISTINCT CASE WHEN ec."dbCompletionStatus" = 2
            THEN ac.acbp_content_id END)      AS "Completed_CBPs",
        COUNT(DISTINCT CASE WHEN ec."dbCompletionStatus" = 2
            AND ec."courseCompletedTimestamp" <= ac.mandate_due_date
            THEN ac.acbp_content_id END)      AS "Completed_Before_Due",
        uo."userOrgID"                        AS mdoid
    FROM user_org_computed uo
    INNER JOIN _acbp_courses ac
        ON uo."userOrgID" = ac.acbp_org_id
    LEFT JOIN enrolment_computed ec
        ON uo."userID" = ec."userID" AND ac.acbp_content_id = ec."courseID"
    LEFT JOIN content_computed cc
        ON ac.acbp_content_id = cc."courseID"
    LEFT JOIN org_hierarchy_select oh
        ON uo."userOrgID" = oh.mdo_id
    WHERE cc."courseStatus" IN ('Live', 'Retired')
    GROUP BY uo."fullName", uo."email", uo."professionalDetails_designation",
             oh.ministry, oh.department, oh.mdo_name,
             uo."userOrgID"
    """
    write_csv_per_mdo(con, summary_sql, report_path, "mdoid",
                      config.cbpSummaryReport)

    # ── APAR CBP Enrollment warehouse ─────────────────────────────
    apar_sql = f"""
    SELECT
        uo."userID"                  AS user_id,
        uo."fullName"                AS full_name,
        uo."email"                   AS email,
        uo."userOrgID"               AS mdo_id,
        cc."courseID"                 AS content_id,
        cc."courseName"              AS content_name,
        cc."category"                AS content_type,
        ec."dbCompletionStatus"      AS completion_status,
        ec."completionPercentage"    AS completion_percentage,
        ec."courseCompletedTimestamp" AS completed_on,
        ac.mandate_due_date          AS mandate_due_date,
        {CURRENT_DATETIME_EXPR}      AS data_last_generated_on
    FROM user_org_computed uo
    INNER JOIN _acbp_courses ac
        ON uo."userOrgID" = ac.acbp_org_id
        AND ac.isapar = 'true'
    LEFT JOIN enrolment_computed ec
        ON uo."userID" = ec."userID" AND ac.acbp_content_id = ec."courseID"
    LEFT JOIN content_computed cc
        ON ac.acbp_content_id = cc."courseID"
    """
    write_warehouse_parquet(con, apar_sql,
                           f"{config.warehouseReportDir}/{config.dwAparCBPEnrollmentTable}")

    con.close()
    log.info("ACBPReport — done")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] ACBPReport at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] ACBPReport — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
