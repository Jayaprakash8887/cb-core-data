"""
User Enrolment Report — DuckDB Migration
Reads from initialized DuckDB, produces:
  - CSV per MDO (ConsumptionReport.csv)
  - Warehouse parquet (user_enrolments)
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
    write_csv_per_mdo, write_warehouse_parquet, duration_format_expr,
    MDO_HIERARCHY_COLUMNS, CURRENT_DATETIME_EXPR,
)

log = logging.getLogger("user-enrolment")

# Completion status SQL case expression (reused)
COMPLETION_STATUS = """
CASE WHEN e."dbCompletionStatus" IS NULL THEN 'not-enrolled'
     WHEN e."dbCompletionStatus" = 0 THEN 'not-started'
     WHEN e."dbCompletionStatus" = 1 THEN 'in-progress'
     ELSE 'completed' END
"""

COMPLETION_PERCENT = """
CASE
    WHEN c."courseResourceCount" = 0 OR e."courseProgress" = 0 OR e."dbCompletionStatus" = 0 THEN 0.0
    WHEN e."dbCompletionStatus" = 2 THEN 100.0
    ELSE LEAST(100.0, GREATEST(0.0, ROUND(100.0 * e."courseProgress" / c."courseResourceCount", 2)))
END
"""


def process_data(config, db_path=None):
    today = datetime.now().strftime("%Y-%m-%d")
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    # ── ACBP plan mandate per (user, org, course) ────────────────
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _acbp_mandate AS
        SELECT DISTINCT
            "userID", "userOrgID", TRIM(UNNEST(string_split("acbpCourseIDList", ','))) AS "courseID",
            TRUE AS live_cbp_plan_mandate
        FROM acbp_select
        WHERE "acbpStatus" = 'Live'
          AND "acbpCourseIDList" IS NOT NULL
    """)

    # ── Platform enrolment + user + content + ACBP ───────────────
    platform_mdo_sql = f"""
    SELECT
        u."fullName"           AS "Full_Name",
        u."designation"        AS "Designation",
        u."userPrimaryEmail"   AS "Email",
        u."userMobile"         AS "Phone_Number",
        {MDO_HIERARCHY_COLUMNS},
        u."group"              AS "Group",
        u."Tag",
        u."cadreName"          AS "Cadre",
        u."civilServiceType"   AS "Civil Service Type",
        u."civilServiceName"   AS "Civil Services",
        u."cadreBatch"         AS "Cadre Batch",
        u."organised_service"  AS "Is From Organised Service of Govt",
        CASE WHEN c."courseOrgName" IS NOT NULL AND TRIM(c."courseOrgName") <> ''
             THEN c."courseOrgName" ELSE c."contentCreator" END AS "Content_Provider",
        c."courseName"         AS "Content_Name",
        c."category"           AS "Content_Type",
        {duration_format_expr('courseDuration', 'Content_Duration')},
        e."batchID"            AS "Batch_Id",
        e."courseBatchName"    AS "Batch_Name",
        TRY_CAST(e."courseBatchStartDate" AS DATE) AS "Batch_Start_Date",
        TRY_CAST(e."courseBatchEndDate" AS DATE)   AS "Batch_End_Date",
        CAST(e."courseEnrolledTimestamp" AS VARCHAR) AS "Enrolled_On",
        {COMPLETION_STATUS}    AS "Status",
        {COMPLETION_PERCENT}   AS "Content_Progress_Percentage",
        TRY_CAST(c."courseLastPublishedOn" AS DATE) AS "Last_Published_On",
        CASE WHEN c."courseStatus" = 'Retired'
             THEN TRY_CAST(c."lastStatusChangedOn" AS DATE) END AS "Content_Retired_On",
        CAST(e."courseCompletedTimestamp" AS VARCHAR) AS "Completed_On",
        CASE WHEN e."issuedCertificateCount" > 0 THEN 'Yes' ELSE 'No' END AS "Certificate_Generated",
        e."userRating"         AS "User_Rating",
        u."userGender"         AS "Gender",
        u."userCategory"       AS "Category",
        json_extract_string(u."additionalProperties", '$.externalSystem') AS "External_System",
        json_extract_string(u."additionalProperties", '$.externalSystemId') AS "External_System_Id",
        u."userOrgID"          AS "mdoid",
        e."certificateID"      AS "Certificate_ID",
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS "Report_Last_Generated_On",
        COALESCE(a.live_cbp_plan_mandate, FALSE) AS "Live_CBP_Plan_Mandate"
    FROM enrolment_computed e
    INNER JOIN content_computed c ON e."courseID" = c."courseID"
    INNER JOIN user_org_computed u ON e."userID" = u."userID"
    LEFT JOIN _acbp_mandate a ON e."userID" = a."userID"
        AND u."userOrgID" = a."userOrgID" AND e."courseID" = a."courseID"
    WHERE c."category" IN ('Course','Program','Blended Program','CuratedCollections','Curated Program')
      AND CAST(u."userStatus" AS INT) = 1
    """

    # ── Marketplace enrolment MDO report ─────────────────────────
    marketplace_mdo_sql = f"""
    SELECT
        u."fullName"               AS "Full_Name",
        u."designation"            AS "Designation",
        u."userPrimaryEmail"       AS "Email",
        u."userMobile"             AS "Phone_Number",
        {MDO_HIERARCHY_COLUMNS},
        u."group"                  AS "Group",
        u."Tag",
        u."cadreName"              AS "Cadre",
        u."civilServiceType"       AS "Civil Service Type",
        u."civilServiceName"       AS "Civil Services",
        u."cadreBatch"             AS "Cadre Batch",
        u."organised_service"      AS "Is From Organised Service of Govt",
        ec."courseOrgName"         AS "Content_Provider",
        ec."courseName"            AS "Content_Name",
        ec."category"              AS "Content_Type",
        ec."courseDuration"        AS "Content_Duration",
        'Not Available'            AS "Batch_Id",
        'Not Available'            AS "Batch_Name",
        NULL::DATE                 AS "Batch_Start_Date",
        NULL::DATE                 AS "Batch_End_Date",
        CAST(ext.enrolled_date AS VARCHAR) AS "Enrolled_On",
        CASE WHEN ext.status IS NULL THEN 'not-enrolled'
             WHEN ext.status = 0 THEN 'not-started'
             WHEN ext.status = 1 THEN 'in-progress'
             ELSE 'completed' END  AS "Status",
        ext."completionPercentage" AS "Content_Progress_Percentage",
        TRY_CAST(ec."courseLastPublishedOn" AS DATE) AS "Last_Published_On",
        NULL::DATE                 AS "Content_Retired_On",
        CAST(ext.completedon AS VARCHAR) AS "Completed_On",
        CASE WHEN ext.issued_certificates IS NOT NULL AND LIST_LENGTH(ext.issued_certificates) > 0
             THEN 'Yes' ELSE 'No' END AS "Certificate_Generated",
        'Not Available'            AS "User_Rating",
        u."userGender"             AS "Gender",
        'External Content'         AS "Category",
        json_extract_string(u."additionalProperties", '$.externalSystem') AS "External_System",
        json_extract_string(u."additionalProperties", '$.externalSystemId') AS "External_System_Id",
        u."userOrgID"              AS "mdoid",
        CASE WHEN ext.issued_certificates IS NOT NULL AND LIST_LENGTH(ext.issued_certificates) > 0
             THEN ext.issued_certificates[-1].identifier ELSE '' END AS "Certificate_ID",
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS "Report_Last_Generated_On",
        FALSE                      AS "Live_CBP_Plan_Mandate"
    FROM external_content_computed ec
    INNER JOIN external_enrolment_computed ext ON ec."content_id" = ext."content_id"
    LEFT JOIN user_org_computed u ON ext.userid = u."userID"
    WHERE CAST(u."userStatus" AS INT) = 1
    """

    combined_sql = f"({platform_mdo_sql}) UNION ALL ({marketplace_mdo_sql})"
    report_path = f"{config.localReportDir}/{config.userEnrolmentReportPath}/{today}"
    write_csv_per_mdo(con, combined_sql, report_path, "mdoid",
                      csv_filename=config.userEnrollmentReport)

    # ── Warehouse: platform enrolments ───────────────────────────
    platform_wh_sql = f"""
    SELECT DISTINCT ON (e."userID", e."courseID", e."batchID")
        e."userID"             AS user_id,
        e."batchID"            AS batch_id,
        e."courseID"           AS content_id,
        CAST(e."courseEnrolledTimestamp" AS VARCHAR) AS enrolled_on,
        {COMPLETION_PERCENT}   AS content_progress_percentage,
        e."courseProgress"     AS resource_count_consumed,
        {COMPLETION_STATUS}    AS user_consumption_status,
        CAST(e."firstCompletedOn" AS VARCHAR) AS first_completed_on,
        CAST(e."firstCompletedOn" AS VARCHAR) AS first_certificate_generated_on,
        CAST(e."courseCompletedTimestamp" AS VARCHAR) AS last_completed_on,
        CAST(e."certificateGeneratedOn" AS VARCHAR) AS last_certificate_generated_on,
        CAST(e."lastContentAccessTimestamp" AS VARCHAR) AS content_last_accessed_on,
        CASE WHEN e."issuedCertificateCount" > 0 THEN 'Yes' ELSE 'No' END AS certificate_generated,
        e."issuedCertificateCount" AS number_of_certificate,
        e."userRating"         AS user_rating,
        e."certificateID"      AS certificate_id,
        COALESCE(a.live_cbp_plan_mandate, FALSE) AS live_cbp_plan_mandate,
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS data_last_generated_on,
        COALESCE(e."karma_points", 0) AS karma_points
    FROM enrolment_computed e
    INNER JOIN content_computed c ON e."courseID" = c."courseID"
    LEFT JOIN user_org_computed u ON e."userID" = u."userID"
    LEFT JOIN _acbp_mandate a ON e."userID" = a."userID"
        AND u."userOrgID" = a."userOrgID" AND e."courseID" = a."courseID"
    WHERE c."category" IN ('Course','Program','Blended Program','CuratedCollections','Curated Program')
    """

    marketplace_wh_sql = """
    SELECT DISTINCT ON (ext.userid, ec."content_id", 'Not Available')
        ext.userid              AS user_id,
        'Not Available'         AS batch_id,
        ec."content_id"         AS content_id,
        CAST(ext.enrolled_date AS VARCHAR) AS enrolled_on,
        ext."completionPercentage" AS content_progress_percentage,
        ext.progress            AS resource_count_consumed,
        CASE WHEN ext.status IS NULL THEN 'not-enrolled'
             WHEN ext.status = 0 THEN 'not-started'
             WHEN ext.status = 1 THEN 'in-progress'
             ELSE 'completed' END AS user_consumption_status,
        CASE WHEN ext.issued_certificates IS NOT NULL AND LIST_LENGTH(ext.issued_certificates) > 0
             THEN CAST(ext.issued_certificates[1].lastIssuedOn AS VARCHAR) ELSE '' END AS first_completed_on,
        CASE WHEN ext.issued_certificates IS NOT NULL AND LIST_LENGTH(ext.issued_certificates) > 0
             THEN CAST(ext.issued_certificates[1].lastIssuedOn AS VARCHAR) ELSE '' END AS first_certificate_generated_on,
        CAST(ext.completedon AS VARCHAR) AS last_completed_on,
        CASE WHEN ext.issued_certificates IS NOT NULL AND LIST_LENGTH(ext.issued_certificates) > 0
             THEN CAST(ext.issued_certificates[-1].lastIssuedOn AS VARCHAR) ELSE '' END AS last_certificate_generated_on,
        'Not Available'         AS content_last_accessed_on,
        CASE WHEN ext.issued_certificates IS NOT NULL AND LIST_LENGTH(ext.issued_certificates) > 0
             THEN 'Yes' ELSE 'No' END AS certificate_generated,
        COALESCE(LIST_LENGTH(ext.issued_certificates), 0) AS number_of_certificate,
        'Not Available'         AS user_rating,
        CASE WHEN ext.issued_certificates IS NOT NULL AND LIST_LENGTH(ext.issued_certificates) > 0
             THEN ext.issued_certificates[-1].identifier ELSE '' END AS certificate_id,
        FALSE                   AS live_cbp_plan_mandate,
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS data_last_generated_on,
        0                       AS karma_points
    FROM external_content_computed ec
    INNER JOIN external_enrolment_computed ext ON ec."content_id" = ext."content_id"
    """

    warehouse_sql = f"({platform_wh_sql}) UNION ALL ({marketplace_wh_sql})"
    write_warehouse_parquet(con, warehouse_sql,
                           f"{config.warehouseReportDir}/{config.dwEnrollmentsTable}")

    con.close()
    log.info("UserEnrolment — done")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] UserEnrolment at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] UserEnrolment — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
