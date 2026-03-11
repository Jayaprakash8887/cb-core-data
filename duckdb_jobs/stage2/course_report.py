"""
Course Report — DuckDB Migration
Reads from initialized DuckDB, produces:
  - CSV per MDO (ContentReport.csv)
  - Warehouse parquet (content, content_resource)
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

log = logging.getLogger("course-report")


def process_data(config, db_path=None):
    today = datetime.now().strftime("%Y-%m-%d")
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    # ── Content Resource warehouse ───────────────────────────────
    resource_sql = """
    WITH hier AS (
        SELECT
            ch.identifier AS "courseID",
            cp."category",
            ch.hierarchy
        FROM content_hierarchy_select ch
        INNER JOIN all_course_program_computed cp ON ch.identifier = cp."courseID"
    ),
    exploded AS (
        SELECT
            h."courseID" AS content_id,
            h."category",
            child.*
        FROM hier h,
        LATERAL (
            SELECT
                UNNEST(json_extract(h.hierarchy, '$.children')) AS first_level
        ) fl,
        LATERAL (
            SELECT
                CASE
                    WHEN h."category" IN ('Program','Curated Program')
                        THEN json_extract_string(fl.first_level, '$.identifier')
                    WHEN json_extract_string(fl.first_level, '$.primaryCategory') = 'Course Unit'
                        THEN json_extract_string(sl.second_level, '$.identifier')
                    ELSE json_extract_string(fl.first_level, '$.identifier')
                END AS resource_id,
                CASE
                    WHEN h."category" IN ('Program','Curated Program')
                        THEN json_extract_string(fl.first_level, '$.name')
                    WHEN json_extract_string(fl.first_level, '$.primaryCategory') = 'Course Unit'
                        THEN json_extract_string(sl.second_level, '$.name')
                    ELSE json_extract_string(fl.first_level, '$.name')
                END AS resource_name,
                CASE
                    WHEN h."category" IN ('Program','Curated Program')
                        THEN json_extract_string(fl.first_level, '$.primaryCategory')
                    WHEN json_extract_string(fl.first_level, '$.primaryCategory') = 'Course Unit'
                        THEN json_extract_string(sl.second_level, '$.primaryCategory')
                    ELSE json_extract_string(fl.first_level, '$.primaryCategory')
                END AS resource_type,
                CASE
                    WHEN h."category" IN ('Program','Curated Program')
                        THEN TRY_CAST(json_extract_string(fl.first_level, '$.duration') AS DOUBLE)
                    WHEN json_extract_string(fl.first_level, '$.primaryCategory') = 'Course Unit'
                        THEN COALESCE(
                            TRY_CAST(json_extract_string(sl.second_level, '$.duration') AS DOUBLE),
                            TRY_CAST(json_extract_string(sl.second_level, '$.expectedDuration') AS DOUBLE))
                    ELSE COALESCE(
                        TRY_CAST(json_extract_string(fl.first_level, '$.duration') AS DOUBLE),
                        TRY_CAST(json_extract_string(fl.first_level, '$.expectedDuration') AS DOUBLE))
                END AS resource_duration
            FROM (SELECT UNNEST(
                COALESCE(json_extract(fl.first_level, '$.children'), '[]'::JSON)
            ) AS second_level) sl
        ) child
        WHERE child.resource_id IS NOT NULL AND child.resource_id <> ''
    )
    SELECT DISTINCT
        content_id,
        resource_id,
        resource_name,
        resource_type,
        CASE WHEN resource_duration IS NULL OR resource_duration = 0 THEN ''
             ELSE LPAD(CAST(CAST(resource_duration/3600 AS INT) AS VARCHAR),2,'0') || ':' ||
                  LPAD(CAST(CAST(resource_duration%3600/60 AS INT) AS VARCHAR),2,'0') || ':' ||
                  LPAD(CAST(CAST(resource_duration%60 AS INT) AS VARCHAR),2,'0')
        END AS resource_duration,
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS data_last_generated_on
    FROM exploded
    """
    write_warehouse_parquet(con, resource_sql,
                           f"{config.warehouseReportDir}/{config.dwContentResourceTable}")

    # ── Aggregated enrolment stats per course ────────────────────
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _course_agg AS
        SELECT
            e."courseID",
            MIN(e."courseCompletedTimestamp")   AS "earliestCourseCompleted",
            MAX(e."courseCompletedTimestamp")   AS "latestCourseCompleted",
            COUNT(*)                           AS "enrolledUserCount",
            SUM(CASE WHEN CASE WHEN e."dbCompletionStatus" IS NULL THEN 'not-enrolled'
                          WHEN e."dbCompletionStatus" = 0 THEN 'not-started'
                          WHEN e."dbCompletionStatus" = 1 THEN 'in-progress'
                          ELSE 'completed' END = 'in-progress' THEN 1 ELSE 0 END)  AS "inProgressCount",
            SUM(CASE WHEN CASE WHEN e."dbCompletionStatus" IS NULL THEN 'not-enrolled'
                          WHEN e."dbCompletionStatus" = 0 THEN 'not-started'
                          WHEN e."dbCompletionStatus" = 1 THEN 'in-progress'
                          ELSE 'completed' END = 'not-started' THEN 1 ELSE 0 END)  AS "notStartedCount",
            SUM(CASE WHEN CASE WHEN e."dbCompletionStatus" IS NULL THEN 'not-enrolled'
                          WHEN e."dbCompletionStatus" = 0 THEN 'not-started'
                          WHEN e."dbCompletionStatus" = 1 THEN 'in-progress'
                          ELSE 'completed' END = 'completed' THEN 1 ELSE 0 END)    AS "completedCount",
            SUM(e."issuedCertificateCountPerContent") AS "totalCertificatesIssued"
        FROM enrolment_computed e
        INNER JOIN content_computed c ON e."courseID" = c."courseID"
        WHERE c."category" IN ('Course','Program','Blended Program','CuratedCollections','Curated Program')
        GROUP BY e."courseID"
    """)

    # ── SCORM detection ──────────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _scorm AS
        SELECT
            ch.identifier AS "courseID",
            GREATEST(
                MAX(CASE WHEN json_extract_string(fl.child, '$.mimeType') LIKE '%html-archive' THEN 1 ELSE 0 END),
                MAX(CASE WHEN json_extract_string(sl.child, '$.mimeType') LIKE '%html-archive' THEN 1 ELSE 0 END)
            ) AS scorm_flag
        FROM content_hierarchy_select ch,
        LATERAL (SELECT UNNEST(json_extract(ch.hierarchy, '$.children')) AS child) fl,
        LATERAL (SELECT UNNEST(COALESCE(json_extract(fl.child, '$.children'), '[]'::JSON)) AS child) sl
        GROUP BY ch.identifier
    """)

    # ── Platform content MDO report ──────────────────────────────
    platform_mdo_sql = f"""
    SELECT
        cc."courseStatus"     AS "Content_Status",
        CASE WHEN cc."courseOrgName" IS NOT NULL AND TRIM(cc."courseOrgName") <> ''
             THEN cc."courseOrgName" ELSE cc."contentCreator" END AS "Content_Provider",
        cc."courseName"       AS "Content_Name",
        cc."category"         AS "Content_Type",
        b."batchID"           AS "Batch_Id",
        b."courseBatchName"   AS "Batch_Name",
        TRY_CAST(b."courseBatchStartDate" AS DATE)  AS "Batch_Start_Date",
        TRY_CAST(b."courseBatchEndDate" AS DATE)    AS "Batch_End_Date",
        {duration_format_expr('courseDuration', 'Content_Duration')},
        a."enrolledUserCount"     AS "Enrolled",
        a."notStartedCount"       AS "Not_Started",
        a."inProgressCount"       AS "In_Progress",
        a."completedCount"        AS "Completed",
        cc."rating"               AS "Content_Rating",
        TRY_CAST(cc."courseLastPublishedOn" AS DATE) AS "Last_Published_On",
        TRY_CAST(a."earliestCourseCompleted" AS DATE) AS "First_Completed_On",
        TRY_CAST(a."latestCourseCompleted" AS DATE)   AS "Last_Completed_On",
        CASE WHEN cc."courseStatus" = 'Retired'
             THEN TRY_CAST(cc."lastStatusChangedOn" AS DATE) END AS "Content_Retired_On",
        a."totalCertificatesIssued" AS "Total_Certificates_Issued",
        cc."courseOrgID"          AS "mdoid",
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS "Report_Last_Generated_On"
    FROM content_computed cc
    LEFT JOIN _course_agg a ON cc."courseID" = a."courseID"
    LEFT JOIN batch_select b ON cc."courseID" = b."courseID"
        AND cc."category" = 'Blended Program'
    WHERE cc."courseStatus" IN ('Live','Draft','Retired','Review')
      AND cc."category" IN ('Course','Program','Blended Program','CuratedCollections','Curated Program')
    """

    # ── Marketplace content MDO report ───────────────────────────
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _mkt_agg AS
        SELECT
            "content_id",
            COUNT(*)                                    AS "enrolledUserCount",
            SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS "inProgressCount",
            SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS "notStartedCount",
            SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS "completedCount",
            SUM(CASE WHEN LIST_LENGTH(issued_certificates) > 0 THEN 1 ELSE 0 END)
                                                        AS "totalCertificatesIssued",
            MIN(completedon) AS "earliestCompletedOn",
            MAX(completedon) AS "latestCompletedOn"
        FROM external_enrolment_computed
        GROUP BY "content_id"
    """)

    marketplace_mdo_sql = """
    SELECT
        ec."courseStatus"     AS "Content_Status",
        ec."courseOrgName"    AS "Content_Provider",
        ec."courseName"      AS "Content_Name",
        ec."category"        AS "Content_Type",
        'Not Available'      AS "Batch_Id",
        'Not Available'      AS "Batch_Name",
        NULL::DATE           AS "Batch_Start_Date",
        NULL::DATE           AS "Batch_End_Date",
        ec."courseDuration"  AS "Content_Duration",
        a."enrolledUserCount"     AS "Enrolled",
        a."notStartedCount"       AS "Not_Started",
        a."inProgressCount"       AS "In_Progress",
        a."completedCount"        AS "Completed",
        NULL::DOUBLE         AS "Content_Rating",
        TRY_CAST(ec."courseLastPublishedOn" AS DATE)  AS "Last_Published_On",
        TRY_CAST(a."earliestCompletedOn" AS DATE)     AS "First_Completed_On",
        TRY_CAST(a."latestCompletedOn" AS DATE)       AS "Last_Completed_On",
        NULL::DATE           AS "Content_Retired_On",
        a."totalCertificatesIssued" AS "Total_Certificates_Issued",
        ec."courseOrgID"     AS "mdoid",
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS "Report_Last_Generated_On"
    FROM external_content_computed ec
    LEFT JOIN _mkt_agg a ON ec."content_id" = a."content_id"
    """

    # ── Combined MDO report → CSV per org ────────────────────────
    combined_mdo_sql = f"({platform_mdo_sql}) UNION ALL ({marketplace_mdo_sql})"
    report_path = f"{config.localReportDir}/{config.courseReportPath}/{today}"
    write_csv_per_mdo(con, combined_mdo_sql, report_path, "mdoid",
                      csv_filename=config.courseReport)

    # ── Content warehouse (platform + marketplace) → Parquet ─────
    platform_wh_sql = f"""
    SELECT
        cc."courseID"          AS content_id,
        cc."courseOrgID"       AS content_provider_id,
        CASE WHEN cc."courseOrgName" IS NOT NULL AND TRIM(cc."courseOrgName") <> ''
             THEN cc."courseOrgName" ELSE cc."contentCreator" END AS content_provider_name,
        cc."courseName"        AS content_name,
        cc."category"          AS content_type,
        b."batchID"            AS batch_id,
        b."courseBatchName"    AS batch_name,
        TRY_CAST(b."courseBatchStartDate" AS DATE) AS batch_start_date,
        TRY_CAST(b."courseBatchEndDate" AS DATE)   AS batch_end_date,
        {duration_format_expr('courseDuration', 'content_duration')},
        cc."rating"            AS content_rating,
        TRY_CAST(cc."courseLastPublishedOn" AS DATE) AS last_published_on,
        CASE WHEN cc."courseStatus" = 'Retired'
             THEN TRY_CAST(cc."lastStatusChangedOn" AS DATE) END AS content_retired_on,
        cc."courseStatus"      AS content_status,
        cc."courseResourceCount" AS resource_count,
        a."totalCertificatesIssued" AS total_certificates_issued,
        cc."courseReviewStatus" AS content_substatus,
        cc."contentLanguage"   AS language,
        cc."courseCategory"    AS content_sub_type,
        COALESCE(s.scorm_flag, 0) AS scorm_flag,
        cc."difficultyLevel"   AS difficulty_level,
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS data_last_generated_on
    FROM content_computed cc
    LEFT JOIN _course_agg a ON cc."courseID" = a."courseID"
    LEFT JOIN batch_select b ON cc."courseID" = b."courseID"
        AND cc."category" = 'Blended Program'
    LEFT JOIN _scorm s ON cc."courseID" = s."courseID"
    WHERE cc."courseStatus" IN ('Live','Draft','Retired','Review')
      AND cc."category" IN ('Course','Program','Blended Program','CuratedCollections','Curated Program')
    """

    marketplace_wh_sql = """
    SELECT
        ec."content_id",
        ec."courseOrgID"       AS content_provider_id,
        ec."courseOrgName"     AS content_provider_name,
        ec."courseName"       AS content_name,
        ec."category"         AS content_type,
        'Not Available'       AS batch_id,
        'Not Available'       AS batch_name,
        NULL::DATE            AS batch_start_date,
        NULL::DATE            AS batch_end_date,
        ec."courseDuration"   AS content_duration,
        'Not Available'       AS content_rating,
        TRY_CAST(ec."courseLastPublishedOn" AS DATE) AS last_published_on,
        NULL::DATE            AS content_retired_on,
        ec."courseStatus"     AS content_status,
        'Not Available'       AS resource_count,
        a."totalCertificatesIssued" AS total_certificates_issued,
        'Not Available'       AS content_substatus,
        'Not Available'       AS language,
        'External Content'    AS content_sub_type,
        0                     AS scorm_flag,
        NULL                  AS difficulty_level,
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS data_last_generated_on
    FROM external_content_computed ec
    LEFT JOIN _mkt_agg a ON ec."content_id" = a."content_id"
    """

    warehouse_sql = f"({platform_wh_sql}) UNION ALL ({marketplace_wh_sql})"
    write_warehouse_parquet(con, warehouse_sql,
                           f"{config.warehouseReportDir}/{config.dwCourseTable}")

    con.close()
    log.info("CourseReport — done")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] CourseReport at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] CourseReport — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
