"""
Blended Program Report — DuckDB Migration
Reads from initialized DuckDB, produces:
  - CSV per MDO for Blended Programs (CBP + MDO reports)
  - Warehouse parquet (bp_enrolments)
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
    MDO_HIERARCHY_COLUMNS,
)

log = logging.getLogger("blended-report")


def process_data(config, db_path=None):
    today = datetime.now().strftime("%Y-%m-%d")
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    # ── Batch + session data ─────────────────────────────────────
    # Build BP content with hierarchy children as sessions
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _bp_base AS
        SELECT
            cc."courseID" AS "bpID",
            cc."category" AS "bpCategory",
            cc."courseName" AS "bpName",
            cc."courseStatus" AS "bpStatus",
            cc."courseReviewStatus" AS "bpReviewStatus",
            cc."courseChannel" AS "bpChannel",
            cc."courseLastPublishedOn" AS "bpLastPublishedOn",
            cc."courseDuration" AS "bpDuration",
            cc."courseResourceCount" AS "bpResourceCount",
            cc."lastStatusChangedOn" AS "bpLastStatusChangedOn",
            cc."programDirectorName" AS "bpProgramDirectorName",
            cc."courseOrgID" AS "bpOrgID",
            cc."courseOrgName" AS "bpOrgName"
        FROM content_computed cc
        WHERE cc."category" = 'Blended Program'
          AND cc."courseStatus" IN ('Live', 'Retired')
          AND cc."courseLastPublishedOn" IS NOT NULL
    """)

    # ── MDO report: enrolment + user + content + batch ───────────
    mdo_sql = f"""
    SELECT
        u."fullName"               AS "Full_Name",
        u."userGender"             AS "Gender",
        u."userCategory"           AS "Category",
        u."maskedPhone"            AS "Masked_Phone",
        u."maskedEmail"            AS "Masked_Email",
        u."userPrimaryEmail"       AS "Email",
        u."userMobile"             AS "Phone_Number",
        u."designation"            AS "Designation",
        u."group"                  AS "Group",
        u."Tag",
        {MDO_HIERARCHY_COLUMNS},
        bp."bpName"                AS "Blended_Program_Name",
        bp."bpStatus"              AS "Blended_Program_Status",
        bp."bpOrgName"             AS "Blended_Program_Provider",
        {duration_format_expr('bpDuration', 'Blended_Program_Duration')},
        b."courseBatchName"        AS "Batch_Name",
        TRY_CAST(b."courseBatchStartDate" AS DATE) AS "Batch_Start_Date",
        TRY_CAST(b."courseBatchEndDate" AS DATE)   AS "Batch_End_Date",
        CAST(e."courseEnrolledTimestamp" AS VARCHAR) AS "Enrolled_On",
        CASE WHEN e."dbCompletionStatus" IS NULL THEN 'not-enrolled'
             WHEN e."dbCompletionStatus" = 0 THEN 'not-started'
             WHEN e."dbCompletionStatus" = 1 THEN 'in-progress'
             ELSE 'completed' END  AS "Status",
        CASE
            WHEN bp."bpResourceCount" = 0 OR e."courseProgress" = 0 OR e."dbCompletionStatus" = 0 THEN 0.0
            WHEN e."dbCompletionStatus" = 2 THEN 100.0
            ELSE LEAST(100.0, GREATEST(0.0, ROUND(100.0 * e."courseProgress" / bp."bpResourceCount", 2)))
        END                        AS "Content_Progress_Percentage",
        CAST(e."courseCompletedTimestamp" AS VARCHAR) AS "Completed_On",
        CASE WHEN e."issuedCertificateCount" > 0 THEN 'Yes' ELSE 'No' END AS "Certificate_Generated",
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS "Report_Last_Generated_On",
        bp."bpOrgID"               AS "mdoid"
    FROM enrolment_computed e
    INNER JOIN _bp_base bp ON e."courseID" = bp."bpID"
    INNER JOIN user_org_computed u ON e."userID" = u."userID"
    LEFT JOIN batch_select b ON e."courseID" = b."courseID" AND e."batchID" = b."batchID"
    WHERE CAST(u."userStatus" AS INT) = 1
    """

    report_path = f"{config.localReportDir}/{config.blendedReportPath}/{today}"
    write_csv_per_mdo(con, mdo_sql, report_path, "mdoid",
                      csv_filename=config.blendedProgramReport)

    # ── Warehouse: bp_enrolments → Parquet ───────────────────────
    warehouse_sql = """
    SELECT DISTINCT ON (e."userID", bp."bpID", e."batchID")
        e."userID"             AS user_id,
        bp."bpID"              AS content_id,
        e."batchID"            AS batch_id,
        bp."bpName"            AS content_name,
        bp."bpOrgID"           AS content_provider_id,
        bp."bpOrgName"         AS content_provider_name,
        bp."bpCategory"        AS content_type,
        b."courseBatchName"    AS batch_name,
        TRY_CAST(b."courseBatchStartDate" AS DATE) AS batch_start_date,
        TRY_CAST(b."courseBatchEndDate" AS DATE)   AS batch_end_date,
        CAST(e."courseEnrolledTimestamp" AS VARCHAR) AS enrolled_on,
        CASE WHEN e."dbCompletionStatus" IS NULL THEN 'not-enrolled'
             WHEN e."dbCompletionStatus" = 0 THEN 'not-started'
             WHEN e."dbCompletionStatus" = 1 THEN 'in-progress'
             ELSE 'completed' END AS user_consumption_status,
        CASE
            WHEN bp."bpResourceCount" = 0 OR e."courseProgress" = 0 OR e."dbCompletionStatus" = 0 THEN 0.0
            WHEN e."dbCompletionStatus" = 2 THEN 100.0
            ELSE LEAST(100.0, GREATEST(0.0, ROUND(100.0 * e."courseProgress" / bp."bpResourceCount", 2)))
        END AS content_progress_percentage,
        CAST(e."courseCompletedTimestamp" AS VARCHAR) AS completed_on,
        CASE WHEN e."issuedCertificateCount" > 0 THEN 'Yes' ELSE 'No' END AS certificate_generated,
        e."certificateID"      AS certificate_id,
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS data_last_generated_on
    FROM enrolment_computed e
    INNER JOIN _bp_base bp ON e."courseID" = bp."bpID"
    LEFT JOIN batch_select b ON e."courseID" = b."courseID" AND e."batchID" = b."batchID"
    """

    write_warehouse_parquet(con, warehouse_sql,
                           f"{config.warehouseReportDir}/{config.dwBPEnrollmentsTable}")

    con.close()
    log.info("BlendedReport — done")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] BlendedReport at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] BlendedReport — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
