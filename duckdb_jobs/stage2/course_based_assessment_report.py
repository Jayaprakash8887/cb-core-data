"""
Course-Based Assessment Report — DuckDB Migration
Reads from initialized DuckDB, produces:
  - CSV per MDO (UserAssessmentReport.csv)
  - Warehouse parquet (assessment_detail)
"""
import os
import sys
import logging
from pathlib import Path
from datetime import datetime

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH
from duckdb_jobs.core.export_utils import write_csv_per_mdo, write_warehouse_parquet, MDO_HIERARCHY_COLUMNS

log = logging.getLogger("cba-report")


def process_data(config, db_path=None):
    today = datetime.now().strftime("%Y-%m-%d")
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    # ── Old assessment data (already computed in initializer) ────
    mdo_sql = f"""
    SELECT
        oa."fullName"           AS "Full_Name",
        oa."userPrimaryEmail"   AS "Email",
        oa."userMobile"         AS "Phone_Number",
        oa."userOrgName"        AS "MDO_Name",
        oa."ministry_name"      AS "Ministry",
        oa."dept_name"          AS "Department",
        oa."designation"        AS "Designation",
        oa."group"              AS "Group",
        oa."Tag",
        oa."courseName"         AS "Course_Name",
        oa."category"           AS "Course_Category",
        oa."courseStatus"        AS "Course_Status",
        oa."assessment_type"    AS "Assessment_Type",
        oa."total_questions"    AS "Total_Questions",
        oa.correct_count        AS "Correct",
        oa.incorrect_count      AS "Incorrect",
        oa.not_answered_count   AS "Not_Answered",
        ROUND(oa.result_percent, 2) AS "Score_Percentage",
        ROUND(oa.pass_percent, 2)   AS "Pass_Percentage",
        oa."Pass"               AS "Pass",
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS "Report_Last_Generated_On",
        oa."courseOrgID"        AS "mdoid"
    FROM old_assessment_computed oa
    WHERE oa."courseName" IS NOT NULL
    """

    report_path = f"{config.localReportDir}/{config.cbaReportPath}/{today}"
    write_csv_per_mdo(con, mdo_sql, report_path, "mdoid",
                      csv_filename=config.cbaReport)

    # ── Warehouse: assessment_detail → Parquet ───────────────────
    warehouse_sql = """
    SELECT
        oa.user_id,
        oa."courseID"            AS content_id,
        oa.source_id             AS assessment_id,
        oa."courseName"          AS course_name,
        oa."category"            AS course_category,
        oa."assessment_type",
        oa."total_questions",
        oa.correct_count,
        oa.incorrect_count,
        oa.not_answered_count,
        ROUND(oa.result_percent, 2) AS score_percentage,
        ROUND(oa.pass_percent, 2)   AS pass_percentage,
        oa."Pass"                AS pass_status,
        CAST(oa.ts_created AS VARCHAR) AS assessment_date,
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS data_last_generated_on
    FROM old_assessment_computed oa
    """

    write_warehouse_parquet(con, warehouse_sql,
                           f"{config.warehouseReportDir}/{config.dwAssessmentTable}")

    con.close()
    log.info("CourseBasedAssessmentReport — done")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] CBA Report at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] CBA Report — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
