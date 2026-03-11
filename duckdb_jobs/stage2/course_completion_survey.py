"""
Course Completion Survey Report — DuckDB Migration
Reads course_completion_survey cache parquet, produces:
  - CSV per MDO (completionSurvey.csv)
  - Warehouse parquet (course_completion_survey_details)
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
    write_csv_per_mdo, write_warehouse_parquet, CURRENT_DATETIME_EXPR,
)

log = logging.getLogger("course-completion-survey")


def process_data(config, db_path=None):
    today = datetime.now().strftime("%Y-%m-%d")
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    cache_path = getattr(config, 'baseCachePath',
                         str(Path(__file__).resolve().parents[2] / "data-res/pq_files/cache_pq"))
    survey_path = f"{cache_path}/courseCompletionSurvey"
    form_ids = getattr(config, 'completionSurveyFormIds', [])

    # Read raw survey parquet and explode responses
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _survey_raw AS
        SELECT * FROM read_parquet('{survey_path}/**/*.parquet', union_by_name=true)
    """)

    # Filter by configured form IDs if available
    form_filter = ""
    if form_ids:
        escaped = ",".join(f"'{fid}'" for fid in form_ids)
        form_filter = f"WHERE formid IN ({escaped})"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _survey_filtered AS
        SELECT * FROM _survey_raw {form_filter}
    """)

    # Explode responses array — each element has question + answer
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _survey_exploded AS
        SELECT
            sr.*,
            json_extract_string(resp.val, '$.question') AS question,
            json_extract_string(resp.val, '$.answer')   AS answer
        FROM _survey_filtered sr,
        LATERAL (
            SELECT UNNEST(CAST(sr.responses AS JSON[])) AS val
        ) resp
        WHERE sr.responses IS NOT NULL
    """)

    # ── Pivot: one row per submission, questions → columns ────────
    # DuckDB PIVOT syntax for dynamic columns
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _survey_pivoted AS
        SELECT
            userid,
            contextid AS content_id,
            contextname AS content_name,
            orgid AS mdo_id,
            formid,
            formversion,
            contentEndSurveyFormid,
            MAX(CASE WHEN question = 'Design' THEN answer END) AS design_rating,
            MAX(CASE WHEN question = 'Content' THEN answer END) AS content_rating,
            MAX(CASE WHEN question = 'Delivery' THEN answer END) AS delivery_rating,
            MAX(CASE WHEN question = 'RoleRelevance' THEN answer END) AS role_relevance_rating,
            MAX(CASE WHEN question = 'Improvement Suggestions' THEN answer END) AS improvement_suggestions
        FROM _survey_exploded
        GROUP BY userid, contextid, contextname, orgid, formid, formversion, contentEndSurveyFormid
    """)

    # ── CSV per MDO ──────────────────────────────────────────────
    csv_sql = """
    SELECT
        userid          AS "User_ID",
        content_id      AS "Content_ID",
        content_name    AS "Content_Name",
        design_rating   AS "Design",
        content_rating  AS "Content",
        delivery_rating AS "Delivery",
        role_relevance_rating AS "Role_Relevance",
        improvement_suggestions AS "Improvement_Suggestions",
        mdo_id
    FROM _survey_pivoted
    """

    report_path = f"{config.localReportDir}/{config.completionSurveyReportPath}/{today}"
    os.makedirs(report_path, exist_ok=True)
    write_csv_per_mdo(con, csv_sql, report_path, "mdo_id",
                      config.completionSurveyReport)

    # ── Warehouse parquet ────────────────────────────────────────
    wh_sql = f"""
    SELECT
        userid                  AS user_id,
        content_id,
        content_name,
        mdo_id,
        TRY_CAST(design_rating AS INTEGER) AS design_rating,
        TRY_CAST(content_rating AS INTEGER) AS content_rating,
        TRY_CAST(delivery_rating AS INTEGER) AS delivery_rating,
        TRY_CAST(role_relevance_rating AS INTEGER) AS role_relevance_rating,
        improvement_suggestions,
        contentEndSurveyFormid  AS form_id,
        {CURRENT_DATETIME_EXPR} AS data_last_generated_on
    FROM _survey_pivoted
    WHERE contentEndSurveyFormid IS NOT NULL
    """
    write_warehouse_parquet(con, wh_sql,
                           f"{config.warehouseReportDir}/{config.dwCourseCompletionSurveryTable}")

    con.close()
    log.info("CourseCompletionSurveyReport — done")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] CourseCompletionSurveyReport at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] CourseCompletionSurveyReport — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
