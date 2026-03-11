"""
KCM Report — DuckDB Migration
Reads from initialized DuckDB, produces:
  - CSV (ContentCompetencyMapping.csv)
  - Warehouse parquet (kcm_dictionary, kcm_content_mapping)
"""
import os
import sys
import logging
from pathlib import Path
from datetime import datetime

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH
from duckdb_jobs.core.export_utils import write_single_csv, write_warehouse_parquet

log = logging.getLogger("kcm-report")


def process_data(config, db_path=None):
    today = datetime.now().strftime("%Y-%m-%d")
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    # ── Extract competencies from content_computed ────────────────
    # competencies_v6 is a JSON array; each element has:
    #   competencyArea, competencyAreaId, competencyTheme, competencyThemeId,
    #   competencySubTheme, competencySubThemeId
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _kcm_raw AS
        SELECT
            cc."courseID",
            cc."courseName",
            cc."category",
            cc."courseOrgID",
            cc."courseOrgName",
            cc."courseStatus",
            comp.val AS competency_json
        FROM content_computed cc,
        LATERAL (
            SELECT UNNEST(
                CAST(cc."competencies_v6" AS JSON[])
            ) AS val
        ) comp
        WHERE cc."competencies_v6" IS NOT NULL
          AND cc."courseStatus" IN ('Live', 'Draft', 'Retired')
    """)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _kcm_parsed AS
        SELECT
            "courseID",
            "courseName",
            "category",
            "courseOrgID",
            "courseOrgName",
            "courseStatus",
            json_extract_string(competency_json, '$.competencyArea')       AS competency_area,
            json_extract_string(competency_json, '$.competencyAreaId')     AS competency_area_id,
            json_extract_string(competency_json, '$.competencyTheme')      AS competency_theme,
            json_extract_string(competency_json, '$.competencyThemeId')    AS competency_theme_id,
            json_extract_string(competency_json, '$.competencySubTheme')   AS competency_sub_theme,
            json_extract_string(competency_json, '$.competencySubThemeId') AS competency_sub_theme_id
        FROM _kcm_raw
    """)

    # ── KCM Dictionary (unique competency entries) → warehouse ───
    kcm_dict_sql = """
    SELECT DISTINCT
        competency_area_id      AS area_id,
        competency_area         AS area_name,
        competency_theme_id     AS theme_id,
        competency_theme        AS theme_name,
        competency_sub_theme_id AS sub_theme_id,
        competency_sub_theme    AS sub_theme_name
    FROM _kcm_parsed
    WHERE competency_area_id IS NOT NULL
    """

    write_warehouse_parquet(con, kcm_dict_sql,
                           f"{config.warehouseReportDir}/{config.dwKcmDictionaryTable}")

    # ── KCM Content Mapping → warehouse ──────────────────────────
    kcm_content_sql = """
    SELECT
        "courseID"               AS content_id,
        "courseName"            AS content_name,
        "category"              AS content_type,
        "courseOrgID"            AS content_provider_id,
        "courseOrgName"          AS content_provider_name,
        "courseStatus"           AS content_status,
        competency_area_id      AS area_id,
        competency_area         AS area_name,
        competency_theme_id     AS theme_id,
        competency_theme        AS theme_name,
        competency_sub_theme_id AS sub_theme_id,
        competency_sub_theme    AS sub_theme_name,
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS data_last_generated_on
    FROM _kcm_parsed
    """

    write_warehouse_parquet(con, kcm_content_sql,
                           f"{config.warehouseReportDir}/{config.dwKcmContentTable}")

    # ── CSV report ───────────────────────────────────────────────
    csv_sql = """
    SELECT
        "courseID"               AS "Content_ID",
        "courseName"            AS "Content_Name",
        "category"              AS "Content_Type",
        "courseOrgName"          AS "Content_Provider",
        "courseStatus"           AS "Content_Status",
        competency_area         AS "Competency_Area",
        competency_theme        AS "Competency_Theme",
        competency_sub_theme    AS "Competency_Sub_Theme"
    FROM _kcm_parsed
    """

    report_path = f"{config.localReportDir}/{config.kcmReportPath}/{today}"
    os.makedirs(report_path, exist_ok=True)
    write_single_csv(con, csv_sql, f"{report_path}/{config.kcmReport}")

    con.close()
    log.info("KCMReport — done")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] KCMReport at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] KCMReport — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
