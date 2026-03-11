"""
Standalone Assessment Report — DuckDB Migration
Reads from initialized DuckDB, produces:
  - CSV per MDO (StandaloneAssessmentReport.csv)
"""
import os
import sys
import logging
from pathlib import Path
from datetime import datetime

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH
from duckdb_jobs.core.export_utils import write_csv_per_mdo

log = logging.getLogger("assessment-report")


def process_data(config, db_path=None):
    today = datetime.now().strftime("%Y-%m-%d")
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    mdo_sql = """
    SELECT
        u."fullName"               AS "Full_Name",
        u."userPrimaryEmail"       AS "Email",
        u."userMobile"             AS "Phone_Number",
        u."userOrgName"            AS "MDO_Name",
        u."ministry_name"          AS "Ministry",
        u."dept_name"              AS "Department",
        u."designation"            AS "Designation",
        u."group"                  AS "Group",
        u."Tag",
        a."assessName"             AS "Assessment_Name",
        a."assessCategory"         AS "Assessment_Type",
        o."orgName"                AS "Assessment_Provider",
        a."assessStatus"           AS "Assessment_Status",
        -- Note: detailed per-question data requires raw_user_assessment + hierarchy
        -- parsing which remains as future enhancement
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS "Report_Last_Generated_On",
        a."assessOrgID"            AS "mdoid"
    FROM all_assessment_computed a
    LEFT JOIN org_computed o ON a."assessOrgID" = o."orgID"
    CROSS JOIN user_org_computed u
    WHERE a."assessStatus" = 'Live'
      AND CAST(u."userStatus" AS INT) = 1
    """
    # Note: The full assessment report requires joining user_assessment raw data
    # with hierarchy children to get per-question scores. The above is a
    # simplified version. For full parity, use the raw_user_assessment view
    # and parse the hierarchy JSON for children details.

    report_path = f"{config.localReportDir}/{config.standaloneAssessmentReportPath}/{today}"
    write_csv_per_mdo(con, mdo_sql, report_path, "mdoid",
                      csv_filename=config.userAssessmentReport)

    con.close()
    log.info("AssessmentReport — done")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] AssessmentReport at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] AssessmentReport — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
