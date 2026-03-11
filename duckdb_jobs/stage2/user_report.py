"""
User Report — DuckDB Migration
Reads from initialized DuckDB, produces:
  - CSV per MDO (UserReport.csv) with org-specific custom fields
  - Warehouse parquet (user_detail, userCustomFields)
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
    MDO_HIERARCHY_COLUMNS, CURRENT_DATETIME_EXPR,
)

log = logging.getLogger("user-report")


def process_data(config, db_path=None):
    today = datetime.now().strftime("%Y-%m-%d")
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    # ── Content learning hours per user ──────────────────────────
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _user_content AS
        SELECT
            ew."userID",
            COUNT(DISTINCT CASE WHEN ew."user_consumption_status"
                IN ('not-started','in-progress','completed') THEN ew."content_id" END)
                AS total_content_enrolments,
            COUNT(DISTINCT CASE WHEN ew."user_consumption_status" = 'completed'
                AND ew."certificateID" IS NOT NULL AND ew."certificateID" <> ''
                THEN ew."content_id" END)
                AS total_content_completions,
            ROUND(COALESCE(SUM(
                CASE WHEN ew."user_consumption_status" = 'completed'
                    AND ew."certificateID" IS NOT NULL AND ew."certificateID" <> ''
                    AND cc."category" = 'Course'
                    THEN COALESCE(cc."courseDuration", 0) ELSE 0 END
            ) / 3600.0, 0), 2) AS total_content_duration
        FROM enrolment_warehouse_computed ew
        LEFT JOIN content_computed cc ON ew."content_id" = cc."courseID"
        GROUP BY ew."userID"
    """)

    # ── Event learning hours per user ────────────────────────────
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _user_events AS
        SELECT
            user_id AS "userID",
            COUNT(DISTINCT CASE WHEN status IN ('not-started','in-progress','completed')
                THEN event_id END) AS total_event_enrolments,
            COUNT(DISTINCT CASE WHEN status = 'completed' THEN event_id END)
                AS total_event_completions,
            ROUND(COALESCE(SUM(
                CASE WHEN status = 'completed' AND certificate_id IS NOT NULL
                     THEN event_duration_seconds ELSE 0 END
            ) / 3600.0, 0), 2) AS total_event_learning_hours_with_certificates
        FROM warehouse_event_enrolments
        GROUP BY user_id
    """)

    # ── MDO-wise User Report → CSV ───────────────────────────────
    mdo_report_sql = f"""
    SELECT
        u."fullName"           AS "Full_Name",
        u."designation"        AS "Designation",
        u."userPrimaryEmail"   AS "Email",
        u."userMobile"         AS "Phone_Number",
        {MDO_HIERARCHY_COLUMNS},
        u."group"              AS "Group",
        u."Tag",
        CAST(epoch_ms(u."userCreatedTimestamp") AS DATE) AS "User_Registration_Date",
        u."cadreName"          AS "Cadre",
        u."civilServiceType"   AS "Civil Service Type",
        u."civilServiceName"   AS "Civil Services",
        u."cadreBatch"         AS "Cadre Batch",
        u."isOnCentralDeputation" AS "Is On Central Deputation",
        u."organised_service"  AS "Is From Organised Service of Govt",
        u."role"               AS "Roles",
        u."userGender"         AS "Gender",
        u."userCategory"       AS "Category",
        json_extract_string(u."additionalProperties", '$.externalSystem') AS "External_System",
        json_extract_string(u."additionalProperties", '$.externalSystemId') AS "External_System_Id",
        u."employeeCode"       AS "Employee_Id",
        CAST(epoch_ms(u."userOrgCreatedDate") AS DATE) AS "MDO_Created_On",
        u."userProfileStatus"  AS "Profile_Status",
        COALESCE(u."weekly_claps_day_before_yesterday", 0) AS "weekly_claps_day_before_yesterday",
        COALESCE(u."total_points", 0) AS "Karma_Points",
        COALESCE(ev.total_event_enrolments, 0) AS "Event_Enrolments",
        COALESCE(ev.total_event_completions, 0) AS "Event_Completions",
        COALESCE(ev.total_event_learning_hours_with_certificates, 0) AS "Event_Learning_Hours",
        COALESCE(cs.total_content_enrolments, 0) AS "Course_Enrolments",
        COALESCE(cs.total_content_completions, 0) AS "Course_Completions",
        COALESCE(cs.total_content_duration, 0) AS "Course_Learning_Hours",
        COALESCE(ev.total_event_enrolments, 0) + COALESCE(cs.total_content_enrolments, 0)
            AS "Total_Enrolments",
        COALESCE(ev.total_event_completions, 0) + COALESCE(cs.total_content_completions, 0)
            AS "Total_Completions",
        COALESCE(ev.total_event_learning_hours_with_certificates, 0) +
            COALESCE(cs.total_content_duration, 0) AS "Total_Learning_Hours",
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS "Report_Last_Generated_On",
        u."userOrgID"          AS "mdoid"
    FROM user_org_computed u
    LEFT JOIN _user_content cs ON u."userID" = cs."userID"
    LEFT JOIN _user_events ev  ON u."userID" = ev."userID"
    WHERE CAST(u."userStatus" AS INT) = 1
    """

    report_path = f"{config.localReportDir}/{config.userReportPath}/{today}"
    write_csv_per_mdo(con, mdo_report_sql, report_path, "mdoid",
                      csv_filename=config.userReport)

    # ── Warehouse: user_detail → Parquet ─────────────────────────
    warehouse_sql = """
    SELECT
        u."userID"                       AS user_id,
        u."userOrgID"                    AS mdo_id,
        u."userStatus"                   AS status,
        COALESCE(u."total_points", 0)    AS no_of_karma_points,
        u."fullName"                     AS full_name,
        u."designation",
        u."userPrimaryEmail"             AS email,
        u."userMobile"                   AS phone_number,
        u."pincode",
        u."group"                        AS groups,
        u."Tag"                          AS tag,
        u."userProfileStatus"            AS profile_status,
        CAST(epoch_ms(u."userCreatedTimestamp") AS VARCHAR) AS user_registration_date,
        CAST(epoch_ms(u."userUpdatedTimestamp") AS VARCHAR) AS profile_last_updated_date,
        u."role"                         AS roles,
        u."userGender"                   AS gender,
        u."userCategory"                 AS category,
        CASE WHEN u."userProfileStatus" = 'NOT-MY-USER' THEN TRUE ELSE FALSE END AS marked_as_not_my_user,
        CASE WHEN u."userProfileStatus" = 'VERIFIED' THEN TRUE ELSE FALSE END    AS is_verified_karmayogi,
        u."userCreatedBy"                AS created_by_id,
        json_extract_string(u."additionalProperties", '$.externalSystem') AS external_system,
        json_extract_string(u."additionalProperties", '$.externalSystemId') AS external_system_id,
        COALESCE(u."weekly_claps_day_before_yesterday", 0) AS weekly_claps_day_before_yesterday,
        COALESCE(ev.total_event_learning_hours_with_certificates, 0) AS total_event_learning_hours,
        COALESCE(cs.total_content_duration, 0) AS total_content_learning_hours,
        COALESCE(ev.total_event_learning_hours_with_certificates, 0) +
            COALESCE(cs.total_content_duration, 0) AS total_learning_hours,
        u."employeeCode"                 AS employee_id,
        u."cadreName"                    AS cadre,
        u."civilServiceType"             AS civil_service_type,
        u."civilServiceName"             AS civil_services,
        u."cadreBatch"                   AS cadre_batch,
        u."isOnCentralDeputation"        AS is_on_central_deputation,
        u."organised_service"            AS is_from_organised_service_of_govt,
        strftime(NOW(), '%Y-%m-%d %I:%M:%S %p') AS data_last_generated_on
    FROM user_org_computed u
    LEFT JOIN _user_content cs ON u."userID" = cs."userID"
    LEFT JOIN _user_events ev  ON u."userID" = ev."userID"
    """

    write_warehouse_parquet(con, warehouse_sql,
                           f"{config.warehouseReportDir}/{config.dwUserTable}")

    con.close()
    log.info("UserReport — done")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] UserReport at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] UserReport — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
