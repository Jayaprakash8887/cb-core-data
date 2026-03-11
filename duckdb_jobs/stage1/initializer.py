"""
DuckDB Initializer Job
======================
Reads all Stage 0 Parquet outputs (from dataExhaust.py) and executes the
equivalent of every Stage 1 (prejoinData.py) transformation as pure SQL,
storing results as persistent tables inside a single DuckDB database file.

The database is configured to use DISK instead of RAM so it can handle
datasets larger than available memory.

Usage:
    python -m duckdb-jobs.initializer            # uses default paths
    python initializer.py --cache-path /custom   # override cache path
    python initializer.py --db-path /out/my.duckdb
"""

import os
import sys
import time
import shutil
import logging
import argparse
from pathlib import Path
from datetime import datetime

import duckdb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_CACHE_PATH = str(BASE_DIR / "data-res" / "pq_files" / "cache_pq")
DEFAULT_DB_PATH = str(BASE_DIR / "output" / "igot.duckdb")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("duckdb-initializer")


# ───────────────────────────────────────────────────────────────────────────
# Helper: run a SQL stage with timing
# ───────────────────────────────────────────────────────────────────────────
def run_stage(con: duckdb.DuckDBPyConnection, name: str, sql: str):
    """Execute *sql* inside *con* and log the wall-clock time."""
    log.info(f"Stage: {name} — Starting...")
    t0 = time.time()
    for statement in sql.strip().split(";"):
        stmt = statement.strip()
        if stmt:
            con.execute(stmt)
    elapsed = time.time() - t0
    log.info(f"Stage: {name} — Done ({elapsed:.2f}s)")


# ───────────────────────────────────────────────────────────────────────────
# Main initializer
# ───────────────────────────────────────────────────────────────────────────
def initialize(cache_path: str = None, db_path: str = None,
               memory_limit: str = "10GB", threads: int = 8):
    """
    Build the persistent DuckDB database from Stage 0 Parquet cache files.

    Parameters
    ----------
    cache_path : str
        Root of the Parquet cache written by dataExhaust.py.
        Default: ``<project>/data-res/pq_files/cache_pq``
    db_path : str
        Where to write the DuckDB database file.
        Default: ``<project>/output/igot.duckdb``
    memory_limit : str
        DuckDB memory budget (spills to disk beyond this).
    threads : int
        Number of DuckDB worker threads.
    """

    cache = cache_path or DEFAULT_CACHE_PATH
    db = db_path or DEFAULT_DB_PATH

    log.info("=" * 72)
    log.info("DuckDB Initializer — Building persistent database")
    log.info(f"  Cache path : {cache}")
    log.info(f"  DB path    : {db}")
    log.info(f"  Memory     : {memory_limit}")
    log.info(f"  Threads    : {threads}")
    log.info("=" * 72)

    total_t0 = time.time()

    # Ensure output directory exists
    os.makedirs(os.path.dirname(db), exist_ok=True)
    tmp_dir = os.path.join(os.path.dirname(db), "duckdb_tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    # Remove stale DB so we get a clean build
    if os.path.exists(db):
        os.remove(db)
        # Remove WAL file if present
        wal = db + ".wal"
        if os.path.exists(wal):
            os.remove(wal)

    con = duckdb.connect(database=db)
    con.execute(f"SET memory_limit = '{memory_limit}';")
    con.execute(f"SET threads = {threads};")
    con.execute(f"SET temp_directory = '{tmp_dir}';")
    con.execute("SET preserve_insertion_order = false;")

    # ──────────────────────────────────────────────────────────────────
    # STAGE 0 — Ingest raw Parquet files into views
    # We use views (not tables) so data is read lazily from Parquet.
    # ──────────────────────────────────────────────────────────────────
    raw_sources = {
        "raw_org":                       "org",
        "raw_org_hierarchy":             "orgHierarchy",
        "raw_es_content":                "esContent",
        "raw_rating_summary":            "ratingSummary",
        "raw_rating":                    "rating",
        "raw_hierarchy":                 "hierarchy",
        "raw_external_content":          "externalContent",
        "raw_user":                      "user",
        "raw_role":                      "role",
        "raw_user_karma_points":         "userKarmaPoints",
        "raw_weekly_claps":              "weeklyClaps",
        "raw_enrolment":                 "enrolment",
        "raw_batch":                     "batch",
        "raw_external_course_enrolments":"externalCourseEnrolments",
        "raw_old_assessment":            "oldAssessmentDetails",
        "raw_event_enrolment":           "eventEnrolmentDetails",
        "raw_event":                     "eventDetails",
        "raw_acbp":                      "acbp",
        "raw_user_assessment":           "userAssessment",
        "raw_kcm_v6":                    "kcmV6",
        "raw_learner_leaderboard":       "learnerLeaderBoard",
        "raw_org_complete_hierarchy":    "orgCompleteHierarchy",
        "raw_user_karma_points_summary": "userKarmaPointsSummary",
    }

    log.info("Registering raw Parquet sources as views...")
    for view_name, folder in raw_sources.items():
        parquet_dir = os.path.join(cache, folder)
        if os.path.isdir(parquet_dir):
            con.execute(f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM read_parquet('{parquet_dir}/**/*.parquet', union_by_name=true)
            """)
            log.info(f"  ✓ {view_name} ← {folder}/")
        else:
            # Create empty view so downstream SQL doesn't break
            log.warning(f"  ✗ {folder}/ not found — creating empty view {view_name}")
            con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT 1 WHERE false")

    # ==================================================================
    # STAGE 1.1 — Org Hierarchy Computation
    # ==================================================================
    run_stage(con, "1.1  Org Hierarchy Computation", f"""
        CREATE OR REPLACE TABLE org_select AS
        SELECT
            id                  AS "orgID",
            orgname             AS "orgName",
            status              AS "orgStatus",
            EPOCH_MS(TRY_CAST(createddate AS BIGINT))  AS "orgCreatedDate",
            organisationtype    AS "orgType",
            organisationsubtype AS "orgSubType"
        FROM raw_org
    ;

        CREATE OR REPLACE TABLE org_hierarchy_select AS
        SELECT
            mdo_id      AS "userOrgID",
            department   AS "dept_name",
            ministry     AS "ministry_name"
        FROM raw_org_hierarchy
    ;

        CREATE OR REPLACE TABLE org_computed AS
        SELECT o.*, h."dept_name", h."ministry_name"
        FROM org_select o
        LEFT JOIN org_hierarchy_select h ON o."orgID" = h."userOrgID"
    """)

    # ==================================================================
    # STAGE 1.2 — Content Ratings & Summary
    # ==================================================================
    run_stage(con, "1.2  Content Ratings & Summary", """
        CREATE OR REPLACE TABLE rating_summary_computed AS
        SELECT
            activityid                  AS "courseID",
            LOWER(activitytype)         AS "categoryLower",
            sum_of_total_ratings        AS "ratingSum",
            total_number_of_ratings     AS "ratingCount",
            sum_of_total_ratings * 1.0
                / total_number_of_ratings  AS "ratingAverage",
            totalcount1stars            AS "count1Star",
            totalcount2stars            AS "count2Star",
            totalcount3stars            AS "count3Star",
            totalcount4stars            AS "count4Star",
            totalcount5stars            AS "count5Star"
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY activityid, LOWER(activitytype)
                ORDER BY total_number_of_ratings DESC
            ) AS _rn
            FROM raw_rating_summary
            WHERE total_number_of_ratings > 0
        ) sub
        WHERE _rn = 1
    ;

        CREATE OR REPLACE TABLE rating_computed AS
        SELECT
            activityid   AS "courseID",
            userid       AS "userID",
            rating       AS "userRating",
            activitytype AS "cbpType",
            createdon    AS "createdOn"
        FROM raw_rating
    ;

        CREATE OR REPLACE TABLE content_rating_computed AS
        SELECT
            "courseID",
            COUNT("userRating")       AS "totalRatings",
            AVG("userRating")         AS "rating"
        FROM rating_computed
        WHERE "userRating" IS NOT NULL
        GROUP BY "courseID"
    """)

    # ==================================================================
    # STAGE 1.3 — All Course/Program (ES)
    # ==================================================================
    run_stage(con, "1.3  All Course/Program (ES)", """
        CREATE OR REPLACE TABLE all_course_program_computed AS
        SELECT * FROM (
            SELECT
                identifier                          AS "courseID",
                "primaryCategory"                   AS "category",
                name                                AS "courseName",
                status                              AS "courseStatus",
                "reviewStatus"                      AS "courseReviewStatus",
                channel                             AS "courseChannel",
                "lastPublishedOn"                   AS "courseLastPublishedOn",
                COALESCE(TRY_CAST(duration AS FLOAT), 0.0)
                                                    AS "courseDuration",
                COALESCE("leafNodesCount", 0)       AS "courseResourceCount",
                "lastStatusChangedOn",
                "programDirectorName",
                cf.val                              AS "courseOrgID",
                "competencies_v6",
                lng.val                             AS "contentLanguage",
                "courseCategory",
                org_name.val                        AS "contentCreator",
                "difficultyLevel",
                ROW_NUMBER() OVER (
                    PARTITION BY identifier, "primaryCategory"
                    ORDER BY identifier
                ) AS _rn
            FROM raw_es_content
            LEFT JOIN LATERAL (SELECT UNNEST("createdFor") AS val) cf ON true
            LEFT JOIN LATERAL (SELECT UNNEST("language") AS val) lng ON true
            LEFT JOIN LATERAL (SELECT UNNEST("organisation") AS val) org_name ON true
        ) sub
        WHERE _rn = 1
    """)

    # ==================================================================
    # STAGE 1.4 — Content Master Data  (course + rating + org)
    # ==================================================================
    run_stage(con, "1.4  Content Master Data", """
        CREATE OR REPLACE TABLE content_computed AS
        SELECT
            cp.*,
            cr."totalRatings",
            cr."rating",
            o."orgName"  AS "courseOrgName",
            o."orgStatus" AS "courseOrgStatus"
        FROM all_course_program_computed cp
        LEFT JOIN content_rating_computed cr ON cp."courseID" = cr."courseID"
        LEFT JOIN org_computed o              ON cp."courseOrgID" = o."orgID"
    """)

    # ==================================================================
    # STAGE 1.5 — Content Hierarchy
    # ==================================================================
    run_stage(con, "1.5  Content Hierarchy", """
        CREATE OR REPLACE TABLE content_hierarchy_select AS
        SELECT identifier, hierarchy
        FROM raw_hierarchy
    """)

    # ==================================================================
    # STAGE 1.6 — Assessment Master Data
    # ==================================================================
    run_stage(con, "1.6  Assessment Master Data", """
        CREATE OR REPLACE TABLE all_assessment_computed AS
        SELECT * FROM (
            SELECT
                identifier                          AS "assessID",
                "primaryCategory"                   AS "assessCategory",
                "courseCategory"                     AS "assessCourseCategory",
                name                                AS "assessName",
                status                              AS "assessStatus",
                "reviewStatus"                      AS "assessReviewStatus",
                channel                             AS "assessChannel",
                COALESCE(TRY_CAST(duration AS FLOAT), 0.0)
                                                    AS "assessDuration",
                COALESCE("leafNodesCount", 0)       AS "assessChildCount",
                "lastPublishedOn"                   AS "assessLastPublishedOn",
                cf.val                              AS "assessOrgID",
                ROW_NUMBER() OVER (
                    PARTITION BY identifier, "primaryCategory"
                    ORDER BY identifier
                ) AS _rn
            FROM raw_es_content
            LEFT JOIN LATERAL (SELECT UNNEST("createdFor") AS val) cf ON true
        ) sub
        WHERE _rn = 1
    """)

    # ==================================================================
    # STAGE 1.7 — External Content
    # ==================================================================
    run_stage(con, "1.7  External Content", """
        CREATE OR REPLACE TABLE external_content_computed AS
        SELECT
            courseid                                     AS "content_id",
            json_extract_string(cios_data, '$.content.name')
                                                         AS "courseName",
            json_extract_string(cios_data, '$.content.duration')
                                                         AS "courseDuration",
            json_extract_string(cios_data, '$.content.lastUpdatedOn')
                                                         AS "courseLastPublishedOn",
            json_extract_string(cios_data, '$.content.contentPartner.id')
                                                         AS "courseOrgID",
            json_extract_string(cios_data, '$.content.contentPartner.contentPartnerName')
                                                         AS "courseOrgName",
            'External Content'                           AS "category",
            'LIVE'                                       AS "courseStatus"
        FROM raw_external_content
    """)

    # ==================================================================
    # STAGE 1.8 — User Profile Computation
    #
    # DuckDB has native JSON functions so we parse profiledetails inline.
    # ==================================================================
    run_stage(con, "1.8  User Profile Computation", """
        CREATE OR REPLACE TABLE user_select AS
        SELECT
            id                                           AS "userID",
            COALESCE(firstname, '')                      AS "firstName",
            COALESCE(lastname, '')                       AS "lastName",
            maskedemail                                  AS "maskedEmail",
            maskedphone                                  AS "maskedPhone",
            COALESCE(rootorgid, '')                      AS "userOrgID",
            status                                      AS "userStatus",
            createddate                                  AS "userCreatedTimestamp",
            updateddate                                  AS "userUpdatedTimestamp",
            createdby                                    AS "userCreatedBy",
            profiledetails                               AS "userProfileDetails",
            -- personalDetails
            json_extract_string(profiledetails, '$.personalDetails.gender')
                                                         AS "userGender",
            json_extract_string(profiledetails, '$.personalDetails.category')
                                                         AS "userCategory",
            json_extract_string(profiledetails, '$.personalDetails.primaryEmail')
                                                         AS "userPrimaryEmail",
            json_extract_string(profiledetails, '$.personalDetails.mobile')
                                                         AS "userMobile",
            json_extract_string(profiledetails, '$.personalDetails.phoneVerified')
                                                         AS "_phoneVerified",
            json_extract_string(profiledetails, '$.personalDetails.pincode')
                                                         AS "pincode",
            -- profileDetails top-level
            json_extract_string(profiledetails, '$.profileImageUrl')
                                                         AS "userProfileImgUrl",
            json_extract_string(profiledetails, '$.profileStatus')
                                                         AS "userProfileStatus",
            COALESCE(TRY_CAST(json_extract(profiledetails, '$.verifiedKarmayogi') AS BOOLEAN), false)
                                                         AS "userVerified",
            TRY_CAST(json_extract(profiledetails, '$.mandatoryFieldsExists') AS BOOLEAN)
                                                         AS "userMandatoryFieldsExists",
            -- employmentDetails
            json_extract_string(profiledetails, '$.employmentDetails.departmentName')
                                                         AS "departmentName",
            json_extract_string(profiledetails, '$.employmentDetails.employeeCode')
                                                         AS "employeeCode",
            -- professionalDetails (first element of array)
            json_extract_string(profiledetails, '$.professionalDetails[0].designation')
                                                         AS "designation",
            json_extract_string(profiledetails, '$.professionalDetails[0].group')
                                                         AS "group",
            -- cadreDetails
            json_extract_string(profiledetails, '$.cadreDetails.cadreName')
                                                         AS "cadreName",
            json_extract_string(profiledetails, '$.cadreDetails.civilServiceType')
                                                         AS "civilServiceType",
            json_extract_string(profiledetails, '$.cadreDetails.civilServiceName')
                                                         AS "civilServiceName",
            json_extract_string(profiledetails, '$.cadreDetails.cadreBatch')
                                                         AS "cadreBatch",
            CASE WHEN json_extract(profiledetails, '$.cadreDetails') IS NOT NULL
                      AND json_extract(profiledetails, '$.cadreDetails.isOnCentralDeputation') IS NOT NULL
                 THEN CASE WHEN TRY_CAST(json_extract(profiledetails, '$.cadreDetails.isOnCentralDeputation') AS BOOLEAN)
                           THEN 'true' ELSE 'false' END
                 ELSE NULL
            END                                          AS "isOnCentralDeputation",
            CASE WHEN json_extract(profiledetails, '$.cadreDetails') IS NOT NULL
                 THEN 'Yes' ELSE 'No'
            END                                          AS "organised_service",
            -- additionalProperties (try both spellings)
            COALESCE(
                json_extract(profiledetails, '$.additionalProperties'),
                json_extract(profiledetails, '$.additionalPropertis')
            )                                            AS "additionalProperties",
            -- fullName
            RTRIM(COALESCE(firstname, '') || ' ' || COALESCE(lastname, ''))
                                                         AS "fullName",
            LOWER(json_extract_string(profiledetails, '$.personalDetails.phoneVerified')) = 'true'
                                                         AS "userPhoneVerified"
        FROM raw_user
    ;

        -- Tag extraction from additionalProperties
        ALTER TABLE user_select ADD COLUMN IF NOT EXISTS "Tag" VARCHAR;
        UPDATE user_select
        SET "Tag" = (
            SELECT STRING_AGG(t.val, ', ')
            FROM (
                SELECT UNNEST(
                    CAST(json_extract("additionalProperties", '$.tag') AS VARCHAR[])
                ) AS val
            ) t
        )
        WHERE "additionalProperties" IS NOT NULL
          AND json_extract("additionalProperties", '$.tag') IS NOT NULL
    ;

        -- Roles (grouped comma-separated per user)
        CREATE OR REPLACE TABLE _user_roles AS
        SELECT "userid" AS "userID", STRING_AGG(role, ', ') AS "role"
        FROM (SELECT "userid", role FROM raw_role) sub
        GROUP BY "userid"
    ;

        -- Karma points (sum per user)
        CREATE OR REPLACE TABLE _user_karma AS
        SELECT userid AS "userID", SUM(COALESCE(TRY_CAST(points AS DOUBLE), 0)) AS "total_points"
        FROM raw_user_karma_points
        GROUP BY userid
    ;

        -- Weekly claps
        CREATE OR REPLACE TABLE _user_claps AS
        SELECT userid AS "userID", total_claps AS "weekly_claps_day_before_yesterday"
        FROM raw_weekly_claps
    ;

        -- Final user_computed: user + roles + karma + claps
        CREATE OR REPLACE TABLE user_computed AS
        SELECT
            u.*,
            r."role",
            k."total_points",
            c."weekly_claps_day_before_yesterday"
        FROM user_select u
        LEFT JOIN _user_roles  r ON u."userID" = r."userID"
        LEFT JOIN _user_karma  k ON u."userID" = k."userID"
        LEFT JOIN _user_claps  c ON u."userID" = c."userID"
    """)

    # ==================================================================
    # STAGE 1.9 — Enrolment Master Data
    # ==================================================================
    run_stage(con, "1.9  Enrolment Master Data", """
        CREATE OR REPLACE TABLE enrolment_select AS
        SELECT
            userid                  AS "userID",
            courseid                AS "courseID",
            batchid                 AS "batchID",
            COALESCE(progress, 0)   AS "courseProgress",
            contentstatus           AS "courseContentStatus",
            status                  AS "dbCompletionStatus",
            completedon             AS "courseCompletedTimestamp",
            enrolled_date           AS "courseEnrolledTimestamp",
            lastcontentaccesstime   AS "lastContentAccessTimestamp",
            COALESCE(LIST_LENGTH(issued_certificates), 0)
                                    AS "issuedCertificateCount",
            CASE WHEN COALESCE(LIST_LENGTH(issued_certificates), 0) > 0
                 THEN 1 ELSE 0
            END                     AS "issuedCertificateCountPerContent",
            CASE WHEN COALESCE(LIST_LENGTH(issued_certificates), 0) > 0
                 THEN issued_certificates[-1].lastIssuedOn ELSE ''
            END                     AS "certificateGeneratedOn",
            CASE WHEN COALESCE(LIST_LENGTH(issued_certificates), 0) > 0
                 THEN issued_certificates[1].lastIssuedOn ELSE ''
            END                     AS "firstCompletedOn",
            CASE WHEN COALESCE(LIST_LENGTH(issued_certificates), 0) > 0
                 THEN issued_certificates[-1].identifier ELSE ''
            END                     AS "certificateID",
            lang_contentstatus      AS "langCourseContentStatus"
        FROM raw_enrolment
        WHERE active = true
    ;

        CREATE OR REPLACE TABLE batch_select AS
        SELECT
            courseid         AS "courseID",
            batchid          AS "batchID",
            name             AS "courseBatchName",
            createdby        AS "courseBatchCreatedBy",
            start_date       AS "courseBatchStartDate",
            end_date         AS "courseBatchEndDate",
            COALESCE(TRY_CAST(batch_attributes AS VARCHAR), '{}')
                             AS "courseBatchAttrs"
        FROM raw_batch
    ;

        -- enrolment + batch
        CREATE OR REPLACE TABLE _enrolment_batch AS
        SELECT e.*, b."courseBatchName", b."courseBatchCreatedBy",
               b."courseBatchStartDate", b."courseBatchEndDate", b."courseBatchAttrs"
        FROM enrolment_select e
        LEFT JOIN batch_select b USING ("courseID", "batchID")
    ;

        -- + user rating
        CREATE OR REPLACE TABLE _enrolment_batch_rating AS
        SELECT eb.*, rc."userRating", rc."cbpType", rc."createdOn"
        FROM _enrolment_batch eb
        LEFT JOIN rating_computed rc ON eb."userID" = rc."userID"
                                     AND eb."courseID" = rc."courseID"
    ;

        -- + karma points per (user, course)
        CREATE OR REPLACE TABLE _user_course_karma AS
        SELECT userid AS "userID", context_id AS "courseID",
               SUM(COALESCE(TRY_CAST(points AS DOUBLE), 0)) AS "karma_points"
        FROM raw_user_karma_points
        GROUP BY userid, context_id
    ;

        CREATE OR REPLACE TABLE enrolment_computed AS
        SELECT ebr.*, uck."karma_points"
        FROM _enrolment_batch_rating ebr
        LEFT JOIN _user_course_karma uck ON ebr."userID" = uck."userID"
                                         AND ebr."courseID" = uck."courseID"
    """)

    # ==================================================================
    # STAGE 1.10 — External Enrolment
    # ==================================================================
    run_stage(con, "1.10 External Enrolment", """
        CREATE OR REPLACE TABLE external_enrolment_computed AS
        SELECT *, courseid AS "content_id"
        FROM raw_external_course_enrolments
    """)

    # ==================================================================
    # STAGE 1.11 — Org-User Mapping with Hierarchy
    # ==================================================================
    run_stage(con, "1.11 Org-User Mapping with Hierarchy", """
        CREATE OR REPLACE TABLE user_org_computed AS
        SELECT
            u.*,
            o."orgName"        AS "userOrgName",
            o."orgStatus"      AS "userOrgStatus",
            o."orgCreatedDate" AS "userOrgCreatedDate",
            o."orgType"        AS "userOrgType",
            o."orgSubType"     AS "userOrgSubType",
            o."dept_name",
            o."ministry_name"
        FROM user_computed u
        INNER JOIN org_computed o ON u."userOrgID" = o."orgID"
    """)

    # ==================================================================
    # STAGE 1.12 — Enrolment Warehouse (platform + marketplace UNION)
    # ==================================================================
    run_stage(con, "1.12 Enrolment Warehouse", """
        -- Step A: platform enrolments with completion logic
        CREATE OR REPLACE TABLE _platform_enrolment_with_progress AS
        SELECT
            e.*,
            c."category", c."courseName", c."courseStatus", c."courseDuration",
            c."courseResourceCount", c."courseOrgID", c."courseOrgName",
            c."courseReviewStatus", c."courseLastPublishedOn",
            c."totalRatings", c."rating",
            -- completionPercentage
            CASE
                WHEN c."courseResourceCount" = 0 OR e."courseProgress" = 0 OR e."dbCompletionStatus" = 0 THEN 0.0
                WHEN e."dbCompletionStatus" = 2 THEN 100.0
                ELSE LEAST(100.0, GREATEST(0.0, 100.0 * e."courseProgress" / c."courseResourceCount"))
            END AS "completionPercentage",
            -- userCourseCompletionStatus
            CASE
                WHEN e."dbCompletionStatus" IS NULL THEN 'not-enrolled'
                WHEN e."dbCompletionStatus" = 0 THEN 'not-started'
                WHEN e."dbCompletionStatus" = 1 THEN 'in-progress'
                ELSE 'completed'
            END AS "userCourseCompletionStatus"
        FROM enrolment_computed e
        LEFT JOIN content_computed c ON e."courseID" = c."courseID"
        WHERE c."category" IN ('Course','Program','Blended Program','CuratedCollections','Curated Program')
    ;

        -- Step B: join with user_org
        CREATE OR REPLACE TABLE _platform_full AS
        SELECT p.*, u."fullName", u."userOrgName", u."ministry_name", u."dept_name"
        FROM _platform_enrolment_with_progress p
        LEFT JOIN user_org_computed u ON p."userID" = u."userID"
    ;

        -- Step C: platform enrolment warehouse rows
        CREATE OR REPLACE TABLE _platform_enrolment_wh AS
        SELECT DISTINCT ON ("userID", "courseID", "batchID")
            "userID",
            "courseID"                     AS "content_id",
            CAST("firstCompletedOn" AS VARCHAR)  AS "first_completed_on",
            "userCourseCompletionStatus"   AS "user_consumption_status",
            "certificateID",
            CAST("courseEnrolledTimestamp" AS VARCHAR) AS "enrolled_on",
            "batchID",
            "completionPercentage"         AS "content_progress_percentage"
        FROM _platform_full
    ;

        -- Step D: marketplace enrolments
        CREATE OR REPLACE TABLE _marketplace_enrolment_wh AS
        SELECT DISTINCT ON (ext.userid, ec."content_id", 'Not Available')
            ext.userid                     AS "userID",
            ec."content_id",
            CASE
                WHEN ext.issued_certificates IS NULL THEN ''
                WHEN LIST_LENGTH(ext.issued_certificates) > 0
                    THEN CAST(ext.issued_certificates[1].lastIssuedOn AS VARCHAR)
                ELSE ''
            END                            AS "first_completed_on",
            CASE
                WHEN ext.status IS NULL THEN 'not-enrolled'
                WHEN ext.status = 0 THEN 'not-started'
                WHEN ext.status = 1 THEN 'in-progress'
                ELSE 'completed'
            END                            AS "user_consumption_status",
            CASE
                WHEN ext.issued_certificates IS NULL THEN ''
                WHEN LIST_LENGTH(ext.issued_certificates) > 0
                    THEN ext.issued_certificates[-1].identifier
                ELSE ''
            END                            AS "certificateID",
            CAST(ext.enrolled_date AS VARCHAR) AS "enrolled_on",
            'Not Available'                AS "batchID",
            ext."completionPercentage"     AS "content_progress_percentage"
        FROM external_content_computed ec
        INNER JOIN external_enrolment_computed ext ON ec."content_id" = ext."content_id"
    ;

        -- Step E: union
        CREATE OR REPLACE TABLE enrolment_warehouse_computed AS
        SELECT * FROM _platform_enrolment_wh
        UNION ALL
        SELECT * FROM _marketplace_enrolment_wh
    """)

    # ==================================================================
    # STAGE 1.13 — User Warehouse
    # ==================================================================
    run_stage(con, "1.13 User Warehouse", """
        -- aggregate content duration/completion per user
        CREATE OR REPLACE TABLE _user_content_stats AS
        SELECT
            ew."userID",
            COUNT(DISTINCT CASE WHEN ew."user_consumption_status"
                IN ('not-started','in-progress','completed') THEN ew."content_id" END)
                AS "total_content_enrolments",
            COUNT(DISTINCT CASE WHEN ew."user_consumption_status" = 'completed'
                AND ew."certificateID" IS NOT NULL AND ew."certificateID" <> ''
                THEN ew."content_id" END)
                AS "total_content_completions",
            ROUND(COALESCE(SUM(
                CASE WHEN ew."user_consumption_status" = 'completed'
                    AND ew."certificateID" IS NOT NULL AND ew."certificateID" <> ''
                    AND cc."category" = 'Course'
                    THEN COALESCE(cc."courseDuration", 0) ELSE 0 END
            ) / 3600.0, 0), 2)
                AS "total_content_duration"
        FROM enrolment_warehouse_computed ew
        LEFT JOIN content_computed cc ON ew."content_id" = cc."courseID"
        GROUP BY ew."userID"
    ;

        -- aggregate event duration/completion per user
        CREATE OR REPLACE TABLE _user_event_stats AS
        SELECT
            user_id AS "userID",
            COUNT(DISTINCT CASE WHEN status IN ('not-started','in-progress','completed')
                THEN event_id END)
                AS "total_event_enrolments",
            COUNT(DISTINCT CASE WHEN status = 'completed' THEN event_id END)
                AS "total_event_completions",
            ROUND(COALESCE(SUM(
                CASE WHEN status = 'completed' AND certificate_id IS NOT NULL
                     THEN event_duration_seconds ELSE 0 END
            ) / 3600.0, 0), 2)
                AS "total_event_learning_hours_with_certificates"
        FROM raw_event_enrolment
        GROUP BY user_id
    ;

        CREATE OR REPLACE TABLE user_warehouse_computed AS
        SELECT
            u."userID"                           AS "user_id",
            u."userOrgID"                        AS "mdo_id",
            u."userStatus"                       AS "status",
            COALESCE(u."total_points", 0)        AS "no_of_karma_points",
            u."fullName"                         AS "full_name",
            u."designation",
            u."userPrimaryEmail"                 AS "email",
            u."userMobile"                       AS "phone_number",
            u."pincode",
            u."group"                            AS "groups",
            u."Tag"                              AS "tag",
            u."userProfileStatus"                AS "profile_status",
            u."userCreatedTimestamp"              AS "user_registration_date",
            u."role"                             AS "roles",
            u."userGender"                       AS "gender",
            u."userCategory"                     AS "category",
            CASE WHEN u."userProfileStatus" = 'NOT-MY-USER' THEN true ELSE false END
                                                 AS "marked_as_not_my_user",
            CASE WHEN u."userProfileStatus" = 'VERIFIED' THEN true ELSE false END
                                                 AS "is_verified_karmayogi",
            u."userCreatedBy"                    AS "created_by_id",
            json_extract_string(u."additionalProperties", '$.externalSystem')
                                                 AS "external_system",
            json_extract_string(u."additionalProperties", '$.externalSystemId')
                                                 AS "external_system_id",
            COALESCE(u."weekly_claps_day_before_yesterday", 0)
                                                 AS "weekly_claps_day_before_yesterday",
            COALESCE(ev."total_event_learning_hours_with_certificates", 0)
                                                 AS "total_event_learning_hours",
            COALESCE(cs."total_content_duration", 0)
                                                 AS "total_content_learning_hours",
            COALESCE(ev."total_event_learning_hours_with_certificates", 0)
              + COALESCE(cs."total_content_duration", 0)
                                                 AS "total_learning_hours",
            u."employeeCode"                     AS "employee_id",
            CURRENT_TIMESTAMP                    AS "data_last_generated_on"
        FROM user_org_computed u
        LEFT JOIN _user_content_stats cs ON u."userID" = cs."userID"
        LEFT JOIN _user_event_stats   ev ON u."userID" = ev."userID"
    """)

    # ==================================================================
    # STAGE 1.14 — Content Warehouse
    # (Complex: aggregation + batch join + SCORM detection + marketplace UNION)
    # ==================================================================
    run_stage(con, "1.14 Content Warehouse", """
        -- course progress aggregation
        CREATE OR REPLACE TABLE _content_agg AS
        SELECT
            e."courseID",
            MIN(e."courseCompletedTimestamp")           AS "earliestCourseCompleted",
            MAX(e."courseCompletedTimestamp")           AS "latestCourseCompleted",
            COUNT(*)                                   AS "enrolledUserCount",
            SUM(CASE WHEN
                CASE WHEN e."dbCompletionStatus" IS NULL THEN 'not-enrolled'
                     WHEN e."dbCompletionStatus" = 0 THEN 'not-started'
                     WHEN e."dbCompletionStatus" = 1 THEN 'in-progress'
                     ELSE 'completed' END = 'in-progress' THEN 1 ELSE 0 END)
                                                       AS "inProgressCount",
            SUM(CASE WHEN
                CASE WHEN e."dbCompletionStatus" IS NULL THEN 'not-enrolled'
                     WHEN e."dbCompletionStatus" = 0 THEN 'not-started'
                     WHEN e."dbCompletionStatus" = 1 THEN 'in-progress'
                     ELSE 'completed' END = 'not-started' THEN 1 ELSE 0 END)
                                                       AS "notStartedCount",
            SUM(CASE WHEN
                CASE WHEN e."dbCompletionStatus" IS NULL THEN 'not-enrolled'
                     WHEN e."dbCompletionStatus" = 0 THEN 'not-started'
                     WHEN e."dbCompletionStatus" = 1 THEN 'in-progress'
                     ELSE 'completed' END = 'completed' THEN 1 ELSE 0 END)
                                                       AS "completedCount",
            SUM(e."issuedCertificateCountPerContent")  AS "totalCertificatesIssued"
        FROM enrolment_computed e
        LEFT JOIN content_computed c ON e."courseID" = c."courseID"
        WHERE c."category" IN ('Course','Program','Blended Program','CuratedCollections','Curated Program')
        GROUP BY e."courseID"
    ;

        -- platform content warehouse
        CREATE OR REPLACE TABLE _content_wh_platform AS
        SELECT
            cc."courseID"              AS "content_id",
            cc."courseOrgID"           AS "content_provider_id",
            COALESCE(NULLIF(TRIM(cc."courseOrgName"),''), cc."contentCreator")
                                       AS "content_provider_name",
            cc."courseName"            AS "content_name",
            cc."category"              AS "content_type",
            bs."batchID"               AS "batch_id",
            bs."courseBatchName"       AS "batch_name",
            TRY_CAST(bs."courseBatchStartDate" AS DATE) AS "batch_start_date",
            TRY_CAST(bs."courseBatchEndDate" AS DATE)   AS "batch_end_date",
            CASE WHEN cc."courseDuration" IS NULL OR cc."courseDuration" = 0 THEN ''
                 ELSE LPAD(CAST(CAST(cc."courseDuration" / 3600 AS INT) AS VARCHAR), 2, '0') || ':'
                   || LPAD(CAST(CAST(cc."courseDuration" % 3600 / 60 AS INT) AS VARCHAR), 2, '0') || ':'
                   || LPAD(CAST(CAST(cc."courseDuration" % 60 AS INT) AS VARCHAR), 2, '0')
            END                        AS "content_duration",
            cc."rating"                AS "content_rating",
            TRY_CAST(cc."courseLastPublishedOn" AS DATE) AS "last_published_on",
            CASE WHEN cc."courseStatus" = 'Retired'
                 THEN TRY_CAST(cc."lastStatusChangedOn" AS DATE) END
                                       AS "content_retired_on",
            cc."courseStatus"          AS "content_status",
            cc."courseResourceCount"   AS "resource_count",
            a."totalCertificatesIssued" AS "total_certificates_issued",
            cc."courseReviewStatus"     AS "content_substatus",
            cc."contentLanguage"       AS "language",
            cc."courseCategory"        AS "content_sub_type",
            0                          AS "scorm_flag",
            CURRENT_TIMESTAMP          AS "data_last_generated_on"
        FROM content_computed cc
        LEFT JOIN _content_agg a ON cc."courseID" = a."courseID"
        LEFT JOIN batch_select bs ON cc."courseID" = bs."courseID"
            AND cc."category" = 'Blended Program'
        WHERE cc."courseStatus" IN ('Live','Draft','Retired','Review')
          AND cc."category" IN ('Course','Program','Blended Program','CuratedCollections','Curated Program')
    ;

        -- marketplace content warehouse
        CREATE OR REPLACE TABLE _mkt_enrol_agg AS
        SELECT
            "content_id",
            COUNT(*)                                   AS "enrolledUserCount",
            SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS "inProgressCount",
            SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS "notStartedCount",
            SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS "completedCount",
            SUM(CASE WHEN LIST_LENGTH(issued_certificates) > 0 THEN 1 ELSE 0 END)
                                                       AS "totalCertificatesIssued"
        FROM external_enrolment_computed
        GROUP BY "content_id"
    ;

        CREATE OR REPLACE TABLE _content_wh_marketplace AS
        SELECT
            ec."content_id",
            ec."courseOrgID"           AS "content_provider_id",
            ec."courseOrgName"         AS "content_provider_name",
            ec."courseName"            AS "content_name",
            ec."category"              AS "content_type",
            'Not Available'            AS "batch_id",
            'Not Available'            AS "batch_name",
            NULL::DATE                 AS "batch_start_date",
            NULL::DATE                 AS "batch_end_date",
            ec."courseDuration"        AS "content_duration",
            'Not Available'            AS "content_rating",
            TRY_CAST(ec."courseLastPublishedOn" AS DATE) AS "last_published_on",
            NULL::DATE                 AS "content_retired_on",
            ec."courseStatus"          AS "content_status",
            'Not Available'            AS "resource_count",
            a."totalCertificatesIssued" AS "total_certificates_issued",
            'Not Available'            AS "content_substatus",
            'Not Available'            AS "language",
            'External Content'         AS "content_sub_type",
            '0'                        AS "scorm_flag",
            CURRENT_TIMESTAMP          AS "data_last_generated_on"
        FROM external_content_computed ec
        LEFT JOIN _mkt_enrol_agg a ON ec."content_id" = a."content_id"
    ;

        CREATE OR REPLACE TABLE content_warehouse_computed AS
        SELECT * FROM _content_wh_platform
        UNION ALL
        SELECT * FROM _content_wh_marketplace
    """)

    # ==================================================================
    # STAGE 1.15 — Warehouse Parquet helper tables
    #              (events, event enrolments with karma)
    # ==================================================================
    run_stage(con, "1.15 Warehouse Helper Tables", """
        CREATE OR REPLACE TABLE warehouse_events AS
        SELECT
            event_id, event_name, event_provider_mdo_id,
            event_start_datetime,
            CAST(duration AS VARCHAR) AS duration,
            event_status, event_type, presenters,
            video_link, recording_link, event_tag,
            speaker_id, speaker_name,
            "typeofEvent"       AS type_of_event,
            "maxEnrolments"     AS max_enrolments,
            "meetingAgenda"     AS meeting_agenda,
            "recordedMediaLink" AS recorded_media_link,
            "noOfAttendes"      AS no_of_attendes,
            "eventDuration"     AS event_duration,
            "meetingSummary"    AS meeting_summary,
            "courseLinked"       AS course_linked
        FROM raw_event
    ;

        CREATE OR REPLACE TABLE _event_karma AS
        SELECT userid AS user_id, context_id AS event_id,
               SUM(CASE WHEN TRY_CAST(points AS INT) IS NOT NULL
                        THEN CAST(points AS INT) ELSE 0 END) AS karma_points
        FROM raw_user_karma_points
        GROUP BY userid, context_id
    ;

        CREATE OR REPLACE TABLE warehouse_event_enrolments AS
        SELECT ee.*, ek.karma_points
        FROM raw_event_enrolment ee
        LEFT JOIN _event_karma ek ON ee.user_id = ek.user_id
                                  AND ee.event_id = ek.event_id
    """)

    # ==================================================================
    # STAGE 1.16 — Old Assessment Data
    # ==================================================================
    run_stage(con, "1.16 Old Assessment Data", """
        CREATE OR REPLACE TABLE old_assessment_computed AS
        SELECT
            oa.*,
            cp."courseName", cp."category", cp."courseStatus",
            cp."courseDuration", cp."courseOrgID",
            u."fullName", u."userOrgID", u."userOrgName",
            u."ministry_name", u."dept_name", u."designation",
            u."group", u."userPrimaryEmail", u."userMobile",
            u."Tag",
            'Learning Resource'        AS "assessment_type",
            oa.correct_count + oa.incorrect_count + oa.not_answered_count
                                       AS "total_questions",
            CASE WHEN oa.result_percent >= oa.pass_percent
                 THEN 'Yes' ELSE 'No'
            END                        AS "Pass"
        FROM (
            SELECT *, user_id AS "userID", parent_source_id AS "courseID"
            FROM raw_old_assessment
        ) oa
        LEFT JOIN all_course_program_computed cp ON oa."courseID" = cp."courseID"
        LEFT JOIN user_org_computed u            ON oa."userID" = u."userID"
    """)

    # ==================================================================
    # STAGE 1.17 — ACBP Select (parse only, DuckDB explosion is complex)
    #
    # The full ACBP user-plan matching is preserved in acbpDFUtil_v3.py
    # which already uses DuckDB internally. Here we store the parsed
    # ACBP select table so it can be consumed by that job or new ones.
    # ==================================================================
    run_stage(con, "1.17 ACBP Select", """
        CREATE OR REPLACE TABLE acbp_select AS
        SELECT
            planid                      AS "acbpID",
            status                      AS "acbpStatus",
            createdby                   AS "acbpCreatedBy",
            isapar,
            name                        AS "cbPlanName",
            CAST(enddate AS VARCHAR)    AS "completionDueDate",
            CAST(publishedat AS VARCHAR) AS "allocatedOn",
            contentlist                 AS "acbpCourseIDList",
            contextdata,
            draftdata
        FROM raw_acbp
    """)

    # ==================================================================
    # Clean up intermediate tables
    # ==================================================================
    log.info("Cleaning up intermediate tables...")
    for tbl in ["_user_roles", "_user_karma", "_user_claps",
                "_enrolment_batch", "_enrolment_batch_rating", "_user_course_karma",
                "_platform_enrolment_with_progress", "_platform_full",
                "_platform_enrolment_wh", "_marketplace_enrolment_wh",
                "_user_content_stats", "_user_event_stats",
                "_content_agg", "_content_wh_platform", "_content_wh_marketplace",
                "_mkt_enrol_agg", "_event_karma"]:
        con.execute(f"DROP TABLE IF EXISTS {tbl}")

    # ==================================================================
    # Summary
    # ==================================================================
    log.info("")
    log.info("=" * 72)
    log.info("DATABASE TABLES CREATED:")
    log.info("=" * 72)
    tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """).fetchall()
    for (t,) in tables:
        count = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        log.info(f"  {t:45s} {count:>12,} rows")

    views = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'VIEW'
        ORDER BY table_name
    """).fetchall()
    log.info(f"\nVIEWS (lazy Parquet sources): {len(views)}")
    for (v,) in views:
        log.info(f"  {v}")

    db_size_mb = os.path.getsize(db) / (1024 * 1024)
    total_elapsed = time.time() - total_t0

    log.info("")
    log.info(f"Database file : {db}")
    log.info(f"Database size : {db_size_mb:.1f} MB")
    log.info(f"Total time    : {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")
    log.info("=" * 72)

    con.close()

    # Clean up temp dir
    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return db


# ───────────────────────────────────────────────────────────────────────────
# CLI entry point
# ───────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="DuckDB Initializer — build persistent database from Stage 0 Parquets")
    parser.add_argument("--cache-path", default=DEFAULT_CACHE_PATH,
                        help=f"Root of Stage 0 Parquet cache (default: {DEFAULT_CACHE_PATH})")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH,
                        help=f"Output DuckDB database file (default: {DEFAULT_DB_PATH})")
    parser.add_argument("--memory-limit", default="10GB",
                        help="DuckDB memory budget (default: 10GB)")
    parser.add_argument("--threads", type=int, default=8,
                        help="DuckDB worker threads (default: 8)")
    args = parser.parse_args()

    initialize(
        cache_path=args.cache_path,
        db_path=args.db_path,
        memory_limit=args.memory_limit,
        threads=args.threads,
    )


if __name__ == "__main__":
    main()
