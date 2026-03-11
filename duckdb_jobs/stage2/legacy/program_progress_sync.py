import findspark

findspark.init()
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, explode, collect_list, struct, lit, size
from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType
import duckdb
import os
import shutil

sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from jobs.config import get_environment_config
from jobs.default_config import create_config


class ContentStatusValidationModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.validation.ContentStatusValidationModel"

    def name(self):
        return "ContentStatusValidationModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    def validate_courses(self, spark, config, enrollment_df, consumption_df, content_warehouse_df):
        """FLOW 1: Validate Course type content - Using PySpark (Original working logic)"""
        print("\n" + "=" * 80)
        print("FLOW 1: COURSE VALIDATION (PySpark)")
        print("=" * 80)

        print("\n[STEP 1] Filtering for Course content with language info...")

        # CHANGE 1: Filter for in-progress enrollments only
        enrollment_inprogress = enrollment_df.filter(
            (col("certificateID") == "") | col("certificateID").isNull()
        )
        print(f"  ✓ In-progress enrollments (no certificate): {enrollment_inprogress.count():,}")

        course_data = content_warehouse_df.filter(
            col("content_type") == "Course"
        ).select("content_id", lower(col("language")).alias("language")).distinct()

        course_count = course_data.count()
        print(f"  ✓ Total Courses: {course_count:,}")

        enrollment_courses = enrollment_inprogress.join(
            course_data,
            enrollment_inprogress["courseid"] == course_data["content_id"],
            "inner"
        ).select(
            col("userid"),
            col("courseid"),
            col("batchid"),
            col("langCourseContentStatus"),
            col("language")
        )

        enrollment_courses_count = enrollment_courses.count()
        print(f"  ✓ Enrollment records for Courses: {enrollment_courses_count:,}")

        print("\n[STEP 2] Aggregating consumption data by language...")

        consumption_select = consumption_df.select(
            col("userid"),
            col("courseid"),
            col("batchid"),
            col("language"),
            col("contentid"),
            col("status")
        )

        consumption_aggregated = consumption_select.groupBy("userid", "courseid", "batchid", "language").agg(
            collect_list(struct("contentid", "status")).alias("content_list")
        )

        consumption_agg_count = consumption_aggregated.count()
        print(f"  ✓ Aggregated consumption records: {consumption_agg_count:,}")

        print("\n[STEP 3] Joining and validating...")

        joined_df = enrollment_courses.alias("enr").join(
            consumption_aggregated.alias("con"),
            (col("enr.userid") == col("con.userid")) &
            (col("enr.courseid") == col("con.courseid")) &
            (col("enr.batchid") == col("con.batchid")) &
            (col("enr.language") == col("con.language")),
            "left"
        )

        mismatch_schema = ArrayType(StructType([
            StructField("contentid", StringType(), True),
            StructField("enrollment_status", IntegerType(), True),
            StructField("consumption_status", IntegerType(), True),
            StructField("issue", StringType(), True),
            StructField("language", StringType(), True)
        ]))

        def find_mismatches(lang_contentstatus, consumption_content_list, content_language):
            if lang_contentstatus is None or content_language is None:
                return []

            lang_lower = content_language.lower() if content_language else None
            enrollment_map = lang_contentstatus.get(lang_lower, {}) if lang_lower else {}

            if not enrollment_map:
                return []

            consumption_map = {}
            if consumption_content_list:
                for item in consumption_content_list:
                    if item and 'contentid' in item and 'status' in item:
                        consumption_map[item['contentid']] = item['status']

            mismatches = []

            for contentid, status in enrollment_map.items():
                if contentid not in consumption_map:
                    mismatches.append({
                        'contentid': contentid,
                        'enrollment_status': status,
                        'consumption_status': None,
                        'issue': 'Missing in consumption table',
                        'language': content_language
                    })
                elif consumption_map[contentid] != status:
                    mismatches.append({
                        'contentid': contentid,
                        'enrollment_status': status,
                        'consumption_status': consumption_map[contentid],
                        'issue': 'Status mismatch',
                        'language': content_language
                    })

            for contentid, status in consumption_map.items():
                if contentid not in enrollment_map:
                    mismatches.append({
                        'contentid': contentid,
                        'enrollment_status': None,
                        'consumption_status': status,
                        'issue': 'Extra in consumption table',
                        'language': content_language
                    })

            return mismatches

        find_mismatches_udf = udf(find_mismatches, mismatch_schema)

        validation_df = joined_df.withColumn(
            "mismatches",
            find_mismatches_udf(
                col("enr.langCourseContentStatus"),
                col("con.content_list"),
                col("enr.language")
            )
        )

        mismatched_records = validation_df.filter(
            col("mismatches").isNotNull() & (size(col("mismatches")) > 0)
        ).select(
            col("enr.userid").alias("userid"),
            col("enr.courseid").alias("courseid"),
            col("enr.batchid").alias("batchid"),
            col("enr.language").alias("language"),
            col("mismatches")
        )

        mismatched_detailed = mismatched_records.withColumn(
            "mismatch", explode(col("mismatches"))
        ).select(
            lit("Course").alias("content_type"),
            "userid",
            "courseid",
            "batchid",
            "language",
            col("mismatch.contentid").alias("contentid"),
            col("mismatch.enrollment_status").alias("enrollment_status"),
            col("mismatch.consumption_status").alias("consumption_status"),
            col("mismatch.issue").alias("issue")
        )

        return mismatched_detailed

    def validate_programs_duckdb(self, spark, config, cached_consumption_path):
        """FLOW 2: Validate Program type content - Using DuckDB (CORRECTED LOGIC)"""
        print("\n" + "=" * 80)
        print("FLOW 2: PROGRAM VALIDATION (DuckDB)")
        print("=" * 80)

        temp_dir = None

        try:
            # Create temp directory
            project_dir = str(Path(__file__).resolve().parents[3])
            temp_dir = f"{project_dir}/temp_program_validation"
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            os.makedirs(temp_dir, exist_ok=True)

            print(f"\n[STEP 1] Exporting data to temp parquet files...")
            print(f"  Temp directory: {temp_dir}")

            # Read and export enrollment data - ALL enrollments (for metric 3)
            enrollment_df = spark.read.parquet(ParquetFileConstants.ENROLMENT_SELECT_PARQUET_FILE)
            enrollment_path = f"{temp_dir}/enrollment.parquet"
            enrollment_df.select("userid", "courseid", "batchid", "courseContentStatus", "certificateID").write.mode(
                "overwrite").parquet(enrollment_path)
            print(f"  ✓ Exported enrollment data")

            # Read consumption from cache
            print(f"  ✓ Using cached consumption data")
            consumption_df = spark.read.parquet(cached_consumption_path)
            consumption_path = f"{temp_dir}/consumption.parquet"
            consumption_df.select("userid", "courseid", "batchid", "contentid", "status").write.mode(
                "overwrite").parquet(consumption_path)
            print(f"  ✓ Exported consumption data")

            # Read and export content warehouse
            content_warehouse_df = spark.read.parquet(ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE)
            content_path = f"{temp_dir}/content.parquet"
            content_warehouse_df.filter(col("content_type").like("%Program%")).select("content_id",
                                                                                      "content_sub_type").write.mode(
                "overwrite").parquet(content_path)
            print(f"  ✓ Exported program IDs")

            # CHANGE 2: Export content_resource with resource_type filter
            content_resource_path = f"{config.warehouseReportDir}/{config.dwContentResourceTable}"
            content_resource_df = spark.read.parquet(content_resource_path)
            resource_path = f"{temp_dir}/content_resource.parquet"
            content_resource_df.select("content_id", "resource_id", "resource_type").write.mode("overwrite").parquet(
                resource_path)
            print(f"  ✓ Exported content_resource data")

            print("\n[STEP 2] Initializing DuckDB...")

            db_path = f"{temp_dir}/validation.duckdb"
            con = duckdb.connect(database=db_path)
            con.execute(f"SET temp_directory='{temp_dir}'")
            con.execute("SET memory_limit='8GB'")

            print("\n[STEP 3] Processing with DuckDB...")

            # Get program IDs
            con.execute(f"""
                CREATE OR REPLACE VIEW programs AS
                SELECT * FROM read_parquet('{content_path}/**.parquet')
            """)

            program_count = con.execute("SELECT COUNT(*) FROM programs").fetchone()[0]
            print(f"  ✓ Programs: {program_count:,}")

            # CHANGE 2: Get program-child relationships - ONLY Course type children
            con.execute(f"""
                CREATE OR REPLACE VIEW program_children AS
                SELECT 
                    cr.content_id as program_id,
                    cr.resource_id as child_course_id
                FROM read_parquet('{resource_path}/**.parquet') cr
                INNER JOIN programs p ON cr.content_id = p.content_id
                WHERE cr.resource_type = 'Course'
            """)

            children_count = con.execute("SELECT COUNT(*) FROM program_children").fetchone()[0]
            print(f"  ✓ Program-Children relationships (Course type only): {children_count:,}")

            # Get all enrollments
            con.execute(f"""
                CREATE OR REPLACE VIEW enrollments AS
                SELECT * FROM read_parquet('{enrollment_path}/**.parquet')
            """)

            # Get all consumption
            con.execute(f"""
                CREATE OR REPLACE VIEW consumption AS
                SELECT * FROM read_parquet('{consumption_path}/**.parquet')
            """)

            print("\n[STEP 4] Finding program enrollments and their child course enrollments...")

            # Get program enrollments - CHANGE 1: Only in-progress (no certificate)
            con.execute("""
                CREATE OR REPLACE TABLE program_enrollments AS
                SELECT 
                    e.userid,
                    e.courseid as program_id,
                    e.batchid as program_batchid,
                    e.courseContentStatus,
                    e.certificateID
                FROM enrollments e
                INNER JOIN programs p ON e.courseid = p.content_id
                WHERE e.certificateID IS NULL OR e.certificateID = ''
            """)

            prog_enr_count = con.execute("SELECT COUNT(*) FROM program_enrollments").fetchone()[0]
            print(f"  ✓ Program enrollments (in-progress only): {prog_enr_count:,}")

            # For each program enrollment, find the child course enrollments for the SAME USER
            con.execute("""
                CREATE OR REPLACE TABLE user_child_enrollments AS
                SELECT 
                    pe.userid,
                    pe.program_id,
                    pe.program_batchid,
                    pc.child_course_id,
                    ce.batchid as child_batchid,
                    ce.certificateID as child_certificateID
                FROM program_enrollments pe
                INNER JOIN program_children pc ON pe.program_id = pc.program_id
                INNER JOIN enrollments ce ON pe.userid = ce.userid AND pc.child_course_id = ce.courseid
            """)

            child_enr_count = con.execute("SELECT COUNT(*) FROM user_child_enrollments").fetchone()[0]
            print(f"  ✓ Child course enrollments (same users, Course type only): {child_enr_count:,}")

            print("\n[STEP 5] Getting consumption for child courses (same users)...")

            # Get consumption records for child courses - SAME USER, SAME COURSE, SAME BATCH
            con.execute("""
                CREATE OR REPLACE TABLE child_consumption AS
                SELECT 
                    uce.userid,
                    uce.program_id,
                    uce.program_batchid,
                    c.contentid,
                    c.status
                FROM user_child_enrollments uce
                INNER JOIN consumption c 
                    ON uce.userid = c.userid 
                    AND uce.child_course_id = c.courseid 
                    AND uce.child_batchid = c.batchid
            """)

            child_cons_count = con.execute("SELECT COUNT(*) FROM child_consumption").fetchone()[0]
            print(f"  ✓ Child consumption records (same users): {child_cons_count:,}")

            # Aggregate expected content for each program enrollment (userid + program_id + program_batchid)
            con.execute("""
                CREATE OR REPLACE TABLE program_expected AS
                SELECT 
                    userid,
                    program_id,
                    program_batchid,
                    contentid,
                    status as expected_status
                FROM child_consumption
            """)

            print("\n[STEP 6] Finding mismatches...")

            con.execute("""
                CREATE OR REPLACE TABLE program_mismatches AS
                WITH expected_agg AS (
                    SELECT 
                        userid,
                        program_id,
                        program_batchid,
                        contentid,
                        expected_status
                    FROM program_expected
                ),
                enrollment_exploded AS (
                    SELECT 
                        userid,
                        program_id,
                        program_batchid,
                        unnest(json_keys(courseContentStatus)) as contentid,
                        CAST(json_extract(courseContentStatus, '$.' || unnest(json_keys(courseContentStatus))) AS INTEGER) as enrollment_status
                    FROM program_enrollments
                    WHERE courseContentStatus IS NOT NULL
                )
                -- Missing in program contentStatus
                SELECT 
                    'Program' as content_type,
                    ea.userid,
                    ea.program_id as courseid,
                    ea.program_batchid as batchid,
                    NULL as language,
                    ea.contentid,
                    NULL as enrollment_status,
                    ea.expected_status as consumption_status,
                    'Missing in program contentStatus' as issue
                FROM expected_agg ea
                LEFT JOIN enrollment_exploded ee 
                    ON ea.userid = ee.userid 
                    AND ea.program_id = ee.program_id 
                    AND ea.program_batchid = ee.program_batchid
                    AND ea.contentid = ee.contentid
                WHERE ee.contentid IS NULL

                UNION ALL

                -- Status mismatch
                SELECT 
                    'Program' as content_type,
                    ee.userid,
                    ee.program_id as courseid,
                    ee.program_batchid as batchid,
                    NULL as language,
                    ee.contentid,
                    ee.enrollment_status,
                    ea.expected_status as consumption_status,
                    'Status mismatch in program' as issue
                FROM enrollment_exploded ee
                INNER JOIN expected_agg ea 
                    ON ee.userid = ea.userid 
                    AND ee.program_id = ea.program_id 
                    AND ee.program_batchid = ea.program_batchid
                    AND ee.contentid = ea.contentid
                WHERE ee.enrollment_status != ea.expected_status

                UNION ALL

                -- Extra in program contentStatus
                SELECT 
                    'Program' as content_type,
                    ee.userid,
                    ee.program_id as courseid,
                    ee.program_batchid as batchid,
                    NULL as language,
                    ee.contentid,
                    ee.enrollment_status,
                    NULL as consumption_status,
                    'Extra in program contentStatus' as issue
                FROM enrollment_exploded ee
                LEFT JOIN expected_agg ea 
                    ON ee.userid = ea.userid 
                    AND ee.program_id = ea.program_id 
                    AND ee.program_batchid = ea.program_batchid
                    AND ee.contentid = ea.contentid
                WHERE ea.contentid IS NULL
            """)

            mismatch_count = con.execute("SELECT COUNT(*) FROM program_mismatches").fetchone()[0]
            print(f"  ✓ Program mismatches found: {mismatch_count:,}")

            # CHANGE 3: Calculate metric - programs without cert but all children have certs
            print("\n[STEP 7] Calculating certificate mismatch metric...")

            con.execute("""
                CREATE OR REPLACE TABLE program_cert_mismatch AS
                WITH program_child_cert_status AS (
                    SELECT 
                        pe.userid,
                        pe.program_id,
                        pe.program_batchid,
                        p.content_sub_type,
                        COUNT(DISTINCT pc.child_course_id) as total_children,
                        SUM(CASE 
                            WHEN ce.certificateID IS NOT NULL AND ce.certificateID != '' 
                            THEN 1 ELSE 0 
                        END) as children_with_cert
                    FROM program_enrollments pe
                    INNER JOIN programs p ON pe.program_id = p.content_id
                    INNER JOIN program_children pc ON pe.program_id = pc.program_id
                    LEFT JOIN enrollments ce 
                        ON pe.userid = ce.userid 
                        AND pc.child_course_id = ce.courseid
                    GROUP BY pe.userid, pe.program_id, pe.program_batchid, p.content_sub_type
                )
                SELECT 
                    userid,
                    program_id,
                    content_sub_type,
                    program_batchid,
                    total_children,
                    children_with_cert
                FROM program_child_cert_status
                WHERE total_children > 0 
                AND children_with_cert = total_children
            """)

            cert_mismatch_count = con.execute("SELECT COUNT(*) FROM program_cert_mismatch").fetchone()[0]
            print(f"  ✓ Programs without cert but all children have certs: {cert_mismatch_count:,}")

            # NEW METRIC 1: Course Assessment validation
            print("\n[STEP 7.5] Programs with all assessments completed...")

            # Get Course Assessment children
            con.execute(f"""
                CREATE OR REPLACE VIEW program_assessment_children AS
                SELECT 
                    cr.content_id as program_id,
                    cr.resource_id as assessment_id
                FROM read_parquet('{resource_path}/**.parquet') cr
                INNER JOIN programs p ON cr.content_id = p.content_id
                WHERE cr.resource_type = 'Course Assessment'
            """)

            # Get programs where all Course children have certs AND all assessments are status 2
            con.execute("""
                CREATE OR REPLACE TABLE program_assessment_ready AS
                WITH programs_all_courses_certified AS (
                    SELECT 
                        pe.userid,
                        pe.program_id,
                        pe.program_batchid,
                        p.content_sub_type,
                        COUNT(DISTINCT pc.child_course_id) as total_children,
                        SUM(CASE 
                            WHEN ce.certificateID IS NOT NULL AND ce.certificateID != '' 
                            THEN 1 ELSE 0 
                        END) as children_with_cert
                    FROM program_enrollments pe
                    INNER JOIN programs p ON pe.program_id = p.content_id
                    INNER JOIN program_children pc ON pe.program_id = pc.program_id
                    LEFT JOIN enrollments ce 
                        ON pe.userid = ce.userid 
                        AND pc.child_course_id = ce.courseid
                    GROUP BY pe.userid, pe.program_id, pe.program_batchid, p.content_sub_type
                    HAVING COUNT(DISTINCT pc.child_course_id) > 0 
                    AND SUM(CASE WHEN ce.certificateID IS NOT NULL AND ce.certificateID != '' THEN 1 ELSE 0 END) = COUNT(DISTINCT pc.child_course_id)
                ),
                assessment_status AS (
                    SELECT 
                        pacc.userid,
                        pacc.program_id,
                        pacc.program_batchid,
                        pacc.content_sub_type,
                        pac.assessment_id,
                        c.status as assessment_status
                    FROM programs_all_courses_certified pacc
                    INNER JOIN program_assessment_children pac ON pacc.program_id = pac.program_id
                    LEFT JOIN consumption c 
                        ON pacc.userid = c.userid 
                        AND pac.assessment_id = c.contentid
                ),
                assessment_summary AS (
                    SELECT 
                        userid,
                        program_id,
                        program_batchid,
                        content_sub_type,
                        COUNT(DISTINCT assessment_id) as total_assessments,
                        SUM(CASE WHEN assessment_status = 2 THEN 1 ELSE 0 END) as assessments_completed,
                        SUM(CASE WHEN assessment_status = 1 THEN 1 ELSE 0 END) as assessments_in_progress,
                        SUM(CASE WHEN assessment_status IS NULL THEN 1 ELSE 0 END) as assessments_not_started
                    FROM assessment_status
                    GROUP BY userid, program_id, program_batchid, content_sub_type
                )
                SELECT * FROM assessment_summary
                WHERE total_assessments = assessments_completed
            """)

            metric1_count = con.execute("SELECT COUNT(*) FROM program_assessment_ready").fetchone()[0]
            print(f"  ✓ Programs ready for cert (all assessments completed): {metric1_count:,}")

            # Export results
            output_path = f"{temp_dir}/program_mismatches.parquet"
            con.execute(f"""
                COPY (SELECT * FROM program_mismatches)
                TO '{output_path}' (FORMAT PARQUET)
            """)

            # Export metric 3
            metric3_output_path = f"{temp_dir}/program_cert_mismatch.parquet"
            con.execute(f"""
                COPY (SELECT * FROM program_cert_mismatch)
                TO '{metric3_output_path}' (FORMAT PARQUET)
            """)

            # Export metric 1
            metric1_output_path = f"{temp_dir}/program_assessment_ready.parquet"
            con.execute(f"""
                COPY (SELECT * FROM program_assessment_ready)
                TO '{metric1_output_path}' (FORMAT PARQUET)
            """)

            con.close()

            print("\n[STEP 8] Reading results back into Spark...")

            # Read back into Spark BEFORE cleanup
            if mismatch_count > 0:
                program_mismatches_df = spark.read.parquet(output_path)
                program_mismatches_df.cache()
                actual_count = program_mismatches_df.count()
                print(f"  ✓ Loaded {actual_count:,} mismatches into Spark")
            else:
                program_mismatches_df = spark.createDataFrame([], StructType([
                    StructField("content_type", StringType(), True),
                    StructField("userid", StringType(), True),
                    StructField("courseid", StringType(), True),
                    StructField("batchid", StringType(), True),
                    StructField("language", StringType(), True),
                    StructField("contentid", StringType(), True),
                    StructField("enrollment_status", IntegerType(), True),
                    StructField("consumption_status", IntegerType(), True),
                    StructField("issue", StringType(), True)
                ]))

            # Read metric 3 results
            if cert_mismatch_count > 0:
                cert_mismatch_df = spark.read.parquet(metric3_output_path)
                cert_mismatch_df.cache()
                cert_mismatch_df.count()
            else:
                cert_mismatch_df = None

            # Read metric 1 results
            if metric1_count > 0:
                metric1_df = spark.read.parquet(metric1_output_path)
                metric1_df.cache()
                metric1_df.count()
            else:
                metric1_df = None

            # NOW cleanup after Spark has loaded the data
            print("\n[STEP 9] Cleaning up temp files...")
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                print(f"  ✓ Cleaned up {temp_dir}")

            return program_mismatches_df, cert_mismatch_df, metric1_df

        except Exception as e:
            print(f"\n❌ Error in DuckDB program validation: {str(e)}")
            import traceback
            traceback.print_exc()

            # Cleanup on error
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

            return spark.createDataFrame([], StructType([
                StructField("content_type", StringType(), True),
                StructField("userid", StringType(), True),
                StructField("courseid", StringType(), True),
                StructField("batchid", StringType(), True),
                StructField("language", StringType(), True),
                StructField("contentid", StringType(), True),
                StructField("enrollment_status", IntegerType(), True),
                StructField("consumption_status", IntegerType(), True),
                StructField("issue", StringType(), True)
            ])), None, None

    def get_courses_ready_for_cert(self, spark, config, cached_consumption_path):
        """NEW METRIC 2: Get courses where all resources are status 2 but no certificate issued"""
        print("\n[METRIC 2] Courses ready for certification...")

        temp_dir = None
        try:
            project_dir = str(Path(__file__).resolve().parents[3])
            temp_dir = f"{project_dir}/temp_course_cert_metric"
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            os.makedirs(temp_dir, exist_ok=True)

            # Export required data
            enrollment_df = spark.read.parquet(ParquetFileConstants.ENROLMENT_SELECT_PARQUET_FILE)
            enrollment_path = f"{temp_dir}/enrollment.parquet"
            enrollment_df.select("userid", "courseid", "batchid", "certificateID",
                                 "langCourseContentStatus").write.mode("overwrite").parquet(enrollment_path)

            consumption_df = spark.read.parquet(cached_consumption_path)
            consumption_path = f"{temp_dir}/consumption.parquet"
            consumption_df.select("userid", "courseid", "batchid", "contentid", "status", "language").write.mode(
                "overwrite").parquet(consumption_path)

            # Export content warehouse with content_sub_type
            content_warehouse_df = spark.read.parquet(ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE)
            content_path = f"{temp_dir}/content.parquet"
            content_warehouse_df.filter(col("content_type") == "Course").select("content_id",
                                                                                "content_sub_type").write.mode(
                "overwrite").parquet(content_path)

            # Export content_resource to get all children of courses
            content_resource_path = f"{config.warehouseReportDir}/{config.dwContentResourceTable}"
            content_resource_df = spark.read.parquet(content_resource_path)
            resource_path = f"{temp_dir}/content_resource.parquet"
            content_resource_df.select("content_id", "resource_id").write.mode("overwrite").parquet(resource_path)

            # DuckDB processing
            db_path = f"{temp_dir}/course_cert.duckdb"
            con = duckdb.connect(database=db_path)
            con.execute(f"SET temp_directory='{temp_dir}'")

            con.execute(f"CREATE VIEW enrollments AS SELECT * FROM read_parquet('{enrollment_path}/**.parquet')")
            con.execute(f"CREATE VIEW consumption AS SELECT * FROM read_parquet('{consumption_path}/**.parquet')")
            con.execute(f"CREATE VIEW courses AS SELECT * FROM read_parquet('{content_path}/**.parquet')")
            con.execute(f"CREATE VIEW content_resource AS SELECT * FROM read_parquet('{resource_path}/**.parquet')")

            print("  → Processing with DuckDB...")

            # FIXED: Match consumption by LANGUAGE to avoid counting duplicates
            con.execute("""
                CREATE TABLE courses_ready_for_cert AS
                WITH course_enrollments AS (
                    SELECT 
                        e.userid,
                        e.courseid,
                        e.batchid,
                        e.langCourseContentStatus
                    FROM enrollments e
                    INNER JOIN courses c ON e.courseid = c.content_id
                    WHERE e.certificateID IS NULL OR e.certificateID = ''
                ),
                -- Get expected children count from content_resource
                course_children AS (
                    SELECT 
                        cr.content_id as courseid,
                        COUNT(DISTINCT cr.resource_id) as expected_children_count
                    FROM content_resource cr
                    INNER JOIN courses c ON cr.content_id = c.content_id
                    GROUP BY cr.content_id
                ),
                enrollment_languages AS (
                    SELECT 
                        ce.userid,
                        ce.courseid,
                        ce.batchid,
                        ce.langCourseContentStatus,
                        unnest(json_keys(langCourseContentStatus)) as language
                    FROM course_enrollments ce
                    WHERE langCourseContentStatus IS NOT NULL
                ),
                enrollment_resources AS (
                    SELECT 
                        el.userid,
                        el.courseid,
                        el.batchid,
                        el.language,
                        unnest(json_keys(json_extract(langCourseContentStatus, '$.' || el.language))) as contentid,
                        CAST(json_extract(
                            langCourseContentStatus, 
                            '$.' || el.language || '.' || unnest(json_keys(json_extract(langCourseContentStatus, '$.' || el.language)))
                        ) AS INTEGER) as expected_status
                    FROM enrollment_languages el
                ),
                -- FIXED: Join consumption by language to match exact records
                consumption_summary AS (
                    SELECT 
                        er.userid,
                        er.courseid,
                        er.batchid,
                        er.language,
                        COUNT(DISTINCT er.contentid) as total_resources_in_enrollment,
                        COUNT(DISTINCT c.contentid) as total_consumption_started,
                        SUM(CASE WHEN c.status = 2 THEN 1 ELSE 0 END) as resources_completed
                    FROM enrollment_resources er
                    LEFT JOIN consumption c 
                        ON er.userid = c.userid 
                        AND er.courseid = c.courseid 
                        AND er.batchid = c.batchid
                        AND er.contentid = c.contentid
                        AND LOWER(er.language) = LOWER(c.language)
                    GROUP BY er.userid, er.courseid, er.batchid, er.language
                )
                -- Final filter: consumption count = expected count AND all status = 2
                SELECT 
                    cs.userid,
                    cs.courseid,
                    cs.batchid,
                    cs.language,
                    cs.total_resources_in_enrollment,
                    cc.expected_children_count,
                    cs.total_consumption_started,
                    cs.resources_completed
                FROM consumption_summary cs
                LEFT JOIN course_children cc ON cs.courseid = cc.courseid
                WHERE cs.total_resources_in_enrollment > 0
                AND cs.total_consumption_started = cs.total_resources_in_enrollment
                AND (cc.expected_children_count IS NULL OR cs.total_consumption_started = cc.expected_children_count)
                AND cs.resources_completed = cs.total_resources_in_enrollment
            """)

            metric2_count = con.execute("SELECT COUNT(*) FROM courses_ready_for_cert").fetchone()[0]
            print(f"  ✓ Courses ready for certification: {metric2_count:,}")

            # Export metric 2
            metric2_output_path = f"{temp_dir}/courses_ready_for_cert.parquet"
            con.execute(f"""
                COPY (SELECT * FROM courses_ready_for_cert)
                TO '{metric2_output_path}' (FORMAT PARQUET)
            """)

            con.close()

            # Read back into Spark and join with content to get content_sub_type
            if metric2_count > 0:
                metric2_df = spark.read.parquet(metric2_output_path)

                # Join with content warehouse to get content_sub_type
                content_with_subtype = spark.read.parquet(ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE) \
                    .filter(col("content_type") == "Course") \
                    .select("content_id", "content_sub_type")

                metric2_df = metric2_df.join(
                    content_with_subtype,
                    metric2_df["courseid"] == content_with_subtype["content_id"],
                    "left"
                ).select(
                    metric2_df["userid"],
                    metric2_df["courseid"],
                    content_with_subtype["content_sub_type"],
                    metric2_df["batchid"],
                    metric2_df["language"],
                    metric2_df["total_resources_in_enrollment"],
                    metric2_df["expected_children_count"],
                    metric2_df["total_consumption_started"],
                    metric2_df["resources_completed"]
                )

                metric2_df.cache()
                metric2_df.count()
            else:
                metric2_df = None

            # Cleanup
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

            return metric2_df

        except Exception as e:
            print(f"\n❌ Error in course cert metric: {str(e)}")
            import traceback
            traceback.print_exc()
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            return None

    def process_data(self, spark, config):
        try:
            print("=" * 80)
            print("CONTENT STATUS VALIDATION - COURSES & PROGRAMS")
            print("=" * 80)
            print(f"Validation started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

            # Read common data
            print("[LOADING DATA] Reading data for validation...")

            enrollment_df = spark.read.parquet(ParquetFileConstants.ENROLMENT_SELECT_PARQUET_FILE)
            print(f"  ✓ Enrollment table loaded")

            # CACHE CASSANDRA CONSUMPTION DATA
            cache_path = getattr(config, 'baseCachePath', '/home/analytics/pyspark/data-res/pq_files/cache_pq/')
            cached_consumption_path = f"{cache_path}/consumption_v2"

            if os.path.exists(cached_consumption_path):
                print(f"  ✓ Using cached consumption from: {cached_consumption_path}")
                consumption_df = spark.read.parquet(cached_consumption_path)
            else:
                print(f"  → Reading consumption from Cassandra (this will take time)...")
                consumption_df = spark.read \
                    .format("org.apache.spark.sql.cassandra") \
                    .options(table="user_content_consumption_v2", keyspace="sunbird_courses") \
                    .load()
                print(f"  → Caching consumption to: {cached_consumption_path}")
                consumption_df.write.mode("overwrite").parquet(cached_consumption_path)
                print(f"  ✓ Cached consumption data")

            print(f"  ✓ Consumption table loaded")

            content_warehouse_df = spark.read.parquet(ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE)
            print(f"  ✓ Content warehouse loaded")

            # FLOW 1: Validate courses (PySpark - working logic)
            course_mismatches = self.validate_courses(spark, config, enrollment_df, consumption_df,
                                                      content_warehouse_df)
            course_mismatch_count = course_mismatches.count()

            # FLOW 2: Validate programs (DuckDB - pass cached consumption path)
            program_mismatches, cert_mismatch_df, metric1_df = self.validate_programs_duckdb(spark, config,
                                                                                             cached_consumption_path)
            program_mismatch_count = program_mismatches.count()

            # NEW METRIC 2: Courses ready for certification
            metric2_df = self.get_courses_ready_for_cert(spark, config, cached_consumption_path)

            # Combine results
            all_mismatches = course_mismatches.union(program_mismatches)
            total_mismatch_count = all_mismatches.count()

            # Summary
            print("\n" + "=" * 80)
            print("VALIDATION SUMMARY")
            print("=" * 80)
            print(f"Course mismatches:                         {course_mismatch_count:>10,}")
            print(f"Program mismatches:                        {program_mismatch_count:>10,}")
            print(f"Total mismatches:                          {total_mismatch_count:>10,}")

            # Display certificate mismatch metric
            if cert_mismatch_df is not None:
                cert_mismatch_count = cert_mismatch_df.count()
                print(f"Programs (no cert) with all children (certs): {cert_mismatch_count:>10,}")
            else:
                print(f"Programs (no cert) with all children (certs): {0:>10,}")

            # Display metric 1
            if metric1_df is not None:
                metric1_count = metric1_df.count()
                print(f"Programs ready for cert (assessments done):    {metric1_count:>10,}")
            else:
                print(f"Programs ready for cert (assessments done):    {0:>10,}")

            # Display metric 2
            if metric2_df is not None:
                metric2_count = metric2_df.count()
                print(f"Courses ready for cert (all resources done):   {metric2_count:>10,}")
            else:
                print(f"Courses ready for cert (all resources done):   {0:>10,}")

            print("=" * 80)

            if total_mismatch_count > 0:
                print("\n" + "=" * 80)
                print("MISMATCH BREAKDOWN BY ISSUE TYPE")
                print("=" * 80)
                all_mismatches.groupBy("content_type", "issue").count().orderBy("content_type", "issue").show(
                    truncate=False)

                # Write output in multiple partitions
                output_path = f"/tmp/content_status_mismatches_filtered_{self.get_date()}"
                all_mismatches.repartition(20).write.mode("overwrite").csv(output_path, header=True)
                print(f"\n✓ Detailed mismatch report written to: {output_path}")
            else:
                print("\n✅ SUCCESS: All records are perfectly synchronized!")

            # Write certificate mismatch report
            if cert_mismatch_df is not None and cert_mismatch_df.count() > 0:
                cert_output_path = f"/tmp/program_cert_mismatch_{self.get_date()}"
                cert_mismatch_df.coalesce(1).write.mode("overwrite").csv(cert_output_path, header=True)
                print(f"✓ Certificate mismatch report written to: {cert_output_path}")

            # Write metric 1 report
            if metric1_df is not None and metric1_df.count() > 0:
                metric1_output_path = f"/tmp/program_assessment_ready_{self.get_date()}"
                metric1_df.coalesce(1).write.mode("overwrite").csv(metric1_output_path, header=True)
                print(f"✓ Program assessment ready report written to: {metric1_output_path}")

            # Write metric 2 report
            if metric2_df is not None and metric2_df.count() > 0:
                metric2_output_path = f"/tmp/courses_ready_for_cert_{self.get_date()}"
                metric2_df.coalesce(1).write.mode("overwrite").csv(metric2_output_path, header=True)
                print(f"✓ Courses ready for certification report written to: {metric2_output_path}")

            print("\n" + "=" * 80)
            print(f"Validation completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 80 + "\n")

        except Exception as e:
            print("\n" + "=" * 80)
            print("❌ ERROR OCCURRED")
            print("=" * 80)
            print(f"Error: {str(e)}")
            import traceback
            traceback.print_exc()
            raise


def main():
    import os
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 pyspark-shell'

    config_dict = get_environment_config()
    config = create_config(config_dict)

    spark = SparkSession.builder \
        .appName("Content Status Validation - Courses & Programs") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "15g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.cassandra.connection.host", config.sparkCassandraConnectionHost) \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.connection.timeoutMS", "600000") \
        .config("spark.cassandra.read.timeoutMS", "600000") \
        .config("spark.cassandra.connection.keepAliveMS", "600000") \
        .config("spark.cassandra.input.fetch.sizeInRows", "1000") \
        .config("spark.cassandra.input.split.sizeInMB", "64") \
        .config("spark.cassandra.input.consistency.level", "LOCAL_ONE") \
        .getOrCreate()

    start_time = datetime.now()
    print(f"\n{'=' * 80}")
    print(f"JOB STARTED: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 80}\n")

    model = ContentStatusValidationModel()
    model.process_data(spark, config)

    end_time = datetime.now()
    duration = end_time - start_time

    print(f"\n{'=' * 80}")
    print(f"JOB COMPLETED: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total duration: {duration}")
    print(f"{'=' * 80}\n")

    spark.stop()


if __name__ == "__main__":
    main()
