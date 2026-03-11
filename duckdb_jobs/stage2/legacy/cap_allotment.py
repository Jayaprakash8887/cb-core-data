import findspark

findspark.init()
import sys
from pathlib import Path
import os
import duckdb
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, from_json, explode, array_join,
    lower, concat_ws, regexp_replace, split, trim, monotonically_increasing_id
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.utils import utils
from jobs.config import get_environment_config
from jobs.default_config import create_config


class CAPAccessControlModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.CAPAccessControlModel"

    def name(self):
        return "CAPAccessControlModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def current_date_time():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def get_access_control_schema(self):
        """Schema for parsing contextdata JSON"""
        return StructType([
            StructField("contentId", StringType(), True),
            StructField("accessControl", StructType([
                StructField("version", StringType(), True),
                StructField("userGroups", ArrayType(StructType([
                    StructField("userGroupId", StringType(), True),
                    StructField("userGroupName", StringType(), True),
                    StructField("userGroupCriteriaList", ArrayType(StructType([
                        StructField("criteriaKey", StringType(), True),
                        StructField("criteriaValue", ArrayType(StringType()), True)
                    ])), True)
                ])), True)
            ]), True)
        ])

    def process_data(self, spark, conf):
        try:
            print("=" * 80)
            print("CAP Access Control Processing - DuckDB Optimized")
            print("=" * 80)

            start_time = time.time()

            warehouse_path = conf.warehouseReportDir
            output_path = getattr(conf, 'baseCachePath', '/home/analytics/pyspark/data-res/pq_files/cache_pq/')

            # Create temp directory
            project_dir = str(Path(__file__).resolve().parents[3])
            temp_dir = f"{project_dir}/temp_cap_duckdb"
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            os.makedirs(temp_dir, exist_ok=True)

            print(f"\n[1/5] Processing CAP access control data...")
            print(f"  Temp directory: {temp_dir}")

            # Read content parquet
            content_df = spark.read.parquet(f"{warehouse_path}/{conf.dwCourseTable}")

            # Read access settings from cache
            access_settings_df = spark.read.parquet(f"{output_path}/accessControlSettings")

            # Filter Live CAPs from content
            cap_df = content_df.filter(
                (col("content_sub_type") == "Comprehensive Assessment Program") &
                (col("content_status") == "Live")
            ).select("content_id", "content_name", "content_status", "content_provider_id")

            cap_count = cap_df.count()
            print(f"  Live CAPs found: {cap_count:,}")

            # Filter access settings for these CAP IDs
            cap_access_df = access_settings_df.filter(
                col("contextidtype") == "Comprehensive Assessment Program"
            ).join(
                cap_df,
                access_settings_df["contextid"] == cap_df["content_id"],
                "inner"
            )

            print(f"  CAPs with access control: {cap_access_df.count():,}")

            # CRITICAL: Pre-process contextdata to wrap scalar criteriaValue in arrays
            cap_access_df = cap_access_df.withColumn(
                "contextdata",
                regexp_replace(
                    col("contextdata"),
                    '"criteriaValue":((?!\\[)(true|false|"[^"]*"|[0-9]+(?:\\.[0-9]+)?))',
                    '"criteriaValue":[$1]'
                )
            )

            # Parse JSON and explode userGroups
            cap_allocation_df = (cap_access_df
            .withColumn("context_data", from_json(col("contextdata"), self.get_access_control_schema()))
            .withColumn("userGroup", explode(col("context_data.accessControl.userGroups")))
            .withColumn(
                "criteria_keys",
                expr("transform(userGroup.userGroupCriteriaList, x -> lower(x.criteriaKey))")
            )
            .withColumn(
                "criteria_values",
                expr("transform(userGroup.userGroupCriteriaList, x -> concat_ws(', ', x.criteriaValue))")
            )
            .withColumn("allotment_type", array_join(col("criteria_keys"), "|"))
            .withColumn("allotment_to", array_join(col("criteria_values"), "|"))
            .withColumn(
                "org_id",
                expr("""
                        CASE
                        WHEN array_contains(criteria_keys, 'rootorgid') THEN
                            filter(criteria_values,
                            (value, idx) -> criteria_keys[idx] = 'rootorgid'
                            )[0]
                        ELSE NULL
                        END
                    """)
            )
            .select(
                col("content_id").alias("cap_id"),
                col("content_name").alias("cap_name"),
                col("content_provider_id").alias("created_by_id"),
                col("org_id"),
                col("allotment_type"),
                col("allotment_to"),
                col("content_status").alias("status")
            )
            )

            allocation_count = cap_allocation_df.count()
            print(f"  CAP allocations (userGroups): {allocation_count:,}")

            # Get distinct CAPs in meta
            meta_distinct_caps = cap_allocation_df.select("cap_id").distinct()
            meta_cap_count = meta_distinct_caps.count()
            print(f"  Distinct CAPs in meta: {meta_cap_count:,}")

            cap_allocation_df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
                f"{conf.warehouseReportDir}/cap_allocation_meta")
            print(f"\nCAP allocation meta data written to warehouse folder")

            print("\n[2/5] Exploding criteria with userGroup tracking...")

            # Add unique identifier for each userGroup
            cap_allocation_with_group = cap_allocation_df.withColumn(
                "user_group_id",
                expr("concat(cap_id, '_', monotonically_increasing_id())")
            )

            # Explode criteria into individual rows but keep user_group_id and cap_name
            cap_criteria_exploded = (cap_allocation_with_group
                                     .withColumn("criteria_types_arr", split(col("allotment_type"), "\\|"))
                                     .withColumn("criteria_values_arr", split(col("allotment_to"), "\\|"))
                                     .withColumn("criteria_idx", expr("sequence(0, size(criteria_types_arr) - 1)"))
                                     .withColumn("criteria_exploded", explode(col("criteria_idx")))
                                     .withColumn("criteria_type",
                                                 expr("lower(trim(criteria_types_arr[criteria_exploded]))"))
                                     .withColumn("criteria_value_raw", expr("criteria_values_arr[criteria_exploded]"))
                                     .select(
                "cap_id", "cap_name", "created_by_id", "org_id", "status",
                "user_group_id",
                "allotment_type", "allotment_to",  # ADDED these columns
                "criteria_type", "criteria_value_raw"
            )
                                     .withColumn("criteria_value_clean", lower(trim(col("criteria_value_raw"))))
                                     )

            # Write to temp parquet
            cap_criteria_path = f"{temp_dir}/cap_criteria.parquet"
            cap_criteria_exploded.write.mode("overwrite").parquet(cap_criteria_path)

            print(f"  Exploded criteria written to temp")

            print("\n[3/5] Initializing DuckDB...")

            # User data path
            user_data_path = f"{warehouse_path}/{conf.dwUserTable}"

            # Initialize DuckDB
            db_path = f"{temp_dir}/cap_processing.duckdb"
            con = duckdb.connect(database=db_path)
            con.execute(f"SET temp_directory='{temp_dir}'")
            con.execute("SET memory_limit='8GB'")
            con.execute("SET threads=4")
            con.execute("SET preserve_insertion_order=false")

            # Get user count
            user_count = con.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{user_data_path}/**.parquet')
                WHERE status = 1
            """).fetchone()[0]
            print(f"  Active users: {user_count:,}")

            print("\n[4/5] Creating user criteria matching views...")

            # Create normalized user view with all required columns
            con.execute(f"""
                CREATE OR REPLACE VIEW users AS
                SELECT 
                    user_id,
                    mdo_id,
                    full_name,
                    email,
                    phone_number,
                    designation,
                    groups,
                    tag,
                    cadre,
                    civil_service_type,
                    civil_services,
                    cadre_batch,
                    is_on_central_deputation,
                    is_verified_karmayogi,
                    status,
                    -- Normalized versions for matching
                    LOWER(TRIM(COALESCE(user_id, ''))) as user_id_lower,
                    LOWER(TRIM(COALESCE(mdo_id, ''))) as mdo_id_lower,
                    LOWER(TRIM(COALESCE(designation, ''))) as designation_lower,
                    LOWER(TRIM(COALESCE(groups, ''))) as groups_lower,
                    LOWER(TRIM(COALESCE(tag, ''))) as tag_lower,
                    LOWER(TRIM(COALESCE(cadre, ''))) as cadre_lower,
                    LOWER(TRIM(COALESCE(civil_service_type, ''))) as civil_service_type_lower,
                    LOWER(TRIM(COALESCE(civil_services, ''))) as civil_services_lower,
                    LOWER(TRIM(COALESCE(cadre_batch, ''))) as cadre_batch_lower,
                    LOWER(TRIM(COALESCE(is_on_central_deputation, ''))) as is_on_central_deputation_lower,
                    LOWER(TRIM(COALESCE(CAST(is_verified_karmayogi AS VARCHAR), ''))) as is_verified_karmayogi_lower,
                    LOWER(TRIM(COALESCE(profile_status, ''))) as profile_status_lower
                FROM read_parquet('{user_data_path}/**.parquet')
                WHERE status = 1
            """)

            # Create CAP criteria view
            con.execute(f"""
                CREATE OR REPLACE VIEW cap_criteria AS
                SELECT * FROM read_parquet('{cap_criteria_path}/**.parquet')
            """)

            # Explode criteria values (comma-separated) - cap_name flows through
            print("  Creating exploded criteria lookup...")
            con.execute(f"""
                CREATE OR REPLACE TABLE criteria_exploded AS
                SELECT 
                    cap_id,
                    cap_name,
                    created_by_id,
                    org_id,
                    status,
                    user_group_id,
                    criteria_type,
                    LOWER(TRIM(cv.value)) as criteria_value
                FROM cap_criteria,
                LATERAL (SELECT unnest(string_split(criteria_value_clean, ',')) as value) cv
                WHERE LENGTH(TRIM(cv.value)) > 0
            """)

            print("\n[5/5] Matching users to CAP allocations with userGroup AND/OR logic...")

            # Count criteria types per userGroup (for AND logic within userGroup)
            con.execute(f"""
                CREATE OR REPLACE TABLE criteria_group_count AS
                SELECT 
                    cap_id,
                    cap_name,
                    created_by_id,
                    user_group_id,
                    COUNT(DISTINCT criteria_type) as total_criteria_types
                FROM criteria_exploded
                GROUP BY cap_id, cap_name, created_by_id, user_group_id
            """)

            # Match users to individual criteria (OR logic within each criteria type)
            con.execute(f"""
                CREATE OR REPLACE TABLE user_criteria_matches AS

                -- rootorgid (mdo_id)
                SELECT DISTINCT 
                    u.user_id,
                    ce.cap_id,
                    ce.cap_name,
                    ce.created_by_id,
                    ce.user_group_id,
                    'rootorgid' as matched_type
                FROM users u
                INNER JOIN criteria_exploded ce 
                    ON ce.criteria_type = 'rootorgid'
                    AND u.mdo_id_lower = ce.criteria_value

                UNION ALL

                -- user, customuser, alluser (direct user_id matching)
                SELECT DISTINCT 
                    u.user_id,
                    ce.cap_id,
                    ce.cap_name,
                    ce.created_by_id,
                    ce.user_group_id,
                    ce.criteria_type as matched_type
                FROM users u
                INNER JOIN criteria_exploded ce 
                    ON ce.criteria_type IN ('user', 'customuser', 'alluser')
                    AND u.user_id_lower = ce.criteria_value

                UNION ALL

                -- designation
                SELECT DISTINCT 
                    u.user_id,
                    ce.cap_id,
                    ce.cap_name,
                    ce.created_by_id,
                    ce.user_group_id,
                    'designation' as matched_type
                FROM users u
                INNER JOIN criteria_exploded ce 
                    ON ce.criteria_type = 'designation'
                    AND u.designation_lower = ce.criteria_value
                INNER JOIN criteria_exploded org_check
                    ON org_check.cap_id = ce.cap_id
                    AND org_check.user_group_id = ce.user_group_id
                    AND org_check.criteria_type = 'rootorgid'
                    AND u.mdo_id_lower = org_check.criteria_value

                UNION ALL

                -- group (maps to groups field)
                SELECT DISTINCT 
                    u.user_id,
                    ce.cap_id,
                    ce.cap_name,
                    ce.created_by_id,
                    ce.user_group_id,
                    'group' as matched_type
                FROM users u
                INNER JOIN criteria_exploded ce 
                    ON ce.criteria_type = 'group'
                    AND u.groups_lower = ce.criteria_value
                INNER JOIN criteria_exploded org_check
                    ON org_check.cap_id = ce.cap_id
                    AND org_check.user_group_id = ce.user_group_id
                    AND org_check.criteria_type = 'rootorgid'
                    AND u.mdo_id_lower = org_check.criteria_value

                UNION ALL

                -- tag (no org check needed)
                SELECT DISTINCT 
                    u.user_id,
                    ce.cap_id,
                    ce.cap_name,
                    ce.created_by_id,
                    ce.user_group_id,
                    'tag' as matched_type
                FROM users u
                INNER JOIN criteria_exploded ce 
                    ON ce.criteria_type = 'tag'
                    AND u.tag_lower = ce.criteria_value

                UNION ALL

                -- cadre
                SELECT DISTINCT 
                    u.user_id,
                    ce.cap_id,
                    ce.cap_name,
                    ce.created_by_id,
                    ce.user_group_id,
                    'cadre' as matched_type
                FROM users u
                INNER JOIN criteria_exploded ce 
                    ON ce.criteria_type = 'cadre'
                    AND u.cadre_lower = ce.criteria_value
                INNER JOIN criteria_exploded org_check
                    ON org_check.cap_id = ce.cap_id
                    AND org_check.user_group_id = ce.user_group_id
                    AND org_check.criteria_type = 'rootorgid'
                    AND u.mdo_id_lower = org_check.criteria_value

                UNION ALL

                -- civil_service_type
                SELECT DISTINCT 
                    u.user_id,
                    ce.cap_id,
                    ce.cap_name,
                    ce.created_by_id,
                    ce.user_group_id,
                    'civil_service_type' as matched_type
                FROM users u
                INNER JOIN criteria_exploded ce 
                    ON ce.criteria_type = 'civil_service_type'
                    AND u.civil_service_type_lower = ce.criteria_value
                INNER JOIN criteria_exploded org_check
                    ON org_check.cap_id = ce.cap_id
                    AND org_check.user_group_id = ce.user_group_id
                    AND org_check.criteria_type = 'rootorgid'
                    AND u.mdo_id_lower = org_check.criteria_value

                UNION ALL

                -- service (maps to civil_services)
                SELECT DISTINCT 
                    u.user_id,
                    ce.cap_id,
                    ce.cap_name,
                    ce.created_by_id,
                    ce.user_group_id,
                    'service' as matched_type
                FROM users u
                INNER JOIN criteria_exploded ce 
                    ON ce.criteria_type = 'service'
                    AND u.civil_services_lower = ce.criteria_value
                INNER JOIN criteria_exploded org_check
                    ON org_check.cap_id = ce.cap_id
                    AND org_check.user_group_id = ce.user_group_id
                    AND org_check.criteria_type = 'rootorgid'
                    AND u.mdo_id_lower = org_check.criteria_value

                UNION ALL

                -- batch (maps to cadre_batch)
                SELECT DISTINCT 
                    u.user_id,
                    ce.cap_id,
                    ce.cap_name,
                    ce.created_by_id,
                    ce.user_group_id,
                    'batch' as matched_type
                FROM users u
                INNER JOIN criteria_exploded ce 
                    ON ce.criteria_type = 'batch'
                    AND u.cadre_batch_lower = ce.criteria_value
                INNER JOIN criteria_exploded org_check
                    ON org_check.cap_id = ce.cap_id
                    AND org_check.user_group_id = ce.user_group_id
                    AND org_check.criteria_type = 'rootorgid'
                    AND u.mdo_id_lower = org_check.criteria_value

                UNION ALL

                -- isoncentraldeputation (maps to is_on_central_deputation)
                SELECT DISTINCT 
                    u.user_id,
                    ce.cap_id,
                    ce.cap_name,
                    ce.created_by_id,
                    ce.user_group_id,
                    'isoncentraldeputation' as matched_type
                FROM users u
                INNER JOIN criteria_exploded ce 
                    ON ce.criteria_type = 'isoncentraldeputation'
                    AND u.is_on_central_deputation_lower = ce.criteria_value
                INNER JOIN criteria_exploded org_check
                    ON org_check.cap_id = ce.cap_id
                    AND org_check.user_group_id = ce.user_group_id
                    AND org_check.criteria_type = 'rootorgid'
                    AND u.mdo_id_lower = org_check.criteria_value

                UNION ALL

                -- isprofileverified (maps to is_verified_karmayogi)
                SELECT DISTINCT 
                    u.user_id,
                    ce.cap_id,
                    ce.cap_name,
                    ce.created_by_id,
                    ce.user_group_id,
                    'isprofileverified' as matched_type
                FROM users u
                INNER JOIN criteria_exploded ce 
                    ON ce.criteria_type = 'isprofileverified'
                    AND u.is_verified_karmayogi_lower = ce.criteria_value
                INNER JOIN criteria_exploded org_check
                    ON org_check.cap_id = ce.cap_id
                    AND org_check.user_group_id = ce.user_group_id
                    AND org_check.criteria_type = 'rootorgid'
                    AND u.mdo_id_lower = org_check.criteria_value

                UNION ALL

                -- profilestatus (maps to profile_status)
                SELECT DISTINCT 
                    u.user_id,
                    ce.cap_id,
                    ce.cap_name,
                    ce.created_by_id,
                    ce.user_group_id,
                    'profilestatus' as matched_type
                FROM users u
                INNER JOIN criteria_exploded ce 
                    ON ce.criteria_type = 'profilestatus'
                    AND u.profile_status_lower = ce.criteria_value
                INNER JOIN criteria_exploded org_check
                    ON org_check.cap_id = ce.cap_id
                    AND org_check.user_group_id = ce.user_group_id
                    AND org_check.criteria_type = 'rootorgid'
                   AND u.mdo_id_lower = org_check.criteria_value
            """)

            # Count how many criteria types each user matched per userGroup
            print("  Counting matched criteria per userGroup...")
            con.execute(f"""
                CREATE OR REPLACE TABLE user_match_counts AS
                SELECT 
                    user_id,
                    cap_id,
                    cap_name,
                    created_by_id,
                    user_group_id,
                    COUNT(DISTINCT matched_type) as matched_types
                FROM user_criteria_matches
                GROUP BY user_id, cap_id, cap_name, created_by_id, user_group_id
            """)

            # Filter to users who matched ALL criteria within at least ONE userGroup (OR between userGroups)
            print("  Filtering for complete matches (AND within userGroup, OR between userGroups)...")
            con.execute(f"""
                CREATE OR REPLACE TABLE complete_matches AS
                SELECT DISTINCT
                    umc.user_id,
                    umc.cap_id,
                    umc.cap_name,
                    umc.created_by_id
                FROM user_match_counts umc
                INNER JOIN criteria_group_count cgc
                    ON umc.cap_id = cgc.cap_id
                    AND umc.user_group_id = cgc.user_group_id
                WHERE umc.matched_types = cgc.total_criteria_types
            """)

            # Join with user details for final output with mapped allotment columns
            print("  Preparing final results with proper column ordering and mapped allotment fields...")
            con.execute(f"""
                CREATE OR REPLACE TABLE final_results AS
                WITH user_allocations AS (
                    SELECT DISTINCT
                        cm.user_id,
                        cm.cap_id,
                        cm.cap_name,
                        cm.created_by_id
                    FROM complete_matches cm
                ),
                cap_allocation_mapped AS (
                    SELECT 
                        cap_id,
                        -- Map criteria types to actual field names for allotment_type
                        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                            allotment_type,
                            'rootorgid', 'mdo_id'),
                            'customuser', 'user_id'),
                            'alluser', 'user_id'),
                            'group', 'groups'),
                            'batch', 'cadre_batch'),
                            'service', 'civil_services'),
                            'isprofileverified', 'is_verified_karmayogi'),
                            'isoncentraldeputation', 'is_on_central_deputation'),
                            'profilestatus', 'profile_status'),
                            'user', 'user_id'
                        ) as allotment_type_mapped,
                        allotment_to as allotment_to_mapped
                    FROM read_parquet('{cap_criteria_path}/**.parquet')
                    WHERE cap_id IN (SELECT DISTINCT cap_id FROM user_allocations)
                ),
                distinct_caps AS (
                    SELECT DISTINCT
                        cap_id,
                        allotment_type_mapped,
                        allotment_to_mapped
                    FROM cap_allocation_mapped
                )
                SELECT DISTINCT
                    u.user_id,
                    u.full_name,
                    u.email,
                    u.phone_number,
                    u.designation,
                    u.groups,
                    u.tag,
                    u.cadre,
                    u.civil_services,
                    u.cadre_batch,
                    u.is_on_central_deputation,
                    ua.cap_id,
                    ua.cap_name,
                    ua.created_by_id,
                    dc.allotment_type_mapped as allotment_type,
                    dc.allotment_to_mapped as allotment_to
                FROM user_allocations ua
                INNER JOIN users u ON ua.user_id = u.user_id
                LEFT JOIN distinct_caps dc ON ua.cap_id = dc.cap_id
            """)

            # Write final output
            output_file = f"{temp_dir}/cap_user_allocation_final.parquet"
            con.execute(f"""
                COPY (
                    SELECT * FROM final_results
                ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
            """)

            elapsed_time = time.time() - start_time

            print("\n" + "=" * 80)
            print("PROCESSING COMPLETE!")
            print("=" * 80)
            print(f"Total time: {elapsed_time:.1f} seconds ({elapsed_time / 60:.1f} minutes)")
            print("=" * 80 + "\n")

            # Read back to Spark
            final_df = spark.read.parquet(output_file)
            final_df_cap_count = final_df.select("cap_id").distinct().count()
            print(f"\n[VERIFICATION] Distinct CAPs in user wise df: {final_df_cap_count:,}")
            final_df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
                f"{conf.warehouseReportDir}/cap_allocation_user_wise")
            print(f"\nUser wise allocation parquet written to warehouse folder")

            # Cleanup
            print("\nCleaning up temporary files...")
            con.close()
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

            print("\n[SUCCESS] CAP Access Control processing completed")

        except Exception as e:
            print(f"❌ Error occurred during CAPAccessControlModel processing: {str(e)}")
            import traceback
            traceback.print_exc()
            raise


def main():
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'

    config_dict = get_environment_config()
    config = create_config(config_dict)

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("CAP Access Control Model - DuckDB") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "15g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
        .getOrCreate()

    # Create model instance
    start_time = datetime.now()
    print(f"[START] CAPAccessControlModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    model = CAPAccessControlModel()
    model.process_data(spark, config)

    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] CAPAccessControlModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()


if __name__ == "__main__":
    main()
