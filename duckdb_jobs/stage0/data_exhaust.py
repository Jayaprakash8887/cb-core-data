import findspark

findspark.init()
from datetime import datetime
import sys
from pathlib import Path
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode_outer, concat, substring, lit, when, size,
    expr, date_format, to_utc_timestamp, current_timestamp, coalesce,
    to_timestamp, isnan, isnull, format_string, array_join
)

from pyspark.sql.functions import (
    col, from_json, explode, explode_outer, concat, substring, lit, when, size,
    expr, date_format, to_utc_timestamp, current_timestamp, coalesce,
    to_timestamp, isnan, isnull, format_string, array_join, first, count, sum, row_number
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType, ArrayType
from pyspark import StorageLevel
import logging

sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from jobs.config import get_environment_config
from jobs.default_config import create_config
from dfutil.utils import utils
from util import schemas
from pyspark.sql.window import Window


class DataExhaustModel:

    @staticmethod
    def duration_format(df, in_col, out_col=None):
        out_col_name = out_col if out_col is not None else in_col

        return df.withColumn(out_col_name,
                             when(col(in_col).isNull(), lit(""))
                             .otherwise(
                                 format_string("%02d:%02d:%02d",
                                               expr(f"{in_col} / 3600").cast("int"),
                                               expr(f"{in_col} % 3600 / 60").cast("int"),
                                               expr(f"{in_col} % 60").cast("int")
                                               )
                             )
                             )

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)

        log_level = getattr(logging, 'INFO')
        self.logger.setLevel(log_level)

    def read_cassandra_table(self, keyspace: str, table: str) -> "DataFrame":
        """Read data from Cassandra table"""
        return self.spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=table, keyspace=keyspace) \
            .load()

    def read_postgres_table(self, url: str, table: str, username: str, password: str) -> "DataFrame":
        """Read data from PostgreSQL table"""
        return self.spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

    def write_parquet(self, df: "DataFrame", path: str, partition_cols: list = None, mode: str = "overwrite"):
        """Write DataFrame to Parquet with optimization"""
        writer = df.coalesce(16)

        if partition_cols:
            writer = writer.write.partitionBy(*partition_cols)
        else:
            writer = writer.write

        writer.mode(mode) \
            .option("compression", "snappy") \
            .parquet(path)

    def duration_format_udf(self, duration_col: str) -> col:
        return when(col(duration_col).isNotNull(),
                    format_string("%02d:%02d:%02d",
                                  expr(f"{duration_col} / 3600").cast("int"),
                                  expr(f"{duration_col} % 3600 / 60").cast("int"),
                                  expr(f"{duration_col} % 60").cast("int")
                                  )).otherwise(lit("00:00:00"))

    def read_cassandra_safe_columns(self, keyspace: str, table: str) -> "DataFrame":
        """Read only safe columns that don't cause timestamp overflow"""
        try:
            self.logger.info(f"Reading safe columns only from {keyspace}.{table}")

            # Define safe columns for user_assessment_master (exclude problematic timestamp columns)
            safe_columns = [
                "correct_count",
                "id",
                "incorrect_count",
                "not_answered_count",
                "parent_content_type",
                "parent_source_id",
                "pass_percent",
                "result_percent",
                "root_org",
                "source_id",
                "source_title",
                "user_id"
            ]

            # Read the table normally first
            df = self.spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .option("keyspace", keyspace) \
                .option("table", table) \
                .load()

            # Select only the safe columns
            df_safe = df.select(*safe_columns)

            self.logger.info(f"Successfully read {len(safe_columns)} safe columns")
            return df_safe

        except Exception as e:
            self.logger.error(f"Failed to read safe columns: {str(e)}")
            raise e

    def process_data(self, output_base_path: str = None):
        """
        Main processing method - optimized for performance
        """
        questionset_hierarchy_schema = StructType([
            StructField("identifier", StringType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("scoreCutoffType", StringType(), True),
            StructField("totalQuestions", IntegerType(), True),
            StructField("primaryCategory", StringType(), True),
            StructField("minimumPassPercentage", FloatType(), True),
            StructField("compatibilityLevel", IntegerType(), True),
            StructField("noOfSection", IntegerType(), True)
        ])

        submit_assessment_response_schema = StructType([
            StructField("result", FloatType(), False),
            StructField("total", IntegerType(), False),
            StructField("blank", IntegerType(), False),
            StructField("correct", IntegerType(), False),
            StructField("incorrect", IntegerType(), False),
            StructField("pass", BooleanType(), False),
            StructField("overallResult", FloatType(), False),
            StructField("passPercentage", FloatType(), False),
            StructField("totalSectionMarks", FloatType(), False),
            StructField("totalPercentage", FloatType(), False),
            StructField("totalMarks", IntegerType(), False),
            StructField("children", ArrayType(StructType([
                StructField("identifier", StringType(), True),
                StructField("name", StringType(), False),
                StructField("result", FloatType(), False),
                StructField("sectionResult", StringType(), False),
                StructField("sectionMarks", StringType(), False)
            ]), True), True)
        ])
        try:
            # Process enrolment data
            self.logger.info("Processing enrolment data...")
            enrolment_df = self.read_cassandra_table(
                self.config.cassandraCourseKeyspace,
                self.config.cassandraUserEnrolmentsTable
            )

            self.write_parquet(enrolment_df, f"{output_base_path}/enrolment")
            enrolment_df.unpersist()

            # Process batch data
            self.logger.info("Processing batch data...")
            batch_df = self.read_cassandra_table(
                self.config.cassandraCourseKeyspace,
                self.config.cassandraCourseBatchTable
            )

            self.write_parquet(batch_df, f"{output_base_path}/batch")
            batch_df.unpersist()

            # Process KCM V6 hierarchy
            self.logger.info("Processing KCM V6 hierarchy...")
            kcm_v6_hierarchy = self.read_cassandra_table(
                self.config.cassandraHierarchyStoreKeyspace,
                self.config.cassandraFrameworkHierarchyTable
            ).filter(col("identifier") == "kcmfinal_fw")

            self.write_parquet(kcm_v6_hierarchy, f"{output_base_path}/kcmV6")
            kcm_v6_hierarchy.unpersist()

            # Process questionset hierarchy
            self.logger.info("Processing questionset hierarchy...")
            questionset_hierarchy_df = self.read_cassandra_table(
                self.config.cassandraHierarchyStoreKeyspace,
                self.config.cassandraQuestionSetHierarchyTable
            )

            questionset_hierarchy_df = questionset_hierarchy_df.withColumn(
                "hierarchy", from_json(col("hierarchy"), questionset_hierarchy_schema)
            ).select(
                col("identifier").alias("questionsetID"),
                col("hierarchy.name").alias("questionsetName"),
                col("hierarchy.status").alias("questionsetStatus"),
                col("hierarchy.totalQuestions").alias("questionsetTotalQuestions"),
                col("hierarchy.primaryCategory").alias("questionsetPrimaryCategory"),
                col("hierarchy.minimumPassPercentage").alias("questionsetMinimumPassPercentage"),
                col("hierarchy.compatibilityLevel").alias("questionsetCompatibilityLevel"),
                col("hierarchy.noOfSection").alias("questionsetNoOfSection"),
                col("hierarchy.scoreCutoffType").alias("scoreCutoffType")
            )
            # write questionset hierarchy data to parquet
            self.write_parquet(questionset_hierarchy_df, f"{output_base_path}/questionsetHierarchy")
            questionset_hierarchy_df.unpersist()

            # Process user assessment data (complex transformation)
            self.logger.info("Processing user assessment data...")
            user_assessment_df = self.read_cassandra_table(
                self.config.cassandraUserKeyspace,
                self.config.cassandraUserAssessmentTable
            ).select(
                col("assessmentid").alias("assessChildID"),
                col("starttime").alias("assessStartTime"),
                col("endtime").alias("assessEndTime"),
                col("status").alias("assessUserStatus"),
                col("userid").alias("userID"),
                col("assessmentreadresponse"),
                col("submitassessmentresponse"),
                col("submitassessmentrequest"),
                col("language").alias("assessLanguage")
            ).fillna("{}", subset=["submitassessmentresponse", "submitassessmentrequest"])

            # Parse JSON columns
            user_assessment_with_json = user_assessment_df.withColumn(
                "readResponse", from_json(col("assessmentreadresponse"), schemas.assessment_read_response_schema)
            ).withColumn(
                "submitRequest", from_json(col("submitassessmentrequest"), schemas.submit_assessment_request_schema)
            ).withColumn(
                "submitResponse", from_json(col("submitassessmentresponse"), submit_assessment_response_schema)
            ).withColumn(
                "assessStartTimestamp", col("assessStartTime")
            ).withColumn(
                "assessEndTimestamp", col("assessEndTime")
            )

            # Extract main assessment fields (this is the base that keeps ALL records)
            final_assessment_df = user_assessment_with_json \
                .select(
                col("assessChildID"),
                col("assessUserStatus"),
                col("userID"),
                col("assessLanguage"),
                col("readResponse.totalQuestions").alias("assessTotalQuestions"),
                col("readResponse.maxQuestions").alias("assessMaxQuestions"),
                col("readResponse.expectedDuration").alias("assessExpectedDuration"),
                col("readResponse.version").alias("assessVersion"),
                col("readResponse.maxAssessmentRetakeAttempts").alias("assessMaxRetakeAttempts"),
                col("readResponse.status").alias("assessReadStatus"),
                col("readResponse.primaryCategory").alias("assessPrimaryCategory"),
                col("submitRequest.batchId").alias("assessBatchID"),
                col("submitRequest.courseId").alias("courseID"),
                col("submitRequest.isAssessment").cast(IntegerType()).alias("assessIsAssessment"),
                col("submitRequest.timeLimit").alias("assessTimeLimit"),
                col("submitResponse.result").alias("assessResult"),
                col("submitResponse.total").alias("assessTotal"),
                col("submitResponse.blank").alias("assessBlank"),
                col("submitResponse.correct").alias("assessCorrect"),
                col("submitResponse.incorrect").alias("assessIncorrect"),
                col("submitResponse.pass").cast(IntegerType()).alias("assessPassOriginal"),  # Keep original
                col("submitResponse.passPercentage").alias("assessPassPercentageOriginal"),  # Keep original
                col("submitResponse.totalSectionMarks").alias("assessTotalSectionMarks"),
                col("submitResponse.totalPercentage").alias("assessOverallResultOriginal"),  # Keep original
                col("submitResponse.totalMarks").alias("assessTotalMarks"),
                col("assessStartTimestamp"),
                col("assessEndTimestamp"),
                col("submitResponse.children").alias("assessSectionChildren")  # Keep for section logic
            )

            # ============================================================================
            # NEW LOGIC: Calculate section-wise pass/fail (only for records with sections)
            # ============================================================================

            # Extract section data using explode_outer to preserve records without sections
            assessment_section_df = user_assessment_with_json \
                .withColumn("assessSections", explode_outer(col("submitResponse.children"))) \
                .select(
                col("assessChildID"),
                col("userID"),
                col("submitResponse.totalPercentage").alias("assessTotalPercentage"),
                col("submitResponse.overallResult").alias("assessOverallResult"),
                col("submitResponse.totalMarks").alias("assessTotalMarks"),
                col("assessStartTimestamp"),
                col("assessSections.sectionResult").alias("assessSectionFinalResult"),
                col("assessSections.sectionMarks").alias("assessSectionMarks"),
                col("submitResponse.passPercentage").alias("assessPassPercentage")
            )

            # Aggregate section-wise data
            section_wise_user_assessment_df = (
                assessment_section_df
                # Cast BEFORE aggregation
                .withColumn("assessSectionMarks", col("assessSectionMarks").cast(FloatType()))
                .withColumn("assessTotalMarks", col("assessTotalMarks").cast(FloatType()))
                .withColumn("assessTotalPercentage", col("assessTotalPercentage").cast(FloatType()))
                .withColumn("assessOverallResult", col("assessOverallResult").cast(FloatType()))
                .withColumn("assessPassPercentage", col("assessPassPercentage").cast(FloatType()))
                .groupBy("assessChildID", "userID", "assessStartTimestamp")
                .agg(
                    count("*").alias("assessTotalSectionCount"),
                    sum(when(col("assessSectionFinalResult") == "pass", 1).otherwise(0)).alias(
                        "assessPassedSectionCount"),
                    sum(when(col("assessSectionFinalResult") == "fail", 1).otherwise(0)).alias(
                        "assessFailedSectionCount"),
                    sum(col("assessSectionMarks")).alias("assessTotalSectionMarks"),
                    first(col("assessTotalMarks")).alias("assessTotalMarks"),
                    first(col("assessTotalPercentage")).alias("assessTotalPercentage"),
                    first(col("assessOverallResult")).alias("assessOverallResult"),
                    first(col("assessPassPercentage")).alias("assessPassPercentage")
                )
                .withColumn(
                    "assessPassedAllSections",
                    when(
                        col("assessPassedSectionCount") == col("assessTotalSectionCount"),
                        lit(1)
                    ).otherwise(lit(0))
                )
                .select(
                    "assessChildID",
                    "userID",
                    "assessStartTimestamp",
                    "assessTotalSectionCount",
                    "assessPassedSectionCount",
                    "assessFailedSectionCount",
                    "assessPassedAllSections",
                    "assessTotalSectionMarks",
                    "assessTotalMarks",
                    "assessTotalPercentage",
                    "assessPassPercentage",
                    "assessOverallResult"
                )
            )

            # Join with questionset hierarchy and apply new pass/fail logic
            final_assessment_data = (
                section_wise_user_assessment_df.alias("sa")
                .join(
                    questionset_hierarchy_df.alias("qh"),
                    col("sa.assessChildID") == col("qh.questionsetID"),
                    "inner"
                )
                # Cast numeric columns
                .withColumn("assessOverallResult", col("assessOverallResult").cast(FloatType()))
                .withColumn("assessPassPercentage", col("assessPassPercentage").cast(FloatType()))
                # Safe effective pass percentage
                .withColumn(
                    "effectivePassPercentage",
                    when(
                        col("assessPassPercentage").isNotNull(),  # ← REMOVE the > 0 condition
                        col("assessPassPercentage")
                    ).otherwise(col("questionsetMinimumPassPercentage")))

                # Final result logic
                .withColumn(
                    "finalResult",
                    when(
                        col("scoreCutoffType") == "SectionLevel",
                        when(
                            col("assessTotalSectionCount") == col("questionsetNoOfSection"),
                            when(col("assessPassedAllSections") == 1, lit("pass")).otherwise(lit("fail"))
                        ).otherwise(lit("fail"))
                    ).otherwise(
                        when(col("assessOverallResult") >= col("effectivePassPercentage"), lit("pass"))
                        .otherwise(lit("fail"))
                    )
                )
                # Marks calculation
                .withColumn(
                    "assessTotalSectionMarks",
                    when(col("scoreCutoffType") == "SectionLevel", col("assessTotalSectionMarks"))
                    .otherwise(col("assessOverallResult"))
                )
                .withColumn(
                    "assessOverallResultNew",
                    when(col("assessTotalSectionMarks").isNotNull(), col("assessTotalSectionMarks"))
                    .otherwise(col("assessOverallResult"))
                )
                .select(
                    col("assessChildID"),
                    col("userID"),
                    col("assessStartTimestamp"),
                    col("questionsetNoOfSection"),
                    col("assessTotalSectionCount"),
                    col("assessPassedSectionCount"),
                    col("assessFailedSectionCount"),
                    col("finalResult"),
                    col("assessTotalSectionMarks"),
                    col("assessTotalMarks"),
                    col("effectivePassPercentage"),
                    col("assessOverallResultNew")
                )
            )

            # Deduplicate using window function
            windowSpec = Window.partitionBy("assessChildID", "userID", "assessStartTimestamp").orderBy(
                col("assessTotalSectionMarks").desc())
            final_assessment_data_deduped = (
                final_assessment_data
                .withColumn("rn", row_number().over(windowSpec))
                .filter(col("rn") == 1)
                .drop("rn")
            )

            # ============================================================================
            # KEY FIX: Use LEFT JOIN to preserve ALL records from final_assessment_df
            # ============================================================================

            fa_main = final_assessment_df.alias("fa_main")
            fa_data = final_assessment_data_deduped.alias("fa_data")

            final_assessment_df_merged = fa_main.join(
                fa_data,
                (col("fa_main.assessChildID") == col("fa_data.assessChildID")) &
                (col("fa_main.userID") == col("fa_data.userID")) &
                (coalesce(col("fa_main.assessStartTimestamp").cast("string"), lit("__NULL__")) ==
                 coalesce(col("fa_data.assessStartTimestamp").cast("string"), lit("__NULL__"))),
                "left"  # LEFT JOIN - this is the key change!
            ) \
                .select(
                col("fa_main.assessChildID"),
                col("fa_main.assessUserStatus"),
                col("fa_main.userID"),
                col("fa_main.assessLanguage"),
                col("fa_main.assessTotalQuestions"),
                col("fa_main.assessMaxQuestions"),
                col("fa_main.assessExpectedDuration"),
                col("fa_main.assessVersion"),
                col("fa_main.assessMaxRetakeAttempts"),
                col("fa_main.assessReadStatus"),
                col("fa_main.assessPrimaryCategory"),
                col("fa_main.assessBatchID"),
                col("fa_main.courseID"),
                col("fa_main.assessIsAssessment"),
                col("fa_main.assessTimeLimit"),
                col("fa_main.assessResult"),
                col("fa_main.assessTotal"),
                col("fa_main.assessBlank"),
                col("fa_main.assessCorrect"),
                col("fa_main.assessIncorrect"),
                # Use new logic if available, otherwise fall back to original
                coalesce(col("fa_data.effectivePassPercentage"), col("fa_main.assessPassPercentageOriginal")).alias(
                    "assessPassPercentage"),
                col("fa_main.assessTotalSectionMarks"),
                coalesce(col("fa_data.assessOverallResultNew"), col("fa_main.assessOverallResultOriginal")).alias(
                    "assessOverallResult"),
                col("fa_main.assessTotalMarks"),
                col("fa_main.assessStartTimestamp"),
                col("fa_main.assessEndTimestamp"),
                # For assessPass: use new logic if available, otherwise use original
                when(col("fa_data.finalResult").isNotNull(),
                     when(col("fa_data.finalResult") == "pass", lit(1)).otherwise(lit(0))
                     ).otherwise(col("fa_main.assessPassOriginal")).alias("assessPass")
            )

            # Write final output
            self.write_parquet(final_assessment_df_merged, f"{output_base_path}/userAssessment")
            # Cleanup
            user_assessment_df.unpersist()
            final_assessment_df.unpersist()
            final_assessment_data.unpersist()
            final_assessment_data_deduped.unpersist()
            section_wise_user_assessment_df.unpersist()
            assessment_section_df.unpersist()

            self.logger.info("User assessment processing completed successfully!")

            # Process content hierarchy
            self.logger.info("Processing content hierarchy...")
            hierarchy_df = self.read_cassandra_table(
                self.config.cassandraHierarchyStoreKeyspace,
                self.config.cassandraContentHierarchyTable
            )

            self.write_parquet(hierarchy_df, f"{output_base_path}/hierarchy")
            hierarchy_df.unpersist()

            # Process rating summary
            self.logger.info("Processing rating summary...")
            rating_summary_df = self.read_cassandra_table(
                self.config.cassandraUserKeyspace,
                self.config.cassandraRatingSummaryTable
            )

            self.write_parquet(rating_summary_df, f"{output_base_path}/ratingSummary")
            rating_summary_df.unpersist()

            # Process ACBP data
            self.logger.info("Processing ACBP data...")
            acbp_df = self.read_cassandra_table(
                self.config.cassandraUserKeyspace,
                self.config.cassandraAcbpTable
            )

            self.write_parquet(acbp_df, f"{output_base_path}/acbp")
            acbp_df.unpersist()

            # Process ratings
            self.logger.info("Processing ratings...")
            rating_df = self.read_cassandra_table(
                self.config.cassandraUserKeyspace,
                self.config.cassandraRatingsTable
            )

            self.write_parquet(rating_df, f"{output_base_path}/rating")
            rating_df.unpersist()

            # Process user roles
            self.logger.info("Processing user roles...")
            role_df = self.read_cassandra_table(
                self.config.cassandraUserKeyspace,
                self.config.cassandraUserRolesTable
            )

            self.write_parquet(role_df, f"{output_base_path}/role")
            role_df.unpersist()

            # Process Elasticsearch content data
            self.logger.info("Processing ES content data...")
            primary_categories = ["Course", "Program", "Blended Program", "Curated Program",
                                  "Standalone Assessment", "CuratedCollections", "Moderated Course"]
            should_clause = ",".join([f'{{"match":{{"primaryCategory.raw":"{pc}"}}}}' for pc in primary_categories])
            fields = ["identifier", "name", "primaryCategory", "status", "reviewStatus", "channel",
                      "duration", "leafNodesCount", "lastPublishedOn", "lastStatusChangedOn",
                      "createdFor", "competencies_v6", "programDirectorName", "language",
                      "courseCategory", "organisation", "childNodes", "difficultyLevel"]
            array_fields = ["createdFor", "language", "organisation", "childNodes"]
            fields_clause = ",".join([f'"{f}"' for f in fields])
            query = f'{{"_source":[{fields_clause}],"query":{{"bool":{{"should":[{should_clause}]}}}}}}'

            es_content_df = utils.read_elasticsearch_data(
                self.spark,
                self.config.sparkElasticsearchConnectionHost,
                self.config.sparkElasticsearchConnectionPort,
                "compositesearch",
                query,
                fields,
                array_fields
            )
            self.write_parquet(es_content_df, f"{output_base_path}/esContent")
            es_content_df.unpersist()

            # Process organization data with hierarchy
            self.logger.info("Processing organization data...")
            org_df = self.read_cassandra_table(
                self.config.cassandraUserKeyspace,
                self.config.cassandraOrgTable
            )

            self.write_parquet(org_df, f"{output_base_path}/org")

            appPostgresUrl = f"jdbc:postgresql://{self.config.appPostgresHost}/{self.config.appPostgresSchema}"
            org_postgres_df = self.read_postgres_table(
                appPostgresUrl,
                self.config.appOrgHierarchyTable,
                self.config.appPostgresUsername,
                self.config.appPostgresCredential
            )

            # Transform organization data
            org_cassandra_df = org_df.withColumn(
                "createddate", to_timestamp(col("createddate"), "yyyy-MM-dd HH:mm:ss:SSSZ")
            ).select(
                col("id").alias("sborgid"),
                col("organisationtype").alias("orgType"),
                col("orgname").alias("cassOrgName"),
                col("iscca").alias("isCCA"),
                col("createddate").alias("orgCreatedDate")
            )

            # Join with hierarchy data
            org_df_with_org_type = org_cassandra_df.join(org_postgres_df, ["sborgid"], "left")

            org_df_with_sborgid = org_df_with_org_type.join(
                org_postgres_df.select(
                    col("sborgid").alias("ministry_id_sborgid"),
                    col("mapid").alias("l1mapid_lookup")
                ),
                col("l1mapid") == col("l1mapid_lookup"),
                "left"
            ).join(
                org_postgres_df.select(
                    col("sborgid").alias("department_id_sborgid"),
                    col("mapid").alias("l2mapid_lookup")
                ),
                col("l2mapid") == col("l2mapid_lookup"),
                "left"
            ).drop("l1mapid_lookup", "l2mapid_lookup")

            # Final organization hierarchy
            org_hierarchy_df = org_df_with_sborgid.select(
                col("sborgid").alias("mdo_id"),
                col("cassOrgName").alias("mdo_name"),
                col("l1orgname").alias("ministry"),
                col("ministry_id_sborgid").alias("ministry_id"),
                col("l2orgname").alias("department"),
                col("department_id_sborgid").alias("department_id"),
                col("isCCA").alias("is_cca"),
                col("orgCreatedDate").alias("mdo_created_on")
            ).withColumn(
                "data_last_generated_on", current_timestamp()
            ).distinct().dropDuplicates(["mdo_id"]).repartition(16)

            self.write_parquet(org_hierarchy_df, f"{output_base_path}/orgHierarchy")
            self.write_parquet(org_postgres_df, f"{output_base_path}/orgCompleteHierarchy")

            org_df.unpersist()
            org_postgres_df.unpersist()

            # process claps data
            self.logger.info("Processing weeklyclaps data...")
            weekly_claps_df = self.read_postgres_table(
                appPostgresUrl,
                self.config.dwLearnerStatsTable,
                self.config.appPostgresUsername,
                self.config.appPostgresCredential
            )
            self.write_parquet(weekly_claps_df, f"{output_base_path}/weeklyClaps")
            # Process marketplace content
            self.logger.info("Processing marketplace content...")
            marketplace_content_df = self.read_postgres_table(
                appPostgresUrl,
                "cios_content_entity",
                self.config.appPostgresUsername,
                self.config.appPostgresCredential
            )

            self.write_parquet(marketplace_content_df, f"{output_base_path}/externalContent")
            marketplace_content_df.unpersist()

            # Process marketplace enrolments
            self.logger.info("Processing marketplace enrolments...")
            marketplace_enrolments_df = self.read_cassandra_table(
                "sunbird_courses",
                "user_external_enrolments"
            )

            self.write_parquet(marketplace_enrolments_df, f"{output_base_path}/externalCourseEnrolments")
            marketplace_enrolments_df.unpersist()

            self.logger.info("Processing old assessments...")
            old_assessments_df = self.read_cassandra_safe_columns(
                self.config.cassandraUserKeyspace,
                self.config.cassandraOldAssesmentTable
            )

            self.write_parquet(old_assessments_df, f"{output_base_path}/oldAssessmentDetails")
            old_assessments_df.unpersist()
            # Process remaining tables efficiently
            tables_to_process = [
                ("user", self.config.cassandraUserKeyspace, self.config.cassandraUserTable),
                ("learnerLeaderBoard", self.config.cassandraUserKeyspace, self.config.cassandraLearnerLeaderBoardTable),
                ("userKarmaPoints", self.config.cassandraUserKeyspace, self.config.cassandraKarmaPointsTable),
                ("userKarmaPointsSummary", self.config.cassandraUserKeyspace,
                 self.config.cassandraKarmaPointsSummaryTable),
            ]

            for table_name, keyspace, table in tables_to_process:
                self.logger.info(f"Processing {table_name}...")
                df = self.read_cassandra_table(keyspace, table)
                self.write_parquet(df, f"{output_base_path}/{table_name}")
                df.unpersist()

            # # Process event data (NLW)
            self.logger.info("Processing event data...")
            object_types = ["Event"]
            should_clause_events = ",".join([f'{{"match":{{"objectType.raw":"{ot}"}}}}' for ot in object_types])
            # Required fields
            required_fields = ["identifier", "name", "objectType", "status", "startDate", "startTime",
                               "duration", "registrationLink", "createdFor", "recordedLinks", "resourceType",
                               "typeofEvent", "maxEnrolments", "meetingAgenda", "creatorDetails",
                               "noOfAttendes", "eventDuration", "courseLinked", "speakerDetails"]

            # Optional fields - don't include in initial query
            optional_fields = ["recordedMediaLink", "meetingSummary"]
            fields_events = required_fields + optional_fields
            array_fields_events = ["createdFor", "recordedLinks"]
            fields_clause_events = ",".join([f'"{f}"' for f in fields_events])
            event_query = f'{{"_source":[{fields_clause_events}],"query":{{"bool":{{"should":[{should_clause_events}]}}}}}}'
            speaker_schema = ArrayType(
                StructType([
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("email", StringType(), True)  # nullable
                ])
            )
            event_data_df = utils.read_elasticsearch_data(
                self.spark,
                self.config.sparkElasticsearchConnectionHost,
                self.config.sparkElasticsearchConnectionPort,
                "compositesearch",
                event_query,
                fields_events,  # Pass all fields including optional ones
                array_fields_events)

            # Only add columns if they're missing from the ES response
            for field in optional_fields:
                if field not in event_data_df.columns:
                    event_data_df = event_data_df.withColumn(field, lit(None).cast(StringType()))
            # Transform event data
            event_details_df = event_data_df.withColumn(
                "event_provider_mdo_id", explode_outer(col("createdFor"))
            ).withColumn(
                "recording_link", explode_outer(col("recordedLinks"))
            ).withColumn(
                "event_start_datetime",
                concat(substring(col("startDate"), 1, 10), lit(" "), substring(col("startTime"), 1, 8))
            ).withColumn(
                "presenters", lit("No presenters available")
            ).withColumn(
                "durationInSecs", col("duration") * 60
            ).withColumn(
                "duration_formatted", self.duration_format_udf("durationInSecs")
            ).withColumn(
                "eventDurationInSecs", col("eventDuration") * 60
            ).withColumn(
                "event_duration_formatted", self.duration_format_udf("eventDurationInSecs")
            ).withColumn(
                "speakerArray", from_json(col("speakerDetails"), speaker_schema)
            ).withColumn(
                "speaker_id", array_join(expr("transform(speakerArray, x -> x.id)"), ", ")
            ).withColumn(
                "speaker_name", array_join(expr("transform(speakerArray, x -> x.name)"), ", ")
            ).select(
                col("identifier").alias("event_id"),
                col("name").alias("event_name"),
                col("event_provider_mdo_id"),
                col("event_start_datetime"),
                col("duration_formatted").alias("duration"),
                col("status").alias("event_status"),
                col("objectType").alias("event_type"),
                col("presenters"),
                col("recording_link"),
                col("registrationLink").alias("video_link"),
                col("resourceType").alias("event_tag").cast(StringType()),
                col("typeofEvent").cast(StringType()),
                col("maxEnrolments").cast(IntegerType()),
                col("meetingAgenda").cast(StringType()),
                col("speakerDetails").cast(StringType()),
                col("speaker_id").cast(StringType()),
                col("speaker_name").cast(StringType()),
                col("recordedMediaLink").cast(StringType()),
                col("noOfAttendes").cast(IntegerType()),
                col("eventDuration").cast(IntegerType()),
                col("meetingSummary").cast(StringType()),
                col("courseLinked").cast(StringType())
            ).dropDuplicates(["event_id"]).fillna(0.0, subset=["duration", "eventDuration"])
            # event_details_df.select("recordedMediaLink").filter(col("recordedMediaLink").isNotNull()).show(50, truncate=False)
            # event_details_df.select("meetingSummary").filter(col("meetingSummary").isNotNull()).show(50, truncate=False)

            # event_details_df.show(15, truncate=False)

            self.write_parquet(event_details_df, f"{output_base_path}/eventDetails")

            # Process event enrolments
            self.logger.info("Processing event enrolments...")
            case_expression = """
                CASE 
                    WHEN ISNULL(status) THEN 'not-enrolled' 
                    WHEN status == 0 THEN 'not-started' 
                    WHEN status == 1 THEN 'in-progress' 
                    ELSE 'completed' 
                END
            """

            events_enrolment_df = self.read_cassandra_table(
                self.config.cassandraCourseKeyspace,
                "user_entity_enrolments"
            ).withColumn(
                "certificate_id",
                when(col("issued_certificates").isNull(), lit(""))
                .otherwise(col("issued_certificates")[size(col("issued_certificates")) - 1]["identifier"])
            ).withColumn(
                "enrolled_on_datetime",
                date_format(to_utc_timestamp(col("enrolled_date"), "Asia/Kolkata"),
                            ParquetFileConstants.DATE_TIME_FORMAT)
            ).withColumn(
                "completed_on_datetime",
                date_format(to_utc_timestamp(col("completedon"), "Asia/Kolkata"), ParquetFileConstants.DATE_TIME_FORMAT)
            ).withColumn(
                "status", expr(case_expression)
            ).withColumn(
                "progress_details",
                from_json(col("lrc_progressdetails"), schemas.event_progress_detail_schema)
            ).select(
                col("userid").alias("user_id"),
                col("contentid").alias("event_id"),
                col("status"),
                col("enrolled_on_datetime"),
                col("completed_on_datetime"),
                col("progress_details"),
                col("certificate_id"),
                col("completionpercentage").alias("completion_percentage")
            )

            # Add duration formatting for events
            events_enrolment_with_duration_df = events_enrolment_df.withColumn(
                "event_duration",
                when(col("progress_details").isNotNull(), col("progress_details.max_size")).otherwise(None)
            ).withColumn(
                "progress_duration",
                when(col("progress_details").isNotNull(), col("progress_details.duration")).otherwise(None)
            ).withColumn(
                "duration",
                when(col("progress_details").isNotNull(), col("progress_details.duration")).otherwise(None)
            ).withColumn(
                "event_duration_seconds",
                when(col("progress_details").isNotNull(), col("progress_details.max_size")).otherwise(None)
            ).drop("progress_details")

            events_enrolment_with_duration_df = self.duration_format(events_enrolment_with_duration_df,
                                                                     "event_duration")
            events_enrolment_with_duration_df = self.duration_format(events_enrolment_with_duration_df,
                                                                     "progress_duration")

            self.write_parquet(events_enrolment_with_duration_df.coalesce(1),
                               f"{output_base_path}/eventEnrolmentDetails")
            events_enrolment_df.unpersist()

            userExtendedProfileDF = self.read_cassandra_table(self.config.cassandraUserKeyspace,
                                                              self.config.cassandraUserExtendedProfileTable)
            self.write_parquet(userExtendedProfileDF, f"{output_base_path}/userExtendedProfile")
            userExtendedProfileDF.unpersist()

            # Process Elasticsearch assessment data
            self.logger.info("Processing final assessment ES content data...")
            primary_categories = ["Course Assessment"]
            must_clause = ",".join([f'{{"match":{{"primaryCategory.raw":"{pc}"}}}}' for pc in primary_categories])
            context_categories = ["Final Program Assessment"]
            context_categories_clause = ",".join(
                [f'{{"match":{{"contextCategory.raw":"{pc}"}}}}' for pc in context_categories])
            fields = ["identifier", "name", "primaryCategory", "status", "reviewStatus", "channel",
                      "expectedDuration", "lastPublishedOn", "lastStatusChangedOn", "minimumPassPercentage",
                      "createdFor", "competencies_v6", "programDirectorName", "language", "courseCategory",
                      "contextCategory"]
            array_fields = ["createdFor", "language", "organisation"]
            fields_clause = ",".join([f'"{f}"' for f in fields])
            query = f'{{"_source":[{fields_clause}],"query":{{"bool":{{"must":[{must_clause},{context_categories_clause}]}}}}}}'

            es_final_assessment_df = utils.read_elasticsearch_data(
                self.spark,
                self.config.sparkElasticsearchConnectionHost,
                self.config.sparkElasticsearchConnectionPort,
                "compositesearch",
                query,
                fields,
                array_fields
            )
            self.write_parquet(es_final_assessment_df, f"{output_base_path}/esFinalAssessment")
            es_final_assessment_df.unpersist()

            # Process access control settings for CAP
            self.logger.info("Processing access control settings for CAP...")
            access_control_settings_df = self.read_cassandra_table(self.config.cassandraCourseKeyspace, self.config.cassandraAccessSettingRulesTable)

            self.write_parquet(access_control_settings_df, f"{output_base_path}/accessControlSettings")
            access_control_settings_df.unpersist()

            # Process course assessment data
            self.logger.info("Processing assessment ES content data...")
            primary_categories = ["Course Assessment"]
            must_clause = ",".join([f'{{"match":{{"primaryCategory.raw":"{pc}"}}}}' for pc in primary_categories])
            fields = ["identifier", "status", "minimumPassPercentage", "contextCategory"]
            fields_clause = ",".join([f'"{f}"' for f in fields])
            array_fields = ["createdFor", "language", "organisation"]
            query = f'{{"_source":[{fields_clause}],"query":{{"bool":{{"must":[{must_clause}]}}}}}}'

            es_course_assessment_df = utils.read_elasticsearch_data(
                self.spark,
                self.config.sparkElasticsearchConnectionHost,
                self.config.sparkElasticsearchConnectionPort,
                "compositesearch",
                query,
                fields,
                array_fields
            )
            self.write_parquet(es_course_assessment_df, f"{output_base_path}/esCourseAssessment")
            es_course_assessment_df.unpersist()

            self.logger.info("ES course assessment data processing completed.")

            # Process Elasticsearch course completion data
            self.logger.info("Processing course completion survey data...")
            fields = ["formId", "contextId", "contextName", "version", "status", "submittedBy", "submittedDate",
                      "responses"]
            fields_clause = ",".join([f'"{f}"' for f in fields])
            array_fields = ["responses.answer", "response.question"]
            query = f'{{"_source":[{fields_clause}],"query":{{"match_all":{{}}}}}}'
            course_completion_survey_df = utils.read_elasticsearch_data(
                self.spark,
                self.config.sparkIGotElasticsearchConnectionHost,
                self.config.sparkElasticsearchConnectionPort,
                "fs-forms-data-v3",
                query,
                fields,
                array_fields
            )
            self.write_parquet(course_completion_survey_df, f"{output_base_path}/courseCompletionSurvey")
            course_completion_survey_df.unpersist()
            self.logger.info("course completion survey data processing completed.")
            self.logger.info("Data processing completed successfully!")

        except Exception as e:
            self.logger.error(f"Error occurred during DataExhaustModel processing: {str(e)}")
            raise e


def create_spark_session_with_packages(config):
    # Set environment variables for PySpark to find packages
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'

    spark = SparkSession.builder \
        .appName('DataExhaustModel') \
        .master("local[*]") \
        .config("spark.executor.memory", '42g') \
        .config("spark.driver.memory", '18g') \
        .config("spark.executor.memoryFraction", '0.7') \
        .config("spark.storage.memoryFraction", '0.2') \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", 'snappy') \
        .config("spark.sql.legacy.json.allowEmptyString.enabled", "true") \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.cassandra.connection.host", config.sparkCassandraConnectionHost) \
        .config("spark.cassandra.connection.port", '9042') \
        .config("spark.cassandra.output.batch.size.rows", '10000') \
        .config("spark.cassandra.connection.keepAliveMS", "60000") \
        .config("spark.cassandra.connection.timeoutMS", '30000') \
        .config("spark.cassandra.read.timeoutMS", '30000') \
        .config("es.nodes", config.sparkElasticsearchConnectionHost) \
        .config("es.port", config.sparkElasticsearchConnectionPort) \
        .config("es.index.auto.create", "false") \
        .config("es.nodes.wan.only", "true") \
        .config("es.nodes.discovery", "false") \
        .getOrCreate()

    return spark


def main():
    config_dict = get_environment_config()
    config = create_config(config_dict)

    # Initialize Spark Session with optimizations from config
    spark = create_spark_session_with_packages(config)

    # Set up logging
    logging.basicConfig(
        level=getattr(logging, 'INFO'),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    logger.info(f"Starting Data Exhaust processing")

    # Initialize and run the model
    model = DataExhaustModel(spark, config)

    output_path = getattr(config, 'baseCachePath', '/home/analytics/pyspark/data-res/pq_files/cache_pq/')
    start_time = datetime.now()

    logger.info(f"[START] Data Exhaust processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Output path: {output_path}")

    try:
        model.process_data(output_path)
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"[END] Data Exhaust processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"[INFO] Total duration: {duration}")
        spark.stop()

    except Exception as e:
        logger.error(f"Data Exhaust processing failed: {str(e)}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
