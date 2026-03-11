import findspark
findspark.init()
import json
import os
import glob
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from pathlib import Path
import sys
import shutil
import io

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import (
    col, from_json, lit, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType
)
from pyspark import StorageLevel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.append(str(Path(__file__).resolve().parents[2]))

from jobs.default_config import create_config
from jobs.config import get_environment_config
from dfutil.utils import utils
from dfutil.dfexport import dfexportutil


class SurveyStatusReportModel:
    """PySpark implementation matching Scala StatusReportModel for generating survey status reports."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.class_name = "SurveyStatusReportModel"
    
    def name(self) -> str:
        """Return model name."""
        return "SurveyStatusReportModel"
    
    def get_date(self) -> str:
        """Get current date in YYYY-MM-DD format."""
        return datetime.now().strftime('%Y-%m-%d')
    
    def get_report_config(self, filter_name: str):
        """
        Get report configuration from MongoDB.
        
        Args:
            filter_name: Filter to identify specific report configuration
            
        Returns:
            Row object containing report configuration
        """
        try:
            logger.info("Querying MongoDB database to get report configurations")
            
            # MongoDB connection details
            mongo_uri = f"mongodb://{self.config.mlSparkMongoConnectionHost}:27017"
            database = self.config.mlMongoDatabase
            collection = self.config.reportConfigCollection
            
            # Read from MongoDB using Spark MongoDB connector
            mongo_df = (self.spark.read
                       .format("mongo")
                       .option("uri", mongo_uri)
                       .option("database", database)
                       .option("collection", collection)
                       .load())
            
            # Filter by report name
            filtered_df = mongo_df.filter(col("report") == filter_name).orderBy(col("_id").desc())
            
            if filtered_df.count() == 0:
                raise ValueError(f"No configuration found for report: {filter_name}")
            
            # Get configuration row
            config_row = filtered_df.collect()[0]
            logger.info(f"Report config for {filter_name}:\n{config_row}")
            
            return config_row
            
        except Exception as e:
            logger.error(f"Error retrieving report config: {str(e)}")
            raise
    
    def get_solution_ids_as_df(self, solution_ids: str) -> DataFrame:
        """
        Convert solution IDs string to DataFrame.
        
        Args:
            solution_ids: Comma-separated solution IDs (format: "id1:name1,id2:name2")
            
        Returns:
            DataFrame with solutionId and solutionName columns
        """
        solution_data = []
        for item in solution_ids.split(','):
            if ':' in item:
                solution_id, solution_name = item.strip().split(':', 1)
                solution_data.append(Row(solutionId=solution_id, solutionName=solution_name))
            else:
                solution_data.append(Row(solutionId=item.strip(), solutionName=item.strip()))
        
        return self.spark.createDataFrame(solution_data)
    
    def load_all_unique_solution_ids(self, datasource: str) -> DataFrame:
        """
        Load all unique solution IDs from Druid datasource.
        
        Args:
            datasource: Druid datasource name
            
        Returns:
            DataFrame with unique solution IDs and names
        """
        query = f'SELECT DISTINCT solutionId, solutionName FROM "{datasource}"'
        result = utils.druidDFOption(query, self.config.mlSparkDruidRouterHost, limit=1000000)
        
        if result is None:
            return self.spark.createDataFrame([], StructType([
                StructField("solutionId", StringType(), True),
                StructField("solutionName", StringType(), True)
            ]))
        
        return result
    
    def get_solutions_end_date(self, solution_ids_df: DataFrame) -> DataFrame:
        """
        Get solution end dates from MongoDB.
        
        Args:
            solution_ids_df: DataFrame containing solution IDs
            
        Returns:
            DataFrame with solution IDs, names, and end dates
        """
        try:
            # Get solution IDs as list
            solution_ids = [row['solutionId'] for row in solution_ids_df.collect()]
            
            # MongoDB query for solutions
            mongo_uri = f"mongodb://{self.config.mlSparkMongoConnectionHost}:27017"
            database = self.config.mlMongoDatabase
            collection = "solutions"
            
            solutions_df = (self.spark.read
                          .format("mongo")
                          .option("uri", mongo_uri)
                          .option("database", database)
                          .option("collection", collection)
                          .load())
            
            # Filter by solution IDs
            filtered_solutions = solutions_df.filter(col("solutionId").isin(solution_ids))
            
            return filtered_solutions.select("solutionId", "solutionName", "endDate")
            
        except Exception as e:
            logger.error(f"Error retrieving solutions end date: {str(e)}")
            raise
    
    def is_solution_within_report_date(self, end_date_str: str) -> bool:
        """
        Check if solution end date is within reporting period.
        Matches Scala logic: endDate.isEqual(today) || (endDate.isAfter(today) || endDate.isAfter(updatedDate)) || endDate.isEqual(updatedDate)
        
        Args:
            end_date_str: End date string in YYYY-MM-DD format
            
        Returns:
            True if solution is within reporting period
        """
        try:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            today = datetime.now().date()
            grace_period = int(self.config.gracePeriod)
            updated_date = today - timedelta(days=grace_period)
            
            # Match Scala logic exactly
            return (end_date == today or 
                    end_date > today or 
                    end_date > updated_date or 
                    end_date == updated_date)
            
        except Exception as e:
            logger.error(f"Error checking solution date: {str(e)}")
            return False
    
    def validate_columns(self, df: DataFrame, required_columns: List[str]) -> bool:
        """
        Validate that all required columns exist in DataFrame.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            
        Returns:
            True if all columns exist, False otherwise
        """
        df_columns = set(df.columns)
        required_columns_set = set(required_columns)
        
        missing_columns = required_columns_set - df_columns
        
        if missing_columns:
            logger.error(f"Missing columns: {missing_columns}")
            return False
        
        return True
    
    def process_profile_data(self, original_df: DataFrame, 
                             profile_schema: StructType, 
                             required_csv_columns: List[col]) -> DataFrame:
        """
        Process user profile data from JSON.
        Matches Scala implementation exactly.
        
        Args:
            original_df: Original DataFrame with userProfile column
            profile_schema: Schema for parsing JSON profile
            required_csv_columns: List of Column expressions with aliases
                
        Returns:
            DataFrame with processed profile data
        """
        has_user_profile = "userProfile" in original_df.columns
        
        if has_user_profile:
            # Parse JSON profile
            parsed_df = original_df.withColumn(
                "parsedProfile", 
                from_json(col("userProfile"), profile_schema)
            )
            return parsed_df.select(*required_csv_columns)
        else:
            # Add empty parsed profile
            empty_parsed_df = original_df.withColumn(
                "parsedProfile", 
                lit(None).cast(StringType())
            )
            return empty_parsed_df.select(*required_csv_columns)
    
    def combine_csv_files(self, input_path: str, output_path: str):
        """
        Combine multiple part CSV files into a single CSV file.
        Matches Scala combineCsvFiles implementation.
        
        Args:
            input_path: Directory containing part-*.csv files
            output_path: Output file path for combined CSV
        """
        try:
            input_dir = Path(input_path)
            output_file = Path(output_path)
            
            # Find all part CSV files
            part_files = sorted([
                f for f in input_dir.glob("part-*.csv")
            ])
            
            if not part_files:
                logger.warning(f"No part CSV files found in {input_path}")
                return
            
            is_first_file = True
            
            # Write combined file
            with open(output_file, 'w', encoding='utf-8') as outfile:
                for part_file in part_files:
                    with open(part_file, 'r', encoding='utf-8') as infile:
                        lines = infile.readlines()
                        
                        for idx, line in enumerate(lines):
                            # Write header only from first file, skip headers from others
                            if is_first_file or idx > 0:
                                outfile.write(line)
                        
                        is_first_file = False
            
            # Delete part files after combining
            for part_file in part_files:
                part_file.unlink()
            
            logger.info(f"Combined {len(part_files)} CSV files into {output_path}")
            
        except Exception as e:
            logger.error(f"Error combining CSV files: {str(e)}")
            raise
    
    def get_solution_id_data(self, columns: str, datasource: str, 
                             solution_id: str, solution_name: str, 
                             batch_size: int, report_path: str,
                             user_profile_schema: StructType,
                             required_csv_columns: List[col],
                             sorting_columns: List[str]):
        """
        Get and process solution data from Druid in batches.
        Matches Scala getSolutionIdData implementation exactly.
        
        Args:
            columns: Comma-separated column names to query
            datasource: Druid datasource name
            solution_id: Solution ID to query
            solution_name: Solution name
            batch_size: Number of surveySubmissionIds per batch
            report_path: Path to save report
            user_profile_schema: Schema for user profile JSON
            required_csv_columns: List of Column expressions with aliases
            sorting_columns: Column names for sorting
        """
        try:
            # Step 1: Get all distinct surveySubmissionIds for this solution
            survey_submission_id_query = f'''
            SELECT DISTINCT surveySubmissionId 
            FROM "{datasource}" 
            WHERE solutionId='{solution_id}'
            '''
            
            survey_submission_ids_df = utils.druidDFOption(
                survey_submission_id_query, 
                self.config.mlSparkDruidRouterHost, 
                limit=1000000
            )
            
            if survey_submission_ids_df is None or survey_submission_ids_df.count() == 0:
                logger.warning(f"No survey submissions found for solutionId: {solution_id}")
                return
            
            # Convert to list of IDs
            survey_submission_ids = [row['surveySubmissionId'] for row in survey_submission_ids_df.collect()]
            
            logger.info(f"Total {len(survey_submission_ids)} Survey Submissions for solutionId: {solution_id}")
            
            # Step 2: Process in batches
            batch_count = 0
            
            # Split into batches
            for i in range(0, len(survey_submission_ids), batch_size):
                batch_count += 1
                batch_survey_submission_ids = survey_submission_ids[i:i + batch_size]
                
                # Build query for this batch
                ids_string = "','".join(batch_survey_submission_ids)
                batch_query = f'''
                SELECT {columns} 
                FROM "{datasource}" 
                WHERE solutionId='{solution_id}' 
                AND surveySubmissionId IN ('{ids_string}')
                '''
                
                # Query Druid for batch
                batch_df = utils.druidDFOption(
                    batch_query, 
                    self.config.mlSparkDruidRouterHost, 
                    limit=1000000
                )
                
                if batch_df is None:
                    logger.warning(f"Batch {batch_count}: No data returned")
                    continue
                
                # Persist to disk for memory management
                batch_df.persist(StorageLevel.DISK_ONLY)
                
                # Step 3: Handle evidences column if present
                if "evidences" in batch_df.columns:
                    base_url = self.config.baseUrlForEvidences
                    
                    @udf(returnType=StringType())
                    def add_base_url(evidences):
                        if evidences and evidences.strip():
                            # Split by comma-space, add base URL, rejoin
                            urls = evidences.split(", ")
                            return ",".join([f"{base_url}{url}" for url in urls])
                        return evidences
                    
                    batch_df = batch_df.withColumn("evidences", add_base_url(col("evidences")))
                
                # Step 4: Process profile data
                final_solution_df = self.process_profile_data(
                    batch_df, 
                    user_profile_schema, 
                    required_csv_columns
                )
                
                # Step 5: Validate columns
                columns_match = self.validate_columns(final_solution_df, sorting_columns)
                
                if columns_match:
                    # Step 6: Sort columns in specified order
                    sorted_final_df = final_solution_df.select(*[col(c) for c in sorting_columns])
                    
                    # Step 7: Generate report in append mode
                    self.generate_report(sorted_final_df, report_path, append=True)
                    
                    logger.info(f"Batch: {batch_count}, Successfully generated survey question csv report for solutionId: {solution_id}")
                else:
                    logger.error(f"Error occurred while matching the data frame columns with config sort columns for solutionId: {solution_id}")
                
                # Unpersist to free memory
                batch_df.unpersist()
            
            logger.info(f"Total {batch_count} batches processed for solutionId: {solution_id}")
            
            # Step 8: Combine all CSV part files into single file
            # Clean solution name for filename
            clean_solution_name = solution_name
            # Remove special characters and normalize spaces
            import re
            clean_solution_name = re.sub(r'[^a-zA-Z0-9\s]', '', clean_solution_name)
            clean_solution_name = re.sub(r'\s+', ' ', clean_solution_name).strip()
            
            output_filename = f"{clean_solution_name}-{solution_id}.csv"
            self.combine_csv_files(
                f"{self.config.localReportDir}/{report_path}",
                f"{self.config.localReportDir}/{report_path}/{output_filename}"
            )
            
        except Exception as e:
            logger.error(f"Error processing solution {solution_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def generate_report(self, df: DataFrame, report_path: str, append: bool = False):
        """
        Generate CSV report from DataFrame.
        
        Args:
            df: DataFrame to export
            report_path: Path to save report
            append: If True, append to existing files (SaveMode.Append)
        """
        try:
            output_path = f"{self.config.localReportDir}/{report_path}"
            
            # Create directory if it doesn't exist
            os.makedirs(output_path, exist_ok=True)
            
            # Write CSV in append mode if specified
            if append:
                df.coalesce(1).write \
                    .mode("append") \
                    .option("header", "true") \
                    .option("encoding", "UTF-8") \
                    .csv(output_path)
            else:
                df.coalesce(1).write \
                    .mode("overwrite") \
                    .option("header", "true") \
                    .option("encoding", "UTF-8") \
                    .csv(output_path)
                    
        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            raise
    
    def generate_survey_question_report(self, solution_id: str, solution_name: str,
                                        columns_to_query: str,
                                        datasource: str,
                                        batch_size: int,
                                        report_path: str,
                                        user_profile_schema: StructType,
                                        required_csv_columns: List[col],
                                        sorting_columns: List[str]):
        """
        Generate survey question report for a solution.
        Matches Scala generateSurveyQuestionReport implementation.
        
        Args:
            solution_id: Solution ID
            solution_name: Solution name
            columns_to_query: Comma-separated column names to query
            datasource: Druid datasource name
            batch_size: Batch size for processing
            report_path: Path to save report
            user_profile_schema: Schema for user profile JSON
            required_csv_columns: List of Column expressions with aliases
            sorting_columns: Column names for sorting
        """
        try:
            self.get_solution_id_data(
                columns_to_query,
                datasource,
                solution_id,
                solution_name,
                batch_size,
                report_path,
                user_profile_schema,
                required_csv_columns,
                sorting_columns
            )
            
            logger.info(f"-------------- Successfully Generated Survey Question CSV In Batches And Combined Into A Single File For A SolutionId: {solution_id} --------------")
            
        except Exception as e:
            logger.error(f"Error generating report for solution {solution_id}: {str(e)}")
            raise
    
    def zip_and_sync_reports(self, local_path: str, remote_path: str):
        """
        Zip local reports and sync to blob storage.
        
        Args:
            local_path: Local directory path
            remote_path: Remote blob storage path
        """
        try:
            # Implement your blob storage sync logic here
            logger.info(f"Zipping and syncing {local_path} to {remote_path}")
            # This is a placeholder - implement according to your blob storage setup
            pass
        except Exception as e:
            logger.error(f"Error zipping and syncing reports: {str(e)}")
            raise
    
    def process_data(self):
        """Main processing method to generate all survey question reports."""
        try:
            today = self.get_date()
            logger.info(f"Starting SurveyStatusReportModel processing for date: {today}")
            
            # Get report configuration from MongoDB
            logger.info("Querying mongo database to get report configurations")
            survey_question_report_config_row = self.get_report_config("surveyQuestionReport")
            
            # Parse configuration
            config_dict = survey_question_report_config_row.asDict()
            
            if 'config' in config_dict and isinstance(config_dict['config'], Row):
                # Config is a nested Row object
                config_row = config_dict['config']
                logger.info(f"Config Row: {config_row}")
                report_columns_row = config_row.reportColumns
                logger.info(f"Report Columns Row: {report_columns_row}")
                sorting_columns_str = config_row.sortingColumns
                logger.info(f"Sorting Columns String: {sorting_columns_str}")
                
                # Convert Row to dict
                report_columns_map = report_columns_row.asDict() if hasattr(report_columns_row, 'asDict') else dict(report_columns_row)
                
                # Check if userProfileColumns exists
                if hasattr(config_row, 'userProfileColumns'):
                    user_profile_columns_map = config_row.userProfileColumns.asDict()
                else:
                    user_profile_columns_map = {}
                
                logger.info("Processed config from nested Row structure")
                
            elif 'config' in config_dict and isinstance(config_dict['config'], str):
                # Config is a JSON string
                config_string = config_dict['config']
                config_map = json.loads(config_string)
                report_columns_map = config_map["reportColumns"]
                user_profile_columns_map = config_map.get("userProfileColumns", {})
                sorting_columns_str = config_map["sortingColumns"]
                
                logger.info("Processed config from JSON string")
                
            elif hasattr(survey_question_report_config_row, 'reportColumns'):
                # Direct Row access (top level)
                report_columns_row = survey_question_report_config_row.reportColumns
                sorting_columns_str = survey_question_report_config_row.sortingColumns
                
                # Convert Row to dict
                report_columns_map = report_columns_row.asDict()
                
                # Check if userProfileColumns exists
                if hasattr(survey_question_report_config_row, 'userProfileColumns'):
                    user_profile_columns_map = survey_question_report_config_row.userProfileColumns.asDict()
                else:
                    user_profile_columns_map = {}
                
                logger.info("Processed config from top-level Row structure")
            else:
                raise ValueError(f"Unable to parse config from MongoDB. Structure: {config_dict}")
            
            logger.info(f"Report Columns Map: {report_columns_map}")
            logger.info(f"report column map keys: {list(report_columns_map.keys())} and values: {list(report_columns_map.values())}")
            
            # Prepare columns to query
            columns_to_query = ",".join(report_columns_map.keys())
            
            # Create user profile schema
            if user_profile_columns_map:
                user_profile_fields = [
                    StructField(key, StringType(), True) 
                    for key in user_profile_columns_map.keys()
                ]
                user_profile_schema = StructType(user_profile_fields)
            else:
                user_profile_schema = StructType([])
            
            # Create Column expressions with aliases (matching Scala logic)
            # reportColumns: Map camelCase to display name
            report_columns = [col(key).alias(report_columns_map[key]) for key in report_columns_map.keys()]
            
            # userProfileColumns: Access from parsedProfile struct
            user_profile_columns = [
                col(f"parsedProfile.{key}").alias(user_profile_columns_map[key]) 
                for key in user_profile_columns_map.keys()
            ] if user_profile_columns_map else []
            
            # Combined columns for selection
            required_csv_columns = report_columns + user_profile_columns
            
            # Parse sorting columns (these are the display names)
            sorting_columns = [c.strip() for c in sorting_columns_str.split(',')]
            
            logger.info(f"Columns to query: {columns_to_query}")
            logger.info(f"Required CSV columns ({len(required_csv_columns)}): {[str(c) for c in required_csv_columns]}")
            logger.info(f"Sorting columns ({len(sorting_columns)}): {sorting_columns}")
            
            report_path = f"{self.config.mlReportPath}/{today}/SurveyQuestionsReport"
            datasource = "sl-survey"
            batch_size = int(self.config.SurveyQuestionReportBatchSize)
            
            # Process solution IDs
            solution_ids = getattr(self.config, 'solutionIDs', None)
            
            if solution_ids and solution_ids.strip():
                logger.info("Processing report requests for specified solutionId's")
                solution_ids_df = self.get_solution_ids_as_df(solution_ids)
                
                for row in solution_ids_df.collect():
                    solution_id = row['solutionId']
                    solution_name = row['solutionName']
                    logger.info(f"Started processing report request for solutionId: {solution_id}")
                    self.generate_survey_question_report(
                        solution_id, solution_name, columns_to_query,
                        datasource, batch_size, report_path,
                        user_profile_schema, required_csv_columns, sorting_columns
                    )
            
            else:
                logger.info("Processing report requests for all solutionId's")
                logger.info("Querying druid to get all the unique solutionId's")
                solution_ids_df = self.load_all_unique_solution_ids(datasource)
                
                if getattr(self.config, 'includeExpiredSolutionIDs', False):
                    logger.info("Generating report for all the expired solutionId's also")
                    for row in solution_ids_df.collect():
                        solution_id = row['solutionId']
                        solution_name = row['solutionName']
                        logger.info(f"Started processing report request for solutionId: {solution_id}")
                        self.generate_survey_question_report(
                            solution_id, solution_name, columns_to_query,
                            datasource, batch_size, report_path,
                            user_profile_schema, required_csv_columns, sorting_columns
                        )
                
                else:
                    logger.info("Query mongodb to get solution end-date for all the unique solutionId's")
                    solutions_end_date_df = self.get_solutions_end_date(solution_ids_df)
                    
                    for row in solutions_end_date_df.collect():
                        solution_id = row['solutionId']
                        solution_name = row['solutionName']
                        end_date = row['endDate']
                        
                        if end_date:
                            end_date_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else str(end_date)
                            logger.info(f"Started processing report request for solutionId: {solution_id}")
                            
                            if self.is_solution_within_report_date(end_date_str):
                                logger.info(f"Solution with Id {solution_id} will ends on {end_date_str}")
                                self.generate_survey_question_report(
                                    solution_id, solution_name, columns_to_query,
                                    datasource, batch_size, report_path,
                                    user_profile_schema, required_csv_columns, sorting_columns
                                )
                            else:
                                logger.info(f"Solution with Id {solution_id} has ended on {end_date_str} date, Hence not generating the report for this ID")
                        else:
                            logger.info(f"End Date for solutionId: {solution_id} is NULL, Hence skipping generating the report for this ID")
            
            # Zip and sync reports
            logger.info("Zipping the csv content folder and syncing to blob storage")
            local_report_path = f"{self.config.localReportDir}/{report_path}"
            utils.zip_and_sync_reports(local_report_path, report_path,config=self.config)
            logger.info("Successfully zipped folder and synced to blob storage")
            
        except Exception as e:
            logger.error(f"Error occurred during SurveyStatusReportModel processing: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            sys.exit(1)


def main():
    """Main entry point for the survey question report generation."""
    
    # Set up Spark packages - only MongoDB connector needed
    os.environ['PYSPARK_SUBMIT_ARGS'] = (
        '--packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 pyspark-shell'
    )

    spark = SparkSession.builder \
        .appName('Survey Status Report Model') \
        .master("local[*]") \
        .config("spark.executor.memory", '15g') \
        .config("spark.driver.memory", '15g') \
        .config("spark.executor.memoryFraction", '0.7') \
        .config("spark.storage.memoryFraction", '0.2') \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", 'snappy') \
        .config("spark.sql.legacy.json.allowEmptyString.enabled", "true") \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate()

    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] SurveyStatusReportModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        model = SurveyStatusReportModel(spark, config)
        model.process_data()
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"[END] SurveyStatusReportModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Total duration: {duration}")
    except Exception as e:
        print(f"[ERROR] Processing failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
