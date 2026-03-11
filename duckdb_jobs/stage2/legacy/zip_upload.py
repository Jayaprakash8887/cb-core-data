import findspark

findspark.init()

import time
from pyspark.sql import SparkSession
from datetime import datetime
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from zipfile import ZipFile, ZIP_STORED, ZIP_DEFLATED
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import shutil
import subprocess
import sys
import glob
import os
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.dfexport import dfexportutil
from dfutil.utils.utils import sync_reports
from jobs.config import get_environment_config
from jobs.default_config import create_config


class ZipUploadModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.ZipUpload"

    def name(self):
        return "ZipUploadModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    def upload_parquet_files(self, config):
        try:
            print("Starting upload of Parquet files to GCP bucket")

            base_path = config.unifiedParquetLocalPath
            user_details_file = os.path.join(base_path, "unified_user_details.parquet")
            enrolments_file = os.path.join(base_path, "unified_enrolments.parquet")
            org_hierarchy_file = os.path.join(base_path, "org_hierarchy.parquet")

            # Check existence
            user_details_exists = os.path.isfile(user_details_file)
            enrolments_exists = os.path.isfile(enrolments_file)
            org_hierarchy_exists = os.path.isfile(org_hierarchy_file)

            if not user_details_exists:
                print(f"WARNING: File not found: {user_details_file}")
            if not enrolments_exists:
                print(f"WARNING: File not found: {enrolments_file}")
            if not org_hierarchy_exists:
                print(f"WARNING: File not found: {org_hierarchy_file}")

            # Proceed only if all exist
            if user_details_exists and enrolments_exists and org_hierarchy_exists:
                sync_reports(base_path, config.unifiedParquetPath, config)
                print("Completed uploading Parquet files to GCP bucket.")
            else:
                print("Upload skipped: One or more required files are missing.")

        except Exception as e:
            print(f"Error uploading Parquet files: {str(e)}")
            raise

    @staticmethod
    def zip_mdoid_folder(args):
        """
        Zip a single MDOID folder with password protection.
        Uses ZIP_DEFLATED for compression.
        """
        mdoid_folder, merged_dir, password = args
        mdoid_path = os.path.join(merged_dir, mdoid_folder)
        if not os.path.isdir(mdoid_path):
            return None
        zip_path = os.path.join(mdoid_path, "reports.zip")
        csv_files = [f for f in os.listdir(mdoid_path) if f.endswith(".csv")]
        if csv_files:
            try:
                with ZipFile(zip_path, 'w', ZIP_DEFLATED) as zipf:
                    zipf.setpassword(password.encode())
                    for csv_file in csv_files:
                        file_path = os.path.join(mdoid_path, csv_file)
                        zipf.write(file_path, arcname=csv_file)
                        os.remove(file_path)  # Delete after adding to zip
                return f"✓ Zipped {mdoid_folder} ({len(csv_files)} files)"
            except Exception as e:
                return f"✗ Failed {mdoid_folder}: {str(e)}"
        return None

    @staticmethod
    def convert_parquet_to_csv(args):
        """
        Convert a parquet *folder* to a single CSV using pandas,
        reading one parquet file at a time to avoid OOM.
        """
        folder, warehouse_base, warehouse_output_dir = args
        folder_path = os.path.join(warehouse_base, folder)

        if not os.path.isdir(folder_path):
            return None

        parquet_files = [f for f in os.listdir(folder_path) if f.endswith(".parquet")]
        if not parquet_files:
            return None

        csv_output = os.path.join(warehouse_output_dir, f"{folder}.csv")

        try:
            first = True
            for pf in parquet_files:
                full_path = os.path.join(folder_path, pf)
                df = pd.read_parquet(full_path)  # only one file in memory at a time
                df.to_csv(
                    csv_output,
                    index=False,
                    mode="w" if first else "a",
                    header=first,
                )
                del df
                first = False

            return f"✓ Converted {folder}"
        except Exception as e:
            return f"✗ Error processing {folder}: {str(e)}"

    @staticmethod
    def copy_file_to_mdoid(args):
        """
        Copy a single CSV file to its destination MDOID folder.
        Used for parallel file copying.
        """
        source_file, dest_dir, dest_filename = args
        try:
            os.makedirs(dest_dir, exist_ok=True)
            dest_path = os.path.join(dest_dir, dest_filename)
            shutil.copy2(source_file, dest_path)
            return True
        except Exception as e:
            print(f"Error copying {source_file}: {e}")
            return False

    def process_data(self, spark, config):
        try:
            overall_start_time = time.time()
            today = self.get_date()
            print("📊 Loading and filtering data...")
            spark = SparkSession.getActiveSession()
            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

            # ------------------ Part 1: Merge & Zip MDOID Reports ------------------ #
            part1_start = time.time()
            print("\n" + "=" * 70)
            print("PART 1: MDOID REPORTS PROCESSING")
            print("=" * 70)
            base_dir = os.path.join(config.localReportDir, config.prefixDirectoryPath)
            directories_to_select = config.pysparkDirectoriesToSelect
            cbp_directories_to_select = config.pysparkCBPDirectoriesToSelect
            today_date = datetime.today().strftime('%Y-%m-%d')
            merged_dir = os.path.join(config.localReportDir, config.destinationDirectoryPath)
            kcm_dir = os.path.join(base_dir, "kcm-report", today_date, "ContentCompetencyMapping")

            # Better KCM file detection
            kcm_file = None
            if os.path.exists(kcm_dir):
                kcm_files = glob.glob(os.path.join(kcm_dir, "*.csv"))
                kcm_file = kcm_files[0] if kcm_files else None

            password = config.password

            # Clean and recreate merged directory
            if os.path.exists(merged_dir):
                shutil.rmtree(merged_dir)
            os.makedirs(merged_dir)

            print("\n📤 Syncing CBP report folders to cloud...")
            cbp_sync_start = time.time()

            for subfolder in cbp_directories_to_select:
                cbp_dir = os.path.join(base_dir, subfolder, today_date)
                if os.path.exists(cbp_dir):
                    print(f"  → Syncing: {cbp_dir}")
                    try:
                        sync_reports(cbp_dir, os.path.join(config.prefixDirectoryPath, subfolder, today_date), config)
                    except Exception as e:
                        print(f"✗ Failed syncing CBP folder {subfolder}: {e}")
                else:
                    print(f"⚠️ CBP directory not found: {cbp_dir}")
            print(f"⏱️  CBP sync completed in {time.time() - cbp_sync_start:.2f}s")

            # Track all MDOID values for KCM distribution
            all_mdoids = set()
            print("\n📁 Collecting CSV files from report directories...")
            collection_start = time.time()
            # Prepare file copy tasks for parallel execution
            copy_tasks = []
            print(f"Copying of mdoid folders started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # Collect CSVs for each mdoid from all specified directories
            for subfolder in directories_to_select:
                report_dir = os.path.join(base_dir, subfolder, today_date)
                if os.path.exists(report_dir):
                    for item in os.listdir(report_dir):
                        item_path = os.path.join(report_dir, item)

                        if item.startswith("mdoid="):
                            if item.endswith(".csv"):
                                mdoid_value = item.replace(".csv", "")
                            else:
                                mdoid_value = item

                            # Track this MDOID for KCM distribution
                            all_mdoids.add(mdoid_value)

                            dest_dir = os.path.join(merged_dir, mdoid_value)

                            if os.path.isdir(item_path):
                                # Directory with CSVs inside
                                for f in os.listdir(item_path):
                                    if f.endswith(".csv"):
                                        source = os.path.join(item_path, f)
                                        copy_tasks.append((source, dest_dir, f))
                            elif item_path.endswith(".csv"):
                                # Single CSV file
                                copy_tasks.append((item_path, dest_dir, f"{subfolder}.csv"))

            print(f"  Found {len(copy_tasks)} files to copy across {len(all_mdoids)} MDOID folders")

            # Parallel file copying
            if copy_tasks:
                print(f"  Copying files in parallel (max_workers=16)...")
                copy_start = time.time()
                with ThreadPoolExecutor(max_workers=16) as executor:
                    list(executor.map(self.copy_file_to_mdoid, copy_tasks))
                print(f"⏱️  File copying completed in {time.time() - copy_start:.2f}s")

            print(f"Copying of mdoid folders completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # Password-protected ZIP creation per mdoid folder (PARALLEL)
            print(f"\n🗜️  Creating password-protected ZIP files for {len(all_mdoids)} MDOID folders...")
            print(f"Operational reports zipping started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            zip_start = time.time()
            mdoid_folders = [f for f in os.listdir(merged_dir) if os.path.isdir(os.path.join(merged_dir, f))]
            zip_tasks = [(folder, merged_dir, password) for folder in mdoid_folders]

            # Use ThreadPoolExecutor for I/O-bound zipping
            with ThreadPoolExecutor(max_workers=12) as executor:
                futures = [executor.submit(self.zip_mdoid_folder, task) for task in zip_tasks]
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        print(f"  {result}")

            print(f"✅ All MDOID folders zipped with password at: {merged_dir}")
            print(f"⏱️  Zipping completed in {time.time() - zip_start:.2f}s")
            print(f"Zipping of operational reports completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            print("\n📤 Syncing MDOID reports to cloud...")
            mdoid_sync_start = time.time()
            sync_reports(merged_dir, config.mdoReportSyncPath, config)
            print(f"⏱️  MDOID sync completed in {time.time() - mdoid_sync_start:.2f}s")

            # Separate KCM sync (existing functionality)
            print(f"\n📤 Syncing KCM file separately to: {config.kcmSyncPath}")
            try:
                kcm_sync_start = time.time()
                sync_reports(kcm_file, config.kcmSyncPath, config)
                print(f"⏱️  KCM sync completed in {time.time() - kcm_sync_start:.2f}s")
            except Exception as e:
                print(f"⚠️ WARNING: Failed to sync KCM file separately: {e}")

            print(f"\n⏱️  Part 1 completed in {time.time() - part1_start:.2f}s")

            # Upload unified parquet files
            print("\n📤 Uploading unified Parquet files...")
            parquet_upload_start = time.time()
            self.upload_parquet_files(config)
            print(f"⏱️  Parquet upload completed in {time.time() - parquet_upload_start:.2f}s")

            # ------------------ Part 2: Convert Parquet to CSV & Zip for full reports ------------------ #
            # Only execute if createFullReport flag is enabled
            if config.createFullReport:
                warehouse_base = config.warehouseReportDir
                warehouse_output_dir = config.warehouseOutputDir
                warehouse_zip_path = os.path.join(warehouse_output_dir, "reports.zip")

                # Clean and recreate output directory
                if os.path.exists(warehouse_output_dir):
                    shutil.rmtree(warehouse_output_dir)
                os.makedirs(warehouse_output_dir)

                # Get list of folders to convert
                folders = [f for f in os.listdir(warehouse_base)
                           if os.path.isdir(os.path.join(warehouse_base, f))]

                print(f"\n📦 Converting {len(folders)} Parquet folders to CSV...")
                conversion_start = time.time()

                # Prepare conversion tasks
                conversion_tasks = [(folder, warehouse_base, warehouse_output_dir) for folder in folders]

                # Parallel Parquet to CSV conversion using pandas
                # Use fewer workers for memory-intensive operations
                with ThreadPoolExecutor(max_workers=1) as executor:
                    futures = [executor.submit(self.convert_parquet_to_csv, task) for task in conversion_tasks]
                    for future in as_completed(futures):
                        result = future.result()
                        if result:
                            print(f"  {result}")

                print(f"⏱️  Conversion completed in {time.time() - conversion_start:.2f}s")

                # Password-protected ZIP of all warehouse CSVs
                print("\n🗜️  Creating password-protected ZIP for warehouse reports...")
                zip_start = time.time()

                warehouse_csvs = [os.path.join(warehouse_output_dir, f)
                                  for f in os.listdir(warehouse_output_dir) if f.endswith(".csv")]

                if warehouse_csvs:
                    with ZipFile(warehouse_zip_path, 'w', ZIP_DEFLATED) as zipf:
                        zipf.setpassword(password.encode())
                        for csv_path in warehouse_csvs:
                            zipf.write(csv_path, arcname=os.path.basename(csv_path))
                    print(f"✅ Warehouse reports zipped at: {warehouse_zip_path}")
                    print(f"⏱️  Zipping completed in {time.time() - zip_start:.2f}s")

                    print("\n📤 Syncing warehouse reports to cloud...")
                    sync_start = time.time()
                    sync_reports(warehouse_zip_path, config.fullReportSyncPath, config)
                    print(f"⏱️  Sync completed in {time.time() - sync_start:.2f}s")
                else:
                    print("⚠️  No CSV files found to zip")

                print(f"\n⏱️  Part 2 completed in {time.time() - part2_start:.2f}s")
            else:
                print("\n" + "=" * 70)
                print("PART 2: SKIPPED (createFullReport flag is disabled)")
                print("=" * 70)
        except Exception as e:
            print(f"✗ Error: {str(e)}")
            raise


def main():
    # Initialize Spark Session with optimized settings
    spark = SparkSession.builder \
        .appName("Zip Upload Model") \
        .config("spark.executor.memory", "42g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.shuffle.io.connectionTimeout", "300s") \
        .config("spark.shuffle.io.maxRetries", "20") \
        .config("spark.shuffle.io.retryWait", "10s") \
        .getOrCreate()

    # Create model instance
    config_dict = get_environment_config()
    config = create_config(config_dict)

    start_time = datetime.now()
    print(f"[START] ZipUpload processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[CONFIG] createFullReport flag: {getattr(config, 'createFullReport', False)}")

    model = ZipUploadModel()
    model.process_data(spark, config)

    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n[END] ZipUpload completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")

    spark.stop()


if __name__ == "__main__":
    main()
