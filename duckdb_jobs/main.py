"""
duckdb-jobs pipeline orchestrator.

Usage:
    # Full pipeline (Stage 0 → Initializer → Stage 2 jobs)
    python -m duckdb_jobs.main --all

    # Stage 0 only (PySpark extraction → Parquet)
    python -m duckdb_jobs.main --stage0

    # Initializer only (Parquet → DuckDB tables)
    python -m duckdb_jobs.main --init

    # Specific Stage 2 jobs
    python -m duckdb_jobs.main --jobs course_report user_report dsr_computation

    # All Stage 2 jobs
    python -m duckdb_jobs.main --stage2

    # Initializer + all Stage 2 (skip Stage 0 if Parquets are fresh)
    python -m duckdb_jobs.main --init --stage2
"""
import argparse
import importlib
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from duckdb_jobs.core.config_loader import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("pipeline")

# ── Stage 2 job registry ────────────────────────────────────────
# DuckDB-migrated jobs (no PySpark dependency)
DUCKDB_JOBS = [
    "course_report",
    "user_enrolment",
    "user_report",
    "blended_report",
    "assessment_report",
    "course_based_assessment_report",
    "kcm_report",
    "acbp_report",
    "course_completion_survey",
    "user_activity",
    "l2_assessments",
    "data_warehouse",
    "dsr_computation",
    "dsr_computation_updated",
    "ministry_metrics",
    "karma_points",
    "learner_leaderboard",
    "inapp_review",
    "nps_upgraded",
    "odcs_recommendation",
    "user_data_to_redis",
    "weekly_claps",
    "ministry_leaderboard",
    "national_learning_week",
    "dashboard_sync",
]

# Non-migrated jobs (still PySpark or standalone Python) — in stage2/legacy/
LEGACY_JOBS = [
    "legacy.org_hierarchy",
    "legacy.zip_upload",
    "legacy.workflow_summarizer",
    "legacy.survey_question_report",
    "legacy.survey_status_report",
    "legacy.cap_allotment",
    "legacy.program_progress_sync",
]

ALL_JOBS = DUCKDB_JOBS + LEGACY_JOBS


def run_stage0():
    """Run PySpark data extraction (Stage 0)."""
    log.info("=" * 60)
    log.info("STAGE 0: Data Extraction (PySpark)")
    log.info("=" * 60)
    from duckdb_jobs.stage0.data_exhaust import main as exhaust_main
    exhaust_main()


def run_initializer():
    """Run DuckDB initializer (Parquet → DuckDB tables)."""
    log.info("=" * 60)
    log.info("INITIALIZER: Parquet → DuckDB")
    log.info("=" * 60)
    from duckdb_jobs.stage1.initializer import main as init_main
    init_main()


def run_job(job_name, config):
    """Run a single Stage 2 job by name."""
    t0 = time.time()
    log.info(f"  [{job_name}] starting...")
    try:
        mod = importlib.import_module(f"duckdb_jobs.stage2.{job_name}")
        if hasattr(mod, "process_data"):
            mod.process_data(config)
        elif hasattr(mod, "main"):
            mod.main()
        else:
            log.warning(f"  [{job_name}] no process_data() or main() found, skipping")
            return False
        elapsed = time.time() - t0
        log.info(f"  [{job_name}] completed in {elapsed:.1f}s")
        return True
    except Exception as e:
        elapsed = time.time() - t0
        log.error(f"  [{job_name}] FAILED after {elapsed:.1f}s — {e}")
        return False


def run_stage2(job_names, config):
    """Run a list of Stage 2 jobs sequentially."""
    log.info("=" * 60)
    log.info(f"STAGE 2: Running {len(job_names)} jobs")
    log.info("=" * 60)

    results = {}
    for name in job_names:
        results[name] = run_job(name, config)

    succeeded = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)
    log.info(f"Stage 2 summary: {succeeded} succeeded, {failed} failed out of {len(job_names)}")
    if failed:
        log.warning("Failed jobs: " + ", ".join(k for k, v in results.items() if not v))
    return results


def main():
    parser = argparse.ArgumentParser(description="iGOT DuckDB Pipeline Orchestrator")
    parser.add_argument("--all", action="store_true", help="Run full pipeline: Stage 0 → Init → Stage 2")
    parser.add_argument("--stage0", action="store_true", help="Run Stage 0 (PySpark extraction)")
    parser.add_argument("--init", action="store_true", help="Run DuckDB initializer")
    parser.add_argument("--stage2", action="store_true", help="Run all Stage 2 jobs")
    parser.add_argument("--jobs", nargs="+", metavar="JOB", help="Run specific Stage 2 jobs by name")
    parser.add_argument("--duckdb-only", action="store_true", help="Run only DuckDB-migrated Stage 2 jobs (skip PySpark jobs)")
    parser.add_argument("--list", action="store_true", help="List all available jobs")
    args = parser.parse_args()

    if args.list:
        print("\nDuckDB-migrated jobs:")
        for j in DUCKDB_JOBS:
            print(f"  {j}")
        print("\nNon-migrated (PySpark/standalone) jobs:")
        for j in LEGACY_JOBS:
            print(f"  {j}")
        return

    if not any([args.all, args.stage0, args.init, args.stage2, args.jobs]):
        parser.print_help()
        return

    t0 = datetime.now()
    log.info(f"Pipeline started at {t0:%Y-%m-%d %H:%M:%S}")

    config = load_config()

    if args.all or args.stage0:
        run_stage0()

    if args.all or args.init:
        run_initializer()

    if args.all or args.stage2:
        job_list = DUCKDB_JOBS if args.duckdb_only else ALL_JOBS
        run_stage2(job_list, config)
    elif args.jobs:
        unknown = [j for j in args.jobs if j not in ALL_JOBS]
        if unknown:
            log.error(f"Unknown jobs: {unknown}. Use --list to see available jobs.")
            sys.exit(1)
        run_stage2(args.jobs, config)

    total = datetime.now() - t0
    log.info(f"Pipeline finished — total time: {total}")


if __name__ == "__main__":
    main()
