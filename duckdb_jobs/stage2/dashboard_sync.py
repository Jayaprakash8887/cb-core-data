"""
Dashboard Sync — DuckDB Migration
The largest and most complex Stage 2 job. Computes dozens of dashboard
metrics from the initialized DuckDB → Redis + Kafka.

NOTE: The original (1548 lines) is a hybrid PySpark+DuckDB job.
This migration uses DuckDB for all reads and Python clients for
Redis/Kafka writes. Some Druid-dependent metrics are preserved
via REST API calls.
"""
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta, time as dtime, timezone

import duckdb
import requests

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH
from dfutil.utils.redis import Redis

log = logging.getLogger("dashboard-sync")


def _druid_query(host, sql, limit=10_000_000):
    url = f"http://{host}/druid/v2/sql"
    resp = requests.post(url, json={"query": sql, "resultFormat": "object",
                                     "context": {"sqlQueryId": "dashboard-sync"}}, timeout=120)
    resp.raise_for_status()
    return resp.json()


def _dispatch_to_kafka(topic, messages, kafka_host):
    """Send messages to Kafka topic via REST proxy or kafka-python."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=kafka_host,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        for msg in messages:
            producer.send(topic, msg)
        producer.flush()
        producer.close()
    except ImportError:
        log.warning("kafka-python not installed, skipping Kafka dispatch")
    except Exception as e:
        log.warning(f"Kafka dispatch failed: {e}")


def process_data(config, db_path=None):
    t0 = time.time()
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    ist = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist)
    today_str = now_ist.strftime("%Y-%m-%d")
    current_dt = now_ist.strftime("%Y-%m-%d %H:%M:%S")

    # ── 1. NPS Score from Druid ──────────────────────────────────
    try:
        nps_result = _druid_query(config.sparkDruidRouterHost, """
            SELECT
                SUM(CASE WHEN rating >= 9 THEN 1 ELSE 0 END) AS promoters,
                SUM(CASE WHEN rating <= 6 THEN 1 ELSE 0 END) AS detractors,
                COUNT(*) AS total
            FROM "nps-score-data"
        """)
        if nps_result and nps_result[0]["total"] > 0:
            r = nps_result[0]
            nps = ((r["promoters"] - r["detractors"]) / r["total"]) * 100
            Redis.update("dashboard_nps_score", f"{nps:.2f}", conf=config)
            Redis.update("dashboard_nps_count", str(r["total"]), conf=config)
    except Exception as e:
        log.warning(f"NPS Druid query failed: {e}")

    # ── 2. Learning Hours ────────────────────────────────────────
    # Content learning hours
    content_hours = con.execute("""
        SELECT COALESCE(SUM(
            CASE WHEN "courseDuration" IS NOT NULL AND "courseDuration" != '0' AND "courseDuration" != ''
            THEN TRY_CAST("courseDuration" AS DOUBLE) / 3600.0
            ELSE 0 END
        ), 0) AS total_hours
        FROM enrolment_warehouse_computed ew
        INNER JOIN content_computed cc ON ew."courseID" = cc."courseID"
        WHERE ew."user_consumption_status" = 'completed'
    """).fetchone()[0]

    # Event learning hours
    event_hours = con.execute("""
        SELECT COALESCE(SUM(
            CASE WHEN we.duration IS NOT NULL
            THEN TRY_CAST(we.duration AS DOUBLE) / 3600.0
            ELSE 0 END
        ), 0)
        FROM warehouse_event_enrolments wee
        INNER JOIN warehouse_events we ON wee.event_id = we.event_id
        WHERE wee.status = 'completed'
    """).fetchone()[0]

    total_learning_hours = content_hours + event_hours
    Redis.update("dashboard_total_learning_hours", f"{total_learning_hours:.2f}", conf=config)

    # ── 3. Certificate Counts ────────────────────────────────────
    total_certs = con.execute("""
        SELECT COUNT(DISTINCT "certificateID")
        FROM enrolment_warehouse_computed
        WHERE "certificateID" IS NOT NULL
    """).fetchone()[0]
    Redis.update("dashboard_total_certificates", str(total_certs), conf=config)

    # Certificates by MDO
    cert_by_mdo = con.execute("""
        SELECT uo."userOrgID" AS mdo_id, CAST(COUNT(DISTINCT ew."certificateID") AS VARCHAR) AS cnt
        FROM enrolment_warehouse_computed ew
        INNER JOIN user_org_computed uo ON ew."userID" = uo."userID"
        WHERE ew."certificateID" IS NOT NULL
        GROUP BY uo."userOrgID"
    """).fetchall()
    Redis.dispatch("dashboard_certificates_generated_by_mdo",
                   {r[0]: r[1] for r in cert_by_mdo if r[0]}, replace=True, conf=config)

    # ── 4. Competency Coverage ───────────────────────────────────
    # Unique competency areas/themes/subthemes in content
    comp_stats = con.execute("""
        SELECT
            COUNT(DISTINCT json_extract_string(comp.val, '$.competencyAreaId')) AS areas,
            COUNT(DISTINCT json_extract_string(comp.val, '$.competencyThemeId')) AS themes,
            COUNT(DISTINCT json_extract_string(comp.val, '$.competencySubThemeId')) AS subthemes
        FROM content_computed cc,
        LATERAL (SELECT UNNEST(CAST(cc."competencies_v6" AS JSON[])) AS val) comp
        WHERE cc."competencies_v6" IS NOT NULL
          AND cc."courseStatus" IN ('Live','Retired')
    """).fetchone()
    Redis.update("dashboard_competency_areas_count", str(comp_stats[0]), conf=config)
    Redis.update("dashboard_competency_themes_count", str(comp_stats[1]), conf=config)
    Redis.update("dashboard_competency_subthemes_count", str(comp_stats[2]), conf=config)

    # ── 5. Trending Courses — top by enrolment count ─────────────
    trending = con.execute("""
        SELECT cc."courseID", cc."courseName", cc."category",
               COUNT(*) AS enrol_count
        FROM enrolment_computed ec
        INNER JOIN content_computed cc ON ec."courseID" = cc."courseID"
        WHERE cc."courseStatus" = 'Live' AND cc."category" = 'Course'
        GROUP BY cc."courseID", cc."courseName", cc."category"
        ORDER BY enrol_count DESC
        LIMIT 10
    """).fetchdf()

    trending_json = json.dumps(trending.to_dict(orient="records"))
    Redis.update("dashboard_trending_courses", trending_json, conf=config)

    # ── 6. Trending Programs ─────────────────────────────────────
    trending_programs = con.execute("""
        SELECT cc."courseID", cc."courseName", cc."category",
               COUNT(*) AS enrol_count
        FROM enrolment_computed ec
        INNER JOIN content_computed cc ON ec."courseID" = cc."courseID"
        WHERE cc."courseStatus" = 'Live'
          AND cc."category" IN ('Program','Blended Program','CuratedCollections','Curated Program')
        GROUP BY cc."courseID", cc."courseName", cc."category"
        ORDER BY enrol_count DESC
        LIMIT 10
    """).fetchdf()
    Redis.update("dashboard_trending_programs", json.dumps(trending_programs.to_dict(orient="records")), conf=config)

    # ── 7. Top 10 Combined (courses + programs + assessments) ────
    top10_combined = con.execute("""
        SELECT cc."courseID" AS content_id, cc."courseName" AS content_name,
               cc."category" AS content_type, COUNT(*) AS enrol_count
        FROM enrolment_computed ec
        INNER JOIN content_computed cc ON ec."courseID" = cc."courseID"
        WHERE cc."courseStatus" = 'Live'
        GROUP BY cc."courseID", cc."courseName", cc."category"
        ORDER BY enrol_count DESC
        LIMIT 10
    """).fetchdf()
    Redis.update("dashboard_top_10_content", json.dumps(top10_combined.to_dict(orient="records")), conf=config)

    # ── 8. Reviews ───────────────────────────────────────────────
    avg_rating = con.execute("""
        SELECT ROUND(AVG(CAST(rating AS DOUBLE)), 2) FROM rating_computed
    """).fetchone()[0]
    Redis.update("dashboard_avg_rating", str(avg_rating or 0), conf=config)

    total_reviews = con.execute("SELECT COUNT(*) FROM rating_computed").fetchone()[0]
    Redis.update("dashboard_total_reviews", str(total_reviews), conf=config)

    # ── 9. Events Analytics ──────────────────────────────────────
    total_events = con.execute("SELECT COUNT(*) FROM warehouse_events").fetchone()[0]
    Redis.update("dashboard_total_events", str(total_events), conf=config)

    total_event_enrol = con.execute("SELECT COUNT(*) FROM warehouse_event_enrolments").fetchone()[0]
    Redis.update("dashboard_total_event_enrolments", str(total_event_enrol), conf=config)

    # ── 10. Per-MDO metrics for Kafka dispatch ───────────────────
    mdo_metrics = con.execute("""
        SELECT
            uo."userOrgID" AS mdo_id,
            uo."userOrgName" AS mdo_name,
            COUNT(DISTINCT uo."userID") AS total_users,
            COUNT(DISTINCT ew."courseID") AS content_enrolled,
            COUNT(DISTINCT CASE WHEN ew."user_consumption_status" = 'completed' THEN ew."courseID" END) AS content_completed,
            COUNT(DISTINCT ew."certificateID") AS certificates
        FROM user_org_computed uo
        LEFT JOIN enrolment_warehouse_computed ew ON uo."userID" = ew."userID"
        GROUP BY uo."userOrgID", uo."userOrgName"
    """).fetchdf()

    kafka_host = getattr(config, 'kafkaHost', None)
    kafka_topic = getattr(config, 'dashboardKafkaTopic', 'dashboard_metrics')
    if kafka_host:
        messages = mdo_metrics.to_dict(orient="records")
        _dispatch_to_kafka(kafka_topic, messages, kafka_host)
        log.info(f"Dispatched {len(messages)} MDO metrics to Kafka")

    # ── 11. Learner Home Page data ───────────────────────────────
    # Content provider counts
    provider_counts = con.execute("""
        SELECT "courseOrgName" AS provider, COUNT(*) AS content_count
        FROM content_computed
        WHERE "courseStatus" = 'Live'
        GROUP BY "courseOrgName"
        ORDER BY content_count DESC
        LIMIT 20
    """).fetchdf()
    Redis.update("dashboard_top_providers", json.dumps(provider_counts.to_dict(orient="records")), conf=config)

    con.close()
    elapsed = time.time() - t0
    log.info(f"[SUCCESS] DashboardSync — completed in {elapsed:.1f}s ({elapsed/60:.1f} min)")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] DashboardSync at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] DashboardSync — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
