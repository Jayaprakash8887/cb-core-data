"""
National Learning Week — DuckDB Migration
Computes NLW metrics (enrolments, certificates, events, leaderboard)
per MDO/ministry → Redis + Postgres.
"""
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta, time as dtime, timezone

import duckdb
import requests

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH
from dfutil.utils.redis import Redis

log = logging.getLogger("national-learning-week")


def _es_query(host, port, index, query_body, fields):
    """Simple Elasticsearch query via REST."""
    url = f"http://{host}:{port}/{index}/_search"
    resp = requests.post(url, json=query_body, timeout=60,
                         headers={"Content-Type": "application/json"})
    resp.raise_for_status()
    hits = resp.json().get("hits", {}).get("hits", [])
    return [h.get("_source", {}) for h in hits]


def process_data(config, db_path=None):
    t0 = time.time()
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    nlw_start = getattr(config, 'nationalLearningWeekStart', '2024-01-01 00:00:00')
    nlw_end = getattr(config, 'nationalLearningWeekEnd', '2024-12-31 23:59:59')
    overrides = getattr(config, 'overridesForSlw', {})
    rollup_orgs = getattr(config, 'rollupRequiredOrgs', [])

    ist = timezone(timedelta(hours=5, minutes=30))
    today_ist = datetime.now(ist).date()
    y_start = datetime.combine(today_ist - timedelta(days=1), dtime.min, tzinfo=ist).strftime("%Y-%m-%d %H:%M:%S")
    y_end = (datetime.combine(today_ist, dtime.min, tzinfo=ist) - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

    # ── Build org map (mdo_id → ministry_id) ─────────────────────
    org_map = dict(con.execute("""
        SELECT mdo_id, COALESCE(ministry_id, mdo_id) AS ministry_id
        FROM org_hierarchy_select
    """).fetchall())

    def _metrics_by_mdo(sql_template, start, end):
        sql = sql_template.format(start=start, end=end)
        return dict(con.execute(sql).fetchall())

    def _apply_overrides(base_dict, metric_fn, overrides_dict):
        result = dict(base_dict)
        for org_id, win in (overrides_dict or {}).items():
            ov = metric_fn(win["start"], win["end"])
            if org_id in ov:
                result[org_id] = ov[org_id]
            elif org_id in result:
                del result[org_id]
        return result

    def _apply_rollup(mdo_dict, ministry_dict, rollup_list, org_map_local):
        result = dict(mdo_dict)
        for org_id in (rollup_list or []):
            min_id = org_map_local.get(org_id, org_id)
            if min_id in ministry_dict:
                result[org_id] = ministry_dict[min_id]
        return result

    # ── METRIC 1: Total Enrolments ───────────────────────────────
    def enrolments_mdo(start, end):
        content_enrol = dict(con.execute(f"""
            SELECT uo."userOrgID" AS mdo_id, CAST(COUNT(*) AS VARCHAR)
            FROM enrolment_warehouse_computed ew
            INNER JOIN user_org_computed uo ON ew."userID" = uo."userID"
            WHERE ew."enrolled_on" >= '{start}' AND ew."enrolled_on" <= '{end}'
            GROUP BY uo."userOrgID"
        """).fetchall())
        event_enrol = dict(con.execute(f"""
            SELECT uo."userOrgID" AS mdo_id, CAST(COUNT(*) AS VARCHAR)
            FROM warehouse_event_enrolments wee
            INNER JOIN user_org_computed uo ON wee.user_id = uo."userID"
            WHERE wee.enrolled_on_datetime >= '{start}' AND wee.enrolled_on_datetime <= '{end}'
            GROUP BY uo."userOrgID"
        """).fetchall())
        merged = {}
        for k in set(list(content_enrol.keys()) + list(event_enrol.keys())):
            merged[k] = str(int(content_enrol.get(k, 0)) + int(event_enrol.get(k, 0)))
        return merged

    enrol_base = enrolments_mdo(nlw_start, nlw_end)
    enrol_final = _apply_overrides(enrol_base, enrolments_mdo, overrides)
    Redis.dispatch("dashboard_total_enrolment_by_ministry_slw_count", enrol_final, replace=True, conf=config)

    # ── METRIC 2: Total Certificates ─────────────────────────────
    def certificates_mdo(start, end):
        c_cert = dict(con.execute(f"""
            SELECT uo."userOrgID" AS mdo_id, CAST(COUNT(*) AS VARCHAR)
            FROM enrolment_warehouse_computed ew
            INNER JOIN user_org_computed uo ON ew."userID" = uo."userID"
            WHERE ew."certificateID" IS NOT NULL
              AND ew."first_completed_on" >= '{start}' AND ew."first_completed_on" <= '{end}'
            GROUP BY uo."userOrgID"
        """).fetchall())
        e_cert = dict(con.execute(f"""
            SELECT uo."userOrgID" AS mdo_id, CAST(COUNT(DISTINCT wee.certificate_id) AS VARCHAR)
            FROM warehouse_event_enrolments wee
            INNER JOIN user_org_computed uo ON wee.user_id = uo."userID"
            WHERE wee.certificate_id IS NOT NULL
              AND wee.completed_on_datetime >= '{start}' AND wee.completed_on_datetime <= '{end}'
            GROUP BY uo."userOrgID"
        """).fetchall())
        merged = {}
        for k in set(list(c_cert.keys()) + list(e_cert.keys())):
            merged[k] = str(int(c_cert.get(k, 0)) + int(e_cert.get(k, 0)))
        return merged

    cert_base = certificates_mdo(nlw_start, nlw_end)
    cert_final = _apply_overrides(cert_base, certificates_mdo, overrides)
    Redis.dispatch("dashboard_certificates_generated_by_ministry_slw_count", cert_final, replace=True, conf=config)

    # ── METRIC 3: Yesterday Certificates ─────────────────────────
    yday_cert = certificates_mdo(y_start, y_end)
    Redis.dispatch("dashboard_certificate_generated_yday_by_ministry_slw_count", yday_cert, replace=True, conf=config)

    # ── METRIC 4: Events Published (from ES if available) ────────
    try:
        es_host = config.sparkElasticsearchConnectionHost
        es_port = config.sparkElasticsearchConnectionPort
        # Query ES for events with resourceType = 'Rajya Karmayogi Saptah'
        es_query = {
            "_source": ["identifier", "resourceType", "resourceTypeDetails", "createdFor"],
            "query": {"bool": {"must": [
                {"range": {"startDate": {"gte": nlw_start.split(" ")[0], "lte": nlw_end.split(" ")[0]}}},
                {"match": {"objectType.raw": "Event"}}
            ]}},
            "size": 10000,
        }
        events = _es_query(es_host, es_port, "compositesearch", es_query,
                           ["identifier", "resourceType", "resourceTypeDetails"])
        rks_events = [e for e in events if e.get("resourceType") == "Rajya Karmayogi Saptah"]
        events_by_org = {}
        for evt in rks_events:
            details = evt.get("resourceTypeDetails", {})
            ministry_ids = details.get("stateOrMinistryId", []) if isinstance(details, dict) else []
            for mid in ministry_ids:
                events_by_org[mid] = str(events_by_org.get(mid, 0) + 1)
        Redis.dispatch("dashboard_events_published_by_ministry_count", events_by_org, replace=True, conf=config)
    except Exception as e:
        log.warning(f"ES events query failed: {e}")

    # ── User Leaderboard → Postgres ──────────────────────────────
    app_pg_url = f"postgresql://{config.appPostgresHost}/{config.appPostgresSchema}"

    # Build leaderboard from enrolments in NLW window
    wcon = duckdb.connect()
    wcon.execute("SET memory_limit='4GB'")

    # Get leaderboard data from main DB
    lb_df = con.execute(f"""
        SELECT
            uo."userID" AS user_id,
            uo."userOrgID" AS org_id,
            uo."fullName" AS full_name,
            uo."userProfileImgUrl" AS profile_image,
            COUNT(DISTINCT ew."courseID") AS courses_completed,
            COALESCE(SUM(TRY_CAST(ew."completionPercentage" AS DOUBLE)), 0) AS total_progress
        FROM user_org_computed uo
        LEFT JOIN enrolment_warehouse_computed ew
            ON uo."userID" = ew."userID"
            AND ew."user_consumption_status" = 'completed'
            AND ew."first_completed_on" >= '{nlw_start}'
            AND ew."first_completed_on" <= '{nlw_end}'
        GROUP BY uo."userID", uo."userOrgID", uo."fullName", uo."userProfileImgUrl"
    """).fetchdf()

    wcon.execute("CREATE TABLE leaderboard AS SELECT * FROM lb_df")
    wcon.execute("""
        CREATE OR REPLACE TABLE ranked_lb AS
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY courses_completed DESC, total_progress DESC) AS rank
        FROM leaderboard
        WHERE org_id IS NOT NULL
    """)

    try:
        wcon.execute("INSTALL postgres; LOAD postgres;")
    except Exception:
        pass

    wcon.execute(f"""
        ATTACH '{app_pg_url}' AS pg_db (
            TYPE POSTGRES,
            USER '{config.appPostgresUsername}',
            PASSWORD '{config.appPostgresCredential}'
        )
    """)
    wcon.execute("DROP TABLE IF EXISTS pg_db.nlw_user_leaderboard_pyspark_test")
    wcon.execute("""
        CREATE TABLE pg_db.nlw_user_leaderboard_pyspark_test AS
        SELECT user_id, org_id, full_name, profile_image, courses_completed, total_progress, rank
        FROM ranked_lb
    """)
    wcon.execute("DETACH pg_db")
    wcon.close()

    con.close()
    log.info(f"[SUCCESS] NationalLearningWeek — completed in {time.time() - t0:.1f}s")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] NationalLearningWeek at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] NationalLearningWeek — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
