"""
Weekly Claps — DuckDB Migration
Updates weekly claps state (platform engagement from Druid),
writes to Postgres (learner_stats table).
"""
import json
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import duckdb
import requests

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH

log = logging.getLogger("weekly-claps")
IST = ZoneInfo("Asia/Kolkata")


def _druid_query(host, sql):
    url = f"http://{host}/druid/v2/sql"
    resp = requests.post(url, json={"query": sql, "resultFormat": "object"}, timeout=120)
    resp.raise_for_status()
    return resp.json()


def _get_week_dates():
    now = datetime.now(IST)
    data_till = now - timedelta(days=1)
    week_start = (data_till - timedelta(days=data_till.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0)
    week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
    return (
        week_start.strftime("%Y-%m-%d %H:%M:%S"),
        week_end.strftime("%Y-%m-%d"),
        week_end.strftime("%Y-%m-%d %H:%M:%S"),
        data_till.strftime("%Y-%m-%d"),
    )


def process_data(config, db_path=None):
    t0 = time.time()
    today = datetime.now().strftime("%Y-%m-%d")
    week_start, week_end, week_end_time, data_till_date = _get_week_dates()

    cache_path = getattr(config, 'baseCachePath',
                         str(Path(__file__).resolve().parents[2] / "data-res/pq_files/cache_pq"))

    # ── Platform engagement from Druid ───────────────────────────
    try:
        engagement = _druid_query(config.sparkDruidRouterHost, f"""
            SELECT uid AS userid,
                   CAST(SUM(total_time_spent) / 60.0 AS FLOAT) AS platformEngagementTime,
                   COUNT(*) AS sessionCount
            FROM "summary-events"
            WHERE dimensions_type = 'app'
              AND __time >= TIMESTAMP '{week_start}'
              AND __time <= TIMESTAMP '{week_end_time}'
              AND uid IS NOT NULL
            GROUP BY 1
        """)
    except Exception as e:
        log.warning(f"Druid engagement query failed: {e}")
        engagement = []

    # ── Load existing weekly claps from cache ────────────────────
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")

    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE existing_claps AS
            SELECT * FROM read_parquet('{cache_path}/weeklyClaps/**/*.parquet', union_by_name=true)
        """)
    except Exception:
        con.execute("""
            CREATE TABLE existing_claps (
                userid VARCHAR, w1 VARCHAR, w2 VARCHAR, w3 VARCHAR, w4 VARCHAR,
                total_claps INTEGER, claps_updated_this_week BOOLEAN,
                last_claps_updated_on TIMESTAMP, last_updated_on VARCHAR
            )
        """)

    # Engagement data into DuckDB
    if engagement:
        con.execute("CREATE OR REPLACE TEMP TABLE engagement (userid VARCHAR, platformEngagementTime FLOAT, sessionCount INTEGER)")
        con.executemany("INSERT INTO engagement VALUES (?, ?, ?)",
                        [(r["userid"], r.get("platformEngagementTime", 0), r.get("sessionCount", 0))
                         for r in engagement])
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE engagement (userid VARCHAR, platformEngagementTime FLOAT, sessionCount INTEGER)")

    cutoff_time = float(getattr(config, 'cutoffTime', 30))

    # ── Merge engagement with existing claps ─────────────────────
    con.execute(f"""
        CREATE OR REPLACE TABLE updated_claps AS
        SELECT
            COALESCE(ec.userid, eg.userid) AS userid,
            ec.w1, ec.w2, ec.w3,
            json_object('timespent', COALESCE(eg.platformEngagementTime, 0),
                        'numberOfSessions', COALESCE(eg.sessionCount, 0)) AS w4,
            COALESCE(ec.total_claps, 0) AS total_claps,
            COALESCE(ec.claps_updated_this_week, false) AS claps_updated_this_week,
            ec.last_claps_updated_on,
            ec.last_updated_on
        FROM existing_claps ec
        FULL OUTER JOIN engagement eg ON ec.userid = eg.userid
    """)

    # Apply clap logic
    is_weekend_rollover = (data_till_date == week_end) and con.execute(
        f"SELECT MAX(last_updated_on) != '{data_till_date}' FROM updated_claps"
    ).fetchone()[0]

    if is_weekend_rollover:
        con.execute(f"""
            UPDATE updated_claps SET
                w1 = w2, w2 = w3, w3 = w4,
                w4 = json_object('timespent', 0, 'numberOfSessions', 0),
                total_claps = CASE
                    WHEN CAST(json_extract(w4, '$.timespent') AS FLOAT) < {cutoff_time} THEN 0
                    WHEN CAST(json_extract(w4, '$.timespent') AS FLOAT) >= {cutoff_time}
                         AND NOT claps_updated_this_week THEN total_claps + 1
                    ELSE total_claps
                END,
                claps_updated_this_week = false,
                last_updated_on = '{data_till_date}'
        """)
    else:
        con.execute(f"""
            UPDATE updated_claps SET
                total_claps = CASE
                    WHEN CAST(json_extract(w4, '$.timespent') AS FLOAT) >= {cutoff_time}
                         AND NOT claps_updated_this_week THEN total_claps + 1
                    ELSE total_claps
                END,
                claps_updated_this_week = CASE
                    WHEN CAST(json_extract(w4, '$.timespent') AS FLOAT) >= {cutoff_time}
                         AND NOT claps_updated_this_week THEN true
                    ELSE claps_updated_this_week
                END,
                last_claps_updated_on = CASE
                    WHEN CAST(json_extract(w4, '$.timespent') AS FLOAT) >= {cutoff_time}
                         AND NOT claps_updated_this_week THEN CURRENT_TIMESTAMP
                    ELSE last_claps_updated_on
                END
        """)

    # ── Write to Postgres ────────────────────────────────────────
    app_pg_url = f"postgresql://{config.appPostgresHost}/{config.appPostgresSchema}"
    try:
        con.execute("INSTALL postgres; LOAD postgres;")
    except Exception:
        pass

    con.execute(f"""
        ATTACH '{app_pg_url}' AS pg_db (
            TYPE POSTGRES,
            USER '{config.appPostgresUsername}',
            PASSWORD '{config.appPostgresCredential}'
        )
    """)
    con.execute(f"DROP TABLE IF EXISTS pg_db.{config.dwLearnerStatsTable}")
    con.execute(f"""
        CREATE TABLE pg_db.{config.dwLearnerStatsTable} AS
        SELECT
            userid,
            CAST(w1 AS VARCHAR) AS w1,
            CAST(w2 AS VARCHAR) AS w2,
            CAST(w3 AS VARCHAR) AS w3,
            CAST(w4 AS VARCHAR) AS w4,
            total_claps,
            claps_updated_this_week,
            last_claps_updated_on,
            last_updated_on
        FROM updated_claps
    """)
    con.execute("DETACH pg_db")

    # Also save updated parquet for cache
    import os
    claps_cache = f"{cache_path}/weeklyClaps"
    os.makedirs(claps_cache, exist_ok=True)
    con.execute(f"""
        COPY (SELECT * FROM updated_claps)
        TO '{claps_cache}/part-00000.snappy.parquet'
        (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    con.close()
    log.info(f"[SUCCESS] WeeklyClaps — completed in {time.time() - t0:.1f}s")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] WeeklyClaps at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] WeeklyClaps — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
