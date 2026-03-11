"""
NPS Upgraded — DuckDB Migration
Identifies users eligible for NPS survey trigger, writes feed to Cassandra.
Criteria: enrolled/completed/rated in last 15 days, minus users who already
submitted or have existing feed.
"""
import uuid
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta

import duckdb
import requests

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH

log = logging.getLogger("nps-upgraded")

UUID_EPOCH_OFFSET = 0x01b21dd213814000


def _timeuuid_to_millis(u):
    if u is None:
        return None
    try:
        return (uuid.UUID(u).time - UUID_EPOCH_OFFSET) // 10_000
    except Exception:
        return None


def _druid_query(host, sql):
    url = f"http://{host}/druid/v2/sql"
    resp = requests.post(url, json={"query": sql, "resultFormat": "object"}, timeout=120)
    resp.raise_for_status()
    return resp.json()


def process_data(config, db_path=None):
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    fifteen_days_ago = today_start - timedelta(days=15)
    fifteen_days_ago_str = fifteen_days_ago.strftime("%Y-%m-%d %H:%M:%S")
    today_str = today_start.strftime("%Y-%m-%d %H:%M:%S")
    fifteen_days_ago_ms = int(fifteen_days_ago.timestamp() * 1000)
    today_ms = int(today_start.timestamp() * 1000)

    # ── C1: Users who submitted/rejected NPS in last 15 days (Druid) ──
    try:
        c1_result = _druid_query(config.sparkDruidRouterHost, """
            SELECT userID AS userid FROM "nps-upgraded-users-data"
            WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '15' DAY
        """)
        submitted_users = {r["userid"] for r in c1_result} if c1_result else set()
    except Exception as e:
        log.warning(f"Druid C1 query failed: {e}")
        submitted_users = set()

    # ── C2: Users enrolled or completed in last 15 days ──────────
    enrolled_completed = con.execute(f"""
        SELECT DISTINCT "userID" AS userid FROM enrolment_computed
        WHERE (TRY_CAST("firstCompletedOn" AS TIMESTAMP) BETWEEN TIMESTAMP '{fifteen_days_ago_str}' AND TIMESTAMP '{today_str}')
           OR (TRY_CAST("courseEnrolledTimestamp" AS TIMESTAMP) BETWEEN TIMESTAMP '{fifteen_days_ago_str}' AND TIMESTAMP '{today_str}')
    """).fetchdf()
    c2_users = set(enrolled_completed["userid"].dropna().tolist())

    # ── C3: Users who rated at least one course in last 15 days ──
    ratings = con.execute("SELECT userid, createdOn FROM rating_computed").fetchdf()
    ratings["rated_ms"] = ratings["createdOn"].apply(_timeuuid_to_millis)
    c3_users = set(
        ratings[(ratings["rated_ms"] >= fifteen_days_ago_ms) & (ratings["rated_ms"] < today_ms)]
        ["userid"].dropna().tolist()
    )

    # ── Eligible = (C2 ∪ C3) - C1 ───────────────────────────────
    eligible = (c2_users | c3_users) - submitted_users
    eligible = {u for u in eligible if u}  # remove None/empty

    log.info(f"C2 (enrolled/completed): {len(c2_users)}")
    log.info(f"C3 (rated): {len(c3_users)}")
    log.info(f"C1 (already submitted): {len(submitted_users)}")
    log.info(f"Eligible: {len(eligible)}")

    if not eligible:
        log.info("No eligible users for NPS")
        con.close()
        return

    # ── Exclude users who already have NPS2 feed in Cassandra ────
    from cassandra.cluster import Cluster

    cluster = Cluster([config.sparkCassandraConnectionHost], port=9042)
    session = cluster.connect(config.cassandraUserFeedKeyspace)

    existing_feeds = set()
    rows = session.execute(f"""
        SELECT userid FROM {config.cassandraUserFeedTable}
        WHERE category = 'NPS2' ALLOW FILTERING
    """)
    for row in rows:
        existing_feeds.add(row.userid)

    final_users = eligible - existing_feeds
    log.info(f"After excluding existing feeds: {len(final_users)}")

    if not final_users:
        session.shutdown()
        cluster.shutdown()
        con.close()
        return

    # ── Write feed entries ───────────────────────────────────────
    survey_id = getattr(config, 'platformRatingSurveyId', '')
    action_str = f'{{"dataValue":"yes","actionData":{{"formId":{survey_id}}}}}'
    today_date = datetime.now().date()

    insert_feed = session.prepare(f"""
        INSERT INTO {config.cassandraUserFeedTable}
        (userid, category, id, createdby, createdon, action, expireon, priority, status, updatedby, updatedon, version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    # Also write to notification history
    cass_history = session.prepare("""
        INSERT INTO sunbird_notifications.notification_feed_history
        (userid, category, id, createdby, createdon, action, expireon, priority, status, updatedby, updatedon, version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    for uid in final_users:
        vals = (
            uid, "NPS2", str(uuid.uuid4()), "platform_rating", today_date,
            action_str, None, 1, "unread", None, None, "v1",
        )
        session.execute(insert_feed, vals)
        session.execute(cass_history, vals)

    session.shutdown()
    cluster.shutdown()
    con.close()
    log.info(f"[SUCCESS] NPSUpgraded — {len(final_users)} feed entries created")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] NPSUpgraded at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] NPSUpgraded — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
