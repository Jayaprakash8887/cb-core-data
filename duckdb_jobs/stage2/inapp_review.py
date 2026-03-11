"""
In-App Review — DuckDB Migration
Creates user feed entries in Cassandra for users who received
weekly claps today.
"""
import uuid
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH

log = logging.getLogger("inapp-review")


def process_data(config, db_path=None):
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    cache_path = getattr(config, 'baseCachePath',
                         str(Path(__file__).resolve().parents[2] / "data-res/pq_files/cache_pq"))

    today = datetime.now().date()

    # End of current week (Sunday)
    days_until_sunday = 6 - today.weekday()  # Monday=0, Sunday=6
    if days_until_sunday < 0:
        days_until_sunday = 0
    expire_date = today + timedelta(days=days_until_sunday)
    expire_dt = datetime.combine(expire_date + timedelta(days=1), datetime.min.time()) - timedelta(microseconds=1)
    expire_epoch_ms = int(expire_dt.timestamp() * 1000)

    today_str = today.strftime("%Y-%m-%d")

    # Filter users who got claps updated today
    eligible_users = con.execute(f"""
        SELECT userid
        FROM read_parquet('{cache_path}/weeklyClaps/**/*.parquet', union_by_name=true)
        WHERE claps_updated_this_week = true
          AND CAST(last_claps_updated_on AS DATE) = DATE '{today_str}'
    """).fetchdf()

    if eligible_users.empty:
        log.info("No eligible users for in-app review today")
        con.close()
        return

    log.info(f"Creating feed entries for {len(eligible_users)} users")

    # ── Write to Cassandra ───────────────────────────────────────
    from cassandra.cluster import Cluster

    cluster = Cluster([config.sparkCassandraConnectionHost], port=9042)
    session = cluster.connect(config.cassandraUserFeedKeyspace)

    insert_feed = session.prepare(f"""
        INSERT INTO {config.cassandraUserFeedTable}
        (userid, expireon, category, id, createdby, createdon, action, priority, status, updatedby, updatedon, version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    for uid in eligible_users["userid"]:
        session.execute(insert_feed, (
            str(uid), expire_epoch_ms, "InAppReview",
            str(uuid.uuid4()), "weekly_claps", today,
            "{}", 1, "unread", None, None, "v1",
        ))

    session.shutdown()
    cluster.shutdown()
    con.close()
    log.info(f"[SUCCESS] InAppReview — {len(eligible_users)} feed entries created")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] InAppReview at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] InAppReview — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
