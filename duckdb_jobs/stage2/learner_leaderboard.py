"""
Learner Leaderboard — DuckDB Migration
Computes per-org leaderboard from karma points, writes to Cassandra.
"""
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH

log = logging.getLogger("learner-leaderboard")


def process_data(config, db_path=None):
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    cache_path = getattr(config, 'baseCachePath',
                         str(Path(__file__).resolve().parents[2] / "data-res/pq_files/cache_pq"))

    # Date range: previous calendar month
    today = datetime.now().date()
    month_end_date = today.replace(day=1) - timedelta(days=1)
    month_start = month_end_date.replace(day=1)
    month_start_str = f"{month_start} 00:00:00"
    month_end_str = f"{month_end_date} 23:59:59"
    month_num = month_start.month
    year_num = month_start.year

    # ── Karma points aggregated for the month ────────────────────
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _karma_month AS
        SELECT userid, SUM(points) AS total_points, MAX(credit_date) AS last_credit_date
        FROM read_parquet('{cache_path}/userKarmaPoints/**/*.parquet', union_by_name=true)
        WHERE credit_date >= '{month_start_str}' AND credit_date <= '{month_end_str}'
        GROUP BY userid
    """)

    # ── Orgs with > 10 users ────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _large_orgs AS
        SELECT "userOrgID"
        FROM user_org_computed
        GROUP BY "userOrgID"
        HAVING COUNT(*) > 10
    """)

    # ── User-org data for large orgs ─────────────────────────────
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _user_org_data AS
        SELECT uo."userID" AS userid, uo."userOrgID" AS org_id,
               uo."fullName" AS fullname, uo."userProfileImgUrl" AS profile_image
        FROM user_org_computed uo
        INNER JOIN _large_orgs lo ON uo."userOrgID" = lo."userOrgID"
    """)

    # ── Join with karma, rank per org ────────────────────────────
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _leaderboard AS
        SELECT
            uod.userid, uod.org_id, uod.fullname, uod.profile_image,
            km.total_points, km.last_credit_date,
            {month_num} AS month, {year_num} AS year,
            DENSE_RANK() OVER (PARTITION BY uod.org_id ORDER BY km.total_points DESC) AS rank,
            ROW_NUMBER() OVER (PARTITION BY uod.org_id ORDER BY
                DENSE_RANK() OVER (PARTITION BY uod.org_id ORDER BY km.total_points DESC),
                km.last_credit_date DESC) AS row_num
        FROM _user_org_data uod
        LEFT JOIN _karma_month km ON uod.userid = km.userid
        WHERE uod.org_id IS NOT NULL AND uod.org_id != ''
    """)

    # ── Previous rank from cache ─────────────────────────────────
    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _prev_rank AS
            SELECT userid, rank AS previous_rank
            FROM read_parquet('{cache_path}/learnerLeaderBoard/**/*.parquet', union_by_name=true)
        """)
    except Exception:
        con.execute("CREATE OR REPLACE TEMP TABLE _prev_rank (userid VARCHAR, previous_rank INTEGER)")

    final_rows = con.execute("""
        SELECT lb.org_id, lb.userid, lb.total_points, lb.rank, lb.row_num,
               lb.fullname, lb.profile_image, lb.month, lb.year,
               COALESCE(pr.previous_rank, 0) AS previous_rank
        FROM _leaderboard lb
        LEFT JOIN _prev_rank pr ON lb.userid = pr.userid
    """).fetchdf()

    # ── Write to Cassandra ───────────────────────────────────────
    from cassandra.cluster import Cluster

    cluster = Cluster([config.sparkCassandraConnectionHost], port=9042)
    session = cluster.connect(config.cassandraUserKeyspace)

    insert_lb = session.prepare(f"""
        INSERT INTO {config.cassandraLearnerLeaderBoardTable}
        (org_id, userid, total_points, rank, row_num, fullname, profile_image, month, year, previous_rank)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    insert_lookup = session.prepare(f"""
        INSERT INTO {config.cassandraLearnerLeaderBoardLookupTable}
        (userid, row_num) VALUES (?, ?)
    """)

    for _, row in final_rows.iterrows():
        session.execute(insert_lb, (
            row["org_id"], row["userid"],
            int(row["total_points"]) if row["total_points"] else 0,
            int(row["rank"]) if row["rank"] else 0,
            int(row["row_num"]),
            row["fullname"], row["profile_image"],
            int(row["month"]), int(row["year"]),
            int(row["previous_rank"]),
        ))
        session.execute(insert_lookup, (row["userid"], int(row["row_num"])))

    session.shutdown()
    cluster.shutdown()
    con.close()
    log.info(f"[SUCCESS] LearnerLeaderboard — wrote {len(final_rows)} rows")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] LearnerLeaderboard at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] LearnerLeaderboard — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
