"""
Karma Points — DuckDB Migration
Computes karma points from course ratings and completions (current month),
writes to Cassandra tables.
"""
import uuid
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH
from dfutil.utils import utils

log = logging.getLogger("karma-points")

UUID_EPOCH_OFFSET = 0x01b21dd213814000
IST = ZoneInfo("Asia/Kolkata")


def _timeuuid_to_millis(u):
    if u is None:
        return None
    try:
        return (uuid.UUID(u).time - UUID_EPOCH_OFFSET) // 10_000
    except Exception:
        return None


def process_data(config, db_path=None):
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    now_ist = datetime.now(IST)
    month_start = (now_ist.replace(day=1) - timedelta(days=1)).replace(day=1)
    month_end = now_ist.replace(day=1)
    month_start_ms = int(month_start.timestamp() * 1000)
    month_end_ms = int(month_end.timestamp() * 1000)

    # ── Ratings this month → karma (2 pts each) ─────────────────
    # rating_computed has: activityid, userid, rating, activitytype, createdOn (timeuuid)
    ratings = con.execute("""
        SELECT activityid, userid, rating, activitytype, createdOn
        FROM rating_computed
    """).fetchdf()

    # Filter by month using timeuuid → millis conversion
    ratings["credit_date_ms"] = ratings["createdOn"].apply(_timeuuid_to_millis)
    ratings = ratings[
        (ratings["credit_date_ms"] >= month_start_ms) &
        (ratings["credit_date_ms"] < month_end_ms)
    ].copy()

    # Get content names
    content_names = con.execute("""
        SELECT "courseID", "courseName", "category"
        FROM content_computed
        WHERE "category" IN ('Course','Program','Blended Program','CuratedCollections','Standalone Assessment','Curated Program')
          AND "courseStatus" IN ('Live','Retired')
    """).fetchdf()

    ratings = ratings.merge(
        content_names.rename(columns={"courseID": "activityid"}),
        on="activityid", how="left"
    )

    rating_karma = []
    for _, row in ratings.iterrows():
        credit_ms = row["credit_date_ms"]
        credit_dt = datetime.fromtimestamp(credit_ms / 1000, tz=IST) if credit_ms else None
        addinfo = json.dumps({"COURSENAME": row.get("courseName", "")})
        rating_karma.append({
            "context_id": str(row["activityid"]),
            "userid": str(row["userid"]),
            "context_type": str(row.get("category", "")),
            "credit_date": credit_dt,
            "operation_type": "RATING",
            "addinfo": addinfo,
            "points": 2,
        })

    # ── Course completions this month → karma (5 or 10 pts) ─────
    completions = con.execute(f"""
        SELECT e."userID" AS userid, e."courseID", e."courseCompletedTimestamp" AS credit_date,
               cc."courseName", cc."category"
        FROM enrolment_computed e
        INNER JOIN content_computed cc ON e."courseID" = cc."courseID" AND cc."category" = 'Course'
        WHERE e."dbCompletionStatus" = 2
          AND TRY_CAST(e."courseCompletedTimestamp" AS TIMESTAMP)
              BETWEEN TIMESTAMP '{month_start.strftime("%Y-%m-%d %H:%M:%S")}'
              AND     TIMESTAMP '{month_end.strftime("%Y-%m-%d %H:%M:%S")}'
    """).fetchdf()

    # Courses with assessments
    courses_with_assess = set(con.execute("""
        SELECT DISTINCT "courseID" FROM all_assessment_computed
        WHERE "assessUserStatus" = 'SUBMITTED' AND "assessChildID" IS NOT NULL
    """).fetchdf()["courseID"].tolist())

    # First 4 completions per user
    completions = completions.sort_values("credit_date")
    completions["row_num"] = completions.groupby("userid").cumcount() + 1
    first4 = completions[completions["row_num"] <= 4]

    completion_karma = []
    for _, row in first4.iterrows():
        has_assess = row["courseID"] in courses_with_assess
        pts = 10 if has_assess else 5
        addinfo = json.dumps({
            "COURSE_COMPLETION": True,
            "COURSENAME": row.get("courseName", ""),
            "ACBP": False,
            "ASSESSMENT": has_assess,
        })
        completion_karma.append({
            "context_id": str(row["courseID"]),
            "userid": str(row["userid"]),
            "context_type": str(row.get("category", "")),
            "credit_date": row["credit_date"],
            "operation_type": "COURSE_COMPLETION",
            "addinfo": addinfo,
            "points": pts,
        })

    all_karma = rating_karma + completion_karma

    if not all_karma:
        log.info("No karma points to write this month")
        con.close()
        return

    # ── Write to Cassandra ───────────────────────────────────────
    # Build DataFrames for Cassandra write (uses existing utils.writeToCassandra)
    # Since we no longer have Spark, use cassandra-driver directly
    from cassandra.cluster import Cluster

    cluster = Cluster([config.sparkCassandraConnectionHost], port=9042)
    session = cluster.connect(config.cassandraUserKeyspace)

    # Karma points table
    insert_karma = session.prepare(f"""
        INSERT INTO {config.cassandraKarmaPointsTable}
        (context_id, userid, context_type, credit_date, operation_type, addinfo, points)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    for kp in all_karma:
        session.execute(insert_karma, (
            kp["context_id"], kp["userid"], kp["context_type"],
            kp["credit_date"], kp["operation_type"], kp["addinfo"], kp["points"],
        ))

    # Lookup table
    insert_lookup = session.prepare(f"""
        INSERT INTO {config.cassandraKarmaPointsLookupTable}
        (user_karma_points_key, operation_type, credit_date)
        VALUES (?, ?, ?)
    """)
    for kp in all_karma:
        key = f"{kp['userid']}|{kp['context_type']}|{kp['context_id']}"
        session.execute(insert_lookup, (key, kp["operation_type"], kp["credit_date"]))

    # Summary table (aggregate + merge with existing)
    user_points = {}
    for kp in all_karma:
        uid = kp["userid"]
        user_points[uid] = user_points.get(uid, 0) + kp["points"]

    # Read existing summary from cache parquet
    cache_path = getattr(config, 'baseCachePath',
                         str(Path(__file__).resolve().parents[2] / "data-res/pq_files/cache_pq"))
    try:
        existing = con.execute(f"""
            SELECT userid, total_points FROM read_parquet('{cache_path}/userKarmaPointsSummary/**/*.parquet')
        """).fetchdf()
        for _, row in existing.iterrows():
            uid = str(row["userid"])
            if uid in user_points:
                user_points[uid] += int(row["total_points"])
            else:
                user_points[uid] = int(row["total_points"])
    except Exception:
        log.info("No existing karma summary found")

    insert_summary = session.prepare(f"""
        INSERT INTO {config.cassandraKarmaPointsSummaryTable}
        (userid, total_points)
        VALUES (?, ?)
    """)
    for uid, pts in user_points.items():
        session.execute(insert_summary, (uid, pts))

    session.shutdown()
    cluster.shutdown()
    con.close()
    log.info(f"[SUCCESS] KarmaPoints — wrote {len(all_karma)} records")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] KarmaPoints at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] KarmaPoints — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
