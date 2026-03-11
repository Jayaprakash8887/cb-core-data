"""
ODCS Recommendation — DuckDB Migration
Computes top-15 recommended content per MDO based on completion
percentage, rating, and enrolment count → Redis.
"""
import sys
import logging
from pathlib import Path
from datetime import datetime

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH
from dfutil.utils.redis import Redis

log = logging.getLogger("odcs-recommendation")


def process_data(config, db_path=None):
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    # Content stats: completion %, avg rating, enrolment count per MDO + content
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _content_stats AS
        WITH enrolments AS (
            SELECT ew."courseID" AS content_id, uw."userOrgID" AS mdo_id,
                   COUNT(*) AS total_enrolments,
                   SUM(CASE WHEN ew."user_consumption_status" = 'completed' THEN 1 ELSE 0 END) AS completed
            FROM enrolment_warehouse_computed ew
            INNER JOIN user_warehouse_computed uw ON ew."userID" = uw."userID"
            INNER JOIN content_warehouse_computed cw ON ew."courseID" = cw.content_id
                AND cw.content_sub_type IN ('Course','Program','Moderated Course','Moderated Program')
            GROUP BY ew."courseID", uw."userOrgID"
        ),
        ratings AS (
            SELECT activityid AS content_id,
                   COUNT(*) AS rating_count,
                   SUM(CAST(rating AS DOUBLE)) / COUNT(*) AS avg_rating
            FROM rating_computed
            WHERE activitytype = 'Course'
            GROUP BY activityid
        )
        SELECT e.mdo_id, e.content_id,
               CASE WHEN e.total_enrolments > 0
                   THEN (e.completed * 100.0 / e.total_enrolments) ELSE 0 END AS completion_pct,
               COALESCE(r.avg_rating, 0) AS avg_rating,
               e.total_enrolments
        FROM enrolments e
        LEFT JOIN ratings r ON e.content_id = r.content_id
    """)

    # Rank per MDO: completion% desc, rating desc, enrolments desc
    recommendations = con.execute("""
        SELECT mdo_id,
               STRING_AGG(content_id, ',' ORDER BY rank) AS top_15_content_ids
        FROM (
            SELECT mdo_id, content_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY mdo_id
                       ORDER BY completion_pct DESC, avg_rating DESC, total_enrolments DESC
                   ) AS rank
            FROM _content_stats
        ) ranked
        WHERE rank <= 15
        GROUP BY mdo_id
    """).fetchall()

    data_dict = {r[0]: r[1] for r in recommendations if r[0]}
    Redis.dispatch("odcs_course_recomendation", data_dict, replace=True, conf=config)

    con.close()
    log.info(f"[SUCCESS] ODCSRecommendation — {len(data_dict)} MDOs")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] ODCSRecommendation at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] ODCSRecommendation — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
