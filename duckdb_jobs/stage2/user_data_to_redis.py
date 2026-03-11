"""
User Data to Redis — DuckDB Migration
Pushes user profile data (name, designation, profile image, etc.)
to Redis as JSON per user_id key.
"""
import json
import sys
import logging
from pathlib import Path
from datetime import datetime

import duckdb
import redis as redis_lib

sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckdb_jobs.core.config_loader import load_config, DB_PATH

log = logging.getLogger("user-data-to-redis")

BATCH_SIZE = 25_000


def process_data(config, db_path=None):
    con = duckdb.connect(db_path or DB_PATH, read_only=True)

    users = con.execute("""
        SELECT
            "userID"                             AS user_id,
            "firstName"                          AS first_name,
            "userProfileImgUrl"                  AS user_profile_img_url,
            "userProfileStatus"                  AS user_profile_status,
            "professionalDetails_designation"    AS designation,
            "userOrgName"                         AS department
        FROM user_org_computed
    """).fetchdf()

    con.close()

    log.info(f"Pushing {len(users)} user records to Redis...")

    redis_client = redis_lib.Redis(
        host=config.redisHost,
        port=int(config.redisPort),
        decode_responses=True,
    )
    pipeline = redis_client.pipeline()
    flushed = 0

    for _, row in users.iterrows():
        uid = row["user_id"]
        if not uid:
            continue
        redis_key = f"user:{uid}"
        redis_value = json.dumps({
            "user_id": uid,
            "first_name": row.get("first_name", ""),
            "user_profile_img_url": row.get("user_profile_img_url", ""),
            "userProfileStatus": row.get("user_profile_status", ""),
            "designation": row.get("designation", ""),
            "department": row.get("department", ""),
        })
        pipeline.set(redis_key, redis_value)
        flushed += 1

        if flushed >= BATCH_SIZE:
            pipeline.execute()
            flushed = 0

    if flushed > 0:
        pipeline.execute()

    redis_client.close()
    log.info(f"[SUCCESS] UserDataToRedis — {len(users)} keys written")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    t0 = datetime.now()
    log.info(f"[START] UserDataToRedis at {t0:%Y-%m-%d %H:%M:%S}")
    process_data(config)
    log.info(f"[END] UserDataToRedis — {datetime.now() - t0}")


if __name__ == "__main__":
    main()
