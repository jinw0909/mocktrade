import json
from utils.settings import MySQLAdapter  # your adapter
import logging  # assume you have a logger

import json
import redis
from datetime import datetime

from pytz import timezone

logger = logging.getLogger('uvicorn')

# decode_responses=True makes redis return str instead of bytes
redis_client = redis.Redis(
    host="localhost", port=6379, db=0, decode_responses=True
)


async def update_position_status_to_redis():
    # logger.info("updating position status to the local redis")
    mysql = MySQLAdapter()
    conn = mysql._get_connection()
    cursor = conn.cursor()
    try:
        # 1) load all active positions
        cursor.execute("""
            SELECT ph.user_id,
                   ph.id AS `pos_id`,
                   ph.symbol,
                   ph.entry_price,
                   ph.amount,
                   ph.side,
                   u.retri_id
              FROM `mocktrade`.`position_history` as ph
              JOIN `mocktrade`.`user` AS u
                ON ph.user_id = u.id 
             WHERE ph.status = 1
        """)
        rows = cursor.fetchall()

        # 2) Group by retri_id
        positions_by_user = {}
        for r in rows:
            uid = r["retri_id"]
            positions_by_user.setdefault(uid, []).append({
                "pos_id": r['pos_id'],
                "symbol": r["symbol"],
                "entry_price": float(r["entry_price"]),
                "amount": float(r["amount"]),
                "side": r["side"],
            })

        # 3) overwrite each active user's Redis hash
        for uid, pos_list in positions_by_user.items():
            key = f"positions:{uid}"
            # Start by deleting old data for this user
            redis_client.delete(key)
            # Then HSET symbol -> JSON for each position
            mapping = { p["symbol"]: json.dumps(p) for p in pos_list }
            if mapping:
                redis_client.hset(key, mapping=mapping)

        # 4) remove any leftover "positions:{uid}" keys for users who no longer have active positions
        for key in redis_client.scan_iter("positions:*"):
            # extract uid portion
            try:
                _, uid = key.split(":", 1)
            except ValueError:
                continue
            if uid not in positions_by_user:
                redis_client.delete(key)

        logger.info(f"Updated positions for {len(positions_by_user)} users at {datetime.now(timezone('Asia/Seoul'))}")

    except Exception:
        logger.exception("Failed to update position status to Redis")
    finally:
        cursor.close()
        conn.close()
