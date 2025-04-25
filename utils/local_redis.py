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


def update_position_status_to_redis():
    logger.info("updating position status to the local redis")
    mysql = MySQLAdapter()
    conn = mysql._get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT user_id,
                   symbol,
                   entry_price,
                   amount,
                   side
              FROM mocktrade.position_history
             WHERE status = 1
        """)
        rows = cursor.fetchall()

        # Group by user_id
        positions_by_user = {}
        for r in rows:
            uid = r["user_id"]
            positions_by_user.setdefault(uid, []).append({
                "symbol": r["symbol"],
                "entry_price": float(r["entry_price"]),
                "amount": float(r["amount"]),
                "side": r["side"],
            })

        # Write into Redis: one hash per user
        for uid, pos_list in positions_by_user.items():
            key = f"positions:{uid}"
            # Start by deleting old data for this user
            redis_client.delete(key)
            # Then HSET symbol -> JSON for each position
            mapping = { p["symbol"]: json.dumps(p) for p in pos_list }
            if mapping:
                redis_client.hset(key, mapping=mapping)

        logger.info(f"Updated positions for {len(positions_by_user)} users at {datetime.now(timezone('Asia/Seoul'))}")

    except Exception:
        logger.exception("Failed to update position status to Redis")
    finally:
        cursor.close()
        conn.close()
