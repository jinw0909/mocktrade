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

mysql = MySQLAdapter()
def update_position_status_to_redis():
    # logger.info("updating position status to the local redis")
    conn = mysql._get_connection()
    cursor = conn.cursor()
    try:
        # 1) load all active positions
        cursor.execute("""
            SELECT 
                   ph.id AS `pos_id`,
                   ph.*,
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
                "margin": r["margin"],
                "margin_type": r["margin_type"],
                "size": r["size"],
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

def update_position_status_per_user(user_id, retri_id):
    conn = None
    cursor = None
    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                ph.*,
                ph.id AS `pos_id`
            FROM mocktrade.position_history as ph
           WHERE ph.user_id = %s
             AND ph.status = 1
        """, (user_id,))
        position_rows = cursor.fetchall()

        if not retri_id:
            cursor.execute("""
                SELECT retri_id
                  FROM mocktrade.user
                 WHERE `id` = %s
                   AND status = 0
                 LIMIT 1
            """, (user_id,))
            row = cursor.fetchone()
            if not row:
                logger.warning(f"user {user_id} not found")
                raise LookupError(f"retri id of user_id={user_id} not found")
            retri_id = row['retri_id']

        positions = [{
            # 'pos_id': r['pos_id'],
            # 'symbol': r['symbol'],
            # 'entry_price': r['entry_price'],
            # 'amount': float(r['amount']),
            # 'side': r['side']
            "pos_id": r['pos_id'],
            "symbol": r["symbol"],
            "entry_price": float(r["entry_price"]),
            "amount": float(r["amount"]),
            "side": r["side"],
            "margin": r["margin"],
            "margin_type": r["margin_type"],
            "size": r["size"],
        } for r in position_rows]

        key = f"positions:{retri_id}"
        redis_client.delete(key)
        mapping = { p['symbol']: json.dumps(p) for p in positions}
        if mapping:
            redis_client.hset(key, mapping=mapping)

        logger.info(f"completed updating position status of user [{user_id}]")

    except Exception:
        logger.exception(f"failed to update position of {user_id} to the local redis")
    finally:
        if conn:
            try: conn.close()
            except: pass
        if cursor:
            try: cursor.close()
            except: pass

def update_order_status_to_redis():
    conn = None
    cursor = None
    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                 oh.`id` AS `or_id`,
                 oh.`user_id`,
                 oh.`symbol`,
                 oh.`type`,
                 oh.`margin_type`,
                 oh.amount,
                 oh.`price`,
                 oh.`order_price`,
                 oh.`magin` AS `margin`,
                 oh.`side`,
                 oh.`tp`,
                 oh.`sl`,
                 u.`retri_id`
            FROM `mocktrade`.`order_history` AS oh
            JOIN `mocktrade`.`user` AS u 
              ON oh.`user_id` = u.`id`
           WHERE oh.`status` = 0  
        """)

        rows = cursor.fetchall()

        # group by retri_id
        orders_by_user = {}
        for r in rows:
            uid = r['retri_id']
            orders_by_user.setdefault(uid, []).append({
                'or_id': r['or_id'],
                'symbol': r['symbol'],
                'price': r['price'],
                'type': r['type'],
                'margin_type': r['margin_type'],
                'margin': r['margin'],
                'side': r['side'],
                'order_price': r['order_price'],
                'amount': r['amount'],
                'tp': r['tp'],
                'sl': r['sl']
            })

        # overwrite each active user's redis hash
        for uid, order_list in orders_by_user.items():
            key = f"orders:{uid}"
            redis_client.delete(key)
            payload = json.dumps(order_list)
            if payload:
                redis_client.set(key, payload)

        # remove any leftover positions
        for key in redis_client.scan_iter("orders:*"):
            try:
                _, uid = key.split(":", 1)
            except ValueError:
                continue
            if uid not in orders_by_user:
                redis_client.delete(key)

        logger.info(f"Updated orders for {len(orders_by_user)} users at {datetime.now(timezone('Asia/Seoul'))}")

    except Exception:
        logger.exception(f"failed to update orders status to Redis")
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

def update_order_status_per_user(user_id, retri_id):
    conn = None
    cursor = None
    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                oh.*,
                oh.`id` AS `or_id`,
                oh.`magin` AS `margin` 
              FROM `mocktrade`.`order_history` AS oh
             WHERE oh.`user_id` = %s
               AND oh.`status` = 0 
        """, (user_id,))
        order_rows = cursor.fetchall()

        if not retri_id:
            cursor.execute("""
                SELECT `retri_id` 
                  FROM `mocktrade`.`user`
                 WHERE `id` = %s
                   AND `status` = 0 
            """, (user_id, ))
            row = cursor.fetchone()
            if not row:
                logger.warning(f"user {user_id} not found")
                raise LookupError(f"retri_id for user_id={user_id} not found")

            retri_id = row['retri_id']

        orders = [{
            'or_id': r['or_id'],
            'symbol': r['symbol'],
            'price': r['price'],
            'type': r['type'],
            'margin_type': r['margin_type'],
            'margin': r['margin'],
            'side': r['side'],
            'order_price': r['order_price'],
            'amount': r['amount'],
            'tp': r['tp'],
            'sl': r['sl']} for r in order_rows]

        key = f"orders:{retri_id}"
        redis_client.delete(key)
        payload = json.dumps(orders)
        if payload:
            redis_client.set(key, payload)

        logger.info(f"completed updating order status of user [{user_id}]")

    except Exception:
        logger.exception(f"Failed to update the order status of user [{user_id}]")
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

def update_balance_status_to_redis():
    conn = None
    cursor = None
    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT `balance`, `retri_id` 
              FROM `mocktrade`.`user` AS u
             WHERE `status` = 0
        """)

        user_rows = cursor.fetchall()

        # group by retri_id
        balances_by_user = {
            r['retri_id']: float(r['balance'])
            for r in user_rows
        }

        for uid, balance in balances_by_user.items():
            key = f"balances:{uid}"
            redis_client.delete(key)
            if balance:
                redis_client.set(key, balance)

        for key in redis_client.scan_iter("balances:*"):
            try:
                _, uid = key.split(":", 1)
            except ValueError:
                continue
            if uid not in balances_by_user:
                redis_client.delete(key)

        logger.info(f"updated balances for {len(balances_by_user)} users at {datetime.now(timezone('Asia/Seoul'))}")

    except Exception:
        logger.exception(f"failed to update balance status to redis")
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

def update_balance_status_per_user(user_id):
    conn = None
    cursor = None
    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT retri_id, balance
              FROM mocktrade.user
             WHERE `id` = %s
               AND status = 0 
        """, (user_id,))

        user_row = cursor.fetchone()
        if not user_row:
            logger.exception(f"could not find user with id of {user_id}")
            return

        uid = user_row['retri_id']
        balance = user_row['balance']

        key = f"balances:{uid}"
        redis_client.delete(key)
        redis_client.set(key, balance)

        logger.info(f"updated balance of user {user_id} to redis ({balance})")

    except Exception:
        logger.exception(f"failed to update balance of user [{user_id}]")
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass


