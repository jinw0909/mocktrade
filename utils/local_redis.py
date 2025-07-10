import json
from utils.connections import MySQLAdapter  # your adapter
import logging  # assume you have a logger

import json
import redis.asyncio as aioredis
from datetime import datetime
from starlette.config import Config

from pytz import timezone

config = Config('.env')
logger = logging.getLogger('uvicorn')

# decode_responses=True makes redis return str instead of bytes
redis_client = aioredis.Redis(
    host="localhost", port=6379, db=0, decode_responses=True
)
price_redis = aioredis.Redis(
    host=config.get('REDIS_HOST'), port=6379, db=0, decode_responses=True
)

mysql = MySQLAdapter()
async def update_position_status_to_redis():
    logger.info("Updating MySQL position status to the local Redis")
    conn = mysql._get_connection()
    cursor = conn.cursor()
    try:
        # 1) load all active positions
        cursor.execute("""
            SELECT 
                   ph.`id` AS `pos_id`,
                   ph.`status` AS `status`,
                   ph.`user_id`,
                   symbol, size, amount, entry_price, liq_price, margin, pnl, margin_type, side, leverage, tp, sl, close_price, unrealized_pnl, unrealized_pnl_pct,
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
            symbol = r["symbol"]
            amount = float(r["amount"])
            entry_price = float(r["entry_price"])
            side = r["side"]
            leverage = int(r["leverage"]) if r["leverage"] else 1.0
            margin_type = r["margin_type"]

            # Get current price from Redis
            price_key = f"price:{symbol}USDT"
            price_raw = await price_redis.get(price_key)
            try:
                current_price = float(price_raw) if price_raw else entry_price
            except:
                current_price = entry_price

            # Recalculate size
            size = current_price * amount
            # Recalculate margin if cross
            margin = float(r["margin"])
            if margin_type == 'cross' and leverage:
                margin = size / leverage
            # Recalculate PnL
            if side == 'buy':
                pnl = (current_price - entry_price) * amount
            else:
                pnl = (entry_price - current_price) * amount

            pnl_pct = (pnl / (entry_price * amount)) * 100 if entry_price else 0.0
            roi_pct = (pnl / margin) * 100 if margin else 0.0

            positions_by_user.setdefault(uid, []).append({
                "pos_id": r['pos_id'],
                "user_id": r['user_id'],
                "symbol": symbol,
                "entry_price": entry_price,
                "liq_price": float(r["liq_price"]),
                "market_price": current_price,
                "amount": amount,
                "side": side,
                "margin": margin,
                "margin_type": margin_type,
                "size": size,
                "leverage": leverage,
                "tp": r["tp"],
                "sl": r["sl"],
                "unrealized_pnl": pnl,
                "unrealized_pnl_pct": pnl_pct,
                "roi_pct": roi_pct
            })

        # 3) overwrite each active user's Redis hash
        for uid, pos_list in positions_by_user.items():
            key = f"positions:{uid}"
            # Start by deleting old data for this user
            await redis_client.delete(key)
            # Then HSET symbol -> JSON for each position
            mapping = { p["symbol"]: json.dumps(p) for p in pos_list }
            if mapping:
                await redis_client.hset(key, mapping=mapping)

        # 4) remove any leftover "positions:{uid}" keys for users who no longer have active positions + availables:{uid}
        async for key in redis_client.scan_iter("positions:*"):
            # extract uid portion
            try:
                _, uid = key.split(":", 1)
            except ValueError:
                continue
            if uid not in positions_by_user:
                await redis_client.delete(key)

        logger.info(f"Updated positions for {len(positions_by_user)} users at {datetime.now(timezone('Asia/Seoul'))}")

    except Exception:
        logger.exception("Failed to update position status to Redis")
    finally:
        cursor.close()
        conn.close()

async def update_position_status_per_user(user_id, retri_id):
    conn = None
    cursor = None
    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                ph.`id` AS `pos_id`,
                user_id, symbol, size, amount, entry_price, liq_price, margin, pnl, margin_type, side, leverage, status, tp, sl, close_price, unrealized_pnl, unrealized_pnl_pct
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

        positions = []
        for r in position_rows:
            symbol = r['symbol']
            entry_price = float(r['entry_price'])
            amount = float(r['amount'])
            side = r['side']
            leverage = int(r['leverage']) if r['leverage'] else 1
            margin_type = r['margin_type']

            # Get current price
            price_key = f"price:{symbol}USDT"
            price_raw = await price_redis.get(price_key)
            try:
                current_price = float(price_raw) if price_raw else entry_price
            except:
                current_price = entry_price

            size = current_price * amount
            margin = float(r['margin'])
            if margin_type == 'cross' and leverage:
                margin = size / leverage

            if side == 'buy':
                pnl = (current_price - entry_price) * amount
            else:
                pnl = (entry_price - current_price) * amount

            pnl_pct = (pnl / (entry_price * amount)) * 100 if entry_price else 0.0
            roi_pct = (pnl / margin) if margin else 0.0

            positions.append({
                "pos_id": r["pos_id"],
                "user_id": r["user_id"],
                "symbol": symbol,
                "entry_price": entry_price,
                "liq_price": float(r["liq_price"]),
                "market_price": current_price,
                "amount": amount,
                "side": side,
                "margin": margin,
                "margin_type": margin_type,
                "size": size,
                "leverage": leverage,
                "tp": r["tp"],
                "sl": r["sl"],
                "unrealized_pnl": pnl,
                "unrealized_pnl_pct": pnl_pct,
                "roi_pct": roi_pct
            })

        key = f"positions:{retri_id}"
        await redis_client.delete(key)
        mapping = { p['symbol']: json.dumps(p) for p in positions}
        if mapping:
            await redis_client.hset(key, mapping=mapping)

        logger.info(f"completed updating position status of user [{user_id}] to redis")

    except Exception:
        logger.exception(f"failed to update position of {user_id} to the local redis")
    finally:
        if conn:
            try: conn.close()
            except: pass
        if cursor:
            try: cursor.close()
            except: pass

async def update_order_status_to_redis():
    logger.info("Updating MySQL order status to the local Redis")
    conn = None
    cursor = None
    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                 oh.`id` AS `or_id`,
                 oh.`status` AS `status`,
                 oh.`magin` AS `margin`,
                 oh.`user_id`,
                 symbol, type, margin_type, side, price, amount, leverage, order_price, po_id, tp, sl,
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
                'user_id': r['user_id'],
                'symbol': r['symbol'],
                'price': r['price'],
                'type': r['type'],
                'margin_type': r['margin_type'],
                'margin': r['margin'],
                'leverage': r['leverage'],
                'side': r['side'],
                'order_price': r['order_price'],
                'amount': r['amount'],
                'tp': r['tp'],
                'sl': r['sl'],
                'po_id': r['po_id']
            })

        # overwrite each active user's redis hash
        for uid, order_list in orders_by_user.items():
            key = f"orders:{uid}"
            # await redis_client.delete(key)
            payload = json.dumps(order_list)
            if payload:
                await redis_client.set(key, payload)

        # remove any leftover positions
        async for key in redis_client.scan_iter("orders:*"):
            try:
                _, uid = key.split(":", 1)
            except ValueError:
                continue
            if uid not in orders_by_user:
                await redis_client.delete(key)

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

async def update_order_status_per_user(user_id, retri_id):
    conn = None
    cursor = None
    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                 oh.`id` AS `or_id`,
                 oh.`status` AS `status`,
                 oh.`magin` AS `margin`,
                 oh.symbol, type, margin_type, side, price, amount, leverage, order_price, po_id, tp, sl, user_id
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
            'user_id': r['user_id'],
            'symbol': r['symbol'],
            'price': r['price'],
            'type': r['type'],
            'margin_type': r['margin_type'],
            'margin': r['margin'],
            'side': r['side'],
            'leverage': r['leverage'],
            'order_price': r['order_price'],
            'amount': r['amount'],
            'tp': r['tp'],
            'sl': r['sl'],
            'po_id': r['po_id']
        } for r in order_rows]

        key = f"orders:{retri_id}"
        # await redis_client.delete(key)
        payload = json.dumps(orders)
        if payload:
            await redis_client.set(key, payload)

        logger.info(f"completed updating order status of user [{user_id}] to redis")

    except Exception:
        logger.exception(f"Failed to update the order status of user [{user_id}]")
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

async def update_balance_status_to_redis():
    logger.info("Updating MySQL balance status to the local Redis")
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
            await redis_client.delete(key)
            if balance:
                await redis_client.set(key, balance)

        async for key in redis_client.scan_iter("balances:*"):
            try:
                _, uid = key.split(":", 1)
            except ValueError:
                continue
            if uid not in balances_by_user:
                await redis_client.delete(key)
                await redis_client.delete(f"availables:{uid}")

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

async def update_balance_status_per_user(user_id, retri_id = None):
    conn = None
    cursor = None
    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        if not user_id:
            cursor.execute("""
                SELECT `id`
                  FROM mocktrade.user
                 WHERE `retri_id` = %s
                   AND status = 0
                 LIMIT 1
            """, (retri_id, ))
            row = cursor.fetchone()
            if not row:
                logger.exception(f"there is no user with a retri_id of {retri_id}")
                return
            user_id = row['id']

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
        await redis_client.delete(key)
        await redis_client.set(key, balance)

        logger.info(f"completed updating balance status of user [{user_id}] to redis ({balance})")

    except Exception:
        logger.exception(f"failed to update balance of user [{user_id}]")
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

# async def update_liq_price():
#     logger.info("Uploading cross position liquidation prices to MySQL")
#     conn = None
#     cursor = None
#     row_count = 0
#
#     try:
#         conn = mysql._get_connection()
#         cursor = conn.cursor()
#
#         async for key in redis_client.scan_iter("liq_prices:*"):
#             raw = await redis_client.get(key)
#             if not raw:
#                 continue
#             cursor.execute("SAVEPOINT liq_updt")
#             try:
#                 data = json.loads(raw)
#                 positions = data.get("positions", [])
#                 for pos in positions:
#                     pos_id = pos.get("pos_id")
#                     liq_price = pos.get("liq_price")
#                     if pos_id is None or liq_price is None:
#                         continue
#
#                     cursor.execute("""
#                         UPDATE mocktrade.position_history
#                         SET `liq_price` = %s
#                         WHERE `id` = %s
#                     """, (liq_price, pos_id))
#
#                     row_count += 1
#
#                 cursor.execute("RELEASE SAVEPOINT liq_updt")
#
#             except Exception as e:
#                 logger.warning(f"Failed to parse or update {key} : {e}")
#                 cursor.execute("ROLLBACK TO SAVEPOINT liq_updt")
#                 cursor.execute("RELEASE SAVEPOINT liq_updt")
#                 continue
#
#         conn.commit()
#         logger.info(f"Updated {row_count} liquidation prices successfully to MySQL")
#
#     except Exception:
#         logger.exception("failed to update liq price from redis to MySQL")
#         conn.rollback()
#
#     finally:
#         cursor and cursor.close()
#         conn and conn.close()

async def update_liq_price():
    logger.info("Uploading cross position liquidation prices to MySQL")
    conn = None
    cursor = None
    row_count = 0

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        # Scan every user’s positions hash
        async for pos_key in redis_client.scan_iter("positions:*"):
            # pos_key is like "positions:447"
            # fetch all field→JSON blobs
            raw_hash = await redis_client.hgetall(pos_key)
            if not raw_hash:
                continue

            # For each symbol entry in that hash
            for symbol, blob in raw_hash.items():
                # blob comes back as bytes; decode & parse
                data = json.loads(blob)

                # only update cross‐margin legs
                if data.get("margin_type") != "cross":
                    continue

                pos_id    = data.get("pos_id")
                liq_price = data.get("liq_price")
                if pos_id is None or liq_price is None:
                    continue

                # Wrap each position update in a savepoint so one bad row won't kill the batch
                cursor.execute("SAVEPOINT liq_updt")
                try:
                    cursor.execute(
                        """
                        UPDATE mocktrade.position_history
                           SET liq_price = %s
                         WHERE id = %s
                        """,
                        (liq_price, pos_id)
                    )
                    row_count += 1
                    cursor.execute("RELEASE SAVEPOINT liq_updt")

                except Exception as e:
                    logger.warning(f"Failed updating pos_id={pos_id} : {e}")
                    cursor.execute("ROLLBACK TO SAVEPOINT liq_updt")
                    cursor.execute("RELEASE SAVEPOINT liq_updt")
                    # continue to next leg
                    continue

        conn.commit()
        logger.info(f"Updated {row_count} liquidation prices successfully to MySQL")

    except Exception:
        if conn:
            conn.rollback()
        logger.exception("Critical failure in update_liq_price")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# async def sync_tpsl_status_per_user(user_id, retri_id = None):
#     logger.info("syncing position tpsl pointer for user {} ...", user_id)
#     conn = None
#     cursor = None
#     row_count = 0
#
#     try:
#         logger.info("hi")
#         conn = mysql._get_connection()
#         cursor = conn.cursor()
#
#         cursor.execute("""
#             SELECT *
#               FROM `mocktrade`.`order_history`
#              WHERE `user_id` = %s
#                AND `order_price` = 0
#                AND `status` = 0
#                AND `type` IN ('tp', 'sl')
#         """, (user_id,))
#
#         order_rows = cursor.fetchall()
#         if not order_rows:
#             return
#
#         for order in order_rows:
#             order_symbol = order['symbol']
#             order_id = order['id']
#
#             if not order_symbol:
#                 continue
#
#             cursor.execute("""
#                 SELECT *
#                   FROM `mocktrade`.`position_history`
#                  WHERE `symbol` = %s
#                    AND `user_id` = %s
#                    AND `status` = 1
#               ORDER BY `id` DESC
#                  LIMIT 1
#             """, (order_symbol, user_id))
#
#             current_position = cursor.fetchone()
#             if not current_position:
#                 continue
#
#             current_price = current_position.get('price')
#             if not current_price:
#                 continue
#             current_position_id = current_position.get['id']
#
#             cursor.execute("""
#                 UPDATE `mocktrade`.`order_history`
#                 SET `price` = %s,
#                     ``
#                 WHERE ``
#             """)
#
#
#
#
#     except Exception:
#         if conn:
#             conn.rollback()
#         logger.warning("error occurred syncing position tpsl pointer for user {}", user_id)
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()



