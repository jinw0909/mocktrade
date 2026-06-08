# import json
# from utils.connections import MySQLAdapter  # your adapter
# import logging  # assume you have a logger
#
# import json
# import redis.asyncio as aioredis
# from datetime import datetime
# from starlette.config import Config
#
# from pytz import timezone
#
# config = Config('.env')
# logger = logging.getLogger('uvicorn')
#
# # decode_responses=True makes redis return str instead of bytes
# redis_client = aioredis.Redis(
#     host=config.get("LOCAL_REDIS"), port=6379, db=0, decode_responses=True
# )
# price_redis = aioredis.Redis(
#     host=config.get('REDIS_HOST_OKX'), port=6379, db=0, decode_responses=True
# )
#
# mysql = MySQLAdapter()
#
# async def update_position_status_to_redis_okx():
#     logger.info("Updating MySQL position status to the local Redis")
#     conn = mysql._get_connection()
#     cursor = conn.cursor()
#
#     try:
#         # 1) load all active positions
#         cursor.execute("""
#                        SELECT
#                            ph.`id` AS `pos_id`,
#                            ph.`status` AS `status`,
#                            ph.`user_id`,
#                            symbol, size, amount, entry_price, liq_price, margin, pnl,
#                            margin_type, side, leverage, tp, sl, close_price,
#                            unrealized_pnl, unrealized_pnl_pct,
#                            u.retri_id
#                        FROM position_history_okx ph
#                                 JOIN user_okx u
#                                      ON ph.user_id = u.id
#                        WHERE ph.status = 1
#                        """)
#         rows = cursor.fetchall()
#
#         positions_by_user = {}
#
#         for r in rows:
#             uid = r["retri_id"]
#             symbol = r["symbol"]
#             user_key = f"positions_okx:{uid}"
#
#             amount = float(r["amount"])
#             entry_price = float(r["entry_price"])
#             side = r["side"]
#             leverage = int(r["leverage"]) if r["leverage"] else 1
#             margin_type = r["margin_type"]
#
#             # Get current price
#             price_key = f"price:{symbol}USDT"
#             price_raw = await price_redis.get(price_key)
#             try:
#                 current_price = float(price_raw) if price_raw else entry_price
#             except:
#                 current_price = entry_price
#
#             size = current_price * amount
#
#             margin = float(r["margin"])
#             if margin_type == "cross" and leverage:
#                 margin = size / leverage
#
#             if side == "buy":
#                 pnl = (current_price - entry_price) * amount
#             else:
#                 pnl = (entry_price - current_price) * amount
#
#             pnl_pct = (pnl / (entry_price * amount)) * 100 if entry_price else 0.0
#             roi_pct = (pnl / margin) * 100 if margin else 0.0
#
#             # ⭐ FIX 1: read existing redis value (to preserve computed liq_price)
#             existing_blob = await redis_client.hget(user_key, symbol)
#             if existing_blob:
#                 try:
#                     existing_liq = json.loads(existing_blob).get("liq_price")
#                 except:
#                     existing_liq = None
#             else:
#                 existing_liq = None
#
#             # ⭐ FIX 2: choose correct liq_price source
#             # Redis has priority because it contains NEWLY computed values
#             final_liq_price = (
#                 existing_liq if existing_liq is not None else float(r["liq_price"])
#             )
#
#             positions_by_user.setdefault(uid, []).append({
#                 "pos_id": r["pos_id"],
#                 "user_id": r["user_id"],
#                 "symbol": symbol,
#                 "entry_price": entry_price,
#                 "liq_price": final_liq_price,   # ⭐ FIX 3 — use selected value
#                 "market_price": current_price,
#                 "amount": amount,
#                 "side": side,
#                 "margin": margin,
#                 "margin_type": margin_type,
#                 "size": size,
#                 "leverage": leverage,
#                 "tp": r["tp"],
#                 "sl": r["sl"],
#                 "unrealized_pnl": pnl,
#                 "unrealized_pnl_pct": pnl_pct,
#                 "roi_pct": roi_pct
#             })
#
#         # 3) Write to Redis
#         for uid, pos_list in positions_by_user.items():
#             key = f"positions_okx:{uid}"
#
#             await redis_client.delete(key)
#
#             mapping = {p["symbol"]: json.dumps(p) for p in pos_list}
#             if mapping:
#                 await redis_client.hset(key, mapping=mapping)
#
#         # 4) cleanup stale users
#         async for key in redis_client.scan_iter("positions_okx:*"):
#             try:
#                 _, uid = key.split(":", 1)
#             except ValueError:
#                 continue
#             if uid not in positions_by_user:
#                 await redis_client.delete(key)
#
#         logger.info(
#             f"Updated positions for {len(positions_by_user)} users at "
#             f"{datetime.now(timezone('Asia/Seoul'))}"
#         )
#
#     except Exception:
#         logger.exception("Failed to update position status to Redis")
#
#     finally:
#         cursor.close()
#         conn.close()
#
# async def update_position_status_per_user_okx(user_id, retri_id):
#     conn = None
#     cursor = None
#     try:
#         conn = mysql._get_connection()
#         cursor = conn.cursor()
#         cursor.execute("""
#             SELECT
#                 ph.`id` AS `pos_id`,
#                 user_id, symbol, size, amount, entry_price, liq_price, margin, pnl, margin_type, side, leverage, status, tp, sl, close_price, unrealized_pnl, unrealized_pnl_pct
#             FROM position_history_okx as ph
#            WHERE ph.user_id = %s
#              AND ph.status = 1
#         """, (user_id,))
#         position_rows = cursor.fetchall()
#
#         if not retri_id:
#             cursor.execute("""
#                 SELECT retri_id
#                   FROM user_okx
#                  WHERE `id` = %s
#                    AND status = 0
#                  LIMIT 1
#             """, (user_id,))
#             row = cursor.fetchone()
#             if not row:
#                 logger.warning(f"user {user_id} not found")
#                 raise LookupError(f"retri id of user_id={user_id} not found")
#             retri_id = row['retri_id']
#
#         positions = []
#         for r in position_rows:
#             symbol = r['symbol']
#             entry_price = float(r['entry_price'])
#             amount = float(r['amount'])
#             side = r['side']
#             leverage = int(r['leverage']) if r['leverage'] else 1
#             margin_type = r['margin_type']
#
#             # Get current price
#             price_key = f"price:{symbol}USDT"
#             price_raw = await price_redis.get(price_key)
#             try:
#                 current_price = float(price_raw) if price_raw else entry_price
#             except:
#                 current_price = entry_price
#
#             size = current_price * amount
#             margin = float(r['margin'])
#             if margin_type == 'cross' and leverage:
#                 margin = size / leverage
#
#             if side == 'buy':
#                 pnl = (current_price - entry_price) * amount
#             else:
#                 pnl = (entry_price - current_price) * amount
#
#             pnl_pct = (pnl / (entry_price * amount)) * 100 if entry_price else 0.0
#             roi_pct = (pnl / margin) * 100 if margin else 0.0
#
#             positions.append({
#                 "pos_id": r["pos_id"],
#                 "user_id": r["user_id"],
#                 "symbol": symbol,
#                 "entry_price": entry_price,
#                 "liq_price": float(r["liq_price"]),
#                 "market_price": current_price,
#                 "amount": amount,
#                 "side": side,
#                 "margin": margin,
#                 "margin_type": margin_type,
#                 "size": size,
#                 "leverage": leverage,
#                 "tp": r["tp"],
#                 "sl": r["sl"],
#                 "unrealized_pnl": pnl,
#                 "unrealized_pnl_pct": pnl_pct,
#                 "roi_pct": roi_pct
#             })
#
#         key = f"positions_okx:{retri_id}"
#         await redis_client.delete(key)
#         mapping = { p['symbol']: json.dumps(p) for p in positions}
#         if mapping:
#             await redis_client.hset(key, mapping=mapping)
#
#         logger.info(f"completed updating position status of user [{user_id}] to redis")
#
#     except Exception:
#         logger.exception(f"failed to update position of {user_id} to the local redis")
#     finally:
#         if conn:
#             try: conn.close()
#             except: pass
#         if cursor:
#             try: cursor.close()
#             except: pass
#
# async def update_order_status_to_redis_okx():
#     logger.info("Updating MySQL order status to the local Redis")
#     conn = None
#     cursor = None
#     try:
#         conn = mysql._get_connection()
#         cursor = conn.cursor()
#         cursor.execute("""
#             SELECT
#                  oh.`id` AS `or_id`,
#                  oh.`status` AS `status`,
#                  oh.`magin` AS `margin`,
#                  oh.`user_id`,
#                  symbol, type, margin_type, side, price, amount, leverage, order_price, po_id, tp, sl,
#                  u.`retri_id`
#             FROM `order_history_okx` AS oh
#             JOIN `user_okx` AS u
#               ON oh.`user_id` = u.`id`
#            WHERE oh.`status` = 0
#         """)
#
#         rows = cursor.fetchall()
#
#         # group by retri_id
#         orders_by_user = {}
#         for r in rows:
#             uid = r['retri_id']
#             orders_by_user.setdefault(uid, []).append({
#                 'or_id': r['or_id'],
#                 'user_id': r['user_id'],
#                 'symbol': r['symbol'],
#                 'price': r['price'],
#                 'type': r['type'],
#                 'margin_type': r['margin_type'],
#                 'margin': r['margin'],
#                 'leverage': r['leverage'],
#                 'side': r['side'],
#                 'order_price': r['order_price'],
#                 'amount': r['amount'],
#                 'tp': r['tp'],
#                 'sl': r['sl'],
#                 'po_id': r['po_id']
#             })
#
#         # overwrite each active user's redis hash
#         for uid, order_list in orders_by_user.items():
#             key = f"orders_okx:{uid}"
#             # await redis_client.delete(key)
#             payload = json.dumps(order_list)
#             if payload:
#                 await redis_client.set(key, payload)
#
#         # remove any leftover positions
#         async for key in redis_client.scan_iter("orders_okx:*"):
#             try:
#                 _, uid = key.split(":", 1)
#             except ValueError:
#                 continue
#             if uid not in orders_by_user:
#                 await redis_client.delete(key)
#
#         logger.info(f"Updated orders for {len(orders_by_user)} users at {datetime.now(timezone('Asia/Seoul'))}")
#
#     except Exception:
#         logger.exception(f"failed to update orders status to Redis")
#     finally:
#         if cursor:
#             try: cursor.close()
#             except: pass
#         if conn:
#             try: conn.close()
#             except: pass
#
# async def update_order_status_per_user_okx(user_id, retri_id):
#     conn = None
#     cursor = None
#     try:
#         conn = mysql._get_connection()
#         cursor = conn.cursor()
#         cursor.execute("""
#             SELECT
#                  oh.`id` AS `or_id`,
#                  oh.`status` AS `status`,
#                  oh.`magin` AS `margin`,
#                  oh.symbol, type, margin_type, side, price, amount, leverage, order_price, po_id, tp, sl, user_id
#               FROM `order_history_okx` AS oh
#              WHERE oh.`user_id` = %s
#                AND oh.`status` = 0
#         """, (user_id,))
#         order_rows = cursor.fetchall()
#
#         if not retri_id:
#             cursor.execute("""
#                 SELECT `retri_id`
#                   FROM `user_okx`
#                  WHERE `id` = %s
#                    AND `status` = 0
#             """, (user_id, ))
#             row = cursor.fetchone()
#             if not row:
#                 logger.warning(f"user {user_id} not found")
#                 raise LookupError(f"retri_id for user_id={user_id} not found")
#
#             retri_id = row['retri_id']
#
#         orders = [{
#             'or_id': r['or_id'],
#             'user_id': r['user_id'],
#             'symbol': r['symbol'],
#             'price': r['price'],
#             'type': r['type'],
#             'margin_type': r['margin_type'],
#             'margin': r['margin'],
#             'side': r['side'],
#             'leverage': r['leverage'],
#             'order_price': r['order_price'],
#             'amount': r['amount'],
#             'tp': r['tp'],
#             'sl': r['sl'],
#             'po_id': r['po_id']
#         } for r in order_rows]
#
#         key = f"orders_okx:{retri_id}"
#         # await redis_client.delete(key)
#         payload = json.dumps(orders)
#         if payload:
#             await redis_client.set(key, payload)
#
#         logger.info(f"completed updating order status of user [{user_id}] to redis")
#
#     except Exception:
#         logger.exception(f"Failed to update the order status of user [{user_id}]")
#     finally:
#         if cursor:
#             try: cursor.close()
#             except: pass
#         if conn:
#             try: conn.close()
#             except: pass
#
# async def update_balance_status_to_redis_okx():
#     logger.info("Updating MySQL balance status to the local Redis")
#     conn = None
#     cursor = None
#     try:
#         conn = mysql._get_connection()
#         cursor = conn.cursor()
#
#         cursor.execute("""
#             SELECT `balance`, `retri_id`
#               FROM `user_okx` AS u
#              WHERE `status` = 0
#         """)
#
#         user_rows = cursor.fetchall()
#
#         # group by retri_id
#         balances_by_user = {
#             r['retri_id']: float(r['balance'])
#             for r in user_rows
#         }
#
#         for uid, balance in balances_by_user.items():
#             key = f"balances_okx:{uid}"
#             await redis_client.delete(key)
#             if balance is not None:
#                 await redis_client.set(key, balance)
#
#         async for key in redis_client.scan_iter("balances_okx:*"):
#             try:
#                 _, uid = key.split(":", 1)
#             except ValueError:
#                 continue
#             if uid not in balances_by_user:
#                 await redis_client.delete(key)
#                 await redis_client.delete(f"availables_okx:{uid}")
#
#         logger.info(f"updated balances for {len(balances_by_user)} users at {datetime.now(timezone('Asia/Seoul'))}")
#
#     except Exception:
#         logger.exception(f"failed to update balance status to redis")
#     finally:
#         if cursor:
#             try: cursor.close()
#             except: pass
#         if conn:
#             try: conn.close()
#             except: pass
#
# async def update_balance_status_per_user_okx(user_id, retri_id = None):
#     conn = None
#     cursor = None
#     try:
#         conn = mysql._get_connection()
#         cursor = conn.cursor()
#
#         if not user_id:
#             cursor.execute("""
#                 SELECT `id`
#                   FROM user_okx
#                  WHERE `retri_id` = %s
#                    AND status = 0
#                  LIMIT 1
#             """, (retri_id, ))
#             row = cursor.fetchone()
#             if not row:
#                 logger.exception(f"there is no user with a retri_id of {retri_id}")
#                 return
#             user_id = row['id']
#
#         cursor.execute("""
#             SELECT retri_id, balance
#               FROM user_okx
#              WHERE `id` = %s
#                AND status = 0
#         """, (user_id,))
#
#         user_row = cursor.fetchone()
#         if not user_row:
#             logger.exception(f"could not find user with id of {user_id}")
#             return
#
#         uid = user_row['retri_id']
#         balance = user_row['balance']
#
#         key = f"balances_okx:{uid}"
#         await redis_client.delete(key)
#         await redis_client.set(key, balance)
#
#         logger.info(f"completed updating balance status of user [{user_id}] to redis ({balance})")
#
#     except Exception:
#         logger.exception(f"failed to update balance of user [{user_id}]")
#     finally:
#         if cursor:
#             try: cursor.close()
#             except: pass
#         if conn:
#             try: conn.close()
#             except: pass
#
# async def update_liq_price_okx():
#     logger.info("Uploading cross position liquidation prices to MySQL")
#     conn = None
#     cursor = None
#     row_count = 0
#
#     try:
#         conn = mysql._get_connection()
#         cursor = conn.cursor()
#
#         # Scan every user’s positions hash
#         async for pos_key in redis_client.scan_iter("positions_okx:*"):
#             # pos_key is like "positions:447"
#             # fetch all field→JSON blobs
#             raw_hash = await redis_client.hgetall(pos_key)
#             if not raw_hash:
#                 continue
#
#             # For each symbol entry in that hash
#             for symbol, blob in raw_hash.items():
#                 # blob comes back as bytes; decode & parse
#                 data = json.loads(blob)
#
#                 # only update cross‐margin legs
#                 if data.get("margin_type") != "cross":
#                     continue
#
#                 pos_id    = data.get("pos_id")
#                 liq_price = data.get("liq_price")
#                 if pos_id is None or liq_price is None:
#                     continue
#
#                 # logger.info(f"Updating liquidation price for pos_id={pos_id} with liq_price={liq_price}")
#                 # Wrap each position update in a savepoint so one bad row won't kill the batch
#                 cursor.execute("SAVEPOINT liq_updt")
#                 try:
#                     cursor.execute(
#                         """
#                         UPDATE position_history_okx
#                            SET liq_price = %s
#                          WHERE id = %s
#                         """,
#                         (liq_price, pos_id)
#                     )
#                     row_count += 1
#                     cursor.execute("RELEASE SAVEPOINT liq_updt")
#
#                 except Exception as e:
#                     logger.warning(f"Failed updating pos_id={pos_id} : {e}")
#                     cursor.execute("ROLLBACK TO SAVEPOINT liq_updt")
#                     cursor.execute("RELEASE SAVEPOINT liq_updt")
#                     # continue to next leg
#                     continue
#
#         conn.commit()
#         logger.info(f"Updated {row_count} liquidation prices successfully to MySQL")
#
#     except Exception:
#         if conn:
#             conn.rollback()
#         logger.exception("Critical failure in update_liq_price")
#
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()
#
#
#
#


import json
import logging
from datetime import datetime

import redis.asyncio as aioredis
from pytz import timezone
from starlette.config import Config

from utils.connections import MySQLAdapter


config = Config(".env")
logger = logging.getLogger("uvicorn")

# API 서버에서 스케줄러 서버의 Redis를 바라봄
redis_client = aioredis.Redis(
    host=config.get("LOCAL_REDIS"), port=6379, db=0, decode_responses=True
)

# 가격 Redis
price_redis = aioredis.Redis(
    host=config.get("REDIS_HOST_OKX"),
    port=6379,
    db=0,
    decode_responses=True,
)

mysql = MySQLAdapter()


def resolve_liq_price(existing_blob, pos_id, db_liq_price):
    """
    Redis에 이미 계산된 청산가가 있고,
    Redis 포지션과 DB 포지션의 pos_id가 같으면 Redis 값을 우선 사용한다.

    동일 심볼 포지션을 종료한 뒤 재진입한 경우에는 pos_id가 달라지므로
    이전 포지션의 청산가를 새 포지션에 물려주지 않는다.
    """

    if existing_blob:
        try:
            existing_data = json.loads(existing_blob)

            existing_pos_id = existing_data.get("pos_id")
            existing_liq_price = existing_data.get("liq_price")

            if (
                    existing_pos_id is not None
                    and str(existing_pos_id) == str(pos_id)
                    and existing_liq_price is not None
            ):
                return float(existing_liq_price)

        except (TypeError, ValueError, json.JSONDecodeError):
            pass

    return float(db_liq_price or 0)


async def update_position_status_to_redis_okx():
    logger.info("Updating MySQL position status to Redis")

    conn = None
    cursor = None

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                ph.id AS pos_id,
                ph.status AS status,
                ph.user_id,
                ph.symbol,
                ph.size,
                ph.amount,
                ph.entry_price,
                ph.liq_price,
                ph.margin,
                ph.pnl,
                ph.margin_type,
                ph.side,
                ph.leverage,
                ph.tp,
                ph.sl,
                ph.close_price,
                ph.unrealized_pnl,
                ph.unrealized_pnl_pct,
                u.retri_id
            FROM position_history_okx AS ph
                     JOIN user_okx AS u
                          ON ph.user_id = u.id
            WHERE ph.status = 1
              AND ph.amount > 0
            """
        )

        rows = cursor.fetchall()
        positions_by_user = {}

        for row in rows:
            retri_id = row["retri_id"]
            symbol = row["symbol"]
            redis_key = f"positions_okx:{retri_id}"

            amount = float(row["amount"] or 0)
            entry_price = float(row["entry_price"] or 0)
            side = row["side"]
            leverage = int(row["leverage"]) if row["leverage"] else 1
            margin_type = row["margin_type"]

            price_key = f"price:{symbol}USDT"
            price_raw = await price_redis.get(price_key)

            try:
                current_price = float(price_raw) if price_raw else entry_price
            except (TypeError, ValueError):
                current_price = entry_price

            size = current_price * amount

            margin = float(row["margin"] or 0)

            if margin_type == "cross" and leverage:
                margin = size / leverage

            if side == "buy":
                pnl = (current_price - entry_price) * amount
            else:
                pnl = (entry_price - current_price) * amount

            initial_notional = entry_price * amount

            pnl_pct = (
                (pnl / initial_notional) * 100
                if initial_notional
                else 0.0
            )

            roi_pct = (
                (pnl / margin) * 100
                if margin
                else 0.0
            )

            # 계산된 cross 청산가가 Redis에 있으면 보존
            # 단, 같은 pos_id일 때만 재사용
            existing_blob = await redis_client.hget(redis_key, symbol)

            final_liq_price = resolve_liq_price(
                existing_blob=existing_blob,
                pos_id=row["pos_id"],
                db_liq_price=row["liq_price"],
            )

            positions_by_user.setdefault(retri_id, []).append(
                {
                    "pos_id": row["pos_id"],
                    "user_id": row["user_id"],
                    "symbol": symbol,
                    "entry_price": entry_price,
                    "liq_price": final_liq_price,
                    "market_price": current_price,
                    "amount": amount,
                    "side": side,
                    "margin": margin,
                    "margin_type": margin_type,
                    "size": size,
                    "leverage": leverage,
                    "tp": row["tp"],
                    "sl": row["sl"],
                    "unrealized_pnl": pnl,
                    "unrealized_pnl_pct": pnl_pct,
                    "roi_pct": roi_pct,
                }
            )

        # 사용자별 활성 포지션 Redis 갱신
        for retri_id, position_list in positions_by_user.items():
            redis_key = f"positions_okx:{retri_id}"

            await redis_client.delete(redis_key)

            mapping = {
                position["symbol"]: json.dumps(position)
                for position in position_list
            }

            if mapping:
                await redis_client.hset(redis_key, mapping=mapping)

        # 활성 포지션이 사라진 사용자 Redis 정리
        async for redis_key in redis_client.scan_iter("positions_okx:*"):
            try:
                _, retri_id = redis_key.split(":", 1)
            except ValueError:
                continue

            if retri_id not in positions_by_user:
                await redis_client.delete(redis_key)

        logger.info(
            "Updated positions for %s users at %s",
            len(positions_by_user),
            datetime.now(timezone("Asia/Seoul")),
        )

    except Exception:
        logger.exception("Failed to update position status to Redis")

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


async def update_position_status_per_user_okx(user_id, retri_id=None):
    conn = None
    cursor = None

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                ph.id AS pos_id,
                ph.user_id,
                ph.symbol,
                ph.size,
                ph.amount,
                ph.entry_price,
                ph.liq_price,
                ph.margin,
                ph.pnl,
                ph.margin_type,
                ph.side,
                ph.leverage,
                ph.status,
                ph.tp,
                ph.sl,
                ph.close_price,
                ph.unrealized_pnl,
                ph.unrealized_pnl_pct
            FROM position_history_okx AS ph
            WHERE ph.user_id = %s
              AND ph.status = 1
              AND ph.amount > 0
            """,
            (user_id,),
        )

        position_rows = cursor.fetchall()

        if not retri_id:
            cursor.execute(
                """
                SELECT retri_id
                FROM user_okx
                WHERE id = %s
                  AND status = 0
                LIMIT 1
                """,
                (user_id,),
            )

            user_row = cursor.fetchone()

            if not user_row:
                logger.warning("User %s not found", user_id)
                raise LookupError(
                    f"retri_id of user_id={user_id} not found"
                )

            retri_id = user_row["retri_id"]

        redis_key = f"positions_okx:{retri_id}"
        positions = []

        for row in position_rows:
            symbol = row["symbol"]

            entry_price = float(row["entry_price"] or 0)
            amount = float(row["amount"] or 0)
            side = row["side"]
            leverage = int(row["leverage"]) if row["leverage"] else 1
            margin_type = row["margin_type"]

            price_key = f"price:{symbol}USDT"
            price_raw = await price_redis.get(price_key)

            try:
                current_price = float(price_raw) if price_raw else entry_price
            except (TypeError, ValueError):
                current_price = entry_price

            size = current_price * amount

            margin = float(row["margin"] or 0)

            if margin_type == "cross" and leverage:
                margin = size / leverage

            if side == "buy":
                pnl = (current_price - entry_price) * amount
            else:
                pnl = (entry_price - current_price) * amount

            initial_notional = entry_price * amount

            pnl_pct = (
                (pnl / initial_notional) * 100
                if initial_notional
                else 0.0
            )

            roi_pct = (
                (pnl / margin) * 100
                if margin
                else 0.0
            )

            # 기존 Redis 값은 삭제 전에 먼저 조회
            # 동일한 pos_id의 포지션에 한해서 계산된 청산가를 보존
            existing_blob = await redis_client.hget(redis_key, symbol)

            final_liq_price = resolve_liq_price(
                existing_blob=existing_blob,
                pos_id=row["pos_id"],
                db_liq_price=row["liq_price"],
            )

            positions.append(
                {
                    "pos_id": row["pos_id"],
                    "user_id": row["user_id"],
                    "symbol": symbol,
                    "entry_price": entry_price,
                    "liq_price": final_liq_price,
                    "market_price": current_price,
                    "amount": amount,
                    "side": side,
                    "margin": margin,
                    "margin_type": margin_type,
                    "size": size,
                    "leverage": leverage,
                    "tp": row["tp"],
                    "sl": row["sl"],
                    "unrealized_pnl": pnl,
                    "unrealized_pnl_pct": pnl_pct,
                    "roi_pct": roi_pct,
                }
            )

        await redis_client.delete(redis_key)

        mapping = {
            position["symbol"]: json.dumps(position)
            for position in positions
        }

        if mapping:
            await redis_client.hset(redis_key, mapping=mapping)

        logger.info(
            "Completed updating position status of user [%s] to Redis",
            user_id,
        )

    except Exception:
        logger.exception(
            "Failed to update position status of user [%s] to Redis",
            user_id,
        )

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


async def update_order_status_to_redis_okx():
    logger.info("Updating MySQL order status to Redis")

    conn = None
    cursor = None

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                oh.id AS or_id,
                oh.status AS status,
                oh.magin AS margin,
                oh.user_id,
                oh.symbol,
                oh.type,
                oh.margin_type,
                oh.side,
                oh.price,
                oh.amount,
                oh.leverage,
                oh.order_price,
                oh.po_id,
                oh.tp,
                oh.sl,
                u.retri_id
            FROM order_history_okx AS oh
                     JOIN user_okx AS u
                          ON oh.user_id = u.id
            WHERE oh.status = 0
            """
        )

        rows = cursor.fetchall()
        orders_by_user = {}

        for row in rows:
            retri_id = row["retri_id"]

            orders_by_user.setdefault(retri_id, []).append(
                {
                    "or_id": row["or_id"],
                    "user_id": row["user_id"],
                    "symbol": row["symbol"],
                    "price": row["price"],
                    "type": row["type"],
                    "margin_type": row["margin_type"],
                    "margin": row["margin"],
                    "leverage": row["leverage"],
                    "side": row["side"],
                    "order_price": row["order_price"],
                    "amount": row["amount"],
                    "tp": row["tp"],
                    "sl": row["sl"],
                    "po_id": row["po_id"],
                }
            )

        for retri_id, order_list in orders_by_user.items():
            redis_key = f"orders_okx:{retri_id}"
            payload = json.dumps(order_list)

            await redis_client.set(redis_key, payload)

        async for redis_key in redis_client.scan_iter("orders_okx:*"):
            try:
                _, retri_id = redis_key.split(":", 1)
            except ValueError:
                continue

            if retri_id not in orders_by_user:
                await redis_client.delete(redis_key)

        logger.info(
            "Updated orders for %s users at %s",
            len(orders_by_user),
            datetime.now(timezone("Asia/Seoul")),
        )

    except Exception:
        logger.exception("Failed to update order status to Redis")

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


async def update_order_status_per_user_okx(user_id, retri_id=None):
    conn = None
    cursor = None

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                oh.id AS or_id,
                oh.status AS status,
                oh.magin AS margin,
                oh.user_id,
                oh.symbol,
                oh.type,
                oh.margin_type,
                oh.side,
                oh.price,
                oh.amount,
                oh.leverage,
                oh.order_price,
                oh.po_id,
                oh.tp,
                oh.sl
            FROM order_history_okx AS oh
            WHERE oh.user_id = %s
              AND oh.status = 0
            """,
            (user_id,),
        )

        order_rows = cursor.fetchall()

        if not retri_id:
            cursor.execute(
                """
                SELECT retri_id
                FROM user_okx
                WHERE id = %s
                  AND status = 0
                LIMIT 1
                """,
                (user_id,),
            )

            user_row = cursor.fetchone()

            if not user_row:
                logger.warning("User %s not found", user_id)
                raise LookupError(
                    f"retri_id of user_id={user_id} not found"
                )

            retri_id = user_row["retri_id"]

        orders = [
            {
                "or_id": row["or_id"],
                "user_id": row["user_id"],
                "symbol": row["symbol"],
                "price": row["price"],
                "type": row["type"],
                "margin_type": row["margin_type"],
                "margin": row["margin"],
                "side": row["side"],
                "leverage": row["leverage"],
                "order_price": row["order_price"],
                "amount": row["amount"],
                "tp": row["tp"],
                "sl": row["sl"],
                "po_id": row["po_id"],
            }
            for row in order_rows
        ]

        redis_key = f"orders_okx:{retri_id}"
        payload = json.dumps(orders)

        await redis_client.set(redis_key, payload)

        logger.info(
            "Completed updating order status of user [%s] to Redis",
            user_id,
        )

    except Exception:
        logger.exception(
            "Failed to update order status of user [%s] to Redis",
            user_id,
        )

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


async def update_balance_status_to_redis_okx():
    logger.info("Updating MySQL balance status to Redis")

    conn = None
    cursor = None

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                balance,
                retri_id
            FROM user_okx
            WHERE status = 0
            """
        )

        user_rows = cursor.fetchall()

        balances_by_user = {
            row["retri_id"]: float(row["balance"] or 0)
            for row in user_rows
        }

        # Redis SET은 기존 값을 원자적으로 덮어쓴다.
        # delete 후 set을 하면 그 사이에 balance key가 잠깐 사라질 수 있으므로
        # 절대 delete하지 않는다.
        for retri_id, balance in balances_by_user.items():
            redis_key = f"balances_okx:{retri_id}"
            await redis_client.set(redis_key, balance)

        # DB에 더 이상 존재하지 않는 유저의 stale key만 정리
        async for redis_key in redis_client.scan_iter("balances_okx:*"):
            try:
                _, retri_id = redis_key.split(":", 1)
            except ValueError:
                continue

            if retri_id not in balances_by_user:
                await redis_client.delete(redis_key)
                await redis_client.delete(f"availables_okx:{retri_id}")

        logger.info(
            "Updated balances for %s users at %s",
            len(balances_by_user),
            datetime.now(timezone("Asia/Seoul")),
        )

    except Exception:
        logger.exception("Failed to update balance status to Redis")

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()

async def update_balance_status_per_user_okx(user_id, retri_id=None):
    conn = None
    cursor = None

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        if not user_id:
            cursor.execute(
                """
                SELECT id
                FROM user_okx
                WHERE retri_id = %s
                  AND status = 0
                LIMIT 1
                """,
                (retri_id,),
            )

            user_row = cursor.fetchone()

            if not user_row:
                logger.warning(
                    "There is no user with retri_id [%s]",
                    retri_id,
                )
                return

            user_id = user_row["id"]

        cursor.execute(
            """
            SELECT
                retri_id,
                balance
            FROM user_okx
            WHERE id = %s
              AND status = 0
            """,
            (user_id,),
        )

        user_row = cursor.fetchone()

        if not user_row:
            logger.warning(
                "Could not find user with id [%s]",
                user_id,
            )
            return

        retri_id = user_row["retri_id"]
        balance = float(user_row["balance"] or 0)

        redis_key = f"balances_okx:{retri_id}"

        # delete 없이 바로 원자적으로 덮어쓰기
        await redis_client.set(redis_key, balance)

        logger.info(
            "Completed updating balance status of user [%s] to Redis (%s)",
            user_id,
            balance,
        )

    except Exception:
        logger.exception(
            "Failed to update balance status of user [%s] to Redis",
            user_id,
        )

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


async def update_liq_price_okx():
    """
    calculation.py가 positions:{retri_id} 해시 내부에 저장한
    cross 포지션 청산가를 MySQL position_history에 업로드한다.
    """

    logger.info("Uploading cross position liquidation prices to MySQL")

    conn = None
    cursor = None
    row_count = 0

    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        async for redis_key in redis_client.scan_iter("positions_okx:*"):
            raw_hash = await redis_client.hgetall(redis_key)

            if not raw_hash:
                continue

            for _, blob in raw_hash.items():
                try:
                    position_data = json.loads(blob)
                except (TypeError, json.JSONDecodeError):
                    logger.warning(
                        "Failed to parse position data from Redis key [%s]",
                        redis_key,
                    )
                    continue

                # cross 청산가만 주기적으로 업로드
                if position_data.get("margin_type") != "cross":
                    continue

                pos_id = position_data.get("pos_id")
                liq_price = position_data.get("liq_price")

                if pos_id is None or liq_price is None:
                    continue

                cursor.execute("SAVEPOINT liq_updt")

                try:
                    cursor.execute(
                        """
                        UPDATE position_history_okx
                        SET liq_price = %s
                        WHERE id = %s
                          AND status = 1
                        """,
                        (liq_price, pos_id),
                    )

                    row_count += cursor.rowcount

                    cursor.execute("RELEASE SAVEPOINT liq_updt")

                except Exception as exc:
                    logger.warning(
                        "Failed updating liquidation price for pos_id=%s: %s",
                        pos_id,
                        exc,
                    )

                    cursor.execute("ROLLBACK TO SAVEPOINT liq_updt")
                    cursor.execute("RELEASE SAVEPOINT liq_updt")

        conn.commit()

        logger.info(
            "Updated %s liquidation prices successfully to MySQL",
            row_count,
        )

    except Exception:
        if conn:
            conn.rollback()

        logger.exception(
            "Critical failure while uploading liquidation prices to MySQL"
        )

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()