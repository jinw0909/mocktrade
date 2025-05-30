# routers/pnl_ws.py
import asyncio
import json
import logging
import time

from datetime import datetime, timedelta
from pytz import timezone
from starlette.config import Config
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from utils.connection_manager import manager
import redis.asyncio as aioredis
from utils.connections import MySQLAdapter

router = APIRouter()
logger = logging.getLogger("pnl_ws")
config = Config('.env')
# Redis clients (reuse your configured URLs)
position_redis = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
price_redis    = aioredis.from_url("redis://" + config.get("REDIS_HOST") + ":6379/0", decode_responses=True)

HEARTBEAT_INTERVAL = 30.0 # seconds
MAINTENANCE_RATE = 0.01

mysql = MySQLAdapter()

async def liq_sender(websocket: WebSocket, user_id):
    logger.info(f"created asyncio task liq_sender for user [{user_id}]")
    try:
        while True:
            try:
                await asyncio.sleep(5.0)
                # 1) Read user's positions, orders, and balance
                bal_key = f"balances:{user_id}"
                pos_key = f"positions:{user_id}"
                ord_key = f"orders:{user_id}"
                avl_key = f"availables:{user_id}"

                balance = float(await position_redis.get(bal_key) or 0)
                raw_pos = await position_redis.hgetall(pos_key)
                positions = [json.loads(v) for v in raw_pos.values()]
                raw_ord = await position_redis.get(ord_key) or "[]"
                orders = json.loads(raw_ord)

                # BUILD CROSS EQUITY
                iso_pos = sum(p['margin'] for p in positions if p["margin_type"] == "isolated")
                iso_ord = sum(o['margin'] for o in orders if o['margin_type'] == 'isolated' and o['type'] in ('limit', 'market'))
                cross_equity = balance - iso_pos - iso_ord

                total_pos = sum(p['margin'] for p in positions)
                total_ord = sum(o['margin'] for o in orders)


                # COMPUTE liq_price FOR EVERY CROSS POSITION
                cross = [p for p in positions if p['margin_type'] == 'cross']
                # precompute other-maint & other-upnl for each pos_id
                other_maint = {
                    p['pos_id']: sum(
                        MAINTENANCE_RATE * (q['size'])
                        for q in cross if q["pos_id"] != p["pos_id"]
                    )
                    for p in cross
                }
                other_upnl = {
                    p['pos_id']: sum(
                        q.get('unrealized_pnl', 0.0)
                        for q in cross if q['pos_id'] != p['pos_id']
                    )
                    for p in cross
                }

                total_upnl = sum(
                    p.get('unrealized_pnl', 0.0)
                    for p in cross
                )

                liq_prices = []
                breaches = []

                for p in cross:
                    pid, entry, amt, side, = p['pos_id'], p['entry_price'], p['amount'], p['side']
                    market_price = p.get('market_price')
                    if market_price is None:
                        raw = await price_redis.get(f"price:{p['symbol']}USDT")
                        market_price = float(raw) if raw else entry
                    my_maint = MAINTENANCE_RATE * (market_price * amt)
                    eq_me = cross_equity - other_maint[pid] + other_upnl[pid]
                    target_pnl = my_maint - eq_me

                    # solve pnl = target_pnl
                    if side == 'buy':
                        lp = max((entry + (target_pnl / amt)), 0.0)
                    else:
                        lp = max((entry - (target_pnl / amt)), 0.0)

                    # fetch current price
                    raw = await price_redis.get(f"price:{p['symbol']}USDT")
                    curr = float(raw) if raw else None

                    liq_prices.append({"pos_id": pid, "symbol": p["symbol"], "liq_price": lp})

                    # check if already past liq
                    if curr is not None:
                        if (side == 'buy' and curr <= lp) or (side == 'sell' and curr >= lp):
                            breaches.append({"pos_id": pid, "symbol": p["symbol"], "liq_price": lp, "current": curr, "pnl": p["unrealized_pnl"]})

                available = balance - total_pos - total_ord + total_upnl
                await position_redis.set(avl_key, available)

                to_liquidate = None
                if breaches:  # position to liquidate exists
                    to_liquidate = min(breaches, key=lambda b: b['pnl'])

                    pnl_liq = to_liquidate['pnl']
                    new_balance = max(balance + pnl_liq, 0.0)

                    # persist to MySQL
                    conn = None
                    cursor = None
                    try:
                        conn = mysql._get_connection()
                        conn.autocommit(False)
                        cursor = conn.cursor()

                        cursor.execute("""
                            UPDATE mocktrade.user
                               SET balance = GREATEST(balance + %s, 0)
                             WHERE retri_id = %s
                        """, (pnl_liq, user_id))

                        cursor.execute("""
                            SELECT `id` FROM `mocktrade`.`user`
                            WHERE `retri_id` = %s
                            AND `status` = 0 
                        """, (user_id, ))
                        row = cursor.fetchone()
                        if not row:
                            logger.warning(f"User not found with retri_id={user_id}")
                            return
                        uid = row['id']

                        cursor.execute("""
                            UPDATE `mocktrade`.`position_history`
                               SET `status` = 3,
                                   `pnl` = %s,
                                   `datetime` = %s 
                             WHERE `symbol` = %s
                               AND `user_id` = %s
                               AND `status` = 1
                             ORDER BY `id` DESC
                             LIMIT 1
                        """, (pnl_liq, datetime.now(timezone('Asia/Seoul')), to_liquidate['symbol'], uid))

                        conn.commit()
                    except Exception:
                        conn.rollback()
                        logger.exception(f"failed to persist the liquidation info of user [{user_id}] to MySQL")
                        return
                    finally:
                        cursor and cursor.close()
                        conn and conn.close()

                    await position_redis.hdel(pos_key, to_liquidate["symbol"])  # erase the liquidated position from redis
                    await position_redis.set(bal_key, new_balance)

                if liq_prices:  # cross position exist
                    try:
                        await websocket.send_json({
                            "liquidation": "liquidation_prices",
                            "available": available,
                            "positions": liq_prices,
                            "to_liquidate": to_liquidate
                        })

                    except (WebSocketDisconnect, RuntimeError):
                        logger.info(f"failed to send cross liquidation price of user {user_id}")
                        break
            except WebSocketDisconnect:
                logger.info(f"WebSocket closed for user {user_id}")
                break
            except Exception as e:
                logger.exception(f"Unexpected error in liq_sender loop for user [{user_id}]")
                await asyncio.sleep(1.0)

    except asyncio.CancelledError:
        logger.info(f"socket error: cannot send liquidation price of user {user_id}")
        return


@router.websocket("/{user_id}")
async def pnl_stream(websocket: WebSocket, user_id: str):
    await manager.connect(user_id, websocket)
    logger.info(f"User {user_id} connected to PnL stream")

    # other_task = asyncio.create_task(liq_sender(websocket, user_id))

    last_heartbeat = time.monotonic()

    try:
        while True:
            # 1) Read this user's positions (might be empty)
            key       = f"positions:{user_id}"
            positions = await position_redis.hgetall(key)
            liq_key = f"liq_prices:{user_id}"

            updates = []
            liq_list = []
            available = 0.0

            # only compute updates if there are positions
            if positions:
                symbols    = list(positions.keys())
                for symbol in symbols:
                    try:
                        info        = json.loads(positions[symbol])

                        payload_data = {
                            "pos_id": info["pos_id"],
                            "symbol": symbol,
                            "current_price": info.get("market_price"),
                            "pnl": info.get("unrealized_pnl"),
                            "pnl_pct": info.get("unrealized_pnl_pct"),
                            "roi_pct": info.get("roi_pct"),
                        }

                        updates.append(payload_data)

                    except KeyError as e:
                        logger.warning(f"User {user_id} — missing key {e.args[0]} for symbol {symbol}, skipping")
                        continue

                liq_raw = await position_redis.get(liq_key)
                if liq_raw:
                    try:
                        liq_info = json.loads(liq_raw)
                        available = liq_info.get("available", 0.0)
                        positions_liq = liq_info.get("positions", [])
                        for pos in positions_liq:
                            liq_list.append({
                                "pos_id": pos["pos_id"],
                                "symbol": pos["symbol"],
                                "liq_price": pos["liq_price"]
                            })
                    except Exception as e:
                        logger.warning(f"Failed to parse liquidation info for user {user_id}: {e!r}")


            # 2) Decide what to send
            payload = None
            if updates:
                total_pnl = sum(item.get("pnl") or 0.0 for item in updates)
                payload   = {
                    "data": updates,
                    "liq": liq_list,
                    "total": total_pnl,
                    "avbl": available
                }
            else:
                now = time.monotonic()
                if now - last_heartbeat >= HEARTBEAT_INTERVAL:
                    payload         = {"type": "heartbeat"}
                    last_heartbeat  = now

            # 3) Send if there’s something to send
            if payload:
                try:
                    await websocket.send_json(payload)
                except (WebSocketDisconnect, RuntimeError):
                    logger.info(f"Socket closed for user {user_id}, stopping loop")
                    break

            # 4) Wait before the next tick
            await asyncio.sleep(1.0)

    except WebSocketDisconnect:
        logger.info(f"User {user_id} disconnected cleanly")
    finally:
        manager.disconnect(user_id, websocket)
