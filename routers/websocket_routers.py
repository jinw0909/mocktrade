# routers/pnl_ws.py
import asyncio
import json
import logging
import time

from starlette.config import Config
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from utils.connection_manager import manager
import redis.asyncio as aioredis

router = APIRouter()
logger = logging.getLogger("pnl_ws")
config = Config('.env')
# Redis clients (reuse your configured URLs)
position_redis = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
price_redis    = aioredis.from_url("redis://" + config.get("REDIS_HOST") + ":6379/0", decode_responses=True)

HEARTBEAT_INTERVAL = 30.0 # seconds
MAINTENANCE_RATE = 0.01

async def liq_sender(websocket: WebSocket, user_id):
    logger.info(f"created asyncio task liq_sender for user [{user_id}]")
    try:
        while True:
            await asyncio.sleep(5.0)
            # 1) Read user's positions, orders, and balance
            bal_key = f"balances:{user_id}"
            pos_key = f"positions:{user_id}"
            ord_key = f"orders:{user_id}"

            balance = float(await position_redis.get(bal_key) or 0)
            raw_pos = await position_redis.hgetall(pos_key)
            positions = [json.loads(v) for v in raw_pos.values()]
            raw_ord = await position_redis.get(ord_key) or "[]"
            orders = json.loads(raw_ord)

            # BUILD CROSS EQUITY
            iso_pos = sum(p['margin'] for p in positions if p["margin_type"] == "isolated")
            iso_ord = sum(o['margin'] for o in orders if o['margin_type'] == 'isolated' and o['type'] in ('limit', 'market'))
            cross_equity = balance - iso_pos - iso_ord

            # COMPUTE liq_price FOR EVERY CROSS POSITION
            cross = [p for p in positions if p['margin_type'] == 'cross']
            # precompute other-maint & other-upnl for each pos_id
            other_maint = {
                p['pos_id']: sum(
                    MAINTENANCE_RATE * (q['entry_price'] * q['amount'])
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

            liq_prices = []
            breaches = []

            for p in cross:
                pid, entry, amt, side = p['pos_id'], p['entry_price'], p['amount'], p['side']
                my_maint = MAINTENANCE_RATE * (entry * amt)
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

            to_liquidate = None
            if breaches:
                to_liquidate = min(breaches, key=lambda b: b['pnl'])

                await position_redis.hdel(pos_key, to_liquidate["symbol"])

                pnl_liq = to_liquidate['pnl']
                new_balance = max(balance + pnl_liq, 0.0)
                await position_redis.set(bal_key, new_balance)

                # ( optional ) persist to MySQL

            await websocket.send_json({
                "liquidation": "liquidation_prices",
                "positions": liq_prices,
                "to_liquidate": to_liquidate
            })

    except asyncio.CancelledError:
        logger.info(f"socket error: cannot send liquidation price of user {user_id}")
        return


@router.websocket("/{user_id}")
async def pnl_stream(websocket: WebSocket, user_id: str):
    await manager.connect(user_id, websocket)
    logger.info(f"User {user_id} connected to PnL stream")

    other_task = asyncio.create_task(liq_sender(websocket, user_id))

    last_heartbeat = time.monotonic()

    try:
        while True:
            # 1) Read this user's positions (might be empty)
            key       = f"positions:{user_id}"
            positions = await position_redis.hgetall(key)

            updates = []
            # only compute updates if there are positions
            if positions:
                symbols    = list(positions.keys())
                price_keys = [f"price:{sym}USDT" for sym in symbols]
                raw_prices = await price_redis.mget(*price_keys)

                pipe = position_redis.pipeline()

                for sym, raw in zip(symbols, raw_prices):
                    if raw is None:
                        continue
                    try:
                        info        = json.loads(positions[sym])
                        pos_id      = info["pos_id"]
                        entry_price = info["entry_price"]
                        amount      = info["amount"]
                        side        = info["side"]
                    except KeyError as e:
                        logger.warning(f"User {user_id} — missing key {e.args[0]} for symbol {sym}, skipping")
                        continue

                    current_price = float(raw)

                    pnl = (
                        (current_price - entry_price) * amount
                        if side == "buy"
                        else (entry_price - current_price) * amount
                    )

                    # Compute percentage: pnl / (entry_price * amount) * 100
                    initial_notional = entry_price * amount
                    if initial_notional:
                        pnl_pct = pnl / initial_notional * 100
                    else:
                        pnl_pct = 0.0

                    updates.append({
                        "pos_id": pos_id,
                        "symbol": sym,
                        "current_price": current_price,
                        "pnl": pnl,
                        "pnl_pct": pnl_pct
                    })

                    # update the stored JSON
                    info["unrealized_pnl"] = pnl
                    info["unrealized_pnl_pct"] = pnl_pct
                    pipe.hset(key, sym, json.dumps(info))

                # execute all the HSETs in one go
                await pipe.execute()

            # 2) Decide what to send
            payload = None
            if updates:
                total_pnl = sum(item["pnl"] for item in updates)
                payload   = {"data": updates, "total": total_pnl}
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
