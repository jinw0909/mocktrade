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

# @router.websocket("/{user_id}")
# async def pnl_stream(websocket: WebSocket, user_id: str):
#     # await websocket.accept()
#     await manager.connect(user_id, websocket)
#     logger.info(f"User {user_id} connected to PnL stream")
#
#     last_heartbeat = time.monotonic()
#
#     try:
#         while True:
#             # 1) Read this user's positions
#             key = f"positions:{user_id}"
#             positions = await position_redis.hgetall(key)
#
#             if positions:
#                 symbols    = list(positions.keys())
#                 price_keys = [f"price:{sym}USDT" for sym in symbols]
#                 raw_prices = await price_redis.mget(*price_keys)
#
#                 updates = []
#                 for sym, raw in zip(symbols, raw_prices):
#                     if raw is None:
#                         continue
#                     try:
#                         info        = json.loads(positions[sym])
#                         pos_id      = info["pos_id"]
#                         entry_price = info["entry_price"]
#                         amount      = info["amount"]
#                         side        = info["side"]
#                     except KeyError as e:
#                         logger.warning(f"User {user_id} — missing key {e.args[0]} for symbol {sym}, skipping")
#                         continue
#
#                     current_price = float(raw)
#
#                     pnl = (
#                         (current_price - entry_price) * amount
#                         if side == "buy"
#                         else (entry_price - current_price) * amount
#                     )
#
#                     # Compute percentage: pnl / (entry_price * amount) * 100
#                     initial_notional = entry_price * amount
#                     if initial_notional:
#                         pnl_pct = pnl / initial_notional * 100
#                     else:
#                         pnl_pct = 0.0
#
#                     updates.append({
#                         "pos_id": pos_id,
#                         "symbol": sym,
#                         "current_price": current_price,
#                         "pnl": pnl,
#                         "pnl_pct": pnl_pct
#                     })
#
#                 if updates:
#                     total_pnl = sum(item["pnl"] for item in updates)
#                     try:
#                         await websocket.send_json({
#                             "data": updates,
#                             "total": total_pnl
#                         })
#
#                     except (WebSocketDisconnect, RuntimeError) as e:
#                         # client hang up
#                         logger.info(f"Socket closed for user {user_id} : {e!r}, stopping PnL loop")
#                         break
#                 else:
#                     now = time.monotonic()
#                     # Only send a heartbeat if enough time has passed
#                     if now - last_heartbeat >= HEARTBEAT_INTERVAL:
#                         await websocket.send_json({"type": "heartbeat"})
#                         last_heartbeat = now
#
#             await asyncio.sleep(1.0)
#
#     except WebSocketDisconnect:
#         logger.info(f"User {user_id} disconnected")
#     except Exception:
#         logger.exception(f"Unexpected error in PnL stream for user {user_id}")
#     finally:
#         # any necessary cleanup here
#         manager.disconnect(user_id, websocket)


@router.websocket("/{user_id}")
async def pnl_stream(websocket: WebSocket, user_id: str):
    await manager.connect(user_id, websocket)
    logger.info(f"User {user_id} connected to PnL stream")

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
