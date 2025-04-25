# routers/pnl_ws.py
import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import redis.asyncio as aioredis

router = APIRouter()
logger = logging.getLogger("pnl_ws")

# Redis clients (reuse your configured URLs)
position_redis = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
price_redis    = aioredis.from_url("redis://172.31.11.200:6379/1", decode_responses=True)

@router.websocket("/{user_id}")
async def pnl_stream(websocket: WebSocket, user_id: int):
    await websocket.accept()
    logger.info(f"User {user_id} connected to PnL stream")
    try:
        while True:
            # 1) Read this user's positions
            key = f"positions:{user_id}"
            positions = await position_redis.hgetall(key)
            if not positions:
                await asyncio.sleep(1.0)
                continue

            symbols    = list(positions.keys())
            price_keys = [f"price:{sym}" for sym in symbols]
            raw_prices = await price_redis.mget(*price_keys)

            updates = []
            for sym, raw in zip(symbols, raw_prices):
                if raw is None:
                    continue
                current_price = float(raw)
                info = json.loads(positions[sym])
                entry_price = info["entry_price"]
                amount      = info["amount"]
                side        = info["side"]

                pnl = (
                    (current_price - entry_price) * amount
                    if side == "buy"
                    else (entry_price - current_price) * amount
                )

                updates.append({
                    "symbol": sym,
                    "current_price": current_price,
                    "pnl": pnl
                })

            if updates:
                await websocket.send_json({"data": updates})

            await asyncio.sleep(1.0)

    except WebSocketDisconnect:
        logger.info(f"User {user_id} disconnected")
    except Exception:
        logger.exception(f"Unexpected error in PnL stream for user {user_id}")
    finally:
        # any necessary cleanup here
        pass
