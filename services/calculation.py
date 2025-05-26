import asyncio
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

from utils.connections import MySQLAdapter
import traceback
from datetime import datetime, timedelta
from pytz import timezone
from utils.price_cache import prices as price_cache
import logging
from utils.symbols import symbols as SYMBOL_CFG
from utils.connection_manager import manager
from utils.local_redis import update_position_status_per_user, update_order_status_per_user, update_balance_status_per_user

# position_redis = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
# price_redis    = aioredis.from_url("redis://" + config.get("REDIS_HOST") + ":6379/0", decode_responses=True)
MAINTENANCE_RATE = 0.01  # or pull from config

class CalculationService(MySQLAdapter):

    def __init__(self):
        super().__init__()
        self._position_redis = None
        self._price_redis = None

    async def get_position_redis(self):
        if self._position_redis is None:
            self._position_redis = await aioredis.from_url(
                "redis://localhost:6379/0", decode_responses=True
            )
        return self._position_redis

    async def get_price_redis(self):
        if self._price_redis is None:
            self._price_redis = await aioredis.from_url(
                "redis://localhost:6379/0", decode_responses=True
            )
        return self._price_redis

    async def close(self):
        if self._position_redis:
            await self._position_redis.close()
        if self._price_redis:
            await self._price_redis.close()

    async def calculate_pnl(self):

        position_redis = await self.get_position_redis()
        price_redis = await self.get_price_redis()

        async for key in position_redis.scan_iter("positions:*"):
            try:
                _, user_id = key.split(":")
            except ValueError:
                continue

            raw_pos = await position_redis.hgetall(key)
            if not raw_pos:
                continue

            updates = []
            pipe = position_redis.pipeline()
            price_keys = [f"price:{sym}USDT" for sym in raw_pos.keys()]
            raw_prices = await price_redis.mget(*price_keys)

            for sym, raw_price in zip(raw_pos.keys(), raw_prices):
                if raw_price is None:
                    continue
                try:
                    pos_data = json.loads(raw_pos[sym])
                except json.JSONDecodeError:
                    continue

                try:
                    entry_price = pos_data["entry_price"]
                    amount = pos_data["amount"]
                    side = pos_data["side"]
                    margin_type = pos_data["margin_type"]
                    leverage = pos_data["leverage"]
                except KeyError as e:
                    logger.warning(f"Missing key {e.args[0]} in position {sym} for user {user_id}")
                    continue

                current_price = float(raw_price)
                size = current_price * amount

                if margin_type == 'cross':
                    margin = size / leverage
                else:
                    margin = pos_data['margin']

                pnl = (
                    (current_price - entry_price) * amount
                    if side == 'buy'
                    else (entry_price - current_price) * amount
                )

                initial_notional = entry_price * amount
                pnl_pct = (pnl / initial_notional * 100) if initial_notional else 0.0
                roi_pct = (pnl / margin * 100) if margin else 0.0

                # Update in-memory
                pos_data["unrealized_pnl"] = pnl
                pos_data["unrealized_pnl_pct"] = pnl_pct
                pos_data["roi_pct"] = roi_pct
                pos_data["margin"] = margin
                pos_data["market_price"] = current_price
                pos_data["size"] = size

                pipe.hset(key, sym, json.dumps(pos_data))

            await pipe.execute()

    async def calculate_liq_prices(self):
        # logger.info("Starting global liquidation price calculation")

        position_redis = await self.get_position_redis()
        price_redis = await self.get_price_redis()

        async for pos_key in position_redis.scan_iter("positions:*"):
            try:
                _, user_id = pos_key.split(":")
                raw_pos = await position_redis.hgetall(pos_key)
                positions = [json.loads(p) for p in raw_pos.values()]
                if not positions:
                    continue

                # load balance and orders
                bal_key = f"balances:{user_id}"
                ord_key = f"orders:{user_id}"
                avl_key = f"availables:{user_id}"
                liq_key = f"liq_prices:{user_id}"

                balance = float(await position_redis.get(bal_key) or 0)
                raw_ord = await position_redis.get(ord_key) or "[]"
                orders = json.loads(raw_ord)

                iso_pos = sum(p["margin"] for p in positions if p["margin_type"] == "isolated")
                iso_ord = sum(o["margin"] for o in orders if o["margin_type"] == "isolated" and o["type"] in ("limit", "market"))
                cross_equity = balance - iso_pos - iso_ord

                total_pos = sum(p["margin"] for p in positions)
                total_ord = sum(o["margin"] for o in orders)
                total_upnl = sum(p.get("unrealized_pnl", 0.0) for p in positions if p["margin_type"] == "cross")
                available = balance - total_pos - total_ord + total_upnl
                await position_redis.set(avl_key, available)

                cross = [p for p in positions if p["margin_type"] == "cross"]

                other_maint = {
                    p["pos_id"]: sum(MAINTENANCE_RATE * q["size"] for q in cross if q["pos_id"] != p["pos_id"])
                    for p in cross
                }
                other_upnl = {
                    p["pos_id"]: sum(q.get("unrealized_pnl", 0.0) for q in cross if q["pos_id"] != p["pos_id"])
                    for p in cross
                }

                liq_prices = []
                breaches = []

                for p in cross:
                    pid, entry, amt, side = p["pos_id"], p["entry_price"], p["amount"], p["side"]
                    market_price = p.get("market_price")
                    if market_price is None:
                        raw = await price_redis.get(f"price:{p['symbol']}USDT")
                        market_price = float(raw) if raw else entry

                    my_maint = MAINTENANCE_RATE * market_price * amt
                    eq_me = cross_equity - other_maint[pid] + other_upnl[pid]
                    target_pnl = my_maint - eq_me

                    if side == "buy":
                        lp = max(entry + target_pnl / amt, 0.0)
                    else:
                        lp = max(entry - target_pnl / amt, 0.0)

                    # Fetch current price for breach detection
                    raw = await price_redis.get(f"price:{p['symbol']}USDT")
                    curr = float(raw) if raw else None

                    liq_prices.append({
                        "pos_id": pid,
                        "symbol": p["symbol"],
                        "liq_price": lp
                    })

                    if curr is not None:
                        if (side == "buy" and curr <= lp) or (side == "sell" and curr >= lp):
                            breaches.append({
                                "pos_id": pid,
                                "symbol": p["symbol"],
                                "liq_price": lp,
                                "current": curr,
                                "pnl": p.get("unrealized_pnl", 0.0)
                            })

                to_liquidate = min(breaches, key=lambda b: b["pnl"]) if breaches else None

                if to_liquidate:
                    pnl_liq = to_liquidate["pnl"]
                    new_balance = max(balance + pnl_liq, 0.0)

                    conn = None
                    cursor = None
                    try:
                        conn = self._get_connection()
                        conn.autocommit(False)
                        cursor = conn.cursor()

                        # 1. Update user's balance
                        cursor.execute("""
                            UPDATE mocktrade.user
                               SET balance = GREATEST(balance + %s, 0)
                             WHERE retri_id = %s
                        """, (pnl_liq, user_id))

                        # 2. Get user ID from retri_id
                        cursor.execute("""
                            SELECT `id` FROM mocktrade.user
                             WHERE retri_id = %s AND status = 0
                        """, (user_id,))
                        row = cursor.fetchone()
                        if not row:
                            logger.warning(f"User not found with retri_id={user_id}")
                            continue

                        uid = row['id']

                        # 3. Mark the latest active position as liquidated
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                               SET status = 3,
                                   pnl = %s,
                                   datetime = %s
                             WHERE symbol = %s
                               AND user_id = %s
                               AND status = 1
                             ORDER BY id DESC
                             LIMIT 1
                        """, (
                            pnl_liq,
                            datetime.now(timezone("Asia/Seoul")),
                            to_liquidate["symbol"],
                            uid
                        ))

                        conn.commit()

                        # 4. Delete liquidated Redis position
                        await position_redis.hdel(pos_key, to_liquidate["symbol"])
                        await position_redis.set(bal_key, new_balance)

                    except Exception:
                        conn.rollback()
                        logger.exception(f"Failed persisting liquidation for retri_id={user_id}")
                    finally:
                        if cursor:
                            cursor.close()
                        if conn:
                            conn.close()


            # Store all results in a single JSON blob
                await position_redis.set(liq_key, json.dumps({
                    "available": available,
                    "positions": liq_prices,
                    "to_liquidate": to_liquidate
                }))

            except Exception:
                logger.exception(f"Failed calculating liq prices for user")
                continue

    async def pnl_loop(self):
        while True:
            try:
                await self.calculate_pnl()
            except Exception:
                logger.exception("Failed during calculate_pnl")
            await asyncio.sleep(2.0)  # every 2 seconds

    async def liq_loop(self):
        while True:
            try:
                await self.calculate_liq_prices()
            except Exception:
                logger.exception("Failed during calculate_liq_prices")
            await asyncio.sleep(7.0)  # every 7 seconds


# async def get_redis_service() -> CalculationService:
#     return CalculationService(position_redis, price_redis)