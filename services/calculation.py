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
from utils.connection_manager import manager
import traceback
from datetime import datetime, timedelta
from pytz import timezone
import logging
from utils.symbols import symbols as SYMBOL_CFG
from utils.connection_manager import manager
from utils.local_redis import update_position_status_per_user, update_order_status_per_user, update_balance_status_per_user

# position_redis = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
# price_redis    = aioredis.from_url("redis://" + config.get("REDIS_HOST") + ":6379/0", decode_responses=True)
MAINTENANCE_RATE = 0.01  # or pull from config
FEE_RATE = 0.0002

def calc_iso_liq_price_from_margin(
        entry_price: float,
        margin: float,
        size: float,
        side: str
) -> float:
    """
    :param entry_price: average entry
    :param margin: initial margin allocated to the position
    :param size: notional (amount * entry_price)
    :param side: 'buy' or 'sell'
    """

    if size == 0:
        return 0.0

    ratio = margin / size  # = 1/effective_leverage

    if side == 'buy':
        # entry_price * ( 1 - 1/leverage)
        return entry_price * (1 - ratio)
    else:
        # entry_price * ( 1 + 1/leverage)
        return entry_price * (1 + ratio)

def calculate_position(current_position, order):
    """
    Calculates the resulting position after applying an order (market or filled limit).
    If position flips, calls mysql.position_flip and returns a fresh position.
    """

    user_id = order['user_id']
    symbol = order['symbol']
    side = order['side']  # buy or sell
    amount = float(order['amount'])
    price = float(order['price'])
    leverage = max(order.get('leverage', 1), current_position.get('leverage', 1))
    margin_type = order['margin_type']
    tp = order.get('tp')
    sl = order.get('sl')

    #helper to round by symbol
    prec = SYMBOL_CFG.get(symbol, {"price": 2, "qty": 3})
    PRICE_DP = prec["price"]
    QTY_DP = prec["qty"]

    order_value = price * amount
    order_margin = order_value / leverage

    # case 1. No current position -> create new
    if not current_position:
        logger.info("case 1, no current position")
        liq_price = calc_iso_liq_price_from_margin(
            price,  # entry_price
            order_margin,  # margin
            order_value,  # size
            side
        )

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": round(amount, QTY_DP),
            "entry_price": round(price, PRICE_DP),
            "size": order_value,
            "margin": order_margin,
            "leverage": leverage,
            "side": side,
            "pnl": 0,
            "margin_type": margin_type,
            "status": 1,  # open
            "liq_price": liq_price
        }

    # Existing position details
    current_side = current_position['side']
    current_amount = float(current_position['amount'])
    current_entry_price = float(current_position['entry_price'])
    current_margin = float(current_position['margin'])
    # current_size = float(current_position['size'])
    # current_pnl = float(current_position.get('pnl') or 0)
    current_tp = current_position.get('tp')
    current_sl = current_position.get('sl')

    # case 2: Same-side -> merge positions
    if current_side == side:
        logger.info("case 2: same side")

        total_amount = current_amount + amount
        total_value = (current_entry_price * current_amount) + (price * amount)
        # 1) guard avg_entry_price
        if total_amount > 0:
            avg_entry_price = total_value / total_amount
        else:
            # fallback to whatever makes sense — e.g. the new order price
            avg_entry_price = price
        # avg_entry_price = total_value / total_amount
        total_size = total_amount * avg_entry_price
        # total_margin = current_margin + order_margin
        leverage = max(leverage, current_position.get('leverage', 1))
        total_margin = (total_size / leverage) if leverage > 0 else current_margin + order_margin
        logger.info(f"current_margin: {current_margin}, order_margin: {order_margin}, total_margin: {total_margin}")
        # 2) guard effective_leverage
        # if total_margin > 0:
        #     effective_leverage = total_size / total_margin
        # else:
        #     # if somehow margin is zero, fall back to your default leverage
        #     effective_leverage = leverage



        # effective_leverage = total_size / total_margin if total_margin else leverage

        liq_price = calc_iso_liq_price_from_margin(
            avg_entry_price,  # entry_price
            total_margin,  # margin
            total_size,  # size
            side
        )

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": round(total_amount, QTY_DP),
            "entry_price": round(avg_entry_price, PRICE_DP),
            "size": total_size,
            # "pnl": current_pnl,
            "pnl": 0,
            "margin": total_margin,
            "leverage": leverage,
            "side": side,
            "margin_type": margin_type,
            "status": 1,
            "liq_price": liq_price,
            "tp": current_tp,
            "sl": current_sl,
        }

    # 🔁 Case 3: Opposite-side → partial close, full close, or flip
    if amount < current_amount:
        logger.info("case 3-1, opposite side, partial close")
        # Partial close — reduce position
        new_amount = current_amount - amount
        close_pnl = (price - current_entry_price) * amount if current_side == 'buy' else (
                                                                                                 current_entry_price - price) * amount
        fee = abs(close_pnl) * FEE_RATE
        net_close = close_pnl - fee
        # new_pnl = current_pnl + close_pnl
        new_pnl = close_pnl
        leverage = current_position.get('leverage', 0)
        new_size = new_amount * current_entry_price
        new_margin = (new_size / leverage) if leverage > 0 else new_size / current_margin * (new_amount / current_amount)

        # effective_leverage = current_size / current_margin if current_margin else leverage
        leverage = max(order.get('leverage', 0), current_position.get('leverage', 0))

        liq_price = calc_iso_liq_price_from_margin(
            current_entry_price,
            new_margin,
            new_size,
            current_side
        )

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": round(new_amount, QTY_DP),
            "entry_price": current_entry_price,
            "size": new_size,
            "margin": new_margin,
            "leverage": leverage,
            "side": current_side,
            "margin_type": margin_type,
            "pnl": 0,
            "close_pnl": round(net_close, PRICE_DP),
            "status": 1,
            "liq_price": round(liq_price, PRICE_DP),
            "tp": current_tp,
            "sl": current_sl,
            "close_price": round(price, PRICE_DP),
            "partial": True
        }

    elif amount == current_amount:
        logger.info("case 3-2, opposite side, full close")
        # Full close — no new position
        close_pnl = (price - current_entry_price) * amount if current_side == 'buy' else (
                                                                                                 current_entry_price - price) * amount
        # new_pnl = current_pnl + close_pnl
        fee = abs(close_pnl) * FEE_RATE
        new_pnl = close_pnl
        net_close = close_pnl - fee

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": 0,
            "entry_price": None,
            "size": 0,
            "margin": 0,
            "leverage": 0,
            "side": current_side,
            "margin_type": margin_type,
            "pnl": 0,
            "close_pnl": round(net_close, PRICE_DP),
            "status": 3,  # fully closed
            "liq_price": None,
            "tp": None,
            "sl": None,
            "close": True,
            "close_price": round(price, PRICE_DP)
        }

    else:
        # Flip — close current, open new opposite
        logger.info("case 3-3, opposite side, flip")
        flip_amount = amount - current_amount
        close_pnl = (price - current_entry_price) * current_amount if current_side == 'buy' else (
                                                                                                         current_entry_price - price) * current_amount
        # new_pnl = close_pnl + current_pnl
        fee = abs(close_pnl) * FEE_RATE
        # new_pnl = close_pnl
        net_close = close_pnl - fee
        new_value = price * flip_amount
        new_margin = new_value / leverage


        liq_price = calc_iso_liq_price_from_margin(
            price,
            new_margin,
            new_value,
            side
        )

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": round(flip_amount, QTY_DP),
            "entry_price": round(price, PRICE_DP),
            "size": new_value,
            "margin": new_margin,
            "leverage": leverage,
            "side": side,  # now flipped
            "margin_type": margin_type,
            "pnl": 0,
            "close_pnl": round(net_close, PRICE_DP),
            "status": 1,
            "opposite": True,
            "liq_price": round(liq_price, PRICE_DP),
            "tp": tp,
            "sl": sl,
            "flip": True,
            "close_price": price
        }


def calculate_new_position(current_position, order):
    user_id = order['user_id']
    symbol = order['symbol']
    # side = order['side']  # side of the TP/SL order: 'buy' meaning closing a short
    amount = float(order['amount'])
    price = float(order['price'])  # this is the exit_price
    leverage = int(max(order.get('leverage', 1), current_position.get('leverage', 1)))
    margin_type = order['margin_type']
    from_order = True if order['order_price'] != 0 else False
    logger.info(f"from order : {from_order} (tpsl order id [{order['or_id']}]")

    # formatting precision
    prec = SYMBOL_CFG.get(symbol, {"price": 2, "qty": 3})
    PRICE_DP = prec["price"]
    QTY_DP = prec["qty"]

    # 1) No existing position ⇒ already closed
    if not current_position:
        return {"status": "closed"}

    # unpack current
    cs = current_position['side']  # 'buy' or 'sell'
    cur_amt = float(current_position['amount'])
    cur_price = float(current_position['entry_price'])
    cur_margin = float(current_position['margin'])
    # cur_size = float(current_position['size'])
    # cur_lev = float(current_position['leverage'])

    # decide how much to close
    close_amt = cur_amt if not from_order else min(amount, cur_amt)
    logger.info(f"close_amt: {close_amt}")
    # compute PnL for the closed portion
    if cs == 'buy':
        raw_pnl = (price - cur_price) * close_amt
    else:  # short
        raw_pnl = (cur_price - price) * close_amt

    fee = abs(raw_pnl) * FEE_RATE
    net_pnl = raw_pnl - fee

    # full-close
    if close_amt >= cur_amt:
        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": 0,
            "entry_price": None,
            "size": 0,
            "margin": 0,
            "leverage": 1,
            "side": cs,
            "margin_type": margin_type,
            "pnl": round(net_pnl, PRICE_DP),
            "close_pnl": round(net_pnl, PRICE_DP),
            "status": 3,
            "liq_price": None,
            "close_price": price,
            "close": True
        }

    # partial-close
    new_amt = cur_amt - close_amt
    new_size = new_amt * cur_price
    new_margin = cur_margin * (new_amt / cur_amt)

    # recalc liquidation for remaining
    new_liq = calc_iso_liq_price_from_margin(
        cur_price,
        new_margin,
        new_size,
        cs
    )

    new_liq = 0 if margin_type == 'cross' else new_liq

    return {
        "user_id": user_id,
        "symbol": symbol,
        "amount": round(new_amt, QTY_DP),
        "entry_price": round(cur_price, PRICE_DP),
        "size": round(new_size, PRICE_DP),
        "margin": round(new_margin, PRICE_DP),
        "leverage": leverage,
        "side": cs,
        "margin_type": margin_type,
        "pnl": 0,  # unrealized remains zero until closed
        "close_pnl": round(net_pnl, PRICE_DP),
        "status": 1,
        "liq_price": round(new_liq, PRICE_DP),
        "close_price": price,
        "partial": True
    }


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
                f"redis://{config.get('REDIS_HOST')}:6379/0", decode_responses=True
            )
        return self._price_redis

    async def close(self):
        logger.info("closing redis connection")
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

                        # 5. send socket message to inform liquidation
                        await asyncio.create_task(
                            manager.notify_user(
                                user_id,
                                { "trigger" : "liquidation_cross", "pos": to_liquidate }
                            )
                        )

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

    async def settle_orders(self):
        position_redis = await self.get_position_redis()
        price_redis = await self.get_price_redis()
        await self.settle_iso_liquidation(position_redis)
        await self.settle_limit_orders(position_redis, price_redis)
        await self.settle_tpsl_orders(position_redis, price_redis)

    async def settle_limit_orders(self, position_redis, price_redis):
        #logger.info("settle limit orders")
        row_count = 0
        pending_notifs = []
        updated_users = {}

        # Iterate all user keys in Redis with open orders
        async for key in position_redis.scan_iter("orders:*"):
            _, user_id = key.split(":")
            raw_orders = await position_redis.get(key)
            if not raw_orders:
                continue

            try:
                orders = json.loads(raw_orders)
            except Exception:
                logger.warning(f"Malformed orders for user {user_id}")
                continue

            # Filter for only active limit orders
            limit_orders = [o for o in orders if o['type'] == 'limit' and o['amount'] > 0]
            if not limit_orders:
                continue
            # logger.info(f"limit orders of user {user_id} : {limit_orders}")
            balance_key = f"balances:{user_id}"
            pos_key = f"positions:{user_id}"
            try:
                # wallet_balance = float(await position_redis.get(balance_key) or 0)
                raw_pos = await position_redis.hgetall(pos_key)
                positions = { sym: json.loads(p) for sym, p in raw_pos.items()}
            except Exception:
                logger.warning(f"Failed to load data for user {user_id}")
                continue

            for order in limit_orders:
                symbol = order['symbol']
                order_price = order['price']
                current_price = float(await price_redis.get(f"price:{symbol}USDT"))
                side = order['side']
                uid = order['user_id']

                if current_price is None:
                    continue
                if (side == 'buy' and order_price < current_price) or (side == 'sell' and order_price > current_price):
                    continue  # not filled yet


                # Execute the order
                logger.info(f"executing order [{order.get('or_id')}]")
                exec_price = current_price

                # Update order in memory
                order['price'] = exec_price

                # Use existing position or None
                current_position = positions.get(symbol, {})
                # Call your calculation logic
                new_position = calculate_position(current_position, order)

                # Persist to MySQL
                current_id = current_position.get('pos_id', 0)
                await self.persist_executed_limit_order(order, new_position, current_id)

                # Prepare WebSocket trigger
                # retri_id = order.get('retri_id') or user_id  # or from user db if needed
                pending_notifs.append((user_id, {"trigger": "limit", "order": order}))
                updated_users[uid] = user_id

                row_count += 1

        # Notify users
        for retri_id, message in pending_notifs:
            await asyncio.create_task(manager.notify_user(retri_id, message))
        for user_id, retri_id in updated_users.items():
            await update_position_status_per_user(user_id, retri_id)
            await update_order_status_per_user(user_id, retri_id)
            await update_balance_status_per_user(user_id, retri_id)

        return row_count

    async def settle_tpsl_orders(self, position_redis, price_redis):
        #logger.info("settle tpsl orders")
        row_count = 0
        pending_notifs = []
        updated_users = {}

        async for key in position_redis.scan_iter("orders:*"):
            _, user_id = key.split(":")
            raw_orders = await position_redis.get(key)
            if not raw_orders:
                continue

            orders = json.loads(raw_orders)
            tpsl_orders = [o for o in orders if o['type'] in ('tp', 'sl')]
            if not tpsl_orders:
                continue

            balance_key = f"balances:{user_id}"
            pos_key = f"positions:{user_id}"

            balance_raw = await position_redis.get(balance_key)
            raw_pos = await position_redis.hgetall(pos_key)

            try:
                # wallet_balance = float(balance_raw or 0)
                positions = {sym: json.loads(p) for sym, p in raw_pos.items()}
            except Exception:
                logger.warning(f"Malformed data for user {user_id}")
                continue

            for order in tpsl_orders:
                symbol = order['symbol']
                order_type = order['type']
                side = order['side']
                exit_price = float(order.get(order_type, 0))  # order['tp'] or order['sl']
                current_price_raw = await price_redis.get(f"price:{symbol}USDT")
                if current_price_raw is None:
                    continue

                try:
                    current_price = float(current_price_raw)
                except ValueError:
                    continue

                # check if it should trigger
                should_settle = (
                    (order_type == 'tp' and ((side == 'sell' and current_price >= exit_price) or (side == 'buy' and current_price <= exit_price))) or
                    (order_type == 'sl' and ((side == 'sell' and current_price <= exit_price) or (side == 'buy' and current_price >= exit_price)))
                )
                if not should_settle:
                    continue

                exec_price = max(current_price, exit_price) if side == 'sell' else min(current_price, exit_price)
                order['price'] = exec_price
                logger.info(f"exit_price: {order['price']}")

                current_position = positions.get(symbol)
                if not current_position:
                    continue

                # Compute new position and persist
                new_position = calculate_new_position(current_position, order)

                current_id = current_position.get("pos_id")
                await self.persist_triggered_tpsl_order(order, new_position, current_id)

                uid = order.get('user_id')
                pending_notifs.append((user_id, {"trigger": "tp/sl", "order": order}))
                updated_users[uid] = user_id

                row_count += 1

        for retri_id, message in pending_notifs:
            await asyncio.create_task(manager.notify_user(retri_id, message))
        for user_id, retri_id in updated_users.items():
            await update_position_status_per_user(user_id, retri_id)
            await update_order_status_per_user(user_id, retri_id)
            await update_balance_status_per_user(user_id, retri_id)

    async def persist_executed_limit_order(self, order, new_position, current_id):
        logger.info(f"persisting limit order [{order.get('or_id')}]")
        conn = None
        cursor = None

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            user_id = order.get('user_id')
            symbol = order.get('symbol')
            order_id = order.get('or_id')

            cursor.execute("""
                UPDATE mocktrade.position_history SET `status` = 2
                WHERE `status` = 1 AND `user_id` = %s AND `symbol` = %s
            """, (user_id, symbol))

            if new_position.get('close'):  # full close
                cursor.execute("""
                    UPDATE `mocktrade`.`position_history`
                       SET `status` = 3,
                           `pnl` = %s,
                           `datetime` = %s,
                           `close_price` = %s
                     WHERE `id` = %s
                """, (
                    new_position.get('close_pnl', 0),
                    datetime.now(timezone('Asia/Seoul')),
                    new_position.get('close_price', 0),
                    current_id
                ))

                # close existing tp/sl orders
                cursor.execute("""
                    UPDATE mocktrade.order_history
                       SET status = 4
                     WHERE symbol = %s 
                       AND user_id = %s
                       AND status = 0
                """, (symbol, user_id))

            elif new_position.get('flip'):
                cursor.execute("""
                    UPDATE `mocktrade`.`position_history`
                       SET `status` = 3,
                           `pnl` = %s,
                           `datetime` = %s,
                           `close_price` = %s
                    WHERE `id` = %s
                """, (
                    new_position.get('close_pnl', 0),
                    datetime.now(timezone('Asia/Seoul')),
                    new_position.get('close_price', 0),
                    current_id
                ))
                cursor.execute("""
                    INSERT INTO `mocktrade`.`position_history` (
                        user_id, symbol, size, amount, 
                        entry_price, liq_price, margin_ratio, margin, 
                        pnl, margin_type, side, leverage, status, 
                        tp, sl, datetime, close_price
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    new_position.get('user_id'), new_position.get('symbol'),
                    new_position.get('size'), new_position.get('amount'),
                    new_position.get('entry_price'), new_position.get('liq_price'),
                    new_position.get('margin_ratio'), new_position.get('margin'),
                    new_position.get('pnl', 0), new_position.get('margin_type'),
                    new_position.get('side'), new_position.get('leverage'), new_position.get('status'),
                    new_position.get('tp', 0), new_position.get('sl', 0), datetime.now(timezone("Asia/Seoul")),
                    new_position.get('close_price')
                ))

                # close existing tp/sl orders
                cursor.execute("""
                    UPDATE mocktrade.order_history
                       SET status = 4
                     WHERE symbol = %s 
                       AND user_id = %s
                       AND status = 0
                """, (symbol, user_id))

            elif new_position.get('partial'):
                cursor.execute("""
                    UPDATE mocktrade.position_history
                    SET `pnl` = %s,
                        `datetime` = %s,
                        `close_price` = %s
                    WHERE `id` = %s
                """, (
                    new_position.get('close_pnl', 0),
                    datetime.now(timezone('Asia/Seoul')),
                    new_position.get('close_price'),
                    current_id
                ))

                insert_sql = """
                  INSERT INTO mocktrade.position_history
                   (user_id, symbol, size, amount, entry_price,
                    liq_price, margin_ratio, margin, pnl,
                    margin_type, side, leverage, status, tp, sl, datetime, close_price)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                cursor.execute(insert_sql, (
                    new_position.get('user_id'), new_position.get('symbol'),
                    new_position.get('size'), new_position.get('amount'),
                    new_position.get('entry_price'), new_position.get('liq_price'),
                    new_position.get('margin_ratio'), new_position.get('margin'),
                    new_position.get('pnl', 0), new_position.get('margin_type'),
                    new_position.get('side'), new_position.get('leverage'), new_position.get('status'),
                    new_position.get('tp', 0), new_position.get('sl', 0), datetime.now(timezone("Asia/Seoul")),
                    0
                ))
            else:  # same side or new position
                insert_sql = """
                  INSERT INTO mocktrade.position_history
                   (user_id, symbol, size, amount, entry_price,
                    liq_price, margin_ratio, margin, pnl,
                    margin_type, side, leverage, status, tp, sl, datetime, close_price)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                cursor.execute(insert_sql, (
                    new_position.get('user_id'), new_position.get('symbol'),
                    new_position.get('size'), new_position.get('amount'),
                    new_position.get('entry_price'), new_position.get('liq_price'),
                    new_position.get('margin_ratio'), new_position.get('margin'),
                    new_position.get('pnl', 0), new_position.get('margin_type'),
                    new_position.get('side'), new_position.get('leverage'), new_position.get('status'),
                    new_position.get('tp', 0), new_position.get('sl', 0), datetime.now(timezone("Asia/Seoul")),
                    new_position.get('close_price')
                ))

            # update wallet for any realized PnL
            close_pnl = new_position.get('close_pnl', 0)
            if close_pnl:
                cursor.execute("""
                    UPDATE mocktrade.user
                       SET balance = GREATEST(balance + %s, 0)
                     WHERE `id` = %s
                """, (close_pnl, user_id))

            # mark order settled
            cursor.execute("""
                UPDATE mocktrade.order_history
                   SET `status` = 1,
                       `po_id` = %s,
                       `update_time` = %s 
                 WHERE `id` = %s
            """, (current_id, datetime.now(timezone('Asia/Seoul')), order_id))

            # 7) If tp/sl was attached
            if (order["tp"] or order["sl"]) and new_position['amount'] > 0:
                logger.info("opening tp/sl orders from the triggered limit order")
                symbol = order['symbol']
                limit_tp = order['tp']
                limit_sl = order['sl']
                limit_amount = order['amount']
                limit_side = order['side']
                leverage = order['leverage']
                margin_type = order['margin_type']
                margin = order['margin']

                order_side = 'sell' if limit_side == 'buy' else 'buy'

                if limit_tp:
                    cursor.execute("""
                                INSERT INTO mocktrade.order_history (
                                    user_id, symbol, `type`, margin_type, magin, leverage, side, amount, status
                                    ,insert_time, update_time, tp, or_id, order_price)
                                VALUES (
                                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s )
                            """, (
                        user_id,
                        symbol,
                        'tp',
                        margin_type,
                        margin,
                        leverage,
                        order_side,
                        limit_amount,
                        0,
                        datetime.now(timezone("Asia/Seoul")),
                        datetime.now(timezone("Asia/Seoul")),
                        limit_tp,
                        current_id,
                        limit_tp
                    ))
                    logger.info("successfully opened the take profit order from the triggered limit order")

                if limit_sl:
                    cursor.execute("""
                                INSERT INTO mocktrade.order_history (
                                    user_id, symbol, `type`, margin_type, magin, leverage, side, amount, status
                                    ,insert_time, update_time, sl, or_id, order_price)
                                VALUES (
                                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """, (
                        user_id,
                        symbol,
                        'sl',
                        margin_type,
                        margin,
                        leverage,
                        order_side,
                        limit_amount,
                        0,
                        datetime.now(timezone("Asia/Seoul")),
                        datetime.now(timezone("Asia/Seoul")),
                        limit_sl,
                        order_id,
                        limit_sl
                    ))
                    logger.info("successfully opened the stop loss order from the triggered limit order")

            conn.commit()
            return user_id
        except Exception:
            logger.exception(f"failed to persist limit order execution of order [{order.get('or_id')}] to MySQL")
            conn.rollback()
        finally:
            cursor and cursor.close()
            conn and conn.close()

    async def persist_triggered_tpsl_order(self, order, new_position, current_id):
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            user_id = order.get('user_id')
            symbol = order.get('symbol')
            order_id = order.get('or_id')

            cursor.execute("""
                UPDATE `mocktrade`.`position_history`
                   SET `status` = 2
                 WHERE `status` = 1
                   AND `user_id` = %s
                   AND `symbol` = %s
            """, (user_id, symbol))

            if new_position.get('close'): # position tp/sl
                cursor.execute("""
                    UPDATE mocktrade.position_history
                       SET `pnl` = %s,
                           `close_price` = %s,
                           `datetime` = %s,
                           `status` = 3
                     WHERE `id` = %s
                """, (
                    new_position.get('close_pnl', 0),
                    new_position.get('close_price', 0),
                    datetime.now(timezone('Asia/Seoul')),
                    current_id
                ))

                cursor.execute("""
                    UPDATE `mocktrade`.`order_history`
                       SET `status` = 4
                     WHERE `type` IN ('tp', 'sl')
                       AND `symbol` = %s
                       AND `user_id` = %s
                       AND `status` = 0
                """, (symbol, user_id))

            else: # partial close
                cursor.execute("""
                    UPDATE `mocktrade`.`position_history`
                       SET `pnl` = %s,
                           `close_price` = %s,
                           `status` = 2,
                           `datetime` = %s
                     WHERE `id` = %s
                """, (
                    new_position.get('close_pnl', 0),
                    new_position.get('close_price', 0),
                    datetime.now(timezone('Asia/Seoul')),
                    current_id
                ))

                cursor.execute("""
                    INSERT INTO mocktrade.position_history (
                        user_id, symbol, size, amount, entry_price,
                        liq_price, margin, pnl,
                        margin_type, side, leverage, status, tp, sl, datetime, close_price 
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    new_position.get('user_id'), new_position.get('symbol'),
                    new_position.get('size'), new_position.get('amount'),
                    new_position.get('entry_price'), new_position.get('liq_price'),
                    new_position.get('margin'),
                    new_position.get('pnl', 0), new_position.get('margin_type'),
                    new_position.get('side'), new_position.get('leverage', 0), new_position.get('status'),
                    new_position.get('tp', 0), new_position.get('sl', 0), datetime.now(timezone("Asia/Seoul")),
                    0
                ))

            close_pnl = new_position.get('close_pnl', 0)
            logger.info(f"applying close PnL of {close_pnl} to user [{user_id}] balance")
            if close_pnl:
                cursor.execute("""
                    UPDATE mocktrade.user 
                       SET `balance` = GREATEST(`balance` + %s, 0)
                     WHERE `id` = %s
                       AND `status` = 0  
                """, (close_pnl, user_id))

            # mark order settled
            cursor.execute("""
                UPDATE `mocktrade`.`order_history`
                   SET `status` = 1,
                       `price` = %s,
                       `update_time` = %s,
                       `po_id` = %s
                 WHERE `id` = %s
            """, (
                order.get('price', 0),
                datetime.now(timezone('Asia/Seoul')),
                current_id,
                order_id
            ))

            cursor.execute("""
                UPDATE mocktrade.order_history
                   SET status = 4
                 WHERE `type` IN ('tp', 'sl')
                   AND po_id = %s
                   AND `symbol` = %s
                   AND `user_id` = %s
                   AND status = 0
            """, (
                order['po_id'],
                symbol,
                user_id
            ))

            # Cancel sibling TP/SL order depending on triggered type
            if order['type'] == 'tp':
                cursor.execute("""
                    UPDATE mocktrade.order_history
                    SET status = 4
                    WHERE `id` = %s
                    AND `type` = 'sl'
                    AND `user_id` = %s
                    AND `symbol` = %s
                    AND status = 0
                """, (
                    order_id + 1,
                    user_id,
                    symbol
                ))
            elif order['type'] == 'sl':
                cursor.execute("""
                    UPDATE mocktrade.order_history
                    SET status = 4
                    WHERE `id` = %s
                    AND `type` = 'tp'
                    AND `user_id` = %s
                    AND `status` = 0 
                """, (
                    order_id - 1,
                    user_id,
                    symbol
                ))

            conn.commit()
        except Exception:
            logger.exception(f"failed to persist tp/sl execution of order [{order.get('or_id')}]")
            conn.rollback()
        finally:
            cursor and cursor.close()
            conn and conn.close()

    async def settle_iso_liquidation(self, position_redis):
        #logger.info("settle iso liquidation")
        conn = None
        cursor = None
        liquidated = 0

        pending_notifs: list[tuple[str, dict]] = []
        liquidated_users = {}

        position_redis = await self.get_position_redis()
        if not position_redis:
            logger.warning("position redis yet to be initialized")
            return

        try:
            conn = self._get_connection()
            conn.autocommit(False)
            cursor = conn.cursor()

            async for key in position_redis.scan_iter("positions:*"):
                _, user_id = key.split(":")
                raw_positions = await position_redis.hgetall(key)

                for symbol, raw in raw_positions.items():
                    try:
                        pos = json.loads(raw)
                    except Exception:
                        logger.warning(f"Skipping malformed Redis pos: {user_id}:{symbol}")
                        continue

                    if pos.get("margin_type") != 'isolated':
                        continue

                    pos_id = pos.get("pos_id")
                    side = pos.get("side")
                    liq_price = pos.get("liq_price")
                    market_price = pos.get("market_price")
                    retri_id = user_id
                    user_id = int(pos.get('user_id'))

                    if None in (pos_id, side, liq_price, market_price):
                        continue

                    should_liquidate = (
                            (side == 'buy' and market_price <= liq_price) or
                            (side == 'sell' and market_price >= liq_price)
                    )

                    if not should_liquidate:
                        continue

                    cursor.execute("SAVEPOINT lq_order")
                    try:
                        margin = float(pos.get("margin", 0))
                        close_pnl = -margin

                        # a) mark previous positions as closed
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                               SET status = 2
                             WHERE status = 1
                               AND symbol = %s
                               AND user_id = %s
                        """, (symbol, user_id))

                        # b) update target position as liquidated
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                               SET pnl = %s,
                                   close_price = %s,
                                   status = 3,
                                   `datetime` = %s
                             WHERE id = %s
                        """, (close_pnl, market_price, datetime.now(timezone("Asia/Seoul")), pos_id))

                        # c) debit wallet
                        cursor.execute("""
                            UPDATE mocktrade.user
                               SET balance = CASE
                                               WHEN balance + %s < 0 THEN 0
                                               ELSE balance + %s
                                             END
                             WHERE id = %s AND status = 0
                        """, (close_pnl, close_pnl, user_id))

                        # d) cancel TP/SL orders
                        cursor.execute("""
                            UPDATE mocktrade.order_history
                               SET status = 4
                             WHERE user_id = %s AND symbol = %s AND type IN ('tp', 'sl')
                        """, (user_id, symbol))

                        pending_notifs.append((
                            retri_id,
                            {"trigger" : "liquidation_isolated", "positions": pos}
                        ))
                        liquidated_users[user_id] = retri_id

                        cursor.execute("RELEASE SAVEPOINT lq_order")
                        liquidated += 1
                        logger.info(f"isolated position [{pos_id}] liquidated")
                    except Exception:
                        cursor.execute("ROLLBACK TO SAVEPOINT lq_order")
                        cursor.execute("RELEASE SAVEPOINT lq_order")
                        logger.exception(f"Failed to liquidate Redis pos_id [{pos_id}]")
                        continue

            conn.commit()
            for retri_id, message in pending_notifs:
                await asyncio.create_task(manager.notify_user(retri_id, message))
            for user_id, retri_id in liquidated_users.items():
                await update_position_status_per_user(user_id, retri_id)
                await update_order_status_per_user(user_id, retri_id)
                await update_balance_status_per_user(user_id, retri_id)

            if liquidated > 0:
                logger.info(f"total {liquidated} isolated positions liquidated")

        except Exception:
            logger.exception("Fatal Error executing liquidate_iso_positions")
            conn.rollback()
            raise
        finally:
            cursor and cursor.close()
            conn and conn.close()



