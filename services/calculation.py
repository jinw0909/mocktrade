import asyncio
# routers/pnl_ws.py
import asyncio
import json
import logging
import time
import math

from datetime import datetime, timedelta
from pytz import timezone
from starlette.config import Config
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from utils.connection_manager import manager
import redis.asyncio as aioredis
from utils.connections import MySQLAdapter

router = APIRouter()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
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

    amount = round(float(order['amount']), QTY_DP)

    if amount <= 0:
        raise ValueError(
            f"Invalid order amount after rounding: "
            f"symbol={symbol}, amount={amount}, qty_dp={QTY_DP}"
        )

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
    # current_amount = float(current_position['amount'])
    current_amount = round(float(current_position['amount']), QTY_DP)
    current_entry_price = float(current_position['entry_price'])
    current_margin = float(current_position['margin'])
    # current_size = float(current_position['size'])
    # current_pnl = float(current_position.get('pnl') or 0)
    current_tp = current_position.get('tp')
    current_sl = current_position.get('sl')

    # case 2: Same-side -> merge positions
    if current_side == side:
        logger.info("case 2: same side")

        # total_amount = current_amount + amount
        total_amount = round(current_amount + amount, QTY_DP)
        total_value = (current_entry_price * current_amount) + (price * amount)
        # 1) guard avg_entry_price
        # if total_amount > 0:
        #     avg_entry_price = total_value / total_amount
        # else:
        #     # fallback to whatever makes sense — e.g. the new order price
        #     avg_entry_price = price

        if total_amount <= 0:
            raise ValueError(
                f"Invalid total amount: symbol={symbol}, total_amount={total_amount}"
            )

        avg_entry_price = total_value / total_amount
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
    remaining_amount = round(current_amount - amount, QTY_DP)
    if remaining_amount > 0:
        logger.info("case 3-1, opposite side, partial close")
        # Partial close — reduce position
        new_amount = remaining_amount

        close_pnl = (price - current_entry_price) * amount if current_side == 'buy' else (
                                                                                                 current_entry_price - price) * amount
        fee = abs(close_pnl) * FEE_RATE
        net_close = close_pnl - fee
        # new_pnl = current_pnl + close_pnl
        # new_pnl = close_pnl
        leverage = current_position.get('leverage', 0)
        new_size = new_amount * current_entry_price
        new_margin = (new_size / leverage) if leverage > 0 else 0.0

        # effective_leverage = current_size / current_margin if current_margin else leverage
        # leverage = max(order.get('leverage', 0), current_position.get('leverage', 0))

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

    elif remaining_amount == 0:
        logger.info("case 3-2, opposite side, full close")
        # Full close — no new position
        close_pnl = (price - current_entry_price) * amount if current_side == 'buy' else (current_entry_price - price) * amount
        # new_pnl = current_pnl + close_pnl
        fee = abs(close_pnl) * FEE_RATE
        # new_pnl = close_pnl
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
        # flip_amount = amount - current_amount
        flip_amount = abs(remaining_amount)
        close_pnl = (price - current_entry_price) * current_amount if current_side == 'buy' else (current_entry_price - price) * current_amount
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
            "amount": flip_amount,
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

    # 1) No existing position ⇒ already closed
    if not current_position:
        return {"status": "closed"}

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


    # unpack current
    cs = current_position['side']  # 'buy' or 'sell'
    # cur_amt = float(current_position['amount'])

    cur_amt = round(float(current_position["amount"]), QTY_DP)
    amount = round(float(order["amount"]), QTY_DP)

    cur_price = float(current_position['entry_price'])
    cur_margin = float(current_position['margin'])
    # cur_size = float(current_position['size'])
    # cur_lev = float(current_position['leverage'])

    # decide how much to close
    close_amt = cur_amt if not from_order else min(amount, cur_amt)
    logger.info(f"close_amt: {close_amt}")
    remaining_amount = round(cur_amt - close_amt, QTY_DP)
    # compute PnL for the closed portion
    if cs == 'buy':
        raw_pnl = (price - cur_price) * close_amt
    else:  # short
        raw_pnl = (cur_price - price) * close_amt

    fee = abs(raw_pnl) * FEE_RATE
    net_pnl = raw_pnl - fee

    # full-close
    # if close_amt >= cur_amt:
    if remaining_amount <= 0:
        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": cur_amt,
            "entry_price": None,
            "size": 0,
            "margin": cur_margin,
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

    else:
        # partial-close
        # new_amt = cur_amt - close_amt
        new_amt = remaining_amount
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


def calc_close_pnl(side: str, amount: float, entry: float, close: float) -> float:
    side_sign = 1.0 if side == "buy" else -1.0
    return side_sign * amount * (close - entry)


class CalculationService(MySQLAdapter):

    def __init__(self):
        super().__init__()
        self._position_redis = None
        self._price_redis = None
        self.missing_symbols = set()

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

    # 🔽🔽🔽 ADD THIS HELPER 🔽🔽🔽
    async def send_redis_signal(self, retri_id, payload):
        position_redis = await self.get_position_redis()
        key = f"signals:{retri_id}"

        logger.info(
            "[redis-signal] retri_id=%s key=%s payload=%s",
            retri_id, key, json.dumps(payload, ensure_ascii=False)
        )

        await position_redis.set(key, json.dumps(payload))

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
                initial_margin = (
                    initial_notional / leverage
                    if leverage
                    else 0.0
                )

                pnl_pct = (
                    pnl / initial_notional * 100
                    if initial_notional
                    else 0.0
                )

                roi_pct = (
                    pnl / initial_margin * 100
                    if initial_margin
                    else 0.0
                )

                # Update in-memory
                pos_data["unrealized_pnl"] = pnl
                pos_data["unrealized_pnl_pct"] = pnl_pct
                pos_data["roi_pct"] = roi_pct
                pos_data["margin"] = margin
                pos_data["market_price"] = current_price
                pos_data["size"] = size

                pipe.hset(key, sym, json.dumps(pos_data))

            await pipe.execute()

    # async def calculate_liq_prices(self):
    #     position_redis = await self.get_position_redis()
    #     price_redis = await self.get_price_redis()
    #
    #     # ----------------------------------------------------------
    #     # 0) GLOBAL FAIL-SAFE: 가격 시스템이 완전히 죽어있으면 전체 스킵
    #     #    - price:* 키가 하나도 없으면 → 어떤 포지션도 청산/강제종료하지 않음
    #     # ----------------------------------------------------------
    #     has_any_price = False
    #     async for _ in price_redis.scan_iter("price:*", count=1):
    #         has_any_price = True
    #         break
    #
    #     if not has_any_price:
    #         logger.error(
    #             "[LIQ] No price:* keys found in Redis. "
    #             "Price system seems DOWN → Skipping all liquidation calculations."
    #         )
    #         return
    #
    #     async for pos_key in position_redis.scan_iter("positions:*"):
    #         try:
    #             _, user_id = pos_key.split(":")
    #             raw_pos = await position_redis.hgetall(pos_key)
    #             positions = [json.loads(p) for p in raw_pos.values()]
    #             if not positions:
    #                 continue
    #
    #             # load balance and orders
    #             bal_key = f"balances:{user_id}"
    #             ord_key = f"orders:{user_id}"
    #             avl_key = f"availables:{user_id}"
    #             # liq_key = f"liq_prices:{user_id}"  # 필요시 재활성화
    #
    #             balance = float(await position_redis.get(bal_key) or 0)
    #             raw_ord = await position_redis.get(ord_key) or "[]"
    #             orders = json.loads(raw_ord)
    #
    #             iso_pos = sum(p["margin"] for p in positions if p["margin_type"] == "isolated")
    #             iso_ord = sum(
    #                 o["margin"]
    #                 for o in orders
    #                 if o["margin_type"] == "isolated" and o["type"] in ("limit", "market")
    #             )
    #             cross_equity = balance - iso_pos - iso_ord
    #
    #             total_pos = sum(p["margin"] for p in positions)
    #             total_ord = sum(o["margin"] for o in orders)
    #             total_upnl = sum(
    #                 p.get("unrealized_pnl", 0.0)
    #                 for p in positions
    #                 if p["margin_type"] == "cross"
    #             )
    #             available = balance - total_pos - total_ord + total_upnl
    #             # await position_redis.set(avl_key, available)
    #             available_to_write = available if available >= 0 else 0
    #
    #             # cross 포지션만 대상
    #             cross = [p for p in positions if p["margin_type"] == "cross"]
    #
    #             # ----------------------------------------------------------
    #             # 가격이 없는 포지션은 "스킵"만 하고 절대 강제청산하지 않는다.
    #             # ----------------------------------------------------------
    #             skipped_no_price = []
    #             valid_cross = []
    #
    #             for p in cross:
    #                 redis_key = f"price:{p['symbol']}USDT"
    #                 raw = await price_redis.get(redis_key)
    #
    #                 if raw is None:
    #                     logger.warning(
    #                         f"[LIQ] No price for {p['symbol']} (user={user_id}, pos_id={p['pos_id']}). "
    #                         "Skipping this position from liquidation calc."
    #                     )
    #                     skipped_no_price.append(p)
    #                     continue
    #
    #                 try:
    #                     p["market_price"] = float(raw)
    #                 except (TypeError, ValueError):
    #                     logger.error(
    #                         f"[LIQ] Invalid price for {p['symbol']} ({raw!r}), "
    #                         f"skipping pos_id={p['pos_id']}."
    #                     )
    #                     skipped_no_price.append(p)
    #                     continue
    #
    #                 valid_cross.append(p)
    #
    #             # 가격이 있는 cross 포지션만 사용
    #             cross = valid_cross
    #
    #             # 가격이 있는 cross 포지션이 하나도 없으면 이 유저는 더 이상 처리할 게 없음
    #             if not cross:
    #                 continue
    #
    #             # 2) precompute other-legs TMM & UPNL
    #             other_tmm = {
    #                 p["pos_id"]: sum(
    #                     MAINTENANCE_RATE * q["market_price"] * q["amount"]
    #                     for q in cross
    #                     if q["pos_id"] != p["pos_id"]
    #                 )
    #                 for p in cross
    #             }
    #             other_upnl = {
    #                 p["pos_id"]: sum(
    #                     q.get("unrealized_pnl", 0.0)
    #                     for q in cross
    #                     if q["pos_id"] != p["pos_id"]
    #                 )
    #                 for p in cross
    #             }
    #
    #             liq_prices = []
    #             breaches = []
    #
    #             for p in cross:
    #                 pid = p["pos_id"]
    #                 E = p["entry_price"]
    #                 S = p["amount"]
    #                 MP = p["market_price"]
    #                 side = 1 if p["side"] == "buy" else -1
    #
    #                 cumB = MAINTENANCE_RATE * E * S
    #                 notional = side * S * E
    #
    #                 num = (
    #                         cross_equity
    #                         - other_tmm[pid]
    #                         + other_upnl[pid]
    #                         + cumB
    #                         - notional
    #                 )
    #                 den = S * MAINTENANCE_RATE - side * S
    #
    #                 if den == 0:
    #                     logger.warning(
    #                         f"[LIQ] Denominator zero for pos_id={pid}, "
    #                         f"user={user_id}, symbol={p['symbol']}. Skipping."
    #                     )
    #                     continue
    #
    #                 raw_lp = num / den
    #
    #                 # side-aware LP 보정
    #                 if side == -1 and raw_lp <= 0:
    #                     logger.info(
    #                         f"[LIQ] Short pos_id={pid} has non-positive LP "
    #                         f"({raw_lp:.6f}), skipping from liquidation table."
    #                     )
    #                     continue
    #
    #                 if side == 1 and raw_lp <= 0:
    #                     lp = 0.0
    #                 else:
    #                     lp = raw_lp
    #
    #                 liq_prices.append(
    #                     {
    #                         "pos_id": pid,
    #                         "symbol": p["symbol"],
    #                         "liq_price": lp,
    #                     }
    #                 )
    #
    #                 if MP is not None:
    #                     if (side == 1 and MP <= lp) or (side == -1 and MP >= lp):
    #                         breaches.append(
    #                             {
    #                                 "pos_id": pid,
    #                                 "symbol": p["symbol"],
    #                                 "liq_price": lp,
    #                                 "current": MP,
    #                                 "pnl": p.get("unrealized_pnl", 0.0),
    #                             }
    #                         )
    #
    #             # 가장 손실(pnl)이 큰 놈 하나만 청산
    #             to_liquidate = min(breaches, key=lambda b: b["pnl"]) if breaches else None
    #
    #             if to_liquidate:
    #                 now_kst = datetime.now(timezone("Asia/Seoul"))
    #                 sym = to_liquidate["symbol"]
    #                 close_price = float(to_liquidate["current"])
    #                 pos_hist_id = int(to_liquidate["pos_id"])
    #
    #                 # 0) Redis에서 포지션 원본 확보 (pnl 재계산용)
    #                 blob = await position_redis.hget(pos_key, sym)
    #                 if not blob:
    #                     logger.warning(f"[CROSS-LIQ] missing redis pos blob user={user_id} sym={sym}")
    #                     continue
    #
    #                 pos = json.loads(blob)
    #                 entry = float(pos["entry_price"])
    #                 amount = float(pos["amount"])
    #                 side = pos["side"]
    #
    #                 # ✅ 현재가 기준으로 손익 재계산
    #                 pnl_liq = float(calc_close_pnl(side, amount, entry, close_price))
    #
    #                 conn = None
    #                 cursor = None
    #                 try:
    #                     conn = self._get_connection()
    #                     conn.autocommit(False)
    #                     cursor = conn.cursor()
    #
    #                     # 1) retri_id -> 내부 uid
    #                     cursor.execute("""
    #                                    SELECT id
    #                                    FROM mocktrade.user
    #                                    WHERE retri_id = %s AND status = 0
    #                                    LIMIT 1
    #                                    """, (user_id,))
    #                     row = cursor.fetchone()
    #                     if not row:
    #                         conn.rollback()
    #                         logger.warning(f"[CROSS-LIQ] user not found retri_id={user_id}")
    #                         continue
    #
    #                     uid = row["id"]
    #
    #                     # 2) position_history: idempotent 청산 확정
    #                     cursor.execute("""
    #                                    UPDATE mocktrade.position_history
    #                                    SET status = 3,
    #                                        pnl = %s,
    #                                        datetime = %s,
    #                                        close_price = %s
    #                                    WHERE id = %s
    #                                      AND user_id = %s
    #                                      AND status = 1
    #                                    """, (pnl_liq, now_kst, close_price, pos_hist_id, uid))
    #
    #                     if cursor.rowcount == 0:
    #                         # 이미 청산/종료 처리됨
    #                         conn.rollback()
    #                         continue
    #
    #                     # 3) user balance 반영 (DB가 진실)
    #                     cursor.execute("""
    #                                    UPDATE mocktrade.user
    #                                    SET balance = GREATEST(balance + %s, 0)
    #                                    WHERE retri_id = %s
    #                                    """, (pnl_liq, user_id))
    #
    #                     # 4) liquidation order_history insert (중복 방지)
    #                     cursor.execute("""
    #                                    SELECT 1
    #                                    FROM mocktrade.order_history
    #                                    WHERE po_id = %s
    #                                      AND type = 'liquidation'
    #                                    LIMIT 1
    #                                    """, (pos_hist_id,))
    #                     exists = cursor.fetchone()
    #
    #                     if not exists:
    #                         liq_side = "sell" if side == "buy" else "buy"
    #                         cursor.execute("""
    #                                        INSERT INTO mocktrade.order_history (
    #                                            user_id, symbol, type, margin_type, side,
    #                                            price, magin, amount, leverage, status,
    #                                            insert_time, update_time, order_price, po_id,
    #                                            tp, sl, order_historycol, or_id
    #                                        ) VALUES (
    #                                                     %s, %s, 'liquidation', 'cross', %s,
    #                                                     %s, %s, %s, %s, %s,
    #                                                     %s, %s, %s, %s,
    #                                                     NULL, NULL, NULL, NULL
    #                                                 )
    #                                        """, (
    #                                            uid, sym, liq_side,
    #                                            close_price,
    #                                            float(pos.get("margin", 0.0)),
    #                                            amount,
    #                                            float(pos.get("leverage", 0.0)),
    #                                            1,
    #                                            now_kst, now_kst,
    #                                            close_price,
    #                                            pos_hist_id
    #                                        ))
    #
    #                     # 5) (중요) 해당 심볼 TP/SL 주문 취소 (cross도 같이 정리하는 게 맞음)
    #                     cursor.execute("""
    #                                    UPDATE mocktrade.order_history
    #                                    SET status = 4
    #                                    WHERE user_id = %s
    #                                      AND symbol = %s
    #                                      AND type IN ('tp','sl')
    #                                      AND status = 1
    #                                    """, (uid, sym))
    #
    #                     conn.commit()
    #
    #                     logger.warning(
    #                         f"[CROSS-LIQ] retri_id={user_id} uid={uid} pos_id={pos_hist_id} sym={sym} "
    #                         f"side={side} entry={entry} close={close_price} amt={amount} pnl={pnl_liq:.4f}"
    #                     )
    #
    #                     # ✅ Redis는 여기서 직접 수정하지 말고, DB→Redis 싱크로 통일
    #                     await update_position_status_per_user(uid, user_id)
    #                     await update_order_status_per_user(uid, user_id)
    #                     await update_balance_status_per_user(uid, user_id)
    #
    #                     # (선택) 소켓 알림은 유지
    #                     await self.send_redis_signal(
    #                         retri_id=user_id,
    #                         payload={"trigger": "liquidation_cross", "pos": {**to_liquidate, "pnl": pnl_liq}},
    #                     )
    #
    #                     # liq_prices에서 제거 (Redis에 liq_price 쓰는 루프에서 빠지게)
    #                     liq_prices = [x for x in liq_prices if x["pos_id"] != pos_hist_id]
    #
    #                 except Exception:
    #                     if conn:
    #                         conn.rollback()
    #                     logger.exception(f"[CROSS-LIQ] failed persisting liquidation retri_id={user_id} sym={sym}")
    #                 finally:
    #                     if cursor:
    #                         cursor.close()
    #                     if conn:
    #                         conn.close()
    #
    #
    #
    #         # Redis에 liq_price만 업데이트 (cross margin만)
    #             for lp_item in liq_prices:
    #                 symbol = lp_item["symbol"]
    #                 new_lp = lp_item["liq_price"]
    #
    #                 blob = await position_redis.hget(pos_key, symbol)
    #                 if not blob:
    #                     continue
    #
    #                 data = json.loads(blob)
    #                 if data.get("margin_type") == "cross":
    #                     data["liq_price"] = new_lp
    #                     await position_redis.hset(pos_key, symbol, json.dumps(data))
    #
    #             await position_redis.set(avl_key, available_to_write)
    #
    #             # 필요시 유저별 liq_prices 테이블 유지하고 싶으면 아래 주석 풀기
    #             # if liq_prices:
    #             #     await position_redis.set(
    #             #         liq_key,
    #             #         json.dumps({"positions": liq_prices})
    #             #     )
    #             # else:
    #             #     await position_redis.delete(liq_key)
    #
    #         except Exception:
    #             logger.exception("Failed calculating liq prices for user")
    #             continue

    async def calculate_liq_prices(self):
        """
        Cross liquidation + liq_price 갱신 + available 갱신

        전제(네가 확인해준 status 규칙):
          - order_history.status = 0  : open/pending (redis orders:* 싱크 대상)
          - order_history.status = 1  : filled/executed (liquidation 기록은 여기)
          - order_history.status = 4  : canceled (tp/sl 취소는 여기)

        핵심:
          1) cross 포지션 breach 중 손실(pnl) 가장 큰 1개만 청산
          2) 청산 시점에 pnl을 현재가 기준으로 재계산(calc_close_pnl)
          3) position_history.status=1 인 포지션만 status=3(청산)으로 바꿈 (idempotent)
          4) order_history에 liquidation 1회 기록 (po_id + type='liquidation' 중복 방지)
          5) 청산된 심볼의 TP/SL(open) 주문을 status=4로 즉시 취소
          6) DB commit 후 Redis 반영(positions 제거, balance set)
          7) DB가 truth이므로 per_user sync 호출로 Redis를 MySQL 기준으로 정합화
          8) 마지막에 liq_price 업데이트 + available set

        주의:
          - MAINTENANCE_RATE, calc_close_pnl, update_*_per_user, send_redis_signal,
            self._get_connection(), self.get_position_redis(), self.get_price_redis()
            는 이미 존재한다고 가정.
        """
        position_redis = await self.get_position_redis()
        price_redis = await self.get_price_redis()

        # ----------------------------------------------------------
        # 0) GLOBAL FAIL-SAFE: price:* 키가 하나도 없으면 전체 스킵
        # ----------------------------------------------------------
        has_any_price = False
        async for _ in price_redis.scan_iter("price:*", count=1):
            has_any_price = True
            break
        if not has_any_price:
            logger.error(
                "[LIQ] No price:* keys found in Redis. "
                "Price system seems DOWN → Skipping all liquidation calculations."
            )
            return

        # cross liquidation 발생 유저들: 커밋 이후 per_user sync
        liquidated_users: dict[int, str] = {}  # {mysql_user_id: retri_id}

        async for pos_key in position_redis.scan_iter("positions:*"):
            try:
                _, retri_id = pos_key.split(":", 1)

                raw_pos = await position_redis.hgetall(pos_key)
                positions = []
                for p in raw_pos.values():
                    try:
                        positions.append(json.loads(p))
                    except Exception:
                        continue

                if not positions:
                    continue

                # ---- redis keys
                bal_key = f"balances:{retri_id}"
                ord_key = f"orders:{retri_id}"
                avl_key = f"availables:{retri_id}"

                # ---- snapshot (루프 시작 시점)
                # ---- snapshot (루프 시작 시점)
                # balance 키 누락을 실제 잔고 0으로 처리하면 오청산이 발생할 수 있다.
                raw_balance = await position_redis.get(bal_key)

                if raw_balance is None:
                    logger.error(
                        "[LIQ-SKIP] Missing Redis balance key. "
                        "Skipping liquidation calculation for safety. retri_id=%s key=%s",
                        retri_id,
                        bal_key,
                    )
                    continue

                try:
                    balance = float(raw_balance)
                except (TypeError, ValueError):
                    logger.error(
                        "[LIQ-SKIP] Invalid Redis balance value. "
                        "Skipping liquidation calculation for safety. "
                        "retri_id=%s key=%s value=%r",
                        retri_id,
                        bal_key,
                        raw_balance,
                    )
                    continue

                if not math.isfinite(balance) or balance < 0:
                    logger.error(
                        "[LIQ-SKIP] Abnormal Redis balance value. "
                        "Skipping liquidation calculation for safety. "
                        "retri_id=%s key=%s value=%r",
                        retri_id,
                        bal_key,
                        raw_balance,
                    )
                    continue

                raw_ord = await position_redis.get(ord_key) or "[]"
                raw_ord = await position_redis.get(ord_key) or "[]"
                try:
                    orders = json.loads(raw_ord)
                except Exception:
                    orders = []

                # ---- isolated가 cross_equity에서 빠져야 함
                iso_pos = sum(p.get("margin", 0.0) for p in positions if p.get("margin_type") == "isolated")
                iso_ord = sum(
                    o.get("margin", 0.0)
                    for o in orders
                    if o.get("margin_type") == "isolated" and o.get("type") in ("limit", "market")
                )
                cross_equity = balance - iso_pos - iso_ord

                # ---- available 계산(= balance - pos_margin - ord_margin + cross_upnl)
                total_pos = sum(p.get("margin", 0.0) for p in positions)
                total_ord = sum(o.get("margin", 0.0) for o in orders)  # ⚠️ 네 구조상 orders redis는 open만 유지됨
                total_upnl = sum(
                    p.get("unrealized_pnl", 0.0)
                    for p in positions
                    if p.get("margin_type") == "cross"
                )

                available = balance - total_pos - total_ord + total_upnl
                available_to_write = max(available, 0.0)

                # ---- cross positions only
                cross = [p for p in positions if p.get("margin_type") == "cross"]
                if not cross:
                    # cross 없더라도 available은 갱신해둘지 정책 선택 가능
                    await position_redis.set(avl_key, available_to_write)
                    continue

                # ----------------------------------------------------------
                # 가격이 없는 포지션은 스킵
                # ----------------------------------------------------------
                valid_cross = []
                for p in cross:
                    sym = p.get("symbol")
                    if not sym:
                        continue

                    redis_key = f"price:{sym}USDT"
                    raw_price = await price_redis.get(redis_key)
                    if raw_price is None:
                        logger.warning(
                            f"[LIQ] No price for {sym} (retri_id={retri_id}, pos_id={p.get('pos_id')}). "
                            "Skipping this position from liquidation calc."
                        )
                        continue

                    try:
                        p["market_price"] = float(raw_price)
                    except (TypeError, ValueError):
                        logger.error(
                            f"[LIQ] Invalid price for {sym} ({raw_price!r}), skipping pos_id={p.get('pos_id')}."
                        )
                        continue

                    valid_cross.append(p)

                cross = valid_cross
                if not cross:
                    await position_redis.set(avl_key, available_to_write)
                    continue

                # ----------------------------------------------------------
                # other-legs TMM & UPNL
                # ----------------------------------------------------------
                other_tmm = {
                    p["pos_id"]: sum(
                        MAINTENANCE_RATE * q["market_price"] * q["amount"]
                        for q in cross
                        if q["pos_id"] != p["pos_id"]
                    )
                    for p in cross
                }
                other_upnl = {
                    p["pos_id"]: sum(
                        q.get("unrealized_pnl", 0.0)
                        for q in cross
                        if q["pos_id"] != p["pos_id"]
                    )
                    for p in cross
                }

                # ----------------------------------------------------------
                # liq_price 계산 + breach 탐지
                # ----------------------------------------------------------
                liq_prices: list[dict] = []
                breaches: list[dict] = []

                for p in cross:
                    pid = p["pos_id"]
                    E = float(p["entry_price"])
                    S = float(p["amount"])
                    if S <= 0:
                        logger.error(
                            "[LIQ-SKIP] Invalid open position amount. "
                            "pos_id=%s retri_id=%s symbol=%s amount=%s",
                            pid,
                            retri_id,
                            p.get("symbol"),
                            S,
                        )
                        continue
                    MP = float(p["market_price"])
                    side_sign = 1 if p.get("side") == "buy" else -1

                    cumB = MAINTENANCE_RATE * E * S
                    notional = side_sign * S * E

                    num = (
                            cross_equity
                            - other_tmm[pid]
                            + other_upnl[pid]
                            + cumB
                            - notional
                    )
                    den = S * MAINTENANCE_RATE - side_sign * S
                    if den == 0:
                        logger.warning(
                            f"[LIQ] Denominator zero for pos_id={pid}, retri_id={retri_id}, symbol={p.get('symbol')}. Skipping."
                        )
                        continue

                    raw_lp = num / den

                    # side-aware 보정
                    if side_sign == -1 and raw_lp <= 0:
                        # short인데 LP <= 0 이면 테이블에서 제외
                        logger.info(
                            f"[LIQ] Short pos_id={pid} has non-positive LP ({raw_lp:.6f}), skipping from liquidation table."
                        )
                        continue

                    lp = 0.0 if (side_sign == 1 and raw_lp <= 0) else raw_lp

                    liq_prices.append({"pos_id": pid, "symbol": p["symbol"], "liq_price": lp})

                    # breach
                    if (side_sign == 1 and MP <= lp) or (side_sign == -1 and MP >= lp):
                        breaches.append(
                            {
                                "pos_id": pid,
                                "symbol": p["symbol"],
                                "liq_price": lp,
                                "current": MP,
                                "pnl": float(p.get("unrealized_pnl", 0.0)),  # 선택 기준용
                            }
                        )

                # ----------------------------------------------------------
                # 가장 손실(pnl)이 큰 1개만 청산
                # ----------------------------------------------------------
                to_liquidate = min(breaches, key=lambda b: b["pnl"]) if breaches else None

                if to_liquidate:
                    sym = to_liquidate["symbol"]

                    # Redis 원본 확보 (pnl 재계산용)
                    blob = await position_redis.hget(pos_key, sym)
                    if not blob:
                        logger.warning(f"[CROSS-LIQ] missing redis pos blob retri_id={retri_id} sym={sym}")
                        to_liquidate = None

                if to_liquidate:
                    now_kst = datetime.now(timezone("Asia/Seoul"))
                    sym = to_liquidate["symbol"]
                    close_price = float(to_liquidate["current"])
                    pos_hist_id = int(to_liquidate["pos_id"])

                    pos = json.loads(blob)
                    entry = float(pos["entry_price"])
                    amount = float(pos["amount"])
                    side = pos["side"]
                    leverage = float(pos.get("leverage", 0.0))
                    margin = float(pos.get("margin", 0.0))

                    # ✅ 현재가 기준 pnl 재계산
                    pnl_liq = float(calc_close_pnl(side, amount, entry, close_price))
                    # new_balance = max(balance + pnl_liq, 0.0)

                    conn = None
                    cursor = None
                    committed = False

                    try:
                        conn = self._get_connection()
                        conn.autocommit(False)
                        cursor = conn.cursor()

                        # 1) retri_id -> 내부 uid
                        # cursor.execute(
                        #     """
                        #     SELECT id
                        #     FROM mocktrade.user
                        #     WHERE retri_id = %s AND status = 0
                        #     LIMIT 1
                        #     """,
                        #     (retri_id,),
                        # )
                        # row = cursor.fetchone()
                        # if not row:
                        #     conn.rollback()
                        #     logger.warning(f"[CROSS-LIQ] user not found retri_id={retri_id}")
                        #     raise RuntimeError("user not found")
                        #
                        # uid = int(row["id"])
                        cursor.execute(
                            """
                            SELECT
                                id,
                                balance
                            FROM mocktrade.user
                            WHERE retri_id = %s
                              AND status = 0
                            LIMIT 1
                            """,
                            (retri_id,),
                        )

                        row = cursor.fetchone()

                        if not row:
                            conn.rollback()
                            logger.warning(
                                "[CROSS-LIQ] User not found. retri_id=%s",
                                retri_id,
                            )
                            raise RuntimeError("user not found")

                        uid = int(row["id"])
                        db_balance = float(row["balance"] or 0.0)

                        # Redis balance가 DB truth와 다르면 청산을 중단하고 Redis를 복구한다.
                        # API 서버 주문 처리와 스케줄러 동기화가 겹쳐도 잘못된 balance로 청산하지 않는다.
                        if abs(db_balance - balance) > 1e-6:
                            await position_redis.set(bal_key, db_balance)

                            conn.rollback()

                            logger.error(
                                "[LIQ-SKIP] Redis / DB balance mismatch. "
                                "Liquidation aborted and Redis balance repaired. "
                                "retri_id=%s uid=%s redis_balance=%s db_balance=%s",
                                retri_id,
                                uid,
                                balance,
                                db_balance,
                            )

                            raise RuntimeError("redis/db balance mismatch")

                        new_balance = max(db_balance + pnl_liq, 0.0)

                        # 2) position_history: 청산 확정 (idempotent)
                        cursor.execute(
                            """
                            UPDATE mocktrade.position_history
                            SET status = 3,
                                pnl = %s,
                                datetime = %s,
                                close_price = %s
                            WHERE id = %s
                              AND user_id = %s
                              AND status = 1
                            """,
                            (pnl_liq, now_kst, close_price, pos_hist_id, uid),
                        )

                        if cursor.rowcount == 0:
                            # 이미 처리됨(다른 루틴/중복 실행)
                            conn.rollback()
                            to_liquidate = None
                            committed = False
                        else:
                            # 3) user balance 반영
                            cursor.execute(
                                """
                                UPDATE mocktrade.user
                                SET balance = GREATEST(balance + %s, 0)
                                WHERE retri_id = %s
                                """,
                                (pnl_liq, retri_id),
                            )

                            # 4) 청산된 심볼의 TP/SL(open) 즉시 취소 (margin lock 해제는 다음 tick available 계산에서 반영)
                            cursor.execute(
                                """
                                UPDATE mocktrade.order_history
                                SET status = 4,
                                    update_time = %s
                                WHERE user_id = %s
                                  AND symbol = %s
                                  AND status = 0
                                  AND type IN ('tp','sl')
                                """,
                                (now_kst, uid, sym),
                            )

                            # 5) order_history liquidation 기록 (중복 방지)
                            cursor.execute(
                                """
                                SELECT 1
                                FROM mocktrade.order_history
                                WHERE po_id = %s AND type = 'liquidation'
                                LIMIT 1
                                """,
                                (pos_hist_id,),
                            )
                            exists = cursor.fetchone()

                            if not exists:
                                liq_side = "sell" if side == "buy" else "buy"

                                cursor.execute(
                                    """
                                    INSERT INTO mocktrade.order_history (
                                        user_id, symbol, type, margin_type, side,
                                        price, magin, amount, leverage, status,
                                        insert_time, update_time, order_price, po_id,
                                        tp, sl, order_historycol, or_id
                                    ) VALUES (
                                                 %s, %s, 'liquidation', 'cross', %s,
                                                 %s, %s, %s, %s, %s,
                                                 %s, %s, %s, %s,
                                                 NULL, NULL, NULL, NULL
                                             )
                                    """,
                                    (
                                        uid, sym, liq_side,
                                        close_price, margin, amount, leverage, 1,  # ✅ status=1 (executed)
                                        now_kst, now_kst, close_price, pos_hist_id,
                                    ),
                                )

                            conn.commit()
                            committed = True

                            logger.warning(
                                f"[CROSS-LIQ] retri_id={retri_id} uid={uid} pos_id={pos_hist_id} sym={sym} "
                                f"side={side} entry={entry} close={close_price} amt={amount} "
                                f"pnl={pnl_liq:.4f} bal_before={balance:.4f} bal_after={new_balance:.4f}"
                            )

                            # DB truth → per_user sync용
                            liquidated_users[uid] = retri_id

                    except Exception:
                        if conn:
                            conn.rollback()
                        logger.exception(f"[CROSS-LIQ] failed persisting liquidation retri_id={retri_id} sym={sym}")
                        committed = False
                    finally:
                        if cursor:
                            try:
                                cursor.close()
                            except:
                                pass
                        if conn:
                            try:
                                conn.close()
                            except:
                                pass

                    # 6) Redis 반영 (DB 커밋 성공 시에만)
                    if committed:
                        # positions 제거
                        await position_redis.hdel(pos_key, sym)
                        # balance 반영
                        await position_redis.set(bal_key, new_balance)

                        # 소켓 알림
                        await self.send_redis_signal(
                            retri_id=retri_id,
                            payload={
                                "trigger": "liquidation_cross",
                                "pos": {**to_liquidate, "pnl": pnl_liq, "entry": entry, "side": side},
                            },
                        )

                        # liq_prices에서 제거
                        liq_prices = [x for x in liq_prices if int(x["pos_id"]) != pos_hist_id]

                        # available 재계산 (방금 청산된 심볼 제외)
                        positions_after = [pp for pp in positions if pp.get("symbol") != sym]
                        total_pos_after = sum(pp.get("margin", 0.0) for pp in positions_after)
                        total_upnl_after = sum(
                            pp.get("unrealized_pnl", 0.0)
                            for pp in positions_after
                            if pp.get("margin_type") == "cross"
                        )
                        available_to_write = max(new_balance - total_pos_after - total_ord + total_upnl_after, 0.0)

                # ----------------------------------------------------------
                # Redis에 liq_price 업데이트 (cross만)
                # ----------------------------------------------------------
                for lp_item in liq_prices:
                    sym2 = lp_item["symbol"]
                    new_lp = float(lp_item["liq_price"])

                    blob2 = await position_redis.hget(pos_key, sym2)
                    if not blob2:
                        continue

                    try:
                        data = json.loads(blob2)
                    except Exception:
                        continue

                    if data.get("margin_type") == "cross":
                        data["liq_price"] = new_lp
                        await position_redis.hset(pos_key, sym2, json.dumps(data))

                # ----------------------------------------------------------
                # available 최종 1회 SET
                # ----------------------------------------------------------
                await position_redis.set(avl_key, max(float(available_to_write), 0.0))

            except Exception:
                logger.exception("Failed calculating liq prices for user")
                continue

        # ----------------------------------------------------------
        # 청산 발생 유저는 MySQL -> Redis per-user 싱크
        # (order_history tp/sl 취소 반영, balance/position 정합화)
        # ----------------------------------------------------------
        for mysql_uid, retri_id in liquidated_users.items():
            try:
                await update_position_status_per_user(mysql_uid, retri_id)
                await update_order_status_per_user(mysql_uid, retri_id)
                await update_balance_status_per_user(mysql_uid, retri_id)
            except Exception:
                logger.exception(f"[CROSS-LIQ] per-user sync failed uid={mysql_uid} retri_id={retri_id}")


    async def settle_orders(self):
        logger.info(f"executing settle orders at {datetime.now(timezone('Asia/Seoul'))}")
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
            # balance_key = f"balances:{user_id}"
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
                side = order['side']
                uid = order['user_id']
                # 1) 가격 먼저 가져오기
                raw_price = await price_redis.get(f"price:{symbol}USDT")

                # 값이 아예 없으면 이 주문은 그냥 스킵
                if raw_price is None:
                    if symbol not in self.missing_symbols:
                        logger.warning(f"[LIMIT] No price for {symbol} — suppressing further warnings")
                        self.missing_symbols.add(symbol)
                    continue


                # 2) float 변환 시도
                try:
                    current_price = float(raw_price)
                except (TypeError, ValueError):
                    logger.error(
                        f"[LIMIT] Invalid price '{raw_price!r}' for symbol={symbol} "
                        f"(user={user_id}, order_id={order.get('id')}) – skipping this order"
                    )
                    continue

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
                updated_id = await self.persist_executed_limit_order(order, new_position, current_id)
                # Prepare WebSocket trigger
                # retri_id = order.get('retri_id') or user_id  # or from user db if needed
                if (updated_id):
                    pending_notifs.append((user_id, {"trigger": "limit", "order": order}))
                    updated_users[uid] = user_id

                    row_count += 1

                    break  # Settle only one order per user

        # Notify users
        for retri_id, message in pending_notifs:
            await self.send_redis_signal(retri_id, message)

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

            # balance_key = f"balances:{user_id}"
            pos_key = f"positions:{user_id}"

            # balance_raw = await position_redis.get(balance_key)
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

                current_position = positions.get(symbol) or {}
                # if not current_position:
                #     continue

                # Compute new position and persist
                new_position = calculate_new_position(current_position, order)

                current_id = current_position.get("pos_id")
                updated_id = await self.persist_triggered_tpsl_order(order, new_position, current_id)
                if updated_id:
                    uid = order.get('user_id')
                    pending_notifs.append((user_id, {"trigger": "tp/sl", "order": order}))
                    updated_users[uid] = user_id

                    row_count += 1

                    break

        for retri_id, message in pending_notifs:
            await self.send_redis_signal(retri_id, message)

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
                SELECT `status` FROM `mocktrade`.`order_history`
                WHERE `id` = %s
            """, (order_id, ))
            order_row = cursor.fetchone()
            if not order_row or order_row['status'] != 0:
                logger.warning(f"this limit order ({order_id}) has already been executed or doesn't exist. Check if the Redis synchronization is functioning properly.")
                return

            new_amount = float(new_position.get("amount") or 0)

            if new_position.get("status") == 1 and new_amount <= 0:
                raise ValueError(
                    "Refusing to persist open position with non-positive amount: "
                    f"user_id={new_position.get('user_id')}, "
                    f"symbol={new_position.get('symbol')}, "
                    f"amount={new_amount}"
                )

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
                       AND type IN ('tp', 'sl')
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
                       AND type IN ('tp', 'sl')
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
                       `price` = %s,
                       `update_time` = %s 
                 WHERE `id` = %s
            """, (current_id, order['price'], datetime.now(timezone('Asia/Seoul')), order_id))

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
                                    ,insert_time, update_time, tp, sl, or_id, order_price)
                                VALUES (
                                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s )
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
                        order.get('order_price', limit_tp)
                    ))
                    logger.info("successfully opened the take profit order from the triggered limit order")

                if limit_sl:
                    cursor.execute("""
                                INSERT INTO mocktrade.order_history (
                                    user_id, symbol, `type`, margin_type, magin, leverage, side, amount, status
                                    ,insert_time, update_time, sl, tp, or_id, order_price)
                                VALUES (
                                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s)
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
                        order.get('order_price', limit_sl)
                    ))
                    logger.info("successfully opened the stop loss order from the triggered limit order")

            conn.commit()
            return user_id
        except Exception:
            logger.exception(f"failed to persist limit order execution of order [{order.get('or_id')}] to MySQL")
            conn and conn.rollback()
            return None
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
            
            # safeguard on not to execute the same order multiple times
            cursor.execute("""
                SELECT `status` FROM `mocktrade`.`order_history`
                WHERE `id` = %s
            """, (order_id, ))
            order_row = cursor.fetchone()
            if not order_row or order_row['status'] != 0:
                logger.warning(f"this tp/sl order ({order_id}) has already executed or doesn't exist. Check if the Redis sync is properly functioning")
                return

            # 여기에 추가
            new_amount = float(new_position.get("amount") or 0)

            if new_position.get("status") == 1 and new_amount <= 0:
                raise ValueError(
                    "Refusing to persist open position with non-positive amount: "
                    f"user_id={new_position.get('user_id')}, "
                    f"symbol={new_position.get('symbol')}, "
                    f"amount={new_amount}"
                )


            cursor.execute("""
                UPDATE `mocktrade`.`position_history`
                   SET `status` = 2
                 WHERE `status` = 1
                   AND `user_id` = %s
                   AND `symbol` = %s
            """, (user_id, symbol))

            if new_position.get('status') == 'closed':  # invalid
                cursor.execute("""
                    UPDATE `mocktrade`.`order_history`
                       SET `status` = 4
                     WHERE `type` IN ('tp', 'sl')
                       AND `symbol` = %s
                       AND `user_id` = %s
                       AND `status` = 0
                """, (symbol, user_id))

            elif new_position.get('close'):  # position tp/sl or order tp/sl that closes everything
                if order.get('order_price', 0) == 0:
                    cursor.execute("""
                        UPDATE mocktrade.order_history
                           SET `magin` = %s,
                               `amount` = %s,
                               `po_id` = %s 
                         WHERE `id` = %s
                    """, (new_position.get('margin', 0), new_position.get('amount', 0), current_id, order_id))

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

                cursor.execute("""
                    UPDATE `mocktrade`.`order_history`
                       SET `status` = 1,
                           `price` = %s,
                           `update_time` = %s
                     WHERE `type` IN ('tp', 'sl')
                       AND `id` = %s
                       AND `user_id` = %s
                       AND `symbol` = %s                    
                """, (order.get('price', 0), datetime.now(timezone('Asia/Seoul')), order_id, user_id, symbol))

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

                # mark order settled
                cursor.execute("""
                    UPDATE `mocktrade`.`order_history`
                       SET `status` = 1,
                           `price` = %s,
                           `update_time` = %s
                     WHERE `id` = %s
                       AND `status` = 0
                """, (
                    order.get('price', 0),
                    datetime.now(timezone('Asia/Seoul')),
                    order_id
                ))

            close_pnl = new_position.get('close_pnl', 0)
            logger.info(f"applying close PnL of {close_pnl} to user [{user_id}]'s wallet")
            if close_pnl:
                cursor.execute("""
                    UPDATE mocktrade.user 
                       SET `balance` = GREATEST(`balance` + %s, 0)
                     WHERE `id` = %s
                       AND `status` = 0  
                """, (close_pnl, user_id))

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
                    AND `symbol` = %s
                    AND `status` = 0 
                """, (
                    order_id - 1,
                    user_id,
                    symbol
                ))

            conn.commit()
            return user_id
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
        #
        # position_redis = await self.get_position_redis()
        # if not position_redis:
        #     logger.warning("position redis yet to be initialized")
        #     return

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

                        # a) target_pos를 먼저 '청산'으로 바꿈 (열려 있을 때만)
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                               SET pnl = %s,
                                   close_price = %s,
                                   status = 3,
                                   `datetime` = %s
                             WHERE id = %s
                               AND status = 1
                        """, (close_pnl, market_price, datetime.now(timezone("Asia/Seoul")), pos_id))

                        if cursor.rowcount == 0:
                            logger.warning(f"position [{pos_id}] is not open. Skipping liquidation.")
                            continue

                        # a) mark previous positions as closed
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                               SET status = 2
                             WHERE status = 1
                               AND symbol = %s
                               AND user_id = %s
                               AND id <> %s 
                        """, (symbol, user_id, pos_id))


                        # b-1) insert liquidation order into order_history
                        liq_side = "sell" if side == "buy" else "buy"

                        # amount: 포지션 수량 (코인 수량이든 contracts든, 네 포지션 정의에 맞게)
                        liq_amount = float(pos.get("amount", 0) or 0)

                        # margin: 격리면 해당 포지션 증거금(=청산으로 날아간 금액)
                        liq_margin = float(pos.get("margin", 0) or 0)

                        # leverage: 포지션 레버리지 (없으면 0 또는 1)
                        liq_leverage = float(pos.get("leverage", 0) or 0)

                        now_kst = datetime.now(timezone("Asia/Seoul"))

                        cursor.execute("""
                                       INSERT INTO mocktrade.order_history (
                                           user_id,
                                           symbol,
                                           type,
                                           margin_type,
                                           side,
                                           price,
                                           magin,
                                           amount,
                                           leverage,
                                           status,
                                           insert_time,
                                           update_time,
                                           order_price,
                                           po_id,
                                           tp,
                                           sl,
                                           order_historycol,
                                           or_id
                                       ) VALUES (
                                                    %s,  -- user_id
                                                    %s,  -- symbol
                                                    %s,  -- type
                                                    %s,  -- margin_type
                                                    %s,  -- side
                                                    %s,  -- price (체결가/청산가)
                                                    %s,  -- magin (증거금)
                                                    %s,  -- amount
                                                    %s,  -- leverage
                                                    %s,  -- status
                                                    %s,  -- insert_time
                                                    %s,  -- update_time
                                                    %s,  -- order_price (주문가: 시장가면 체결가로 맞춤)
                                                    %s,  -- po_id (포지션 id 연결: pos_id 사용)
                                                    %s,  -- tp
                                                    %s,  -- sl
                                                    %s,  -- order_historycol (모르면 NULL)
                                                    %s   -- or_id (모르면 NULL)
                                                )
                                       """, (
                                           user_id,
                                           symbol,
                                           "liquidation",
                                           "isolated",
                                           liq_side,
                                           float(market_price),  # price
                                           liq_margin,           # magin (컬럼명이 magin인 경우)
                                           liq_amount,
                                           liq_leverage,
                                           1,                 # status: 네 프로젝트에서 "filled/closed"가 몇 번인지에 맞춰 수정
                                           now_kst,
                                           now_kst,
                                           float(market_price),  # order_price: 시장가면 보통 체결가로 기록
                                           pos_id,            # po_id: 너가 이미 pos_id 가지고 있음
                                           None,              # tp
                                           None,              # sl
                                           None,              # order_historycol
                                           None               # or_id
                                       ))

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

                        await position_redis.hdel(key, symbol)

                        logger.info(f"isolated position [{pos_id}] liquidated")
                        break

                    except Exception:
                        cursor.execute("ROLLBACK TO SAVEPOINT lq_order")
                        cursor.execute("RELEASE SAVEPOINT lq_order")
                        logger.exception(f"Failed to liquidate Redis pos_id [{pos_id}]")
                        continue

            conn.commit()

            for retri_id, message in pending_notifs:
                await self.send_redis_signal(retri_id, message)

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


