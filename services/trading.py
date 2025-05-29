import asyncio

from utils.connections import MySQLAdapter
from starlette.config import Config
import traceback
from datetime import datetime, timedelta
from pytz import timezone
import redis.asyncio as aioredis
from collections import defaultdict
import logging

from utils.symbols import symbols as SYMBOL_CFG
from utils.connection_manager import manager
from utils.local_redis import update_position_status_per_user, update_order_status_per_user, update_balance_status_per_user
config = Config('.env')
redis_client = aioredis.Redis(
    host=config.get('REDIS_HOST'), port=6379, db=0, decode_responses=True
)
logger = logging.getLogger(__name__)

FEE_RATE = 0.0002  #0.02%


def calculate_new_position(current_position, order):
    # logger.info(f"applying order_id of {order['id']}")
    user_id = order['user_id']
    symbol = order['symbol']
    side = order['side']  # side of the TP/SL order: 'buy' meaning closing a short
    amount = float(order['amount'])
    price = float(order['price'])  # this is the exit_price
    leverage = float(order['leverage'])
    margin_type = order['margin_type']
    from_order = bool(order['from_order'])
    logger.info(f"from order {from_order} (tpsl order id = {order['id']}")

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
    cur_size = float(current_position['size'])
    cur_lev = float(current_position['leverage'])

    # # 2) Liquidation check
    # liq_price = float(current_position['liq_price'])
    # if (cs == 'buy' and price <= liq_price) or (cs == 'sell' and price >= liq_price):
    #     # forced liquidation
    #     return {
    #         "user_id": user_id,
    #         "symbol": symbol,
    #         "amount": 0,
    #         "entry_price": None,
    #         "size": 0,
    #         "margin": 0,
    #         "leverage": cur_lev,
    #         "side": cs,
    #         "margin_type": margin_type,
    #         "pnl": -cur_margin,
    #         "status": 4,
    #         "liq_price": None,
    #         "close_price": price,
    #         "close": True
    #     }

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
            "leverage": 0,
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
    leverage = float(order['leverage'])
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

    # ci = float(current_position['liq_price'])
    # cs = current_position['side']
    # if (cs == 'buy' and price <= ci) or (cs == 'sell' and price >= ci):
    #     # return a force liquidation result immediately
    #     return {
    #         "user_id": current_position['user_id'],
    #         "symbol": current_position['symbol'],
    #         "amount": 0,
    #         "entry_price": None,
    #         "size": 0,
    #         "margin": 0,
    #         "leverage": float(current_position['leverage']),
    #         "side": cs,
    #         "margin_type": current_position['margin_type'],
    #         "pnl": -float(current_position['margin']),
    #         "status": 4,
    #         "liq_price": None,
    #         "close_price": price,
    #         "close": True
    #     }

    # Existing position details
    current_side = current_position['side']
    current_amount = float(current_position['amount'])
    current_entry_price = float(current_position['entry_price'])
    current_margin = float(current_position['margin'])
    current_size = float(current_position['size'])
    current_pnl = float(current_position.get('pnl') or 0)
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
        leverage = max(leverage, current_position.get('leverage', 0))
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
        new_pnl = close_pnl
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


def calc_iso_liq_price(entry_price: float,
                       leverage: float,
                       side: str) -> float | None:
    if side == 'buy':  # LONG
        return entry_price * (1 - 1 / leverage)
    else:  # SHORT
        return entry_price * (1 + 1 / leverage)


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


def compute_cross_liq_price(
        entry_price: float,
        amount: float,
        buffer: float,
        side: str
) -> float:

    # guard against zero-size
    if amount == 0:
        return 0.0

    maintenance_rate = 0.01
    # 3) Solve for the price at which equity == 0
    if side == 'buy':
        # long -> liquidate when price falls to this level
        # return max((entry_price - buffer / amount) / (1 - maintenance_rate), 0.0)
        return max(
            entry_price * (1 + 0.01) - buffer / amount,
            0.0
        )

    else:
        # short -> liquidate when price rises to this level
        # return max((entry_price + buffer / amount) / ( 1 + maintenance_rate), 0.0)
        return max(
            entry_price * (1 - 0.01) + buffer / amount,
            0.0
        )


def should_liquidate(
        side: str,
        current_price: float,
        liq_price: float) -> bool:
    """
    Check whether a position has crossed its liquidation threshold.
    :param side: 'buy' for long 'sell' for short
    :param current_price: the latest market price
    :param liq_price: the computed liquidation price
    :return: True if the position should be liquidated now
    """

    if current_price is None:
        return False

    if side == 'buy':
        # long -> liquidate when market <= liq_price
        return current_price <= liq_price
    else:
        # short -> liquidate when market >= liq_price
        return current_price >= liq_price


class TradingService(MySQLAdapter):

    async def settle_limit_orders(self):
        conn = None
        cursor = None
        row_count = 0

        # 1) prepare a place to stash pending notifications
        pending_notifs: list[tuple[str, dict]] = []
        updated_users = {}

        try:
            conn = self._get_connection()
            conn.autocommit(False)
            cursor = conn.cursor()

            # # 1) Load prices & open orders
            # cursor.execute("SELECT symbol, price FROM mocktrade.prices")
            # price_dict = {r['symbol']: r['price'] for r in cursor.fetchall()}

            cursor.execute("""
                SELECT * FROM mocktrade.order_history
                 WHERE `type`= 'limit'
                   AND `status`= 0
                   AND `amount` > 0
                ORDER BY `id`
            """)
            open_orders = cursor.fetchall()

            if not open_orders:
                conn.rollback()
                return 0  # or return a message/count

            for order in open_orders:
                cursor.execute("SAVEPOINT tp_order")
                try:
                    symbol = order["symbol"]
                    order_price = order["price"]
                    current_price = float(await redis_client.get(f"price:{symbol}USDT"))
                    side = order["side"]
                    order_id = order["id"]
                    user_id = order["user_id"]

                    # only process fills
                    if current_price is None:
                        continue
                    if (side == 'buy' and order_price < current_price) or (side == 'sell' and order_price > current_price):
                        continue

                    # 1) apply execution-price & recompute margin
                    exec_price = current_price
                    exec_amount = float(order['amount'])
                    leverage = float(order['leverage'])
                    exec_margin = (exec_price * exec_amount) / leverage

                    # 1-1) update the in-memory dict so calculate_positions sees real values
                    order['price'] = exec_price
                    order['margin'] = exec_margin

                    # 1-2) persist those execution values back to the order_history row
                    cursor.execute("""
                        UPDATE mocktrade.order_history
                           SET price = %s,
                               magin = %s,
                               update_time = %s
                         WHERE id = %s
                    """, (
                        exec_price, exec_margin, datetime.now(timezone("Asia/Seoul")), order_id
                    ))

                    # 2) Read balance & position
                    cursor.execute("SELECT balance, retri_id FROM `mocktrade`.`user` WHERE `id`= %s AND status = 0", (user_id,))
                    user_row = cursor.fetchone()
                    wallet_balance = user_row.get("balance", 0)
                    retri_id = user_row.get("retri_id")


                    cursor.execute("""
                      SELECT * FROM mocktrade.position_history
                       WHERE user_id = %s AND symbol = %s AND status = 1
                       ORDER BY `id` DESC LIMIT 1
                       FOR UPDATE
                    """, (user_id, symbol))
                    current_position = cursor.fetchone()

                    # 3) Compute new_position
                    new_position = calculate_position(current_position, order)

                    if current_position:
                        pos_id = current_position.get('id')
                    else:
                        pos_id = None

                    # 4) Persist new position ( + close old ones )
                    cursor.execute("""
                        UPDATE mocktrade.position_history SET status = 2
                        WHERE `status` = 1 AND `user_id` = %s AND `symbol` = %s
                    """, (user_id, symbol))

                    if new_position.get('close'): # full close
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                               SET `status` = 3,
                                   `pnl` = %s,
                                   `datetime` = %s,
                                   `close_price` = %s
                             WHERE `id` = %s
                        """, (
                            new_position.get('close_pnl', 0),
                            datetime.now(timezone('Asia/Seoul')),
                            new_position.get('close_price', 0),
                            pos_id
                        ))
                    elif new_position.get('flip'):  # position flip
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                            SET `status` = 3,
                                `pnl` = %s,
                                `datetime` = %s,
                                `close_price` = %s
                            WHERE `id` = %s 
                        """, (
                            new_position.get('close_pnl', 0),
                            datetime.now(timezone('Asia/Seoul')),
                            new_position.get('close_price', 0),
                            pos_id
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

                    elif new_position.get('partial'):  # partial close
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                               SET pnl = %s,
                                   `datetime` = %s,
                                   `close_price` = %s
                             WHERE `id` = %s
                        """, (
                            new_position.get('close_pnl', 0),
                            datetime.now(timezone('Asia/Seoul')),
                            new_position.get('close_price'),
                            pos_id
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

                    # 5) Update wallet for any realized PnL
                    close_pnl = new_position.get('close_pnl', 0)
                    # if close_pnl:
                    #     new_bal = wallet_balance + close_pnl
                    #     if new_bal < 0:
                    #         raise RuntimeError("Balance negative")
                    #     cursor.execute(
                    #         "UPDATE mocktrade.user SET balance = %s WHERE id = %s",
                    #         (new_bal, user_id)
                    #     )
                    if close_pnl:
                        new_bal = max(wallet_balance + close_pnl, 0)
                        cursor.execute(
                            "UPDATE mocktrade.user SET balance = %s WHERE `id` = %s",
                            (new_bal, user_id)
                        )


                    # 6) Mark order settled
                    cursor.execute("""
                        UPDATE mocktrade.order_history
                        SET status = 1,
                            po_id = %s,
                            update_time = %s
                        WHERE `id` = %s
                    """, (pos_id, datetime.now(timezone('Asia/Seoul')), order_id,))

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
                        margin = order['magin']

                        order_side = 'sell' if limit_side == 'buy' else 'buy'

                        if limit_tp:
                            cursor.execute("""
                                INSERT INTO mocktrade.order_history (
                                    user_id, symbol, `type`, margin_type, magin, leverage, side, amount, status
                                    ,insert_time, update_time, tp, or_id )
                                VALUES (
                                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s )
                            """, (
                                user_id,
                                symbol,
                                'tp',
                                margin_type,
                                exec_margin,
                                leverage,
                                order_side,
                                limit_amount,
                                0,
                                datetime.now(timezone("Asia/Seoul")),
                                datetime.now(timezone("Asia/Seoul")),
                                limit_tp,
                                order_id
                            ))
                            logger.info("successfully opened the take profit order from the triggered limit order")

                        if limit_sl:
                            cursor.execute("""
                                INSERT INTO mocktrade.order_history (
                                    user_id, symbol, `type`, margin_type, magin, leverage, side, amount, status
                                    ,insert_time, update_time, sl, or_id )
                                VALUES (
                                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s )
                            """, (
                                user_id,
                                symbol,
                                'sl',
                                margin_type,
                                exec_margin,
                                leverage,
                                order_side,
                                limit_amount,
                                0,
                                datetime.now(timezone("Asia/Seoul")),
                                datetime.now(timezone("Asia/Seoul")),
                                limit_sl,
                                order_id
                            ))
                            logger.info("successfully opened the stop loss order from the triggered limit order")
                    pending_notifs.append((
                        retri_id,
                        {"trigger": "limit", "order": order}
                    ))

                    updated_users[user_id] = retri_id

                    cursor.execute("RELEASE SAVEPOINT tp_order")
                    row_count += 1

                except Exception:
                    cursor.execute("ROLLBACK TO SAVEPOINT tp_order")
                    cursor.execute("RELEASE SAVEPOINT tp_order")
                    logger.exception(f"failed processing order {order['id']}, skipping to next")
                    continue

            conn.commit()

            # fire all websockets in one batch
            # logger.info(f"pending notifs: {pending_notifs}")
            if pending_notifs:
                for retri_id, message in pending_notifs:
                    # schedule on the same loop
                    await asyncio.create_task(
                        manager.notify_user(retri_id, message)
                    )
            if updated_users:
                logger.info(f"updated_users: {updated_users}")
                for user_id, retri_id in updated_users.items():
                    await update_position_status_per_user(user_id, retri_id)
                    await update_order_status_per_user(user_id, retri_id)
                    await update_balance_status_per_user(user_id)
            return row_count

        except Exception:
            conn.rollback()
            logger.exception("failed to settle limit orders")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    async def settle_tpsl_orders(self):
        conn = None
        cursor = None
        row_count = 0

        pending_notifs : list[tuple[str, dict]] = []
        updated_users = {}

        try:
            conn = self._get_connection()
            conn.autocommit(False)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT 
                    *,
                    IF (oh.type = 'tp', oh.tp, oh.sl) AS exit_price,
                    CASE
                        WHEN oh.or_id IS NOT NULL
                          OR (oh.order_price IS NOT NULL AND oh.order_price <> 0)
                        THEN TRUE
                        ELSE FALSE
                    END AS from_order
                FROM mocktrade.order_history AS oh
                WHERE oh.status = 0 AND oh.amount > 0
                AND oh.type in ('tp', 'sl')
            """)

            open_tpsl_orders = cursor.fetchall()
            if not open_tpsl_orders:
                conn.rollback()
                return 0

            for order in open_tpsl_orders:
                cursor.execute("SAVEPOINT sp_order")
                try:
                    order_id = order['id']
                    user_id = order['user_id']
                    symbol = order['symbol']
                    exit_price = order['exit_price']
                    from_order = order['from_order']
                    amount = order['amount']
                    leverage = order['leverage']
                    side = order['side']
                    order_type = order['type']

                    current_price = float(await redis_client.get(f"price:{symbol}USDT"))

                    if current_price is None:
                        continue

                    should_settle = False

                    if order_type == 'tp':
                        # take profit
                        if (side == 'sell' and current_price >= exit_price) or (
                                side == 'buy' and current_price <= exit_price):
                            should_settle = True
                    else:
                        # stop loss
                        if (side == 'sell' and current_price <= exit_price) or (
                                side == 'buy' and current_price >= exit_price):
                            should_settle = True

                    if not should_settle:
                        continue

                    if side == 'sell':
                        exec_price = max(current_price, exit_price)
                    else:
                        exec_price = min(current_price, exit_price)

                    # exec_price = current_price
                    exec_amount = float(amount)
                    # exec_margin = (exec_price * exec_amount) / leverage

                    # 1) Persist execution values back to the order_history table
                    order['price'] = exec_price
                    # order['margin'] = exec_margin

                    cursor.execute("""
                        UPDATE mocktrade.order_history
                        SET price = %s, 
                            update_time = %s
                        WHERE id = %s
                    """, (exec_price, datetime.now(timezone('Asia/Seoul')), order_id))

                    # 2) Read balance and position
                    cursor.execute("""
                        SELECT balance, retri_id FROM `mocktrade`.`user`
                        WHERE id = %s
                        AND status = 0
                    """, (user_id,))
                    user_row = cursor.fetchone()
                    wallet_balance = user_row.get('balance', 0)
                    retri_id = user_row.get('retri_id')

                    cursor.execute("""
                        SELECT * FROM mocktrade.position_history
                        WHERE symbol = %s
                        AND user_id = %s
                        AND status = 1
                        ORDER BY `id` DESC
                        LIMIT 1
                        FOR UPDATE
                    """, (symbol, user_id))
                    current_position = cursor.fetchone()

                    if current_position['status'] in (3, 4) or not current_position:
                        continue

                    # 3) Compute new position status
                    new_position = calculate_new_position(current_position, order)

                    # 4) Persist new position
                    cursor.execute("""
                        UPDATE mocktrade.position_history
                        SET status = 2
                        WHERE status = 1
                        AND user_id = %s
                        AND symbol = %s                
                    """, (user_id, symbol))

                    if new_position.get('close'): # position tp/sl
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                               SET pnl = %s,
                                   close_price = %s,
                                   status = 3,
                                   `datetime` = %s
                             WHERE `id` = %s   
                        """, (
                            new_position.get('close_pnl', 0),
                            new_position.get('close_price', 0),
                            datetime.now(timezone('Asia/Seoul')),
                            current_position.get('id')
                        ))
                    else: # partial close (order tp/sl)
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                               SET pnl = %s,
                                   close_price = %s,
                                   status = 2,
                                   `datetime` = %s
                             WHERE `id` = %s
                        """, (
                            new_position.get('close_pnl', 0),
                            new_position.get('close_price', 0),
                            datetime.now(timezone('Asia/Seoul')),
                            current_position.get('id')
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
                            new_position.get('side'), current_position.get('leverage', 0), new_position.get('status'),
                            new_position.get('tp', 0), new_position.get('sl', 0), datetime.now(timezone("Asia/Seoul")),
                            0
                        ))

                    # 5) Update wallet for any realized pnl
                    close_pnl = new_position.get('close_pnl', 0)
                    if close_pnl:
                        new_bal = max(wallet_balance + close_pnl, 0)
                        cursor.execute(
                            "UPDATE mocktrade.user SET balance = %s WHERE `id` = %s",
                            (new_bal, user_id)
                        )

                    # 6) Mark this order settled
                    cursor.execute("""
                        UPDATE mocktrade.order_history
                        SET status = 1,
                            `update_time` = %s,
                            `po_id` = %s
                        WHERE id = %s
                    """, (datetime.now(timezone('Asia/Seoul')), current_position.get('id'), order_id,))

                    # mark its pair as closed
                    if from_order:
                        if order['or_id'] is not None:
                            cursor.execute("""
                                UPDATE mocktrade.order_history
                                SET status = 4
                                WHERE or_id = %s AND `id` <> %s
                            """, (order['or_id'], order['id']))

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

                    # 7) close relevant tp/sl positions
                    if new_position.get('close'):
                        cursor.execute("""
                            UPDATE mocktrade.order_history
                            SET status = 4
                            WHERE `type` IN ('tp', 'sl')
                            AND `symbol` = %s
                            AND `user_id` = %s
                            AND status = 0
                        """, (symbol, user_id))

                    pending_notifs.append((
                        retri_id,
                        { "trigger": "tp/sl", "order" : order}
                    ))
                    updated_users[user_id] = retri_id

                    cursor.execute("RELEASE SAVEPOINT sp_order")
                    row_count += 1
                except Exception:
                    cursor.execute("ROLLBACK TO SAVEPOINT sp_order")
                    cursor.execute("RELEASE SAVEPOINT sp_order")
                    logger.exception(f"failed to execute tp/sl order of [{order['id']}]")
                    continue

            conn.commit()
            if pending_notifs:
                for user_id, message in pending_notifs:
                    await asyncio.create_task(
                        manager.notify_user(user_id, message)
                    )
            if updated_users:
                for user_id, retri_id in updated_users.items():
                    await update_position_status_per_user(user_id, retri_id)
                    await update_order_status_per_user(user_id, retri_id)
                    await update_balance_status_per_user(user_id)

            return row_count

        except Exception:
            if conn:
                conn.rollback()
            logger.exception("Failed to apply valid tp/sl orders")
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

    async def close_position(self, retri_id, symbol):
        # closed_users = {}
        try:
            with self._get_connection() as conn, conn.cursor() as cursor:

                # 1. get the actual user_id
                cursor.execute("""
                    SELECT `id` FROM `mocktrade`.`user`
                    WHERE `retri_id` = %s
                    ORDER BY `id`
                    LIMIT 1
                """, (retri_id,))
                row = cursor.fetchone()
                if not row:
                    return {"error": "could not find a user with that retri_id"}
                user_id = row['id']

                if not user_id:
                    print("could not find a user with the retri_id")
                    return {"error": "could not find a user with the retri_id"}
                logger.info(f"user_id: {user_id}")

                # 1. Get the active position
                find_sql = """
                    SELECT * FROM mocktrade.position_history
                    WHERE user_id = %s
                    AND symbol = %s
                    AND status = 1
                    ORDER BY `datetime` DESC
                    LIMIT 1
                """
                cursor.execute(find_sql, (user_id, symbol))
                find_result = cursor.fetchone()

                if not find_result:
                    return {"error": "No active position to close"}

                position_id = find_result["id"]
                entry_price = float(find_result["entry_price"])
                amount = float(find_result["amount"])
                side = find_result["side"]
                # margin = float(find_result['margin'])
                # margin_type = find_result['margin_type']

                current_price = float(await redis_client.get(f"price:{symbol}USDT"))
                if current_price is None:
                    logger.error(f"could not find the value of {symbol}")
                    raise RuntimeError(f"Could not find the value of {symbol}")

                # 3. calculate pnl
                if side == 'buy':
                    raw_pnl = (current_price - entry_price) * amount
                elif side == 'sell':
                    raw_pnl = (entry_price - current_price) * amount

                # 4. Mark position as closed (insert new position)
                cursor.execute("""
                    UPDATE mocktrade.position_history
                    SET status = 2 
                    WHERE status = 1
                    AND user_id = %s
                    AND symbol = %s
                """, (user_id, symbol))

                cursor.execute("""
                    UPDATE mocktrade.position_history
                       SET status = 3,
                           pnl = %s,
                           `datetime` = %s,
                           close_price = %s 
                     WHERE `id` = %s
                """, (
                    raw_pnl, datetime.now(timezone("Asia/Seoul")), current_price, position_id
                ))

                update_sql = """
                    UPDATE mocktrade.order_history 
                    SET `status` = 4
                    WHERE `type` IN ('tp', 'sl')
                    AND `user_id` = %s
                    AND `symbol` = %s
                    AND `status` = 0
                """

                cursor.execute(update_sql, (user_id, symbol))

                # 5. Update user's balance
                balance_sql = """
                    SELECT balance FROM mocktrade.user
                    WHERE id = %s
                """
                cursor.execute(balance_sql, (user_id,))
                balance_result = cursor.fetchone()
                current_balance = float(balance_result["balance"])
                new_balance = current_balance + raw_pnl

                update_balance_sql = """
                    UPDATE mocktrade.user 
                    SET balance = %s
                    WHERE id = %s
                """
                cursor.execute(update_balance_sql, (new_balance, user_id))

                conn.commit()

            await update_position_status_per_user(user_id, retri_id)
            await update_order_status_per_user(user_id, retri_id)
            await update_balance_status_per_user(user_id, retri_id)

            return {
                "message": "Position closed with PnL calculation",
                "derived_pnl": raw_pnl,
                "updated_balance": new_balance,
                "user_id": user_id,
                "symbol": symbol,
                "side": side,
                "current_price": current_price,
                "entry_price": entry_price
            }

        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return {"error": f"Failed to close position: {str(e)}"}

    async def liquidate_positions(self):
        conn = None
        cursor = None
        liquidated = 0

        pending_notifs: list[tuple[str, dict]] = []
        liquidated_users = {}

        try:
            conn = self._get_connection()
            conn.autocommit(False)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT * 
                  FROM mocktrade.position_history
                 WHERE liq_price IS NOT NULL
                   AND status = 1
                   AND margin_type = 'isolated'
            """)

            # logic to check the side(buy or sell), liq_price, and the current price of symbol and execute the liquidation when condition is met
            active_positions = cursor.fetchall()
            #3) Iterate and liquidate if necessary
            for pos in active_positions:
                cursor.execute("SAVEPOINT lq_order")
                try:
                    symbol = pos['symbol']
                    side = pos['side']
                    liq_price = float(pos['liq_price'])
                    current_price = float(await redis_client.get(f"price:{symbol}USDT"))

                    if current_price is None:
                        continue

                    # check liquidation condition
                    if (side == 'buy' and current_price <= liq_price) or (side == 'sell' and current_price >= liq_price):
                        user_id = pos['user_id']
                        pos_id = pos['id']
                        margin = float(pos['margin'])
                        close_pnl = -margin  # isolated -> full margin loss

                        # a) mark the old position snapshots as "closed"
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                            SET status = 2
                            WHERE status = 1
                            AND symbol = %s
                            AND user_id = %s
                        """, (symbol, user_id))

                        # b) update position status to liquidated
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                               SET pnl = %s,
                                   close_price = %s,
                                   status = 3,
                                   `datetime` = %s
                             WHERE `id` = %s
                        """, (
                            close_pnl, liq_price, datetime.now(timezone('Asia/Seoul')), pos_id
                        ))

                        # c) debit the user's wallet by the lost margin
                        cursor.execute("""
                            UPDATE `mocktrade`.`user`
                            SET balance = 
                                CASE 
                                    WHEN balance + %s < 0 THEN 0
                                    ELSE balance + %s
                                END
                            WHERE id = %s
                            AND status = 0    
                        """, (close_pnl, close_pnl, user_id))

                        # d) cancel any TP/SL orders for this user + symbol
                        cursor.execute("""
                            UPDATE mocktrade.order_history
                            SET status = 4
                            WHERE user_id = %s
                            AND symbol = %s
                            AND `type` IN ('tp', 'sl')    
                        """, (user_id, symbol))

                        # e) get the retri_id of user
                        cursor.execute("""
                            SELECT retri_id FROM mocktrade.user
                            WHERE `id` = %s
                            AND `status` = 0
                        """, (user_id,))

                        retri_id = cursor.fetchone()['retri_id']

                        pending_notifs.append((
                            retri_id,
                            {"trigger": "liquidation_isolated", "position": pos}
                        ))
                        liquidated_users[user_id] = retri_id
                        liquidated += 1

                        cursor.execute("RELEASE SAVEPOINT lq_order")
                except Exception:
                    cursor.execute("ROLLBACK TO SAVEPOINT lq_order")
                    cursor.execute("RELEASE SAVEPOINT lq_order")
                    logger.exception(f"failed to liquidate position with id of [{pos['id']}]")

            conn.commit()

            for retri_id, message in pending_notifs:
                # schedule on the same loop
                await asyncio.create_task(
                    manager.notify_user(retri_id, message)
                )
            for user_id, retri_id in liquidated_users.items():
                await update_position_status_per_user(user_id, retri_id)
                await update_order_status_per_user(user_id, retri_id)
                await update_balance_status_per_user(user_id, retri_id)

            return liquidated

        except Exception as e:
            if conn:
                conn.rollback()
            print(str(e))
            traceback.print_exc()
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    async def calculate_cross_positions(self):
        conn = None
        cursor = None
        row_count = 0
        pending_notifs: list[tuple[str, dict]] = []
        try:
            conn = self._get_connection()
            conn.autocommit(False)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT * FROM mocktrade.position_history
                WHERE status = 1
                ORDER BY `id`
            """)
            rows = cursor.fetchall()

            # group by user_id
            positions_by_user = defaultdict(list)
            for row in rows:
                positions_by_user[row['user_id']].append(row)

            # iterate through each user_id bucket
            for user_id, positions in positions_by_user.items():
                cursor.execute("SAVEPOINT lqc_calc")
                try:
                    cursor.execute("""
                        SELECT retri_id, balance 
                          FROM mocktrade.user
                         WHERE id = %s
                           AND status = 0
                    """, (user_id,))

                    user_row = cursor.fetchone()
                    wallet_balance = user_row.get('balance', 0)
                    retri_id = user_row.get('retri_id')
                    # logger.info(f"wallet balance of user {user_id}: {wallet_balance}")

                    # frozen margin from isolated positions
                    iso_initial = sum(p['margin'] for p in positions if p['margin_type'] == 'isolated')
                    # logger.info(f"iso_initial of user {user_id}: {iso_initial}")

                    # frozen margin from isolated orders
                    cursor.execute("""
                        SELECT COALESCE(SUM(magin), 0) AS frozen 
                          FROM mocktrade.order_history
                         WHERE user_id = %s
                           AND status = 0
                           AND margin_type = 'isolated' 
                           AND `type` IN ('market', 'limit') 
                    """, (user_id,))
                    order_margin = cursor.fetchone().get('frozen', 0)
                    # logger.info(f"order_margin of user {user_id}: {order_margin}")

                    cross_equity = wallet_balance - iso_initial - order_margin
                    cross_positions = [p for p in positions if p['margin_type'] == 'cross']
                    # logger.info(f"cross positions of user {user_id}: {cross_positions}")

                    updated_positions = []
                    # 청산 대상 탐색
                    for pos in cross_positions:
                        maint_other = sum((p['entry_price'] or 0) * (p['amount'] or 0) * 0.01 for p in cross_positions if p is not pos)
                        unrealized_pnl = sum((p['unrealized_pnl'] or 0) for p in cross_positions if p is not pos)
                        buffer_for_pos = cross_equity - maint_other + unrealized_pnl
                        new_liq = compute_cross_liq_price(
                            entry_price=pos['entry_price'],
                            amount=pos['amount'],
                            buffer=buffer_for_pos,
                            side=pos['side']
                        )
                        # logger.info(f"maint_other: {maint_other}, unrealized_pnl: {unrealized_pnl}, buffer_for_pos: {buffer_for_pos}, new_liq: {new_liq}")
                        prec = SYMBOL_CFG.get(pos['symbol'], {"price": 2, "qty": 3})
                        PRICE_DP = prec["price"]
                        new_liq = round(new_liq, PRICE_DP)

                        # current_price = price_cache.get(pos['symbol'])
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                               SET liq_price = %s
                             WHERE `id` = %s
                        """, (new_liq, pos['id'], ))

                        row_count += 1

                        # collect for WS
                        updated_positions.append({
                            "pos_id": pos["id"],
                            "symbol": pos["symbol"],
                            "price": new_liq
                        })
                    ws_msg = {
                        "liquidation": "liquidation_price",
                        "positions": updated_positions
                    }
                    pending_notifs.append((
                        retri_id,
                        ws_msg
                    ))

                    cursor.execute("RELEASE SAVEPOINT lqc_calc")

                except Exception:
                    cursor.execute("ROLLBACK TO SAVEPOINT lqc_calc")
                    cursor.execute("RELEASE SAVEPOINT lqc_calc")
                    logger.exception(f"failed to calculate the liquidation price of user with id of [{user_id}]")
                    continue

            conn.commit()
            for user_id, messages in pending_notifs:
                if len(messages.get('positions')) > 0:
                    await asyncio.create_task(
                        manager.notify_user(user_id, messages)
                    )

            return { "row_count": row_count }

        except Exception:
            logger.exception("Failed to calculate cross_positions")
            if conn:
                conn.rollback()
            return { "row_count" : 0 }
        finally:
            if cursor:
                try: cursor.close()
                except: pass
            if conn:
                try: conn.close()
                except: pass

    async def liquidate_cross_positions(self):
        conn = None
        cursor = None
        pending_notifs : list[tuple[str, dict]] = []
        liq_count = 0
        liquidated_users = {}

        try:
            conn = self._get_connection()
            conn.autocommit(False)
            cursor = conn.cursor()

            # 1) grab all active cross-margin positions
            cursor.execute("""
                SELECT * 
                  FROM mocktrade.position_history
                 WHERE status = 1
                   AND margin_type = 'cross'
              ORDER BY `id`
            """)
            rows = cursor.fetchall()
            positions_by_user = defaultdict(list)
            for r in rows:
                positions_by_user[r['user_id']].append(r)

            for user_id, positions in positions_by_user.items():
                breaches = []
                for pos in positions:
                    unrealized_pnl = pos['unrealized_pnl']
                    symbol = pos['symbol']
                    price = float(await redis_client.get(f"price:{symbol}USDT"))

                    if price is None:
                        continue

                    if unrealized_pnl < 0 and should_liquidate(pos['side'], price, pos['liq_price']):
                        breaches.append((unrealized_pnl, pos, price))

                if not breaches:
                    continue

                # sort so the largest relative breach first
                breaches.sort(key=lambda x: x[0])
                worst_pos = breaches[0][1]

                cursor.execute("SAVEPOINT lqc_exec")
                try:
                    symbol = worst_pos['symbol']
                    entry_price = worst_pos['entry_price']
                    amount = worst_pos['amount']
                    liq_price = worst_pos['liq_price']
                    side = worst_pos['side']
                    pos_id = worst_pos['id']
                    user_id = worst_pos['user_id']

                    cursor.execute("""
                            UPDATE mocktrade.position_history
                               SET status = 2
                             WHERE symbol = %s
                               AND user_id = %s
                               AND status = 1
                        """, (symbol, user_id))

                    pnl_liq = ((liq_price - entry_price) if side == 'buy' else (entry_price - liq_price)) * amount

                    cursor.execute("""
                            UPDATE mocktrade.position_history
                               SET pnl = %s,
                                   status = 3,
                                   `datetime` = %s,
                                   close_price = %s
                             WHERE id = %s
                        """, (pnl_liq, datetime.now(timezone('Asia/Seoul')), liq_price, pos_id))

                    cursor.execute("""
                            UPDATE mocktrade.user
                            SET balance = 
                                CASE 
                                    WHEN balance + %s < 0 THEN 0
                                    ELSE balance + %s
                                END
                                , `datetime` = %s
                            WHERE id = %s
                        """, (pnl_liq, pnl_liq, datetime.now(timezone('Asia/Seoul')), user_id))

                    cursor.execute("""
                            SELECT retri_id FROM mocktrade.user
                            WHERE id = %s
                            AND status = 0
                        """, (user_id,))
                    retri_id = cursor.fetchone().get('retri_id')
                    pending_notifs.append((
                        retri_id,
                        { "trigger": "liquidation_cross", "pos": worst_pos }
                    ))

                    liq_count += 1
                    liquidated_users[user_id] = retri_id

                    cursor.execute("RELEASE SAVEPOINT lqc_exec")

                except Exception:
                    cursor.execute("ROLLBACK TO SAVEPOINT lqc_exec")
                    cursor.execute("RELEASE SAVEPOINT lqc_exec")
                    logger.exception(f"failed to execute liquidate cross on position with id of [{worst_pos['id']}]")
                    continue

            conn.commit()
            for retri_id, message in pending_notifs:
                await asyncio.create_task(
                    manager.notify_user(retri_id, message)
                )
            for user_id, retri_id in liquidated_users.items():
                await update_position_status_per_user(user_id, retri_id)
                await update_order_status_per_user(user_id, retri_id)
                await update_balance_status_per_user(user_id, retri_id)

            return {
                "liq_count" : liq_count
            }

        except Exception:
            logger.exception("Failed to calculate cross_positions")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                try: cursor.close()
                except: pass
            if conn:
                try: conn.close()
                except: pass

    async def calculate_unrealized_pnl(self):
        conn = None
        cursor = None
        row_count = 0
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # # 1) load market prices
            # cursor.execute("SELECT symbol, price FROM mocktrade.prices")
            # price_dict = {r['symbol']: r['price'] for r in cursor.fetchall()}

            # 2) load only the fields we need for active positions
            cursor.execute("""
                SELECT
                  id,
                  user_id,
                  symbol,
                  entry_price,
                  amount,
                  side,
                  leverage,
                  margin
                FROM mocktrade.position_history
                WHERE status = 1
            """)
            active_positions = cursor.fetchall()

            # 3) loop & compute
            for pos in active_positions:
                pos_id        = pos['id']
                user_id       = pos['user_id']
                symbol        = pos['symbol']
                entry_price   = float(pos['entry_price'])
                amount        = float(pos['amount'])
                side          = pos['side']
                original_margin = float(pos['margin'])

                current_price = float(await redis_client.get(f"price:{symbol}USDT"))
                if current_price is None:
                    print(f"[WARN] no price for symbol {symbol}, skipping")
                    continue

                # PnL formula flips for shorts
                if side == 'buy':
                    unrealized_pnl = (current_price - entry_price) * amount
                else:  # sell/short
                    unrealized_pnl = (entry_price - current_price) * amount

                # percent of your initial margin
                if original_margin != 0:
                    unrealized_pnl_pct = (unrealized_pnl / original_margin) * 100
                else:
                    unrealized_pnl_pct = 0.0

                # 4) update the row
                cursor.execute(
                    """
                    UPDATE mocktrade.position_history
                       SET unrealized_pnl            = %s,
                           unrealized_pnl_pct = %s
                     WHERE id     = %s
                       AND user_id = %s
                       AND status  = 1
                    """,
                    (unrealized_pnl,
                     unrealized_pnl_pct,
                     pos_id,
                     user_id)
                )
                row_count += 1

            conn.commit()
            return row_count

        except Exception:
            if conn:
                conn.rollback()
            traceback.print_exc()
            raise

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()