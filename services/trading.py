import asyncio

from utils.settings import MySQLAdapter
import traceback
from datetime import datetime, timedelta
from pytz import timezone
from utils.price_cache import prices as price_cache
import logging
from utils.symbols import symbols as SYMBOL_CFG
from utils.connection_manager import manager

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
    # (you’ll need your own helper or inline formula)
    # new_liq = calc_iso_liq_price(cur_price, leverage, cs)

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
        # liq_price = calc_iso_liq_price(
        #     price,
        #     leverage,
        #     side
        # )
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
        avg_entry_price = total_value / total_amount
        total_size = total_amount * avg_entry_price
        total_margin = current_margin + order_margin
        logger.info(f"current_margin: {current_margin}, order_margin: {order_margin}, total_margin: {total_margin}")
        effective_leverage = total_size / total_margin if total_margin else leverage

        # released_margin = calc_released_margin(current_margin, total_margin)
        # liq_price = calc_iso_liq_price(
        #     avg_entry_price,
        #     round(effective_leverage, 4),
        #     side
        # )
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
            "leverage": round(effective_leverage, 4),
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
        new_margin = current_margin * (new_amount / current_amount)
        new_size = new_amount * current_entry_price

        effective_leverage = current_size / current_margin if current_margin else leverage

        # released_margin = calc_released_margin(current_margin, new_margin)
        # liq_price = calc_iso_liq_price(
        #     current_entry_price,
        #     round(effective_leverage, 4),
        #     current_side
        # )

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
            "leverage": current_size / current_margin if current_margin else leverage,
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

        # liq_price = calc_iso_liq_price(
        #     price, leverage, side)

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
            "close": True,
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


class TradingService(MySQLAdapter):

    def settle_limit_orders(self):
        conn = None
        cursor = None
        row_count = 0

        # 1) prepare a place to stash pending notifications
        pending_notifs: list[tuple[str, dict]] = []

        try:
            conn = self._get_connection()
            conn.autocommit(False)
            cursor = conn.cursor()

            # # 1) Load prices & open orders
            # cursor.execute("SELECT symbol, price FROM mocktrade.prices")
            # price_dict = {r['symbol']: r['price'] for r in cursor.fetchall()}
            price_dict = price_cache

            cursor.execute("""
                SELECT * FROM mocktrade.order_history
                 WHERE `type`= 'limit'
                   AND `status`= 0
                ORDER BY `id` DESC
            """)
            open_orders = cursor.fetchall()

            if not open_orders:
                conn.rollback()
                return 0  # or return a message/count

            for order in open_orders:
                symbol = order["symbol"]
                order_price = order["price"]
                current_price = price_dict.get(symbol)
                side = order["side"]
                order_id = order["id"]
                user_id = order["user_id"]

                # only process fills
                if current_price is None:
                    continue
                if (side == 'buy' and order_price < current_price) or \
                        (side == 'sell' and order_price > current_price):
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

                if new_position.get('close'):
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

                elif new_position.get('partial'):
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

                # 7) close relevant tp/sl orders
                # if the position being closed, close all the related tp/sl orders
                if new_position.get('close'):
                    cursor.execute("""
                        UPDATE `mocktrade`.`order_history` SET `status` = 4, update_time = %s
                        WHERE `type` IN ('tp', 'sl') AND `user_id` = %s AND `symbol` = %s
                        AND `status` = 0
                    """, (user_id, datetime.now(timezone('Asia/Seoul')), symbol))

                # 8) If tp/sl order was attached
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
                row_count += 1

            conn.commit()

            # fire all websockets in one batch
            # logger.info(f"pending notifs: {pending_notifs}")
            for retri_id, message in pending_notifs:
                # schedule on the same loop
                asyncio.create_task(
                    manager.notify_user(retri_id, message)
                )

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

    def execute_tpsl(self, price_dict):
        """
        price_dict: { symbol: current_price, ... }
        Returns number of orders settled.
        """
        mysql = MySQLAdapter()
        conn = mysql._get_connection()
        conn.autocommit(False)
        cursor = conn.cursor()
        settled = 0

        try:
            # 1) Grab all live TP/SL orders + their active position metadata
            cursor.execute("""
                SELECT o.*,
                CASE 
                    WHEN o.`type` = 'tp' THEN o.tp
                    ELSE o.sl
                END AS exit_price
                FROM mocktrade.order_history AS o
                WHERE `status` = 0
                AND type IN ('tp', 'sl')
            """)
            open_tpsls = cursor.fetchall()

            # 2) Iterate and settle
            for o in open_tpsls:
                order_id = o['id']
                symbol = o['symbol']
                user_id = o['user_id']
                current_price = price_dict.get(symbol)
                if current_price is None:
                    continue

                raw_price = price_dict.get(symbol)
                if raw_price is None:
                    continue

                # pull per-symbol decimal places
                prec = SYMBOL_CFG.get(symbol, {"price": 2, "qty": 3})
                PRICE_DP = prec['price']
                QTY_DP = prec['qty']

                # quantize market price
                current_price = round(raw_price, PRICE_DP)

                # logger.info(f"current price: {current_price}")

                cursor.execute("""
                    SELECT * FROM `mocktrade`.`position_history`
                    WHERE user_id = %s
                    AND symbol = %s
                    ORDER BY `id` DESC
                    LIMIT 1
                    FOR UPDATE
                """, (user_id, symbol))
                pos = cursor.fetchone()

                if not pos:
                    cursor.execute("""
                        UPDATE mocktrade.order_history
                           SET `status` = 4
                         WHERE `id` = %s
                    """, (order_id,))
                    logger.error("Cannot find a position of a symbol to apply tp/sl order ")
                    continue

                tp_sl_type = o['type']  # 'tp' or 'sl'
                pos_side = pos['side']  # 'buy' (long) or 'sell' (short)
                exit_price = float(o['exit_price'])
                pos_status = pos['status']
                pos_id = pos['id']

                logger.info(f'pos_id: {pos["id"]}')

                # skip if the position is already closed or liquidated
                if pos_status in (3, 4):
                    # close all open tp/sl orders for the symbol and continue
                    cursor.execute("""
                        UPDATE mocktrade.order_history
                           SET `status` = 4
                         WHERE `status` = 0
                           AND `user_id` = %s
                           AND `symbol` = %s
                           AND `id` = %s
                    """, (user_id, symbol, pos_id))
                    logger.info("Position is closed; cancelling TP/SL")
                    continue

                elif pos_status == 1:

                    # 2a) trigger check based on position side + tp/sl
                    triggered = False
                    if pos_side == 'buy':
                        # closing a long
                        if tp_sl_type == 'tp' and current_price >= exit_price:
                            triggered = True
                        if tp_sl_type == 'sl' and current_price <= exit_price:
                            triggered = True
                    else:
                        # closing a short
                        if tp_sl_type == 'tp' and current_price <= exit_price:
                            triggered = True
                        if tp_sl_type == 'sl' and current_price >= exit_price:
                            triggered = True

                    if not triggered:
                        continue

                    # --- 2.4) Determine how much to close
                    or_id = o.get('or_id')
                    po_id = o.get('po_id')
                    order_amt = round(float(o['amount']), QTY_DP)
                    pos_amt = float(pos['amount'])

                    if or_id is None and po_id is not None:
                        close_amt = pos_amt
                    else:
                        close_amt = min(order_amt, pos_amt)
                    # close_amt = min(order_amt, pos_amt)

                    exec_price = current_price
                    exec_amount = round(close_amt, QTY_DP)
                    leverage = float(pos['leverage'])
                    entry_price = float(pos['entry_price'])
                    current_margin = float(pos['margin'])

                    # --2.5) Calculate commissions & realize PnL
                    notional = exec_price * exec_amount
                    commission = notional * FEE_RATE

                    # 2c) realized PnL based *only* on the position side
                    if pos_side == 'buy':
                        gross_pnl = (exec_price - entry_price) * exec_amount
                    else:
                        gross_pnl = (entry_price - exec_price) * exec_amount
                    closed_pnl = gross_pnl - commission
                    # exec_margin = (exec_price * exec_amount) / lev

                    # persist execution values back to order_history
                    cursor.execute("""
                        UPDATE mocktrade.order_history
                        SET price = %s,
                        update_time = %s
                        WHERE `id` = %s
                    """, (exec_price,
                          datetime.now(timezone("Asia/Seoul")),
                          order_id
                          ))

                    # entry_price = float(pos['entry_price'])
                    # current_margin = float(pos['margin'])
                    # old_pnl = float(pos['pnl'])
                    # lev = float(pos['leverage'])
                    # # side_of_pos = pos['side']  # reloaded for clarity

                    # forced-liquidation guard
                    # if this loss would wipe out all margin, liquidate instead of partial/full close
                    if closed_pnl <= -current_margin:
                        # mark old as closed
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                            SET status = 2
                            WHERE symbol = %s
                            AND user_id = %s
                        """, symbol, user_id)
                        #insert a liquidation snapshot
                        cursor.execute("""
                            INSERT INTO mocktrade.position_history (
                                user_id, symbol, size, amount, entry_price,
                                liq_price, margin, pnl, margin_type,
                                side, leverage, status, datetime, close_price
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                        """, (
                            user_id,
                            symbol,
                            0,  # no remaining
                            0,
                            None,
                            None,
                            0,
                            closed_pnl,  # full margin loss
                            pos['margin_type'],
                            pos_side,
                            leverage,
                            3,  # liquidated status
                            datetime.now(timezone("Asia/Seoul")),
                            exec_price
                        ))

                        # debit user
                        cursor.execute("""
                            UPDATE mocktrade.user
                               SET balance = balance + %s
                             WHERE id = %s
                               AND status = 0
                        """, (closed_pnl, user_id))
                        # cancel any sibling TP/SLs
                        cursor.execute("""
                            UPDATE mocktrade.order_history
                               SET status = 4
                             WHERE user_id = %s
                               AND symbol  = %s
                               AND type    IN ('tp','sl')
                        """, (user_id, symbol))
                        # mark this TP/SL as filled
                        cursor.execute("""
                            UPDATE mocktrade.order_history
                               SET status      = 1,
                                   update_time = %s
                             WHERE id = %s
                        """, (datetime.now(timezone("Asia/Seoul")), order_id))
                        settled += 1
                        continue

                    # 2d) compute new position values
                    new_amt = pos_amt - close_amt
                    # new_pnl = old_pnl + closed_pnl
                    # new_pnl = closed_pnl
                    new_size = new_amt * entry_price
                    new_margin = new_size / leverage if new_amt > 0 else 0.0
                    if new_amt > 0:
                        new_liq = self.calc_iso_liq_price(
                            entry_price, leverage, pos_side
                        )
                        new_status = 1
                    else:
                        new_liq = None
                        new_status = 3

                    # 3) persist position update
                    cursor.execute("""
                        UPDATE mocktrade.position_history
                        SET status = 2
                        WHERE status = 1
                        AND user_id = %s
                        AND symbol = %s   
                    """, (user_id, symbol))

                    cursor.execute("""
                        INSERT INTO mocktrade.position_history (
                            user_id, symbol, size, amount, entry_price, liq_price, margin, pnl, margin_type, 
                            side, leverage, status, datetime, close_price
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s       
                        )
                    """, (
                        user_id,
                        symbol,
                        round(new_size, PRICE_DP),
                        round(new_amt, QTY_DP),
                        entry_price,
                        new_liq,
                        round(new_margin, PRICE_DP),
                        round(closed_pnl, PRICE_DP),
                        pos['margin_type'],
                        pos_side,
                        leverage,
                        new_status,
                        datetime.now(timezone('Asia/Seoul')),
                        exec_price
                    ))

                    # 4) update user balance
                    cursor.execute("""
                        SELECT balance FROM `mocktrade`.`user` 
                        WHERE id = %s AND `status` = 0
                        FOR UPDATE
                    """, (user_id,))
                    bal = float(cursor.fetchone()['balance'])
                    cursor.execute("""
                        UPDATE `mocktrade`.`user`
                        SET balance     = %s
                        WHERE id = %s
                        AND `status` = 0
                    """, (bal + closed_pnl, user_id))

                    # 5) mark TP/SL order filled
                    cursor.execute("""
                            UPDATE mocktrade.order_history
                               SET `status` = 1,
                                   `update_time` = %s 
                             WHERE `id` = %s
                        """, (datetime.now(timezone('Asia/Seoul')), order_id))

                    # 6) If the position closed, cancel all the related tp/sl orders
                    if new_status == 3:
                        cursor.execute("""
                            UPDATE mocktrade.order_history
                            SET status = 4
                            WHERE status = 0
                            AND user_id = %s
                            AND symbol = %s
                        """, (user_id, symbol))

                    # 7) if the triggered tp/sl order is opened from the limit order, close the opposite side order
                    if or_id:
                        cursor.execute("""
                            UPDATE mocktrade.order_history
                            SET status = 4
                            WHERE status = 0
                            AND user_id = %s
                            AND symbol = %s
                            AND or_id = %s
                        """, (user_id, symbol, or_id))

                    logger.info(f"executed tp/sl order with id of: {order_id}")
                    settled += 1

            conn.commit()
            return settled

        except Exception:
            conn.rollback()
            logger.exception("failed to execute tp/sl order")
            raise

        finally:
            conn.close()


    def settle_tpsl_orders(self):
        conn = None
        cursor = None
        row_count = 0

        pending_notifs : list[tuple[str, dict]] = []

        try:
            conn = self._get_connection()
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
                WHERE oh.status = 0
                AND oh.type in ('tp', 'sl')
            """)

            open_tpsl_orders = cursor.fetchall()
            if not open_tpsl_orders:
                conn.rollback()
                return 0

            price_dict = price_cache
            for order in open_tpsl_orders:
                # logger.info(f"order: {order}")
                order_id = order['id']
                user_id = order['user_id']
                symbol = order['symbol']
                exit_price = order['exit_price']
                from_order = order['from_order']
                amount = order['amount']
                leverage = order['leverage']
                side = order['side']
                order_type = order['type']

                current_price = price_dict.get(symbol)
                # logger.info(f"current price : {current_price}, symbol: {symbol}")

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
                # logger.info(f"current position id: {current_position['id']}")

                if current_position['status'] in (3, 4):
                    continue
                if not current_position:
                    continue
                # 3) Compute new position status
                new_position = calculate_new_position(current_position, order)

                # 4) Persist new position
                # logger.info("setting the previous position status to 2")
                cursor.execute("""
                    UPDATE mocktrade.position_history
                    SET status = 2
                    WHERE status = 1
                    AND user_id = %s
                    AND symbol = %s                
                """, (user_id, symbol))

                if new_position.get('close'):
                    # logger.info("position closed. updating the last position")
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
                else:
                    # partial close
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
                        new_position.get('side'), new_position.get('leverage'), new_position.get('status'),
                        new_position.get('tp', 0), new_position.get('sl', 0), datetime.now(timezone("Asia/Seoul")),
                        0
                    ))

                # 5) Update wallet for any realized pnl
                close_pnl = new_position.get('close_pnl', 0)
                # if close_pnl:
                #     new_bal = wallet_balance + close_pnl
                #     if new_bal < 0:
                #         raise RuntimeError("Balance negative")
                #     cursor.execute("""
                #         UPDATE mocktrade.user
                #         SET balance = %s
                #         WHERE id = %s AND status = 0
                #     """, (new_bal, user_id))
                if close_pnl:
                    new_bal = max(wallet_balance + close_pnl, 0)
                    cursor.execute(
                        "UPDATE mocktrade.user SET balance = %s WHERE `id` = %s",
                        (new_bal, user_id)
                    )


                # 6) Mark this order settled
                if from_order:
                    cursor.execute("""
                        UPDATE mocktrade.order_history
                        SET status = 4
                        WHERE or_id = %s 
                    """, (order['or_id'],))

                    cursor.execute("""
                        UPDATE mocktrade.order_history
                           SET status = 4
                         WHERE `type` IN ('tp', 'sl')
                           AND po_id = %s
                           AND `symbol` = %s
                           AND `user_id` = %s
                    """, (
                        order['po_id'],
                        symbol,
                        user_id
                    ))

                cursor.execute("""
                    UPDATE mocktrade.order_history
                    SET status = 1,
                        `update_time` = %s,
                        `po_id` = %s
                    WHERE id = %s
                """, (datetime.now(timezone('Asia/Seoul')), current_position.get('id'), order_id,))



                row_count += 1

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

            conn.commit()
            for user_id, message in pending_notifs:
                asyncio.create_task(
                    manager.notify_user(user_id, message)
                )
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
