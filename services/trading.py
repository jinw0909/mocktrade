from utils.settings import MySQLAdapter
import traceback
from datetime import datetime,timedelta


def calc_iso_liq_price(entry_price: float,
                       leverage: float,
                       side: str) -> float | None:

    if side == 'buy':     # LONG
        return entry_price * (1 - 1/leverage)
    else:                 # SHORT
        return entry_price * (1 + 1/leverage)


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

    order_value = price * amount
    order_margin = order_value / leverage

    # case 1. No current position -> create new
    if not current_position:

        liq_price = calc_iso_liq_price(
            price,
            leverage,
            side
        )

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": amount,
            "entry_price": price,
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
    current_size = float(current_position['size'])
    current_pnl = float(current_position.get('pnl') or 0)

    # case 2: Same-side -> merge positions
    if current_side == side:
        total_amount = current_amount + amount
        total_value = (current_entry_price * current_amount) + (price * amount)
        avg_entry_price = total_value / total_amount
        total_size = total_amount * avg_entry_price
        total_margin = current_margin + order_margin
        effective_leverage = total_size / total_margin if total_margin else leverage

        # released_margin = calc_released_margin(current_margin, total_margin)
        liq_price = calc_iso_liq_price(
            avg_entry_price,
            round(effective_leverage, 4),
            side
        )

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": total_amount,
            "entry_price": avg_entry_price,
            "size": total_size,
            "pnl": current_pnl,
            "margin": total_margin,
            "leverage": round(effective_leverage, 4),
            "side": side,
            "margin_type": margin_type,
            "status": 1,
            "liq_price": liq_price
        }

    # 🔁 Case 3: Opposite-side → partial close, full close, or flip
    if amount < current_amount:
        # Partial close — reduce position
        new_amount = current_amount - amount
        close_pnl = (price - current_entry_price) * amount if current_side == 'buy' else (
                                                                                                 current_entry_price - price) * amount
        new_pnl = current_pnl + close_pnl
        new_margin = current_margin * (new_amount / current_amount)
        new_size = new_amount * current_entry_price

        effective_leverage = current_size / current_margin if current_margin else leverage

        # released_margin = calc_released_margin(current_margin, new_margin)
        liq_price = calc_iso_liq_price(
            current_entry_price,
            round(effective_leverage, 4),
            current_side
        )

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": new_amount,
            "entry_price": current_entry_price,
            "size": new_size,
            "margin": new_margin,
            "leverage": current_size / current_margin if current_margin else leverage,
            "side": current_side,
            "margin_type": margin_type,
            "pnl": new_pnl,
            "close_pnl": close_pnl,
            "status": 1,
            "liq_price": liq_price,
        }

    elif amount == current_amount:
        # Full close — no new position
        close_pnl = (price - current_entry_price) * amount if current_side == 'buy' else (current_entry_price - price) * amount
        new_pnl = current_pnl + close_pnl

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
            "pnl": new_pnl,
            "close_pnl": close_pnl,
            "status": 3,  # fully closed
            "liq_price": None
        }

    else:
        # Flip — close current, open new opposite
        flip_amount = amount - current_amount
        close_pnl = (price - current_entry_price) * current_amount if current_side == 'buy' else (
                                                                                                         current_entry_price - price) * current_amount
        new_pnl = close_pnl + current_pnl
        new_value = price * flip_amount
        new_margin = new_value / leverage

        liq_price = calc_iso_liq_price(
            price, leverage, side)

        return {
            "user_id": user_id,
            "symbol": symbol,
            "amount": flip_amount,
            "entry_price": price,
            "size": new_value,
            "margin": new_margin,
            "leverage": leverage,
            "side": side,  # now flipped
            "margin_type": margin_type,
            "pnl": new_pnl,
            "close_pnl": close_pnl,
            "status": 1,
            "opposite": True,
            "liq_price": liq_price
        }


def settle_limit_orders():
    mysql = MySQLAdapter()
    conn = mysql._get_connection()
    row_count = 0
    try:
        conn.autocommit(False)
        cursor = conn.cursor()

        # 1) Load prices & open orders
        cursor.execute("SELECT symbol, price FROM mocktrade.prices")
        price_dict = {r['symbol']: r['price'] for r in cursor.fetchall()}

        cursor.execute("""
            SELECT * FROM mocktrade.order_history
             WHERE `type`='limit'
               AND `status`=0
               AND `margin_type`='isolated'
            ORDER BY insert_time
        """)
        open_orders = cursor.fetchall()

        if not open_orders:
            conn.rollback()
            return 0  # or return a message/count

        for order in open_orders:
            symbol       = order["symbol"]
            order_price  = order["price"]
            current_price = price_dict.get(symbol)
            side         = order["side"]
            order_id     = order["id"]
            user_id      = order["user_id"]

            # only process fills
            if current_price is None:
                continue
            if (side=='buy' and order_price < current_price) or \
                    (side=='sell' and order_price > current_price):
                continue

            row_count += 1
            print("row_count += 1 : ", row_count)
            # 2) Read balance & position
            cursor.execute("SELECT balance FROM mocktrade.user WHERE id=%s", (user_id,))
            wallet_balance = cursor.fetchone()["balance"]

            cursor.execute("""
              SELECT * FROM mocktrade.position_history
               WHERE user_id=%s AND symbol=%s AND status = 1
               ORDER BY datetime DESC LIMIT 1
            """, (user_id, symbol))
            current_position = cursor.fetchone()

            # 3) Compute new_position
            new_position = calculate_position(current_position, order)

            # 4) Persist new position + close old one
            insert_sql = """
              INSERT INTO mocktrade.position_history
               (user_id, symbol, size, amount, entry_price,
                liq_price, margin_ratio, margin, pnl,
                margin_type, side, leverage, status, tp, sl, datetime)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,NULL,NULL,NOW())
            """
            cursor.execute(insert_sql, (
                new_position.get('user_id'), new_position.get('symbol'),
                new_position.get('size'), new_position.get('amount'),
                new_position.get('entry_price'), new_position.get('liq_price'),
                new_position.get('margin_ratio'), new_position.get('margin'),
                new_position.get('pnl'), new_position.get('margin_type'),
                new_position.get('side'), new_position.get('leverage')
            ))
            if current_position:
                cursor.execute(
                    "UPDATE mocktrade.position_history SET status=2 WHERE id=%s",
                    (current_position['id'],)
                )

            # 5) Update wallet for any realized PnL
            close_pnl = new_position.get('close_pnl', 0)
            if close_pnl:
                new_bal = wallet_balance + close_pnl
                if new_bal < 0:
                    raise RuntimeError("Balance negative")
                cursor.execute(
                    "UPDATE mocktrade.user SET balance=%s WHERE id=%s",
                    (new_bal, user_id)
                )

            # 6) Mark order settled
            cursor.execute(
                "UPDATE mocktrade.order_history SET status=1 WHERE id=%s",
                (order_id,)
            )

        conn.commit()
        return row_count

    except Exception:
        conn.rollback()
        traceback.print_exc()
        raise
    finally:
        conn.close()


def execute_tpsl(price_dict):
    """
    price_dict: { symbol: current_price, ... }
    Returns number of orders settled.
    """
    mysql = MySQLAdapter()
    conn  = mysql._get_connection()
    conn.autocommit(False)
    cursor = conn.cursor()
    settled = 0

    try:
        # 1) Grab all live TP/SL orders + their active position metadata
        cursor.execute("""
        SELECT
          oh.id          AS order_id,
          oh.user_id,
          oh.symbol,
          oh.type        AS tp_sl_type,   -- 'tp' or 'sl'
          oh.amount      AS order_amount,
          CASE WHEN oh.type = 'tp' THEN oh.tp ELSE oh.sl END AS exit_price,
          ph.id          AS pos_id,
          ph.side        AS pos_side,     -- 'buy' or 'sell'
          ph.amount      AS pos_amount,
          ph.entry_price,
          ph.pnl         AS pos_pnl,
          ph.leverage
        FROM mocktrade.order_history oh
        JOIN mocktrade.position_history ph
          ON oh.po_id = ph.id
        WHERE oh.type   IN ('tp','sl')
          AND oh.status = 0
          AND ph.status = 1
        """)
        open_tpsls = cursor.fetchall()

        # 2) Iterate and settle
        for o in open_tpsls:
            symbol        = o['symbol']
            current_price = price_dict.get(symbol)
            if current_price is None:
                continue
            print("current price: ", current_price)

            tp_sl_type    = o['tp_sl_type']      # 'tp' or 'sl'
            pos_side      = o['pos_side']        # 'buy' (long) or 'sell' (short)
            exit_price    = float(o['exit_price'])

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
            print("current_price: ", current_price)
            # 2b) fetch freshest position state
            cursor.execute("""
                SELECT amount AS pos_amount,
                       entry_price,
                       pnl       AS pos_pnl,
                       leverage,
                       side      AS pos_side
                FROM mocktrade.position_history
                WHERE id=%s AND status=1
                FOR UPDATE
            """, (o['pos_id'],))
            pos = cursor.fetchone()
            if not pos:
                # already closed
                cursor.execute(
                    "UPDATE mocktrade.order_history SET status=2 WHERE id=%s",
                    (o['order_id'],)
                )
                continue

            order_amt  = float(o['order_amount'])
            pos_amt    = float(pos['pos_amount'])
            close_amt  = min(order_amt, pos_amt)
            entry_price= float(pos['entry_price'])
            old_pnl    = float(pos['pos_pnl'])
            lev        = float(pos['leverage'])
            side_of_pos= pos['pos_side']  # reloaded for clarity

            # 2c) realized PnL based *only* on the position side
            if side_of_pos == 'buy':
                closed_pnl = (current_price - entry_price) * close_amt
            else:
                closed_pnl = (entry_price - current_price) * close_amt

            # 2d) compute new position values
            new_amt    = pos_amt - close_amt
            new_pnl    = old_pnl + closed_pnl
            new_size   = new_amt * entry_price
            new_margin = new_size / lev if new_amt > 0 else 0.0
            if new_amt > 0:
                new_liq    = calc_iso_liq_price(
                    entry_price,
                    new_margin,
                    side_of_pos
                )
                new_status = 1
            else:
                new_liq    = None
                new_status = 3

            # 3) persist position update
            cursor.execute("""
            UPDATE mocktrade.position_history
               SET amount     = %s,
                   size       = %s,
                   margin     = %s,
                   pnl        = %s,
                   liq_price  = %s,
                   status     = %s,
                   datetime   = %s
             WHERE id = %s
            """, (
                new_amt,
                new_size,
                new_margin,
                new_pnl,
                new_liq,
                new_status,
                datetime.now(),
                o['pos_id']
            ))

            # 4) update user balance
            cursor.execute("SELECT balance FROM mocktrade.user WHERE id=%s", (o['user_id'],))
            bal = float(cursor.fetchone()['balance'])
            cursor.execute("""
            UPDATE mocktrade.user
            SET balance     = %s
            WHERE id = %s
            """, (bal + closed_pnl, o['user_id']))

            # 5) mark TP/SL order filled
            cursor.execute("""
            UPDATE mocktrade.order_history
               SET status      = 1,
                   update_time = %s
             WHERE id = %s
            """, (datetime.now(), o['order_id']))

            settled += 1

        conn.commit()
        return settled

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()









def settle_tpsl_orders():
    mysql = MySQLAdapter()
    try:
        with mysql._get_connection() as conn, conn.cursor() as cursor:

            cursor.execute("SELECT price, symbol FROM mocktrade.prices")
            price_dict = {r['symbol']: r['price'] for r in cursor.fetchall()}

            count = execute_tpsl(price_dict)
            return count
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        return {"error": "Error during settling tpsl orders"}