from utils.settings import MySQLAdapter
import traceback
from datetime import datetime, timedelta
from pytz import timezone
from utils.price_cache import prices as price_cache


class TradingService(MySQLAdapter):

    def calc_iso_liq_price(self, entry_price: float,
                           leverage: float,
                           side: str) -> float | None:
        if side == 'buy':  # LONG
            return entry_price * (1 - 1 / leverage)
        else:  # SHORT
            return entry_price * (1 + 1 / leverage)

    def calculate_position(self, current_position, order):
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
            print("case 1, no current position")
            liq_price = self.calc_iso_liq_price(
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

        ci = float(current_position['liq_price'])
        cs = current_position['side']
        if (cs == 'buy' and price <= ci) or (cs == 'sell' and price >= ci):
            # return a force liquidation result immediately
            return {
                "user_id": current_position['user_id'],
                "symbol": current_position['symbol'],
                "amount": 0,
                "entry_price": None,
                "size": 0,
                "margin": 0,
                "leverage": float(current_position['leverage']),
                "side": cs,
                "margin_type": current_position['margin_type'],
                "pnl": -float(current_position['margin']),
                "status": 4,
                "liq_price": None,
                "close_price": price
            }

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
            print("case 2: same side")
            total_amount = current_amount + amount
            total_value = (current_entry_price * current_amount) + (price * amount)
            avg_entry_price = total_value / total_amount
            total_size = total_amount * avg_entry_price
            total_margin = current_margin + order_margin
            effective_leverage = total_size / total_margin if total_margin else leverage

            # released_margin = calc_released_margin(current_margin, total_margin)
            liq_price = self.calc_iso_liq_price(
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
            print("case 3-1, opposite side, partial close")
            # Partial close — reduce position
            new_amount = current_amount - amount
            close_pnl = (price - current_entry_price) * amount if current_side == 'buy' else (
                                                                                                     current_entry_price - price) * amount
            # new_pnl = current_pnl + close_pnl
            new_pnl = close_pnl
            new_margin = current_margin * (new_amount / current_amount)
            new_size = new_amount * current_entry_price

            effective_leverage = current_size / current_margin if current_margin else leverage

            # released_margin = calc_released_margin(current_margin, new_margin)
            liq_price = self.calc_iso_liq_price(
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
                "tp": current_tp,
                "sl": current_sl,
                "close_price": price
            }

        elif amount == current_amount:
            print("case 3-2, opposite side, full close")
            # Full close — no new position
            close_pnl = (price - current_entry_price) * amount if current_side == 'buy' else (
                                                                                                     current_entry_price - price) * amount
            # new_pnl = current_pnl + close_pnl
            new_pnl = close_pnl

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
                "liq_price": None,
                "tp": None,
                "sl": None,
                "close": True,
                "close_price": price
            }

        else:
            # Flip — close current, open new opposite
            print("case 3-3, opposite side, flip")
            flip_amount = amount - current_amount
            close_pnl = (price - current_entry_price) * current_amount if current_side == 'buy' else (
                                                                                                             current_entry_price - price) * current_amount
            # new_pnl = close_pnl + current_pnl
            new_pnl = close_pnl
            new_value = price * flip_amount
            new_margin = new_value / leverage

            liq_price = self.calc_iso_liq_price(
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
                "liq_price": liq_price,
                "tp": None,
                "sl": None,
                "close": True,
                "close_price": price
            }

    # def open_tpsl_orders(order):
    #     mysql = MySQLAdapter()
    #     conn = None
    #     cursor = None
    #
    #     try:
    #         print("opening tp/sl orders from the triggered limit order")
    #         user_id = order['user_id']
    #         symbol = order['symbol']
    #         limit_tp = order['tp']
    #         limit_sl = order['sl']
    #         limit_amount = order['amount']
    #         limit_side = order['side']
    #
    #         order_side = 'sell' if limit_side == 'buy' else 'buy'
    #         conn = mysql._get_connection()
    #         cursor = conn.cursor()
    #
    #         if limit_tp:
    #             cursor.execute("""
    #                 INSERT INTO mocktrade.order_history (
    #                     user_id, symbol, `type`, margin_type, side, amount, status
    #                     ,insert_time, update_time, tp )
    #                 VALUES (
    #                     %s,%s,%s,%s,%s,%s,%s,%s,%s,%s )
    #             """, (
    #                 user_id,
    #                 symbol,
    #                 'tp',
    #                 'isolated',
    #                 order_side,
    #                 limit_amount,
    #                 0,
    #                 datetime.now(timezone("Asia/Seoul")),
    #                 datetime.now(timezone("Asia/Seoul")),
    #                 limit_tp
    #             ))
    #
    #         if limit_sl:
    #             cursor.execute("""
    #                 INSERT INTO mocktrade.order_history (
    #                     user_id, symbol, `type`, margin_type, side, amount, status
    #                     ,insert_time, update_time, sl )
    #                 VALUES (
    #                     %s,%s,%s,%s,%s,%s,%s,%s,%s,%s )
    #             """, (
    #                 user_id,
    #                 symbol,
    #                 'sl',
    #                 'isolated',
    #                 order_side,
    #                 limit_amount,
    #                 0,
    #                 datetime.now(timezone("Asia/Seoul")),
    #                 datetime.now(timezone("Asia/Seoul")),
    #                 limit_sl
    #             ))
    #
    #         conn.commit()
    #         print("successfully created tp/sl orders from the triggered limit order")
    #         return {"success": "successfully created tp/sl orders from the triggered limit order"}
    #
    #     except Exception as e:
    #         conn.rollback()
    #         print(str(e))
    #         traceback.print_exc()
    #         raise
    #
    #     finally:
    #         if cursor:
    #             cursor.close()
    #         if conn:
    #             conn.close()

    def settle_limit_orders(self):
        conn = None
        cursor = None
        row_count = 0
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
                   AND `margin_type` = 'isolated'
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
                cursor.execute("SELECT balance FROM `mocktrade`.`user` WHERE `id`= %s", (user_id,))
                wallet_balance = cursor.fetchone()["balance"]

                cursor.execute("""
                  SELECT * FROM mocktrade.position_history
                   WHERE user_id = %s AND symbol = %s AND status = 1
                   ORDER BY `id` DESC LIMIT 1
                   FOR UPDATE
                """, (user_id, symbol))
                current_position = cursor.fetchone()

                # 3) Compute new_position
                new_position = self.calculate_position(current_position, order)

                # 4) Persist new position ( + close old ones )
                cursor.execute("""
                    UPDATE mocktrade.position_history SET status = 2
                    WHERE `status` = 1 AND `user_id` = %s AND `symbol` = %s
                """, (user_id, symbol))

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
                    new_position.get('pnl'), new_position.get('margin_type'),
                    new_position.get('side'), new_position.get('leverage'), new_position.get('status'),
                    new_position.get('tp'), new_position.get('sl'), datetime.now(timezone("Asia/Seoul")),
                    new_position.get('close_price')
                ))
                # update the old positions to status 2
                # if current_position:
                #     cursor.execute(
                #         "UPDATE mocktrade.position_history SET status = 2 WHERE id = %s",
                #         (current_position['id'],)
                #     )

                # if the position being closed, close all the related tp/sl orders
                if new_position.get('close'):
                    cursor.execute("""
                        UPDATE `mocktrade`.`order_history` SET `status` = 4
                        WHERE `type` IN ('tp', 'sl') AND `user_id` = %s AND `symbol` = %s
                    """, (user_id, symbol))

                # 5) Update wallet for any realized PnL
                close_pnl = new_position.get('close_pnl', 0)
                if close_pnl:
                    new_bal = wallet_balance + close_pnl
                    if new_bal < 0:
                        raise RuntimeError("Balance negative")
                    cursor.execute(
                        "UPDATE mocktrade.user SET balance = %s WHERE id = %s",
                        (new_bal, user_id)
                    )

                # 6) Mark order settled
                cursor.execute(
                    "UPDATE mocktrade.order_history SET status = 1 WHERE id = %s",
                    (order_id,)
                )

                if (order["tp"] or order["sl"]) and new_position['amount'] > 0:
                    print("opening tp/sl orders from the triggered limit order")
                    symbol = order['symbol']
                    limit_tp = order['tp']
                    limit_sl = order['sl']
                    limit_amount = order['amount']
                    limit_side = order['side']

                    order_side = 'sell' if limit_side == 'buy' else 'buy'

                    if limit_tp:
                        cursor.execute("""
                            INSERT INTO mocktrade.order_history (
                                user_id, symbol, `type`, margin_type, side, amount, status
                                ,insert_time, update_time, tp, or_id )
                            VALUES (
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s )
                        """, (
                            user_id,
                            symbol,
                            'tp',
                            'isolated',
                            order_side,
                            limit_amount,
                            0,
                            datetime.now(timezone("Asia/Seoul")),
                            datetime.now(timezone("Asia/Seoul")),
                            limit_tp,
                            order_id
                        ))
                        print("successfully opened the take profit order from the triggered limit order")

                    if limit_sl:
                        cursor.execute("""
                            INSERT INTO mocktrade.order_history (
                                user_id, symbol, `type`, margin_type, side, amount, status
                                ,insert_time, update_time, sl, or_id )
                            VALUES (
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s )
                        """, (
                            user_id,
                            symbol,
                            'sl',
                            'isolated',
                            order_side,
                            limit_amount,
                            0,
                            datetime.now(timezone("Asia/Seoul")),
                            datetime.now(timezone("Asia/Seoul")),
                            limit_sl,
                            order_id
                        ))
                        print("successfully opened the stop loss order from the triggered limit order")

                row_count += 1
                print("row_count += 1 : ", row_count)

            conn.commit()
            return row_count

        except Exception:
            conn.rollback()
            traceback.print_exc()
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
                print("current price: ", current_price)

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
                    print("Cannot find a position of a symbol to apply tp/sl order ")
                    continue

                tp_sl_type = o['type']  # 'tp' or 'sl'
                pos_side = pos['side']  # 'buy' (long) or 'sell' (short)
                exit_price = float(o['exit_price'])
                pos_status = pos['status']
                pos_id = pos['id']

                print('pos_id: ', pos['id'])

                # skip if the position is already closed or liquidated
                if pos_status == 3 or pos_status == 4:
                    # close all open tp/sl orders for the symbol and continue
                    cursor.execute("""
                        UPDATE mocktrade.order_history
                           SET `status` = 4
                         WHERE `status` = 0
                           AND `user_id` = %s
                           AND `symbol` = %s
                           AND `id` = %s
                    """, (user_id, symbol, pos_id))
                    print("Position is closed")
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

                    # override with actual fill values
                    or_id = o.get('or_id')
                    po_id = o.get('po_id')
                    order_amt = float(o['amount'])
                    pos_amt = float(pos['amount'])

                    if or_id is None and po_id is not None:
                        close_amt = pos_amt
                    else:
                        close_amt = min(order_amt, pos_amt)
                    # close_amt = min(order_amt, pos_amt)

                    exec_price = current_price
                    exec_amount = close_amt
                    lev = float(pos['leverage'])

                    pos_margin = float(pos['margin'])

                    # exec_margin = (exec_price * exec_amount) / lev

                    # update in-memory so calculate/DB uses real values
                    o['price'] = exec_price
                    # o['margin'] = exec_margin

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

                    entry_price = float(pos['entry_price'])
                    current_margin = float(pos['margin'])
                    old_pnl = float(pos['pnl'])
                    lev = float(pos['leverage'])
                    # side_of_pos = pos['side']  # reloaded for clarity

                    # 2c) realized PnL based *only* on the position side
                    if pos_side == 'buy':
                        closed_pnl = (current_price - entry_price) * close_amt
                    else:
                        closed_pnl = (entry_price - current_price) * close_amt

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
                            lev,
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
                    new_pnl = closed_pnl
                    new_size = new_amt * entry_price
                    new_margin = new_size / lev if new_amt > 0 else 0.0
                    if new_amt > 0:
                        new_liq = self.calc_iso_liq_price(
                            entry_price, lev, pos_side
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
                        new_size,
                        new_amt,
                        entry_price,
                        new_liq,
                        new_margin,
                        new_pnl,
                        pos['margin_type'],
                        pos_side,
                        lev,
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

                    print("executed tp/sl order with id of: ", order_id)
                    settled += 1

            conn.commit()
            return settled

        except Exception:
            conn.rollback()
            traceback.print_exc()
            raise

        finally:
            conn.close()

    def settle_tpsl_orders(self):
        mysql = MySQLAdapter()
        try:
            with mysql._get_connection() as conn, conn.cursor() as cursor:

                cursor.execute("SELECT price, symbol FROM mocktrade.prices")
                price_dict = {r['symbol']: r['price'] for r in cursor.fetchall()}

                count = self.execute_tpsl(price_dict)
                return count
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return {"error": "Error during settling tpsl orders"}
