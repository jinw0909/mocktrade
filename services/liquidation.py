import asyncio

from utils.connections import MySQLAdapter
import traceback
from datetime import datetime, timedelta
from pytz import timezone
from utils.price_cache import prices as price_cache
import logging
from utils.symbols import symbols as SYMBOL_CFG
from utils.connection_manager import manager
from utils.local_redis import update_position_status_per_user, update_order_status_per_user, update_balance_status_per_user
from collections import defaultdict
from services.calculation import CalculationService

logger = logging.getLogger(__name__)

calculation = CalculationService()

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


class LiquidationService(MySQLAdapter):

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

            # cursor.execute("""
            #     SELECT symbol, price
            #     FROM prices
            # """)
            # price_dict = {r['symbol']: float(r['price']) for r in cursor.fetchall()}
            price_dict = price_cache

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
                    current_price = price_dict.get(symbol)

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

                        cursor.execute("RELEASE SAVEPOINT lq_order")
                        liquidated += 1
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
        liquidated_positions = []
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
                    price = price_cache.get(pos['symbol'])

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

                    liquidated_positions.append({
                        'user_id': user_id,
                        'position_id': pos_id
                    })
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

            return liquidated_positions

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
            price_dict = price_cache

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

                current_price = price_dict.get(symbol)
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
