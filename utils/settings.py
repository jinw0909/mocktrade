import os
import sys

import httpx
import pymysql.cursors
import logging

from pymysql.cursors import DictCursor
import json
from pymysql.connections import Connection
from starlette.config import Config
from datetime import datetime, timedelta
from pytz import timezone
import pytz
# from boto3 import client
from base64 import b64decode
from utils.make_error import MakeErrorType
import pandas as pd
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
# from utils.make_error import MakeErrorType
from base64 import b64decode
# from models import *
from decimal import Decimal
import math
import numpy as np
import warnings
import pandas as pd
import numpy as np
import time
import traceback

warnings.filterwarnings('ignore')
import redis

config = Config(".env")

from utils.price_cache import prices as price_cache

logger = logging.getLogger("uvicorn")


def compute_cross_liq_price(
        entry_price: float,
        amount: float,
        leverage: float,
        available_balance: float,
        side: str
) -> float:
    """
    Compute the cross-margin liquidation price for a single position,
    using a fixed 0.5% maintenance margin

    Args:
        entry_price: the fill price of the position
        amount: number of coins/contracts
        leverage: the leverage factor
        available_balance: current free cross-margin balance
        side: 'buy' for long, 'sell' for short

    Returns:
        liq_price: the price at which this position would be liquidated
    """
    # fixed 0.5% maintenance rate
    maintenance_margin_rate = 0.005

    # 1) Notional value of the position
    notional = entry_price * amount

    # 2) Maintenance margin requirement (the collateral buffer)
    maint_margin = notional * maintenance_margin_rate

    # 3) Solve for the price at which equity == maintenance margin
    # Equity = available_balance + (P_liq - entry) * amount  for longs
    # Equity  =available_balance + (entry - P_liq) * amount  for shorts
    if side == 'buy':
        # long -> liquidate when price falls to this level
        liq = (maint_margin - available_balance) / amount + entry_price
    else:
        # short -> liquidate when price rises to this level
        liq = entry_price - (maint_margin - available_balance) / amount

    # prices cannot go below zero
    return max(liq, 0.0)


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


class MySQLAdapter:

    def __init__(self) -> None:

        # self.KMS_CLIENT= client("kms", region_name='ap-northeast-2')
        self.exchange_id = 1
        self.now = datetime.now(timezone('Asia/Seoul'))
        self.return_dict_data = dict(results=[], reCode=1, message='Server Error')
        self.status_code = 200
        self.status = 0
        self.check = 0

    # DB Connection 확인
    def _get_connection(self):
        try:
            # print(config.get('USER1'))
            # print(config.get('HOST'))
            # print(config.get('PASS'))
            # print(config.get('DBNAME'))
            connection = Connection(host=config.get('HOST'),
                                    user=config.get('USER1'),
                                    password=config.get('PASS'),
                                    database=config.get('DBNAME'),
                                    cursorclass=pymysql.cursors.DictCursor)
            connection.ping(False)

        except Exception as e:
            logger.exception("failed to get db connection")
        else:
            return connection

    def _get_redis(self):
        try:
            # print(config.get('USER1'))
            # print(config.get('HOST'))
            # print(config.get('PASS'))
            # print(config.get('DBNAME'))
            # connection = rd = redis.Redis(host='172.31.11.200', port=6379, db=0)
            connection = rd = redis.Redis(host=config.get("REDIS_HOST"), port=6379, db=0)


        except Exception as e:
            logger.exception("failed to get redis connection")
        else:
            return connection

    def set_tpsl(self, order_no, tp, sl):
        conn = self._get_connection()
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = """
                        UPDATE mocktrade.order_history
                        SET tp = %s,  sl = %s
                        WHERE order_id = %s
                    """
                    cursor.execute(sql, (tp, sl, order_no))
                    conn.commit()
                    return "tp and sl updated successfully"
        except Exception as e:
            logger.exception("failed to set tp and sl")

        return "failed to set tp sl of order"

    def get_userId(self, order_no):

        # self.return_dict_data=dict(page=0,size=0,totalPages=0,totalCount=0,results=[], reCode=1, message='Server Error')
        conn = self._get_connection()
        check = MakeErrorType()
        new_list = []

        try:
            if conn:
                with conn.cursor() as cursor:

                    sql = f"SELECT user_id FROM order_history where id={order_no}"

                    cursor.execute(sql)
                    result = cursor.fetchone()

                    logger.info(f"{result}")

                    if result:

                        return result[0]

                    else:
                        return 0

            else:
                logger.error("No DB connection")
                return 0
        except Exception as e:
            logger.exception("failed to get user from the order")
            return 0

    def get_user(self, user_no):
        conn = self._get_connection()
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = f"SELECT * FROM user WHERE id = {user_no}"
                    cursor.execute(sql)
                    row = cursor.fetchone()
                conn.close()
                return row  # returns a dict or None
        except Exception as e:
            logger.exception("failed to get the user info")
            return None

    def fetch_price(self):
        symbols = {
            "BTC": "bitcoin",
            "ETH": "ethereum",
            "XRP": "ripple"
        }

        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": ",".join(symbols.values()),
            "vs_currencies": "usd"
        }

        try:
            response = httpx.get(url, params=params, timeout=10)
            response.raise_for_status()
            prices = response.json()

            conn = self._get_connection()
            now = datetime.now(timezone("Asia/Seoul"))
            saved_prices = {}

            if conn:
                with conn.cursor() as cursor:
                    for symbol, cg_id in symbols.items():
                        price = prices.get(cg_id, {}).get("usd")
                        if price is not None:
                            saved_prices[symbol] = price
                            sql = """
                                INSERT INTO mocktrade.prices (symbol, price, updatedAt)
                                VALUES (%s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                    price = VALUES(price),
                                    updatedAt = VALUES(updatedAt)
                            """
                            cursor.execute(sql, (symbol, price, now))
                conn.commit()
                conn.close()
                return {
                    "message": "Prices fetched and stored successfully",
                    "timestamp": now.isoformat(),
                    "data": saved_prices
                }
            else:
                logger.error("DB connection failed")
                return {
                    "error": "DB connection failed"
                }

        except Exception as e:
            print(f"Error in fetch_price: {e}")
            return {"error": str(e)}

    def get_price(self):
        conn = self._get_connection()
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = f"SELECT * FROM mocktrade.prices"
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                conn.close()
                return rows
            else:
                logger.error("Could not connect to the database")
                return {"error": "Could not connect to the database"}
        except Exception as e:
            logger.exception(f"Error during retrieving prices from db")
            return {"error": str(e)}

    def get_limit_orders_by_status(self, status: int):
        conn = self._get_connection()
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = """
                        SELECT * FROM mocktrade.order_history
                        WHERE `type` = 'limit'
                        AND `status` = %s
                        AND `margin_type` = 'isolated'
                        ORDER BY `insert_time` ASC
                    """
                    cursor.execute(sql, (status,))
                    rows = cursor.fetchall()
                conn.close()
                return rows
            else:
                logger.error("Could not get db connection")
                return {"error": "Could not get db connection"}
        except Exception as e:
            logger.exception(f"Error during retrieving limit orders by type: ")
            return {"error": str(e)}

    def update_status(self, order_id, status):
        conn = self._get_connection()
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = """
                        UPDATE mocktrade.order_history 
                        SET status = %s
                        WHERE id = %s 
                        AND margin_type = 'isolated'
                    """
                    cursor.execute(sql, (status, order_id))
                conn.commit()
                conn.close()
            else:
                return {"error": "Could not connect to DB"}
        except Exception as e:
            logger.exception(f"Error updating order status")
            return {"error": str(e)}

    def get_settled_orders(self, user_id, symbol):
        conn = self._get_connection()
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = """
                        SELECT * FROM mocktrade.order_history
                        WHERE user_id = %s 
                        AND symbol = %s
                        AND margin_type = 'isolated'
                        AND status = 1
                    """
                    cursor.execute(sql, (user_id, symbol))
                conn.close()
                result = cursor.fetchall()
                return result
            else:
                logger.exception("Failed to get db connection")
                return {"error": "Failed to connect to the database"}

        except Exception as e:
            logger.exception("Error during retrieving settled orders")
            return {"error": str(e)}

    def insert_position_history(self, position, user_id):
        conn = self._get_connection()
        symbol = position["symbol"]
        try:
            if conn:
                with conn.cursor() as cursor:
                    update_sql = """
                        UPDATE mocktrade.position_history
                        SET status = 2 
                        WHERE user_id = %s
                        AND symbol = %s
                        AND margin_type = 'isolated'
                    """

                    cursor.execute(update_sql, (user_id, symbol))

                    insert_sql = """
                        INSERT INTO mocktrade.position_history (
                            user_id, symbol, side, entry_price,
                            amount, size, margin, margin_type, leverage, liq_price,
                            datetime, status  
                        ) VALUES (
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s  
                        )    
                    """
                    now = datetime.now(timezone('Asia/Seoul'))
                    cursor.execute(insert_sql, (
                        user_id,
                        position['symbol'],
                        position['side'],
                        position['entry_price'],
                        position['amount'],
                        position['size'],
                        position['margin'],
                        'isolated',
                        position['leverage'],
                        position['liq_price'],
                        now,
                        1
                    ))

                conn.commit()
                conn.close()
                return {"message": "Position history inserted successfully"}

            else:
                return {"error": "Failed to connect to the database"}

        except Exception as e:
            logger.exception("failed to insert position history")
            return {"error": str(e)}

    def close_position(self, retri_id, symbol):
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
                margin = float(find_result['margin'])

                # 2. Get current price
                # price_sql = """
                #     SELECT price FROM mocktrade.prices
                #     WHERE symbol = %s
                #     ORDER BY updatedAt DESC
                #     LIMIT 1
                # """
                # cursor.execute(price_sql, (symbol,))
                # price_result = cursor.fetchone()
                # if not price_result:
                #     return {"error": "Price not available"}
                # current_price = float(price_result["price"])
                current_price = price_cache.get(symbol)
                if current_price is None:
                    logger.error(f"could not find the value of {symbol}")
                    raise RuntimeError(f"Could not find the value of {symbol}")


                # 3. calculate pnl
                if side == 'buy':
                    raw_pnl = (current_price - entry_price) * amount
                elif side == 'sell':
                    raw_pnl = (entry_price - current_price) * amount

                # 3a. forced-liquidation guard: cap losses at -margin
                if raw_pnl <= -margin:
                    derived_pnl = -margin
                    new_status = 4
                else:
                    derived_pnl = raw_pnl
                    new_status = 3

                # 4. Mark position as closed (insert new position)
                cursor.execute("""
                    UPDATE mocktrade.position_history
                    SET status = 2 
                    WHERE status = 1
                    AND user_id = %s
                    AND symbol = %s
                """, (user_id, symbol))

                insert_sql = """
                    INSERT INTO mocktrade.position_history (
                        user_id, symbol, size, amount, entry_price, liq_price,
                        margin, pnl, margin_type, side, leverage, status, tp, sl, `datetime`, close_price
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                cursor.execute(insert_sql, (
                    user_id,
                    symbol,
                    0,
                    0,
                    entry_price,
                    None,
                    0,
                    derived_pnl,
                    'isolated',
                    side,
                    0,
                    new_status,
                    None,
                    None,
                    datetime.now(timezone('Asia/Seoul')),
                    current_price
                ))

                # update_sql = """
                #     UPDATE mocktrade.position_history
                #     SET pnl = COALESCE(pnl, 0) + %s, status = 3
                #     WHERE id = %s
                # """

                update_sql = """
                    UPDATE mocktrade.order_history 
                    SET `status` = 4
                    WHERE `type` IN ('tp', 'sl')
                    AND `user_id` = %s
                    AND `symbol` = %s
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
                new_balance = current_balance + derived_pnl

                update_balance_sql = """
                    UPDATE mocktrade.user 
                    SET balance = %s
                    WHERE id = %s
                """
                cursor.execute(update_balance_sql, (new_balance, user_id))

                conn.commit()

            return {
                "message": "Position closed with PnL calculation",
                "derived_pnl": derived_pnl,
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

    def get_current_position(self, user_id, symbol):
        conn = self._get_connection()
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = """
                        SELECT * FROM mocktrade.position_history
                        WHERE user_id = %s
                        AND symbol = %s
                        ORDER BY `datetime` DESC
                        LIMIT 1
                    """
                    cursor.execute(sql, (user_id, symbol,))
                    result = cursor.fetchone()
                conn.close()
                return result
            else:
                return {"error": "Failed to connect tp the database"}
        except Exception as e:
            print(str(e))
            return {"error": f"Error during retrieving a previous position: {str(e)}"}

    def insert_position(self, new_position, old_id=None):
        print("running insert_position()")
        try:
            with self._get_connection() as conn, conn.cursor() as cursor:

                insert_sql = """
                    INSERT INTO mocktrade.position_history
                    (user_id, symbol, size, amount, entry_price, liq_price, margin_ratio, margin,
                    pnl, margin_type, side, leverage, status, tp, sl, datetime)
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_sql, (
                    new_position.get('user_id'),
                    new_position.get('symbol'),
                    new_position.get('size'),
                    new_position.get('amount'),
                    new_position.get('entry_price'),
                    new_position.get('liq_price'),
                    new_position.get('margin_ratio'),
                    new_position.get('margin'),
                    new_position.get('pnl'),
                    new_position.get('margin_type'),
                    new_position.get('side'),
                    new_position.get('leverage'),
                    new_position.get('status'),
                    new_position.get('tp'),
                    new_position.get('sl'),
                    datetime.now(timezone('Asia/Seoul'))
                ))

                if old_id:
                    update_sql = """
                        UPDATE mocktrade.position_history
                        SET status = 2
                        WHERE id = %s
                    """
                    cursor.execute(update_sql, (old_id,))
            conn.commit()
        except Exception as e:
            print(str(e))
            return {"error": str(e)}

    def update_pnl(self, user_id, new_balance):

        update_sql = """
            UPDATE mocktrade.user
            SET balance = %s
            WHERE id = %s
        """

        try:
            with self._get_connection() as conn, conn.cursor() as cursor:
                cursor.execute(update_sql, (new_balance, user_id,))
                conn.commit()
                return {"new_balance": new_balance}

        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return {"error": {str(e)}}

    def get_user_balance(self, user_id):
        sql = """
            SELECT balance FROM `mocktrade`.`user`
            WHERE id = %s
            ORDER BY `datetime` DESC
            LIMIT 1
        """

        try:
            with self._get_connection() as conn, conn.cursor() as cursor:
                cursor.execute(sql, (user_id,))
                row = cursor.fetchone()
                if row is None:
                    raise ValueError(f"User {user_id} not found")
                return row["balance"]
        except Exception as e:
            print(f"Failed to retrieve balance for {user_id}: {e}")
            traceback.print_exc()
            raise

    def liquidate_positions(self):
        conn = None
        cursor = None
        liquidated = 0

        try:
            conn = self._get_connection()
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

                    # a) mark the old position snapshot as "closed"
                    cursor.execute("""
                        UPDATE mocktrade.position_history
                        SET status = 2
                        WHERE status = 1
                        AND symbol = %s
                        AND user_id = %s
                    """, (symbol, user_id))

                    # b) insert a new "liquidation" record
                    cursor.execute("""
                        INSERT INTO mocktrade.position_history (
                            user_id, symbol, size, amount, entry_price,
                            liq_price, margin, pnl, margin_type,
                            side, leverage, status, datetime, close_price
                        ) VALUES (
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                        )
                    """, (
                        user_id,
                        symbol,
                        0,
                        0,
                        None,
                        None,
                        0,
                        close_pnl,  # realized PnL: full margin loss
                        pos['margin_type'],
                        side,
                        pos['leverage'],
                        4,  # liquidated
                        datetime.now(timezone('Asia/Seoul')),
                        current_price  # price at which it was liquidated
                    ))

                    # c) debit the user's wallet by the lost margin
                    cursor.execute("""
                        UPDATE `mocktrade`.`user`
                        SET balance = balance + %s
                        WHERE id = %s
                        AND status = 0    
                    """, (close_pnl, user_id))

                    # d) cancel any TP/SL orders for this user + symbol
                    cursor.execute("""
                        UPDATE mocktrade.order_history
                        SET status = 4
                        WHERE user_id = %s
                        AND symbol = %s
                        AND `type` IN ('tp', 'sl')    
                    """, (user_id, symbol))

                    liquidated += 1

            conn.commit()
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

    def calculate_unrealized_pnl(self):
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

    def liquidate_cross_positions(self):
        conn = None
        cursor = None
        row_count = 0
        liq_count = 0
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # 1) grab all active cross-margin positions
            cursor.execute("""
                SELECT * 
                  FROM mocktrade.position_history
                 WHERE margin_type = 'cross'
                   AND status = 1
            """)
            rows = cursor.fetchall()

            # 2) group them by user_id
            cross_by_user = defaultdict(list)
            for row in rows:
                cross_by_user[row['user_id']].append(row)

            # 3) iterate each user's bucket
            for user_id, positions in cross_by_user.items():
                # — a) wallet balance
                cursor.execute("""
                    SELECT balance
                      FROM mocktrade.user
                     WHERE id     = %s
                       AND status = 0
                    LIMIT 1
                """, (user_id,))
                wallet_balance = cursor.fetchone()['balance'] or 0

                # — b) frozen margin from these cross positions
                cross_position_margin = sum(p['margin'] for p in positions)

                # — c) frozen margin from pending cross orders
                cursor.execute("""
                    SELECT COALESCE(SUM(magin),0) AS frozen
                      FROM mocktrade.order_history
                     WHERE user_id    = %s
                       AND status     = 0
                       AND margin_type = 'cross'
                       AND type IN ('market','limit')
                """, (user_id,))
                order_margin = cursor.fetchone()['frozen'] or 0

                # — d) unrealized PnL from these cross positions
                unrealized_pnl = sum(p['unrealized_pnl'] for p in positions)

                # — e) compute available balance
                available_balance = (
                        wallet_balance
                        - cross_position_margin
                        - order_margin
                        + unrealized_pnl
                )

                # 4) now loop each position for liq-price updates and possible liquidation
                to_liquidate = []
                for pos in positions:
                    current = price_cache[pos['symbol']]
                    new_liq = compute_cross_liq_price(
                        entry_price = pos['entry_price'],
                        amount = pos['amount'],
                        leverage = pos['leverage'],
                        available_balance = available_balance,
                        side = pos['side']
                        # ..maintenance params
                    )
                    if should_liquidate(pos['side'], current, new_liq):
                        to_liquidate.append((pos, current))

                # 2) liquidate them in one batch
                for pos, exit_price in to_liquidate:
                    # a) close the old position
                    cursor.execute("""
                        UPDATE mocktrade.position_history
                        SET status = 2
                        WHERE id = %s    
                    """, (pos['id'],))

                    # b) compute realized PnL on the fill
                    pnl_liq = ((exit_price - pos['entry_price']) if pos['side'] == 'buy' else (pos['entry_price'] - exit_price)) * pos['amount']

                    # c) insert the liquidation record (status = 4)
                    cursor.execute("""
                        INSERT INTO mocktrade.position_history (
                          user_id, symbol, size, amount, entry_price,
                          liq_price, margin,  pnl,
                          margin_type, side, leverage, status, tp, sl, datetime, close_price
                        ) VALUES (
                          %s, %s, 0, 0, 0,
                          0, 0, %s,
                          %s, %s, %s, 4, %s, %s, %s, %s
                        )
                    """, (
                            pos['user_id'], pos['symbol'], pnl_liq,
                            pos['margin_type'], pos['side'], pos['leverage'],
                            pos['tp'], pos['sl'],
                            datetime.now(timezone("Asia/Seoul")),
                            exit_price
                        ))

                    liq_count += 1

                    # d) free its collateral and PnL back into our pool
                    available_balance += pos['margin']

                # 3) remove them from your working list
                remaining = [p for p in positions if p['id'] not in {p[0]['id'] for p in to_liquidate}]

                # 4) final pass: update liq_price on survivors
                for pos in remaining:
                    final_liq = compute_cross_liq_price(
                        entry_price = pos['entry_price'],
                        amount = pos['amount'],
                        leverage = pos['leverage'],
                        available_balance = available_balance,
                        side = pos['side']
                        # ..maintenance params
                    )
                    if final_liq != pos['liq_price']:
                        cursor.execute("""
                            UPDATE mocktrade.position_history
                            SET liq_price = %s
                            WHERE id = %s
                        """, (final_liq, pos['id']))

                    row_count += 1

            conn.commit()
            return {
                "row_count" : row_count,
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

