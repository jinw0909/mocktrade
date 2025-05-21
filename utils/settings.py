import asyncio
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

from utils.symbols import symbols as SYMBOL_CFG
from utils.connection_manager import manager
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
                margin_type = find_result['margin_type']

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
                # if raw_pnl <= -margin:
                #     derived_pnl = -margin
                #     new_status = 4
                # else:
                #     derived_pnl = raw_pnl
                #     new_status = 3

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

                # insert_sql = """
                #     INSERT INTO mocktrade.position_history (
                #         user_id, symbol, size, amount, entry_price, liq_price,
                #         margin, pnl, margin_type, side, leverage, status, tp, sl, `datetime`, close_price
                #     ) VALUES (
                #         %s, %s, %s, %s, %s, %s,
                #         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                #     )
                # """
                # cursor.execute(insert_sql, (
                #     user_id,
                #     symbol,
                #     0,
                #     0,
                #     entry_price,
                #     None,
                #     0,
                #     raw_pnl,
                #     margin_type,
                #     side,
                #     0,
                #     3,
                #     0,
                #     0,
                #     datetime.now(timezone('Asia/Seoul')),
                #     current_price
                # ))

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

        pending_notifs: list[tuple[str, dict]] = []

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

                        # b) insert a new "liquidation" record
                        # cursor.execute("""
                        #     INSERT INTO mocktrade.position_history (
                        #         user_id, symbol, size, amount, entry_price,
                        #         liq_price, margin, pnl, margin_type,
                        #         side, leverage, status, datetime, close_price
                        #     ) VALUES (
                        #         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                        #     )
                        # """, (
                        #     user_id,
                        #     symbol,
                        #     0,
                        #     0,
                        #     None,
                        #     None,
                        #     0,
                        #     close_pnl,  # realized PnL: full margin loss
                        #     pos['margin_type'],
                        #     side,
                        #     pos['leverage'],
                        #     3,  # liquidated
                        #     datetime.now(timezone('Asia/Seoul')),
                        #     current_price  # price at which it was liquidated
                        # ))

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

                        cursor.execute("RELEASE SAVEPOINT lq_order")
                        liquidated += 1
                except Exception:
                    cursor.execute("ROLLBACK TO SAVEPOINT lq_order")
                    cursor.execute("RELEASE SAVEPOINT lq_order")
                    logger.exception(f"failed to liquidate position with id of [{pos['id']}]")

            conn.commit()

            for retri_id, message in pending_notifs:
                # schedule on the same loop
                asyncio.create_task(
                    manager.notify_user(retri_id, message)
                )
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
        pending_notifs : list[tuple[str, dict]] = []
        liquidated_positions = []

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

                    cursor.execute("RELEASE SAVEPOINT lqc_exec")

                except Exception:
                    cursor.execute("ROLLBACK TO SAVEPOINT lqc_exec")
                    cursor.execute("RELEASE SAVEPOINT lqc_exec")
                    logger.exception(f"failed to execute liquidate cross on position with id of [{worst_pos['id']}]")
                    continue

            conn.commit()
            for retri_id, message in pending_notifs:
                asyncio.create_task(
                    manager.notify_user(retri_id, message)
                )

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

    def calculate_cross_positions(self):
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
                        maint_other = sum(p['entry_price'] * p['amount'] * 0.01 for p in cross_positions if p is not pos)
                        unrealized_pnl = sum(p['unrealized_pnl'] for p in cross_positions if p is not pos)
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
                    asyncio.create_task(
                        manager.notify_user(user_id, messages)
                    )

            return { "row_count": row_count }

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




