import os
import sys

import httpx
import pymysql.cursors
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
            print(e)
        else:
            return connection

    def _get_redis(self):
        try:
            # print(config.get('USER1'))
            # print(config.get('HOST'))
            # print(config.get('PASS'))
            # print(config.get('DBNAME'))
            connection = rd = redis.Redis(host='172.31.11.200', port=6379, db=0)


        except Exception as e:
            print(e)
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
            print(e)

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

                    print(result)

                    if result:

                        return result[0]

                    else:
                        return 0

            else:
                print("No DB connection")
                return 0
        except Exception as e:
            print(e)
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
            print(e)
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
                print("DB connection failed")
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
                print("Could not connect to the database")
                return {"error": "Could not connect to the database"}
        except Exception as e:
            print(f"Error during retrieving prices from db {str(e)}")
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
                    cursor.execute(sql, (status, ))
                    rows = cursor.fetchall()
                conn.close()
                return rows
            else:
                print("Could not get db connection")
                return { "error" : "Could not get db connection" }
        except Exception as e:
            print(f"Error during retrieving limit orders by type : {str(e)}")
            return { "error" : str(e)}

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
                return { "error": "Could not connect to DB" }
        except Exception as e:
            print(f"Error updating order status {str(e)}")
            return { "error": str(e) }

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
                return {"error" : "Failed to connect to the database"}

        except Exception as e:
            print(f"Error during retrieving settled orders")
            return { "error" : str(e) }

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
                return { "message" : "Position history inserted successfully" }

            else:
                return { "error": "Failed to connect to the database"}

        except Exception as e:
            print(str(e))
            return { "error" : str(e)}

    def close_position(self, user_id, symbol):
        try:
            with self._get_connection() as conn, conn.cursor() as cursor:
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
                    return { "error" : "No active position to close" }

                position_id = find_result["id"]
                entry_price = float(find_result["entry_price"])
                amount = float(find_result["amount"])
                side = find_result["side"]

                # 2. Get current price
                price_sql = """
                    SELECT price FROM mocktrade.prices
                    WHERE symbol = %s
                    ORDER BY updatedAt DESC
                    LIMIT 1
                """
                cursor.execute(price_sql, (symbol,))
                price_result = cursor.fetchone()
                if not price_result:
                    return { "error": "Price not available" }
                current_price = float(price_result["price"])

                # 3. calculate pnl
                if side == 'buy':
                    derived_pnl = (current_price - entry_price) * amount
                elif side == 'sell':
                    derived_pnl = (entry_price - current_price) * amount

                # 4. Mark position as closed
                update_sql = """
                    UPDATE mocktrade.position_history 
                    SET pnl = COALESCE(pnl, 0) + %s, status = 3
                    WHERE id = %s
                """
                cursor.execute(update_sql, (derived_pnl, position_id))

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
            return { "error" : f"Failed to close position: {str(e)}"}

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
                return { "error" : "Failed to connect tp the database" }
        except Exception as e:
            print(str(e))
            return { "error" : f"Error during retrieving a previous position: {str(e)}"}

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
            return {"error" : str(e)}

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





