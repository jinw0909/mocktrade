import asyncio
import httpx
from utils.connections import MySQLAdapter
import traceback
from datetime import datetime, timedelta
from pytz import timezone
from utils.price_cache import prices as price_cache
import logging
from utils.symbols import symbols as SYMBOL_CFG
from utils.connection_manager import manager
from utils.local_redis import update_position_status_per_user, update_order_status_per_user, update_balance_status_per_user
from utils.make_error import MakeErrorType

logger = logging.getLogger(__name__)


class SettingsService(MySQLAdapter):
    def set_tpsl(self, order_no, tp, sl):
        conn = self._get_connection()
        try:
            if conn:
                with conn.cursor() as cursor:
                    sql = """
                        UPDATE mocktrade.order_history
                           SET tp = %s, 
                               sl = %s
                         WHERE id = %s
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
                        ORDER BY `insert_time`
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

    async def close_position(self, retri_id, symbol):
        closed_users = {}
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