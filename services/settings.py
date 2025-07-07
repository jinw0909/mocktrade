import asyncio
import os
from pprint import pformat

import httpx
from utils.connections import MySQLAdapter
import traceback
from datetime import datetime, timedelta
from pytz import timezone
import logging
from utils.symbols import symbols as SYMBOL_CFG
from utils.connection_manager import manager
from utils.local_redis import update_position_status_per_user, update_order_status_per_user, update_balance_status_per_user
from utils.make_error import MakeErrorType
import requests
from utils.symbols import symbols as SYMBOL_CACHE

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

    def update_precision(self):
        PRECISION_API = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        conn = None
        cursor = None
        count = 0
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT `symbol` FROM `mocktrade`.`symbol`")

            # 1) load existing symbols into a set for quick lookup
            existing = {row["symbol"] for row in cursor.fetchall()}

            # 2) fetch and parse Binance exchangeInfo
            resp = requests.get(PRECISION_API, timeout=10)
            resp.raise_for_status()
            info = resp.json()

            # 3) iterate over each symbol in the API payload
            for entry in info.get("symbols", []):
                full_sym = entry.get("symbol", "")
                if not full_sym.endswith("USDT"):
                    continue

                base = full_sym[:-4] # strip off 'USDT'
                if base in existing:
                    # already in your table -> skip
                    continue

                price_prec = entry.get("pricePrecision")
                qty_prec = entry.get("quantityPrecision")

                # 4) insert the new row
                cursor.execute("""
                    INSERT INTO `mocktrade`.`symbol` (symbol, price, qty)
                    VALUES (%s, %s, %s)
                """, (base, price_prec, qty_prec))
                logger.info(f"Added new symbol {base}: price={price_prec}, qty={qty_prec}")

                count += 1

            # 5) commit if all went well
            conn.commit()

            return {
                "status" : "success",
                "message": f"total {count} number of precision info added to the symbol table"
            }

        except Exception as e:
            if conn:
                conn.rollback()
            logger.warning(f"Failed to update precision from the binance API: {e!r}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    def get_symbol_precision_map(self) -> dict[str, dict[str, int]]:
        """
        Returns a dict of { base_symbol: {"price" : price_prec, "qty": qty_prec}, ...}
        by querying mocktrade.symbol
        """
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT symbol, price, qty FROM mocktrade.symbol")
            rows = cursor.fetchall()

            out: dict[str, dict[str, int]] = {}
            for r in rows:
                try:
                    base = r["symbol"]
                    price = int(r["price"])
                    qty = int(r["qty"])
                except (KeyError, TypeError, ValueError):
                    continue
                out[base] = {"price": price, "qty": qty}
            return out
        except Exception as e:
            if conn:
                conn.rollback()
            logger.warning(f"Failed to generate a symbol precision cache {e!r}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def reload_symbol_cache(self) -> None:
        """
        Overwrite the global SYMBOL_CACHE in place with fresh values
        """
        logger.info("Reloading Symbol Cache")
        fresh = self.get_symbol_precision_map()
        SYMBOL_CACHE.clear()
        SYMBOL_CACHE.update(fresh)






