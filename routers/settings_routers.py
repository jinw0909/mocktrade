import os.path
import traceback
import logging
from pprint import pformat

from fastapi import APIRouter
from starlette.responses import JSONResponse
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from utils.connections import MySQLAdapter
from services.settings import SettingsService
from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import Optional


router = APIRouter()
logger = logging.getLogger('uvicorn')

mysql = MySQLAdapter()
settings = SettingsService()

@router.post('/tpsl', summary='set tp/sl of a single order', tags=['SETTINGS API'])
async def api_tpsl(order_no: int, user_no: int, tp: float, sl: float):

    try:
        userId = settings.get_userId(order_no)
        if userId == 0 or userId != user_no:
            logger.error('Invalid Request')
            responseMessage = "Invalid Request"
        else:
            updateResult = settings.set_tpsl(order_no, tp, sl)
            logger.info(f"{updateResult}")
            responseMessage = updateResult

    except Exception as e:
        logger.exception("failed to set tp/sl orders")
        responseMessage = f"Exception occurred: {str(e)}"

    return JSONResponse(content={"message": responseMessage}, status_code=200)


@router.post('/getUser', summary='get user test', tags=["SETTINGS API"])
async def api_getUser(user_no: int):
    try:
        user = settings.get_user(user_no)
        return user
    except Exception as e:
        logger.exception("failed to get user")
    # return JSONResponse(content = {"message": "reload test"}, status_Code = 200)


@router.post('/fetchPriceFromExternal', summary='fetch crypto price from outer source', tags=["SETTINGS API"])
async def api_fetchPrice():
    try:
        fetchResult = settings.fetch_price()
        return fetchResult
    except Exception as e:
        logger.exception("failed to fetch prices")
        return "Error during crypto price fetching"

@router.get('/fetchPriceFromRedis', summary='fetch crypto price from redis', tags=["SETTINGS API"])
async def api_fetchPriceFromRedis(symbol:str):
    try:
        rd = mysql._get_redis()
        key = f'price:{symbol}USDT'
        price = rd.get(key)
        price = float(price.decode())
        return price
    except Exception:
        logger.exception("Error during fetching price from redis")
        return "Error during fetching price from redis"

@router.get('/createSymbolCache', summary='create the symbol cache', tags=["SETTINGS API"])
async def api_createSymbolCache():
    conn = None
    cursor = None
    OUTPUT_PATH = os.path.join(
        os.path.dirname(__file__),
        '..', 'utils', 'symbols.py'
    )
    try:
        conn = mysql._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT `symbol`, `price`, `qty` 
              FROM mocktrade.symbol
        """)
        rows = cursor.fetchall()
        # cursor.close()
        # conn.close()

        logger.info(f"the symbol table: {rows}")

        symbol_cache = {}
        for row in rows:
            symbol = row['symbol']
            price = row['price']
            qty = row['qty']
            try:
                pi = int(price)
                qi = int(qty)
            except (TypeError, ValueError):
                logger.warning(f"Skipping invalid row: {(symbol, price, qty)}")
                continue
            symbol_cache[symbol] = {"price": pi, "qty": qi}

        header = (
            "# THIS FILE IS AUTO‐GENERATED — do not edit by hand!\n"
            "from typing import Dict\n\n"
            "symbols: Dict[str, Dict[str, int]] = "
        )
        body = pformat(symbol_cache, indent=2)
        content = header + body + "\n"

        # write it out
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            f.write(content)

        print(f"Wrote {len(symbol_cache)} symbols to {OUTPUT_PATH!r}")
        return {
            "success":       True,
            "cached_symbols": len(symbol_cache),
            "module_path":   OUTPUT_PATH
        }

    except Exception:
        if conn:
            conn.rollback()
        logger.exception("Failed to create the symbols cache")
        raise
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass



