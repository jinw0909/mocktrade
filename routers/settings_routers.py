import traceback

from fastapi import APIRouter
from starlette.responses import JSONResponse
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from utils.settings import MySQLAdapter
from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import Optional

from scheduler import update_all_prices
from utils.price_cache import prices as price_cache

router = APIRouter()


@router.get('/test', summary='test router', tags=['SETTINGS API'])
async def api_test():
    # '''

    # '''
    mysql = MySQLAdapter()
    
    try:
        mysql.get_signal()
    except Exception as e:
        print(e)
    
    return JSONResponse(content = {"message": "Hello Test"}, status_code = 200) 
    

    return JSONResponse(content={"message": "Hello Test"}, status_code=200)


@router.post('/tpsl', summary='set tp/sl of a single order', tags=['SETTINGS API'])
async def api_tpsl(order_no: int, user_no: int, tp: float, sl: float):
    mysql = MySQLAdapter()
    responseMessage = ''

    try:
        userId = mysql.get_userId(order_no)
        if userId == 0 or userId != user_no:
            print('Invalid Request')
            responseMessage = "Invalid Request"
        else:
            updateResult = mysql.set_tpsl(order_no, tp, sl)
            print(updateResult)
            responseMessage = updateResult

    except Exception as e:
        print(e)
        responseMessage = f"Exception occurred: {str(e)}"

    return JSONResponse(content={"message": responseMessage}, status_code=200)


@router.post('/getUser', summary='get user test', tags=["SETTINGS API"])
async def api_getUser(user_no: int):
    mysql = MySQLAdapter()
    try:
        user = mysql.get_user(user_no)
        return user
    except Exception as e:
        print(e)
    # return JSONResponse(content = {"message": "reload test"}, status_Code = 200)


@router.post('/fetchPrice', summary='fetch crypto price', tags=["SETTINGS API"])
async def api_fetchPrice():
    mysql = MySQLAdapter()
    try:
        fetchResult = mysql.fetch_price()
        return fetchResult
    except Exception as e:
        print(e)
        return "Error during crypto price fetching"

@router.get('/fetchPriceFromRedis', summary='fetch crypto price from redis', tags=["SETTINGS API"])
async def api_fetchPriceFromRedis(symbol:str):
    mysql = MySQLAdapter()
    try:
        rd = mysql._get_redis()
        key = f'price:{symbol}USDT'
        price = rd.get(key)
        price = float(price.decode())
        return price
    except Exception:
        traceback.print_exc()
        return "Error during fetching price from redis"


@router.get('/updatePriceFromRedis', summary='update all coin prices from redis', tags=["SETTINGS API"])
async def api_updatePriceFromRedis():
    try:
        update_all_prices()
        print("completed updating from redis")
        return "completed updating from redis"
    except Exception:
        traceback.print_exc()
        return "Error during updating prices from redis"

@router.post('/modifyPriceCache', summary='modify the price of a symbol in price cache', tags=["SETTINGS API"])
async def api_modifyPriceCache(symbol: str, price: float):
    try:
        price_cache[symbol] = price
        return {"success": f"successfully updated the price of {symbol} to {price}"}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, detail=str(e))

@router.get('/priceCache', summary='show the current price cache', tags=["SETTINGS API"])
async def api_showPriceCache():
    try:
        return { "prices": price_cache }
    except Exception:
        print("Error, couldn't show the price cache")
        return {"error": "Failed to show the price cache" }