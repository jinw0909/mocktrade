from fastapi import APIRouter
from starlette.responses import JSONResponse
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from utils.trei import MySQLAdapter
from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import Optional
router= APIRouter()



@router.get('/btc-signal', summary='SIGNAL', tags=['SIGNAL API'])
async def api_select():

    
    """

   
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.get_signal()
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.post('/buy_limit_order', summary='ORDER', tags=['ORDER API'])
async def api_select(user_no: int, symbol: str, margin_type: int, leverage: int,price:float, usdt: Optional[float] = 0, amount: Optional[float] = 0,tp: Optional[float] = 0, sl: Optional[float] = 0):

    
    """

    
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.buy_limit_order(user_no,symbol,margin_type,leverage,price,usdt,amount,tp,sl)
            


    except Exception as e:
        print(e)


    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)




@router.post('/sell_limit_order', summary='ORDER', tags=['ORDER API'])
async def api_select(user_no: int, symbol: str, margin_type: int, leverage: int,price:float, usdt: Optional[float] = 0, amount: Optional[float] = 0,tp: Optional[float] = 0, sl: Optional[float] = 0):

    
    """
    
   
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.sell_limit_order(user_no,symbol,margin_type,leverage,price,usdt,amount,tp,sl)
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.post('/buy_market_order', summary='ORDER', tags=['ORDER API'])
async def api_select(user_no: int, symbol: str, margin_type: int, leverage: int, usdt: Optional[float] = 0, amount: Optional[float] = 0,tp: Optional[float] = 0, sl: Optional[float] = 0):

    
    """

   
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.buy_market_order(user_no,symbol,margin_type,leverage,usdt,amount,tp,sl)
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)




@router.post('/sell_market_order', summary='ORDER', tags=['ORDER API'])
async def api_select(user_no: int, symbol: str, margin_type: int, leverage: int, usdt: Optional[float] = 0, amount: Optional[float] = 0,tp: Optional[float] = 0, sl: Optional[float] = 0):

    
    """

   
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.sell_market_order(user_no,symbol,margin_type,leverage,usdt,amount,tp,sl)
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.post('/cancle_order', summary='ORDER', tags=['ORDER API'])
async def api_select(user_no: int,ordid:int):

    
    """

   
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.cancel_order(user_no,ordid)
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



@router.post('/close_position', summary='CLOSE POSITION', tags=['ORDER API'])
async def api_select(user_no: int,position_id:int):

    
    """

   
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.cancel_position(user_no,position_id)
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)




@router.post('/position_tp_sl', summary='POSITION TP SL', tags=['ORDER API'])
async def api_select(user_no: int,position_id:int,tp:float=0,sl:float=0):

    
    """

   
    """
    mysql=MySQLAdapter()
    
    try:    
        mysql.update_tpsl_position(user_no,position_id,tp,sl)
            


    except Exception as e:
        print(e)
        

    return JSONResponse(mysql.return_dict_data, status_code=mysql.status_code)



